"""Rubric 1.6 — an interrupted snapshot, and an interrupted swap.

RUBRIC_STATUS held 1.6 at 3 partly on this: *"If the connector stops during a snapshot,
the connector begins a new snapshot when it restarts"* (Debezium's own Postgres docs), so
with an append-style destination the abandoned partial snapshot is still there and the
restarted snapshot duplicates every row it re-reads.

The shadow-table swap (ADR 0001 D7) is what makes that safe, and safe *by construction*
rather than by event identity: the snapshot lands in `<table>__cdcf_tmp`, the swap is one
transaction, and a re-snapshot drops and rebuilds the shadow. So there are exactly two
dangerous instants, and both are tested here at an exact anchor rather than by racing a
`kill -9`:

* mid-snapshot, with a chunk committed into the shadow — the partial image must be
  invisible, and the restart must land every row once;
* between the `DROP` of the live table and the `RENAME` of the shadow over it — the one
  moment at which the live table does not exist. A crash here must leave the OLD table
  intact, which is only true if the destination honours DDL transactionally, which is
  probed per run rather than assumed (`transactional_ddl`).
"""

from __future__ import annotations

import pytest
from support.fixtures import Sandbox

#: Whole module `slow`: the fixture is two pipeline runs over 3 000 preloaded rows, and the default suite already carries a guard for the same
#: mechanism (`tests/rubric/1.1_exactly_once_pk/test_1_1_fault_interleavings.py::
#: test_a_crash_during_the_snapshot_phase_leaves_no_partial_table`). What is *only* here
#: is the full content comparison and the torn swap, and both are worth their minute.
pytestmark = pytest.mark.slow

#: Getting a fault to land *inside* the snapshot phase took three measurements, and they
#: are worth recording because each one is a real property of the applier:
#:
#: 1. 405 rows in 60-row chunks: **one** data group. A commit group is closed once per
#:    Debezium batch (`_handle` evaluates the triggers after feeding a whole batch), and
#:    405 records arrive in one batch. `applied_events: 420, batches: 1, returncode: 0`.
#: 2. Same, plus `CDC_COMMIT_MAX_EVENTS=60`: still one group, for the same reason - the
#:    trigger is *checked* once per batch, so a smaller threshold changes nothing.
#: 3. 3 000 rows, `CDC_COMMIT_MAX_EVENTS=1000`: STILL one group, and this is the
#:    interesting one. A group only holds *complete units*, and a snapshot chunk only
#:    closes at `snapshot.chunk.events`, a change of source table, or the end of the
#:    snapshot. At the default 50 000 the customers chunk is still open when the first
#:    batch is exhausted, so `self._group` is empty and there is nothing to trigger on.
#:
#: So all three have to be true at once: enough rows to fill more than one batch, chunks
#: small enough to *close* inside the first one, and a group trigger that fires on them.
#: Verified state at the crash: one shadow table, `table_state.snapshot_state='in_progress'`,
#: no live table at all.
PRELOAD = 3000
CHUNKED = {"CDC_COMMIT_MAX_EVENTS": "1000", "CDC_SNAPSHOT_CHUNK_EVENTS": "500"}


def _source(box: Sandbox) -> set[str]:
    return {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}


def _dest(box: Sandbox) -> set[str]:
    return {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }


def _shadows(box: Sandbox) -> list[str]:
    return [
        str(r[0])
        for r in box.duck_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name LIKE '%__cdcf_tmp'"
        )
    ]


def _shadow_counts(box: Sandbox) -> dict[str, int]:
    """Read durable row counts before recovery changes the interrupted state."""
    return {
        name: int(box.duck_query(f"SELECT count(*) FROM {box.table(name)}")[0][0])
        for name in _shadows(box)
    }


@pytest.fixture(scope="module")
def interrupted(tmp_path_factory, postgres_cluster):
    """Crash inside the snapshot phase, then restart and let it finish."""
    box = Sandbox(
        "interrupted_snapshot", tmp_path_factory.mktemp("sbx_interrupted"), postgres_cluster
    )
    try:
        box.reseed()
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT 'bulk-' || i, "
            f"'bulk-' || i || '@example.com' FROM generate_series(1, {PRELOAD}) i",
            one_transaction=True,
        )
        # The FIRST data group, not the second: with more than one batch of snapshot
        # records the first group commits ~2048 rows into shadow tables and the swap does
        # not happen until the batch carrying `snapshot_last` arrives. Crashing here is
        # therefore the state the item is about - a partial image, durably committed,
        # invisible.
        crashed = box.run(
            reset_state=True,
            max_seconds=200,
            expect_success=False,
            extra_env={**CHUNKED, "CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
        )
        mid_crash_tables = _dest(box) if _dest_exists(box) else set()
        mid_crash_shadows = _shadows(box)
        mid_crash_shadow_counts = _shadow_counts(box)
        mid_crash_fault = box.fired_fault()
        mid_crash_commit_log = box.duck_query(
            "SELECT commit_id, trigger, unit_count, event_count, fenced_units, "
            "tables_touched FROM _cdc_flight.commit_log ORDER BY commit_id"
        )
        recovered = box.run(max_seconds=240, extra_env=CHUNKED)
        yield {
            "box": box,
            "crashed": crashed,
            "recovered": recovered,
            "mid_crash_tables": mid_crash_tables,
            "mid_crash_shadows": mid_crash_shadows,
            "mid_crash_shadow_counts": mid_crash_shadow_counts,
            "mid_crash_fault": mid_crash_fault,
            "mid_crash_commit_log": mid_crash_commit_log,
        }
    finally:
        box.cleanup()
        box.reseed()


def _dest_exists(box: Sandbox) -> bool:
    return bool(
        box.duck_query(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            ["cdcflight_app_customers"],
        )
    )


def test_the_crash_landed_inside_the_snapshot_phase(interrupted):
    assert interrupted["crashed"]["returncode"] == 137, interrupted["crashed"]
    # The snapshot had not finished, so nothing was swapped into place yet.
    assert interrupted["crashed"].get("snapshot_swaps", 0) == 0, interrupted["crashed"]


def test_the_partial_snapshot_was_durable_and_invisible(interrupted):
    """Both halves. A shadow held rows; the live table did not exist.

    The first half is what makes the second one mean something: if nothing had been
    committed there would be no partial state to hide.
    """
    fault = interrupted["mid_crash_fault"]
    assert fault and fault["point"] == "post_commit_pre_ack" and fault["nth"] == 1, fault
    commits = interrupted["mid_crash_commit_log"]
    assert commits and commits[0][0] == 1 and commits[0][1] == "snapshot_chunk", commits
    assert commits[0][3] > 0 and commits[0][4] == 0, commits
    assert "cdcflight_app_customers" in commits[0][5], commits
    assert interrupted["mid_crash_shadows"], (
        "no shadow table survived the crash, so no partial image was ever durable and "
        "this scenario proves nothing"
    )
    assert all(count > 0 for count in interrupted["mid_crash_shadow_counts"].values())
    assert interrupted["mid_crash_tables"] == set(), interrupted["mid_crash_tables"]


def test_the_restart_lands_every_row_exactly_once(interrupted):
    box = interrupted["box"]
    assert interrupted["recovered"]["ok"] is True, interrupted["recovered"]
    assert _dest(box) == _source(box)
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
        f"{box.table('cdcflight_app_customers')}"
    )[0]
    assert total == distinct, f"{total - distinct} duplicated rows after a re-snapshot"
    assert total == len(_source(box)), (total, len(_source(box)))


def test_no_shadow_table_survives_a_finished_run(interrupted):
    assert _shadows(interrupted["box"]) == []


# --------------------------------------------------------------------------- #
# the swap itself
# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_crash_between_the_drop_and_the_rename_keeps_the_old_table(
    tmp_path_factory, postgres_cluster
):
    """The one instant at which the live table does not exist.

    This is the fault that would turn a backfill into an outage if the swap were not
    transactional, and it is the reason `probe_transactional_ddl` exists. Asserted on a
    table that already had rows, so "the old table survived" is a statement about
    contents and not just about a name in `information_schema`.
    """
    box = Sandbox("swap_crash", tmp_path_factory.mktemp("sbx_swap_crash"), postgres_cluster)
    try:
        box.reseed()
        first = box.run(reset_state=True, max_seconds=150)
        assert first["ok"] is True and first["transactional_ddl"] is True, first
        before = _dest(box)
        assert before, "the baseline run landed nothing, so there is nothing to protect"

        # Force a re-snapshot, and crash inside its swap.
        box.duck_write(
            "UPDATE _cdc_flight.table_state SET snapshot_state = 'awaiting_snapshot' "
            "WHERE source_table = 'customers'"
        )
        crashed = box.run(
            max_seconds=150, expect_success=False, extra_env={"CDC_FAULT_INJECT": "swap:1"}
        )
        assert crashed["returncode"] == 137, crashed
        assert _dest(box) == before, (
            "the live table did not survive a crash between the DROP and the RENAME"
        )
        assert _shadows(box) == [], _shadows(box)

        # And the table is still owed a snapshot, so the next run finishes the job.
        owed = box.duck_query(
            "SELECT snapshot_state FROM _cdc_flight.table_state WHERE source_table = 'customers'"
        )
        assert owed and str(owed[0][0]) == "awaiting_snapshot", owed
        recovered = box.run(max_seconds=200)
        assert recovered["ok"] is True, recovered
        assert recovered["resnapshot_swapped"] == ["app.customers"], recovered
        assert _dest(box) == _source(box)
    finally:
        box.cleanup()
        box.reseed()
