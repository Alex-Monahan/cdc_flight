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
from conftest import Sandbox

#: Whole module `slow`: the fixture is two pipeline runs over 400 preloaded rows with
#: 60-row snapshot chunks, and the default suite already carries a guard for the same
#: mechanism (`tests/1.1_exactly_once_pk/test_1_1_fault_interleavings.py::
#: test_a_crash_during_the_snapshot_phase_leaves_no_partial_table`). What is *only* here
#: is the full content comparison and the torn swap, and both are worth their minute.
pytestmark = pytest.mark.slow

PRELOAD = 400


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
        # Small chunks so the snapshot spans several commit groups and the fault lands
        # with at least one chunk already durable in a shadow table.
        crashed = box.run(
            reset_state=True,
            max_seconds=150,
            expect_success=False,
            extra_env={
                "CDC_SNAPSHOT_CHUNK_EVENTS": "60",
                "CDC_FAULT_INJECT": "post_commit_pre_ack:2",
            },
        )
        mid_crash_tables = _dest(box) if _dest_exists(box) else set()
        mid_crash_shadows = _shadows(box)
        recovered = box.run(max_seconds=200, extra_env={"CDC_SNAPSHOT_CHUNK_EVENTS": "60"})
        yield {
            "box": box,
            "crashed": crashed,
            "recovered": recovered,
            "mid_crash_tables": mid_crash_tables,
            "mid_crash_shadows": mid_crash_shadows,
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


def test_the_partial_snapshot_was_never_visible(interrupted):
    """Committed chunks live in a shadow, so the live table shows nothing partial."""
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
