"""Rubric 1.7/1.8 — the two OPERATOR routes, killed mid-sequence, against real Postgres.

`--accept-orphan-offsets` and `--reset-state` are the two commands a human reaches for
when something has already gone wrong. Both used to be multi-step durable mutations that
nothing journalled *before* they started destroying evidence, and the last review round
proved that costs the same thing twice (Codex r1 BLOCKER-1 and MAJOR-4):

* orphan acceptance dropped the slot and unlinked `offsets.dat` and only then wrote the
  recovery journal. A hard exit in that gap left no resume row, no offsets file, no slot
  and no journal — which the next run reads as an ordinary `fresh_start`. Under a
  configured non-data `snapshot.mode` it then streams onto a destination nobody rebuilt:
  the operator's authorised rebuild silently did not happen;
* `--reset-state` was argued convergent without a journal. It is not: with the resume
  row deleted and a positioned slot over a populated destination, the next run's slot
  check returns the deliberate `no_durable_destination_row` refusal *before*
  `will_snapshot_everything` is computed, and repeating the flag does not drop that slot.
  The forced `snapshot.mode='initial'` was process-local too.

Both are journalled recoveries now, and this file is the evidence: a real `cdc-flight`
process, a real slot, `os._exit` at the anchor, then a restart **without repeating the
flag** and under `--snapshot-mode no_data`, and finally exact source/destination counts.

`no_data` is the load-bearing part of the configuration. A run that forgot the
obligation would start streaming and the counts would be short; a run that reads the
journal forces a data-reading mode because the journal says one is owed.

Slow lane: two baselines, four killed runs and four recoveries against a real cluster.
"""

from __future__ import annotations

import pytest
from conftest import Sandbox

ROWS = 15


def _equal_to_source(box: Sandbox, note: str) -> None:
    for source, target in (
        ("app.customers", "cdcflight_app_customers"),
        ("app.sensor_readings", "cdcflight_app_sensor_readings"),
    ):
        src = box.pg_query(f"SELECT count(*) FROM {source}")[0][0]
        dst = box.scalar(f"SELECT count(*) FROM {box.table(target)}")
        assert dst == src, f"{note}: {target} holds {dst} rows for {src} source rows"


def _extra_rows(box: Sandbox, tag: str) -> None:
    box.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            f"'{tag}-' || i, '{tag}-' || i || '@example.com' "
            f"FROM generate_series(1, {ROWS}) i",
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
            f"'{tag.upper()}', i * 0.5, 'C' FROM generate_series(1, {ROWS}) i",
        ],
        one_transaction=True,
    )


# --------------------------------------------------------------------------- #
# BLOCKER-1 — `--accept-orphan-offsets`
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def orphan_crash(tmp_path_factory, postgres_cluster):
    """Build an orphan, authorise the rebuild, and die at `recovery_requested`.

    `recovery_requested` fires the instant the journal row and the table obligation
    commit and BEFORE anything is destroyed — which is exactly the cut that used to be
    impossible to survive, because the destruction came first and there was no journal
    at all until it was over.
    """
    box = Sandbox("orphan_crash", tmp_path_factory.mktemp("sbx_orphan"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _extra_rows(box, "orph")
        box.run(max_seconds=150)
        assert box.offset_file.exists()

        # The orphan: the same offsets file, a destination that has never seen it.
        other = box.dir / "other.duckdb"
        orphan_env = {"CDC_DUCKDB_PATH": str(other)}

        box.clear_fired_fault()
        killed = box.run(
            max_seconds=180, timeout=260, expect_success=False,
            accept_orphan_offsets=True,
            extra_env={**orphan_env, "CDC_FAULT_INJECT": "recovery_requested:1"},
        )
        fired = box.fired_fault()

        # The restart does NOT repeat the flag, and asks for a snapshot mode that reads
        # no table data. Only the journal can make this run rebuild anything.
        finished = box.run(
            max_seconds=300, timeout=400, snapshot_mode="no_data",
            extra_env=orphan_env,
        )
        yield {
            "box": box, "killed": killed, "fired": fired, "finished": finished,
            "other": other,
        }
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_the_orphan_route_journals_before_it_destroys_anything(orphan_crash):
    fired = orphan_crash["fired"]
    assert fired is not None, orphan_crash["killed"].get("output", "")[-3000:]
    assert fired["point"] == "recovery_requested", fired
    assert orphan_crash["killed"]["returncode"] == 137
    # NOTHING was destroyed before the journal: the file the operator authorised
    # deleting is still there, because the intent is durable first.
    box: Sandbox = orphan_crash["box"]
    assert box.offset_file.exists(), (
        "the offsets file was deleted before the journal committed; that is the exact "
        "ordering BLOCKER-1 was about"
    )


@pytest.mark.slow
def test_the_next_run_finishes_the_orphan_rebuild_without_the_flag(orphan_crash):
    summary = orphan_crash["finished"]
    assert summary["ok"] is True, summary
    resumed = summary.get("recovery_resumed") or {}
    assert resumed.get("resumed_from") == "requested", summary
    assert summary.get("recovery_cleared"), summary
    # `--snapshot-mode no_data` was overridden BY THE JOURNAL, not by a local variable.
    assert summary.get("recovery_journal", {}).get("snapshot_mode") == "initial", summary


@pytest.mark.slow
def test_the_orphan_destination_equals_the_source_exactly(orphan_crash):
    box: Sandbox = orphan_crash["box"]
    original = box.env["CDC_DUCKDB_PATH"]
    box.env["CDC_DUCKDB_PATH"] = str(orphan_crash["other"])
    try:
        _equal_to_source(box, "after a crash inside the orphan rebuild")
        assert box.duck_query("SELECT count(*) FROM _cdc_flight.recovery_state")[0][0] == 0
        owed = box.duck_query(
            "SELECT source_schema, source_table, snapshot_state FROM "
            "_cdc_flight.table_state WHERE snapshot_state <> 'complete'"
        )
        assert owed == [], owed
    finally:
        box.env["CDC_DUCKDB_PATH"] = original


# --------------------------------------------------------------------------- #
# MAJOR-4 — `--reset-state`
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def reset_crash(tmp_path_factory, postgres_cluster):
    """A populated destination, a positioned slot, and a reset that dies half-way.

    The cut is `recovery_resume_point_deleted`: the state directory and the durable
    resume point are gone and the slot is NOT. That is precisely the shape the old
    convergence argument got wrong — the next run's slot check sees a positioned slot
    over a populated destination and returns `no_durable_destination_row`, a refusal,
    before it ever computes `will_snapshot_everything`.
    """
    box = Sandbox("reset_crash", tmp_path_factory.mktemp("sbx_reset"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _extra_rows(box, "rst")
        box.run(max_seconds=150)

        box.clear_fired_fault()
        killed = box.run(
            max_seconds=180, timeout=260, expect_success=False, reset_state=True,
            extra_env={"CDC_FAULT_INJECT": "recovery_resume_point_deleted:1"},
        )
        fired = box.fired_fault()
        # No flag, and a snapshot mode that reads nothing.
        finished = box.run(max_seconds=300, timeout=400, snapshot_mode="no_data")
        yield {"box": box, "killed": killed, "fired": fired, "finished": finished}
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_the_reset_route_is_journalled_and_resumable(reset_crash):
    fired = reset_crash["fired"]
    assert fired is not None, reset_crash["killed"].get("output", "")[-3000:]
    assert fired["point"] == "recovery_resume_point_deleted", fired
    assert reset_crash["killed"]["returncode"] == 137


@pytest.mark.slow
def test_the_next_run_finishes_the_reset_without_repeating_the_flag(reset_crash):
    summary = reset_crash["finished"]
    assert summary["ok"] is True, summary
    resumed = summary.get("recovery_resumed") or {}
    assert resumed.get("resumed_from") == "offsets_file_deleted", summary
    assert summary.get("recovery_journal", {}).get("decision") == "operator_reset", summary
    assert summary.get("recovery_cleared"), summary
    # The old argument's second false step: the forced data-reading mode was a local
    # variable, so this run — asked for `no_data` — would have streamed onto tables it
    # had just declared empty.
    assert summary.get("recovery_journal", {}).get("snapshot_mode") == "initial", summary


@pytest.mark.slow
def test_the_reset_destination_equals_the_source_exactly(reset_crash):
    box: Sandbox = reset_crash["box"]
    _equal_to_source(box, "after a crash inside --reset-state")
    assert box.duck_query("SELECT count(*) FROM _cdc_flight.recovery_state")[0][0] == 0


@pytest.mark.slow
def test_cdc_works_again_after_both_operator_routes(reset_crash):
    box: Sandbox = reset_crash["box"]
    box.sql("INSERT INTO app.customers (name, email) VALUES ('after-reset', 'a@x.com')")
    assert box.run(max_seconds=180)["ok"] is True
    assert box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} "
        "WHERE name = 'after-reset'"
    ) == 1
