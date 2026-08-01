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


# --------------------------------------------------------------------------- #
# Codex r2 BLOCKER-1 — the exact empty-source-table reproduction
# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_reset_rebuilds_a_table_the_source_has_emptied(tmp_path_factory, postgres_cluster):
    """The shape that made the first journalled reset certify stale rows as success.

    A source relation with **zero rows** emits no Debezium snapshot records, so
    `SnapshotCoordinator` never opens a shadow for it and never swaps one in — the
    destination table keeps exactly what it had. The first cut put every captured table
    at lifecycle `none`, recorded `tables_marked=0`, and accepted both `none` and a
    missing row as finished, so `--reset-state` cleared its own journal, exited
    `ok: true`, and left rows the source had truncated away (Codex r2 BLOCKER-1,
    reproduced against this cluster).

    The obligation is now real (`awaiting_snapshot` for every captured table) and
    completion demands `complete` for each of them; a table proven empty at the source
    reaches `complete` through the same three-fact `EmptinessEvidence` check the
    blocking re-snapshot has always used, so the reset still converges in ONE run.
    """
    box = Sandbox("reset_empty", tmp_path_factory.mktemp("sbx_reset_empty"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        before = box.scalar(f"SELECT count(*) FROM {box.table('cdcflight_app_documents')}")
        assert before > 0, "the fixture must start with rows to make the test meaningful"

        # Emptied at the source WITHOUT the pipeline running, so no truncate/delete
        # event ever reaches the destination: only a rebuild can notice.
        box.sql("TRUNCATE app.documents")
        assert box.pg_query("SELECT count(*) FROM app.documents")[0][0] == 0

        summary = box.run(reset_state=True, max_seconds=200, timeout=280)
        assert summary["ok"] is True, summary
        assert summary.get("recovery_cleared"), (
            f"the reset did not finish its own journal: {summary}"
        )
        assert box.scalar(
            f"SELECT count(*) FROM {box.table('cdcflight_app_documents')}"
        ) == 0, "the reset certified stale rows as a fresh image"
        assert "app.documents" in (summary.get("verified_empty_after_snapshot") or []), (
            "the table must be completed through POSITIVE verified-empty evidence, not "
            f"by a predicate that accepts `none`: {summary}"
        )
        states = dict(
            box.duck_query(
                "SELECT source_table, snapshot_state FROM _cdc_flight.table_state"
            )
        )
        assert states.get("documents") == "complete", states
        assert not [t for t, s in states.items() if s != "complete"], states
        _equal_to_source(box, "after a reset over an emptied source table")
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_a_reset_of_an_entirely_empty_source_clears_in_one_run(
    tmp_path_factory, postgres_cluster
):
    """The other end of the empty-table shape: EVERY captured relation is empty.

    Requiring a resume point of every recovery was right — it is what says the rebuilt
    image was handed over to a stream — but an entirely empty capture set emits zero
    Debezium records, so the applier commits zero groups and writes no resume point at
    all. `--reset-state` then failed with `recovery_uncleared`, and so did every run
    after it, because no new fact could ever produce a commit group: deterministically
    non-convergent, which is a 4.7 failure even though it fails closed (Codex r3
    MAJOR-1, reproduced against six truncated tables).

    `record_empty_handoff` writes the durable position from the fence the emptiness was
    proven at. That claims nothing untrue: the fence was sampled before the counts, on
    its own statement, and every captured relation then counted zero under REPEATABLE
    READ — so no transaction at or below it left a row anywhere in the capture set.
    """
    box = Sandbox("reset_all_empty", tmp_path_factory.mktemp("sbx_all_empty"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        # One statement, because the fixture's tables are FK-linked.
        box.sql(
            "TRUNCATE app.customers, app.orders, app.sensor_readings, app.documents, "
            "app.wide_types, app.audit_log CASCADE"
        )

        summary = box.run(reset_state=True, max_seconds=200, timeout=280)
        assert summary["ok"] is True, summary
        assert summary.get("recovery_cleared"), (
            f"an all-empty reset did not converge in one run: {summary}"
        )
        assert summary.get("empty_handoff_lsn"), (
            f"no durable handoff point was recorded for the empty capture set: {summary}"
        )
        states = dict(
            box.duck_query(
                "SELECT source_table, snapshot_state FROM _cdc_flight.table_state"
            )
        )
        assert states and not [t for t, s in states.items() if s != "complete"], states
        assert box.duck_query("SELECT count(*) FROM _cdc_flight.recovery_state")[0][0] == 0
        for target in ("cdcflight_app_customers", "cdcflight_app_documents"):
            assert box.scalar(f"SELECT count(*) FROM {box.table(target)}") == 0

        # ... and the NEXT ordinary run is a plain success, not a re-armed recovery loop.
        again = box.run(max_seconds=180)
        assert again["ok"] is True, again
        assert not again.get("recovery_still_armed"), again

        # ... and CDC works from there.
        box.sql("INSERT INTO app.customers (name, email) VALUES ('after-empty', 'a@x.com')")
        assert box.run(max_seconds=180)["ok"] is True
        assert box.scalar(
            f"SELECT count(*) FROM {box.table('cdcflight_app_customers')}"
        ) == 1
    finally:
        box.cleanup()
        box.reseed()
