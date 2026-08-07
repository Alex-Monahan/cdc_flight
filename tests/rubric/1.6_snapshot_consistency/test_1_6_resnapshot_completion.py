"""Rubric 1.6 — "the re-snapshot completed" must mean EVERY REQUESTED TABLE completed.

The reproducing tests for the worst defect the 1.6-1.8 review round found (Codex B1 =
Opus BLOCKER-1). Two independent halves of the same claim:

1. **The stop signal.** `Applier.snapshot_completed` used to become true when "at least
   one swap happened and no table is currently mid-snapshot". At a Debezium batch
   boundary that lands between table A's last record and table B's first, *no* table is
   mid-snapshot and A has swapped — so the supervisor stopped a two-table re-snapshot
   after one table. The authoritative signal is Debezium's ordered Initial Snapshot
   `COMPLETED` callback after every per-table terminal callback.

2. **The classification.** `resnapshot._finish_empty_tables` treated *every* requested
   table that had not swapped as "the source relation held no rows", and ran
   `DELETE FROM` against its live destination table. A table the engine never reached is
   not an empty table. The `still_owed` guard meant to catch that was dead code, because
   the same function unconditionally appended every pending table to `emptied`.

These run in the DEFAULT suite: they are the guard for a silent-destruction path, and a
guard that only runs in an opt-in lane is not a guard. The end-to-end multi-table
evidence lives in `test_1_6_resnapshot_multi_table.py`.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import DATASET, Lab, snap

from cdc_flight import destination as dest_mod
from cdc_flight import resnapshot as resnapshot_mod
from cdc_flight.assembler import UNIT_SNAPSHOT_CHUNK
from cdc_flight.config import DROP_LOG, DROP_REPLICATE
from cdc_flight.errors import EngineFailure
from cdc_flight.snapshot_completion import SnapshotCompletion

PIPELINE = "test_resnap_completion"


# --------------------------------------------------------------------------- #
# 1. the stop signal
# --------------------------------------------------------------------------- #
def test_one_table_finishing_is_not_the_snapshot_finishing(tmp_path):
    """A closed table chunk with no global `last` marker must NOT stop the engine.

    This is the exact interleaving of Opus BLOCKER-1: table A's chunk closes with
    `last_for_table=True` (the assembler closes it because a record for table B
    arrived), the commit group ends there, and table B has not entered the snapshot
    coordinator yet. `snapshot_completed` used to flip here.
    """
    lab = Lab(tmp_path / "stop.duckdb")
    lab.applier.snapshot_completion = SnapshotCompletion.full_snapshot(
        {"app.customers", "app.orders"}
    )
    lab.applier.snapshot_completion.observe_notification("STARTED", {})
    try:
        # Table A's whole image, closed as "last for this table" but NOT as
        # "last of the snapshot" — exactly what Debezium emits when another table
        # is still to come.
        lab.run(
            [
                snap("customers", 100, ident=1, value="a"),
                snap("customers", 100, ident=2, value="b", marker="last_in_data_collection"),
            ]
        )
        lab.applier.snapshot_completion.observe_notification(
            "TABLE_SCAN_COMPLETED",
            {
                "scanned_collection": "app.customers",
                "status": "SUCCEEDED",
                "total_rows_scanned": "2",
            },
        )
        assert lab.applier.snapshots.swaps == 1, "table A should have swapped"
        assert not lab.applier.snapshots.active, "no table is mid-snapshot here"
        assert lab.applier.snapshot_completed is False, (
            "the re-snapshot engine must not be told the snapshot is over: Debezium "
            "has not emitted its `snapshot=last` marker, so a later table may still "
            "be waiting to be scanned (Codex B1 / Opus BLOCKER-1)"
        )

        # Now table B arrives and IS the last of the snapshot.
        lab.run(
            [
                snap("orders", 100, ident=1, value="x"),
                snap("orders", 100, ident=2, value="y", marker="last"),
            ]
        )
        lab.applier.snapshot_completion.observe_notification(
            "TABLE_SCAN_COMPLETED",
            {
                "scanned_collection": "app.orders",
                "status": "SUCCEEDED",
                "total_rows_scanned": "2",
            },
        )
        lab.applier.snapshot_completion.observe_notification("COMPLETED", {})
        assert lab.applier.snapshot_completed is True, (
            "Debezium's ordered end-of-snapshot callback is the authoritative signal"
        )
    finally:
        lab.close()


def test_the_completion_flag_records_which_tables_the_engine_reached(tmp_path):
    """`snapshot_tables_seen` is the positive evidence the empty check needs."""
    lab = Lab(tmp_path / "seen.duckdb", full_snapshot=True)
    try:
        lab.run([snap("customers", 100, ident=1, value="a", marker="last_in_data_collection")])
        assert lab.applier.snapshot_tables_seen == {"app.customers"}
        assert lab.applier.snapshot_final_seen is False
    finally:
        lab.close()


# --------------------------------------------------------------------------- #
# 2. the classification
# --------------------------------------------------------------------------- #
def _destination_with_a_live_table(path, rows: int = 1000):
    con = duckdb.connect(str(path))
    dest_mod.ensure_control_schema(con)
    dest_mod.ensure_dataset(con, DATASET)
    con.execute(
        f"CREATE OR REPLACE TABLE {DATASET}.cdcflight_app_orders AS "
        f"SELECT i AS id, 'row-' || i AS name FROM generate_series(1, {rows}) t(i)"
    )
    con.execute(
        "INSERT INTO _cdc_flight.table_state (pipeline, source_schema, source_table, "
        "target_table, snapshot_state) VALUES (?, 'app', 'orders', "
        "'cdcflight_app_orders', 'awaiting_snapshot')",
        [PIPELINE],
    )
    return con


def test_an_unreached_table_is_never_classified_empty(tmp_path):
    """The destructive path: the engine stopped early, so nothing is proven empty.

    Before the fix this deleted every row of a healthy live destination table and
    wrote an audit row claiming "the source relation held no rows at the
    re-snapshot's consistent point". Nothing had verified that.
    """
    con = _destination_with_a_live_table(tmp_path / "unreached.duckdb")
    try:
        before = con.execute(
            f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
        ).fetchone()[0]
        assert before == 1000

        emptied = resnapshot_mod.finish_verified_empty_tables(
            con,
            pipeline=PIPELINE,
            dataset=DATASET,
            tables=[("app", "orders", "cdcflight_app_orders")],
            done=set(),
            evidence=resnapshot_mod.EmptinessEvidence(
                # The engine did NOT reach a global end of snapshot.
                snapshot_phase_ended=False,
                tables_seen=set(),
                source_empty_at={},
                wal_lsn=None,
            ),
        )

        assert emptied == [], "an unreached table must not be classified empty"
        after = con.execute(
            f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
        ).fetchone()[0]
        assert after == 1000, "the live destination table must be untouched"
        state = con.execute(
            "SELECT snapshot_state FROM _cdc_flight.table_state WHERE pipeline = ?",
            [PIPELINE],
        ).fetchone()[0]
        assert state == "awaiting_snapshot", "the table is still owed a snapshot"
    finally:
        con.close()


def test_a_table_the_engine_scanned_but_did_not_swap_is_never_classified_empty(tmp_path):
    """Records arrived for the table, so "no records means empty" is false for it."""
    con = _destination_with_a_live_table(tmp_path / "partial.duckdb")
    try:
        emptied = resnapshot_mod.finish_verified_empty_tables(
            con,
            pipeline=PIPELINE,
            dataset=DATASET,
            tables=[("app", "orders", "cdcflight_app_orders")],
            done=set(),
            evidence=resnapshot_mod.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen={"app.orders"},          # it produced records
                source_empty_at={"app.orders": 0},   # and the source says empty NOW
                wal_lsn=123456,
            ),
        )
        assert emptied == []
        assert con.execute(
            f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
        ).fetchone()[0] == 1000
    finally:
        con.close()


def test_a_table_the_source_still_holds_rows_for_is_never_classified_empty(tmp_path):
    con = _destination_with_a_live_table(tmp_path / "nonempty.duckdb")
    try:
        emptied = resnapshot_mod.finish_verified_empty_tables(
            con,
            pipeline=PIPELINE,
            dataset=DATASET,
            tables=[("app", "orders", "cdcflight_app_orders")],
            done=set(),
            evidence=resnapshot_mod.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={"app.orders": 4},  # the source disagrees
                wal_lsn=123456,
            ),
        )
        assert emptied == []
        assert con.execute(
            f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
        ).fetchone()[0] == 1000
    finally:
        con.close()


def test_a_verified_empty_table_is_emptied_and_fenced_at_the_verified_lsn(tmp_path):
    """Positive evidence on all three counts: this one really is empty."""
    con = _destination_with_a_live_table(tmp_path / "empty.duckdb")
    try:
        emptied = resnapshot_mod.finish_verified_empty_tables(
            con,
            pipeline=PIPELINE,
            dataset=DATASET,
            tables=[("app", "orders", "cdcflight_app_orders")],
            done=set(),
            evidence=resnapshot_mod.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={"app.orders": 0},
                wal_lsn=987654,
            ),
        )
        assert emptied == ["app.orders"]
        assert con.execute(
            f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
        ).fetchone()[0] == 0
        state, lsn = con.execute(
            "SELECT snapshot_state, snapshot_lsn FROM _cdc_flight.table_state "
            "WHERE pipeline = ?",
            [PIPELINE],
        ).fetchone()
        assert state == "complete"
        # The watermark is the LSN sampled BEFORE the emptiness was verified, never a
        # value read from a slot that may have advanced: a transaction that commits
        # after the verification must be applied, not fenced.
        assert lsn == 987654
        events = con.execute(
            "SELECT event, rows_removed FROM _cdc_flight.table_events WHERE pipeline = ?",
            [PIPELINE],
        ).fetchall()
        assert events == [("resnapshot_empty", 1000)]
    finally:
        con.close()


@pytest.mark.parametrize(
    "drop_mode, expected_event_applied, expect_table",
    [(DROP_LOG, False, True), (DROP_REPLICATE, True, False)],
)
def test_final_source_absence_applies_policy_after_quarantine(
    tmp_path, drop_mode, expected_event_applied, expect_table
):
    """A stale recreate plan cannot choose destruction before final source evidence."""
    con = _destination_with_a_live_table(
        tmp_path / f"source_missing_{drop_mode}.duckdb", rows=3
    )
    try:
        logged, dropped = resnapshot_mod.finish_source_missing_tables(
            con,
            pipeline=PIPELINE,
            dataset=DATASET,
            tables=[("app", "orders", "cdcflight_app_orders")],
            done=set(),
            evidence=resnapshot_mod.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={},
                wal_lsn=123456,
                source_missing={"app.orders"},
            ),
            drop_mode=drop_mode,
        )
        assert logged == (["app.orders"] if drop_mode == DROP_LOG else [])
        assert dropped == (["app.orders"] if drop_mode == DROP_REPLICATE else [])
        assert (
            con.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [DATASET, "cdcflight_app_orders"],
            ).fetchone()[0]
            == int(expect_table)
        )
        if expect_table:
            assert con.execute(
                f"SELECT count(*) FROM {DATASET}.cdcflight_app_orders"
            ).fetchone()[0] == 3
            assert con.execute(
                "SELECT snapshot_state FROM _cdc_flight.table_state WHERE pipeline = ?",
                [PIPELINE],
            ).fetchone()[0] == "complete"
        else:
            assert con.execute(
                "SELECT count(*) FROM _cdc_flight.table_state WHERE pipeline = ?",
                [PIPELINE],
            ).fetchone()[0] == 0
        assert con.execute(
            "SELECT event, applied FROM _cdc_flight.table_events WHERE pipeline = ?",
            [PIPELINE],
        ).fetchall() == [("dropped", expected_event_applied)]
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 2b. an entirely empty capture set has no record for the marker to ride on
# --------------------------------------------------------------------------- #
class _FakeApplier:
    def __init__(self, final_seen: bool, seen: set[str]):
        # The closed row domain is explicit even in this callback-free fake. An empty
        # `seen` set means no rows were observed, not that every table name is valid.
        expected = set(seen) or {"app.customers"}
        self.completion = SnapshotCompletion.full_snapshot(expected)
        # A committed snapshot row is legal only after Debezium's STARTED callback.
        self.completion.observe_notification("STARTED", {})
        units = [
            SimpleNamespace(
                kind=UNIT_SNAPSHOT_CHUNK,
                schema=qualified.split(".")[0],
                table=qualified.split(".")[1],
                snapshot_last=final_seen,
                fenced=False,
                event_count=1,
            )
            for qualified in seen
        ]
        if final_seen and not units:
            units = [
                SimpleNamespace(
                    kind=UNIT_SNAPSHOT_CHUNK,
                    schema="app",
                    table="customers",
                    snapshot_last=True,
                    fenced=False,
                    event_count=1,
                )
            ]
        self.completion.observe_committed_group(units, snapshot_active=not final_seen)


def test_debeziums_ordered_callback_ends_the_snapshot_phase():
    applier = _FakeApplier(True, {"app.customers"})
    applier.completion = SnapshotCompletion.full_snapshot({"app.customers"})
    applier.completion.observe_notification("STARTED", {})
    applier.completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )
    applier.completion.observe_notification("COMPLETED", {})
    assert resnapshot_mod.snapshot_phase_ended(applier.completion) is True


def test_an_empty_table_ends_from_its_terminal_callback_not_streaming():
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )
    completion.observe_notification("COMPLETED", {})
    assert resnapshot_mod.snapshot_phase_ended(completion) is True


def test_an_interrupted_engine_never_counts_as_an_ended_snapshot_phase():
    for _stop_reason in ("max_seconds", "work_done", "hung", "engine_error", "source_dark"):
        completion = _FakeApplier(False, set()).completion
        assert resnapshot_mod.snapshot_phase_ended(completion) is False
    # ... and neither does a run that scanned SOMETHING but never saw the marker.
    assert resnapshot_mod.snapshot_phase_ended(
        _FakeApplier(False, {"app.customers"}).completion
    ) is False


# --------------------------------------------------------------------------- #
# 3. the guard that used to be dead code
# --------------------------------------------------------------------------- #
def test_still_owed_is_reachable_and_raises(tmp_path):
    """`still_owed` was provably `[]` for every input, so its `EngineFailure` was dead.

    Now that a table can end a re-snapshot neither swapped nor verified-empty, the
    guard has a live branch: the run must fail, and the tables must stay owed.
    """
    con = _destination_with_a_live_table(tmp_path / "owed.duckdb")
    try:
        outcome = resnapshot_mod.ResnapshotOutcome(
            requested=["app.customers", "app.orders"],
            swapped=["app.customers"],
            emptied=[],
        )
        with pytest.raises(EngineFailure) as raised:
            resnapshot_mod.assert_every_requested_table_completed(outcome)
        assert "app.orders" in str(raised.value)
        assert "app.customers" not in str(raised.value)
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# 4. the two readings of the consistent point (Codex B2 / Opus Q1)
# --------------------------------------------------------------------------- #
def test_agreeing_readings_of_the_consistent_point_are_accepted():
    assert resnapshot_mod.agree_on_consistent_point(5000, 5000) == 5000
    assert resnapshot_mod.agree_on_consistent_point(5000, None) == 5000


def test_disagreeing_readings_of_the_consistent_point_hard_fail():
    """`min()` traded a 1.6 violation for a 1.2 one. Both reviewers: hard-fail.

    Fencing too low re-applies, which *duplicates* on a keyless table and breaks
    rubric 1.2's exactly-once claim. A disagreement also falsifies the assumption
    that either reading identifies the exported snapshot at all, so neither value is
    a boundary anything may rest on.
    """
    with pytest.raises(EngineFailure) as raised:
        resnapshot_mod.agree_on_consistent_point(5000, 9000)
    message = str(raised.value)
    assert "5000" in message and "9000" in message
    assert "DISAGREE" in message


def test_no_reading_at_all_is_not_a_consistent_point():
    assert resnapshot_mod.agree_on_consistent_point(None, None) is None
