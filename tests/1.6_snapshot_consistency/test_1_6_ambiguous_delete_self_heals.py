"""Rubric 4.7 — an undecidable fold must self-heal, not fail identically for ever.

`AmbiguousDelete` was the right *immediate* answer (refuse the group rather than commit a
guess; the rubric's own scale puts an error above silent loss) and the wrong *eventual*
one: the group rolls back, Invariant O replays the transaction, the fold hits the same
ambiguity, the run fails again. For ever. That is a permanent manual-intervention case,
which is exactly what rubric 4.7 scores.

The repair uses 1.6's machinery. The exception now names its table; the applier records a
`awaiting_snapshot` request on the **independent** connection so it survives the rollback
that must still happen; and the next run re-snapshots that table. Termination is not a
hope: the re-snapshot's consistent point `C` is taken *after* the offending transaction
(we had already received it, so it was already in WAL), so the per-table watermark fences
the transaction that cannot be folded. One re-snapshot, always.

The honest cost, asserted here so it cannot be quietly forgotten: a re-snapshot replaces
**current state**. The individual change events of the fenced span are never delivered, so
a changelog (rubric 8.2) has a discontinuity there. That is not a shortcut - the ambiguity
*was* that the events did not say what they did, so there is no per-event history to
recover. `table_events` records where the discontinuity is.
"""

from __future__ import annotations

import pytest
from applier_lab import Lab, data, end
from conftest import Sandbox

TABLE = "customers"

# The one shape the fold genuinely cannot decide, copied from
# `tests/1.4_pk_updates/test_1_4_fold_counterexamples.py` where it is derived: two
# different in-group rows wear one key and the delete's before-image matches neither.
# Postgres cannot produce it (a deferrable key forces REPLICA IDENTITY FULL), which is
# why reaching it means the input is not what the fold is entitled to assume.


def _row(ident: int, name: str) -> dict:
    return {"id": ident, "name": name}


def insert(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, after=_row(ident, name), op="c")


def delete(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, before=_row(ident, name), op="d")


def keyed(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return insert(txn_id, order, lsn, ident, name)


def txn(number: str, events: list, *, table: str = "app.customers") -> list:
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, {table: len(events)})]


def _lab(tmp_path) -> Lab:
    return Lab(tmp_path / "ambiguous.duckdb")


def test_the_exception_names_the_table_it_could_not_fold(tmp_path):
    """Without this the recovery cannot know what to rebuild."""
    from cdc_flight.errors import AmbiguousDelete

    box = _lab(tmp_path)
    try:
        box.run(txn("1", [keyed("1", 1, 100, 1, "a"), keyed("1", 2, 101, 2, "b")]))
        with pytest.raises(AmbiguousDelete) as raised:
            box.run(
                txn(
                    "2",
                    [
                        delete("2", 1, 200, 1, "a"),
                        insert("2", 2, 200, 3, "a"),
                        delete("2", 3, 201, 2, "b"),
                        insert("2", 4, 201, 3, "b"),
                        delete("2", 5, 202, 3, "ghost"),
                    ],
                )
            )
        assert raised.value.source_schema == "app"
        assert raised.value.source_table == TABLE
        assert raised.value.target == f"cdcflight_app_{TABLE}"
    finally:
        box.close()


def test_the_undecidable_fold_queues_a_resnapshot_that_survives_the_rollback(tmp_path):
    """The group still rolls back; the request to rebuild must not roll back with it."""
    from cdc_flight.errors import AmbiguousDelete

    box = _lab(tmp_path)
    try:
        box.run(txn("1", [keyed("1", 1, 100, 1, "a"), keyed("1", 2, 101, 2, "b")]))
        before = box.rows(f"cdcflight_app_{TABLE}", "id, name")
        with pytest.raises(AmbiguousDelete):
            box.run(
                txn(
                    "2",
                    [
                        delete("2", 1, 200, 1, "a"),
                        insert("2", 2, 200, 3, "a"),
                        delete("2", 3, 201, 2, "b"),
                        insert("2", 4, 201, 3, "b"),
                        delete("2", 5, 202, 3, "ghost"),
                    ],
                )
            )
        # (1) nothing was committed: the destination still holds the pre-group state.
        assert box.rows(f"cdcflight_app_{TABLE}", "id, name") == before
        # (2) and the rebuild request DID survive that rollback.
        state = box.q(
            "SELECT source_table, snapshot_state FROM _cdc_flight.table_state "
            "WHERE source_table = ?",
            [TABLE],
        )
        assert state == [(TABLE, "awaiting_snapshot")], state
        assert box.applier.ambiguous_resnapshots_queued == 1
        # (3) and an operator is told, with the honesty note about history attached.
        alerts = box.q(
            "SELECT code, message FROM _cdc_flight.alerts WHERE code = 'ambiguous_delete_resnapshot'"
        )
        assert alerts, box.q("SELECT code FROM _cdc_flight.alerts")
        assert "no human action is required" in alerts[0][1]
        assert "history" in alerts[0][1]
    finally:
        box.close()


def test_the_permanent_failure_is_still_available_deliberately(tmp_path):
    """`CDC_AMBIGUOUS_RESNAPSHOT=0` keeps the old behaviour for anyone who wants it."""
    from cdc_flight.errors import AmbiguousDelete

    box = Lab(tmp_path / "ambiguous_off.duckdb", resnapshot_on_ambiguity=False)
    try:
        box.run(txn("1", [keyed("1", 1, 100, 1, "a"), keyed("1", 2, 101, 2, "b")]))
        with pytest.raises(AmbiguousDelete):
            box.run(
                txn(
                    "2",
                    [
                        delete("2", 1, 200, 1, "a"),
                        insert("2", 2, 200, 3, "a"),
                        delete("2", 3, 201, 2, "b"),
                        insert("2", 4, 201, 3, "b"),
                        delete("2", 5, 202, 3, "ghost"),
                    ],
                )
            )
        assert box.applier.ambiguous_resnapshots_queued == 0
        state = box.q(
            "SELECT snapshot_state FROM _cdc_flight.table_state WHERE source_table = ?",
            [TABLE],
        )
        assert state and state[0][0] != "awaiting_snapshot", state
    finally:
        box.close()


@pytest.mark.slow
def test_a_queued_resnapshot_is_carried_out_and_history_is_marked(
    tmp_path_factory, postgres_cluster
):
    """The other half, against a real pipeline: the queue is acted on, once.

    The `awaiting_snapshot` row an `AmbiguousDelete` leaves behind is the same row 1.5's
    `recreated` action and 1.8's recovery leave, so this is the end-to-end statement that
    a *queued* rebuild happens automatically, lands a correct image, and records where the
    per-event history is discontinuous.
    """
    box = Sandbox(
        "ambiguous_e2e", tmp_path_factory.mktemp("sbx_ambiguous"), postgres_cluster
    )
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        box.sql("INSERT INTO app.customers (name, email) VALUES ('ambig', 'a@x.com')")
        box.run(max_seconds=150)

        # Exactly what the applier writes on an undecidable fold.
        box.duck_write(
            "UPDATE _cdc_flight.table_state SET snapshot_state = 'awaiting_snapshot' "
            "WHERE source_table = 'customers'"
        )
        healed = box.run(max_seconds=200)
        assert healed["ok"] is True, healed
        assert healed["resnapshot_swapped"] == ["app.customers"], healed

        source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
        dest = {
            str(r[0])
            for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
        }
        assert dest == source

        # And the discontinuity is recorded rather than implied.
        marks = box.duck_query(
            "SELECT event, source_table, lsn, detail FROM _cdc_flight.table_events "
            "WHERE event = 'resnapshot' ORDER BY commit_id DESC"
        )
        assert marks, box.duck_query("SELECT DISTINCT event FROM _cdc_flight.table_events")
        assert str(marks[0][1]) == "customers"
        assert int(marks[0][2]) == healed["resnapshot_consistent_lsn"]
        assert "current state" in str(marks[0][3])

        # It must not loop: the table is no longer owed a snapshot.
        again = box.run(max_seconds=150)
        assert again["ok"] is True
        assert "resnapshot_swapped" not in again, again.get("resnapshot_swapped")
    finally:
        box.cleanup()
        box.reseed()


def test_the_fence_argument_that_makes_it_terminate():
    """Why one re-snapshot is always enough, as an executable statement.

    The offending transaction has already been delivered, so its commit LSN is already in
    WAL when the re-snapshot starts; `C` is taken after that, so the watermark fences it.
    """
    from cdc_flight.assembler import UNIT_TXN, CompleteUnit
    from cdc_flight.planner import GroupPlan

    consistent_point = 5_000
    plan = GroupPlan(
        con=None,
        commit_id=1,
        registry_of=lambda: None,
        snapshots=None,
        spill=None,
        truncate_mode="replicate",
        created_in_txn=set(),
        watermarks={"app.customers": consistent_point},
    )

    class _Event:
        schema = "app"
        table = "customers"

    offending = CompleteUnit(kind=UNIT_TXN, events=[], records=[], last_lsn=4_999)
    later = CompleteUnit(kind=UNIT_TXN, events=[], records=[], last_lsn=5_000)
    assert plan._below_watermark(_Event(), offending.last_lsn, plan.watermarks) is True
    assert plan._below_watermark(_Event(), later.last_lsn, plan.watermarks) is False
    # A different table in the same transaction is untouched: only the rebuilt table's
    # history is replaced.
    class _Other(_Event):
        table = "orders"

    assert plan._below_watermark(_Other(), offending.last_lsn, plan.watermarks) is False
