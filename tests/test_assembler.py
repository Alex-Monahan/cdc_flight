"""Unit tests for the transaction assembler (ADR 0001 §3.2). No Postgres, no JVM.

The assembler is the component that makes rubric 1.3 structural: if it can be
made to emit a unit that is not a whole Postgres transaction, then a commit group
can contain half of one and no amount of destination transactionality helps. Each
test below is one way of trying to make it do that.
"""

from __future__ import annotations

import pytest

from cdc_flight.assembler import (
    UNIT_CONTROL,
    UNIT_SNAPSHOT_CHUNK,
    UNIT_TXN,
    TransactionAssembler,
)
from cdc_flight.envelope import (
    KIND_DATA,
    KIND_HEARTBEAT,
    KIND_SNAPSHOT,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    PendingRecord,
)
from cdc_flight.errors import TransactionAssemblyError


def begin(txn: str, lsn: int = 100) -> PendingRecord:
    return PendingRecord(
        raw=object(), kind=KIND_TXN_BEGIN, topic="p.transaction", nbytes=10,
        txn_id=txn, lsn=lsn, txn_status="BEGIN",
    )


def end(txn: str, count: int, lsn: int = 200, per_table=None) -> PendingRecord:
    rec = PendingRecord(
        raw=object(), kind=KIND_TXN_END, topic="p.transaction", nbytes=10,
        txn_id=txn, lsn=lsn, txn_status="END", txn_event_count=count,
    )
    rec.txn_data_collections = dict(per_table or {})
    return rec


def data(txn: str, order: int, lsn: int, table: str = "customers") -> PendingRecord:
    return PendingRecord(
        raw=object(), kind=KIND_DATA, topic=f"p.{table}", nbytes=100, op="c",
        schema="app", table=table, lsn=lsn, txn_id=txn, total_order=order,
        after={"id": order},
    )


def snapshot(table: str, lsn: int = 50, marker: str = "true") -> PendingRecord:
    return PendingRecord(
        raw=object(), kind=KIND_SNAPSHOT, topic=f"p.{table}", nbytes=100, op="r",
        schema="app", table=table, lsn=lsn, snapshot=marker, after={"id": 1},
    )


def feed_all(assembler, records):
    units = []
    for rec in records:
        units.extend(assembler.feed(rec))
    return units


# --------------------------------------------------------------------------- #
# the boundary rule
# --------------------------------------------------------------------------- #
def test_a_transaction_is_emitted_only_on_a_verified_end():
    a = TransactionAssembler()
    assert feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102)]) == []
    assert a.buffered_events == 2

    units = a.feed(end("7", 2, lsn=103))
    assert len(units) == 1
    unit = units[0]
    assert unit.kind == UNIT_TXN
    assert unit.txn_id == "7"
    assert len(unit.events) == 2
    # BEGIN + 2 data + END all have to be acknowledged, in order.
    assert len(unit.records) == 4
    assert unit.records[-1].kind == KIND_TXN_END
    assert unit.last_lsn == 103


def test_an_event_count_mismatch_is_fatal():
    """A commit group may contain only transactions we can *prove* whole."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="END declares 2 events"):
        a.feed(end("7", 2))


def test_a_per_table_count_mismatch_is_fatal():
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101, "customers"), data("7", 2, 102, "orders")])
    with pytest.raises(TransactionAssemblyError, match=r"events for app\.orders"):
        a.feed(end("7", 2, per_table={"app.customers": 1, "app.orders": 2}))


def test_a_txid_change_without_an_end_is_fatal_not_a_fallback():
    """Rev 1 of the ADR treated this as a boundary. It is a consistency error:
    treating it as a boundary is how a partial Postgres transaction gets
    committed (Codex 2)."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="transaction id changed"):
        a.feed(data("8", 1, 102))


def test_a_begin_inside_an_open_transaction_is_fatal():
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="while transaction 7 is still open"):
        a.feed(begin("8"))


def test_a_data_event_without_transaction_metadata_is_fatal():
    """`provide.transaction.metadata=true` is mandatory, so its absence must be
    loud rather than silently degrading to per-batch commits."""
    a = TransactionAssembler()
    rec = data("7", 1, 101)
    rec.txn_id = None
    with pytest.raises(TransactionAssemblyError, match="no transaction id"):
        a.feed(rec)


def test_the_un_ended_tail_is_discarded_at_shutdown():
    """ADR §3.2: safe precisely because Invariant O means nothing was acked."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102)])
    assert a.discard_open_unit() == 2
    assert a.buffered_events == 0
    assert a.discarded_tail_events == 2


# --------------------------------------------------------------------------- #
# control units
# --------------------------------------------------------------------------- #
def test_a_heartbeat_outside_a_transaction_is_its_own_control_unit():
    """Rubric 4.4: a group of nothing but control units still advances the slot."""
    a = TransactionAssembler()
    hb = PendingRecord(raw=object(), kind=KIND_HEARTBEAT, topic="__debezium-heartbeat.p",
                       nbytes=5, lsn=500)
    units = a.feed(hb)
    assert len(units) == 1
    assert units[0].kind == UNIT_CONTROL
    assert units[0].events == []
    assert units[0].last_lsn == 500


def test_a_heartbeat_inside_a_transaction_is_carried_by_it():
    """Otherwise an unconditional heartbeat branch declares a half-buffered
    transaction complete and the resume point jumps over it (Opus M3)."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    hb = PendingRecord(raw=object(), kind=KIND_HEARTBEAT, topic="__debezium-heartbeat.p",
                       nbytes=5, lsn=150)
    assert a.feed(hb) == []
    units = a.feed(end("7", 1, lsn=200))
    assert len(units) == 1
    assert [r.kind for r in units[0].records] == [
        KIND_TXN_BEGIN, KIND_DATA, KIND_HEARTBEAT, KIND_TXN_END
    ]


def test_an_orphan_end_after_a_restart_is_a_control_unit_not_an_error():
    """MEASURED: on restart the transaction context is restored from the offset,
    so the first data event of the NEXT transaction makes Debezium emit an END
    for the already-finished one (`TransactionMonitor.java:98-105`). Treating
    that as a violation would make every restart fatal."""
    a = TransactionAssembler()
    units = a.feed(end("6", 3, lsn=99))
    assert len(units) == 1
    assert units[0].kind == UNIT_CONTROL
    assert units[0].events == []
    assert a.orphan_end_markers == 1


def test_a_transaction_can_open_without_a_begin():
    """Debezium suppresses the duplicate BEGIN when the context was restored
    (`TransactionMonitor.java:130-136`). The unit still cannot complete without a
    matching END."""
    a = TransactionAssembler()
    assert a.feed(data("7", 1, 101)) == []
    assert a.implicit_txn_opens == 1
    units = a.feed(end("7", 1, lsn=102))
    assert len(units) == 1 and units[0].kind == UNIT_TXN


# --------------------------------------------------------------------------- #
# snapshot chunks (ADR §3.5 / Opus B3)
# --------------------------------------------------------------------------- #
def test_snapshot_records_are_cut_into_chunks_by_size():
    """Snapshot records carry no BEGIN/END, so without an explicit chunk boundary
    the whole snapshot buffers in memory and nothing ever commits."""
    a = TransactionAssembler(snapshot_chunk_events=3)
    units = feed_all(a, [snapshot("customers", 50) for _ in range(7)])
    assert [u.kind for u in units] == [UNIT_SNAPSHOT_CHUNK, UNIT_SNAPSHOT_CHUNK]
    assert [len(u.events) for u in units] == [3, 3]
    assert a.buffered_events == 1


def test_a_snapshot_chunk_ends_when_the_table_changes():
    a = TransactionAssembler(snapshot_chunk_events=1000)
    units = feed_all(a, [snapshot("customers"), snapshot("customers"), snapshot("orders")])
    assert len(units) == 1
    assert units[0].table == "customers"
    assert units[0].snapshot_last_for_table is True


def test_the_last_snapshot_marker_closes_the_chunk_and_the_snapshot():
    a = TransactionAssembler()
    units = feed_all(a, [snapshot("customers"), snapshot("customers", marker="last")])
    assert len(units) == 1
    assert units[0].snapshot_last is True
    assert units[0].snapshot_last_for_table is True


def test_the_first_streaming_record_closes_the_snapshot_phase():
    """The swap has to happen even for a table whose snapshot ended without a
    `last` marker, or the live table never appears."""
    a = TransactionAssembler()
    assert feed_all(a, [snapshot("customers"), snapshot("customers")]) == []
    units = a.feed(begin("7"))
    assert len(units) == 1
    assert units[0].kind == UNIT_SNAPSHOT_CHUNK
    assert units[0].snapshot_last is True


# --------------------------------------------------------------------------- #
# spill (ADR §3.4)
# --------------------------------------------------------------------------- #
def test_spill_takes_events_out_of_memory_without_changing_the_boundary():
    staged: list[list] = []

    def on_spill(events):
        staged.append(list(events))
        return len(events)

    a = TransactionAssembler(spill_events=2, on_spill=on_spill)
    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102), data("7", 3, 103)])
    assert sum(len(batch) for batch in staged) == 2
    assert a.buffered_events == 1

    units = a.feed(end("7", 3, lsn=104))
    assert len(units) == 1
    unit = units[0]
    # The boundary rule still holds against the FULL count, spilled or not.
    assert unit.event_count == 3
    assert unit.spilled is True
    assert unit.spilled_events == 2
