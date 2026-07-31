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
    a = TransactionAssembler(keep_all_records=True)
    assert feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102)]) == []
    assert a.buffered_events == 2

    units = a.feed(end("7", 2, lsn=103, per_table={"app.customers": 2}))
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


def test_an_end_with_no_event_count_is_fatal():
    """`declared is None` used to skip the check and emit the unit as WHOLE.

    The boundary rule is the whole of 1.3 and it says the marker's `event_count`
    must *equal* the number of events buffered. `None` equals nothing, so an END
    without a count is not proof of anything (Codex 2 / Opus M-1).
    """
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="no event_count"):
        a.feed(end("7", None))


def test_an_end_with_a_non_integral_event_count_is_fatal():
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="event_count"):
        a.feed(end("7", "not-a-number"))


def test_per_table_counts_are_still_checked_after_a_spill():
    """Validation must not weaken when the storage representation changes.

    The per-table check was disabled wholesale as soon as any event spilled, and
    the claimed "the drain re-derives them" had no corresponding comparison
    anywhere (Codex 2).
    """
    a = TransactionAssembler(spill_events=2, on_spill=lambda events, **kw: len(events))
    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102), data("7", 3, 103)])
    with pytest.raises(TransactionAssemblyError, match=r"events for app\.customers"):
        a.feed(end("7", 3, per_table={"app.customers": 4}))


def test_a_table_the_marker_never_declared_is_fatal():
    """The comparison has to run in both directions, or a misrouted event is silent."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101, "customers"), data("7", 2, 102, "orders")])
    with pytest.raises(TransactionAssemblyError, match="never declared"):
        a.feed(end("7", 2, per_table={"app.customers": 1}))


def test_a_streaming_event_without_a_total_order_is_fatal():
    """Keyless identity is `<lsn>:<txId>:<total_order>`; a missing ordinal collapses
    two accepted events onto one identity (Codex 4)."""
    a = TransactionAssembler()
    rec = data("7", 1, 101)
    rec.total_order = None
    with pytest.raises(TransactionAssemblyError, match="total_order"):
        a.feed(rec)


def test_a_duplicate_total_order_inside_one_transaction_is_fatal():
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101)])
    with pytest.raises(TransactionAssemblyError, match="total_order 1 twice"):
        a.feed(data("7", 1, 101))


def test_a_non_positive_total_order_is_fatal():
    a = TransactionAssembler()
    with pytest.raises(TransactionAssemblyError, match="total_order"):
        a.feed(data("7", 0, 101))


def test_the_ordinal_set_must_be_exactly_one_to_n():
    """`event_count` implies the ordinals `1..N`; a gap means an event we never saw."""
    a = TransactionAssembler()
    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 3, 102)])
    with pytest.raises(TransactionAssemblyError, match="ordinal"):
        a.feed(end("7", 2))


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
    a = TransactionAssembler(keep_all_records=True)
    feed_all(a, [begin("7"), data("7", 1, 101)])
    hb = PendingRecord(raw=object(), kind=KIND_HEARTBEAT, topic="__debezium-heartbeat.p",
                       nbytes=5, lsn=150)
    assert a.feed(hb) == []
    units = a.feed(end("7", 1, lsn=200, per_table={"app.customers": 1}))
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
    units = a.feed(end("7", 1, lsn=102, per_table={"app.customers": 1}))
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
    `last` marker, or the live table never appears — but it is a **per-table**
    swap, not a swap of every shadow that happens to exist.

    `snapshot_last` (which swaps *every* shadow) is only ever set when Debezium
    actually said `last` (Opus M-7). Setting it from any non-snapshot record is
    live-table destruction the moment incremental snapshots interleave with
    streaming events.
    """
    a = TransactionAssembler()
    assert feed_all(a, [snapshot("customers"), snapshot("customers")]) == []
    units = a.feed(begin("7"))
    assert len(units) == 1
    assert units[0].kind == UNIT_SNAPSHOT_CHUNK
    assert units[0].snapshot_last_for_table is True
    assert units[0].snapshot_last is False


def test_an_incremental_snapshot_record_is_refused_until_rubric_3_3_owns_it():
    """Incremental chunk records are interleaved with streaming events and never
    carry a `last` marker, so the snapshot-phase machinery here cannot host them
    safely (Opus M-7). Refusing is loud; guessing destroys a live table."""
    a = TransactionAssembler()
    with pytest.raises(TransactionAssemblyError, match="incremental"):
        a.feed(snapshot("customers", marker="incremental"))


# --------------------------------------------------------------------------- #
# spill (ADR §3.4)
# --------------------------------------------------------------------------- #
def test_spill_takes_events_out_of_memory_without_changing_the_boundary():
    staged: list[list] = []

    def on_spill(events, **_identity):
        staged.append(list(events))
        return len(events)

    a = TransactionAssembler(spill_events=2, on_spill=on_spill)
    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102), data("7", 3, 103)])
    assert sum(len(batch) for batch in staged) == 2
    assert a.buffered_events == 1

    units = a.feed(end("7", 3, lsn=104, per_table={"app.customers": 3}))
    assert len(units) == 1
    unit = units[0]
    # The boundary rule still holds against the FULL count, spilled or not.
    assert unit.event_count == 3
    assert unit.spilled is True
    assert unit.spilled_events == 2


# --------------------------------------------------------------------------- #
# retention (ADR §15/A16)
# --------------------------------------------------------------------------- #
def test_by_default_a_unit_retains_only_its_terminal_record():
    """MEASURED: retaining one JPype reference per record collapses throughput on a
    large transaction (12 500 events/s for the first 88 000, then ~1 000/s). Only
    the terminal record's offset matters, because `markProcessed()` is a
    last-write-wins map put, so the rest are released as soon as they are
    superseded."""
    a = TransactionAssembler()
    records = [begin("7"), data("7", 1, 101), data("7", 2, 102)]
    feed_all(a, records)
    units = a.feed(end("7", 2, lsn=103, per_table={"app.customers": 2}))
    unit = units[0]

    assert len(unit.records) == 1
    assert unit.records[0].kind == KIND_TXN_END
    # ... and the superseded Java references really were let go.
    assert [r.raw for r in records] == [None, None, None]
    # The data events themselves are still there - only the ACK list was trimmed.
    assert len(unit.events) == 2
    assert unit.last_lsn == 103


def test_ack_every_record_keeps_the_whole_chain():
    a = TransactionAssembler(keep_all_records=True)
    records = [begin("7"), data("7", 1, 101), data("7", 2, 102)]
    feed_all(a, records)
    unit = a.feed(end("7", 2, lsn=103, per_table={"app.customers": 2}))[0]
    assert len(unit.records) == 4
    assert all(r.raw is not None for r in records)


def test_spill_does_not_retrigger_on_every_subsequent_event():
    """The bug this pins cost 30x throughput.

    The spill threshold must test what is still **in memory**, not the unit's
    total size. Testing the total means that once a large transaction crosses the
    byte threshold it stays crossed for every remaining record, so each record
    becomes its own spill statement.
    """
    calls: list[int] = []

    def on_spill(events, **_identity):
        calls.append(len(events))
        return len(events)

    # bytes threshold only: 100-byte events, spill at 250 bytes
    a = TransactionAssembler(spill_events=10**9, spill_bytes=250, on_spill=on_spill)
    feed_all(a, [begin("7")] + [data("7", i, 100 + i) for i in range(1, 13)])
    a.feed(end("7", 12, lsn=200, per_table={"app.customers": 12}))

    # 12 events x 100 bytes = 1200 bytes => 4 spills of 3, never 1-per-event.
    assert calls == [3, 3, 3, 3], calls
    assert all(n > 1 for n in calls)


def test_an_open_unit_that_has_spilled_blocks_the_group_from_closing():
    """Invariant B's one remaining escape hatch, closed.

    Spilled rows are staged inside the commit group's own transaction. If the group
    were allowed to close while the unit that owns them is still open, the drain
    would apply events from a transaction whose END has not arrived - a partial
    Postgres transaction committed at the destination. The group itself cannot
    catch this, because it contains only whole units.
    """
    a = TransactionAssembler(spill_events=2, on_spill=lambda events, **_: len(events))
    assert a.open_unit_has_spilled is False

    feed_all(a, [begin("7"), data("7", 1, 101), data("7", 2, 102)])
    assert a.open_unit_has_spilled is True, "the spill did not register"

    units = a.feed(end("7", 2, lsn=103, per_table={"app.customers": 2}))
    assert len(units) == 1
    assert units[0].spilled_events == 2
    # Once the unit is whole, the group is free to close and drain.
    assert a.open_unit_has_spilled is False
