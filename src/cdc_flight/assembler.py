"""`TransactionAssembler` — turn a record stream into whole units (ADR 0001 §3.2).

The assembler is the only place in the applier that knows about transaction
boundaries. It consumes `PendingRecord`s and emits `CompleteUnit`s that are
**already proven whole**, so `commit_group()` contains no boundary conditionals
at all (Codex 2's "code-judo" note, adopted by ADR rev 2).

The one boundary rule:

> A Postgres transaction is complete, and therefore eligible for a commit group,
> **only** when its Debezium `END` marker has been received **and** the marker's
> `event_count` equals the number of events buffered for that transaction id
> (and each `data_collections[].event_count` equals the per-table count).

Consequences, enforced here and asserted by `tests/1.3_atomic_batches/`:

* a `txId` change without an intervening `END` is fatal, not a fallback;
* an `event_count` mismatch is fatal;
* at shutdown the un-`END`ed tail is **discarded** (`discard_open_unit()`), which
  is safe precisely because Invariant O means nothing about it was acknowledged.

Two shapes that look like violations and are not, both **measured** against
Debezium 3.6 rather than assumed:

1. **A duplicate `END` for the previous transaction after a restart.**
   `TransactionContext` is restored from the offset, and the offset we resume
   from is the one written when the previous transaction's `END` was emitted -
   at which point `transactionCommittedEvent` had not yet called
   `endTransaction()`. The first data event of the *next* transaction therefore
   sees a different `txId` and makes `TransactionMonitor.dataEvent` emit an
   `END` for the already-finished transaction
   (`TransactionMonitor.java:98-105`). It carries no events of ours, so it
   becomes an offset-only control unit.
2. **Snapshot records with no `BEGIN`/`END` at all.** `dispatchSnapshotEvent`
   never reaches `transactionMonitor.dataEvent`, so the snapshot phase produces
   no transaction metadata (ADR §3.5 / Opus B3). Snapshot records are cut into
   `snapshot_chunk` units instead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .envelope import (
    KIND_DATA,
    KIND_HEARTBEAT,
    KIND_MESSAGE,
    KIND_SCHEMA_CHANGE,
    KIND_SNAPSHOT,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    SNAPSHOT_LAST,
    SNAPSHOT_TABLE_LAST,
    PendingRecord,
)
from .errors import TransactionAssemblyError

log = logging.getLogger("cdc_flight.assembler")

UNIT_TXN = "txn"
UNIT_SNAPSHOT_CHUNK = "snapshot_chunk"
UNIT_CONTROL = "control"


@dataclass
class CompleteUnit:
    """A whole PG transaction, a whole snapshot chunk, or a control unit."""

    kind: str
    #: data events only, in source order
    events: list[PendingRecord] = field(default_factory=list)
    #: every record of the unit, in arrival order - what gets acknowledged
    records: list[PendingRecord] = field(default_factory=list)
    txn_id: str | None = None
    last_lsn: int = 0
    commit_lsn: int | None = None
    nbytes: int = 0
    table: str | None = None
    schema: str | None = None
    snapshot_last_for_table: bool = False
    snapshot_last: bool = False
    #: set by the applier when the unit is at or below the durable resume point
    fenced: bool = False
    #: set by the applier when the unit's events were staged to `spill_events`
    spilled: bool = False
    spilled_events: int = 0

    @property
    def terminal(self) -> PendingRecord | None:
        return self.records[-1] if self.records else None

    @property
    def event_count(self) -> int:
        return len(self.events) + self.spilled_events

    def tables_touched(self) -> set[str]:
        return {
            f"{e.schema}.{e.table}" for e in self.events if e.schema and e.table
        }


class _OpenTxn:
    __slots__ = (
        "begin_seen", "events", "mem_bytes", "nbytes", "per_table", "records", "txn_id",
    )

    def __init__(self, txn_id: str, begin_seen: bool):
        self.txn_id = txn_id
        self.begin_seen = begin_seen
        self.events: list[PendingRecord] = []
        self.records: list[PendingRecord] = []
        #: total size of the unit, for the group's byte trigger
        self.nbytes = 0
        #: size of what is still IN MEMORY. Reset on every spill - see
        #: `_maybe_spill_txn` for why conflating the two was a 30x throughput bug.
        self.mem_bytes = 0
        self.per_table: dict[str, int] = {}


class _OpenChunk:
    __slots__ = ("events", "mem_bytes", "nbytes", "records", "schema", "table")

    def __init__(self, schema: str | None, table: str | None):
        self.schema = schema
        self.table = table
        self.events: list[PendingRecord] = []
        self.records: list[PendingRecord] = []
        self.nbytes = 0
        self.mem_bytes = 0


class TransactionAssembler:
    """Emits `CompleteUnit`s, and nothing else, from a record stream."""

    def __init__(
        self,
        *,
        snapshot_chunk_events: int = 50_000,
        snapshot_chunk_bytes: int = 64 * 1024 * 1024,
        on_spill: Any = None,
        spill_events: int = 500_000,
        spill_bytes: int = 64 * 1024 * 1024,
        keep_all_records: bool = False,
    ):
        self.snapshot_chunk_events = snapshot_chunk_events
        self.snapshot_chunk_bytes = snapshot_chunk_bytes
        #: When False (the default) a unit retains only its MOST RECENT record and
        #: releases the Java reference on every earlier one. That is safe because
        #: `markProcessed()` is a last-write-wins map put, so only the terminal
        #: record's offset matters (ADR §14.6, answered in §15/A16), and it is
        #: necessary because a 200 000-event transaction otherwise holds 200 000
        #: live JPype global references and throughput collapses as the JVM
        #: struggles: MEASURED 12 500 events/s for the first 88 000 and then
        #: ~1 000 events/s. `CDC_ACK_EVERY_RECORD=1` restores full retention.
        self.keep_all_records = keep_all_records
        #: callback(unit_events) -> int, invoked while a unit is over the hard
        #: spill threshold. Returns how many events it took off our hands.
        self.on_spill = on_spill
        self.spill_events = spill_events
        self.spill_bytes = spill_bytes

        self._txn: _OpenTxn | None = None
        self._chunk: _OpenChunk | None = None
        self._txn_spilled = 0
        self._chunk_spilled = 0
        #: counters exposed for the run summary / observability
        self.units_emitted = 0
        self.discarded_tail_events = 0
        self.orphan_end_markers = 0
        self.implicit_txn_opens = 0

    # -- introspection ------------------------------------------------------ #
    @property
    def open_transaction_id(self) -> str | None:
        return self._txn.txn_id if self._txn else None

    @property
    def open_unit_has_spilled(self) -> bool:
        """True while the *incomplete* unit has rows staged in `spill_events`.

        The applier must not close a commit group in this state. Spilled rows are
        staged inside the group's own transaction, so draining them would apply
        events belonging to a transaction whose `END` has not arrived - i.e. commit
        part of a Postgres transaction, which is exactly what Invariant B forbids.
        Nothing else in the design would catch it, because the group itself
        contains only whole units.
        """
        return self._txn_spilled > 0 or self._chunk_spilled > 0

    @property
    def buffered_events(self) -> int:
        n = len(self._txn.events) if self._txn else 0
        n += len(self._chunk.events) if self._chunk else 0
        return n

    @property
    def buffered_bytes(self) -> int:
        n = self._txn.nbytes if self._txn else 0
        n += self._chunk.nbytes if self._chunk else 0
        return n

    # -- the state machine -------------------------------------------------- #
    def feed(self, rec: PendingRecord) -> list[CompleteUnit]:
        kind = rec.kind
        if kind == KIND_SNAPSHOT:
            return self._feed_snapshot(rec)

        units: list[CompleteUnit] = []
        # Any non-snapshot record ends the snapshot phase: the shadow tables can
        # be swapped in (ADR §3.5 / §7).
        units.extend(self._close_chunk(force=True, last_for_table=True, last=True))

        if kind == KIND_TXN_BEGIN:
            units.extend(self._feed_begin(rec))
        elif kind == KIND_TXN_END:
            units.extend(self._feed_end(rec))
        elif kind == KIND_DATA:
            units.extend(self._feed_data(rec))
        elif kind in (KIND_HEARTBEAT, KIND_MESSAGE, KIND_SCHEMA_CHANGE):
            units.extend(self._feed_control(rec))
        else:
            units.extend(self._feed_control(rec))
        self.units_emitted += len(units)
        return units

    # -- streaming ---------------------------------------------------------- #
    def _feed_begin(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is not None:
            raise TransactionAssemblyError(
                f"Debezium emitted BEGIN for transaction {rec.txn_id} while "
                f"transaction {self._txn.txn_id} is still open "
                f"({len(self._txn.events)} events buffered). Transaction metadata "
                "is not self-consistent, so a commit group could contain part of a "
                "Postgres transaction (ADR 0001 §3.2)."
            )
        if rec.txn_id is None:
            raise TransactionAssemblyError("BEGIN marker without a transaction id")
        self._txn = _OpenTxn(rec.txn_id, begin_seen=True)
        self._retain(self._txn, rec)
        self._txn.nbytes += rec.nbytes
        return []

    def _feed_data(self, rec: PendingRecord) -> list[CompleteUnit]:
        if rec.txn_id is None:
            raise TransactionAssemblyError(
                f"streaming data event on {rec.topic} carries no transaction id; "
                "`provide.transaction.metadata=true` is mandatory (ADR 0001 §3.2)"
            )
        if self._txn is None:
            # Debezium suppresses a duplicate BEGIN when the transaction context
            # was restored from the offset (`TransactionMonitor.java:130-136`).
            # Opening implicitly is safe: the unit still cannot complete until an
            # END whose event_count matches, and a mismatch is fatal below.
            self.implicit_txn_opens += 1
            log.debug("opening transaction %s implicitly (no BEGIN seen)", rec.txn_id)
            self._txn = _OpenTxn(rec.txn_id, begin_seen=False)
        elif self._txn.txn_id != rec.txn_id:
            raise TransactionAssemblyError(
                f"transaction id changed from {self._txn.txn_id} to {rec.txn_id} "
                f"without an END marker ({len(self._txn.events)} events buffered). "
                "That is a fatal consistency error, not a boundary (ADR 0001 §3.2)."
            )
        self._txn.events.append(rec)
        self._retain(self._txn, rec)
        self._txn.nbytes += rec.nbytes
        self._txn.mem_bytes += rec.nbytes
        table = rec.qualified_table
        if table:
            self._txn.per_table[table] = self._txn.per_table.get(table, 0) + 1
        self._maybe_spill_txn()
        return []

    def _feed_end(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is None:
            # Expected exactly once per restart - see the module docstring.
            self.orphan_end_markers += 1
            log.debug("END marker for %s with no open transaction", rec.txn_id)
            return [self._control_unit(rec)]
        if self._txn.txn_id != rec.txn_id:
            raise TransactionAssemblyError(
                f"END marker for transaction {rec.txn_id} while transaction "
                f"{self._txn.txn_id} is open (ADR 0001 §3.2)"
            )

        txn = self._txn
        spilled = self._txn_spilled
        self._txn = None
        self._txn_spilled = 0
        self._retain(txn, rec)
        txn.nbytes += rec.nbytes

        buffered = len(txn.events) + spilled
        declared = rec.txn_event_count
        if declared is not None and int(declared) != buffered:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END declares {declared} events, "
                f"{buffered} were buffered. A commit group may only contain "
                "transactions we can prove whole (ADR 0001 §3.2)."
            )
        if not spilled:
            # Per-table counts are only checkable while every event is in memory;
            # in spill mode the drain re-derives them from `spill_events`.
            for table, declared_n in rec.txn_data_collections.items():
                actual = txn.per_table.get(table, 0)
                if actual != declared_n:
                    raise TransactionAssemblyError(
                        f"transaction {rec.txn_id}: END declares {declared_n} events "
                        f"for {table}, {actual} were buffered (ADR 0001 §3.2)"
                    )

        commit_lsn = rec.lsn or max(
            (e.lsn or 0 for e in txn.events), default=0
        )
        unit = CompleteUnit(
            kind=UNIT_TXN,
            events=txn.events,
            records=txn.records,
            txn_id=txn.txn_id,
            commit_lsn=commit_lsn,
            last_lsn=max(commit_lsn or 0, *( [0] + [e.lsn or 0 for e in txn.events])),
            nbytes=txn.nbytes,
            spilled=bool(spilled),
            spilled_events=spilled,
        )
        return [unit]

    def _feed_control(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is not None:
            # ADR §3.2: a control record inside an open transaction is carried by
            # that transaction, never emitted on its own - otherwise a heartbeat
            # would advance the resume point past a half-buffered transaction.
            self._retain(self._txn, rec)
            self._txn.nbytes += rec.nbytes
            return []
        return [self._control_unit(rec)]

    def _control_unit(self, rec: PendingRecord) -> CompleteUnit:
        return CompleteUnit(
            kind=UNIT_CONTROL,
            events=[],
            records=[rec],
            txn_id=rec.txn_id,
            last_lsn=rec.lsn or 0,
            commit_lsn=rec.lsn,
            nbytes=rec.nbytes,
        )

    # -- snapshot ----------------------------------------------------------- #
    def _feed_snapshot(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is not None:  # pragma: no cover - would be a Debezium bug
            raise TransactionAssemblyError(
                "snapshot record arrived while a streaming transaction is open"
            )
        units: list[CompleteUnit] = []
        if self._chunk is not None and (
            self._chunk.table != rec.table or self._chunk.schema != rec.schema
        ):
            units.extend(self._close_chunk(force=True, last_for_table=True, last=False))
        if self._chunk is None:
            self._chunk = _OpenChunk(rec.schema, rec.table)
        self._chunk.events.append(rec)
        self._retain(self._chunk, rec)
        self._chunk.nbytes += rec.nbytes
        self._chunk.mem_bytes += rec.nbytes
        self._maybe_spill_chunk()

        table_last = rec.snapshot in SNAPSHOT_TABLE_LAST
        overflow = (
            len(self._chunk.events) + self._chunk_spilled >= self.snapshot_chunk_events
            or self._chunk.nbytes >= self.snapshot_chunk_bytes
        )
        if table_last or overflow:
            units.extend(
                self._close_chunk(
                    force=True,
                    last_for_table=table_last,
                    last=rec.snapshot == SNAPSHOT_LAST,
                )
            )
        return units

    def _close_chunk(
        self, *, force: bool, last_for_table: bool, last: bool
    ) -> list[CompleteUnit]:
        if self._chunk is None:
            return []
        chunk = self._chunk
        spilled = self._chunk_spilled
        self._chunk = None
        self._chunk_spilled = 0
        unit = CompleteUnit(
            kind=UNIT_SNAPSHOT_CHUNK,
            events=chunk.events,
            records=chunk.records,
            schema=chunk.schema,
            table=chunk.table,
            last_lsn=max((e.lsn or 0 for e in chunk.events), default=0),
            nbytes=chunk.nbytes,
            snapshot_last_for_table=last_for_table,
            snapshot_last=last,
            spilled=bool(spilled),
            spilled_events=spilled,
        )
        return [unit]

    def _retain(self, target, rec: PendingRecord) -> None:
        """Add `rec` to the unit's acknowledgement list, dropping what it supersedes."""
        if self.keep_all_records:
            target.records.append(rec)
            return
        for older in target.records:
            older.raw = None
        target.records = [rec]

    # -- spill (ADR 0001 §3.4) ---------------------------------------------- #
    def _maybe_spill_txn(self) -> None:
        """Stage the in-memory tail when it exceeds the hard threshold.

        The condition is on what is still **in memory**, not on the unit's total
        size. Testing the total was a 30x throughput bug: once a 200 000-event
        transaction passed 64 MB the threshold stayed tripped for every remaining
        record, so each record became its own `INSERT INTO spill_events`.
        MEASURED before the fix: 12 500 events/s up to ~88 000 events, then
        ~1 000 events/s; after it, the whole 200 000 in one spill batch.
        """
        if self.on_spill is None or self._txn is None:
            return
        if (
            len(self._txn.events) < self.spill_events
            and self._txn.mem_bytes < self.spill_bytes
        ):
            return
        staged = self.on_spill(self._txn.events)
        if staged:
            self._txn_spilled += staged
            retained = set(map(id, self._txn.records))
            for event in self._txn.events:
                if id(event) not in retained:
                    event.raw = None
            self._txn.events = []
            self._txn.mem_bytes = 0

    def _maybe_spill_chunk(self) -> None:
        if self.on_spill is None or self._chunk is None:
            return
        if (
            len(self._chunk.events) < self.spill_events
            and self._chunk.mem_bytes < self.spill_bytes
        ):
            return
        staged = self.on_spill(self._chunk.events)
        if staged:
            self._chunk_spilled += staged
            retained = set(map(id, self._chunk.records))
            for event in self._chunk.events:
                if id(event) not in retained:
                    event.raw = None
            self._chunk.events = []
            self._chunk.mem_bytes = 0

    # -- shutdown ----------------------------------------------------------- #
    def discard_open_unit(self) -> int:
        """Drop the un-`END`ed tail. Returns how many data events were discarded.

        ADR 0001 §3.2: safe by construction, because Invariant O means the offset
        store still points before every one of them, so they replay on the next
        run. This replaces rev 1's shutdown heuristic, which could commit a
        partial Postgres transaction (Codex 2).
        """
        discarded = 0
        if self._txn is not None:
            discarded += len(self._txn.events) + self._txn_spilled
            log.info(
                "discarding un-ENDed tail of transaction %s (%s events); it replays "
                "on the next run",
                self._txn.txn_id,
                discarded,
            )
            self._txn = None
            self._txn_spilled = 0
        if self._chunk is not None:
            discarded += len(self._chunk.events) + self._chunk_spilled
            self._chunk = None
            self._chunk_spilled = 0
        self.discarded_tail_events += discarded
        return discarded

    def close_snapshot_chunk(self) -> list[CompleteUnit]:
        """Emit the buffered snapshot chunk without claiming the snapshot ended."""
        return self._close_chunk(force=True, last_for_table=False, last=False)
