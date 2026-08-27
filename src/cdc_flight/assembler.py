"""`TransactionAssembler` — turn a record stream into whole units (ADR 0001 §3.2).

The assembler is the only place in the applier that knows about transaction
boundaries. It consumes `PendingRecord`s and emits `CompleteUnit`s that are
**already proven whole**, so `commit_group()` contains no boundary conditionals
at all (Codex 2's "code-judo" note, adopted by ADR rev 2).

The one boundary rule:

> A Postgres transaction is complete, and therefore eligible for a commit group,
> **only** when its Debezium `END` marker has been received **and** the marker
> carries an `event_count` that equals the number of events counted for that
> transaction id, **and** the declared per-table `data_collections` counts equal
> the observed per-table counts *in both directions*, **and** the observed
> `transaction.total_order` ordinals are exactly `1..event_count`.

Consequences, enforced here and asserted by `tests/rubric/1.3_atomic_batches/` and
`tests/unit/test_assembler.py`:

* a `txId` change without an intervening `END` is fatal, not a fallback;
* an `event_count` mismatch is fatal, and so is a **missing** `event_count`: the
  rule says "equals", and `None` equals nothing (Codex 2 / Opus M-1);
* the counters the rule is checked against (`count`, `per_table`, `orders`) are
  maintained on arrival and are never touched by spilling, so a unit is proven
  identically in memory and on disk. Nothing about the proof is conditional on
  the storage representation;
* a missing, non-positive or repeated `total_order` is fatal, because that value
  is part of `cdcf_event_id` and a collision silently drops an accepted event
  (Codex 4);
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
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .envelope import (
    KIND_DATA,
    KIND_HEARTBEAT,
    KIND_MESSAGE,
    KIND_SCHEMA_CHANGE,
    KIND_SNAPSHOT,
    KIND_SNAPSHOT_BOUNDARY,
    KIND_TRUNCATE,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    SNAPSHOT_INCREMENTAL,
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
    #: source-qualified relations observed by this unit, including events that were
    #: staged to spill before the unit was closed
    touched_tables: set[str] = field(default_factory=set)
    snapshot_last_for_table: bool = False
    snapshot_last: bool = False
    #: set by the applier when the unit is at or below the durable resume point
    fenced: bool = False
    #: set by the applier when the unit's events were staged to `spill_events`
    spilled: bool = False
    spilled_events: int = 0
    #: `_cdc_flight.spill_events.unit_seq` of this unit's staged rows, or None.
    #: The drain reads and fences per unit, which is what lets a group interleave
    #: `spilled prefix -> in-memory tail -> next unit` in true source order
    #: (Opus B-1) and lets a fenced unit suppress its own prefix (Codex 5).
    spill_unit_seq: int | None = None
    #: events counted and validated for a discard-only re-snapshot stream unit.
    #: They are intentionally never retained or sent to the destination spill table.
    discarded_events: int = 0
    #: Subset of ``event_count`` that is genuine delivery evidence for the service
    #: liveness clocks.  The applier marks its own signal-table row separately so it
    #: remains in the whole-transaction/offset proof without becoming progress.
    #: ``None`` preserves compatibility for hand-built units; production assembler
    #: units always carry the explicit count.
    delivery_events: int | None = None
    #: True for stock incremental snapshot chunks.  These chunks are bounded
    #: units but are not initial-snapshot terminal evidence.
    incremental: bool = False
    #: True for the Debezium signal relation's initial snapshot chunk.  The
    #: relation must remain captured for control-plane writes, but it is not a
    #: destination table and must not create a shadow or snapshot swap.
    ignored: bool = False

    @property
    def terminal(self) -> PendingRecord | None:
        return self.records[-1] if self.records else None

    @property
    def event_count(self) -> int:
        return len(self.events) + self.spilled_events + self.discarded_events

    def tables_touched(self) -> set[str]:
        touched = set(self.touched_tables)
        touched.update(
            f"{e.schema}.{e.table}" for e in self.events if e.schema and e.table
        )
        return touched


class _OpenTxn:
    __slots__ = (
        "begin_seen", "count", "delivery_events", "events", "last_lsn", "mem_bytes", "message_count",
        "nbytes", "orders", "per_table", "records", "spill_unit_seq", "spilled",
        "touched_tables", "txn_id",
    )

    def __init__(self, txn_id: str, begin_seen: bool):
        self.txn_id = txn_id
        self.begin_seen = begin_seen
        self.events: list[PendingRecord] = []
        self.records: list[PendingRecord] = []
        self.last_lsn = 0
        #: total size of the unit, for the group's byte trigger
        self.nbytes = 0
        #: size of what is still IN MEMORY. Reset on every spill - see
        #: `_maybe_spill_txn` for why conflating the two was a 30x throughput bug.
        self.mem_bytes = 0
        #: Counters that are maintained INDEPENDENTLY of the retained payloads, so
        #: spilling changes the storage representation and never the proof
        #: (Codex 2). `len(events)` is not a count of the transaction.
        self.count = 0
        #: Count of data records that are delivery evidence. This deliberately differs
        #: from ``count``: the Flight's signal-table rows stay in the transaction proof
        #: but do not refresh service liveness.
        self.delivery_events = 0
        self.per_table: dict[str, int] = {}
        #: relation names are retained independently of the event payloads so admission
        #: remains correct after the payload prefix is moved to `spill_events`
        self.touched_tables: set[str] = set()
        #: every `transaction.total_order` seen, so a duplicate or a gap is loud
        self.orders: set[int] = set()
        #: counted events that belong to no captured table (logical-decoding
        #: messages); they occupy an ordinal and a `data_collections` entry.
        self.message_count = 0
        #: how many events were handed to the spill callback
        self.spilled = 0
        #: identity of this unit in `_cdc_flight.spill_events`, allocated on its
        #: first spill so the drain can order and fence per unit.
        self.spill_unit_seq: int | None = None


class _OpenChunk:
    __slots__ = (
        "count",
        "delivery_events",
        "events",
        "incremental",
        "mem_bytes",
        "nbytes",
        "records",
        "saw_last",
        "schema",
        "spill_unit_seq",
        "spilled",
        "table",
        "touched_tables",
    )

    def __init__(self, schema: str | None, table: str | None, *, incremental: bool = False):
        self.schema = schema
        self.table = table
        self.events: list[PendingRecord] = []
        self.records: list[PendingRecord] = []
        self.nbytes = 0
        self.mem_bytes = 0
        self.count = 0
        self.delivery_events = 0
        self.spilled = 0
        self.spill_unit_seq: int | None = None
        self.touched_tables: set[str] = set()
        #: True once Debezium actually said `snapshot=last` for this chunk. Only
        #: then may the chunk claim the whole snapshot ended (Opus M-7).
        self.saw_last = False
        self.incremental = incremental


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
        discard_streaming: bool = False,
        incremental_enabled: bool = False,
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
        #: A re-snapshot applies only snapshot chunks. Streaming units still have to
        #: reach a verified END so their source boundary is proven, but their payload
        #: is never retained or handed to the destination spill callback.
        self.discard_streaming = discard_streaming
        #: Existing callers retain the deliberate refusal until the stock
        #: incremental coordinator has admitted this path explicitly.
        self.incremental_enabled = incremental_enabled
        #: callback(unit_events) -> int, invoked while a unit is over the hard
        #: spill threshold. Returns how many events it took off our hands.
        self.on_spill = on_spill
        self.spill_events = spill_events
        self.spill_bytes = spill_bytes

        self._txn: _OpenTxn | None = None
        self._chunk: _OpenChunk | None = None
        #: Stock incremental READ records are not part of a PostgreSQL transaction,
        #: but Debezium can deliver one between BEGIN and END of an unrelated
        #: streaming transaction.  Hold it until that transaction is proven whole;
        #: applying it earlier would either commit a partial PostgreSQL transaction
        #: or let the READ overtake the CDC event that follows it.
        self._deferred_snapshots: deque[PendingRecord] = deque()
        #: monotone identity for spilled units within this process
        self._spill_unit_seq = 0
        #: per-table arrival ordinal for the snapshot phase (ADR §6 / Codex 1)
        self._snapshot_ordinals: dict[str, int] = {}
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
        return (self._txn is not None and self._txn.spilled > 0) or (
            self._chunk is not None and self._chunk.spilled > 0
        )

    @property
    def buffered_events(self) -> int:
        n = len(self._txn.events) if self._txn else 0
        n += len(self._chunk.events) if self._chunk else 0
        n += len(self._deferred_snapshots)
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
            if self._txn is not None:
                if rec.snapshot != SNAPSHOT_INCREMENTAL or not self.incremental_enabled:
                    raise TransactionAssemblyError(
                        "snapshot record arrived while a streaming transaction is open"
                    )
                self._deferred_snapshots.append(rec)
                return []
            units = self._feed_snapshot(rec)
            self.units_emitted += len(units)
            return units

        units: list[CompleteUnit] = []
        # Any non-snapshot record ends the snapshot phase for the table whose chunk
        # is open, so *that table's* shadow can be swapped in (ADR §3.5 / §7). It
        # does NOT claim the whole snapshot ended - see `_close_chunk`.
        units.extend(self._close_chunk(last_for_table=True))

        if kind == KIND_TXN_BEGIN:
            units.extend(self._feed_begin(rec))
        elif kind == KIND_TXN_END:
            units.extend(self._feed_end(rec))
        elif kind in (KIND_DATA, KIND_TRUNCATE):
            # A truncate goes through Debezium's `changeRecord` path, so it is counted
            # in `END.event_count`, occupies a `total_order` ordinal and appears in
            # `data_collections`. Feeding it as anything else would make every
            # transaction that truncates a table fail the completeness rule.
            units.extend(self._feed_data(rec))
        elif kind in (KIND_HEARTBEAT, KIND_MESSAGE, KIND_SCHEMA_CHANGE):
            units.extend(self._feed_control(rec))
        else:
            units.extend(self._feed_control(rec))
        if self._txn is None and self._deferred_snapshots:
            units.extend(self._flush_deferred_snapshots())
        self.units_emitted += len(units)
        return units

    def _flush_deferred_snapshots(self) -> list[CompleteUnit]:
        """Release queued incremental READs after the enclosing PG transaction."""
        units: list[CompleteUnit] = []
        while self._deferred_snapshots and self._txn is None:
            units.extend(self._feed_snapshot(self._deferred_snapshots.popleft()))
        return units

    def feed_snapshot_boundary(self, rec: PendingRecord) -> list[CompleteUnit]:
        """Emit the explicit, non-acknowledgeable Initial Snapshot boundary.

        The terminal notification is not an ordinary control record. Its Connect
        offset must join the final destination transaction, while its Debezium handle
        must remain pending until the completion machine has accepted the exact table
        set and all declared rows are durable. Keeping this operation separate from
        :meth:`feed` makes that ownership visible at the assembler boundary.
        """
        if rec.kind != KIND_SNAPSHOT_BOUNDARY or rec.raw is not None:
            raise TransactionAssemblyError(
                "snapshot boundary must be a typed record without an acknowledgeable "
                "Debezium handle"
            )
        if self._txn is not None:
            raise TransactionAssemblyError(
                "snapshot boundary arrived while a streaming transaction is open"
            )
        units = self._close_chunk(last_for_table=True)
        units.append(self._control_unit(rec))
        self.units_emitted += len(units)
        return units

    # -- streaming ---------------------------------------------------------- #
    def _feed_begin(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is not None:
            raise TransactionAssemblyError(
                f"Debezium emitted BEGIN for transaction {rec.txn_id} while "
                f"transaction {self._txn.txn_id} is still open "
                f"({self._txn.count} events buffered). Transaction metadata "
                "is not self-consistent, so a commit group could contain part of a "
                "Postgres transaction (ADR 0001 §3.2)."
            )
        if rec.txn_id is None:
            raise TransactionAssemblyError("BEGIN marker without a transaction id")
        self._txn = _OpenTxn(rec.txn_id, begin_seen=True)
        self._retain(self._txn, rec)
        self._txn.nbytes += rec.nbytes
        return []

    def _validate_ordinal(self, rec: PendingRecord) -> int:
        """`transaction.total_order` is part of the event identity, so it is a
        contract, not a hint (Codex 4).

        Keyless identity is `<event lsn>:<txId>:<total_order>` and the keyless
        collection is a dict keyed on it, so an absent or repeated ordinal makes
        two *accepted* events collide and one of them disappear. Validating here is
        what makes the identity structurally unique rather than conventionally
        unique: the assembler is the only producer of units, so nothing that
        reaches the identity builder can collide.
        """
        order = rec.total_order
        if order is None:
            raise TransactionAssemblyError(
                f"streaming event on {rec.topic} (txn {rec.txn_id}) carries no "
                "transaction.total_order. It is part of `cdcf_event_id`, so without "
                "it two events of one transaction share an identity and one is lost "
                "(ADR 0001 §6)."
            )
        try:
            order = int(order)
        except (TypeError, ValueError) as exc:
            raise TransactionAssemblyError(
                f"transaction.total_order {rec.total_order!r} is not an integer"
            ) from exc
        if order < 1:
            raise TransactionAssemblyError(
                f"transaction.total_order must be a 1-based ordinal, got {order}"
            )
        return order

    def _feed_data(self, rec: PendingRecord) -> list[CompleteUnit]:
        if rec.txn_id is None:
            raise TransactionAssemblyError(
                f"streaming data event on {rec.topic} carries no transaction id; "
                "`provide.transaction.metadata=true` is mandatory (ADR 0001 §3.2)"
            )
        order = self._validate_ordinal(rec)
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
        if order in self._txn.orders:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id} declared total_order {order} twice. Two "
                "accepted events would then share one `cdcf_event_id` and one of "
                "them would be silently dropped (ADR 0001 §6)."
            )
        self._txn.orders.add(order)
        rec.total_order = order
        self._txn.count += 1
        if rec.is_delivery_data:
            self._txn.delivery_events += 1
        self._retain(self._txn, rec)
        self._txn.nbytes += rec.nbytes
        self._txn.last_lsn = max(self._txn.last_lsn, rec.lsn or 0)
        table = rec.qualified_table
        if table:
            self._txn.per_table[table] = self._txn.per_table.get(table, 0) + 1
            self._txn.touched_tables.add(table)
        if not self.discard_streaming:
            self._txn.events.append(rec)
            self._txn.mem_bytes += rec.nbytes
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
        spilled = txn.spilled
        self._txn = None
        self._retain(txn, rec)
        txn.nbytes += rec.nbytes
        self._verify_complete(txn, rec)

        commit_lsn = rec.lsn or txn.last_lsn
        unit = CompleteUnit(
            kind=UNIT_TXN,
            events=txn.events,
            records=txn.records,
            txn_id=txn.txn_id,
            commit_lsn=commit_lsn,
            last_lsn=max(commit_lsn or 0, *( [0] + [e.lsn or 0 for e in txn.events])),
            nbytes=txn.nbytes,
            touched_tables=set(txn.touched_tables),
            spilled=bool(spilled),
            spilled_events=spilled,
            spill_unit_seq=txn.spill_unit_seq,
            discarded_events=txn.count - len(txn.events) - spilled,
            delivery_events=txn.delivery_events,
        )
        return [unit]

    def _verify_complete(self, txn: _OpenTxn, rec: PendingRecord) -> None:
        """The boundary rule, in one place and in **every** storage mode.

        Three things used to weaken it (Codex 2 / Opus M-1):

        * a missing `END.event_count` skipped the total check entirely, so a unit
          with no proof at all was emitted as whole;
        * the per-table check was disabled wholesale as soon as any event spilled,
          and the claimed "the drain re-derives them" had no comparison anywhere;
        * the comparison ran in one direction only, so an *observed* table the
          marker never mentioned was accepted.

        The counters compared here (`txn.count`, `txn.per_table`, `txn.orders`) are
        maintained as records arrive and are never touched by spilling, so
        "in memory" and "spilled" are proven identically.
        """
        declared = rec.txn_event_count
        if declared is None:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END marker carries no event_count, so the "
                f"{txn.count} events buffered cannot be proven to be the whole "
                "transaction. A commit group may only contain transactions we can "
                "prove whole (ADR 0001 §3.2)."
            )
        try:
            declared_total = int(declared)
        except (TypeError, ValueError) as exc:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END event_count {declared!r} is not an integer"
            ) from exc
        if declared_total != txn.count:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END declares {declared_total} events, "
                f"{txn.count} were buffered. A commit group may only contain "
                "transactions we can prove whole (ADR 0001 §3.2)."
            )

        # `total_order` is 1..N over the counted events of the transaction, so the
        # exact set is implied by the declared count. A gap means an event we never
        # saw; a duplicate is already fatal in `_feed_data`.
        expected = set(range(1, txn.count + 1))
        if txn.orders != expected:
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END declares {declared_total} events but the "
                f"observed transaction.total_order ordinals are {sorted(txn.orders)} "
                f"(missing {sorted(expected - txn.orders)}, unexpected "
                f"{sorted(txn.orders - expected)}). The ordinal set implied by "
                "event_count is 1..N (ADR 0001 §6)."
            )

        # Per-table, in both directions. `allowance` covers counted events that
        # belong to no captured table (logical-decoding messages get their own
        # `data_collections` pseudo-entry, ADR §15/A19 / Opus M-5).
        allowance = txn.message_count
        for table, declared_n in rec.txn_data_collections.items():
            actual = txn.per_table.get(table, 0)
            if actual == declared_n:
                continue
            if actual == 0 and declared_n <= allowance:
                allowance -= declared_n
                continue
            raise TransactionAssemblyError(
                f"transaction {rec.txn_id}: END declares {declared_n} events "
                f"for {table}, {actual} were buffered (ADR 0001 §3.2)"
            )
        for table, actual in txn.per_table.items():
            if table not in rec.txn_data_collections:
                raise TransactionAssemblyError(
                    f"transaction {rec.txn_id}: {actual} events were buffered for "
                    f"{table}, which the END marker never declared. The comparison "
                    "runs in both directions, or a misrouted event is invisible "
                    "(ADR 0001 §3.2)."
                )

    def _feed_control(self, rec: PendingRecord) -> list[CompleteUnit]:
        if self._txn is not None:
            # ADR §3.2: a control record inside an open transaction is carried by
            # that transaction, never emitted on its own - otherwise a heartbeat
            # would advance the resume point past a half-buffered transaction.
            self._retain(self._txn, rec)
            self._txn.nbytes += rec.nbytes
            if rec.kind == KIND_MESSAGE:
                # VERIFIED against vendored Debezium 3.6:
                # `LogicalDecodingMessageMonitor.java:106` calls
                # `transactionMonitor.dataEvent(...)`, so a transactional logical
                # message IS counted in `END.event_count`, occupies a
                # `total_order` ordinal, and gets its own `data_collections`
                # pseudo-entry. Not counting it made a transaction containing one
                # fatal - which matters because ADR D9's source heartbeat is
                # specified as exactly this mechanism (Opus M-5). It carries no
                # row of ours, so it is counted and not applied.
                order = self._validate_ordinal(rec)
                if order in self._txn.orders:
                    raise TransactionAssemblyError(
                        f"transaction {rec.txn_id} declared total_order {order} twice "
                        "(logical-decoding message)"
                    )
                self._txn.orders.add(order)
                self._txn.count += 1
                self._txn.message_count += 1
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
        if rec.snapshot == SNAPSHOT_INCREMENTAL and not self.incremental_enabled:
            # Keep the baseline safety boundary for callers that have not admitted a
            # stock incremental run.  The backfill coordinator opts in explicitly.
            raise TransactionAssemblyError(
                "record carries source.snapshot='incremental'. Incremental snapshots "
                "are admitted only through the stock incremental backfill route; "
                "the ordinary initial-snapshot assembler refuses them."
            )
        units: list[CompleteUnit] = []
        if self._chunk is not None and (
            self._chunk.table != rec.table or self._chunk.schema != rec.schema
        ):
            units.extend(self._close_chunk(last_for_table=True))
        if self._chunk is None:
            self._chunk = _OpenChunk(
                rec.schema, rec.table, incremental=rec.snapshot == SNAPSHOT_INCREMENTAL
            )
        elif self._chunk.incremental != (rec.snapshot == SNAPSHOT_INCREMENTAL):
            units.extend(self._close_chunk(last_for_table=False))
            self._chunk = _OpenChunk(
                rec.schema, rec.table, incremental=rec.snapshot == SNAPSHOT_INCREMENTAL
            )
        if rec.qualified_table:
            self._chunk.touched_tables.add(rec.qualified_table)
        # Assigned HERE, on arrival, so the ordinal is arrival order whether the
        # record is later spilled or kept in memory. Deriving it at apply time made
        # a spilled snapshot chunk take a *streaming* identity (Codex 1).
        key = rec.qualified_table or ""
        ordinal = self._snapshot_ordinals.get(key, 0) + 1
        self._snapshot_ordinals[key] = ordinal
        rec.snapshot_ordinal = ordinal

        self._chunk.events.append(rec)
        self._chunk.count += 1
        if rec.is_delivery_data:
            self._chunk.delivery_events += 1
        if rec.snapshot == SNAPSHOT_LAST:
            self._chunk.saw_last = True
        self._retain(self._chunk, rec)
        self._chunk.nbytes += rec.nbytes
        self._chunk.mem_bytes += rec.nbytes
        self._maybe_spill_chunk()

        table_last = rec.snapshot in SNAPSHOT_TABLE_LAST
        overflow = (
            self._chunk.count >= self.snapshot_chunk_events
            or self._chunk.nbytes >= self.snapshot_chunk_bytes
        )
        if table_last or overflow:
            units.extend(self._close_chunk(last_for_table=table_last))
        return units

    def _close_chunk(self, *, last_for_table: bool) -> list[CompleteUnit]:
        if self._chunk is None:
            return []
        chunk = self._chunk
        self._chunk = None
        unit = CompleteUnit(
            kind=UNIT_SNAPSHOT_CHUNK,
            events=chunk.events,
            records=chunk.records,
            schema=chunk.schema,
            table=chunk.table,
            touched_tables=set(chunk.touched_tables),
            last_lsn=max((e.lsn or 0 for e in chunk.events), default=0),
            nbytes=chunk.nbytes,
            snapshot_last_for_table=last_for_table and not chunk.incremental,
            # ONLY when Debezium actually said so. Deriving "the whole snapshot
            # ended" from "a non-snapshot record arrived" swaps EVERY shadow over
            # its live table, which is live-table destruction the moment anything
            # can interleave with a snapshot stream (Opus M-7).
            snapshot_last=chunk.saw_last,
            spilled=bool(chunk.spilled),
            spilled_events=chunk.spilled,
            spill_unit_seq=chunk.spill_unit_seq,
            incremental=chunk.incremental,
            delivery_events=chunk.delivery_events,
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
    def _spill(self, target, events: list[PendingRecord], snapshot) -> int:
        """Hand `events` to the applier's staging callback, with explicit identity.

        The callback is told **which unit** the rows belong to and **whether they
        are snapshot rows** (and of which table). It used to be told neither, so it
        inferred the phase from mutable applier state that a different part of the
        applier only initialised later - which is how the first spilled chunk of
        every snapshot ended up staged into the live table with a streaming
        identity (Codex 1), and how a fenced unit's staged prefix stayed
        indistinguishable from everyone else's (Codex 5).
        """
        if target.spill_unit_seq is None:
            self._spill_unit_seq += 1
            target.spill_unit_seq = self._spill_unit_seq
        return self.on_spill(events, unit_seq=target.spill_unit_seq, snapshot=snapshot)

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
        staged = self._spill(self._txn, self._txn.events, None)
        if staged:
            self._txn.spilled += staged
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
        staged = self._spill(
            self._chunk, self._chunk.events, (self._chunk.schema, self._chunk.table)
        )
        if staged:
            self._chunk.spilled += staged
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
            discarded += self._txn.count
            log.info(
                "discarding un-ENDed tail of transaction %s (%s events); it replays "
                "on the next run",
                self._txn.txn_id,
                discarded,
            )
            self._txn = None
        if self._chunk is not None:
            discarded += self._chunk.count
            self._chunk = None
        discarded += len(self._deferred_snapshots)
        self._deferred_snapshots.clear()
        self.discarded_tail_events += discarded
        return discarded
