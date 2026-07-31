"""The transactional applier (ADR 0001 D1/D2/D3) — rubric 1.1, 1.2, 1.3.

One MotherDuck/DuckDB transaction per **commit group**; a commit group holds an
integral number of *whole* Postgres transactions (or whole snapshot chunks);
the resume point is written **inside** that transaction; Debezium is
acknowledged **after** it commits.

```
BEGIN TRANSACTION
    renew lease                       # 4.2 - the loser fails before it writes
    apply whole units, all tables     # 1.3 - multi-table atomicity
    write _cdc_flight.commit_log      # 1.7 / 6.1 audit trail
    write _cdc_flight.debezium_offsets# (4) data ∧ state atomic
COMMIT                                # <- the only durability event
markProcessed() / markBatchFinished() # <- the only thing in the window
next poll() -> performCommit() -> flushLsn()
```

**Invariant O** (ADR §4.1) is the whole correctness argument: at every instant,
every offset reachable through Debezium's offset store corresponds to data
already committed at the destination. Nothing enters that store before `COMMIT`,
so no lifecycle path — the poll loop (L1), a graceful `close()` (L2) or an error
teardown (L3) — can confirm an LSN to Postgres that the destination has not
committed. Loss therefore requires the slot to advance past durable data, which
is impossible by construction; duplication requires the engine to resume before
the durable resume point, which is impossible because that point is what we hand
it.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from . import apply_sql, destination, naming, offset_file
from .assembler import (
    UNIT_CONTROL,
    UNIT_SNAPSHOT_CHUNK,
    UNIT_TXN,
    CompleteUnit,
    TransactionAssembler,
)
from .destination import CONTROL_SCHEMA, Lease, ResumePoint
from .envelope import PendingRecord, decode
from .envelope import offsets_of as envelope_offsets
from .errors import ResumePointDrift
from .faults import maybe_crash
from .naming import (
    CDCF_COMMIT_ID,
    CDCF_EVENT_ID,
    CDCF_TOTAL_ORDER,
    quote,
)

log = logging.getLogger("cdc_flight.applier")

DBZ_COLUMN_TYPES = {
    "dbz_op": apply_sql.VARCHAR,
    "dbz_lsn": apply_sql.BIGINT,
    "dbz_tx_id": apply_sql.BIGINT,
    "dbz_schema": apply_sql.VARCHAR,
    "dbz_table": apply_sql.VARCHAR,
    "dbz_source_ts_ms": apply_sql.BIGINT,
}
APPLIER_COLUMN_TYPES = {
    CDCF_COMMIT_ID: apply_sql.BIGINT,
    CDCF_EVENT_ID: apply_sql.VARCHAR,
    CDCF_TOTAL_ORDER: apply_sql.BIGINT,
    **DBZ_COLUMN_TYPES,
}


@dataclass
class ApplierConfig:
    """Trigger policy (ADR §3.3). Soft triggers close a group at the *next* unit
    boundary and can never split a unit; the spill thresholds are the only hard
    ones and they change storage representation, never visibility."""

    commit_max_age: float = 5.0
    commit_max_events: int = 200_000
    commit_max_bytes: int = 256 * 1024 * 1024
    unit_spill_events: int = 500_000
    unit_spill_bytes: int = 64 * 1024 * 1024
    snapshot_chunk_events: int = 50_000
    snapshot_chunk_bytes: int = 64 * 1024 * 1024
    max_batch_size: int = 2048
    repair_offset_file: bool = True
    verify_offset_file: bool = True
    #: ADR 0001 §14.6, answered. `markProcessed(record)` is
    #: `offsetWriter.offset(record.sourcePartition(), record.sourceOffset())`
    #: (`AsyncEmbeddedEngine.java:1361-1366`) - a last-write-wins map put - so
    #: marking every record of a unit in order ends at exactly the value marking
    #: only its terminal record produces. Marking every record costs one JPype
    #: round trip each, which on a 200 000-event transaction is 200 000 of them
    #: and holds 200 000 Java references alive. Terminal-only is the default;
    #: `CDC_ACK_EVERY_RECORD=1` restores the conservative behaviour.
    ack_every_record: bool = False


@dataclass
class _SnapshotTable:
    schema: str
    table: str
    target: str
    shadow: str
    ordinal: int = 0
    reset_done: bool = False


@dataclass
class _TableWork:
    """Everything one destination table needs from one commit group."""

    target: str
    key_columns: tuple[str, ...] = ()
    keyless: bool = False
    columns: dict[str, str] = field(default_factory=dict)
    #: ordered, deduplicated identity keys touched by the group
    touched: dict[tuple, None] = field(default_factory=dict)
    #: identity key -> final row (None when the key's last event is a delete).
    #: A dict, so insertion order IS source order and membership is O(1). It used
    #: to be paired with an `order` list and `if key not in order`, which is a
    #: linear scan per event: MEASURED 458 s for one 200 000-event transaction,
    #: 1.6 s after this change.
    final: dict[tuple, dict | None] = field(default_factory=dict)
    snapshot: bool = False
    events: int = 0


class Applier:
    """Debezium change handler + destination writer. One instance per run."""

    def __init__(
        self,
        con,
        *,
        pipeline: str,
        namespace: str,
        dataset: str,
        topic_prefix: str,
        offset_path,
        resume_point: ResumePoint,
        config: ApplierConfig,
        lease: Lease,
        runner_id: str,
        verifier=None,
        transactional_ddl: bool = True,
    ):
        self.con = con
        self.pipeline = pipeline
        self.namespace = namespace
        self.dataset = dataset
        self.topic_prefix = topic_prefix
        self.offset_path = offset_path
        self.resume_point = resume_point
        self.cfg = config
        self.lease = lease
        self.runner_id = runner_id
        self.verifier = verifier
        self.transactional_ddl = transactional_ddl

        self.registry = apply_sql.SchemaRegistry(con, dataset)
        self.assembler = TransactionAssembler(
            snapshot_chunk_events=config.snapshot_chunk_events,
            snapshot_chunk_bytes=config.snapshot_chunk_bytes,
            spill_events=config.unit_spill_events,
            spill_bytes=config.unit_spill_bytes,
            on_spill=self._spill_events,
            keep_all_records=config.ack_every_record,
        )

        self._group: list[CompleteUnit] = []
        self._group_events = 0
        self._group_bytes = 0
        self._group_opened_at = time.monotonic()
        self._group_is_snapshot = False
        self._close_requested = False
        self._txn_open = False
        self._spill_commit_id: int | None = None
        self._spill_unit_seq = 0
        self._spill_rows = 0

        self._snapshot: dict[str, _SnapshotTable] = {}
        self._snapshot_epoch = resume_point.snapshot_epoch
        self._snapshot_session = False
        self._created_in_txn: set[str] = set()

        self._committer = None
        self._lock = threading.Lock()
        self._in_flight = 0
        self.last_batch_at = time.monotonic()

        # -- counters surfaced in the run summary (rubric 6.1) --------------- #
        self.record_count = 0
        self.batch_count = 0
        self.data_batch_count = 0
        self.skipped_count = 0
        self.commit_groups = 0
        self.data_commit_groups = 0
        self.applied_events = 0
        self.fenced_units = 0
        self.fenced_events = 0
        self.spilled_events = 0
        self.swaps = 0
        self.table_counts: dict[str, int] = {}
        self.last_commit_id = resume_point.commit_id
        self.error: BaseException | None = None
        self._next_commit_id = destination.next_commit_id(con)
        self._pending_offset_blob: bytes | None = None
        self._pending_offset_key_blob: bytes | None = None

        self._timer_stop = threading.Event()
        self._timer = threading.Thread(
            target=self._age_timer, name="cdc-commit-age", daemon=True
        )
        self._timer.start()

    # ------------------------------------------------------------------ #
    # supervisor-facing surface (kept identical to the previous handler)
    # ------------------------------------------------------------------ #
    @property
    def busy(self) -> bool:
        with self._lock:
            return self._in_flight > 0

    @property
    def seconds_since_last_batch(self) -> float:
        with self._lock:
            return time.monotonic() - self.last_batch_at

    def snapshot_counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self.table_counts)

    def stats(self) -> dict:
        return {
            "commit_groups": self.commit_groups,
            "data_commit_groups": self.data_commit_groups,
            "applied_events": self.applied_events,
            "fenced_units": self.fenced_units,
            "fenced_events": self.fenced_events,
            "spilled_events": self.spilled_events,
            "snapshot_swaps": self.swaps,
            "discarded_tail_events": self.assembler.discarded_tail_events,
            "orphan_end_markers": self.assembler.orphan_end_markers,
            "implicit_txn_opens": self.assembler.implicit_txn_opens,
            "last_commit_id": self.last_commit_id,
            "durable_lsn": self.resume_point.last_lsn,
            "transactional_ddl": self.transactional_ddl,
        }

    def _age_timer(self) -> None:
        """Ask for a group close on age. It can only ever *request*: the commit
        itself must happen on the poll thread, because `RecordCommitter` is
        explicitly not thread safe (`AsyncEmbeddedEngine.java:1341`)."""
        while not self._timer_stop.wait(0.5):
            if self._group and (
                time.monotonic() - self._group_opened_at >= self.cfg.commit_max_age
            ):
                self._close_requested = True

    def shutdown(self) -> None:
        self._timer_stop.set()

    # ------------------------------------------------------------------ #
    # the Debezium callback
    # ------------------------------------------------------------------ #
    def handle_batch(self, records, committer) -> None:
        with self._lock:
            self._in_flight += 1
        try:
            self._handle(records, committer)
        except BaseException as exc:
            self.error = exc
            raise
        finally:
            with self._lock:
                self._in_flight -= 1
                self.last_batch_at = time.monotonic()

    # pydbzengine compatibility, used only if something calls the old shape.
    def handleJsonBatch(self, records):  # pragma: no cover - not the live path
        raise RuntimeError("the applier needs the RecordCommitter; use handle_batch()")

    def _handle(self, records, committer) -> None:
        self._committer = committer
        n = len(records)
        data_in_batch = 0
        for raw in records:
            rec = decode(raw, topic_prefix=self.topic_prefix)
            if rec.is_data:
                data_in_batch += 1
            else:
                self.skipped_count += 1
            for unit in self.assembler.feed(rec):
                self._add_unit(unit)

        with self._lock:
            self.batch_count += 1
            self.record_count += n
            if data_in_batch:
                self.data_batch_count += 1

        if data_in_batch:
            maybe_crash("decode", self.data_batch_count)

        if not self._group:
            return
        # ADR §3.3 soft triggers, plus one pragmatic rule the ADR's pseudocode
        # needs and does not state: Debezium calls `markBatchFinished()` itself on
        # an *empty* poll and never calls us, so a group left buffered when the
        # stream goes quiet would never commit. A batch smaller than
        # `max.batch.size` means the queue drained, so commit now; a full batch
        # means more is already queued, so keep accumulating up to the triggers.
        drained = n < self.cfg.max_batch_size
        if self._close_requested or drained or self._soft_trigger_hit():
            self.commit_group(self._trigger_name(drained))

    def _soft_trigger_hit(self) -> bool:
        return (
            self._group_events >= self.cfg.commit_max_events
            or self._group_bytes >= self.cfg.commit_max_bytes
            or time.monotonic() - self._group_opened_at >= self.cfg.commit_max_age
        )

    def _trigger_name(self, drained: bool) -> str:
        if self._group_is_snapshot:
            return "snapshot_chunk"
        if self._group_events >= self.cfg.commit_max_events:
            return "events"
        if self._group_bytes >= self.cfg.commit_max_bytes:
            return "bytes"
        if self._close_requested:
            return "time"
        return "drained" if drained else "time"

    # ------------------------------------------------------------------ #
    # group assembly
    # ------------------------------------------------------------------ #
    def _add_unit(self, unit: CompleteUnit) -> None:
        is_snapshot = unit.kind == UNIT_SNAPSHOT_CHUNK
        # ADR §3.5: snapshot units are never mixed with streaming units, so a
        # commit_log row unambiguously says which phase it belongs to.
        if self._group and is_snapshot != self._group_is_snapshot:
            self.commit_group("snapshot_chunk" if self._group_is_snapshot else "phase")
        if not self._group:
            self._group_is_snapshot = is_snapshot
            self._group_opened_at = time.monotonic()

        if unit.kind == UNIT_TXN and unit.last_lsn and unit.last_lsn <= self.resume_point.last_lsn:
            # ADR §4.4 idempotency fence. Correctness does not depend on it - the
            # resume point already excludes these - but it is the difference
            # between "a replay is dropped" and "a replay is trusted", and it is
            # what makes the `CDC_OFFSET_FILE_REPAIR=0` mode safe.
            unit.fenced = True
            self.fenced_units += 1
            self.fenced_events += unit.event_count
            log.info(
                "fencing already-durable transaction %s (lsn %s <= durable %s)",
                unit.txn_id, unit.last_lsn, self.resume_point.last_lsn,
            )

        if not self.cfg.ack_every_record and len(unit.records) > 1:
            # Keep the terminal record (that is what carries the offset) and let
            # go of every other Java reference in the unit. This is what bounds
            # JVM memory for a large transaction; see ApplierConfig.
            for record in unit.records[:-1]:
                record.raw = None
            unit.records = [unit.records[-1]]

        self._group.append(unit)
        self._group_events += unit.event_count
        self._group_bytes += unit.nbytes

    def _reset_group(self) -> None:
        self._group = []
        self._group_events = 0
        self._group_bytes = 0
        self._group_opened_at = time.monotonic()
        self._group_is_snapshot = False
        self._close_requested = False
        self._created_in_txn = set()
        self._spill_commit_id = None
        self._spill_unit_seq = 0
        self._spill_rows = 0

    # ------------------------------------------------------------------ #
    # the transaction
    # ------------------------------------------------------------------ #
    def commit_group(self, trigger: str) -> None:
        group = self._group
        if not group:
            return
        commit_id = self._spill_commit_id or self._next_commit_id
        opened_at = destination.now()
        has_data = any(not u.fenced and u.events for u in group) or self._spill_rows > 0

        if not self._txn_open:
            self.con.execute("BEGIN TRANSACTION")
            self._txn_open = True
        try:
            if has_data:
                maybe_crash("begin", self.data_commit_groups + 1)
            self.lease.renew(self.con)
            stats = self._apply_units(group, commit_id)
            new_point = self._resume_point_for(group)
            destination.write_commit_log(
                self.con,
                commit_id=commit_id,
                pipeline=self.pipeline,
                runner_id=self.runner_id,
                opened_at=opened_at,
                committed_at=destination.now(),
                trigger=trigger,
                unit_count=sum(1 for u in group if not u.fenced),
                event_count=stats["events"],
                fenced_units=sum(1 for u in group if u.fenced),
                spilled=any(u.spilled for u in group),
                first_txn_id=stats["first_txn_id"],
                last_txn_id=stats["last_txn_id"],
                first_lsn=stats["first_lsn"],
                last_lsn=stats["last_lsn"],
                max_source_ts=_epoch_ms(stats["max_source_ts"]),
                tables_touched=sorted(_live_names(stats["tables"])),
            )
            destination.write_resume_point(
                self.con,
                pipeline=self.pipeline,
                namespace=self.namespace,
                point=new_point,
                commit_id=commit_id,
                offset_blob=self._pending_offset_blob,
                offset_key_blob=self._pending_offset_key_blob,
            )
            if has_data:
                maybe_crash("pre_commit", self.data_commit_groups + 1)
            self.con.execute("COMMIT")
            self._txn_open = False
            if has_data:
                maybe_crash("post_commit_pre_ack", self.data_commit_groups + 1)
        except BaseException:
            self._rollback_quietly()
            raise

        # ── the ONLY window that matters, and it contains nothing else ──────
        marked = 0
        for unit in group:
            for rec in unit.records:
                if rec.raw is None:  # released by `_add_unit`
                    continue
                self._committer.markProcessed(rec.raw)
                marked += 1
        before = self.verifier.before() if self.verifier else None
        self._committer.markBatchFinished()
        if self.verifier is not None:
            self.verifier.after(before, marked=marked)
        if has_data:
            maybe_crash("post_ack", self.data_commit_groups + 1)
        # next poll() -> performCommit() -> flushLsn(new)  ── nothing between ──

        self.commit_groups += 1
        if has_data:
            self.data_commit_groups += 1
        self.applied_events += stats["events"]
        self.last_commit_id = commit_id
        self.resume_point = new_point
        self._next_commit_id = commit_id + 1
        self._capture_offset_file(new_point)
        self._reset_group()

    def _rollback_quietly(self) -> None:
        if not self._txn_open:
            return
        try:
            self.con.execute("ROLLBACK")
        except Exception:  # pragma: no cover - never mask the original error
            log.debug("rollback failed", exc_info=True)
        finally:
            self._txn_open = False
            # Every CREATE / ALTER we issued is gone with the transaction, so the
            # cached destination shape is now a lie. Rebuilding it is cheap and
            # not doing it is how a rolled-back run corrupts the next one.
            self.registry = apply_sql.SchemaRegistry(self.con, self.dataset)
            self._created_in_txn = set()

    # -- resume point ------------------------------------------------------- #
    def _resume_point_for(self, group: list[CompleteUnit]) -> ResumePoint:
        terminal: PendingRecord | None = None
        for unit in reversed(group):
            if unit.records:
                terminal = unit.records[-1]
                break
        if terminal is not None and terminal.source_offset is None and terminal.raw is not None:
            # Decoded lazily: only this one record's Connect offset is needed, and
            # reading it for all 200 000 of them is what made decode the bottleneck.
            terminal.source_partition, terminal.source_offset = envelope_offsets(terminal.raw)
        last_unit = group[-1]
        last_lsn = max(
            [self.resume_point.last_lsn] + [u.last_lsn or 0 for u in group]
        )
        total_order = None
        for unit in reversed(group):
            if unit.events:
                total_order = unit.events[-1].total_order
                break
        return ResumePoint(
            partition=(terminal.source_partition if terminal else self.resume_point.partition) or {},
            offset=(terminal.source_offset if terminal else self.resume_point.offset) or {},
            last_lsn=last_lsn,
            last_txn_id=last_unit.txn_id or self.resume_point.last_txn_id,
            last_total_order=total_order,
            commit_id=self.last_commit_id,
            snapshot_epoch=self._snapshot_epoch,
        )

    def _capture_offset_file(self, point: ResumePoint) -> None:
        """Snapshot `offsets.dat` after the acknowledgement (ADR §4.3).

        The bytes belong to the group that has *just* been acknowledged, so they
        can only ride on the *next* group's transaction. They are redundant -
        `resume_json` is the source of truth - but they let start-up rebuild a
        byte-exact file, and they make format drift visible immediately.
        """
        if not self.cfg.verify_offset_file:
            return
        try:
            entries = offset_file.read(self.offset_path)
        except Exception:  # pragma: no cover
            return
        if not entries:
            return
        key = next(iter(entries))
        self._pending_offset_key_blob = key
        self._pending_offset_blob = _serialise_entries(entries)
        file_offsets = offset_file.parse_offsets(entries)
        if not file_offsets:
            return
        _partition, offset = file_offsets[0]
        file_lsn = offset_file.lsn_of(offset)
        if file_lsn is not None and point.last_lsn and file_lsn > point.last_lsn:
            # Invariant O says this cannot happen: nothing enters the offset store
            # before COMMIT. If it ever does, it is the ADR-rev-2 bug class.
            raise ResumePointDrift(
                f"offsets.dat claims lsn {file_lsn}, ahead of the durable resume "
                f"point {point.last_lsn}. Invariant O is violated (ADR 0001 §4.3)."
            )

    # ------------------------------------------------------------------ #
    # applying units
    # ------------------------------------------------------------------ #
    def _apply_units(self, group: list[CompleteUnit], commit_id: int) -> dict:
        work: dict[str, _TableWork] = {}
        stats = {
            "events": 0,
            "tables": set(),
            "first_txn_id": None,
            "last_txn_id": None,
            "first_lsn": None,
            "last_lsn": None,
            "max_source_ts": None,
        }
        swaps: list[_SnapshotTable] = []
        swap_all = False

        for unit in group:
            if unit.fenced:
                continue
            if unit.kind == UNIT_CONTROL:
                continue
            if unit.kind == UNIT_SNAPSHOT_CHUNK:
                self._group_is_snapshot = True
                state = self._snapshot_state(unit.schema, unit.table)
                for event in unit.events:
                    self._collect(work, event, commit_id, snapshot=state, stats=stats)
                if unit.snapshot_last_for_table and state is not None:
                    swaps.append(state)
                if unit.snapshot_last:
                    swap_all = True
                continue

            for event in unit.events:
                self._collect(work, event, commit_id, snapshot=None, stats=stats)
            if unit.txn_id:
                stats["first_txn_id"] = stats["first_txn_id"] or unit.txn_id
                stats["last_txn_id"] = unit.txn_id

        maybe_crash("mid_apply", self.data_commit_groups + 1)

        for table_work in work.values():
            self._write_table(table_work)
            if table_work.events:
                stats["tables"].add(table_work.target)
                with self._lock:
                    self.table_counts[table_work.target] = (
                        self.table_counts.get(table_work.target, 0) + table_work.events
                    )

        if self._spill_rows:
            stats["events"] += self._drain_spill(commit_id, stats)

        if swap_all:
            swaps = list(self._snapshot.values())
        for state in swaps:
            self._swap(state, commit_id, stats)
        return stats

    def _collect(
        self,
        work: dict[str, _TableWork],
        event: PendingRecord,
        commit_id: int,
        *,
        snapshot: _SnapshotTable | None,
        stats: dict,
    ) -> None:
        if not event.schema or not event.table:
            return
        if snapshot is not None:
            target = snapshot.shadow
            snapshot.ordinal += 1
            event_id = (
                f"snap:{self._snapshot_epoch}:{event.schema}.{event.table}:"
                f"{snapshot.ordinal}"
            )
        else:
            target = self._target_table(event.schema, event.table)
            event_id = _stream_event_id(event)

        item = work.get(target)
        if item is None:
            item = _TableWork(target=target, keyless=event.key is None, snapshot=snapshot is not None)
            if event.key:
                item.key_columns = tuple(naming.normalize(k) for k in event.key)
            else:
                item.key_columns = (CDCF_EVENT_ID,)
            work[target] = item

        row = self._row_for(event, commit_id, event_id, snapshot=snapshot is not None)
        for column, value in row.items():
            item.columns[column] = apply_sql.widen(
                item.columns.get(column), apply_sql.sql_type(value)
            )
        item.events += 1
        stats["events"] += 1
        if event.lsn:
            stats["first_lsn"] = stats["first_lsn"] or event.lsn
            stats["last_lsn"] = event.lsn
        if event.source_ts_ms:
            stats["max_source_ts"] = max(stats["max_source_ts"] or 0, event.source_ts_ms)

        if item.keyless:
            key = (event_id,)
            item.touched[key] = None
            item.final[key] = row
            return

        raw_key = tuple(event.key[k] for k in event.key)
        item.touched.setdefault(raw_key, None)
        # A primary-key UPDATE under REPLICA IDENTITY FULL arrives as one event
        # whose `before` carries the OLD key. Touching both keys is what makes
        # "delete old, insert new" fall out of the normal path (rubric 1.4).
        if event.before and all(k in event.before for k in event.key):
            old_key = tuple(event.before[k] for k in event.key)
            if old_key != raw_key:
                item.touched.setdefault(old_key, None)
        item.final[raw_key] = None if event.op == "d" else row

    def _row_for(
        self, event: PendingRecord, commit_id: int, event_id: str, *, snapshot: bool
    ) -> dict[str, Any]:
        image = event.after if event.op != "d" else event.before
        row: dict[str, Any] = {}
        for column, value in (image or {}).items():
            row[naming.normalize(column)] = value
        row[CDCF_COMMIT_ID] = commit_id
        row[CDCF_EVENT_ID] = event_id
        # A snapshot record has no transaction, so it has no ordinal. Leaving it
        # NULL is what tells a consumer "this identity is not txn-derived".
        row[CDCF_TOTAL_ORDER] = None if snapshot else event.total_order
        row["dbz_op"] = event.op
        row["dbz_lsn"] = event.lsn
        row["dbz_tx_id"] = None if snapshot else _as_int(event.txn_id)
        row["dbz_schema"] = event.schema
        row["dbz_table"] = event.table
        row["dbz_source_ts_ms"] = event.source_ts_ms
        return row

    def _write_table(self, item: _TableWork) -> None:
        # A column every event left NULL tells us nothing about its type; VARCHAR
        # is the honest placeholder and `widen()` upgrades it the moment a real
        # value shows up (rubric 2.1/2.5 own the better answer).
        columns = {
            col: ctype or apply_sql.VARCHAR for col, ctype in item.columns.items()
        }
        # The applier's own columns have KNOWN types; they are declared, never
        # inferred. Widening them against a group in which they all happened to be
        # NULL is how `cdcf_total_order` silently became VARCHAR.
        columns.update(APPLIER_COLUMN_TYPES)
        for column in item.key_columns:
            columns.setdefault(column, apply_sql.VARCHAR)

        table, created = self.registry.ensure(
            item.target, columns=columns, key_columns=item.key_columns
        )
        if created:
            self._created_in_txn.add(item.target)
        # A table this transaction created is empty, so the DELETE half of the
        # merge cannot match anything: skipping it turns a snapshot into a pure
        # bulk insert instead of N key probes against a growing table.
        fresh = item.target in self._created_in_txn

        column_order = [c for c in table.columns if c in columns] + [
            c for c in columns if c not in table.columns
        ]
        column_order = list(dict.fromkeys(column_order))

        if not (fresh or item.snapshot):
            keys = [
                tuple(
                    apply_sql.bind(value, table.columns.get(col, apply_sql.VARCHAR))
                    for col, value in zip(item.key_columns, key, strict=False)
                )
                for key in item.touched
            ]
            apply_sql.delete_keys(self.con, table, item.key_columns, keys)

        rows: list[list] = []
        for row in item.final.values():
            if row is None:
                continue
            rows.append(
                [
                    apply_sql.bind(row.get(col), table.columns.get(col, apply_sql.VARCHAR))
                    for col in column_order
                ]
            )
        apply_sql.insert_rows(self.con, table, column_order, rows)

    # ------------------------------------------------------------------ #
    # snapshot phase (ADR §3.5 + D7)
    # ------------------------------------------------------------------ #
    def _target_table(self, schema: str, table: str) -> str:
        key = f"{schema}.{table}"
        state = self._snapshot.get(key)
        if state is not None:
            # CDC that arrives while a table is being backfilled is applied to the
            # shadow table, so the swap is instantaneous and CDC never stops
            # (ADR §7 note 2 - this is what makes rubric 3.3 "simple").
            return state.shadow
        return naming.destination_table(self.topic_prefix, schema, table)

    def _snapshot_state(self, schema: str | None, table: str | None) -> _SnapshotTable | None:
        if not schema or not table:
            return None
        key = f"{schema}.{table}"
        state = self._snapshot.get(key)
        if state is not None:
            return state
        if not self._snapshot_session:
            self._snapshot_session = True
            self._snapshot_epoch = self.resume_point.snapshot_epoch + 1
            log.info("snapshot session started, epoch=%s", self._snapshot_epoch)
        target = naming.destination_table(self.topic_prefix, schema, table)
        state = _SnapshotTable(
            schema=schema, table=table, target=target, shadow=naming.shadow_table(target)
        )
        # A crash mid-snapshot means Debezium re-snapshots from the beginning
        # (`InitialSnapshotter.shouldSnapshotData` returns true while the offset
        # says a snapshot was in progress). Dropping the shadow here is what makes
        # that idempotent - not event identity (ADR §3.5).
        self.con.execute(
            f"DROP TABLE IF EXISTS {quote(self.dataset)}.{quote(state.shadow)}"
        )
        self.registry.forget(state.shadow)
        self._created_in_txn.add(state.shadow)
        self.con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.table_state WHERE pipeline = ? AND "
            "source_schema = ? AND source_table = ?",
            [self.pipeline, schema, table],
        )
        self.con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.table_state "
            "(pipeline, source_schema, source_table, target_table, snapshot_state, "
            " snapshot_epoch) VALUES (?,?,?,?,'in_progress',?)",
            [self.pipeline, schema, table, target, self._snapshot_epoch],
        )
        self._snapshot[key] = state
        return state

    def _swap(self, state: _SnapshotTable, commit_id: int, stats: dict) -> None:
        key = f"{state.schema}.{state.table}"
        if key not in self._snapshot:
            return
        shadow = f"{quote(self.dataset)}.{quote(state.shadow)}"
        live = f"{quote(self.dataset)}.{quote(state.target)}"
        exists = self.con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [self.dataset, state.shadow],
        ).fetchone()[0]
        if exists:
            if self.transactional_ddl:
                self.con.execute(f"DROP TABLE IF EXISTS {live}")
                self.con.execute(
                    f"ALTER TABLE {shadow} RENAME TO {quote(state.target)}"
                )
            else:
                # ADR §7's documented fallback; the rubric explicitly allows it
                # ("BEGIN / COMMIT transactionality fine too").
                self.con.execute(
                    f"CREATE OR REPLACE TABLE {live} AS SELECT * FROM {shadow}"
                )
                self.con.execute(f"DROP TABLE {shadow}")
            self.registry.forget(state.shadow)
            self.registry.forget(state.target)
            self._created_in_txn.discard(state.shadow)
            self.swaps += 1
            stats["tables"].add(state.target)
        self.con.execute(
            f"UPDATE {CONTROL_SCHEMA}.table_state SET snapshot_state = 'complete', "
            "snapshot_lsn = ?, last_commit_id = ? WHERE pipeline = ? AND "
            "source_schema = ? AND source_table = ?",
            [stats.get("last_lsn"), commit_id, self.pipeline, state.schema, state.table],
        )
        self._snapshot.pop(key, None)
        if not self._snapshot:
            self._snapshot_session = False

    # ------------------------------------------------------------------ #
    # spill (ADR §3.4)
    # ------------------------------------------------------------------ #
    def _spill_events(self, events: list[PendingRecord]) -> int:
        """Stage a unit's events inside the group's own transaction.

        Because staging and drain are in the *same* transaction, nothing is ever
        visible early, so rubric 1.3 is not weakened; and a crash rolls the
        staging rows back with everything else, so there is no orphan cleanup
        problem (ADR §3.4).
        """
        if not events:
            return 0
        if not self._txn_open:
            self.con.execute("BEGIN TRANSACTION")
            self._txn_open = True
            self._spill_commit_id = self._next_commit_id
        commit_id = self._spill_commit_id or self._next_commit_id
        self._spill_unit_seq += 1
        rows = []
        for seq, event in enumerate(events, start=1):
            if not event.schema or not event.table:
                continue
            snapshot_state = self._snapshot.get(f"{event.schema}.{event.table}")
            if snapshot_state is not None:
                snapshot_state.ordinal += 1
                event_id = (
                    f"snap:{self._snapshot_epoch}:{event.schema}.{event.table}:"
                    f"{snapshot_state.ordinal}"
                )
                target = snapshot_state.shadow
            else:
                event_id = _stream_event_id(event)
                target = self._target_table(event.schema, event.table)
            rows.append(
                [
                    commit_id, self._spill_unit_seq, event.total_order or seq, target,
                    event.schema, event.table, event.lsn, event.txn_id,
                    event.total_order, event_id, event.op, event.source_ts_ms,
                    json.dumps(event.before, default=str) if event.before else None,
                    json.dumps(event.after, default=str) if event.after else None,
                    json.dumps(event.key, default=str) if event.key else None,
                ]
            )
        apply_sql.bulk_insert(
            self.con,
            f"{CONTROL_SCHEMA}.spill_events",
            ["commit_id", "unit_seq", "event_seq", "target_table", "source_schema",
             "source_table", "lsn", "txn_id", "total_order", "cdcf_event_id", "op",
             "source_ts_ms", "before_json", "after_json", "key_json"],
            rows,
            [apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.VARCHAR,
             apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BIGINT, apply_sql.VARCHAR,
             apply_sql.BIGINT, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BIGINT,
             apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR],
        )
        self._spill_rows += len(rows)
        self.spilled_events += len(rows)
        maybe_crash("spill", self.data_commit_groups + 1)
        return len(events)

    def _drain_spill(self, commit_id: int, stats: dict) -> int:
        """Project the staged rows into their target tables, then clear them.

        Reads `ORDER BY unit_seq, event_seq`, which is exactly source order, so
        the merge semantics are identical to the in-memory path.
        """
        staged = self.con.execute(
            f"SELECT target_table, source_schema, source_table, lsn, txn_id, total_order, "
            "       cdcf_event_id, op, source_ts_ms, before_json, after_json, key_json "
            f"FROM {CONTROL_SCHEMA}.spill_events WHERE commit_id = ? "
            "ORDER BY unit_seq, event_seq",
            [commit_id],
        ).fetchall()
        work: dict[str, _TableWork] = {}
        applied = 0
        for row in staged:
            (
                target, schema, table, lsn, txn_id, total_order, event_id, op,
                source_ts_ms, before_json, after_json, key_json,
            ) = row
            event = PendingRecord(
                raw=None, kind="data", topic="", nbytes=0, op=op, schema=schema,
                table=table, lsn=lsn, txn_id=txn_id, total_order=total_order,
                source_ts_ms=source_ts_ms,
                key=json.loads(key_json) if key_json else None,
                before=json.loads(before_json) if before_json else None,
                after=json.loads(after_json) if after_json else None,
            )
            item = work.get(target)
            if item is None:
                item = _TableWork(
                    target=target,
                    keyless=event.key is None,
                    snapshot=target.endswith(naming.SHADOW_SUFFIX),
                )
                item.key_columns = (
                    tuple(naming.normalize(k) for k in event.key)
                    if event.key
                    else (CDCF_EVENT_ID,)
                )
                work[target] = item
            self._collect_prepared(item, event, commit_id, event_id, stats)
            applied += 1
        for item in work.values():
            self._write_table(item)
            stats["tables"].add(item.target)
        self.con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.spill_events WHERE commit_id = ?", [commit_id]
        )
        self._spill_rows = 0
        return applied

    def _collect_prepared(
        self, item: _TableWork, event: PendingRecord, commit_id: int, event_id: str, stats: dict
    ) -> None:
        row = self._row_for(event, commit_id, event_id, snapshot=item.snapshot)
        for column, value in row.items():
            item.columns[column] = apply_sql.widen(
                item.columns.get(column), apply_sql.sql_type(value)
            )
        item.events += 1
        if event.lsn:
            stats["first_lsn"] = stats["first_lsn"] or event.lsn
            stats["last_lsn"] = event.lsn
        if item.keyless:
            key = (event_id,)
        else:
            key = tuple(event.key[k] for k in event.key)
            if event.before and all(k in event.before for k in event.key):
                old = tuple(event.before[k] for k in event.key)
                if old != key:
                    item.touched.setdefault(old, None)
        item.touched.setdefault(key, None)
        item.final[key] = None if (event.op == "d" and not item.keyless) else row

    # ------------------------------------------------------------------ #
    # shutdown
    # ------------------------------------------------------------------ #
    def drain_on_shutdown(self) -> int:
        """Discard the un-`END`ed tail (ADR §3.2). Returns discarded event count.

        Deliberately does NOT try to commit: the tail cannot be proven whole, and
        Invariant O guarantees nothing about it was acknowledged, so replaying it
        is free.
        """
        self.shutdown()
        return self.assembler.discard_open_unit()


def _stream_event_id(event: PendingRecord) -> str:
    """`"<commit lsn>:<txId>:<transaction.total_order>"` (ADR §6).

    `total_order` is the connector's own 1-based ordinal within the transaction,
    restored from the offset on restart, so it is stable across a replay of the
    same WAL. `source.sequence` is NOT an ordinal (it is
    `[lastCommitLsn, currentLsn]`, `SourceInfo.java:180-196`) and several events
    can share one LSN, which is why the LSN alone cannot be the identity
    (Codex 3).
    """
    return f"{event.lsn}:{event.txn_id}:{event.total_order}"


def _live_names(tables: set[str]) -> set[str]:
    """Report the table an operator knows about, not the shadow it landed in."""
    return {
        t[: -len(naming.SHADOW_SUFFIX)] if t.endswith(naming.SHADOW_SUFFIX) else t
        for t in tables
    }


def _epoch_ms(value) -> Any:
    """Debezium's `source.ts_ms` as a timestamp, so end-to-end lag is a SQL
    subtraction rather than an arithmetic puzzle for whoever writes rubric 6.1."""
    if value is None:
        return None
    from datetime import UTC, datetime

    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


def _as_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _serialise_entries(entries: dict[bytes, bytes]) -> bytes:
    return json.dumps(
        {k.decode("utf-8", "replace"): v.decode("utf-8", "replace") for k, v in entries.items()},
        separators=(",", ":"),
    ).encode("utf-8")
