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
from dataclasses import dataclass
from typing import Any

from . import apply_sql, destination, naming, offset_file, table_work
from .assembler import (
    UNIT_CONTROL,
    UNIT_SNAPSHOT_CHUNK,
    UNIT_TXN,
    CompleteUnit,
    TransactionAssembler,
)
from .destination import Lease, ResumePoint
from .envelope import PendingRecord, decode
from .envelope import offsets_of as envelope_offsets
from .errors import ResumePointDrift
from .faults import maybe_crash
from .snapshot import SnapshotCoordinator, SnapshotTable
from .spill import SpillBuffer, StagedEvent
from .table_work import TableWork

log = logging.getLogger("cdc_flight.applier")


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
    #: PRIMARY KEY on every generated table's identity columns (Opus M-2).
    destination_constraints: bool = True
    #: ADR 0001 §14.6, answered. `markProcessed(record)` is
    #: `offsetWriter.offset(record.sourcePartition(), record.sourceOffset())`
    #: (`AsyncEmbeddedEngine.java:1361-1366`) - a last-write-wins map put - so
    #: marking every record of a unit in order ends at exactly the value marking
    #: only its terminal record produces. Marking every record costs one JPype
    #: round trip each, which on a 200 000-event transaction is 200 000 of them
    #: and holds 200 000 Java references alive. Terminal-only is the default;
    #: `CDC_ACK_EVERY_RECORD=1` restores the conservative behaviour.
    ack_every_record: bool = False


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

        self.registry = apply_sql.SchemaRegistry(
            con, dataset, constraints=config.destination_constraints
        )
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
        self._pending_verification: tuple | None = None

        self._created_in_txn: set[str] = set()
        # ADR §3.5 / D7 and §3.4 live in their own modules now (Codex 8): the
        # snapshot-spill blocker was a direct consequence of spill routing reaching
        # into snapshot state that a different part of this file initialised later.
        self.snapshots = SnapshotCoordinator(
            con,
            dataset=dataset,
            pipeline=pipeline,
            topic_prefix=topic_prefix,
            created_in_txn=self._created_in_txn,
            get_registry=lambda: self.registry,
            epoch=resume_point.snapshot_epoch,
            transactional_ddl=transactional_ddl,
        )
        self.spill = SpillBuffer(con)

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
        self.fenced_spilled_events = 0
        self.deferred_units = 0
        self.deferred_events = 0
        self.table_counts: dict[str, int] = {}
        self.last_commit_id = resume_point.commit_id
        self.error: BaseException | None = None
        self._next_commit_id = destination.next_commit_id(con, pipeline)
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
            "fenced_spilled_events": self.fenced_spilled_events,
            # Whole units that were buffered but never committed at shutdown. Safe -
            # Invariant O means they replay - but a run that reports `ok: true` while
            # silently deferring transactions should say so (Opus MINOR-9).
            "deferred_units": self.deferred_units,
            "deferred_events": self.deferred_events,
            "snapshot_swaps": self.snapshots.swaps,
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
        # The previous group's offset flush is verified here, outside the
        # commit->ack window, now that Debezium has polled at least once since it
        # (Codex 7).
        self._run_pending_verification()
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
        if self.assembler.open_unit_has_spilled:
            # Invariant B: the rows staged for the still-open unit live in this
            # group's transaction, so committing now would drain a PARTIAL Postgres
            # transaction into the destination. Wait for its END.
            return
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
        # `.clear()`, not a fresh set: `SnapshotCoordinator` holds the same object.
        self._created_in_txn.clear()
        self._spill_commit_id = None

    # ------------------------------------------------------------------ #
    # the transaction
    # ------------------------------------------------------------------ #
    def commit_group(self, trigger: str) -> None:
        group = self._group
        if not group:
            return
        commit_id = self._spill_commit_id or self._next_commit_id
        opened_at = destination.now()
        # NOT `or spill.rows > 0`: staged rows belonging only to *fenced*
        # units are about to be discarded, and counting them made a group with no
        # applicable content a "data group", which shifts every `<nth>`-indexed
        # fault anchor by one (Codex 5).
        has_data = any(
            not u.fenced and (u.events or u.spilled_events) for u in group
        )

        if not self._txn_open:
            self.con.execute("BEGIN TRANSACTION")
            self._txn_open = True
        try:
            if has_data:
                maybe_crash("begin", self.data_commit_groups + 1)
            self.lease.renew(self.con)
            stats = self._apply_units(group, commit_id, has_data=has_data)
            new_point = self._resume_point_for(group, commit_id)
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
                tables_touched=sorted(table_work.live_names(stats["tables"])),
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
            # Principle (3): the pre-flush fingerprint of `offsets.dat` is taken
            # HERE, before the commit, because it is only a *forensic* baseline -
            # it does not need to lengthen the commit->ack path (Codex 7).
            offset_fingerprint = self.verifier.before() if self.verifier else None
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
        self._committer.markBatchFinished()
        if has_data:
            maybe_crash("post_ack", self.data_commit_groups + 1)
        # next poll() -> performCommit() -> flushLsn(new)  ── nothing between ──
        # No filesystem work, no hashing: the "did the flush happen" check is a
        # liveness canary, not a prerequisite under Invariant O, so it runs on the
        # next batch (or at shutdown) once the connector has had its poll/commit
        # opportunity (Codex 7).
        if self.verifier is not None and marked:
            self._pending_verification = (offset_fingerprint, marked)

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
            self.registry = apply_sql.SchemaRegistry(
                self.con, self.dataset, constraints=self.cfg.destination_constraints
            )
            self._created_in_txn.clear()

    # -- resume point ------------------------------------------------------- #
    def _resume_point_for(self, group: list[CompleteUnit], commit_id: int) -> ResumePoint:
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
        if last_lsn > self.resume_point.last_lsn and (
            terminal is None or not terminal.source_offset
        ):
            # `envelope.offsets_of()` returns `(None, None)` for every bridge
            # failure, and the old code then paired a NEWER `last_lsn` with the
            # PREVIOUS (or an empty) offset map. Debezium would resume from the
            # older offset while our fence claimed the newer LSN was durable, so a
            # replay would be fenced away: silent loss (Codex 3). Refuse the commit
            # instead - a rollback replays, which is free.
            raise ResumePointDrift(
                f"commit group would advance last_lsn to {last_lsn} but the terminal "
                "record's Connect offset could not be read, so the resume point would "
                "pair a newer LSN with an older offset map (ADR 0001 §4.3)"
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
            # The group being written, not the previous one. `ResumePoint.to_json`
            # omits it and `read_resume_point` takes it from its own column, so this
            # was dead but looked live (Opus MINOR-16).
            commit_id=commit_id,
            snapshot_epoch=self.snapshots.epoch,
        )

    def _run_pending_verification(self) -> None:
        """Check a deferred offset flush, now that the connector has polled again.

        Deliberately outside the commit->ack window (Codex 7). It is still
        meaningful there: `markBatchFinished()` on an *empty* poll comes from an
        independent committer that never marked a record, so `beginFlush()` finds
        nothing to flush and does not rewrite the file - only our own
        acknowledgement can have moved it.
        """
        pending = self._pending_verification
        if pending is None or self.verifier is None:
            return
        self._pending_verification = None
        before, marked = pending
        self.verifier.after(before, marked=marked)

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
    def _apply_units(self, group: list[CompleteUnit], commit_id: int, *, has_data: bool) -> dict:
        """One ordered pass over the group, whatever each unit's storage mode is.

        This used to be two passes - "write every in-memory `TableWork`, then
        drain `spill_events`" - and that split cannot be correct in either order
        (Opus B-1). A unit that spills keeps accumulating an in-memory **tail**
        after the spill, so its staged rows are *earlier* in source order than its
        own tail; and a group can hold `unit1 (spilled + tail), unit2 (wholly in
        memory)`, whose correct order interleaves the two representations. The
        measured consequences were a destination left holding the **earlier** value
        with the later change event gone (existing table), and the **same primary
        key twice** (table created inside the group, so both passes skipped the
        DELETE half of the merge).

        So: walk the units in group order, and for each one load its staged prefix
        into the *shared* `work` map before collecting its in-memory tail. One
        `table_work.write()` per destination table, source order preserved end to
        end, and the merge sees the whole group at once.
        """
        work: dict[str, TableWork] = {}
        stats = {
            "events": 0,
            "tables": set(),
            "first_txn_id": None,
            "last_txn_id": None,
            "first_lsn": None,
            "last_lsn": None,
            "max_source_ts": None,
        }
        swaps: list[SnapshotTable] = []
        swap_all = False
        staged_units = any(u.spill_unit_seq is not None for u in group)

        for unit in group:
            if unit.fenced:
                # ADR §4.4 / Codex 5: the fence is set at `_add_unit`, which is the
                # unit's END - long after its prefix was staged. Skipping only the
                # in-memory half re-applied the prefix of a transaction the
                # destination already holds, which made A9's "the fence alone
                # prevents duplication" false for every spilled unit. The staged
                # rows are deleted with the rest below, inside this transaction.
                if unit.spill_unit_seq is not None:
                    self.fenced_spilled_events += unit.spilled_events
                continue
            if unit.kind == UNIT_CONTROL:
                continue
            if unit.kind == UNIT_SNAPSHOT_CHUNK:
                self._group_is_snapshot = True
                state = self.snapshots.state_for(unit.schema, unit.table)
                if unit.spill_unit_seq is not None:
                    self._load_staged(unit, work, commit_id, stats)
                for event in unit.events:
                    self._collect(work, event, commit_id, snapshot=state, stats=stats)
                if unit.snapshot_last_for_table and state is not None:
                    swaps.append(state)
                if unit.snapshot_last:
                    swap_all = True
                continue

            if unit.spill_unit_seq is not None:
                self._load_staged(unit, work, commit_id, stats)
            for event in unit.events:
                self._collect(work, event, commit_id, snapshot=None, stats=stats)
            if unit.txn_id:
                stats["first_txn_id"] = stats["first_txn_id"] or unit.txn_id
                stats["last_txn_id"] = unit.txn_id

        for index, item in enumerate(work.values()):
            table_work.write(
                self.con, self.registry, item, self._created_in_txn
            )
            if index == 0 and has_data:
                # The anchor documented as "some tables written, others not". It
                # used to fire BEFORE this loop, so it could not detect a
                # transaction torn between table A and table B - the one
                # interleaving rubric 1.3 is about (Codex 6). It is also gated on
                # `has_data` now, like every other anchor, because `<nth>` counts
                # data-carrying groups (Opus MINOR-2).
                maybe_crash("mid_apply", self.data_commit_groups + 1)
            if item.events:
                stats["tables"].add(item.target)
                with self._lock:
                    self.table_counts[item.target] = (
                        self.table_counts.get(item.target, 0) + item.events
                    )

        if staged_units:
            self.spill.clear(commit_id)

        if swap_all:
            swaps = self.snapshots.states()
        for state in swaps:
            if self.snapshots.swap(
                state, commit_id=commit_id, snapshot_lsn=stats.get("last_lsn")
            ):
                stats["tables"].add(state.target)
        return stats

    def _collect(
        self,
        work: dict[str, TableWork],
        event: PendingRecord,
        commit_id: int,
        *,
        snapshot: SnapshotTable | None,
        stats: dict,
    ) -> None:
        if not event.schema or not event.table:
            return
        if snapshot is not None:
            target = snapshot.shadow
            event_id = self.snapshots.event_id(event)
        else:
            target = self.snapshots.target_table(event.schema, event.table)
            event_id = _stream_event_id(event)
        item = table_work.work_for(work, target, event, snapshot is not None)
        self._collect_prepared(item, event, commit_id, event_id, stats)

    # ------------------------------------------------------------------ #
    # spill (ADR §3.4)
    # ------------------------------------------------------------------ #
    def _spill_events(
        self,
        events: list[PendingRecord],
        *,
        unit_seq: int,
        snapshot: tuple[str | None, str | None] | None = None,
    ) -> int:
        """Stage one unit's events inside the group's own transaction (ADR §3.4).

        `unit_seq` and `snapshot` are **inputs**, not inferences. This callback used
        to look the phase up in the applier's snapshot mapping, which `_apply_units`
        populates only later, so on the first spilled chunk of every snapshot it
        concluded "streaming" and staged the rows into the **live** table with a
        `<lsn>:None:None` identity; a consumer could then see a partial snapshot, and
        the swap dropped those rows (Codex 1). Resolving the shadow *here*, through
        the coordinator, is what makes that impossible; `unit_seq` is what lets the
        drain order and fence per unit (Opus B-1, Codex 5).
        """
        if not events:
            return 0
        if not self._txn_open:
            self.con.execute("BEGIN TRANSACTION")
            self._txn_open = True
            self._spill_commit_id = self._next_commit_id
        commit_id = self._spill_commit_id or self._next_commit_id
        # Creates the shadow table, its `table_state` row and the snapshot epoch
        # BEFORE any record of this table can be staged.
        state = self.snapshots.state_for(*snapshot) if snapshot is not None else None

        prepared: list[StagedEvent] = []
        for event in events:
            if not event.schema or not event.table:
                continue
            if state is not None:
                prepared.append(
                    StagedEvent(
                        event=event,
                        event_id=self.snapshots.event_id(event),
                        target=state.shadow,
                        seq=event.snapshot_ordinal,
                    )
                )
            else:
                prepared.append(
                    StagedEvent(
                        event=event,
                        event_id=_stream_event_id(event),
                        target=self.snapshots.target_table(event.schema, event.table),
                        # Mandatory and validated by the assembler, so there is
                        # nothing to substitute a local sequence for: doing that gave
                        # a replay a different identity (Codex 4).
                        seq=event.total_order,
                    )
                )
        staged = self.spill.stage(
            commit_id=commit_id, unit_seq=unit_seq, prepared=prepared
        )
        self.spilled_events += staged
        maybe_crash("spill", self.data_commit_groups + 1)
        return len(events)

    def _load_staged(
        self, unit: CompleteUnit, work: dict[str, TableWork], commit_id: int, stats: dict
    ) -> None:
        """Load one unit's staged prefix into the group's shared `work` map."""
        for staged in self.spill.load(commit_id=commit_id, unit_seq=unit.spill_unit_seq):
            item = table_work.work_for(
                work,
                staged.target,
                staged.event,
                staged.target.endswith(naming.SHADOW_SUFFIX),
            )
            self._collect_prepared(item, staged.event, commit_id, staged.event_id, stats)

    def _collect_prepared(
        self, item: TableWork, event: PendingRecord, commit_id: int, event_id: str, stats: dict
    ) -> None:
        """Fold one event into the plan, in either storage mode.

        In-memory collection and staged-row projection used to be two functions that
        drifted: the drain updated neither `table_counts` nor `stats["max_source_ts"]`,
        so a spilled group under-reported in `last_run.json` and in
        `commit_log.max_source_ts` (Opus MINOR-1). There is one path now.
        """
        row = table_work.row_for(event, commit_id, event_id, snapshot=item.snapshot)
        table_work.collect(item, event, row, event_id)
        stats["events"] += 1
        if event.lsn:
            stats["first_lsn"] = stats["first_lsn"] or event.lsn
            stats["last_lsn"] = event.lsn
        if event.source_ts_ms:
            stats["max_source_ts"] = max(stats["max_source_ts"] or 0, event.source_ts_ms)

    # ------------------------------------------------------------------ #
    # shutdown
    # ------------------------------------------------------------------ #
    def drain_on_shutdown(self) -> int:
        """Discard the un-`END`ed tail (ADR §3.2). Returns discarded event count.

        Deliberately does NOT try to commit: the tail cannot be proven whole, and
        Invariant O guarantees nothing about it was acknowledged, so replaying it
        is free. Whole units still buffered in the group are equally safe to drop,
        but they used to vanish without being counted anywhere, so a run could
        report `ok: true` having silently deferred entire transactions
        (Opus MINOR-9). They are counted into the summary now.
        """
        self.shutdown()
        if self._group:
            self.deferred_units += len(self._group)
            self.deferred_events += sum(u.event_count for u in self._group)
            log.info(
                "deferring %s whole unit(s) / %s events buffered at shutdown; they "
                "replay on the next run (Invariant O)",
                len(self._group), self.deferred_events,
            )
            self._group = []
        # A staging transaction may still be open (a large unit was spilling when the
        # engine stopped). Roll it back explicitly, or the lease DELETE that follows
        # in `pipeline.run`'s `finally` joins that transaction and is discarded by
        # `con.close()`, leaving the lease alive until its TTL (Opus MINOR-8).
        self._rollback_quietly()
        try:
            self._run_pending_verification()
        except BaseException as exc:
            # Recorded rather than raised: this runs in a `finally`, and raising
            # here would replace whatever exception is already in flight. The
            # supervisor checks `handler.error` after that block, so it still fails
            # the run.
            log.error("the last commit group's offset flush could not be verified: %s", exc)
            if self.error is None:
                self.error = exc
        return self.assembler.discard_open_unit()


def _stream_event_id(event: PendingRecord) -> str:
    """`"<event lsn>:<source.txId>:<transaction.total_order>"` (ADR §6, §15/A3).

    The **event's own** LSN, not the transaction's commit LSN (ADR §15/A3 records
    the change; this docstring and `apply_sql`'s used to say "commit lsn" —
    Opus MINOR-14).

    `total_order` is the connector's own 1-based ordinal within the transaction, so
    it is stable across a replay of the same WAL: a resume point can only ever sit
    on a transaction boundary, so a replayed transaction renumbers from 1 and
    recomputes identical identities. `source.sequence` is NOT an ordinal (it is
    `[lastCommitLsn, currentLsn]`, `SourceInfo.java:180-196`) and several events can
    share one LSN, which is why the LSN alone cannot be the identity (Codex 3).

    Uniqueness is **structural, not conventional**, and only because
    `TransactionAssembler` refuses a unit whose ordinals are absent, non-positive,
    repeated, or not exactly `1..event_count` (Codex 4; ADR §15/A18). Without that
    validation two accepted events could reach this function with the same triple
    and the keyless collection would silently keep one of them.
    """
    return f"{event.lsn}:{event.txn_id}:{event.total_order}"


def _epoch_ms(value) -> Any:
    """Debezium's `source.ts_ms` as a timestamp, so end-to-end lag is a SQL
    subtraction rather than an arithmetic puzzle for whoever writes rubric 6.1."""
    if value is None:
        return None
    from datetime import UTC, datetime

    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


def _serialise_entries(entries: dict[bytes, bytes]) -> bytes:
    return json.dumps(
        {k.decode("utf-8", "replace"): v.decode("utf-8", "replace") for k, v in entries.items()},
        separators=(",", ":"),
    ).encode("utf-8")
