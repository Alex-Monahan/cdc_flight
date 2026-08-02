"""The transactional applier (ADR 0001 D1/D2/D3) — rubric 1.1, 1.2, 1.3.

One MotherDuck/DuckDB transaction per **commit group**; a commit group holds an
integral number of *whole* Postgres transactions (or whole snapshot chunks);
the resume point is written **inside** that transaction; Debezium is
acknowledged **after** it commits.

```
BEGIN TRANSACTION
    renew lease                       # 4.2 - the loser fails before it writes
    apply whole units, all tables     # 1.3 - multi-table atomicity
    apply due catalog DDL             # 1.5 - fenced on this group's resume point
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

Invariant O bounds *ordering*, and that is not the whole of exactly-once: it also
has to be true that what the group commits is the semantically right answer. A
durably committed **wrong fold** advances the slot just as happily as a right one.
That is why this file owns the commit protocol *and only that* (ADR §15/A29,
§18/A37): the fold lives in `planner.py` + `table_work.py`, the destructive-DDL
policy in `catalog_apply.py`, and the two never share a dispatcher with anything
else — the last two review rounds both found defects that existed only because a
second path had grown alongside the first.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from . import apply_sql, destination, resume, self_heal, table_work
from .applier_config import ApplierConfig
from .assembler import (
    UNIT_SNAPSHOT_CHUNK,
    UNIT_TXN,
    CompleteUnit,
    TransactionAssembler,
)
from .catalog_apply import CatalogCoordinator
from .commit_group import CommitResult, OpenGroup
from .destination import AlertSink, Lease, ResumePoint
from .envelope import KIND_SNAPSHOT_BOUNDARY, PendingRecord, decode
from .errors import AmbiguousDelete, DestinationIdentityCollision
from .faults import arm_group, maybe_crash, wrap_destination
from .planner import GroupPlan, stream_event_id
from .run_state import COMMIT_ACK
from .snapshot import SnapshotCoordinator
from .snapshot_completion import SnapshotCompletion, SnapshotObservationError
from .snapshot_notifications import decode_notification
from .spill import SpillBuffer, StagedEvent

log = logging.getLogger("cdc_flight.applier")


@dataclass
class _FencedCommitContext:
    """The real owner of a fenced overlap's staging transaction.

    A spill starts before the assembler has an ``END`` marker and therefore before
    ``_add_unit`` can decide how the completed unit will be grouped.  Re-snapshot
    streaming is known to be fenced at that earlier point, so its staged rows get a
    dedicated transaction connection and group from the beginning.  The main
    ``OpenGroup`` can consequently remain blocked on a snapshot boundary without
    lending its transaction to the throwaway unit.
    """

    group: OpenGroup
    con: Any
    spill: SpillBuffer
    registry: Any
    close_con: Any | None = None


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
        catalog=None,
        watermarks: dict[str, int] | None = None,
        completion: SnapshotCompletion | None = None,
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
        #: `catalog.CatalogWatcher` or None. The only source of DROP TABLE knowledge
        #: (rubric 1.5): logical decoding does not carry DDL at all.
        self.catalog = catalog
        #: rubric 1.6: `"<schema>.<table>" -> snapshot_lsn`. A source transaction whose
        #: **commit** LSN is below a table's watermark is already inside that table's
        #: snapshot image, so its events for that table are dropped. Per table, because
        #: only the re-snapshotted tables have a new image; per *commit* LSN, because a
        #: transaction that straddles the consistent point is in no image at all and
        #: must be applied in full (`cdc_flight.resnapshot`).
        self.watermarks: dict[str, int] = dict(watermarks or {})
        #: The one owner of this invocation's snapshot completion state. The default
        #: keeps the in-process laboratory's full-snapshot behaviour; production callers
        #: pass the policy selected during acquisition.
        self.snapshot_completion = completion or SnapshotCompletion.full_snapshot()
        #: the consistent point of the snapshot this run applied, if any
        self.last_snapshot_lsn: int | None = None

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

        #: The open commit group, as ONE object. Replaced wholesale at COMMIT and at
        #: ROLLBACK; there is deliberately no way to reset part of it.
        self.group = OpenGroup()
        #: NOT part of the group: the offset-flush check is deliberately deferred to the
        #: NEXT batch, so it outlives the group it belongs to (Codex 7).
        self._pending_verification: tuple | None = None

        # ADR §3.5 / D7, §3.4, the fold and the catalog policy all live in their own
        # modules (ADR §15/A29, §18/A37): every blocker of the last two review rounds
        # was a consequence of two paths doing one job inside one file.
        self.snapshots = SnapshotCoordinator(
            con,
            dataset=dataset,
            pipeline=pipeline,
            topic_prefix=topic_prefix,
            # a CALLABLE: the group object is replaced at every COMMIT/ROLLBACK
            created_in_txn=lambda: self.group.created_in_txn,
            get_registry=lambda: self.registry,
            epoch=resume_point.snapshot_epoch,
            transactional_ddl=transactional_ddl,
        )
        self.spill = SpillBuffer(con)
        #: Streaming units on a re-snapshot are fenced before their END arrives. If
        #: such a unit spills, this map owns the actual destination transaction until
        #: the assembler emits the complete unit. It is separate from ``self.group``
        #: so a blocked snapshot group never accidentally owns the overlap's rows.
        self._fenced_commits: dict[int, _FencedCommitContext] = {}
        self.alerts = AlertSink(con, pipeline=pipeline)
        # rubric 1.9: an illegal table-lifecycle transition must reach an operator, and
        # the only connection that survives this group's rollback is the sink's.
        self.snapshots.alerts = self.alerts
        self.catalog_coordinator = CatalogCoordinator(
            catalog=catalog,
            pipeline=pipeline,
            topic_prefix=topic_prefix,
            drop_mode=config.drop_mode,
            registry_of=lambda: self.registry,
            max_destructive_per_group=config.drop_max_per_group,
            allow_mass_drop=config.drop_allow_mass,
            revalidate=config.drop_revalidate,
        )

        self._committer = None
        self._lock = threading.Lock()
        self._quiescence = threading.Condition(self._lock)
        self._in_flight = 0
        self._callback_sealed = False
        self._callback_seal_reason: str | None = None
        self._callback_batches_rejected = 0
        self._callback_records_rejected = 0
        self.last_batch_at = time.monotonic()

        # -- counters surfaced in the run summary (rubric 6.1) --------------- #
        self.record_count = 0
        self.batch_count = 0
        self.data_batch_count = 0
        self.skipped_count = 0
        self.snapshot_notification_count = 0
        self._pending_snapshot_notifications: list[Any] = []
        self.commit_groups = 0
        self.data_commit_groups = 0
        self.applied_events = 0
        self.fenced_units = 0
        self.fenced_events = 0
        self.spilled_events = 0
        self.fenced_spilled_events = 0
        self.deferred_units = 0
        self.deferred_events = 0
        self.truncates_applied = 0
        self.truncates_logged = 0
        self.resnapshot_discarded_events = 0
        #: rubric 4.7: undecidable folds turned into automatic table rebuilds
        self.ambiguous_resnapshots_queued = 0
        #: events dropped because their transaction is already inside a table's image
        self.watermark_fenced_events = 0
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
    def callback_quiesced(self) -> bool:
        """True only when admission is sealed and every admitted callback has left."""
        with self._lock:
            return self._callback_sealed and self._in_flight == 0

    @property
    def seconds_since_last_batch(self) -> float:
        with self._lock:
            return time.monotonic() - self.last_batch_at

    def snapshot_counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self.table_counts)

    @property
    def snapshot_completion_required(self) -> bool:
        return self.snapshot_completion.required

    @property
    def snapshot_completed(self) -> bool:
        return self.snapshot_completion.completed

    @property
    def snapshot_final_seen(self) -> bool:
        return self.snapshot_completion.marker_seen

    @property
    def snapshot_tables_seen(self) -> set[str]:
        return self.snapshot_completion.tables_seen

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
            "alerts_out_of_transaction": self.alerts.independent,
            # rubric 1.5
            "truncates_applied": self.truncates_applied,
            "truncates_logged": self.truncates_logged,
            # rubric 1.6: events that belonged to a transaction already inside a
            # table's snapshot image, and (for a re-snapshot applier) streaming events
            # that belong to the real slot rather than to the throwaway one.
            "watermark_fenced_events": self.watermark_fenced_events,
            "resnapshot_discarded_events": self.resnapshot_discarded_events,
            "ambiguous_resnapshots_queued": self.ambiguous_resnapshots_queued,
            "snapshot_consistent_lsn": self.last_snapshot_lsn,
            "snapshot_notifications": self.snapshot_notification_count,
            "snapshot_notifications_pending": len(
                self._pending_snapshot_notifications
            ),
            **self.snapshot_completion.as_dict(),
            # Round 8 MAJOR-1: this is the callback/connection ownership proof. A late
            # callback after the seal is a recorded no-op and can never decode, write,
            # mutate replay state, or acknowledge Debezium.
            "callback_boundary": "sealed" if self._callback_sealed else "open",
            "callback_seal_reason": self._callback_seal_reason,
            "callback_batches_rejected": self._callback_batches_rejected,
            "callback_records_rejected": self._callback_records_rejected,
            "callback_quiesced": self.callback_quiesced,
            **self.catalog_coordinator.summary(),
            **(self.catalog.summary() if self.catalog is not None else {}),
        }

    @property
    def tables_dropped(self) -> int:
        return self.catalog_coordinator.tables_dropped

    @property
    def catalog_changes_applied(self) -> int:
        return self.catalog_coordinator.changes_applied

    def _age_timer(self) -> None:
        """Ask for a group close on age. It can only ever *request*: the commit
        itself must happen on the poll thread, because `RecordCommitter` is
        explicitly not thread safe (`AsyncEmbeddedEngine.java:1341`)."""
        while not self._timer_stop.wait(0.5):
            if self.group.units and (
                time.monotonic() - self.group.opened_at >= self.cfg.commit_max_age
            ):
                self.group.close_requested = True

    def shutdown(self, *, reason: str = "supervisor_shutdown") -> None:
        """Seal callback admission and stop the age timer.

        This is a lifecycle boundary, not merely timer cleanup. Once it returns, a new
        Debezium callback is a recorded no-op. An already-admitted callback may still be
        using ``con``; callers must prove :attr:`callback_quiesced` (or call
        :meth:`wait_for_quiescence`) before drain, lease release, or handle retirement.
        """
        with self._quiescence:
            if not self._callback_sealed:
                self._callback_sealed = True
                self._callback_seal_reason = reason
            self._quiescence.notify_all()
        self._timer_stop.set()

    def wait_for_quiescence(self, timeout: float) -> bool:
        """Wait at most ``timeout`` for every callback admitted before the seal."""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._quiescence:
            while not (self._callback_sealed and self._in_flight == 0):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._quiescence.wait(remaining)
            return True

    # ------------------------------------------------------------------ #
    # the Debezium callback
    # ------------------------------------------------------------------ #
    def handle_batch(self, records, committer) -> None:
        with self._quiescence:
            if self._callback_sealed:
                self._callback_batches_rejected += 1
                self._callback_records_rejected += len(records)
                log.error(
                    "rejected a Debezium callback containing %s record(s): the callback "
                    "boundary is sealed (%s)",
                    len(records), self._callback_seal_reason,
                )
                return
            self._in_flight += 1
        try:
            self._handle(records, committer)
        except BaseException as exc:
            self.error = exc
            raise
        finally:
            with self._quiescence:
                self._in_flight -= 1
                self.last_batch_at = time.monotonic()
                if self._in_flight == 0:
                    self._quiescence.notify_all()

    # pydbzengine compatibility, used only if something calls the old shape.
    def handleJsonBatch(self, records):  # pragma: no cover - not the live path
        raise RuntimeError("the applier needs the RecordCommitter; use handle_batch()")

    def _handle(self, records, committer) -> None:
        self._committer = committer
        # The previous group's offset flush is verified here, outside the
        # commit->ack window, now that Debezium has polled at least once since it
        # (Codex 7).
        self._run_pending_verification()
        source_records = 0
        data_in_batch = 0
        for raw in records:
            notification = decode_notification(raw, topic_prefix=self.topic_prefix)
            if notification is not None and self.assembler.open_transaction_id is not None:
                raise SnapshotObservationError(
                    "snapshot notification arrived inside an open streaming transaction; "
                    "its callback order cannot be acknowledged safely"
                )
            if notification is not None:
                self.snapshot_notification_count += 1
                boundary = None
                if notification.observation == "COMPLETED":
                    # Validate the terminal observation BEFORE any destination
                    # transaction can write its source offset. The raw notification
                    # remains pending; this decoded boundary is deliberately not
                    # acknowledgeable and exists only to carry its Connect offset into
                    # the final destination transaction.
                    boundary = decode(
                        raw, topic_prefix=self.topic_prefix, want_offsets=True
                    )
                    if not boundary.source_partition or not boundary.source_offset:
                        raise SnapshotObservationError(
                            "COMPLETED snapshot notification has no Connect offset; "
                            "refusing to advance the destination resume point"
                        )
                    boundary.kind = KIND_SNAPSHOT_BOUNDARY
                    boundary.raw = None
                self.snapshot_completion.observe_notification(
                    notification.observation, notification.data
                )
                self._pending_snapshot_notifications.append(raw)
                if boundary is not None:
                    # The ordered terminal callback is the boundary after the last row
                    # callback. Feed only this explicit boundary through the assembler:
                    # progress callbacks must never fragment snapshot chunks or swap
                    # shadows, and the pending raw notification must not be acked as a
                    # normal commit-group record.
                    for unit in self.assembler.feed_snapshot_boundary(boundary):
                        self._add_unit(unit)
                continue

            source_records += 1
            rec = decode(raw, topic_prefix=self.topic_prefix)
            if rec.is_data:
                data_in_batch += 1
            else:
                self.skipped_count += 1
            for unit in self.assembler.feed(rec):
                self._add_unit(unit)

        with self._lock:
            self.batch_count += 1
            self.record_count += source_records
            if data_in_batch:
                self.data_batch_count += 1

        if data_in_batch:
            maybe_crash("decode", self.data_batch_count)

        if not self.group.units:
            return
        # ADR §3.3 soft triggers, plus one pragmatic rule the ADR's pseudocode
        # needs and does not state: Debezium calls `markBatchFinished()` itself on
        # an *empty* poll and never calls us, so a group left buffered when the
        # stream goes quiet would never commit. A batch smaller than
        # `max.batch.size` means the queue drained, so commit now; a full batch
        # means more is already queued, so keep accumulating up to the triggers.
        drained = source_records < self.cfg.max_batch_size
        if self.assembler.open_unit_has_spilled:
            # Invariant B: the rows staged for the still-open unit live in this
            # group's transaction, so committing now would drain a PARTIAL Postgres
            # transaction into the destination. Wait for its END.
            return
        if self.group.close_requested or drained or self._soft_trigger_hit():
            self.commit_group(self._trigger_name(drained))

    def _soft_trigger_hit(self) -> bool:
        return (
            self.group.events >= self.cfg.commit_max_events
            or self.group.nbytes >= self.cfg.commit_max_bytes
            or time.monotonic() - self.group.opened_at >= self.cfg.commit_max_age
        )

    def _trigger_name(self, drained: bool) -> str:
        if self.group.is_snapshot:
            return "snapshot_chunk"
        if self.group.events >= self.cfg.commit_max_events:
            return "events"
        if self.group.nbytes >= self.cfg.commit_max_bytes:
            return "bytes"
        if self.group.close_requested:
            return "time"
        return "drained" if drained else "time"

    # ------------------------------------------------------------------ #
    # group assembly
    # ------------------------------------------------------------------ #
    def _add_unit(self, unit: CompleteUnit) -> None:
        resnapshot_fenced = self._fence_resnapshot_unit(unit)
        is_snapshot = self._is_snapshot_unit(unit)
        was_snapshot = self.group.is_snapshot
        if (
            resnapshot_fenced
            and self.group.units
            and self.group.is_snapshot
            and self._has_snapshot_boundary(self.group.units)
        ):
            # A fenced overlap is not a live-stream admission. If the terminal
            # snapshot group is still under-counted, keep that group intact and
            # discard the overlap in its own group instead.
            result = self.commit_group("snapshot_chunk")
            if result is CommitResult.BLOCKED:
                self._commit_fenced_resnapshot_unit(unit)
                return
        if not is_snapshot and not resnapshot_fenced:
            # This must happen before an open snapshot group is committed or the
            # incoming streaming unit is appended. The completion machine, not the
            # current group's row shape, owns the phase barrier.
            if (
                self.group.units
                and self.group.is_snapshot
                and self._has_snapshot_boundary(self.group.units)
            ):
                # A terminal boundary is itself the proof-bearing phase barrier. If
                # its projected rows are ready, commit that snapshot group first;
                # `observe_committed_group()` then takes completion_notified ->
                # callbacks_complete, after which the stream edge can be checked.
                result = self.commit_group("snapshot_chunk")
                if result is not CommitResult.COMMITTED:
                    self.snapshot_completion.check_streaming_admission()
                    raise SnapshotObservationError(
                        "cannot cross the snapshot phase boundary with commit result "
                        f"{result.value}"
                    )
            else:
                # An open snapshot group without its terminal boundary must never be
                # committed merely because a stream unit arrived.
                self.snapshot_completion.check_streaming_admission()
        # ADR §3.5: snapshot units are never mixed with streaming units, so a
        # commit_log row unambiguously says which phase it belongs to. The explicit
        # terminal boundary is control-shaped but belongs to the snapshot group so its
        # offset commits atomically with the final snapshot rows.
        if (
            self.group.units
            and is_snapshot != self.group.is_snapshot
        ):
            result = self.commit_group(
                "snapshot_chunk" if was_snapshot else "phase"
            )
            if result is not CommitResult.COMMITTED:
                raise SnapshotObservationError(
                    f"cannot cross the snapshot phase boundary with commit result "
                    f"{result.value}"
                )
        # A spilled fenced unit was staged by its own transaction before the END
        # marker arrived. It must never be appended to the main group after a phase
        # commit, because that would leave its staged rows owned by a different group.
        if resnapshot_fenced and unit.spill_unit_seq is not None:
            self._commit_fenced_resnapshot_unit(unit)
            return
        if not is_snapshot and not resnapshot_fenced:
            # For a phase mismatch this runs only after the prior snapshot group has
            # committed. For an empty group it is the admission edge that used to be
            # skipped entirely.
            self.snapshot_completion.enter_streaming()
        self._append_unit(unit, is_snapshot=is_snapshot)

    def _commit_fenced_resnapshot_unit(self, unit: CompleteUnit) -> None:
        """Commit a fenced overlap without taking ownership of snapshot completion.

        The assembler may have staged the unit before it emitted ``CompleteUnit``.
        In that case the context already owns a live DuckDB transaction. A unit with
        no staged prefix still gets the same explicit context, which keeps this path
        honest and avoids rebinding ``self.group`` while another transaction is open.
        """
        context = None
        if unit.spill_unit_seq is not None:
            context = self._fenced_commits.pop(unit.spill_unit_seq, None)
        if context is None:
            context = self._new_fenced_commit_context()
        try:
            self._append_unit(unit, is_snapshot=False, group=context.group)
            result = self.commit_group(
                "resnapshot_overlap",
                group_obj=context.group,
                con=context.con,
                spill=context.spill,
                registry=context.registry,
            )
            if result is not CommitResult.COMMITTED:
                raise SnapshotObservationError(
                    "cannot discard fenced re-snapshot overlap with commit result "
                    f"{result.value}"
                )
        finally:
            if context.close_con is not None:
                context.close_con.close()

    def _append_unit(
        self, unit: CompleteUnit, *, is_snapshot: bool, group: OpenGroup | None = None
    ) -> None:
        target_group = self.group if group is None else group
        if not target_group.units:
            target_group.is_snapshot = is_snapshot
            target_group.opened_at = time.monotonic()

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

        target_group.units.append(unit)
        target_group.events += unit.event_count
        target_group.nbytes += unit.nbytes

    def _new_fenced_commit_context(self) -> _FencedCommitContext:
        """Open the isolated owner used by a fenced streaming unit.

        ``DuckDBPyConnection.cursor()`` is a separate transaction context. The
        parent connection may still hold a snapshot-group staging transaction, so
        using this owner is what makes the two commit decisions independent rather
        than a Python-level swap of ``OpenGroup`` metadata.
        """
        if self.group.txn_open:
            raise SnapshotObservationError(
                "cannot isolate a fenced overlap while the main OpenGroup owns an "
                "open destination transaction: both groups publish one shared resume "
                "point, and DuckDB cannot safely commit the overlap beside that owner; "
                "refusing before staging any overlap rows"
            )
        close_con = self.con.cursor()
        try:
            con = wrap_destination(close_con)
            context = _FencedCommitContext(
                group=OpenGroup(),
                con=con,
                spill=SpillBuffer(con),
                registry=apply_sql.SchemaRegistry(
                    con, self.dataset, constraints=self.cfg.destination_constraints
                ),
                close_con=close_con,
            )
            con.execute("BEGIN TRANSACTION")
            context.group.txn_open = True
            context.group.spill_commit_id = self._reserve_commit_id()
        except BaseException:
            close_con.close()
            raise
        return context

    def _fence_resnapshot_unit(self, unit: CompleteUnit) -> bool:
        """Discard throwaway-slot streaming before the live phase barrier.

        A re-snapshot engine can deliver a transaction while its callbacks are still
        in flight. That transaction belongs to the real slot, so it must be fenced,
        acknowledged, and kept in its own commit group without advancing the shared
        snapshot-completion machine into live streaming.
        """
        if not self.cfg.resnapshot or unit.kind != UNIT_TXN:
            return False
        unit.fenced = True
        self.fenced_units += 1
        self.fenced_events += unit.event_count
        self.resnapshot_discarded_events += unit.event_count
        return True

    @staticmethod
    def _is_snapshot_unit(unit: CompleteUnit) -> bool:
        """Classify row and synthetic control units by their source phase."""
        return unit.kind == UNIT_SNAPSHOT_CHUNK or any(
            record.kind == KIND_SNAPSHOT_BOUNDARY for record in unit.records
        )

    @staticmethod
    def _has_snapshot_boundary(units: list[CompleteUnit]) -> bool:
        return any(
            record.kind == KIND_SNAPSHOT_BOUNDARY
            for unit in units
            for record in unit.records
        )

    def _reset_group(self) -> None:
        """One assignment, and that is the whole point (rubric 1.9).

        This used to be fourteen assignments by name, with a SECOND copy of the same
        list in `_reset_after_rollback()` that had to stay in sync with it. Opus MAJOR-1
        is what the divergence cost: the success path reset the group and the failure
        path did not, so a rolled-back group was folded twice and a key-reuse shape lost
        a row.

        The precise claim, and it is narrower than the one this docstring used to make
        (Codex r5 MINOR-2 found the overclaim still here after the ADR and RUBRIC_STATUS
        were corrected): BOTH reset paths are this one assignment, so neither can forget
        a field - which is the defect that was measured. `OpenGroup` is a mutable
        dataclass with public collections, so a partial *mutation* is representable; the
        one deliberate one is the named `discard_units()`.
        """
        self.group = OpenGroup()

    def _reserve_commit_id(self) -> int:
        """Reserve a per-pipeline commit id for an owner opened before COMMIT."""
        commit_id = self._next_commit_id
        self._next_commit_id += 1
        return commit_id

    # ------------------------------------------------------------------ #
    # the transaction
    # ------------------------------------------------------------------ #
    def commit_group(
        self,
        trigger: str,
        *,
        group_obj: OpenGroup | None = None,
        con=None,
        spill: SpillBuffer | None = None,
        registry=None,
    ) -> CommitResult:
        """Commit one explicitly owned group/transaction context.

        The normal path uses ``self.group`` and the applier connection. A fenced
        re-snapshot overlap can instead pass an isolated ``OpenGroup`` plus its
        connection and spill buffer. Keeping ownership as arguments is important:
        rebinding ``self.group`` cannot transfer a DuckDB transaction that is already
        open on the original connection.
        """
        target_group = self.group if group_obj is None else group_obj
        target_con = self.con if con is None else con
        target_spill = self.spill if spill is None else spill
        target_registry = self.registry if registry is None else registry
        group = target_group.units
        if not group:
            return CommitResult.EMPTY
        # Snapshot rows are observations of the same closed protocol as the direct
        # notifications. Validate their state and declared counts before BEGIN/COMMIT;
        # a terminal boundary also waits here until its final buffered rows make the
        # completion proof terminal. The post-commit observer only records evidence
        # that has now become durable.
        if not self.snapshot_completion.commit_ready(group):
            return CommitResult.BLOCKED
        acknowledge_snapshot_notifications = (
            self.snapshot_completion.will_complete_after_commit(group)
        )
        commit_id = target_group.spill_commit_id or self._next_commit_id
        opened_at = destination.now()
        # Tell the destination-fault wrapper which data group this is, so a
        # `destination_*` fault fires at the group the spec names rather than at one
        # the wrapper inferred from the SQL it happened to see (rubric 1.7).
        fault_group = self.data_commit_groups + 1
        arm_group(fault_group)
        # NOT `or spill.rows > 0`: staged rows belonging only to *fenced*
        # units are about to be discarded, and counting them made a group with no
        # applicable content a "data group", which shifts every `<nth>`-indexed
        # fault anchor by one (Codex 5).
        has_data = any(
            not u.fenced and (u.events or u.spilled_events) for u in group
        )
        # A fenced overlap still has a real commit boundary even though it applies no
        # user rows. Exercise the protocol anchors there without counting it as a data
        # group; otherwise the transaction-owner path would be invisible to the fault
        # matrix.
        fault_enabled = has_data or trigger == "resnapshot_overlap"

        if not target_group.txn_open:
            target_con.execute("BEGIN TRANSACTION")
            target_group.txn_open = True
        try:
            if has_data:
                maybe_crash("begin", fault_group)
            if target_group is self.group:
                self.lease.renew(target_con)
            else:
                # A fenced discard group writes no user rows and may run beside the
                # main snapshot's open DuckDB transaction. Renewing the shared lease
                # row from that isolated writer would create a write conflict with the
                # snapshot owner; the applier already holds the lease, and this
                # bounded discard does not need a second lease mutation.
                log.debug("fenced overlap reuses the applier lease without renewing it")
            stats = self._apply_units(
                group,
                commit_id,
                has_data=has_data,
                group_obj=target_group,
                con=target_con,
                spill=target_spill,
                registry=target_registry,
            )
            new_point = resume.point_for(
                group,
                previous=self.resume_point,
                commit_id=commit_id,
                snapshot_epoch=self.snapshots.epoch,
            )
            # rubric 1.5: DDL the stream cannot carry, fenced on the resume point this
            # group is about to make durable.
            self._apply_catalog_changes(
                commit_id,
                new_point.last_lsn,
                stats,
                group_obj=target_group,
                con=target_con,
            )
            destination.write_commit_log(
                target_con,
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
                target_con,
                pipeline=self.pipeline,
                namespace=self.namespace,
                point=new_point,
                commit_id=commit_id,
                offset_blob=self._pending_offset_blob,
                offset_key_blob=self._pending_offset_key_blob,
            )
            if fault_enabled:
                maybe_crash("pre_commit", fault_group)
            # Principle (3): the pre-flush fingerprint of `offsets.dat` is taken
            # HERE, before the commit, because it is only a *forensic* baseline -
            # it does not need to lengthen the commit->ack path (Codex 7).
            offset_fingerprint = self.verifier.before() if self.verifier else None
            # rubric 1.9 / ADR §20: the commit->ack exclusion, as a flag other threads
            # can read. Entered BEFORE the COMMIT (so no observability write can be
            # mid-statement when the window opens) and left after the acknowledgement.
            # One attribute assignment, no lock, no allocation - see
            # `run_state._CommitAckWindow` for why that is the only acceptable cost here.
            stage = ["observability_gate"]
            marked = 0
            with self_heal.commit_watchdog(
                self.cfg.commit_timeout, commit_id, stage=lambda: stage[0]
            ):
                # INSIDE the watchdog (Codex r3 MAJOR-2). `enter()` waits, without a
                # bound of its own, until no independent write is in flight — that is
                # what makes the exclusion absolute rather than instrumented — and the
                # watchdog bounds both COMMIT and every acknowledgement below.
                COMMIT_ACK.enter()
                try:
                    stage[0] = "commit"
                    if target_group is self.group:
                        self.con.execute("COMMIT")
                    else:
                        target_con.execute("COMMIT")
                    target_group.txn_open = False
                    if fault_enabled:
                        maybe_crash("post_commit_pre_ack", fault_group)

                    # The only operations in the guarded post-COMMIT path are the
                    # acknowledgement calls. Pending snapshot notifications join the
                    # same plan only once the pure pre-commit completion check says this
                    # group will make the callback proof terminal.
                    stage[0] = "ack"
                    pending = (
                        list(self._pending_snapshot_notifications)
                        if acknowledge_snapshot_notifications
                        else []
                    )
                    for unit in group:
                        for rec in unit.records:
                            if rec.raw is None:  # released by `_add_unit`
                                continue
                            self._committer.markProcessed(rec.raw)
                            marked += 1
                    for raw in pending:
                        self._committer.markProcessed(raw)
                        marked += 1
                    self._committer.markBatchFinished()
                    if pending:
                        # Do not discard the handles until markBatchFinished succeeds.
                        del self._pending_snapshot_notifications[: len(pending)]
                finally:
                    # A mark call can raise; a stuck window would silently drop every
                    # later phase write, so the gate is closed in all cases.
                    COMMIT_ACK.leave()
        except (AmbiguousDelete, DestinationIdentityCollision) as ambiguous:
            # Rubric 4.7. The group still rolls back - a fold that cannot be decided is
            # never committed - but a bare rollback here is a *permanent* failure: the
            # transaction replays on the next run and hits the same ambiguity, for ever,
            # which is a manual-intervention case. So the table is marked for a
            # re-snapshot on the independent connection, where the request survives this
            # rollback, and the next run rebuilds it. The re-snapshot's consistent point
            # is necessarily after this transaction (we already received it, so it is
            # already in WAL), so the per-table watermark fences the transaction that
            # cannot be folded and the loop terminates after exactly one re-snapshot
            # (ADR 0001 §19/A47).
            COMMIT_ACK.leave()
            self._request_resnapshot_for(ambiguous)
            if target_group is self.group:
                self._rollback_quietly()
            else:
                self._rollback_quietly(group_obj=target_group, con=target_con)
            raise
        except BaseException:
            if target_group is self.group:
                self._rollback_quietly()
            else:
                self._rollback_quietly(group_obj=target_group, con=target_con)
            raise

        if fault_enabled:
            maybe_crash("post_ack", fault_group)
        # next poll() -> performCommit() -> flushLsn(new)  ── nothing between ──
        # No filesystem work, no hashing: the "did the flush happen" check is a
        # liveness canary, not a prerequisite under Invariant O, so it runs on the
        # next batch (or at shutdown) once the connector has had its poll/commit
        # opportunity (Codex 7).
        if self.verifier is not None and marked:
            self._pending_verification = (offset_fingerprint, marked)

        self._settle_catalog(target_group)
        self._flush_alerts(target_group)
        # Snapshot completion is one policy shared by the main and re-snapshot engines.
        # Row evidence is recorded only after COMMIT; direct per-table/global callbacks
        # and declared/committed counts take the terminal edge. Row markers are
        # diagnostic only and never prove completion.
        self.snapshot_completion.observe_committed_group(
            group, snapshot_active=self.snapshots.active
        )
        self.commit_groups += 1
        if has_data:
            self.data_commit_groups += 1
        self.applied_events += stats["events"]
        self.last_commit_id = commit_id
        self.resume_point = new_point
        self._next_commit_id = max(self._next_commit_id, commit_id + 1)
        if self.cfg.verify_offset_file:
            self._pending_offset_key_blob, self._pending_offset_blob = (
                resume.capture_offset_file(self.offset_path, new_point)
            )
        if target_group is self.group:
            self._reset_group()
        return CommitResult.COMMITTED

    def _request_resnapshot_for(
        self, ambiguous: AmbiguousDelete | DestinationIdentityCollision
    ) -> None:
        """Rubric 4.7's automatic rebuild request. The policy is `cdc_flight.self_heal`."""
        recorded, alert = self_heal.request_resnapshot_for(
            ambiguous,
            alerts=self.alerts,
            pipeline=self.pipeline,
            topic_prefix=self.topic_prefix,
            enabled=self.cfg.resnapshot_on_ambiguity,
        )
        if alert is not None:
            self.group.pending_alerts.append(alert)
        self.ambiguous_resnapshots_queued += int(recorded)

    def _rollback_quietly(
        self, *, group_obj: OpenGroup | None = None, con=None
    ) -> None:
        target_group = self.group if group_obj is None else group_obj
        target_con = self.con if con is None else con
        if not target_group.txn_open:
            if target_group is self.group:
                self._reset_after_rollback()
            return
        try:
            target_con.execute("ROLLBACK")
        except Exception:  # pragma: no cover - never mask the original error
            log.debug("rollback failed", exc_info=True)
        finally:
            target_group.txn_open = False
            if target_group is self.group:
                self._reset_after_rollback()
            else:
                failed = target_group.discard_units()
                if failed:
                    self.deferred_units += len(failed)
                    self.deferred_events += sum(u.event_count for u in failed)

    def _rollback_fenced_commits(self) -> None:
        """Release incomplete isolated spill owners at shutdown/error teardown."""
        contexts = list(self._fenced_commits.values())
        self._fenced_commits.clear()
        for context in contexts:
            try:
                if context.group.txn_open:
                    context.con.execute("ROLLBACK")
            except Exception:  # pragma: no cover - teardown must not mask the cause
                log.debug("fenced overlap rollback failed", exc_info=True)
            finally:
                context.group.txn_open = False
                if context.close_con is not None:
                    try:
                        context.close_con.close()
                    except Exception:  # pragma: no cover
                        log.debug("fenced overlap connection close failed", exc_info=True)

    def _reset_after_rollback(self) -> None:
        """Everything the discarded transaction touched, in memory as well.

        `_reset_group()` used to be called **only** on the success path, so a group
        whose COMMIT failed stayed buffered and was folded a second time by the next
        `commit_group` — alongside whatever had arrived since. For an idempotent shape
        that is harmless, which is why the fault tests passed; for a key-reuse shape it
        is not, and it was measured to lose a row (Opus MAJOR-1). The ADR's own rule is
        that a rolled-back group replays *from the source*, and this is what makes that
        true of the process as well as of the offset store.
        """
        # Markers describe an apply that did not happen, and the catalog work of a
        # rolled-back group must stay pending so it is applied (or re-detected)
        # rather than silently forgotten.
        alerts = [a for a in self.group.pending_alerts if a.get("on_rollback")]  # Codex 7
        # Every CREATE / ALTER we issued is gone with the transaction, so the
        # cached destination shape is now a lie. Rebuilding it is cheap and
        # not doing it is how a rolled-back run corrupts the next one.
        self.registry = apply_sql.SchemaRegistry(
            self.con, self.dataset, constraints=self.cfg.destination_constraints
        )
        failed = list(self.group.units)
        self._reset_group()
        if failed:
            self.deferred_units += len(failed)
            self.deferred_events += sum(u.event_count for u in failed)
            log.warning(
                "discarding %s buffered unit(s) after a failed commit group; they "
                "replay from the source (Invariant O)", len(failed),
            )
        # Raised AFTER the group state is clean, on the independent connection, so
        # "the destructive change could not be applied" reaches an operator even
        # though everything else about the attempt was rolled back.
        for alert in alerts:
            self._raise_alert(alert)

    # -- resume point (ADR §4.3, `resume.py`) ------------------------------- #
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

    # ------------------------------------------------------------------ #
    # applying units — one ordered pass, delegated to the planner
    # ------------------------------------------------------------------ #
    def _apply_units(
        self,
        group: list[CompleteUnit],
        commit_id: int,
        *,
        has_data: bool,
        group_obj: OpenGroup,
        con,
        spill: SpillBuffer,
        registry,
    ) -> dict:
        plan = GroupPlan(
            con,
            commit_id=commit_id,
            registry_of=lambda: registry,
            snapshots=self.snapshots,
            spill=spill,
            truncate_mode=self.cfg.truncate_mode,
            created_in_txn=group_obj.created_in_txn,
            watermarks=self.watermarks,
        )
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
                    plan.staged_units = True
                continue
            if unit.kind == UNIT_SNAPSHOT_CHUNK:
                group_obj.is_snapshot = True
            plan.add_unit(unit)

        # The `mid_apply` anchor is documented as "some tables written, others not".
        # It has to fire BETWEEN two table writes, or it cannot detect a transaction
        # torn between table A and table B - the one interleaving rubric 1.3 is about
        # (Codex 6) - and it is gated on `has_data` like every other anchor, because
        # `<nth>` counts data-carrying groups (Opus MINOR-2).
        anchor = None
        if has_data:
            def anchor() -> None:
                maybe_crash("mid_apply", self.data_commit_groups + 1)
        stats = plan.write(after_first_table=anchor)
        for target, (schema, table) in plan.created_tables.items():
            destination.register_table(
                con,
                pipeline=self.pipeline,
                source_schema=schema,
                source_table=table,
                target_table=target,
            )
        with self._lock:
            for target, count in plan.table_counts.items():
                self.table_counts[target] = self.table_counts.get(target, 0) + count
        self.truncates_applied += plan.truncates_applied
        self.truncates_logged += plan.truncates_logged
        self.watermark_fenced_events += plan.watermark_fenced_events
        if group_obj.is_snapshot and stats.get("last_lsn"):
            # Every snapshot record of one snapshot carries the exported snapshot's
            # consistent point, so this is `C` (rubric 1.6, `cdc_flight.resnapshot`).
            self.last_snapshot_lsn = stats["last_lsn"]
        group_obj.source_tables |= plan.source_tables
        group_obj.table_events.extend(plan.markers())
        self._flush_table_events(commit_id, group_obj=group_obj, con=con)
        return stats

    # ------------------------------------------------------------------ #
    # table-level events and catalog DDL (rubric 1.5)
    # ------------------------------------------------------------------ #
    def _flush_table_events(
        self, commit_id: int, *, group_obj: OpenGroup, con
    ) -> None:
        """Write this group's `table_events` rows, inside its transaction.

        Deliberately transactional with the data: "the destination table was emptied"
        and "here is the source event that emptied it" must become true together, or
        the audit trail can outlive a rolled-back apply and describe something that
        never happened.
        """
        for marker in group_obj.table_events:
            destination.write_table_event(
                con,
                pipeline=self.pipeline,
                commit_id=commit_id,
                seq=group_obj.next_table_event_seq(),
                **marker,
            )
        group_obj.table_events = []

    def _apply_catalog_changes(
        self,
        commit_id: int,
        durable_lsn: int,
        stats: dict,
        *,
        group_obj: OpenGroup,
        con,
    ) -> None:
        """Apply the source-catalog changes whose fence has opened (rubric 1.5).

        Runs inside the commit group's transaction, *after* the group's events, so a
        `DROP` cannot remove rows that an event of this same group had still to add,
        and a crash between the drop and the resume-point write replays both. The
        policy - supersession, revalidation, the circuit breaker, `awaiting_snapshot` -
        is `catalog_apply.CatalogCoordinator`'s; this is only where it is executed.
        """
        coordinator = self.catalog_coordinator
        if not coordinator.enabled:
            return
        plan = coordinator.plan(durable_lsn)
        if not plan.actions and not plan.relations and not plan.alerts:
            return
        group_obj.catalog_plan = plan
        group_obj.table_events.extend(coordinator.apply(con, plan, stats))
        # A destructive action that could not be applied is exactly the signal an
        # operator must still get when the group rolls back; one that describes an
        # applied action must NOT outlive the rollback that undid it (Codex 7).
        group_obj.pending_alerts.extend(plan.alerts)
        if group_obj.table_events:
            self._flush_table_events(commit_id, group_obj=group_obj, con=con)

    def _settle_catalog(self, group_obj: OpenGroup) -> None:
        """Forget the catalog work this group made durable. Runs after COMMIT."""
        if self.catalog is None:
            return
        plan = group_obj.catalog_plan
        if plan is not None:
            self.catalog_coordinator.settle(plan, group_obj.source_tables)
            group_obj.catalog_plan = None
        elif group_obj.source_tables:
            self.catalog.observe_replicated(group_obj.source_tables)

    def _flush_alerts(self, group_obj: OpenGroup) -> None:
        for alert in group_obj.pending_alerts:
            self._raise_alert(alert)
        group_obj.pending_alerts = []

    def _raise_alert(self, alert: dict) -> None:
        self.alerts.raise_alert(
            severity=alert["severity"],
            code=alert["code"],
            message=alert["message"],
            context=alert.get("context"),
        )

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
        to look the phase up in the applier's snapshot mapping, which the apply pass
        populates only later, so on the first spilled chunk of every snapshot it
        concluded "streaming" and staged the rows into the **live** table with a
        `<lsn>:None:None` identity; a consumer could then see a partial snapshot, and
        the swap dropped those rows (Codex 1). Resolving the shadow *here*, through
        the coordinator, is what makes that impossible; `unit_seq` is what lets the
        drain order and fence per unit (Opus B-1, Codex 5).
        """
        if not events:
            return 0
        # In a re-snapshot applier every streaming unit is fenced. Decide that at
        # spill time, before the assembler has emitted its END, and give the staged
        # rows their own connection/group. The main connection may already own an
        # uncommitted snapshot group, so sharing it here would make later ownership
        # depend on which unit happened to finish first.
        fenced_context = None
        if self.cfg.resnapshot and snapshot is None:
            fenced_context = self._fenced_commits.get(unit_seq)
            if fenced_context is None:
                fenced_context = self._new_fenced_commit_context()
                self._fenced_commits[unit_seq] = fenced_context
            owner_group = fenced_context.group
            owner_con = fenced_context.con
            owner_spill = fenced_context.spill
        else:
            owner_group = self.group
            owner_con = self.con
            owner_spill = self.spill
        if not owner_group.txn_open:
            owner_con.execute("BEGIN TRANSACTION")
            owner_group.txn_open = True
            owner_group.spill_commit_id = self._reserve_commit_id()
        commit_id = owner_group.spill_commit_id or self._next_commit_id
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
                        event_id=stream_event_id(event),
                        target=self.snapshots.target_table(event.schema, event.table),
                        # Mandatory and validated by the assembler, so there is
                        # nothing to substitute a local sequence for: doing that gave
                        # a replay a different identity (Codex 4).
                        seq=event.total_order,
                    )
                )
        staged = owner_spill.stage(
            commit_id=commit_id, unit_seq=unit_seq, prepared=prepared
        )
        self.spilled_events += staged
        maybe_crash("spill", self.data_commit_groups + 1)
        return len(events)

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
        self._rollback_fenced_commits()
        if self.group.units:
            self.deferred_units += len(self.group.units)
            self.deferred_events += sum(u.event_count for u in self.group.units)
            log.info(
                "deferring %s whole unit(s) / %s events buffered at shutdown; they "
                "replay on the next run (Invariant O)",
                len(self.group.units), self.deferred_events,
            )
            self.group.discard_units()
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


def _epoch_ms(value) -> Any:
    """Debezium's `source.ts_ms` as a timestamp, so end-to-end lag is a SQL
    subtraction rather than an arithmetic puzzle for whoever writes rubric 6.1."""
    if value is None:
        return None
    from datetime import UTC, datetime

    return datetime.fromtimestamp(value / 1000.0, tz=UTC)
