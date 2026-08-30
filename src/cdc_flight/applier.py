"""The transactional applier (ADR 0001 D1/D2/D3) — rubric 1.1, 1.2, 1.3.

One MotherDuck/DuckDB transaction per **commit group**; a commit group holds an
integral number of *whole* Postgres transactions (or whole snapshot chunks);
the resume point is written **inside** that transaction; Debezium is
acknowledged **after** it commits.

```
BEGIN TRANSACTION
    renew lease                       # 4.2 - the loser fails before it writes
    apply whole units, all tables     # 1.3 - multi-table atomicity
    apply schema DDL before row DML   # 2.1/2.2 - avoid mixed DDL/DML version checks
    apply due table DDL after rows    # 1.5 - fenced on this group's resume point
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
from typing import Any

from . import (
    apply_sql,
    catalog_commit,
    commit_protocol,
    destination,
    failure_containment,
    schema_epoch,
    self_heal,
    spill_protocol,
    spill_refusal,
    unit_admission,
    unit_apply,
)
from .assembler import UNIT_CONTROL, UNIT_SNAPSHOT_CHUNK, CompleteUnit, TransactionAssembler
from .backfill import (
    BackfillCoordinator,
    decode_incremental_notification,
    decode_incremental_record,
)
from .catalog_apply import CatalogCoordinator, CatalogPlan
from .commit_group import CommitResult, OpenGroup
from .config import ApplierConfig, resolve_control_schema
from .destination import AlertSink, Lease, OccurrenceKey, ResumePoint
from .envelope import (
    KIND_HEARTBEAT,
    KIND_SNAPSHOT_BOUNDARY,
    PendingRecord,
    decode,
    offsets_of,
)
from .errors import (
    AdmissionError,
    AmbiguousDelete,
    DestinationIdentityCollision,
    SchemaEvolutionRefused,
    as_schema_refusal,
)
from .faults import matrix_crash, maybe_crash
from .marker_accounting import SourceMarkerReceiptCounter
from .occurrence import _commit_reservation
from .policy import PolicyGate
from .delete_modes import DeleteModeResolver
from .snapshot import SnapshotCoordinator
from .snapshot_completion import (
    SnapshotCompletion,
    SnapshotObservationError,
    decode_notification,
)
from .spill import SpillBuffer

log = logging.getLogger("cdc_flight.applier")
OWNER = "applier-lifecycle"


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
        marker_prefixes: tuple[str, ...] | None = None,
        offset_path,
        resume_point: ResumePoint,
        config: ApplierConfig,
        lease: Lease,
        runner_id: str,
        signal_data_collection: str | None = None,
        verifier=None,
        transactional_ddl: bool = True,
        catalog=None,
        watermarks: dict[str, int] | None = None,
        completion: SnapshotCompletion | None = None,
        snapshot_audit=None, descriptor_provider=None,
        binary_handling_mode: str = "base64", hstore_handling_mode: str = "map",
        control_schema: str | None = None,
        service_context=None,
        source_cluster_id: str | None = None,
        source_timeline: int | None = None,
        strict_event_identity: bool = False,
    ):
        self.con = con
        self.pipeline = pipeline
        self.namespace = namespace
        self.dataset = dataset
        self.topic_prefix = topic_prefix
        self.signal_data_collection = signal_data_collection
        self.ignored_source_tables = (
            {signal_data_collection} if signal_data_collection else set()
        )
        self.offset_path = offset_path
        self.resume_point = resume_point
        self.cfg = config
        self.lease = lease
        self.runner_id = runner_id
        self.verifier = verifier
        self.transactional_ddl = transactional_ddl
        self.control_schema = resolve_control_schema(control_schema)
        # Service mode supplies the parent/epoch fence; batch callers leave this
        # unset and retain the finite adapter's exact callback surface.
        self.service_context = service_context
        self.source_cluster_id = source_cluster_id
        self.source_timeline = source_timeline
        self.strict_event_identity = bool(strict_event_identity)
        self.delete_policy = config.delete_policy
        self._pending_delete_policy: DeleteModeResolver | None = None
        self.policy_gate = PolicyGate(config.pii_policy)
        #: `catalog.CatalogWatcher` or None. The only source of DROP TABLE knowledge
        #: (rubric 1.5): logical decoding does not carry DDL at all.
        self.catalog = catalog
        self.descriptor_provider = descriptor_provider
        self.binary_handling_mode, self.hstore_handling_mode = str(binary_handling_mode), str(hstore_handling_mode)
        #: rubric 1.6: `"<schema>.<table>" -> snapshot_lsn`. A source transaction whose
        #: **commit** LSN is below a table's watermark is already inside that table's
        #: snapshot image, so its events for that table are dropped. Per table, because
        #: only the re-snapshotted tables have a new image; per *commit* LSN, because a
        #: transaction that straddles the consistent point is in no image at all and
        #: must be applied in full (`cdc_flight.resnapshot`).
        self.watermarks: dict[str, int] = dict(watermarks or {})
        #: The one owner of this invocation's snapshot completion state; production callers pass the acquisition policy.
        self.snapshot_completion = completion or SnapshotCompletion.full_snapshot()
        #: the consistent point of the snapshot this run applied, if any
        self.last_snapshot_lsn: int | None = None
        #: The highest source LSN this run has RECEIVED from the connector, seeded
        #: with the durable resume point.  This is the per-slot reference the idle
        #: proof needs: `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`
        #: is CLUSTER-wide, so any other database in the same PostgreSQL cluster
        #: inflates it without a single byte of it being ours (review r12, R12-3:
        #: a co-tenant made every bounded run burn its whole `--max-seconds`).
        #: `highest_source_lsn - confirmed_flush_lsn` is ours and only ours.
        self.highest_source_lsn: int = int(resume_point.last_lsn or 0)
        #: The highest LSN of an application-data record delivered to this Flight.
        #: Heartbeats, logical messages, and the Flight's signal table advance the
        #: ordinary source offset above, but never this liveness high-water mark.
        self.highest_source_data_lsn: int = int(resume_point.last_lsn or 0)

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
            discard_streaming=config.resnapshot,
            incremental_enabled=True,
        )

        #: The open commit group, as ONE object. Replaced wholesale at COMMIT and at
        #: ROLLBACK; there is deliberately no way to reset part of it.
        self.group = OpenGroup()
        #: NOT part of the group: the offset-flush check is deliberately deferred to the
        #: NEXT batch, so it outlives the group it belongs to (Codex 7).
        self._pending_verification: tuple | None = None

        self.backfill = BackfillCoordinator(
            con,
            pipeline=pipeline,
            control_schema=self.control_schema,
            topic_prefix=topic_prefix,
        )

        # ADR §3.5 / D7: the fold and catalog policy live in dedicated modules.
        def _on_swap(state, snapshot_lsn, commit_id):
            if snapshot_audit is not None:
                snapshot_audit(state, snapshot_lsn, commit_id)
            self.backfill.complete_swap(
                state, snapshot_lsn=snapshot_lsn, commit_id=commit_id,
                in_transaction=True,
            )

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
            control_schema=self.control_schema,
            on_swap=_on_swap,
        )
        # Reattach active incremental runs before the next callback can route a row.
        # This path never drops or recreates a shadow; the legacy in-progress
        # snapshot recovery remains owned by destination/table_lifecycle.
        for run in self.backfill.active_runs():
            # A newly requested run already has its deterministic shadow name in
            # durable state, but it does not own an image yet.  Reattaching that
            # name before Debezium's STARTED notification lets a completion control
            # unit swap an empty shadow, after which the real READ rows reopen the
            # lifecycle and leave it permanently in_progress.  Only a run that has
            # crossed into loading (or retained recovery work) may reattach a
            # destination shadow at process start.
            if (
                run.effective_mode == "incremental"
                and run.shadow_table
                and run.state in {
                    "loading", "ready_to_swap", "swapping", "retry_wait", "blocked",
                }
            ):
                self.snapshots.reattach(
                    schema=run.source_schema,
                    table=run.source_table,
                    shadow=run.shadow_table,
                    run_id=run.run_id,
                    incremental=run.effective_mode == "incremental",
                )
        self.spill = SpillBuffer(
            con,
            binary_mode=self.binary_handling_mode,
            hstore_mode=self.hstore_handling_mode,
            control_schema=self.control_schema,
            policy_gate=self.policy_gate,
            require_sanitized=True,
        )
        self.alerts = AlertSink(
            con, pipeline=pipeline, control_schema=self.control_schema
        )
        # One durable admission snapshot per applier/run.  GroupPlan receives this
        # set; it must not query control state once per table or schema epoch.
        self.blocked_schema_tables = destination.blocked_schema_tables(
            con, pipeline, control_schema=self.control_schema
        )
        #: Relations whose streaming rows this run holds out of a RETAINED
        #: destination image because they owe a replacement snapshot under
        #: `CDC_DROP_MODE=log` (see `unit_admission.hold_log_owed_tail`).  Kept
        #: separate from `blocked_schema_tables` because the durable authority is
        #: the table lifecycle, not a schema refusal.  DIAGNOSTICS AND ALERT
        #: DEDUPLICATION ONLY: the set the planner actually reads is
        #: `OpenGroup.held_tables`, which expires with the group that observed the
        #: obligation, because that obligation can be discharged mid-run.
        self.held_streaming_tables: set[str] = set()
        # rubric 1.9: an illegal table-lifecycle transition must reach an operator, and
        # the only connection that survives this group's rollback is the sink's.
        self.snapshots.alerts = self.alerts
        self.catalog_coordinator = CatalogCoordinator(
            catalog=catalog,
            pipeline=pipeline,
            topic_prefix=topic_prefix,
            drop_mode=config.drop_mode,
            registry_of=lambda: self.registry,
            lifecycle_con=self.con,
            control_schema=self.control_schema,
            max_destructive_per_group=config.drop_max_per_group,
            allow_mass_drop=config.drop_allow_mass,
            policy_gate=self.policy_gate,
        )
        if self.catalog is not None:
            # Catalog-backed snapshot/backfill readers use the same application gate
            # as streaming records. This is an instance attribute so compatibility
            # watcher implementations remain usable without a new constructor API.
            try:
                self.catalog.policy_gate = self.policy_gate
            except (AttributeError, TypeError):
                # A few embedders pass a bound descriptor method as ``catalog``
                # rather than the watcher object. The callable itself is not
                # mutable; its owner is still configured above when available.
                pass
        if self.descriptor_provider is not None:
            try:
                self.descriptor_provider.policy_gate = self.policy_gate
            except (AttributeError, TypeError):
                # Bound methods and other immutable callables can still be used by
                # the normal source-read path. They receive the gate through their
                # owner (the watcher) or through explicit method arguments.
                pass
        self._schema_epochs = schema_epoch.SchemaEpochCoordinator(
            spill=self.spill,
            apply_units=self._apply_units,
            apply_catalog_phase=self._apply_catalog_phase,
            backfill_schema=lambda phase: self.catalog_coordinator.backfill_schema(
                self.con, phase
            ),
            clear_spill=self.spill.clear,
        )

        self._committer = None
        self._lock = threading.Lock()
        self._quiescence = threading.Condition(self._lock)
        # The service supervisor may request a lease renewal while Debezium is
        # otherwise idle.  All destination-connection users share this lock, so a
        # renewal can never race a callback transaction or the COMMIT_ACK gate.
        self._destination_operation_lock = threading.RLock()
        self._in_flight = 0
        self._callback_sealed = False
        self._callback_seal_reason: str | None = None
        self._callback_batches_rejected = 0
        self._callback_records_rejected = 0
        self._drain_requested = False
        #: Raw records belonging to Flight-owned source marker transactions that
        #: crossed this callback boundary.  This is deliberately based on receipt,
        #: not on SourceMarker.writes: a shutdown marker can be written and then be
        #: rejected after admission is sealed.
        self.source_marker_records_received = 0
        self._source_marker_receipts = SourceMarkerReceiptCounter(marker_prefixes)
        self.last_batch_at = time.monotonic()
        # A stock Debezium heartbeat is a control-only callback. It must advance the
        # slot, but it must not reset the completion watermark's data-quiet clock or a
        # read-only fallback can wait forever on a quiet source that heartbeats every
        # five seconds.
        self._last_callback_had_data = False
        #: A Flight signal row is source activity outside the configured delivery
        #: route. It keeps the transaction/event-count proof, but it disqualifies the
        #: quiet-source alternative until a real capture-table record arrives. A
        #: correctly configured source that is genuinely quiet has no such activity;
        #: a quarantined application row is real delivery evidence and clears it.
        self._ignored_source_activity = False

        # -- counters surfaced in the run summary (rubric 6.1) --------------- #
        self.record_count = 0
        self.batch_count = 0
        self.data_batch_count = 0
        self.skipped_count = 0
        self.snapshot_notification_count = 0
        self._pending_snapshot_notifications: list[Any] = []
        self._pending_backfill_notifications: list[Any] = []
        #: Re-snapshot streaming units are complete source observations but have no
        #: destination side. Keep only their acknowledgeable terminal handles until a
        #: preceding snapshot group is durable; they must never enter ``self.group``.
        self._pending_discarded_records: list[Any] = []
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
        self.quarantined_events = 0
        self.unscoped_refusals = 0
        #: Unknown third-party/builtin failures contained at a source-table boundary.
        #: The run still fails loudly after its healthy co-published work commits.
        self._contained_failures: list[dict] = []
        #: Explicit operator acknowledgements are diagnostic only: the relation stays
        #: blocked and stale until its full resnapshot completes.
        self._acknowledged_quarantines: set[str] = set()
        #: One commit-protocol retry may exclude a table whose destination SQL error
        #: invalidated the first transaction. It is cleared immediately after that
        #: retry; durable quarantine is the control-plane authority thereafter.
        self._excluded_destination_tables: set[str] = set()
        #: rubric 4.7: undecidable folds turned into automatic table rebuilds
        self.ambiguous_resnapshots_queued = 0
        #: events dropped because their transaction is already inside a table's image
        self.watermark_fenced_events = 0
        self.table_counts: dict[str, int] = {}
        self.last_commit_id = resume_point.commit_id
        #: Set immediately after the destination COMMIT for supervisor timing
        #: evidence; it is never used to authorize an acknowledgement.
        self.last_commit_monotonic: float | None = None
        self.error: BaseException | None = None
        self._next_commit_id = destination.next_commit_id(
            con, pipeline, control_schema=self.control_schema
        )
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
    def source_quiet_ready(self) -> bool:
        """Whether control-plane activity has not invalidated quiet admission."""
        with self._lock:
            ignored_source_activity = self._ignored_source_activity
        return self.snapshot_completed and not ignored_source_activity

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
            "quarantined_events": self.quarantined_events,
            "blocked_schema_tables": sorted(self.blocked_schema_tables),
            "held_streaming_tables": sorted(self.held_streaming_tables),
            "unscoped_refusals": self.unscoped_refusals,
            "contained_failures": list(self._contained_failures),
            "acknowledged_quarantines": sorted(self._acknowledged_quarantines),
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
            "backfill_notifications_pending": len(self._pending_backfill_notifications),
            **self.snapshot_completion.as_dict(),
            # Round 8 MAJOR-1: this is the callback/connection ownership proof. A late
            # callback after the seal is a recorded no-op and can never decode, write,
            # mutate replay state, or acknowledge Debezium.
            "callback_boundary": "sealed" if self._callback_sealed else "open",
            "callback_seal_reason": self._callback_seal_reason,
            "callback_batches_rejected": self._callback_batches_rejected,
            "callback_records_rejected": self._callback_records_rejected,
            "callback_quiesced": self.callback_quiesced,
            "drain_requested": self._drain_requested,
            "source_marker_records_received": self.source_marker_records_received,
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

    def wait_for_internal_teardown(self, timeout: float) -> bool:
        """Join the applier's age thread after callback quiescence is proved.

        The age thread can request a group close, but it never owns the Debezium
        committer.  Stopping and joining it after callback quiescence makes the
        supervisor's own runtime teardown explicit without using stock Debezium's
        ``close()`` as a callback barrier.
        """
        self._timer_stop.set()
        if threading.current_thread() is self._timer:
            return True
        self._timer.join(timeout=max(0.0, timeout))
        return not self._timer.is_alive()

    # ------------------------------------------------------------------ #
    # the Debezium callback
    # ------------------------------------------------------------------ #
    def handle_batch(self, records, committer) -> None:
        with self._destination_operation_lock:
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
            if self.service_context is not None:
                self.service_context.operation_started()
            callback_progressed = False
            try:
                self._handle(records, committer)
                # A callback is Flight-owned evidence only after the handler has
                # returned successfully.  A sampler observation or a callback that
                # is still wedged in decode/commit is not forward progress.
                # Only source-data records are liveness progress. Debezium
                # heartbeats, transaction metadata, and Flight bookkeeping can
                # make a non-empty callback without delivering any application
                # data; counting that callback kept an excluded route alive.
                callback_progressed = self._last_callback_had_data
                if self.service_context is not None:
                    if records:
                        # A callback can certify the admitted stock walsender even
                        # when it carries only a heartbeat. This is identity
                        # evidence, not liveness progress, and the context keeps
                        # those clocks separate.
                        self.service_context.note_engine_identity()
                    if callback_progressed:
                        self.service_context.note_engine_callback()
            except BaseException as exc:
                self.error = exc
                raise
            finally:
                if self.service_context is not None:
                    self.service_context.operation_finished(
                        progressed=callback_progressed
                    )
                with self._quiescence:
                    self._in_flight -= 1
                    if self._last_callback_had_data:
                        self.last_batch_at = time.monotonic()
                    if self._in_flight == 0:
                        self._quiescence.notify_all()

    def renew_service_lease(self) -> bool:
        """Apply one serialized service renewal when the callback is quiescent."""
        if self.service_context is None or not self.service_context.renew_requested:
            return False
        with self._destination_operation_lock:
            with self._quiescence:
                if self._callback_sealed or self._in_flight:
                    return False
            return self.service_context.renew_once(self.lease, self.con)

    def _handle(self, records, committer) -> None:
        self._committer = committer
        self._last_callback_had_data = False
        # The previous group's offset flush is verified here, outside the
        # commit->ack window, now that Debezium has polled at least once since it
        # (Codex 7).
        self._run_pending_verification()
        source_records = 0
        data_in_batch = 0
        for raw in records:
            incremental_notification = decode_incremental_notification(
                raw, topic_prefix=self.topic_prefix
            )
            if incremental_notification is not None:
                self._handle_incremental_notification(raw, incremental_notification)
                continue
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
            if rec.incremental:
                run = self.backfill.incremental_owner(rec.schema or "", rec.table or "")
                rec = decode_incremental_record(
                    raw,
                    topic_prefix=self.topic_prefix,
                    signal_id=run.signal_id if run is not None else None,
                )
                if run is not None and run.state == "complete":
                    # The terminal run is retained until source-offset
                    # reconciliation. A replay in that interval is already
                    # durable in the published shadow image; acknowledge it as a
                    # control record rather than ever feeding a stale READ into
                    # the new live table.
                    rec.kind = KIND_HEARTBEAT
                    rec.schema = None
                    rec.table = None
                    rec.op = None
                else:
                    # A row may be replayed before its STARTED notification's
                    # commit is observed. Do not create lifecycle/shadow state
                    # outside the commit protocol; GroupPlan will admit it after
                    # BEGIN, while a committed STARTED notification has already
                    # installed the route.
                    self._ensure_backfill_route(rec.schema, rec.table, run, create=False)
            self._sanitize_record(rec)
            ignored_source_record = rec.qualified_table in self.ignored_source_tables
            if ignored_source_record:
                # The source signal row is a control-plane request. It still
                # participates in the source transaction/offset proof as a
                # counted data event, but GroupPlan ignores the relation before
                # destination materialisation. Relabeling it as a heartbeat here
                # would make the transaction assembler count zero events against
                # Debezium's END event_count=1. The explicit record flag instead
                # lets the assembler keep proof and service liveness separate.
                with self._lock:
                    self._ignored_source_activity = True
            rec.ignored_source_record = ignored_source_record
            self.source_marker_records_received += self._source_marker_receipts.observe(rec)
            if rec.lsn is not None:
                self.highest_source_lsn = max(self.highest_source_lsn, int(rec.lsn))
            if rec.is_data and not ignored_source_record:
                with self._lock:
                    self._ignored_source_activity = False
                if rec.lsn is not None:
                    self.highest_source_data_lsn = max(
                        self.highest_source_data_lsn, int(rec.lsn)
                    )
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
        self._last_callback_had_data = bool(data_in_batch)

        if data_in_batch:
            maybe_crash("decode", self.data_batch_count)
            if self.service_context is not None:
                # This runs on the callback thread after records entered the
                # in-memory assembler but before commit admission.  A hard child
                # death here leaves the durable resume point untouched.
                matrix_crash("service_callback_midstream")

        if (
            self.service_context is not None
            and self.assembler.open_transaction_id is not None
        ):
            # An incomplete Postgres unit may span callbacks, but it is never a
            # destination durability boundary.  A restart replays it from the
            # last committed source point.
            matrix_crash("service_pg_transaction_open")

        if not self.group.units:
            # A discard-only re-snapshot poll has no destination transaction. The
            # handles remain pending until a replacement snapshot/terminal group
            # reaches the guarded COMMIT -> acknowledgement path below. An empty
            # Debezium poll is not a durability boundary for the throwaway slot.
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
        if self._drain_requested:
            self.group.close_requested = True
        if self.group.close_requested or drained or self._soft_trigger_hit():
            self.commit_group(self._trigger_name(drained))

    def _ensure_backfill_route(
        self, schema: str | None, table: str | None, run=None, *, create: bool = True
    ):
        if not schema or not table:
            return
        run = run or self.backfill.active(schema, table)
        if run is None or run.state not in {
            "requested", "preparing", "loading", "ready_to_swap", "swapping",
            "retry_wait", "blocked",
        }:
            return
        if f"{schema}.{table}" not in {state.schema + "." + state.table for state in self.snapshots.states()}:
            if not create:
                return
            self.snapshots.state_for(
                schema,
                table,
                retain_existing=True,
                incremental=run.effective_mode == "incremental",
                run_id=run.run_id,
            )

    def _handle_incremental_notification(self, raw, notification) -> None:
        # Queue state with the source notification. The commit protocol applies
        # it after its one BEGIN and before row DML, so notification state,
        # shadow rows, progress, and the resume point share one transaction.
        self._pending_backfill_notifications.append(notification)
        decoded = decode(raw, topic_prefix=self.topic_prefix, want_offsets=True)
        decoded.kind = KIND_HEARTBEAT
        decoded.schema = None
        decoded.table = None
        decoded.op = None
        decoded.incremental_rows = notification.rows
        if notification.observation == "TABLE_SCAN_COMPLETED" and notification.table:
            schema, table = notification.table.split(".", 1)
            unit = CompleteUnit(
                kind=UNIT_SNAPSHOT_CHUNK,
                records=[decoded],
                schema=schema,
                table=table,
                nbytes=decoded.nbytes,
                incremental=True,
                snapshot_last_for_table=(
                    (notification.status or "").upper()
                    in {"SUCCEEDED", "EMPTY", "COMPLETED"}
                ),
            )
        else:
            unit = CompleteUnit(
                kind=UNIT_CONTROL,
                records=[decoded],
                nbytes=decoded.nbytes,
                last_lsn=decoded.lsn or 0,
                commit_lsn=decoded.lsn,
            )
        self._add_unit(unit)

    def _apply_backfill_notifications(self) -> None:
        """Apply queued stock state after commit_protocol has opened its transaction."""
        for notification in self._pending_backfill_notifications:
            run = self.backfill.observe_notification(notification, in_transaction=True)
            if notification.table and "." in notification.table:
                schema, table = notification.table.split(".", 1)
                self._ensure_backfill_route(schema, table, run)

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
        unit_admission.add_unit(self, unit)

    def _sanitize_record(self, record: PendingRecord) -> PendingRecord:
        """Apply the one post-decode/pre-assembler policy boundary.

        Descriptor lookup is a source-catalog read. It is intentionally performed
        before the record enters the transaction assembler, while all destination
        DDL/DML remains owned by the later commit group.
        """
        self._activate_delete_policy_at_boundary()
        provider = self.descriptor_provider or (
            getattr(self.catalog, "descriptors_for", None)
            if self.catalog is not None
            else None
        )
        context = None
        if provider is not None and record.qualified_table:
            context = provider(record.qualified_table)
        # The policy gate replaces the connector object with an opaque
        # acknowledgement handle.  Capture only the non-sensitive Connect offset
        # facts before that replacement so the durable resume-point builder never
        # needs to inspect the handle (or retain a decoded source mapping).
        if record.raw is not None and record.source_offset is None:
            record.source_partition, record.source_offset = offsets_of(record.raw)
        record.delete_mode = self.delete_policy.resolve(record.qualified_table)
        record.delete_policy_epoch = int(self.delete_policy.epoch)
        record.delete_policy_digest = self.delete_policy.digest
        self.policy_gate.sanitize(record, context)
        return record

    def request_delete_policy(self, policy: DeleteModeResolver) -> None:
        """Stage a delete-policy change for the next PostgreSQL transaction.

        A configuration reload can arrive while Debezium is delivering an open
        source transaction.  Applying it to one row in that transaction would make
        the durable event ledger describe two semantics for one source commit.  The
        request is therefore held until the assembler has observed the transaction's
        END (or until the next implicit transaction boundary).
        """
        if not isinstance(policy, DeleteModeResolver):
            raise TypeError("delete policy requests require DeleteModeResolver")
        if policy.digest == self.delete_policy.digest:
            self._pending_delete_policy = None
            return
        self._pending_delete_policy = policy

    def _activate_delete_policy_at_boundary(self) -> None:
        pending = self._pending_delete_policy
        if pending is None or self.assembler.open_transaction_id is not None:
            return
        previous = self.delete_policy
        self.delete_policy = pending
        self._pending_delete_policy = None
        # If the preceding whole transaction is buffered, make it its own
        # destination transaction.  The current record has not entered the
        # assembler yet, so this is a genuine source-transaction boundary.
        if self.group.units and previous.digest != pending.digest:
            self.commit_group("delete_policy_boundary")

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
    def commit_group(self, trigger: str) -> CommitResult:
        return commit_protocol.commit_group(self, trigger)

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

    def _arm_commit_timeout_alert(self, commit_id: int) -> None:
        """Arm a durable watchdog alert before the commit/ack exclusion opens.

        The watchdog callback is intentionally not allowed to perform destination or
        logging I/O: it can fire while ``COMMIT_ACK`` is active. Leaving this row in
        place after a hard exit is the observable timeout; a successful group clears it
        only after the exclusion has closed.
        """
        armed = self.alerts.raise_alert_once(
            severity="critical",
            code="commit_timeout",
            message=(
                f"commit group {commit_id} has an armed bounded commit watchdog; if "
                "this alert remains, the commit or acknowledgement did not return "
                "within the watchdog timeout. The commit is AMBIGUOUS and the "
                "destination may already be durable, so the next run must reconcile "
                "it before claiming success"
            ),
            condition_key="commit_timeout",
            occurrence_key=OccurrenceKey.from_commit(
                _commit_reservation(self.pipeline, commit_id),
                pipeline=self.pipeline,
            ),
            context={
                "commit_id": commit_id,
                "armed_before_commit_ack_window": True,
                "timeout_seconds": self.cfg.commit_timeout,
                "runner_id": self.runner_id,
            },
        )
        if not armed:
            log.critical(
                "could not durably arm the commit watchdog alert for commit_id=%s",
                commit_id,
            )

    def _clear_commit_timeout_alert(self, commit_id: int) -> None:
        """Clear the conservative watchdog alert after COMMIT_ACK has closed."""
        self.alerts.clear_alert_once(
            code="commit_timeout",
            condition_key="commit_timeout",
            occurrence_key=OccurrenceKey.from_commit(
                _commit_reservation(self.pipeline, commit_id),
                pipeline=self.pipeline,
            ),
        )

    def hold_streaming_tail(self, tables) -> None:
        """Hold these relations' ordinary stream rows out of a retained image.

        See `unit_admission.hold_log_owed_tail`.  One deduplicated alert per
        relation per run: the durable authority is `table_state.snapshot_state`,
        which already records the replacement-snapshot obligation, so a second
        durable row here would be a second writer of the same fact.
        """
        for qualified in tables:
            # The GROUP decides what is skipped; the run-scoped set is diagnostics
            # and alert deduplication only. See `OpenGroup.held_tables`.
            self.group.held_tables.add(qualified)
            if qualified in self.held_streaming_tables:
                continue
            self.held_streaming_tables.add(qualified)
            # The obligation can survive a process restart.  The run-scoped set
            # above handles duplicate observations in one run; the durable marker
            # bounds the alert across runs for the same snapshot obligation.  A later
            # snapshot epoch is a new incident and must be observable again.
            con = getattr(self, "con", None)
            state_row = None
            if con is not None and hasattr(con, "execute"):
                state_row = con.execute(
                    f"SELECT snapshot_epoch FROM "
                    f"{destination._control_table(self.control_schema, 'table_state')} "
                    "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                    [
                        self.pipeline,
                        qualified.split(".", 1)[0],
                        qualified.split(".", 1)[1],
                    ],
                ).fetchone()
            marker = f"{qualified}:{state_row[0] if state_row else 0}"
            if (
                con is not None
                and destination.alert_marker_exists(
                    con,
                    pipeline=self.pipeline,
                    code="streaming_tail_held_for_resnapshot",
                    marker_key="snapshot_obligation",
                    marker_value=marker,
                    control_schema=self.control_schema,
                )
            ):
                continue
            self.alerts.raise_alert(
                severity="warning",
                code="streaming_tail_held_for_resnapshot",
                message=(
                    f"{qualified} owes a replacement snapshot under drop_mode=log; "
                    "its streaming rows are held out of the retained destination "
                    "image until that snapshot completes. Other relations in the "
                    "same transaction are unaffected."
                ),
                context={
                    "source_relation": qualified,
                    "snapshot_obligation": marker,
                    "resnapshot_required": True,
                },
            )

    def _record_schema_refusal(self, refused: SchemaEvolutionRefused) -> None:
        if refused.refusal_recorded:
            return
        spill_refusal.record_schema_refusal(self, refused)
        if refused.source_schema and refused.source_table:
            self.blocked_schema_tables.add(
                f"{refused.source_schema}.{refused.source_table}"
            )

    def _contain_table_failure(self, refused: SchemaEvolutionRefused, original) -> None:
        failure_containment.contain_table_failure(self, refused, original)

    def _contain_destination_failure(
        self,
        refused: SchemaEvolutionRefused,
        original: Exception,
        *,
        destination_execution: bool = True,
    ) -> str:
        return failure_containment.contain_destination_failure(
            self,
            refused,
            original,
            destination_execution=destination_execution,
        )

    def _contextualize_schema_refusal(self, refused: SchemaEvolutionRefused) -> None:
        """Attach scope only when the failed group identifies exactly one relation."""
        events = [event for unit in self.group.units for event in unit.events]
        candidates = {
            (item.schema, item.table, item.qualified_table)
            for item in events
            if item.schema and item.table
        }
        if not refused.source_schema and not refused.source_table:
            if len(refused.source_tables) == 1:
                refused.source_schema, refused.source_table, refused.target = (
                    refused.source_tables[0]
                )
            elif len(candidates) == 1:
                refused.source_schema, refused.source_table, refused.target = (
                    next(iter(candidates))
                )
        if refused.detected_lsn is None:
            lsns = [int(item.lsn) for item in events if item.lsn is not None]
            if lsns:
                refused.detected_lsn = max(lsns)

    def _handle_spill_refusal(
        self, refused: SchemaEvolutionRefused, events: list[PendingRecord]
    ) -> None:
        spill_refusal.handle(self, refused, events)

    def _rollback_quietly(self) -> None:
        if not self.group.txn_open:
            self._reset_after_rollback()
            return
        try:
            self.con.execute("ROLLBACK")
        except Exception:  # pragma: no cover - never mask the original error
            log.debug("rollback failed", exc_info=True)
        finally:
            self.group.txn_open = False
            self._reset_after_rollback()

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

    # -- resume point (ADR §4.3, `offsets.py`) ------------------------------- #
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
    def _apply_units_by_schema_epoch(
        self,
        group: list[CompleteUnit],
        commit_id: int,
        *,
        has_data: bool,
        catalog_plan: CatalogPlan | None,
        catalog_stats: dict,
    ) -> dict:
        return self._schema_epochs.apply(
            group,
            commit_id,
            has_data=has_data,
            catalog_plan=catalog_plan,
            catalog_stats=catalog_stats,
            created_in_txn=self.group.created_in_txn,
        )

    @staticmethod
    def _refuse_mixed_schema_epoch(events: list, actions: list) -> None:
        schema_epoch.refuse_mixed_schema_epoch(events, actions)

    def _apply_units(
        self,
        group: list[CompleteUnit],
        commit_id: int,
        *,
        has_data: bool,
        clear_spill: bool = True,
        created_in_txn: set[str] | None = None,
        excluded_tables: set[str] | None = None,
    ) -> dict:
        return unit_apply.apply_units(
            self,
            group,
            commit_id,
            has_data=has_data,
            clear_spill=clear_spill,
            created_in_txn=created_in_txn,
            excluded_tables=(
                self._excluded_destination_tables
                if excluded_tables is None
                else excluded_tables
            ),
        )

    # ------------------------------------------------------------------ #
    # table-level events and catalog DDL (rubric 1.5)
    # ------------------------------------------------------------------ #
    def _flush_table_events(self, commit_id: int) -> None:
        catalog_commit.flush_table_events(self, commit_id)

    def _plan_catalog_changes(self, durable_lsn: int) -> CatalogPlan | None:
        return catalog_commit.plan_catalog_changes(self, durable_lsn)

    def _apply_catalog_phase(
        self,
        commit_id: int,
        plan: CatalogPlan,
        stats: dict,
        *,
        schema_only: bool,
    ) -> None:
        catalog_commit.apply_catalog_phase(
            self, commit_id, plan, stats, schema_only=schema_only
        )

    def _settle_catalog(self, group_obj: OpenGroup) -> None:
        catalog_commit.settle_catalog(self, group_obj)

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
        try:
            return spill_protocol.stage_events(
                self, events, unit_seq=unit_seq, snapshot=snapshot
            )
        except AdmissionError as error:
            refused = as_schema_refusal(error, refusal_origin="spill_protocol")
            self._handle_spill_refusal(refused, events)
            raise
        except Exception:
            # Descriptor enrichment is a source control read, not a table-DML
            # boundary.  If the driver/session fails after stage_events opened
            # the spill transaction, close that transaction here while preserving
            # the original run-level error; never convert it into a table refusal.
            self._rollback_quietly()
            raise

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

    def request_drain(self) -> None:
        """Ask the next admitted callback to flush a complete group before seal.

        The age timer and this request only set intent.  ``handle_batch`` remains the
        sole owner of ``commit_group`` and of Debezium acknowledgements, preserving
        the callback-thread ownership rule during a service drain.
        """
        self._drain_requested = True
        self.group.close_requested = True
