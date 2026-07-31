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

import contextlib
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any

from . import apply_sql, destination, naming, resume, table_work
from .assembler import (
    UNIT_SNAPSHOT_CHUNK,
    UNIT_TXN,
    CompleteUnit,
    TransactionAssembler,
)
from .catalog_apply import CatalogCoordinator
from .config import (
    DROP_MODES,
    DROP_REPLICATE,
    TRUNCATE_MODES,
    TRUNCATE_REPLICATE,
)
from .destination import AlertSink, Lease, ResumePoint
from .envelope import PendingRecord, decode
from .errors import AmbiguousDelete, DestinationIdentityCollision
from .faults import arm_group, maybe_crash
from .planner import GroupPlan, stream_event_id
from .snapshot import SnapshotCoordinator
from .spill import SpillBuffer, StagedEvent

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
    #: rubric 1.5, `CDC_TRUNCATE_MODE` / `CDC_DROP_MODE`. `replicate` is what the
    #: rubric's 5 asks for ("replicated just like Postgres handles them"); the other
    #: modes exist because "faithful" destroys destination data, and an operator who
    #: wants the audit trail without the destruction should not have to fork.
    truncate_mode: str = TRUNCATE_REPLICATE
    drop_mode: str = DROP_REPLICATE
    #: rubric 1.5 circuit breaker (Opus MAJOR-3 / Q2). At most this many destination
    #: tables may be destroyed by one commit group; the whole set is refused when the
    #: limit is exceeded, never half of it.
    drop_max_per_group: int = 1
    drop_allow_mass: bool = False
    #: Re-read the source relation immediately before destroying its destination
    #: table, and fail closed if the source cannot be asked (Codex 4).
    drop_revalidate: bool = True
    #: How long `COMMIT` may take before the process aborts (rubric 1.7 / 4.5).
    #: 0 disables the watchdog.
    commit_timeout: float = 300.0
    #: rubric 4.7: an undecidable fold (`AmbiguousDelete`) queues an automatic
    #: re-snapshot of the affected table instead of failing identically for ever.
    #: `CDC_AMBIGUOUS_RESNAPSHOT=0` restores the permanent-failure behaviour.
    resnapshot_on_ambiguity: bool = True
    #: rubric 1.6: this applier is serving a **re-snapshot** engine, not the pipeline's
    #: own stream. It applies snapshot chunks and DISCARDS streaming units: the
    #: re-snapshot's slot is a throwaway whose offsets nobody reads, so a streaming
    #: event applied here would be delivered a second time by the real slot. See
    #: `cdc_flight.resnapshot`.
    resnapshot: bool = False

    def __post_init__(self) -> None:
        # A typo must not silently restore Debezium's "truncates are skipped" default.
        if self.truncate_mode not in TRUNCATE_MODES:
            raise ValueError(
                f"CDC_TRUNCATE_MODE={self.truncate_mode!r} is not one of {TRUNCATE_MODES}"
            )
        if self.drop_mode not in DROP_MODES:
            raise ValueError(f"CDC_DROP_MODE={self.drop_mode!r} is not one of {DROP_MODES}")


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
        #: the consistent point of the snapshot this run applied, if any
        self.last_snapshot_lsn: int | None = None
        #: True once every table that entered the snapshot phase has been swapped in
        self.snapshot_completed = False

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
        # ADR §3.5 / D7, §3.4, the fold and the catalog policy all live in their own
        # modules (ADR §15/A29, §18/A37): every blocker of the last two review rounds
        # was a consequence of two paths doing one job inside one file.
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
        self.alerts = AlertSink(con, pipeline=pipeline)
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
        self.truncates_applied = 0
        self.truncates_logged = 0
        self.resnapshot_discarded_events = 0
        #: rubric 4.7: undecidable folds turned into automatic table rebuilds
        self.ambiguous_resnapshots_queued = 0
        #: events dropped because their transaction is already inside a table's image
        self.watermark_fenced_events = 0
        #: `_cdc_flight.table_events` rows collected while applying THIS group, all
        #: written inside its transaction.
        self._table_events: list[dict] = []
        self._table_event_seq = 0
        #: the catalog plan this group is committing, settled only after COMMIT
        self._catalog_plan = None
        #: alerts raised only once the transaction has settled (Codex 7)
        self._pending_alerts: list[dict] = []
        #: source tables this group actually wrote, handed to the watcher after COMMIT
        self._group_source_tables: set[str] = set()
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

        if self.cfg.resnapshot and unit.kind == UNIT_TXN:
            # A re-snapshot engine streams for as long as it takes us to notice the
            # snapshot finished. Those events belong to the real slot, which has not
            # consumed them, so applying them here would be a duplicate delivery.
            unit.fenced = True
            self.fenced_units += 1
            self.fenced_events += unit.event_count
            self.resnapshot_discarded_events += unit.event_count

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
        self._table_events = []
        self._table_event_seq = 0
        self._catalog_plan = None
        self._pending_alerts = []
        self._group_source_tables = set()

    # ------------------------------------------------------------------ #
    # the transaction
    # ------------------------------------------------------------------ #
    def commit_group(self, trigger: str) -> None:
        group = self._group
        if not group:
            return
        commit_id = self._spill_commit_id or self._next_commit_id
        opened_at = destination.now()
        # Tell the destination-fault wrapper which data group this is, so a
        # `destination_*` fault fires at the group the spec names rather than at one
        # the wrapper inferred from the SQL it happened to see (rubric 1.7).
        arm_group(self.data_commit_groups + 1)
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
            new_point = resume.point_for(
                group,
                previous=self.resume_point,
                commit_id=commit_id,
                snapshot_epoch=self.snapshots.epoch,
            )
            # rubric 1.5: DDL the stream cannot carry, fenced on the resume point this
            # group is about to make durable.
            self._apply_catalog_changes(commit_id, new_point.last_lsn, stats)
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
            with _commit_watchdog(self.cfg.commit_timeout, commit_id):
                self.con.execute("COMMIT")
            self._txn_open = False
            if has_data:
                maybe_crash("post_commit_pre_ack", self.data_commit_groups + 1)
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
            self._request_resnapshot_for(ambiguous)
            self._rollback_quietly()
            raise
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

        self._settle_catalog()
        self._flush_alerts()
        if self.snapshots.swaps and not self.snapshots.active:
            # Every table that entered the snapshot phase has been swapped in and the
            # swap is durable. `cdc_flight.resnapshot` stops its engine on this rather
            # than waiting out an idle window it does not need.
            self.snapshot_completed = True
        self.commit_groups += 1
        if has_data:
            self.data_commit_groups += 1
        self.applied_events += stats["events"]
        self.last_commit_id = commit_id
        self.resume_point = new_point
        self._next_commit_id = commit_id + 1
        if self.cfg.verify_offset_file:
            self._pending_offset_key_blob, self._pending_offset_blob = (
                resume.capture_offset_file(self.offset_path, new_point)
            )
        self._reset_group()

    def _request_resnapshot_for(
        self, ambiguous: AmbiguousDelete | DestinationIdentityCollision
    ) -> None:
        """Turn an undecidable fold into a durable re-snapshot request (rubric 4.7).

        Deliberately best-effort and deliberately loud either way: the run still fails
        with a non-zero exit, because "I could not fold this and I have queued a rebuild"
        is information an operator wants even though no human action is required.

        The honesty note that belongs with it, and which `resnapshot` records in
        `table_events`: a re-snapshot replaces **current state**. The individual change
        events of the fenced span are not delivered, so a changelog (rubric 8.2) sees a
        discontinuity there - an image at the consistent point rather than the events
        that produced it. Current state is exact; per-event history for that span is not
        recoverable, because the ambiguity was precisely that the events did not say what
        they did.
        """
        if not self.cfg.resnapshot_on_ambiguity:
            log.error(
                "CDC_AMBIGUOUS_RESNAPSHOT=0: not queueing a re-snapshot for the fold "
                "that could not be decided, so this failure will repeat on every run "
                "until a human intervenes"
            )
            return
        schema, table = ambiguous.source_schema, ambiguous.source_table
        if not schema or not table:
            log.error(
                "an undecidable fold did not name its table, so no re-snapshot can be "
                "queued for it: %s", ambiguous,
            )
            return
        target = ambiguous.target or naming.destination_table(
            self.topic_prefix, schema, table
        )
        recorded = self.alerts.request_snapshot(
            pipeline=self.pipeline, schema=schema, table=table, target=target
        )
        self._pending_alerts.append(
            {
                "severity": "critical",
                "code": "ambiguous_delete_resnapshot",
                "on_rollback": True,
                "message": (
                    f"the fold for {schema}.{table} could not be decided, so the commit "
                    "group was refused. "
                    + (
                        "The table is now marked awaiting_snapshot and the next run "
                        "rebuilds it automatically; no human action is required, but "
                        "per-event history for the rebuilt span is replaced by the "
                        "snapshot image (rubric 4.7 / ADR 0001 §19/A47)."
                        if recorded
                        else "The re-snapshot request could NOT be recorded, so this "
                        "failure WILL repeat until a human intervenes."
                    )
                ),
                "context": {
                    "source_schema": schema,
                    "source_table": table,
                    "target_table": target,
                    "resnapshot_queued": recorded,
                    "detail": str(ambiguous),
                },
            }
        )
        self.ambiguous_resnapshots_queued += int(recorded)

    def _rollback_quietly(self) -> None:
        if not self._txn_open:
            self._reset_after_rollback()
            return
        try:
            self.con.execute("ROLLBACK")
        except Exception:  # pragma: no cover - never mask the original error
            log.debug("rollback failed", exc_info=True)
        finally:
            self._txn_open = False
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
        alerts = [a for a in self._pending_alerts if a.get("on_rollback")]  # Codex 7
        # Every CREATE / ALTER we issued is gone with the transaction, so the
        # cached destination shape is now a lie. Rebuilding it is cheap and
        # not doing it is how a rolled-back run corrupts the next one.
        self.registry = apply_sql.SchemaRegistry(
            self.con, self.dataset, constraints=self.cfg.destination_constraints
        )
        failed = list(self._group)
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
    def _apply_units(self, group: list[CompleteUnit], commit_id: int, *, has_data: bool) -> dict:
        plan = GroupPlan(
            self.con,
            commit_id=commit_id,
            registry_of=lambda: self.registry,
            snapshots=self.snapshots,
            spill=self.spill,
            truncate_mode=self.cfg.truncate_mode,
            created_in_txn=self._created_in_txn,
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
                self._group_is_snapshot = True
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
                self.con,
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
        if self._group_is_snapshot and stats.get("last_lsn"):
            # Every snapshot record of one snapshot carries the exported snapshot's
            # consistent point, so this is `C` (rubric 1.6, `cdc_flight.resnapshot`).
            self.last_snapshot_lsn = stats["last_lsn"]
        self._group_source_tables |= plan.source_tables
        self._table_events.extend(plan.markers())
        self._flush_table_events(commit_id)
        return stats

    # ------------------------------------------------------------------ #
    # table-level events and catalog DDL (rubric 1.5)
    # ------------------------------------------------------------------ #
    def _flush_table_events(self, commit_id: int) -> None:
        """Write this group's `table_events` rows, inside its transaction.

        Deliberately transactional with the data: "the destination table was emptied"
        and "here is the source event that emptied it" must become true together, or
        the audit trail can outlive a rolled-back apply and describe something that
        never happened.
        """
        for marker in self._table_events:
            destination.write_table_event(
                self.con,
                pipeline=self.pipeline,
                commit_id=commit_id,
                seq=self._next_table_event_seq(),
                **marker,
            )
        self._table_events = []

    def _next_table_event_seq(self) -> int:
        self._table_event_seq += 1
        return self._table_event_seq

    def _apply_catalog_changes(self, commit_id: int, durable_lsn: int, stats: dict) -> None:
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
        self._catalog_plan = plan
        self._table_events.extend(coordinator.apply(self.con, plan, stats))
        # A destructive action that could not be applied is exactly the signal an
        # operator must still get when the group rolls back; one that describes an
        # applied action must NOT outlive the rollback that undid it (Codex 7).
        self._pending_alerts.extend(plan.alerts)
        if self._table_events:
            self._flush_table_events(commit_id)

    def _settle_catalog(self) -> None:
        """Forget the catalog work this group made durable. Runs after COMMIT."""
        if self.catalog is None:
            return
        plan = self._catalog_plan
        if plan is not None:
            self.catalog_coordinator.settle(plan, self._group_source_tables)
            self._catalog_plan = None
        elif self._group_source_tables:
            self.catalog.observe_replicated(self._group_source_tables)

    def _flush_alerts(self) -> None:
        for alert in self._pending_alerts:
            self._raise_alert(alert)
        self._pending_alerts = []

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
                        event_id=stream_event_id(event),
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


@contextlib.contextmanager
def _commit_watchdog(timeout: float, commit_id: int):
    """Bound `COMMIT`. A hung commit kills the process; that is the honest answer.

    Rubric 1.7 requires every injected fault to end in a clean recovery or a loud
    failure. A `COMMIT` that never returns is neither, and nothing in DuckDB or the
    MotherDuck client imposes a deadline of its own, so the run would hang for ever
    holding the lease (which is also rubric 4.5's "hanging or locking that prevents
    recovery").

    Hard-exiting is safe precisely *because* of Invariant O. The commit is ambiguous
    - it may already have been durable server-side - but nothing has entered
    Debezium's offset store, so the next run reads whichever of `W` / `W-prime` the
    destination actually holds and resumes from exactly there (ADR 0001 §4.6 F5).
    Exit code 75 (`EX_TEMPFAIL`) rather than the fault injector's 137: this is a real
    operational failure and a supervisor should retry it.
    """
    if not timeout or timeout <= 0:
        yield
        return

    def _fire() -> None:  # pragma: no cover - exercised by the fault test in a child
        log.critical(
            "destination COMMIT for commit_id=%s did not return within %.0fs; aborting "
            "the process. The commit is AMBIGUOUS and that is safe: nothing was "
            "acknowledged to Debezium, so the next run resumes from whatever the "
            "destination actually holds (ADR 0001 §4.6 F5).",
            commit_id, timeout,
        )
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(75)

    timer = threading.Timer(timeout, _fire)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


def _epoch_ms(value) -> Any:
    """Debezium's `source.ts_ms` as a timestamp, so end-to-end lag is a SQL
    subtraction rather than an arithmetic puzzle for whoever writes rubric 6.1."""
    if value is None:
        return None
    from datetime import UTC, datetime

    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


