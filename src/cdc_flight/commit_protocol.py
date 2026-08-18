"""Commit-group durability and acknowledgement ownership.

The applier remains the lifecycle facade; this module owns the transaction protocol
from BEGIN through the guarded COMMIT/ack boundary and post-commit bookkeeping.
"""

from __future__ import annotations

from . import commit_metadata, destination, offsets, self_heal, table_writer
from .commit_group import CommitResult, OpenGroup
from .errors import (
    AdmissionError,
    AmbiguousDelete,
    DestinationExecutionFailure,
    DestinationIdentityCollision,
    TableWriteFailure,
    as_schema_refusal,
)
from .faults import arm_group, maybe_crash
from .run_state import COMMIT_ACK

OWNER = "commit-durability"


def commit_group(self, trigger: str) -> CommitResult:
    """Commit the one destination-owned group.

    Re-snapshot streaming units are discarded before this method is reached. A
    single owner therefore publishes the only shared resume point, and this method
    has no alternate connection/group context that could overtake it.
    """
    group = self.group.units
    original_group = self.group
    if not group:
        # In particular, never acknowledge a discard-only re-snapshot tail here:
        # this method has not opened or committed a MotherDuck transaction.
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
    commit_id = self.group.spill_commit_id or self._next_commit_id
    opened_at = destination.now()
    # Tell the destination-fault wrapper which data group this is, so a
    # `destination_*` fault fires at the group the spec names rather than at one
    # the wrapper inferred from the SQL it happened to see (rubric 1.7).
    fault_group = self.data_commit_groups + 1
    arm_group(fault_group)
    has_incremental = any(getattr(unit, "incremental", False) for unit in group)
    has_snapshot_unit = any(unit.kind == "snapshot_chunk" for unit in group)
    if not self.group.txn_open:
        self.con.execute("BEGIN TRANSACTION")
        self.group.txn_open = True
    try:
        self._apply_backfill_notifications()
        self.lease.renew(self.con)
        new_point = offsets.point_for(
            group,
            previous=self.resume_point,
            commit_id=commit_id,
            snapshot_epoch=self.snapshots.epoch,
        )
        catalog_plan = self._plan_catalog_changes(new_point.last_lsn)
        # NOT `or spill.rows > 0`: staged rows belonging only to *fenced*
        # units are about to be discarded, and counting them made a group with no
        # applicable content a "data group", which shifts every `<nth>`-indexed
        # fault anchor by one (Codex 5). Compute this AFTER the plan fence so a
        # same-group replacement unit cannot make a fenced-only group look like
        # data merely because it arrived before catalog planning.
        has_data = any(
            not u.fenced and (u.events or u.spilled_events) for u in group
        )
        fault_enabled = has_data
        if has_data:
            maybe_crash("begin", fault_group)
        if has_incremental:
            maybe_crash("incremental_chunk_before_shadow_write", fault_group)
        catalog_stats = {"tables": set()}
        stats = self._apply_units_by_schema_epoch(
            group,
            commit_id,
            has_data=has_data,
            catalog_plan=catalog_plan,
            catalog_stats=catalog_stats,
        )
        # Incremental snapshot cursor/state is written only after its shadow rows
        # have been folded and materialised, and before the single destination
        # COMMIT.  It therefore cannot outrun MotherDuck durability (Invariant O).
        if has_incremental:
            maybe_crash("incremental_chunk_after_shadow_write_before_progress", fault_group)
        self.backfill.commit_progress(group, in_transaction=True)
        if has_incremental:
            maybe_crash("incremental_chunk_after_progress_before_md_commit", fault_group)
        if has_snapshot_unit:
            maybe_crash("before_swap_commit", fault_group)
        stats["tables"].update(catalog_stats["tables"])
        if catalog_plan is not None:
            self._apply_catalog_phase(
                commit_id, catalog_plan, stats, schema_only=False
            )
            self.group.pending_alerts.extend(catalog_plan.alerts)
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
            max_source_ts=commit_metadata.epoch_ms(stats["max_source_ts"]),
            tables_touched=sorted(table_writer.live_names(stats["tables"])),
            control_schema=self.control_schema,
        )
        destination.write_resume_point(
            self.con,
            pipeline=self.pipeline,
            namespace=self.namespace,
            point=new_point,
            commit_id=commit_id,
            offset_blob=self._pending_offset_blob,
            offset_key_blob=self._pending_offset_key_blob,
            control_schema=self.control_schema,
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
        # A non-snapshot group can be durable without containing a replacement
        # image. Keep discard-only handles pending across that boundary too; the
        # only group that may discharge them is a snapshot/terminal group.
        pending_discards = (
            list(self._pending_discarded_records)
            if self.group.is_snapshot
            else []
        )
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
                self.con.execute("COMMIT")
                self.group.txn_open = False
                if has_incremental:
                    maybe_crash("after_md_commit_before_markProcessed", fault_group)
                if has_snapshot_unit:
                    maybe_crash("after_swap_commit_before_ack", fault_group)
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
                for record in pending_discards:
                    if record.raw is None:
                        continue
                    self._committer.markProcessed(record.raw)
                    marked += 1
                if has_incremental:
                    maybe_crash("after_markProcessed_before_markBatchFinished", fault_group)
                self._committer.markBatchFinished()
            finally:
                # A mark call can raise; a stuck window would silently drop every
                # later phase write, so the gate is closed immediately after the
                # last acknowledgement in all cases.
                COMMIT_ACK.leave()
            if has_incremental:
                maybe_crash("after_ack_before_next_poll", fault_group)
            self._pending_backfill_notifications.clear()
            if pending:
                # Do not discard the handles until markBatchFinished succeeds.
                del self._pending_snapshot_notifications[: len(pending)]
            if pending_discards:
                del self._pending_discarded_records[: len(pending_discards)]
    except AdmissionError as error:
        refused = as_schema_refusal(error, refusal_origin="typed_planner")
        self._contextualize_schema_refusal(refused)
        self._rollback_quietly()
        self._record_schema_refusal(refused)
        raise
    except (DestinationExecutionFailure, TableWriteFailure) as failure:
        # A destination SQL error may have aborted the connection, and a Python
        # materializer failure may have happened after this table's DELETE. In both
        # cases the only safe table boundary is a full group rollback: the independent
        # sink records the refusal, then the same source transaction is replayed with
        # only the failed relation excluded. Commit/resume state is written only by
        # the successful retry.
        self._rollback_quietly()
        qualified = self._contain_destination_failure(
            failure.refused,
            failure.original,
            destination_execution=isinstance(failure, DestinationExecutionFailure),
        )
        self.group = OpenGroup(
            opened_at=original_group.opened_at,
            units=list(group),
            events=original_group.events,
            nbytes=original_group.nbytes,
            is_snapshot=original_group.is_snapshot,
            close_requested=original_group.close_requested,
            spill_commit_id=original_group.spill_commit_id,
        )
        self._excluded_destination_tables = {qualified}
        try:
            return commit_group(self, trigger)
        finally:
            self._excluded_destination_tables.clear()
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
        self._rollback_quietly()
        raise
    except BaseException:
        self._rollback_quietly()
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

    self._settle_catalog(self.group)
    self._flush_alerts(self.group)
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
    # A throwaway re-snapshot has its own Debezium offset file, which may already
    # include acknowledged duplicate streaming records that arrived after the
    # snapshot image's point. It is disposable handoff evidence, not the main
    # destination resume point; comparing that file with the snapshot group's
    # temporary point would manufacture an Invariant-O drift (r15 acceptance).
    if self.cfg.verify_offset_file and not self.cfg.resnapshot:
        self._pending_offset_key_blob, self._pending_offset_blob = (
            offsets.capture_offset_file(self.offset_path, new_point)
        )
    self._reset_group()
    return CommitResult.COMMITTED


__all__ = ["OWNER", "commit_group"]
