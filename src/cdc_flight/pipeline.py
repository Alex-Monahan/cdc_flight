"""cdc_flight: Postgres -> Debezium embedded engine -> the transactional applier
-> DuckDB / MotherDuck.

    cdc-flight --destination duckdb --max-seconds 60

The engine runs on a background thread; the main thread supervises it and closes
it once the change stream has been quiet for `--idle-seconds` (and the *source*
agrees the connector is idle), or `--max-seconds` elapses.

**The dlt load path is gone** (ADR 0001 D1/D10). `dlt.pipeline.run()` cannot host
the resume point in the destination transaction, cannot span tables in one
transaction, and opens one transaction per table inside every load package
(`repos/dlt/dlt/destinations/insert_job_client.py:24`,
`repos/dlt/dlt/load/load.py:637-647`), so rubric 1.1/1.2/1.3 are unreachable
through it. dlt survives as a *library*: `cdc_flight.naming` calls its
`snake_case` normaliser so destination identifiers stay byte-identical across
this migration.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path

# Runtime compatibility, not a test workaround. This must run before any project import
# can load PyArrow: 25.0.0's mimalloc backend has reproducibly crashed while an Arrow
# table was built on Debezium's JPype callback thread. Operators may explicitly select a
# different proven-safe pool; the production default is the Arrow system allocator.
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

from . import acquisition, naming
from . import catalog as catalog_mod
from . import catalog_baseline as baseline_mod
from . import destination as dest_mod
from . import faults as faults_mod
from . import reconcile as reconcile_mod
from . import recovery as recovery_mod
from . import resnapshot as resnapshot_mod
from . import resnapshot_batches as rbs
from . import resnapshot_recovery as resnapshot_recovery_mod
from . import standby as standby_mod
from .backfill import BackfillCoordinator
from .completion_stage import PostEngineCompletion
from .config import (
    ApplierConfig,
    CatalogConfig,
    DestinationConfig,
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    applier_settings,
    finite_run_lease_ttl,
)
from .debezium_props import assert_no_internal_topic_collision, build_properties
from .destination import (
    EpisodeReceipt,
    Lease,
    OccurrenceKey,
    RunState,
)
from .destination_fence import EpochFencedConnection
from .errors import (
    AlertPersistenceFailure,
    EngineFailure,
    LeaseLost,
    OffsetUnusable,
    SlotAheadOfDestination,
)
from .faults import validate_env as validate_fault_env
from .machines import (
    PHASE_RECONCILING,
    PHASE_RECOVERING,
    PHASE_SNAPSHOTTING,
    PHASE_STOPPING,
)
from .naming import control_table
from .ownership import DestinationOwnership
from .run_state import RunOutcome, RunPhaseWriter
from .snapshot_completion import SnapshotCompletion
from .supervisor import run_engine_bounded  # noqa: F401 - compatibility re-export

log = logging.getLogger("cdc_flight.pipeline")
OWNER = "pipeline-orchestration"

# --------------------------------------------------------------------------- #
# entrypoint
# --------------------------------------------------------------------------- #
def run(
    *,
    destination: str | None = None,
    max_seconds: float | None = None,
    idle_seconds: float | None = None,
    min_records: int | None = None,
    snapshot_mode: str | None = None,
    reset_state: bool = False,
    accept_orphan_offsets: bool = False,
    service_context=None,
) -> dict:
    """Run one Flight invocation.

    This API is process-terminal after a failed callback-quiescence proof. In that one
    state it writes the failure summary and hard-exits instead of returning or raising
    to an in-process caller, because the callback owns resources which must not outlive
    the lease and overlap another invocation in the same interpreter.
    """
    service_mode = service_context is not None
    service_policy = service_context.policy if service_mode else None
    if service_context is not None:
        service_context.assert_writable()
        # Admission/reconciliation/engine construction is one bounded startup
        # operation.  MotherDuck and source catalog work can legitimately take
        # longer than the ordinary idle-progress clock on a contended Flight;
        # the service watchdog must use its separate operation budget until the
        # Debezium thread is actually running.
        service_context.operation_started()

    # Parse CDC_FAULT_INJECT once, here, so a typo fails the run instead of
    # leaving a fault test vacuously green (Codex 9).
    fault_spec = validate_fault_env()
    if fault_spec:
        log.warning("fault injection armed: point=%s group=%s action=%s", *fault_spec)

    source = SourceConfig()
    replication = ReplicationConfig()
    dest = DestinationConfig(**({"kind": destination} if destination else {}))
    control_schema = dest.control_schema
    run_values = {
        k: v
        for k, v in {
            "max_seconds": max_seconds,
            "idle_seconds": idle_seconds,
            "min_records": min_records,
        }.items()
        if v is not None
    }
    if service_mode:
        # Service lifetime is unbounded.  Keep the finite adapter's defaults
        # intact, but give the service a commit/close budget that fits inside
        # its lease unless the operator explicitly supplied a different value.
        run_values["max_seconds"] = float("inf")
        if "CDC_COMMIT_TIMEOUT" not in os.environ:
            run_values["commit_timeout"] = min(
                service_policy.commit_timeout_seconds,
                service_policy.lease_ttl_seconds / 2,
            )
        if "CDC_CLOSE_TIMEOUT" not in os.environ:
            run_values["close_timeout"] = min(
                service_policy.close_timeout_seconds,
                service_policy.lease_ttl_seconds / 2,
            )
    run_cfg = RunConfig(**run_values)

    replication.state_dir.mkdir(parents=True, exist_ok=True)
    settings = applier_settings()
    # `skipped.operations` decides whether a TRUNCATE is decoded at all. The pipeline
    # always retains it so the generation fence can distinguish a physical rewrite
    # from a replacement; the destination truncate policy is applied later.
    props = build_properties(
        source,
        replication,
        snapshot_mode=snapshot_mode,
        truncate_mode=settings["truncate_mode"],
        jdbc_socket_timeout_seconds=run_cfg.jdbc_socket_timeout_seconds,
        jdbc_connect_timeout_seconds=run_cfg.jdbc_connect_timeout_seconds,
    )
    # A captured table whose topic collides with `<prefix>.transaction` would be
    # decoded as transaction metadata and never applied. Not reachable with the
    # pinned topic-naming strategy, and asserted rather than reasoned about
    # (Opus MINOR-6).
    assert_no_internal_topic_collision(replication.topic_prefix, source.tables)
    signal_data_collection = props.get("signal.data.collection")

    def is_signal_relation(qualified: str) -> bool:
        return bool(signal_data_collection and qualified == signal_data_collection)

    snapshot_capture_names = tuple(
        table for table in source.tables if not is_signal_relation(table)
    )
    namespace = props["name"]
    run_state = (
        RunState.service(
            dest.pipeline_name,
            service_id=service_context.service_id,
            worker_generation=service_context.worker_generation,
            lease_epoch=service_context.fencing_epoch,
        )
        if service_context is not None
        else RunState.new(dest.pipeline_name)
    )
    runner_id = run_state.runner_id

    log.info(
        "source=%s:%s/%s tables=%s slot=%s snapshot=%s destination=%s",
        source.host, source.port, source.dbname, source.tables,
        replication.slot_name, props["snapshot.mode"], dest.kind,
    )

    # rubric 1.7: a `destination_*` fault is injected by wrapping the one connection
    # the applier writes through, rather than by scattering anchors through the SQL
    # builders. `AlertSink`'s independent `cursor()` is delegated untouched on
    # purpose - see `faults.FaultyConnection`.
    con = None
    try:
        # A service bootstrap must not create a MotherDuck database before it owns
        # an epoch.  The admission step has already proved the physical destination
        # exists; the pipeline attaches the epoch before exposing a writable handle.
        con = dest_mod.connect(dest, create_database=not service_mode)
    except BaseException as exc:
        # DuckDB's file lock is acquired by `connect()` before the pipeline has a
        # destination handle. Keep this failure on a durable local sidecar so a
        # second pipeline cannot disappear with only a non-zero exit code.
        failure_summary = {"stop_reason": "destination_unavailable"}
        _record_run_failure_alert(
            None,
            dest=dest,
            run_state=run_state,
            exc=exc,
            summary=failure_summary,
        )
        with contextlib.suppress(Exception):
            exc.summary = failure_summary
        raise
    summary_extra: dict = {}
    lease: Lease | None = None
    lease_held = False
    phases: RunPhaseWriter | None = None
    initial_recovery_phase = recovery_mod.PHASE_ABSENT
    initial_interruption_marker = "absent"
    if faults_mod.matrix_armed():
        # The capability-armed matrix is allowed to inspect durable state before the
        # first lifecycle cut.  Production runs do not execute these probes.
        try:
            row = con.execute(
                f"SELECT phase FROM {control_table(control_schema, 'recovery_state')} "
                "WHERE pipeline = ? AND namespace = ?",
                [dest.pipeline_name, namespace],
            ).fetchone()
        except Exception:
            row = None
        if row:
            initial_recovery_phase = str(row[0])
        initial_interruption_marker = resnapshot_recovery_mod.interruption_marker_state(
            replication.state_dir / "resnapshot"
        )
    ownership = DestinationOwnership(
        recovery_phase=initial_recovery_phase,
        interruption_marker=initial_interruption_marker,
    )
    run_ok = False
    #: The run's ONE outcome, shared by the supervisor, the terminal `RUN_PHASE`
    #: transition and the returned summary (Codex r1 MAJOR-2). `RunOutcome` refuses a
    #: downgrade, so a later, less severe reason cannot overwrite an earlier diagnosis.
    outcome = RunOutcome()
    #: The dict `main()` will print and persist. Held so the outer `finally` can update
    #: it AFTER the terminal phase transitions, rather than shipping a summary sampled
    #: while the run was still `draining`.
    reported: dict | None = None
    try:
        if service_context is not None:
            destination_lease_key = dest.resolve_physical_lease_key(con)
            lease = service_context.lease or Lease(
                destination_lease_key,
                owner_id=service_context.service_id,
                ttl_seconds=service_policy.lease_ttl_seconds,
                control_schema=control_schema,
                label=dest.pipeline_name,
                lease_id=service_context.lease_id,
                fencing_epoch=service_context.fencing_epoch,
                service_id=service_context.service_id,
                worker_generation=service_context.worker_generation,
            )
            lease.attach(con)
            con = EpochFencedConnection(con, lease, service_context)
            con = faults_mod.wrap_destination(
                con, control_schema=control_schema
            )
            service_context.bind(lease, con)
            lease_held = True
        else:
            con = faults_mod.wrap_destination(
                con, control_schema=control_schema
            )
        dest_mod.ensure_control_schema(con, control_schema)
        dest_mod.ensure_dataset(con, dest.dataset_name)
        summary_extra["source_role"] = source.role
        if source.role == "standby":
            # This is a read-only capability gate.  Stock Debezium remains the
            # decoder, and all source writes (publication admission and the
            # transactional marker) continue to use ``CDC_PRIMARY_DSN`` through
            # the existing catalog/health routes.  A standby without these facts
            # is snapshot-only; do not construct a connector that would appear
            # healthy while its local logical slot is absent or invalid.
            standby = standby_mod.inspect(
                source.dsn,
                replication.slot_name,
                primary_dsn=source.primary_dsn,
                physical_slot_name=source.physical_slot_name,
                connect_timeout=run_cfg.jdbc_connect_timeout_seconds,
                statement_timeout_ms=run_cfg.jdbc_socket_timeout_seconds * 1000,
            )
            summary_extra["standby_capability"] = standby.as_dict()
            log.info(
                "validated recovery-mode standby source: system=%s timeline=%s "
                "local_slot=%s physical_slot=%s",
                standby.system_identifier,
                standby.timeline_id,
                standby.local_slot_name,
                standby.receiver_slot_name,
            )
        summary_extra["fallback_alerts_replayed"] = dest_mod.replay_fallback_alerts(
            con, dest
        )
        # MotherDuck's database and schema comparison rules are resolved by the live
        # catalog, not guessed from configuration spelling. Local DuckDB uses the
        # opened file's device/inode plus the same server-resolved schema names.
        destination_lease_key = dest.resolve_physical_lease_key(con)
        # rubric 1.9 / ADR §4.8: one `_cdc_flight.heartbeat` row per run, moved through
        # the `RUN_PHASE` machine on its OWN connection. "Where is this run" stops being
        # a source-line position in a 470-line function and becomes a query. The
        # periodic liveness/lag writer is still 4.4/6.1's.
        phases = RunPhaseWriter(
            con,
            pipeline=dest.pipeline_name,
            runner_id=runner_id,
            outcome=outcome,
            control_schema=control_schema,
        )

        applier_cfg = ApplierConfig(
            max_batch_size=int(props["max.batch.size"]),
            commit_timeout=run_cfg.commit_timeout,
            **settings,
        )

        # rubric 1.8 — checked on EVERY slot acquisition, before anything reads the
        # resume point, because the recovery *deletes* the resume point. Doing this
        # after reconciliation would mean reconciling against a position we are about to
        # declare unusable.
        #
        # The lease is taken first: this path mutates destination state (marks tables,
        # deletes the resume point, drops the slot), and a second runner doing that
        # concurrently is exactly what rubric 4.2 exists to prevent.
        if service_context is not None:
            if service_context.lease_key != destination_lease_key:
                raise LeaseLost(
                    "the service was admitted for a different physical destination"
                )
            # Admission happened before the engine was constructed.  The same
            # process now attaches its one data connection to that exact epoch;
            # it never reacquires or starts a second process generation.
            if lease is None:
                raise LeaseLost("service pipeline has no admitted lease")
            reclaimed = getattr(lease, "reclaimed_receipt", None)
            if reclaimed is not None:
                # The predecessor may have been SIGKILLed or hard-exited by its
                # stall watchdog, so it could not safely write an alert itself.
                # Emit the successor-owned, once-per-old-lease recovery alert
                # before any engine work begins. A clean release never carries a
                # reclaimed receipt and therefore remains silent.
                reclaim_summary = {
                    "stop_reason": "service_holder_reclaimed",
                    "reclaimed_service_holder": True,
                }
                reclaim_failure = LeaseLost(
                    "the previous service holder expired before this Flight took over",
                    lease_state=reclaimed,
                )
                _record_run_failure_alert(
                    con,
                    dest=dest,
                    run_state=run_state,
                    exc=reclaim_failure,
                    summary=reclaim_summary,
                )
                summary_extra.update(
                    {
                        "reclaimed_service_holder": True,
                        "recovery_alert_code": "service_holder_reclaimed",
                    }
                )
                lease.reclaimed_receipt = None
        else:
            lease = Lease(
                destination_lease_key,
                owner_id=runner_id,
                ttl_seconds=finite_run_lease_ttl(run_cfg),
                control_schema=control_schema,
                label=dest.pipeline_name,
            )
            lease.acquire(con)
            lease_held = True

        # rubric 4.7: a throwaway `_rs` slot left behind by an interrupted re-snapshot
        # holds WAL on the source for ever and counts against `max_replication_slots`.
        # Swept unconditionally, by the one name this pipeline derives from its own slot
        # (Opus MAJOR-2, observed leaking twice on the shared cluster in one day).
        summary_extra["stale_resnapshot_slot"] = resnapshot_mod.sweep_stale_slot(
            source.dsn, replication.slot_name
        )

        captured_tables = acquisition.captured_tables(
            con,
            dest.pipeline_name,
            source,
            replication,
            control_schema=control_schema,
        )
        captured_tables = [
            table for table in captured_tables
            if not is_signal_relation(f"{table[0]}.{table[1]}")
        ]
        if reset_state:
            # Journalled BEFORE the first destructive step, and before the generic
            # resume below picks it up (Codex r1 MAJOR-4).
            summary_extra["reset_state"] = acquisition.journal_the_reset(
                con, source=source, replication=replication, dest=dest,
                namespace=namespace, captured=captured_tables, phases=phases,
            )

        # A recovery an earlier process did not finish is resumed BEFORE the slot check
        # looks at anything: its intermediate state is, by construction, indistinguish-
        # able from a fresh problem, and the Flight used to diagnose its own half-done
        # work as an operator error and refuse for ever (Codex B3 / Opus MAJOR-1).
        journal, resumed = acquisition.resume_any_journalled_recovery(
            con, source=source, replication=replication, dest=dest, namespace=namespace,
            phases=phases,
        )
        if resumed is not None:
            summary_extra["recovery_resumed"] = resumed
        if journal is not None:
            faults_mod.runtime_state(recovery_phase=journal.phase)

        phases.ensure(PHASE_RECONCILING)
        verdict, recovery = acquisition.check_the_slot(
            con,
            source=source,
            replication=replication,
            dest=dest,
            namespace=namespace,
            captured=captured_tables,
            # Deliberately independent of `--accept-orphan-offsets`: whether the file is
            # trusted, refused or deleted is reconciliation's decision, and it is the one
            # place that knows the difference. The slot check only has to stay out of it.
            orphan_file=(
                replication.offset_file.exists()
                and replication.offset_file.stat().st_size > 0
            ),
        )
        summary_extra["slot_check"] = verdict.as_dict()
        if recovery is not None:
            phases.to(PHASE_RECOVERING, detail=verdict.decision)
            summary_extra["slot_recovery"] = recovery
            journal = recovery_mod.read(
                con,
                pipeline=dest.pipeline_name,
                namespace=namespace,
                control_schema=control_schema,
            )
        if journal is not None:
            # A recovery has deleted the resume point, so the run has to snapshot data
            # whatever `snapshot.mode` said. `no_data` plus "every table is owed a
            # snapshot" is a run that streams onto tables it knows are wrong.
            #
            # Read from the JOURNAL, not from a local variable: the intent has to
            # outlive the process that formed it. A crash after the slot was dropped
            # used to leave no row, no file and no slot, which the next run called an
            # ordinary fresh start - and a fresh start under a configured `no_data` mode
            # streams onto tables that were never rebuilt (Codex B3).
            if props["snapshot.mode"] not in reconcile_mod.SNAPSHOT_MODES_WITH_DATA:
                log.warning(
                    "snapshot.mode=%s does not read table data; using %r for this "
                    "recovery run (journal %s)",
                    props["snapshot.mode"], journal.snapshot_mode, journal.recovery_id,
                )
                props["snapshot.mode"] = (
                    journal.snapshot_mode or recovery_mod.FORCED_SNAPSHOT_MODE
                )
            summary_extra["recovery_journal"] = journal.as_dict()
            faults_mod.runtime_state(recovery_phase=journal.phase)

        phases.ensure(PHASE_RECONCILING)
        reconciliation = reconcile_mod.reconcile(
            con,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            offset_path=replication.offset_file,
            accept_orphan=accept_orphan_offsets,
            repair=applier_cfg.repair_offset_file,
            dsn=source.dsn,
            slot_name=replication.slot_name,
        )
        summary_extra["reconciliation"] = reconciliation.decision
        summary_extra["reconciliation_detail"] = reconciliation.message
        log.info("start-up reconciliation: %s (%s)", reconciliation.decision, reconciliation.message)

        # rubric 4.7 / Codex r1 BLOCKER-1: an operator who passed
        # `--accept-orphan-offsets` has authorised a rebuild, and the rebuild is now a
        # journalled recovery like every other one — **journal first, destroy second**.
        #
        # It used to be the other way round. `offsets` dropped the slot and
        # unlinked the file and only then did this block record why, which put a crash
        # window between destroying the evidence and writing the obligation: a hard exit
        # there left no row, no file, no slot and no journal, the next run called that an
        # ordinary `fresh_start`, and a configured non-data `snapshot.mode` streamed onto
        # a destination nobody had rebuilt. `reconcile()` now classifies and nothing
        # more; `begin()` makes the intent and the table obligation durable together;
        # `resume()` performs the file / row / slot ladder, idempotently, from whatever
        # phase survives.
        if reconciliation.decision == "orphan_accepted_resnapshot" and journal is None:
            previous_slot = dest_mod.read_slot_state(
                con,
                dest.pipeline_name,
                replication.slot_name,
                control_schema=control_schema,
            )
            slot_receipt = dest_mod.write_slot_state(
                con,
                pipeline=dest.pipeline_name,
                slot_name=replication.slot_name,
                observation=previous_slot.as_dict() if previous_slot is not None else {},
                verdict="no_durable_destination_row",
                verdict_message="an operator accepted the orphan offsets resnapshot",
                control_schema=control_schema,
            )
            journal = recovery_mod.begin(
                con,
                pipeline=dest.pipeline_name,
                namespace=namespace,
                decision=recovery_mod.ORPHAN_DECISION,
                message=(
                    "an operator passed --accept-orphan-offsets: the untrusted "
                    "offsets file and the unaccounted slot are to be removed and every "
                    "captured table is owed a fresh image"
                ),
                slot_name=replication.slot_name,
                offset_path=replication.offset_file,
                captured_tables=captured_tables,
                forget_catalog=False,
                slot_receipt=slot_receipt,
                context={"file_lsn": reconciliation.file_lsn},
                control_schema=control_schema,
            )
            summary_extra["recovery_journal"] = journal.as_dict()
            summary_extra["orphan_recovery"] = recovery_mod.resume(
                con,
                pipeline=dest.pipeline_name,
                namespace=namespace,
                record=journal,
                dsn=source.dsn,
                control_schema=control_schema,
            )
        if (
            journal is not None
            and props["snapshot.mode"] not in reconcile_mod.SNAPSHOT_MODES_WITH_DATA
        ):
            log.warning(
                "a rebuild is owed (%s) but snapshot.mode=%s does not read table data; "
                "using %r", journal.decision, props["snapshot.mode"],
                journal.snapshot_mode or recovery_mod.FORCED_SNAPSHOT_MODE,
            )
            props["snapshot.mode"] = (
                journal.snapshot_mode or recovery_mod.FORCED_SNAPSHOT_MODE
            )

        # ADR §4.7 - the Invariant-O guard, at start-up. `snapshot_mode` is what
        # decides the "slot exists / no durable destination row" cell (Codex 3).
        summary_extra["invariant_o_start"] = reconcile_mod.check_invariant_o(
            con, pipeline=dest.pipeline_name, namespace=namespace,
            dsn=source.dsn, slot_name=replication.slot_name,
            snapshot_mode=props["snapshot.mode"],
            control_schema=control_schema,
        )

        # A hard exit can land after the final recovery snapshot commit but before
        # PostEngineCompletion gets to clear the journal.  At that point the durable
        # table lifecycle and resume row are already the positive terminal evidence
        # that `complete_if_ready()` requires.  Discharge that evidence before
        # constructing the main engine: Debezium quite correctly emits `SKIPPED` when
        # its offsets file already names the committed hand-off, and a still-required
        # snapshot callback machine would misclassify that valid restart as a protocol
        # error.  This path is reachable only with a journal; ordinary runs do no extra
        # recovery work.
        if journal is not None:
            completion = recovery_mod.complete_if_ready(
                con,
                pipeline=dest.pipeline_name,
                namespace=namespace,
                record=journal,
                control_schema=control_schema,
            )
            if completion.cleared:
                summary_extra["recovery_cleared_before_engine"] = completion.recovery_id
                journal = None

        # ADR §14.1's open question, answered by measurement rather than by guess.
        # AFTER the lease: the probe DROPs and CREATEs shared
        # `_cdc_flight.__ddl_probe_*` tables, so a runner that is about to be
        # rejected by the lease could otherwise drop the incumbent's probe tables
        # mid-probe and make the incumbent conclude `transactional_ddl=False`
        # (Opus MINOR-7).
        transactional_ddl = dest_mod.probe_transactional_ddl(
            con, control_schema=control_schema
        )
        summary_extra["transactional_ddl"] = transactional_ddl

        # rubric 1.6 — the blocking re-snapshot phase, BEFORE the main stream starts.
        #
        # Tables reach this queue three ways: a source relation dropped and recreated
        # (1.5), rubric 1.8's slot-mismatch recovery, and an operator asking. It runs
        # here and not later because the whole hand-over argument is "the main stream has
        # not consumed anything yet, so there is no in-flight event to buffer": see
        # `cdc_flight.resnapshot`. A run whose own `snapshot.mode` is about to re-read
        # every table needs none of this - the fresh snapshot IS the re-snapshot.
        # A table left `in_progress` by a process that died inside its snapshot is
        # durable, non-terminal, and was selected by no durable queue - so the recovery
        # journal's "nothing owes a snapshot any more" test could pass over it and the
        # run could log that every captured table had a fresh image while one sat half
        # built (architecture review, finding 1). Nothing is mid-snapshot at start-up by
        # definition, so `in_progress` here means exactly that, and promoting it is what
        # makes it discoverable from durable state after ANY crash - including the ones
        # `except BaseException` never sees.
        # rubric 1.9 / SM-E, and it has to happen HERE — before the owed queue is read,
        # because marking a relation owed is how an unrelatable one gets rebuilt in THIS
        # run rather than in the next.
        #
        # `mark_unconfirmed` reads the durable baseline, computes (from durable state
        # alone) which relations hold rows this pipeline cannot relate to any identity
        # at the source, writes the obligation down, and marks those relations
        # `awaiting_snapshot`. The write comes first and is unconditional, so a crash, a
        # kill, an unreadable catalog and a clean refusal all leave the same statement:
        # `successful_polls` is process memory and could only ever describe the process
        # that was already dead (Codex r5 BLOCKER-1).
        # It runs for EVERY run, including one with no watcher at all. A run under
        # `CDC_DROP_MODE=ignore` plainly did not read the catalog, so it cannot claim
        # the registry still describes the source — and that mode is exactly how a
        # destination comes to hold rows with no registry in the first place (it is how
        # the round-5 reviewer built the precondition). It marks, it does not act:
        # `reconcile=False`, because a run with no watcher cannot confirm what it would
        # rebuild, and rebuilding every run would re-snapshot the world for ever.
        catalog_cfg = CatalogConfig()
        catalog_enabled = applier_cfg.drop_mode != "ignore" and catalog_cfg.poll_seconds > 0
        if source.auto_discovery and not catalog_enabled:
            # Publication-driven discovery requires a live catalog watcher to narrow
            # the initial snapshot and to hand new relations through admission. When
            # catalog polling is explicitly disabled (for example DROP_MODE=ignore),
            # keep the configured capture bounded instead of letting Debezium snapshot
            # every table in the publication while completion still expects CDC_TABLES.
            props["schema.include.list"] = source.schema
            configured_tables = [
                table for table in source.tables if not is_signal_relation(table)
            ]
            if signal_data_collection and signal_data_collection not in configured_tables:
                configured_tables.append(signal_data_collection)
            props["table.include.list"] = ",".join(configured_tables)
        baseline = baseline_mod.mark_unconfirmed(
            con, pipeline=dest.pipeline_name, dataset=dest.dataset_name,
            runner_id=runner_id, reconcile=catalog_enabled,
            control_schema=control_schema,
        )
        summary_extra.update(baseline.as_dict())

        # Discovery is observed before the owed queue is read.  A newly visible
        # relation is immediately routed through the existing durable table lifecycle;
        # its single-table re-snapshot then runs before the main slot starts consuming
        # it.  This is the same hand-over used for recreated relations, so pre-existing
        # rows are never mistaken for a stream tail.
        watcher = None
        discovered = ()
        descriptor_provider = None
        if catalog_enabled:
            watcher = catalog_mod.CatalogWatcher(
                dsn=source.dsn,
                primary_dsn=source.primary_dsn,
                publication=replication.publication_name,
                schema=source.schema,
                schemas=source.schemas,
                all_schemas=source.auto_discovery and source.schemas is None,
                auto_discover=source.auto_discovery,
                publication_ownership=source.publication_ownership,
                include={t if "." in t else f"{source.schema}.{t}" for t in source.tables},
                known=catalog_mod.read_known_relations(
                    con, dest.pipeline_name, control_schema=control_schema
                ),
                replicated=catalog_mod.seed_from_table_state(
                    con, dest.pipeline_name, control_schema=control_schema
                ),
                gone=catalog_mod.gone_from_table_state(con, dest.pipeline_name, control_schema=control_schema),
                unrelatable=set(baseline.unmarked),
                poll_seconds=catalog_cfg.poll_seconds,
                emit_marker=catalog_cfg.emit_marker,
                marker_prefix=catalog_cfg.marker_prefix,
                grace_seconds=catalog_cfg.grace_seconds,
                confirm_polls=catalog_cfg.confirm_polls,
                marker_max_writes=catalog_cfg.marker_max_writes or None,
                binary_handling_mode=props.get("binary.handling.mode", "base64"),
                hstore_handling_mode=props.get("hstore.handling.mode", "map"),
            ).start()
            catalog_refusals = watcher.schema_refusals()
            for refused in catalog_refusals:
                source_tables = refused.source_tables or (
                    ((refused.source_schema, refused.source_table, refused.target),)
                    if refused.source_schema and refused.source_table
                    else ()
                )
                for source_schema, source_table, target_table in source_tables:
                    dest_mod.record_schema_refusal(
                        con, pipeline=dest.pipeline_name, source_schema=source_schema,
                        source_table=source_table, target_table=target_table,
                        detected_lsn=refused.detected_lsn, reason=str(refused),
                        input_fingerprint=refused.input_fingerprint,
                        source_fingerprint=refused.source_fingerprint,
                    )
            if catalog_refusals:
                summary_extra["catalog_schema_refusals"] = [
                    str(refused) for refused in catalog_refusals
                ]
            discovered = watcher.new_relations(
                exclude={signal_data_collection} if signal_data_collection else None
            )
            if discovered:
                dest_mod.request_snapshot(
                    con,
                    pipeline=dest.pipeline_name,
                    tables=[
                        (
                            relation.schema,
                            relation.table,
                            naming.destination_table(
                                replication.topic_prefix, relation.schema, relation.table
                            ),
                        )
                        for relation in discovered
                    ],
                    detail="a new source relation was discovered by the catalog watcher",
                    control_schema=control_schema,
                )
                summary_extra["discovered_relations"] = [
                    relation.qualified for relation in discovered
                ]
            observed = tuple(
                relation
                for relation in watcher.captured_relations()
                if not is_signal_relation(relation.qualified)
            )
            if observed and source.auto_discovery:
                # Debezium is publication-driven in discovery mode. Snapshot completion
                # therefore expects the relations observed at the run's start, including
                # a relation that was not in CDC_TABLES.
                observed_names = sorted(
                    relation.qualified for relation in observed if not relation.is_partition
                )
                if signal_data_collection and signal_data_collection not in observed_names:
                    observed_names.append(signal_data_collection)
                props["table.include.list"] = ",".join(observed_names)
                captured_tables = [
                    (
                        relation.schema,
                        relation.table,
                        naming.destination_table(
                            replication.topic_prefix, relation.schema, relation.table
                        ),
                    )
                    for relation in observed
                    if not relation.is_partition
                ]
                snapshot_capture_names = tuple(
                    name for name in watcher.snapshot_names()
                    if not is_signal_relation(name)
                )
            if catalog_cfg.grace_seconds:
                log.warning(
                    "CDC_CATALOG_GRACE=%.0fs: a destructive catalog action will be "
                    "applied after that long even though the destination has not "
                    "reached the LSN at which it was detected. In-flight events for "
                    "the table can then re-create it as a zombie holding pre-drop "
                    "rows, so this mode is EXPLICITLY EXCLUDED from the structural "
                    "correctness guarantee (ADR 0001 §18/A38).",
                    catalog_cfg.grace_seconds,
                )
        else:
            # The live watcher is intentionally disabled for explicit no-catalog
            # modes, but the typed path must never infer source types from the
            # Connect envelope.  Resolve the configured relation/type trees once
            # from PostgreSQL and retain only the immutable descriptor map for this
            # run.  A missing or failed read propagates as a startup refusal; there
            # is no legacy fallback here.
            from .catalog_descriptors import provider_for_source

            try:
                descriptor_provider = provider_for_source(source)
            except ValueError as exc:
                raise EngineFailure(str(exc), dict(summary_extra)) from exc

        interrupted_resnapshot = resnapshot_recovery_mod.requeue_interrupted(
            con,
            pipeline=dest.pipeline_name,
            state_dir=replication.state_dir / "resnapshot",
            control_schema=control_schema,
        )
        if interrupted_resnapshot:
            summary_extra["interrupted_resnapshot_requeued"] = interrupted_resnapshot
        interrupted = dest_mod.promote_interrupted_snapshots(
            con, dest.pipeline_name, control_schema=control_schema
        )
        if interrupted:
            summary_extra["interrupted_snapshots_requeued"] = interrupted
        owed = dest_mod.tables_awaiting_snapshot(
            con, dest.pipeline_name, control_schema=control_schema
        )
        will_snapshot_everything = (
            props["snapshot.mode"] == "always"
            or (
                reconciliation.resume_point.last_lsn == 0
                and props["snapshot.mode"] in reconcile_mod.SNAPSHOT_MODES_WITH_DATA
            )
        )
        if (
            owed
            and not will_snapshot_everything
            and acquisition.resnapshot_enabled()
            and reconciliation.resume_point.last_lsn == 0
        ):
            # A47/A53: the throwaway re-snapshot is only safe because the MAIN slot is
            # retaining WAL continuously from the durable resume point throughout — the
            # image at `C` hands over to a stream that never stopped. With no durable
            # resume point there is no such guarantee: the main slot may not exist yet,
            # and a transaction committing between the throwaway slot's lifetime and the
            # main slot's creation would be retained by neither (Codex B3, "retain at
            # least one slot continuously").
            #
            # Reachable with `--snapshot-mode no_data` (or any non-data mode) on a fresh
            # state directory that still owes tables. An armed recovery cannot reach it,
            # because the journal forces a data-reading mode precisely so that the main
            # engine's own coordinated snapshot IS the rebuild.
            raise EngineFailure(
                f"{len(owed)} table(s) owe a snapshot and there is no durable resume "
                f"point, but snapshot.mode={props['snapshot.mode']!r} does not read "
                "table data. A throwaway re-snapshot here would leave no slot retaining "
                "WAL between the image and the main stream, so a transaction committing "
                "in between would be in neither. Use a snapshot mode that backfills."
                + (
                    f" (recovery {journal.recovery_id} is armed)"
                    if journal is not None else ""
                ),
                dict(summary_extra),
            )
        if owed and not will_snapshot_everything and acquisition.resnapshot_enabled():
            phases.to(PHASE_SNAPSHOTTING, detail=f"{len(owed)} table(s) owed")
            resnapshot_passes, latest_resnapshot, snapshot_epoch = (
                rbs.run_owed(
                    con,
                    source=source,
                    replication=replication,
                    pipeline=dest.pipeline_name,
                    dataset=dest.dataset_name,
                    owed=owed,
                    settings=settings,
                    run_cfg=run_cfg,
                    lease=lease,
                    runner_id=runner_id,
                    transactional_ddl=transactional_ddl,
                    epoch_base=reconciliation.resume_point.snapshot_epoch,
                    namespace=namespace,
                    ownership=ownership,
                    new_relations={relation.qualified for relation in discovered},
                    drop_mode=applier_cfg.drop_mode,
                    control_schema=control_schema, catalog=watcher,
                    resnapshot_run=resnapshot_mod.run,
                )
            )
            summary_extra.update(latest_resnapshot)
            summary_extra.update(rbs.summarize_passes(resnapshot_passes))
            reconciliation.resume_point.snapshot_epoch = max(
                reconciliation.resume_point.snapshot_epoch, snapshot_epoch
            )
            if watcher is not None and discovered:
                watcher.complete_discoveries(
                    {relation.qualified for relation in discovered}
                )
        elif owed:
            log.warning(
                "%s table(s) are marked awaiting_snapshot and are NOT being "
                "re-snapshotted on this run (%s): %s",
                len(owed),
                "the run snapshots everything anyway" if will_snapshot_everything
                else "CDC_RESNAPSHOT=0",
                ", ".join(f"{s}.{t}" for s, t, _ in owed),
            )
            unhandled = [f"{s}.{t}" for s, t, _ in owed]
            summary_extra["tables_awaiting_snapshot_unhandled"] = unhandled
            # Asked of DURABLE STATE (`include_owed=True`), not of what this run
            # remembers marking: the run that discovers a relation refuses, and so does
            # every later one, until something actually rebuilds it. Keyed on
            # `baseline.unreconciled` the guarantee would last exactly one run.
            # An owing lifecycle is itself enough to block a successful run. The
            # physical target may be empty or already absent after quarantine; row
            # presence is not a trust signal.
            skipped_baseline = list(unhandled)
            if skipped_baseline and not will_snapshot_everything:
                # A QUEUED REBUILD IS NOT A FINISHED ONE (Codex r6 BLOCKER-2, reproduced).
                #
                # `CDC_RESNAPSHOT=0` is an explicit operator opt-out of automatic repair,
                # and for an ordinary owed table it means what it says: the data stays
                # stale, flagged and queryable. It cannot mean that here. These relations
                # are owed a rebuild *because this run could not relate the rows they
                # hold to any identity at the source*, so continuing would stream the
                # replacement relation's events onto the old relation's rows and — worse
                # — let the watcher adopt the replacement generation, after which nothing can
                # ever detect it again. Measured: source `[999]`, destination
                # `[1, 2, 999]`, lifecycle `awaiting_snapshot`, registry at the new oid,
                # baseline `valid`, exit 0.
                #
                # Raised HERE, before the engine starts and before anything is adopted,
                # which is what the opt-out's own contract promises: detect, alert, exit
                # non-zero, mutate nothing.
                raise EngineFailure(
                    f"{len(skipped_baseline)} relation(s) hold destination rows this "
                    "pipeline cannot relate to any identity at the source "
                    f"({', '.join(skipped_baseline)}), and automatic re-snapshot is "
                    "switched off (CDC_RESNAPSHOT=0), so nothing will rebuild them. "
                    "Continuing would let this run adopt the observed identity over rows "
                    "that may belong to a different relation. Re-enable CDC_RESNAPSHOT, "
                    "or rebuild those tables by hand",
                    dict(summary_extra),
                )

        # A RefreshScheduler full request is destination-durable work. Consume it
        # on this normal pipeline start through the existing blocking stock
        # resnapshot hand-off, before the main engine can stream onto the old image.
        # The run is prepared before the throwaway engine and completed by its atomic
        # swap callback, so a restart sees either the old active run or the fully
        # published image. This is deliberately startup consumption: a live main-
        # engine polling loop would create a second source owner.
        backfill = BackfillCoordinator(
            con,
            pipeline=dest.pipeline_name,
            control_schema=control_schema,
            topic_prefix=replication.topic_prefix,
        )
        scheduled_full = [
            run for run in backfill.active_runs()
            if run.effective_mode == "full"
            and run.state in {"requested", "preparing", "loading"}
        ]
        if scheduled_full and not will_snapshot_everything:
            prepared: list = []

            def prepare_full() -> None:
                for run in scheduled_full:
                    prepared.append(backfill.prepare(run, in_transaction=True))

            backfill.repository.transaction(prepare_full)
            full_tables = [
                (run.source_schema, run.source_table, run.target_table)
                for run in prepared
            ]
            phases.to(PHASE_SNAPSHOTTING, detail="scheduled full refresh")
            scheduled_result = resnapshot_mod.run(
                con,
                source=source,
                replication=replication,
                pipeline=dest.pipeline_name,
                dataset=dest.dataset_name,
                tables=full_tables,
                settings=settings,
                run_cfg=run_cfg,
                lease=lease,
                runner_id=runner_id,
                transactional_ddl=transactional_ddl,
                epoch_base=reconciliation.resume_point.snapshot_epoch,
                reason="scheduled full refresh",
                namespace=namespace,
                ownership=ownership,
                new_relations=set(),
                drop_mode=applier_cfg.drop_mode,
                control_schema=control_schema,
                on_swap=lambda state, snapshot_lsn, commit_id: backfill.complete_full_swap(
                    state,
                    snapshot_lsn=snapshot_lsn,
                    commit_id=commit_id,
                    in_transaction=True,
                ),
            )
            summary_extra["scheduled_full_refresh"] = scheduled_result.as_dict()
            reconciliation.resume_point.snapshot_epoch = max(
                reconciliation.resume_point.snapshot_epoch,
                scheduled_result.snapshot_epoch,
            )

        # The refusal above is intentionally before the coordinator's
        # `phases.to(PHASE_STREAMING)` transition.

        # rubric 1.6: the per-table snapshot watermark, read AFTER any re-snapshot so it
        # carries the image the main stream now has to hand over from.
        watermarks = resnapshot_mod.read_watermarks(
            con, dest.pipeline_name, control_schema=control_schema
        )
        summary_extra["snapshot_watermarks"] = len(watermarks)

        # A journalled recovery forces the MAIN engine into a data-reading snapshot even
        # when the interrupted run already committed a durable first group. In that case
        # `will_snapshot_everything` is false by design (the resume point is non-zero),
        # but the recovery snapshot's callbacks are still required evidence. The
        # throwaway re-snapshot has its own required completion machine; an ordinary
        # streaming run remains `not_required` here.
        snapshot_completion_required = will_snapshot_everything or journal is not None
        snapshot_completion = SnapshotCompletion.for_capture(
            snapshot_completion_required,
            schema=source.schema,
            tables=snapshot_capture_names,
        )
        completion_stage = PostEngineCompletion(
            con=con,
            source_dsn=source.dsn,
            slot_name=replication.slot_name,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            dataset=dest.dataset_name,
            snapshot_mode=props["snapshot.mode"],
            destination=dest,
            runner_id=runner_id,
            watcher=watcher,
            journal=journal,
            baseline=baseline,
            snapshot_completion=snapshot_completion,
            outcome=outcome,
            base_summary=summary_extra,
            drop_mode=applier_cfg.drop_mode,
        )

        from .discovery_coordinator import LiveDiscoveryCoordinator

        # Construction invariant retained across the coordinator boundary:
        # `ownership.attach(applier)` must precede `engine.consumer`, so a
        # consumer-construction failure retires an idle owner rather than
        # exposing an unowned callback.

        catalog_flush_exclude = set(
            baseline_mod.unrebuilt_relations(
                con,
                pipeline=dest.pipeline_name,
                dataset=dest.dataset_name,
                control_schema=control_schema,
            )
        )
        # The signal relation is a stock Debezium control channel, not a source
        # data relation whose catalog identity belongs in destination history.
        # It remains in the connector capture list so source signalling works, but
        # its catalog observation must never create a destination ownership row.
        if signal_data_collection:
            catalog_flush_exclude.add(signal_data_collection)

        coordinator = LiveDiscoveryCoordinator(
            con=con,
            source=source,
            replication=replication,
            destination=dest,
            namespace=namespace,
            run_cfg=run_cfg,
            applier_cfg=applier_cfg,
            props=props,
            settings=settings,
            watcher=watcher,
            discovered=discovered,
            catalog_cfg=catalog_cfg,
            phases=phases,
            lease=lease,
            runner_id=runner_id,
            transactional_ddl=transactional_ddl,
            ownership=ownership,
            snapshot_completion=snapshot_completion,
            completion_stage=completion_stage,
            main_resume=reconciliation.resume_point,
            watermarks=watermarks,
            outcome=outcome,
            summary_extra=summary_extra,
            resnapshot_enabled=acquisition.resnapshot_enabled(),
            descriptor_provider=descriptor_provider,
            catalog_flush_exclude=catalog_flush_exclude,
            service_context=service_context,
        )
        try:
            reported = coordinator.run()
            run_ok = coordinator.run_ok
            return reported
        except EngineFailure as failure:
            outcome.record(failure.summary.get("stop_reason") or "engine_error")
            reported = failure.summary
            # EngineFailure is the normal fail-closed boundary for supervisor-owned
            # failures (source-dark, bounded timeout, incomplete watermark, and
            # connector shutdown).  It must take the same durable alert path as an
            # acquisition/lease exception; otherwise the process exits non-zero while
            # the operator sees only the local summary.
            _record_run_failure_alert(
                con,
                dest=dest,
                run_state=run_state,
                exc=failure,
                summary=reported,
            )
            raise
    except BaseException as exc:
        # Anything that unwound before (or around) the engine: a refusal, a lease loss,
        # a control-schema failure. It used to leave `terminal_reason=None`, so the
        # heartbeat's terminal row said nothing while `main()` recorded `error`
        # (Codex r1 MAJOR-2). Recorded on the SAME outcome, and only when nothing more
        # specific has been diagnosed - `error` is the most severe value in the
        # precedence and would otherwise bury `engine_error` or `source_dark`.
        if not outcome.failed:
            outcome.record("error")
        # ...and the run's ONE phase/outcome projection is attached to the escaping
        # exception, so `last_run.json` carries it too. Fixing the *normal* path left
        # this one behind: a lease refusal wrote heartbeat `failed/error` while
        # `last_run.json` carried no `run_phase` and no `run_outcome` at all, because
        # `main()` built its summary from the exception and `reported` was still None
        # (Codex r2 MAJOR-2). The outer `finally` below updates this very dict AFTER the
        # terminal transitions, and `main()` reads it after that, so the two agree.
        reported = reported if reported is not None else {}
        reported.update(dict(getattr(exc, "summary", {}) or {}))
        reported.update(summary_extra)
        reported.setdefault("stop_reason", outcome.value)
        if phases is not None and hasattr(phases, "record_log"):
            phases.record_log(
                level="CRITICAL",
                event="run_failure",
                message=(
                    f"run failed before success could be claimed: "
                    f"{type(exc).__name__}: {exc}"
                ),
                context={
                    "error_type": type(exc).__name__,
                    "stop_reason": reported.get("stop_reason"),
                },
                force=True,
            )
        _record_run_failure_alert(
            con,
            dest=dest,
            run_state=run_state,
            exc=exc,
            summary=reported,
        )
        with contextlib.suppress(Exception):  # a BaseException without a __dict__
            exc.summary = reported
        raise
    finally:
        try:
            _teardown_destination(
                con=con,
                ownership=ownership,
                reported=reported,
                phases=phases,
                lease=lease,
                lease_held=lease_held,
                run_ok=run_ok,
                service_mode=service_context is not None,
                service_context=service_context,
                hard_exit_on_transfer=True,
            )
        finally:
            if service_context is not None:
                service_context.close()


def _record_run_failure_alert(
    con,
    *,
    dest: DestinationConfig,
    run_state: RunState,
    exc: BaseException,
    summary: dict,
) -> None:
    """Project every pre-engine/run-boundary failure onto the durable alert surface.

    The supervisor owns connector-time failures, while this outer boundary owns
    acquisition, lease, offset, and teardown failures. Keeping this projection here
    means a failure before an Applier/AlertSink exists cannot silently become only a
    local summary record.
    """
    if con is None:
        code = "destination_unavailable"
        condition_key = f"{code}:{dest.pipeline_name}"
        incident_key = (
            f"{type(exc).__module__}.{type(exc).__qualname__}:"
            f"{hashlib.sha256(str(exc).encode()).hexdigest()}"
        )
        message = (
            f"cdc_flight could not open destination for pipeline {dest.pipeline_name!r}: "
            f"{type(exc).__name__}: {exc}. The durable fallback alert is at the sidecar "
            "path and will be replayed when the destination becomes available."
        )
        try:
            path = dest_mod.persist_fallback_alert(
                dest,
                run_state=run_state,
                severity="critical",
                code=code,
                message=message,
                condition_key=condition_key,
                incident_key=incident_key,
                context={
                    "error_type": type(exc).__name__,
                    "stop_reason": summary.get("stop_reason"),
                    "destination_unavailable": True,
                },
            )
            summary["fallback_alert_path"] = str(path)
            log.critical("destination unavailable; durable alert written to %s", path)
        except BaseException as alert_exc:
            # A failed fallback is itself a failed health signal. Do not return to the
            # caller and let the original exception look like an ordinary, diagnosed
            # failure: the replacement says plainly that alerting is broken and
            # retains the original exception in `original_failure`.
            log.critical(
                "destination unavailable and fallback alert could not be persisted",
                exc_info=True,
            )
            print(
                "CRITICAL: destination unavailable and fallback alert failed: "
                f"{type(alert_exc).__name__}: {alert_exc}",
                file=sys.stderr,
                flush=True,
            )
            raise AlertPersistenceFailure(
                code=code,
                original_failure=exc,
                alert_failure=alert_exc,
                summary=summary,
            ) from alert_exc
        return
    if summary.get("connector_failure_alert") in {"recorded", "already_recorded"}:
        return
    # Slot reconciliation raises after it has durably emitted the named verdict
    # (`slot_missing`, `slot_ahead_of_destination`, or `slot_recreated`).  Projecting
    # the same exception again as a generic run_failure creates two operator incidents
    # for one loss of source position; the named verdict is the complete diagnosis.
    if isinstance(exc, SlotAheadOfDestination):
        return
    source_health_episode = None
    if (
        summary.get("stop_reason") == "service_holder_reclaimed"
        and isinstance(exc, LeaseLost)
        and exc.lease_state is not None
    ):
        code = "service_holder_reclaimed"
        severity = "critical"
        condition_key = code
        occurrence_key = OccurrenceKey.from_lease(
            exc.lease_state, pipeline=dest.pipeline_name
        )
    elif isinstance(exc, LeaseLost) or "lease" in str(exc).lower():
        code = "concurrent_destination_run"
        severity = "critical"
        # The runner id identifies the attempt, not the occurrence. LeaseLost carries
        # the holder/pipeline identity separately from its changing expiry text.
        condition_key = code
        occurrence_key = (
            OccurrenceKey.from_lease(
                exc.lease_state, pipeline=dest.pipeline_name
            )
            if isinstance(exc, LeaseLost) and exc.lease_state is not None
            else OccurrenceKey.from_run(run_state, pipeline=dest.pipeline_name)
        )
    elif isinstance(exc, OffsetUnusable):
        code = "offset_unusable"
        severity = "critical"
        condition_key = f"{code}:{hashlib.sha256(str(exc).encode()).hexdigest()}"
        occurrence_key = (
            OccurrenceKey.from_offset_row(
                exc.offset_row,
                pipeline=dest.pipeline_name,
                namespace=exc.offset_row.state.namespace,
            )
            if exc.offset_row is not None
            else OccurrenceKey.from_run(run_state, pipeline=dest.pipeline_name)
        )
    elif summary.get("slot_check"):
        code = str(summary["slot_check"].get("decision") or "slot_check_failed")
        severity = "critical"
        slot_check = summary["slot_check"]
        condition_key = code
        slot_name = slot_check.get("slot_name")
        slot_receipt = (
            dest_mod.read_slot_state(
                con,
                dest.pipeline_name,
                slot_name,
                control_schema=dest.control_schema,
            )
            if isinstance(slot_name, str) and slot_name.strip()
            else None
        )
        occurrence_key = (
            OccurrenceKey.from_slot_state(
                slot_receipt,
                pipeline=dest.pipeline_name,
                slot_name=slot_name,
            )
            if slot_receipt is not None
            else OccurrenceKey.from_run(run_state, pipeline=dest.pipeline_name)
        )
    elif summary.get("stop_reason") == "source_dark":
        code = "source_dark"
        severity = "critical"
        # This is an episode identity, not a pipeline identity.  It is updated before
        # the alert is emitted, so recovery followed by another outage cannot reuse a
        # once-ever marker.
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        try:
            source_health_episode = dest_mod.observe_source_health(
                con,
                pipeline=dest.pipeline_name,
                state="dark",
                control_schema=dest.control_schema,
            )
        except Exception:
            log.critical("could not persist the source-dark episode", exc_info=True)
        episode_id = (
            source_health_episode.episode_id
            if isinstance(source_health_episode, EpisodeReceipt)
            else None
        )
        summary["source_health_episode"] = episode_id
        condition_key = code
        occurrence_key = (
            OccurrenceKey.from_episode(
                source_health_episode, pipeline=dest.pipeline_name
            )
            if isinstance(source_health_episode, EpisodeReceipt)
            else OccurrenceKey.from_run(run_state, pipeline=dest.pipeline_name)
        )
    elif summary.get("stop_reason") in {"max_seconds", "engine_error", "hung"}:
        code = "run_incomplete"
        severity = "critical"
        condition_key = (
            f"{code}:{hashlib.sha256(str(exc).encode()).hexdigest()}:"
            f"{summary.get('stop_reason')}"
        )
        occurrence_key = OccurrenceKey.from_run(
            run_state, pipeline=dest.pipeline_name
        )
    else:
        code = "run_failure"
        severity = "critical"
        condition_key = f"{code}:{hashlib.sha256(str(exc).encode()).hexdigest()}"
        occurrence_key = OccurrenceKey.from_run(
            run_state, pipeline=dest.pipeline_name
        )
    if code == "service_holder_reclaimed":
        message = (
            f"cdc_flight reclaimed an expired or stalled service holder for pipeline "
            f"{dest.pipeline_name!r}; the successor is now the fenced writer. "
            f"Evidence: {type(exc).__name__}: {exc}"
        )
    else:
        message = (
            f"cdc_flight run for pipeline {dest.pipeline_name!r} failed before it could "
            f"claim success: {type(exc).__name__}: {exc}"
        )
    try:
        # A failed lease acquisition can leave the destination connection inside the
        # implicit transaction opened by its probe (MotherDuck reports this as a
        # conflict/aborted transaction).  The alert is an operator-side fact, not part
        # of that failed attempt; clear the probe transaction before writing it so the
        # failure cannot be durable only in last_run.json.
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        alert_sink = dest_mod.AlertSink(
            con,
            pipeline=dest.pipeline_name,
            control_schema=dest.control_schema,
        )
        try:
            raised = alert_sink.raise_alert_once(
                severity=severity,
                code=code,
                message=message,
                condition_key=condition_key,
                occurrence_key=occurrence_key,
                context={
                    "runner_id": run_state.runner_id,
                    "error_type": type(exc).__name__,
                    "stop_reason": summary.get("stop_reason"),
                    "source_health_episode": (
                        source_health_episode.as_dict()
                        if isinstance(source_health_episode, EpisodeReceipt)
                        else source_health_episode
                    ),
                },
            )
            if not raised and not dest_mod.alert_identity_exists(
                alert_sink._sink if alert_sink.independent else con,
                pipeline=dest.pipeline_name,
                code=code,
                condition_key=condition_key,
                occurrence_key=occurrence_key,
                control_schema=dest.control_schema,
            ):
                raise AlertPersistenceFailure(
                    code=code,
                    original_failure=exc,
                    alert_failure=RuntimeError(
                        f"the alert sink did not persist {condition_key}"
                    ),
                    summary=summary,
                )
        finally:
            alert_sink.close()
    except AlertPersistenceFailure:
        raise
    except Exception:
        log.warning("could not persist run failure alert %s", code, exc_info=True)


def _teardown_destination(
    *, con, ownership: DestinationOwnership, reported: dict | None,
    phases: RunPhaseWriter | None, lease: Lease | None, lease_held: bool, run_ok: bool,
    service_mode: bool = False,
    service_context=None,
    hard_exit_on_transfer: bool = False,
) -> None:
    """Make the one terminal ownership decision for every destination handle."""
    # This also seals and retires a constructed-but-never-activated applier. Consumer
    # construction failures have no callback owner and must not leak an idle timer,
    # alert cursor, lease and parent connection.
    destination_quiescent = ownership.retire_if_quiescent(
        reason="pipeline_teardown"
    )
    if not destination_quiescent:
        if reported is not None:
            if phases is not None:
                reported.update(phases.summary())
            reported["destination_connection_release"] = "abandoned"
            reported["destination_connection_release_reason"] = "live_applier_callback"
            reported["heartbeat_sink_retirement"] = "abandoned"
            reported["heartbeat_sink_retirement_reason"] = "live_applier_callback"
            reported["destination_ownership_state"] = ownership.state
        log.critical(
            "destination teardown skipped: a live applier callback retains exclusive "
            "ownership"
        )
        if ownership.callback_owned and hard_exit_on_transfer:
            terminal = reported if reported is not None else {}
            terminal.setdefault("ok", False)
            terminal.setdefault("stop_reason", "hung")
            terminal.setdefault(
                "error",
                "an admitted callback retained terminal ownership after failed "
                "quiescence",
            )
            terminal.setdefault("error_type", "EngineFailure")
            _write_summary(terminal)
            shutdown_and_exit(1)
        return

    # The lease is acquired before recovery mutates state. `lease_held` rather than
    # `lease is not None` prevents a failed acquisition from deleting the incumbent.
    if phases is not None:
        try:
            phases.ensure(PHASE_STOPPING)
        except Exception:  # pragma: no cover - every phase declares `-> stopping`
            log.error("could not record the stopping phase", exc_info=True)
    if lease_held and lease is not None:
        # Service rows are retained as ``released`` so the next scheduled
        # admission can advance the fencing epoch visibly.  Batch leases keep
        # their established delete-on-release behavior.
        if service_mode:
            # This is the production release edge, rather than the admission
            # fallback below.  Keeping the matrix point on the actual write
            # path preserves the hard-death proof after the single-process
            # teardown has already attempted the fenced release.
            faults_mod.matrix_crash("service_lease_release")
            lease.release(con, retain=True)
            if service_context is not None:
                service_context.note_lease_release_attempted()
        else:
            lease.release(con)
    if phases is not None:
        try:
            phases.finish(ok=run_ok)
        except Exception:  # pragma: no cover
            log.error("could not record the terminal run phase", exc_info=True)
        # Terminalise and retire the heartbeat child before retiring its parent.
        try:
            phases.close()
        except Exception:  # pragma: no cover - retirement is internally guarded
            log.error("could not retire the heartbeat sink", exc_info=True)
        if reported is not None:
            reported.update(phases.summary())
    release = dest_mod.release_connection(con)
    if reported is not None:
        reported["destination_connection_release"] = release.state
        if release.error is not None:
            reported["destination_connection_close_error"] = release.error


def shutdown_and_exit(code: int = 0, timeout: float = 15.0) -> None:
    """Tear the JVM down and guarantee the process actually exits.

    Debezium leaves non-daemon JVM threads behind, so a plain `return` from
    `main()` leaves the interpreter hanging forever after the work is done.
    """
    # Arm the hard-exit bound BEFORE touching output.  A subprocess whose parent
    # is waiting for its exit can fill the stdout pipe with stock Debezium/JVM
    # diagnostics; flushing that pipe is I/O, not a safe precondition for the
    # watchdog.  Previously the watchdog was created after these flushes, so a
    # source-dark close could write its accurate summary and then remain alive
    # forever in ``flush()`` while the scheduled successor waited.
    watchdog = threading.Timer(timeout, lambda: os._exit(code))
    watchdog.daemon = True
    watchdog.start()
    try:
        sys.stdout.flush()
        sys.stderr.flush()
        import jpype
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
    except Exception:
        log.debug("JVM shutdown raised; exiting anyway", exc_info=True)
    os._exit(code)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-flight", description=__doc__)
    parser.add_argument("--destination", choices=["duckdb", "motherduck"], default=None)
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument("--idle-seconds", type=float, default=None)
    parser.add_argument("--min-records", type=int, default=None)
    parser.add_argument(
        "--snapshot-mode",
        default=None,
        help="Debezium snapshot.mode (initial, no_data, initial_only, always, when_needed, ...)",
    )
    # The two DESTRUCTIVE flags, and the help says every surface they touch. The old
    # text named two of five for `--reset-state` and one of four for the orphan hatch,
    # which is not an operator-facing contract (Codex r2 MINOR-3). Both are journalled
    # recoveries: the intent is durable before the first mutation, and a crash part-way
    # through is finished by the next run WITHOUT repeating the flag.
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help=(
            "start over. DESTRUCTIVE: removes the whole Debezium state directory "
            "(including offsets.dat), deletes the destination's resume point, resets "
            "every table's snapshot bookkeeping and marks every captured table for a "
            "fresh image, discards the recorded source catalog, deletes the pipeline "
            "lease, and DROPS THE REPLICATION SLOT (Debezium only pairs a snapshot "
            "with an exact WAL position when it creates the slot itself, ADR 0001 "
            "§19/A45). Destination tables keep their rows until each one's fresh image "
            "is swapped in. Journalled, so an interrupted reset is resumed."
        ),
    )
    parser.add_argument(
        "--accept-orphan-offsets",
        action="store_true",
        help=(
            "authorise a rebuild when offsets.dat has no matching destination row "
            "(ADR 0001 §4.5); without this the run REFUSES to start. DESTRUCTIVE: "
            "deletes that offsets file, DROPS THE REPLICATION SLOT, and forces a "
            "data-reading snapshot of every captured table. Journalled first, so the "
            "obligation survives a crash mid-sequence."
        ),
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )

    try:
        result = run(
            destination=args.destination,
            max_seconds=args.max_seconds,
            idle_seconds=args.idle_seconds,
            min_records=args.min_records,
            snapshot_mode=args.snapshot_mode,
            reset_state=args.reset_state,
            accept_orphan_offsets=args.accept_orphan_offsets,
        )
    # `BaseException`, not `Exception`: Ctrl-C must still reach
    # `shutdown_and_exit()`, because Debezium leaves non-daemon JVM threads
    # behind and a bare `raise` would hang the interpreter forever.
    except BaseException as exc:
        log.exception("pipeline run failed")
        summary = dict(getattr(exc, "summary", {}) or {})
        summary.setdefault("stop_reason", "error")
        summary["ok"] = False
        summary["error"] = str(exc)
        summary["error_type"] = type(exc).__name__
        _write_summary(summary)
        shutdown_and_exit(1)
        return 1  # unreachable; keeps type checkers happy

    _write_summary(result)
    shutdown_and_exit(0)
    return 0


def _write_summary(summary: dict) -> None:
    # This is the last observable point before ``shutdown_and_exit``.  Keeping
    # the interval keyed to the accepted completion decision, rather than to
    # process start, makes the assertion insensitive to JVM startup and other
    # unrelated host load while still covering destination drain, engine close,
    # lease release, and the outer pipeline teardown.
    stop_at = summary.pop("_completion_stop_at_monotonic", None)
    if stop_at is not None:
        summary["completion_stop_to_summary_sec"] = round(
            max(0.0, time.monotonic() - float(stop_at)), 3
        )
    payload = json.dumps(summary, indent=2, sort_keys=True, default=str)
    print(payload)
    try:
        state_dir = ReplicationConfig().state_dir
        state_dir.mkdir(parents=True, exist_ok=True)
        Path(state_dir / "last_run.json").write_text(payload)
    except Exception:  # pragma: no cover - never let reporting mask the outcome
        log.warning("could not write last_run.json", exc_info=True)


if __name__ == "__main__":
    sys.exit(main())
