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
import json
import logging
import os
import sys
import threading
import uuid
from pathlib import Path

from . import acquisition
from . import catalog as catalog_mod
from . import destination as dest_mod
from . import faults as faults_mod
from . import reconcile as reconcile_mod
from . import recovery as recovery_mod
from . import resnapshot as resnapshot_mod
from .applier import Applier, ApplierConfig
from .config import (
    CatalogConfig,
    DestinationConfig,
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    applier_settings,
    lease_ttl_seconds,
)
from .debezium_props import assert_no_internal_topic_collision, build_properties
from .destination import CONTROL_SCHEMA, Lease
from .errors import EngineFailure
from .faults import validate_env as validate_fault_env
from .machines import (
    PHASE_RECONCILING,
    PHASE_RECOVERING,
    PHASE_SNAPSHOTTING,
    PHASE_STOPPING,
    PHASE_STREAMING,
)
from .run_state import RunOutcome, RunPhaseWriter
from .source_health import SourceHealth
from .supervisor import run_engine_bounded

log = logging.getLogger("cdc_flight.pipeline")


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
) -> dict:
    # Parse CDC_FAULT_INJECT once, here, so a typo fails the run instead of
    # leaving a fault test vacuously green (Codex 9).
    fault_spec = validate_fault_env()
    if fault_spec:
        log.warning("fault injection armed: point=%s group=%s action=%s", *fault_spec)

    source = SourceConfig()
    replication = ReplicationConfig()
    dest = DestinationConfig(**({"kind": destination} if destination else {}))
    run_cfg = RunConfig(
        **{
            k: v
            for k, v in {
                "max_seconds": max_seconds,
                "idle_seconds": idle_seconds,
                "min_records": min_records,
            }.items()
            if v is not None
        }
    )

    replication.state_dir.mkdir(parents=True, exist_ok=True)
    settings = applier_settings()
    # `skipped.operations` is what decides whether a TRUNCATE is decoded at all, so
    # the truncate policy has to be known before the engine properties are built
    # (rubric 1.5).
    props = build_properties(
        source,
        replication,
        snapshot_mode=snapshot_mode,
        truncate_mode=settings["truncate_mode"],
    )
    # A captured table whose topic collides with `<prefix>.transaction` would be
    # decoded as transaction metadata and never applied. Not reachable with the
    # pinned topic-naming strategy, and asserted rather than reasoned about
    # (Opus MINOR-6).
    assert_no_internal_topic_collision(replication.topic_prefix, source.tables)
    namespace = props["name"]
    runner_id = uuid.uuid4().hex

    log.info(
        "source=%s:%s/%s tables=%s slot=%s snapshot=%s destination=%s",
        source.host, source.port, source.dbname, source.tables,
        replication.slot_name, props["snapshot.mode"], dest.kind,
    )

    # rubric 1.7: a `destination_*` fault is injected by wrapping the one connection
    # the applier writes through, rather than by scattering anchors through the SQL
    # builders. `AlertSink`'s independent `cursor()` is delegated untouched on
    # purpose - see `faults.FaultyConnection`.
    con = faults_mod.wrap_destination(dest_mod.connect(dest))
    summary_extra: dict = {}
    lease: Lease | None = None
    lease_held = False
    phases: RunPhaseWriter | None = None
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
        dest_mod.ensure_control_schema(con)
        dest_mod.ensure_dataset(con, dest.dataset_name)
        # rubric 1.9 / ADR §4.8: one `_cdc_flight.heartbeat` row per run, moved through
        # the `RUN_PHASE` machine on its OWN connection. "Where is this run" stops being
        # a source-line position in a 470-line function and becomes a query. The
        # periodic liveness/lag writer is still 4.4/6.1's.
        phases = RunPhaseWriter(
            con, pipeline=dest.pipeline_name, runner_id=runner_id, outcome=outcome
        )

        if reset_state:
            # The one part of `--reset-state` that is NOT journalled, and the one part
            # that does not need to be: a lease row destroys no data and records no
            # obligation. It is cleared before the lease is acquired because an operator
            # saying "start over" is also saying "break whatever claims to hold this
            # pipeline"; `Lease.acquire` reclaims a dead owner on its own, so this only
            # covers an owner whose host we cannot check.
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.lease WHERE pipeline = ?",
                [dest.pipeline_name],
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
        lease = Lease(dest.pipeline_name, owner_id=runner_id, ttl_seconds=lease_ttl_seconds())
        lease.acquire(con)
        lease_held = True

        # rubric 4.7: a throwaway `_rs` slot left behind by an interrupted re-snapshot
        # holds WAL on the source for ever and counts against `max_replication_slots`.
        # Swept unconditionally, by the one name this pipeline derives from its own slot
        # (Opus MAJOR-2, observed leaking twice on the shared cluster in one day).
        summary_extra["stale_resnapshot_slot"] = resnapshot_mod.sweep_stale_slot(
            source.dsn, replication.slot_name
        )

        captured_tables = acquisition.captured_tables(con, dest.pipeline_name, source, replication)

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
                con, pipeline=dest.pipeline_name, namespace=namespace
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
        # It used to be the other way round. `offset_reconcile` dropped the slot and
        # unlinked the file and only then did this block record why, which put a crash
        # window between destroying the evidence and writing the obligation: a hard exit
        # there left no row, no file, no slot and no journal, the next run called that an
        # ordinary `fresh_start`, and a configured non-data `snapshot.mode` streamed onto
        # a destination nobody had rebuilt. `reconcile()` now classifies and nothing
        # more; `begin()` makes the intent and the table obligation durable together;
        # `resume()` performs the file / row / slot ladder, idempotently, from whatever
        # phase survives.
        if reconciliation.decision == "orphan_accepted_resnapshot" and journal is None:
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
                context={"file_lsn": reconciliation.file_lsn},
            )
            summary_extra["recovery_journal"] = journal.as_dict()
            summary_extra["orphan_recovery"] = recovery_mod.resume(
                con,
                pipeline=dest.pipeline_name,
                namespace=namespace,
                record=journal,
                dsn=source.dsn,
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
        )

        # ADR §14.1's open question, answered by measurement rather than by guess.
        # AFTER the lease: the probe DROPs and CREATEs shared
        # `_cdc_flight.__ddl_probe_*` tables, so a runner that is about to be
        # rejected by the lease could otherwise drop the incumbent's probe tables
        # mid-probe and make the incumbent conclude `transactional_ddl=False`
        # (Opus MINOR-7).
        transactional_ddl = dest_mod.probe_transactional_ddl(con)
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
        interrupted = dest_mod.promote_interrupted_snapshots(con, dest.pipeline_name)
        if interrupted:
            summary_extra["interrupted_snapshots_requeued"] = interrupted
        owed = dest_mod.tables_awaiting_snapshot(con, dest.pipeline_name)
        will_snapshot_everything = (
            reconciliation.resume_point.last_lsn == 0
            and props["snapshot.mode"] in reconcile_mod.SNAPSHOT_MODES_WITH_DATA
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
            resnap = resnapshot_mod.run(
                con,
                source=source,
                replication=replication,
                pipeline=dest.pipeline_name,
                dataset=dest.dataset_name,
                tables=owed,
                settings=settings,
                run_cfg=run_cfg,
                lease=lease,
                runner_id=runner_id,
                transactional_ddl=transactional_ddl,
                epoch_base=reconciliation.resume_point.snapshot_epoch,
                reason=f"{len(owed)} table(s) marked awaiting_snapshot",
                namespace=namespace,
            )
            summary_extra.update(resnap.as_dict())
            # The main applier's snapshot identities must stay disjoint from the ones the
            # re-snapshot just wrote, and the epoch is what makes them disjoint.
            con.execute(
                f"UPDATE {CONTROL_SCHEMA}.debezium_offsets SET snapshot_epoch = "
                "greatest(snapshot_epoch, ?) WHERE pipeline = ? AND namespace = ?",
                [
                    reconciliation.resume_point.snapshot_epoch + len(owed) + 1,
                    dest.pipeline_name,
                    namespace,
                ],
            )
            reconciliation.resume_point.snapshot_epoch += len(owed) + 1
        elif owed:
            log.warning(
                "%s table(s) are marked awaiting_snapshot and are NOT being "
                "re-snapshotted on this run (%s): %s",
                len(owed),
                "the run snapshots everything anyway" if will_snapshot_everything
                else "CDC_RESNAPSHOT=0",
                ", ".join(f"{s}.{t}" for s, t, _ in owed),
            )
            summary_extra["tables_awaiting_snapshot_unhandled"] = [
                f"{s}.{t}" for s, t, _ in owed
            ]

        # rubric 1.6: the per-table snapshot watermark, read AFTER any re-snapshot so it
        # carries the image the main stream now has to hand over from.
        watermarks = resnapshot_mod.read_watermarks(con, dest.pipeline_name)
        summary_extra["snapshot_watermarks"] = len(watermarks)

        # rubric 1.5: `DROP TABLE` is not in the replication stream, so the source
        # catalog is polled on its own connection. Started BEFORE the engine, so a
        # table dropped while this pipeline was down is detected on this run rather
        # than one poll interval into it.
        catalog_cfg = CatalogConfig()
        watcher = None
        if applier_cfg.drop_mode != "ignore" and catalog_cfg.poll_seconds > 0:
            watcher = catalog_mod.CatalogWatcher(
                dsn=source.dsn,
                publication=replication.publication_name,
                schema=source.schema,
                include={t if "." in t else f"{source.schema}.{t}" for t in source.tables},
                known=catalog_mod.read_known_relations(con, dest.pipeline_name),
                replicated=catalog_mod.seed_from_table_state(con, dest.pipeline_name),
                poll_seconds=catalog_cfg.poll_seconds,
                emit_marker=catalog_cfg.emit_marker,
                marker_prefix=catalog_cfg.marker_prefix,
                grace_seconds=catalog_cfg.grace_seconds,
                confirm_polls=catalog_cfg.confirm_polls,
                marker_max_writes=catalog_cfg.marker_max_writes or None,
            ).start()
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

        # Imported late: importing pydbzengine boots a JVM.
        from .engine import SupervisedDebeziumEngine

        applier = Applier(
            con,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            dataset=dest.dataset_name,
            topic_prefix=replication.topic_prefix,
            offset_path=replication.offset_file,
            resume_point=reconciliation.resume_point,
            config=applier_cfg,
            lease=lease,
            runner_id=runner_id,
            transactional_ddl=transactional_ddl,
            catalog=watcher,
            watermarks=watermarks,
        )
        engine = SupervisedDebeziumEngine(
            properties=props,
            handler=applier,
            offset_file=replication.offset_file,
            always_commit_offsets=props.get("offset.flush.interval.ms") == "0",
        )
        # Wired EXPLICITLY. It used to be attached as a side effect of
        # `engine.consumer`'s `cached_property` being evaluated before `engine`'s,
        # which is a third-party property-evaluation order (Opus B2 note): correct
        # today, invisible if it ever changes. Touching `engine.consumer` here makes
        # the dependency a statement, and the assertion makes it a checked one.
        applier.verifier = None
        engine.consumer  # noqa: B018 - builds the consumer and attaches the verifier
        if applier.cfg.verify_offset_file:
            assert applier.verifier is not None, (
                "the offset-flush verifier was not attached to the applier; a silently "
                "failed markBatchFinished() would be invisible (ADR 0001 §4.2)"
            )
        health = SourceHealth(
            dsn=source.dsn,
            slot_name=replication.slot_name,
            max_lag_bytes=run_cfg.idle_max_lag_bytes,
        ).start()

        def _decorate(result: dict) -> dict:
            result.update(summary_extra)
            result["destination"] = dest.kind
            result["dataset"] = dest.dataset_name
            result["runner_id"] = runner_id
            if dest.kind == "duckdb":
                result["duckdb_path"] = str(dest.duckdb_path)
            else:
                result["motherduck_database"] = dest.motherduck_database
            return result

        terminating_modes = {"initial_only", "recovery_only"}
        try:
            phases.to(PHASE_STREAMING)
            result = run_engine_bounded(
                engine, applier, run_cfg, health,
                engine_terminates_normally=props["snapshot.mode"] in terminating_modes,
                catalog=watcher,
                catalog_drain_seconds=catalog_cfg.drain_seconds,
                phases=phases,
                # ONE outcome per run (Codex r1 MAJOR-2). The supervisor used to build
                # its own and the phase writer another, so `last_run.json` shipped
                # `stop_reason="idle"` beside `run_outcome="max_seconds"` on every
                # ordinary run and a severe result could be published as the mild
                # default.
                outcome=outcome,
            )
            summary_extra["invariant_o_end"] = reconcile_mod.check_invariant_o(
                con, pipeline=dest.pipeline_name, namespace=namespace,
                dsn=source.dsn, slot_name=replication.slot_name,
                snapshot_mode=props["snapshot.mode"],
            )
            # QUIESCE, VALIDATE, FLUSH, REPORT — in that order (Codex r3 BLOCKER-1).
            # `run_engine_bounded` has stopped the watcher and refused to return at all
            # unless it proved the thread dead, so nothing can add dirty state now.
            # Persisting here rather than only through a commit group is the fix: a run
            # that committed NO groups used to persist nothing, so everything the
            # watcher learned vanished — and after an offline drop-and-recreate the next
            # run accepted the replacement oid as though it had always owned that
            # relation, leaving the old relation's rows beside the new one's for ever.
            learned = dest_mod.flush_learned_relations(
                con, pipeline=dest.pipeline_name, catalog=watcher
            )
            if learned:
                summary_extra["source_relations_persisted"] = learned
            # The recovery is over when the work it asked for has actually been done.
            # The PREDICATE LIVES IN `recovery.py` (Codex r1 MAJOR-5): it validates the
            # captured obligation the journal recorded, performs the `armed -> absent`
            # edge itself, and returns a typed result. Clearing any earlier would throw
            # away the forced snapshot mode the rest of the rebuild depends on.
            if journal is not None:
                # A captured relation that is EMPTY at the source emits no snapshot
                # records, so the coordinator never opens a shadow and never swaps one
                # in — and the destination table keeps whatever it held. Under a
                # journalled obligation that is stale data certified as a rebuild
                # (Codex r2 BLOCKER-1, measured: a `--reset-state` reported `ok: true`
                # over two rows the source had truncated away). The blocking
                # re-snapshot has always closed this with three independent facts;
                # this is the same machinery asked the same question after the MAIN
                # engine's snapshot, so an operator's reset converges in one run rather
                # than failing and self-healing on the next.
                emptied, fence = resnapshot_mod.finish_empty_tables_after_main_snapshot(
                    con,
                    pipeline=dest.pipeline_name,
                    dataset=dest.dataset_name,
                    dsn=source.dsn,
                    owed=dest_mod.tables_awaiting_snapshot(con, dest.pipeline_name),
                    applier=applier,
                    stop_reason=str(result.get("stop_reason")),
                )
                if emptied:
                    summary_extra["verified_empty_after_snapshot"] = emptied
                    # An entirely empty capture set emits no records, so the applier
                    # commits no group and writes no resume point — and a recovery that
                    # correctly demands one would never clear, on any run, for ever
                    # (Codex r3 MAJOR-1). The handoff point is recorded from the fence
                    # the emptiness was proven at; see `record_empty_handoff` for why
                    # that claims nothing untrue.
                    if resnapshot_mod.record_empty_handoff(
                        con, pipeline=dest.pipeline_name, namespace=namespace,
                        fence_lsn=fence,
                    ):
                        summary_extra["empty_handoff_lsn"] = fence
                completion = recovery_mod.complete_if_ready(
                    con, pipeline=dest.pipeline_name, namespace=namespace, record=journal,
                )
                if completion.cleared:
                    summary_extra["recovery_cleared"] = completion.recovery_id
                else:
                    # The blueprint's nesting invariant: no successful stopped run while
                    # a destructive recovery is uncleared. It used to add a summary key
                    # and let the run report `ok: true` over a half-finished rebuild —
                    # `run_ok` came from the supervisor result and nothing else looked
                    # (Codex r1 MAJOR-5). Raised rather than flagged, because a summary
                    # that says `ok: false` behind a zero exit code is the same defect
                    # one layer out.
                    summary_extra["recovery_still_armed"] = completion.recovery_id
                    summary_extra["recovery_still_owed"] = list(completion.still_owed)
                    outcome.record("recovery_uncleared")
                    result["stop_reason"] = outcome.value
                    raise EngineFailure(
                        f"recovery {completion.recovery_id} is still armed at shutdown: "
                        f"{completion.reason}. The destination is knowingly mid-rebuild, "
                        "so this run is not a success",
                        result,
                    )
            run_ok = bool(result.get("ok"))
            outcome.record(result.get("stop_reason") or outcome.value)
            reported = _decorate(result)
            return reported
        except EngineFailure as failure:
            outcome.record(failure.summary.get("stop_reason") or "engine_error")
            reported = _decorate(failure.summary)
            raise
        finally:
            health.stop()
            if watcher is not None:
                watcher.stop()
            applier.shutdown()
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
        with contextlib.suppress(Exception):  # a BaseException without a __dict__
            exc.summary = reported
        raise
    finally:
        # The lease is now acquired much earlier - rubric 1.8's recovery mutates
        # destination state and must not race a second runner - so releasing it has to
        # move out here with it. `lease_held` rather than `lease is not None`: a run that
        # failed to acquire must not delete the incumbent's row.
        # Guarded, all of it: this is the outermost `finally`, and an observability
        # write that raises here would replace the run's real failure with a
        # bookkeeping one.
        if phases is not None:
            try:
                phases.ensure(PHASE_STOPPING)
            except Exception:  # pragma: no cover - every phase declares `-> stopping`
                log.error("could not record the stopping phase", exc_info=True)
        if lease_held and lease is not None:
            lease.release(con)
        if phases is not None:
            try:
                phases.finish(ok=run_ok)
            except Exception:  # pragma: no cover
                log.error("could not record the terminal run phase", exc_info=True)
            # TERMINALISE FIRST, THEN REPORT (Codex r1 MAJOR-2, open question 6). The
            # summary used to be sampled before the `stopping`/`stopped` transitions, so
            # every successful run shipped `run_phase="draining"` while the destination
            # heartbeat correctly held `stopped`. `reported` is the very dict `main()`
            # prints and persists, so updating it here makes `last_run.json` and the
            # heartbeat two projections of one state.
            if reported is not None:
                reported.update(phases.summary())
            phases.close()
        try:
            con.close()
        except Exception:  # pragma: no cover
            log.debug("closing the destination connection failed", exc_info=True)


def shutdown_and_exit(code: int = 0, timeout: float = 15.0) -> None:
    """Tear the JVM down and guarantee the process actually exits.

    Debezium leaves non-daemon JVM threads behind, so a plain `return` from
    `main()` leaves the interpreter hanging forever after the work is done.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    watchdog = threading.Timer(timeout, lambda: os._exit(code))
    watchdog.daemon = True
    watchdog.start()
    try:
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
