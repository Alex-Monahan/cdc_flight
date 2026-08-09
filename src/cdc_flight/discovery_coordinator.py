"""Live catalog-discovery hand-off orchestration.

Discovery is a bounded sub-run: the current engine is quiesced, the main slot keeps
retaining WAL, the newly admitted table is rebuilt, and a fresh no-data engine resumes
from the same durable destination point.  Keeping this coordinator separate from the
top-level pipeline makes that ownership boundary reviewable as one component.
"""

from __future__ import annotations

import dataclasses
import logging
import time
from typing import Any

from . import destination as dest_mod
from . import naming
from . import resnapshot as resnapshot_mod
from .applier import Applier
from .config import CatalogConfig, ReplicationConfig, RunConfig, SourceConfig
from .errors import EngineFailure
from .machines import PHASE_SNAPSHOTTING, PHASE_STREAMING
from .ownership import DestinationOwnership
from .run_state import RunOutcome, RunPhaseWriter
from .snapshot_completion import SnapshotCompletion
from .source_health import SourceHealth
from .supervisor import run_engine_bounded

log = logging.getLogger("cdc_flight.discovery_coordinator")


class LiveDiscoveryCoordinator:
    """Run the initial engine and any safe live-discovery hand-offs."""

    def __init__(
        self,
        *,
        con,
        source: SourceConfig,
        replication: ReplicationConfig,
        destination,
        namespace: str,
        run_cfg: RunConfig,
        applier_cfg,
        props: dict,
        settings: dict,
        watcher,
        discovered,
        catalog_cfg: CatalogConfig,
        phases: RunPhaseWriter,
        lease,
        runner_id: str,
        transactional_ddl: bool,
        ownership: DestinationOwnership,
        snapshot_completion: SnapshotCompletion,
        completion_stage,
        main_resume,
        watermarks: dict[str, int],
        outcome: RunOutcome,
        summary_extra: dict[str, Any],
        resnapshot_enabled: bool,
        catalog_flush_exclude: set[str] | None = None,
    ) -> None:
        self.con = con
        self.source = source
        self.replication = replication
        self.destination = destination
        self.namespace = namespace
        self.run_cfg = run_cfg
        self.applier_cfg = applier_cfg
        self.props = props
        self.settings = settings
        self.watcher = watcher
        self.discovered = discovered
        self.catalog_cfg = catalog_cfg
        self.phases = phases
        self.lease = lease
        self.runner_id = runner_id
        self.transactional_ddl = transactional_ddl
        self.ownership = ownership
        self.snapshot_completion = snapshot_completion
        self.completion_stage = completion_stage
        self.main_resume = main_resume
        self.watermarks = dict(watermarks)
        self.outcome = outcome
        self.summary_extra = summary_extra
        self._resnapshot_enabled = resnapshot_enabled
        self.catalog_flush_exclude = set(catalog_flush_exclude or ())

        self.applier = None
        self.health = None
        self.result: dict | None = None
        self.reported: dict | None = None
        self.run_ok = False

    def run(self) -> dict:
        """Run engines until no newly admitted relation needs a hand-off."""
        # The setting is evaluated by the same policy helper as the startup path.  It
        # is passed in as a boolean so this module does not own environment parsing.
        discovery_handoff_enabled = bool(
            self.watcher is not None and self.source.auto_discovery and self._resnapshot_enabled
        )
        handled_discoveries = {relation.qualified for relation in self.discovered}
        live_discovered: list[str] = []
        discovery_handoffs = 0
        run_started = time.monotonic()
        first_engine = True

        try:
            while True:
                remaining = self.run_cfg.max_seconds - (time.monotonic() - run_started)
                if remaining <= 0:
                    raise EngineFailure(
                        "the live discovery hand-off exhausted the run deadline before "
                        "the resumed engine could start",
                        dict(self.summary_extra),
                    )
                engine_props = self.props if first_engine else self._resume_properties()
                completion_for_engine = (
                    self.snapshot_completion
                    if first_engine
                    else SnapshotCompletion.streaming_only()
                )
                self.applier = self._build_applier(
                    engine_props, completion_for_engine
                )
                # Imported only when the first engine is actually needed: importing
                # pydbzengine boots the JVM in supported deployments.
                from .engine import SupervisedDebeziumEngine

                engine = SupervisedDebeziumEngine(
                    properties=engine_props,
                    handler=self.applier,
                    offset_file=self.replication.offset_file,
                    always_commit_offsets=engine_props.get("offset.flush.interval.ms") == "0",
                )
                self._wire_consumer(engine, self.applier)
                self.health = SourceHealth(
                    dsn=self.source.dsn,
                    slot_name=self.replication.slot_name,
                    max_lag_bytes=self.run_cfg.idle_max_lag_bytes,
                ).start()
                if self.phases.phase != PHASE_STREAMING:
                    self.phases.to(PHASE_STREAMING)
                self.ownership.activate(self.applier)
                iteration_outcome = (
                    self.outcome if not discovery_handoff_enabled else RunOutcome()
                )

                def discovery_ready(
                    current_completion=completion_for_engine,
                    current_watcher=self.watcher,
                    excluded=handled_discoveries,
                ) -> bool:
                    return bool(
                        current_watcher is not None
                        and current_completion.completed
                        and current_watcher.new_relations(exclude=excluded)
                    )

                self.result = run_engine_bounded(
                    engine,
                    self.applier,
                    dataclasses.replace(self.run_cfg, max_seconds=remaining),
                    self.health,
                    engine_terminates_normally=(
                        engine_props["snapshot.mode"] in {"initial_only", "recovery_only"}
                    ),
                    catalog=self.watcher,
                    catalog_drain_seconds=self.catalog_cfg.drain_seconds,
                    phases=self.phases,
                    # Intermediate engines have local outcomes; the final engine owns
                    # the run-level outcome so ``work_done`` cannot mask clean idle.
                    outcome=iteration_outcome,
                    completion=completion_for_engine,
                    quiescence_observer=self.ownership.quiescence_observer(self.applier),
                    keep_catalog=discovery_handoff_enabled,
                    stop_when=discovery_ready if discovery_handoff_enabled else None,
                )
                self.health.stop()
                self.health = None

                newly_discovered = (
                    list(self.watcher.new_relations(exclude=handled_discoveries))
                    if discovery_handoff_enabled and self.watcher is not None
                    else []
                )
                if not newly_discovered:
                    if iteration_outcome is not self.outcome:
                        self.outcome.record(self.result.get("stop_reason") or "idle")
                    break

                # Both sides of this boundary are proofs, not best-effort cleanup:
                # watcher.stop() must return dead before destination state is changed,
                # and ownership must be retired before a second engine is constructed.
                if not self.watcher.stop():
                    raise EngineFailure(
                        "the catalog watcher did not quiesce before the live "
                        "discovery hand-off; refusing to restart it over a live "
                        "polling thread",
                        dict(self.result),
                    )
                if not self.ownership.retire_if_quiescent(reason="discovery_handoff"):
                    raise EngineFailure(
                        "the main engine callback did not quiesce before a live "
                        "discovery hand-off; the destination remains callback-owned",
                        dict(self.result),
                    )
                self.main_resume = dest_mod.read_resume_point(
                    self.con,
                    self.destination.pipeline_name,
                    self.namespace,
                    control_schema=self.destination.control_schema,
                ) or self.applier.resume_point
                tables = [
                    (
                        relation.schema,
                        relation.table,
                        naming.destination_table(
                            self.replication.topic_prefix, relation.schema, relation.table
                        ),
                    )
                    for relation in newly_discovered
                ]
                dest_mod.request_snapshot(
                    self.con,
                    pipeline=self.destination.pipeline_name,
                    tables=tables,
                    detail=(
                        "a new source relation was discovered while the pipeline was "
                        "running; the main slot remains active during its re-snapshot"
                    ),
                    control_schema=self.destination.control_schema,
                )
                self.phases.to(
                    PHASE_SNAPSHOTTING, detail="live catalog discovery hand-off"
                )
                resnap = resnapshot_mod.run(
                    self.con,
                    source=self.source,
                    replication=self.replication,
                    pipeline=self.destination.pipeline_name,
                    dataset=self.destination.dataset_name,
                    tables=tables,
                    settings=self.settings,
                    run_cfg=dataclasses.replace(self.run_cfg, max_seconds=remaining),
                    lease=self.lease,
                    runner_id=self.runner_id,
                    transactional_ddl=self.transactional_ddl,
                    epoch_base=self.main_resume.snapshot_epoch,
                    reason="live catalog discovery",
                    namespace=self.namespace,
                    ownership=self.ownership,
                    new_relations={relation.qualified for relation in newly_discovered},
                    drop_mode=self.applier_cfg.drop_mode,
                    control_schema=self.destination.control_schema,
                )
                self.watcher.complete_discoveries(
                    {relation.qualified for relation in newly_discovered}
                )
                discovery_handoffs += 1
                live_discovered.extend(relation.qualified for relation in newly_discovered)
                self.summary_extra.update(
                    {
                        "live_discovery_handoffs": discovery_handoffs,
                        "live_discovered_relations": list(live_discovered),
                        "live_resnapshot": resnap.as_dict(),
                    }
                )
                # The re-snapshot callback advanced this durable epoch in the same
                # transaction as the image/audit.  Only project that committed value
                # into the next main applier; no post-swap state write remains here.
                self.main_resume.snapshot_epoch = max(
                    self.main_resume.snapshot_epoch,
                    resnap.snapshot_epoch,
                )
                self.watermarks = resnapshot_mod.read_watermarks(
                    self.con,
                    self.destination.pipeline_name,
                    control_schema=self.destination.control_schema,
                )
                handled_discoveries.update(
                    relation.qualified for relation in newly_discovered
                )
                self.phases.to(
                    PHASE_STREAMING, detail="live discovery hand-off complete"
                )
                self.watcher.start()
                first_engine = False

            report = self.completion_stage.finish(self.result)
            self.run_ok = report.run_ok
            self.outcome.record(report.summary.get("stop_reason") or self.outcome.value)
            self.reported = report.summary
            return self.reported
        except EngineFailure as failure:
            self.outcome.record(failure.summary.get("stop_reason") or "engine_error")
            self.reported = failure.summary
            raise
        finally:
            if self.health is not None:
                self.health.stop()
            if self.applier is not None:
                self.applier.shutdown()
            watcher_quiesced = True
            if self.watcher is not None:
                watcher_quiesced = self.watcher.stop()
            if watcher_quiesced and self.ownership.retire_if_quiescent(
                reason="discovery_coordinator_teardown"
            ):
                # Supervisor failures happen before PostEngineCompletion, but a
                # failed admission is still durable state that must survive this run.
                # Flush only after both writers are quiescent and never adopt an
                # unrelatable baseline while doing so.
                try:
                    learned = dest_mod.flush_learned_relations(
                        self.con,
                        pipeline=self.destination.pipeline_name,
                        catalog=self.watcher,
                        exclude=self.catalog_flush_exclude,
                        control_schema=self.destination.control_schema,
                    )
                    if learned:
                        self.summary_extra["source_relations_persisted"] = learned
                except Exception as exc:  # preserve the original run failure
                    self.summary_extra["source_relations_flush_error"] = (
                        f"{type(exc).__name__}: {exc}"
                    )
                    log.error(
                        "could not persist source-catalog observations during teardown",
                        exc_info=True,
                    )

    def _resume_properties(self) -> dict:
        """Make the second engine an explicit stream resume."""
        properties = {**self.props, "snapshot.mode": "no_data"}
        properties.pop("table.include.list", None)
        return properties

    def _build_applier(self, properties: dict, completion) -> Applier:
        applier = Applier(
            self.con,
            pipeline=self.destination.pipeline_name,
            namespace=self.namespace,
            dataset=self.destination.dataset_name,
            topic_prefix=self.replication.topic_prefix,
            offset_path=self.replication.offset_file,
            resume_point=self.main_resume,
            config=self.applier_cfg,
            lease=self.lease,
            runner_id=self.runner_id,
            transactional_ddl=self.transactional_ddl,
            catalog=self.watcher,
            watermarks=self.watermarks,
            completion=completion,
            binary_handling_mode=self.props.get("binary.handling.mode", "base64"),
            hstore_handling_mode=self.props.get("hstore.handling.mode", "map"),
            control_schema=self.destination.control_schema,
        )
        self.ownership.attach(applier)
        return applier

    @staticmethod
    def _wire_consumer(engine, applier) -> None:
        """Construct the cached consumer and verify its offset hook when enabled."""
        applier.verifier = None
        engine.consumer  # noqa: B018 - builds the consumer and attaches the verifier
        if applier.cfg.verify_offset_file:
            assert applier.verifier is not None, (
                "the offset-flush verifier was not attached to the applier; a "
                "silently failed markBatchFinished() would be invisible "
                "(ADR 0001 §4.2)"
            )
