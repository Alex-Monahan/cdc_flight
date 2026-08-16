"""Post-engine durable discharge and final run-summary construction.

The engine callback runtime and the work that proves a run may report success have
different ownership. This stage owns the latter: it flushes catalog observations,
discharges any journalled rebuild, confirms the source-catalog baseline, and only then
publishes the summary. Returning a typed report keeps those durable checks out of the
engine supervision loop without turning the extraction into a pass-through helper.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from . import catalog_baseline as baseline_mod
from . import destination as dest_mod
from . import reconcile as reconcile_mod
from . import recovery as recovery_mod
from . import resnapshot as resnapshot_mod
from . import table_lifecycle
from .config import DROP_LOG
from .errors import EngineFailure
from .run_state import RunOutcome
from .snapshot_completion import SnapshotCompletion


@dataclass(frozen=True)
class CompletionReport:
    """The only result the pipeline needs after post-engine discharge."""

    summary: dict
    run_ok: bool


@dataclass
class PostEngineCompletion:
    """Discharge durable obligations before a run can be published as successful."""

    con: Any
    source_dsn: str
    slot_name: str
    pipeline: str
    namespace: str
    dataset: str
    snapshot_mode: str
    destination: Any
    runner_id: str
    watcher: Any
    journal: Any
    baseline: Any
    snapshot_completion: SnapshotCompletion
    outcome: RunOutcome
    base_summary: Mapping[str, Any]
    drop_mode: str = DROP_LOG

    def finish(self, engine_result: Mapping[str, Any]) -> CompletionReport:
        """Run the post-engine proof and return the final, publishable summary."""
        result = dict(engine_result)
        extra: dict[str, Any] = {}
        extra["invariant_o_end"] = reconcile_mod.check_invariant_o(
            self.con,
            pipeline=self.pipeline,
            namespace=self.namespace,
            dsn=self.source_dsn,
            slot_name=self.slot_name,
            snapshot_mode=self.snapshot_mode,
            control_schema=self.destination.control_schema,
        )

        learned = dest_mod.flush_learned_relations(
            self.con,
            pipeline=self.pipeline,
            catalog=self.watcher,
            exclude=set(
                baseline_mod.unrebuilt_relations(
                    self.con,
                    pipeline=self.pipeline,
                    dataset=self.dataset,
                    control_schema=self.destination.control_schema,
                )
            ),
            control_schema=self.destination.control_schema,
        )
        if learned:
            extra["source_relations_persisted"] = learned

        if self.journal is not None:
            self._discharge_recovery(result, extra)

        owing = table_lifecycle.owing_work(
            self.con, self.pipeline, control_schema=self.destination.control_schema
        )
        if owing:
            extra["tables_awaiting_snapshot_unhandled"] = owing
            self.outcome.record("catalog_unresolved")
            result["stop_reason"] = self.outcome.value
            raise EngineFailure(
                "the destination still has table-lifecycle work owed at shutdown: "
                + ", ".join(owing)
                + ". A table is not trusted merely because its physical target is "
                "empty or absent; a complete replacement image is required",
                self._summary(result, extra),
            )

        if self.watcher is not None:
            self._confirm_baseline(result, extra)

        pending_refusals = dest_mod.pending_schema_refusals(
            self.con,
            self.pipeline,
            control_schema=self.destination.control_schema,
        )
        if pending_refusals:
            names = [f"{schema}.{table}" for schema, table, _reason in pending_refusals]
            extra["schema_refusals_pending"] = names
            self.outcome.record("catalog_unresolved")
            result["stop_reason"] = self.outcome.value
            raise EngineFailure(
                "schema evolution refusal(s) remain unresolved at shutdown: "
                + ", ".join(names)
                + ". A complete replacement snapshot is required before success",
                self._summary(result, extra),
            )

        quarantined = list(self.base_summary.get("resnapshot_quarantine_run_not_ok", []))
        if quarantined:
            # The re-snapshot already committed the quarantine and the main engine
            # above has had the opportunity to apply healthy peers and advance the
            # main slot.  Do not turn that bounded, table-scoped containment into a
            # successful run merely because the final streaming engine was idle.
            self.outcome.record("engine_error")
            result["stop_reason"] = self.outcome.value
            result["error_cause_type"] = "SchemaEvolutionRefused"
            extra["resnapshot_quarantine_run_not_ok"] = quarantined
            raise EngineFailure(
                "automatic re-snapshot durably quarantined relation(s) "
                + ", ".join(quarantined)
                + "; healthy peers were allowed to proceed, but this run is NOT-OK "
                "until a complete replacement image resolves the quarantine",
                self._summary(result, extra),
            )

        summary = self._summary(result, extra)
        return CompletionReport(summary=summary, run_ok=bool(result.get("ok")))

    def _discharge_recovery(self, result: dict, extra: dict[str, Any]) -> None:
        """Finish empty-table evidence, then clear the journalled obligation."""
        emptied, fence = resnapshot_mod.finish_empty_tables_after_main_snapshot(
            self.con,
            pipeline=self.pipeline,
            dataset=self.dataset,
            dsn=self.source_dsn,
            owed=dest_mod.tables_awaiting_snapshot(
                self.con,
                self.pipeline,
                control_schema=self.destination.control_schema,
            ),
            completion=self.snapshot_completion,
            drop_mode=self.drop_mode,
            control_schema=self.destination.control_schema,
        )
        if emptied:
            extra["verified_empty_after_snapshot"] = emptied
            extra["verified_empty_fence_lsn"] = fence

        completion = recovery_mod.complete_if_ready(
            self.con,
            pipeline=self.pipeline,
            namespace=self.namespace,
            record=self.journal,
            verified_empty=emptied,
            control_schema=self.destination.control_schema,
        )
        if completion.cleared:
            extra["recovery_cleared"] = completion.recovery_id
            return

        extra["recovery_still_armed"] = completion.recovery_id
        extra["recovery_still_owed"] = list(completion.still_owed)
        self.outcome.record("recovery_uncleared")
        result["stop_reason"] = self.outcome.value
        raise EngineFailure(
            f"recovery {completion.recovery_id} is still armed at shutdown: "
            f"{completion.reason}. The destination is knowingly mid-rebuild, so this "
            "run is not a success",
            self._summary(
                result,
                extra,
            ),
        )

    def _confirm_baseline(self, result: dict, extra: dict[str, Any]) -> None:
        """Adopt catalog identities only after all rebuild work has settled."""
        baseline = baseline_mod.confirm(
            self.con,
            pipeline=self.pipeline,
            dataset=self.dataset,
            check=self.baseline,
            successful_polls=self.watcher.successful_polls,
            runner_id=self.runner_id,
            control_schema=self.destination.control_schema,
        )
        extra.update(baseline.as_dict())
        if baseline.valid:
            return

        self.outcome.record("engine_error")
        result["stop_reason"] = self.outcome.value
        raise EngineFailure(
            "the source-catalog baseline is "
            f"{baseline.state!r} at shutdown: {baseline.reason}. Until every "
            "relation the destination holds rows for can be related to an identity at "
            "the source, adopting what we observe would present one relation's rows as "
            "another's. Refusing to report success",
            self._summary(result, extra),
        )

    def _summary(self, result: Mapping[str, Any], extra: Mapping[str, Any]) -> dict:
        """Merge diagnostics in the same precedence as the old final projection."""
        summary = dict(result)
        summary.update(self.base_summary)
        summary.update(extra)
        summary["destination"] = self.destination.kind
        summary["dataset"] = self.destination.dataset_name
        summary["runner_id"] = self.runner_id
        if self.destination.kind == "duckdb":
            summary["duckdb_path"] = str(self.destination.duckdb_path)
        else:
            summary["motherduck_database"] = self.destination.motherduck_database
        return summary
