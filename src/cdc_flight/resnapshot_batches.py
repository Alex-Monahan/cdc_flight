"""Table-scoped automatic re-snapshot batching."""

from __future__ import annotations

from . import destination
from .errors import SchemaEvolutionRefused


def run_owed(
    con,
    *,
    source,
    replication,
    pipeline: str,
    dataset: str,
    owed: list[tuple[str, str, str]],
    settings,
    run_cfg,
    lease,
    runner_id: str,
    transactional_ddl: bool,
    epoch_base: int,
    namespace: str,
    ownership,
    new_relations: set[str],
    drop_mode: str,
    control_schema: str | None,
    resnapshot_run,
) -> tuple[list[dict], dict, int]:
    """Retry pending refusals alone, then rebuild healthy tables together."""
    quarantined_names = destination.quarantined_tables(
        con, pipeline, control_schema=control_schema
    )
    for schema, table, target in owed:
        if f"{schema}.{table}" in quarantined_names:
            # Declared quarantined -> pending trigger.  The owed marker remains
            # durable before the throwaway snapshot reads current source state.
            destination.reactivate_schema_refusal(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                target_table=target,
                control_schema=control_schema,
            )
    pending_names = {
        f"{schema}.{table}"
        for schema, table, _reason in destination.pending_schema_refusals(
            con, pipeline, control_schema=control_schema
        )
    }
    retry_batches = [
        [table] for table in owed if f"{table[0]}.{table[1]}" in pending_names
    ]
    retry_names = {
        f"{table[0]}.{table[1]}" for batch in retry_batches for table in batch
    }
    remaining = [
        table for table in owed if f"{table[0]}.{table[1]}" not in retry_names
    ]
    batches = [*retry_batches, *([remaining] if remaining else [])]
    passes: list[dict] = []
    snapshot_epoch = epoch_base
    for batch in batches:
        batch_names = ", ".join(f"{schema}.{table}" for schema, table, _ in batch)
        retry = len(batch) == 1 and batch_names in retry_names
        reason = (
            f"retrying pending schema refusal for {batch_names}"
            if retry
            else f"{len(batch)} table(s) marked awaiting_snapshot"
        )
        try:
            result = resnapshot_run(
                con,
                source=source,
                replication=replication,
                pipeline=pipeline,
                dataset=dataset,
                tables=batch,
                settings=settings,
                run_cfg=run_cfg,
                lease=lease,
                runner_id=runner_id,
                transactional_ddl=transactional_ddl,
                epoch_base=snapshot_epoch,
                reason=reason,
                namespace=namespace,
                ownership=ownership,
                new_relations=new_relations,
                drop_mode=drop_mode,
                control_schema=control_schema,
            )
            detail = result.as_dict()
            snapshot_epoch = max(snapshot_epoch, result.snapshot_epoch)
        except SchemaEvolutionRefused as refused:
            # The refusal writer has already scoped this batch's table.  Continue
            # with healthy tables so one bad relation cannot stop their snapshot.
            if not refused.refusal_recorded:
                refused.source_schema = refused.source_schema or (
                    batch[0][0] if len(batch) == 1 else None
                )
                refused.source_table = refused.source_table or (
                    batch[0][1] if len(batch) == 1 else None
                )
                refused.target = refused.target or (
                    batch[0][2] if len(batch) == 1 else None
                )
                if refused.source_schema and refused.source_table:
                    destination.record_schema_refusal(
                        con,
                        pipeline=pipeline,
                        source_schema=refused.source_schema,
                        source_table=refused.source_table,
                        target_table=refused.target,
                        detected_lsn=refused.detected_lsn,
                        reason=str(refused),
                        input_fingerprint=refused.input_fingerprint,
                        refusal_class=refused.refusal_class,
                        control_schema=control_schema,
                    )
                    refused.refusal_recorded = True
            detail = {
                "resnapshot_requested": [f"{s}.{t}" for s, t, _ in batch],
                "resnapshot_error": f"{type(refused).__name__}: {refused}",
                "resnapshot_reason": reason,
                "resnapshot_quarantined": [],
            }
        passes.append(detail)
    return passes, (passes[-1] if passes else {}), snapshot_epoch
