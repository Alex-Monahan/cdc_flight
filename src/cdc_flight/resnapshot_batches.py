"""Table-scoped automatic re-snapshot batching."""

from __future__ import annotations

from . import destination


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
        passes.append(detail)
        snapshot_epoch = max(snapshot_epoch, result.snapshot_epoch)
    return passes, (passes[-1] if passes else {}), snapshot_epoch
