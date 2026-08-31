"""Apply complete source units through the shared planner.

This is the row-writing seam of the applier. The commit protocol supplies one
transaction and one whole-unit list; this module turns that list into a ``GroupPlan``
and reports the small set of counters/markers the owner exposes.
"""

from __future__ import annotations

import time

from . import destination, spill_refusal
from .faults import maybe_crash
from .planner import GroupPlan


def apply_units(
    applier,
    group,
    commit_id: int,
    *,
    has_data: bool,
    clear_spill: bool = True,
    created_in_txn: set[str] | None = None,
    excluded_tables: set[str] | None = None,
) -> dict:
    """Apply a list of whole units without changing their source order."""
    created_in_txn = (
        applier.group.created_in_txn
        if created_in_txn is None
        else created_in_txn
    )
    # A pre-assembler refusal is already scoped and its row image has been sealed
    # away. Persist the decision after this destination transaction is open, before
    # planning any rows, so refusal state and healthy peer data share one COMMIT.
    for unit in group:
        for refused in getattr(unit, "admission_refusals", ()):
            if refused.refusal_recorded:
                continue
            spill_refusal.record_schema_refusal(
                applier,
                refused,
                transaction_open=True,
            )
            refused.refusal_recorded = True
    plan = GroupPlan(
        applier.con,
        commit_id=commit_id,
        registry_of=lambda: applier.registry,
        snapshots=applier.snapshots,
        spill=applier.spill,
        truncate_mode=applier.cfg.truncate_mode,
        created_in_txn=created_in_txn,
        watermarks=applier.watermarks,
        descriptor_provider=(
            applier.descriptor_provider
            or (
                getattr(applier.catalog, "descriptors_for", None)
                if applier.catalog is not None
                else None
            )
        ),
        toast_policy_provider=(
            getattr(applier.catalog, "toast_policy_for", None)
            if applier.catalog is not None
            else None
        ),
        toast_admission_provider=(
            getattr(applier.catalog, "admit_toast_event", None)
            if applier.catalog is not None
            else None
        ),
        toast_admission_end_provider=(
            getattr(applier.catalog, "end_toast_admission", None)
            if applier.catalog is not None
            else None
        ),
        binary_handling_mode=(
            getattr(applier.catalog, "binary_handling_mode", applier.binary_handling_mode)
            if applier.catalog is not None
            else applier.binary_handling_mode
        ),
        hstore_handling_mode=(
            getattr(applier.catalog, "hstore_handling_mode", applier.hstore_handling_mode)
            if applier.catalog is not None
            else applier.hstore_handling_mode
        ),
        pipeline=applier.pipeline,
        control_schema=applier.control_schema,
        # Quarantined relations plus any relation whose stream this run holds out
        # of a retained image pending a replacement snapshot (round 13, R12-2).
        blocked_tables=applier.blocked_schema_tables | applier.group.held_tables,
        ignored_tables=applier.ignored_source_tables,
        excluded_tables=excluded_tables,
        contain_table_failure=applier._contain_table_failure,
        source_cluster_id=applier.source_cluster_id,
        source_timeline=applier.source_timeline,
        strict_event_identity=applier.strict_event_identity,
        delete_policy=applier.delete_policy,
        policy_gate=applier.policy_gate,
    )
    if plan._event_ledger is not None:
        stream_pairs = [
            (
                applier.snapshots.target_table(event.schema, event.table),
                str(event.txn_id),
            )
            for unit in group
            if unit.kind != "snapshot_chunk"
            for event in unit.events
            if (
                event.txn_id is not None
                and event.schema
                and event.table
                and not event.incremental
            )
        ]
        plan._event_ledger.prefetch_transactions(stream_pairs)
    fold_started = (
        time.perf_counter() if getattr(applier, "_perf_timing", False) else None
    )
    try:
        for unit in group:
            if unit.fenced:
                if unit.spill_unit_seq is not None:
                    applier.fenced_spilled_events += unit.spilled_events
                    plan.staged_units = True
                continue
            if unit.kind == "snapshot_chunk" and not getattr(unit, "incremental", False):
                applier.group.is_snapshot = True
            plan.add_unit(unit)
    finally:
        if fold_started is not None:
            plan.stats["fold_sec"] = time.perf_counter() - fold_started

    anchor = None
    if has_data:
        def anchor() -> None:
            maybe_crash("mid_apply", applier.data_commit_groups + 1)

    stats = plan.write(after_first_table=anchor, clear_spill=clear_spill)
    applier.group.created_in_txn.update(created_in_txn)
    for target, (schema, table) in plan.created_tables.items():
        destination.register_table(
            applier.con,
            pipeline=applier.pipeline,
            source_schema=schema,
            source_table=table,
            target_table=target,
            control_schema=applier.control_schema,
        )
    with applier._lock:
        for target, count in plan.table_counts.items():
            applier.table_counts[target] = applier.table_counts.get(target, 0) + count
    applier.truncates_applied += plan.truncates_applied
    applier.truncates_logged += plan.truncates_logged
    applier.watermark_fenced_events += plan.watermark_fenced_events
    applier.quarantined_events += stats.get("quarantined_events", 0)
    if applier.group.is_snapshot and stats.get("last_lsn"):
        applier.last_snapshot_lsn = stats["last_lsn"]
    applier.group.source_tables |= plan.source_tables
    applier.group.table_events.extend(plan.markers())
    applier._flush_table_events(commit_id)
    return stats
