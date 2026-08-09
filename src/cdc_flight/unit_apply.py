"""Apply complete source units through the shared planner.

This is the row-writing seam of the applier. The commit protocol supplies one
transaction and one whole-unit list; this module turns that list into a ``GroupPlan``
and reports the small set of counters/markers the owner exposes.
"""

from __future__ import annotations

from . import destination
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
) -> dict:
    """Apply a list of whole units without changing their source order."""
    created_in_txn = (
        applier.group.created_in_txn
        if created_in_txn is None
        else created_in_txn
    )
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
    )
    for unit in group:
        if unit.fenced:
            if unit.spill_unit_seq is not None:
                applier.fenced_spilled_events += unit.spilled_events
                plan.staged_units = True
            continue
        if unit.kind == "snapshot_chunk":
            applier.group.is_snapshot = True
        plan.add_unit(unit)

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
    if applier.group.is_snapshot and stats.get("last_lsn"):
        applier.last_snapshot_lsn = stats["last_lsn"]
    applier.group.source_tables |= plan.source_tables
    applier.group.table_events.extend(plan.markers())
    applier._flush_table_events(commit_id)
    return stats
