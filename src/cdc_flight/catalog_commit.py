"""Catalog planning and state markers inside an applier commit group."""

from __future__ import annotations

from . import destination
from .catalog import CHANGE_SCHEMA
from .catalog_apply import CatalogPlan


def flush_table_events(applier, commit_id: int) -> None:
    """Write table-level markers in the same transaction as their data."""
    for marker in applier.group.table_events:
        destination.write_table_event(
            applier.con,
            pipeline=applier.pipeline,
            commit_id=commit_id,
            seq=applier.group.next_table_event_seq(),
            **marker,
        )
    applier.group.table_events = []


def plan_catalog_changes(applier, durable_lsn: int):
    coordinator = applier.catalog_coordinator
    if not coordinator.enabled:
        return None
    plan = coordinator.plan(durable_lsn)
    if not plan.actions and not plan.relations and not plan.alerts:
        return None
    applier.group.catalog_plan = plan
    return plan


def apply_catalog_phase(
    applier,
    commit_id: int,
    plan: CatalogPlan,
    stats: dict,
    *,
    schema_only: bool,
) -> None:
    """Apply one catalog phase inside the open destination transaction."""
    schema_actions = tuple(
        action for action in plan.actions if action.change.kind == CHANGE_SCHEMA
    )
    actions = schema_actions if schema_only else tuple(
        action for action in plan.actions if action.change.kind != CHANGE_SCHEMA
    )
    schema_names = {action.change.qualified for action in schema_actions}
    relations = tuple(
        relation
        for relation in plan.relations
        if (relation.qualified in schema_names) == schema_only
    )
    phase = CatalogPlan(
        actions=actions,
        relations=relations,
        refused=plan.refused if not schema_only else (),
    )
    if not phase.actions and not phase.relations and not phase.refused:
        return
    applier.group.table_events.extend(
        applier.catalog_coordinator.apply(applier.con, phase, stats)
    )
    if applier.group.table_events:
        flush_table_events(applier, commit_id)


def apply_catalog_changes(applier, commit_id: int, durable_lsn: int, stats: dict) -> None:
    """Compatibility wrapper for callers that apply a complete catalog plan."""
    plan = plan_catalog_changes(applier, durable_lsn)
    if plan is None:
        return
    apply_catalog_phase(applier, commit_id, plan, stats, schema_only=False)
    applier.group.pending_alerts.extend(plan.alerts)


def settle_catalog(applier, group_obj) -> None:
    """Forget catalog work only after its destination transaction commits."""
    if applier.catalog is None:
        return
    plan = group_obj.catalog_plan
    if plan is not None:
        applier.catalog_coordinator.settle(plan, group_obj.source_tables)
        group_obj.catalog_plan = None
    elif group_obj.source_tables:
        applier.catalog.observe_replicated(group_obj.source_tables)
