"""Table-scoped failure persistence for the transactional applier.

The applier owns lifecycle state, but these two boundaries are deliberately kept
small and separate from the commit loop.  A destination execution error is first
rolled back by the commit protocol and then recorded on an independent sink;
healthy relations are replayed with only the failed relation excluded.
"""

from __future__ import annotations

import hashlib
import json
import logging

from . import destination, naming, spill_refusal

log = logging.getLogger("cdc_flight.failure_containment")
OWNER = "failure-containment"


def input_fingerprint(event) -> str:
    """Identify a durable refusal boundary without including the bad value."""
    descriptors = {
        name: descriptor
        for attribute in ("key_descriptors", "before_descriptors", "after_descriptors")
        for name, descriptor in getattr(event, attribute, {}).items()
    }
    payload = {
        "table": event.qualified_table,
        "descriptors": {
            str(name): getattr(descriptor, "fingerprint", repr(descriptor))
            for name, descriptor in sorted(descriptors.items())
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=repr)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def event_fingerprint(event) -> str:
    try:
        return input_fingerprint(event)
    except Exception:
        payload = f"{event.qualified_table}:{event.lsn}:{event.txn_id}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def item_fingerprint(item) -> str:
    payload = {
        "table": (
            f"{item.source_schema}.{item.source_table}"
            if item.source_schema and item.source_table
            else item.target
        ),
        "target": item.target,
        "columns": sorted(item.columns),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def as_contained_refusal(
    error: Exception,
    *,
    source_schema: str,
    source_table: str,
    target: str,
    detected_lsn: int | None,
    fingerprint: str,
):
    """Attach the current plan item's source relation to a DML failure."""
    from .errors import SchemaEvolutionRefused

    if isinstance(error, SchemaEvolutionRefused):
        error.source_schema = error.source_schema or source_schema
        error.source_table = error.source_table or source_table
        error.target = error.target or target
        error.detected_lsn = (
            error.detected_lsn if error.detected_lsn is not None else detected_lsn
        )
        error.input_fingerprint = error.input_fingerprint or fingerprint
        error.refusal_origin = error.refusal_origin or "typed_planner"
        return error
    try:
        detail = str(error)
    except Exception:
        detail = "<exception text unavailable>"
    exception_type = f"{type(error).__module__}.{type(error).__qualname__}"
    return SchemaEvolutionRefused(
        f"unrecognised table-scoped materialization failure for "
        f"{source_schema}.{source_table} ({exception_type}): {detail}",
        source_schema=source_schema,
        source_table=source_table,
        target=target,
        detected_lsn=detected_lsn,
        input_fingerprint=fingerprint,
        refusal_origin="typed_planner",
    )


def mark_blocked_event(plan, qualified: str, target: str, refused, original: Exception) -> None:
    """Remove an event already covered by a durable table refusal."""
    plan._contained_tables.add(qualified)
    plan.blocked_tables.add(qualified)
    plan.stats["contained_events"] += 1
    plan.stats["contained_tables"].add(qualified)
    plan.stats["quarantined_events"] += 1
    plan.work.pop(target, None)
    if target in plan.created_in_txn:
        try:
            plan.con.execute(
                f"DROP TABLE IF EXISTS {naming.quote(plan.registry.dataset)}."
                f"{naming.quote(target)}"
            )
        except Exception as cleanup_error:
            raise original from cleanup_error
        plan.registry.forget(target)
        plan.created_in_txn.discard(target)
    plan.source_tables.discard(qualified)
    plan.stats["tables"].discard(target)
    plan.column_presence = [row for row in plan.column_presence if row[0] != target]
    plan.created_tables.pop(target, None)
    plan._failed_snapshot_targets.add(target)
    plan._swaps = [
        state
        for state in plan._swaps
        if state.target != target and state.shadow != target
    ]
    if plan._contain_table_failure is None:
        raise original
    plan._contain_table_failure(refused, original)


def contain_table_failure(applier, refused, original) -> None:
    """Persist a table-scoped materialization failure in the open group."""
    if not refused.source_schema or not refused.source_table:
        # There is no honest table to quarantine.  Keep an unscoped failure loud;
        # the commit protocol will roll back the whole group and the supervisor
        # will report it as a run-level failure.
        raise original
    qualified = f"{refused.source_schema}.{refused.source_table}"
    prior_state = destination.schema_refusal_state(
        applier.con,
        pipeline=applier.pipeline,
        source_schema=refused.source_schema,
        source_table=refused.source_table,
        control_schema=applier.control_schema,
    )
    spill_refusal.record_schema_refusal(
        applier,
        refused,
        transaction_open=True,
        deferred_alerts=applier.group.pending_alerts,
    )
    refused.refusal_recorded = True
    applier.blocked_schema_tables.add(qualified)
    try:
        detail = str(original)
    except Exception:
        detail = "<exception text unavailable>"
    exception_type = f"{type(original).__module__}.{type(original).__qualname__}"
    fingerprint = refused.input_fingerprint or qualified
    if not destination.alert_marker_exists(
        applier.con,
        pipeline=applier.pipeline,
        code="table_exception_contained",
        marker_key="input_fingerprint",
        marker_value=fingerprint,
        control_schema=applier.control_schema,
    ):
        applier.group.pending_alerts.append(
            {
                "severity": "critical",
                "code": "table_exception_contained",
                "message": (
                    f"{qualified} was contained after an unrecognised "
                    f"table-scoped materialization failure ({exception_type}); "
                    "the healthy tables in the source transaction remain eligible"
                ),
                "context": {
                    "source_relation": qualified,
                    "target_table": refused.target,
                    "input_fingerprint": fingerprint,
                    "exception_type": exception_type,
                    "exception_message": detail,
                    "run_not_ok": True,
                },
            }
        )
    acknowledged = (
        prior_state == destination.REFUSAL_QUARANTINED
        and qualified in applier.cfg.acknowledged_quarantines
    )
    if acknowledged:
        applier._acknowledged_quarantines.add(qualified)
        log.warning(
            "operator acknowledged stale quarantined relation %s; it remains "
            "blocked until a complete resnapshot resolves it",
            qualified,
        )
    applier._contained_failures.append(
        {
            "source_relation": qualified,
            "target_table": refused.target,
            "exception_type": exception_type,
            "exception_message": detail,
            "input_fingerprint": fingerprint,
            "refusal_state": destination.REFUSAL_PENDING,
            "acknowledged": acknowledged,
        }
    )
    # Preserve fail-loud semantics without interrupting this source transaction's
    # healthy peer.  The supervisor turns the non-None cause into a non-zero run
    # outcome after the commit/slot-advance path has completed.
    if applier.error is None and not acknowledged:
        applier.error = refused


def contain_destination_failure(
    applier,
    refused,
    original: Exception,
    *,
    destination_execution: bool = True,
) -> str:
    """Record a failure after destination rollback, using the independent sink."""
    if not refused.source_schema or not refused.source_table:
        raise original
    sink = applier.alerts._sink if applier.alerts.independent else None
    if sink is None:
        raise original
    spill_refusal.record_schema_refusal(
        applier,
        refused,
        connection=sink,
    )
    refused.refusal_recorded = True
    qualified = f"{refused.source_schema}.{refused.source_table}"
    applier.blocked_schema_tables.add(qualified)
    try:
        detail = str(original)
    except Exception:
        detail = "<exception text unavailable>"
    exception_type = f"{type(original).__module__}.{type(original).__qualname__}"
    fingerprint = refused.input_fingerprint or qualified
    if not destination.alert_marker_exists(
        sink,
        pipeline=applier.pipeline,
        code="table_exception_contained",
        marker_key="input_fingerprint",
        marker_value=fingerprint,
        control_schema=applier.control_schema,
    ):
        origin = "destination" if destination_execution else "Python/materializer"
        applier.alerts.raise_alert(
            severity="critical",
            code="table_exception_contained",
            message=(
                f"{qualified} was contained after the {origin} raised "
                f"{exception_type}; healthy source tables were replayed and "
                "the run remains NOT-OK"
            ),
            context={
                "source_relation": qualified,
                "target_table": refused.target,
                "input_fingerprint": fingerprint,
                "exception_type": exception_type,
                "exception_message": detail,
                "destination_execution": destination_execution,
                "run_not_ok": True,
            },
        )
    applier._contained_failures.append(
        {
            "source_relation": qualified,
            "target_table": refused.target,
            "exception_type": exception_type,
            "exception_message": detail,
            "input_fingerprint": fingerprint,
            "refusal_state": destination.REFUSAL_PENDING,
            "destination_execution": destination_execution,
        }
    )
    if applier.error is None:
        applier.error = refused
    return qualified
