"""Table-scoped failure persistence for the transactional applier.

The applier owns lifecycle state, but these two boundaries are deliberately kept
small and separate from the commit loop.  A destination execution error is first
rolled back by the commit protocol and then recorded on an independent sink;
healthy relations are replayed with only the failed relation excluded.
"""

from __future__ import annotations

import logging

from . import destination, spill_refusal

log = logging.getLogger("cdc_flight.failure_containment")


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
