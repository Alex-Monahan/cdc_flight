"""Durable recovery for descriptor failures at the spill boundary."""

from __future__ import annotations

import logging

from .errors import SchemaEvolutionRefused

log = logging.getLogger("cdc_flight.spill_refusal")


def record_schema_refusal(applier, refused: SchemaEvolutionRefused) -> None:
    if not refused.source_schema or not refused.source_table:
        return
    from . import destination

    destination.record_schema_refusal(
        applier.con,
        pipeline=applier.pipeline,
        source_schema=refused.source_schema,
        source_table=refused.source_table,
        target_table=refused.target,
        detected_lsn=refused.detected_lsn,
        reason=str(refused),
        input_fingerprint=refused.input_fingerprint,
        control_schema=applier.control_schema,
    )


def handle(applier, refused: SchemaEvolutionRefused, events) -> None:
    """Rollback the spill transaction, then durably queue automatic recovery."""
    event = next((item for item in events if item.schema and item.table), None)
    if event is not None:
        refused.source_schema = refused.source_schema or event.schema
        refused.source_table = refused.source_table or event.table
        refused.target = refused.target or event.qualified_table
        lsns = [int(item.lsn) for item in events if item.lsn is not None]
        if refused.detected_lsn is None and lsns:
            refused.detected_lsn = max(lsns)
    applier._rollback_quietly()
    record_schema_refusal(applier, refused)
    if refused.source_schema and refused.source_table and refused.target:
        queued = applier.alerts.request_snapshot(
            pipeline=applier.pipeline,
            schema=refused.source_schema,
            table=refused.source_table,
            target=refused.target,
        )
        if not queued:
            log.error(
                "descriptor refusal for %s.%s was recorded but its independent "
                "re-snapshot request could not be queued",
                refused.source_schema,
                refused.source_table,
            )


__all__ = ["handle", "record_schema_refusal"]
