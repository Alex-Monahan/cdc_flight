"""The common scoped route for every schema/value refusal.

Refusal origins are deliberately broader than the spill boundary: typed value
encoding and descriptor enrichment in ``planner``/``table_work``, schema-fence
classification in ``schema_epoch``/``schema_evolution``, destination DDL and
shadow/UNION construction in ``schema_ddl``/``schema_registry``/``schema_shadow``,
ADD-column reads in ``schema_backfill``/``catalog_apply``, source descriptor and
durable-catalog reads in ``catalog_poll``/``catalog_descriptors``/``catalog_state``,
and spill decoding in ``spill_protocol``.  Commit-time origins enter
``Applier._record_schema_refusal`` through ``commit_protocol``; spill origins enter
``handle``; watcher/startup origins persist through ``catalog`` and
``discovery_coordinator``; snapshot-setup origins enter ``resnapshot``.  All of
those edges call ``destination.record_schema_refusal`` with the same stable scoped
identity, lifecycle mark, alert/quarantine behavior, and re-snapshot obligation.
An origin with no source scope raises a critical unscoped alert and cannot be
reported successful; it is never assigned to an arbitrary table.
"""

from __future__ import annotations

import logging

from .errors import SchemaEvolutionRefused

log = logging.getLogger("cdc_flight.spill_refusal")


def record_schema_refusal(applier, refused: SchemaEvolutionRefused) -> None:
    if not refused.source_schema or not refused.source_table:
        applier.unscoped_refusals += 1
        applier.alerts.raise_alert(
            severity="critical",
            code="schema_refusal_unscoped",
            message=(
                "a schema/value refusal had no source-table context; the run is not "
                "safe to report successful and no table was advanced"
            ),
            context={"reason": str(refused), "refusal_class": refused.refusal_class},
        )
        refused.refusal_recorded = True
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
        refusal_class=refused.refusal_class,
        control_schema=applier.control_schema,
    )
    refused.refusal_recorded = True


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
