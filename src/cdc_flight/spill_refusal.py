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


def _capture_source_fingerprint(applier, refused: SchemaEvolutionRefused) -> None:
    """Capture the current source schema before durable quarantine.

    Some schema refusals are raised by the destination schema phase, after the
    catalog watcher has already supplied the event but before that exception has a
    relation fingerprint attached.  The quarantine retry gate needs a baseline to
    distinguish a real source-schema repair from an unchanged retry.  Read the
    source relation here while the watcher connection is still available; an
    unavailable read deliberately leaves the field unknown and therefore blocks
    automatic reactivation.
    """
    if refused.source_fingerprint or not refused.source_schema or not refused.source_table:
        return
    catalog = getattr(applier, "catalog", None)
    dsn = getattr(catalog, "dsn", None)
    if not dsn:
        return
    from .catalog_descriptors import source_relation_fingerprint

    exists, fingerprint = source_relation_fingerprint(
        dsn, refused.source_schema, refused.source_table
    )
    if exists and fingerprint:
        refused.source_fingerprint = fingerprint


def record_schema_refusal(
    applier,
    refused: SchemaEvolutionRefused,
    *,
    transaction_open: bool = False,
    deferred_alerts: list[dict] | None = None,
) -> None:
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

    _capture_source_fingerprint(applier, refused)
    destination.record_schema_refusal(
        applier.con,
        pipeline=applier.pipeline,
        source_schema=refused.source_schema,
        source_table=refused.source_table,
        target_table=refused.target,
        detected_lsn=refused.detected_lsn,
        reason=str(refused),
        input_fingerprint=refused.input_fingerprint,
        source_fingerprint=refused.source_fingerprint,
        control_schema=applier.control_schema,
        transaction_open=transaction_open,
        deferred_alerts=deferred_alerts,
    )
    refused.refusal_recorded = True


def handle(applier, refused: SchemaEvolutionRefused, events) -> None:
    """Rollback the spill transaction, then durably queue automatic recovery."""
    candidates = {
        (item.schema, item.table, item.qualified_table)
        for item in events
        if item.schema and item.table
    }
    if not refused.source_schema and not refused.source_table:
        if len(refused.source_tables) == 1:
            refused.source_schema, refused.source_table, refused.target = (
                refused.source_tables[0]
            )
        elif len(candidates) == 1:
            refused.source_schema, refused.source_table, refused.target = (
                next(iter(candidates))
            )
    if candidates:
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
