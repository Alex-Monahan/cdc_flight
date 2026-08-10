"""Persist schema/value refusals raised before a re-snapshot Applier exists."""

from __future__ import annotations

from . import destination
from .errors import SchemaEvolutionRefused


def cause(error: BaseException) -> SchemaEvolutionRefused | None:
    """Find a scoped refusal through the wrapper chain raised by snapshot setup."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, SchemaEvolutionRefused):
            return current
        current = current.__cause__ or current.__context__
    return None


def persist(
    con,
    *,
    refused: SchemaEvolutionRefused,
    pipeline: str,
    tables: list[tuple[str, str, str]],
    control_schema: str | None,
) -> None:
    """Route setup-time refusals through the common scoped durable writer.

    Descriptor setup can fail before an Applier exists. Providers name every affected
    relation through ``source_tables``; if they cannot, this helper only falls back to
    a single requested table and never guesses a broad scope.
    """
    if refused.refusal_recorded:
        return
    targets = refused.source_tables or ()
    if not targets and refused.source_schema and refused.source_table:
        targets = ((refused.source_schema, refused.source_table, refused.target),)
    if not targets and len(tables) == 1:
        targets = (tables[0],)
    if not targets:
        return
    for schema, table, target in targets:
        refused.source_schema = refused.source_schema or schema
        refused.source_table = refused.source_table or table
        refused.target = refused.target or target
        destination.record_schema_refusal(
            con,
            pipeline=pipeline,
            source_schema=schema,
            source_table=table,
            target_table=target,
            detected_lsn=refused.detected_lsn,
            reason=str(refused),
            input_fingerprint=refused.input_fingerprint,
            refusal_class=refused.refusal_class,
            control_schema=control_schema,
        )
    refused.refusal_recorded = True
