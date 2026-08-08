"""The source-relation registry: what the catalog watcher learned, made durable.

Split out of `destination.py`, which crossed the thermo-nuclear review's 1,000-line
giant-file threshold. This is a coherent piece rather than an arbitrary cut: three
functions, one table, one job — `_cdc_flight.source_relations` is the **only** thing that
makes a `DROP TABLE` or a drop-and-recreate detectable across a restart, because the
persisted `(relation_oid, relation_filenode, relation_type_oid)` token is what the
next run compares against.

That is also why the flush below exists at all. The registry used to be written
exclusively inside a commit group, so a run that committed **no groups** persisted
nothing it had learned — and after an offline drop-and-recreate the next run accepted the
replacement generation token as though it had always owned that relation, leaving the old relation's
rows beside the new one's for ever (Codex r3 BLOCKER-1, reproduced end to end).
"""

from __future__ import annotations

import contextlib
import json
import logging

from .control_schema import CONTROL_SCHEMA
from .machines import require_admission_state

log = logging.getLogger("cdc_flight.source_relations")

__all__ = ["flush_learned_relations", "forget_source_relation", "upsert_source_relation"]


def _now():
    from .destination import now

    return now()


def upsert_source_relation(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    relation_oid: int,
    relation_filenode: int | None = None,
    relation_type_oid: int | None = None,
    published: bool,
    replica_identity: str | None,
    admission_state: str = "external",
    columns=(),
) -> None:
    """Record what the source catalog says, inside the commit group's transaction.

    DELETE + INSERT rather than an upsert: the destination is DuckDB/MotherDuck and
    this is the same pattern `write_resume_point` uses, so there is one idiom for
    "replace this row" in the whole control schema.
    """
    admission_state = require_admission_state(admission_state)
    first_seen = con.execute(
        f"SELECT first_seen_at FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchall()
    current = _now()
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.source_relations "
        "(pipeline, source_schema, source_table, relation_oid, relation_filenode, "
        " relation_type_oid, published, admission_state, replica_identity, "
        " columns_json, first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            pipeline, source_schema, source_table, relation_oid, relation_filenode,
            relation_type_oid, published,
            admission_state,
            replica_identity,
            json.dumps(
                [
                    {
                        "attnum": column.attnum,
                        "name": column.name,
                        "type_oid": column.type_oid,
                        "type_name": column.type_name,
                        "typmod": column.typmod,
                        "descriptor": (
                            column.descriptor.to_dict() if column.descriptor is not None else None
                        ),
                        "nullable": column.nullable,
                        "has_missing_default": column.has_missing_default,
                        "missing_value_text": (
                            str(column.missing_value)
                            if column.has_missing_default and column.missing_value is not None
                            else None
                        ),
                    }
                    for column in columns
                ],
                sort_keys=True,
            ),
            (first_seen[0][0] if first_seen else current), current,
        ],
    )


def forget_source_relation(con, *, pipeline: str, source_schema: str, source_table: str) -> None:
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )


def flush_learned_relations(
    con, *, pipeline: str, catalog, exclude: set[str] | None = None
) -> list[str]:
    """Persist what the catalog watcher learned, in its own transaction. Returns names.

    `source_relations` is the ONLY thing that makes a `DROP TABLE` or a
    drop-and-recreate detectable across a restart: without the persisted generation
    token the next run has nothing to compare against. It was written
    exclusively through `CatalogCoordinator.apply()`, which runs inside a commit group —
    so a run that committed **no groups at all** persisted nothing, and everything the
    watcher had learned vanished at shutdown (Codex r3 BLOCKER-1). The measured
    consequence: a quiet run, then an offline drop-and-recreate, and the next run
    accepts the replacement token as though it had always owned that relation — leaving
    the old relation's rows beside the new one's, permanently, because from then on the
    persisted oid agrees with the source.

    Called once per run, **after** the watcher has been quiesced, so a poll cannot add
    dirty state the flush will not see. The `exclude` guard is the same one the commit
    path uses and it is not optional: a persisted row carrying the NEW oid of a relation
    whose destructive action is still pending would make the next run agree with the
    source and never notice the drop at all.
    """
    if catalog is None:
        return []
    # Two independent reasons a learned oid must not become history yet, and they are
    # the same rule: **persisted state may not run ahead of the action it implies.**
    #
    # 1. a fenced action for this relation is still pending (table DDL or schema DDL);
    # 2. the catalog baseline could not relate this relation's destination rows to any
    #    identity at the source and nothing has rebuilt it yet (Codex r6 BLOCKER-2).
    #    Writing the observed oid here would make the NEXT run agree with the source and
    #    never ask again — the same silent inconsistency one run later, reached through
    #    a failing run rather than a successful one.
    pending_fenced = (
        catalog.pending_fenced()
        if hasattr(catalog, "pending_fenced")
        else catalog.pending_destructive()
    )
    blocked = {c.qualified for c in pending_fenced} | set(exclude or ())
    relations = catalog.dirty(exclude=blocked)
    if not relations:
        return []
    con.execute("BEGIN TRANSACTION")
    try:
        for relation in relations:
            upsert_source_relation(
                con,
                pipeline=pipeline,
                source_schema=relation.schema,
                source_table=relation.table,
                relation_oid=relation.oid,
                relation_filenode=relation.relfilenode,
                relation_type_oid=relation.relation_type_oid,
                published=relation.published,
                replica_identity=relation.replica_identity,
                admission_state=require_admission_state(relation.admission_state),
                columns=relation.columns,
            )
        con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise
    names = [relation.qualified for relation in relations]
    catalog.clear_dirty(names)
    log.info("persisted %s learned source relation(s): %s", len(names), ", ".join(names))
    return names
