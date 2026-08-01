"""The source-relation registry: what the catalog watcher learned, made durable.

Split out of `destination.py`, which crossed the thermo-nuclear review's 1,000-line
giant-file threshold. This is a coherent piece rather than an arbitrary cut: three
functions, one table, one job — `_cdc_flight.source_relations` is the **only** thing that
makes a `DROP TABLE` or a drop-and-recreate detectable across a restart, because the
persisted `relation_oid` is what the next run compares against.

That is also why the flush below exists at all. The registry used to be written
exclusively inside a commit group, so a run that committed **no groups** persisted
nothing it had learned — and after an offline drop-and-recreate the next run accepted the
replacement oid as though it had always owned that relation, leaving the old relation's
rows beside the new one's for ever (Codex r3 BLOCKER-1, reproduced end to end).
"""

from __future__ import annotations

import contextlib
import logging

from .control_schema import CONTROL_SCHEMA

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
    published: bool,
    replica_identity: str | None,
) -> None:
    """Record what the source catalog says, inside the commit group's transaction.

    DELETE + INSERT rather than an upsert: the destination is DuckDB/MotherDuck and
    this is the same pattern `write_resume_point` uses, so there is one idiom for
    "replace this row" in the whole control schema.
    """
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
        "(pipeline, source_schema, source_table, relation_oid, published, "
        " replica_identity, first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
        [
            pipeline, source_schema, source_table, relation_oid, published,
            replica_identity, (first_seen[0][0] if first_seen else current), current,
        ],
    )


def forget_source_relation(con, *, pipeline: str, source_schema: str, source_table: str) -> None:
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )


def flush_learned_relations(con, *, pipeline: str, catalog) -> list[str]:
    """Persist what the catalog watcher learned, in its own transaction. Returns names.

    `source_relations` is the ONLY thing that makes a `DROP TABLE` or a
    drop-and-recreate detectable across a restart: without the persisted
    `relation_oid` the next run has nothing to compare against. It was written
    exclusively through `CatalogCoordinator.apply()`, which runs inside a commit group —
    so a run that committed **no groups at all** persisted nothing, and everything the
    watcher had learned vanished at shutdown (Codex r3 BLOCKER-1). The measured
    consequence: a quiet run, then an offline drop-and-recreate, and the next run
    accepts the replacement oid as though it had always owned that relation — leaving
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
    blocked = {c.qualified for c in catalog.pending_destructive()}
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
                published=relation.published,
                replica_identity=relation.replica_identity,
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
