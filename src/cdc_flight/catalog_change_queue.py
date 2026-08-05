"""Watcher-owned queue mutations used by final generation planning."""

from __future__ import annotations

from . import catalog_generation
from .catalog_state import CHANGE_DROPPED, CHANGE_RECREATED, CatalogChange
from .machines import CHANGE_APPLIED, CHANGE_DUE, CHANGE_SUPERSEDED, LIVE_CHANGE_STATES


def make_change(watcher, kind, relation, lsn: int, **oids) -> CatalogChange:
    oids.setdefault("new_relation", relation)
    if relation is not None:
        oids.setdefault("new_identity", catalog_generation.identity_for(relation))
    old_relation = oids.get("old_relation")
    if old_relation is not None:
        oids.setdefault("old_identity", catalog_generation.identity_for(old_relation))
    return CatalogChange(
        kind=kind, schema=relation.schema, table=relation.table, detected_lsn=lsn, **oids
    )


def supersede_recreated(watcher, change: CatalogChange, current) -> CatalogChange | None:
    with watcher._lock:
        old_relation = catalog_generation.retained_relation(change, watcher.known)
        relation = watcher.known.get(change.qualified) or change.new_relation
        identity = catalog_generation.coerce_identity(current)
        if relation is None or identity is None:
            return None
        current_relation = catalog_generation.with_identity(relation, identity)
        if change.state not in {CHANGE_APPLIED, CHANGE_SUPERSEDED}:
            change.to(CHANGE_SUPERSEDED)
            watcher.superseded += 1
        watcher._changes = [
            item for item in watcher._changes if item.state in LIVE_CHANGE_STATES
        ]
        watcher.known[change.qualified] = current_relation
        watcher._dirty[change.qualified] = current_relation
        replacement = CatalogChange(
            kind=CHANGE_RECREATED,
            schema=change.schema,
            table=change.table,
            detected_lsn=change.detected_lsn,
            old_oid=(old_relation.oid if old_relation else change.old_oid),
            new_oid=identity.oid,
            old_identity=(
                catalog_generation.identity_for(old_relation)
                if old_relation
                else change.old_identity
            ),
            new_identity=identity,
            old_relation=old_relation,
            new_relation=current_relation,
            state=CHANGE_DUE,
        )
        watcher._changes.append(replacement)
        return replacement


def reclassify_recreated_as_drop(watcher, change: CatalogChange) -> CatalogChange:
    with watcher._lock:
        old_relation = catalog_generation.retained_relation(change, watcher.known)
        if change.state not in {CHANGE_APPLIED, CHANGE_SUPERSEDED}:
            change.to(CHANGE_SUPERSEDED)
            watcher.superseded += 1
        watcher._changes = [
            item for item in watcher._changes if item.state in LIVE_CHANGE_STATES
        ]
        if old_relation is None:
            watcher.known.pop(change.qualified, None)
            watcher._dirty.pop(change.qualified, None)
        else:
            watcher.known[change.qualified] = old_relation
            watcher._dirty[change.qualified] = old_relation
        replacement = CatalogChange(
            kind=CHANGE_DROPPED,
            schema=change.schema,
            table=change.table,
            detected_lsn=change.detected_lsn,
            old_oid=(old_relation.oid if old_relation else change.old_oid),
            old_identity=(
                catalog_generation.identity_for(old_relation)
                if old_relation
                else change.old_identity
            ),
            old_relation=old_relation,
            new_relation=old_relation,
            state=CHANGE_DUE,
        )
        watcher._changes.append(replacement)
        return replacement
