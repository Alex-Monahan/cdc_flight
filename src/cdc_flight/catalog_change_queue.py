"""Watcher-owned queue mutations used by final generation planning."""

from __future__ import annotations

import logging

from . import catalog_generation
from .catalog_state import CHANGE_RECREATED, CatalogChange
from .machines import (
    CHANGE_APPLIED,
    CHANGE_DUE,
    CHANGE_SUPERSEDED,
    CHANGE_UNCONFIRMED,
    LIVE_CHANGE_STATES,
)

log = logging.getLogger("cdc_flight.catalog")


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


def settle(
    watcher, changes: list[CatalogChange], planned_epoch: int | None = None
) -> None:
    """Settle a committed plan even if the poller superseded it after due()."""
    with watcher._lock:
        advanced = planned_epoch is not None and watcher._epoch > planned_epoch
        if advanced:
            log.debug(
                "settling catalog plan from epoch %s after watcher advanced to %s",
                planned_epoch,
                watcher._epoch,
            )
        done = {
            id(change)
            for change in changes
            if change.state in {CHANGE_DUE, CHANGE_SUPERSEDED, CHANGE_APPLIED}
        }
        for change in changes:
            if change.state in {CHANGE_DUE, CHANGE_SUPERSEDED}:
                change.to(CHANGE_APPLIED)
        if done:
            watcher._changes = [
                change for change in watcher._changes if id(change) not in done
            ]


def clear_dirty_if_current(
    watcher, relations, planned_epoch: int | None = None
) -> None:
    """Clear only the source observations represented by a committed plan."""
    with watcher._lock:
        advanced = planned_epoch is not None and watcher._epoch > planned_epoch
        for relation in relations:
            name = relation.qualified
            current = watcher._dirty.get(name)
            if current is None:
                continue
            if current is relation or current == relation:
                watcher._dirty.pop(name, None)
                continue
            if not advanced and catalog_generation.identities_equal(current, relation):
                watcher._dirty.pop(name, None)


def confirm(watcher, name: str, change: CatalogChange) -> CatalogChange | None:
    """Carry one confirmation object through the complete lifecycle-token streak."""
    def identity_shape(candidate: CatalogChange) -> tuple:
        identity = catalog_generation.coerce_identity(
            candidate.new_identity or candidate.new_oid
        )
        return (
            identity.oid,
            identity.relfilenode,
            identity.reltype_oid,
        ) if identity is not None else (None, None, None)

    def shape(candidate: CatalogChange) -> tuple:
        return (
            candidate.kind,
            identity_shape(candidate),
            tuple(
                (
                    item.kind,
                    item.attnum,
                    item.old_name,
                    item.new_name,
                    item.type_oid,
                    item.type_name,
                    item.nullable,
                    item.type_changed,
                )
                for item in candidate.column_changes
            ),
        )

    seen = watcher._unconfirmed.get(name)
    if seen is not None and shape(seen) != shape(change):
        seen.to(CHANGE_SUPERSEDED)
        watcher.superseded += 1
        seen = None
    if seen is None:
        change.to(CHANGE_UNCONFIRMED)
        tracked = change
    else:
        tracked = seen
        tracked.confirmations += 1
        tracked.detected_lsn = change.detected_lsn
        tracked.to(CHANGE_UNCONFIRMED)
    if tracked.confirmations < watcher.confirm_polls:
        watcher._unconfirmed[name] = tracked
        log.info(
            "%s observed for %s (%s/%s confirming polls); not queued yet",
            tracked.kind,
            name,
            tracked.confirmations,
            watcher.confirm_polls,
        )
        return None
    watcher._unconfirmed.pop(name, None)
    return tracked
