"""Generation decisions shared by catalog observation, planning and admission.

The source relation OID is the generation token available to this pipeline.  The
ordering is the ordering of observations: once a queued recreate recorded one OID,
an immediately re-read present relation with a different OID is the newer generation
for that queued action.  The OID is not treated as a globally increasing integer;
PostgreSQL can wrap and reuse its four-byte OID values.  That same-OID lifecycle is a
documented boundary of this identity model (ADR 0001, A72; R8-M3).
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from .catalog_state import CHANGE_DROPPED, CHANGE_RECREATED, CatalogChange
from .machines import CHANGE_APPLIED, CHANGE_DUE, CHANGE_SUPERSEDED, LIVE_CHANGE_STATES


class _Unknown:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "UNKNOWN"


UNKNOWN = _Unknown()

GENERATION_CURRENT = "current"
GENERATION_NEWER = "newer"
GENERATION_ABSENT = "absent"
GENERATION_UNKNOWN = "unknown"


@dataclass(frozen=True)
class GenerationCheck:
    """The result of comparing a queued recreate with one source read."""

    state: str
    current_oid: object


def check(expected_oid: int | None, current_oid: object) -> GenerationCheck:
    """Classify the current source fact for a queued recreate.

    ``UNKNOWN`` is deliberately distinct from ``None``: a failed source read cannot
    be interpreted as a drop.  Any present OID different from the queued OID is newer
    in the observation order; no numeric comparison is made because OID wraparound is
    not an ordering signal.
    """
    if current_oid is UNKNOWN:
        return GenerationCheck(GENERATION_UNKNOWN, current_oid)
    if current_oid is None:
        return GenerationCheck(GENERATION_ABSENT, current_oid)
    if expected_oid is not None and int(current_oid) == int(expected_oid):
        return GenerationCheck(GENERATION_CURRENT, current_oid)
    return GenerationCheck(GENERATION_NEWER, current_oid)


def has_newer_recreate(changes, current_oid: int) -> bool:
    """Whether a source observation supersedes any queued recreate."""
    return any(
        check(change.new_oid, current_oid).state == GENERATION_NEWER
        for change in changes
    )


def pending_for(watcher, qualified: str, previous):
    """Return live recreate/drop work and the relation whose image is retained."""
    recreates = [
        change
        for change in watcher._live()
        if change.qualified == qualified and change.kind == CHANGE_RECREATED
    ]
    drop = next(
        (
            change
            for change in watcher._live()
            if change.qualified == qualified and change.kind == CHANGE_DROPPED
        ),
        None,
    )
    candidate = recreates[0] if recreates else drop or previous
    return recreates, retained_relation(watcher, candidate)


def retained_relation(watcher, change):
    """Return the relation whose physical destination image is still retained."""
    if hasattr(change, "old_relation"):
        relation = change.old_relation or watcher.known.get(change.qualified)
        relation = relation or change.new_relation
        old_oid = change.old_oid
    else:
        relation = change
        old_oid = None
    if relation is not None and old_oid is not None:
        relation = replace(relation, oid=int(old_oid))
    return relation


def dropped_change(watcher, relation, lsn: int):
    return watcher._change(
        CHANGE_DROPPED,
        relation,
        lsn,
        old_oid=relation.oid,
        old_relation=relation,
    )


def recreated_change(watcher, current, old_relation, lsn: int):
    return watcher._change(
        CHANGE_RECREATED,
        current,
        lsn,
        old_oid=old_relation.oid,
        new_oid=current.oid,
        old_relation=old_relation,
    )


def supersede_recreated(watcher, change, current_oid: int):
    """Replace a stale recreate with the only live action for ``current_oid``.

    The old object reaches ``superseded`` and the physical old image is carried to the
    replacement. A real watcher has the full relation shape; a hand-built legacy
    change without one is refused by the caller rather than guessed.
    """
    with watcher._lock:
        old_relation = retained_relation(watcher, change)
        relation = watcher.known.get(change.qualified) or change.new_relation
        current = replace(relation, oid=int(current_oid)) if relation else None
        if current is None:
            return None
        if change.state not in {CHANGE_APPLIED, CHANGE_SUPERSEDED}:
            change.to(CHANGE_SUPERSEDED)
            watcher.superseded += 1
        watcher._changes = [
            item for item in watcher._changes if item.state in LIVE_CHANGE_STATES
        ]
        watcher.known[change.qualified] = current
        watcher._dirty[change.qualified] = current
        replacement = CatalogChange(
            kind=CHANGE_RECREATED,
            schema=change.schema,
            table=change.table,
            detected_lsn=change.detected_lsn,
            old_oid=old_relation.oid if old_relation else change.old_oid,
            new_oid=int(current_oid),
            old_relation=old_relation,
            new_relation=current,
            state=CHANGE_DUE,
        )
        watcher._changes.append(replacement)
        return replacement


def reclassify_recreated_as_drop(watcher, change):
    """Replace a stale recreate with a due final-drop observation."""
    with watcher._lock:
        old_relation = retained_relation(watcher, change)
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
            old_oid=old_relation.oid if old_relation else change.old_oid,
            old_relation=old_relation,
            new_relation=old_relation,
            state=CHANGE_DUE,
        )
        watcher._changes.append(replacement)
        return replacement
