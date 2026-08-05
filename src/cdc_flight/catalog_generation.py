"""Pure generation identities and catalog-state constructors.

PostgreSQL relation OIDs can be reused.  The durable lifecycle token is therefore
``(oid, relfilenode, reltype_oid)``: ``reltype_oid`` completes the token for
partitioned parents, whose ``relfilenode`` is zero.  This module deliberately has
no source I/O and no commit-time authority.  The asynchronous catalog watcher owns
observation; these helpers compare the durable observations and construct queued
obligations.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from .catalog_state import CHANGE_DROPPED, CHANGE_RECREATED, CatalogChange
from .machines import LIVE_CHANGE_STATES


@dataclass(frozen=True)
class RelationIdentity:
    """The durable source-generation token."""

    oid: int
    relfilenode: int | None = None
    reltype_oid: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "oid", int(self.oid))
        if self.relfilenode is not None:
            object.__setattr__(self, "relfilenode", int(self.relfilenode))
        if self.reltype_oid is not None:
            object.__setattr__(self, "reltype_oid", int(self.reltype_oid))

    @property
    def complete(self) -> bool:
        """Whether this token can distinguish a physical source generation."""
        return self.relfilenode is not None and (
            self.relfilenode != 0 or self.reltype_oid is not None
        )


def identity_for(relation) -> RelationIdentity:
    """Extract a token from a ``SourceRelation``-shaped value."""
    if isinstance(relation, RelationIdentity):
        return relation
    return RelationIdentity(
        relation.oid,
        getattr(relation, "relfilenode", None),
        getattr(relation, "relation_type_oid", None),
    )


def coerce_identity(value) -> RelationIdentity | None:
    """Coerce a durable token, source relation, or legacy integer."""
    if value is None:
        return None
    if isinstance(value, RelationIdentity):
        return value
    if isinstance(value, (tuple, list)) and len(value) in {2, 3}:
        return RelationIdentity(value[0], value[1], value[2] if len(value) == 3 else None)
    if isinstance(value, int):
        return RelationIdentity(value)
    if hasattr(value, "oid"):
        return identity_for(value)
    return None


def with_identity(relation, identity: RelationIdentity):
    """Project a relation shape onto a durable token."""
    return replace(
        relation,
        oid=identity.oid,
        relfilenode=identity.relfilenode,
        relation_type_oid=identity.reltype_oid,
    )


def identities_equal(left, right) -> bool:
    """Compare complete or legacy-compatible tokens without source I/O."""
    left_identity = coerce_identity(left)
    right_identity = coerce_identity(right)
    return left_identity is not None and left_identity == right_identity


def lifecycle_identities_equal(left, right) -> bool:
    """Compare present relations without mistaking TRUNCATE for DROP/CREATE.

    PostgreSQL rewrites ``relfilenode`` for an ordinary TRUNCATE while the relation
    and its row type remain the same.  A real catalog observation includes the row
    type OID, so that complete type identity is the lifecycle discriminator for a
    present relation.  Legacy/test-shaped rows without a type token fail closed to
    the full physical-token comparison, preserving the same-OID guard for incomplete
    historical state.

    The durable token still stores all three fields.  This helper only defines the
    present-relation comparison used by the asynchronous watcher; it is not proof
    that a row from one generation belongs to another.
    """
    left_identity = coerce_identity(left)
    right_identity = coerce_identity(right)
    if left_identity is None or right_identity is None:
        return False
    if left_identity.oid != right_identity.oid:
        return False
    if (
        left_identity.reltype_oid is not None
        and right_identity.reltype_oid is not None
    ):
        return left_identity.reltype_oid == right_identity.reltype_oid
    return left_identity == right_identity


def has_newer_recreate(changes, current) -> bool:
    """Return true when a queued recreate is older than the observed token."""
    current_identity = coerce_identity(current)
    if current_identity is None:
        return False
    for change in changes:
        expected = coerce_identity(
            getattr(change, "new_identity", None) or change.new_oid
        )
        if expected is not None and expected != current_identity:
            return True
    return False


def pending_for(changes, known, qualified: str, previous):
    """Return live recreate/drop work and the relation whose image is retained."""
    live = [change for change in changes if change.state in LIVE_CHANGE_STATES]
    recreates = [
        change
        for change in live
        if change.qualified == qualified and change.kind == CHANGE_RECREATED
    ]
    drop = next(
        (
            change
            for change in live
            if change.qualified == qualified and change.kind == CHANGE_DROPPED
        ),
        None,
    )
    candidate = recreates[0] if recreates else drop or previous
    return recreates, retained_relation(candidate, known)


def retained_relation(change, known):
    """Return the relation whose physical destination image is still retained."""
    if change is None:
        return None
    if hasattr(change, "old_relation"):
        relation = change.old_relation or known.get(change.qualified)
        relation = relation or change.new_relation
        old_identity = getattr(change, "old_identity", None)
        old_identity = old_identity or change.old_oid
    else:
        relation = change
        old_identity = None
    identity = coerce_identity(old_identity)
    if relation is not None and identity is not None:
        relation = with_identity(relation, identity)
    return relation


def dropped_change(relation, lsn: int):
    identity = identity_for(relation)
    return CatalogChange(
        CHANGE_DROPPED,
        relation.schema,
        relation.table,
        lsn,
        old_oid=identity.oid,
        old_identity=identity,
        old_relation=relation,
        new_relation=relation,
    )


def recreated_change(current, old_relation, lsn: int):
    old_identity = identity_for(old_relation) if old_relation is not None else None
    new_identity = identity_for(current)
    return CatalogChange(
        CHANGE_RECREATED,
        current.schema,
        current.table,
        lsn,
        old_oid=old_identity.oid if old_identity else None,
        new_oid=new_identity.oid,
        old_identity=old_identity,
        new_identity=new_identity,
        old_relation=old_relation,
        new_relation=current,
    )
