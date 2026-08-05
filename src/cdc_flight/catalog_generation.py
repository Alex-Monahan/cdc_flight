"""Pure generation identities, proof values and catalog-state constructors.

PostgreSQL's relation OID is not enough to identify a source lifecycle: it is a
four-byte value and can be reused.  The durable token is therefore ``(oid,
relfilenode, reltype)``.  ``relfilenode`` is zero for partitioned parents, so the
row-type OID completes that otherwise ambiguous case.  The physical token is
nullable only for legacy destination rows and test doubles; a real source proof
always carries it.

This module deliberately does not mutate ``CatalogWatcher``.  The watcher owns its
change queue and exposes the small state-changing methods used by the coordinator.
Keeping the generation code pure makes the proof consumed by observation, planning
and admission one value with one set of classifications, rather than a second state
owner reaching into watcher private fields.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass, replace

from .catalog_state import CHANGE_DROPPED, CHANGE_RECREATED, CatalogChange
from .machines import LIVE_CHANGE_STATES


class _Unknown:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "UNKNOWN"


UNKNOWN = _Unknown()

GENERATION_CURRENT = "current"
GENERATION_NEWER = "newer"
GENERATION_ABSENT = "absent"
GENERATION_UNKNOWN = "unknown"
GENERATION_AMBIGUOUS = "ambiguous"
GENERATION_BOUNDARY_UNPROVEN = "boundary_unproven"

# The matrix tests derive their proof dimension from this declaration.  A source
# read that cannot prove the token or its WAL coverage is never silently current.
GENERATION_PROOF_STATES = (
    GENERATION_CURRENT,
    GENERATION_NEWER,
    GENERATION_ABSENT,
    GENERATION_UNKNOWN,
    GENERATION_AMBIGUOUS,
    GENERATION_BOUNDARY_UNPROVEN,
)


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
        # PostgreSQL reports 0 for partitioned parents. Their row type is the
        # additional generation discriminator; ordinary relations need only their
        # nonzero physical file. Legacy values remain incomplete and fail closed when
        # a real proof must distinguish them.
        return self.relfilenode is not None and (
            self.relfilenode != 0 or self.reltype_oid is not None
        )


@dataclass(frozen=True)
class GenerationProof:
    """One source identity observation and its source-WAL coverage."""

    identity: RelationIdentity | None = None
    source_lsn: int | None = None
    error: str | None = None
    #: Bare integer readers are retained only as a compatibility shape for tests and
    #: old embedders. Production ``CatalogWatcher`` proofs are never legacy.
    legacy: bool = False

    @classmethod
    def unknown(cls, error: str) -> GenerationProof:
        return cls(error=error)


@dataclass(frozen=True)
class GenerationCheck:
    """The result of comparing a planned generation with one proof."""

    state: str
    current_oid: object
    current_identity: RelationIdentity | None = None
    source_lsn: int | None = None


@dataclass
class GenerationProofLease:
    """A proof plus any source lock held until the destination commit boundary."""

    proofs: dict[str, GenerationProof]
    _release: Callable[[], None] | None = None
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        if self._release is not None:
            with contextlib.suppress(Exception):
                self._release()

    def __enter__(self) -> GenerationProofLease:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()


def identity_for(relation) -> RelationIdentity:
    """Extract a token from a ``SourceRelation``-shaped value."""
    filenode = getattr(relation, "relfilenode", None)
    type_oid = getattr(relation, "relation_type_oid", None)
    return RelationIdentity(relation.oid, filenode, type_oid)


def with_identity(relation, identity: RelationIdentity):
    """Project a watcher relation shape onto a newly proven token."""
    return replace(
        relation,
        oid=identity.oid,
        relfilenode=identity.relfilenode,
        relation_type_oid=identity.reltype_oid,
    )


def coerce_identity(value) -> RelationIdentity | None:
    if value is UNKNOWN or value is None:
        return None
    if isinstance(value, RelationIdentity):
        return value
    if isinstance(value, GenerationProof):
        return value.identity
    if isinstance(value, (tuple, list)) and len(value) in {2, 3}:
        return RelationIdentity(value[0], value[1], value[2] if len(value) == 3 else None)
    if hasattr(value, "oid"):
        return identity_for(value)
    if isinstance(value, int):
        return RelationIdentity(value)
    return None


def coerce_proof(value) -> GenerationProof:
    if isinstance(value, GenerationProof):
        return value
    if value is UNKNOWN:
        return GenerationProof.unknown("the source generation could not be read")
    if value is None:
        return GenerationProof()
    if isinstance(value, (RelationIdentity, tuple, list)):
        return GenerationProof(identity=coerce_identity(value), legacy=False)
    if isinstance(value, int):
        return GenerationProof(identity=RelationIdentity(value), legacy=True)
    identity = coerce_identity(value)
    if identity is not None:
        return GenerationProof(identity=identity, legacy=True)
    return GenerationProof.unknown(f"unsupported source generation value {value!r}")


def normalize_proofs(values: dict[str, object], names) -> dict[str, GenerationProof]:
    return {name: coerce_proof(values.get(name, UNKNOWN)) for name in names}


def check(
    expected, current, *, minimum_lsn: int | None = None
) -> GenerationCheck:
    """Classify a proof, failing closed for incomplete or uncovered evidence."""
    proof = coerce_proof(current)
    expected_identity = coerce_identity(expected)
    if proof.error:
        return GenerationCheck(GENERATION_UNKNOWN, current, None, proof.source_lsn)
    if proof.identity is None:
        return GenerationCheck(GENERATION_ABSENT, current, None, proof.source_lsn)
    current_identity = proof.identity
    if expected_identity is None:
        return GenerationCheck(
            GENERATION_AMBIGUOUS, current, current_identity, proof.source_lsn
        )

    if minimum_lsn is not None:
        if proof.source_lsn is None and not proof.legacy:
            return GenerationCheck(
                GENERATION_BOUNDARY_UNPROVEN,
                current,
                current_identity,
                proof.source_lsn,
            )
        if proof.source_lsn is not None and proof.source_lsn < int(minimum_lsn):
            return GenerationCheck(
                GENERATION_BOUNDARY_UNPROVEN,
                current,
                current_identity,
                proof.source_lsn,
            )

    same_oid = expected_identity.oid == current_identity.oid
    same_filenode = expected_identity.relfilenode == current_identity.relfilenode
    same_type = expected_identity.reltype_oid == current_identity.reltype_oid
    if same_oid and same_filenode and same_type:
        if (
            not proof.legacy
            and (not expected_identity.complete or not current_identity.complete)
        ):
            return GenerationCheck(
                GENERATION_AMBIGUOUS, current, current_identity, proof.source_lsn
            )
        return GenerationCheck(
            GENERATION_CURRENT, current, current_identity, proof.source_lsn
        )
    if same_oid and same_filenode and not same_type:
        # A nonzero relfilenode is already a complete physical-generation proof, so a
        # missing row-type value on one side is legacy-compatible. With relfilenode=0,
        # however, a changed or missing row type is genuinely ambiguous/newer.
        if expected_identity.relfilenode not in (None, 0) and (
            expected_identity.reltype_oid is None or current_identity.reltype_oid is None
        ):
            return GenerationCheck(
                GENERATION_CURRENT, current, current_identity, proof.source_lsn
            )
        state = (
            GENERATION_NEWER
            if expected_identity.complete and current_identity.complete
            else GENERATION_AMBIGUOUS
        )
        return GenerationCheck(state, current, current_identity, proof.source_lsn)
    if same_oid and expected_identity.relfilenode != current_identity.relfilenode:
        # A complete token versus a legacy token is not an equality proof. Two
        # complete tokens with different relfilenodes are a newer generation.
        state = (
            GENERATION_AMBIGUOUS
            if not expected_identity.complete or not current_identity.complete
            else GENERATION_NEWER
        )
        return GenerationCheck(state, current, current_identity, proof.source_lsn)
    if not same_oid:
        return GenerationCheck(GENERATION_NEWER, current, current_identity, proof.source_lsn)

    if not expected_identity.complete and not current_identity.complete and not proof.legacy:
        return GenerationCheck(
            GENERATION_AMBIGUOUS, current, current_identity, proof.source_lsn
        )

    return GenerationCheck(GENERATION_CURRENT, current, current_identity, proof.source_lsn)


def identities_equal(left, right) -> bool:
    return identity_for(left) == identity_for(right)


def has_newer_recreate(changes, current) -> bool:
    return any(
        check(
            getattr(change, "new_identity", None) or change.new_oid,
            current,
        ).state
        == GENERATION_NEWER
        for change in changes
    )


def pending_for(changes, known, qualified: str, previous):
    """Return live recreate/drop work and the relation whose image is retained."""
    live = [change for change in changes if change.state in LIVE_CHANGE_STATES]
    recreates = [
        change for change in live
        if change.qualified == qualified and change.kind == CHANGE_RECREATED
    ]
    drop = next(
        (
            change for change in live
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
