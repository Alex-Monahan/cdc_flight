"""Stable source-event identity and the shared destination-ledger contract.

The ledger is intentionally small and boring.  It records the source facts that
make an event an event, plus a digest of the source payload.  The caller inserts
the row on the *same* DuckDB/MotherDuck transaction as the data mutation.  A
replay after a destination commit therefore observes ``applied`` and becomes a
no-op; a replay whose payload differs is a refusal, never a second interpretation
of the source row.

The normal streaming identity does not contain a table name or a key.  Those are
collision guards stored beside it.  A keyless row consequently cannot acquire a
lineage merely because a Python process happened to see it first.  The compatibility
identity is retained only for old hand-built adapter callers that do not carry the
source lineage facts; production callers can request the strong form and fail
closed when a required fact is absent.
"""

from __future__ import annotations

import base64
import hashlib
import json
import pickle
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .config import resolve_control_schema
from .errors import DestinationIdentityCollision

_MISSING = object()


def _jsonable(value: Any) -> Any:
    """Turn values into deterministic digest material without type synthesis."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        return {"__decimal__": [sign, list(digits), exponent]}
    if isinstance(value, (date, datetime)):
        return {
            "__datetime__": [
                type(value).__module__,
                type(value).__qualname__,
                value.toordinal(),
                getattr(value, "hour", 0),
                getattr(value, "minute", 0),
                getattr(value, "second", 0),
                getattr(value, "microsecond", 0),
            ]
        }
    if isinstance(value, Mapping):
        pairs = [(_jsonable(key), _jsonable(item)) for key, item in value.items()]
        return {"__mapping__": sorted(pairs, key=lambda pair: canonical_json(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        values = [_jsonable(item) for item in value]
        return {"__set__": sorted(values, key=canonical_json)}
    # A digest is not a destination value. Never call repr()/str() on an unfamiliar
    # source object: either preserve a private byte-level identity or retain only
    # its type when the object is not safely serializable.
    try:
        encoded = pickle.dumps(value, protocol=4)
    except Exception:
        encoded = None
    return {
        "__opaque__": [
            type(value).__module__,
            type(value).__qualname__,
            hashlib.sha256(encoded).hexdigest() if encoded is not None else None,
        ]
    }


def canonical_json(value: Any) -> str:
    return json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def payload_digest(event: Any) -> str:
    """Digest source operation/images and descriptor fingerprints, not arrival facts."""
    descriptor_groups = {}
    for name in ("key_descriptors", "before_descriptors", "after_descriptors"):
        descriptors = getattr(event, name, {}) or {}
        descriptor_groups[name] = {
            str(column): getattr(
                descriptor,
                "fingerprint",
                f"{type(descriptor).__module__}.{type(descriptor).__qualname__}",
            )
            for column, descriptor in sorted(descriptors.items(), key=lambda pair: str(pair[0]))
        }
    material = {
        "operation": getattr(event, "op", None),
        "key": getattr(event, "key", None),
        "before": getattr(event, "before", None),
        "after": getattr(event, "after", None),
        "descriptors": descriptor_groups,
        "snapshot_identity": getattr(event, "snapshot_identity", None),
    }
    if getattr(event, "kind", None) == "logical_message":
        # Message content is already exact bytes at the decoder boundary. The
        # digest must preserve that byte identity and the route metadata; a
        # textual round trip would make a same-ID non-UTF-8 collision invisible.
        # ``event_ts_ms`` is connector arrival metadata, not a source fact: stock
        # Debezium assigns a new value when the same WAL message is replayed.
        material["message_prefix"] = getattr(event, "message_prefix", None)
        material["message_content"] = getattr(event, "message_content", None)
        material["message_transactional"] = getattr(event, "message_transactional", None)
        material["source_sequence"] = getattr(event, "source_sequence", None)
    return hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()


def key_guard_digest(key: Mapping[str, Any] | None) -> str | None:
    if key is None:
        return None
    return hashlib.sha256(canonical_json(key).encode("utf-8")).hexdigest()


def relation_generation_from_relation(relation: Any) -> str | None:
    """Format the catalog's physical relation-generation tuple."""
    if relation is None:
        return None
    oid = getattr(relation, "oid", getattr(relation, "relation_oid", None))
    filenode = getattr(relation, "relfilenode", getattr(relation, "relation_filenode", None))
    type_oid = getattr(
        relation, "relation_type_oid", getattr(relation, "type_oid", None)
    )
    if oid is None:
        return None
    # ``relfilenode`` is zero for partitioned parents.  It remains a fact, not an
    # omitted field; the row type OID completes that generation token.
    return f"{int(oid)}:{'' if filenode is None else int(filenode)}:{'' if type_oid is None else int(type_oid)}"


def relation_generation_for(
    qualified: str | None, *, event: Any = None, provider: Any = None, con: Any = None,
    pipeline: str | None = None, control_schema: str | None = None,
) -> str | None:
    """Resolve a relation token from the live watcher or durable catalog facts."""
    explicit = getattr(event, "relation_generation", None) if event is not None else None
    if explicit is not None:
        return str(explicit)
    if not qualified:
        return None

    method = getattr(provider, "relation_generation_for", None)
    if method is not None:
        generation = method(qualified)
        if generation is not None:
            return str(generation)
    owner = getattr(provider, "__self__", None)
    known = getattr(owner, "known", None)
    if isinstance(known, Mapping):
        generation = relation_generation_from_relation(known.get(qualified))
        if generation is not None:
            return generation

    if con is not None and pipeline:
        try:
            schema, table = str(qualified).split(".", 1)
            from .naming import control_table

            row = con.execute(
                f"SELECT relation_oid, relation_filenode, relation_type_oid "
                f"FROM {control_table(resolve_control_schema(control_schema), 'source_relations')} "
                "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                [pipeline, schema, table],
            ).fetchone()
        except Exception:
            row = None
        if row is not None:
            return f"{int(row[0])}:{'' if row[1] is None else int(row[1])}:{'' if row[2] is None else int(row[2])}"
    return None


def latest_snapshot_epoch(
    con, *, pipeline: str, control_schema: str | None = None
) -> int:
    """Return the highest committed initial-snapshot epoch for ``pipeline``.

    A process-local snapshot counter is not enough after a crash: a recovery run
    can have no durable resume row while the earlier snapshot's ledger rows are
    still committed.  Reading the durable ledger before opening the next snapshot
    keeps its ``snap:`` identities disjoint without adding a second destination
    transaction.  Older destinations may not have the additive ledger table yet;
    the caller's resume epoch remains authoritative in that compatibility case.
    """
    try:
        from .naming import control_table

        row = con.execute(
            f"SELECT max(snapshot_epoch) FROM "
            f"{control_table(resolve_control_schema(control_schema), 'event_ledger')} "
            "WHERE pipeline = ? AND snapshot_epoch IS NOT NULL",
            [pipeline],
        ).fetchone()
    except Exception:
        return 0
    return int(row[0] or 0) if row is not None else 0


def _component(value: Any) -> str:
    raw = str(value).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") or "_"


@dataclass(frozen=True)
class EventIdentity:
    event_id: str
    source_schema: str | None
    source_table: str | None
    source_cluster_id: str | None
    source_timeline: int | None
    relation_generation: str | None
    txn_id: str | None
    commit_lsn: int | None
    source_lsn: int | None
    total_order: int | None
    operation: str | None
    payload_digest: str
    key_guard_digest: str | None
    policy_epoch: int
    snapshot_epoch: int | None = None
    policy_digest: str | None = None
    delete_mode: str | None = None
    strong: bool = False

    @property
    def ledger_eligible(self) -> bool:
        return self.strong or self.snapshot_epoch is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "source_cluster_id": self.source_cluster_id,
            "source_timeline": self.source_timeline,
            "relation_generation": self.relation_generation,
            "txn_id": self.txn_id,
            "commit_lsn": self.commit_lsn,
            "source_lsn": self.source_lsn,
            "total_order": self.total_order,
            "operation": self.operation,
            "payload_digest": self.payload_digest,
            "key_guard_digest": self.key_guard_digest,
            "policy_epoch": self.policy_epoch,
            "snapshot_epoch": self.snapshot_epoch,
            "policy_digest": self.policy_digest,
            "delete_mode": self.delete_mode,
        }


def identity_for(
    event: Any,
    *,
    event_id: str | None = None,
    source_cluster_id: str | None = None,
    source_timeline: int | None = None,
    relation_generation: str | None = None,
    commit_lsn: int | None = None,
    policy_epoch: int | None = None,
    target_table: str | None = None,
    require_strong: bool = False,
    digest: str | None = None,
) -> EventIdentity:
    """Build one identity from source facts, with no arrival-time components."""
    del target_table  # Target is a collision guard, never source identity.
    supplied_event_id = event_id
    snapshot_id = getattr(event, "snapshot_identity", None)
    if snapshot_id is None and (
        bool(getattr(event, "incremental", False))
        or bool(getattr(event, "snapshot", None))
        or str(supplied_event_id or "").startswith("snap:")
    ):
        snapshot_id = supplied_event_id
    incremental = bool(getattr(event, "incremental", False))
    snapshot_epoch = None
    if snapshot_id:
        stable_id = str(snapshot_id)
        # Both initial-snapshot ordinals and incremental READ identities are
        # backfill identities.  Their run/cursor is already encoded in the id;
        # zero is the explicit non-streaming epoch used in the ledger row.
        snapshot_epoch = 0
        if not incremental:
            parts = stable_id.split(":", 3)
            if len(parts) >= 2 and parts[0] == "snap":
                try:
                    snapshot_epoch = int(parts[1])
                except ValueError:
                    snapshot_epoch = None
            if snapshot_epoch is None:
                snapshot_epoch = 0
    else:
        stable_id = event_id

    cluster = source_cluster_id
    if cluster is None:
        cluster = getattr(event, "source_cluster_id", None)
    timeline = source_timeline
    if timeline is None:
        timeline = getattr(event, "source_timeline", None)
    generation = relation_generation
    if generation is None:
        generation = getattr(event, "relation_generation", None)
    txn_id = getattr(event, "txn_id", None)
    source_commit_lsn = commit_lsn
    if source_commit_lsn is None:
        source_commit_lsn = getattr(event, "commit_lsn", None)
    order = getattr(event, "total_order", None)
    try:
        timeline = int(timeline) if timeline is not None else None
    except (TypeError, ValueError):
        timeline = None
    try:
        source_commit_lsn = int(source_commit_lsn) if source_commit_lsn is not None else None
    except (TypeError, ValueError):
        source_commit_lsn = None
    try:
        order = int(order) if order is not None else None
    except (TypeError, ValueError):
        order = None
    try:
        epoch = int(policy_epoch if policy_epoch is not None else getattr(event, "policy_epoch", 0) or 0)
    except (TypeError, ValueError):
        epoch = 0

    strong = all(
        value is not None and value != ""
        for value in (cluster, timeline, generation, txn_id, source_commit_lsn, order)
    )
    if stable_id is None and strong:
        stable_id = ".".join(
            (
                "v2",
                _component(cluster),
                str(timeline),
                _component(generation),
                _component(txn_id),
                str(source_commit_lsn),
                str(order),
            )
        )
    if stable_id is None:
        # Compatibility only.  It is never ledger-eligible and strict production
        # callers reject it below.  Existing adapter fixtures rely on this spelling.
        stable_id = f"{getattr(event, 'lsn', None)}:{txn_id}:{order}"
    if require_strong and not (strong or snapshot_id):
        raise DestinationIdentityCollision(
            "source event is missing cluster/timeline/relation-generation/transaction "
            "commit-LSN/order identity facts; refusing to guess an id",
            source_schema=getattr(event, "schema", None),
            source_table=getattr(event, "table", None),
        )
    return EventIdentity(
        event_id=str(stable_id),
        source_schema=getattr(event, "schema", None),
        source_table=getattr(event, "table", None),
        source_cluster_id=str(cluster) if cluster is not None else None,
        source_timeline=timeline,
        relation_generation=str(generation) if generation is not None else None,
        txn_id=str(txn_id) if txn_id is not None else None,
        commit_lsn=source_commit_lsn,
        source_lsn=(
            int(event.lsn)
            if getattr(event, "lsn", None) is not None
            else None
        ),
        total_order=order,
        operation=getattr(event, "op", None),
        payload_digest=digest or payload_digest(event),
        key_guard_digest=key_guard_digest(getattr(event, "key", None)),
        policy_epoch=epoch,
        snapshot_epoch=snapshot_epoch,
        policy_digest=getattr(event, "policy_digest", None),
        delete_mode=getattr(event, "delete_mode", None),
        strong=strong,
    )


def message_identity_for(
    event: Any,
    *,
    event_id: str | None = None,
    source_cluster_id: str | None = None,
    source_timeline: int | None = None,
    commit_lsn: int | None = None,
    policy_epoch: int | None = None,
    require_strong: bool = False,
    digest: str | None = None,
) -> EventIdentity:
    """Build a logical-message identity without inventing a transaction.

    Transactional messages use source cluster/timeline + transaction id + source
    event LSN + Debezium ordinal. Non-transactional messages use source
    cluster/timeline + message LSN, or the source sequence when that is the only
    source cursor present. The destination commit LSN is retained as a collision
    guard, but is not used as the identity component because it is only known when
    the transaction's END marker arrives (after spill staging).
    """
    cluster = source_cluster_id
    if cluster is None:
        cluster = getattr(event, "source_cluster_id", None)
    timeline = source_timeline
    if timeline is None:
        timeline = getattr(event, "source_timeline", None)
    try:
        timeline = int(timeline) if timeline is not None else None
    except (TypeError, ValueError):
        timeline = None
    source_lsn = getattr(event, "lsn", None)
    try:
        source_lsn = int(source_lsn) if source_lsn is not None else None
    except (TypeError, ValueError):
        source_lsn = None
    source_commit_lsn = commit_lsn
    if source_commit_lsn is None:
        source_commit_lsn = getattr(event, "commit_lsn", None)
    try:
        source_commit_lsn = (
            int(source_commit_lsn) if source_commit_lsn is not None else None
        )
    except (TypeError, ValueError):
        source_commit_lsn = None
    txn_id = getattr(event, "txn_id", None)
    try:
        order = getattr(event, "total_order", None)
        order = int(order) if order is not None else None
    except (TypeError, ValueError):
        order = None
    sequence = getattr(event, "source_sequence", None)
    transactional = getattr(event, "message_transactional", None)
    if transactional is None:
        transactional = txn_id is not None or order is not None
    transactional = bool(transactional)
    cursor = source_lsn if source_lsn is not None else source_commit_lsn
    if transactional:
        missing = [
            name
            for name, value in (
                ("cluster", cluster),
                ("timeline", timeline),
                ("transaction id", txn_id),
                ("total order", order),
                ("source LSN", cursor),
            )
            if value is None or value == ""
        ]
        if missing:
            raise DestinationIdentityCollision(
                "transactional logical message is missing stable identity fact(s): "
                + ", ".join(missing),
                source_schema=None,
                source_table=None,
            )
    else:
        if cursor is None and not sequence:
            raise DestinationIdentityCollision(
                "non-transactional logical message has neither source LSN nor source "
                "sequence; refusing to invent an arrival identity",
                source_schema=None,
                source_table=None,
            )
        if cluster is None or cluster == "" or timeline is None:
            raise DestinationIdentityCollision(
                "non-transactional logical message is missing source cluster/timeline "
                "identity facts; refusing to guess an id",
                source_schema=None,
                source_table=None,
            )

    strong = bool(
        cluster not in (None, "")
        and timeline is not None
        and (
            (transactional and txn_id not in (None, "") and order is not None and cursor is not None)
            or (not transactional and (cursor is not None or sequence))
        )
    )
    if event_id is not None:
        stable_id = str(event_id)
    elif strong:
        components = ["message-v1", _component(cluster), str(timeline)]
        if transactional:
            components.extend(("tx", _component(txn_id), str(order), str(cursor)))
        elif cursor is not None:
            components.extend(("lsn", str(cursor)))
        else:
            components.extend(("sequence", _component(sequence)))
        stable_id = ".".join(components)
    else:
        # Compatibility adapters may not know source lineage. They can inspect the
        # identity, but production planners reject the non-ledger-eligible result.
        stable_id = ".".join(
            (
                "message-v1-unscoped",
                "tx" if transactional else "event",
                _component(txn_id if transactional else (cursor or sequence)),
                str(order) if transactional else "_",
            )
        )
    try:
        epoch = int(
            policy_epoch
            if policy_epoch is not None
            else getattr(event, "policy_epoch", 0) or 0
        )
    except (TypeError, ValueError):
        epoch = 0
    return EventIdentity(
        event_id=stable_id,
        source_schema=None,
        source_table=None,
        source_cluster_id=str(cluster) if cluster is not None else None,
        source_timeline=timeline,
        relation_generation=None,
        txn_id=str(txn_id) if txn_id is not None else None,
        commit_lsn=source_commit_lsn,
        source_lsn=source_lsn,
        total_order=order,
        operation="m",
        payload_digest=digest or payload_digest(event),
        key_guard_digest=None,
        policy_epoch=epoch,
        policy_digest=getattr(event, "policy_digest", None),
        delete_mode=None,
        strong=strong,
    )


def stable_event_id(event: Any, **kwargs: Any) -> str:
    return identity_for(event, **kwargs).event_id


def assert_same_identity(existing: Mapping[str, Any], identity: EventIdentity) -> None:
    """Raise on a same-ID payload or source-fact collision."""
    checks = {
        "operation": identity.operation,
        "payload_digest": identity.payload_digest,
        "source_schema": identity.source_schema,
        "source_table": identity.source_table,
        "source_cluster_id": identity.source_cluster_id,
        "source_timeline": identity.source_timeline,
        "relation_generation": identity.relation_generation,
        "txn_id": identity.txn_id,
        "commit_lsn": identity.commit_lsn,
        "source_lsn": identity.source_lsn,
        "total_order": identity.total_order,
        "key_guard_digest": identity.key_guard_digest,
        "policy_epoch": identity.policy_epoch,
        "snapshot_epoch": identity.snapshot_epoch,
        "policy_digest": identity.policy_digest,
        "delete_mode": identity.delete_mode,
    }
    for name, expected in checks.items():
        observed = existing.get(name)
        if observed != expected:
            raise DestinationIdentityCollision(
                f"event ledger identity collision for {identity.event_id!r}: "
                f"{name} durable={observed!r} replay={expected!r}",
                source_schema=identity.source_schema,
                source_table=identity.source_table,
            )


__all__ = [
    "EventIdentity",
    "assert_same_identity",
    "canonical_json",
    "identity_for",
    "key_guard_digest",
    "latest_snapshot_epoch",
    "message_identity_for",
    "payload_digest",
    "relation_generation_for",
    "relation_generation_from_relation",
    "stable_event_id",
]
