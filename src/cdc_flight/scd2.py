"""SCD2 relation bundles for source-ordered history/current projections.

This module is the relation-level owner for rubric 8.2.  It has no snapshot
reconstruction shortcut: a current-only image cannot manufacture history before
the history boundary.  Every row version is admitted with the shared event ledger,
and the ledger claim, history mutation, and current marking all use the caller's
single open destination transaction.
"""

from __future__ import annotations

import base64
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import destination, event_ledger, naming
from .errors import AdmissionError, AmbiguousDelete
from .typed_types import SourceTypeDescriptor, encode_value, native_type

HISTORY_META = {
    "__cdcf_scd2_source_identity": "VARCHAR NOT NULL",
    "__cdcf_scd2_relation_generation": "VARCHAR NOT NULL",
    "__cdcf_scd2_valid_from": "VARCHAR NOT NULL",
    "__cdcf_scd2_valid_to": "VARCHAR",
    "__cdcf_scd2_is_current": "BOOLEAN NOT NULL",
    "__cdcf_scd2_operation": "VARCHAR NOT NULL",
    "__cdcf_scd2_event_id": "VARCHAR NOT NULL",
    "__cdcf_scd2_commit_lsn": "BIGINT",
    "__cdcf_scd2_txn_id": "VARCHAR",
    "__cdcf_scd2_observed_at": "TIMESTAMPTZ",
    "__cdcf_scd2_policy_epoch": "BIGINT NOT NULL",
    "__cdcf_scd2_payload_digest": "VARCHAR NOT NULL",
    "__cdcf_scd2_key_json": "VARCHAR NOT NULL",
    "__cdcf_scd2_image_json": "VARCHAR",
}
_EVENT_COLUMN = "__cdcf_scd2_event_id"
_IDENTITY_COLUMN = "__cdcf_scd2_source_identity"
_FROM_COLUMN = "__cdcf_scd2_valid_from"
_TO_COLUMN = "__cdcf_scd2_valid_to"
_CURRENT_COLUMN = "__cdcf_scd2_is_current"
_OP_COLUMN = "__cdcf_scd2_operation"
_IMAGE_COLUMN = "__cdcf_scd2_image_json"
_KEY_COLUMN = "__cdcf_scd2_key_json"


class SCD2IdentityRefused(AdmissionError, AmbiguousDelete):
    """An SCD2 event has no source identity or no safe predecessor."""

    def __init__(
        self,
        message: str,
        *,
        source_schema: str | None = None,
        source_table: str | None = None,
        target: str | None = None,
    ):
        # Keep the richer AmbiguousDelete payload so the commit protocol can
        # request a history-aware resnapshot, while AdmissionError remains the
        # package-wide classification root.
        AmbiguousDelete.__init__(
            self,
            message,
            source_schema=source_schema,
            source_table=source_table,
            target=target,
        )


class HistoryRefreshRefused(AdmissionError, AmbiguousDelete):
    """A current-only image cannot reconstruct history before its boundary."""

    def __init__(
        self,
        message: str,
        *,
        source_schema: str | None = None,
        source_table: str | None = None,
        target: str | None = None,
    ):
        AmbiguousDelete.__init__(
            self,
            message,
            source_schema=source_schema,
            source_table=source_table,
            target=target,
        )


@dataclass(frozen=True)
class SCD2RelationBundle:
    pipeline: str
    source_schema: str
    source_table: str
    target_table: str
    columns: dict[str, SourceTypeDescriptor | Any]
    key_columns: tuple[str, ...]
    relation_generation: str
    history_table: str | None = None
    current_view: str | None = None
    policy_epoch: int = 0

    def __post_init__(self) -> None:
        if not self.pipeline or not self.source_schema or not self.source_table:
            raise ValueError("an SCD2 bundle requires a source relation")
        if not self.relation_generation:
            raise ValueError("an SCD2 bundle requires a relation generation")
        normalized = {naming.normalize(str(name)): value for name, value in self.columns.items()}
        object.__setattr__(self, "columns", normalized)
        object.__setattr__(
            self,
            "key_columns",
            tuple(naming.normalize(str(column)) for column in self.key_columns),
        )

    @property
    def history_name(self) -> str:
        return self.history_table or f"{self.target_table}__scd2_history"

    @property
    def current_name(self) -> str:
        return self.current_view or f"{self.target_table}__current"


@dataclass(frozen=True)
class SCD2Event:
    pipeline: str
    target_table: str
    source_schema: str
    source_table: str
    event_id: str
    operation: str
    key: dict[str, Any] | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    identity: event_ledger.EventIdentity
    observed_at: datetime | None = None

    @classmethod
    def from_pending(
        cls,
        event,
        *,
        event_id: str,
        pipeline: str,
        target_table: str,
        source_cluster_id: str | None,
        source_timeline: int | None,
        relation_generation: str | None,
        commit_lsn: int | None,
        policy_epoch: int = 0,
    ) -> SCD2Event:
        identity = event_ledger.identity_for(
            event,
            event_id=event_id,
            source_cluster_id=source_cluster_id,
            source_timeline=source_timeline,
            relation_generation=relation_generation,
            commit_lsn=commit_lsn,
            policy_epoch=policy_epoch,
            target_table=target_table,
            require_strong=True,
        )
        operation = str(event.op or "").lower()
        if operation not in {"c", "r", "u", "d"}:
            raise _refusal(event, target_table, f"unsupported SCD2 operation {operation!r}")
        if not event.key:
            raise _refusal(
                event,
                target_table,
                "SCD2 has no individual lineage for an arbitrary keyless row",
            )
        observed_at = None
        if getattr(event, "source_ts_ms", None) is not None:
            with contextlib.suppress(OverflowError, OSError, ValueError):
                observed_at = datetime.fromtimestamp(
                    int(event.source_ts_ms) / 1000, tz=UTC
                )
        return cls(
            pipeline=pipeline,
            target_table=target_table,
            source_schema=str(event.schema),
            source_table=str(event.table),
            event_id=identity.event_id,
            operation=operation,
            key=dict(event.key),
            before=dict(event.before) if event.before is not None else None,
            after=dict(event.after) if event.after is not None else None,
            identity=identity,
            observed_at=observed_at,
        )

    @property
    def order_token(self) -> str:
        return transaction_order_token(self.identity)

    @property
    def source_identity(self) -> str:
        return event_ledger.canonical_json(self.key)


@dataclass(frozen=True)
class SCD2ApplyResult:
    event_id: str
    replayed: bool
    order_token: str
    current: bool


@dataclass(frozen=True)
class SCD2TableEvent:
    """A table-level event such as TRUNCATE; it is not a row identity."""

    pipeline: str
    target_table: str
    source_schema: str
    source_table: str
    event_id: str
    identity: event_ledger.EventIdentity

    @property
    def order_token(self) -> str:
        return transaction_order_token(self.identity)


def transaction_order_token(identity: event_ledger.EventIdentity) -> str:
    """Return a source-order token; source timestamps never participate."""
    required = (
        identity.source_cluster_id,
        identity.source_timeline,
        identity.relation_generation,
        identity.commit_lsn,
        identity.total_order,
    )
    if any(value is None or value == "" for value in required):
        raise SCD2IdentityRefused(
            "SCD2 transaction order needs source cluster, timeline, relation "
            "generation, commit LSN, and transaction total_order"
        )
    def component(value: Any) -> str:
        return base64.urlsafe_b64encode(str(value).encode()).decode().rstrip("=") or "_"

    return ".".join(
        (
            component(identity.source_cluster_id),
            f"{int(identity.source_timeline):020d}",
            component(identity.relation_generation),
            f"{int(identity.commit_lsn):020d}",
            f"{int(identity.total_order):020d}",
        )
    )


def _qualified(name: str) -> str:
    return ".".join(naming.quote(part) for part in str(name).split("."))


def _sql_type(value: Any) -> str:
    if isinstance(value, SourceTypeDescriptor):
        return native_type(value).sql
    if hasattr(value, "sql"):
        return str(value.sql)
    if isinstance(value, str) and value.strip():
        # This is an explicitly supplied destination SQL contract, not a type
        # inferred from a row value. Production calls pass descriptors.
        return value
    raise SCD2IdentityRefused("SCD2 source column has no catalog-authoritative native type")


def _source_columns(bundle: SCD2RelationBundle) -> tuple[str, ...]:
    columns = tuple(bundle.columns)
    overlap = set(columns) & set(HISTORY_META)
    if overlap:
        raise SCD2IdentityRefused(
            f"source column(s) collide with SCD2 metadata: {sorted(overlap)!r}",
            source_schema=bundle.source_schema,
            source_table=bundle.source_table,
            target=bundle.target_table,
        )
    missing_keys = set(bundle.key_columns) - set(columns)
    if missing_keys:
        raise SCD2IdentityRefused(
            f"SCD2 key column(s) are not in the source descriptor: {sorted(missing_keys)!r}",
            source_schema=bundle.source_schema,
            source_table=bundle.source_table,
            target=bundle.target_table,
        )
    return columns


def ensure_bundle(con, bundle: SCD2RelationBundle, *, control_schema: str | None = None) -> None:
    """Create history/current and record the bundle in the caller's transaction."""
    columns = _source_columns(bundle)
    definitions = [
        f"{naming.quote(column)} {_sql_type(bundle.columns[column])}"
        for column in columns
    ]
    definitions.extend(
        f"{naming.quote(column)} {type_name}" for column, type_name in HISTORY_META.items()
    )
    definitions.append(f"UNIQUE ({naming.quote(_EVENT_COLUMN)})")
    history = _qualified(bundle.history_name)
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {history} ({', '.join(definitions)})"
    )
    selected = ", ".join(naming.quote(column) for column in (*columns, *HISTORY_META))
    con.execute(
        f"CREATE OR REPLACE VIEW {_qualified(bundle.current_name)} AS "
        f"SELECT {selected} FROM {history} "
        f"WHERE {naming.quote(_CURRENT_COLUMN)} AND "
        f"{naming.quote(_OP_COLUMN)} <> 'd'"
    )
    control = destination._control_table(control_schema, "scd2_bundles")
    con.execute(
        f"DELETE FROM {control} WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [bundle.pipeline, bundle.source_schema, bundle.source_table],
    )
    con.execute(
        f"INSERT INTO {control} "
        "(pipeline, source_schema, source_table, target_table, history_table, current_view, "
        " relation_generation, key_columns, policy_epoch, state, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            bundle.pipeline,
            bundle.source_schema,
            bundle.source_table,
            bundle.target_table,
            bundle.history_name,
            bundle.current_name,
            bundle.relation_generation,
            list(bundle.key_columns),
            bundle.policy_epoch,
            "active",
            destination.now(),
        ],
    )


def _refusal(event, target: str, message: str) -> SCD2IdentityRefused:
    return SCD2IdentityRefused(
        f"{event.schema}.{event.table}: {message}",
        source_schema=event.schema,
        source_table=event.table,
        target=target,
    )


def _complete_image(
    bundle: SCD2RelationBundle,
    image: dict[str, Any] | None,
    *,
    event: SCD2Event,
    label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if image is None or any(column not in image for column in bundle.columns):
        missing = sorted(set(bundle.columns) - set(image or {}))
        raise SCD2IdentityRefused(
            f"{event.source_schema}.{event.source_table}: SCD2 {label} is missing "
            f"source column(s) {missing!r}; refusing to guess a predecessor",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )
    raw = {column: image[column] for column in bundle.columns}
    encoded: dict[str, Any] = {}
    for column, value in raw.items():
        descriptor = bundle.columns[column]
        try:
            encoded[column] = (
                encode_value(value, descriptor)
                if isinstance(descriptor, SourceTypeDescriptor)
                else value
            )
        except Exception as exc:
            raise SCD2IdentityRefused(
                f"{event.source_schema}.{event.source_table}: SCD2 {label} "
                f"value for {column!r} is not natively bindable: {exc}",
                source_schema=event.source_schema,
                source_table=event.source_table,
                target=event.target_table,
            ) from exc
    return raw, encoded


def _history_rows(con, bundle: SCD2RelationBundle, source_identity: str) -> list[dict[str, Any]]:
    table = _qualified(bundle.history_name)
    rows = con.execute(
        f"SELECT {_qualified(_EVENT_COLUMN)}, {_qualified(_FROM_COLUMN)}, "
        f"       {_qualified(_TO_COLUMN)}, {_qualified(_CURRENT_COLUMN)}, "
        f"       {_qualified(_OP_COLUMN)}, {_qualified(_IMAGE_COLUMN)} "
        f"FROM {table} WHERE {_qualified(_IDENTITY_COLUMN)} = ? "
        f"ORDER BY {_qualified(_FROM_COLUMN)}",
        [source_identity],
    ).fetchall()
    return [
        {
            "event_id": str(row[0]),
            "from": str(row[1]),
            "to": str(row[2]) if row[2] is not None else None,
            "current": bool(row[3]),
            "operation": str(row[4]),
            "image_json": row[5],
        }
        for row in rows
    ]


def _verify_before(event: SCD2Event, row: dict[str, Any]) -> None:
    if event.before is None:
        raise SCD2IdentityRefused(
            f"{event.source_schema}.{event.source_table}: {event.operation} has no "
            "before-image for predecessor verification",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )
    observed = event_ledger.canonical_json(event.before)
    if row["image_json"] != observed:
        raise SCD2IdentityRefused(
            f"{event.source_schema}.{event.source_table}: SCD2 {event.operation} "
            "before-image does not match its source-order predecessor",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )


def _truncate_boundary(con, bundle: SCD2RelationBundle) -> str | None:
    row = con.execute(
        f"SELECT max({naming.quote(_FROM_COLUMN)}) FROM {_qualified(bundle.history_name)} "
        f"WHERE {naming.quote(_OP_COLUMN)} = 't'"
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _close(con, bundle: SCD2RelationBundle, row: dict[str, Any], token: str) -> None:
    con.execute(
        f"UPDATE {_qualified(bundle.history_name)} SET "
        f"{naming.quote(_TO_COLUMN)} = ?, {naming.quote(_CURRENT_COLUMN)} = false "
        f"WHERE {naming.quote(_EVENT_COLUMN)} = ?",
        [token, row["event_id"]],
    )


def _insert(
    con,
    bundle: SCD2RelationBundle,
    event: SCD2Event,
    *,
    source_identity: str,
    token: str,
    valid_to: str | None,
    is_current: bool,
    raw_image: dict[str, Any] | None,
    encoded_image: dict[str, Any] | None,
) -> None:
    columns = _source_columns(bundle)
    metadata = list(HISTORY_META)
    all_columns = [*columns, *metadata]
    values = [
        *(encoded_image.get(column) if encoded_image is not None else None for column in columns),
        source_identity,
        event.identity.relation_generation,
        token,
        valid_to,
        is_current,
        event.operation,
        event.event_id,
        event.identity.commit_lsn,
        event.identity.txn_id,
        event.observed_at,
        event.identity.policy_epoch,
        event.identity.payload_digest,
        event_ledger.canonical_json(event.key),
        event_ledger.canonical_json(raw_image) if raw_image is not None else None,
    ]
    placeholders = ", ".join("?" for _ in all_columns)
    con.execute(
        f"INSERT INTO {_qualified(bundle.history_name)} "
        f"({', '.join(naming.quote(column) for column in all_columns)}) "
        f"VALUES ({placeholders})",
        values,
    )


def apply_event(
    con,
    event: SCD2Event,
    *,
    bundle: SCD2RelationBundle | None = None,
    control_schema: str | None = None,
) -> SCD2ApplyResult:
    """Apply one row version or replay it as an exact ledger no-op."""
    if bundle is None:
        raise ValueError("apply_event requires the descriptor-backed relation bundle")
    if event.identity.relation_generation != bundle.relation_generation:
        raise SCD2IdentityRefused(
            f"{event.source_schema}.{event.source_table}: event relation generation "
            "does not match the SCD2 bundle; refusing a cross-generation splice",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )
    token = event.order_token
    source_identity = event.source_identity
    ensure_bundle(con, bundle, control_schema=control_schema)
    if destination.claim_event_ledger(
        con,
        event.identity,
        pipeline=event.pipeline,
        target_table=event.target_table,
        source_lsn=event.identity.source_lsn,
        control_schema=control_schema,
    ):
        return SCD2ApplyResult(event.event_id, True, token, False)

    rows = _history_rows(con, bundle, source_identity)
    current = [row for row in rows if row["current"]]
    if len(current) > 1:
        raise SCD2IdentityRefused(
            f"{event.target_table}: SCD2 identity has {len(current)} current versions",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )
    current_row = current[0] if current else None
    exact = next((row for row in rows if row["from"] == token), None)
    if exact is not None:
        raise SCD2IdentityRefused(
            f"{event.target_table}: two source events share SCD2 order token {token!r}",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )

    raw_image = None
    encoded_image = None
    if event.operation in {"c", "r", "u"}:
        raw_image, encoded_image = _complete_image(
            bundle, event.after, event=event, label="after-image"
        )
    elif event.operation == "d":
        raw_image, encoded_image = _complete_image(
            bundle, event.before, event=event, label="before-image"
        )

    if not rows:
        if event.operation not in {"c", "r"}:
            raise SCD2IdentityRefused(
                f"{event.target_table}: {event.operation} has no predecessor in SCD2 history",
                source_schema=event.source_schema,
                source_table=event.source_table,
                target=event.target_table,
            )
        _insert(
            con,
            bundle,
            event,
            source_identity=source_identity,
            token=token,
            valid_to=None,
            is_current=True,
            raw_image=raw_image,
            encoded_image=encoded_image,
        )
        return SCD2ApplyResult(event.event_id, False, token, True)

    predecessor = next((row for row in reversed(rows) if row["from"] < token), None)
    successor = next((row for row in rows if row["from"] > token), None)
    late = current_row is not None and token < current_row["from"]
    if current_row is None and event.operation in {"c", "r"}:
        # A table truncate closes every row lineage but is not a row DELETE.  A
        # later source INSERT/READ can therefore establish a fresh current row;
        # an event at or before the truncate boundary is deliberately not guessed
        # into the post-truncate lineage.
        boundary = _truncate_boundary(con, bundle)
        if boundary is not None and token > boundary:
            _insert(
                con,
                bundle,
                event,
                source_identity=source_identity,
                token=token,
                valid_to=None,
                is_current=True,
                raw_image=raw_image,
                encoded_image=encoded_image,
            )
            return SCD2ApplyResult(event.event_id, False, token, True)
    if event.operation in {"u", "d"}:
        if predecessor is None:
            raise SCD2IdentityRefused(
                f"{event.target_table}: {event.operation} has no source-order predecessor",
                source_schema=event.source_schema,
                source_table=event.source_table,
                target=event.target_table,
            )
        _verify_before(event, predecessor)
    elif late and predecessor is None:
        raise SCD2IdentityRefused(
            f"{event.target_table}: late {event.operation} would precede the known history "
            "boundary; refusing to guess an earlier version",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )

    if not late:
        if current_row is None:
            raise SCD2IdentityRefused(
                f"{event.target_table}: SCD2 history has no current predecessor",
                source_schema=event.source_schema,
                source_table=event.source_table,
                target=event.target_table,
            )
        if event.operation in {"c", "r"} and current_row["from"] >= token:
            raise SCD2IdentityRefused(
                f"{event.target_table}: create is not newer than the current version",
                source_schema=event.source_schema,
                source_table=event.source_table,
                target=event.target_table,
            )
        _close(con, bundle, current_row, token)
        _insert(
            con,
            bundle,
            event,
            source_identity=source_identity,
            token=token,
            valid_to=None,
            is_current=True,
            raw_image=raw_image,
            encoded_image=encoded_image,
        )
        return SCD2ApplyResult(event.event_id, False, token, True)

    # A late event is inserted between the predecessor and successor.  The
    # successor remains current; only the predecessor's right boundary changes.
    if predecessor is None:
        raise SCD2IdentityRefused(
            f"{event.target_table}: late event has no verified predecessor",
            source_schema=event.source_schema,
            source_table=event.source_table,
            target=event.target_table,
        )
    _close(con, bundle, predecessor, token)
    _insert(
        con,
        bundle,
        event,
        source_identity=source_identity,
        token=token,
        valid_to=successor["from"] if successor is not None else None,
        is_current=False,
        raw_image=raw_image,
        encoded_image=encoded_image,
    )
    return SCD2ApplyResult(event.event_id, False, token, False)


def apply_truncate(
    con,
    bundle: SCD2RelationBundle,
    event: SCD2TableEvent,
    *,
    control_schema: str | None = None,
) -> SCD2ApplyResult:
    """Close current row versions and record one structural truncate marker."""
    ensure_bundle(con, bundle, control_schema=control_schema)
    token = event.order_token
    if destination.claim_event_ledger(
        con,
        event.identity,
        pipeline=event.pipeline,
        target_table=event.target_table,
        source_lsn=event.identity.source_lsn,
        control_schema=control_schema,
    ):
        return SCD2ApplyResult(event.event_id, True, token, False)
    history = _qualified(bundle.history_name)
    con.execute(
        f"UPDATE {history} SET {naming.quote(_TO_COLUMN)} = ?, "
        f"{naming.quote(_CURRENT_COLUMN)} = false WHERE {naming.quote(_CURRENT_COLUMN)}",
        [token],
    )
    _insert(
        con,
        bundle,
        SCD2Event(
            pipeline=event.pipeline,
            target_table=event.target_table,
            source_schema=event.source_schema,
            source_table=event.source_table,
            event_id=event.event_id,
            operation="t",
            key={"__table__": f"{event.source_schema}.{event.source_table}"},
            before=None,
            after=None,
            identity=event.identity,
        ),
        source_identity=event_ledger.canonical_json(
            {"__table__": f"{event.source_schema}.{event.source_table}"}
        ),
        token=token,
        valid_to=None,
        is_current=False,
        raw_image=None,
        encoded_image=None,
    )
    return SCD2ApplyResult(event.event_id, False, token, False)


def refuse_current_only_refresh(
    *, source_schema: str, source_table: str, target_table: str, history_boundary: Any
) -> None:
    raise HistoryRefreshRefused(
        f"{source_schema}.{source_table}: a current-only PostgreSQL snapshot cannot "
        f"reconstruct SCD2 history before boundary {history_boundary!r}",
        source_schema=source_schema,
        source_table=source_table,
        target=target_table,
    )


__all__ = [
    "HISTORY_META",
    "HistoryRefreshRefused",
    "SCD2ApplyResult",
    "SCD2Event",
    "SCD2IdentityRefused",
    "SCD2RelationBundle",
    "SCD2TableEvent",
    "apply_event",
    "apply_truncate",
    "ensure_bundle",
    "refuse_current_only_refresh",
    "transaction_order_token",
]
