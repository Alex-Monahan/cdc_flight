"""Logical-message policy and the durable application-message consumer.

PostgreSQL logical messages are bytes.  This module deliberately keeps the
connector's base64 decoding at the envelope boundary and makes the destination
consumer a normal, queryable relation rather than a callback side effect.  The
shared event ledger remains the replay fence; ``logical_message_audit`` is the
value-free operational surface for internal, delivered, replayed, and rejected
observations.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import apply_sql
from .config import resolve_control_schema
from .errors import DestinationIdentityCollision
from .naming import control_table, quote

LOGICAL_MESSAGE_TABLE = "cdcflight_logical_messages"
LOGICAL_MESSAGE_HEARTBEAT_PREFIX = "cdc_flight_heartbeat"
DEFAULT_APPLICATION_PREFIX_ALLOWLIST = ("app_.*",)


def normalize_prefix_allowlist(patterns: str | Iterable[str]) -> tuple[str, ...]:
    """Validate and normalize the regexes used by stock Debezium.

    Debezium's ``message.prefix.include.list`` is a comma-separated include
    predicate.  Python uses ``fullmatch`` for the same policy so a message can
    never be admitted merely because an allowlisted expression matched a suffix.
    """
    if isinstance(patterns, str):
        values = tuple(item.strip() for item in patterns.split(",") if item.strip())
    else:
        values = tuple(str(item).strip() for item in patterns if str(item).strip())
    if not values:
        raise ValueError(
            "the logical-message application prefix allowlist must contain at least "
            "one non-empty regex"
        )
    for pattern in values:
        try:
            re.compile(pattern)
        except re.error as exc:
            raise ValueError(
                f"logical-message application prefix regex {pattern!r} is invalid"
            ) from exc
    return tuple(dict.fromkeys(values))


@dataclass(frozen=True)
class MessagePrefixPolicy:
    """Route Flight-owned marker messages before application messages."""

    application_patterns: tuple[str, ...] = DEFAULT_APPLICATION_PREFIX_ALLOWLIST
    marker_prefixes: tuple[str, ...] = ("cdcf",)
    heartbeat_prefix: str = LOGICAL_MESSAGE_HEARTBEAT_PREFIX

    def __post_init__(self) -> None:
        patterns = normalize_prefix_allowlist(self.application_patterns)
        markers = tuple(
            dict.fromkeys(str(item).strip() for item in self.marker_prefixes if str(item).strip())
        )
        if not markers:
            raise ValueError(
                "logical-message marker prefixes must preserve at least one Flight "
                "control namespace"
            )
        heartbeat = str(self.heartbeat_prefix).strip()
        if not heartbeat:
            raise ValueError("the Flight heartbeat logical-message prefix is required")
        object.__setattr__(self, "application_patterns", patterns)
        object.__setattr__(self, "marker_prefixes", markers)
        object.__setattr__(self, "heartbeat_prefix", heartbeat)
        object.__setattr__(
            self,
            "_compiled_application",
            tuple(re.compile(pattern) for pattern in patterns),
        )

    def classify(self, prefix: str | None) -> str:
        """Return ``application``, ``internal``, or ``rejected``."""
        value = "" if prefix is None else str(prefix)
        if value == self.heartbeat_prefix:
            return "internal"
        if any(
            value == marker or value.startswith(f"{marker}_")
            for marker in self.marker_prefixes
        ):
            return "internal"
        if any(pattern.fullmatch(value) for pattern in self._compiled_application):
            return "application"
        return "rejected"

    def include_list(self) -> str:
        """Render the exact stock Debezium include-list property value."""
        return message_prefix_include_list(
            self.application_patterns,
            marker_prefixes=self.marker_prefixes,
            heartbeat_prefix=self.heartbeat_prefix,
        )


def message_prefix_include_list(
    application_patterns: str | Iterable[str],
    *,
    marker_prefixes: Iterable[str] = ("cdcf",),
    heartbeat_prefix: str = LOGICAL_MESSAGE_HEARTBEAT_PREFIX,
) -> str:
    """Build ``message.prefix.include.list`` for stock Debezium 3.6.

    The Flight namespaces are always included so a source-side completion marker,
    catalog fence, or idle heartbeat cannot disappear at the connector boundary.
    Python still routes those namespaces to the internal path.
    """
    applications = normalize_prefix_allowlist(application_patterns)
    values = [f"^{re.escape(str(heartbeat_prefix).strip())}$"]
    for marker in dict.fromkeys(
        str(item).strip() for item in marker_prefixes if str(item).strip()
    ):
        escaped = re.escape(marker)
        values.append(f"^{escaped}(?:_|$).*")
    values.extend(applications)
    return ",".join(dict.fromkeys(values))


def target_table(dataset: str) -> str:
    """Return the stable ledger target for the public message relation."""
    return f"{dataset}.{LOGICAL_MESSAGE_TABLE}"


def qualified_table(dataset: str) -> str:
    return f"{quote(dataset)}.{quote(LOGICAL_MESSAGE_TABLE)}"


def ensure_table(con, dataset: str) -> None:
    """Create the public relation during destination setup or an open group."""
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {qualified_table(dataset)} (
            pipeline              VARCHAR NOT NULL,
            message_id            VARCHAR NOT NULL,
            prefix                VARCHAR NOT NULL,
            content               BLOB NOT NULL,
            is_transactional      BOOLEAN NOT NULL,
            source_schema         VARCHAR,
            source_table          VARCHAR,
            source_cluster_id     VARCHAR,
            source_timeline       BIGINT,
            source_lsn            BIGINT,
            source_sequence       VARCHAR,
            txn_id                VARCHAR,
            total_order           BIGINT,
            commit_lsn            BIGINT,
            source_ts_ms          BIGINT,
            event_ts_ms           BIGINT,
            destination_commit_id BIGINT NOT NULL,
            delivery_state        VARCHAR NOT NULL,
            delivered_at          TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pipeline, message_id)
        )"""
    )


def _row_values(row: dict[str, Any]) -> list[Any]:
    return [
        row["pipeline"],
        row["message_id"],
        row["prefix"],
        bytes(row["content"]),
        bool(row["is_transactional"]),
        row.get("source_schema"),
        row.get("source_table"),
        row.get("source_cluster_id"),
        row.get("source_timeline"),
        row.get("source_lsn"),
        row.get("source_sequence"),
        row.get("txn_id"),
        row.get("total_order"),
        row.get("commit_lsn"),
        row.get("source_ts_ms"),
        row.get("event_ts_ms"),
        row["destination_commit_id"],
        row.get("delivery_state", "delivered"),
        row.get("delivered_at") or datetime.now(UTC),
    ]


def insert_rows(con, dataset: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    apply_sql.bulk_insert(
        con,
        qualified_table(dataset),
        [
            "pipeline", "message_id", "prefix", "content", "is_transactional",
            "source_schema", "source_table", "source_cluster_id", "source_timeline",
            "source_lsn", "source_sequence", "txn_id", "total_order", "commit_lsn",
            "source_ts_ms", "event_ts_ms", "destination_commit_id", "delivery_state",
            "delivered_at",
        ],
        [_row_values(row) for row in rows],
        [
            apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BLOB,
            apply_sql.BOOLEAN, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR,
            apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.VARCHAR, apply_sql.VARCHAR,
            apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.BIGINT,
            apply_sql.BIGINT, apply_sql.VARCHAR, "TIMESTAMPTZ",
        ],
    )


_MESSAGE_COLUMNS = (
    "pipeline", "message_id", "prefix", "content", "is_transactional",
    "source_schema", "source_table", "source_cluster_id", "source_timeline",
    "source_lsn", "source_sequence", "txn_id", "total_order", "commit_lsn",
    "source_ts_ms", "event_ts_ms", "destination_commit_id", "delivery_state",
    "delivered_at",
)


def read_row(
    con, *, dataset: str, pipeline: str, message_id: str
) -> dict[str, Any] | None:
    columns = ", ".join(quote(column) for column in _MESSAGE_COLUMNS)
    row = con.execute(
        f"SELECT {columns} FROM {qualified_table(dataset)} "
        "WHERE pipeline = ? AND message_id = ?",
        [pipeline, message_id],
    ).fetchone()
    if row is None:
        return None
    values = dict(zip(_MESSAGE_COLUMNS, row, strict=True))
    values["content"] = bytes(values["content"])
    return values


def assert_row_matches(
    observed: dict[str, Any] | None,
    expected: dict[str, Any],
) -> None:
    """Detect a ledger/public-row split or a same-ID payload collision."""
    if observed is None:
        raise DestinationIdentityCollision(
            f"logical message {expected['message_id']!r} has a durable ledger claim "
            "but no materialized consumer row",
            target=target_table(str(expected["dataset"])),
        )
    for name in (
        "message_id", "prefix", "is_transactional", "source_cluster_id",
        "source_timeline", "source_lsn", "source_sequence", "txn_id",
        "total_order", "commit_lsn", "source_ts_ms", "event_ts_ms",
    ):
        if observed.get(name) != expected.get(name):
            raise DestinationIdentityCollision(
                f"logical message identity collision for {expected['message_id']!r}: "
                f"{name} durable={observed.get(name)!r} replay={expected.get(name)!r}",
                target=target_table(str(expected["dataset"])),
            )
    if bytes(observed.get("content") or b"") != bytes(expected["content"]):
        raise DestinationIdentityCollision(
            f"logical message identity collision for {expected['message_id']!r}: "
            "content bytes differ",
            target=target_table(str(expected["dataset"])),
        )


def write_audit_rows(con, *, control_schema: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    control_schema = resolve_control_schema(control_schema)
    columns = [
        "pipeline", "target_table", "message_id", "prefix", "byte_length",
        "is_transactional", "source_cluster_id", "source_timeline", "source_lsn",
        "source_sequence", "txn_id", "total_order", "commit_lsn",
        "destination_commit_id", "status", "rejection_reason", "observed_at",
    ]
    values = [
        [
            row["pipeline"], row["target_table"], row["message_id"], row["prefix"],
            len(bytes(row["content"])), bool(row["is_transactional"]),
            row.get("source_cluster_id"), row.get("source_timeline"), row.get("source_lsn"),
            row.get("source_sequence"), row.get("txn_id"), row.get("total_order"),
            row.get("commit_lsn"), row["destination_commit_id"], row["status"],
            row.get("rejection_reason"), row.get("observed_at") or datetime.now(UTC),
        ]
        for row in rows
    ]
    apply_sql.bulk_insert(
        con,
        control_table(control_schema, "logical_message_audit"),
        columns,
        values,
        [
            apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR,
            apply_sql.BIGINT, apply_sql.BOOLEAN, apply_sql.VARCHAR, apply_sql.BIGINT,
            apply_sql.BIGINT, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BIGINT,
            apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.VARCHAR, apply_sql.VARCHAR,
            "TIMESTAMPTZ",
        ],
        replace=True,
    )


def read_logical_messages(
    con, *, dataset: str = "cdc_raw", pipeline: str | None = None,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Read the user-actionable message consumer surface.

    Only application messages are materialized here; Flight-owned heartbeats and
    markers are intentionally visible through the value-free audit surface instead.
    The returned ``content`` value is always ``bytes``.
    """
    columns = ", ".join(quote(column) for column in _MESSAGE_COLUMNS)
    clauses: list[str] = []
    params: list[Any] = []
    if pipeline is not None:
        clauses.append("pipeline = ?")
        params.append(pipeline)
    if prefix is not None:
        clauses.append("prefix = ?")
        params.append(prefix)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = con.execute(
        f"SELECT {columns} FROM {qualified_table(dataset)}{where} "
        "ORDER BY coalesce(commit_lsn, source_lsn), source_lsn, message_id",
        params,
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        value = dict(zip(_MESSAGE_COLUMNS, row, strict=True))
        value["content"] = bytes(value["content"])
        out.append(value)
    return out


__all__ = [
    "DEFAULT_APPLICATION_PREFIX_ALLOWLIST",
    "LOGICAL_MESSAGE_HEARTBEAT_PREFIX",
    "LOGICAL_MESSAGE_TABLE",
    "MessagePrefixPolicy",
    "assert_row_matches",
    "ensure_table",
    "insert_rows",
    "message_prefix_include_list",
    "normalize_prefix_allowlist",
    "qualified_table",
    "read_logical_messages",
    "read_row",
    "target_table",
    "write_audit_rows",
]
