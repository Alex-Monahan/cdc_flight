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
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from . import apply_sql
from .config import resolve_control_schema, source_connection_kwargs
from .errors import DestinationIdentityCollision, LogicalMessageObligationUnresolved
from .naming import control_table, quote

LOGICAL_MESSAGE_TABLE = "cdcflight_logical_messages"
LOGICAL_MESSAGE_HEARTBEAT_PREFIX = "cdc_flight_heartbeat"
DEFAULT_APPLICATION_PREFIX_ALLOWLIST = ("app_.*",)
SOURCE_MESSAGE_PROBE_VERSION = 1
SOURCE_MESSAGE_PROBE_STATUS_EMPTY = "no_application_message"
SOURCE_MESSAGE_PROBE_STATUS_PRESENT = "application_message_present"
SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN = "unknown"


@dataclass(frozen=True)
class MessageDeliveryState:
    """The durable message certificate used by acquisition recovery.

    A local replay sidecar can be lost with a state directory or superseded by a
    newly-created source slot.  The message claim, consumer row, and audit row live
    in MotherDuck instead, so recovery derives this state from the destination every
    time.  ``certified_message_ids`` are complete atomic deliveries; ``obligations``
    are split or inconsistent certificates that must stop a destructive recovery.
    """

    certified_message_ids: tuple[str, ...] = ()
    #: Source WAL positions for the complete certificates above.  These positions
    #: are a narrow exception to the in-primitive source-message guard: an observed
    #: message at one of these positions is already durable in all three destination
    #: certificate rows and is therefore not an undischarged obligation.
    certified_source_lsns: tuple[int, ...] = ()
    obligations: tuple[dict[str, Any], ...] = ()
    #: A pending replay marker was present when this state was derived. This is
    #: deliberately separate from obligations: an empty join plus a marker is
    #: an unknown state, not an empty obligation set.
    replay_intent_present: bool = False
    #: Evidence from the bounded source-slot probe, when an empty destination join
    #: needed to be resolved before a destructive route could proceed.
    source_evidence: dict[str, Any] | None = None
    #: True only when the source probe completed and positively found no
    #: application message after the durable destination point.
    unknown_resolved: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "certified_message_ids": list(self.certified_message_ids),
            "certified_count": len(self.certified_message_ids),
            "certified_source_lsns": list(self.certified_source_lsns),
            "obligations": [dict(item) for item in self.obligations],
            "obligation_count": len(self.obligations),
            "replay_intent_present": self.replay_intent_present,
            "source_evidence": (
                dict(self.source_evidence) if self.source_evidence is not None else None
            ),
            "unknown_resolved": self.unknown_resolved,
        }


@dataclass(frozen=True)
class SourceMessageEvidence:
    """Bounded source-side evidence for an empty durable message join.

    A destination join can only name messages Flight has already observed. When a
    replay marker exists and that join is empty, this probe asks PostgreSQL's stock
    pgoutput slot to peek without consuming it. no_application_message is the
    only result that resolves the unknown state; a present message, a missing
    slot, a malformed pgoutput record, or a timeout is fail-closed.
    """

    status: str
    slot_name: str | None = None
    plugin: str | None = None
    system_identifier: str | None = None
    timeline_id: int | None = None
    after_lsn: int | None = None
    confirmed_flush_lsn: int | None = None
    scanned_records: int = 0
    application_messages: tuple[dict[str, Any], ...] = ()
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "probe_version": SOURCE_MESSAGE_PROBE_VERSION,
            "status": self.status,
            "slot_name": self.slot_name,
            "plugin": self.plugin,
            "system_identifier": self.system_identifier,
            "timeline_id": self.timeline_id,
            "after_lsn": self.after_lsn,
            "confirmed_flush_lsn": self.confirmed_flush_lsn,
            "scanned_records": self.scanned_records,
            "application_messages": [dict(item) for item in self.application_messages],
            "error": self.error,
        }


def _source_identity(value: object) -> tuple[str | None, int | None] | None:
    """Return a source lineage pair, preserving an incomplete pair as unknown."""
    if isinstance(value, SourceMessageEvidence):
        return value.system_identifier, value.timeline_id
    if isinstance(value, Mapping):
        if "system_identifier" not in value or "timeline_id" not in value:
            return None
        system_identifier = value.get("system_identifier")
        timeline_id = value.get("timeline_id")
    elif isinstance(value, (tuple, list)) and len(value) == 2:
        system_identifier, timeline_id = value
    else:
        return None
    try:
        normalized_timeline = int(timeline_id) if timeline_id is not None else None
    except (TypeError, ValueError):
        normalized_timeline = None
    return (
        str(system_identifier) if system_identifier is not None else None,
        normalized_timeline,
    )


def _source_identity_error(
    expected: tuple[str | None, int | None],
    actual: tuple[str | None, int | None],
) -> str | None:
    """Classify a lineage mismatch with reconcile.py's existing route vocabulary."""
    expected_system, expected_timeline = expected
    actual_system, actual_timeline = actual
    if expected_system is None or actual_system is None:
        return (
            "source_identity_changed: the source system_identifier is unavailable "
            f"(expected={expected_system!r}, actual={actual_system!r})"
        )
    if expected_system != actual_system:
        return (
            "source_identity_changed: source system_identifier changed "
            f"from {expected_system!r} to {actual_system!r}"
        )
    if expected_timeline is None or actual_timeline is None:
        return (
            "source_timeline_changed: the source timeline_id is unavailable "
            f"(expected={expected_timeline!r}, actual={actual_timeline!r})"
        )
    if expected_timeline != actual_timeline:
        return (
            "source_timeline_changed: source timeline_id changed "
            f"from {expected_timeline} to {actual_timeline}"
        )
    return None


def _decode_pgoutput_message(data: object) -> dict[str, Any] | None:
    """Decode only pgoutput's logical-message record, without consuming the slot."""
    raw = bytes(data)
    if not raw or raw[0] != ord("M"):
        return None
    if len(raw) < 15:
        raise ValueError("pgoutput logical-message record is truncated")
    flags = raw[1]
    source_lsn = struct.unpack(">Q", raw[2:10])[0]
    prefix_end = raw.find(b"\0", 10)
    if prefix_end < 0:
        raise ValueError("pgoutput logical-message record has no prefix terminator")
    try:
        prefix = raw[10:prefix_end].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("pgoutput logical-message prefix is not UTF-8") from exc
    content_length_at = prefix_end + 1
    if len(raw) < content_length_at + 4:
        raise ValueError("pgoutput logical-message record has no content length")
    content_length = struct.unpack(
        ">I", raw[content_length_at:content_length_at + 4]
    )[0]
    content_at = content_length_at + 4
    if len(raw) != content_at + content_length:
        raise ValueError("pgoutput logical-message content length does not match record")
    return {
        "source_lsn": source_lsn,
        "prefix": prefix,
        "byte_length": content_length,
        "is_transactional": bool(flags & 1),
    }


def _probe_source_message_evidence_connection(
    conn,
    *,
    slot_name: str,
    publication_name: str,
    after_lsn: int,
    application_patterns: Iterable[str] = DEFAULT_APPLICATION_PREFIX_ALLOWLIST,
    expected_source_identity: Mapping[str, object] | tuple[object, object] | None = None,
) -> SourceMessageEvidence:
    """Probe one already-open source connection.

    This is intentionally separate from :func:`peek_source_message_evidence`.  The
    ordinary recovery certificate uses a short-lived read-only connection, while the
    slot-drop primitive must perform the same probe and the drop on one guarded source
    connection.  Keeping the decoder here prevents the destructive primitive from
    falling back to a second, independently-timed probe.
    """
    policy = MessagePrefixPolicy(application_patterns=tuple(application_patterns))
    slot = conn.execute(
        "SELECT plugin, (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint, "
        "(SELECT system_identifier::text FROM pg_control_system()), "
        "(SELECT timeline_id FROM pg_control_checkpoint()) "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (slot_name,),
    ).fetchone()
    if slot is None:
        return SourceMessageEvidence(
            status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
            slot_name=slot_name,
            after_lsn=after_lsn,
            error="source slot is absent; its old WAL cannot be inspected",
        )
    plugin = str(slot[0]) if slot[0] is not None else None
    confirmed = int(slot[1]) if slot[1] is not None else None
    source_system_identifier = str(slot[2]) if slot[2] is not None else None
    source_timeline_id = int(slot[3]) if slot[3] is not None else None
    actual_source_identity = (source_system_identifier, source_timeline_id)
    if expected_source_identity is not None:
        expected = _source_identity(expected_source_identity)
        identity_error = (
            "source_identity_changed: the expected source identity is incomplete"
            if expected is None
            else _source_identity_error(expected, actual_source_identity)
        )
        if identity_error is not None:
            return SourceMessageEvidence(
                status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
                slot_name=slot_name,
                plugin=plugin,
                system_identifier=source_system_identifier,
                timeline_id=source_timeline_id,
                after_lsn=after_lsn,
                confirmed_flush_lsn=confirmed,
                error=identity_error,
            )
    if plugin != "pgoutput":
        return SourceMessageEvidence(
            status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
            slot_name=slot_name,
            plugin=plugin,
            system_identifier=source_system_identifier,
            timeline_id=source_timeline_id,
            after_lsn=after_lsn,
            confirmed_flush_lsn=confirmed,
            error=f"source slot plugin {plugin!r} is not stock pgoutput",
        )
    if confirmed is None or confirmed > after_lsn:
        return SourceMessageEvidence(
            status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
            slot_name=slot_name,
            plugin="pgoutput",
            system_identifier=source_system_identifier,
            timeline_id=source_timeline_id,
            after_lsn=after_lsn,
            confirmed_flush_lsn=confirmed,
            error=(
                "source slot has already acknowledged beyond the durable destination "
                "LSN; its peek cannot prove the intervening WAL was delivered"
            ),
        )
    rows = conn.execute(
        """
        SELECT (changes.lsn - '0/0'::pg_lsn)::bigint, changes.data
        FROM pg_logical_slot_peek_binary_changes(
            %s, NULL, NULL,
            'proto_version', '1',
            'publication_names', %s,
            'messages', 'true'
        ) AS changes
        """,
        (slot_name, publication_name),
    ).fetchall()

    application_messages: list[dict[str, Any]] = []
    for lsn, data in rows:
        source_lsn = int(lsn)
        if source_lsn <= after_lsn:
            continue
        try:
            message = _decode_pgoutput_message(data)
        except ValueError as exc:
            return SourceMessageEvidence(
                status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
                slot_name=slot_name,
                plugin="pgoutput",
                system_identifier=source_system_identifier,
                timeline_id=source_timeline_id,
                after_lsn=after_lsn,
                confirmed_flush_lsn=confirmed,
                scanned_records=len(rows),
                error=str(exc),
            )
        if message is None:
            continue
        # The LSN in the pgoutput payload is the message's source position. Keep the
        # row LSN as the authoritative ordering value as well; a disagreement is
        # malformed evidence, not a reason to guess.
        if message["source_lsn"] != source_lsn:
            return SourceMessageEvidence(
                status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
                slot_name=slot_name,
                plugin="pgoutput",
                system_identifier=source_system_identifier,
                timeline_id=source_timeline_id,
                after_lsn=after_lsn,
                confirmed_flush_lsn=confirmed,
                scanned_records=len(rows),
                error="pgoutput message LSN disagrees with its slot record LSN",
            )
        if policy.classify(message["prefix"]) == "application":
            application_messages.append(message)

    return SourceMessageEvidence(
        status=(
            SOURCE_MESSAGE_PROBE_STATUS_PRESENT
            if application_messages
            else SOURCE_MESSAGE_PROBE_STATUS_EMPTY
        ),
        slot_name=slot_name,
        plugin="pgoutput",
        system_identifier=source_system_identifier,
        timeline_id=source_timeline_id,
        after_lsn=after_lsn,
        confirmed_flush_lsn=confirmed,
        scanned_records=len(rows),
        application_messages=tuple(application_messages),
    )


def peek_source_message_evidence(
    dsn: str | None,
    *,
    slot_name: str | None,
    publication_name: str | None,
    after_lsn: int | None,
    application_patterns: Iterable[str] = DEFAULT_APPLICATION_PREFIX_ALLOWLIST,
    expected_source_identity: Mapping[str, object] | tuple[object, object] | None = None,
    connect_timeout: int = 5,
    statement_timeout_ms: int = 4000,
) -> SourceMessageEvidence:
    """Peek for an unobserved application message without advancing PostgreSQL.

    The server statement timeout and client TCP timeout bound the whole operation.
    There is intentionally no upto_nchanges truncation: stopping after a small
    prefix could call a live obligation absent merely because it was later in the
    slot. The query is read-only and returns unknown on every transport, slot,
    plugin, or decode failure, so an unavailable source can never authorize a
    destructive route.
    """
    if not dsn or not slot_name or not publication_name or after_lsn is None:
        return SourceMessageEvidence(
            status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
            slot_name=slot_name,
            after_lsn=after_lsn,
            error="source slot probe is missing its DSN, slot, publication, or durable LSN",
        )
    try:
        import psycopg

        with psycopg.connect(
            dsn,
            autocommit=True,
            **source_connection_kwargs(
                connect_timeout=connect_timeout,
                socket_timeout_seconds=max(1, statement_timeout_ms / 1000),
                statement_timeout_ms=statement_timeout_ms,
            ),
        ) as conn:
            return _probe_source_message_evidence_connection(
                conn,
                slot_name=slot_name,
                publication_name=publication_name,
                after_lsn=after_lsn,
                application_patterns=application_patterns,
                expected_source_identity=expected_source_identity,
            )
    except Exception as exc:
        return SourceMessageEvidence(
            status=SOURCE_MESSAGE_PROBE_STATUS_UNKNOWN,
            slot_name=slot_name,
            after_lsn=after_lsn,
            error=f"{type(exc).__name__}: {exc}",
        )


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
    # ``source_sequence`` is retained in the consumer row for observability, but it
    # is a connector cursor rendering rather than a stable source-event fact.  Stock
    # Debezium can assign a different rendering when it replays the same WAL message;
    # the ledger digest and the durable source identity already guard the bytes and
    # event, so comparing this field would reject a valid replay certificate.
    for name in (
        "message_id", "prefix", "is_transactional", "source_cluster_id",
        "source_timeline", "source_lsn", "txn_id",
        "total_order", "commit_lsn", "source_ts_ms",
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


def read_delivery_state(
    con,
    *,
    dataset: str,
    pipeline: str,
    control_schema: str | None = None,
) -> MessageDeliveryState:
    """Derive message obligations from the durable ledger/consumer certificate.

    The three rows are one logical delivery certificate: the application consumer
    row, its non-internal ``event_ledger`` claim, and the matching audit row.  The
    query deliberately uses a full join, so a claim without a consumer row and a
    consumer row without a claim are both visible.  A full recovery may preserve a
    complete certificate, but it must stop before removing source/replay evidence
    when the certificate is split.

    Internal Flight messages are excluded from the application certificate.  They
    intentionally have a ledger/audit pair but no public consumer row.
    """
    control = resolve_control_schema(control_schema)
    message_target = target_table(dataset)
    consumer_exists = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [dataset, LOGICAL_MESSAGE_TABLE],
    ).fetchone()
    if consumer_exists:
        consumer_source = f"""
            SELECT message_id, prefix, is_transactional, source_cluster_id,
                   source_timeline, source_lsn, source_sequence, txn_id,
                   total_order, commit_lsn, destination_commit_id,
                   delivery_state
            FROM {qualified_table(dataset)}
            WHERE pipeline = ?
        """
        consumer_params: list[Any] = [pipeline]
    else:
        # Destination setup creates this relation in production, but keeping the
        # derived query total makes it safe for a first recovery on an older database.
        consumer_source = """
            SELECT CAST(NULL AS VARCHAR) AS message_id,
                   CAST(NULL AS VARCHAR) AS prefix,
                   CAST(NULL AS BOOLEAN) AS is_transactional,
                   CAST(NULL AS VARCHAR) AS source_cluster_id,
                   CAST(NULL AS BIGINT) AS source_timeline,
                   CAST(NULL AS BIGINT) AS source_lsn,
                   CAST(NULL AS VARCHAR) AS source_sequence,
                   CAST(NULL AS VARCHAR) AS txn_id,
                   CAST(NULL AS BIGINT) AS total_order,
                   CAST(NULL AS BIGINT) AS commit_lsn,
                   CAST(NULL AS BIGINT) AS destination_commit_id,
                   CAST(NULL AS VARCHAR) AS delivery_state
            WHERE FALSE
        """
        consumer_params = []

    query = f"""
        WITH claims AS (
            SELECT event_id, state, source_cluster_id, source_timeline,
                   source_lsn, txn_id, total_order, commit_lsn
            FROM {control_table(control, 'event_ledger')}
            WHERE pipeline = ?
              AND target_table = ?
              AND state <> 'internal'
        ), consumers AS (
            {consumer_source}
        ), audits AS (
            SELECT message_id, prefix, is_transactional, source_cluster_id,
                   source_timeline, source_lsn, source_sequence, txn_id,
                   total_order, commit_lsn, destination_commit_id, status
            FROM {control_table(control, 'logical_message_audit')}
            WHERE pipeline = ?
              AND target_table = ?
              AND status <> 'internal'
        ), joined AS (
            SELECT
                coalesce(claims.event_id, consumers.message_id, audits.message_id)
                    AS message_id,
                claims.event_id IS NOT NULL AS has_ledger,
                consumers.message_id IS NOT NULL AS has_consumer,
                audits.message_id IS NOT NULL AS has_audit,
                claims.state AS ledger_state,
                consumers.delivery_state,
                audits.status AS audit_status,
                consumers.prefix AS consumer_prefix,
                audits.prefix AS audit_prefix,
                consumers.is_transactional AS consumer_transactional,
                audits.is_transactional AS audit_transactional,
                claims.source_cluster_id AS ledger_cluster,
                consumers.source_cluster_id AS consumer_cluster,
                audits.source_cluster_id AS audit_cluster,
                claims.source_timeline AS ledger_timeline,
                consumers.source_timeline AS consumer_timeline,
                audits.source_timeline AS audit_timeline,
                claims.source_lsn AS ledger_lsn,
                consumers.source_lsn AS consumer_lsn,
                audits.source_lsn AS audit_lsn,
                claims.txn_id AS ledger_txn_id,
                consumers.txn_id AS consumer_txn_id,
                audits.txn_id AS audit_txn_id,
                claims.total_order AS ledger_total_order,
                consumers.total_order AS consumer_total_order,
                audits.total_order AS audit_total_order,
                claims.commit_lsn AS ledger_commit_lsn,
                consumers.commit_lsn AS consumer_commit_lsn,
                audits.commit_lsn AS audit_commit_lsn,
                consumers.destination_commit_id AS consumer_commit_id,
                audits.destination_commit_id AS audit_commit_id
            FROM claims
            FULL OUTER JOIN consumers
              ON claims.event_id = consumers.message_id
            FULL OUTER JOIN audits
              ON coalesce(claims.event_id, consumers.message_id) = audits.message_id
        )
        SELECT *,
            (
                has_ledger
                AND has_consumer
                AND has_audit
                AND ledger_state IN ('applied', 'replayed')
                AND delivery_state IN ('delivered', 'replayed')
                AND audit_status IN ('delivered', 'replayed')
                AND consumer_prefix IS NOT DISTINCT FROM audit_prefix
                AND consumer_transactional IS NOT DISTINCT FROM audit_transactional
                AND ledger_cluster IS NOT DISTINCT FROM consumer_cluster
                AND consumer_cluster IS NOT DISTINCT FROM audit_cluster
                AND ledger_timeline IS NOT DISTINCT FROM consumer_timeline
                AND consumer_timeline IS NOT DISTINCT FROM audit_timeline
                AND ledger_lsn IS NOT DISTINCT FROM consumer_lsn
                AND consumer_lsn IS NOT DISTINCT FROM audit_lsn
                AND ledger_txn_id IS NOT DISTINCT FROM consumer_txn_id
                AND consumer_txn_id IS NOT DISTINCT FROM audit_txn_id
                AND ledger_total_order IS NOT DISTINCT FROM consumer_total_order
                AND consumer_total_order IS NOT DISTINCT FROM audit_total_order
                AND ledger_commit_lsn IS NOT DISTINCT FROM consumer_commit_lsn
                AND consumer_commit_lsn IS NOT DISTINCT FROM audit_commit_lsn
                AND consumer_commit_id IS NOT DISTINCT FROM audit_commit_id
            ) AS certified
        FROM joined
        ORDER BY message_id
    """
    rows = con.execute(
        query,
        [pipeline, message_target, *consumer_params, pipeline, message_target],
    ).fetchall()
    certified: list[str] = []
    certified_source_lsns: list[int] = []
    obligations: list[dict[str, Any]] = []
    for row in rows:
        values = dict(zip(
            (
                "message_id", "has_ledger", "has_consumer", "has_audit",
                "ledger_state", "delivery_state", "audit_status",
                "consumer_prefix", "audit_prefix", "consumer_transactional",
                "audit_transactional", "ledger_cluster", "consumer_cluster",
                "audit_cluster", "ledger_timeline", "consumer_timeline",
                "audit_timeline", "ledger_lsn", "consumer_lsn", "audit_lsn",
                "ledger_txn_id", "consumer_txn_id", "audit_txn_id",
                "ledger_total_order", "consumer_total_order", "audit_total_order",
                "ledger_commit_lsn", "consumer_commit_lsn", "audit_commit_lsn",
                "consumer_commit_id", "audit_commit_id", "certified",
            ),
            row,
            strict=True,
        ))
        message_id = str(values["message_id"])
        if values["certified"]:
            certified.append(message_id)
            if values["consumer_lsn"] is not None:
                certified_source_lsns.append(int(values["consumer_lsn"]))
            continue
        issues: list[str] = []
        if not values["has_ledger"]:
            issues.append("consumer_or_audit_without_ledger")
        if not values["has_consumer"]:
            issues.append("ledger_or_audit_without_consumer")
        if not values["has_audit"]:
            issues.append("ledger_or_consumer_without_audit")
        if values["has_ledger"] and values["ledger_state"] not in {"applied", "replayed"}:
            issues.append("ledger_not_applied")
        if values["has_consumer"] and values["delivery_state"] not in {"delivered", "replayed"}:
            issues.append("consumer_not_delivered")
        if values["has_audit"] and values["audit_status"] not in {"delivered", "replayed"}:
            issues.append("audit_not_delivered")
        if not issues:
            issues.append("certificate_identity_mismatch")
        obligations.append(
            {
                "message_id": message_id,
                "issues": issues,
                "has_ledger": bool(values["has_ledger"]),
                "has_consumer": bool(values["has_consumer"]),
                "has_audit": bool(values["has_audit"]),
            }
        )
    return MessageDeliveryState(
        certified_message_ids=tuple(certified),
        certified_source_lsns=tuple(certified_source_lsns),
        obligations=tuple(obligations),
    )


def require_recovery_message_certificate(
    con,
    *,
    dataset: str,
    pipeline: str,
    control_schema: str | None = None,
    replay_intent_path=None,
    source_dsn: str | None = None,
    source_slot_name: str | None = None,
    source_publication_name: str | None = None,
    source_application_patterns: Iterable[str] = DEFAULT_APPLICATION_PREFIX_ALLOWLIST,
    known_source_evidence: dict[str, Any] | None = None,
    known_message_state: dict[str, Any] | None = None,
    expected_source_identity: Mapping[str, object] | tuple[object, object] | None = None,
    replay_intent_namespace: str | None = None,
) -> MessageDeliveryState:
    """Refuse recovery unless a marker is positively discharged.

    The durable join is authoritative for messages Flight observed. Its empty result
    is not authoritative for messages that never arrived. When a replay marker is
    present and the join is empty, a bounded read-only pgoutput peek resolves that
    unknown state: only a completed scan with no application message permits the
    destructive route. A source message, a missing slot, or any probe failure raises
    before the recovery journal or its destructive ladder can proceed.
    """
    from . import destination as dest_mod
    from . import offsets

    state = read_delivery_state(
        con,
        dataset=dataset,
        pipeline=pipeline,
        control_schema=control_schema,
    )
    if state.obligations:
        details = "; ".join(
            f"{item['message_id']}: {','.join(item['issues'])}"
            for item in state.obligations
        )
        raise LogicalMessageObligationUnresolved(
            "full source recovery refused: durable logical-message delivery "
            f"certificate is incomplete ({details})",
            obligations=state.obligations,
        )
    if replay_intent_path is None:
        return state

    intent = offsets.read_replay_intent(replay_intent_path)
    if intent is None:
        return state
    durable_point = dest_mod.read_resume_point(
        con,
        pipeline,
        intent.namespace,
        control_schema=control_schema,
    )
    expected_namespace = replay_intent_namespace or intent.namespace
    if (
        durable_point is None
        and isinstance(known_message_state, dict)
        and known_message_state.get("replay_intent_present") is True
        and (
            state.certified_message_ids
            or (
                isinstance(known_message_state.get("source_evidence"), dict)
                and known_message_state["source_evidence"].get("probe_version")
                == SOURCE_MESSAGE_PROBE_VERSION
                and known_message_state["source_evidence"].get("status")
                == SOURCE_MESSAGE_PROBE_STATUS_EMPTY
                and not known_message_state["source_evidence"].get(
                    "application_messages"
                )
                and not known_message_state["source_evidence"].get("error")
            )
        )
    ):
        # The recovery journal deliberately deletes the destination resume row before
        # dropping the source slot. Its persisted journal identity is the proof that
        # lets a later phase validate the marker without trying to validate against a
        # row that this same recovery has already removed. It is not a source-message
        # certificate: the source probe below is intentionally fresh on every entry.
        if intent.pipeline != pipeline or intent.namespace != expected_namespace:
            raise LogicalMessageObligationUnresolved(
                "recovery replay marker identity no longer matches its journal",
                obligations=(
                    {
                        "message_id": f"source-slot:{intent.namespace}",
                        "issues": ["replay_intent_identity_mismatch"],
                        "has_ledger": False,
                        "has_consumer": False,
                        "has_audit": False,
                    },
                ),
            )
    else:
        offsets.validate_replay_intent(
            intent,
            pipeline=pipeline,
            namespace=expected_namespace,
            durable_point=durable_point,
        )
    state = replace(state, replay_intent_present=True)

    # A complete three-way certificate is positive proof for the observed message
    # obligation. This path intentionally remains usable when the old source slot has
    # already disappeared: the certificate is in MotherDuck, not in that slot.
    if state.certified_message_ids:
        return state
    # An empty source result is a point-in-time observation, never a reusable
    # certificate. In particular, recovery.resume() receives the result recorded by
    # recovery.begin(), but must probe again at this destructive entry. When the
    # previous recovery phase has already deleted the durable row, the old evidence
    # supplies only the source position and lineage against which to re-probe.
    if expected_source_identity is None and known_source_evidence is not None:
        expected_source_identity = {
            "system_identifier": known_source_evidence.get("system_identifier"),
            "timeline_id": known_source_evidence.get("timeline_id"),
        }
    after_lsn = durable_point.last_lsn if durable_point is not None else None
    if after_lsn is None or after_lsn <= 0:
        offset_lsn = (
            durable_point.offset.get("lsn") if durable_point is not None else None
        )
        after_lsn = int(offset_lsn) if offset_lsn is not None else None
    if after_lsn is None and known_source_evidence is not None:
        known_after_lsn = known_source_evidence.get("after_lsn")
        if known_after_lsn is not None:
            after_lsn = int(known_after_lsn)
    evidence = peek_source_message_evidence(
        source_dsn,
        slot_name=source_slot_name,
        publication_name=source_publication_name,
        after_lsn=after_lsn,
        application_patterns=source_application_patterns,
        expected_source_identity=expected_source_identity,
    ).as_dict()

    if evidence.get("status") != SOURCE_MESSAGE_PROBE_STATUS_EMPTY:
        probe_obligation = {
            "message_id": f"source-slot:{source_slot_name or 'unknown'}",
            "issues": [
                (
                    "source_slot_application_message_unobserved"
                    if evidence.get("status") == SOURCE_MESSAGE_PROBE_STATUS_PRESENT
                    else (
                        "source_identity_changed"
                        if str(evidence.get("error", "")).startswith(
                            "source_identity_changed:"
                        )
                        else (
                            "source_timeline_changed"
                            if str(evidence.get("error", "")).startswith(
                                "source_timeline_changed:"
                            )
                            else "source_slot_evidence_unknown"
                        )
                    )
                )
            ],
            "has_ledger": False,
            "has_consumer": False,
            "has_audit": False,
            "source_evidence": evidence,
        }
        raise LogicalMessageObligationUnresolved(
            "full source recovery refused: the pending replay marker has no "
            "positive destination certificate and the source-slot probe did not "
            f"prove it empty ({evidence.get('status')}, "
            f"{probe_obligation['message_id']})",
            obligations=(probe_obligation,),
        )
    return replace(
        state,
        source_evidence=evidence,
        unknown_resolved=True,
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
    "MessageDeliveryState",
    "MessagePrefixPolicy",
    "SourceMessageEvidence",
    "assert_row_matches",
    "ensure_table",
    "insert_rows",
    "message_prefix_include_list",
    "normalize_prefix_allowlist",
    "peek_source_message_evidence",
    "qualified_table",
    "read_delivery_state",
    "read_logical_messages",
    "read_row",
    "require_recovery_message_certificate",
    "target_table",
    "write_audit_rows",
]
