"""Fail-closed capability checks for the supported hot-standby source path.

The stock Debezium PostgreSQL connector is still the decoder.  This module only
proves that the endpoint handed to it is a *live recovery-mode decoder*: a
PostgreSQL 16-or-newer hot standby, fed by a healthy physical receiver, with a
local (not synchronized failover) logical ``pgoutput`` slot.  It never creates,
advances, or drops a slot and it never writes to the standby.

That distinction matters.  A synced failover slot is useful failover metadata,
but it is not a decoder attached to the standby.  A read-only standby without
local slot administration is snapshot-only for this Flight; treating either as
CDC would turn a successful-looking read into an unbounded source gap.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .config import source_connection_kwargs

MINIMUM_SERVER_VERSION_NUM = 160000
REQUIRED_PLUGIN = "pgoutput"
UNHEALTHY_WAL_STATUS = frozenset({"lost", "unreserved"})


class StandbyCapabilityError(RuntimeError):
    """The configured standby cannot safely host the stock logical decoder."""


@dataclass(frozen=True)
class StandbyObservation:
    """Read-only facts collected from the standby and its primary."""

    server_version_num: int | None
    in_recovery: bool
    wal_level: str | None
    primary_wal_level: str | None
    hot_standby_feedback: bool
    receiver_status: str | None
    receiver_slot_name: str | None
    expected_physical_slot_name: str | None
    local_slot_name: str
    local_slot_type: str | None
    local_slot_plugin: str | None
    local_slot_active: bool | None
    local_slot_synced: bool | None
    local_slot_failover: bool | None
    local_slot_wal_status: str | None
    local_slot_catalog_xmin: str | None
    local_slot_invalidation_reason: str | None
    system_identifier: str | None
    timeline_id: int | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def unsupported_reasons(
    observation: StandbyObservation,
    *,
    minimum_server_version_num: int = MINIMUM_SERVER_VERSION_NUM,
) -> tuple[str, ...]:
    """Return every failed capability guard, without collapsing witnesses."""

    reasons: list[str] = []
    if (
        observation.server_version_num is None
        or observation.server_version_num < minimum_server_version_num
    ):
        reasons.append(
            f"PostgreSQL {minimum_server_version_num // 10000}+ is required for "
            f"standby logical decoding (got {observation.server_version_num!r})"
        )
    if not observation.in_recovery:
        reasons.append("the source endpoint is not in recovery; it is not the standby path")
    if observation.wal_level and observation.wal_level not in {"replica", "logical"}:
        reasons.append(f"standby wal_level is {observation.wal_level!r}, not replica/logical")
    if observation.primary_wal_level != "logical":
        reasons.append(
            "the primary must report wal_level=logical before a standby logical slot "
            f"can decode (got {observation.primary_wal_level!r})"
        )
    if not observation.hot_standby_feedback:
        reasons.append("hot_standby_feedback is off")
    if observation.receiver_status != "streaming":
        reasons.append(
            "the physical receiver is not streaming "
            f"(status={observation.receiver_status!r})"
        )
    if (
        observation.expected_physical_slot_name is not None
        and observation.receiver_slot_name
        != observation.expected_physical_slot_name
    ):
        reasons.append(
            "the physical receiver is using a different slot "
            f"(expected={observation.expected_physical_slot_name!r}, "
            f"observed={observation.receiver_slot_name!r})"
        )
    if observation.local_slot_type != "logical":
        reasons.append(
            f"local slot {observation.local_slot_name!r} is not logical "
            f"(type={observation.local_slot_type!r})"
        )
    if observation.local_slot_plugin != REQUIRED_PLUGIN:
        reasons.append(
            f"local slot {observation.local_slot_name!r} is not {REQUIRED_PLUGIN!r} "
            f"(plugin={observation.local_slot_plugin!r})"
        )
    # PG17 exposes these failover columns.  A missing value on PG16 is a fact that
    # the older server cannot assert, so it is not treated as a synchronized slot;
    # an explicit true value is always refused.
    if observation.local_slot_synced is True:
        reasons.append("a synchronized failover slot is not a live standby decoder")
    if observation.local_slot_failover is True:
        reasons.append("a failover slot is not the required local logical slot")
    if observation.local_slot_wal_status in UNHEALTHY_WAL_STATUS:
        reasons.append(
            f"local logical slot WAL status is {observation.local_slot_wal_status!r}"
        )
    if observation.local_slot_invalidation_reason:
        reasons.append(
            "local logical slot is invalidated: "
            f"{observation.local_slot_invalidation_reason}"
        )
    return tuple(reasons)


def assert_supported(observation: StandbyObservation) -> StandbyObservation:
    """Raise before engine construction unless every standby guard holds."""

    reasons = unsupported_reasons(observation)
    if reasons:
        raise StandbyCapabilityError(
            "standby logical decoding is unsupported; this endpoint is "
            "snapshot-only until the operator repairs the topology: "
            + "; ".join(reasons)
        )
    return observation


def _scalar(conn, sql: str, params: list[Any] | tuple[Any, ...] = ()):
    row = conn.execute(sql, params).fetchone()
    return None if row is None else row[0]


def _available_slot_columns(conn) -> set[str]:
    """Discover versioned ``pg_replication_slots`` columns before selecting them."""

    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_replication_slots'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def inspect(
    dsn: str,
    slot_name: str,
    *,
    primary_dsn: str | None = None,
    physical_slot_name: str | None = None,
    connect_timeout: int = 5,
    statement_timeout_ms: int = 4000,
) -> StandbyObservation:
    """Collect standby and primary capability facts using read-only sessions.

    The primary query is intentionally separate: a standby reports its own
    recovery ``wal_level`` and cannot prove what the primary was configured to
    retain.  ``primary_dsn`` is used only for ``SHOW wal_level`` and identity
    metadata; no publication or marker write is performed here.
    """

    import psycopg

    kwargs = source_connection_kwargs(
        connect_timeout=connect_timeout,
        socket_timeout_seconds=max(1, statement_timeout_ms / 1000),
        statement_timeout_ms=statement_timeout_ms,
    )
    with psycopg.connect(dsn, **kwargs) as conn:
        server_version_num = _scalar(conn, "SHOW server_version_num")
        in_recovery = bool(_scalar(conn, "SELECT pg_is_in_recovery()"))
        wal_level = _scalar(conn, "SHOW wal_level")
        feedback = str(_scalar(conn, "SHOW hot_standby_feedback") or "off").lower() == "on"
        receiver = conn.execute(
            "SELECT status, slot_name FROM pg_stat_wal_receiver LIMIT 1"
        ).fetchone()
        receiver_status = str(receiver[0]) if receiver and receiver[0] is not None else None
        receiver_slot_name = str(receiver[1]) if receiver and receiver[1] is not None else None

        columns = _available_slot_columns(conn)
        optional = {
            name: (
                f"s.{name}" if name in columns else "NULL"
            )
            for name in (
                "slot_name",
                "plugin",
                "slot_type",
                "active",
                "synced",
                "failover",
                "wal_status",
                "catalog_xmin",
                "invalidation_reason",
            )
        }
        row = conn.execute(
            "SELECT "
            + ", ".join(optional[name] for name in optional)
            + " FROM pg_replication_slots s WHERE s.slot_name = %s",
            (slot_name,),
        ).fetchone()

        values = dict(zip(optional, row or (None,) * len(optional), strict=True))
        system_identifier = _scalar(
            conn,
            "SELECT system_identifier::text FROM pg_control_system()",
        )
        timeline_id = _scalar(conn, "SELECT timeline_id FROM pg_control_checkpoint()")

    primary_wal_level = None
    if primary_dsn:
        with psycopg.connect(primary_dsn, **kwargs) as conn:
            primary_wal_level = str(_scalar(conn, "SHOW wal_level") or "").lower() or None
    elif not in_recovery:
        primary_wal_level = str(wal_level or "").lower() or None

    observation = StandbyObservation(
        server_version_num=int(server_version_num) if server_version_num is not None else None,
        in_recovery=in_recovery,
        wal_level=str(wal_level).lower() if wal_level is not None else None,
        primary_wal_level=primary_wal_level,
        hot_standby_feedback=feedback,
        receiver_status=receiver_status,
        receiver_slot_name=receiver_slot_name,
        expected_physical_slot_name=physical_slot_name,
        local_slot_name=slot_name,
        local_slot_type=(str(values["slot_type"]) if values["slot_type"] is not None else None),
        local_slot_plugin=(str(values["plugin"]) if values["plugin"] is not None else None),
        local_slot_active=(bool(values["active"]) if values["active"] is not None else None),
        local_slot_synced=(bool(values["synced"]) if values["synced"] is not None else None),
        local_slot_failover=(bool(values["failover"]) if values["failover"] is not None else None),
        local_slot_wal_status=(
            str(values["wal_status"]) if values["wal_status"] is not None else None
        ),
        local_slot_catalog_xmin=(
            str(values["catalog_xmin"]) if values["catalog_xmin"] is not None else None
        ),
        local_slot_invalidation_reason=(
            str(values["invalidation_reason"])
            if values["invalidation_reason"] is not None
            else None
        ),
        system_identifier=(str(system_identifier) if system_identifier is not None else None),
        timeline_id=int(timeline_id) if timeline_id is not None else None,
    )
    return assert_supported(observation)


__all__ = [
    "MINIMUM_SERVER_VERSION_NUM",
    "REQUIRED_PLUGIN",
    "StandbyCapabilityError",
    "StandbyObservation",
    "assert_supported",
    "inspect",
    "unsupported_reasons",
]
