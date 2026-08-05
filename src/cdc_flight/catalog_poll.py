"""Bounded source-catalog polling coordinator."""

from __future__ import annotations

import json
import logging

from . import catalog_observation as observation_mod
from . import faults as faults_mod
from .catalog_state import FENCED, SourceRelation, _missing_value
from .machines import (
    CATALOG_SCHEMA_LIVENESS,
    SCHEMA_EMPTY,
    SCHEMA_ERROR,
    SCHEMA_UNAVAILABLE,
    SCHEMA_VISIBLE,
)
from .schema_evolution import SourceColumn
from .states import IllegalTransition, UnknownState

log = logging.getLogger("cdc_flight.catalog_poll")


def connect(watcher, *, autocommit: bool = True):
    import psycopg

    return psycopg.connect(
        watcher.dsn,
        autocommit=autocommit,
        connect_timeout=watcher.connect_timeout,
        options=f"-c statement_timeout={watcher.query_timeout_ms}",
        keepalives=1,
        keepalives_idle=1,
        keepalives_interval=1,
        keepalives_count=2,
        tcp_user_timeout=watcher.query_timeout_ms,
    )


def poll_quietly(watcher):
    try:
        # Keep the watcher method as the seam for tests and embedders that need to
        # substitute one observation. The method itself delegates back here in the
        # normal implementation.
        return watcher.poll()
    except (IllegalTransition, UnknownState) as illegal:
        _mark_liveness_error(watcher)
        watcher.machine_error = f"{type(illegal).__name__}: {illegal}"
        log.critical(
            "catalog state machine violated during a poll; this run cannot be "
            "reported successful: %s",
            watcher.machine_error,
        )
        return []
    except Exception as exc:  # pragma: no cover - exercised through the thread
        _mark_liveness_error(watcher)
        watcher.last_error = f"{type(exc).__name__}: {exc}"
        log.warning("catalog poll failed: %s", watcher.last_error)
        return []


def _mark_liveness_error(watcher) -> None:
    """Keep source-query failure in the declared per-schema liveness machine."""
    with watcher._lock:
        names = set(watcher.schemas) or {
            qualified.partition(".")[0] for qualified in watcher.known
        }
        for name in names:
            watcher._schema_liveness[name] = CATALOG_SCHEMA_LIVENESS.parse(SCHEMA_ERROR)


def poll(watcher):
    faults_mod.maybe_fail_repeatedly("catalog_poll")
    with connect(watcher) as conn:
        schema_array = None if watcher.all_schemas else sorted(watcher.schemas)
        rows = conn.execute(
            observation_mod.CATALOG_SQL,
            (watcher.publication, schema_array, schema_array),
        ).fetchall()
        liveness_rows = conn.execute(
            observation_mod.SCHEMA_LIVENESS_SQL,
            (schema_array, schema_array),
        ).fetchall()
        observed_liveness = {
            str(schema): (SCHEMA_VISIBLE if int(count) > 0 else SCHEMA_EMPTY)
            for schema, count in liveness_rows
        }
        expected_schemas = set(watcher.schemas)
        if watcher.all_schemas:
            expected_schemas |= {name.partition(".")[0] for name in watcher.known}
        for name in expected_schemas:
            observed_liveness.setdefault(name, SCHEMA_UNAVAILABLE)
        for state in observed_liveness.values():
            CATALOG_SCHEMA_LIVENESS.parse(state)
        with watcher._lock:
            watcher._schema_liveness = observed_liveness
        partition_rows = conn.execute(
            observation_mod.PARTITION_SQL,
            (watcher.publication, schema_array, schema_array),
        ).fetchall()
        lsn = int(conn.execute(observation_mod.LSN_SQL).fetchone()[0])
        observed: dict[str, SourceRelation] = {}
        for row in rows:
            raw_columns = row[9] if len(row) > 9 else []
            if isinstance(raw_columns, str):
                try:
                    raw_columns = json.loads(raw_columns)
                except ValueError:
                    raw_columns = []
            columns = tuple(
                SourceColumn(
                    attnum=int(raw["attnum"]),
                    name=str(raw["name"]),
                    type_oid=int(raw["type_oid"]),
                    type_name=str(raw["type_name"]),
                    nullable=bool(raw.get("nullable", True)),
                    has_missing_default=bool(raw.get("has_missing_default", False)),
                    missing_value=_missing_value(
                        raw.get("missing_value_text"), str(raw["type_name"])
                    ),
                )
                for raw in (raw_columns or [])
            )
            observed[f"{row[0]}.{row[1]}"] = SourceRelation(
                schema=row[0],
                table=row[1],
                oid=int(row[2]),
                relfilenode=(int(row[3]) if row[3] is not None else None),
                relation_type_oid=(int(row[4]) if row[4] is not None else None),
                replica_identity=str(row[5]),
                published=bool(row[6]),
                columns=columns,
                publication_all_tables=bool(row[7]) if len(row) > 7 else False,
                is_partition=bool(row[8]) if len(row) > 8 else False,
            )
        if not observed:
            with watcher._lock:
                watcher.polls += 1
                watcher.empty_polls += 1
                watcher.last_lsn = lsn
            watcher.last_error = (
                f"the polled schema {watcher.schema!r} contains no tables at all; "
                "this observation was DISCARDED rather than read as a mass drop"
            )
            log.error("catalog poll: %s", watcher.last_error)
            return []
        with watcher._lock:
            watcher._snapshot_partitions = {
                f"{row[0]}.{row[1]}" for row in partition_rows
            }
        added = watcher._compare(observed, lsn)
        watcher._ensure_published(conn, observed, added)
        with watcher._lock:
            watcher.successful_polls += 1
        unfenced = [change for change in watcher.pending() if change.kind in FENCED]
        if unfenced:
            watcher._emit_marker(
                conn,
                [change for change in added if change.kind in FENCED] or unfenced,
            )
    if watcher.marker.last_error is None and not watcher._admission_errors:
        watcher.last_error = None
    return added
