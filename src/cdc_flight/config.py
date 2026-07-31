"""Central configuration for the cdc_flight baseline pipeline.

Everything is overridable through environment variables so the same code runs
from a test fixture, a Makefile target, or (later) a MotherDuck Flight.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


@dataclass(frozen=True)
class SourceConfig:
    """Connection details for the project-local Postgres cluster."""

    host: str = field(default_factory=lambda: _env("PGHOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(_env("PGPORT", "15432")))
    user: str = field(default_factory=lambda: _env("PGUSER", "postgres"))
    password: str = field(default_factory=lambda: _env("PGPASSWORD", "postgres"))
    dbname: str = field(default_factory=lambda: _env("PGDATABASE", "cdc_source"))
    schema: str = field(default_factory=lambda: _env("CDC_SCHEMA", "app"))

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

    @property
    def tables(self) -> list[str]:
        raw = _env(
            "CDC_TABLES",
            "customers,orders,sensor_readings,documents,wide_types,audit_log",
        )
        return [f"{self.schema}.{t.strip()}" for t in raw.split(",") if t.strip()]


@dataclass(frozen=True)
class ReplicationConfig:
    """Debezium / logical-decoding identifiers."""

    slot_name: str = field(default_factory=lambda: _env("CDC_SLOT_NAME", "cdc_flight_slot"))
    publication_name: str = field(
        default_factory=lambda: _env("CDC_PUBLICATION", "cdc_flight_pub")
    )
    topic_prefix: str = field(default_factory=lambda: _env("CDC_TOPIC_PREFIX", "cdcflight"))
    snapshot_mode: str = field(default_factory=lambda: _env("CDC_SNAPSHOT_MODE", "initial"))
    state_dir: Path = field(
        default_factory=lambda: Path(_env("CDC_STATE_DIR", str(PROJECT_DIR / ".cdc_state")))
    )

    @property
    def offset_file(self) -> Path:
        return self.state_dir / "offsets.dat"


@dataclass(frozen=True)
class DestinationConfig:
    """Where dlt writes the change events."""

    kind: str = field(default_factory=lambda: _env("CDC_DESTINATION", "duckdb"))
    pipeline_name: str = field(default_factory=lambda: _env("CDC_PIPELINE_NAME", "cdc_flight"))
    dataset_name: str = field(default_factory=lambda: _env("CDC_DATASET", "cdc_raw"))
    duckdb_path: Path = field(
        default_factory=lambda: Path(
            _env("CDC_DUCKDB_PATH", str(PROJECT_DIR / "cdc_flight.duckdb"))
        )
    )
    motherduck_database: str = field(
        default_factory=lambda: _env("CDC_MD_DATABASE", "cdc_flight_dev")
    )
    pipelines_dir: Path = field(
        default_factory=lambda: Path(
            _env("CDC_PIPELINES_DIR", str(PROJECT_DIR / ".cdc_state" / "dlt_pipelines"))
        )
    )


@dataclass(frozen=True)
class RunConfig:
    """Stop conditions for a single engine run.

    The baseline runs the Debezium engine as a bounded batch job (the shape a
    MotherDuck Flight needs) rather than an unbounded daemon.
    """

    max_seconds: float = field(default_factory=lambda: float(_env("CDC_MAX_SECONDS", "90")))
    idle_seconds: float = field(default_factory=lambda: float(_env("CDC_IDLE_SECONDS", "8")))
    min_records: int = field(default_factory=lambda: int(_env("CDC_MIN_RECORDS", "0")))
    #: How far the slot's `confirmed_flush_lsn` may trail `pg_current_wal_lsn()`
    #: and still allow the supervisor to call a quiet stream "idle". A quiet
    #: stream with a large backlog means the connector is not streaming - most
    #: often Debezium's 10 s retriable-restart backoff, which is longer than the
    #: 8 s idle window (ADR 0001 §9.1, review finding Opus B5).
    idle_max_lag_bytes: int = field(
        default_factory=lambda: int(_env("CDC_IDLE_MAX_LAG_BYTES", str(64 * 1024)))
    )
    #: How long `engine.close()` may take before the run is declared hung. It runs
    #: on its own supervised thread, because a hang *inside* close would otherwise
    #: never reach the join-based watchdog (rubric 4.5).
    close_timeout: float = field(
        default_factory=lambda: float(_env("CDC_CLOSE_TIMEOUT", "30"))
    )


def _flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def applier_settings() -> dict:
    """Trigger policy and containment switches for the transactional applier.

    Defaults follow ADR 0001 §3.3. `CDC_OFFSET_FILE_REPAIR=0` is the one switch
    an operator would not normally touch: it disables the `offsets.dat` rebuild
    so the applier's idempotency fence carries correctness on its own. The suite
    runs the crash scenario both ways on purpose - if correctness ever came to
    depend on the repair, that would be an ordering argument, and ADR 0001 exists
    because ordering arguments are not good enough.
    """
    return {
        "commit_max_age": float(_env("CDC_COMMIT_MAX_AGE", "5")),
        "commit_max_events": int(_env("CDC_COMMIT_MAX_EVENTS", "200000")),
        "commit_max_bytes": int(_env("CDC_COMMIT_MAX_BYTES", str(256 * 1024 * 1024))),
        "unit_spill_events": int(_env("CDC_UNIT_SPILL_EVENTS", "500000")),
        "unit_spill_bytes": int(_env("CDC_UNIT_SPILL_BYTES", str(64 * 1024 * 1024))),
        "snapshot_chunk_events": int(_env("CDC_SNAPSHOT_CHUNK_EVENTS", "50000")),
        "snapshot_chunk_bytes": int(_env("CDC_SNAPSHOT_CHUNK_BYTES", str(64 * 1024 * 1024))),
        "repair_offset_file": _flag("CDC_OFFSET_FILE_REPAIR", True),
        "verify_offset_file": _flag("CDC_VERIFY_OFFSET_FILE", True),
        "ack_every_record": _flag("CDC_ACK_EVERY_RECORD", False),
        #: Destination-side PRIMARY KEY on every replicated table's identity
        #: columns (Opus M-2). On by default: it is what makes "duplication is
        #: impossible" a property of the destination rather than of the applier.
        #: `CDC_DESTINATION_CONSTRAINTS=0` falls back to the post-apply uniqueness
        #: assertion inside the commit group.
        "destination_constraints": _flag("CDC_DESTINATION_CONSTRAINTS", True),
        #: rubric 1.5. `replicate` empties the destination table inside the commit
        #: group, exactly as Postgres emptied the source, and records a marker in
        #: `_cdc_flight.table_events`. `log` records the marker and keeps the rows
        #: (the rubric's "tombstone/soft delete" behaviour, =3). `ignore` restores
        #: Debezium's default of not even decoding the event.
        "truncate_mode": _env("CDC_TRUNCATE_MODE", TRUNCATE_REPLICATE).strip().lower(),
        #: rubric 1.5. `replicate` drops the destination table when the source table
        #: is gone; `log` records the marker only; `ignore` disables detection.
        "drop_mode": _env("CDC_DROP_MODE", DROP_REPLICATE).strip().lower(),
    }


#: Truncate policies (`CDC_TRUNCATE_MODE`).
TRUNCATE_REPLICATE = "replicate"
TRUNCATE_LOG = "log"
TRUNCATE_IGNORE = "ignore"
TRUNCATE_MODES = (TRUNCATE_REPLICATE, TRUNCATE_LOG, TRUNCATE_IGNORE)

#: Drop policies (`CDC_DROP_MODE`).
DROP_REPLICATE = "replicate"
DROP_LOG = "log"
DROP_IGNORE = "ignore"
DROP_MODES = (DROP_REPLICATE, DROP_LOG, DROP_IGNORE)


@dataclass(frozen=True)
class CatalogConfig:
    """Source-catalog polling — the only way to see a `DROP TABLE` (rubric 1.5).

    The interval is deliberately short: rubric 2.3 wants new tables discovered
    "automatically on short interval", and this is the mechanism it will use, so the
    default is chosen for that rather than for drop detection alone. One poll is two
    small catalog queries on a separate connection.
    """

    poll_seconds: float = field(
        default_factory=lambda: float(_env("CDC_CATALOG_POLL_SECONDS", "10"))
    )
    #: Emit `pg_logical_emit_message(false, …)` on the source after a change is
    #: detected, so an LSN past the DDL is guaranteed to flow and the fence can open
    #: (ADR 0001 D9). Writes go to the PRIMARY; with 7.2's replica reads this is the
    #: same separate primary connection D9 already requires.
    emit_marker: bool = field(default_factory=lambda: _flag("CDC_CATALOG_MARKER", True))
    marker_prefix: str = field(
        default_factory=lambda: _env("CDC_CATALOG_MARKER_PREFIX", "cdcf_catalog")
    )
    #: 0 = never apply a DDL action the fence has not cleared. Anything else trades
    #: "the destination table might be re-created by an in-flight event" for
    #: "the drop is applied even though the source cannot be written to".
    grace_seconds: float = field(
        default_factory=lambda: float(_env("CDC_CATALOG_GRACE", "0"))
    )


def lease_ttl_seconds() -> float:
    return float(_env("CDC_LEASE_TTL", "60"))


def motherduck_token() -> str | None:
    """MotherDuck accepts either spelling of the env var; prefer the lowercase one."""
    return os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
