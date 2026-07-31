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


def motherduck_token() -> str | None:
    """MotherDuck accepts either spelling of the env var; prefer the lowercase one."""
    return os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
