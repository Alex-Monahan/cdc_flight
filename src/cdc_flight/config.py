"""Central configuration for the cdc_flight baseline pipeline.

Everything is overridable through environment variables so the same code runs
from a test fixture, a Makefile target, or (later) a MotherDuck Flight.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _instance_id() -> str:
    """Return the instance namespace used by default Postgres artifacts."""
    raw = _env(
        "CDC_TEST_INSTANCE_ID",
        _env("CDC_TEST_PGPORT", _env("PGPORT", "15432")),
    )
    return re.sub(r"[^a-z0-9_]+", "_", raw.lower()).strip("_") or "pg15432"


def _default_slot_name() -> str:
    """Keep the default logical slot unique when several clusters share a host."""
    prefix = _env("CDC_SLOT_PREFIX", "cdc_flight_slot_")
    return f"{prefix}{_instance_id()}"[:63]


def _instance_runtime_root() -> Path:
    """Keep normal pipeline artifacts disjoint across selected instances."""
    return PROJECT_DIR / ".cdc_instances" / _instance_id()


@dataclass(frozen=True)
class SourceConfig:
    """Connection details for the project-local Postgres cluster."""

    host: str = field(default_factory=lambda: _env("PGHOST", "127.0.0.1"))
    port: int = field(
        default_factory=lambda: int(_env("CDC_TEST_PGPORT", _env("PGPORT", "15432")))
    )
    user: str = field(default_factory=lambda: _env("PGUSER", "postgres"))
    password: str = field(default_factory=lambda: _env("PGPASSWORD", "postgres"))
    dbname: str = field(
        default_factory=lambda: _env(
            "CDC_TEST_PGDATABASE", _env("PGDATABASE", "cdc_source")
        )
    )
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

    @property
    def schemas(self) -> set[str] | None:
        """Optional catalog scope; ``None`` means all non-system schemas."""
        raw = os.environ.get("CDC_SCHEMA_INCLUDE_LIST") or os.environ.get("CDC_SCHEMAS")
        if not raw:
            return None
        return {item.strip() for item in raw.split(",") if item.strip()}

    @property
    def auto_discovery(self) -> bool:
        return _flag("CDC_AUTO_DISCOVERY", True)

    @property
    def publication_ownership(self) -> str:
        """Who may admit an auto-discovered table to the publication.

        The default is deliberately explicit: the Flight owns the table-scoped
        publication admission it performs. Deployments whose publication is managed
        outside this process set ``CDC_PUBLICATION_OWNERSHIP=external``; in that mode
        discovery waits for membership and never issues an ALTER.
        """
        value = _env("CDC_PUBLICATION_OWNERSHIP", "flight").strip().lower()
        if value not in {"flight", "external"}:
            raise ValueError(
                "CDC_PUBLICATION_OWNERSHIP must be 'flight' or 'external', "
                f"got {value!r}"
            )
        return value


@dataclass(frozen=True)
class ReplicationConfig:
    """Debezium / logical-decoding identifiers."""

    slot_name: str = field(default_factory=lambda: _env("CDC_SLOT_NAME", _default_slot_name()))
    publication_name: str = field(
        default_factory=lambda: _env("CDC_PUBLICATION", "cdc_flight_pub")
    )
    topic_prefix: str = field(default_factory=lambda: _env("CDC_TOPIC_PREFIX", "cdcflight"))
    snapshot_mode: str = field(default_factory=lambda: _env("CDC_SNAPSHOT_MODE", "initial"))
    state_dir: Path = field(
        default_factory=lambda: Path(
            _env("CDC_STATE_DIR", str(_instance_runtime_root() / "cdc_state"))
        )
    )

    @property
    def offset_file(self) -> Path:
        return self.state_dir / "offsets.dat"


@dataclass(frozen=True)
class DestinationConfig:
    """Where dlt writes the change events."""

    kind: str = field(default_factory=lambda: _env("CDC_DESTINATION", "duckdb"))
    pipeline_name: str = field(
        default_factory=lambda: _env("CDC_PIPELINE_NAME", f"cdc_flight_{_instance_id()}")
    )
    dataset_name: str = field(default_factory=lambda: _env("CDC_DATASET", "cdc_raw"))
    duckdb_path: Path = field(
        default_factory=lambda: Path(
            _env("CDC_DUCKDB_PATH", str(_instance_runtime_root() / "cdc_flight.duckdb"))
        )
    )
    motherduck_database: str = field(
        default_factory=lambda: _env("CDC_MD_DATABASE", "cdc_flight_dev")
    )
    pipelines_dir: Path = field(
        default_factory=lambda: Path(
            _env(
                "CDC_PIPELINES_DIR",
                str(_instance_runtime_root() / "cdc_state" / "dlt_pipelines"),
            )
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
    #: How long the *source* may be completely unaskable before the run is failed
    #: (TODO 4.6(b)). A sampler that has never succeeded is exempt - see
    #: `SourceHealth.may_declare_idle` - so this only ever fires on a source that
    #: was answering and went dark, which is the silently-dead-node shape. It makes
    #: detection bounded instead of "whenever --max-seconds happens to expire".
    source_dark_seconds: float = field(
        default_factory=lambda: float(_env("CDC_SOURCE_DARK_SECONDS", "45"))
    )
    #: How long a destination `COMMIT` may take before the run is aborted with a
    #: non-zero exit (rubric 1.7 / 4.5). A hung COMMIT is otherwise unbounded, and
    #: "the process hangs" is neither a clean recovery nor a loud failure. Killing
    #: the process is safe *because* of Invariant O: the offset store is untouched
    #: until after COMMIT returns, so whichever way the ambiguous commit went, the
    #: next run resumes at exactly what the destination holds.
    commit_timeout: float = field(
        default_factory=lambda: float(_env("CDC_COMMIT_TIMEOUT", "300"))
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
        #: (the rubric's "tombstone/soft delete" behaviour, =3). `ignore` is a
        #: destination no-op; the raw event remains decoded for generation proofing.
        "truncate_mode": _env("CDC_TRUNCATE_MODE", TRUNCATE_REPLICATE).strip().lower(),
        #: rubric 1.5. `replicate` drops the destination table when the source table
        #: is gone; `log` records the marker only; `ignore` disables detection.
        "drop_mode": _env("CDC_DROP_MODE", DROP_REPLICATE).strip().lower(),
        #: rubric 1.5 circuit breaker (Opus MAJOR-3 / Q2). A genuine single
        #: `DROP TABLE` is the overwhelmingly common real case and stays fully
        #: automatic; every plural case is either a schema-level migration
        #: (`DROP SCHEMA … CASCADE`, `DROP SCHEMA; CREATE SCHEMA`) or a
        #: misconfiguration (a DSN repointed at an empty database, a failover target,
        #: a source mid-`pg_restore`), and both want a human. The whole set is
        #: refused, never half of it.
        "drop_max_per_group": int(_env("CDC_DROP_MAX_PER_POLL", "1")),
        "drop_allow_mass": _flag("CDC_DROP_ALLOW_MASS", False),
        #: Disable only the plain-drop DDL revalidation. Replacement generations still
        #: require the final proof because bypassing that fence can merge lifecycles.
        "drop_revalidate": _flag("CDC_DROP_REVALIDATE", True),
        #: rubric 4.7. An undecidable fold used to be a PERMANENT failure: the group
        #: rolls back (correctly), the transaction replays, the same ambiguity is hit,
        #: for ever. That is a manual-intervention case, which 4.7 scores. Default ON:
        #: the affected table is queued for an automatic re-snapshot, whose consistent
        #: point necessarily fences the transaction that cannot be folded.
        "resnapshot_on_ambiguity": _flag("CDC_AMBIGUOUS_RESNAPSHOT", True),
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
    #: Emit a **transactional** `pg_logical_emit_message(true, …)` on the source after
    #: a change is detected, so an LSN past the DDL is guaranteed to flow and the fence
    #: can open (ADR 0001 D9; `cdc_flight.source_marker` records the measurement that
    #: makes `true` load-bearing). Writes go to the PRIMARY; with 7.2's replica reads
    #: this is the same separate primary connection D9 already requires.
    emit_marker: bool = field(default_factory=lambda: _flag("CDC_CATALOG_MARKER", True))
    marker_prefix: str = field(
        default_factory=lambda: _env("CDC_CATALOG_MARKER_PREFIX", "cdcf")
    )
    #: 0 = never apply a DDL action the fence has not cleared. Anything else trades
    #: "the destination table might be re-created by an in-flight event" for
    #: "the drop is applied even though the source cannot be written to", and a
    #: non-zero value is EXCLUDED from the structural correctness claim
    #: (ADR 0001 §18/A38).
    grace_seconds: float = field(
        default_factory=lambda: float(_env("CDC_CATALOG_GRACE", "0"))
    )
    #: How many consecutive polls must agree before a DESTRUCTIVE change is queued at
    #: all (Opus Q5). Costs at most one poll interval of latency on a real drop and
    #: removes a whole class of transient-catalog and mid-DDL false positive.
    confirm_polls: int = field(
        default_factory=lambda: int(_env("CDC_DROP_CONFIRM_POLLS", "2"))
    )
    #: Cap on how many fence markers one run writes to the source while a destructive
    #: change stays unresolved (Opus MINOR-1): a fence that never opens would
    #: otherwise write one WAL record per poll for ever against a source cdc_flight
    #: otherwise only reads. 0 = uncapped.
    marker_max_writes: int = field(
        default_factory=lambda: int(_env("CDC_CATALOG_MARKER_MAX", "60"))
    )
    #: How long a quiet run holds the engine open after its final catalog poll, so a
    #: destructive change it just queued can be fenced and applied by THIS run rather
    #: than the next one (Codex 6). A change still unresolved after this makes the run
    #: non-successful.
    drain_seconds: float = field(
        default_factory=lambda: float(_env("CDC_CATALOG_DRAIN_SECONDS", "30"))
    )


def lease_ttl_seconds() -> float:
    return float(_env("CDC_LEASE_TTL", "60"))


def motherduck_token() -> str | None:
    """MotherDuck accepts either spelling of the env var; prefer the lowercase one."""
    return os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
