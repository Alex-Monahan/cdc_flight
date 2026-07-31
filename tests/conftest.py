"""Fixtures for the cdc_flight test suite.

Everything runs natively: a project-local Postgres cluster on :15432 driven by
`scripts/pg.sh`, the Debezium embedded engine inside a JVM, and DuckDB on disk.
No Docker, no Kafka, no testcontainers.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import duckdb
import psycopg
import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
PG_SH = PROJECT_DIR / "scripts" / "pg.sh"
VENV_BIN = PROJECT_DIR / ".venv" / "bin"

sys.path.insert(0, str(PROJECT_DIR / "src"))

from cdc_flight.config import DestinationConfig, ReplicationConfig, SourceConfig


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _pg(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(PG_SH), *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=180,
    )


def _executable(name: str) -> str:
    """Prefer the project venv's console scripts, fall back to PATH."""
    candidate = VENV_BIN / name
    return str(candidate) if candidate.exists() else name


# --------------------------------------------------------------------------- #
# session-scoped environment
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def postgres_cluster() -> Iterator[SourceConfig]:
    """Start (if needed) the project-local Postgres cluster and load the schema.

    The cluster is intentionally left running afterwards: `initdb` + start costs a
    few seconds and every test session reseeds anyway. `make down` stops it.
    """
    if not PG_SH.exists():
        pytest.skip("scripts/pg.sh missing")
    _pg("start")
    _pg("seed")
    yield SourceConfig()


@pytest.fixture
def source_conn(postgres_cluster: SourceConfig) -> Iterator[psycopg.Connection]:
    with psycopg.connect(postgres_cluster.dsn) as conn:
        yield conn


@pytest.fixture
def fresh_seed(postgres_cluster: SourceConfig) -> SourceConfig:
    """Reload schema + seed data so a test starts from a known row set."""
    _pg("seed")
    return postgres_cluster


# --------------------------------------------------------------------------- #
# CDC state
# --------------------------------------------------------------------------- #
@pytest.fixture
def cdc_env(tmp_path: Path, postgres_cluster: SourceConfig) -> Iterator[dict[str, str]]:
    """Per-test Debezium offsets, dlt state, replication slot and DuckDB file."""
    slot = f"test_slot_{os.getpid()}_{abs(hash(tmp_path)) % 100000}"
    env = {
        **os.environ,
        "CDC_STATE_DIR": str(tmp_path / "cdc_state"),
        "CDC_PIPELINES_DIR": str(tmp_path / "cdc_state" / "dlt_pipelines"),
        "CDC_DUCKDB_PATH": str(tmp_path / "cdc_flight.duckdb"),
        "CDC_SLOT_NAME": slot,
        "CDC_PIPELINE_NAME": "cdc_flight_test",
        "RUNTIME__DLTHUB_TELEMETRY": "false",
    }
    _drop_slot(postgres_cluster, slot)
    yield env
    _drop_slot(postgres_cluster, slot)
    shutil.rmtree(tmp_path / "cdc_state", ignore_errors=True)


def _drop_slot(source: SourceConfig, slot: str) -> None:
    try:
        with psycopg.connect(source.dsn, autocommit=True) as conn:
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# actions
# --------------------------------------------------------------------------- #
@pytest.fixture
def run_pipeline(cdc_env: dict[str, str]):
    """Run the pipeline CLI as a subprocess and return its JSON summary.

    A subprocess (rather than an in-process call) keeps each run's JVM lifecycle
    clean - JPype allows exactly one JVM per process, and Debezium leaves
    non-daemon threads behind.
    """

    def _run(
        *,
        destination: str = "duckdb",
        max_seconds: float = 90,
        idle_seconds: float = 8,
        min_records: int = 0,
        reset_state: bool = False,
        snapshot_mode: str | None = None,
        extra_env: dict[str, str] | None = None,
        timeout: float = 300,
    ) -> dict:
        cmd = [
            _executable("cdc-flight"),
            "--destination",
            destination,
            "--max-seconds",
            str(max_seconds),
            "--idle-seconds",
            str(idle_seconds),
            "--min-records",
            str(min_records),
        ]
        if reset_state:
            cmd.append("--reset-state")
        if snapshot_mode:
            cmd += ["--snapshot-mode", snapshot_mode]

        env = {**cdc_env, **(extra_env or {})}
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env=env, cwd=PROJECT_DIR, timeout=timeout
        )
        if proc.returncode != 0:
            raise AssertionError(
                f"pipeline exited {proc.returncode}\n--- stdout ---\n{proc.stdout[-4000:]}"
                f"\n--- stderr ---\n{proc.stderr[-4000:]}"
            )
        # Debezium logs to stdout as well, so read the machine-readable summary
        # the CLI writes rather than trying to carve JSON out of the log stream.
        summary = Path(env["CDC_STATE_DIR"]) / "last_run.json"
        assert summary.exists(), f"no run summary at {summary}\n{proc.stdout[-4000:]}"
        return json.loads(summary.read_text())

    return _run


@pytest.fixture
def generate_changes(cdc_env: dict[str, str]):
    def _gen(scale: int = 1, seed: int = 42, waves: int = 1) -> dict:
        proc = subprocess.run(
            [
                _executable("cdc-datagen"),
                "changes",
                "--scale",
                str(scale),
                "--seed",
                str(seed),
                "--waves",
                str(waves),
            ],
            capture_output=True,
            text=True,
            env=cdc_env,
            cwd=PROJECT_DIR,
            check=True,
            timeout=120,
        )
        return json.loads(proc.stdout)

    return _gen


@pytest.fixture
def duck(cdc_env: dict[str, str]):
    """Read-only DuckDB connection to whatever the pipeline just wrote."""
    path = cdc_env["CDC_DUCKDB_PATH"]

    def _connect() -> duckdb.DuckDBPyConnection:
        return duckdb.connect(path, read_only=True)

    return _connect


# --------------------------------------------------------------------------- #
# misc
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def dataset() -> str:
    return DestinationConfig().dataset_name


@pytest.fixture(scope="session")
def replication() -> ReplicationConfig:
    return ReplicationConfig()
