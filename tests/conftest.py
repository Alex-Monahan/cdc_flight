"""Fixtures for the cdc_flight test suite.

Everything runs natively: a project-local Postgres cluster on :15432 driven by
`scripts/pg.sh`, the Debezium embedded engine inside a JVM, and DuckDB on disk.
No Docker, no Kafka, no testcontainers.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
PG_SH = PROJECT_DIR / "scripts" / "pg.sh"
VENV_BIN = PROJECT_DIR / ".venv" / "bin"

#: Tables the pipeline replicates. Used to fingerprint the shared source so a
#: concurrent writer produces a diagnostic instead of a mystery assertion.
CAPTURED_TABLES = (
    "customers",
    "orders",
    "sensor_readings",
    "documents",
    "wide_types",
    "audit_log",
)

sys.path.insert(0, str(PROJECT_DIR / "src"))
# `tests/applier_lab.py` is shared by the per-rubric suites in subdirectories,
# which pytest imports with only their own directory on `sys.path`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

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


@pytest.fixture(autouse=True)
def _reset_fault_spec():
    """`faults` caches the parsed `CDC_FAULT_INJECT` (it is read from inside the
    commit->ack window, which must contain nothing else - Codex 7). Re-read it
    around every test so an in-process fault cannot leak into the next one."""
    from cdc_flight import faults

    faults.refresh()
    yield
    faults.refresh()


# --------------------------------------------------------------------------- #
# session-scoped environment
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def exclusive_source() -> Iterator[Path]:
    """Serialise whole test sessions against the shared Postgres cluster.

    Every sandbox has a private slot, offset directory and DuckDB file, but they
    all mutate the *same* `app` schema and publication, and `reseed()` drops and
    recreates both (`sql/01_schema.sql:7-12`, `:142-150`). Two concurrent
    sessions - two reviewers running `make test` at once against :15432, which is
    exactly what happened during the 1.0 review - therefore corrupt each other:
    one session's snapshot picks up another's rows, or its publication vanishes
    mid-run. That produced the review's "1 failed, 21 passed" and Codex's
    "healthy snapshot contained 40 rather than 20 records".

    A whole-session exclusive `flock` is the cheapest fix that actually removes
    the class of failure rather than papering over one symptom (Opus B4,
    Codex 12). Per-worker databases would be better and are the follow-up if the
    suite is ever parallelised.
    """
    lock_path = PROJECT_DIR / ".pytest-source.lock"
    lock_path.touch(exist_ok=True)
    handle = lock_path.open("r+")
    waited = 0.0
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if waited == 0.0:
                print(
                    f"\nwaiting for another test session to release {lock_path} "
                    "(the Postgres cluster on :15432 is shared)"
                )
            time.sleep(1.0)
            waited += 1.0
            if waited > 1800:
                pytest.fail(f"timed out waiting 30 min for {lock_path}")
    try:
        yield lock_path
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


@pytest.fixture(scope="session")
def postgres_cluster(exclusive_source: Path) -> Iterator[SourceConfig]:
    """Start (if needed) the project-local Postgres cluster and load the schema.

    The cluster is intentionally left running afterwards: `initdb` + start costs a
    few seconds and every test session reseeds anyway. `make down` stops it.
    """
    if not PG_SH.exists():
        pytest.skip("scripts/pg.sh missing")
    _pg("start")
    _pg("seed")
    yield SourceConfig()


def source_fingerprint(source: SourceConfig) -> dict[str, int]:
    """Row counts of every captured table.

    Compared across a window in which the test itself makes no source changes;
    a difference means *something else* wrote to the shared cluster, which is a
    much more useful failure message than `assert 110 == 0`.
    """
    with psycopg.connect(source.dsn, autocommit=True) as conn:
        return {
            table: conn.execute(f"SELECT count(*) FROM app.{table}").fetchone()[0]
            for table in CAPTURED_TABLES
        }


def kill_walsender(source: SourceConfig, slot: str) -> int:
    """Terminate the walsender backend holding `slot`. Returns how many were killed.

    Scoped to one slot on purpose: killing every walsender would also kill any
    other suite's replication connection on this shared cluster.
    """
    with psycopg.connect(source.dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
            "WHERE slot_name = %s AND active",
            (slot,),
        ).fetchall()
    return len(rows)


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
        expect_success: bool = True,
        accept_orphan_offsets: bool = False,
    ) -> dict:
        return _invoke_pipeline(
            {**cdc_env, **(extra_env or {})},
            destination=destination,
            max_seconds=max_seconds,
            idle_seconds=idle_seconds,
            min_records=min_records,
            reset_state=reset_state,
            snapshot_mode=snapshot_mode,
            timeout=timeout,
            expect_success=expect_success,
            accept_orphan_offsets=accept_orphan_offsets,
        )

    return _run


def _invoke_pipeline(
    env: dict[str, str],
    *,
    destination: str = "duckdb",
    max_seconds: float = 90,
    idle_seconds: float = 8,
    min_records: int = 0,
    reset_state: bool = False,
    snapshot_mode: str | None = None,
    timeout: float = 300,
    expect_success: bool = True,
    accept_orphan_offsets: bool = False,
) -> dict:
    """Run the `cdc-flight` CLI once and return its summary plus process outcome.

    A subprocess (rather than an in-process call) keeps each run's JVM lifecycle
    clean - JPype allows exactly one JVM per process, and Debezium leaves
    non-daemon threads behind.
    """
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
    if accept_orphan_offsets:
        cmd.append("--accept-orphan-offsets")
    if snapshot_mode:
        cmd += ["--snapshot-mode", snapshot_mode]

    # Drop any previous summary so the one we read back is unambiguously this
    # run's - a crash-injected run writes none at all.
    summary_path = Path(env["CDC_STATE_DIR"]) / "last_run.json"
    summary_path.unlink(missing_ok=True)

    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=PROJECT_DIR, timeout=timeout
    )
    if expect_success and proc.returncode != 0:
        raise AssertionError(
            f"pipeline exited {proc.returncode}\n--- stdout ---\n{proc.stdout[-4000:]}"
            f"\n--- stderr ---\n{proc.stderr[-4000:]}"
        )

    # Debezium logs to stdout as well, so read the machine-readable summary the
    # CLI writes rather than trying to carve JSON out of the log stream.
    summary: dict[str, Any] = {}
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
    if expect_success:
        assert summary, f"no run summary at {summary_path}\n{proc.stdout[-4000:]}"
    summary["returncode"] = proc.returncode
    # Kept short on purpose: this dict is printed verbatim in assertion messages.
    summary["output"] = (proc.stdout + proc.stderr)[-6000:]
    return summary


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
# module-scoped sandbox (used by the rubric gap suites under tests/<item>_*/)
# --------------------------------------------------------------------------- #
class Sandbox:
    """An isolated CDC environment: own slot, offsets, dlt state and DuckDB file.

    The rubric gap suites need a *scenario* (several pipeline runs plus source
    DML) that many assertions then interrogate. Re-running the scenario per test
    would cost ~30 s each, so the scenario fixture is module-scoped - pytest runs
    all tests in a module consecutively, so a module-scoped sandbox is never
    interleaved with another module's `seed` calls.
    """

    DATASET = "cdc_raw"

    def __init__(self, name: str, base: Path, source: SourceConfig):
        self.name = name
        self.dir = base
        self.dir.mkdir(parents=True, exist_ok=True)
        self.source = source
        self.slot = re.sub(r"[^a-z0-9_]", "_", f"t_{name}_{os.getpid()}".lower())[:60]
        self.duckdb_path = self.dir / "cdc_flight.duckdb"
        self.state_dir = self.dir / "cdc_state"
        self.env = {
            **os.environ,
            "CDC_STATE_DIR": str(self.state_dir),
            "CDC_PIPELINES_DIR": str(self.state_dir / "dlt_pipelines"),
            "CDC_DUCKDB_PATH": str(self.duckdb_path),
            "CDC_SLOT_NAME": self.slot,
            "CDC_PIPELINE_NAME": f"cdc_flight_{re.sub(r'[^a-z0-9_]', '_', name.lower())}",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
        }
        self.drop_slot()

    # -- lifecycle ---------------------------------------------------------- #
    @property
    def offset_file(self) -> Path:
        return self.state_dir / "offsets.dat"

    def drop_slot(self) -> None:
        _drop_slot(self.source, self.slot)

    def reseed(self) -> None:
        _pg("seed")

    def cleanup(self) -> None:
        self.drop_slot()

    # -- source ------------------------------------------------------------- #
    def sql(self, statements: str | list[str], *, one_transaction: bool = False) -> None:
        if isinstance(statements, str):
            statements = [statements]
        with psycopg.connect(self.source.dsn, autocommit=not one_transaction) as conn:
            for stmt in statements:
                conn.execute(stmt)
            if one_transaction:
                conn.commit()

    def pg_query(self, stmt: str, params: tuple | None = None) -> list[tuple]:
        with psycopg.connect(self.source.dsn, autocommit=True) as conn:
            return conn.execute(stmt, params).fetchall()

    # -- pipeline ----------------------------------------------------------- #
    def run(self, *, extra_env: dict[str, str] | None = None, **kwargs) -> dict:
        kwargs.setdefault("max_seconds", 120)
        kwargs.setdefault("idle_seconds", 6)
        return _invoke_pipeline({**self.env, **(extra_env or {})}, **kwargs)

    def spawn(
        self,
        *,
        max_seconds: float = 300,
        idle_seconds: float = 10,
        destination: str = "duckdb",
        extra_env: dict[str, str] | None = None,
        capture: bool = False,
    ) -> subprocess.Popen:
        """Start the pipeline as a killable child process (fault injection)."""
        sink = subprocess.PIPE if capture else subprocess.DEVNULL
        return subprocess.Popen(
            [
                _executable("cdc-flight"),
                "--destination",
                destination,
                "--max-seconds",
                str(max_seconds),
                "--idle-seconds",
                str(idle_seconds),
            ],
            env={**self.env, **(extra_env or {})},
            cwd=PROJECT_DIR,
            stdout=sink,
            stderr=sink,
            text=capture,
        )

    def last_summary(self) -> dict:
        """The JSON summary the CLI wrote for its most recent run, if any."""
        path = self.state_dir / "last_run.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def kill_walsender(self) -> int:
        return kill_walsender(self.source, self.slot)

    # -- destination -------------------------------------------------------- #
    def duck_query(self, stmt: str, params: list | None = None) -> list[tuple]:
        con = duckdb.connect(str(self.duckdb_path), read_only=True)
        try:
            return con.execute(stmt, params or []).fetchall()
        finally:
            con.close()

    def scalar(self, stmt: str, params: list | None = None):
        return self.duck_query(stmt, params)[0][0]

    def table(self, name: str) -> str:
        return f'"{self.DATASET}"."{name}"'


CRASH_REPLAY_CUSTOMERS = 50
CRASH_REPLAY_READINGS = 60
#: How many *byte-identical* keyless rows the scenario inserts on purpose.
#: This is the case that separates exactly-once delivery from deduplication: a
#: `SELECT DISTINCT` (or any dedupe by row content) collapses these two rows and
#: is therefore WRONG, while a crash-replay copy of them must not survive
#: (Opus M6 / Codex 8).
CRASH_REPLAY_IDENTICAL = 2
IDENTICAL_SENSOR = "REPLAY-IDENTICAL"
IDENTICAL_READING_AT = "2026-07-30T12:00:00+00:00"
IDENTICAL_VALUE = 42.5


@pytest.fixture(scope="session")
def crash_replay(tmp_path_factory, postgres_cluster: SourceConfig) -> Iterator[dict]:
    """Deterministic crash in the at-least-once window, then a restart.

    Shared by `tests/1.1_exactly_once_pk/` and `tests/1.2_exactly_once_nopk/` so
    the ~75 s scenario is paid for once. It is safe as a session fixture because
    the whole scenario completes inside a single fixture setup - no other test
    can interleave a `seed` into it - and every later assertion only reads the
    sandbox's private DuckDB file. It also **reseeds on teardown**, so the shared
    cluster is left in its canonical state for whatever runs next.

    The fault is injected at `post_commit_pre_ack` (see `cdc_flight.faults`): the
    batch is committed to the destination and then the process is `os._exit`-ed
    *before* `markProcessed()` / `markBatchFinished()` run, so Debezium's offset
    file still points before the batch and the replication slot was never
    confirmed past it. That is precisely the window a `kill -9` hits, made exact.

    (Rolling the offset *file* back instead does not work reliably: Postgres will
    not stream from before the slot's `restart_lsn`, which has already advanced,
    so only the tail of the batch replays. Measured, not assumed.)

    The scenario writes three things in ONE Postgres transaction:

    * `CRASH_REPLAY_CUSTOMERS` rows in a table WITH a primary key (1.1);
    * `CRASH_REPLAY_READINGS` rows with distinct values in a table WITHOUT one (1.2);
    * `CRASH_REPLAY_IDENTICAL` **byte-identical** rows in that same keyless table.

    `source_events` is the ledger every "exactly once" assertion is measured
    against: the number of change events Postgres actually produced.
    """
    box = Sandbox("crash_replay", tmp_path_factory.mktemp("sbx_crash_replay"), postgres_cluster)
    try:
        box.reseed()
        baseline = box.run(reset_state=True, max_seconds=150)

        box.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT "
                "'replay-c-' || i, 'replay-c-' || i || '@example.com' "
                f"FROM generate_series(1, {CRASH_REPLAY_CUSTOMERS}) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                "'REPLAY', i * 1.5, 'C' "
                f"FROM generate_series(1, {CRASH_REPLAY_READINGS}) i",
                # Two rows that are identical in every column, inserted on
                # purpose. Nothing downstream may treat them as one.
                "INSERT INTO app.sensor_readings (sensor_id, reading_at, value, unit) "
                f"SELECT '{IDENTICAL_SENSOR}', TIMESTAMPTZ '{IDENTICAL_READING_AT}', "
                f"{IDENTICAL_VALUE}, 'C' FROM generate_series(1, {CRASH_REPLAY_IDENTICAL}) i",
            ],
            one_transaction=True,
        )

        crashed = box.run(
            max_seconds=150,
            expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
        )
        if crashed["returncode"] != 137:
            # A bare `assert` here errors every test in 1.1 and 1.2 with the same
            # opaque message; say what actually went wrong instead (Opus M7).
            raise RuntimeError(
                "fault injection did not fire at post_commit_pre_ack:1. Expected the "
                f"process to exit 137, got {crashed['returncode']}. This usually means "
                "the fault point moved or the first DATA batch was consumed before the "
                f"fault could arm. summary={ {k: v for k, v in crashed.items() if k != 'output'} }"
                f"\n--- tail ---\n{crashed.get('output', '')[-3000:]}"
            )

        replayed = box.run(max_seconds=150)

        yield {
            "box": box,
            "baseline": baseline,
            "crashed": crashed,
            "replayed": replayed,
            "customers": CRASH_REPLAY_CUSTOMERS,
            "readings": CRASH_REPLAY_READINGS,
            "identical": CRASH_REPLAY_IDENTICAL,
            "identical_sensor": IDENTICAL_SENSOR,
            # The ledger: exactly how many change events the source produced in
            # the replayed transaction. "Exactly once" means the destination
            # holds this many change events, no more and no fewer.
            "source_events": CRASH_REPLAY_CUSTOMERS + CRASH_REPLAY_READINGS + CRASH_REPLAY_IDENTICAL,
        }
    finally:
        box.cleanup()
        # Leave the shared source in its canonical state (Opus B4 / Codex 12).
        with contextlib.suppress(Exception):  # teardown must not mask a failure
            _pg("seed")


@pytest.fixture(scope="module")
def sandbox(request, tmp_path_factory, postgres_cluster: SourceConfig) -> Iterator[Sandbox]:
    name = Path(request.module.__file__).stem.replace("test_", "")
    box = Sandbox(name, tmp_path_factory.mktemp(f"sbx_{abs(hash(name)) % 10000}"), postgres_cluster)
    try:
        yield box
    finally:
        box.cleanup()
        shutil.rmtree(box.state_dir, ignore_errors=True)


# --------------------------------------------------------------------------- #
# misc
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="session")
def dataset() -> str:
    return DestinationConfig().dataset_name


@pytest.fixture(scope="session")
def replication() -> ReplicationConfig:
    return ReplicationConfig()
