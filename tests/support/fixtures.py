"""Fixtures for the cdc_flight test suite.

Everything runs natively: a project-local Postgres cluster driven by
`scripts/pg.sh`, the Debezium embedded engine inside a JVM, and DuckDB on disk.
No Docker, no Kafka, no testcontainers. Physical-cluster ownership is derived
from the canonical data directory, host, and port; the logical instance ID only
names databases, slots, and other non-authority resources.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Must precede DuckDB/PyArrow imports. PyArrow 25.0.0's default mimalloc backend can
# SIGSEGV on the JPype JVM callback thread used by the live recovery path (ADR A14/A66).
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

import duckdb
import psycopg
import pytest

PROJECT_DIR = Path(__file__).resolve().parents[2]
VENV_BIN = PROJECT_DIR / ".venv" / "bin"
MATRIX_CHILD = PROJECT_DIR / "tests" / "support" / "crash_matrix_child.py"
SANDBOX_IDLE_SECONDS = 6

#: Debezium delivers a transactional logical message exactly like any other source
#: transaction: BEGIN, the message, END. `cdc_flight` writes two kinds, and they
#: are the only writes it ever makes to a source: the run's own completion
#: watermark (`cdc_flight.completion_watermark`) and the catalog fence / idle
#: slot hand-off (`cdc_flight.source_marker`).
MARKER_RECORDS = 3


def source_records(summary: dict) -> int:
    """Records a run received that the SOURCE, not the Flight itself, produced.

    ``SourceMarker.writes`` is a source-side fact, not a delivery fact: the shutdown
    marker can be written after the last admitted callback and never reach this run.
    Production summaries therefore report the exact raw marker records that crossed
    callback admission.  Keep the old arithmetic only for summaries from older
    builds that do not carry that receipt counter.
    """
    received = summary.get("source_marker_records_received")
    if received is not None:
        return summary["records"] - int(received)
    written = summary.get("completion_watermark_arms", 0) + summary.get(
        "source_marker", {}
    ).get("source_markers", 0)
    return summary["records"] - MARKER_RECORDS * written


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
# The support package is shared by the per-rubric suites and direct subprocess
# drivers; the repository's top-level conftest adds `tests/` to `sys.path`.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cdc_flight.config import (
    DestinationConfig,
    ReplicationConfig,
    SourceConfig,
    motherduck_token,
)
from support import postgres_test_instance
from support.motherduck_probe import _drop_database, _drop_schema, create_database

POSTGRES_TEST_INSTANCE = postgres_test_instance.INSTANCE
PG_SH = POSTGRES_TEST_INSTANCE.pg_sh
_enforce_no_worker_restarts = postgres_test_instance._enforce_no_worker_restarts
pytest_configure = postgres_test_instance.pytest_configure
pytest_sessionstart = postgres_test_instance.pytest_sessionstart
pytest_unconfigure = postgres_test_instance.pytest_unconfigure
exclusive_source = postgres_test_instance.exclusive_source
postgres_cluster = postgres_test_instance.postgres_cluster

TEST_PGPORT = POSTGRES_TEST_INSTANCE.port
TEST_PGDATABASE = POSTGRES_TEST_INSTANCE.database
TEST_PGDATA = POSTGRES_TEST_INSTANCE.data_dir
TEST_PGSOCKET = POSTGRES_TEST_INSTANCE.socket_dir
TEST_PGLOG = POSTGRES_TEST_INSTANCE.log_path
TEST_CLUSTER_SENTINEL = POSTGRES_TEST_INSTANCE.sentinel
TEST_INSTANCE_ID = POSTGRES_TEST_INSTANCE.instance_id
TEST_SLOT_PREFIX = POSTGRES_TEST_INSTANCE.slot_prefix
TEMPLATE_DATABASE_PREFIX = POSTGRES_TEST_INSTANCE.template_database_prefix
WORKER_DATABASE_PREFIX = POSTGRES_TEST_INSTANCE.worker_database_prefix
TEST_LOCK_PATH = POSTGRES_TEST_INSTANCE.run_lock_path
TEST_SETUP_LOCK_PATH = POSTGRES_TEST_INSTANCE.setup_lock_path

_pg = POSTGRES_TEST_INSTANCE.pg
_acquire_test_run_lock = POSTGRES_TEST_INSTANCE.acquire_run_lock
_required_replication_capacity = POSTGRES_TEST_INSTANCE.required_replication_capacity
_isolated_source = POSTGRES_TEST_INSTANCE.isolated_source
_require_disposable_cluster = POSTGRES_TEST_INSTANCE.require_disposable_cluster
_owned_database_name = POSTGRES_TEST_INSTANCE.owns_database
_reset_test_database = POSTGRES_TEST_INSTANCE.reset_test_database
_source_environment = POSTGRES_TEST_INSTANCE.source_environment
_drop_slot = POSTGRES_TEST_INSTANCE.drop_slot


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
    faults.reset_arrivals()
    yield
    faults.refresh()
    faults.reset_arrivals()


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
    """Clone the canonical template so a test starts from a known row set."""
    _reset_test_database(postgres_cluster)
    return postgres_cluster


# --------------------------------------------------------------------------- #
# CDC state
# --------------------------------------------------------------------------- #
@pytest.fixture
def cdc_env(tmp_path: Path, postgres_cluster: SourceConfig) -> Iterator[dict[str, str]]:
    """Per-test Debezium offsets, dlt state, replication slot and DuckDB file."""
    path_digest = hashlib.sha256(str(tmp_path).encode()).hexdigest()[:10]
    suffix = f"{os.getpid()}_{path_digest}"
    slot = f"{TEST_SLOT_PREFIX[: 63 - len(suffix)]}{suffix}"
    env = {
        **_source_environment(postgres_cluster),
        "CDC_STATE_DIR": str(tmp_path / "cdc_state"),
        "CDC_PIPELINES_DIR": str(tmp_path / "cdc_state" / "dlt_pipelines"),
        "CDC_DUCKDB_PATH": str(tmp_path / "cdc_flight.duckdb"),
        "CDC_SLOT_NAME": slot,
        "CDC_PIPELINE_NAME": f"cdc_flight_test_{TEST_INSTANCE_ID}",
        "RUNTIME__DLTHUB_TELEMETRY": "false",
    }
    _drop_slot(postgres_cluster, slot)
    yield env
    _drop_slot(postgres_cluster, slot)
    shutil.rmtree(tmp_path / "cdc_state", ignore_errors=True)


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
    matrix_arm: bool = False,
) -> dict:
    """Run the `cdc-flight` CLI once and return its summary plus process outcome.

    A subprocess (rather than an in-process call) keeps each run's JVM lifecycle
    clean - JPype allows exactly one JVM per process, and Debezium leaves
    non-daemon threads behind.
    """
    executable = (
        [sys.executable, str(MATRIX_CHILD)]
        if matrix_arm
        else [_executable("cdc-flight")]
    )
    cmd = [
        *executable,
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
        cmd,
        capture_output=True,
        text=True,
        env=env,
        cwd=PROJECT_DIR,
        timeout=timeout,
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
# MotherDuck state
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MotherDuckWorker:
    """One cloud database owned by one pytest worker for the whole session."""

    token: str
    database: str
    worker_id: str


def _motherduck_worker_id() -> str:
    raw = os.environ.get("PYTEST_XDIST_WORKER", "serial")
    return re.sub(r"[^a-z0-9_]", "_", raw.lower()).strip("_") or "serial"


@pytest.fixture(scope="session")
def motherduck_worker() -> Iterator[MotherDuckWorker]:
    """Create exactly one clearly named MotherDuck database per xdist worker."""

    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    worker_id = _motherduck_worker_id()
    database = (
        f"cdc_flight_md_{TEST_INSTANCE_ID}_{worker_id}_{uuid.uuid4().hex[:10]}"
    )
    create_database(token, database)
    try:
        yield MotherDuckWorker(token, database, worker_id)
    finally:
        _drop_database(token, database)


def _motherduck_case(worker: MotherDuckWorker) -> dict[str, str]:
    """Allocate a unique schema and dataset inside one worker database."""
    suffix = uuid.uuid4().hex[:10]
    control_schema = f"_cdc_flight_{worker.worker_id}_{suffix}"
    dataset = f"cdc_md_{worker.worker_id}_{suffix}"
    return {
        "token": worker.token,
        "database": worker.database,
        "worker_id": worker.worker_id,
        "control_schema": control_schema,
        "dataset": dataset,
    }


def _motherduck_case_cleanup(worker: MotherDuckWorker, case: dict[str, str]) -> None:
    _drop_schema(worker.token, worker.database, case["control_schema"])


@pytest.fixture(scope="module")
def motherduck_module_case(
    motherduck_worker: MotherDuckWorker,
) -> Iterator[dict[str, str]]:
    """Give a module one unique schema, cleaned up after all its assertions.

    This is intentionally separate from ``motherduck_case``.  A module fixture may
    share the destination state only when the module's tests are all read-only
    assertions over one setup scenario; ordinary tests keep the per-test fixture.
    """
    case = _motherduck_case(motherduck_worker)
    try:
        yield case
    finally:
        _motherduck_case_cleanup(motherduck_worker, case)


@pytest.fixture
def motherduck_case(motherduck_worker: MotherDuckWorker) -> Iterator[dict[str, str]]:
    """Give one test a unique schema inside its worker-owned database."""

    case = _motherduck_case(motherduck_worker)
    try:
        yield case
    finally:
        _motherduck_case_cleanup(motherduck_worker, case)


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
        raw_slot = re.sub(
            r"[^a-z0-9_]", "_", f"{TEST_SLOT_PREFIX}t_{name}_{os.getpid()}".lower()
        )
        # PostgreSQL limits slot names to 63 bytes.  Leave three bytes for the
        # live-discovery fixture's ``_rs`` suffix, and keep a digest in the
        # truncated portion: names such as ``destination_commit`` and
        # ``destination_commit_late`` must never alias on one xdist worker.
        slot_digest = hashlib.sha256(raw_slot.encode()).hexdigest()[:10]
        self.slot = f"{raw_slot[:49]}_{slot_digest}"
        self.duckdb_path = self.dir / "cdc_flight.duckdb"
        self.state_dir = self.dir / "cdc_state"
        self.env = {
            **_source_environment(source),
            "CDC_STATE_DIR": str(self.state_dir),
            "CDC_PIPELINES_DIR": str(self.state_dir / "dlt_pipelines"),
            "CDC_DUCKDB_PATH": str(self.duckdb_path),
            "CDC_SLOT_NAME": self.slot,
            "CDC_PIPELINE_NAME": (
                f"cdc_flight_{TEST_INSTANCE_ID}_"
                f"{re.sub(r'[^a-z0-9_]', '_', name.lower())}"
            ),
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
        self.drop_slot()
        _reset_test_database(self.source)

    def cleanup(self) -> None:
        self.drop_slot()

    # -- source ------------------------------------------------------------- #
    def sql(
        self,
        statements: str | list[str],
        *,
        one_transaction: bool = False,
        report_affected: bool = False,
    ) -> int | None:
        if isinstance(statements, str):
            statements = [statements]
        affected = 0
        with psycopg.connect(self.source.dsn, autocommit=not one_transaction) as conn:
            for stmt in statements:
                if report_affected:
                    result = conn.execute(stmt)
                    if result.rowcount > 0:
                        affected += result.rowcount
                else:
                    conn.execute(stmt)
            if one_transaction:
                conn.commit()
        return affected if report_affected else None

    def pg_query(self, stmt: str, params: tuple | None = None) -> list[tuple]:
        with psycopg.connect(self.source.dsn, autocommit=True) as conn:
            return conn.execute(stmt, params).fetchall()

    def wait_for_slot_active(
        self,
        *,
        process: subprocess.Popen | None = None,
        timeout: float = 30.0,
        poll_seconds: float = 0.1,
    ) -> None:
        """Wait until this sandbox's live pipeline owns its replication slot.

        A live-discovery scenario must issue its DDL after the main engine has
        connected; otherwise the same assertions could accidentally exercise
        restart-time discovery.  Polling ``pg_replication_slots.active`` is the
        predicate that proves that condition.  The catalog watcher is started
        before the engine in the pipeline, and the scenario separately waits for
        its throwaway snapshot slot to start and retire, so this replaces only
        the old arbitrary startup sleep.
        """
        deadline = time.monotonic() + timeout
        while True:
            if process is not None and process.poll() is not None:
                raise AssertionError(
                    "live-discovery pipeline exited before its main replication slot "
                    f"became active (returncode={process.returncode})"
                )
            if self.pg_query(
                "SELECT 1 FROM pg_replication_slots "
                "WHERE slot_name = %s AND active",
                (self.slot,),
            ):
                return
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"replication slot {self.slot!r} did not become active within "
                    f"{timeout:.1f}s"
                )
            time.sleep(poll_seconds)

    # -- pipeline ----------------------------------------------------------- #
    def run(self, *, extra_env: dict[str, str] | None = None, **kwargs) -> dict:
        kwargs.setdefault("max_seconds", 120)
        kwargs.setdefault("idle_seconds", SANDBOX_IDLE_SECONDS)
        return _invoke_pipeline({**self.env, **(extra_env or {})}, **kwargs)

    def spawn(
        self,
        *,
        max_seconds: float = 300,
        idle_seconds: float = 10,
        destination: str = "duckdb",
        extra_env: dict[str, str] | None = None,
        capture: bool = False,
        matrix_arm: bool = False,
    ) -> subprocess.Popen:
        """Start the pipeline as a killable child process (fault injection)."""
        sink = subprocess.PIPE if capture else subprocess.DEVNULL
        env = {**self.env, **(extra_env or {})}
        executable = (
            [sys.executable, str(MATRIX_CHILD)]
            if matrix_arm
            else [_executable("cdc-flight")]
        )
        return subprocess.Popen(
            [
                *executable,
                "--destination",
                destination,
                "--max-seconds",
                str(max_seconds),
                "--idle-seconds",
                str(idle_seconds),
            ],
            env=env,
            cwd=PROJECT_DIR,
            stdout=sink,
            stderr=sink,
            text=capture,
        )

    def last_summary(self) -> dict:
        """The JSON summary the CLI wrote for its most recent run, if any."""
        path = self.state_dir / "last_run.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def fired_fault(self) -> dict | None:
        """Which fault anchor actually fired in the last run, per the run itself.

        Rubric 1.7's claim is about a *named* anchor producing a *named* outcome, and an
        exit code cannot carry that: `-9`, `137` and `1` are all "the process died" and
        one of them is what the harness does when it gives up. `faults.record_fired()`
        writes this before it exits, fsynced, so even a hard `os._exit` leaves it
        (Codex M2 / Opus MAJOR-5).
        """
        from cdc_flight import faults

        return faults.read_fired_record(self.state_dir)

    def clear_fired_fault(self) -> None:
        (self.state_dir / "fault_fired.json").unlink(missing_ok=True)

    def kill_walsender(self) -> int:
        return kill_walsender(self.source, self.slot)

    # -- destination -------------------------------------------------------- #
    def duck_query(self, stmt: str, params: list | None = None) -> list[tuple]:
        con = duckdb.connect(str(self.duckdb_path), read_only=True)
        try:
            return con.execute(stmt, params or []).fetchall()
        finally:
            con.close()

    def duck_write(self, stmt: str, params: list | None = None) -> None:
        """Mutate the destination directly, between runs.

        Used to put the destination into a state a *cause* would have produced - a table
        marked `awaiting_snapshot`, a rewritten `slot_state` row - without also having to
        reproduce the cause. Only ever called with no pipeline running.
        """
        con = duckdb.connect(str(self.duckdb_path))
        try:
            con.execute(stmt, params or [])
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

    Shared by `tests/rubric/1.1_exactly_once_pk/` and `tests/rubric/1.2_exactly_once_nopk/` so
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
        # Leave this worker's source in its canonical state before another module.
        with contextlib.suppress(Exception):  # teardown must not mask a failure
            _reset_test_database(postgres_cluster)


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
