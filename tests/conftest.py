"""Fixtures for the cdc_flight test suite.

Everything runs natively: a project-local Postgres cluster driven by
`scripts/pg.sh`, the Debezium embedded engine inside a JVM, and DuckDB on disk.
No Docker, no Kafka, no testcontainers.  The Postgres instance namespace is
derived from ``CDC_TEST_PGPORT`` (or ``CDC_TEST_INSTANCE_ID``) so independent
sessions do not share database, lock, or slot names.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any, TextIO

# Must precede DuckDB/PyArrow imports. PyArrow 25.0.0's default mimalloc backend can
# SIGSEGV on the JPype JVM callback thread used by the live recovery path (ADR A14/A66).
os.environ.setdefault("ARROW_DEFAULT_MEMORY_POOL", "system")

import duckdb
import psycopg
import pytest
from psycopg import sql

PROJECT_DIR = Path(__file__).resolve().parents[1]
PG_SH = PROJECT_DIR / "scripts" / "pg.sh"
VENV_BIN = PROJECT_DIR / ".venv" / "bin"
TEST_PGPORT = int(os.environ.get("CDC_TEST_PGPORT", "15432"))
TEST_PGDATABASE = os.environ.get("CDC_TEST_PGDATABASE", "cdc_source")
TEST_PGDATA = Path(
    os.environ.get(
        "CDC_TEST_PGDATA",
        str(
            PROJECT_DIR
            / (".pgdata" if TEST_PGPORT == 15432 else f".pgdata_{TEST_PGPORT}")
        ),
    )
)
TEST_PGSOCKET = Path(os.environ.get("CDC_TEST_PGSOCKET", str(TEST_PGDATA)))
TEST_PGLOG = Path(
    os.environ.get("CDC_TEST_PGLOG", str(TEST_PGDATA / "server.log"))
)
TEST_CLUSTER_SENTINEL = TEST_PGDATA / ".cdc_flight_disposable_test_cluster"


def _safe_instance_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_") or f"pg{TEST_PGPORT}"


TEST_INSTANCE_ID = _safe_instance_id(
    os.environ.get("CDC_TEST_INSTANCE_ID", f"pg{TEST_PGPORT}")
)
TEST_SLOT_PREFIX = _safe_instance_id(
    os.environ.get("CDC_TEST_SLOT_PREFIX", f"test_slot_{TEST_INSTANCE_ID}_")
)
# Preserve a trailing separator when a caller supplies a prefix without one.
if not TEST_SLOT_PREFIX.endswith("_"):
    TEST_SLOT_PREFIX += "_"


def _database_prefix(name: str, default: str) -> str:
    prefix = re.sub(r"[^a-z0-9_]+", "_", os.environ.get(name, default).lower())
    return prefix if prefix.endswith("_") else f"{prefix}_"


TEMPLATE_DATABASE_PREFIX = _database_prefix(
    "CDC_TEST_TEMPLATE_DATABASE_PREFIX",
    f"cdc_flight_test_template_{TEST_INSTANCE_ID}_",
)
WORKER_DATABASE_PREFIX = _database_prefix(
    "CDC_TEST_WORKER_DATABASE_PREFIX",
    f"cdc_flight_test_{TEST_INSTANCE_ID}_",
)
TEST_LOCK_PATH = Path(
    os.environ.get(
        "CDC_TEST_LOCK_PATH",
        str(PROJECT_DIR / f".pytest-source-{TEST_INSTANCE_ID}.lock"),
    )
)
TEST_SETUP_LOCK_PATH = Path(
    os.environ.get(
        "CDC_TEST_SETUP_LOCK_PATH",
        str(PROJECT_DIR / f".pytest-source-{TEST_INSTANCE_ID}-setup.lock"),
    )
)

# Pin defaults used by SourceConfig in this pytest process before any fixture
# constructs one.  In particular, an unrelated PGPORT in the parent shell must
# not redirect a test run away from its selected CDC_TEST_PGPORT.
os.environ.setdefault("CDC_TEST_PGPORT", str(TEST_PGPORT))
os.environ.setdefault("CDC_TEST_PGDATA", str(TEST_PGDATA))
os.environ.setdefault("CDC_TEST_PGSOCKET", str(TEST_PGSOCKET))
os.environ.setdefault("CDC_TEST_PGLOG", str(TEST_PGLOG))
os.environ.setdefault("CDC_TEST_PGDATABASE", TEST_PGDATABASE)
os.environ.setdefault("CDC_TEST_INSTANCE_ID", TEST_INSTANCE_ID)
os.environ.setdefault("CDC_TEST_SLOT_PREFIX", TEST_SLOT_PREFIX)
SANDBOX_IDLE_SECONDS = 6

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
    env = {
        **os.environ,
        "CDC_TEST_PGPORT": str(TEST_PGPORT),
        "CDC_TEST_PGDATA": str(TEST_PGDATA),
        "CDC_TEST_PGSOCKET": str(TEST_PGSOCKET),
        "CDC_TEST_PGLOG": str(TEST_PGLOG),
        "PGPORT": str(TEST_PGPORT),
        "CDC_TEST_PGDATABASE": TEST_PGDATABASE,
        "PGDATABASE": TEST_PGDATABASE,
    }
    return subprocess.run(
        [str(PG_SH), *args],
        capture_output=True,
        text=True,
        check=check,
        timeout=180,
        env=env,
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
    faults.reset_arrivals()
    yield
    faults.refresh()
    faults.reset_arrivals()


# --------------------------------------------------------------------------- #
# session-scoped environment
# --------------------------------------------------------------------------- #
_RUN_LOCK_HANDLE: TextIO | None = None


def _acquire_test_run_lock(
    lock_path: Path,
    *,
    run_uid: str,
    wait_seconds: float = 1800,
    poll_seconds: float = 1,
) -> TextIO:
    """Acquire the instance's test-run ownership lock.

    The kernel lock is the authority. File contents are diagnostic metadata only:
    after a crash the OS releases the lock, and the next owner may replace stale
    metadata immediately. Conversely, metadata age never permits takeover while
    another process still holds the lock.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    deadline = time.monotonic() + wait_seconds
    announced = False
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            handle.seek(0)
            owner = handle.read().strip() or "unknown owner"
            if not announced:
                print(
                    f"\nwaiting for test-run owner of {TEST_INSTANCE_ID} "
                    f"at {lock_path}: {owner}"
                )
                announced = True
            if time.monotonic() >= deadline:
                handle.close()
                raise TimeoutError(
                    f"timed out waiting for test-run lock {lock_path}: {owner}"
                ) from None
            time.sleep(poll_seconds)

    metadata = {
        "hostname": socket.gethostname(),
        "instance_id": TEST_INSTANCE_ID,
        "pid": os.getpid(),
        "run_uid": run_uid,
        "started_at": time.time(),
    }
    handle.seek(0)
    handle.truncate()
    json.dump(metadata, handle, sort_keys=True)
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
    return handle


def pytest_configure(config) -> None:
    """Let the controller own the selected Postgres instance for the whole run."""
    if hasattr(config, "workerinput"):
        return
    global _RUN_LOCK_HANDLE
    if _RUN_LOCK_HANDLE is not None:
        return
    run_uid = config.getoption("testrunuid", default=None) or uuid.uuid4().hex
    try:
        _RUN_LOCK_HANDLE = _acquire_test_run_lock(TEST_LOCK_PATH, run_uid=run_uid)
    except TimeoutError as exc:
        raise pytest.UsageError(str(exc)) from exc


def pytest_unconfigure(config) -> None:
    """Release ownership after every worker and finalizer has stopped."""
    if hasattr(config, "workerinput"):
        return
    global _RUN_LOCK_HANDLE
    if _RUN_LOCK_HANDLE is None:
        return
    fcntl.flock(_RUN_LOCK_HANDLE, fcntl.LOCK_UN)
    _RUN_LOCK_HANDLE.close()
    _RUN_LOCK_HANDLE = None


@pytest.fixture(scope="session")
def exclusive_source() -> Iterator[Path]:
    """Provide the worker-setup lock nested inside controller run ownership."""
    lock_path = TEST_SETUP_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch(exist_ok=True)
    yield lock_path


@contextlib.contextmanager
def _cluster_setup_lock(lock_path: Path) -> Iterator[None]:
    """Serialize only cluster/template setup across xdist workers."""
    with lock_path.open("r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _worker_database_name() -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    worker = re.sub(r"[^a-z0-9_]", "_", worker.lower())
    return f"{WORKER_DATABASE_PREFIX}{worker}"[:63]


def _template_database_name() -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "main")
    worker = re.sub(r"[^a-z0-9_]", "_", worker.lower())
    return f"{TEMPLATE_DATABASE_PREFIX}{worker}"[:63]


def _isolated_source(dbname: str) -> SourceConfig:
    source = SourceConfig(dbname=dbname)
    if source.host != "127.0.0.1":
        pytest.fail(
            f"test isolation refused non-local Postgres host {source.host!r}; "
            "expected '127.0.0.1'"
        )
    if source.port != TEST_PGPORT:
        pytest.fail(
            f"test isolation refused to use Postgres port {source.port}; "
            f"expected {TEST_PGPORT}"
        )
    return source


_VERIFIED_CLUSTER_IDENTITY: tuple[str, int, Path] | None = None


def _require_disposable_cluster(source: SourceConfig) -> None:
    """Prove destructive test operations target this provisioned test cluster."""
    global _VERIFIED_CLUSTER_IDENTITY
    expected = (source.host, source.port, TEST_PGDATA.resolve())
    if expected == _VERIFIED_CLUSTER_IDENTITY:
        return
    if source.host != "127.0.0.1":
        raise RuntimeError(f"refusing destructive operation on host {source.host!r}")
    if not TEST_CLUSTER_SENTINEL.is_file():
        raise RuntimeError(
            f"refusing destructive operation: missing {TEST_CLUSTER_SENTINEL}"
        )
    admin = replace(source, dbname="postgres")
    with psycopg.connect(admin.dsn, autocommit=True, connect_timeout=10) as conn:
        data_directory = Path(conn.execute("SHOW data_directory").fetchone()[0]).resolve()
        server_port = int(conn.execute("SHOW port").fetchone()[0])
    if data_directory != expected[2] or server_port != TEST_PGPORT:
        raise RuntimeError(
            "refusing destructive operation on unexpected cluster: "
            f"data_directory={data_directory}, port={server_port}; "
            f"expected {expected[2]}, port={TEST_PGPORT}"
        )
    _VERIFIED_CLUSTER_IDENTITY = expected


def _drop_database(admin: SourceConfig, dbname: str) -> None:
    """Terminate this disposable database's backends, slots, and database."""
    _require_disposable_cluster(admin)
    with psycopg.connect(admin.dsn, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        slots = conn.execute(
            "SELECT r.slot_name FROM pg_replication_slots AS r "
            "WHERE r.database = %s",
            (dbname,),
        ).fetchall()
        for (slot,) in slots:
            with contextlib.suppress(Exception):
                conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname)))


def _create_database(admin: SourceConfig, dbname: str, template: str) -> None:
    with psycopg.connect(admin.dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(dbname), sql.Identifier(template)
            )
        )


def _reset_test_database(source: SourceConfig) -> None:
    admin = replace(source, dbname="postgres")
    _drop_database(admin, source.dbname)
    _create_database(admin, source.dbname, _template_database_name())


def _source_environment(source: SourceConfig) -> dict[str, str]:
    """Route every child process to this worker's isolated database and port."""
    return {
        **os.environ,
        "PGHOST": source.host,
        "PGPORT": str(source.port),
        "CDC_TEST_PGPORT": str(source.port),
        "PGUSER": source.user,
        "PGPASSWORD": source.password,
        "PGDATABASE": source.dbname,
        "CDC_TEST_PGDATABASE": source.dbname,
    }


@pytest.fixture(scope="session")
def postgres_cluster(exclusive_source: Path) -> Iterator[SourceConfig]:
    """Start one cluster, then clone a private database for this pytest worker."""
    if not PG_SH.exists():
        pytest.skip("scripts/pg.sh missing")

    source = _isolated_source(TEST_PGDATABASE)
    admin = replace(source, dbname="postgres")
    worker_source = replace(source, dbname=_worker_database_name())
    with _cluster_setup_lock(exclusive_source):
        _pg("start")
        _pg("seed")
        _sweep_stale_test_slots(source)
        template_database = _template_database_name()
        _drop_database(admin, template_database)
        _create_database(admin, template_database, source.dbname)
        _drop_database(admin, worker_source.dbname)
        _create_database(admin, worker_source.dbname, template_database)

    _sweep_stale_test_slots(worker_source)
    try:
        yield worker_source
    finally:
        with contextlib.suppress(Exception):
            _drop_database(admin, worker_source.dbname)


def _sweep_stale_test_slots(source: SourceConfig) -> None:
    """Drop replication slots left behind by earlier sessions (Opus MAJOR-2).

    Every sandbox slot is named `t_<scenario>_<pid>` and dropped on cleanup, but a hard
    crash - which several fault scenarios cause ON PURPOSE - leaves one behind, and so
    does a `_rs` throwaway from an interrupted re-snapshot. A logical slot holds WAL for
    ever and counts against `max_replication_slots`, so the leaks accumulate until the
    suite fails with "all replication slots are in use" - which is exactly how this run
    failed once while the fix was being written, and how two independent review sessions
    degraded the shared cluster in a single day.

    Safe to do unconditionally at worker start: the query is restricted to this
    worker's database, and an ACTIVE slot is never touched.
    """
    try:
        with psycopg.connect(source.dsn, autocommit=True, connect_timeout=10) as conn:
            stale = [
                row[0]
                for row in conn.execute(
                    "SELECT slot_name FROM pg_replication_slots "
                    "WHERE NOT active AND database = %s "
                    "AND (slot_name LIKE 't\\_' || chr(37) "
                    "OR slot_name LIKE chr(37) || '\\_rs')",
                    (source.dbname,),
                ).fetchall()
            ]
            for name in stale:
                with contextlib.suppress(Exception):
                    conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            if stale:
                print(f"\nswept {len(stale)} stale replication slot(s): {sorted(stale)}")
    except Exception as exc:  # pragma: no cover - hygiene must never fail a session
        print(f"\ncould not sweep stale replication slots: {exc}")


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
    suffix = f"{os.getpid()}_{abs(hash(tmp_path)) % 100000}"
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
        self.slot = re.sub(
            r"[^a-z0-9_]", "_", f"{TEST_SLOT_PREFIX}t_{name}_{os.getpid()}".lower()
        )[:60]
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
