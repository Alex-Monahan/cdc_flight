"""Physical Postgres ownership and lifecycle for the pytest suite."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TextIO

import psycopg
import pytest
from psycopg import sql

from cdc_flight.config import SourceConfig

PROJECT_DIR = Path(__file__).resolve().parents[1]
PG_SH = PROJECT_DIR / "scripts" / "pg.sh"


def _safe_identifier(value: str, fallback: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_") or fallback


def _database_prefix(environ: Mapping[str, str], name: str, default: str) -> str:
    prefix = re.sub(r"[^a-z0-9_]+", "_", environ.get(name, default).lower())
    return prefix if prefix.endswith("_") else f"{prefix}_"


@dataclass(frozen=True)
class PostgresTestInstance:
    """One immutable logical namespace bound to one physical test cluster."""

    project_dir: Path
    pg_sh: Path
    host: str
    port: int
    database: str
    data_dir: Path
    socket_dir: Path
    log_path: Path
    instance_id: str
    slot_prefix: str
    template_database_prefix: str
    worker_database_prefix: str
    lock_dir: Path
    physical_key: str
    run_lock_path: Path
    setup_lock_path: Path
    sentinel: Path
    _verified_identity: tuple[str, int, Path] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        project_dir: Path = PROJECT_DIR,
    ) -> PostgresTestInstance:
        env = os.environ if environ is None else environ
        port = int(env.get("CDC_TEST_PGPORT", "15432"))
        default_data = project_dir / (
            ".pgdata" if port == 15432 else f".pgdata_{port}"
        )
        data_dir = Path(env.get("CDC_TEST_PGDATA", str(default_data))).resolve()
        instance_id = _safe_identifier(
            env.get("CDC_TEST_INSTANCE_ID", f"pg{port}"), f"pg{port}"
        )
        slot_prefix = _safe_identifier(
            env.get("CDC_TEST_SLOT_PREFIX", f"test_slot_{instance_id}_"),
            f"test_slot_{instance_id}",
        )
        if not slot_prefix.endswith("_"):
            slot_prefix += "_"
        template_prefix = _database_prefix(
            env,
            "CDC_TEST_TEMPLATE_DATABASE_PREFIX",
            f"cdc_flight_test_template_{instance_id}_",
        )
        worker_prefix = _database_prefix(
            env,
            "CDC_TEST_WORKER_DATABASE_PREFIX",
            f"cdc_flight_test_{instance_id}_",
        )
        host = env.get("PGHOST", "127.0.0.1")
        physical_identity = f"{host}\0{port}\0{data_dir}"
        physical_key = hashlib.sha256(physical_identity.encode()).hexdigest()[:20]
        lock_dir = Path(
            env.get("CDC_TEST_LOCK_DIR", str(project_dir / ".pytest-instance-locks"))
        ).resolve()
        return cls(
            project_dir=project_dir,
            pg_sh=project_dir / "scripts" / "pg.sh",
            host=host,
            port=port,
            database=env.get("CDC_TEST_PGDATABASE", "cdc_source"),
            data_dir=data_dir,
            socket_dir=Path(env.get("CDC_TEST_PGSOCKET", str(data_dir))).resolve(),
            log_path=Path(
                env.get("CDC_TEST_PGLOG", str(data_dir / "server.log"))
            ).resolve(),
            instance_id=instance_id,
            slot_prefix=slot_prefix,
            template_database_prefix=template_prefix,
            worker_database_prefix=worker_prefix,
            lock_dir=lock_dir,
            physical_key=physical_key,
            run_lock_path=lock_dir / f"postgres-{physical_key}.lock",
            setup_lock_path=lock_dir / f"postgres-{physical_key}-setup.lock",
            sentinel=data_dir / ".cdc_flight_disposable_test_cluster",
        )

    @property
    def physical_identity(self) -> tuple[str, int, Path]:
        return (self.host, self.port, self.data_dir)

    def pg(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        env = {
            **os.environ,
            "CDC_TEST_PGPORT": str(self.port),
            "CDC_TEST_PGDATA": str(self.data_dir),
            "CDC_TEST_PGSOCKET": str(self.socket_dir),
            "CDC_TEST_PGLOG": str(self.log_path),
            "PGPORT": str(self.port),
            "CDC_TEST_PGDATABASE": self.database,
            "PGDATABASE": self.database,
        }
        return subprocess.run(
            [str(self.pg_sh), *args],
            capture_output=True,
            text=True,
            check=check,
            timeout=180,
            env=env,
        )

    def acquire_run_lock(
        self,
        *,
        run_uid: str,
        wait_seconds: float = 1800,
        poll_seconds: float = 1,
    ) -> TextIO:
        """Acquire kernel-enforced ownership of this physical cluster."""
        self.run_lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.run_lock_path.open("a+")
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
                        "\nwaiting for test-run owner of physical Postgres "
                        f"{self.physical_key} at {self.run_lock_path}: {owner}"
                    )
                    announced = True
                if time.monotonic() >= deadline:
                    handle.close()
                    raise TimeoutError(
                        f"timed out waiting for test-run lock {self.run_lock_path}: {owner}"
                    ) from None
                time.sleep(poll_seconds)

        metadata = {
            "data_dir": str(self.data_dir),
            "hostname": socket.gethostname(),
            "instance_id": self.instance_id,
            "physical_key": self.physical_key,
            "pid": os.getpid(),
            "port": self.port,
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

    def isolated_source(self, dbname: str) -> SourceConfig:
        source = SourceConfig(dbname=dbname)
        if source.host != "127.0.0.1":
            pytest.fail(
                f"test isolation refused non-local Postgres host {source.host!r}; "
                "expected '127.0.0.1'"
            )
        if source.port != self.port:
            pytest.fail(
                f"test isolation refused to use Postgres port {source.port}; "
                f"expected {self.port}"
            )
        return source

    def require_disposable_cluster(self, source: SourceConfig) -> None:
        """Prove destructive operations target the bound provisioned cluster."""
        expected = (source.host, source.port, self.data_dir)
        if expected == self._verified_identity:
            return
        if source.host != "127.0.0.1":
            raise RuntimeError(f"refusing destructive operation on host {source.host!r}")
        if not self.sentinel.is_file():
            raise RuntimeError(
                f"refusing destructive operation: missing {self.sentinel}"
            )
        admin = replace(source, dbname="postgres")
        with psycopg.connect(admin.dsn, autocommit=True, connect_timeout=10) as conn:
            actual_data = Path(conn.execute("SHOW data_directory").fetchone()[0]).resolve()
            actual_port = int(conn.execute("SHOW port").fetchone()[0])
        if actual_data != self.data_dir or actual_port != self.port:
            raise RuntimeError(
                "refusing destructive operation on unexpected cluster: "
                f"data_directory={actual_data}, port={actual_port}; "
                f"expected {self.data_dir}, port={self.port}"
            )
        object.__setattr__(self, "_verified_identity", expected)

    def worker_database_name(self) -> str:
        worker = _safe_identifier(
            os.environ.get("PYTEST_XDIST_WORKER", "main"), "main"
        )
        return f"{self.worker_database_prefix}{worker}"[:63]

    def template_database_name(self) -> str:
        worker = _safe_identifier(
            os.environ.get("PYTEST_XDIST_WORKER", "main"), "main"
        )
        return f"{self.template_database_prefix}{worker}"[:63]

    def owns_database(self, dbname: str) -> bool:
        return dbname.startswith(
            (self.worker_database_prefix, self.template_database_prefix)
        )

    def drop_database(self, admin: SourceConfig, dbname: str) -> None:
        self.require_disposable_cluster(admin)
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
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
            )

    def create_database(self, admin: SourceConfig, dbname: str, template: str) -> None:
        with psycopg.connect(admin.dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                    sql.Identifier(dbname), sql.Identifier(template)
                )
            )

    def reset_test_database(self, source: SourceConfig) -> None:
        admin = replace(source, dbname="postgres")
        self.drop_database(admin, source.dbname)
        self.create_database(admin, source.dbname, self.template_database_name())

    def sweep_stale_instance_artifacts(
        self, source: SourceConfig
    ) -> dict[str, list[str]]:
        self.require_disposable_cluster(source)
        admin = replace(source, dbname="postgres")
        with psycopg.connect(admin.dsn, autocommit=True) as conn:
            databases = sorted(
                row[0]
                for row in conn.execute(
                    "SELECT datname FROM pg_database "
                    "WHERE starts_with(datname, %s) OR starts_with(datname, %s)",
                    (self.worker_database_prefix, self.template_database_prefix),
                ).fetchall()
                if self.owns_database(row[0])
            )
        for dbname in databases:
            self.drop_database(admin, dbname)

        with psycopg.connect(admin.dsn, autocommit=True) as conn:
            slots = conn.execute(
                "SELECT slot_name, active_pid FROM pg_replication_slots "
                "WHERE starts_with(slot_name, %s)",
                (self.slot_prefix,),
            ).fetchall()
            for _slot_name, active_pid in slots:
                if active_pid is not None:
                    conn.execute("SELECT pg_terminate_backend(%s)", (active_pid,))
            for slot_name, _active_pid in slots:
                conn.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))

        swept = {"databases": databases, "slots": sorted(row[0] for row in slots)}
        if databases or slots:
            print(f"\nswept stale artifacts for {self.instance_id}: {swept}")
        return swept

    @staticmethod
    def required_replication_capacity(worker_count: int) -> int:
        return worker_count * 2 + 4

    def assert_replication_budget(
        self, source: SourceConfig, worker_count: int
    ) -> None:
        required = self.required_replication_capacity(worker_count)
        admin = replace(source, dbname="postgres")
        with psycopg.connect(admin.dsn, autocommit=True) as conn:
            slots = int(conn.execute("SHOW max_replication_slots").fetchone()[0])
            senders = int(conn.execute("SHOW max_wal_senders").fetchone()[0])
        if slots < required or senders < required:
            raise RuntimeError(
                "insufficient replication budget: "
                f"workers={worker_count}, required={required}, "
                f"max_replication_slots={slots}, max_wal_senders={senders}"
            )

    def source_environment(self, source: SourceConfig) -> dict[str, str]:
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

    def sweep_stale_test_slots(self, source: SourceConfig) -> None:
        try:
            with psycopg.connect(
                source.dsn, autocommit=True, connect_timeout=10
            ) as conn:
                stale = [
                    row[0]
                    for row in conn.execute(
                        "SELECT slot_name FROM pg_replication_slots "
                        "WHERE NOT active AND database = %s "
                        "AND starts_with(slot_name, %s)",
                        (source.dbname, self.slot_prefix),
                    ).fetchall()
                ]
                for name in stale:
                    with contextlib.suppress(Exception):
                        conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                if stale:
                    print(
                        f"\nswept {len(stale)} stale replication slot(s): {sorted(stale)}"
                    )
        except Exception as exc:  # pragma: no cover - hygiene must never fail a session
            print(f"\ncould not sweep stale replication slots: {exc}")

    @staticmethod
    def drop_slot(source: SourceConfig, slot: str) -> None:
        try:
            with psycopg.connect(source.dsn, autocommit=True) as conn:
                conn.execute(
                    "SELECT pg_drop_replication_slot(slot_name) "
                    "FROM pg_replication_slots WHERE slot_name = %s",
                    (slot,),
                )
        except Exception:
            pass


INSTANCE = PostgresTestInstance.from_environ()

# Pin the physical and logical layout before any fixture constructs SourceConfig.
os.environ.setdefault("CDC_TEST_PGPORT", str(INSTANCE.port))
os.environ.setdefault("CDC_TEST_PGDATA", str(INSTANCE.data_dir))
os.environ.setdefault("CDC_TEST_PGSOCKET", str(INSTANCE.socket_dir))
os.environ.setdefault("CDC_TEST_PGLOG", str(INSTANCE.log_path))
os.environ.setdefault("CDC_TEST_PGDATABASE", INSTANCE.database)
os.environ.setdefault("CDC_TEST_INSTANCE_ID", INSTANCE.instance_id)
os.environ.setdefault("CDC_TEST_SLOT_PREFIX", INSTANCE.slot_prefix)

_RUN_LOCK_HANDLE: TextIO | None = None


def _enforce_no_worker_restarts(config) -> None:
    workers = getattr(config.option, "numprocesses", None)
    if not workers:
        return
    restarts = getattr(config.option, "maxworkerrestart", None)
    if restarts not in (None, 0, "0"):
        raise pytest.UsageError(
            "the disposable test lane requires --max-worker-restart=0; "
            "replacement workers cannot inherit crashed-worker cleanup"
        )
    config.option.maxworkerrestart = 0


def _worker_count(config) -> int:
    workers = getattr(config.option, "numprocesses", None)
    if isinstance(workers, int):
        return max(workers, 1)
    if isinstance(workers, str) and workers.isdigit():
        return max(int(workers), 1)
    if workers in ("auto", "logical"):
        return os.cpu_count() or 1
    return 1


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config) -> None:
    if hasattr(config, "workerinput"):
        return
    _enforce_no_worker_restarts(config)
    global _RUN_LOCK_HANDLE
    if _RUN_LOCK_HANDLE is not None:
        return
    run_uid = config.getoption("testrunuid", default=None) or uuid.uuid4().hex
    try:
        _RUN_LOCK_HANDLE = INSTANCE.acquire_run_lock(run_uid=run_uid)
    except TimeoutError as exc:
        raise pytest.UsageError(str(exc)) from exc


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session) -> None:
    config = session.config
    if hasattr(config, "workerinput"):
        return
    try:
        INSTANCE.pg("start")
        source = INSTANCE.isolated_source(INSTANCE.database)
        INSTANCE.require_disposable_cluster(source)
        INSTANCE.sweep_stale_instance_artifacts(source)
        INSTANCE.assert_replication_budget(source, _worker_count(config))
        INSTANCE.pg("seed")
    except Exception as exc:
        raise pytest.UsageError(f"test-cluster startup refused: {exc}") from exc


def pytest_unconfigure(config) -> None:
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
    INSTANCE.setup_lock_path.parent.mkdir(parents=True, exist_ok=True)
    INSTANCE.setup_lock_path.touch(exist_ok=True)
    yield INSTANCE.setup_lock_path


@contextlib.contextmanager
def _cluster_setup_lock(lock_path: Path) -> Iterator[None]:
    with lock_path.open("r+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


@pytest.fixture(scope="session")
def postgres_cluster(exclusive_source: Path) -> Iterator[SourceConfig]:
    if not INSTANCE.pg_sh.exists():
        pytest.skip("scripts/pg.sh missing")

    source = INSTANCE.isolated_source(INSTANCE.database)
    admin = replace(source, dbname="postgres")
    worker_source = replace(source, dbname=INSTANCE.worker_database_name())
    with _cluster_setup_lock(exclusive_source):
        template_database = INSTANCE.template_database_name()
        INSTANCE.drop_database(admin, template_database)
        INSTANCE.create_database(admin, template_database, source.dbname)
        INSTANCE.drop_database(admin, worker_source.dbname)
        INSTANCE.create_database(admin, worker_source.dbname, template_database)

    INSTANCE.sweep_stale_test_slots(worker_source)
    try:
        yield worker_source
    finally:
        with contextlib.suppress(Exception):
            INSTANCE.drop_database(admin, worker_source.dbname)
