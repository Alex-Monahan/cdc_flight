"""Native primary/physical-standby topology for rubric 7.2.

This helper owns only the second disposable PostgreSQL cluster used by the replica
lane.  Its data directory is derived from the repository and its port in the same
shape as ``scripts/pg.sh``; it never passes a caller-selected PGDATA to that script
and never changes the shared logical-slot startup gate.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import psycopg

from cdc_flight.config import SourceConfig
from cdc_flight.standby import StandbyObservation, assert_supported, inspect

PROJECT_DIR = Path(__file__).resolve().parents[2]
STANDBY_PORT_OFFSET = 3
FORBIDDEN_REVIEW_PORTS = frozenset({15433, 15434})
TOPOLOGY_MARKER = ".cdc_flight_p72_standby"


def _pg_bin(name: str) -> str:
    configured = os.environ.get("PGBIN")
    if configured:
        candidate = Path(configured) / name
        if candidate.exists():
            return str(candidate)
    version = os.environ.get("PG_VERSION", "18")
    homebrew = Path(f"/opt/homebrew/opt/postgresql@{version}/bin/{name}")
    if homebrew.exists():
        return str(homebrew)
    found = shutil.which(name)
    if found:
        return found
    raise RuntimeError(f"PostgreSQL executable {name!r} is not installed")


def _derived_standby_port(primary_port: int) -> int:
    port = int(primary_port) + STANDBY_PORT_OFFSET
    if port == primary_port or port in FORBIDDEN_REVIEW_PORTS:
        raise RuntimeError(
            f"the derived standby port {port} is not available for the 7.2 topology"
        )
    return port


def _lsn_value(conn, expression: str) -> int | None:
    row = conn.execute(f"SELECT {expression}").fetchone()
    return None if row is None or row[0] is None else int(row[0])


@dataclass
class StandbyTopology:
    """A real primary plus physical receiver plus local logical-slot owner."""

    primary: SourceConfig
    runtime_dir: Path
    port: int
    data_dir: Path
    socket_dir: Path
    log_path: Path
    physical_slot: str
    local_slot: str
    _provisioned: bool = False

    @classmethod
    def from_environment(cls, runtime_dir: Path) -> StandbyTopology:
        primary_port = int(os.environ.get("CDC_TEST_PGPORT", os.environ.get("PGPORT", "15432")))
        port = _derived_standby_port(primary_port)
        data_dir = (PROJECT_DIR / f".pgdata_{port}").resolve()
        expected = (PROJECT_DIR / f".pgdata_{port}").resolve()
        if data_dir != expected:
            raise RuntimeError("standby PGDATA did not resolve to its derived repository path")
        suffix = re.sub(r"[^a-z0-9_]", "_", f"{port}_{os.getpid()}".lower())
        primary = SourceConfig(
            host=os.environ.get("PGHOST", "127.0.0.1"),
            port=primary_port,
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "postgres"),
            dbname=os.environ.get("CDC_TEST_PGDATABASE", "cdc_source"),
        )
        return cls(
            primary=primary,
            runtime_dir=runtime_dir,
            port=port,
            data_dir=data_dir,
            socket_dir=data_dir,
            log_path=data_dir / "server.log",
            physical_slot=f"cdc_p72_physical_{suffix}"[:63],
            local_slot=f"cdc_p72_local_{suffix}"[:63],
        )

    @property
    def standby_dsn(self) -> str:
        return (
            f"postgresql://{self.primary.user}:{self.primary.password}@"
            f"{self.primary.host}:{self.port}/{self.primary.dbname}"
        )

    @property
    def primary_dsn(self) -> str:
        return self.primary.dsn

    @property
    def marker_path(self) -> Path:
        return self.data_dir / TOPOLOGY_MARKER

    @property
    def pg_ctl(self) -> str:
        return _pg_bin("pg_ctl")

    def _command_environment(self) -> dict[str, str]:
        return {
            **os.environ,
            "PGHOST": self.primary.host,
            "PGPORT": str(self.port),
            "PGUSER": self.primary.user,
            "PGPASSWORD": self.primary.password,
            "PGDATABASE": self.primary.dbname,
        }

    def _admin(self, dsn: str):
        return psycopg.connect(
            dsn,
            autocommit=True,
            connect_timeout=10,
            options="-c statement_timeout=10000",
        )

    def _standby_command(self, *args: str, timeout: float = 180.0) -> subprocess.CompletedProcess:
        return subprocess.run(
            [self.pg_ctl, "-D", str(self.data_dir), *args],
            env=self._command_environment(),
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        )

    def _assert_target_is_ours(self) -> None:
        """Validate the only target before a recoverable explicit cleanup."""
        if self.data_dir != (PROJECT_DIR / f".pgdata_{self.port}").resolve():
            raise RuntimeError(f"refusing non-derived standby data directory {self.data_dir}")
        if not self.data_dir.exists():
            return
        if not self.data_dir.is_dir():
            raise RuntimeError(f"refusing non-directory standby target {self.data_dir}")
        if self.marker_path.is_file():
            return
        # The first native prototype predates this helper. It is accepted only when
        # all of the physical-standby facts identify it as a disposable base backup;
        # an unmarked ordinary directory is never removed.
        if (
            (self.data_dir / "PG_VERSION").is_file()
            and (self.data_dir / "standby.signal").is_file()
            and (self.data_dir / ".cdc_flight_disposable_test_cluster").is_file()
            and (self.data_dir / "postgresql.auto.conf").is_file()
        ):
            return
        raise RuntimeError(
            f"refusing to replace unmarked standby directory {self.data_dir}; "
            "the 7.2 helper owns only its sentinel-marked derived target"
        )

    def _stop_existing(self) -> None:
        if not (self.data_dir / "postmaster.pid").is_file():
            return
        subprocess.run(
            [self.pg_ctl, "-D", str(self.data_dir), "-m", "fast", "-w", "-t", "60", "stop"],
            env=self._command_environment(),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if (self.data_dir / "postmaster.pid").is_file():
            raise RuntimeError(f"standby cluster did not stop at {self.data_dir}")

    def _remove_existing_target(self) -> None:
        self._assert_target_is_ours()
        if not self.data_dir.exists():
            return
        self._stop_existing()
        shutil.rmtree(self.data_dir)

    def _create_physical_slot(self) -> None:
        with self._admin(self.primary.dsn) as conn:
            row = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (self.physical_slot,),
            ).fetchone()
            if row is not None:
                if row[0]:
                    raise RuntimeError(f"physical slot {self.physical_slot!r} is active")
                conn.execute("SELECT pg_drop_replication_slot(%s)", (self.physical_slot,))
            conn.execute(
                "SELECT pg_create_physical_replication_slot(%s)",
                (self.physical_slot,),
            )

    def _basebackup(self) -> None:
        self.data_dir.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            [
                _pg_bin("pg_basebackup"),
                "-h", self.primary.host,
                "-p", str(self.primary.port),
                "-U", self.primary.user,
                "-D", str(self.data_dir),
                "-Fp",
                "-Xs",
                "-P",
                "-R",
                "-S", self.physical_slot,
            ],
            env={**self._command_environment(), "PGAPPNAME": "cdc_flight_p72_basebackup"},
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )
        if result.returncode != 0:  # pragma: no cover - check=True owns this branch
            raise RuntimeError("pg_basebackup failed")

        auto_conf = self.data_dir / "postgresql.auto.conf"
        with auto_conf.open("a", encoding="utf-8") as handle:
            handle.write(
                "\n# cdc_flight 7.2 derived physical standby overrides\n"
                f"listen_addresses = 'localhost'\nport = {self.port}\n"
                f"unix_socket_directories = '{self.socket_dir}'\n"
                "hot_standby = on\nhot_standby_feedback = on\n"
            )
        self.marker_path.write_text(
            json.dumps(
                {
                    "data_dir": str(self.data_dir),
                    "port": self.port,
                    "primary_port": self.primary.port,
                    "physical_slot": self.physical_slot,
                    "local_slot": self.local_slot,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    def _start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.socket_dir.mkdir(parents=True, exist_ok=True)
        self._standby_command(
            "-l", str(self.log_path), "-w", "-t", "120", "start", timeout=180
        )

    def _receiver_is_streaming(self) -> bool:
        try:
            with self._admin(self.standby_dsn) as conn:
                row = conn.execute(
                    "SELECT status, slot_name FROM pg_stat_wal_receiver LIMIT 1"
                ).fetchone()
                return bool(row and row[0] == "streaming" and row[1] == self.physical_slot)
        except psycopg.Error:
            return False

    def _wait_until(self, predicate, *, timeout: float, description: str) -> None:
        deadline = time.monotonic() + timeout
        while True:
            if predicate():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for {description}")
            time.sleep(min(0.25, remaining))

    def _wait_for_receiver_and_replay(self) -> None:
        self._wait_until(
            self._receiver_is_streaming,
            timeout=180,
            description=f"physical receiver on {self.physical_slot}",
        )

        def caught_up() -> bool:
            try:
                with self._admin(self.primary.dsn) as primary:
                    primary_lsn = _lsn_value(primary, "pg_current_wal_lsn() - '0/0'")
                with self._admin(self.standby_dsn) as standby:
                    received_lsn = _lsn_value(
                        standby, "pg_last_wal_receive_lsn() - '0/0'"
                    )
                return (
                    primary_lsn is not None
                    and received_lsn is not None
                    and received_lsn >= primary_lsn
                )
            except psycopg.Error:
                return False

        self._wait_until(caught_up, timeout=180, description="standby replay to primary WAL")

    def _local_slot_facts(self) -> tuple | None:
        try:
            with self._admin(self.standby_dsn) as conn:
                return conn.execute(
                    "SELECT slot_type, plugin, active, synced, failover, wal_status, "
                    "to_jsonb(s)->>'invalidation_reason' "
                    "FROM pg_replication_slots AS s WHERE slot_name = %s",
                    (self.local_slot,),
                ).fetchone()
        except psycopg.Error:
            return None

    def _create_local_slot(self) -> None:
        facts = self._local_slot_facts()
        if facts is not None:
            if facts[0] != "logical" or facts[1] != "pgoutput":
                raise RuntimeError(f"standby slot has unexpected shape: {facts!r}")
            return
        slot_literal = self.local_slot.replace("'", "''")
        subprocess.run(
            [
                _pg_bin("psql"),
                "-h", self.primary.host,
                "-p", str(self.port),
                "-U", self.primary.user,
                "-d", self.primary.dbname,
                "-v", "ON_ERROR_STOP=1",
                "-Atqc",
                f"SELECT pg_create_logical_replication_slot('{slot_literal}', 'pgoutput')",
            ],
            env=self._command_environment(),
            capture_output=True,
            text=True,
            check=True,
            timeout=480,
        )
        self._wait_until(
            lambda: self._local_slot_facts() is not None,
            timeout=30,
            description=f"local standby slot {self.local_slot}",
        )

    def provision(self) -> None:
        if self._provisioned:
            return
        self._remove_existing_target()
        self._create_physical_slot()
        try:
            self._basebackup()
            self._start()
            self._wait_for_receiver_and_replay()
            self._create_local_slot()
            self.assert_preconditions()
            self._provisioned = True
        except BaseException:
            with contextlib.suppress(Exception):
                self.cleanup()
            raise

    def assert_preconditions(self) -> StandbyObservation:
        observation = inspect(
            self.standby_dsn,
            self.local_slot,
            primary_dsn=self.primary.dsn,
            physical_slot_name=self.physical_slot,
            connect_timeout=10,
            statement_timeout_ms=10000,
        )
        assert_supported(observation)
        if observation.local_slot_name != self.local_slot:
            raise AssertionError(observation)
        return observation

    def local_slot_status(self) -> dict[str, Any] | None:
        facts = self._local_slot_facts()
        if facts is None:
            return None
        return {
            "slot_type": facts[0],
            "plugin": facts[1],
            "active": bool(facts[2]),
            "synced": bool(facts[3]),
            "failover": bool(facts[4]),
            "wal_status": facts[5],
            "invalidation_reason": facts[6],
        }

    def local_slot_confirmed_lsn(self) -> int | None:
        """Return the local standby slot's confirmed flush position as an integer."""
        rows = self.standby_query(
            "SELECT (confirmed_flush_lsn - '0/0')::bigint "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (self.local_slot,),
        )
        return None if not rows or rows[0][0] is None else int(rows[0][0])

    def wait_for_slot_active(
        self, *, process: subprocess.Popen | None = None, timeout: float = 60
    ) -> None:
        def active() -> bool:
            if process is not None and process.poll() is not None:
                raise AssertionError(
                    "standby pipeline exited before local slot activation "
                    f"(returncode={process.returncode})"
                )
            try:
                with self._admin(self.standby_dsn) as conn:
                    return bool(
                        conn.execute(
                            "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                            (self.local_slot,),
                        ).fetchone()[0]
                    )
            except (psycopg.Error, TypeError, AttributeError):
                return False

        self._wait_until(active, timeout=timeout, description="local logical slot active")

    def primary_sql(self, statement: str, params: tuple | None = None) -> None:
        with self._admin(self.primary.dsn) as conn:
            conn.execute(statement, params)

    def primary_sql_with_wal(
        self, statement: str, params: tuple | None = None
    ) -> int:
        """Commit one primary DML statement and return a primary WAL witness."""
        with self._admin(self.primary.dsn) as conn:
            conn.execute(statement, params)
            row = conn.execute(
                "SELECT (pg_current_wal_lsn() - '0/0')::bigint"
            ).fetchone()
        if row is None or row[0] is None:
            raise RuntimeError("primary DML did not expose a WAL position")
        return int(row[0])

    def standby_query(self, statement: str, params: tuple | None = None) -> list[tuple]:
        with self._admin(self.standby_dsn) as conn:
            return conn.execute(statement, params).fetchall()

    def primary_query(self, statement: str, params: tuple | None = None) -> list[tuple]:
        with self._admin(self.primary.dsn) as conn:
            return conn.execute(statement, params).fetchall()

    def stream_facts(self) -> dict[str, Any]:
        # Fact collection itself is a precondition fence.  The primary WAL can
        # advance between the two read-only sessions below (another test or a
        # bounded source marker may commit in that interval); report only after
        # the receiver has caught up to a primary WAL sample, never by relying on
        # a startup sleep or on an elapsed-time guess.
        self._wait_for_receiver_and_replay()
        observation = self.assert_preconditions()
        with self._admin(self.primary.dsn) as conn:
            primary_wal = _lsn_value(conn, "pg_current_wal_lsn() - '0/0'")
        with self._admin(self.standby_dsn) as conn:
            received_wal = _lsn_value(conn, "pg_last_wal_receive_lsn() - '0/0'")
        return {
            "primary_dsn": self.primary.dsn,
            "standby_dsn": self.standby_dsn,
            "primary_port": self.primary.port,
            "standby_port": self.port,
            "standby_data_dir": str(self.data_dir),
            "physical_slot": self.physical_slot,
            "local_slot": self.local_slot,
            "primary_wal": primary_wal,
            "standby_receive_wal": received_wal,
            "observation": observation.as_dict(),
        }

    def make_case(self, root: Path, *, name: str) -> StandbyCase:
        return StandbyCase(self, root, name)

    def drop_local_slot(self) -> None:
        with self._admin(self.standby_dsn) as conn:
            active = conn.execute(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                (self.local_slot,),
            ).fetchone()
            if active and active[0] is not None:
                conn.execute("SELECT pg_terminate_backend(%s)", (active[0],))
        self.wait_for_slot_inactive(timeout=30)
        with self._admin(self.standby_dsn) as conn:
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (self.local_slot,),
            )

    def wait_for_slot_inactive(self, *, timeout: float = 30) -> None:
        """Wait until the local slot no longer has a decoder backend."""

        def inactive() -> bool:
            status = self.local_slot_status()
            return status is None or not status["active"]

        self._wait_until(inactive, timeout=timeout, description="local slot inactivity")

    def repair_local_slot(self) -> None:
        """Create the local decoder slot after a recorded loss/invalidation."""
        if not self._provisioned:
            raise RuntimeError("cannot repair a standby topology before provisioning")
        self._wait_for_receiver_and_replay()
        self._create_local_slot()
        self.assert_preconditions()

    def cleanup(self) -> None:
        """Stop the explicit standby target, drop its primary slot, and prove cleanup."""
        self._stop_existing()
        with contextlib.suppress(Exception), self._admin(self.primary.dsn) as conn:
            row = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (self.physical_slot,),
            ).fetchone()
            if row is not None and not row[0]:
                conn.execute(
                    "SELECT pg_drop_replication_slot(%s)", (self.physical_slot,)
                )
        self._assert_target_is_ours()
        if self.marker_path.is_file():
            shutil.rmtree(self.data_dir)
        self._provisioned = False


@dataclass
class StandbyCase:
    """One destination/state namespace over an already-provisioned topology."""

    topology: StandbyTopology
    root: Path
    name: str

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_dir = self.root / "cdc_state"
        self.duckdb_path = self.root / "cdc_flight.duckdb"
        self.pipeline = f"cdc_p72_{self.name}_{os.getpid()}"
        self.env = {
            **os.environ,
            "PGHOST": self.topology.primary.host,
            "PGPORT": str(self.topology.port),
            "PGUSER": self.topology.primary.user,
            "PGPASSWORD": self.topology.primary.password,
            "PGDATABASE": self.topology.primary.dbname,
            "CDC_TEST_PGPORT": str(self.topology.port),
            "CDC_TEST_PGDATA": str(self.topology.data_dir),
            "CDC_TEST_PGSOCKET": str(self.topology.socket_dir),
            "CDC_TEST_PGLOG": str(self.topology.log_path),
            "CDC_TEST_PGDATABASE": self.topology.primary.dbname,
            "CDC_SOURCE_ROLE": "standby",
            "CDC_PRIMARY_DSN": self.topology.primary.dsn,
            "CDC_PRIMARY_PHYSICAL_SLOT": self.topology.physical_slot,
            "CDC_SLOT_NAME": self.topology.local_slot,
            "CDC_STATE_DIR": str(self.state_dir),
            "CDC_PIPELINES_DIR": str(self.state_dir / "dlt_pipelines"),
            "CDC_DUCKDB_PATH": str(self.duckdb_path),
            "CDC_PIPELINE_NAME": self.pipeline,
            "CDC_TEST_SLOT_STARTUP_LOCK": str(
                self.topology.data_dir.parent
                / ".pytest-instance-locks"
                / f"p72-standby-{self.topology.port}-slot-startup.lock"
            ),
            "RUNTIME__DLTHUB_TELEMETRY": "false",
        }

    @property
    def offset_file(self) -> Path:
        return self.state_dir / "offsets.dat"

    def spawn_service(
        self,
        *,
        destination: str = "duckdb",
        extra_env: dict[str, str] | None = None,
        capture: bool = False,
    ) -> subprocess.Popen:
        from support.fixtures import _popen_with_slot_startup_gate

        executable = Path(__file__).resolve().parents[2] / ".venv" / "bin" / "cdc-flight-service"
        sink = subprocess.PIPE if capture else subprocess.DEVNULL
        environment = {
            **self.env,
            "max_runtime_sec": "0",
            **(extra_env or {}),
        }
        return _popen_with_slot_startup_gate(
            [str(executable), "--destination", destination],
            env=environment,
            cwd=PROJECT_DIR,
            stdout=sink,
            stderr=sink,
            text=capture,
        )

    def invoke(
        self,
        *,
        destination: str = "duckdb",
        max_seconds: float = 180,
        idle_seconds: float = 8,
        timeout: float = 420,
        reset_state: bool = False,
        extra_env: dict[str, str] | None = None,
        expect_success: bool = True,
    ) -> dict:
        from support.fixtures import _invoke_pipeline

        return _invoke_pipeline(
            {**self.env, **(extra_env or {})},
            destination=destination,
            max_seconds=max_seconds,
            idle_seconds=idle_seconds,
            timeout=timeout,
            reset_state=reset_state,
            expect_success=expect_success,
        )

    def duck_query(self, statement: str, params: list | None = None) -> list[tuple]:
        con = duckdb.connect(str(self.duckdb_path), read_only=True)
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    def wait_for_phase(
        self,
        phase: str,
        *,
        process: subprocess.Popen | None = None,
        timeout: float = 90,
    ) -> None:
        def reached() -> bool:
            if process is not None and process.poll() is not None:
                raise AssertionError(
                    f"standby process exited before durable phase {phase!r}: "
                    f"returncode={process.returncode}"
                )
            if not self.duckdb_path.exists():
                return False
            try:
                return bool(
                    self.duck_query(
                        "SELECT 1 FROM _cdc_flight.heartbeat "
                        "WHERE pipeline = ? AND phase = ? LIMIT 1",
                        [self.pipeline, phase],
                    )
                )
            except duckdb.Error:
                return False

        deadline = time.monotonic() + timeout
        while True:
            if reached():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for durable run phase {phase!r}")
            time.sleep(min(0.25, remaining))

    def wait_for_destination(
        self,
        statement: str,
        params: list | None = None,
        *,
        predicate=lambda rows: bool(rows),
        process: subprocess.Popen | None = None,
        timeout: float = 90,
    ) -> list[tuple]:
        deadline = time.monotonic() + timeout
        while True:
            if process is not None and process.poll() is not None:
                raise AssertionError(
                    "standby process exited before destination witness "
                    f"(returncode={process.returncode})"
                )
            try:
                rows = self.duck_query(statement, params)
            except duckdb.Error:
                rows = []
            if predicate(rows):
                return rows
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return rows
            time.sleep(min(0.25, remaining))

    def last_summary(self) -> dict:
        path = self.state_dir / "last_run.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def terminate(self, process: subprocess.Popen, *, timeout: float = 90) -> int:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        return process.wait(timeout=timeout)

    def close_output(self, process: subprocess.Popen) -> str:
        output = ""
        if process.stdout is not None:
            output += process.stdout.read() or ""
        if process.stderr is not None:
            output += process.stderr.read() or ""
        return output[-12000:]
