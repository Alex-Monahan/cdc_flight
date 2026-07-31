"""Shared helpers for the rubric baseline probes.

A "probe" is a small, throwaway experiment that answers one rubric question with
observed behaviour instead of a guess. Probes live outside `tests/` on purpose:
they are evidence-gathering scripts for `RUBRIC_STATUS.md`, not regression tests,
and several of them deliberately break the source schema.

Every probe gets its own replication slot, Debezium offset file, dlt state
directory and DuckDB file under `probes/.out/<name>/`, so probes never collide
with `make pipeline` or with the pytest suite.

Usage:

    uv run python probes/p01_dml_edge_cases.py

Each probe prints a JSON document to stdout.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import psycopg

PROJECT_DIR = Path(__file__).resolve().parents[1]
PG_SH = PROJECT_DIR / "scripts" / "pg.sh"
VENV_BIN = PROJECT_DIR / ".venv" / "bin"
OUT_DIR = PROJECT_DIR / "probes" / ".out"

sys.path.insert(0, str(PROJECT_DIR / "src"))

from cdc_flight.config import SourceConfig  # noqa: E402

DATASET = "cdc_raw"


def executable(name: str) -> str:
    candidate = VENV_BIN / name
    return str(candidate) if candidate.exists() else name


# --------------------------------------------------------------------------- #
# Postgres
# --------------------------------------------------------------------------- #
def dsn() -> str:
    return SourceConfig().dsn


def sql(statements: str | list[str], autocommit: bool = True) -> None:
    if isinstance(statements, str):
        statements = [statements]
    with psycopg.connect(dsn(), autocommit=autocommit) as conn:
        for stmt in statements:
            conn.execute(stmt)
        if not autocommit:
            conn.commit()


def try_sql(statements: str | list[str]) -> list[dict[str, Any]]:
    """Run statements one at a time, recording failures instead of raising."""
    if isinstance(statements, str):
        statements = [statements]
    results = []
    for stmt in statements:
        try:
            sql(stmt)
            results.append({"sql": stmt[:120], "ok": True})
        except Exception as exc:
            results.append({"sql": stmt[:120], "ok": False, "error": f"{type(exc).__name__}: {exc}"})
    return results


def query(stmt: str, params: tuple | None = None) -> list[tuple]:
    with psycopg.connect(dsn(), autocommit=True) as conn:
        return conn.execute(stmt, params).fetchall()


def reseed() -> None:
    subprocess.run([str(PG_SH), "seed"], check=True, capture_output=True, text=True, timeout=180)


def drop_slot(slot: str) -> None:
    try:
        with psycopg.connect(dsn(), autocommit=True) as conn:
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )
    except Exception:
        pass


def slot_info(slot: str) -> dict[str, Any]:
    rows = query(
        "SELECT slot_name, active, restart_lsn::text, confirmed_flush_lsn::text, "
        "       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (slot,),
    )
    if not rows:
        return {}
    name, active, restart, confirmed, lag = rows[0]
    return {
        "slot": name,
        "active": active,
        "restart_lsn": restart,
        "confirmed_flush_lsn": confirmed,
        "retained_wal": lag,
    }


# --------------------------------------------------------------------------- #
# probe environment
# --------------------------------------------------------------------------- #
class Probe:
    def __init__(self, name: str, *, clean: bool = True, tables: str | None = None):
        self.name = name
        self.dir = OUT_DIR / name
        if clean and self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.slot = f"probe_{name}"[:60]
        self.duckdb_path = self.dir / "probe.duckdb"
        self.env = {
            **os.environ,
            "CDC_STATE_DIR": str(self.dir / "cdc_state"),
            "CDC_PIPELINES_DIR": str(self.dir / "cdc_state" / "dlt_pipelines"),
            "CDC_DUCKDB_PATH": str(self.duckdb_path),
            "CDC_SLOT_NAME": self.slot,
            "CDC_PIPELINE_NAME": f"probe_{name}",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
        }
        if tables:
            self.env["CDC_TABLES"] = tables
        self.findings: dict[str, Any] = {"probe": name}
        drop_slot(self.slot)

    # -- pipeline ----------------------------------------------------------- #
    def run_pipeline(
        self,
        *,
        reset_state: bool = False,
        max_seconds: float = 90,
        idle_seconds: float = 6,
        min_records: int = 0,
        snapshot_mode: str | None = None,
        timeout: float = 300,
        extra_env: dict[str, str] | None = None,
        expect_success: bool = True,
    ) -> dict[str, Any]:
        cmd = [
            executable("cdc-flight"),
            "--destination",
            "duckdb",
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
        env = {**self.env, **(extra_env or {})}
        proc = subprocess.run(
            cmd, capture_output=True, text=True, env=env, cwd=PROJECT_DIR, timeout=timeout
        )
        summary_path = Path(env["CDC_STATE_DIR"]) / "last_run.json"
        summary: dict[str, Any] = {}
        if proc.returncode == 0 and summary_path.exists():
            summary = json.loads(summary_path.read_text())
        summary["returncode"] = proc.returncode
        if proc.returncode != 0:
            summary["stderr_tail"] = proc.stderr[-3000:]
            summary["stdout_tail"] = proc.stdout[-3000:]
            if expect_success:
                raise AssertionError(
                    f"pipeline exited {proc.returncode}\n{proc.stdout[-3000:]}\n"
                    f"{proc.stderr[-3000:]}"
                )
        return summary

    # -- destination inspection --------------------------------------------- #
    def duck(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.duckdb_path), read_only=True)

    def tables(self) -> list[str]:
        con = self.duck()
        try:
            return sorted(
                t
                for (t,) in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                    [DATASET],
                ).fetchall()
            )
        finally:
            con.close()

    def columns(self, table: str) -> dict[str, str]:
        con = self.duck()
        try:
            return dict(
                con.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                    [DATASET, table],
                ).fetchall()
            )
        finally:
            con.close()

    def rows(self, stmt: str) -> Any:
        """Query the destination. Errors are returned, not raised: a probe that
        asks "did column X appear?" should record "no" rather than crash."""
        try:
            con = self.duck()
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        try:
            return con.execute(stmt).fetchall()
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        finally:
            con.close()

    def cleanup(self) -> None:
        drop_slot(self.slot)

    def emit(self) -> None:
        print(json.dumps(self.findings, indent=2, default=str, sort_keys=True))
