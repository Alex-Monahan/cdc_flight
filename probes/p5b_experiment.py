"""Repeatable P5b measurements against the local Postgres and MotherDuck.

This is an evidence probe, not a pass/fail test.  It deliberately reports the
wall-clock child lifetime (including JVM startup and teardown), the source-side
transaction rate, and the destination summary separately.  The probe owns one
uniquely named MotherDuck database and removes it in ``finally``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import psycopg

PROJECT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIR / "tests"))

from _common import Probe, executable, reseed, sql  # noqa: E402
from support.motherduck_probe import _drop_database, create_database  # noqa: E402


def _load_average() -> str:
    return subprocess.check_output(["uptime"], text=True).strip()


def _source_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PGUSER', 'postgres')}:{os.environ.get('PGPASSWORD', 'postgres')}"
        f"@{os.environ.get('PGHOST', '127.0.0.1')}:{os.environ.get('CDC_TEST_PGPORT', os.environ.get('PGPORT', '15432'))}"
        f"/{os.environ.get('CDC_TEST_PGDATABASE', os.environ.get('PGDATABASE', 'cdc_source'))}"
    )


def _start_pipeline(
    env: dict[str, str], *, reset_state: bool = False, max_seconds: float = 900,
    idle_seconds: float = 10, min_records: int = 0,
) -> tuple[subprocess.Popen[str], float]:
    command = [
        "/usr/bin/time",
        "-l",
        executable("cdc-flight"),
        "--destination",
        "motherduck",
        "--max-seconds",
        str(max_seconds),
        "--idle-seconds",
        str(idle_seconds),
        "--min-records",
        str(min_records),
    ]
    if reset_state:
        command.append("--reset-state")
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        cwd=PROJECT_DIR,
    )
    return process, started


def _finish_pipeline(
    process: subprocess.Popen[str], started: float, env: dict[str, str], *, timeout: float = 1800
) -> dict:
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        stdout, stderr = process.communicate()
        raise TimeoutError(f"pipeline exceeded {timeout}s: {stderr[-2000:]}") from exc
    wall = time.monotonic() - started
    summary_path = Path(env["CDC_STATE_DIR"]) / "last_run.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    summary.update(
        {
            "child_wall_sec": round(wall, 3),
            "returncode": process.returncode,
            "host_load": _load_average(),
            "stdout_tail": stdout[-1000:],
            "stderr_tail": stderr[-3000:],
        }
    )
    # macOS `/usr/bin/time -l` prints the value before the label (without a
    # colon), e.g. `690061312  maximum resident set size`.
    match = re.search(r"(\d+)\s+maximum resident set size", stderr)
    if match:
        summary["max_rss_bytes"] = int(match.group(1))
    return summary


def _pipeline(
    probe: Probe,
    env: dict[str, str],
    *,
    reset_state: bool = False,
    max_seconds: float = 900,
    idle_seconds: float = 10,
    timeout: float = 1800,
) -> dict:
    del probe
    process, started = _start_pipeline(
        env, reset_state=reset_state, max_seconds=max_seconds, idle_seconds=idle_seconds
    )
    return _finish_pipeline(process, started, env, timeout=timeout)


def _wait_for_slot(
    probe: Probe, process: subprocess.Popen[str], timeout: float = 60
) -> None:
    deadline = time.monotonic() + timeout
    with psycopg.connect(_source_dsn(), autocommit=True) as connection:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    f"pipeline exited before slot activation: {process.returncode}"
                )
            rows = connection.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s AND active",
                (probe.slot,),
            ).fetchall()
            if rows:
                return
            time.sleep(0.1)
    raise RuntimeError(f"slot {probe.slot!r} did not become active")


def _one_row_transactions(prefix: str, count: int) -> dict[str, float | int]:
    started = time.monotonic()
    with psycopg.connect(_source_dsn(), autocommit=True) as connection:
        for index in range(count):
            connection.execute(
                "INSERT INTO app.customers (name, email, lifetime_value) VALUES (%s, %s, %s)",
                (f"{prefix}-{index}", f"{prefix}-{index}@example.com", index % 100),
            )
    elapsed = time.monotonic() - started
    return {
        "rows": count,
        "transactions": count,
        "wall_sec": round(elapsed, 3),
        "source_tps": round(count / max(elapsed, 0.001), 2),
    }


def _paced_one_row_transactions(
    prefix: str, count: int, *, workers: int = 8, target_tps: float = 500.0
) -> dict[str, float | int]:
    """Drive independent one-row commits at a sustained source rate."""
    started = time.monotonic()

    def write_worker(worker: int) -> None:
        with psycopg.connect(_source_dsn(), autocommit=True) as connection:
            for index in range(worker, count, workers):
                connection.execute(
                    "INSERT INTO app.customers (name, email, lifetime_value) VALUES (%s, %s, %s)",
                    (f"{prefix}-{index}", f"{prefix}-{index}@example.com", index % 100),
                )
                due = started + (index + 1) / target_tps
                remaining = due - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(write_worker, worker) for worker in range(workers)]
        for future in futures:
            future.result()
    elapsed = time.monotonic() - started
    return {
        "rows": count,
        "transactions": count,
        "workers": workers,
        "target_tps": target_tps,
        "wall_sec": round(elapsed, 3),
        "source_tps": round(count / max(elapsed, 0.001), 2),
    }


def _one_transaction(prefix: str, count: int) -> dict[str, float | int]:
    started = time.monotonic()
    sql(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        f"SELECT '{prefix}-' || i, '{prefix}-' || i || '@example.com', (i % 100)::numeric "
        f"FROM generate_series(1, {count}) i"
    )
    elapsed = time.monotonic() - started
    return {
        "rows": count,
        "transactions": 1,
        "wall_sec": round(elapsed, 3),
        "source_tps": round(1 / max(elapsed, 0.001), 2),
        "source_rows_per_sec": round(count / max(elapsed, 0.001), 2),
    }


def _destination_count(database: str, token: str, dataset: str, prefix: str) -> int:
    connection = duckdb.connect(f"md:{database}?motherduck_token={token}")
    try:
        connection.execute("FORCE CHECKPOINT")
        return int(
            connection.execute(
                f'SELECT count(*) FROM "{dataset}"."cdcflight_app_customers" '
                "WHERE name LIKE ?",
                [f"{prefix}-%"],
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _case(
    *,
    token: str,
    database: str,
    name: str,
    rows: int,
    shape: str,
    commit_max_age: str | None = None,
) -> dict:
    probe = Probe(f"p5b_{name}", tables="customers")
    dataset = f"p5b_{name}_{uuid.uuid4().hex[:8]}"
    prefix = f"p5b-{name}-{uuid.uuid4().hex[:8]}"
    env = {
        **probe.env,
        "CDC_DESTINATION": "motherduck",
        "CDC_MD_DATABASE": database,
        "CDC_DATASET": dataset,
        "CDC_TABLES": "customers",
        "CDC_AUTO_DISCOVERY": "0",
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
    }
    if commit_max_age is not None:
        env["CDC_COMMIT_MAX_AGE"] = commit_max_age
    result: dict = {
        "name": name,
        "shape": shape,
        "rows": rows,
        "dataset": dataset,
        "source_prefix": prefix,
        "host_load_before": _load_average(),
    }
    try:
        reseed()
        if name == "sustained":
            result["snapshot"] = _pipeline(probe, env, reset_state=True)
            # Start the stream before the source writer.  The writer waits for
            # the child's slot, making the measured interval a warm stream rather
            # than a preloaded bounded batch.
            result["host_load_before_stream"] = _load_average()
            process, child_started = _start_pipeline(env, min_records=rows)
            _wait_for_slot(probe, process)
            source_started = time.monotonic()
            result["source"] = _paced_one_row_transactions(prefix, rows)
            result["source_start_after_child_sec"] = round(
                source_started - child_started, 3
            )
            result["stream"] = _finish_pipeline(process, child_started, env)
            result["warm_interval_sec"] = round(
                result["stream"]["child_wall_sec"]
                - result["source_start_after_child_sec"],
                3,
            )
        elif shape == "one_row_tx":
            result["snapshot"] = _pipeline(probe, env, reset_state=True)
            result["source"] = _one_row_transactions(prefix, rows)
            result["run"] = _pipeline(probe, env)
        elif shape == "one_tx":
            result["snapshot"] = _pipeline(probe, env, reset_state=True)
            result["source"] = _one_transaction(prefix, rows)
            result["run"] = _pipeline(probe, env)
        else:
            raise ValueError(shape)
        result["destination_rows"] = _destination_count(database, token, dataset, prefix)
        return result
    finally:
        probe.cleanup()


def main() -> None:
    token = os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")  # noqa: SIM112
    if not token:
        raise SystemExit("motherduck_token is not set")
    database = f"cdc_p5b_{uuid.uuid4().hex[:10]}"
    create_database(token, database)
    findings: dict = {"database": database, "host_load_start": _load_average()}
    try:
        findings["cold"] = _case(
            token=token, database=database, name="cold", rows=20_000, shape="one_row_tx"
        )
        findings["fifty_k"] = _case(
            token=token, database=database, name="fifty_k", rows=50_000, shape="one_tx"
        )
        findings["sustained"] = _case(
            token=token,
            database=database,
            name="sustained",
            rows=30_000,
            shape="one_row_tx",
            commit_max_age="15",
        )
    finally:
        _drop_database(token, database)
    print(json.dumps(findings, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
