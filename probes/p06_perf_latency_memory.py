"""Probe: throughput, latency and memory.

Rubric items answered: 5.1 (large changes), 5.2 (latency), 5.3 (source TPS),
5.4 (memory guardrails).

Three measurements:
  A. one transaction inserting 50 000 rows      -> "large change" throughput
  B. 5 000 single-row transactions              -> per-transaction overhead
  C. one row inserted while the engine is live  -> capture latency
Max RSS of the pipeline process is captured with /usr/bin/time -l.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time

from _common import PROJECT_DIR, Probe, dsn, executable, query, reseed, sql

BULK_ROWS = 50_000
TXN_COUNT = 5_000


def run_timed(env, *, max_seconds: float, idle_seconds: float, reset: bool = False) -> dict:
    cmd = [
        "/usr/bin/time",
        "-l",
        executable("cdc-flight"),
        "--destination",
        "duckdb",
        "--max-seconds",
        str(max_seconds),
        "--idle-seconds",
        str(idle_seconds),
        "--min-records",
        "0",
    ]
    if reset:
        cmd.append("--reset-state")
    t0 = time.time()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, env=env, cwd=PROJECT_DIR, timeout=1800
    )
    wall = time.time() - t0
    rss = re.search(r"(\d+)\s+maximum resident set size", proc.stderr)
    summary = {}
    from pathlib import Path

    sp = Path(env["CDC_STATE_DIR"]) / "last_run.json"
    if sp.exists():
        summary = json.loads(sp.read_text())
    summary["wall_sec"] = round(wall, 2)
    summary["max_rss_mb"] = round(int(rss.group(1)) / 1024 / 1024, 1) if rss else None
    summary["returncode"] = proc.returncode
    summary["batch_log_times"] = re.findall(r"(\d\d:\d\d:\d\d),\d+ INFO .*loading batch", proc.stdout)
    return summary


def main() -> None:
    p = Probe("p06_perf_latency_memory")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    # -- A. one big transaction --------------------------------------------
    t0 = time.time()
    sql(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        f"SELECT 'bulk '||i, 'bulk'||i||'@example.com', (i % 1000)::numeric "
        f"FROM generate_series(1, {BULK_ROWS}) i"
    )
    p.findings["A_pg_insert_sec"] = round(time.time() - t0, 2)
    runA = run_timed(p.env, max_seconds=1500, idle_seconds=10)
    runA["rows_per_sec"] = (
        round(runA.get("records", 0) / max(runA["wall_sec"] - 10, 0.01), 1) if runA else None
    )
    p.findings["A_bulk_50k_one_txn"] = runA

    # -- B. many small transactions ----------------------------------------
    import psycopg

    t0 = time.time()
    with psycopg.connect(dsn(), autocommit=True) as conn:
        for i in range(TXN_COUNT):
            conn.execute(
                "INSERT INTO app.customers (name, email) VALUES (%s, %s)",
                (f"txn {i}", f"txn{i}@example.com"),
            )
    pg_sec = time.time() - t0
    p.findings["B_pg_commit_sec"] = round(pg_sec, 2)
    p.findings["B_pg_tps"] = round(TXN_COUNT / pg_sec, 1)
    runB = run_timed(p.env, max_seconds=1500, idle_seconds=10)
    runB["rows_per_sec"] = round(runB.get("records", 0) / max(runB["wall_sec"] - 10, 0.01), 1)
    p.findings["B_5k_single_row_txns"] = runB

    # -- C. capture latency of a single row on a live engine ---------------
    marker = {"t0_ms": None}

    def insert_later():
        time.sleep(12)
        marker["t0_ms"] = int(time.time() * 1000)
        sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('latency probe', 'latency-probe@example.com')"
        )

    th = threading.Thread(target=insert_later)
    th.start()
    runC = run_timed(p.env, max_seconds=45, idle_seconds=12)
    th.join()
    p.findings["C_latency_run"] = runC
    row = p.rows(
        "SELECT dbz_source_ts_ms, dbz_ts_ms FROM cdc_raw.cdcflight_app_customers "
        "WHERE name = 'latency probe'"
    )
    p.findings["C_row"] = row
    if isinstance(row, list) and row:
        src, dbz = row[0]
        p.findings["C_commit_to_debezium_ms"] = dbz - src
        p.findings["C_insert_call_to_debezium_ms"] = dbz - marker["t0_ms"]

    # -- WAL retained while idle (no heartbeat) ----------------------------
    p.findings["slot_state"] = query(
        "SELECT slot_name, active, restart_lsn::text, "
        "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (p.slot,),
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
