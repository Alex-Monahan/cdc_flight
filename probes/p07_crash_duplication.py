"""Probe: SIGKILL mid-load, then restart.

Rubric items answered: 1.1 / 1.2 (delivery guarantees), 1.7 (failures must not
cause correctness issues).

Sequence
  1. snapshot run
  2. insert 60 000 rows in one transaction
  3. start the pipeline, let it load for a while, `kill -9`
  4. restart the pipeline and let it finish
  5. count how many source rows arrived more than once
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

from _common import PROJECT_DIR, Probe, executable, query, reseed, sql

BULK_ROWS = 60_000
KILL_AFTER_SEC = 18


def main() -> None:
    p = Probe("p07_crash_duplication")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    sql(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        f"SELECT 'crash '||i, 'crash'||i||'@example.com', (i % 997)::numeric "
        f"FROM generate_series(1, {BULK_ROWS}) i"
    )
    p.findings["pg_rows"] = query("SELECT count(*) FROM app.customers")[0][0]

    cmd = [
        executable("cdc-flight"),
        "--destination",
        "duckdb",
        "--max-seconds",
        "900",
        "--idle-seconds",
        "10",
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=p.env,
        cwd=PROJECT_DIR,
        start_new_session=True,
    )
    time.sleep(KILL_AFTER_SEC)
    killed = proc.poll() is None
    if killed:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=60)
    p.findings["killed_mid_load"] = killed
    p.findings["killed_returncode"] = proc.returncode

    after_kill = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name LIKE 'crash %'"
    )
    p.findings["rows_loaded_before_kill"] = after_kill

    # restart and drain
    p.findings["restart_run"] = p.run_pipeline(max_seconds=900, idle_seconds=10, timeout=1200)

    p.findings["total_crash_rows"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name LIKE 'crash %'"
    )
    p.findings["distinct_crash_rows"] = p.rows(
        "SELECT count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers WHERE name LIKE 'crash %'"
    )
    p.findings["duplicate_ids"] = p.rows(
        "SELECT count(*) FROM (SELECT id FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'crash %' GROUP BY id HAVING count(*) > 1)"
    )
    p.findings["missing_rows"] = p.rows(
        f"SELECT {BULK_ROWS} - count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'crash %'"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
