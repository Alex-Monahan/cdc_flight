"""Probe: does a lost offset flush duplicate rows?

Rubric items answered: 1.1 / 1.2 (delivery guarantees), 1.7 (failures must not
cause correctness issues).

p07 tried to catch the window with a SIGKILL and lost the race (the load
finished first). This probe removes the timing luck and reproduces exactly the
same state deterministically:

  `offset.flush.interval.ms = 1000`, and pydbzengine calls
  `committer.markBatchFinished()` only *after* `handleJsonBatch` returns
  (`repos/pydbzengine/pydbzengine/_jvm.py:121-124`). So a SIGKILL in the
  window between "dlt committed the rows" and "offsets.dat hit the disk"
  leaves the destination ahead of the offset file.

Restoring a previous `offsets.dat` puts the process in precisely that state.
Case B then repeats the experiment with a real SIGKILL against a big enough
change set that the kill lands mid-load.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

from _common import PROJECT_DIR, Probe, executable, reseed, sql

REPLAY_ROWS = 1_000
KILL_ROWS = 400_000
KILL_AFTER_SEC = 14


def main() -> None:
    p = Probe("p13_offset_replay")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    state_dir = Path(p.env["CDC_STATE_DIR"])
    offsets = state_dir / "offsets.dat"
    backup = state_dir / "offsets.before"
    shutil.copy(offsets, backup)

    # ---- Case A: deterministic "offset flush was lost" -------------------
    sql(
        "INSERT INTO app.customers (name, email) "
        f"SELECT 'replay '||i, 'replay'||i||'@example.com' "
        f"FROM generate_series(1, {REPLAY_ROWS}) i"
    )
    p.findings["runA_first_load"] = p.run_pipeline(max_seconds=300, idle_seconds=8)
    p.findings["A_rows_after_first"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'replay %'"
    )
    shutil.copy(backup, offsets)  # <- the crash: the flush never reached disk
    p.findings["runA_replay"] = p.run_pipeline(max_seconds=300, idle_seconds=8)
    p.findings["A_rows_after_replay"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'replay %'"
    )
    p.findings["A_duplicated_ids"] = p.rows(
        "SELECT count(*) FROM (SELECT id FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'replay %' GROUP BY id HAVING count(*) > 1)"
    )

    # ---- Case B: real SIGKILL, large enough to land mid-load -------------
    sql(
        "INSERT INTO app.customers (name, email) "
        f"SELECT 'kill '||i, 'kill'||i||'@example.com' "
        f"FROM generate_series(1, {KILL_ROWS}) i"
    )
    proc = subprocess.Popen(
        [
            executable("cdc-flight"),
            "--destination",
            "duckdb",
            "--max-seconds",
            "1800",
            "--idle-seconds",
            "10",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=p.env,
        cwd=PROJECT_DIR,
        start_new_session=True,
    )
    time.sleep(KILL_AFTER_SEC)
    p.findings["B_killed_mid_load"] = proc.poll() is None
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=60)
    p.findings["B_rows_before_restart"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'kill %'"
    )
    p.findings["runB_restart"] = p.run_pipeline(max_seconds=1800, idle_seconds=10, timeout=2400)
    p.findings["B_rows_after_restart"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'kill %'"
    )
    p.findings["B_duplicated_ids"] = p.rows(
        "SELECT count(*) FROM (SELECT id FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'kill %' GROUP BY id HAVING count(*) > 1)"
    )
    p.findings["B_expected_rows"] = KILL_ROWS

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
