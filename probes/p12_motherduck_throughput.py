"""Probe: what does the *target* destination actually cost?

Rubric items answered: 5.1 / 5.3 (throughput against MotherDuck rather than a
local DuckDB file), 1.3 (how many MotherDuck commits one CDC run produces).

Deliberately small (5 000 rows, one throwaway dataset, dropped afterwards) --
MotherDuck is the smoke-test path in this repo, not the dev loop.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid

from _common import PROJECT_DIR, Probe, executable, reseed, sql

BULK_ROWS = 5_000
MD_DATABASE = "cdc_flight_dev"


def main() -> None:
    # MotherDuck's own convention is the lowercase spelling; config.py prefers it too.
    token = os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")  # noqa: SIM112
    p = Probe("p12_motherduck_throughput")
    if not token:
        p.findings["skipped"] = "motherduck_token not set"
        p.emit()
        return

    dataset = f"probe_{uuid.uuid4().hex[:8]}"
    md_env = {
        "CDC_DESTINATION": "motherduck",
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": MD_DATABASE,
        "motherduck_token": token,
        "MOTHERDUCK_TOKEN": token,
    }
    p.findings["dataset"] = dataset
    reseed()

    def run_md(**kw):
        cmd = [
            executable("cdc-flight"),
            "--destination",
            "motherduck",
            "--max-seconds",
            str(kw.get("max_seconds", 600)),
            "--idle-seconds",
            str(kw.get("idle_seconds", 10)),
            "--min-records",
            "0",
        ]
        if kw.get("reset_state"):
            cmd.append("--reset-state")
        t0 = time.time()
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**p.env, **md_env},
            cwd=PROJECT_DIR,
            timeout=1800,
        )
        wall = time.time() - t0
        import json
        from pathlib import Path

        sp = Path(p.env["CDC_STATE_DIR"]) / "last_run.json"
        summary = json.loads(sp.read_text()) if sp.exists() else {}
        summary["wall_sec"] = round(wall, 2)
        summary["returncode"] = proc.returncode
        if proc.returncode != 0:
            summary["stderr_tail"] = proc.stderr[-2000:]
        return summary

    p.findings["run0_snapshot_md"] = run_md(reset_state=True, max_seconds=600)

    sql(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        f"SELECT 'mdbulk '||i, 'mdbulk'||i||'@example.com', (i % 100)::numeric "
        f"FROM generate_series(1, {BULK_ROWS}) i"
    )
    run = run_md(max_seconds=900)
    run["rows_per_sec"] = round(run.get("records", 0) / max(run["wall_sec"] - 18, 0.01), 1)
    p.findings["bulk_5k_md"] = run

    import duckdb

    con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={token}")
    try:
        p.findings["md_row_count"] = con.execute(
            f'SELECT count(*) FROM "{MD_DATABASE}"."{dataset}"."cdcflight_app_customers"'
        ).fetchone()[0]
        p.findings["md_load_packages"] = con.execute(
            f'SELECT count(*) FROM "{MD_DATABASE}"."{dataset}"."_dlt_loads"'
        ).fetchone()[0]
    finally:
        con.execute(f'DROP SCHEMA IF EXISTS "{MD_DATABASE}"."{dataset}" CASCADE')
        con.close()

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
