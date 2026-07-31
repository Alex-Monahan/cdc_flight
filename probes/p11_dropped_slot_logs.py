"""Probe: what does the process actually *say* when the replication slot is gone?

Follow-up to p10 case A, which showed a 1-second run reporting `records: 0`,
`stop_reason: engine_finished` and **exit code 0** after the slot was dropped.
This probe captures the full stdout/stderr so the failure mode is documented
rather than inferred, and checks whether the slot is recreated.

Rubric items: 4.1 (failed slot), 4.3 (no hang, but no recovery either),
6.2 (alerting: a silent success is the worst possible signal).
"""

from __future__ import annotations

import re
import subprocess

from _common import PROJECT_DIR, Probe, drop_slot, executable, query, reseed, sql


def main() -> None:
    p = Probe("p11_dropped_slot_logs")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    sql("INSERT INTO app.customers (name, email) VALUES ('pre drop', 'predrop@example.com')")
    drop_slot(p.slot)

    proc = subprocess.run(
        [
            executable("cdc-flight"),
            "--destination",
            "duckdb",
            "--max-seconds",
            "60",
            "--idle-seconds",
            "6",
        ],
        capture_output=True,
        text=True,
        env=p.env,
        cwd=PROJECT_DIR,
        timeout=200,
    )
    p.findings["returncode"] = proc.returncode
    combined = proc.stdout + proc.stderr
    p.findings["error_lines"] = [
        line
        for line in combined.splitlines()
        if re.search(r"ERROR|WARN|Exception|failed|slot", line, re.I)
    ][-40:]
    p.findings["slot_recreated"] = query(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (p.slot,)
    )[0][0]
    p.findings["rows_landed"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name = 'pre drop'"
    )

    # A second run, now that the slot may have been recreated.
    p.findings["second_run"] = p.run_pipeline(max_seconds=60, idle_seconds=6, expect_success=False)
    p.findings["rows_landed_after_second"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name = 'pre drop'"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
