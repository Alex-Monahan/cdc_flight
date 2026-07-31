"""Probe: can the pipeline read from a Postgres physical replica?

Rubric item answered: 7.2 (read from a replica, light workload on the primary).

Builds a throwaway hot standby of the project cluster with `pg_basebackup` on
port 15433, points the pipeline at it, and reports what happens. The standby is
torn down at the end; the primary on :15432 is untouched apart from one
temporary physical replication connection.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

from _common import PROJECT_DIR, Probe, query, reseed, sql

PGBIN = Path(os.environ.get("PGBIN", "/opt/homebrew/opt/postgresql@18/bin"))
REPLICA_DIR = PROJECT_DIR / ".pgdata_replica"
REPLICA_PORT = 15433


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300, **kw)


def teardown() -> None:
    if (REPLICA_DIR / "postmaster.pid").exists():
        sh([str(PGBIN / "pg_ctl"), "-D", str(REPLICA_DIR), "-m", "immediate", "stop"])
        time.sleep(1)
    shutil.rmtree(REPLICA_DIR, ignore_errors=True)


def main() -> None:
    p = Probe("p09_replica")
    reseed()
    teardown()

    env = {**os.environ, "PGPASSWORD": "postgres"}
    base = sh(
        [
            str(PGBIN / "pg_basebackup"),
            "-h",
            "127.0.0.1",
            "-p",
            "15432",
            "-U",
            "postgres",
            "-D",
            str(REPLICA_DIR),
            "-X",
            "stream",
            "-R",
            "-C",
            "-S",
            "probe_standby_slot",
            "-P",
        ],
        env=env,
    )
    p.findings["basebackup_returncode"] = base.returncode
    p.findings["basebackup_stderr"] = base.stderr[-1500:]
    if base.returncode != 0:
        teardown()
        p.emit()
        return

    with (REPLICA_DIR / "postgresql.auto.conf").open("a") as fh:
        fh.write(f"\nport = {REPLICA_PORT}\nhot_standby = on\nhot_standby_feedback = on\n")
        fh.write(f"unix_socket_directories = '{REPLICA_DIR}'\n")

    start = sh(
        [
            str(PGBIN / "pg_ctl"),
            "-D",
            str(REPLICA_DIR),
            "-l",
            str(REPLICA_DIR / "server.log"),
            "-w",
            "-t",
            "60",
            "start",
        ]
    )
    p.findings["replica_start_returncode"] = start.returncode
    p.findings["replica_start_stderr"] = start.stderr[-1000:]

    check = sh(
        [
            str(PGBIN / "psql"),
            "-h",
            "127.0.0.1",
            "-p",
            str(REPLICA_PORT),
            "-U",
            "postgres",
            "-d",
            "cdc_source",
            "-tAc",
            "select pg_is_in_recovery(), count(*) from app.customers",
        ],
        env=env,
    )
    p.findings["replica_state"] = check.stdout.strip()
    p.findings["replica_check_stderr"] = check.stderr[-800:]

    # generate something to capture, on the PRIMARY
    sql("INSERT INTO app.customers (name, email) VALUES ('replica probe', 'replica@example.com')")
    time.sleep(2)

    run = p.run_pipeline(
        reset_state=True,
        max_seconds=90,
        idle_seconds=8,
        expect_success=False,
        extra_env={"PGPORT": str(REPLICA_PORT)},
    )
    p.findings["pipeline_against_replica"] = run
    p.findings["destination_tables"] = p.tables()
    p.findings["customers_landed"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers"
    )

    # was a slot created on the PRIMARY? (it should not have been)
    p.findings["primary_slots"] = query(
        "SELECT slot_name, slot_type, active FROM pg_replication_slots ORDER BY 1"
    )

    teardown()
    query("SELECT pg_drop_replication_slot('probe_standby_slot') "
          "FROM pg_replication_slots WHERE slot_name = 'probe_standby_slot'")
    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
