"""Round 1 slow lane: the production service operation and a fresh process."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from support.fixtures import PROJECT_DIR, _executable

pytestmark = [pytest.mark.slow]


def _invoke_policy(sandbox, table: str, mode: str) -> dict:
    env = {
        **os.environ,
        **sandbox.env,
        "max_runtime_sec": "0",
        "CDC_TABLES": "customers",
        "CDC_AUTO_DISCOVERY": "0",
    }
    summary_path = sandbox.state_dir / "last_run.json"
    summary_path.unlink(missing_ok=True)
    process = subprocess.run(
        [
            _executable("cdc-flight-service"),
            "--destination",
            "duckdb",
            "set-history-mode",
            "--table",
            table,
            "--mode",
            mode,
        ],
        cwd=PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert process.returncode == 0, (
        f"policy operation exited {process.returncode}\n"
        f"stdout={process.stdout[-4000:]}\nstderr={process.stderr[-4000:]}"
    )
    assert summary_path.exists(), process.stdout[-4000:]
    return json.loads(summary_path.read_text())


def _fresh_process_policy_rows(sandbox) -> dict[str, str]:
    code = (
        "import json, sys, duckdb\n"
        "con = duckdb.connect(sys.argv[1], read_only=True)\n"
        "try:\n"
        "    rows = con.execute(\"SELECT source_table, history_mode FROM "
        "_cdc_flight.table_state WHERE pipeline = ? ORDER BY source_table\", [sys.argv[2]]).fetchall()\n"
        "    print(json.dumps({str(table): str(mode) for table, mode in rows}))\n"
        "finally:\n"
        "    con.close()\n"
    )
    probe = subprocess.run(
        [sys.executable, "-c", code, str(sandbox.duckdb_path), sandbox.env["CDC_PIPELINE_NAME"]],
        cwd=PROJECT_DIR,
        env={**os.environ, **sandbox.env},
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return json.loads(probe.stdout)


def test_external_policy_admission_survives_restart_with_one_scd2_table(sandbox):
    sandbox.reseed()
    policy = _invoke_policy(sandbox, "app.customers", "scd2")
    assert policy["operation"] == "set-history-mode"
    assert policy["history_mode"] == "scd2"
    assert policy["qualified_table"] == "app.customers"
    assert policy["policy_authority"] == "table_state.history_mode"
    assert policy["transaction_scope"] == "one_destination_transaction"
    assert policy["service_heartbeat"]["fencing_epoch"] >= 1
    assert policy["source_relation"]["primary_key_columns"] == ["id"]
    assert sandbox.duck_query(
        "SELECT state, fencing_epoch FROM _cdc_flight.lease"
    ) == [("released", policy["service_heartbeat"]["fencing_epoch"])]

    # This is a separate normal cdc-flight process. It reopens the admitted policy;
    # the test never creates or updates table_state itself. The peer is deliberately
    # not configured, so its durable default remains current-only (`none`).
    run = sandbox.run(
        reset_state=False,
        snapshot_mode="no_data",
        max_seconds=180,
        timeout=360,
        extra_env={"CDC_TABLES": "customers", "CDC_AUTO_DISCOVERY": "0"},
    )
    assert run["ok"] is True, run

    rows = _fresh_process_policy_rows(sandbox)
    assert rows["customers"] == "scd2"
    assert rows.get("orders", "none") == "none"
    assert sandbox.duck_query(
        "SELECT history_mode FROM _cdc_flight.table_state "
        "WHERE pipeline = ? AND source_table = ?",
        [sandbox.env["CDC_PIPELINE_NAME"], "customers"],
    ) == [("scd2",)]
