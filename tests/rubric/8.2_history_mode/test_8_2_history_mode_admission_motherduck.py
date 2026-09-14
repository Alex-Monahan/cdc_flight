"""Round 1 MotherDuck lane: policy state is one destination transaction."""

from __future__ import annotations

import json
import os
import subprocess

import duckdb
import pytest
from support.fixtures import PROJECT_DIR, _executable

from cdc_flight.naming import quote

pytestmark = [pytest.mark.motherduck]


def test_external_policy_admission_is_scoped_and_durable_on_motherduck(
    sandbox, motherduck_case
):
    sandbox.reseed()
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    dataset = motherduck_case["dataset"]
    control_schema = motherduck_case["control_schema"]
    dsn = f"md:{database}?motherduck_token={token}"
    bootstrap = duckdb.connect(dsn)
    bootstrap.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(dataset)}")
    bootstrap.close()
    env = {
        **os.environ,
        **sandbox.env,
        "CDC_DESTINATION": "motherduck",
        "CDC_MD_DATABASE": database,
        "CDC_DATASET": dataset,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
        "max_runtime_sec": "0",
        "CDC_TABLES": "customers,orders",
        "CDC_AUTO_DISCOVERY": "0",
    }
    summary_path = sandbox.state_dir / "last_run.json"
    summary_path.unlink(missing_ok=True)
    try:
        process = subprocess.run(
            [
                _executable("cdc-flight-service"),
                "--destination",
                "motherduck",
                "set-history-mode",
                "--table",
                "app.customers",
                "--mode",
                "scd2",
            ],
            cwd=PROJECT_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=240,
        )
        assert process.returncode == 0, (
            f"policy operation exited {process.returncode}\n"
            f"stdout={process.stdout[-4000:]}\nstderr={process.stderr[-4000:]}"
        )
        summary = json.loads(summary_path.read_text())
        assert summary["history_mode"] == "scd2"
        assert summary["qualified_table"] == "app.customers"
        assert summary["policy_authority"] == "table_state.history_mode"
        assert summary["transaction_scope"] == "one_destination_transaction"
        assert summary["source_relation"]["primary_key_columns"] == ["id"]

        con = duckdb.connect(dsn)
        try:
            con.execute("FORCE CHECKPOINT")
            rows = con.execute(
                f"SELECT source_table, history_mode, snapshot_state, target_table "
                f"FROM {quote(control_schema)}.table_state "
                "WHERE pipeline = ? ORDER BY source_table",
                [sandbox.env["CDC_PIPELINE_NAME"]],
            ).fetchall()
            assert rows == [("customers", "scd2", "none", "cdcflight_app_customers")]
            assert con.execute(
                f"SELECT count(*) FROM {quote(control_schema)}.scd2_bundles"
            ).fetchone() == (0,)
        finally:
            con.close()
    finally:
        cleanup = duckdb.connect(dsn)
        try:
            cleanup.execute(f"DROP SCHEMA IF EXISTS {quote(dataset)} CASCADE")
        finally:
            cleanup.close()
