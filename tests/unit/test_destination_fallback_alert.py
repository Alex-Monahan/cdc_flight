from __future__ import annotations

import json
import os
import subprocess
import sys

_HOLDER = r'''
import sys
import time
import duckdb
from cdc_flight.destination import DUCKDB_CONNECT_CONFIG

con = duckdb.connect(sys.argv[1], config=DUCKDB_CONNECT_CONFIG)
print("READY", flush=True)
time.sleep(12)
con.close()
'''

_PIPELINE = r'''
from cdc_flight.pipeline import run
try:
    run(max_seconds=1)
except BaseException as exc:
    print(type(exc).__name__, exc, flush=True)
    raise
'''


def test_real_second_process_records_lock_failure_without_destination_connection(tmp_path):
    """A real pre-connect DuckDB lock failure leaves an fsynced operator alert."""
    path = tmp_path / "locked.duckdb"
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    holder = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "READY"
        contender_env = dict(env)
        contender_env.update(
            {
                "CDC_DESTINATION": "duckdb",
                "CDC_DUCKDB_PATH": str(path),
                "CDC_PIPELINE_NAME": "peer-pipeline",
                "CDC_SLOT_NAME": "peer-slot",
                "CDC_STATE_DIR": str(tmp_path / "peer-state"),
                "CDC_PIPELINES_DIR": str(tmp_path / "peer-pipelines"),
                "CDC_TEST_PGPORT": "15432",
                "PGPORT": "15432",
            }
        )
        contender = subprocess.run(
            [sys.executable, "-c", _PIPELINE],
            capture_output=True,
            text=True,
            env=contender_env,
            timeout=30,
        )
        assert contender.returncode != 0
        sidecar = path.with_name(path.name + ".cdc_alerts.jsonl")
        rows = [json.loads(line) for line in sidecar.read_text().splitlines()]
        assert len(rows) == 1, rows
        assert rows[0]["code"] == "destination_unavailable"
        assert rows[0]["severity"] == "critical"
        assert rows[0]["context"]["destination_unavailable"] is True
        assert "destination unavailable" in contender.stdout.lower() or (
            "destination unavailable" in contender.stderr.lower()
        )
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=20)


def test_fallback_alert_failure_is_logged_critically(tmp_path, monkeypatch, caplog):
    import logging

    from cdc_flight import destination as destination_mod
    from cdc_flight.config import DestinationConfig
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(kind="duckdb", duckdb_path=tmp_path / "missing.duckdb")
    monkeypatch.setattr(
        destination_mod,
        "persist_fallback_alert",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("sidecar denied")),
    )
    caplog.set_level(logging.CRITICAL, logger="cdc_flight.pipeline")
    _record_run_failure_alert(
        None,
        dest=dest,
        runner_id="runner",
        exc=OSError("database locked"),
        summary={"stop_reason": "destination_unavailable"},
    )
    assert "fallback alert could not be persisted" in caplog.text
