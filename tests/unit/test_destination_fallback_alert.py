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

    import pytest

    from cdc_flight import destination as destination_mod
    from cdc_flight.config import DestinationConfig
    from cdc_flight.destination import RunState
    from cdc_flight.errors import AlertPersistenceFailure
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(kind="duckdb", duckdb_path=tmp_path / "missing.duckdb")
    monkeypatch.setattr(
        destination_mod,
        "persist_fallback_alert",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("sidecar denied")),
    )
    caplog.set_level(logging.CRITICAL, logger="cdc_flight.pipeline")
    with pytest.raises(AlertPersistenceFailure) as failure:
        _record_run_failure_alert(
            None,
            dest=dest,
            run_state=RunState.new(dest.pipeline_name),
            exc=OSError("database locked"),
            summary={"stop_reason": "destination_unavailable"},
        )
    assert "fallback alert could not be persisted" in caplog.text
    assert failure.value.summary["alerting_broken"] is True
    assert "database locked" in str(failure.value.original_failure)


def test_fallback_alert_directory_fails_loudly_without_sidecar(tmp_path, capsys):
    import pytest

    from cdc_flight.config import DestinationConfig
    from cdc_flight.destination import RunState, fallback_alert_path
    from cdc_flight.errors import AlertPersistenceFailure
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(kind="duckdb", duckdb_path=tmp_path / "locked.duckdb")
    episode_path = dest.duckdb_path.with_name(
        dest.duckdb_path.name + ".cdc_alerts.jsonl.episode.json"
    )
    episode_path.mkdir()

    with pytest.raises(AlertPersistenceFailure) as failure:
        _record_run_failure_alert(
            None,
            dest=dest,
            run_state=RunState.new(dest.pipeline_name),
            exc=OSError("database locked"),
            summary={"stop_reason": "destination_unavailable"},
        )

    assert not fallback_alert_path(dest).exists()
    assert "ALERTING BROKEN" in str(failure.value)
    assert "fallback alert failed" in capsys.readouterr().err


def test_fallback_replay_keeps_distinct_outages_distinct(tmp_path):
    import duckdb

    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import RunState, fallback_alert_path, replay_fallback_alerts
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(
        kind="duckdb",
        pipeline_name="fallback-episodes",
        duckdb_path=tmp_path / "dest.duckdb",
    )
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        summary = {"stop_reason": "destination_unavailable"}
        first_run = RunState.new(dest.pipeline_name)
        second_run = RunState.new(dest.pipeline_name)
        _record_run_failure_alert(
            None, dest=dest, run_state=first_run, exc=OSError("first lock"), summary=summary
        )
        _record_run_failure_alert(
            None, dest=dest, run_state=second_run, exc=OSError("second lock"), summary=summary
        )
        sidecar_rows = [
            json.loads(line)
            for line in fallback_alert_path(dest).read_text().splitlines()
        ]
        assert len(sidecar_rows) == 2
        assert [row["runner_id"] for row in sidecar_rows] == [
            first_run.runner_id,
            second_run.runner_id,
        ]
        assert [row["marker_value"] for row in sidecar_rows] == [
            "destination_unavailable:fallback-episodes:occurrence:episode:1",
            "destination_unavailable:fallback-episodes:occurrence:episode:2",
        ]

        assert replay_fallback_alerts(con, dest) == 2
        rows = con.execute(
            'SELECT code, context FROM "_cdc_flight".alerts '
            "WHERE pipeline = ? ORDER BY raised_at",
            [dest.pipeline_name],
        ).fetchall()
        assert [row[0] for row in rows] == [
            "destination_unavailable", "destination_unavailable"
        ]
        assert ["episode:1" in row[1] for row in rows] == [True, False]
        assert replay_fallback_alerts(con, dest) == 0
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight".alerts WHERE pipeline = ?',
            [dest.pipeline_name],
        ).fetchone()[0] == 2
    finally:
        con.close()


def test_fallback_replay_collapses_repeated_observations_of_one_outage(tmp_path):
    import duckdb

    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import RunState, fallback_alert_path, replay_fallback_alerts
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(
        kind="duckdb",
        pipeline_name="fallback-repeat",
        duckdb_path=tmp_path / "dest.duckdb",
    )
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        summary = {"stop_reason": "destination_unavailable"}
        first_run = RunState.new(dest.pipeline_name)
        second_run = RunState.new(dest.pipeline_name)
        third_run = RunState.new(dest.pipeline_name)
        _record_run_failure_alert(
            None, dest=dest, run_state=first_run, exc=OSError("same lock"), summary=summary
        )
        _record_run_failure_alert(
            None, dest=dest, run_state=second_run, exc=OSError("same lock"), summary=summary
        )
        sidecar_rows = [
            json.loads(line)
            for line in fallback_alert_path(dest).read_text().splitlines()
        ]
        assert len(sidecar_rows) == 2
        assert {row["marker_value"] for row in sidecar_rows} == {
            "destination_unavailable:fallback-repeat:occurrence:episode:1"
        }
        assert replay_fallback_alerts(con, dest) == 1
        assert replay_fallback_alerts(con, dest) == 0
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight".alerts WHERE pipeline = ?',
            [dest.pipeline_name],
        ).fetchone()[0] == 1

        # Recovery closes episode 1; the same failure signature is a new episode
        # rather than a once-ever marker.
        _record_run_failure_alert(
            None, dest=dest, run_state=third_run, exc=OSError("same lock"), summary=summary
        )
        assert replay_fallback_alerts(con, dest) == 1
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight".alerts WHERE pipeline = ?',
            [dest.pipeline_name],
        ).fetchone()[0] == 2
    finally:
        con.close()


def test_fallback_rebuilds_episode_floor_when_journal_is_lost(tmp_path):
    import duckdb

    from cdc_flight import destination as destination_mod
    from cdc_flight.config import DestinationConfig
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import RunState
    from cdc_flight.destination_alerts import _fallback_alert_episode_path
    from cdc_flight.pipeline import _record_run_failure_alert

    dest = DestinationConfig(
        kind="duckdb",
        pipeline_name="fallback-journal-loss",
        duckdb_path=tmp_path / "dest.duckdb",
    )
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        summary = {"stop_reason": "destination_unavailable"}
        first_run = RunState.new(dest.pipeline_name)
        second_run = RunState.new(dest.pipeline_name)
        _record_run_failure_alert(
            None, dest=dest, run_state=first_run, exc=OSError("first lock"), summary=summary
        )
        assert destination_mod.replay_fallback_alerts(con, dest) == 1
        _fallback_alert_episode_path(dest).unlink()

        _record_run_failure_alert(
            None, dest=dest, run_state=second_run, exc=OSError("second lock"), summary=summary
        )
        rows = [
            json.loads(line)
            for line in destination_mod.fallback_alert_path(dest).read_text().splitlines()
        ]
        assert [row["episode_id"] for row in rows] == [1, 2]
        assert destination_mod.replay_fallback_alerts(con, dest) == 1
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight".alerts WHERE pipeline = ?',
            [dest.pipeline_name],
        ).fetchone()[0] == 2
    finally:
        con.close()
