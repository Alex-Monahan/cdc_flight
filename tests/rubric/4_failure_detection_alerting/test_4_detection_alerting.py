"""Phase-1 contracts for rubric 4 and rubric 6.

These tests intentionally describe the operator-facing contract: a bad run has a
non-zero process exit and one durable alert, while a healthy run remains quiet.  The
real-process cases are kept in the slow lane because they use the project PostgreSQL
cluster and two live Debezium processes.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import duckdb
import pytest

from cdc_flight.config import ReplicationConfig, RunConfig, SourceConfig
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.debezium_props import build_properties


def _alerts(box):
    return box.duck_query(
        "SELECT severity, code, message FROM _cdc_flight.alerts "
        "ORDER BY raised_at"
    )


def test_debezium_idle_heartbeat_and_socket_timeout_are_configured():
    props = build_properties(SourceConfig(), ReplicationConfig())

    assert props.get("heartbeat.interval.ms") == "5000"
    assert props.get("heartbeat.action.query") == (
        "SELECT pg_logical_emit_message(true, 'cdc_flight_heartbeat', '')"
    )
    # `driver.socketTimeout` is pgjdbc's bounded read timeout, in seconds.  It is
    # distinct from the Python sampler's query timeout and must reach the stock engine.
    assert props.get("driver.socketTimeout") == "60"
    assert props.get("driver.connectTimeout") == "5"


def test_every_run_wait_budget_is_explicit_and_positive():
    cfg = RunConfig()
    for name in (
        "max_seconds",
        "close_timeout",
        "source_dark_seconds",
        "commit_timeout",
        "jdbc_socket_timeout_seconds",
        "engine_start_timeout",
    ):
        value = getattr(cfg, name, None)
        assert isinstance(value, (int, float)) and value > 0, (
            f"RunConfig.{name} must be a positive bound for every production wait"
        )


def test_control_schema_has_durable_operator_log_with_replication_lag(tmp_path):
    path = tmp_path / "observability.duckdb"
    con = duckdb.connect(str(path))
    try:
        ensure_control_schema(con)
        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'run_logs'"
            ).fetchall()
        }
        assert {
            "level",
            "event",
            "message",
            "replication_lag_bytes",
            "slot_confirmed_flush_lsn",
            "context",
        } <= columns
    finally:
        con.close()


def test_inventory_has_no_unresolved_recovery_cases():
    adr = (
        Path(__file__).resolve().parents[3]
        / "docs"
        / "adr"
        / "0001-transactional-applier.md"
    ).read_text()
    block = adr[adr.index("#### A51.2 —") : adr.index("**The counts, parsed from")]
    rows = []
    for line in block.splitlines():
        if not line.startswith("| ") or line.startswith("| # |") or set(line) <= set("|- "):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        assert len(cells) == 7, line
        match = re.search(r"\b(AUTO|MANUAL|UNDEFINED)\b", cells[-1])
        assert match, line
        rows.append(match.group(1))
    classes = {name: rows.count(name) for name in ("AUTO", "MANUAL", "UNDEFINED")}
    # The branch resolves every previously undefined row into an explicit manual
    # boundary where no safe automatic repair exists.  The existing rubric inventory
    # test deliberately keeps the >2-manual ceiling visible; this test prevents an
    # unresolved case from being silently counted as either class.
    assert classes == {"AUTO": 47, "MANUAL": 23, "UNDEFINED": 0}, classes


@pytest.mark.slow
def test_dropped_slot_can_be_refused_loudly_and_is_alerted(sandbox):
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0, first

    sandbox.sql(
        "INSERT INTO app.customers (name, email) VALUES "
        "('slot-loss-detection', 'slot-loss@example.com')"
    )
    sandbox.drop_slot()
    # The production default is deliberate automatic recovery: the slot is recreated
    # only inside the journaled full re-snapshot path.  This proof selects the explicit
    # refuse mode so the failure contract is exercised as well; no run may claim that
    # the dropped slot was harmless.
    failed = sandbox.run(
        max_seconds=120,
        expect_success=False,
        extra_env={"CDC_RESNAPSHOT": "0"},
    )

    assert failed["returncode"] != 0, failed
    assert failed.get("ok") is False, failed
    alerts = _alerts(sandbox)
    assert any(row[1] in {"slot_missing", "slot_recreated"} for row in alerts), alerts
    assert [row[1] for row in alerts if row[0] == "critical"] == ["slot_missing"], alerts


@pytest.mark.slow
def test_corrupt_durable_offset_is_not_a_success_and_is_alerted(sandbox):
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0, first

    sandbox.duck_write(
        "UPDATE _cdc_flight.debezium_offsets SET resume_json = ? "
        "WHERE pipeline = ?",
        ["{this is not a resume point", first.get("pipeline") or sandbox.env["CDC_PIPELINE_NAME"]],
    )
    failed = sandbox.run(max_seconds=60, expect_success=False)

    assert failed["returncode"] != 0, failed
    assert failed.get("ok") is False, failed
    alerts = _alerts(sandbox)
    assert any(row[1] == "offset_unusable" for row in alerts), alerts

    # Re-reading the same malformed durable row is the same occurrence, not a new
    # incident.  The second real process must still fail, but it may not multiply the
    # operator alert.
    failed_again = sandbox.run(max_seconds=60, expect_success=False)
    assert failed_again["returncode"] != 0, failed_again
    repeated = [row for row in _alerts(sandbox) if row[1] == "offset_unusable"]
    assert len(repeated) == 1, repeated


@pytest.mark.slow
def test_idle_source_heartbeat_advances_confirmed_flush_lsn(sandbox):
    """A quiet real source still receives the stock Debezium heartbeat action.

    The action is a transactional logical message, not an application-table write;
    this checks both properties against PostgreSQL and observes the running engine's
    effective configuration through its durable summary.
    """
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0, first
    before_rows = sandbox.pg_query("SELECT count(*) FROM app.customers")[0][0]
    before = sandbox.pg_query(
        "SELECT (confirmed_flush_lsn - '0/0')::bigint "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (sandbox.slot,),
    )[0][0]

    process = sandbox.spawn(
        max_seconds=14,
        idle_seconds=10,
        extra_env={
            "CDC_COMPLETION_WATERMARK": "0",
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_IDLE_SECONDS": "10",
        },
    )
    try:
        sandbox.wait_for_slot_active(process=process, timeout=30)
        deadline = time.monotonic() + 9
        observed = before
        while time.monotonic() < deadline:
            row = sandbox.pg_query(
                "SELECT (confirmed_flush_lsn - '0/0')::bigint "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (sandbox.slot,),
            )
            if row and row[0][0] is not None:
                observed = row[0][0]
                if observed > before:
                    break
            time.sleep(0.25)
        returncode = process.wait(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=20)

    summary = sandbox.last_summary()
    assert returncode == 0, summary
    assert observed > before, (before, observed, summary)
    assert sandbox.pg_query("SELECT count(*) FROM app.customers")[0][0] == before_rows
    effective = summary.get("engine_effective_configuration", {})
    assert effective["heartbeat.interval.ms"] == 5000, effective
    assert effective["heartbeat.action.query"].startswith(
        "SELECT pg_logical_emit_message(true"
    ), effective
    assert summary.get("slot_confirmed_pos", 0) >= observed, summary


@pytest.mark.slow
def test_different_slots_cannot_write_the_same_destination(sandbox):
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0, first

    peer_slot = f"{sandbox.slot[:53]}_p"
    peer_state = f"{sandbox.env['CDC_STATE_DIR']}_peer"
    peer_env = {
        "CDC_SLOT_NAME": peer_slot,
        "CDC_STATE_DIR": peer_state,
        "CDC_PIPELINES_DIR": f"{peer_state}/dlt_pipelines",
        "CDC_PIPELINE_NAME": f"{sandbox.env['CDC_PIPELINE_NAME']}_peer",
        "CDC_COMPLETION_WATERMARK": "0",
    }
    one = sandbox.spawn(
        max_seconds=35,
        idle_seconds=20,
        extra_env={"CDC_COMPLETION_WATERMARK": "0"},
        capture=True,
    )
    two = None
    try:
        sandbox.wait_for_slot_active(process=one, timeout=30)
        two = sandbox.spawn(
            max_seconds=35,
            idle_seconds=20,
            extra_env=peer_env,
            capture=True,
        )
        # Both are real processes.  Give the contender enough time to reach the shared
        # destination lease before collecting either result.
        time.sleep(2)
        rc_two = two.wait(timeout=90)
        rc_one = one.wait(timeout=90)
        out_two = two.stdout.read() if two.stdout is not None else ""
        out_one = one.stdout.read() if one.stdout is not None else ""
    finally:
        for proc in (one, two):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=20)

    # A local DuckDB file is itself single-writer locked before a second process can
    # reach `_cdc_flight.lease`; the process still fails closed, and the stderr is the
    # durable run evidence.  The MotherDuck case below exercises the same lease path
    # where two server connections can both reach the shared control row and verifies
    # the critical alert row.
    assert rc_two != 0, (rc_one, rc_two, out_one[-4000:], out_two[-4000:])
    assert "lock" in out_two.lower(), out_two[-4000:]


@pytest.mark.motherduck
@pytest.mark.slow
def test_real_motherduck_concurrent_slots_persist_one_alert(sandbox, motherduck_case):
    """Two real processes and two slots contend for one physical MD destination."""
    sandbox.reseed()
    md_env = {
        "CDC_MD_DATABASE": motherduck_case["database"],
        "CDC_DATASET": motherduck_case["dataset"],
        "CDC_CONTROL_SCHEMA": motherduck_case["control_schema"],
        "MOTHERDUCK_TOKEN": motherduck_case["token"],
        "motherduck_token": motherduck_case["token"],
    }
    first = sandbox.run(
        destination="motherduck",
        reset_state=True,
        max_seconds=150,
        extra_env=md_env,
    )
    assert first["returncode"] == 0, first

    peer_state = f"{sandbox.env['CDC_STATE_DIR']}_md_peer"
    peer_slot = f"{sandbox.slot[:53]}_m"
    peer_env = {
        **md_env,
        "CDC_SLOT_NAME": peer_slot,
        "CDC_STATE_DIR": peer_state,
        "CDC_PIPELINES_DIR": f"{peer_state}/dlt_pipelines",
        "CDC_PIPELINE_NAME": f"{sandbox.env['CDC_PIPELINE_NAME']}_md_peer",
        "CDC_COMPLETION_WATERMARK": "0",
    }
    one = sandbox.spawn(
        destination="motherduck",
        max_seconds=14,
        idle_seconds=10,
        extra_env={**md_env, "CDC_COMPLETION_WATERMARK": "0"},
        capture=True,
    )
    two = None
    try:
        sandbox.wait_for_slot_active(process=one, timeout=30)
        two = sandbox.spawn(
            destination="motherduck",
            max_seconds=30,
            idle_seconds=10,
            extra_env=peer_env,
            capture=True,
        )
        rc_two = two.wait(timeout=60)
        out_two = two.stdout.read() if two.stdout is not None else ""
        rc_one = one.wait(timeout=60)
        out_one = one.stdout.read() if one.stdout is not None else ""
    finally:
        for proc in (one, two):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=20)

    md = duckdb.connect(
        f"md:{motherduck_case['database']}?motherduck_token={motherduck_case['token']}"
    )
    try:
        alerts = md.execute(
            f'SELECT severity, code, message FROM "{motherduck_case["control_schema"]}".alerts '
            "WHERE code = ? ORDER BY raised_at",
            ["concurrent_destination_run"],
        ).fetchall()
    finally:
        md.close()
    assert rc_two != 0, (rc_one, rc_two, out_one[-3000:], out_two[-3000:])
    assert len(alerts) == 1, alerts
    assert any(
        row[0] == "critical" and row[1] == "concurrent_destination_run"
        for row in alerts
    ), (alerts, out_two[-3000:])


@pytest.mark.slow
def test_healthy_run_has_lagged_operator_log_without_failure_alert(sandbox):
    sandbox.reseed()
    summary = sandbox.run(reset_state=True, max_seconds=150)
    assert summary["returncode"] == 0, summary
    assert summary.get("ok") is True, summary
    effective = summary.get("engine_effective_configuration", {})
    assert effective.get("heartbeat.interval.ms") == 5000, effective
    assert effective.get("heartbeat.action.query", "").startswith(
        "SELECT pg_logical_emit_message(true"
    ), effective
    assert effective.get("driver.socketTimeout") == "60", effective
    logs = sandbox.duck_query(
        "SELECT count(*), max(replication_lag_bytes) FROM _cdc_flight.run_logs "
        "WHERE pipeline = ?",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    )
    assert logs[0][0] > 0, logs
    assert logs[0][1] is not None, logs
    # This module intentionally reuses one sandbox to keep the real-process proofs
    # affordable. Historical failures from the dropped-slot/offset tests remain
    # durable operator evidence, but they are not alerts for this healthy runner.
    current_runner = summary["runner_id"]
    failure_alerts = []
    for severity, code, context in sandbox.duck_query(
        "SELECT severity, code, context FROM _cdc_flight.alerts "
        "WHERE pipeline = ? ORDER BY raised_at",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    ):
        payload = json.loads(context or "{}")
        if (
            payload.get("runner_id") == current_runner
            and severity in {"error", "critical"}
            and code != "operator_reset"
        ):
            failure_alerts.append((severity, code))
    assert failure_alerts == [], failure_alerts
