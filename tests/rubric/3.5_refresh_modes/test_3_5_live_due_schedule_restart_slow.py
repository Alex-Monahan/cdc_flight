"""§3.5: the normal service owns due selection and survives scheduled work cuts."""

from __future__ import annotations

import contextlib
import json
import signal
import subprocess
import time
from datetime import UTC, datetime, timedelta

import duckdb
import psycopg
import pytest

from cdc_flight.backfill import BackfillCoordinator, RefreshPolicy, RefreshScheduler
from cdc_flight.control_schema import ensure_control_schema

TABLES = "customers,orders,audit_log"
PHASES = (
    "after_request_commit_before_signal",
    "after_signal_before_started",
    "after_md_commit_before_markProcessed",
)
FAULT_NTH = {
    "after_request_commit_before_signal": 1,
    "after_signal_before_started": 1,
    "after_md_commit_before_markProcessed": 2,
}


def _service_env(sandbox=None) -> dict[str, str]:
    env = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": TABLES,
        # Make the existing service-owner recheck a quick, deterministic poll for
        # the live mode-change edge. This is still the normal service entrypoint.
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "0.5",
        "CDC_SERVICE_LEASE_TTL": "100",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
        # The service remains intentionally quiet while the restart assertions
        # observe durable recovery. Keep the existing source-dark failure mode
        # enabled, but give this bounded observation enough room to finish.
        "CDC_SOURCE_DARK_SECONDS": "120",
        # The test observes source publication while the normal owner holds the
        # DuckDB file lock; keep the service alive through the bounded restart
        # observation, then stop it explicitly.
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "80",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_COMMIT_TIMEOUT": "10",
        "CDC_COMMIT_TIMEOUT": "10",
        "CDC_SERVICE_CLOSE_TIMEOUT": "2",
        "CDC_CLOSE_TIMEOUT": "2",
        "CDC_ENGINE_THREAD_TIMEOUT": "2",
        # Keep the stock incremental snapshot in several durable chunks so the
        # post-commit/pre-ack cut leaves a real loading run and a non-empty cursor
        # for the mode-change assertion. This is a connector property, not a
        # production row-size limit.
        "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": "1",
        "CDC_SNAPSHOT_CHUNK_EVENTS": "1",
    }
    if sandbox is not None:
        env["CDC_BACKFILL_LIVE_STATE_PATH"] = str(
            sandbox.dir / "backfill_live_state.jsonl"
        )
    return env


def _configure_due_policies(sandbox) -> None:
    """Configure only durable policy rows; do not request or admit work here."""
    due = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        ensure_control_schema(con, "_cdc_flight")
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        scheduler = RefreshScheduler(coordinator)
        scheduler.configure(
            RefreshPolicy(
                "app",
                "customers",
                mode="incremental",
                interval_seconds=3600,
                next_due_at=due,
            )
        )
        scheduler.configure(
            RefreshPolicy(
                "app",
                "orders",
                mode="full",
                interval_seconds=3600,
                next_due_at=due,
            )
        )
        scheduler.configure(
            RefreshPolicy(
                "app",
                "audit_log",
                mode="cdc",
                interval_seconds=3600,
                next_due_at=due,
            )
        )


def _live_terminal_backfill(sandbox) -> dict | None:
    path = sandbox.dir / "backfill_live_state.jsonl"
    if not path.exists():
        return None
    terminal = None
    for line in path.read_text(encoding="utf-8").splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            entry = json.loads(line)
            if (
                entry.get("table") == "app.customers"
                and entry.get("state") == "complete"
                and entry.get("notification_status") == "COMPLETED"
                and entry.get("table_state") == "complete"
            ):
                terminal = entry
    return terminal


def _change_policy_mode(sandbox, *, table: str, mode: str) -> None:
    """Make a durable policy edit while the previously admitted run is active."""
    # The mode edit is the transition under test; leave its next scheduled
    # acquisition in the future so the active incremental generation, not a new
    # full request, owns the restart. The next due poll will observe the new mode
    # after this preserved run has finished.
    due = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        ensure_control_schema(con, "_cdc_flight")
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        RefreshScheduler(coordinator).configure(
            RefreshPolicy(
                "app",
                table,
                mode=mode,
                interval_seconds=3600,
                next_due_at=due,
            )
        )


def _run_rows(sandbox) -> list[tuple]:
    return sandbox.duck_query(
        "SELECT run_id, source_table, state, effective_mode, "
        "last_processed_key_json, shadow_table "
        "FROM _cdc_flight.backfill_runs "
        "WHERE pipeline = ? ORDER BY source_table, created_at, run_id",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    )


def _policy_rows(sandbox) -> list[tuple]:
    return sandbox.duck_query(
        "SELECT source_table, mode, next_due_at "
        "FROM _cdc_flight.refresh_policy WHERE pipeline = ? ORDER BY source_table",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    )


def _signal_rows(sandbox) -> list[tuple]:
    return sandbox.pg_query(
        "SELECT id, type, data FROM app.cdc_flight_signal "
        "WHERE type = 'execute-snapshot' ORDER BY id"
    )


def _wait_for(predicate, *, sandbox, process=None, timeout: float = 180.0):
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if process is not None and process.poll() is not None:
            stdout, stderr = process.communicate()
            stdout_text = stdout or ""
            stderr_text = stderr or ""
            raise AssertionError(
                f"service exited before durable predicate: rc={process.returncode}\n"
                f"summary={sandbox.last_summary()}\n"
                f"stdout={stdout_text[-4000:]}\nstderr={stderr_text[-6000:]}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError("timed out waiting for the durable scheduled-work predicate")
        time.sleep(0.1)


def _stop_service(process) -> str:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
    stdout, stderr = process.communicate(timeout=5)
    stdout_text = stdout or ""
    stderr_text = stderr or ""
    return f"stdout={stdout_text[-4000:]}\nstderr={stderr_text[-6000:]}"


def _baseline_and_change_source(sandbox, label: str) -> set[str]:
    prior_run_ids = (
        {row[0] for row in _run_rows(sandbox)}
        if sandbox.duckdb_path.exists()
        else set()
    )
    (sandbox.dir / "backfill_live_state.jsonl").unlink(missing_ok=True)
    sandbox.reseed()
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=150,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": TABLES},
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline
    _configure_due_policies(sandbox)
    sandbox.sql(
        [
            f"UPDATE app.customers SET name = 'p35-{label}' WHERE id = 1",
            "UPDATE app.orders SET total_amount = 987.65 WHERE id = 1",
        ],
        one_transaction=True,
    )
    return prior_run_ids


def _new_run_rows(sandbox, prior_run_ids: set[str]) -> list[tuple]:
    return [row for row in _run_rows(sandbox) if row[0] not in prior_run_ids]


def _assert_exact_images_and_no_duplicates(sandbox) -> None:
    with psycopg.connect(sandbox.source.dsn) as source:
        source_customers = source.execute(
            "SELECT id, name, email, lifetime_value, is_active "
            "FROM app.customers ORDER BY id"
        ).fetchall()
        source_orders = source.execute(
            "SELECT id, customer_id, status, total_amount, currency, note "
            "FROM app.orders ORDER BY id"
        ).fetchall()
        source_audit = source.execute(
            "SELECT id, actor, action FROM app.audit_log ORDER BY id"
        ).fetchall()

    assert sandbox.duck_query(
        'SELECT id, name, email, lifetime_value, is_active '
        'FROM "cdc_raw"."cdcflight_app_customers" ORDER BY id'
    ) == source_customers
    assert sandbox.duck_query(
        'SELECT id, customer_id, status, total_amount, currency, note '
        'FROM "cdc_raw"."cdcflight_app_orders" ORDER BY id'
    ) == source_orders
    assert sandbox.duck_query(
        'SELECT id, actor, action '
        'FROM "cdc_raw"."cdcflight_app_audit_log" ORDER BY id'
    ) == source_audit

    assert sandbox.duck_query(
        'SELECT count(*), count(DISTINCT id) '
        'FROM "cdc_raw"."cdcflight_app_customers"'
    )[0][0] == sandbox.pg_query("SELECT count(*) FROM app.customers")[0][0]
    assert sandbox.duck_query(
        'SELECT count(*), count(DISTINCT id) '
        'FROM "cdc_raw"."cdcflight_app_orders"'
    )[0][0] == sandbox.pg_query("SELECT count(*) FROM app.orders")[0][0]


def _assert_completed_matrix_case(
    sandbox,
    *,
    run_ids: dict[str, str],
    summary: dict,
    require_full_summary: bool = True,
) -> None:
    current_run_ids = set(run_ids.values())
    rows = [row for row in _run_rows(sandbox) if row[0] in current_run_ids]
    by_table = {row[1]: row for row in rows}
    assert set(by_table) == {"customers", "orders"}, rows
    assert {by_table[table][0] for table in by_table} == set(run_ids.values())
    assert all(by_table[table][2] == "complete" for table in by_table), rows
    assert all(by_table[table][5] for table in by_table), rows
    assert by_table["orders"][3] == "full", rows
    assert sandbox.duck_query(
        "SELECT count(*) FROM _cdc_flight.backfill_runs "
        "WHERE pipeline = ? AND source_table = 'audit_log'",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    ) == [(0,)]
    assert sandbox.duck_query(
        "SELECT count(*) FROM _cdc_flight.backfill_signal_intents "
        "WHERE pipeline = ? AND state <> 'published'",
        [sandbox.env["CDC_PIPELINE_NAME"]],
    ) == [(0,)]
    signals = _signal_rows(sandbox)
    assert signals
    assert len({row[0] for row in signals}) == len(signals), signals
    if require_full_summary:
        assert "scheduled_full_refresh" in summary
        assert "app.orders" in summary["scheduled_full_refresh"]["resnapshot_swapped"]
    _assert_exact_images_and_no_duplicates(sandbox)


@pytest.mark.slow
def test_service_itself_selects_due_modes_and_recovers_restart_matrix(sandbox):
    """Due selection, recovery, and mode transition stay inside the service owner."""
    # First establish the positive due-selection evidence on an uninterrupted normal
    # service run. No request_tables()/admit_*() call exists in this test: the only
    # test-side writes are the three durable policy configurations.
    _baseline_and_change_source(sandbox, "clean")
    process = sandbox.spawn_service(capture=False, extra_env=_service_env(sandbox))
    try:
        # DuckDB's single owner keeps the destination file locked while the normal
        # service is alive. Observe only source publication in that interval; inspect
        # durable destination state after the owner exits.
        _wait_for(
            lambda: len(_signal_rows(sandbox)) == 1,
            sandbox=sandbox,
            process=process,
            timeout=120,
        )
        time.sleep(15)
        summary_text = _stop_service(process)
        summary = sandbox.last_summary()
        assert summary.get("ok") is True, summary_text
        polls = summary.get("scheduled_refresh_polls", [])
        assert any(
            set(poll["selected"]) == {"app.orders"}
            and poll["cdc_skipped"] == ["app.audit_log"]
            and poll["owner"] == "service-destination-owner"
            and poll["source_effect_route"] == "reconcile_signal_effects"
            and poll["source_signal_inserted_directly"] is False
            for poll in polls
        ), polls
        assert any(
            set(poll["selected"]) == {"app.customers"}
            and poll["published_signal_ids"]
            and poll["owner"] == "service-destination-owner"
            and poll["source_effect_route"] == "reconcile_signal_effects"
            and poll["source_signal_inserted_directly"] is False
            for poll in polls
        ), polls
        _assert_completed_matrix_case(
            sandbox,
            run_ids={row[1]: row[0] for row in _run_rows(sandbox)},
            summary=summary,
        )
    finally:
        if process.poll() is None:
            with contextlib.suppress(Exception):
                _stop_service(process)

    # Each cut starts from a fresh baseline, so the assertions below isolate the
    # durable phase rather than allowing an earlier case to satisfy a later one.
    for phase in PHASES:
        prior_run_ids = _baseline_and_change_source(
            sandbox, phase.replace("_", "-")[:24]
        )
        sandbox.clear_fired_fault()
        process = sandbox.spawn_service(
            capture=False,
            matrix_arm=True,
            extra_env={
                **_service_env(sandbox),
                "CDC_FAULT_INJECT": f"{phase}:{FAULT_NTH[phase]}",
            },
        )
        try:
            returncode = process.wait(timeout=180)
            output = _stop_service(process)
            assert returncode != 0, (phase, returncode, output)
            fired = sandbox.fired_fault()
            assert fired is not None and fired["point"] == phase, {
                "phase": phase,
                "fired": fired,
                "last_summary": sandbox.last_summary(),
                "runs": _new_run_rows(sandbox, prior_run_ids),
                "output": output,
            }
        finally:
            if process.poll() is None:
                with contextlib.suppress(Exception):
                    process.kill()
                    process.wait(timeout=30)
            with contextlib.suppress(Exception):
                process.communicate(timeout=5)

        try:
            before_restart = _wait_for(
                lambda: _new_run_rows(sandbox, prior_run_ids)
                if len(_new_run_rows(sandbox, prior_run_ids)) == 2
                else None,
                sandbox=sandbox,
                timeout=30,
            )
        except AssertionError as exc:
            raise AssertionError(
                f"{exc}; phase={phase}; runs={_run_rows(sandbox)}; "
                f"policies={_policy_rows(sandbox)}"
            ) from exc
        run_ids = {row[1]: row[0] for row in before_restart}
        by_table_before_restart = {row[1]: row for row in before_restart}
        assert by_table_before_restart["customers"][2] in {
            "requested",
            "preparing",
            "loading",
            "ready_to_swap",
        }, before_restart
        assert by_table_before_restart["orders"][2] in {
            "complete",
            "requested",
            "preparing",
            "loading",
            "ready_to_swap",
        }, before_restart
        assert all(row[5] for row in before_restart), before_restart
        assert len(_policy_rows(sandbox)) == 3
        assert all(row[2] is not None for row in _policy_rows(sandbox)), _policy_rows(sandbox)

        # The third cut leaves a committed incremental cursor before the source
        # acknowledgement. Change the durable policy while that run is still active;
        # the owner must preserve this run/cursor rather than create a replacement.
        cursor_before = {row[1]: row[4] for row in before_restart}
        if phase == "after_md_commit_before_markProcessed":
            assert cursor_before["customers"] is not None, before_restart
            _change_policy_mode(sandbox, table="customers", mode="full")
            changed = {row[0]: row[1] for row in _policy_rows(sandbox)}
            assert changed["customers"] == "full", changed

        # The process died with the service lease held. The normal external launcher
        # would wait for this bounded expiry before the next service invocation.
        time.sleep(106)
        process = sandbox.spawn_service(
            capture=False,
            extra_env={
                **_service_env(sandbox),
            },
        )
        try:
            _wait_for(lambda: len(_signal_rows(sandbox)) >= 1, sandbox=sandbox, process=process, timeout=120)
            # DuckDB's single-owner lock prevents a second connection from reading
            # run rows while the service is live. The production post-commit
            # sidecar is the existing live-stock observation boundary: it proves
            # the same committed terminal state without becoming a work trigger.
            _wait_for(
                lambda: _live_terminal_backfill(sandbox),
                sandbox=sandbox,
                process=process,
                timeout=75,
            )
            summary_text = _stop_service(process)
            summary = sandbox.last_summary()
            assert summary.get("ok") is True, (
                f"phase={phase}; ok={summary.get('ok')}; status={summary.get('status')}; "
                f"stop={summary.get('stop_reason')}; error_type={summary.get('error_type')}; "
                f"error={summary.get('error')}; cause={summary.get('error_cause_type')}; "
                f"original={summary.get('original_failure')}; "
                f"alert={summary.get('alerting_error')}; "
                f"slot_check={summary.get('slot_check')}; "
                f"polls={summary.get('scheduled_refresh_polls')}; "
                f"preserved={summary.get('interrupted_incremental_runs_preserved')}; "
                f"runs={_run_rows(sandbox)}; policies={_policy_rows(sandbox)}; "
                f"output={summary_text}"
            )
            recovered = {row[1]: row for row in _run_rows(sandbox)}
            assert {recovered[table][0] for table in recovered} == set(run_ids.values())
            if phase == "after_md_commit_before_markProcessed":
                before_key = json.loads(cursor_before["customers"])["id"]["value"]
                after_key = json.loads(recovered["customers"][4])["id"]["value"]
                assert after_key >= before_key
                assert recovered["customers"][0] == run_ids["customers"]
                assert any(
                    run_ids["customers"] in effect.get("run_ids", [])
                    and effect.get("kind") == "admission"
                    for effect in summary.get("backfill_signal_recoveries", [])
                ), summary.get("backfill_signal_recoveries")
            _assert_completed_matrix_case(
                sandbox,
                run_ids=run_ids,
                summary=summary,
                require_full_summary=False,
            )
        finally:
            if process.poll() is None:
                with contextlib.suppress(Exception):
                    _stop_service(process)
