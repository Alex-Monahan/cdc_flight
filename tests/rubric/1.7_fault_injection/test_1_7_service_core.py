"""Real-process proofs for the Steps 1-3 service boundary."""

from __future__ import annotations

import contextlib
import inspect
import os
import signal
import time

import duckdb
import pytest
from support.fixtures import Sandbox
from support.motherduck_probe import connect as motherduck_connect

from cdc_flight import faults
from cdc_flight.discovery_coordinator import LiveDiscoveryCoordinator

# Plan §4.4 is an inventory, not a claim that one generic crash anchor covers every
# lifetime edge.  Keep the mapping explicit so a newly reachable service cut cannot
# disappear into the batch-only matrix.
SERVICE_CUT_COVERAGE = {
    "single-process startup admission": ("service_startup",),
    "callback before complete unit": ("service_callback_midstream",),
    "complete unit staged before destination commit": ("service_before_md_commit",),
    "destination commit before acknowledgement": (
        "service_after_md_commit_before_ack",
    ),
    "one acknowledgement before batch finish": ("service_after_one_ack_before_finish",),
    "open PostgreSQL transaction across callbacks": ("service_pg_transaction_open",),
    "incremental backfill transaction edges": (
        "incremental_chunk_before_shadow_write",
        "incremental_chunk_after_shadow_write_before_progress",
        "incremental_chunk_after_progress_before_md_commit",
        "after_md_commit_before_markProcessed",
        "after_markProcessed_before_markBatchFinished",
        "after_ack_before_next_poll",
    ),
    "physical lease acquire/renew/release": (
        "service_lease_acquire",
        "service_lease_renewal",
        "service_lease_release",
    ),
    "heartbeat/run-log/source-health writes": (
        "service_heartbeat_write",
        "service_run_log_write",
        "service_source_health_write",
    ),
}

UNREACHABLE_SERVICE_CUTS = {
    "scheduler request": (
        "no service scheduler thread issues a second source signal; incremental "
        "requests are the existing callback-owned stock path"
    ),
    "full-refresh handoff": (
        "LiveDiscoveryCoordinator requires service_context is None for a live "
        "handoff; service mode has only startup full-refresh consumption"
    ),
    "completion/watermark/shutdown terminal markers": (
        "these are finite batch/drain markers, not reachable mid-stream service cuts; "
        "the shared batch matrix retains them"
    ),
}


def _wait_for_service(box: Sandbox, process, *, timeout: float = 45.0) -> int:
    """Use PostgreSQL liveness while the one service process owns the destination."""
    box.wait_for_slot_active(process=process, timeout=timeout)
    return process.pid


def _stop(process, *, expected: set[int] | None = None) -> int:
    expected = expected or {0}
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    returncode = process.wait(timeout=90)
    if returncode not in expected:
        output = _service_process_output(process)
        raise AssertionError(f"returncode={returncode}\n{output[-12000:]}")
    return returncode


@pytest.mark.slow
def test_service_real_process_drains_and_releases_one_epoch(
    tmp_path_factory, postgres_cluster
):
    box = Sandbox(
        "service_core_drain",
        tmp_path_factory.mktemp("sbx_service_core_drain"),
        postgres_cluster,
    )
    process = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        process = box.spawn_service(
                matrix_arm=True,
                capture=True,
                extra_env={
                "CDC_SERVICE_ID": "service-core-drain",
                "CDC_SERVICE_LEASE_TTL": "120",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "10",
                "CDC_SERVICE_COMMIT_TIMEOUT": "30",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "45",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
                # Keep the graceful-drain proof finite while allowing the stock
                # JVM close to complete under the slow lane.
                "CDC_CLOSE_TIMEOUT": "60",
                "CDC_ENGINE_THREAD_TIMEOUT": "60",
            },
            )
        service_pid = _wait_for_service(box, process)
        assert service_pid == process.pid
        box.wait_for_slot_active(process=process, timeout=45)
        box.sql(
            "INSERT INTO app.customers (name, email) "
            "VALUES ('service-core-row', 'service-core@example.com')"
        )
        # DuckDB is intentionally not queried while the service process owns its
        # process-level file lock.  Allow the live connector to consume the row,
        # then assert the durable result after the bounded drain closes the handle.
        time.sleep(5)

        assert _stop(process) == 0
        process = None
        released = box.duck_query(
            "SELECT state, fencing_epoch, service_id "
            "FROM _cdc_flight.lease"
        )
        assert released == [("released", 1, "service-core-drain")]
        assert box.scalar(
            'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
            "WHERE name = ?",
            ["service-core-row"],
        ) == 1
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_service_destination_write_hang_is_bounded_before_commit_ack(
    tmp_path_factory, postgres_cluster
):
    """A real Flight blocked in pre-COMMIT destination I/O is hard-fenced."""
    box = Sandbox(
        "service_core_destination_hang",
        tmp_path_factory.mktemp("sbx_service_core_destination_hang"),
        postgres_cluster,
    )
    process = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        process = box.spawn_service(
            matrix_arm=True,
            capture=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-destination-hang",
                "CDC_SERVICE_LEASE_TTL": "12",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_COMMIT_TIMEOUT": "2",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "6",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "3",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "2",
                "CDC_FAULT_INJECT": "destination_hang:1",
                "CDC_FAULT_HANG_PHASE": "pre_commit",
                "CDC_FAULT_HANG_SECONDS": "600",
                "CDC_COMMIT_TIMEOUT": "2",
            },
        )
        box.wait_for_slot_active(process=process, timeout=45)
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('service-destination-hang', 'service-destination-hang@example.com')"
        )
        fired_at = None
        deadline = time.monotonic() + 20
        while process.poll() is None and time.monotonic() < deadline:
            fired = box.fired_fault()
            if fired and fired["point"] == "destination_hang":
                fired_at = time.monotonic()
                break
            time.sleep(0.05)
        assert fired_at is not None, box.fired_fault()
        returncode = process.wait(timeout=20)
        assert returncode != 0
        assert time.monotonic() - fired_at < 8, (
            "the pre-COMMIT destination hang outlived its 2-second service bound"
        )
        fired = box.fired_fault()
        assert fired and fired["action"].startswith("hang:600.0:pre_commit"), fired
        assert box.duck_query(
            "SELECT state, service_id FROM _cdc_flight.lease"
        ) == [("held", "service-core-destination-hang")]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_service_rechecks_and_fails_closed_after_mid_life_slot_drop(
    tmp_path_factory, postgres_cluster
):
    """Dropping the live source slot is detected after startup, not on next run."""
    box = Sandbox(
        "service_core_slot_drop",
        tmp_path_factory.mktemp("sbx_service_core_slot_drop"),
        postgres_cluster,
    )
    process = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        process = box.spawn_service(
            capture=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-slot-drop",
                "CDC_SERVICE_LEASE_TTL": "12",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_COMMIT_TIMEOUT": "2",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "6",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "3",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "2",
                "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "0.5",
            },
        )
        box.wait_for_slot_active(process=process, timeout=45)
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('service-slot-drop', 'service-slot-drop@example.com')"
        )
        box.kill_walsender()
        drop_deadline = time.monotonic() + 15
        while True:
            try:
                box.pg_query(
                    "SELECT pg_drop_replication_slot(%s)",
                    (box.slot,),
                )
                break
            except Exception:
                if time.monotonic() >= drop_deadline:
                    raise
                box.kill_walsender()
                time.sleep(0.1)
        returncode = process.wait(timeout=30)
        output = ""
        if process.stdout is not None:
            output = process.stdout.read() or ""
        if process.stderr is not None:
            output += process.stderr.read() or ""
        assert returncode != 0, output
        assert "disappeared during streaming" in output.lower(), output[-5000:]
        assert box.duck_query(
            "SELECT state, service_id FROM _cdc_flight.lease"
        ) == [("released", "service-core-slot-drop")]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_quiet_connected_holder_survives_watchdog_and_successors_stand_down(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """A real active stock slot, with no changes, is not a hung service."""
    box = Sandbox(
        "service_quiet_connected",
        tmp_path_factory.mktemp("sbx_service_quiet_connected"),
        postgres_cluster,
    )
    holder = None
    successors = []
    case = motherduck_case
    service_env = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_ID": "service-quiet-connected",
        "CDC_SERVICE_LEASE_TTL": "75",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "45",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_SOURCE_HEALTH_STALE_SECONDS": "15",
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "5",
        "CDC_SERVICE_CLOSE_TIMEOUT": "20",
        "CDC_CLOSE_TIMEOUT": "20",
        "CDC_ENGINE_THREAD_TIMEOUT": "20",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    try:
        box.reseed()
        box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=service_env,
            max_seconds=180,
            timeout=300,
        )
        holder = box.spawn_service(
            destination="motherduck", capture=True, extra_env=service_env
        )
        box.wait_for_slot_active(process=holder, timeout=45)

        # This crosses both the 45 s local stall threshold and the 50 s
        # stall+grace boundary. The only thing keeping the holder healthy is
        # the fresh active/quiet slot observation and its lease CAS.
        time.sleep(52)
        assert holder.poll() is None, _service_process_output(holder)

        for _ in range(2):
            successor = box.spawn_service(
                destination="motherduck", capture=True, extra_env=service_env
            )
            successors.append(successor)
            assert successor.wait(timeout=20) == 0, _service_process_output(successor)
            summary = box.last_summary()
            assert summary.get("stop_reason") == "stand_down", summary
            assert summary.get("status") == "SUCCEEDED", summary
            assert holder.poll() is None, _service_process_output(holder)

        assert _stop(holder) == 0
        holder = None
    finally:
        for process in successors:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=30)
        if holder is not None and holder.poll() is None:
            holder.kill()
            holder.wait(timeout=30)
        if holder is not None:
            holder.communicate(timeout=5)
        for process in successors:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_ignored_signal_does_not_refresh_liveness(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """A signal row proves source ordering, not Flight delivery progress."""
    box = Sandbox(
        "service_ignored_signal_liveness",
        tmp_path_factory.mktemp("sbx_service_ignored_signal_liveness"),
        postgres_cluster,
    )
    process = None
    case = motherduck_case
    service_env = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_ID": "service-ignored-signal-liveness",
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "25",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_SOURCE_HEALTH_STALE_SECONDS": "8",
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "1",
        "CDC_SOURCE_DARK_SECONDS": "8",
        "CDC_SERVICE_COMMIT_TIMEOUT": "10",
        "CDC_SERVICE_CLOSE_TIMEOUT": "15",
        "CDC_CLOSE_TIMEOUT": "15",
        "CDC_ENGINE_THREAD_TIMEOUT": "15",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    try:
        box.reseed()
        baseline = box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=service_env,
            max_seconds=180,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline

        process = box.spawn_service(
            destination="motherduck",
            capture=True,
            extra_env=service_env,
        )
        try:
            box.wait_for_slot_active(process=process, timeout=45)
        except AssertionError:
            print("ignored-signal startup output:", _service_process_output(process))
            raise
        box.sql(
            "INSERT INTO app.cdc_flight_signal (id, type, data) VALUES "
            "('service-ignored-signal', 'execute-snapshot', "
            "'{\"data-collections\":[],\"type\":\"incremental\"}')"
        )
        returncode = process.wait(timeout=45)
        output = _service_process_output(process)
        summary = box.last_summary()
        lease = md_query(
            f'SELECT state, fencing_epoch, service_id FROM "{case["control_schema"]}"."lease"'
        )
        alerts = md_query(
            f'SELECT severity, code FROM "{case["control_schema"]}"."alerts" '
            "WHERE pipeline = ? ORDER BY raised_at",
            [box.env["CDC_PIPELINE_NAME"]],
        )
        commit_log = md_query(
            f'SELECT commit_id, event_count, unit_count, last_lsn '
            f'FROM "{case["control_schema"]}"."commit_log" '
            "WHERE pipeline = ? ORDER BY commit_id",
            [box.env["CDC_PIPELINE_NAME"]],
        )
        print(
            "ignored-signal probe:",
            {
                "returncode": returncode,
                "summary": summary,
                "lease": lease,
                "alerts": alerts,
                "commit_log": commit_log,
            },
        )

        assert returncode != 0, output
        assert summary.get("stop_reason") == "source_dark", (summary, output)
        assert summary.get("records", 0) >= 1, (summary, output)
        assert summary.get("skipped", 0) >= 1, (summary, output)
        assert summary.get("data_batches") == 0, (summary, output)
        assert summary.get("data_commit_groups") == 0, (summary, output)
        assert summary.get("applied_events") >= 1, (summary, output)
        signal_commits = [
            row for row in commit_log if row[1] == 1 and row[3] is not None
        ]
        assert signal_commits, commit_log
        heartbeat = summary.get("service_heartbeat", {})
        assert heartbeat.get("engine_callback_age_sec") is None, summary
        assert heartbeat.get("engine_commit_age_sec") is None, summary
        assert heartbeat.get("engine_ack_age_sec") is None, summary
        assert heartbeat.get("engine_progress_age_sec") is None, summary
        assert lease == [("released", 1, "service-ignored-signal-liveness")]
        assert any(
            severity == "critical" and code == "source_dark"
            for severity, code in alerts
        ), alerts
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_quarantined_table_activity_keeps_lease_alive(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """A quarantined application row is data evidence, unlike a signal row."""
    box = Sandbox(
        "service_quarantined_table_liveness",
        tmp_path_factory.mktemp("sbx_service_quarantined_table_liveness"),
        postgres_cluster,
    )
    process = None
    case = motherduck_case
    service_env = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_TABLES": "service_quarantined",
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_CATALOG_POLL_SECONDS": "1",
        "CDC_SERVICE_ID": "service-quarantined-table-liveness",
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "15",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_SOURCE_HEALTH_STALE_SECONDS": "8",
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "1",
        "CDC_SOURCE_DARK_SECONDS": "8",
        "CDC_SERVICE_COMMIT_TIMEOUT": "10",
        "CDC_SERVICE_CLOSE_TIMEOUT": "15",
        "CDC_CLOSE_TIMEOUT": "15",
        "CDC_ENGINE_THREAD_TIMEOUT": "15",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    try:
        box.reseed()
        box.sql(
            [
                "CREATE TABLE app.service_quarantined "
                "(id integer PRIMARY KEY, name text)",
                "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.service_quarantined",
            ],
            one_transaction=True,
        )
        baseline = box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=service_env,
            max_seconds=180,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline

        box.sql(
            [
                "ALTER TABLE app.service_quarantined ADD COLUMN v_box box "
                "DEFAULT '((0,0),(1,1))'::box",
                "INSERT INTO app.service_quarantined (id, name) "
                "VALUES (1, 'quarantined-row')",
            ],
            one_transaction=True,
        )
        quarantine_runs = []
        for iteration in range(4):
            quarantine_runs.append(
                box.run(
                    destination="motherduck",
                    extra_env=service_env,
                    max_seconds=20,
                    timeout=60,
                    expect_success=False,
                    min_records=1 if iteration == 0 else 0,
                )
            )
        assert all(run["ok"] is False for run in quarantine_runs), quarantine_runs
        assert md_query(
            f'SELECT state FROM "{case["control_schema"]}"."schema_refusals" '
            "WHERE source_table = 'service_quarantined'"
        ) == [("quarantined",)]

        process = box.spawn_service(
            destination="motherduck",
            capture=True,
            extra_env=service_env,
        )
        box.wait_for_slot_active(process=process, timeout=45)
        box.sql(
            "INSERT INTO app.service_quarantined (id, name) "
            "VALUES (2, 'quarantined-row-2')"
        )

        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            assert process.poll() is None, _service_process_output(process)
            if md_query(
                f'SELECT state FROM "{case["control_schema"]}"."schema_refusals" '
                "WHERE source_table = 'service_quarantined'"
            ) == [("quarantined",)]:
                break
            time.sleep(0.5)
        else:
            raise AssertionError("the live row never reached durable quarantine")

        time.sleep(12)
        assert process.poll() is None, _service_process_output(process)
        lease = md_query(
            f'SELECT state, fencing_epoch, service_id FROM "{case["control_schema"]}"."lease"'
        )
        # Service leases use ``held`` while an admitted process owns the epoch;
        # ``active`` is the finite-run state and would misread a healthy holder.
        assert lease == [("held", 1, "service-quarantined-table-liveness")]
        process.send_signal(signal.SIGTERM)
        returncode = process.wait(timeout=60)
        output = _service_process_output(process)
        summary = box.last_summary()
        alerts = md_query(
            f'SELECT severity, code FROM "{case["control_schema"]}"."alerts" '
            "WHERE pipeline = ? ORDER BY raised_at",
            [box.env["CDC_PIPELINE_NAME"]],
        )
        lease_after = md_query(
            f'SELECT state, fencing_epoch, service_id FROM "{case["control_schema"]}"."lease"'
        )
        print(
            "quarantined-table probe:",
            {
                "returncode": returncode,
                "summary": summary,
                "lease_while_running": lease,
                "lease_after": lease_after,
                "alerts": alerts,
            },
        )

        # The controlled SIGTERM is not a classification probe. The liveness
        # property was established before teardown: the service was still
        # running with its lease held after the quarantined row, rather than
        # being killed by source-dark or the no-progress watchdog. Teardown may
        # separately observe source_dark while the engine is closing; that is a
        # different operator situation from engine_error and is not folded into
        # the quarantine-liveness claim.
        assert returncode != -9, (summary, output)
        assert summary.get("error_cause_type") == "SchemaEvolutionRefused", (
            summary,
            output,
        )
        assert summary.get("service_heartbeat", {}).get("stalled") is False, summary
        assert summary.get("quarantined_events", 0) > 0, (summary, output)
        assert summary.get("data_batches", 0) >= 1, (summary, output)
        assert lease_after == [("released", 1, "service-quarantined-table-liveness")]
        assert any(code == "schema_table_quarantined" for _, code in alerts), alerts
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_excluded_capture_route_fails_closed_and_releases_lease(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """Publication membership must overlap the connector route, not merely exist."""
    box = Sandbox(
        "service_excluded_capture_route",
        tmp_path_factory.mktemp("sbx_service_excluded_capture_route"),
        postgres_cluster,
    )
    process = None
    case = motherduck_case
    service_env = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "orders",
        "CDC_SERVICE_ID": "service-excluded-capture-route",
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
        # Give the invalid-route engine enough bounded time to publish its
        # source-specific diagnosis before the local no-progress watchdog can
        # compete for the failure path and its alert sink.
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "30",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_SOURCE_HEALTH_STALE_SECONDS": "20",
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "1",
        # Let the source-specific diagnosis win before the local no-progress
        # watchdog's 30-second symptom path.
        "CDC_SOURCE_DARK_SECONDS": "20",
        "CDC_SERVICE_COMMIT_TIMEOUT": "5",
        "CDC_SERVICE_CLOSE_TIMEOUT": "15",
        "CDC_CLOSE_TIMEOUT": "15",
        "CDC_ENGINE_THREAD_TIMEOUT": "15",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    try:
        box.reseed()
        baseline = box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=service_env,
            max_seconds=180,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline

        # Leave one unrelated table in the publication. The membership witness is
        # intentionally true; the configured-route witness must be false because
        # this connector is configured for app.orders only.
        box.sql(
            "ALTER PUBLICATION cdc_flight_pub DROP TABLE "
            "app.orders, app.sensor_readings, app.documents, app.wide_types, "
            "app.audit_log, app.cdc_flight_signal"
        )
        assert box.pg_query(
            "SELECT schemaname, tablename FROM pg_publication_tables "
            "WHERE pubname = 'cdc_flight_pub'"
        ) == [("app", "customers")]

        process = box.spawn_service(
            destination="motherduck",
            capture=True,
            extra_env=service_env,
        )
        box.wait_for_slot_active(process=process, timeout=45)
        # This row is real source activity, but is outside table.include.list and
        # therefore must not be mistaken for delivery to this Flight.
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('service-excluded-route', 'service-excluded-route@example.com')"
        )
        returncode = process.wait(timeout=45)
        output = _service_process_output(process)
        summary = box.last_summary()

        lease = md_query(
            f'SELECT state, fencing_epoch, service_id FROM "{case["control_schema"]}"."lease"'
        )
        destination_customers = md_query(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [case["dataset"], "cdcflight_app_customers"],
        )
        source_dark_alerts = md_query(
            f'SELECT severity, code FROM "{case["control_schema"]}"."alerts" '
            "WHERE pipeline = ? AND code = 'source_dark'",
            [box.env["CDC_PIPELINE_NAME"]],
        )

        # Safety is independent of the operator classification: this route must
        # fail closed and release ownership even if a future diagnosis assertion
        # regresses.
        assert returncode != 0, output
        assert lease == [("released", 1, "service-excluded-capture-route")]
        assert destination_customers == [(0,)]
        assert summary.get("data_batches") == 0, (summary, output)
        assert summary.get("data_commit_groups") == 0, (summary, output)

        # Diagnosis remains exact. `source_dark` and `engine_error` describe
        # different operator situations and are never accepted interchangeably.
        assert summary.get("stop_reason") == "source_dark", (summary, output)
        assert summary.get("source_publication_has_tables") is True, (summary, output)
        assert summary.get("source_publication_has_configured_tables") is False, (
            summary,
            output,
        )
        heartbeat = summary.get("service_heartbeat", {})
        assert heartbeat.get("engine_callback_age_sec") is None, (summary, output)
        assert heartbeat.get("engine_commit_age_sec") is None, (summary, output)
        assert heartbeat.get("engine_ack_age_sec") is None, (summary, output)
        assert summary.get("source_dark_detected_after_sec") is not None, (
            summary,
            output,
        )
        assert summary["source_dark_detected_after_sec"] < 25, (summary, output)
        assert source_dark_alerts == [("critical", "source_dark")]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_stock_walsender_retry_backoff_fails_closed_alerts_and_successor_takes_over(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """Kill the real stock walsender repeatedly while Debezium's Java thread retries."""
    box = Sandbox(
        "service_walsender_retry_backoff",
        tmp_path_factory.mktemp("sbx_service_walsender_retry_backoff"),
        postgres_cluster,
    )
    holder = None
    successor = None
    case = motherduck_case
    service_env = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_ID": "service-walsender-retry",
        "CDC_SERVICE_LEASE_TTL": "75",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "45",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_SOURCE_HEALTH_STALE_SECONDS": "15",
        "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "30",
        # Keep this real stock retry proof bounded in the slow lane while the
        # quiet-direction test separately crosses the production 45/50 s watchdog.
        "CDC_SOURCE_DARK_SECONDS": "20",
        "CDC_SERVICE_CLOSE_TIMEOUT": "20",
        "CDC_CLOSE_TIMEOUT": "20",
        "CDC_ENGINE_THREAD_TIMEOUT": "20",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    try:
        box.reseed()
        box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=service_env,
            max_seconds=180,
            timeout=300,
        )
        holder = box.spawn_service(
            destination="motherduck", capture=True, extra_env=service_env
        )
        box.wait_for_slot_active(process=holder, timeout=45)
        # Give the independent sampler time to record the healthy stock
        # walsender before the first kill.  Without this witness the run would
        # prove only startup absence, not a live engine entering retry/backoff.
        time.sleep(2)

        killed_at = time.monotonic()
        # The helper kills only this sandbox's slot. Debezium remains alive and
        # retries the stock JDBC replication connection; whenever it reattaches,
        # kill that new walsender too so the source stays in the real retry shape.
        engine_was_alive_during_retry = False
        while holder.poll() is None and time.monotonic() - killed_at < 60:
            box.kill_walsender()
            time.sleep(0.2)
            if not engine_was_alive_during_retry and time.monotonic() - killed_at >= 12:
                assert holder.poll() is None, _service_process_output(holder)
                engine_was_alive_during_retry = True

        returncode = holder.wait(timeout=60)
        output = _service_process_output(holder)
        summary = box.last_summary()
        assert engine_was_alive_during_retry, output
        assert returncode != 0, output
        assert summary.get("slot_ever_streamed") is True, (summary, output)
        assert summary.get("slot_stream_interruptions", 0) >= 1, (summary, output)
        assert summary.get("engine_thread_alive_at_source_dark") is True, (
            summary,
            output,
        )
        assert summary.get("stop_reason") == "source_dark", (summary, output)
        assert summary.get("source_dark_detected_after_sec") is not None, (
            summary,
            output,
        )
        assert summary["source_dark_detected_after_sec"] < 35, (summary, output)
        alert_rows = md_query(
            f'SELECT severity, code FROM "{case["control_schema"]}"."alerts" '
            "WHERE pipeline = ? AND code = 'source_dark'",
            [box.env["CDC_PIPELINE_NAME"]],
        )
        assert alert_rows == [("critical", "source_dark")], (alert_rows, summary, output)

        # The failed holder released its retained lease; this is a new scheduled
        # instance, not an in-process restart. It must acquire the next epoch and
        # attach the same stock slot rather than stand down.
        successor = box.spawn_service(
            destination="motherduck", capture=True, extra_env=service_env
        )
        box.wait_for_slot_active(process=successor, timeout=60)
        assert successor.poll() is None, _service_process_output(successor)
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('service-retry-successor', 'service-retry-successor@example.com')"
        )
        time.sleep(4)
        assert _stop(successor) == 0
        successor = None
        assert md_query(
            f'SELECT fencing_epoch FROM "{case["control_schema"]}"."lease"'
        ) == [(2,)]
        assert md_query(
            f'SELECT count(*) FROM "{case["dataset"]}"."cdcflight_app_customers" '
            "WHERE name = ?",
            ["service-retry-successor"],
        ) == [(1,)]
    finally:
        if successor is not None and successor.poll() is None:
            successor.kill()
            successor.wait(timeout=30)
        if holder is not None and holder.poll() is None:
            holder.kill()
            holder.wait(timeout=30)
        if holder is not None:
            holder.communicate(timeout=5)
        if successor is not None:
            successor.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_service_one_process_long_life_keeps_one_generation_and_exact_waves(
    tmp_path_factory, postgres_cluster
):
    """Several separated commits run through one live process generation."""
    box = Sandbox(
        "service_core_long_life",
        tmp_path_factory.mktemp("sbx_service_core_long_life"),
        postgres_cluster,
    )
    process = None
    pipeline = box.env["CDC_PIPELINE_NAME"]
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        process = box.spawn_service(
            extra_env={
                "CDC_SERVICE_ID": "service-core-long-life",
                "CDC_SERVICE_LEASE_TTL": "30",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
                "CDC_SERVICE_COMMIT_TIMEOUT": "10",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "10",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
                "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "1",
                "CDC_COMMIT_MAX_AGE": "0.25",
                "CDC_COMMIT_MAX_EVENTS": "1",
            }
        )
        box.wait_for_slot_active(process=process, timeout=45)
        time.sleep(1.0)
        for wave in range(5):
            box.sql(
                "INSERT INTO app.customers (name, email) VALUES "
                f"('service-long-life-{wave}', "
                f"'service-long-life-{wave}@example.com')"
            )
            # Separate source transactions and a gap longer than the service age
            # trigger force observable commit groups rather than one callback batch.
            time.sleep(1.1)
        time.sleep(2.0)
        assert _stop(process) == 0
        process = None

        runners = box.duck_query(
            "SELECT DISTINCT runner_id FROM _cdc_flight.commit_log "
            "WHERE pipeline = ? AND runner_id LIKE ?",
            [pipeline, "service-core-long-life:%"],
        )
        assert len(runners) == 1, runners
        service_commits = box.scalar(
            "SELECT count(*) FROM _cdc_flight.commit_log "
            "WHERE pipeline = ? AND runner_id = ? AND event_count > 0",
            [pipeline, runners[0][0]],
        )
        assert service_commits >= 5, service_commits
        assert box.scalar(
            f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} "
            "WHERE name LIKE 'service-long-life-%'"
        ) == 5
        assert box.duck_query(
            "SELECT state, service_id FROM _cdc_flight.lease"
        ) == [("released", "service-core-long-life")]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_service_cli_runs_against_real_motherduck_destination(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """The single-process CLI reaches the real MotherDuck connection."""
    case = motherduck_case
    box = Sandbox(
        "service_core_motherduck",
        tmp_path_factory.mktemp("sbx_service_core_motherduck"),
        postgres_cluster,
    )
    process = None
    environment = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_ID": "service-core-motherduck",
        # MotherDuck's first JVM snapshot is a real remote operation; keep
        # the documented 60-second lease margin rather than making bootstrap itself
        # race a deliberately tiny local-test TTL.
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "10",
        "CDC_SERVICE_COMMIT_TIMEOUT": "30",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "30",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
    }
    try:
        box.reseed()
        baseline = box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=environment,
            max_seconds=180,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline
        process = box.spawn_service(
            destination="motherduck",
            extra_env=environment,
            capture=True,
        )
        try:
            box.wait_for_slot_active(process=process, timeout=60)
        except AssertionError as exc:
            output = ""
            if process.stdout is not None:
                output += process.stdout.read() or ""
            if process.stderr is not None:
                output += process.stderr.read() or ""
            raise AssertionError(f"{exc}\n--- service output ---\n{output[-12000:]}") from exc
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('service-motherduck-row', 'service-motherduck@example.com')"
        )
        time.sleep(6)
        assert _stop(process) == 0
        process = None

        con = motherduck_connect(case["token"], case["database"])
        try:
            count = con.execute(
                f'SELECT count(*) FROM "{case["dataset"]}"."cdcflight_app_customers" '
                "WHERE name = ?",
                ["service-motherduck-row"],
            ).fetchone()[0]
            assert count == 1
            lease = con.execute(
                f'SELECT state, service_id FROM "{case["control_schema"]}"."lease"'
            ).fetchall()
            assert lease == [("released", "service-core-motherduck")], lease
        finally:
            con.close()
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_two_real_service_instances_have_one_holder_and_one_stand_down(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    box = Sandbox(
        "service_core_contention",
        tmp_path_factory.mktemp("sbx_service_core_contention"),
        postgres_cluster,
    )
    first = second = third = None
    case = motherduck_case
    environment = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_LEASE_TTL": "120",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "10",
        "CDC_SERVICE_COMMIT_TIMEOUT": "30",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "45",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    alerts_table = f'"{case["control_schema"]}"."alerts"'
    try:
        box.reseed()
        box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=environment,
            max_seconds=180,
            timeout=300,
        )
        first = box.spawn_service(
            matrix_arm=True,
            destination="motherduck",
            extra_env={
                **environment,
                "CDC_SERVICE_ID": "service-core-a",
            },
        )
        second = box.spawn_service(
            matrix_arm=True,
            capture=True,
            destination="motherduck",
            extra_env={
                **environment,
                "CDC_SERVICE_ID": "service-core-b",
            },
        )
        deadline = time.monotonic() + 60
        while first.poll() is None and second.poll() is None and time.monotonic() < deadline:
            time.sleep(0.1)
        assert (first.poll() is None) != (second.poll() is None), (
            _service_process_output(first) + _service_process_output(second)
        )
        holder = first if first.poll() is None else second
        stand_down = second if holder is first else first
        assert stand_down.returncode == 0, _service_process_output(stand_down)
        assert box.last_summary()["run_outcome"] == "stand_down"
        assert box.last_summary()["status"] == "SUCCEEDED"
        assert box.last_summary()["stand_down"] is True
        box.wait_for_slot_active(process=holder, timeout=60)

        def alert_rows() -> list[tuple]:
            return md_query(
                f"SELECT raised_at, severity, code, message, context "
                f"FROM {alerts_table} "
                "ORDER BY raised_at, severity, code, message, context"
            )

        alerts_before = alert_rows()
        third = box.spawn_service(
            matrix_arm=True,
            capture=True,
            destination="motherduck",
            extra_env={**environment, "CDC_SERVICE_ID": "service-core-c"},
        )
        assert third.wait(timeout=60) == 0, _service_process_output(third)
        assert box.last_summary()["run_outcome"] == "stand_down"
        # MotherDuck can expose a fresh connection's catalog snapshot before the
        # previous read's rows are visible.  Wait for the same durable row set, but
        # fail if a stand-down adds any alert; this is stronger than comparing a
        # count and preserves the no-mutation property under cloud read jitter.
        deadline = time.monotonic() + 15
        while True:
            alerts_after = alert_rows()
            if alerts_after == alerts_before:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "stand-down changed the durable alert rows: "
                    f"before={alerts_before!r} after={alerts_after!r}"
                )
            time.sleep(0.5)

        _stop(holder)
        if holder is first:
            first = None
        else:
            second = None
    finally:
        for process in (first, second, third):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_crashed_instance_is_reclaimed_after_expiry_and_fences_old_generation(
    tmp_path_factory, postgres_cluster
):
    """A real Flight death leaves no old generation able to publish after epoch 2."""
    box = Sandbox(
        "service_core_parent_loss",
        tmp_path_factory.mktemp("sbx_service_core_parent_loss"),
        postgres_cluster,
    )
    first = second = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        first = box.spawn_service(
            matrix_arm=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-old",
                "CDC_SERVICE_LEASE_TTL": "8",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_COMMIT_TIMEOUT": "2",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "4",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "6",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "1",
            },
        )
        _wait_for_service(box, first)
        os.kill(first.pid, signal.SIGKILL)
        assert first.wait(timeout=30) == -signal.SIGKILL

        # A hard-dead Flight cannot release.  The replacement waits for the
        # authoritative lease expiry, then takes the next fencing epoch.
        time.sleep(9)
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('after-parent-loss', 'after-parent-loss@example.com')"
        )
        second = box.spawn_service(
            matrix_arm=True,
            capture=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-new",
                "CDC_SERVICE_LEASE_TTL": "8",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_COMMIT_TIMEOUT": "2",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "4",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "6",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "1",
            },
        )
        _wait_for_service(box, second)
        time.sleep(5)
        assert _stop(second) == 0
        second = None

        row = box.duck_query(
            "SELECT state, fencing_epoch, service_id "
            "FROM _cdc_flight.lease"
        )
        assert row == [("released", 2, "service-core-new")]
        assert box.scalar(
            'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
            "WHERE name = ?",
            ["after-parent-loss"],
        ) == 1
        assert box.duck_query(
            "SELECT code FROM _cdc_flight.alerts "
            "WHERE code = 'service_holder_reclaimed'"
        ) == [("service_holder_reclaimed",)]
    finally:
        for process in (first, second):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.motherduck
def test_sigstop_expiry_takeover_sigcont_has_zero_old_fence_writes(
    tmp_path_factory, postgres_cluster, motherduck_case
):
    """A frozen real process cannot resurrect as a writer after epoch takeover."""
    case = motherduck_case
    box = Sandbox(
        "service_core_sigstop_resurrection",
        tmp_path_factory.mktemp("sbx_service_core_sigstop_resurrection"),
        postgres_cluster,
    )
    first = second = None
    environment = {
        "CDC_MD_DATABASE": case["database"],
        "CDC_DATASET": case["dataset"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_SERVICE_LEASE_TTL": "30",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
        "CDC_SERVICE_COMMIT_TIMEOUT": "10",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "20",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
    }

    def md_query(statement: str, params=None) -> list[tuple]:
        con = motherduck_connect(case["token"], case["database"])
        try:
            return con.execute(statement, params or []).fetchall()
        finally:
            con.close()

    lease_table = f'"{case["control_schema"]}"."lease"'
    commit_table = f'"{case["control_schema"]}"."commit_log"'
    data_table = f'"{case["dataset"]}"."cdcflight_app_customers"'
    try:
        box.reseed()
        baseline = box.run(
            reset_state=True,
            destination="motherduck",
            extra_env=environment,
            max_seconds=180,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline

        old_environment = {
            **environment,
            "CDC_SERVICE_ID": "service-freeze-old",
        }
        first = box.spawn_service(
            destination="motherduck",
            extra_env=old_environment,
            capture=True,
        )
        box.wait_for_slot_active(process=first, timeout=60)
        old_row = md_query(
            f"SELECT worker_generation, fencing_epoch, expires_at "
            f"FROM {lease_table} WHERE state = 'held'"
        )
        assert len(old_row) == 1, old_row
        old_generation, old_epoch, expires_at = old_row[0]
        old_commits_before = md_query(
            f"SELECT count(*) FROM {commit_table} WHERE runner_id = ?",
            [old_generation],
        )[0][0]

        os.kill(first.pid, signal.SIGSTOP)
        # Use the destination's own clock to prove the old lease is expired.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if md_query(
                f"SELECT expires_at <= current_timestamp FROM {lease_table} "
                "WHERE fencing_epoch = ?",
                [old_epoch],
            )[0][0]:
                break
            time.sleep(0.25)
        else:
            raise AssertionError(f"epoch {old_epoch} did not expire after {expires_at}")

        # The stopped Debezium connection owns the source slot.  Releasing that
        # source-side connection lets the successor attach; the old process stays
        # SIGSTOP-frozen and is resumed only after the successor is live.
        box.kill_walsender()
        tag = "service-sigstop-successor-row"
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            f"('{tag}', '{tag}@example.com')"
        )
        second = box.spawn_service(
            destination="motherduck",
            extra_env={**environment, "CDC_SERVICE_ID": "service-freeze-new"},
            capture=True,
        )
        box.wait_for_slot_active(process=second, timeout=60)

        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if md_query(
                f"SELECT count(*) FROM {data_table} WHERE name = ?", [tag]
            )[0][0] == 1:
                break
            if second.poll() is not None:
                break
            time.sleep(0.5)
        assert second.poll() is None, _service_process_output(second)
        assert md_query(
            f"SELECT count(*) FROM {data_table} WHERE name = ?", [tag]
        )[0][0] == 1

        # Resurrect the old process.  It may report the source connection loss or
        # drain, but any callback that reaches a destination commit must fail the
        # epoch fence.  The count below measures old-generation commit rows, not
        # merely the final row count.
        os.kill(first.pid, signal.SIGCONT)
        time.sleep(8)
        if first.poll() is None:
            first.send_signal(signal.SIGTERM)
        first.wait(timeout=45)
        old_commits_after = md_query(
            f"SELECT count(*) FROM {commit_table} WHERE runner_id = ?",
            [old_generation],
        )[0][0]
        assert old_commits_after - old_commits_before == 0
        assert md_query(
            f"SELECT count(*) FROM {data_table} WHERE name = ?", [tag]
        )[0][0] == 1

        second.send_signal(signal.SIGTERM)
        assert second.wait(timeout=90) == 0
        current = md_query(
            f"SELECT state, fencing_epoch, service_id FROM {lease_table}"
        )
        assert current == [("released", old_epoch + 1, "service-freeze-new")], current
    finally:
        if first is not None and first.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(first.pid, signal.SIGCONT)
            first.kill()
            first.wait(timeout=30)
        if second is not None and second.poll() is None:
            second.kill()
            second.wait(timeout=30)
        box.cleanup()
        box.reseed()


def _service_process_output(process) -> str:
    output = ""
    if process.stdout is not None:
        output += process.stdout.read() or ""
    if process.stderr is not None:
        output += process.stderr.read() or ""
    return output[-12000:]


INHERITED_SERVICE_CUTS = (
    "recovery_requested_recorded",
    "recovery_offsets_file_deleted_recorded",
    "recovery_resume_point_deleted_recorded",
    "recovery_armed_recorded",
    "ownership_available",
    "ownership_attached",
    "ownership_active",
    "ownership_callback_owned",
)

SERVICE_CUTS = (
    *INHERITED_SERVICE_CUTS,
    "service_startup",
    "service_callback_midstream",
    "service_before_md_commit",
    "service_after_md_commit_before_ack",
    "service_after_one_ack_before_finish",
    "service_pg_transaction_open",
    "service_lease_acquire",
    "service_lease_renewal",
    "service_lease_release",
    "service_heartbeat_write",
    "service_run_log_write",
    "service_source_health_write",
)


def test_plan_4_4_service_cut_inventory_names_every_gap():
    covered = {
        point
        for points in SERVICE_CUT_COVERAGE.values()
        for point in points
    }
    assert set(faults.SERVICE_MATRIX_POINTS) <= set(SERVICE_CUTS)
    assert covered <= set(SERVICE_CUTS) | set(faults.BACKFILL_POINTS)
    assert "service_startup" in set(SERVICE_CUTS)
    assert set(UNREACHABLE_SERVICE_CUTS) == {
        "scheduler request",
        "full-refresh handoff",
        "completion/watermark/shutdown terminal markers",
    }

    coordinator_source = inspect.getsource(LiveDiscoveryCoordinator.run)
    assert "self.service_context is None" in coordinator_source
    assert "discovery_handoff_enabled" in coordinator_source
    # The explicit exclusions are part of the evidence: they prevent a future
    # reviewer from treating an unimplemented service handoff as a green cell.
    assert all(UNREACHABLE_SERVICE_CUTS.values())

SIGKILL_SERVICE_CUTS = (
    "service_startup",
    "service_callback_midstream",
    "service_after_md_commit_before_ack",
    "service_lease_renewal",
)


def _service_cut_environment(box: Sandbox, cut: str) -> dict[str, str]:
    environment = {
        "CDC_CRASH_MATRIX_CUT": cut,
        "CDC_CRASH_MATRIX_STATE": "service_crash_matrix_state.json",
        "CDC_SERVICE_ID": f"service-cut-{cut}",
        "CDC_SERVICE_LEASE_TTL": "8",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "1",
        "CDC_SERVICE_COMMIT_TIMEOUT": "2",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "4",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "6",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "1",
    }
    if cut == "ownership_callback_owned":
        # Force the real callback-quiescence failure to its bounded transfer.  The
        # child then hard-exits at the production ownership transition; no test state
        # is assigned by the harness. Give this proof a separate watchdog margin:
        # the callback must publish its failed-quiescence transfer before either the
        # destination commit bound or the local service stall bound expires.
        environment.update(
            {
                "CDC_SERVICE_LEASE_TTL": "30",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
                "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "15",
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "20",
                "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "2",
                "CDC_SERVICE_COMMIT_TIMEOUT": "28",
                "CDC_FAULT_INJECT": "destination_hang:1",
                "CDC_FAULT_HANG_SECONDS": "30",
                # The callback-owned cell needs the bounded drain/quiescence proof
                # to publish ownership before the independent destination watchdog
                # fires. Keep the commit bound below the thirty-second lease, while
                # leaving the close bound at one second as the actual proof bound.
                "CDC_COMMIT_TIMEOUT": "28",
                "CDC_CLOSE_TIMEOUT": "1",
            }
        )
    return environment


def _advance_slot_past_new_rows(box: Sandbox) -> None:
    durable = box.duck_query(
        "SELECT last_lsn FROM _cdc_flight.debezium_offsets "
        "WHERE pipeline = ? AND namespace = ?",
        [box.env["CDC_PIPELINE_NAME"], "cdc-flight-engine"],
    )
    assert durable, "baseline did not leave a durable Debezium resume point"
    box.pg_query(
        "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, pg_current_wal_lsn())",
        (box.slot,),
    )
    confirmed = box.pg_query(
        "SELECT (confirmed_flush_lsn - '0/0')::bigint "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    assert confirmed and int(confirmed[0][0]) > int(durable[0][0]), (
        "the service recovery cell did not create a real slot-ahead-of-destination "
        f"state: confirmed={confirmed!r}, durable={durable!r}"
    )


def _duck_query_after_service_exit(box: Sandbox, statement: str) -> list[tuple]:
    """Wait for the service-owned DuckDB handle to retire before probing durability."""
    deadline = time.monotonic() + 45
    while True:
        try:
            return box.duck_query(statement)
        except duckdb.IOException as exc:
            if "Conflicting lock" not in str(exc) or time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def _run_one_service_cut(box: Sandbox, cut: str) -> dict:
    box.reseed()
    box.run(reset_state=True, max_seconds=150, timeout=240)
    pipeline = box.env["CDC_PIPELINE_NAME"]
    baseline_commit_count = box.scalar(
        "SELECT count(*) FROM _cdc_flight.commit_log WHERE pipeline = ?",
        [pipeline],
    )
    baseline_resume = box.duck_query(
        "SELECT last_lsn FROM _cdc_flight.debezium_offsets "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, "cdc-flight-engine"],
    )
    assert baseline_resume, "service matrix baseline has no durable resume point"
    baseline_lsn = int(baseline_resume[0][0])
    tag = f"service-cut-row-{cut}"
    if cut in {*INHERITED_SERVICE_CUTS, "service_callback_midstream"}:
        # The matrix arm is in the service callback, not in this test process.
        box.sql(
            [
                "SET synchronous_commit = on",
                f"INSERT INTO app.customers (name, email) VALUES "
                f"('{tag}', '{tag}@example.com')",
            ],
            one_transaction=True,
        )
    elif cut in {
        "service_before_md_commit",
        "service_after_md_commit_before_ack",
        "service_after_one_ack_before_finish",
    }:
        box.sql(
            [
                "SET synchronous_commit = on",
                f"INSERT INTO app.customers (name, email) VALUES "
                f"('{tag}', '{tag}@example.com')",
            ],
            one_transaction=True,
        )
    elif cut == "service_pg_transaction_open":
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT "
            "'service-open-' || i, 'service-open-' || i || '@example.com' "
            "FROM generate_series(1, 3000) i",
            one_transaction=True,
        )
    if cut in {
        "recovery_requested_recorded",
        "recovery_offsets_file_deleted_recorded",
        "recovery_resume_point_deleted_recorded",
        "recovery_armed_recorded",
    }:
        _advance_slot_past_new_rows(box)

    service_environment = _service_cut_environment(box, cut)
    process = box.spawn_service(
        matrix_arm=True,
        extra_env=service_environment,
    )
    try:
        if cut in {
            "service_lease_release",
        }:
            box.wait_for_slot_active(process=process, timeout=45)
        if cut == "service_lease_release" and process.poll() is None:
            process.send_signal(signal.SIGTERM)
        if cut == "ownership_callback_owned":
            deadline = time.monotonic() + 45
            while process.poll() is None and time.monotonic() < deadline:
                fired = box.fired_fault()
                if fired and fired["point"] == "destination_hang":
                    process.send_signal(signal.SIGTERM)
                    break
                time.sleep(0.05)
        returncode = process.wait(timeout=90)
        # The bounded lock-retrying probe below is the synchronization point for
        # the single process's destination handle retirement.
        fired = box.fired_fault()
        # Capture the generation's durable ownership record before the finite
        # recovery adapter acquires and releases the same physical lease.  The
        # post-recovery table is intentionally empty on the batch path, so
        # querying only after recovery would erase the evidence of the service
        # generation's terminal fence.
        lease_before_recovery = _duck_query_after_service_exit(
            box,
            "SELECT state, fencing_epoch, service_id "
            "FROM _cdc_flight.lease"
        )
        # This is intentionally before `box.run()` below.  Recovery is allowed to
        # repair a missing state row, but it must not be the reason this test claims
        # data/state atomicity.  The post-MD-commit cut must expose both facts from
        # the crashed Flight's own durable transaction while no recovery run owns
        # the destination.
        pre_recovery_row_count = (
            box.scalar(
                'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
                "WHERE name = ?",
                [tag],
            )
            if cut != "service_pg_transaction_open"
            else box.scalar(
                'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
                "WHERE name LIKE \'service-open-%\'"
            )
        )
        pre_recovery_commit_count = box.scalar(
            "SELECT count(*) FROM _cdc_flight.commit_log WHERE pipeline = ?",
            [pipeline],
        )
        pre_recovery_resume = box.duck_query(
            "SELECT last_lsn FROM _cdc_flight.debezium_offsets "
            "WHERE pipeline = ? AND namespace = ?",
            [pipeline, "cdc-flight-engine"],
        )
        pre_recovery = {
            "row_count": pre_recovery_row_count,
            "new_commit_count": pre_recovery_commit_count - baseline_commit_count,
            "baseline_lsn": baseline_lsn,
            "resume_last_lsn": (
                int(pre_recovery_resume[0][0]) if pre_recovery_resume else None
            ),
        }
        if lease_before_recovery[0][0] == "held":
            # A hard process death leaves the row held until the authoritative
            # expiry.  Batch recovery must not bypass that fence.
            time.sleep(float(service_environment["CDC_SERVICE_LEASE_TTL"]) + 1)
        recovery = box.run(
            max_seconds=180,
            timeout=260,
            expect_success=False,
        )
        row_count = box.scalar(
            'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
            "WHERE name = ?",
            [tag],
        ) if cut != "service_pg_transaction_open" else box.scalar(
            "SELECT count(*) FROM \"cdc_raw\".\"cdcflight_app_customers\" "
            "WHERE name LIKE 'service-open-%'"
        )
        lease_after_recovery = _duck_query_after_service_exit(
            box,
            "SELECT state, fencing_epoch, service_id FROM _cdc_flight.lease"
        )
        return {
            "returncode": returncode,
            "fired": fired,
            "recovery": recovery,
            "row_count": row_count,
            "lease": lease_before_recovery,
            "lease_after_recovery": lease_after_recovery,
            "pre_recovery": pre_recovery,
        }
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)


@pytest.fixture(scope="module")
def service_crash_matrix(tmp_path_factory, postgres_cluster):
    box = Sandbox(
        "service_crash_matrix",
        tmp_path_factory.mktemp("sbx_service_crash_matrix"),
        postgres_cluster,
    )
    results = {}
    try:
        for cut in SERVICE_CUTS:
            box.clear_fired_fault()
            (box.state_dir / "service_crash_matrix_state.json").unlink(missing_ok=True)
            results[cut] = _run_one_service_cut(box, cut)
        return results
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
@pytest.mark.parametrize("cut", SERVICE_CUTS)
def test_every_service_cut_is_a_real_child_death_with_durable_recovery(
    service_crash_matrix, cut
):
    result = service_crash_matrix[cut]
    assert result["returncode"] != 0, result
    assert result["fired"] and result["fired"]["point"] == cut, result
    assert result["fired"]["pid"] != os.getpid(), result
    assert result["recovery"]["returncode"] == 0, result
    assert result["recovery"].get("ok") is True, result
    assert len(result["lease"]) == 1, result
    assert result["lease"][0][0] in {"released", "held"}, result
    assert result["lease_after_recovery"] == [], result
    if cut == "service_pg_transaction_open":
        assert result["row_count"] == 3000, result
    elif cut in {
        "recovery_requested_recorded",
        "recovery_offsets_file_deleted_recorded",
        "recovery_resume_point_deleted_recorded",
        "recovery_armed_recorded",
        "ownership_available",
        "ownership_attached",
        "ownership_active",
        "ownership_callback_owned",
        "service_callback_midstream",
        "service_before_md_commit",
        "service_after_md_commit_before_ack",
        "service_after_one_ack_before_finish",
    }:
        assert result["row_count"] == 1, result


def test_service_data_and_state_are_observed_atomic_before_recovery(service_crash_matrix):
    """The post-commit cut exposes data and its state before repair can mask a mutant."""
    result = service_crash_matrix["service_after_md_commit_before_ack"]
    pre = result["pre_recovery"]
    assert pre["row_count"] == 1, pre
    assert pre["new_commit_count"] == 1, pre
    assert pre["resume_last_lsn"] is not None
    assert pre["resume_last_lsn"] > pre["baseline_lsn"], pre


@pytest.mark.slow
@pytest.mark.parametrize("cut", SIGKILL_SERVICE_CUTS)
def test_service_sigkill_edges_leave_one_durable_owner(
    tmp_path_factory, postgres_cluster, cut
):
    """Use an actual signal at startup, callback, commit, and renewal edges."""
    box = Sandbox(
        f"service_sigkill_{cut}",
        tmp_path_factory.mktemp(f"sbx_service_sigkill_{cut}"),
        postgres_cluster,
    )
    process = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        tag = f"service-sigkill-row-{cut}"
        if cut in {
            "service_callback_midstream",
            "service_after_md_commit_before_ack",
            # The renewal cut needs a live own-progress witness; without one a
            # quiet service can correctly remain outside the renewal fold after
            # an ignored control-plane signal, so there is no renewal edge to
            # inject.
            "service_lease_renewal",
        }:
            box.sql(
                f"INSERT INTO app.customers (name, email) VALUES "
                f"('{tag}', '{tag}@example.com')"
            )
        hold = box.dir / f"sigkill_{cut}.ready"
        process = box.spawn_service(
            matrix_arm=True,
            extra_env={
                **_service_cut_environment(box, cut),
                "CDC_CRASH_MATRIX_HOLD": str(hold),
            },
        )
        deadline = time.monotonic() + 90
        while not hold.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert hold.exists(), f"{cut} did not reach its held edge"
        fired = box.fired_fault()
        assert fired and fired["point"] == cut, fired
        victim = process.pid if cut == "service_lease_renewal" else int(fired["pid"])
        os.kill(victim, signal.SIGKILL)
        returncode = process.wait(timeout=90)
        assert returncode != 0
        if cut == "service_lease_renewal":
            assert returncode == -signal.SIGKILL
        assert fired["pid"] != os.getpid()

        # Parent loss and bounded engine retirement are separate operation bounds;
        # retry the read-only probe while the child-owned DuckDB handle retires, but
        # fail if it does not release within the finite crash-harness bound.
        lease = _duck_query_after_service_exit(
            box,
            "SELECT state, fencing_epoch, service_id FROM _cdc_flight.lease"
        )
        assert len(lease) == 1, lease
        assert lease[0][0] in {"released", "held"}, lease
        if lease[0][0] == "held":
            time.sleep(9)

        recovery = box.run(max_seconds=180, timeout=260, expect_success=False)
        assert recovery["returncode"] == 0, recovery
        assert recovery.get("ok") is True, recovery
        if cut in {
            "service_callback_midstream",
            "service_after_md_commit_before_ack",
        }:
            assert box.scalar(
                'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
                "WHERE name = ?",
                [tag],
            ) == 1
        assert box.duck_query("SELECT * FROM _cdc_flight.lease") == []
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        box.cleanup()
        box.reseed()
