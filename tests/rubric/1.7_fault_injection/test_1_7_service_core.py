"""Real-process proofs for the Steps 1-3 service boundary."""

from __future__ import annotations

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
    "worker authenticated startup": ("service_worker_startup",),
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


def _wait_for_worker(box: Sandbox, process, *, timeout: float = 45.0) -> int:
    """Use PostgreSQL liveness while the worker owns the DuckDB file lock."""
    box.wait_for_slot_active(process=process, timeout=timeout)
    return process.pid


def _stop(process, *, expected: set[int] | None = None) -> int:
    expected = expected or {0}
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    returncode = process.wait(timeout=90)
    assert returncode in expected, returncode
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
            extra_env={
                "CDC_SERVICE_ID": "service-core-drain",
                "CDC_SERVICE_LEASE_TTL": "12",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                    "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
                    "CDC_SERVICE_WORKER_HEARTBEAT_TIMEOUT": "8",
                    # Keep the graceful-drain proof finite while allowing the
                    # stock JVM close to complete under the two-worker slow lane.
                    "CDC_CLOSE_TIMEOUT": "60",
                    "CDC_ENGINE_THREAD_TIMEOUT": "60",
                    "CDC_SERVICE_DRAIN_DEADLINE_SECONDS": "90",
                },
            )
        worker_pid = _wait_for_worker(box, process)
        assert worker_pid == process.pid
        box.wait_for_slot_active(process=process, timeout=45)
        box.sql(
            "INSERT INTO app.customers (name, email) "
            "VALUES ('service-core-row', 'service-core@example.com')"
        )
        # DuckDB is intentionally not queried while the service worker owns its
        # process-level file lock.  Allow the live connector to consume the row,
        # then assert the durable result after the bounded drain closes the handle.
        time.sleep(5)

        assert _stop(process) == 0
        process = None
        released = box.duck_query(
            "SELECT state, fencing_epoch, service_id, worker_pid "
            "FROM _cdc_flight.lease"
        )
        assert released == [("released", 1, "service-core-drain", None)]
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
    """A real worker blocked in pre-COMMIT destination I/O is hard-fenced."""
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
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
                "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
                "CDC_SERVICE_OPERATION_TIMEOUT_SECONDS": "5",
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
            "SELECT state, service_id, worker_pid FROM _cdc_flight.lease"
        ) == [("released", "service-core-destination-hang", None)]
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
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
                "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
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
            "SELECT state, service_id, worker_pid FROM _cdc_flight.lease"
        ) == [("released", "service-core-slot-drop", None)]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        if process is not None:
            process.communicate(timeout=5)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_service_one_worker_long_life_keeps_one_generation_and_exact_waves(
    tmp_path_factory, postgres_cluster
):
    """Several separated commits run through one live worker generation."""
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
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
                "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
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
            "SELECT state, service_id, worker_pid FROM _cdc_flight.lease"
        ) == [("released", "service-core-long-life", None)]
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
    """The supervisor's CLI destination reaches the worker's real MD connection."""
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
        # MotherDuck's first worker/JVM snapshot is a real remote operation; keep
        # the documented 60-second lease margin rather than making bootstrap itself
        # race a deliberately tiny local-test TTL.
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "10",
        "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
        "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
        "CDC_SERVICE_OPERATION_TIMEOUT_SECONDS": "30",
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
def test_two_real_service_supervisors_cannot_start_two_workers(
    tmp_path_factory, postgres_cluster
):
    box = Sandbox(
        "service_core_contention",
        tmp_path_factory.mktemp("sbx_service_core_contention"),
        postgres_cluster,
    )
    first = second = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150, timeout=240)
        first = box.spawn_service(
            matrix_arm=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-a",
                "CDC_SERVICE_LEASE_TTL": "12",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
            },
        )
        incumbent_pid = _wait_for_worker(box, first)
        second = box.spawn_service(
            matrix_arm=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-b",
                "CDC_SERVICE_LEASE_TTL": "12",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "3",
            },
        )
        assert second.wait(timeout=45) != 0
        assert first.poll() is None
        assert first.pid == incumbent_pid
        _stop(first)
        first = None
    finally:
        for process in (first, second):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=30)
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_sigkill_supervisor_fences_old_worker_before_same_destination_takeover(
    tmp_path_factory, postgres_cluster
):
    """A real parent death leaves no old generation able to publish after epoch 2."""
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
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "2",
                "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
            },
        )
        _wait_for_worker(box, first)
        os.kill(first.pid, signal.SIGKILL)
        assert first.wait(timeout=30) == -signal.SIGKILL

        # The worker receives EOF on the parent IPC channel and must drain/close
        # before the replacement is admitted.  This wait is deliberately longer
        # than the configured parent-loss bound and is followed by durable checks.
        time.sleep(5)
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            "('after-parent-loss', 'after-parent-loss@example.com')"
        )
        second = box.spawn_service(
            matrix_arm=True,
            extra_env={
                "CDC_SERVICE_ID": "service-core-new",
                "CDC_SERVICE_LEASE_TTL": "8",
                "CDC_SERVICE_LEASE_RENEW_SECONDS": "2",
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
                "CDC_SERVICE_PARENT_LOSS_SECONDS": "2",
                "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
            },
        )
        _wait_for_worker(box, second)
        time.sleep(5)
        assert _stop(second) == 0
        second = None

        row = box.duck_query(
            "SELECT state, fencing_epoch, service_id, worker_pid "
            "FROM _cdc_flight.lease"
        )
        assert row == [("released", 2, "service-core-new", None)]
        assert box.scalar(
            'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" '
            "WHERE name = ?",
            ["after-parent-loss"],
        ) == 1
    finally:
        for process in (first, second):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=30)
        box.cleanup()
        box.reseed()


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
    "service_worker_startup",
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
    assert "service_worker_startup" in set(SERVICE_CUTS)
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
    "service_worker_startup",
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
        "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": "0.25",
        "CDC_SERVICE_PARENT_LOSS_SECONDS": "2",
        "CDC_SERVICE_MAX_WORKER_RESTARTS": "0",
    }
    if cut == "ownership_callback_owned":
        # Force the real callback-quiescence failure to its bounded transfer.  The
        # child then hard-exits at the production ownership transition; no test state
        # is assigned by the harness.
        environment.update(
            {
                "CDC_FAULT_INJECT": "destination_hang:1",
                "CDC_FAULT_HANG_SECONDS": "30",
                "CDC_COMMIT_TIMEOUT": "300",
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


def _duck_query_after_worker_exit(box: Sandbox, statement: str) -> list[tuple]:
    """Wait for a child-owned DuckDB handle to retire before probing durability."""
    deadline = time.monotonic() + 45
    while True:
        try:
            return box.duck_query(statement)
        except duckdb.IOException as exc:
            if "Conflicting lock" not in str(exc) or time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def _wait_for_pid_exit(pid: int | None, *, timeout: float = 45.0) -> None:
    """Prove the old service worker is gone before a takeover attempt."""
    if pid is None:
        return
    deadline = time.monotonic() + timeout
    while True:
        try:
            os.kill(int(pid), 0)
        except ProcessLookupError:
            return
        except PermissionError:
            pass
        if time.monotonic() >= deadline:
            raise AssertionError(f"service worker pid {pid} did not exit before takeover")
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
        # The matrix arm is in the worker callback, not in this parent process.
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

    process = box.spawn_service(
        matrix_arm=True,
        extra_env=_service_cut_environment(box, cut),
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
        # Parent-loss cleanup is bounded by the worker's IPC timeout.  It is
        # important to wait before reopening DuckDB for the recovery child.  The
        # bounded lock-retrying probe below is the synchronization point; a fixed
        # post-exit sleep only adds host contention to this 20-cell real-process
        # matrix without proving anything about handle retirement.
        fired = box.fired_fault()
        # Capture the generation's durable ownership record before the finite
        # recovery adapter acquires and releases the same physical lease.  The
        # post-recovery table is intentionally empty on the batch path, so
        # querying only after recovery would erase the evidence of the service
        # generation's terminal fence.
        lease_before_recovery = _duck_query_after_worker_exit(
            box,
            "SELECT state, fencing_epoch, worker_pid, worker_generation "
            "FROM _cdc_flight.lease"
        )
        _wait_for_pid_exit(lease_before_recovery[0][2])
        # This is intentionally before `box.run()` below.  Recovery is allowed to
        # repair a missing state row, but it must not be the reason this test claims
        # data/state atomicity.  The post-MD-commit cut must expose both facts from
        # the crashed worker's own durable transaction while no recovery worker owns
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
        lease_after_recovery = _duck_query_after_worker_exit(
            box,
            "SELECT state, fencing_epoch, worker_pid FROM _cdc_flight.lease"
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
    assert result["lease"][0][0] in {
        "released",
        "supervisor_held",
        "worker_active",
        "worker_starting",
    }, result
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
        lease = _duck_query_after_worker_exit(
            box,
            "SELECT state, fencing_epoch, worker_pid FROM _cdc_flight.lease"
        )
        assert len(lease) == 1, lease
        assert lease[0][0] in {
            "released",
            "supervisor_held",
            "worker_active",
            "worker_starting",
        }, lease
        _wait_for_pid_exit(lease[0][2])

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
