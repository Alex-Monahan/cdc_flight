"""Real-process proofs for the Steps 1-3 service boundary."""

from __future__ import annotations

import os
import signal
import time

import duckdb
import pytest
from support.fixtures import Sandbox


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
