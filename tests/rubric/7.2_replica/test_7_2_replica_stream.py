"""Rubric 7.2's real primary -> physical standby -> local logical slot proof.

This lane intentionally runs the production service entrypoint.  The fixture first
provisions and checks a native physical receiver and a logical slot owned by the
standby.  Each service arm gets a durable, fsynced ``STREAM_ARMED`` witness only
after a successful baseline snapshot; a unique primary DML row written afterwards,
followed by local-slot confirmation and destination observation, is the only live
streaming proof.
"""

from __future__ import annotations

import json
import os
import signal
import time
import uuid
from pathlib import Path

import pytest
from support.standby_topology import StandbyCase, StandbyTopology

pytestmark = [
    pytest.mark.slow,
    pytest.mark.e2e,
    pytest.mark.xdist_group("p72_replica_stream"),
]


def _write_durable(path: Path, payload: dict) -> None:
    """Publish one test witness atomically, with file and directory durability."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    try:
        directory_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:  # pragma: no cover - macOS permits opening this directory
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _wait_until(predicate, *, timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if predicate():
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for {description}")
        time.sleep(min(0.25, remaining))


def _arm_stream(
    case: StandbyCase,
    topology: StandbyTopology,
    process,
    baseline: dict,
    label: str,
) -> Path:
    """Establish the post-snapshot barrier without a startup sleep."""
    assert baseline.get("ok") is True, baseline
    assert baseline.get("snapshot_completed") is True, baseline
    topology.wait_for_slot_active(process=process, timeout=120)
    facts = topology.stream_facts()
    observation = facts["observation"]
    assert observation["in_recovery"] is True, facts
    assert observation["receiver_status"] == "streaming", facts
    assert observation["receiver_slot_name"] == topology.physical_slot, facts
    assert observation["local_slot_name"] == topology.local_slot, facts
    assert observation["local_slot_type"] == "logical", facts
    assert observation["local_slot_plugin"] == "pgoutput", facts
    path = case.root / f"{label}_STREAM_ARMED.json"
    _write_durable(
        path,
        {
            "event": "STREAM_ARMED",
            "classification": "NEVER_ARMED",
            "label": label,
            "pipeline": case.pipeline,
            "source_role": "standby",
            "standby_port": topology.port,
            "local_slot": topology.local_slot,
            "physical_slot": topology.physical_slot,
            "baseline_snapshot_completed": True,
            "standby_observation": observation,
            "standby_receive_wal": facts["standby_receive_wal"],
            "primary_wal": facts["primary_wal"],
        },
    )
    witness = json.loads(path.read_text(encoding="utf-8"))
    assert witness["event"] == "STREAM_ARMED"
    assert witness["classification"] == "NEVER_ARMED"
    return path


def _mark_fired(path: Path, *, sentinel: str, source_lsn: int) -> None:
    witness = json.loads(path.read_text(encoding="utf-8"))
    witness.update(
        {
            "classification": "FIRED",
            "sentinel": sentinel,
            "sentinel_source_lsn": source_lsn,
        }
    )
    _write_durable(path, witness)
    assert json.loads(path.read_text(encoding="utf-8"))["classification"] == "FIRED"


def _insert_sentinel(topology: StandbyTopology, prefix: str) -> tuple[str, int]:
    sentinel = f"{prefix}_{uuid.uuid4().hex[:12]}"
    source_lsn = topology.primary_sql_with_wal(
        "INSERT INTO app.customers (name, email) VALUES (%s, %s)",
        (sentinel, f"{sentinel}@example.com"),
    )
    return sentinel, source_lsn


def _wait_for_post_arm_ack(
    topology: StandbyTopology,
    process,
    source_lsn: int,
    *,
    timeout: float = 180,
) -> None:
    """Use the standby slot's confirmed flush as the live callback proof."""

    def acknowledged() -> bool:
        if process.poll() is not None:
            raise AssertionError(
                "NEVER_ARMED: the live service exited before the post-arm sentinel "
                f"was confirmed (returncode={process.returncode})"
            )
        confirmed = topology.local_slot_confirmed_lsn()
        return confirmed is not None and confirmed >= source_lsn

    _wait_until(
        acknowledged,
        timeout=timeout,
        description=f"standby local slot confirmation at {source_lsn}",
    )


def _await_duck_row(case: StandbyCase, sentinel: str) -> list[tuple]:
    rows = case.wait_for_destination(
        'SELECT id, name, email, cdcf_event_id, cdcf_commit_id, dbz_lsn '
        'FROM "cdc_raw"."cdcflight_app_customers" WHERE name = ?',
        [sentinel],
        predicate=lambda result: len(result) == 1,
        timeout=90,
    )
    assert len(rows) == 1, rows
    return rows


def _assert_clean_shutdown(summary: dict) -> None:
    assert summary.get("ok") is True, summary
    history = summary["shutdown_sequence_history"]
    assert history.index("admission_sealed") < history.index("callbacks_quiescent"), history
    assert history.index("callbacks_quiescent") < history.index("own_executors_stopped"), history
    assert history.index("own_executors_stopped") < history.index("engine_closing"), history
    assert history.index("engine_closing") < history.index("engine_closed"), history
    assert history.index("engine_closed") < history.index("engine_thread_stopped"), history
    assert summary.get("applier_quiesced") is True, summary
    assert summary.get("close_hung") is not True, summary
    assert "callback_owned" not in history, history


def _service_env(service_id: str) -> dict[str, str]:
    return {
        "CDC_SERVICE_ID": service_id,
        "CDC_SERVICE_LEASE_TTL": "60",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "10",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "30",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "20",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
        "CDC_SERVICE_COMMIT_TIMEOUT": "30",
        "CDC_SERVICE_CLOSE_TIMEOUT": "2",
        "CDC_CLOSE_TIMEOUT": "2",
        "CDC_ENGINE_THREAD_TIMEOUT": "2",
    }


def _stop_after_failure(case: StandbyCase, process) -> None:
    if process is None or process.poll() is not None:
        return
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except TimeoutError:
        process.kill()
        process.wait(timeout=30)


def test_real_standby_stream_invalidation_and_common_shutdown(
    tmp_path_factory,
    standby_topology: StandbyTopology,
):
    """Prove route, live streaming, callback-held shutdown, and local-slot repair."""
    topology = standby_topology
    case = topology.make_case(
        tmp_path_factory.mktemp("p72_replica_stream_case"), name="stream"
    )
    initial_facts = topology.stream_facts()
    initial_observation = initial_facts["observation"]
    assert initial_observation["in_recovery"] is True, initial_facts
    assert initial_observation["receiver_status"] == "streaming", initial_facts
    assert initial_observation["receiver_slot_name"] == topology.physical_slot, initial_facts
    assert initial_observation["local_slot_name"] == topology.local_slot, initial_facts
    assert initial_observation["local_slot_type"] == "logical", initial_facts
    assert initial_observation["local_slot_plugin"] == "pgoutput", initial_facts
    assert initial_observation["system_identifier"] == initial_observation[
        "primary_system_identifier"
    ], initial_facts
    assert initial_observation["timeline_id"] == initial_observation["primary_timeline_id"], (
        initial_facts
    )
    assert initial_facts["standby_receive_wal"] >= initial_facts["primary_wal"], initial_facts

    baseline = case.invoke(
        reset_state=True,
        max_seconds=300,
        idle_seconds=10,
        timeout=900,
    )
    assert baseline["ok"] is True, baseline
    assert baseline["source_role"] == "standby", baseline
    routes = baseline["source_routes"]
    assert routes["read_replication_dsn"] == topology.standby_dsn, baseline
    assert routes["source_write_dsn"] == topology.primary_dsn, baseline
    assert routes["slot_owner_dsn"] == topology.standby_dsn, baseline
    assert baseline["engine_effective_configuration"]["heartbeat.action.query"] == "", baseline
    assert baseline["engine_effective_configuration"]["heartbeat_action_disabled"] is True, baseline

    process = None
    callback_process = None
    loss_process = None
    sentinel_names: list[str] = []
    try:
        # Clean service drain. The unique source row is inserted only after the
        # durable post-snapshot barrier and the standby slot confirms its callback.
        process = case.spawn_service(extra_env=_service_env("p72-stream-clean"))
        clean_armed = _arm_stream(case, topology, process, baseline, "clean")
        clean_sentinel, clean_lsn = _insert_sentinel(topology, "p72_clean")
        sentinel_names.append(clean_sentinel)
        _wait_for_post_arm_ack(topology, process, clean_lsn)
        rc = case.terminate(process, timeout=120)
        assert rc == 0, {"returncode": rc, "summary": case.last_summary()}
        process = None
        _mark_fired(clean_armed, sentinel=clean_sentinel, source_lsn=clean_lsn)
        _await_duck_row(case, clean_sentinel)
        clean_summary = case.last_summary()
        _assert_clean_shutdown(clean_summary)
        assert clean_summary["source_routes"]["slot_owner_dsn"] == topology.standby_dsn

        # Callback-held shutdown. The destination fault is configured before the
        # child starts, but its arm file is created only after STREAM_ARMED. The
        # callback witness therefore proves a real post-arm source sentinel reached
        # the destination write before shutdown was requested.
        callback_path = case.root / "CALLBACK_ENTERED"
        callback_arm = case.root / "CALLBACK_ARMED"
        callback_path.unlink(missing_ok=True)
        callback_arm.unlink(missing_ok=True)
        case.clear_fired_fault()
        callback_env = {
            **_service_env("p72-stream-callback"),
            "CDC_FAULT_INJECT": "destination_hang:1",
            "CDC_FAULT_HANG_PHASE": "pre_commit",
            "CDC_FAULT_HANG_SECONDS": "60",
            "CDC_TEST_CALLBACK_ENTERED": str(callback_path),
            "CDC_TEST_DESTINATION_FAULT_ARM": str(callback_arm),
        }
        callback_process = case.spawn_service(extra_env=callback_env)
        callback_stream_armed = _arm_stream(
            case, topology, callback_process, baseline, "callback"
        )
        _write_durable(callback_arm, {"event": "CALLBACK_FAULT_ARMED"})
        callback_sentinel, callback_lsn = _insert_sentinel(topology, "p72_callback")
        sentinel_names.append(callback_sentinel)

        def callback_fired() -> bool:
            if callback_process.poll() is not None:
                raise AssertionError(
                    "NEVER_ARMED: callback child exited before CALLBACK_ENTERED "
                    f"(returncode={callback_process.returncode})"
                )
            fired = case.fired_fault()
            return (
                callback_path.is_file()
                and fired is not None
                and fired.get("point") == "destination_hang"
            )

        _wait_until(callback_fired, timeout=90, description="post-arm callback entry")
        _mark_fired(callback_stream_armed, sentinel=callback_sentinel, source_lsn=callback_lsn)
        callback_process.send_signal(signal.SIGTERM)
        callback_rc = callback_process.wait(timeout=90)
        assert callback_rc != 0, {"returncode": callback_rc, "summary": case.last_summary()}
        callback_process = None
        callback_summary = case.last_summary()
        callback_history = callback_summary.get("shutdown_sequence_history", [])
        assert callback_summary.get("applier_quiesced") is False, callback_summary
        assert "admission_sealed" in callback_history, callback_summary
        assert "callback_owned" in callback_history, callback_summary
        assert "engine_closing" not in callback_history, callback_summary
        assert "engine_closed" not in callback_history, callback_summary
        assert callback_summary.get("destination_owner") == "live_applier_callback", callback_summary
        assert not case.duck_query(
            'SELECT 1 FROM "cdc_raw"."cdcflight_app_customers" WHERE name = ?',
            [callback_sentinel],
        ), callback_summary
        topology.wait_for_slot_inactive(timeout=60)

        # Local-slot loss is exercised while a fresh production service is live.
        # The already confirmed row remains durable; loss must stop before another
        # acknowledgement and must create a destination-owned recovery obligation.
        case.clear_fired_fault()
        loss_process = case.spawn_service(extra_env=_service_env("p72-stream-loss"))
        loss_armed = _arm_stream(case, topology, loss_process, baseline, "loss")
        loss_sentinel, loss_lsn = _insert_sentinel(topology, "p72_loss")
        sentinel_names.append(loss_sentinel)
        _wait_for_post_arm_ack(topology, loss_process, loss_lsn)
        topology.drop_local_slot()
        assert topology.local_slot_status() is None
        loss_rc = loss_process.wait(timeout=150)
        assert loss_rc != 0, {"returncode": loss_rc, "summary": case.last_summary()}
        loss_process = None
        _mark_fired(loss_armed, sentinel=loss_sentinel, source_lsn=loss_lsn)
        _await_duck_row(case, loss_sentinel)
        loss_summary = case.last_summary()
        assert loss_summary.get("local_slot_failure"), loss_summary
        assert loss_summary["local_slot_failure"]["kind"] in {"lost", "invalidated"}, loss_summary
        assert loss_summary["source_routes"]["slot_owner_dsn"] == topology.standby_dsn, loss_summary
        assert loss_summary.get("final_ack_required") is not True, loss_summary
        recovery_rows = case.duck_query(
            "SELECT decision, phase FROM _cdc_flight.recovery_state "
            "WHERE pipeline = ? AND namespace = ?",
            [case.pipeline, "cdc-flight-engine"],
        )
        assert recovery_rows and recovery_rows[0][0] in {"slot_missing", "slot_invalidated"}, (
            recovery_rows,
            loss_summary,
        )

        # With the local slot still absent, the next admission may resume the
        # journal but must refuse before Debezium can choose a primary slot.
        refused = case.invoke(
            max_seconds=120,
            timeout=300,
            expect_success=False,
        )
        assert refused["returncode"] != 0, refused
        assert "standby" in (
            refused.get("error", "") + refused.get("output", "")
        ).lower(), refused
        assert topology.local_slot_status() is None
        assert case.duck_query(
            'SELECT 1 FROM "cdc_raw"."cdcflight_app_customers" WHERE name = ?',
            [loss_sentinel],
        ), refused

        # Operator repair is explicit and local. The following production run then
        # performs the journal's fenced full resnapshot and clears the obligation.
        topology.repair_local_slot()
        recovered = case.invoke(
            max_seconds=420,
            idle_seconds=10,
            timeout=900,
        )
        assert recovered["ok"] is True, recovered
        assert recovered["source_routes"]["read_replication_dsn"] == topology.standby_dsn
        assert recovered["source_routes"]["source_write_dsn"] == topology.primary_dsn
        assert recovered["source_routes"]["slot_owner_dsn"] == topology.standby_dsn
        assert recovered.get("recovery_cleared") or recovered.get("recovery_resumed"), recovered
        assert topology.local_slot_status() is not None
        for sentinel in sentinel_names:
            assert case.duck_query(
                'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers" WHERE name = ?',
                [sentinel],
            ) == [(1,)], (sentinel, recovered)
    finally:
        _stop_after_failure(case, process)
        _stop_after_failure(case, callback_process)
        _stop_after_failure(case, loss_process)
        topology.wait_for_slot_inactive(timeout=60)
        if sentinel_names:
            placeholders = ", ".join("%s" for _ in sentinel_names)
            topology.primary_sql(
                f"DELETE FROM app.customers WHERE name IN ({placeholders})",
                tuple(sentinel_names),
            )
