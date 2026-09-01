"""Rubric 7.2's MotherDuck durability proof for a standby decoder."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import duckdb
import pytest
from support.motherduck_probe import connect
from support.standby_topology import StandbyCase, StandbyTopology

pytestmark = [
    pytest.mark.motherduck,
    pytest.mark.e2e,
    pytest.mark.xdist_group("p72_replica_motherduck"),
]


def _write_durable(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
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


def _q(token: str, database: str, statement: str, params=()) -> list[tuple]:
    con = connect(token, database)
    try:
        return con.execute(statement, params).fetchall()
    finally:
        con.close()


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _wait_md_phase(
    case: StandbyCase,
    topology: StandbyTopology,
    md: dict[str, str],
    process,
    phase: str,
    *,
    timeout: float = 180,
) -> None:
    heartbeat = f"{_quoted(md['control_schema'])}.heartbeat"

    def reached() -> bool:
        if process.poll() is not None:
            raise AssertionError(
                f"NEVER_ARMED: standby MotherDuck child exited before {phase!r} "
                f"(returncode={process.returncode})"
            )
        try:
            return bool(
                _q(
                    md["token"],
                    md["database"],
                    f"SELECT 1 FROM {heartbeat} WHERE pipeline = ? AND phase = ? LIMIT 1",
                    (case.pipeline, phase),
                )
            )
        except duckdb.Error:
            return False

    _wait_until(reached, timeout=timeout, description=f"MotherDuck phase {phase!r}")


def _arm_stream(
    case: StandbyCase,
    topology: StandbyTopology,
    md: dict[str, str],
    process,
    baseline: dict,
    label: str,
) -> Path:
    assert baseline.get("ok") is True, baseline
    assert baseline.get("snapshot_completed") is True, baseline
    topology.wait_for_slot_active(process=process, timeout=180)
    _wait_md_phase(case, topology, md, process, "streaming")
    facts = topology.stream_facts()
    observation = facts["observation"]
    assert observation["in_recovery"] is True, facts
    assert observation["receiver_status"] == "streaming", facts
    assert observation["local_slot_name"] == topology.local_slot, facts
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
        },
    )
    assert json.loads(path.read_text(encoding="utf-8"))["classification"] == "NEVER_ARMED"
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


def _md_receipt(md: dict[str, str], case: StandbyCase, sentinel: str) -> dict | None:
    dataset = _quoted(md["dataset"])
    control = _quoted(md["control_schema"])
    data_rows = _q(
        md["token"],
        md["database"],
        f"SELECT cdcf_event_id, cdcf_commit_id, dbz_lsn "
        f"FROM {dataset}.cdcflight_app_customers WHERE name = ?",
        (sentinel,),
    )
    if len(data_rows) != 1:
        return None
    event_id, commit_id, dbz_lsn = data_rows[0]
    if event_id is None or commit_id is None or dbz_lsn is None:
        return None
    ledger = _q(
        md["token"],
        md["database"],
        f"SELECT state, source_schema, source_table, source_lsn, commit_lsn "
        f"FROM {control}.event_ledger "
        "WHERE pipeline = ? AND target_table = 'cdcflight_app_customers' AND event_id = ?",
        (case.pipeline, event_id),
    )
    commits = _q(
        md["token"],
        md["database"],
        f"SELECT event_count, first_lsn, last_lsn FROM {control}.commit_log "
        "WHERE pipeline = ? AND commit_id = ?",
        (case.pipeline, commit_id),
    )
    offsets = _q(
        md["token"],
        md["database"],
        f"SELECT commit_id, last_lsn, resume_json FROM {control}.debezium_offsets "
        "WHERE pipeline = ? AND namespace = 'cdc-flight-engine'",
        (case.pipeline,),
    )
    state = _q(
        md["token"],
        md["database"],
        f"SELECT snapshot_state FROM {control}.table_state "
        "WHERE pipeline = ? AND source_schema = 'app' AND source_table = 'customers'",
        (case.pipeline,),
    )
    if len(ledger) != 1 or len(commits) != 1 or len(offsets) != 1 or len(state) != 1:
        return None
    ledger_row = ledger[0]
    commit_row = commits[0]
    offset_row = offsets[0]
    if ledger_row[0] != "applied" or ledger_row[1:3] != ("app", "customers"):
        return None
    if state[0][0] != "complete":
        return None
    if int(offset_row[1]) < int(dbz_lsn) or int(commit_row[2]) < int(dbz_lsn):
        return None
    return {
        "sentinel": sentinel,
        "event_id": str(event_id),
        "commit_id": int(commit_id),
        "dbz_lsn": int(dbz_lsn),
        "ledger_source_lsn": ledger_row[3],
        "ledger_commit_lsn": ledger_row[4],
        "commit_event_count": int(commit_row[0]),
        "commit_first_lsn": commit_row[1],
        "commit_last_lsn": int(commit_row[2]),
        "offset_commit_id": int(offset_row[0]),
        "offset_last_lsn": int(offset_row[1]),
        "resume_json": str(offset_row[2]),
        "table_state": state[0][0],
    }


def _wait_md_committed(
    case: StandbyCase,
    topology: StandbyTopology,
    md: dict[str, str],
    process,
    sentinel: str,
    *,
    timeout: float = 300,
) -> dict:
    result: dict | None = None

    def committed() -> bool:
        nonlocal result
        if process.poll() is not None:
            output = case.close_output(process)
            try:
                server_log = topology.log_path.read_text(encoding="utf-8")[-12000:]
            except OSError as exc:
                server_log = f"<standby log unavailable: {exc}>"
            (case.root / "motherduck-child-output.log").write_text(
                output, encoding="utf-8"
            )
            (case.root / "standby-server-log-tail.log").write_text(
                server_log, encoding="utf-8"
            )
            interesting = "\n".join(
                line
                for line in output.splitlines()
                if any(
                    token in line
                    for token in (
                        "ERROR",
                        "WARN",
                        "Exception",
                        "exception",
                        "failed",
                        "Failed",
                        "disconnect",
                        "timeout",
                    )
                )
            )
            print("MOTHERDUCK CHILD DIAGNOSTICS:\n" + interesting, flush=True)
            print("STANDBY SERVER LOG TAIL:\n" + server_log, flush=True)
            raise AssertionError(
                "NEVER_ARMED: the standby MotherDuck child exited before the "
                f"durable row/state/ledger/resume tuple for {sentinel!r} "
                f"(returncode={process.returncode})\n{interesting}\n{server_log}"
            )
        result = _md_receipt(md, case, sentinel)
        if result is None:
            return False
        confirmed = topology.local_slot_confirmed_lsn()
        return confirmed is not None and confirmed >= result["offset_last_lsn"]

    _wait_until(committed, timeout=timeout, description=f"MotherDuck commit for {sentinel}")
    assert result is not None
    return result


def _service_env(service_id: str) -> dict[str, str]:
    # MotherDuck's independent consistency reads can take longer than the local
    # DuckDB service's liveness budget.  Keep the same fail-closed ordering, but
    # give this remote destination a bounded budget that is still below the lease
    # expiry; these values do not refresh liveness and do not weaken the callback
    # or commit guards.
    return {
        "CDC_SERVICE_ID": service_id,
        "CDC_SERVICE_LEASE_TTL": "180",
        "CDC_SERVICE_LEASE_RENEW_SECONDS": "20",
        "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "60",
        "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "120",
        "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "10",
        "CDC_SERVICE_COMMIT_TIMEOUT": "90",
        "CDC_SERVICE_CLOSE_TIMEOUT": "60",
        "CDC_CLOSE_TIMEOUT": "60",
        "CDC_ENGINE_THREAD_TIMEOUT": "60",
    }


def _stop_if_running(process) -> None:
    if process is None or process.poll() is not None:
        return
    process.kill()
    process.wait(timeout=30)


def test_standby_motherduck_commit_ack_and_replay_boundary(
    tmp_path_factory,
    standby_topology: StandbyTopology,
    motherduck_module_case: dict[str, str],
):
    """Prove one atomic MotherDuck commit and a replay before slot acknowledgement."""
    topology = standby_topology
    md = motherduck_module_case
    case = topology.make_case(
        tmp_path_factory.mktemp("p72_replica_motherduck_case"), name="motherduck"
    )
    md_env = {
        "CDC_DATASET": md["dataset"],
        "CDC_MD_DATABASE": md["database"],
        "CDC_CONTROL_SCHEMA": md["control_schema"],
        "MOTHERDUCK_TOKEN": md["token"],
        "motherduck_token": md["token"],
    }
    baseline = case.invoke(
        destination="motherduck",
        reset_state=True,
        max_seconds=300,
        idle_seconds=10,
        timeout=900,
        extra_env=md_env,
    )
    assert baseline["ok"] is True, baseline
    assert baseline["source_role"] == "standby", baseline
    assert baseline["source_routes"]["read_replication_dsn"] == topology.standby_dsn, baseline
    assert baseline["source_routes"]["source_write_dsn"] == topology.primary_dsn, baseline
    assert baseline["source_routes"]["slot_owner_dsn"] == topology.standby_dsn, baseline
    assert baseline["engine_effective_configuration"]["heartbeat.action.query"] == "", baseline
    assert baseline["engine_effective_configuration"]["heartbeat_action_disabled"] is True, baseline

    process = None
    replay_process = None
    restart_process = None
    sentinels: list[str] = []
    try:
        process = case.spawn_service(
            destination="motherduck",
            extra_env={**md_env, **_service_env("p72-md-first")},
            capture=True,
        )
        first_armed = _arm_stream(case, topology, md, process, baseline, "md_first")
        first_sentinel, first_lsn = _insert_sentinel(topology, "p72_md_first")
        sentinels.append(first_sentinel)
        first_receipt = _wait_md_committed(
            case, topology, md, process, first_sentinel
        )
        _mark_fired(first_armed, sentinel=first_sentinel, source_lsn=first_lsn)
        _write_durable(
            case.root / "MD_COMMITTED.json",
            {"event": "MD_COMMITTED", "sentinel": first_sentinel, **first_receipt},
        )
        first_confirmed = topology.local_slot_confirmed_lsn()
        assert first_confirmed is not None and first_confirmed >= first_receipt["offset_last_lsn"]
        assert case.terminate(process, timeout=120) == 0
        process = None

        # The fault is not present in the first service. It is armed only after
        # STREAM_ARMED and MD_COMMITTED have both been durably witnessed above.
        replay_process = case.spawn_service(
            destination="motherduck",
            extra_env={
                **md_env,
                **_service_env("p72-md-replay"),
                "CDC_FAULT_INJECT": "post_commit_pre_ack:1",
            },
        )
        replay_armed = _arm_stream(case, topology, md, replay_process, baseline, "md_replay")
        second_sentinel, second_lsn = _insert_sentinel(topology, "p72_md_replay")
        sentinels.append(second_sentinel)

        def replay_fault_fired() -> bool:
            if replay_process.poll() is not None:
                # The child can only be classified FIRED after the fsynced anchor
                # record names this exact post-commit/pre-ack cut.
                fired = case.fired_fault()
                if fired is None or fired.get("point") != "post_commit_pre_ack":
                    raise AssertionError(
                        "NEVER_ARMED: replay child exited without the named "
                        f"post-arm fault witness (returncode={replay_process.returncode})"
                    )
            fired = case.fired_fault()
            return fired is not None and fired.get("point") == "post_commit_pre_ack"

        _wait_until(
            replay_fault_fired,
            timeout=300,
            description="post-arm MotherDuck post_commit_pre_ack witness",
        )
        replay_rc = replay_process.wait(timeout=60)
        assert replay_rc != 0, {"returncode": replay_rc, "fault": case.fired_fault()}
        replay_process = None
        _mark_fired(replay_armed, sentinel=second_sentinel, source_lsn=second_lsn)
        second_receipt = _md_receipt(md, case, second_sentinel)
        assert second_receipt is not None, (
            "FIRED: the destination commit did not leave the complete durable "
            f"tuple for {second_sentinel!r}"
        )
        crash_confirmed = topology.local_slot_confirmed_lsn()
        assert crash_confirmed is not None, second_receipt
        assert crash_confirmed <= first_receipt["offset_last_lsn"], {
            "confirmed_after_replay_crash": crash_confirmed,
            "first": first_receipt,
            "second": second_receipt,
        }
        assert crash_confirmed < second_receipt["offset_last_lsn"], {
            "confirmed_after_replay_crash": crash_confirmed,
            "second": second_receipt,
        }

        # Reopen the same production pipeline. Its event ledger makes the replay a
        # no-op while the acknowledgement now advances the standby-owned slot.
        restart_process = case.spawn_service(
            destination="motherduck",
            extra_env={**md_env, **_service_env("p72-md-restart")},
        )
        restart_armed = _arm_stream(
            case, topology, md, restart_process, baseline, "md_restart"
        )
        replayed = _wait_md_committed(
            case, topology, md, restart_process, second_sentinel
        )
        _mark_fired(restart_armed, sentinel=second_sentinel, source_lsn=second_lsn)
        assert replayed["event_id"] == second_receipt["event_id"], replayed
        assert replayed["commit_id"] == second_receipt["commit_id"], replayed
        assert topology.local_slot_confirmed_lsn() >= second_receipt["offset_last_lsn"]
        assert case.terminate(restart_process, timeout=120) == 0
        restart_process = None

        for sentinel in sentinels:
            rows = _q(
                md["token"],
                md["database"],
                f"SELECT count(*) FROM {_quoted(md['dataset'])}.cdcflight_app_customers "
                "WHERE name = ?",
                (sentinel,),
            )
            assert rows == [(1,)], (sentinel, rows)
    finally:
        _stop_if_running(process)
        _stop_if_running(replay_process)
        _stop_if_running(restart_process)
        topology.wait_for_slot_inactive(timeout=60)
        if sentinels:
            placeholders = ", ".join("%s" for _ in sentinels)
            topology.primary_sql(
                f"DELETE FROM app.customers WHERE name IN ({placeholders})",
                tuple(sentinels),
            )
