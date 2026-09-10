"""§3.6 MotherDuck proof: callback admission precedes durable slot progress."""

from __future__ import annotations

import contextlib
import json
import signal
import subprocess
import threading
import time

import duckdb
import pytest

from cdc_flight import naming
from cdc_flight.backfill import BackfillCoordinator, RefreshPolicy, RefreshScheduler
from cdc_flight.control_schema import ensure_control_schema

pytestmark = [
    pytest.mark.slow,
    pytest.mark.motherduck,
    pytest.mark.xdist_group("md_3_6_health_admission"),
]

TABLES = "customers,orders"
NAMESPACE = "cdc-flight-engine"
REFRESH = "FORCE CHECKPOINT"


def _dsn(case: dict[str, str]) -> str:
    return f"md:{case['database']}?motherduck_token={case['token']}"


def _with_md(case: dict[str, str], callback):
    # Match the MotherDuck fixture's plain connection configuration. The service
    # owns the configured writer; this observer only refreshes a read snapshot.
    con = duckdb.connect(_dsn(case))
    try:
        con.execute(REFRESH)
        return callback(con)
    finally:
        con.close()


def _configure_policy(case: dict[str, str], pipeline: str) -> None:
    def configure(con):
        ensure_control_schema(con, case["control_schema"])
        coordinator = BackfillCoordinator(
            con,
            pipeline=pipeline,
            control_schema=case["control_schema"],
            topic_prefix="cdcflight",
        )
        scheduler = RefreshScheduler(coordinator)
        scheduler.configure(
            RefreshPolicy(
                "app",
                "customers",
                mode="incremental",
                size_threshold_bytes=1,
            )
        )
        scheduler.configure(
            RefreshPolicy("app", "orders", mode="incremental", enabled=False)
        )

    _with_md(case, configure)


def _source_signals(sandbox, excluded: set[str]) -> list[tuple]:
    return [
        row
        for row in sandbox.pg_query(
            "SELECT id, type, data FROM app.cdc_flight_signal "
            "WHERE type = 'execute-snapshot' ORDER BY id"
        )
        if row[0] not in excluded
    ]


def _md_snapshot(case: dict[str, str], pipeline: str, signal_id: str | None = None):
    control = naming.control_table(case["control_schema"], "backfill_runs")
    intents = naming.control_table(case["control_schema"], "backfill_signal_intents")
    admissions = naming.control_table(case["control_schema"], "source_health_admissions")
    facts = naming.control_table(case["control_schema"], "source_data_facts")
    offsets = naming.control_table(case["control_schema"], "debezium_offsets")
    commits = naming.control_table(case["control_schema"], "commit_log")
    target = naming.destination_table("cdcflight", "app", "customers")

    def read(con):
        run_sql = (
            f"SELECT source_schema || '.' || source_table, state, trigger_reason, "
            "signal_id, effective_mode, notification_status, last_processed_key_json, "
            f"row_count, last_source_lsn FROM {control} "
            "WHERE pipeline = ?"
        )
        run_params: list[object] = [pipeline]
        if signal_id is not None:
            run_sql += " AND signal_id = ?"
            run_params.append(signal_id)
        run_sql += " ORDER BY source_schema, source_table"
        runs = con.execute(run_sql, run_params).fetchall()
        intent = None
        admission = None
        if signal_id is not None:
            intent = con.execute(
                f"SELECT state, kind, tables_json FROM {intents} "
                "WHERE pipeline = ? AND signal_id = ?",
                [pipeline, signal_id],
            ).fetchone()
            admission = con.execute(
                f"SELECT confirmed_flush_lsn, current_wal_lsn, observation_id, "
                f"signal_id, trigger_reason, source_data_lsn FROM {admissions} "
                "WHERE pipeline = ? AND signal_id = ?",
                [pipeline, signal_id],
            )
            admission = admission.fetchall()
        source_facts = con.execute(
            f"SELECT commit_id, source_lsn, source_ts_ms FROM {facts} "
            "WHERE pipeline = ? AND source_schema = 'app' AND source_table = 'customers' "
            "ORDER BY commit_id",
            [pipeline],
        ).fetchall()
        offset = con.execute(
            f"SELECT commit_id, last_lsn, resume_json FROM {offsets} "
            "WHERE pipeline = ? AND namespace = ?",
            [pipeline, NAMESPACE],
        ).fetchone()
        commit_rows = con.execute(
            f"SELECT commit_id, first_lsn, last_lsn, event_count FROM {commits} "
            "WHERE pipeline = ? ORDER BY commit_id",
            [pipeline],
        ).fetchall()
        try:
            data = con.execute(
                f"SELECT id, name, email, lifetime_value, is_active, cdcf_commit_id "
                f"FROM {naming.quote(case['dataset'])}.{naming.quote(target)} "
                "WHERE id IN (1, 6) ORDER BY id"
            ).fetchall()
        except duckdb.Error:
            data = []
        return {
            "runs": runs,
            "intent": intent,
            "admissions": admission,
            "facts": source_facts,
            "offset": offset,
            "commits": commit_rows,
            "data": data,
        }

    return _with_md(case, read)


def _wait_for(predicate, *, sandbox, process=None, timeout: float = 300.0):
    deadline = time.monotonic() + timeout
    while True:
        with contextlib.suppress(Exception):
            value = predicate()
            if value:
                return value
        if process is not None and process.poll() is not None:
            raise AssertionError(
                "the normal MotherDuck service exited before callback admission: "
                f"returncode={process.returncode}; summary={sandbox.last_summary()}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError(
                "timed out waiting for callback-connected MotherDuck admission: "
                f"summary={sandbox.last_summary()}"
            )
        time.sleep(0.2)


def _stop_service(process) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=30)
    process.communicate(timeout=5)


def _start_service(sandbox, case: dict[str, str], live_state):
    return sandbox.spawn_service(
        destination="motherduck",
        capture=False,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_TABLES": TABLES,
            "CDC_DATASET": case["dataset"],
            "CDC_MD_DATABASE": case["database"],
            "CDC_CONTROL_SCHEMA": case["control_schema"],
            "MOTHERDUCK_TOKEN": case["token"],
            "motherduck_token": case["token"],
            # The invariant loop remains five seconds; policy/health work remains
            # on the inherited 25-second owner poll.
            "CDC_SERVICE_INVARIANT_CHECK_SECONDS": "5",
            "CDC_SERVICE_LEASE_TTL": "240",
            "CDC_SERVICE_LEASE_RENEW_SECONDS": "5",
            "CDC_SERVICE_HEARTBEAT_BOUND_SECONDS": "20",
            "CDC_SOURCE_DARK_SECONDS": "180",
            "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "180",
            "CDC_SERVICE_STALL_EXIT_GRACE_SECONDS": "5",
            "CDC_SERVICE_COMMIT_TIMEOUT": "120",
            "CDC_COMMIT_TIMEOUT": "120",
            "CDC_SERVICE_CLOSE_TIMEOUT": "30",
            "CDC_CLOSE_TIMEOUT": "30",
            "CDC_ENGINE_THREAD_TIMEOUT": "30",
            "CDC_ACK_EVERY_RECORD": "1",
            "CDC_BACKFILL_LIVE_STATE_PATH": str(live_state),
        },
    )


def test_real_motherduck_health_callback_commits_data_state_and_cursor_before_slot(
    sandbox, motherduck_module_case
):
    """The real sampler/owner path publishes a signal before a durable slot move."""
    case = motherduck_module_case
    pipeline = sandbox.env["CDC_PIPELINE_NAME"]
    env = {
        "CDC_DATASET": case["dataset"],
        "CDC_MD_DATABASE": case["database"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
    }

    sandbox.reseed()
    baseline = sandbox.run(
        reset_state=True,
        destination="motherduck",
        max_seconds=240,
        idle_seconds=6,
        timeout=600,
        extra_env={**env, "CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": TABLES},
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline
    _configure_policy(case, pipeline)

    live_state = sandbox.dir / "motherduck_backfill_live_state.jsonl"
    live_state.unlink(missing_ok=True)
    process = None
    observer_stop = threading.Event()
    observed: list[dict] = []
    try:
        before = {row[0] for row in _source_signals(sandbox, set())}
        process = _start_service(sandbox, case, live_state)
        sandbox.wait_for_slot_active(process=process, timeout=90)
        slot_before = sandbox.pg_query(
            "SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (sandbox.slot,),
        )[0][0]
        observed_signal_id: str | None = None

        def observe() -> None:
            while not observer_stop.is_set():
                with contextlib.suppress(Exception):
                    observed.append(_md_snapshot(case, pipeline, observed_signal_id))
                observer_stop.wait(0.25)

        watcher = threading.Thread(target=observe, name="md-health-observer", daemon=True)
        watcher.start()
        # This is the only event the test supplies. It is source data, not a
        # destination admission or a StockSignalWriter call.
        sandbox.sql(
            "UPDATE app.customers SET name = 'p3d-md-health-customer' WHERE id = 1; "
            "INSERT INTO app.customers (id, name, email) VALUES "
            "(6, 'p3d-md-health-new', 'p3d-md-health-new@example.com')",
            one_transaction=True,
        )

        signals = _wait_for(
            lambda: _source_signals(sandbox, before),
            sandbox=sandbox,
            process=process,
            timeout=360,
        )
        assert len(signals) == 1, signals
        signal_id, signal_type, payload = signals[0]
        observed_signal_id = signal_id
        assert signal_type == "execute-snapshot"
        assert json.loads(payload)["data-collections"] == ["app.customers"]

        admitted = _wait_for(
            lambda: (
                snapshot
                if (
                    (snapshot := _md_snapshot(case, pipeline, signal_id))["admissions"]
                    and snapshot["intent"] is not None
                    and snapshot["intent"][0] == "published"
                )
                else None
            ),
            sandbox=sandbox,
            process=process,
            timeout=360,
        )
        final = _wait_for(
            lambda: (
                snapshot
                if (
                    (snapshot := _md_snapshot(case, pipeline, signal_id))["data"]
                    and len(snapshot["runs"]) == 1
                    and snapshot["runs"][0][1] == "complete"
                    and snapshot["runs"][0][5] == "COMPLETED"
                    and snapshot["runs"][0][6] is not None
                    and snapshot["facts"]
                )
                else None
            ),
            sandbox=sandbox,
            process=process,
            timeout=360,
        )
        observer_stop.set()
        watcher.join(timeout=20)
        assert not watcher.is_alive()

        _stop_service(process)
        process = None
        summary = sandbox.last_summary()

        # The service summary identifies the actual sampler observation and the
        # destination owner. The test supplied neither admission nor publication.
        admissions = [
            item
            for item in summary.get("source_health_admissions", [])
            if item.get("signal_id") == signal_id and item.get("admitted")
        ]
        assert admissions, summary
        admission = admissions[0]
        assert admission["owner"] == "service-destination-owner"
        assert admission["source_effect_route"] == "reconcile_signal_effects"
        assert admission["source_signal_inserted_directly"] is False
        assert admission["published_signal_ids"] == [signal_id]
        assert admission["reasons"] == {"app.customers": "bytes"}

        run = final["runs"][0]
        assert run[0] == "app.customers"
        assert run[2] == "bytes"
        assert run[3] == signal_id
        assert run[4] == "incremental"
        assert final["intent"] == ("published", "admission", '["app.customers"]')
        source_rows = sandbox.pg_query(
            "SELECT id, name, email, lifetime_value, is_active FROM app.customers "
            "WHERE id IN (1, 6) ORDER BY id"
        )
        assert [row[:5] for row in final["data"]] == source_rows

        admission_row = admitted["admissions"]
        assert len(admission_row) == 1
        _confirmed, _current, _observation_id, admitted_signal, reason, source_lsn = admission_row[0]
        assert admitted_signal == signal_id
        assert reason == "bytes"
        assert source_lsn is not None
        facts = [fact for fact in final["facts"] if int(fact[1]) == int(source_lsn)]
        assert facts and all(fact[2] is not None for fact in facts), {
            "admission": admission_row,
            "facts": final["facts"],
            "offset": final["offset"],
            "runs": final["runs"],
        }

        # One MD snapshot contains the replacement row, complete backfill state,
        # exact reason, cursor, published intent and the source-data fact. The
        # observer must never see a target row with a partial terminal image.
        complete_observations = [
            item
            for item in observed
            if item["data"]
            and any(row[0] == 6 for row in item["data"])
            and item["intent"] is not None
        ]
        assert complete_observations
        terminal_image_observations = [
            item
            for item in complete_observations
            if item["runs"] and item["runs"][0][1] == "complete"
        ]
        assert terminal_image_observations
        for item in terminal_image_observations:
            # The ordinary CDC row and the requested run can legitimately precede
            # the final swap. The terminal image must contain the complete run,
            # exact reason and cursor together with the replacement row.
            assert item["intent"] == final["intent"]
            assert len(item["runs"]) == 1
            assert item["runs"][0][1:] == final["runs"][0][1:]

        # The destination commit/ack trace is process evidence, not a test-side
        # timestamp. Every fact is tied to a commit observed before acknowledgement.
        trace = summary.get("destination_commit_ack_trace", [])
        assert trace, summary
        trace_by_commit = {int(entry["commit_id"]): entry for entry in trace}
        assert {int(fact[0]) for fact in facts} <= set(trace_by_commit)
        assert all(
            entry["committed_at_monotonic"] <= entry["acknowledged_at_monotonic"]
            for entry in trace
        )
        assert summary.get("commit_to_slot_confirmation", {}).get("confirmed") is True
        slot_after = sandbox.pg_query(
            "SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (sandbox.slot,),
        )[0][0]
        assert int(slot_after) >= max(int(fact[1]) for fact in facts)
        assert int(slot_after) >= int(slot_before)
        assert final["offset"] is not None
        assert final["offset"][1] >= max(int(fact[1]) for fact in facts)
        commit_ids = {int(commit[0]) for commit in final["commits"]}
        assert {int(row[5]) for row in final["data"] if row[5] is not None} <= commit_ids
    finally:
        observer_stop.set()
        if "watcher" in locals():
            watcher.join(timeout=20)
        _stop_service(process)
