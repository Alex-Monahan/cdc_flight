"""§3.6 live proof: SourceHealth queues, the service admits, stock publishes."""

from __future__ import annotations

import contextlib
import json
import signal
import subprocess
import time

import duckdb
import pytest

from cdc_flight import naming
from cdc_flight.backfill import BackfillCoordinator, RefreshPolicy, RefreshScheduler
from cdc_flight.control_schema import ensure_control_schema

pytestmark = pytest.mark.slow

TABLES = "customers,orders"
TABLE_NAMES = ("customers", "orders")
AUDIT_ROWS = 2_000


def _configure_policy(
    sandbox,
    table: str,
    *,
    size_threshold: int | None,
    time_threshold: int | None,
) -> None:
    """Persist only the policy; the running service owns health admission."""
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
                mode="incremental",
                size_threshold_bytes=size_threshold,
                time_threshold_ms=time_threshold,
            )
        )
        # The module intentionally reuses one destination file across its three
        # bounded subcases. Disable the prior subcase's policy so a later health
        # observation cannot select an unrelated table and obscure the target's
        # exact durable reason.
        for other in TABLE_NAMES:
            if other == table:
                continue
            RefreshScheduler(coordinator).configure(
                RefreshPolicy("app", other, mode="incremental", enabled=False)
            )


def _wait_for(predicate, *, sandbox, process=None, timeout: float = 240.0):
    deadline = time.monotonic() + timeout
    while True:
        with contextlib.suppress(Exception):
            value = predicate()
            if value:
                return value
        if process is not None and process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                "normal stock service exited before the production health admission: "
                f"returncode={process.returncode}\n"
                f"summary={sandbox.last_summary()}\n"
                f"stdout={(stdout or '')[-4000:]}\n"
                f"stderr={(stderr or '')[-8000:]}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError(
                "timed out waiting for the production health admission; "
                f"summary={sandbox.last_summary()}"
            )
        time.sleep(0.1)


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


def _source_signal_rows(sandbox, excluded_ids: set[str] | None = None) -> list[tuple]:
    excluded_ids = excluded_ids or set()
    return [
        row
        for row in sandbox.pg_query(
        "SELECT id, type, data FROM app.cdc_flight_signal "
        "WHERE type = 'execute-snapshot' ORDER BY id"
        )
        if row[0] not in excluded_ids
    ]


def _facts(sandbox, table: str, *, after_lsn: int | None = None) -> list[tuple]:
    query = (
        "SELECT source_lsn, source_ts_ms FROM _cdc_flight.source_data_facts "
        "WHERE pipeline = ? AND source_schema = 'app' AND source_table = ?"
    )
    params: list[object] = [sandbox.env["CDC_PIPELINE_NAME"], table]
    if after_lsn is not None:
        query += " AND source_lsn > ?"
        params.append(after_lsn)
    return sandbox.duck_query(query + " ORDER BY commit_id", params)


def _runs(sandbox, signal_id: str) -> list[tuple]:
    return sandbox.duck_query(
        "SELECT source_schema || '.' || source_table, state, trigger_reason, "
        "signal_id, effective_mode, notification_status, last_processed_key_json "
        "FROM _cdc_flight.backfill_runs "
        "WHERE pipeline = ? AND signal_id = ? ORDER BY source_schema, source_table",
        [sandbox.env["CDC_PIPELINE_NAME"], signal_id],
    )


def _service_completion_witness(path, signal_id: str) -> list[dict] | None:
    """Return the service's completion-side notification, not a test-side event."""
    rows = _live_state_for_signal(path, signal_id)
    if any(
        row.get("notification_status") == "COMPLETED"
        and row.get("state") in {"complete", "ready_to_swap", "swapping"}
        for row in rows
    ):
        return rows
    return None


def _live_state_for_signal(path, signal_id: str) -> list[dict]:
    """Read the service's append-only backfill witness without opening DuckDB."""
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            row = json.loads(line)
            if row.get("signal_id") == signal_id:
                rows.append(row)
    return rows


def _baseline(sandbox) -> None:
    sandbox.reseed()
    result = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": TABLES},
    )
    assert result["stop_reason"] in {"idle", "engine_finished"}, result


def _start_service(sandbox, live_state):
    return sandbox.spawn_service(
        capture=False,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_TABLES": TABLES,
            # The invariant loop remains on its inherited five-second cadence;
            # policy/health work is throttled by the production 25-second poll.
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
            # This keeps the normal stock commit/ack boundary observable while a
            # large but bounded source transaction is delivered. It adds no
            # connector or source owner and does not publish an admission.
            "CDC_ACK_EVERY_RECORD": "1",
            "CDC_BACKFILL_LIVE_STATE_PATH": str(live_state),
        },
    )


def _source_transaction(sandbox, table: str) -> None:
    if table == "customers":
        statement = (
            "UPDATE app.customers SET name = 'p3d-health-customer' WHERE id = 1; "
            "INSERT INTO app.customers (id, name, email) VALUES "
            "(6, 'p3d-health-new', 'p3d-health-new@example.com')"
        )
    elif table == "orders":
        # The age-only case uses a delivered orders row. Its Debezium source
        # timestamp is known and naturally ages past the 1 ms threshold while the
        # normal service waits for its policy poll.
        statement = "UPDATE app.orders SET total_amount = 987.65 WHERE id = 1"
    elif table == "audit_log":
        statement = (
            "UPDATE app.orders SET total_amount = 987.65 WHERE id = 1; "
            "INSERT INTO app.audit_log (occurred_at, actor, action, payload) "
            "SELECT '2026-08-15T00:00:00Z'::timestamptz, 'p3d-health', 'insert', "
            "jsonb_build_object('n', g) FROM generate_series(1, "
            f"{AUDIT_ROWS}) AS rows(g)"
        )
    else:  # pragma: no cover - the three cases are fixed below
        raise AssertionError(table)
    sandbox.sql(statement, one_transaction=True)


def _assert_source_result(sandbox, table: str) -> None:
    if table == "customers":
        source = sandbox.pg_query(
            "SELECT id, name, email, lifetime_value, is_active "
            "FROM app.customers WHERE id = 1"
        )
        destination = sandbox.duck_query(
            f'SELECT id, name, email, lifetime_value, is_active FROM "cdc_raw"."'
            f'{naming.destination_table("cdcflight", "app", "customers")}" WHERE id = 1'
        )
        assert destination == source
        assert sandbox.duck_query(
            f'SELECT id, name, email FROM "cdc_raw"."'
            f'{naming.destination_table("cdcflight", "app", "customers")}" WHERE id = 6'
        ) == sandbox.pg_query(
            "SELECT id, name, email FROM app.customers WHERE id = 6"
        )
    elif table == "orders":
        source = sandbox.pg_query(
            "SELECT id, customer_id, status, total_amount, currency, note "
            "FROM app.orders WHERE id = 1"
        )
        destination = sandbox.duck_query(
            f'SELECT id, customer_id, status, total_amount, currency, note FROM "cdc_raw"."'
            f'{naming.destination_table("cdcflight", "app", "orders")}" WHERE id = 1'
        )
        assert destination == source
    else:
        assert table == "audit_log"
        source = sandbox.pg_query(
            "SELECT count(*), min(id), max(id) FROM app.audit_log "
            "WHERE actor = 'p3d-health'"
        )[0]
        destination = sandbox.duck_query(
            f'SELECT count(*), min(id), max(id) FROM "cdc_raw"."'
            f'{naming.destination_table("cdcflight", "app", "audit_log")}" '
            "WHERE actor = 'p3d-health'"
        )[0]
        assert destination == source
        assert source[0] == AUDIT_ROWS


def _run_subcase(
    sandbox,
    *,
    table: str,
    size_threshold: int | None,
    time_threshold: int | None,
    expected_reason: str,
) -> dict:
    _baseline(sandbox)
    _configure_policy(
        sandbox,
        table,
        # This is deliberately a policy-only setup; no test-side admission or
        # signal writer is involved. The normal service must observe the source
        # transaction and commit its source-data fact before the age predicate can
        # select it; an unknown age therefore remains a refusal in production.
        size_threshold=size_threshold,
        time_threshold=time_threshold,
    )
    live_state = sandbox.dir / "backfill_live_state.jsonl"
    live_state.unlink(missing_ok=True)
    process = None
    try:
        signal_ids_before = {row[0] for row in _source_signal_rows(sandbox)}
        process = _start_service(sandbox, live_state)
        sandbox.wait_for_slot_active(process=process, timeout=60)
        _source_transaction(sandbox, table)

        signals = _wait_for(
            lambda: _source_signal_rows(sandbox, signal_ids_before),
            sandbox=sandbox,
            process=process,
            timeout=300,
        )
        assert len(signals) == 1, signals
        signal_id, signal_type, payload = signals[0]
        assert signal_type == "execute-snapshot"
        payload = json.loads(payload)
        assert payload["data-collections"] == [f"app.{table}"]

        # Wait for a notification emitted by the running destination owner. The
        # sidecar is only a service-produced progress witness; the durable run is
        # read after shutdown, when the service has released its DuckDB writer.
        _wait_for(
            lambda: _service_completion_witness(live_state, signal_id),
            sandbox=sandbox,
            process=process,
            timeout=300,
        )
        _stop_service(process)
        process = None
        rows = _runs(sandbox, signal_id)
        assert len(rows) == 1 and rows[0][1] == "complete" and rows[0][5] == "COMPLETED"
        summary = sandbox.last_summary()
        admissions = [
            item
            for item in summary.get("source_health_admissions", [])
            if item.get("admitted") and item.get("signal_id") == signal_id
        ]
        assert admissions, summary
        admission = next(
            item
            for item in admissions
            if item.get("owner") == "service-destination-owner"
            and item.get("source_effect_route") == "reconcile_signal_effects"
            and item.get("source_signal_inserted_directly") is False
            and item.get("published_signal_ids") == [signal_id]
            and item.get("reasons") == {f"app.{table}": expected_reason}
        )
        assert admission
        assert any(
            item.get("owner") == "service-destination-owner"
            and item.get("source_effect_route") == "reconcile_signal_effects"
            and item.get("source_signal_inserted_directly") is False
            and item.get("published_signal_ids") == [signal_id]
            and item.get("reasons") == {f"app.{table}": expected_reason}
            for item in admissions
        ), admissions
        assert rows[0][0] == f"app.{table}"
        assert rows[0][2] == expected_reason
        assert rows[0][3] == signal_id
        assert rows[0][4] == "incremental"
        assert rows[0][6] is not None
        _assert_source_result(sandbox, table)

        facts = _facts(
            sandbox,
            table,
            after_lsn=int(admission["confirmed_flush_lsn"]),
        )
        assert facts and all(lsn is not None and timestamp is not None for lsn, timestamp in facts)
        assert all(int(lsn) > int(admission["confirmed_flush_lsn"]) for lsn, _ in facts)
        trace = summary.get("destination_commit_ack_trace", [])
        assert trace, summary
        trace_by_lsn = {int(entry["source_lsn"]): entry for entry in trace}
        fact_lsns = {int(lsn) for lsn, _timestamp in facts}
        assert fact_lsns <= set(trace_by_lsn)
        assert all(
            entry["committed_at_monotonic"] <= entry["acknowledged_at_monotonic"]
            for entry in trace
        )
        final_confirmed = sandbox.pg_query(
            "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::BIGINT "
            "FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (sandbox.slot,),
        )[0][0]
        assert final_confirmed is not None
        assert int(final_confirmed) >= max(fact_lsns)
        assert summary.get("commit_to_slot_confirmation", {}).get("confirmed") is True
        queue = summary["source_health_observation_queue"]
        assert queue["bounded_capacity"] == 1
        assert queue["published"] > 0 and queue["consumed"] > 0
        return {
            "table": table,
            "reason": expected_reason,
            "signal_id": signal_id,
            "admissions": admissions,
            "summary": summary,
        }
    finally:
        _stop_service(process)


def test_real_source_health_callback_creates_size_age_and_both_admission(sandbox):
    """The normal service creates all three production health-admission shapes."""
    size = _run_subcase(
        sandbox,
        table="customers",
        size_threshold=1,
        time_threshold=None,
        expected_reason="bytes",
    )
    age = _run_subcase(
        sandbox,
        table="customers",
        # Keep the initial age policy below the eventual threshold only after a
        # durable fact exists; the selected policy itself is age-only.
        size_threshold=None,
        time_threshold=1,
        expected_reason="time",
    )
    both = _run_subcase(
        sandbox,
        table="customers",
        size_threshold=1,
        time_threshold=1,
        expected_reason="both",
    )
    assert [size["reason"], age["reason"], both["reason"]] == [
        "bytes",
        "time",
        "both",
    ]
