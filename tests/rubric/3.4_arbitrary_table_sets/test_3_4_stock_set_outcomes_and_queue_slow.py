"""§3.4 live stock proof for isolated arbitrary-set outcomes and queue dispatch."""

from __future__ import annotations

import contextlib
import json
import os
import re
import time
from dataclasses import asdict

import duckdb
import psycopg
import pytest
from support.live_stock import (
    BoundedSourceCommitLedger,
    LiveStockHarness,
    SourceTransactionWriter,
    assert_exact_rows,
    value_multiset,
)

from cdc_flight import destination, naming
from cdc_flight.backfill import (
    BackfillCoordinator,
    RefreshScheduler,
    StockSignalWriter,
)

pytestmark = pytest.mark.slow

HEALTHY_TABLE = "customers"
EMPTY_TABLE = "orders"
FAILED_TABLE = "sensor_readings"
PEER_TABLE = "wide_types"
ACTIVE_TABLE = "documents"


def _worker_namespace(label: str) -> tuple[str, str, str]:
    raw_worker = os.environ.get("PYTEST_XDIST_WORKER", "serial")
    worker = re.sub(r"[^a-z0-9_]", "_", raw_worker.lower()).strip("_") or "serial"
    return (
        f"_cdc_flight_{label}_{worker}",
        f"cdc_raw_{label}_{worker}",
        f"cdc_flight_{label}_{worker}",
    )


def _configure(sandbox, label: str) -> tuple[str, str, str]:
    control_schema, dataset, pipeline = _worker_namespace(label)
    sandbox.env.update(
        {
            "CDC_PIPELINE_NAME": pipeline,
            "CDC_CONTROL_SCHEMA": control_schema,
            "CDC_DATASET": dataset,
        }
    )
    return control_schema, dataset, pipeline


def _baseline(sandbox, captured_tables: str) -> None:
    sandbox.reseed()
    result = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": captured_tables},
    )
    assert result["stop_reason"] in {"idle", "engine_finished"}, result


def _live_entries(path) -> list[dict]:
    if not path.exists():
        return []
    entries = []
    for line in path.read_text(encoding="utf-8").splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            entries.append(json.loads(line))
    return entries


def _wait_for_entry(path, predicate, *, process=None, timeout: float = 180.0) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        matches = [entry for entry in _live_entries(path) if predicate(entry)]
        if matches:
            return matches[-1]
        if process is not None and process.poll() is not None:
            stdout, stderr = process.communicate()
            entries = _live_entries(path)
            raise AssertionError(
                "production stock exited before the required durable sidecar state: "
                f"returncode={process.returncode}, stdout={stdout[-3000:]!r}, "
                f"stderr={stderr[-6000:]!r}, sidecar={entries[-12:]}"
            )
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"required durable sidecar state did not appear within {timeout:.1f}s; "
                f"entries={_live_entries(path)[-12:]}"
            )
        time.sleep(0.1)


def _wait_for_source_successor(sandbox, *, first_signal_id: str, timeout: float = 180.0):
    deadline = time.monotonic() + timeout
    while True:
        rows = sandbox.pg_query(
            "SELECT id, type, data FROM app.cdc_flight_signal "
            "WHERE id LIKE 'queued-%' ORDER BY id"
        )
        if rows:
            if len(rows) != 1:
                raise AssertionError(f"duplicate successor source signals: {rows}")
            signal_id, signal_type, payload = rows[0]
            assert signal_id != first_signal_id
            assert signal_type == "execute-snapshot"
            return str(signal_id), json.loads(payload)
        if time.monotonic() >= deadline:
            raise AssertionError("production owner did not publish a queued successor signal")
        time.sleep(0.2)


def _target(dataset: str, table: str) -> str:
    return naming.destination_table("cdcflight", "app", table)


def _terminal_entry(path, signal_id: str, table: str, *, process=None) -> dict:
    qualified = f"app.{table}"
    return _wait_for_entry(
        path,
        lambda entry: (
            entry.get("signal_id") == signal_id
            and entry.get("table") == qualified
            and entry.get("observation") == "TABLE_SCAN_COMPLETED"
        ),
        process=process,
    )


def _source_and_destination_images(
    sandbox,
    dataset: str,
    tables: tuple[str, ...] = (HEALTHY_TABLE, EMPTY_TABLE, FAILED_TABLE, PEER_TABLE),
) -> dict[str, tuple[list, list]]:
    source_queries = {
        HEALTHY_TABLE: (
            "SELECT id, name, email, lifetime_value, is_active "
            "FROM app.customers ORDER BY id"
        ),
        EMPTY_TABLE: (
            "SELECT id, customer_id, status, total_amount, currency, note "
            "FROM app.orders ORDER BY id"
        ),
        FAILED_TABLE: (
            "SELECT sensor_id, reading_at, value, unit, meta "
            "FROM app.sensor_readings ORDER BY sensor_id, reading_at, value"
        ),
        PEER_TABLE: "SELECT id, col_text FROM app.wide_types ORDER BY id",
    }
    destination_queries = {
        HEALTHY_TABLE: (
            f'SELECT id, name, email, lifetime_value, is_active '
            f'FROM "{dataset}"."{_target(dataset, HEALTHY_TABLE)}" ORDER BY id'
        ),
        EMPTY_TABLE: (
            f'SELECT id, customer_id, status, total_amount, currency, note '
            f'FROM "{dataset}"."{_target(dataset, EMPTY_TABLE)}" ORDER BY id'
        ),
        FAILED_TABLE: (
            f'SELECT sensor_id, reading_at, value, unit, meta '
            f'FROM "{dataset}"."{_target(dataset, FAILED_TABLE)}" '
            "ORDER BY sensor_id, reading_at, value"
        ),
        PEER_TABLE: (
            f'SELECT id, col_text FROM "{dataset}"."{_target(dataset, PEER_TABLE)}" '
            "ORDER BY id"
        ),
    }
    with psycopg.connect(sandbox.source.dsn) as source:
        source_rows = {
            table: source.execute(source_queries[table]).fetchall() for table in tables
        }
    destination_rows = {
        table: sandbox.duck_query(destination_queries[table]) for table in tables
    }
    return {
        table: (source_rows[table], destination_rows[table])
        for table in tables
    }


def _assert_run_and_claim_state(
    sandbox,
    control_schema: str,
    pipeline: str,
    request_id: str,
    signal_id: str,
    run_ids: tuple[str, ...],
) -> list[tuple]:
    runs = sandbox.duck_query(
        f'SELECT run_id, request_id, source_table, state, effective_mode, error_code, '
        f'notification_status, signal_id, shadow_table, last_processed_key_json '
        f'FROM "{control_schema}"."backfill_runs" '
        "WHERE pipeline = ? AND request_id = ? ORDER BY source_table",
        [pipeline, request_id],
    )
    assert len(runs) == len(run_ids)
    assert {row[0] for row in runs} == set(run_ids)
    assert {row[1] for row in runs} == {request_id}
    assert {row[7] for row in runs} == {signal_id}
    claims = sandbox.duck_query(
        f'SELECT source_table, claim_state FROM "{control_schema}"."shadow_claims" '
        "WHERE pipeline = ? AND source_table IN (?, ?, ?) ORDER BY source_table",
        [pipeline, HEALTHY_TABLE, EMPTY_TABLE, FAILED_TABLE],
    )
    assert claims == [
        (HEALTHY_TABLE, "free"),
        (EMPTY_TABLE, "free"),
        (FAILED_TABLE, "free"),
    ]
    return runs


def test_live_set_isolates_empty_failed_and_healthy_tables(sandbox):
    """One real stock set publishes peers independently, including NO_PRIMARY_KEY."""
    control_schema, dataset, pipeline = _configure(sandbox, "set_outcomes")
    captured_tables = f"{HEALTHY_TABLE},{EMPTY_TABLE},{FAILED_TABLE},{PEER_TABLE}"
    _baseline(sandbox, captured_tables)
    sandbox.sql(
        [
            "UPDATE app.customers SET name = 'p3b-healthy' WHERE id = 1",
            "DELETE FROM app.orders",
        ],
        one_transaction=True,
    )

    request_id = "p3b-set-outcomes-request"
    signal_id = "p3b-set-outcomes-signal"
    selected = tuple(f"app.{table}" for table in (HEALTHY_TABLE, EMPTY_TABLE, FAILED_TABLE))
    harness = LiveStockHarness(
        sandbox,
        control_schema=control_schema,
        dataset=dataset,
        pipeline=pipeline,
        source_table=HEALTHY_TABLE,
        signal_tables=selected,
        request_id=request_id,
        signal_id=signal_id,
    )
    process = None
    ledger: BoundedSourceCommitLedger | None = None
    try:
        admitted_signal, run_ids = harness.admit_and_publish()
        assert admitted_signal == signal_id
        assert len(run_ids) == 3
        harness.start_normal_stock(captured_tables=captured_tables)
        process = harness.process
        boundary = harness.wait_for_scan_boundary()
        assert boundary.state == "loading"
        assert boundary.table_state == "in_progress"

        ledger = BoundedSourceCommitLedger.open(sandbox, slot_prefix="p3b_ledger")
        writer = SourceTransactionWriter(sandbox.source.dsn, label_prefix="p3b-")
        writer.commit(
            "p3b-peer-one",
            "ordinary_cdc",
            "UPDATE app.wide_types SET col_smallint = %s, col_text = %s WHERE id = 1",
            (31001, "p3b-peer-one"),
        )
        writer.commit(
            "p3b-peer-two",
            "ordinary_cdc",
            "UPDATE app.wide_types SET col_smallint = %s, col_text = %s WHERE id = 1",
            (31002, "p3b-peer-two"),
        )
        source_commits = ledger.read(
            {"p3b-peer-one", "p3b-peer-two"},
            evidence={
                "p3b-peer-one": "new-tuple: id[integer]:1 col_smallint[smallint]:31001",
                "p3b-peer-two": "new-tuple: id[integer]:1 col_smallint[smallint]:31002",
            },
        )
        assert ledger.consuming_reads == 1
        assert len(source_commits) == 2
        assert len({fact.xid for fact in source_commits.values()}) == 2

        healthy_terminal = _terminal_entry(
            harness.live_state_path, signal_id, HEALTHY_TABLE, process=process
        )
        empty_terminal = _terminal_entry(
            harness.live_state_path, signal_id, EMPTY_TABLE, process=process
        )
        failed_terminal = _terminal_entry(
            harness.live_state_path, signal_id, FAILED_TABLE, process=process
        )
        assert healthy_terminal["state"] in {"ready_to_swap", "complete"}
        assert healthy_terminal["notification_status"] in {"TABLE_SCAN_COMPLETED", "COMPLETED"}
        assert empty_terminal["state"] == "complete"
        assert empty_terminal["notification_status"] in {"TABLE_SCAN_COMPLETED", "COMPLETED"}
        assert failed_terminal["state"] == "blocked"
        assert failed_terminal["notification_status"] in {"TABLE_SCAN_COMPLETED", "COMPLETED"}
        assert healthy_terminal["observed_at_monotonic_ns"] < failed_terminal[
            "observed_at_monotonic_ns"
        ]
        assert empty_terminal["observed_at_monotonic_ns"] < failed_terminal[
            "observed_at_monotonic_ns"
        ]

        stdout, stderr = process.communicate(timeout=240)
        assert process.returncode != 0, (stdout[-3000:], stderr[-6000:])
        summary = sandbox.last_summary()
        trace = summary.get("backfill_notification_trace", [])
        assert any(
            entry.get("signal_id") == signal_id
            and entry.get("table") == f"app.{HEALTHY_TABLE}"
            for entry in trace
        )
        assert any(
            entry.get("signal_id") == signal_id
            and entry.get("table") == f"app.{FAILED_TABLE}"
            and entry.get("status") == "NO_PRIMARY_KEY"
            for entry in trace
        )
        trace_status = {
            entry["table"]: entry.get("status")
            for entry in trace
            if entry.get("signal_id") == signal_id
            and entry.get("observation") == "TABLE_SCAN_COMPLETED"
        }
        assert trace_status[f"app.{HEALTHY_TABLE}"] in {"SUCCEEDED", "COMPLETED"}
        assert trace_status[f"app.{EMPTY_TABLE}"] == "EMPTY"
        assert trace_status[f"app.{FAILED_TABLE}"] == "NO_PRIMARY_KEY"

        runs = _assert_run_and_claim_state(
            sandbox, control_schema, pipeline, request_id, signal_id, run_ids
        )
        by_table = {row[2]: row for row in runs}
        assert by_table[HEALTHY_TABLE][3:7] == (
            "complete",
            "incremental",
            None,
            "COMPLETED",
        )
        assert by_table[EMPTY_TABLE][3:7] == (
            "complete",
            "incremental",
            None,
            "COMPLETED",
        )
        assert by_table[FAILED_TABLE][3:7] == (
            "blocked",
            "full",
            "NO_PRIMARY_KEY",
            "TABLE_SCAN_COMPLETED",
        )
        assert by_table[FAILED_TABLE][8]
        assert by_table[FAILED_TABLE][9] is None

        signal_rows = sandbox.pg_query(
            "SELECT id, type, data FROM app.cdc_flight_signal WHERE id = %s",
            (signal_id,),
        )
        assert len(signal_rows) == 1
        assert signal_rows[0][1] == "execute-snapshot"
        assert json.loads(signal_rows[0][2])["data-collections"] == list(selected)

        states = sandbox.duck_query(
            f'SELECT source_table, snapshot_state FROM "{control_schema}"."table_state" '
            "WHERE pipeline = ? AND source_table IN (?, ?, ?) ORDER BY source_table",
            [pipeline, HEALTHY_TABLE, EMPTY_TABLE, FAILED_TABLE],
        )
        assert states == [
            (HEALTHY_TABLE, "complete"),
            (EMPTY_TABLE, "complete"),
            (FAILED_TABLE, "in_progress"),
        ]

        images = _source_and_destination_images(sandbox, dataset)
        assert_exact_rows(*images[HEALTHY_TABLE], label=HEALTHY_TABLE)
        assert_exact_rows(*images[EMPTY_TABLE], label=EMPTY_TABLE)
        assert value_multiset(images[FAILED_TABLE][0]) == value_multiset(
            images[FAILED_TABLE][1]
        )
        assert len(images[FAILED_TABLE][0]) == len(images[FAILED_TABLE][1])
        assert_exact_rows(*images[PEER_TABLE], label=PEER_TABLE)
        assert images[PEER_TABLE][1][-1][1] == "p3b-peer-two"

        failed_target = _target(dataset, FAILED_TABLE)
        failed_shadow = by_table[FAILED_TABLE][8]
        physical = sandbox.duck_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name IN (?, ?) ORDER BY table_name",
            [dataset, failed_target, failed_shadow],
        )
        # The blocked full-fallback route retains the published target image and
        # durable shadow/cursor metadata; its unpromoted physical shadow is cleaned
        # by the existing snapshot session teardown.
        assert physical == [(failed_target,)]
        print(
            "ROUND_B_SET_OUTCOMES_TRACE "
            + json.dumps(
                {
                    "boundary": asdict(boundary),
                    "terminal_entries": [
                        healthy_terminal,
                        empty_terminal,
                        failed_terminal,
                    ],
                    "writer_commits": [asdict(commit) for commit in writer.commits],
                    "source_commits": {
                        label: asdict(fact) for label, fact in source_commits.items()
                    },
                    "ledger": {
                        "slot": ledger.slot,
                        "peek_polls": ledger.peek_polls,
                        "consuming_reads": ledger.consuming_reads,
                    },
                    "notification_trace": trace,
                },
                sort_keys=True,
                default=str,
            )
        )
    finally:
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                process.communicate(timeout=120)
        if ledger is not None:
            ledger.close()


def test_live_queued_sets_coalesce_and_dispatch(sandbox):
    """A live owner publishes one coalesced successor after the first set."""
    control_schema, dataset, pipeline = _configure(sandbox, "queued_sets")
    captured_tables = f"{ACTIVE_TABLE},{HEALTHY_TABLE},{EMPTY_TABLE},{PEER_TABLE}"
    sandbox.reseed()
    sandbox.sql(
        "INSERT INTO app.documents (id, title, body, body_bytes, revision) "
        "SELECT i, 'p3b-queue-' || i::text, 'queue-seed-' || i::text, "
        "length('queue-seed-' || i::text), 1 "
        "FROM generate_series(10000, 21999) AS rows(i)",
        one_transaction=True,
    )
    _baseline(sandbox, captured_tables)

    first_request_id = "p3b-queue-first-request"
    first_signal_id = "p3b-queue-first-signal"
    first_harness = LiveStockHarness(
        sandbox,
        control_schema=control_schema,
        dataset=dataset,
        pipeline=pipeline,
        source_table=ACTIVE_TABLE,
        signal_tables=(f"app.{ACTIVE_TABLE}",),
        request_id=first_request_id,
        signal_id=first_signal_id,
    )
    first_signal, first_run_ids = first_harness.admit_and_publish()
    assert first_signal == first_signal_id
    assert len(first_run_ids) == 1

    queued_request_ids = ("p3b-queue-customers", "p3b-queue-orders")
    queued_signal_ids = ("p3b-queue-customers-signal", "p3b-queue-orders-signal")
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        destination.ensure_control_schema(con, control_schema)
        destination.ensure_dataset(con, dataset)
        coordinator = BackfillCoordinator(
            con,
            pipeline=pipeline,
            control_schema=control_schema,
            topic_prefix="cdcflight",
        )
        scheduler = RefreshScheduler(
            coordinator,
            signal_writer=StockSignalWriter(
                sandbox.source.dsn,
                data_collection="app.cdc_flight_signal",
            ),
        )
        queued_customers, no_runs_customers = scheduler.request_tables(
            (f"app.{HEALTHY_TABLE}",),
            mode="incremental",
            request_id=queued_request_ids[0],
            signal_id=queued_signal_ids[0],
        )
        queued_orders, no_runs_orders = scheduler.request_tables(
            (f"app.{EMPTY_TABLE}",),
            mode="incremental",
            request_id=queued_request_ids[1],
            signal_id=queued_signal_ids[1],
        )
        assert queued_customers.queued is True
        assert queued_orders.queued is True
        assert no_runs_customers == no_runs_orders == ()
        assert coordinator.signal_queue.queued()

    process = None
    ledger: BoundedSourceCommitLedger | None = None
    try:
        first_harness.start_normal_stock(captured_tables=captured_tables)
        process = first_harness.process
        boundary = first_harness.wait_for_scan_boundary()
        assert boundary.state == "loading"

        ledger = BoundedSourceCommitLedger.open(sandbox, slot_prefix="p3b_queue_ledger")
        writer = SourceTransactionWriter(sandbox.source.dsn, label_prefix="p3b-")
        writer.commit(
            "p3b-queue-peer-one",
            "ordinary_cdc",
            "UPDATE app.wide_types SET col_smallint = %s, col_text = %s WHERE id = 1",
            (32001, "p3b-queue-peer-one"),
        )
        writer.commit(
            "p3b-queue-peer-two",
            "ordinary_cdc",
            "UPDATE app.wide_types SET col_smallint = %s, col_text = %s WHERE id = 1",
            (32002, "p3b-queue-peer-two"),
        )
        source_commits = ledger.read(
            {"p3b-queue-peer-one", "p3b-queue-peer-two"},
            evidence={
                "p3b-queue-peer-one": "new-tuple: id[integer]:1 col_smallint[smallint]:32001",
                "p3b-queue-peer-two": "new-tuple: id[integer]:1 col_smallint[smallint]:32002",
            },
        )
        assert ledger.consuming_reads == 1

        first_terminal = _terminal_entry(
            first_harness.live_state_path,
            first_signal_id,
            ACTIVE_TABLE,
            process=process,
        )
        assert first_terminal["state"] in {"ready_to_swap", "complete"}
        successor_id, successor_payload = _wait_for_source_successor(
            sandbox, first_signal_id=first_signal_id
        )
        assert successor_payload == {
            "data-collections": [f"app.{HEALTHY_TABLE}", f"app.{EMPTY_TABLE}"],
            "type": "incremental",
        }
        successor_customers = _terminal_entry(
            first_harness.live_state_path,
            successor_id,
            HEALTHY_TABLE,
            process=process,
        )
        successor_orders = _terminal_entry(
            first_harness.live_state_path,
            successor_id,
            EMPTY_TABLE,
            process=process,
        )
        assert successor_customers["state"] in {"ready_to_swap", "complete"}
        assert successor_orders["state"] in {"ready_to_swap", "complete"}
        assert successor_customers["notification_status"] in {"TABLE_SCAN_COMPLETED", "COMPLETED"}
        assert successor_orders["notification_status"] in {"TABLE_SCAN_COMPLETED", "COMPLETED"}

        stdout, stderr = process.communicate(timeout=300)
        assert process.returncode == 0, (stdout[-3000:], stderr[-6000:])
        summary = sandbox.last_summary()
        dispatches = summary.get("backfill_queue_dispatches", [])
        assert len(dispatches) == 1, dispatches
        assert dispatches[0]["signal_id"] == successor_id
        assert dispatches[0]["tables"] == [
            f"app.{HEALTHY_TABLE}",
            f"app.{EMPTY_TABLE}",
        ]
        assert dispatches[0]["source_signal_inserted"] is True

        queue_rows = sandbox.duck_query(
            f'SELECT request_id, state, dispatch_signal_id, tables_json '
            f'FROM "{control_schema}"."backfill_signal_queue" '
            "WHERE pipeline = ? ORDER BY request_id",
            [pipeline],
        )
        assert queue_rows == [
            (
                queued_request_ids[0],
                "dispatched",
                successor_id,
                json.dumps([f"app.{HEALTHY_TABLE}"], separators=(",", ":")),
            ),
            (
                queued_request_ids[1],
                "dispatched",
                successor_id,
                json.dumps([f"app.{EMPTY_TABLE}"], separators=(",", ":")),
            ),
        ]

        run_rows = sandbox.duck_query(
            f'SELECT source_table, request_id, signal_id, state, notification_status '
            f'FROM "{control_schema}"."backfill_runs" '
            "WHERE pipeline = ? ORDER BY source_table",
            [pipeline],
        )
        assert len(run_rows) == 3
        assert sum(row[2] == successor_id for row in run_rows) == 2
        assert sum(row[2] == first_signal_id for row in run_rows) == 1
        successor_runs = [row for row in run_rows if row[2] == successor_id]
        assert {row[0] for row in successor_runs} == {HEALTHY_TABLE, EMPTY_TABLE}
        assert len({row[1] for row in successor_runs}) == 1
        assert all(row[3:] == ("complete", "COMPLETED") for row in successor_runs)

        source_signal_rows = sandbox.pg_query(
            "SELECT id, data FROM app.cdc_flight_signal "
            "WHERE id IN (%s, %s, %s) OR id LIKE 'queued-%%' ORDER BY id",
            (first_signal_id, queued_signal_ids[0], queued_signal_ids[1]),
        )
        assert {row[0] for row in source_signal_rows} == {
            first_signal_id,
            successor_id,
        }
        assert sum(row[0] == successor_id for row in source_signal_rows) == 1

        images = _source_and_destination_images(
            sandbox, dataset, (HEALTHY_TABLE, EMPTY_TABLE, PEER_TABLE)
        )
        first_target = _target(dataset, ACTIVE_TABLE)
        with psycopg.connect(sandbox.source.dsn) as source:
            source_active = source.execute(
                f"SELECT id, title, body, body_bytes, revision FROM app.{ACTIVE_TABLE} ORDER BY id"
            ).fetchall()
        destination_active = sandbox.duck_query(
            f'SELECT id, title, body, body_bytes, revision '
            f'FROM "{dataset}"."{first_target}" ORDER BY id'
        )
        assert_exact_rows(source_active, destination_active, label=ACTIVE_TABLE)
        assert_exact_rows(*images[HEALTHY_TABLE], label=HEALTHY_TABLE)
        assert_exact_rows(*images[EMPTY_TABLE], label=EMPTY_TABLE)
        assert_exact_rows(*images[PEER_TABLE], label=PEER_TABLE)
        assert images[PEER_TABLE][1][-1][1] == "p3b-queue-peer-two"

        claims = sandbox.duck_query(
            f'SELECT source_table, claim_state FROM "{control_schema}"."shadow_claims" '
            "WHERE pipeline = ? ORDER BY source_table",
            [pipeline],
        )
        assert all(state == "free" for _table, state in claims)
        print(
            "ROUND_B_QUEUE_TRACE "
            + json.dumps(
                {
                    "boundary": asdict(boundary),
                    "first_terminal": first_terminal,
                    "successor_id": successor_id,
                    "successor_payload": successor_payload,
                    "successor_terminals": [successor_customers, successor_orders],
                    "dispatches": dispatches,
                    "writer_commits": [asdict(commit) for commit in writer.commits],
                    "source_commits": {
                        label: asdict(fact) for label, fact in source_commits.items()
                    },
                    "ledger": {
                        "slot": ledger.slot,
                        "peek_polls": ledger.peek_polls,
                        "consuming_reads": ledger.consuming_reads,
                    },
                },
                sort_keys=True,
                default=str,
            )
        )
    finally:
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                process.communicate(timeout=120)
        if ledger is not None:
            ledger.close()
