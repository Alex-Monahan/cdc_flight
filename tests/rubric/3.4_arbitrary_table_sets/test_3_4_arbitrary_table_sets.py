"""§3.4 tests: one stock signal, independent table runs."""

from __future__ import annotations

from collections import Counter
from types import SimpleNamespace

import duckdb
import psycopg
import pytest
from support.applier_lab import Lab, begin, data, end, heartbeat
from support.backfill_lab import notification, require_backfill

from cdc_flight.assembler import UNIT_SNAPSHOT_CHUNK, CompleteUnit
from cdc_flight.backfill import BackfillCoordinator, StockSignalWriter
from cdc_flight.control_schema import ensure_control_schema


def test_one_signal_accepts_an_arbitrary_noncontiguous_table_set():
    """Proves signalling is set-based, not a database-wide snapshot toggle."""
    backfill = require_backfill()
    signal = backfill.IncrementalSignal(
        signal_id="s-1",
        tables=("app.customers", "app.sensor_readings", "billing.customers"),
    )
    assert signal.tables == ("app.customers", "app.sensor_readings", "billing.customers")
    assert backfill.encode_signal(signal)["data-collections"] == list(signal.tables)


def test_empty_signal_is_a_durable_noop():
    """An empty arbitrary set must not create a phantom run or clear a table."""
    backfill = require_backfill()
    assert backfill.IncrementalSignal(signal_id="empty", tables=()).is_noop


def test_empty_durable_request_creates_no_run_or_signal():
    """The durable arbitrary-set entry point keeps an empty request a true no-op."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="empty")
        signal, runs = coordinator.request_tables((), signal_id="empty-signal")
        assert signal.is_noop
        assert runs == ()
        assert coordinator.active_runs() == []
    finally:
        con.close()


def test_one_failed_table_does_not_block_completed_peers():
    """Per-table terminal status is independent and preserves the failed shadow."""
    backfill = require_backfill()
    outcomes = backfill.TableSetCoordinator().apply_outcomes(
        {
            "app.customers": ("SUCCEEDED", [(1, "ok")]),
            "app.orders": ("NO_PRIMARY_KEY", []),
            "app.audit_log": ("SUCCEEDED", [(9, "ok")]),
        }
    )
    assert outcomes["app.customers"].state == "complete"
    assert outcomes["app.audit_log"].state == "complete"
    assert outcomes["app.orders"].state in {"retry_wait", "blocked"}
    assert outcomes["app.orders"].shadow_retained is True


def test_durable_request_tables_uses_one_signal_for_independent_runs():
    """Proves an arbitrary set is one stock request with independently durable runs."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(
            con, pipeline="arbitrary", topic_prefix="fleet"
        )
        signal, runs = coordinator.request_tables(
            ("app.customers", "billing.audit_log"),
            request_id="request-1",
            signal_id="signal-1",
        )
        assert signal.tables == ("app.customers", "billing.audit_log")
        assert {run.request_id for run in runs} == {"request-1"}
        assert {run.signal_id for run in runs} == {"signal-1"}
        assert {run.target_table for run in runs} == {
            "fleet_app_customers",
            "fleet_billing_audit_log",
        }
        again, same_runs = coordinator.request_tables(
            signal.tables, request_id="request-1", signal_id="signal-1"
        )
        assert again.signal_id == signal.signal_id
        assert {run.run_id for run in same_runs} == {run.run_id for run in runs}
        assert len(coordinator.active_runs()) == 2
    finally:
        con.close()


def test_second_active_signal_is_durably_coalesced_for_the_next_request():
    """Stock correlation stays serialized while the successor request is retained."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="signals")
        coordinator.request_tables(("app.customers",), signal_id="signal-1")
        queued, runs = coordinator.request_tables(
            ("app.orders",), signal_id="signal-2", request_id="request-2"
        )
        assert queued.queued is True
        assert queued.signal_id == "signal-2"
        assert runs == ()
        assert coordinator.signal_queue.queued()[0].tables == ("app.orders",)
        assert coordinator.dispatch_queued() is None
        coordinator.prepare(coordinator.active("app", "customers"))
        coordinator.complete_swap(
            SimpleNamespace(schema="app", table="customers"),
            snapshot_lsn=17,
            commit_id=1,
        )
        dispatched = coordinator.dispatch_queued()
        assert dispatched is not None
        successor, successor_runs = dispatched
        assert successor.queued is False
        assert successor.tables == ("app.orders",)
        assert {run.signal_id for run in successor_runs} == {successor.signal_id}
        assert coordinator.signal_queue.queued() == []
    finally:
        con.close()


def test_live_empty_incremental_scan_publishes_an_empty_shadow(tmp_path):
    """A zero-READ terminal outcome replaces the old image, rather than no-oping."""
    backfill = require_backfill()
    lab = Lab(tmp_path / "empty.duckdb")
    try:
        lab.run(
            [
                begin("old", 10),
                data("old", 1, 11, key={"id": 1}, after={"id": 1, "value": "old"}),
                end("old", 1, 12, {"app.customers": 1}),
            ]
        )
        run = lab.applier.backfill.prepare(
            lab.applier.backfill.request("app", "customers", mode="incremental")
        )
        lab.applier._ensure_backfill_route("app", "customers", run)
        terminal = backfill.IncrementalNotification(
            observation="TABLE_SCAN_COMPLETED",
            data={"scanned_collection": "app.customers", "status": "EMPTY"},
            signal_id=run.signal_id,
            table="app.customers",
            status="EMPTY",
            rows=0,
            chunk_id="empty",
            last_processed_key=None,
        )
        lab.applier._pending_backfill_notifications.append(terminal)
        record = heartbeat(20)
        record.incremental_rows = 0
        lab.applier._add_unit(
            CompleteUnit(
                kind=UNIT_SNAPSHOT_CHUNK,
                records=[record],
                schema="app",
                table="customers",
                nbytes=10,
                incremental=True,
                snapshot_last_for_table=True,
            )
        )
        lab.commit("incremental_empty")
        assert lab.q(
            f'SELECT id, value FROM "{lab.dataset}"."{lab.target("customers")}"'
        ) == []
        assert lab.applier.backfill.repository.get(run.run_id).state == "complete"
    finally:
        lab.close()


def test_one_stock_table_error_retains_its_run_and_selects_full_fallback():
    """A keyless/error table is isolated; a successful peer remains publishable."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="error-peer")
        _signal, runs = coordinator.request_tables(
            ("app.customers", "app.sensor_readings"), signal_id="error-signal"
        )
        for table in ("app.customers", "app.sensor_readings"):
            coordinator.observe_notification(
                backfill.decode_incremental_notification(
                    notification("STARTED", table=table, signal_id="error-signal")
                )
            )
        coordinator.observe_notification(
            backfill.decode_incremental_notification(
                notification(
                    "TABLE_SCAN_COMPLETED",
                    table="app.customers",
                    rows=0,
                    status="EMPTY",
                    signal_id="error-signal",
                )
            )
        )
        coordinator.observe_notification(
            backfill.decode_incremental_notification(
                notification(
                    "TABLE_SCAN_COMPLETED",
                    table="app.sensor_readings",
                    rows=0,
                    status="NO_PRIMARY_KEY",
                    signal_id="error-signal",
                )
            )
        )
        customers = coordinator.active("app", "customers")
        sensor = coordinator.active("app", "sensor_readings")
        assert customers.state == "ready_to_swap"
        assert sensor.state == "blocked"
        assert sensor.effective_mode == "full"
        assert sensor.error_code == "NO_PRIMARY_KEY"
        assert coordinator.claims.state("app", "sensor_readings")[0] == "free"
        assert {run.source_table for run in runs} == {"customers", "sensor_readings"}
    finally:
        con.close()


@pytest.mark.slow
def test_live_empty_keyed_table_publishes_a_real_empty_image(sandbox):
    """Stock's EMPTY terminal outcome replaces the old live image on PostgreSQL."""
    sandbox.reseed()
    tables = "customers"
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=150,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline
    sandbox.sql(
        ["DELETE FROM app.orders", "DELETE FROM app.customers"],
        one_transaction=True,
    )

    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        from cdc_flight.control_schema import ensure_control_schema

        ensure_control_schema(con, "_cdc_flight")
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        signal, runs = coordinator.request_tables(
            ("app.customers",),
            request_id="live-empty-request",
            signal_id="live-empty-signal",
        )
        assert runs[0].state == "requested"
        StockSignalWriter(
            sandbox.source.dsn,
            data_collection="app.cdc_flight_signal",
        ).insert(signal)

    process = sandbox.spawn(
        max_seconds=240,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
        capture=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=240)
        assert process.returncode == 0, (stdout[-4000:], stderr[-7000:])
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)

    with psycopg.connect(sandbox.source.dsn) as source:
        source_rows = source.execute(
            "SELECT id, name, email, lifetime_value, is_active "
            "FROM app.customers ORDER BY id"
        ).fetchall()
    destination_rows = sandbox.duck_query(
        'SELECT id, name, email, lifetime_value, is_active '
        'FROM "cdc_raw"."cdcflight_app_customers" ORDER BY id'
    )
    assert {row[0] for row in destination_rows} == {row[0] for row in source_rows}
    assert Counter(destination_rows) == Counter(source_rows) == Counter()
    assert sandbox.duck_query(
        "SELECT state, effective_mode, error_code FROM _cdc_flight.backfill_runs "
        "WHERE request_id = 'live-empty-request'"
    ) == [("complete", "incremental", None)]
