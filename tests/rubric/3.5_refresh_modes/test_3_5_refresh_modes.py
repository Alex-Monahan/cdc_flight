"""§3.5 tests: CDC-only, scheduled full, and scheduled incremental per table."""

from __future__ import annotations

import duckdb
import psycopg
import pytest
from support.backfill_lab import require_backfill

from cdc_flight.backfill import BackfillCoordinator, RefreshScheduler, StockSignalWriter


def test_each_refresh_mode_is_a_closed_per_table_domain():
    """Proves the three requested modes are durable configuration values."""
    backfill = require_backfill()
    assert backfill.REFRESH_MODES == ("cdc", "full", "incremental")
    for mode in backfill.REFRESH_MODES:
        policy = backfill.RefreshPolicy("app", "customers", mode=mode)
        assert policy.mode == mode


def test_schedule_admission_does_not_destroy_active_progress():
    """A mode change while loading preserves run id, shadow, cursor, and rows."""
    backfill = require_backfill()
    coordinator = backfill.RefreshCoordinator()
    run = coordinator.start("app", "customers", mode="incremental", cursor=7)
    changed = coordinator.change_mode("app", "customers", "full")
    assert changed.mode == "full"
    assert coordinator.active_run("app", "customers").run_id == run.run_id
    assert coordinator.active_run("app", "customers").cursor == 7


def test_full_mode_reuses_shadow_swap_and_incremental_mode_uses_stock_signal():
    """Proves mode selection changes acquisition only, never the publication path."""
    backfill = require_backfill()
    coordinator = backfill.RefreshCoordinator()
    assert coordinator.acquisition_for("full") == "blocking_stock_resnapshot"
    assert coordinator.acquisition_for("incremental") == "stock_signal"
    assert coordinator.publication_for("full") == "shadow_atomic_swap"
    assert coordinator.publication_for("incremental") == "shadow_atomic_swap"


def test_keyless_incremental_result_selects_the_existing_full_fallback():
    """The stock NO_PRIMARY_KEY boundary is explicit; no arrival ordinal is invented."""
    backfill = require_backfill()
    decision = backfill.capability_decision("NO_PRIMARY_KEY", requested_mode="incremental")
    assert decision.effective_mode == "full"
    assert decision.reason == "stock_no_primary_key"
    assert decision.stable_cursor is False


def test_durable_mode_change_preserves_active_cursor():
    """A policy edit changes future acquisition, never the active run progress."""
    import duckdb

    from cdc_flight.control_schema import ensure_control_schema

    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = require_backfill().BackfillCoordinator(con, pipeline="modes")
        run = coordinator.prepare(
            coordinator.request("app", "customers", mode="incremental")
        )
        coordinator.repository.update_progress(
            run, last_key_json='{"id":4}', chunks=1, rows=4
        )
        policy = coordinator.set_mode("app", "customers", "full")
        persisted = coordinator.repository.get(run.run_id)
        assert policy.mode == "full"
        assert coordinator.policies.get("app", "customers").mode == "full"
        assert persisted.last_processed_key_json == '{"id":4}'
        assert persisted.state == "loading"
    finally:
        con.close()


def test_scheduler_dispatches_incremental_signal_and_full_request_without_ack_ownership():
    """The scheduler selects acquisition mode while one coordinator owns durability."""
    import duckdb

    from cdc_flight.control_schema import ensure_control_schema

    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = require_backfill().BackfillCoordinator(con, pipeline="scheduler")
        scheduler = require_backfill().RefreshScheduler(coordinator)
        signal, incremental = scheduler.request_tables(
            ("app.customers",), mode="incremental", signal_id="scheduled-signal"
        )
        assert signal.signal_id == "scheduled-signal"
        assert incremental[0].effective_mode == "incremental"
        full_signal, full = scheduler.request_tables(
            ("app.orders",), mode="full", request_id="full-request"
        )
        assert full_signal is None
        assert full[0].effective_mode == "full"
        assert coordinator.active("app", "customers").state == "requested"
        assert coordinator.active("app", "orders").state == "requested"
    finally:
        con.close()


def test_scheduler_policy_and_successor_work_survive_process_restart(tmp_path):
    """Mode policy, active progress, and a queued successor rehydrate from disk."""
    import duckdb

    from cdc_flight.control_schema import ensure_control_schema

    path = tmp_path / "scheduler-restart.duckdb"
    backfill = require_backfill()
    con = duckdb.connect(str(path))
    try:
        ensure_control_schema(con)
        first = backfill.BackfillCoordinator(con, pipeline="restart-modes")
        scheduler = backfill.RefreshScheduler(first)
        scheduler.configure(
            backfill.RefreshPolicy("app", "customers", mode="incremental", interval_seconds=15)
        )
        scheduler.configure(backfill.RefreshPolicy("app", "orders", mode="full"))
        scheduler.configure(backfill.RefreshPolicy("app", "audit_log", mode="cdc"))
        signal, incremental = scheduler.request_tables(
            ("app.customers",), mode="incremental", signal_id="restart-signal"
        )
        assert signal.queued is False
        assert incremental[0].state == "requested"
        full_signal, full = scheduler.request_tables(
            ("app.orders",), mode="full", request_id="restart-full"
        )
        assert full_signal is None
        assert full[0].effective_mode == "full"
        queued, no_runs = scheduler.request_tables(
            ("app.audit_log",), mode="incremental", signal_id="queued-after-restart",
            request_id="queued-after-restart",
        )
        assert queued.queued is True
        assert no_runs == ()
    finally:
        con.close()

    reopened = duckdb.connect(str(path))
    try:
        ensure_control_schema(reopened)
        second = backfill.BackfillCoordinator(reopened, pipeline="restart-modes")
        assert second.policies.get("app", "customers").mode == "incremental"
        assert second.policies.get("app", "orders").mode == "full"
        assert second.policies.get("app", "audit_log").mode == "cdc"
        active = {run.source_table: run for run in second.active_runs()}
        assert active["customers"].signal_id == "restart-signal"
        assert active["orders"].effective_mode == "full"
        assert second.signal_queue.queued()[0].request_id == "queued-after-restart"
    finally:
        reopened.close()


@pytest.mark.slow
def test_live_restart_consumes_full_and_incremental_modes_independently(sandbox):
    """A restart runs the durable full hand-off and the stock signal side by side."""
    sandbox.reseed()
    tables = "customers,orders,audit_log"
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=150,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline
    sandbox.sql("UPDATE app.orders SET total_amount = 999.99 WHERE id = 1")

    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        from cdc_flight.control_schema import ensure_control_schema

        ensure_control_schema(con, "_cdc_flight")
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        scheduler = RefreshScheduler(
            coordinator,
            signal_writer=StockSignalWriter(
                sandbox.source.dsn,
                data_collection="app.cdc_flight_signal",
            ),
        )
        scheduler.configure(
            require_backfill().RefreshPolicy("app", "customers", mode="incremental")
        )
        scheduler.configure(require_backfill().RefreshPolicy("app", "orders", mode="full"))
        scheduler.configure(require_backfill().RefreshPolicy("app", "audit_log", mode="cdc"))
        full_signal, full_runs = scheduler.request_tables(
            ("app.orders",), mode="full", request_id="live-full-request"
        )
        assert full_signal is None
        assert full_runs[0].state == "requested"
        signal, incremental_runs = scheduler.request_tables(
            ("app.customers",),
            mode="incremental",
            request_id="live-incremental-request",
            signal_id="live-incremental-signal",
        )
        assert signal.queued is False
        assert incremental_runs[0].effective_mode == "incremental"

    process = sandbox.spawn(
        max_seconds=300,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
        capture=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=300)
        assert process.returncode == 0, (stdout[-5000:], stderr[-8000:])
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)

    summary = sandbox.last_summary()
    assert "app.orders" in summary["scheduled_full_refresh"]["resnapshot_swapped"]
    runs = sandbox.duck_query(
        "SELECT source_table, state, effective_mode FROM _cdc_flight.backfill_runs "
        "WHERE request_id IN ('live-full-request', 'live-incremental-request') "
        "ORDER BY source_table"
    )
    assert runs == [("customers", "complete", "incremental"), ("orders", "complete", "full")]
    assert sandbox.duck_query(
        "SELECT count(*) FROM _cdc_flight.backfill_runs "
        "WHERE source_table = 'audit_log'"
    ) == [(0,)]

    with psycopg.connect(sandbox.source.dsn) as source:
        source_orders = source.execute(
            "SELECT id, customer_id, status::text, total_amount, currency, note "
            "FROM app.orders ORDER BY id"
        ).fetchall()
        source_customers = source.execute(
            "SELECT id, name, email FROM app.customers ORDER BY id"
        ).fetchall()
    assert sandbox.duck_query(
        'SELECT id, customer_id, status, total_amount, currency, note '
        'FROM "cdc_raw"."cdcflight_app_orders" ORDER BY id'
    ) == source_orders
    assert sandbox.duck_query(
        'SELECT id, name, email FROM "cdc_raw"."cdcflight_app_customers" ORDER BY id'
    ) == source_customers
