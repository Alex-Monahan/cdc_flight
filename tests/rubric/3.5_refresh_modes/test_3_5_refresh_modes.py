"""§3.5 tests: CDC-only, scheduled full, and scheduled incremental per table."""

from __future__ import annotations

from support.backfill_lab import require_backfill


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
