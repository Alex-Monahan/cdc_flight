"""§3.6 tests: byte OR time fall-behind with durable trigger reasons."""

from __future__ import annotations

import duckdb
from support.backfill_lab import require_backfill

from cdc_flight.control_schema import ensure_control_schema


def test_size_and_time_thresholds_use_or_semantics():
    """Either configured lag predicate must request a backfill."""
    backfill = require_backfill()
    assert backfill.fall_behind_reason(
        current_wal_lsn=200, confirmed_flush_lsn=100,
        oldest_pending_source_ts_ms=None, now_ms=10_000,
        size_threshold_bytes=50, time_threshold_ms=5_000,
    ) == "bytes"
    assert backfill.fall_behind_reason(
        current_wal_lsn=101, confirmed_flush_lsn=100,
        oldest_pending_source_ts_ms=1_000, now_ms=10_000,
        size_threshold_bytes=50, time_threshold_ms=5_000,
    ) == "time"
    assert backfill.fall_behind_reason(
        current_wal_lsn=200, confirmed_flush_lsn=100,
        oldest_pending_source_ts_ms=1_000, now_ms=10_000,
        size_threshold_bytes=50, time_threshold_ms=5_000,
    ) == "both"


def test_unknown_pending_age_is_not_fabricated_from_last_applied_time():
    """No pending source timestamp means time is unknown/false, never stale-age trigger."""
    backfill = require_backfill()
    assert backfill.fall_behind_reason(
        current_wal_lsn=101, confirmed_flush_lsn=100,
        oldest_pending_source_ts_ms=None, now_ms=10_000,
        size_threshold_bytes=50, time_threshold_ms=1,
        last_applied_source_ts_ms=0,
    ) is None


def test_scheduler_persists_reason_and_never_advances_the_slot():
    """The scheduler creates durable work only; the applier owns source acknowledgement."""
    backfill = require_backfill()
    scheduler = backfill.FallBehindScheduler()
    run = scheduler.admit("app.customers", reason="both", confirmed_flush_lsn=100)
    assert run.trigger_reason == "both"
    assert scheduler.confirmed_flush_lsn == 100
    assert scheduler.slot_advance_calls == 0


def test_backoff_and_hysteresis_coalesce_duplicate_requests():
    """Repeated lag samples retain one active run and a retry deadline."""
    backfill = require_backfill()
    scheduler = backfill.FallBehindScheduler()
    first = scheduler.admit("app.customers", reason="bytes", confirmed_flush_lsn=100)
    second = scheduler.admit("app.customers", reason="time", confirmed_flush_lsn=100)
    assert second.run_id == first.run_id
    assert set(second.trigger_reasons) == {"bytes", "time"}
    assert second.retry_at is not None


def test_durable_coordinator_admits_or_trigger_without_advancing_the_slot():
    """The production coordinator persists the OR reason and leaves acknowledgement elsewhere."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="lag")
        signal, runs = coordinator.admit_fall_behind(
            ("app.customers", "app.audit_log"),
            current_wal_lsn=200,
            confirmed_flush_lsn=100,
            oldest_pending_source_ts_ms=None,
            now_ms=10_000,
            size_threshold_bytes=50,
            time_threshold_ms=5_000,
            request_id="lag-request",
            signal_id="lag-signal",
        )
        assert signal.tables == ("app.customers", "app.audit_log")
        assert {run.trigger_reason for run in runs} == {"bytes"}
        assert {run.request_id for run in runs} == {"lag-request"}
        assert coordinator.active_runs()
    finally:
        con.close()
