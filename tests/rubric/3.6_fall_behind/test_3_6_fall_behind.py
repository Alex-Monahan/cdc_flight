"""§3.6 tests: byte OR time fall-behind with durable trigger reasons."""

from __future__ import annotations

import time

import duckdb
import pytest
from support.backfill_lab import require_backfill
from support.fixtures import kill_walsender

from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.source_health import SourceHealth


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


@pytest.mark.slow
def test_live_slot_sampler_drives_byte_and_age_requests(sandbox):
    """A real slot sample admits and executes both durable trigger paths."""
    backfill = require_backfill()
    sandbox.reseed()
    baseline = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline

    health = SourceHealth(
        sandbox.source.dsn,
        sandbox.slot,
        interval=0.1,
        query_timeout_ms=2000,
    )
    process = None
    try:
        initial = health.sample_once()
        if not initial.exists or initial.confirmed_pos is None:
            # Keep the fallback bounded: this only repairs an environment in which
            # the supervisor dropped its inactive slot at shutdown.
            process = sandbox.spawn(max_seconds=120, idle_seconds=90, capture=True)
            sandbox.wait_for_slot_active(process=process, timeout=45)
            initial = health.sample_once()
            kill_walsender(sandbox.source, sandbox.slot)
        assert initial.exists is True
        assert initial.confirmed_pos is not None
        if initial.streaming:
            kill_walsender(sandbox.source, sandbox.slot)
        health._ingest(initial)

        oldest_source_ms = sandbox.pg_query(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint"
        )[0][0]
        sandbox.sql(
            "INSERT INTO app.audit_log (occurred_at, actor, action, payload) "
            "SELECT '2026-08-15T00:00:00Z'::timestamptz, 'lag-test', 'insert', "
            "jsonb_build_object('n', g) FROM generate_series(1, 10000) AS s(g)",
            one_transaction=True,
        )
        current_lsn = sandbox.pg_query(
            "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
        )[0][0]
        after_write = health.sample_once()
        health._ingest(after_write)
        assert after_write.exists is True
        assert after_write.confirmed_pos == initial.confirmed_pos
        assert current_lsn > after_write.confirmed_pos

        # The timestamp is a source-side observation of an admitted pending unit;
        # it is not the last-applied timestamp.  Both samples are folded through
        # the production SourceHealth object before durable admission.
        now_ms = int(time.time() * 1000)
        assert health.fall_behind_reason(
            current_wal_lsn=current_lsn,
            oldest_pending_source_ts_ms=None,
            now_ms=now_ms,
            size_threshold_bytes=1,
            time_threshold_ms=None,
        ) == "bytes"
        assert health.fall_behind_reason(
            current_wal_lsn=current_lsn,
            oldest_pending_source_ts_ms=oldest_source_ms - 10_000,
            now_ms=now_ms,
            size_threshold_bytes=None,
            time_threshold_ms=1,
        ) == "time"

        byte_db = duckdb.connect(str(sandbox.duckdb_path))
        try:
            ensure_control_schema(byte_db)
            byte_coordinator = backfill.BackfillCoordinator(
                byte_db,
                pipeline=sandbox.env["CDC_PIPELINE_NAME"],
                control_schema="_cdc_flight",
                topic_prefix="cdcflight",
            )
            byte_result = byte_coordinator.admit_fall_behind(
                ("app.customers",),
                current_wal_lsn=current_lsn,
                confirmed_flush_lsn=after_write.confirmed_pos,
                oldest_pending_source_ts_ms=None,
                now_ms=now_ms,
                size_threshold_bytes=1,
                time_threshold_ms=None,
                request_id="live-bytes-request",
                signal_id="live-bytes-signal",
            )
            assert byte_result[1][0].trigger_reason == "bytes"
            assert byte_coordinator.repository.get(
                byte_result[1][0].run_id
            ).trigger_reason == "bytes"
            backfill.StockSignalWriter(
                sandbox.source.dsn,
                data_collection="app.cdc_flight_signal",
            ).insert(byte_result[0])
        finally:
            byte_db.close()

        first = sandbox.run(max_seconds=240, idle_seconds=6)
        assert first["stop_reason"] in {"idle", "engine_finished"}, first
        assert sandbox.duck_query(
            "SELECT state, trigger_reason FROM _cdc_flight.backfill_runs "
            "WHERE request_id = 'live-bytes-request'"
        ) == [("complete", "bytes")]

        # Create a second real pending source unit after the first trigger has
        # completed.  Its observed admission time is supplied to SourceHealth; it
        # is not derived from the last applied destination timestamp.
        pending_source_ms = sandbox.pg_query(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint"
        )[0][0]
        sandbox.sql(
            "INSERT INTO app.audit_log (occurred_at, actor, action, payload) "
            "VALUES (clock_timestamp(), 'lag-age-test', 'insert', '{\"n\":1}'::jsonb)",
            one_transaction=True,
        )
        age_lsn = sandbox.pg_query(
            "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
        )[0][0]
        age_sample = health.sample_once()
        health._ingest(age_sample)
        assert age_sample.confirmed_pos is not None
        assert age_lsn >= age_sample.confirmed_pos
        age_now_ms = int(time.time() * 1000)
        assert health.fall_behind_reason(
            current_wal_lsn=age_lsn,
            oldest_pending_source_ts_ms=pending_source_ms - 10_000,
            now_ms=age_now_ms,
            size_threshold_bytes=None,
            time_threshold_ms=1,
        ) == "time"

        age_db = duckdb.connect(str(sandbox.duckdb_path))
        try:
            ensure_control_schema(age_db)
            age_coordinator = backfill.BackfillCoordinator(
                age_db,
                pipeline=sandbox.env["CDC_PIPELINE_NAME"],
                control_schema="_cdc_flight",
                topic_prefix="cdcflight",
            )
            age_result = age_coordinator.admit_fall_behind(
                ("app.orders",),
                current_wal_lsn=age_lsn,
                confirmed_flush_lsn=age_sample.confirmed_pos,
                oldest_pending_source_ts_ms=pending_source_ms - 10_000,
                now_ms=age_now_ms,
                size_threshold_bytes=None,
                time_threshold_ms=1,
                request_id="live-age-request",
                signal_id="live-age-signal",
            )
            assert age_result[1][0].trigger_reason == "time"
            assert age_coordinator.repository.get(
                age_result[1][0].run_id
            ).trigger_reason == "time"
            backfill.StockSignalWriter(
                sandbox.source.dsn,
                data_collection="app.cdc_flight_signal",
            ).insert(age_result[0])
        finally:
            age_db.close()
        second = sandbox.run(max_seconds=240, idle_seconds=6)
        assert second["stop_reason"] in {"idle", "engine_finished"}, second
        assert sandbox.duck_query(
            "SELECT state, trigger_reason FROM _cdc_flight.backfill_runs "
            "WHERE request_id = 'live-age-request'"
        ) == [("complete", "time")]
    finally:
        health.stop()
        if process is not None and process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)
