"""§3.4 tests: one stock signal, independent table runs."""

from __future__ import annotations

import duckdb
import pytest
from support.backfill_lab import require_backfill

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


def test_second_active_signal_cannot_overtake_the_first_request():
    """Stock notification correlation is serialized rather than silently ambiguous."""
    backfill = require_backfill()
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="signals")
        coordinator.request_tables(("app.customers",), signal_id="signal-1")
        with pytest.raises(backfill.BackfillInvariantError, match="second stock signal"):
            coordinator.request_tables(("app.customers",), signal_id="signal-2")
    finally:
        con.close()
