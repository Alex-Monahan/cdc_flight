"""Slow stock-connector proof for the §3 incremental destination route."""

from __future__ import annotations

import json
from decimal import Decimal

import duckdb
import psycopg
import pytest

from cdc_flight import destination
from cdc_flight.backfill import BackfillCoordinator, identity_set, value_multiset

pytestmark = pytest.mark.slow


def test_stock_signal_runs_arbitrary_set_while_streaming(sandbox):
    """A durable request and stock signal keep selected and unselected CDC live."""
    sandbox.reseed()
    baseline = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline

    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        destination.ensure_control_schema(con, "_cdc_flight")
        destination.ensure_dataset(con, sandbox.DATASET)
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        signal, runs = coordinator.request_tables(
            ("app.customers", "app.orders"),
            request_id="p3-stock-request",
            signal_id="p3-stock-signal",
        )
        assert signal.tables == ("app.customers", "app.orders")
        assert {run.source_table for run in runs} == {"customers", "orders"}

    process = sandbox.spawn(max_seconds=240, idle_seconds=90, capture=True)
    # The contended one-reader acquisition p99/max is 61.604 s; 74 s is its
    # measured 20%-headroom bound, shared with the source-task start budget.
    sandbox.wait_for_slot_active(process=process, timeout=74)
    payload = json.dumps(
        {
            "data-collections": list(signal.tables),
            "type": "incremental",
        },
        separators=(",", ":"),
    )
    # The signal is source data and the ordinary writes below are deliberately
    # independent transactions.  They prove the live CDC route is still active
    # while stock scans the requested tables.
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as source:
        source.execute(
            "INSERT INTO app.cdc_flight_signal (id, type, data) VALUES (%s, %s, %s)",
            ("p3-stock-signal", "execute-snapshot", payload),
        )
    # One whole PostgreSQL transaction deliberately contains UPDATE, DELETE (and
    # its FK cascade), and INSERT while the stock scan is live.  The final identity
    # and value oracles below catch a lost delete or a duplicate row that a count
    # assertion would miss.
    with psycopg.connect(sandbox.source.dsn) as source, source.transaction():
        source.execute(
            "UPDATE app.customers SET name = %s, updated_at = now() WHERE id = 1",
            ("p3-stock-live",),
        )
        source.execute("DELETE FROM app.customers WHERE id = 2")
        source.execute(
            "INSERT INTO app.orders (customer_id, total_amount, currency) "
            "VALUES (1, 777.77, 'USD')"
        )

    stdout, stderr = process.communicate(timeout=240)
    assert process.returncode == 0, (stdout[-3000:], stderr[-6000:])
    summary = sandbox.last_summary()
    assert summary["stop_reason"] in {"idle", "engine_finished"}, summary

    runs = sandbox.duck_query(
        "SELECT source_table, state, signal_id FROM _cdc_flight.backfill_runs "
        "ORDER BY source_table"
    )
    assert runs == [("customers", "complete", "p3-stock-signal"), ("orders", "complete", "p3-stock-signal")]

    customers = sandbox.duck_query(
        'SELECT id, name FROM "cdc_raw"."cdcflight_app_customers" WHERE id = 1'
    )
    orders = sandbox.duck_query(
        'SELECT total_amount FROM "cdc_raw"."cdcflight_app_orders" '
        "WHERE total_amount = 777.77"
    )
    assert customers == [(1, "p3-stock-live")]
    assert orders == [(Decimal("777.77"),)]

    # Counts alone can hide one omission plus one wrong row. Compare stable source
    # identities and value multisets for both selected tables after publication.
    with psycopg.connect(sandbox.source.dsn) as source:
        source_customers = source.execute(
            "SELECT id, name, email, lifetime_value, is_active "
            "FROM app.customers ORDER BY id"
        ).fetchall()
        source_orders = source.execute(
            "SELECT id, customer_id, status, total_amount, currency, note "
            "FROM app.orders ORDER BY id"
        ).fetchall()
    destination_customers = sandbox.duck_query(
        'SELECT id, name, email, lifetime_value, is_active '
        'FROM "cdc_raw"."cdcflight_app_customers" ORDER BY id'
    )
    destination_orders = sandbox.duck_query(
        'SELECT id, customer_id, status, total_amount, currency, note '
        'FROM "cdc_raw"."cdcflight_app_orders" ORDER BY id'
    )
    assert identity_set(destination_customers) == identity_set(source_customers)
    assert value_multiset(destination_customers) == value_multiset(source_customers)
    assert identity_set(destination_orders) == identity_set(source_orders)
    assert value_multiset(destination_orders) == value_multiset(source_orders)
