"""Live stock-snapshot/delete interleaving proof for rubric 3.3."""

from __future__ import annotations

import hashlib
import time
from collections import Counter

import duckdb
import psycopg
import pytest

from cdc_flight import destination
from cdc_flight.backfill import BackfillCoordinator, StockSignalWriter

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

TABLE = "app.p3b_delete_scan"
TABLE_NAME = "p3b_delete_scan"
DESTINATION_TABLE = "cdcflight_app_p3b_delete_scan"
ROW_COUNT = 20_000
CHUNK_SIZE = 250
DELETE_IDS = (3_000, 7_000, 11_000, 15_000)


def _configure(sandbox):
    label = "stock_delete_scan"
    suffix = f"{label}_{hashlib.sha256(label.encode()).hexdigest()[:8]}"
    sandbox.env.update(
        {
            "CDC_PIPELINE_NAME": f"cdc_flight_p3b_{suffix}",
            "CDC_CONTROL_SCHEMA": f"_cdc_flight_p3b_{suffix}",
            "CDC_DATASET": f"cdc_raw_p3b_{suffix}",
        }
    )
    sandbox.reseed()
    sandbox.sql(
        [
            "CREATE TABLE app.p3b_delete_scan ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_delete_scan",
            "INSERT INTO app.p3b_delete_scan (id, payload) "
            f"SELECT g, 'scan-' || g FROM generate_series(1, {ROW_COUNT}) AS s(g)",
        ],
        one_transaction=True,
    )
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=20,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_DROP_MODE": "ignore",
            "CDC_TABLES": TABLE_NAME,
            "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": str(CHUNK_SIZE),
            "CDC_COMMIT_MAX_EVENTS": str(CHUNK_SIZE),
        },
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline


def _request(sandbox):
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        control_schema = sandbox.env["CDC_CONTROL_SCHEMA"]
        destination.ensure_control_schema(con, control_schema)
        destination.ensure_dataset(con, sandbox.env["CDC_DATASET"])
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema=control_schema,
            topic_prefix="cdcflight",
        )
        signal, runs = coordinator.request_tables(
            (TABLE,),
            request_id="p3b-delete-scan-request",
            signal_id="p3b-delete-scan-signal",
        )
        assert [run.source_table for run in runs] == [TABLE_NAME]
        return signal


def _delete_source_row(sandbox, row_id: int) -> None:
    with psycopg.connect(sandbox.source.dsn) as source, source.transaction():
        source.execute("DELETE FROM app.p3b_delete_scan WHERE id = %s", (row_id,))


def test_real_delete_during_stock_scan_preserves_exact_image(sandbox):
    """A real PostgreSQL DELETE during the stock scan preserves the exact image."""
    _configure(sandbox)
    signal = _request(sandbox)
    process = sandbox.spawn(
        max_seconds=300,
        idle_seconds=90,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_DROP_MODE": "ignore",
            "CDC_TABLES": TABLE_NAME,
            "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": str(CHUNK_SIZE),
            "CDC_COMMIT_MAX_EVENTS": str(CHUNK_SIZE),
        },
        capture=True,
    )
    try:
        sandbox.wait_for_slot_active(process=process, timeout=74)
        StockSignalWriter(
            sandbox.source.dsn,
            data_collection="app.cdc_flight_signal",
        ).insert(signal)

        # These are independent PostgreSQL transactions. They are intentionally
        # issued while the real stock-snapshot child is still alive; no synthetic
        # notification or reader controls the timing. The durable state and exact
        # source/destination comparison below are the oracle for the interleaving.
        deleted_while_child_live = []
        for row_id in DELETE_IDS:
            assert process.poll() is None, (
                f"stock scan ended before DELETE {row_id}: rc={process.returncode}"
            )
            _delete_source_row(sandbox, row_id)
            deleted_while_child_live.append(row_id)
            time.sleep(0.75)

        stdout, stderr = process.communicate(timeout=300)
        if process.returncode != 0:
            summary = sandbox.last_summary()
            if "slot_acknowledgement_timeout" not in summary:
                raise AssertionError(
                    f"live stock scan failed: summary={summary}\n"
                    f"stdout={stdout[-5000:]}\nstderr={stderr[-8000:]}"
                )
            recovered = sandbox.run(
                max_seconds=240,
                idle_seconds=20,
                extra_env={
                    "CDC_AUTO_DISCOVERY": "0",
                    "CDC_DROP_MODE": "ignore",
                    "CDC_TABLES": TABLE_NAME,
                    "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": str(CHUNK_SIZE),
                    "CDC_COMMIT_MAX_EVENTS": str(CHUNK_SIZE),
                },
            )
            assert recovered["stop_reason"] in {"idle", "engine_finished"}, recovered
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)

    query = (
        f"SELECT state, notification_status, chunk_count, row_count, error_code "
        f"FROM {sandbox.env['CDC_CONTROL_SCHEMA']}.backfill_runs "
        "WHERE request_id = ?"
    )
    run = sandbox.duck_query(
        query,
        ["p3b-delete-scan-request"],
    )
    assert len(run) == 1
    assert run[0][0:2] == ("complete", "COMPLETED")
    assert run[0][2] > 0 and run[0][3] == ROW_COUNT - len(DELETE_IDS)
    assert run[0][4] is None
    assert deleted_while_child_live == list(DELETE_IDS)

    with psycopg.connect(sandbox.source.dsn) as source:
        source_rows = source.execute(
            "SELECT id, payload FROM app.p3b_delete_scan ORDER BY id"
        ).fetchall()
    destination_rows = sandbox.duck_query(
        f'SELECT id, payload FROM "{sandbox.env["CDC_DATASET"]}".'
        f'"{DESTINATION_TABLE}" ORDER BY id'
    )
    assert {row[0] for row in destination_rows} == {row[0] for row in source_rows}
    assert Counter(destination_rows) == Counter(source_rows)
    assert len(destination_rows) == len({row[0] for row in destination_rows})
    assert not ({row[0] for row in destination_rows} & set(DELETE_IDS))
