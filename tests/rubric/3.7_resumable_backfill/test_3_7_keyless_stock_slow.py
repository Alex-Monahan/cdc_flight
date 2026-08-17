"""Live proof of the stock keyless incremental boundary."""

from __future__ import annotations

import json

import duckdb
import psycopg
import pytest

from cdc_flight.backfill import BackfillCoordinator

pytestmark = pytest.mark.slow


def test_live_stock_keyless_signal_records_the_full_fallback_boundary(sandbox):
    """The real stock connector reports NO_PRIMARY_KEY; no fake cursor is created."""
    sandbox.reseed()
    tables = "customers,sensor_readings"
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=150,
        idle_seconds=6,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        from cdc_flight import destination

        destination.ensure_control_schema(con, "_cdc_flight")
        destination.ensure_dataset(con, sandbox.DATASET)
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema="_cdc_flight",
            topic_prefix="cdcflight",
        )
        signal, _runs = coordinator.request_tables(
            ("app.customers", "app.sensor_readings"),
            request_id="p3-keyless-request",
            signal_id="p3-keyless-signal",
        )
    process = sandbox.spawn(
        max_seconds=240,
        idle_seconds=90,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
        capture=True,
    )
    try:
        sandbox.wait_for_slot_active(process=process, timeout=45)
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as source:
            source.execute(
                "INSERT INTO app.cdc_flight_signal (id, type, data) VALUES (%s, %s, %s)",
                (
                    "p3-keyless-signal",
                    "execute-snapshot",
                    json.dumps(
                        {"data-collections": list(signal.tables), "type": "incremental"},
                        separators=(",", ":"),
                    ),
                ),
                )
        stdout, stderr = process.communicate(timeout=240)
        # Stock emits the keyless capability failure and the current production
        # handoff fails closed with the old image retained.  A non-zero bounded
        # outcome is intentional evidence of the documented ceiling, not a green
        # success claim for an unimplemented resumable keyless cursor.
        assert process.returncode != 0, (stdout[-3000:], stderr[-6000:])
        assert "destination still owes a table lifecycle rebuild" in stderr
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)

    rows = sandbox.duck_query(
        "SELECT source_table, state, effective_mode, error_code, last_processed_key_json "
        "FROM _cdc_flight.backfill_runs WHERE request_id = 'p3-keyless-request' "
        "ORDER BY source_table"
    )
    assert rows[0][0] == "customers"
    sensor = next(row for row in rows if row[0] == "sensor_readings")
    assert sensor[1] == "blocked"
    assert sensor[2] == "full"
    assert sensor[3] == "NO_PRIMARY_KEY"
    assert sensor[4] is None
