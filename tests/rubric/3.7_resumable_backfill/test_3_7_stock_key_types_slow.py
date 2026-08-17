"""Live stock incremental resume coverage for composite and UUID primary keys."""

from __future__ import annotations

import json
from collections import Counter

import duckdb
import psycopg
import pytest

from cdc_flight.backfill import BackfillCoordinator

pytestmark = pytest.mark.slow


def _identity(rows):
    return {(row[0], row[1]) for row in rows}


def _values(rows):
    return Counter(tuple(row) for row in rows)


def test_stock_incremental_resume_handles_composite_and_uuid_keys(sandbox):
    """Stock signal rows resume by source key, including a composite and a UUID PK."""
    sandbox.reseed()
    sandbox.sql(
        [
            "CREATE TABLE app.p3_resume_composite ("
            "tenant_id integer NOT NULL, row_id bigint NOT NULL, payload text NOT NULL, "
            "PRIMARY KEY (tenant_id, row_id))",
            "CREATE TABLE app.p3_resume_uuid ("
            "id uuid PRIMARY KEY, payload text NOT NULL)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3_resume_composite",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3_resume_uuid",
            "INSERT INTO app.p3_resume_composite VALUES "
            "(1, 1, 'c-11'), (1, 2, 'c-12'), (2, 1, 'c-21')",
            "INSERT INTO app.p3_resume_uuid VALUES "
            "('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'u-1'), "
            "('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'u-2')",
        ],
        one_transaction=True,
    )
    tables = "p3_resume_composite,p3_resume_uuid"
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
        signal, runs = coordinator.request_tables(
            ("app.p3_resume_composite", "app.p3_resume_uuid"),
            request_id="p3-key-types-request",
            signal_id="p3-key-types-signal",
        )
        assert {run.source_table for run in runs} == {
            "p3_resume_composite",
            "p3_resume_uuid",
        }

    process = sandbox.spawn(
        max_seconds=240,
        idle_seconds=90,
        extra_env={"CDC_AUTO_DISCOVERY": "0", "CDC_TABLES": tables},
        capture=True,
    )
    try:
        sandbox.wait_for_slot_active(process=process, timeout=45)
        payload = json.dumps(
            {"data-collections": list(signal.tables), "type": "incremental"},
            separators=(",", ":"),
        )
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as source:
            source.execute(
                "INSERT INTO app.cdc_flight_signal (id, type, data) VALUES (%s, %s, %s)",
                ("p3-key-types-signal", "execute-snapshot", payload),
            )
        with psycopg.connect(sandbox.source.dsn) as source, source.transaction():
            source.execute(
                "UPDATE app.p3_resume_composite SET payload = 'c-updated' "
                "WHERE tenant_id = 1 AND row_id = 1"
            )
            source.execute(
                "DELETE FROM app.p3_resume_composite WHERE tenant_id = 1 AND row_id = 2"
            )
            source.execute(
                "INSERT INTO app.p3_resume_composite VALUES (2, 2, 'c-inserted')"
            )
            source.execute(
                "UPDATE app.p3_resume_uuid SET payload = 'u-updated' "
                "WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"
            )
        stdout, stderr = process.communicate(timeout=240)
        assert process.returncode == 0, (stdout[-3000:], stderr[-6000:])
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)

    runs = sandbox.duck_query(
        "SELECT source_table, state FROM _cdc_flight.backfill_runs "
        "WHERE request_id = 'p3-key-types-request' ORDER BY source_table"
    )
    assert runs == [
        ("p3_resume_composite", "complete"),
        ("p3_resume_uuid", "complete"),
    ]

    with psycopg.connect(sandbox.source.dsn) as source:
        source_composite = source.execute(
            "SELECT tenant_id, row_id, payload FROM app.p3_resume_composite ORDER BY tenant_id, row_id"
        ).fetchall()
        source_uuid = source.execute(
            "SELECT id, payload FROM app.p3_resume_uuid ORDER BY id"
        ).fetchall()
    destination_composite = sandbox.duck_query(
        'SELECT tenant_id, row_id, payload FROM "cdc_raw"."cdcflight_app_p3_resume_composite" '
        "ORDER BY tenant_id, row_id"
    )
    destination_uuid = sandbox.duck_query(
        'SELECT id, payload FROM "cdc_raw"."cdcflight_app_p3_resume_uuid" '
        "ORDER BY id"
    )
    assert _identity(destination_composite) == _identity(source_composite)
    assert _values(destination_composite) == _values(source_composite)
    assert _identity(destination_uuid) == _identity(source_uuid)
    assert _values(destination_uuid) == _values(source_uuid)
    assert len(destination_composite) == len(_identity(destination_composite))
    assert len(destination_uuid) == len(_identity(destination_uuid))
