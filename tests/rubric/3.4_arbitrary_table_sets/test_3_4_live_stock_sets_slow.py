"""Live stock proofs for independent table outcomes and queued coalescing."""

from __future__ import annotations

import hashlib
import json
from collections import Counter

import duckdb
import pytest

from cdc_flight import destination, naming
from cdc_flight.backfill import (
    BackfillCoordinator,
    RefreshScheduler,
    StockSignalWriter,
)

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

GOOD = "app.p3b_set_good"
EMPTY = "app.p3b_set_empty"
KEYLESS = "app.p3b_set_keyless"
OUTCOME_TABLES = (GOOD, EMPTY, KEYLESS)

QUEUE_A = "app.p3b_queue_a"
QUEUE_B = "app.p3b_queue_b"
QUEUE_C = "app.p3b_queue_c"
QUEUE_TABLES = (QUEUE_A, QUEUE_B, QUEUE_C)

SIGNAL_COLLECTION = "app.cdc_flight_signal"
CHUNK_SIZE = 100


def _configure_namespace(sandbox, label: str) -> None:
    suffix = f"{label}_{hashlib.sha256(label.encode()).hexdigest()[:8]}"
    sandbox.env.update(
        {
            "CDC_PIPELINE_NAME": f"cdc_flight_p3b_{suffix}",
            "CDC_CONTROL_SCHEMA": f"_cdc_flight_p3b_{suffix}",
            "CDC_DATASET": f"cdc_raw_p3b_{suffix}",
        }
    )


def _control_schema(sandbox) -> str:
    return sandbox.env.get("CDC_CONTROL_SCHEMA", "_cdc_flight")


def _dataset(sandbox) -> str:
    return sandbox.env.get("CDC_DATASET", sandbox.DATASET)


def _capture_env(sandbox, tables: tuple[str, ...]) -> dict[str, str]:
    return {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_DROP_MODE": "ignore",
        "CDC_TABLES": ",".join(table.split(".", 1)[1] for table in tables),
        "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": str(CHUNK_SIZE),
        "CDC_COMMIT_MAX_EVENTS": str(CHUNK_SIZE),
    }


def _baseline(sandbox, tables: tuple[str, ...]) -> None:
    result = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=20,
        extra_env=_capture_env(sandbox, tables),
    )
    assert result["stop_reason"] in {"idle", "engine_finished"}, result


def _write_signal(sandbox, signal) -> None:
    StockSignalWriter(
        sandbox.source.dsn,
        data_collection=SIGNAL_COLLECTION,
    ).insert(signal)
    rows = sandbox.pg_query(
        "SELECT id, type, data FROM app.cdc_flight_signal WHERE id = %s",
        (signal.signal_id,),
    )
    assert len(rows) == 1
    assert rows[0][1] == "execute-snapshot"
    assert json.loads(rows[0][2])["data-collections"] == list(signal.tables)


def _run_signal(
    sandbox,
    tables: tuple[str, ...],
    *,
    expected_failure_table: str | None = None,
    recover_expected_failure: bool = False,
) -> dict:
    process = sandbox.spawn(
        max_seconds=300,
        idle_seconds=90,
        extra_env=_capture_env(sandbox, tables),
        capture=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=300)
        if process.returncode != 0:
            summary = sandbox.last_summary()
            if "slot_acknowledgement_timeout" not in summary:
                # The durable per-table rows below are the oracle for the
                # intentionally refused peer.  Do not classify that outcome by
                # matching an exception/log string from the process summary.
                if expected_failure_table is not None:
                    if recover_expected_failure:
                        recovered = sandbox.run(
                            max_seconds=240,
                            idle_seconds=20,
                            extra_env=_capture_env(sandbox, tables),
                        )
                        assert recovered["stop_reason"] in {
                            "idle",
                            "engine_finished",
                        }, recovered
                        return recovered
                    return summary
                raise AssertionError(
                    f"live stock process failed: summary={summary}\n"
                    f"stdout={stdout[-5000:]}\nstderr={stderr[-8000:]}"
                )
            recovered = sandbox.run(
                max_seconds=240,
                idle_seconds=20,
                extra_env=_capture_env(sandbox, tables),
            )
            assert recovered["stop_reason"] in {"idle", "engine_finished"}, recovered
            return recovered
        return sandbox.last_summary()
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)


def _request(sandbox, tables, *, request_id: str, signal_id: str):
    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        control_schema = _control_schema(sandbox)
        destination.ensure_control_schema(con, control_schema)
        destination.ensure_dataset(con, _dataset(sandbox))
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema=control_schema,
            topic_prefix="cdcflight",
        )
        return coordinator.request_tables(
            tables,
            request_id=request_id,
            signal_id=signal_id,
        )


def _source_rows(sandbox, table: str) -> list[tuple]:
    return sandbox.pg_query(f"SELECT id, payload FROM {table} ORDER BY id")


def _destination_rows(sandbox, table: str) -> list[tuple]:
    schema, source_table = table.split(".", 1)
    target = naming.destination_table("cdcflight", schema, source_table)
    return sandbox.duck_query(
        f'SELECT id, payload FROM "{_dataset(sandbox)}"."{target}" '
        "ORDER BY id"
    )


def _assert_exact(sandbox, table: str) -> None:
    source = _source_rows(sandbox, table)
    landed = _destination_rows(sandbox, table)
    assert {row[0] for row in landed} == {row[0] for row in source}
    assert Counter(landed) == Counter(source)
    assert len(landed) == len({row[0] for row in landed})


def _seed_outcome_tables(sandbox) -> None:
    _configure_namespace(sandbox, "set_outcome")
    sandbox.reseed()
    sandbox.sql(
        [
            "CREATE TABLE app.p3b_set_good ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "CREATE TABLE app.p3b_set_empty ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "CREATE TABLE app.p3b_set_keyless ("
            "id integer NOT NULL, payload text NOT NULL)",
            "ALTER TABLE app.p3b_set_keyless REPLICA IDENTITY FULL",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_set_good",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_set_empty",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_set_keyless",
            "INSERT INTO app.p3b_set_good VALUES (1, 'good-1'), (2, 'good-2')",
            "INSERT INTO app.p3b_set_empty VALUES (1, 'old-1'), (2, 'old-2')",
            "INSERT INTO app.p3b_set_keyless VALUES (1, 'keyless-1'), (2, 'keyless-2')",
        ],
        one_transaction=True,
    )


def test_live_stock_set_isolates_empty_and_failed_table_from_healthy_peers(sandbox):
    """One stock signal publishes healthy/empty peers and retains a failed peer."""
    _seed_outcome_tables(sandbox)
    _baseline(sandbox, OUTCOME_TABLES)
    sandbox.sql("DELETE FROM app.p3b_set_empty", one_transaction=True)

    signal, runs = _request(
        sandbox,
        OUTCOME_TABLES,
        request_id="p3b-set-outcome-request",
        signal_id="p3b-set-outcome-signal",
    )
    assert {run.source_table for run in runs} == {
        "p3b_set_good",
        "p3b_set_empty",
        "p3b_set_keyless",
    }
    _write_signal(sandbox, signal)
    result = _run_signal(
        sandbox,
        OUTCOME_TABLES,
        expected_failure_table="app.p3b_set_keyless",
    )
    assert result["stop_reason"] in {"idle", "engine_finished", "catalog_unresolved"}, result

    runs_by_table = {
        row[0]: row
        for row in sandbox.duck_query(
            "SELECT source_table, state, effective_mode, notification_status, "
            f"error_code, row_count FROM {_control_schema(sandbox)}.backfill_runs "
            "WHERE request_id = ? ORDER BY source_table",
            ["p3b-set-outcome-request"],
        )
    }
    assert runs_by_table["p3b_set_good"][:5] == (
        "p3b_set_good",
        "complete",
        "incremental",
        "COMPLETED",
        None,
    )
    assert runs_by_table["p3b_set_empty"][:5] == (
        "p3b_set_empty",
        "complete",
        "incremental",
        "COMPLETED",
        None,
    )
    assert runs_by_table["p3b_set_keyless"][:5] == (
        "p3b_set_keyless",
        "blocked",
        "full",
        "TABLE_SCAN_COMPLETED",
        "NO_PRIMARY_KEY",
    )
    assert runs_by_table["p3b_set_good"][5] == 2
    assert runs_by_table["p3b_set_empty"][5] == 0
    assert runs_by_table["p3b_set_keyless"][5] == 0

    _assert_exact(sandbox, GOOD)
    _assert_exact(sandbox, EMPTY)
    _assert_exact(sandbox, KEYLESS)
    assert _destination_rows(sandbox, EMPTY) == []


def _seed_queue_tables(sandbox) -> None:
    _configure_namespace(sandbox, "set_queue")
    sandbox.reseed()
    sandbox.sql(
        [
            "CREATE TABLE app.p3b_queue_a ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "CREATE TABLE app.p3b_queue_b ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "CREATE TABLE app.p3b_queue_c ("
            "id integer PRIMARY KEY, payload text NOT NULL)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_queue_a",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_queue_b",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_queue_c",
            "INSERT INTO app.p3b_queue_a (id, payload) "
            "SELECT g, 'queue-a-' || g FROM generate_series(1, 4000) AS s(g)",
            "INSERT INTO app.p3b_queue_b VALUES (1, 'queue-b-1'), (2, 'queue-b-2')",
            "INSERT INTO app.p3b_queue_c VALUES (1, 'queue-c-1'), (2, 'queue-c-2')",
        ],
        one_transaction=True,
    )


def test_live_queued_stock_requests_coalesce_into_one_successor_signal(sandbox):
    """Queued requests become one real successor signal after the active run."""
    _seed_queue_tables(sandbox)
    _baseline(sandbox, QUEUE_TABLES)
    signal_a, runs_a = _request(
        sandbox,
        (QUEUE_A,),
        request_id="p3b-queue-a-request",
        signal_id="p3b-queue-a-signal",
    )
    assert [run.source_table for run in runs_a] == ["p3b_queue_a"]
    queued_b, no_runs_b = _request(
        sandbox,
        (QUEUE_B,),
        request_id="p3b-queue-b-request",
        signal_id="p3b-queue-b-signal",
    )
    queued_c, no_runs_c = _request(
        sandbox,
        (QUEUE_C,),
        request_id="p3b-queue-c-request",
        signal_id="p3b-queue-c-signal",
    )
    assert queued_b.queued is True and queued_c.queued is True
    assert no_runs_b == () and no_runs_c == ()

    queued_rows = sandbox.duck_query(
        "SELECT request_id, state, tables_json, dispatch_signal_id "
        f"FROM {_control_schema(sandbox)}.backfill_signal_queue ORDER BY request_id"
    )
    assert [(row[0], row[1], json.loads(row[2]), row[3]) for row in queued_rows] == [
        ("p3b-queue-b-request", "queued", [QUEUE_B], None),
        ("p3b-queue-c-request", "queued", [QUEUE_C], None),
    ]

    _write_signal(sandbox, signal_a)
    first = _run_signal(
        sandbox,
        (QUEUE_A,),
        expected_failure_table=QUEUE_A,
        recover_expected_failure=True,
    )
    assert first["stop_reason"] in {"idle", "engine_finished"}, first

    with duckdb.connect(str(sandbox.duckdb_path)) as con:
        control_schema = _control_schema(sandbox)
        coordinator = BackfillCoordinator(
            con,
            pipeline=sandbox.env["CDC_PIPELINE_NAME"],
            control_schema=control_schema,
            topic_prefix="cdcflight",
        )
        scheduler = RefreshScheduler(
            coordinator,
            signal_writer=StockSignalWriter(
                sandbox.source.dsn,
                data_collection=SIGNAL_COLLECTION,
            ),
        )
        successor = scheduler.dispatch_queued()
        assert successor is not None
        successor_signal, successor_runs = successor
        assert successor_signal.tables == (QUEUE_B, QUEUE_C)
        assert {run.source_table for run in successor_runs} == {
            "p3b_queue_b",
            "p3b_queue_c",
        }
        assert {run.signal_id for run in successor_runs} == {successor_signal.signal_id}

    source_signal = sandbox.pg_query(
        "SELECT type, data FROM app.cdc_flight_signal WHERE id = %s",
        (successor_signal.signal_id,),
    )
    assert len(source_signal) == 1
    assert source_signal[0][0] == "execute-snapshot"
    assert json.loads(source_signal[0][1])["data-collections"] == [QUEUE_B, QUEUE_C]
    assert sandbox.duck_query(
        "SELECT request_id, state, dispatch_signal_id "
        f"FROM {_control_schema(sandbox)}.backfill_signal_queue ORDER BY request_id"
    ) == [
        ("p3b-queue-b-request", "dispatched", successor_signal.signal_id),
        ("p3b-queue-c-request", "dispatched", successor_signal.signal_id),
    ]

    second = _run_signal(sandbox, (QUEUE_A, QUEUE_B, QUEUE_C))
    assert second["stop_reason"] in {"idle", "engine_finished"}, second
    completed = sandbox.duck_query(
        "SELECT source_table, state, effective_mode, signal_id "
        f"FROM {_control_schema(sandbox)}.backfill_runs "
        "WHERE source_table IN ('p3b_queue_a', 'p3b_queue_b', 'p3b_queue_c') "
        "ORDER BY source_table"
    )
    assert completed[0][0:3] == ("p3b_queue_a", "complete", "incremental")
    assert completed[1][0:3] == ("p3b_queue_b", "complete", "incremental")
    assert completed[2][0:3] == ("p3b_queue_c", "complete", "incremental")
    assert completed[1][3] == completed[2][3] == successor_signal.signal_id
    for table in QUEUE_TABLES:
        _assert_exact(sandbox, table)
