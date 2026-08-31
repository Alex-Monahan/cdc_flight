"""Live stock incremental crash coverage for rubric 3.7."""

from __future__ import annotations

import hashlib
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import duckdb
import psycopg
import pytest

from cdc_flight import destination
from cdc_flight.backfill import (
    BackfillCoordinator,
    IncrementalSignal,
    StockSignalWriter,
)

pytestmark = pytest.mark.slow

QUALIFIED_TABLES = ("app.p3b_resume_composite", "app.p3b_resume_uuid")
TABLE_CASES = {
    "composite": ("p3b_resume_composite", "app.p3b_resume_composite"),
    "uuid": ("p3b_resume_uuid", "app.p3b_resume_uuid"),
}
SIGNAL_ID = "p3b-stock-crash-signal"
REQUEST_ID = "p3b-stock-crash-request"
CHILD = Path(__file__).resolve().parents[2] / "support" / "crash_matrix_child.py"
LOAD_SENSITIVE_POINTS = frozenset(
    {
        "after_md_commit_before_markProcessed",
        "after_markProcessed_before_markBatchFinished",
    }
)


def _seed_key_types(sandbox, label: str, table_specs) -> None:
    readable = re.sub(r"[^a-z0-9_]", "_", label.lower())
    suffix = f"{readable[:23]}_{hashlib.sha256(label.encode()).hexdigest()[:8]}"
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
            "CREATE TABLE app.p3b_resume_composite ("
            "tenant_id integer NOT NULL, row_id bigint NOT NULL, payload text NOT NULL, "
            "PRIMARY KEY (tenant_id, row_id))",
            "CREATE TABLE app.p3b_resume_uuid ("
            "id uuid PRIMARY KEY, payload text NOT NULL)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_resume_composite",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.p3b_resume_uuid",
            "INSERT INTO app.p3b_resume_composite (tenant_id, row_id, payload) "
            "SELECT ((g - 1) % 17) + 1, g, 'composite-' || g "
            "FROM generate_series(1, 1200) AS s(g)",
            "INSERT INTO app.p3b_resume_uuid (id, payload) "
            "SELECT gen_random_uuid(), 'uuid-' || g "
            "FROM generate_series(1, 1200) AS s(g)",
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
            "CDC_TABLES": ",".join(name for name, _qualified in table_specs),
            "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": "400",
            "CDC_COMMIT_MAX_EVENTS": "400",
        },
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline


def _request(sandbox, table_specs):
    qualified_tables = tuple(qualified for _name, qualified in table_specs)
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
            qualified_tables,
            request_id=REQUEST_ID,
            signal_id=SIGNAL_ID,
        )
        assert signal.tables == qualified_tables
        assert {run.source_table for run in runs} == {
            name for name, _qualified in table_specs
        }
        return signal


def _write_signal(sandbox, signal) -> None:
    StockSignalWriter(
        sandbox.source.dsn,
        data_collection="app.cdc_flight_signal",
    ).insert(signal)


def _write_anchor_update(sandbox, *, point: str, nth: int, table_specs) -> str:
    table_name = table_specs[0][0]
    payload = f"anchor-{table_name}-{point}-{nth}"
    with psycopg.connect(sandbox.source.dsn) as source:
        if table_name == "p3b_resume_composite":
            result = source.execute(
                "UPDATE app.p3b_resume_composite SET payload = %s "
                "WHERE tenant_id = 1 AND row_id = 1",
                (payload,),
            )
        else:
            key = source.execute(
                "SELECT id FROM app.p3b_resume_uuid ORDER BY id LIMIT 1"
            ).fetchone()[0]
            result = source.execute(
                "UPDATE app.p3b_resume_uuid SET payload = %s WHERE id = %s",
                (payload, key),
            )
        assert result.rowcount == 1
        source.commit()
        # Sample after COMMIT so the observed LSN includes this transaction's
        # commit record; sampling inside the transaction can precede the update's
        # own WAL record and let the following signal share its delivery group.
        update_lsn = source.execute(
            "SELECT pg_current_wal_lsn()"
        ).fetchone()[0]
    return str(update_lsn)


def _wait_for_anchor_update_durable(sandbox, process, update_lsn: str) -> None:
    """Wait for the target update's actual destination commit outcome.

    The source slot's confirmed flush LSN is advanced only after the product
    has committed the corresponding change in the destination. This target
    update is the observed preceding data group for the crash anchor. The
    deadline only bounds a broken pipeline; completion is gated by that durable
    outcome, not by elapsed time or a scheduling sleep.
    """
    deadline = time.monotonic() + 180
    while True:
        if process.poll() is not None:
            raise AssertionError(
                "pipeline exited before the target update became durable "
                f"(returncode={process.returncode})"
            )
        if sandbox.pg_query(
            "SELECT confirmed_flush_lsn IS NOT NULL "
            "AND confirmed_flush_lsn >= %s::pg_lsn "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (update_lsn, sandbox.slot),
        ) == [(True,)]:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(
                "target update did not reach its durable destination commit"
            )
        time.sleep(0.1)


def _child_env(
    sandbox,
    *,
    point: str,
    nth: int,
    table_specs,
    insert_signal: bool = False,
):
    qualified_tables = tuple(qualified for _name, qualified in table_specs)
    return {
        **sandbox.env,
        "CDC_FAULT_INJECT": f"{point}:{nth}",
        "CDC_BACKFILL_SIGNAL_CHILD": "1",
        "CDC_BACKFILL_TABLES": ",".join(qualified_tables),
        "CDC_BACKFILL_SIGNAL_ID": SIGNAL_ID,
        "CDC_BACKFILL_REQUEST_ID": REQUEST_ID,
        "CDC_BACKFILL_SOURCE_DSN": sandbox.source.dsn,
        "CDC_BACKFILL_INSERT_SIGNAL": "1" if insert_signal else "0",
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_DROP_MODE": "ignore",
        "CDC_TABLES": ",".join(name for name, _qualified in table_specs),
        "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": "400",
        "CDC_COMMIT_MAX_EVENTS": "400",
    }


def _run_admission_child(
    sandbox, *, point: str, table_specs, insert_signal: bool
) -> subprocess.CompletedProcess:
    sandbox.clear_fired_fault()
    proc = subprocess.run(
        [sys.executable, str(CHILD)],
        cwd=Path(__file__).resolve().parents[3],
        env=_child_env(
            sandbox,
            point=point,
            nth=1,
            table_specs=table_specs,
            insert_signal=insert_signal,
        ),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert proc.returncode == 137, (
        point,
        proc.returncode,
        proc.stdout[-3000:],
        proc.stderr[-6000:],
    )
    fired = sandbox.fired_fault()
    assert fired is not None
    assert fired["point"] == point
    assert fired["nth"] == 1
    assert fired["action"] == "137"
    return proc


def _run_pipeline_crash(sandbox, signal, *, point: str, nth: int, table_specs) -> None:
    sandbox.clear_fired_fault()
    process = sandbox.spawn(
        max_seconds=360,
        idle_seconds=90,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_DROP_MODE": "ignore",
            "CDC_TABLES": ",".join(name for name, _qualified in table_specs),
            "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": "400",
            "CDC_COMMIT_MAX_EVENTS": "400",
            "CDC_FAULT_INJECT": f"{point}:{nth}",
        },
        capture=True,
        matrix_arm=True,
    )
    try:
        sandbox.wait_for_slot_active(process=process, timeout=74)
        if point in LOAD_SENSITIVE_POINTS:
            _write_signal(sandbox, signal)
            update_lsn = _write_anchor_update(
                sandbox, point=point, nth=nth, table_specs=table_specs
            )
            _wait_for_anchor_update_durable(sandbox, process, update_lsn)
        else:
            _write_signal(sandbox, signal)
        stdout, stderr = process.communicate(timeout=360)
        fired = sandbox.fired_fault()
        assert process.returncode == 137, (
            point,
            nth,
            process.returncode,
            fired,
            stdout[-3000:],
            stderr[-6000:],
        )
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=60)
    fired = sandbox.fired_fault()
    assert fired is not None, (point, nth)
    assert fired["point"] == point
    assert fired["nth"] == nth
    assert fired["action"] == "137"


def _recover_and_assert_exact(sandbox, table_specs) -> None:
    recovery = sandbox.run(
        max_seconds=360,
        idle_seconds=90,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_DROP_MODE": "ignore",
            "CDC_TABLES": ",".join(name for name, _qualified in table_specs),
            "CDC_INCREMENTAL_SNAPSHOT_CHUNK_SIZE": "400",
            "CDC_COMMIT_MAX_EVENTS": "400",
        },
    )
    assert recovery["stop_reason"] in {"idle", "engine_finished"}, recovery

    runs = sandbox.duck_query(
        f"SELECT source_table, state, last_processed_key_json, "
        "maximum_key_json, chunk_count, row_count, error_code "
        f"FROM {sandbox.env['CDC_CONTROL_SCHEMA']}.backfill_runs "
        "WHERE request_id = ? ORDER BY source_table",
        [REQUEST_ID],
    )
    assert [row[0] for row in runs] == [name for name, _qualified in table_specs]
    for row in runs:
        assert row[1] == "complete"
        assert row[2] is not None
        assert row[3] is not None
        assert row[4] > 0
        assert row[5] > 0
        assert row[6] is None

    with psycopg.connect(sandbox.source.dsn) as source:
        source_rows = {
            name: source.execute(
                "SELECT tenant_id, row_id, payload FROM app.p3b_resume_composite "
                "ORDER BY tenant_id, row_id"
                if name == "p3b_resume_composite"
                else "SELECT id, payload FROM app.p3b_resume_uuid ORDER BY id"
            ).fetchall()
            for name, _qualified in table_specs
        }
    for name, _qualified in table_specs:
        if name == "p3b_resume_composite":
            destination_rows = sandbox.duck_query(
                f'SELECT tenant_id, row_id, payload FROM "{sandbox.env["CDC_DATASET"]}".'
                '"cdcflight_app_p3b_resume_composite" ORDER BY tenant_id, row_id'
            )
            source_keys = {tuple(row[:2]) for row in source_rows[name]}
            destination_keys = {tuple(row[:2]) for row in destination_rows}
        else:
            destination_rows = sandbox.duck_query(
                f'SELECT id, payload FROM "{sandbox.env["CDC_DATASET"]}".'
                '"cdcflight_app_p3b_resume_uuid" ORDER BY id'
            )
            source_keys = {row[0] for row in source_rows[name]}
            destination_keys = {row[0] for row in destination_rows}
        assert destination_keys == source_keys
        assert Counter(destination_rows) == Counter(source_rows[name])
        assert len(destination_rows) == len(destination_keys)


@pytest.mark.parametrize(
    ("point", "insert_signal_after_crash"),
    [
        pytest.param("before_request_md_commit", False, id="before-request-commit"),
        pytest.param(
            "after_request_commit_before_signal", False,
            id="after-request-before-source-signal",
        ),
        pytest.param("after_signal_before_started", True, id="after-source-signal"),
    ],
)
def test_live_stock_admission_crash_edges_leave_recoverable_durable_work(
    sandbox, point, insert_signal_after_crash
):
    """Real coordinator/source-signal admission cuts remain recoverable."""
    _seed_key_types(sandbox, point, tuple(TABLE_CASES.values()))
    _run_admission_child(
        sandbox,
        point=point,
        table_specs=tuple(TABLE_CASES.values()),
        insert_signal=point == "after_signal_before_started",
    )

    if point == "before_request_md_commit":
        assert sandbox.duck_query(
            f"SELECT count(*) FROM {sandbox.env['CDC_CONTROL_SCHEMA']}.backfill_runs "
            "WHERE request_id = ?",
            [REQUEST_ID],
        ) == [(0,)]
        return

    if insert_signal_after_crash:
        assert sandbox.pg_query(
            "SELECT count(*) FROM app.cdc_flight_signal WHERE id = %s",
            (SIGNAL_ID,),
        ) == [(1,)]
    else:
        _write_signal(sandbox, IncrementalSignal(SIGNAL_ID, QUALIFIED_TABLES))

    _recover_and_assert_exact(sandbox, tuple(TABLE_CASES.values()))


@pytest.mark.parametrize(
    "table_kind",
    [pytest.param("composite", id="composite"), pytest.param("uuid", id="uuid")],
)
@pytest.mark.parametrize(
    ("point", "nth"),
    [
        pytest.param("incremental_chunk_before_shadow_write", 2, id="before-shadow"),
        pytest.param(
            "incremental_chunk_after_shadow_write_before_progress", 2,
            id="after-shadow-before-progress",
        ),
        pytest.param(
            "incremental_chunk_after_progress_before_md_commit", 2,
            id="after-progress-before-commit",
        ),
        pytest.param("after_md_commit_before_markProcessed", 2, id="after-commit-before-ack"),
        pytest.param(
            "after_markProcessed_before_markBatchFinished", 2,
            id="after-ack-before-batch-finished",
        ),
        pytest.param("after_ack_before_next_poll", 2, id="after-batch-finished"),
        pytest.param("before_swap_commit", 2, id="before-swap-commit"),
        pytest.param("after_swap_commit_before_ack", 2, id="after-swap-before-ack"),
    ],
)
def test_live_stock_composite_uuid_resume_survives_every_incremental_crash_point(
    sandbox, table_kind, point, nth
):
    """Every implemented stock backfill crash cut resumes exact composite/UUID images."""
    table_specs = (TABLE_CASES[table_kind],)
    _seed_key_types(sandbox, f"{table_kind}_{point}", table_specs)
    signal = _request(sandbox, table_specs)
    _run_pipeline_crash(sandbox, signal, point=point, nth=nth, table_specs=table_specs)
    _recover_and_assert_exact(sandbox, table_specs)
