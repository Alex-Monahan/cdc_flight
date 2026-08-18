"""Real ordinary-acquisition proof for keyless snapshot/live overlap."""

from __future__ import annotations

import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

TABLE = "app.sensor_readings"
TARGET = '"cdc_raw"."cdcflight_app_sensor_readings"'
SNAPSHOT_ROWS = 80_000
WRITER_SECONDS = 14.0
WRITER_INTERVAL_SECONDS = 0.015
TAG = "ordinary-snapshot-race"


def _tuple_counts(rows: list[tuple]) -> Counter[tuple]:
    """Count native driver values, preserving physical keyless multiplicity."""
    return Counter(tuple(row) for row in rows)


def _duplicate_count(source: Counter[tuple], destination: Counter[tuple]) -> int:
    return sum(max(destination[row] - source[row], 0) for row in destination)


def _missing_count(source: Counter[tuple], destination: Counter[tuple]) -> int:
    return sum(max(source[row] - destination[row], 0) for row in source)


class _ConcurrentWriter:
    def __init__(self, sandbox):
        self.sandbox = sandbox
        self.started = threading.Event()
        self.stop_requested = threading.Event()
        self.written = 0
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="keyless-race-writer")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.stop_requested.set()
        self._thread.join(timeout=30)
        assert not self._thread.is_alive(), "concurrent source writer did not stop"

    def _run(self) -> None:
        base_time = datetime(2026, 8, 17, tzinfo=UTC)
        deadline = time.monotonic() + WRITER_SECONDS
        try:
            with psycopg.connect(self.sandbox.source.dsn, autocommit=True) as conn:
                self.started.set()
                ordinal = 0
                while (
                    not self.stop_requested.is_set()
                    and time.monotonic() < deadline
                ):
                    ordinal += 1
                    conn.execute(
                        f"INSERT INTO {TABLE} (sensor_id, reading_at, value, unit) "
                        "VALUES (%s, %s, %s, %s)",
                        (
                            TAG,
                            base_time + timedelta(microseconds=ordinal),
                            900_000.0 + ordinal,
                            "C",
                        ),
                    )
                    self.written += 1
                    self.stop_requested.wait(WRITER_INTERVAL_SECONDS)
        except BaseException as exc:  # surfaced by the test after the child exits
            self.error = exc


def test_ordinary_initial_acquisition_has_no_keyless_snapshot_live_duplicates(sandbox):
    """One ordinary stock acquisition must preserve native keyless multiplicity.

    This is intentionally the production `cdc-flight` initial path, not the blocking
    resnapshot helper. The environment still requests four readers to prove that no
    operator/environment seam silently re-arms the old hazard; the production builder
    pins the effective connector property to one.
    """
    sandbox.reseed()
    sandbox.sql(
        f"INSERT INTO {TABLE} (sensor_id, reading_at, value, unit) "
        "SELECT 'ordinary-snapshot-bulk', "
        "TIMESTAMPTZ '2026-08-01T00:00:00Z' + i * INTERVAL '1 microsecond', "
        f"i::double precision, 'C' FROM generate_series(1, {SNAPSHOT_ROWS}) AS i",
        one_transaction=True,
    )

    process = sandbox.spawn(
        max_seconds=300,
        idle_seconds=6,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_SNAPSHOT_MAX_THREADS": "4",
        },
    )
    writer = _ConcurrentWriter(sandbox)
    writer_started = False
    try:
        writer.start()
        writer_started = True
        assert writer.started.wait(10), "concurrent writer did not connect"
        returncode = process.wait(timeout=480)
    finally:
        if writer_started:
            writer.stop()
        if process.poll() is None:
            process.kill()
            process.wait(timeout=30)

    assert writer.error is None, repr(writer.error)
    assert writer.written > 0, "the concurrent writer inserted no tagged rows"
    assert returncode == 0, returncode
    summary = sandbox.last_summary()
    assert summary["ok"] is True, summary

    source_rows = _tuple_counts(
        sandbox.pg_query(
            f"SELECT sensor_id, reading_at, value, unit FROM {TABLE} "
            "WHERE sensor_id = %s",
            (TAG,),
        )
    )
    destination_rows = _tuple_counts(
        sandbox.duck_query(
            f"SELECT sensor_id, reading_at, value, unit FROM {TARGET} "
            "WHERE sensor_id = ?",
            [TAG],
        )
    )
    duplicates = _duplicate_count(source_rows, destination_rows)
    missing = _missing_count(source_rows, destination_rows)
    print(
        "ordinary keyless acquisition: "
        f"env_snapshot_threads=4 production_pin=1 "
        f"writer_rows={writer.written} source_rows={sum(source_rows.values())} "
        f"destination_rows={sum(destination_rows.values())} "
        f"duplicates={duplicates} missing={missing}"
    )
    assert duplicates == 0, destination_rows - source_rows
    assert missing == 0, source_rows - destination_rows
    assert destination_rows == source_rows
