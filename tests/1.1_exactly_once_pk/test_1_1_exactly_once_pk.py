"""Rubric 1.1 - exactly-once delivery for tables WITH a primary key.

See README.md for the failure mode and the test conventions.
"""

from __future__ import annotations

import signal
import time

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
REPLAY_FILTER = "name LIKE 'replay-c-%'"

TARGET = (
    "rubric 1.1: exactly-once needs the Debezium offset committed inside the "
    "same MotherDuck transaction as the rows (ADR 0001)"
)


def test_scenario_actually_replayed(crash_replay):
    """Guard: if the replay run loaded nothing, the fault was not injected."""
    replayed = crash_replay["replayed"]
    assert replayed["returncode"] == 0, replayed
    assert replayed["records"] > 0, (
        "restoring offsets.dat did not cause a replay, so the duplication "
        f"assertions below would be vacuous: {replayed}"
    )


def test_gap_replay_duplicates_pk_rows(crash_replay):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands."""
    box = crash_replay["box"]
    n = crash_replay["customers"]
    rows, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE {REPLAY_FILTER}"
    )[0]
    assert distinct == n, f"expected {n} distinct replayed customers, got {distinct}"
    assert rows > distinct, (
        "expected at-least-once duplication after the offset rollback; if this "
        "fails the pipeline may already be exactly-once - update RUBRIC_STATUS"
    )


def test_gap_duplication_is_a_whole_batch(crash_replay):
    """The duplicate is the *entire* replayed window, not a stray row."""
    box = crash_replay["box"]
    per_id = box.duck_query(
        f"SELECT count(*) AS c FROM {CUSTOMERS} WHERE {REPLAY_FILTER} GROUP BY id ORDER BY c DESC"
    )
    assert per_id[0][0] >= 2, per_id[:5]


def test_no_rows_are_lost(crash_replay):
    """Regression guard: at-least-once must never decay into at-most-once."""
    box = crash_replay["box"]
    n = crash_replay["customers"]
    missing = box.duck_query(
        f"SELECT count(*) FROM generate_series(1, {n}) g(i) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {CUSTOMERS} c "
        "WHERE c.name = 'replay-c-' || g.i)"
    )[0][0]
    assert missing == 0, f"{missing} source rows never reached the destination"


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_exactly_once_pk(crash_replay):
    """TARGET BEHAVIOUR - each source change event lands exactly once."""
    box = crash_replay["box"]
    n = crash_replay["customers"]
    rows, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE {REPLAY_FILTER}"
    )[0]
    assert rows == n, f"expected exactly {n} rows, got {rows}"
    assert distinct == n


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_no_duplicate_change_events(crash_replay):
    """TARGET BEHAVIOUR - (lsn, table, key) identifies a change event uniquely."""
    box = crash_replay["box"]
    dupes = box.duck_query(
        f"SELECT count(*) FROM (SELECT dbz_lsn, id FROM {CUSTOMERS} "
        f"WHERE {REPLAY_FILTER} GROUP BY 1, 2 HAVING count(*) > 1)"
    )[0][0]
    assert dupes == 0, f"{dupes} change events delivered more than once"


@pytest.mark.slow
def test_slow_real_sigkill_duplicates(sandbox):
    """The un-simulated version of the same fault: SIGKILL mid-load.

    Kept out of `make test` because it needs a large transaction to widen the
    kill window (probes/p07 lost the race at 60 k rows). The deterministic test
    above covers the same defect in the default suite.
    """
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)

    rows = 200_000
    sandbox.sql(
        "INSERT INTO app.customers (name, email) SELECT "
        "'kill9-' || i, 'kill9-' || i || '@example.com' "
        f"FROM generate_series(1, {rows}) i"
    )

    proc = sandbox.spawn(max_seconds=300, idle_seconds=10)
    time.sleep(45)  # JVM start (~17 s) plus enough loading to be mid-stream
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=60)

    landed_before = sandbox.scalar(
        f"SELECT count(*) FROM {CUSTOMERS} WHERE name LIKE 'kill9-%'"
    )
    assert landed_before > 0, "SIGKILL landed before anything was written; widen the window"

    sandbox.run(max_seconds=300, idle_seconds=10)
    total, distinct = sandbox.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE name LIKE 'kill9-%'"
    )[0]
    assert distinct == rows, f"rows lost: {rows - distinct}"
    assert total > distinct, "expected duplication after a real SIGKILL (at-least-once)"
