"""Rubric 1.2 - exactly-once delivery for tables WITHOUT a primary key.

See README.md for the failure mode and the test conventions.
"""

from __future__ import annotations

import pytest

READINGS = '"cdc_raw"."cdcflight_app_sensor_readings"'
REPLAY_FILTER = "sensor_id = 'REPLAY'"

TARGET = (
    "rubric 1.2: exactly-once for keyless tables needs the transactional applier "
    "plus a synthetic identity from the envelope (ADR 0001)"
)
TARGET_KEY = (
    "rubric 1.2: keyless tables need a synthetic key "
    "(lsn, tx_id, ordinal-within-transaction) - ADR 0001"
)


def test_gap_replay_duplicates_keyless_rows(crash_replay):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {REPLAY_FILTER}")
    assert rows > n, (
        f"expected more than {n} rows after the offset rollback (at-least-once); "
        f"got {rows}"
    )


def test_gap_dedup_is_impossible_without_a_key(crash_replay):
    """The duplicates carry no distinguishing field, so nothing downstream can fix them.

    Every column - including the CDC metadata - is identical between the
    original delivery and the replay, so `SELECT DISTINCT` would also delete
    legitimately identical readings.
    """
    box = crash_replay["box"]
    total, distinct = box.duck_query(
        "SELECT count(*), count(DISTINCT (sensor_id, reading_at, value, unit, "
        f"dbz_op, dbz_lsn, dbz_tx_id)) FROM {READINGS} WHERE {REPLAY_FILTER}"
    )[0]
    assert total > distinct, (
        "expected byte-identical duplicates; if they now differ, a synthetic key "
        "may have been added - update RUBRIC_STATUS"
    )


def test_no_readings_are_lost(crash_replay):
    """Regression guard: at-least-once must never decay into at-most-once."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    distinct_values = box.scalar(
        f"SELECT count(DISTINCT value) FROM {READINGS} WHERE {REPLAY_FILTER}"
    )
    assert distinct_values == n, f"{n - distinct_values} readings never arrived"


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_exactly_once_nopk(crash_replay):
    """TARGET BEHAVIOUR - each keyless change event lands exactly once."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {REPLAY_FILTER}")
    assert rows == n, f"expected exactly {n} rows, got {rows}"


@pytest.mark.xfail(reason=TARGET_KEY, strict=True)
def test_target_synthetic_key_is_present(crash_replay):
    """TARGET BEHAVIOUR - a keyless table still gets a unique row identity."""
    box = crash_replay["box"]
    columns = {
        c
        for (c,) in box.duck_query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' "
            "AND table_name = 'cdcflight_app_sensor_readings'"
        )
    }
    assert "cdcf_event_id" in columns, sorted(columns)
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {READINGS}"
    )[0]
    assert total == distinct, "synthetic event id is not unique"
