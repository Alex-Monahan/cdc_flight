"""FIX ROUND 15: real PostgreSQL FULL-identity keyless DELETE proofs."""

from __future__ import annotations

import contextlib

import psycopg
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


LOCAL_TABLE = "app.r15_keyless_delete_hard"
LOCAL_CAPTURE = {
    "CDC_AUTO_DISCOVERY": "0",
    "CDC_TABLES": "r15_keyless_delete_hard",
}
LOCAL_TARGET = '"cdc_raw"."cdcflight_app_r15_keyless_delete_hard"'


def _rows_from_source(sandbox) -> list[tuple]:
    return sandbox.pg_query(
        f"SELECT id, value, note, length(body), md5(body) FROM {LOCAL_TABLE} "
        "ORDER BY id, value, note NULLS FIRST, md5(body)"
    )


def _rows_from_destination(sandbox) -> list[tuple]:
    return sandbox.duck_query(
        f"SELECT id, value, note, length(body), md5(body) FROM {LOCAL_TARGET} "
        'ORDER BY id, value, note NULLS FIRST, md5(body)'
    )


def _assert_equal(sandbox) -> None:
    assert _rows_from_destination(sandbox) == _rows_from_source(sandbox)


@pytest.fixture(scope="module")
def hard_keyless_table(sandbox):
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {LOCAL_TABLE}")
        conn.execute(
            f"CREATE TABLE {LOCAL_TABLE} "
            "(id integer, value text, note text, body text)"
        )
        conn.execute(f"ALTER TABLE {LOCAL_TABLE} REPLICA IDENTITY FULL")
        conn.execute(
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE " + LOCAL_TABLE
        )
        large_a = "a" * 12000
        large_b = "b" * 12000
        conn.execute(
            f"INSERT INTO {LOCAL_TABLE} (id, value, note, body) VALUES "
            "(1, 'duplicate', NULL, %s), "
            "(1, 'duplicate', NULL, %s), "
            "(1, 'duplicate', NULL, %s), "
            "(2, 'reinsert', 'same', %s), "
            "(3, 'update', 'old', %s), "
            "(4, 'null-delete', NULL, %s), "
            "(5, 'crash-delete', 'old', %s)",
            [large_a, large_a, large_a, large_b, large_a, large_b, large_a],
        )
    try:
        baseline = sandbox.run(reset_state=True, extra_env=LOCAL_CAPTURE)
        assert baseline["ok"] is True, baseline
        _assert_equal(sandbox)
        yield sandbox
    finally:
        with contextlib.suppress(Exception), psycopg.connect(
            sandbox.source.dsn, autocommit=True
        ) as conn:
            conn.execute("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + LOCAL_TABLE)
            conn.execute(f"DROP TABLE IF EXISTS {LOCAL_TABLE}")


def _delete_one(table: str, predicate: str) -> str:
    return (
        f"DELETE FROM {table} WHERE ctid = ("
        f"SELECT ctid FROM {table} WHERE {predicate} LIMIT 1)"
    )


def test_real_keyless_delete_duplicate_null_and_toast_image(hard_keyless_table):
    """One of N identical rows, explicit NULL, and a large before-image are exact."""
    sandbox = hard_keyless_table
    sandbox.sql(_delete_one(LOCAL_TABLE, "id = 1"))
    result = sandbox.run(extra_env=LOCAL_CAPTURE)
    assert result["ok"] is True, result
    assert len(_rows_from_source(sandbox)) == 6
    _assert_equal(sandbox)


def test_real_keyless_delete_then_identical_reinsert_is_one_physical_row(
    hard_keyless_table,
):
    sandbox = hard_keyless_table
    sandbox.sql(
        [
            _delete_one(LOCAL_TABLE, "id = 2"),
            f"INSERT INTO {LOCAL_TABLE} VALUES (2, 'reinsert', 'same', repeat('b', 12000))",
        ],
        one_transaction=True,
    )
    result = sandbox.run(extra_env=LOCAL_CAPTURE)
    assert result["ok"] is True, result
    _assert_equal(sandbox)


def test_real_keyless_delete_combined_with_update_and_null_delete(hard_keyless_table):
    sandbox = hard_keyless_table
    sandbox.sql(
        [
            f"UPDATE {LOCAL_TABLE} SET value = 'updated', note = NULL "
            f"WHERE ctid = (SELECT ctid FROM {LOCAL_TABLE} WHERE id = 3 LIMIT 1)",
            _delete_one(LOCAL_TABLE, "id = 4"),
        ],
        one_transaction=True,
    )
    result = sandbox.run(extra_env=LOCAL_CAPTURE)
    assert result["ok"] is True, result
    _assert_equal(sandbox)


def test_real_keyless_delete_replay_after_post_commit_crash_is_idempotent(
    hard_keyless_table,
):
    """The data row and event ledger survive/replay as one destination transaction."""
    sandbox = hard_keyless_table
    sandbox.sql(_delete_one(LOCAL_TABLE, "id = 5"))
    crashed = sandbox.run(
        extra_env={**LOCAL_CAPTURE, "CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
        expect_success=False,
        max_seconds=150,
    )
    assert crashed["returncode"] == 137, crashed
    recovered = sandbox.run(extra_env=LOCAL_CAPTURE)
    assert recovered["ok"] is True, recovered
    _assert_equal(sandbox)
    assert sandbox.duck_query(
        "SELECT count(*) FROM _cdc_flight.keyless_events "
        "WHERE target_table = 'cdcflight_app_r15_keyless_delete_hard' "
        "AND operation = 'd'"
    ) == [(4,)]
