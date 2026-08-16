"""FIX ROUND 15: keyless DELETE recovery at every transactional fault anchor."""

from __future__ import annotations

import contextlib

import psycopg
import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


TABLE = "app.r15_keyless_delete_anchor"
PEER = "app.r15_keyless_delete_anchor_peer"
CAPTURE = {
    "CDC_AUTO_DISCOVERY": "0",
    "CDC_TABLES": "r15_keyless_delete_anchor,r15_keyless_delete_anchor_peer",
}
ANCHORS = (
    "decode",
    "begin",
    "spill",
    "mid_apply",
    "pre_commit",
    "post_commit_pre_ack",
    "post_ack",
    "destination_write",
    "destination_commit",
    "destination_commit_late",
    "destination_hang",
    "destination_close",
)


@pytest.fixture(scope="module")
def anchor_box(sandbox):
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE}, {PEER}")
        conn.execute(f"CREATE TABLE {TABLE} (id integer, value text, body text)")
        conn.execute(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")
        conn.execute(f"CREATE TABLE {PEER} (id integer PRIMARY KEY, marker text)")
        conn.execute(f"ALTER PUBLICATION cdc_flight_pub ADD TABLE {TABLE}, {PEER}")
        conn.execute(
            f"INSERT INTO {TABLE} SELECT i, 'anchor-' || i, repeat('x', 9000) "
            "FROM generate_series(1, 12) AS i"
        )
        conn.execute(f"INSERT INTO {PEER} VALUES (0, 'baseline')")
    try:
        baseline = sandbox.run(reset_state=True, extra_env=CAPTURE, max_seconds=150)
        assert baseline["ok"] is True, baseline
        yield sandbox
    finally:
        with contextlib.suppress(Exception), psycopg.connect(
            sandbox.source.dsn, autocommit=True
        ) as conn:
            conn.execute("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + TABLE + ", " + PEER)
            conn.execute(f"DROP TABLE IF EXISTS {TABLE}, {PEER}")


def _source_rows(box) -> tuple[list[tuple], list[tuple]]:
    return (
        box.pg_query(
            f"SELECT id, value, length(body), md5(body) FROM {TABLE} ORDER BY id"
        ),
        box.pg_query(f"SELECT id, marker FROM {PEER} ORDER BY id"),
    )


def _destination_rows(box) -> tuple[list[tuple], list[tuple]]:
    return (
        box.duck_query(
            'SELECT id, value, length(body), md5(body) FROM '
            '"cdc_raw"."cdcflight_app_r15_keyless_delete_anchor" ORDER BY id'
        ),
        box.duck_query(
            'SELECT id, marker FROM '
            '"cdc_raw"."cdcflight_app_r15_keyless_delete_anchor_peer" ORDER BY id'
        ),
    )


@pytest.mark.parametrize("anchor", ANCHORS)
def test_keyless_delete_recovers_at_each_anchor(anchor_box, anchor):
    box = anchor_box
    ident = ANCHORS.index(anchor) + 1
    box.clear_fired_fault()
    box.sql(
        [
            f"DELETE FROM {TABLE} WHERE ctid = ("
            f"SELECT ctid FROM {TABLE} WHERE id = {ident} LIMIT 1)",
            f"INSERT INTO {PEER} VALUES ({ident}, 'peer-{ident}')",
        ],
        one_transaction=True,
    )
    fault_env = {**CAPTURE, "CDC_FAULT_INJECT": f"{anchor}:1"}
    if anchor == "spill":
        fault_env.update({"CDC_UNIT_SPILL_EVENTS": "1", "CDC_UNIT_SPILL_BYTES": "512"})
    if anchor == "destination_hang":
        fault_env.update(
            {"CDC_COMMIT_TIMEOUT": "5", "CDC_FAULT_HANG_SECONDS": "600"}
        )
    failed = box.run(
        extra_env=fault_env,
        expect_success=False,
        max_seconds=60 if anchor == "destination_hang" else 150,
        timeout=120 if anchor == "destination_hang" else 300,
    )
    assert failed["returncode"] != 0, failed
    fired = box.fired_fault()
    assert fired is not None and fired["point"] == anchor, fired

    recovered = box.run(extra_env=CAPTURE, max_seconds=150)
    assert recovered["ok"] is True, recovered
    assert _destination_rows(box) == _source_rows(box)
