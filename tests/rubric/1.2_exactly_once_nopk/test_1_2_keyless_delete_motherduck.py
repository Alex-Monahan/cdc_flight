"""FIX ROUND 15: the keyless DELETE matrix against real PostgreSQL and MotherDuck."""

from __future__ import annotations

import contextlib

import psycopg
import pytest
from support.motherduck_probe import connect

pytestmark = [pytest.mark.motherduck, pytest.mark.slow, pytest.mark.e2e]


TABLE = "app.r15_keyless_delete_md"
CAPTURE = {
    "CDC_AUTO_DISCOVERY": "0",
    "CDC_TABLES": "r15_keyless_delete_md",
}
ANCHOR_TABLE = "app.r15_keyless_delete_md_anchor"
ANCHOR_PEER = "app.r15_keyless_delete_md_anchor_peer"
ANCHOR_CAPTURE = {
    "CDC_AUTO_DISCOVERY": "0",
    "CDC_TABLES": "r15_keyless_delete_md_anchor,r15_keyless_delete_md_anchor_peer",
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


def _source_rows(box) -> list[tuple]:
    return box.pg_query(
        f"SELECT id, value, note, length(body), md5(body) FROM {TABLE} "
        "ORDER BY id, value, note NULLS FIRST, md5(body)"
    )


def test_real_postgres_keyless_delete_matrix_on_motherduck(
    tmp_path, postgres_cluster, motherduck_case
):
    from support.fixtures import Sandbox

    box = Sandbox("r15_keyless_delete_md", tmp_path / "sandbox", postgres_cluster)
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    dataset = motherduck_case["dataset"]
    control_schema = motherduck_case["control_schema"]
    env = {
        **CAPTURE,
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
    }
    md = None
    qualified = f'"{dataset}"."cdcflight_app_r15_keyless_delete_md"'
    try:
        box.reseed()
        with psycopg.connect(box.source.dsn, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
            conn.execute(f"CREATE TABLE {TABLE} (id integer, value text, note text, body text)")
            conn.execute(f"ALTER TABLE {TABLE} REPLICA IDENTITY FULL")
            conn.execute("ALTER PUBLICATION cdc_flight_pub ADD TABLE " + TABLE)
            conn.execute(
                f"INSERT INTO {TABLE} VALUES "
                "(1, 'duplicate', NULL, repeat('a', 12000)), "
                "(1, 'duplicate', NULL, repeat('a', 12000)), "
                "(1, 'duplicate', NULL, repeat('a', 12000)), "
                "(2, 'reinsert', 'same', repeat('b', 12000)), "
                "(3, 'update', 'old', repeat('c', 12000)), "
                "(4, 'null-delete', NULL, repeat('d', 12000))"
            )
        baseline = box.run(
            destination="motherduck",
            reset_state=True,
            extra_env=env,
            max_seconds=240,
            timeout=420,
        )
        assert baseline["ok"] is True, baseline
        md = connect(token, database)

        def assert_equal():
            landed = md.execute(
                f"SELECT id, value, note, length(body), md5(body) FROM {qualified} "
                'ORDER BY id, value, note NULLS FIRST, md5(body)'
            ).fetchall()
            assert landed == _source_rows(box)

        assert_equal()
        box.sql(
            f"DELETE FROM {TABLE} WHERE ctid = (SELECT ctid FROM {TABLE} WHERE id = 1 LIMIT 1)"
        )
        result = box.run(destination="motherduck", extra_env=env, max_seconds=240, timeout=420)
        assert result["ok"] is True, result
        assert_equal()

        box.sql(
            [
                f"DELETE FROM {TABLE} WHERE ctid = (SELECT ctid FROM {TABLE} WHERE id = 2 LIMIT 1)",
                f"INSERT INTO {TABLE} VALUES (2, 'reinsert', 'same', repeat('b', 12000))",
            ],
            one_transaction=True,
        )
        result = box.run(destination="motherduck", extra_env=env, max_seconds=240, timeout=420)
        assert result["ok"] is True, result
        assert_equal()

        box.sql(
            [
                f"UPDATE {TABLE} SET value = 'updated', note = NULL WHERE ctid = "
                f"(SELECT ctid FROM {TABLE} WHERE id = 3 LIMIT 1)",
                f"DELETE FROM {TABLE} WHERE ctid = (SELECT ctid FROM {TABLE} WHERE id = 4 LIMIT 1)",
            ],
            one_transaction=True,
        )
        result = box.run(destination="motherduck", extra_env=env, max_seconds=240, timeout=420)
        assert result["ok"] is True, result
        assert_equal()

        # A real post-commit crash is the replay case for a DELETE: the destination
        # transaction, including the keyless event ledger, is durable, but the
        # source offset is not.  Recovery must not remove a second identical row or
        # resurrect the deleted one.
        box.sql(
            f"INSERT INTO {TABLE} VALUES (5, 'replay', 'old', repeat('e', 12000))"
        )
        assert box.run(destination="motherduck", extra_env=env, max_seconds=240, timeout=420)["ok"]
        box.sql(
            f"DELETE FROM {TABLE} WHERE ctid = (SELECT ctid FROM {TABLE} WHERE id = 5 LIMIT 1)"
        )
        crashed = box.run(
            destination="motherduck",
            extra_env={**env, "CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
            expect_success=False,
            max_seconds=240,
            timeout=420,
        )
        assert crashed["returncode"] == 137, crashed
        assert box.fired_fault() is not None
        recovered = box.run(destination="motherduck", extra_env=env, max_seconds=240, timeout=420)
        assert recovered["ok"] is True, recovered
        assert_equal()
    finally:
        with contextlib.suppress(Exception), psycopg.connect(
            box.source.dsn, autocommit=True
        ) as conn:
            conn.execute("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + TABLE)
            conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
        if md is not None:
            with contextlib.suppress(Exception):
                md.execute(f'DROP SCHEMA IF EXISTS "{dataset}" CASCADE')
            md.close()
        box.cleanup()


@pytest.mark.parametrize("anchor", ANCHORS)
def test_real_postgres_keyless_delete_recovers_at_each_anchor_on_motherduck(
    tmp_path, postgres_cluster, motherduck_case, anchor
):
    """The complete crash/restart matrix is real PostgreSQL plus MotherDuck."""
    from support.fixtures import Sandbox

    box = Sandbox(f"r15_keyless_delete_md_{anchor}", tmp_path / "sandbox", postgres_cluster)
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    dataset = motherduck_case["dataset"]
    control_schema = motherduck_case["control_schema"]
    env = {
        **ANCHOR_CAPTURE,
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
    }
    md = None
    qualified = f'"{dataset}"."cdcflight_app_r15_keyless_delete_md_anchor"'

    def source_rows():
        return box.pg_query(
            f"SELECT id, value, length(body), md5(body) FROM {ANCHOR_TABLE} ORDER BY id"
        )

    def destination_rows():
        return md.execute(
            f"SELECT id, value, length(body), md5(body) FROM {qualified} ORDER BY id"
        ).fetchall()

    try:
        box.reseed()
        with psycopg.connect(box.source.dsn, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {ANCHOR_TABLE}, {ANCHOR_PEER}")
            conn.execute(
                f"CREATE TABLE {ANCHOR_TABLE} (id integer, value text, body text)"
            )
            conn.execute(f"ALTER TABLE {ANCHOR_TABLE} REPLICA IDENTITY FULL")
            conn.execute(
                f"CREATE TABLE {ANCHOR_PEER} (id integer PRIMARY KEY, marker text)"
            )
            conn.execute(
                "ALTER PUBLICATION cdc_flight_pub ADD TABLE "
                f"{ANCHOR_TABLE}, {ANCHOR_PEER}"
            )
            conn.execute(
                f"INSERT INTO {ANCHOR_TABLE} VALUES "
                "(1, 'delete', repeat('x', 9000)), (2, 'keep', repeat('y', 9000))"
            )

        baseline = box.run(
            destination="motherduck",
            reset_state=True,
            extra_env=env,
            max_seconds=240,
            timeout=420,
        )
        assert baseline["ok"] is True, baseline
        md = connect(token, database)
        assert destination_rows() == source_rows()

        box.sql(
            [
                f"DELETE FROM {ANCHOR_TABLE} WHERE ctid = ("
                f"SELECT ctid FROM {ANCHOR_TABLE} WHERE id = 1 LIMIT 1)",
                f"INSERT INTO {ANCHOR_PEER} VALUES (1, 'healthy-{anchor}')",
            ],
            one_transaction=True,
        )
        fault_env = {**env, "CDC_FAULT_INJECT": f"{anchor}:1"}
        if anchor == "spill":
            fault_env.update({"CDC_UNIT_SPILL_EVENTS": "1", "CDC_UNIT_SPILL_BYTES": "512"})
        if anchor == "destination_hang":
            fault_env.update({"CDC_COMMIT_TIMEOUT": "5", "CDC_FAULT_HANG_SECONDS": "600"})
        failed = box.run(
            destination="motherduck",
            extra_env=fault_env,
            expect_success=False,
            max_seconds=60 if anchor == "destination_hang" else 240,
            timeout=120 if anchor == "destination_hang" else 420,
        )
        assert failed["returncode"] != 0, failed
        fired = box.fired_fault()
        assert fired is not None and fired["point"] == anchor, fired

        # A cloud destination can commit the row before PostgreSQL's next feedback
        # poll observes the durable LSN.  That is a loud, safe failed run (the
        # destination is durable and the source transaction is still replayable),
        # not a reason to weaken the acknowledgement proof.  Give the normal
        # restart path two more bounded opportunities; only an explicit
        # slot_acknowledgement_timeout may take the first retry.
        recovered = None
        for attempt in range(3):
            candidate = box.run(
                destination="motherduck",
                extra_env=env,
                expect_success=attempt == 2,
                max_seconds=240,
                timeout=420,
            )
            if candidate.get("ok"):
                recovered = candidate
                break
            assert "slot_acknowledgement_timeout" in candidate, candidate
        assert recovered is not None and recovered["ok"] is True, recovered
        assert destination_rows() == source_rows()
        assert md.execute(
            f"SELECT id, marker FROM \"{dataset}\".\"cdcflight_app_r15_keyless_delete_md_anchor_peer\""
        ).fetchall() == [(1, f"healthy-{anchor}")]
    finally:
        with contextlib.suppress(Exception), psycopg.connect(
            box.source.dsn, autocommit=True
        ) as conn:
            conn.execute(
                "ALTER PUBLICATION cdc_flight_pub DROP TABLE "
                f"{ANCHOR_TABLE}, {ANCHOR_PEER}"
            )
            conn.execute(f"DROP TABLE IF EXISTS {ANCHOR_TABLE}, {ANCHOR_PEER}")
        if md is not None:
            with contextlib.suppress(Exception):
                md.close()
        box.cleanup()
