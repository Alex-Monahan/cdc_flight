"""Real PostgreSQL-to-DuckDB hard/soft current-state delete effects."""

from __future__ import annotations

import contextlib
import json

import pytest


pytestmark = pytest.mark.e2e

HARD_TABLE = "app.p8_delete_hard"
SOFT_TABLE = "app.p8_delete_soft"
CAPTURE = {
    "CDC_TABLES": "p8_delete_hard,p8_delete_soft",
    "CDC_AUTO_DISCOVERY": "0",
}
SOFT_RULE = json.dumps({SOFT_TABLE: "soft"})


@pytest.fixture(scope="module")
def delete_scenario(sandbox):
    sandbox.reseed()
    sandbox.sql(
        [
            f"CREATE TABLE {HARD_TABLE} (id integer PRIMARY KEY, name text)",
            f"CREATE TABLE {SOFT_TABLE} (id integer PRIMARY KEY, name text)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE " + HARD_TABLE,
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE " + SOFT_TABLE,
            f"INSERT INTO {HARD_TABLE} VALUES (1, 'hard-before')",
            f"INSERT INTO {SOFT_TABLE} VALUES (1, 'soft-before')",
        ]
    )
    try:
        env = {**CAPTURE, "CDC_DELETE_MODE": "hard", "CDC_DELETE_POLICY_EPOCH": "1"}
        baseline = sandbox.run(reset_state=True, extra_env=env, max_seconds=180)
        assert baseline["ok"] is True, baseline

        sandbox.sql(
            [
                f"DELETE FROM {HARD_TABLE} WHERE id = 1",
                f"DELETE FROM {SOFT_TABLE} WHERE id = 1",
            ],
            one_transaction=True,
        )
        delete_run = sandbox.run(
            extra_env={
                **CAPTURE,
                "CDC_DELETE_MODE": "hard",
                "CDC_DELETE_MODE_RULES": SOFT_RULE,
                "CDC_DELETE_POLICY_EPOCH": "2",
            },
            max_seconds=180,
        )
        assert delete_run["ok"] is True, delete_run

        # A later source INSERT is the only allowed way to clear a soft tombstone.
        sandbox.sql(f"INSERT INTO {SOFT_TABLE} VALUES (1, 'soft-reinserted')")
        reinsert_run = sandbox.run(
            extra_env={
                **CAPTURE,
                "CDC_DELETE_MODE": "hard",
                "CDC_DELETE_MODE_RULES": SOFT_RULE,
                "CDC_DELETE_POLICY_EPOCH": "2",
            },
            max_seconds=180,
        )
        assert reinsert_run["ok"] is True, reinsert_run
        yield sandbox
    finally:
        with contextlib.suppress(Exception):
            sandbox.sql("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + HARD_TABLE)
        with contextlib.suppress(Exception):
            sandbox.sql("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + SOFT_TABLE)
        sandbox.reseed()


def test_hard_delete_is_physical_and_soft_delete_is_a_hidden_tombstone(delete_scenario):
    box = delete_scenario
    hard_target = "cdcflight_app_p8_delete_hard"
    soft_target = "cdcflight_app_p8_delete_soft"
    assert box.duck_query(
        f"SELECT count(*) FROM {box.table(hard_target)}"
    ) == [(0,)]
    assert box.duck_query(
        f"SELECT id, name, cdcf_deleted FROM {box.table(soft_target)}"
    ) == [(1, "soft-reinserted", False)]
    assert box.duck_query(
        f"SELECT count(*) FROM {box.table(soft_target + '__live')}"
    ) == [(1,)]


def test_delete_metadata_policy_state_and_ledger_are_durable(delete_scenario):
    box = delete_scenario
    for target, mode in (
        ("cdcflight_app_p8_delete_hard", "hard"),
        ("cdcflight_app_p8_delete_soft", "soft"),
    ):
        assert box.duck_query(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "AND column_name IN ('cdcf_deleted', 'cdcf_delete_event_id', 'cdcf_delete_lsn')",
            [box.DATASET, target],
        ) == [(3,)]
        assert box.duck_query(
            "SELECT delete_mode, delete_policy_epoch, delete_policy_digest "
            "FROM _cdc_flight.table_state WHERE target_table = ?",
            [target],
        )[0][:2] == (mode, 2)
    assert box.duck_query(
        "SELECT target_table, delete_mode, policy_epoch, effect_state "
        "FROM _cdc_flight.delete_ledger ORDER BY target_table"
    ) == [
        ("cdcflight_app_p8_delete_hard", "hard", 2, "applied"),
        ("cdcflight_app_p8_delete_soft", "soft", 2, "applied"),
    ]
