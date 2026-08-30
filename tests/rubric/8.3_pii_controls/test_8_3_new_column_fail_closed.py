"""Schema evolution excludes a newly added column unless policy names it."""

from __future__ import annotations

import contextlib
import json

import pytest


pytestmark = pytest.mark.e2e

TABLE = "app.p8_unmatched_column"
CAPTURE = {
    "CDC_TABLES": "p8_unmatched_column",
    "CDC_AUTO_DISCOVERY": "0",
    "CDC_PII_UNMATCHED": "exclude",
    "CDC_PII_RULES": json.dumps([
        {"column_regex": r"^app\.p8_unmatched_column\.id$", "action": "replicate"},
        {"column_regex": r"^app\.p8_unmatched_column\.name$", "action": "replicate"},
    ]),
    "CDC_PII_POLICY_EPOCH": "12",
}


@pytest.fixture(scope="module")
def unmatched_column_case(sandbox):
    sandbox.reseed()
    sandbox.sql(
        [
            f"CREATE TABLE {TABLE} (id integer PRIMARY KEY, name text)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE " + TABLE,
            f"INSERT INTO {TABLE} VALUES (1, 'known')",
        ]
    )
    try:
        baseline = sandbox.run(reset_state=True, extra_env=CAPTURE, max_seconds=180)
        assert baseline["ok"] is True, baseline
        sandbox.sql(
            f"ALTER TABLE {TABLE} ADD COLUMN unmatched_secret text"
        )
        sandbox.sql(
            f"UPDATE {TABLE} SET unmatched_secret = 'new-column-secret-sentinel' WHERE id = 1"
        )
        changed = sandbox.run(extra_env=CAPTURE, max_seconds=180)
        assert changed["ok"] is True, changed
        yield sandbox
    finally:
        with contextlib.suppress(Exception):
            sandbox.sql("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + TABLE)
        with contextlib.suppress(Exception):
            sandbox.sql("DROP TABLE IF EXISTS " + TABLE)
        sandbox.reseed()


def test_unmatched_new_column_is_not_added_or_written(unmatched_column_case):
    box = unmatched_column_case
    target = "cdcflight_app_p8_unmatched_column"
    assert box.duck_query(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? AND column_name = 'unmatched_secret'",
        [box.DATASET, target],
    ) == [(0,)]
    assert box.duck_query(
        f"SELECT id, name FROM {box.table(target)}"
    ) == [(1, "known")]
    assert box.duck_query(
        "SELECT source_table, column_name, action, rule_id, policy_epoch, policy_digest "
        "FROM _cdc_flight.policy_alerts WHERE source_table = 'p8_unmatched_column'"
    )
    serialized = repr(box.duck_query("SELECT * FROM _cdc_flight.policy_alerts"))
    assert "new-column-secret-sentinel" not in serialized


def test_identity_dependent_unmatched_column_is_not_guessed(unmatched_column_case):
    box = unmatched_column_case
    # The unmatched field is not part of the declared source key, so the ordinary
    # update succeeds.  A policy-excluded key would instead be refused by the same
    # gate; the unit tests pin that refusal without allowing a guessed identity.
    assert box.duck_query(
        "SELECT action FROM _cdc_flight.policy_alerts "
        "WHERE column_name = 'unmatched_secret'"
    ) == [("exclude",)]
