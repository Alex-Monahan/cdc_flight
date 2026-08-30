"""Crash/replay proof for keyed and keyless deletes in both modes."""

from __future__ import annotations

import pytest


pytestmark = [pytest.mark.slow, pytest.mark.e2e]


CAPTURE = {
    "CDC_TABLES": "customers,sensor_readings",
    "CDC_AUTO_DISCOVERY": "0",
}


@pytest.mark.parametrize("mode", ["hard", "soft"])
@pytest.mark.parametrize("anchor", ["pre_commit", "post_commit_pre_ack"])
def test_keyed_and_keyless_delete_replay_is_exactly_once(sandbox, mode, anchor):
    sandbox.reseed()
    base = {**CAPTURE, "CDC_DELETE_MODE": mode, "CDC_DELETE_POLICY_EPOCH": "1"}
    baseline = sandbox.run(reset_state=True, extra_env=base, max_seconds=180)
    assert baseline["ok"] is True, baseline

    sandbox.sql(
        [
            "INSERT INTO app.customers (name, email) "
            "VALUES ('p8-delete-crash-keyed', 'p8-delete-crash-keyed@example.invalid')",
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) "
            "VALUES ('p8-delete-crash-keyless', 901.25, 'C')",
        ],
        one_transaction=True,
    )
    delivered = sandbox.run(extra_env=base, max_seconds=180)
    assert delivered["ok"] is True, delivered
    customer_id = sandbox.pg_query(
        "SELECT id FROM app.customers WHERE name = 'p8-delete-crash-keyed'"
    )[0][0]

    sandbox.sql(
        [
            "DELETE FROM app.customers WHERE id = %s" % int(customer_id),
            "DELETE FROM app.sensor_readings WHERE ctid = ("
            "SELECT ctid FROM app.sensor_readings "
            "WHERE sensor_id = 'p8-delete-crash-keyless' LIMIT 1)",
        ],
        one_transaction=True,
    )
    crashed = sandbox.run(
        extra_env={**base, "CDC_FAULT_INJECT": f"{anchor}:1"},
        expect_success=False,
        max_seconds=180,
    )
    assert crashed["returncode"] == 137, crashed
    recovered = sandbox.run(extra_env=base, max_seconds=180)
    assert recovered["ok"] is True, recovered

    keyed = sandbox.duck_query(
        "SELECT count(*), coalesce(bool_or(cdcf_deleted), false) "
        "FROM \"cdc_raw\".\"cdcflight_app_customers\" "
        "WHERE id = ?",
        [customer_id],
    )[0]
    keyless = sandbox.duck_query(
        "SELECT count(*), coalesce(bool_or(cdcf_deleted), false) "
        "FROM \"cdc_raw\".\"cdcflight_app_sensor_readings\" "
        "WHERE sensor_id = 'p8-delete-crash-keyless'"
    )[0]
    if mode == "hard":
        assert keyed == (0, False)
        assert keyless == (0, False)
    else:
        assert keyed == (1, True)
        assert keyless == (1, True)
        assert sandbox.duck_query(
            "SELECT count(*) FROM \"cdc_raw\".\"cdcflight_app_customers__live\" "
            "WHERE id = ?",
            [customer_id],
        ) == [(0,)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM \"cdc_raw\".\"cdcflight_app_sensor_readings__live\" "
            "WHERE sensor_id = 'p8-delete-crash-keyless'"
        ) == [(0,)]

    ledger = sandbox.duck_query(
        "SELECT count(*), count(DISTINCT event_id) FROM _cdc_flight.delete_ledger "
        "WHERE target_table IN ('cdcflight_app_customers', 'cdcflight_app_sensor_readings')"
    )
    assert ledger == [(2, 2)]
