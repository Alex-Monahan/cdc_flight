"""Crash/replay preserves a sanitized destination and redacted diagnostics."""

from __future__ import annotations

import json

import hashlib
import hmac
import pytest


pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _policy_env(tmp_path):
    salt = tmp_path / "p8-crash-policy-salt"
    salt.write_bytes(b"p8-crash-private-salt")
    salt.chmod(0o600)
    rules = [
        {"column_regex": r"^app\.customers\.id$", "action": "replicate"},
        {"column_regex": r"^app\.customers\.name$", "action": "mask", "replacement": "[MASKED]"},
        {"column_regex": r"^app\.customers\.email$", "action": "hash", "algorithm": "HMAC-SHA-256", "salt_id": "p8-crash-v1"},
    ]
    return {
        "CDC_TABLES": "customers",
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_PII_UNMATCHED": "exclude",
        "CDC_PII_RULES": json.dumps(rules),
        "CDC_PII_HASH_SALT_FILE": str(salt),
        "CDC_PII_POLICY_EPOCH": "4",
    }, salt


@pytest.mark.parametrize("anchor", ["pre_commit", "post_commit_pre_ack"])
def test_sanitized_values_and_diagnostics_survive_crash_replay(sandbox, tmp_path, anchor):
    sandbox.reseed()
    policy, salt = _policy_env(tmp_path)
    baseline = sandbox.run(reset_state=True, extra_env=policy, max_seconds=180)
    assert baseline["ok"] is True, baseline

    sentinel_name = "p8-name-sentinel-never-durable"
    sentinel_email = "p8-email-sentinel-never-durable@example.invalid"
    sandbox.sql(
        "INSERT INTO app.customers (name, email) VALUES (%s, %s)"
        % (repr(sentinel_name), repr(sentinel_email))
    )
    crashed = sandbox.run(
        extra_env={**policy, "CDC_FAULT_INJECT": f"{anchor}:1"},
        expect_success=False,
        max_seconds=180,
    )
    assert crashed["returncode"] == 137, crashed
    recovered = sandbox.run(extra_env=policy, max_seconds=180)
    assert recovered["ok"] is True, recovered
    ident = sandbox.pg_query(
        "SELECT id FROM app.customers WHERE name = %s" % repr(sentinel_name)
    )[0][0]
    expected_email = hmac.new(
        b"p8-crash-private-salt", sentinel_email.encode(), hashlib.sha256
    ).hexdigest()
    assert sandbox.duck_query(
        "SELECT id, name, email FROM \"cdc_raw\".\"cdcflight_app_customers\" "
        "WHERE id = ?",
        [ident],
    ) == [(ident, "[MASKED]", expected_email)]

    # The source originals and the private salt are absent from all durable
    # policy/spill/log surfaces and from the process summary left by the crash.
    for table in ("_cdc_flight.spill_events", "_cdc_flight.run_logs", "_cdc_flight.alerts"):
        rows = sandbox.duck_query(f"SELECT * FROM {table}")
        serialized = repr(rows)
        assert sentinel_name not in serialized
        assert sentinel_email not in serialized
        assert "p8-crash-private-salt" not in serialized
    summary = sandbox.last_summary()
    assert sentinel_name not in json.dumps(summary)
    assert sentinel_email not in json.dumps(summary)
    assert "p8-crash-private-salt" not in json.dumps(summary)
