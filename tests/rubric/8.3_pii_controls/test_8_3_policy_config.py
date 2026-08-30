"""Rubric §8.3 policy compiler and secret-boundary tests."""

from __future__ import annotations

import os

import pytest

from cdc_flight.policy import PIIPolicy, PolicyConfigurationError


def _salt(tmp_path):
    path = tmp_path / "pii-salt"
    path.write_bytes(b"test-only-secret-salt")
    os.chmod(path, 0o600)
    return path


def test_rules_are_fully_qualified_and_unmatched_is_fail_closed(tmp_path):
    policy = PIIPolicy.from_manifest(
        [
            {"column_regex": r"^app\.customers\.id$", "action": "replicate"},
            {"column_regex": r"^app\.customers\.email$", "action": "exclude"},
        ]
    )
    assert policy.rule_for("app.customers", "email").action == "exclude"
    assert policy.rule_for("app.customers", "new_column").action == "exclude"
    assert policy.rule_for("app.customers", "id").action == "replicate"
    assert policy.unmatched == "exclude"
    assert "test-only-secret-salt" not in repr(policy)


@pytest.mark.parametrize(
    "manifest",
    [
        [{"column_regex": r"^customers\.email$", "action": "exclude"}],
        [
            {"column_regex": r"^app\.customers\..*$", "action": "exclude"},
            {"column_regex": r"^app\.customers\.email$", "action": "mask", "replacement": "x"},
        ],
        [{"column_regex": r"^app\.customers\.ssn$", "action": "truncate"}],
        [{"column_regex": r"^app\.customers\.ssn$", "action": "hash", "algorithm": "SHA256", "salt_id": "v1"}],
    ],
)
def test_policy_rejects_ambiguous_or_incomplete_rules(manifest, tmp_path):
    with pytest.raises(PolicyConfigurationError):
        PIIPolicy.from_manifest(manifest, salt_file=_salt(tmp_path))


def test_hash_requires_private_salt_and_never_exposes_it(tmp_path):
    manifest = [
        {
            "column_regex": r"^app\.customers\.ssn$",
            "action": "hash",
            "algorithm": "HMAC-SHA-256",
            "salt_id": "pii-v1",
        }
    ]
    with pytest.raises(PolicyConfigurationError):
        PIIPolicy.from_manifest(manifest)
    salt = _salt(tmp_path)
    policy = PIIPolicy.from_manifest(manifest, salt_file=salt, epoch=4)
    assert policy.safe_manifest()["salt_id"] == "pii-v1"
    assert "test-only-secret-salt" not in repr(policy)
    assert "test-only-secret-salt" not in str(policy.safe_manifest())


def test_private_salt_permissions_are_enforced(tmp_path):
    path = _salt(tmp_path)
    os.chmod(path, 0o644)
    with pytest.raises(PolicyConfigurationError, match="private regular file"):
        PIIPolicy.from_manifest(
            [
                {
                    "column_regex": r"^app\.customers\.ssn$",
                    "action": "hash",
                    "algorithm": "HMAC-SHA-256",
                    "salt_id": "v1",
                }
            ],
            salt_file=path,
        )

