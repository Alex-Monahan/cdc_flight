"""Rubric §8.3 replay identity is over sanitized, versioned data only."""

from __future__ import annotations

import os

import pytest
from support.applier_lab import data

from cdc_flight.event_ledger import payload_digest
from cdc_flight.policy import PIIPolicy, PolicyGate, PostgreSQLOutputText
from cdc_flight.typed_types import SourceTypeDescriptor


def test_memory_and_replay_sanitized_digests_match_without_plaintext(tmp_path):
    salt = tmp_path / "salt"
    salt.write_bytes(b"replay-salt")
    salt.chmod(0o600)
    policy = PIIPolicy.from_manifest(
        [
            {"column_regex": r"^app\.replay_pii\.id$", "action": "replicate"},
            {"column_regex": r"^app\.replay_pii\.email$", "action": "hash", "algorithm": "HMAC-SHA-256", "salt_id": "replay-v1"},
        ],
        salt_file=salt,
    )
    descriptor = SourceTypeDescriptor(
        25, "pg_catalog.text", "text", output_function_oid=25
    )

    def build(secret):
        event = data(
            "replay", 1, 100, table="replay_pii", key={"id": 1},
            after={"id": 1, "email": secret}
        )
        event.after_descriptors = {"id": descriptor, "email": descriptor}
        event.output_texts = {"after": {"email": PostgreSQLOutputText(secret, 25)}}
        PolicyGate(policy).sanitize(event, event.after_descriptors)
        return event, payload_digest(event)

    first, first_digest = build("one-secret@example.invalid")
    second, second_digest = build("one-secret@example.invalid")
    assert first_digest == second_digest
    assert first.after["email"] == second.after["email"]
    assert "one-secret@example.invalid" not in repr(first)
    assert policy.salt_id == "replay-v1"
    assert "replay-salt" not in repr(policy)
