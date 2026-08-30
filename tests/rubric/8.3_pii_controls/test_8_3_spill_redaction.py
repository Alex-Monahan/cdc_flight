"""Rubric §8.3 spill boundary: only sanitized values are durable."""

from __future__ import annotations

import pickle

import pytest
from support.applier_lab import Lab, data

from cdc_flight.planner import stream_event_id
from cdc_flight.policy import PIIPolicy, PolicyGate
from cdc_flight.spill import SpillBuffer, StagedEvent
from cdc_flight.typed_types import SourceTypeDescriptor


def _policy(tmp_path):
    salt = tmp_path / "salt"
    salt.write_bytes(b"spill-only-salt")
    salt.chmod(0o600)
    return PIIPolicy.from_manifest(
        [
            {"column_regex": r"^app\.spill_pii\.id$", "action": "replicate"},
            {"column_regex": r"^app\.spill_pii\.excluded$", "action": "exclude"},
            {"column_regex": r"^app\.spill_pii\.masked$", "action": "mask", "replacement": "[MASK]"},
            {"column_regex": r"^app\.spill_pii\.hashed$", "action": "hash", "algorithm": "HMAC-SHA-256", "salt_id": "spill-v1"},
            {"column_regex": r"^app\.spill_pii\.truncated$", "action": "truncate", "max_chars": 3},
        ],
        salt_file=salt,
    )


def test_spill_refuses_an_unsanitized_record(tmp_path):
    box = Lab(tmp_path / "strict-spill.duckdb")
    try:
        event = data(
            "spill", 1, 100, table="spill_pii", key={"id": 1},
            after={"id": 1, "secret": "raw-secret"}
        )
        with pytest.raises(Exception, match="unsanitized"):
            SpillBuffer(
                box.con,
                policy_gate=PolicyGate(_policy(tmp_path)),
                require_sanitized=True,
            ).stage(
                commit_id=1,
                unit_seq=1,
                prepared=[
                    StagedEvent(
                        event=event,
                        event_id=stream_event_id(event),
                        target=box.target("spill_pii"),
                        seq=1,
                    )
                ],
            )
    finally:
        box.close()


def test_post_gate_event_and_spill_state_have_no_original_or_salt(tmp_path):
    # This drives the actual sanitizer and the actual Applier/SpillBuffer contract;
    # the source values that are policy-transformed are never passed to spill.
    box = Lab(tmp_path / "sanitized-spill.duckdb")
    try:
        policy = _policy(tmp_path)
        gate = PolicyGate(policy)
        descriptor = SourceTypeDescriptor(
            25, "pg_catalog.text", "text", output_function_oid=25
        )
        event = data(
            "spill", 1, 100, table="spill_pii", key={"id": 1},
            after={
                "id": 1,
                "excluded": "excluded-secret",
                "masked": "masked-secret",
                "hashed": "hashed-secret",
                "truncated": "truncated-secret",
            },
        )
    # The direct fixture descriptor has no output proof for the hash/truncate fields;
    # supply the source-output wrapper exactly as the PostgreSQL projection does.
        event.after_descriptors = {
            name: descriptor for name in event.after
        }
        event.after_descriptors["id"] = SourceTypeDescriptor(
            23, "pg_catalog.int4", "int4", output_function_oid=43
        )
        from cdc_flight.policy import PostgreSQLOutputText

        event.output_texts = {
            "after": {
                "hashed": PostgreSQLOutputText("hashed-secret-output", 25),
                "truncated": PostgreSQLOutputText("truncated-secret-output", 25),
            }
        }
        gate.sanitize(event, event.after_descriptors)
        hashed = event.after["hashed"]
        assert event.after == {
            "id": 1,
            "masked": "[MASK]",
            "hashed": hashed,
            "truncated": "tru",
        }
        assert "excluded" not in event.after
        assert event.raw is not None
        assert "excluded-secret" not in repr(event)
        assert "spill-only-salt" not in repr(event)
        with pytest.raises(TypeError, match="non-serializable"):
            pickle.dumps(event.raw)  # acknowledgement handle is process-local

        spill = SpillBuffer(box.con, policy_gate=gate, require_sanitized=True)
        spill.stage(
            commit_id=9,
            unit_seq=1,
            prepared=[
                StagedEvent(
                    event=event,
                    event_id=stream_event_id(event),
                    target=box.target("spill_pii"),
                    seq=1,
                )
            ],
        )
        durable = box.q(
            "SELECT before_json, after_json, key_json, policy_digest "
            "FROM _cdc_flight.spill_events WHERE commit_id = 9 AND unit_seq = 1"
        )
        assert len(durable) == 1
        assert all(
            value is None
            or all(secret not in value for secret in ("excluded-secret", "masked-secret", "hashed-secret", "truncated-secret", "spill-only-salt"))
            for value in durable[0][:3]
        )
        assert durable[0][3] == policy.digest
        replayed = spill.load(commit_id=9, unit_seq=1)
        assert len(replayed) == 1
        assert replayed[0].event.after == event.after
    finally:
        box.close()
