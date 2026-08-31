"""Re-snapshot hydration cannot bypass the application PII gate."""

from __future__ import annotations

import hashlib
import hmac
import types

import pytest

from cdc_flight.applier import Applier
from cdc_flight.catalog_descriptors import RelationDescriptorProvider
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.planner import GroupPlan
from cdc_flight.policy import (
    PIIPolicy,
    PolicyGate,
    PolicyValueRefused,
    PostgreSQLOutputText,
)
from cdc_flight.typed_types import SourceTypeDescriptor

SENTINEL = "ORIGINAL_XML_ARRAY_SENTINEL"
ID = SourceTypeDescriptor(
    23,
    "pg_catalog.int4",
    "int4",
    output_function_oid=42,
)
XML = SourceTypeDescriptor(
    142,
    "pg_catalog.xml",
    "xml",
    output_function_oid=143,
)
XML_ARRAY = SourceTypeDescriptor(
    143,
    "pg_catalog.xml[]",
    "array",
    array_element=XML,
    output_function_oid=144,
)


def _policy(action: str, salt_path) -> PIIPolicy:
    rule = {
        "column_regex": r"^app\.p8_resnapshot\.x$",
        "action": action,
    }
    if action == "mask":
        rule["replacement"] = "[MASKED]"
    elif action == "truncate":
        rule["max_chars"] = 7
    elif action == "hash":
        rule.update({"algorithm": "HMAC-SHA-256", "salt_id": "resnap-v1"})
    return PIIPolicy.from_manifest(
        [rule], unmatched="replicate", salt_file=salt_path
    )


def _key_policy(action: str, salt_path) -> PIIPolicy:
    rule = {
        "column_regex": r"^app\.p8_resnapshot\.id$",
        "action": action,
    }
    if action == "mask":
        rule["replacement"] = "[MASKED]"
    elif action == "truncate":
        rule["max_chars"] = 7
    elif action == "hash":
        rule.update({"algorithm": "HMAC-SHA-256", "salt_id": "resnap-v1"})
    return PIIPolicy.from_manifest(
        [rule], unmatched="replicate", salt_file=salt_path
    )


def _provider() -> RelationDescriptorProvider:
    provider = RelationDescriptorProvider(
        {"app.p8_resnapshot": {"id": ID, "x": XML_ARRAY}},
        source_dsn="unused-for-unit-test",
    )

    def read_event_columns(owner, event, value_columns):
        if owner.policy_gate is None:
            # This is the pre-fix behavior the regression must make impossible:
            # an unconfigured provider returns the source image unchanged.
            return {name: SENTINEL for name in value_columns}
        raw = {name: PostgreSQLOutputText(SENTINEL, XML_ARRAY.output_function_oid)
               for name in value_columns}
        output_texts = {
            name: raw[name] for name in value_columns
        }
        return owner.policy_gate.sanitize_mapping(
            event.qualified_table,
            raw,
            {name: XML_ARRAY for name in value_columns},
            output_texts=output_texts,
        )

    provider.read_event_columns = types.MethodType(read_event_columns, provider)
    return provider


def _event(gate: PolicyGate) -> PendingRecord:
    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="cdcflight.app.p8_resnapshot",
        nbytes=1,
        op="r",
        schema="app",
        table="p8_resnapshot",
        lsn=100,
        key={"id": 1},
        after={"id": 1},
        sanitized=True,
        policy_epoch=gate.policy.epoch,
        policy_digest=gate.policy.digest,
        key_descriptors={"id": ID},
        after_descriptors={"id": ID},
    )
    return event


def _plan(provider, gate):
    plan = object.__new__(GroupPlan)
    plan.descriptor_provider = provider.descriptors_for
    plan.policy_gate = gate
    plan._catalog_descriptor_cache = {}
    return plan


@pytest.mark.parametrize("action", ["exclude", "mask", "hash", "truncate"])
def test_resnapshot_hydration_applies_every_policy_control(tmp_path, action):
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"resnapshot-private-salt")
    salt_path.chmod(0o600)
    gate = PolicyGate(_policy(action, salt_path))
    provider = _provider()
    event = _event(gate)

    # This is the production constructor operation.  Before the fix, the bound
    # method assignment was swallowed and this owner remained ungated.
    Applier._attach_policy_gate(
        provider.descriptors_for, gate, owner_name="descriptor provider"
    )
    assert provider.policy_gate is gate
    _plan(provider, gate)._enrich_descriptors(event)

    if action == "exclude":
        assert "x" not in (event.after or {})
        assert "x" not in event.after_descriptors
    else:
        expected = {
            "mask": "[MASKED]",
            "hash": hmac.new(
                b"resnapshot-private-salt", SENTINEL.encode(), hashlib.sha256
            ).hexdigest(),
            "truncate": SENTINEL[:7],
        }[action]
        assert event.after["x"] == expected
        assert event.after["x"] != SENTINEL
        assert dict(event.after_descriptors["x"].metadata)["policy_action"] == action
        assert event.typed_after.field("x").value == expected


def test_resnapshot_gate_attachment_rejects_immutable_provider():
    class Immutable:
        __slots__ = ()

    with pytest.raises(TypeError, match="writable policy_gate"):
        Applier._attach_policy_gate(
            Immutable(), PolicyGate(), owner_name="immutable provider"
        )


def test_resnapshot_source_recovery_rejects_every_transformed_key_before_query(
    tmp_path,
):
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"resnapshot-private-salt")
    salt_path.chmod(0o600)
    from cdc_flight.catalog_support import _read_event_columns

    for action in ("exclude", "mask", "hash", "truncate"):
        gate = PolicyGate(_key_policy(action, salt_path))
        event = _event(gate)
        event.key = {"id": 1}
        with pytest.raises(PolicyValueRefused, match="source key"):
            # The fake connection must never be reached: key policy is checked
            # before any source-row acquisition or hydration assignment.
            _read_event_columns(
                object(),
                event,
                ("x",),
                {"id": "id", "x": "x"},
                policy_gate=gate,
                descriptors={"x": XML_ARRAY, "id": ID},
            )


def test_resnapshot_recovery_without_a_gate_is_a_schema_refusal():
    provider = RelationDescriptorProvider(
        {"app.p8_resnapshot": {"x": XML_ARRAY}},
        source_dsn="unused-for-unit-test",
    )
    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="cdcflight.app.p8_resnapshot",
        nbytes=1,
        op="r",
        schema="app",
        table="p8_resnapshot",
    )
    with pytest.raises(SchemaEvolutionRefused, match="policy gate"):
        provider.read_event_columns(event, ("x",))

    from cdc_flight.catalog_support import _read_event_columns

    with pytest.raises(SchemaEvolutionRefused, match="policy gate"):
        _read_event_columns(object(), event, ("x",), {"x": "x"})
