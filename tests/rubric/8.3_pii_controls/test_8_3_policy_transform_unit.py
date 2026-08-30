"""Rubric §8.3 transforms, provenance, and fail-closed output handling."""

from __future__ import annotations

import hashlib
import hmac

import pytest
from support.applier_lab import Lab, data

import cdc_flight.applier as applier_module
from cdc_flight.errors import AdmissionError, SchemaEvolutionRefused
from cdc_flight.policy import (
    AcknowledgementHandle,
    PIIPolicy,
    PolicyValueRefused,
    PostgreSQLOutputText,
)
from cdc_flight.typed_types import SourceTypeDescriptor, TypedImage


def _descriptor(kind, oid, *, child=None, fields=()):
    return SourceTypeDescriptor(
        oid,
        f"app.{kind}_{oid}",
        kind,
        array_element=child,
        composite_fields=fields,
        output_function_oid=oid + 1000,
    )


TEXT = _descriptor("text", 25)
INT = _descriptor("int4", 23)
DATE = _descriptor("date", 1082)
UUID = _descriptor("uuid", 2950)
JSON = _descriptor("json", 114)
JSONB = _descriptor("jsonb", 3802)
BYTEA = _descriptor("bytea", 17)
INET = _descriptor("inet", 869)
CIDR = _descriptor("cidr", 650)
ARRAY = _descriptor("array", 1009, child=TEXT)
RANGE = _descriptor("range", 3904, child=INT)
COMPOSITE = _descriptor("composite", 9000, fields=(("id", INT), ("note", TEXT)))
MONEY = _descriptor("money", 790)
XML = _descriptor("xml", 142)


def _policy(action, column, *, salt_path=None, max_chars=None, replacement=None):
    raw = {"column_regex": rf"^app\.pii\.{column}$", "action": action}
    if replacement is not None:
        raw["replacement"] = replacement
    if max_chars is not None:
        raw["max_chars"] = max_chars
    if action == "hash":
        raw.update({"algorithm": "HMAC-SHA-256", "salt_id": "test-v1"})
    return PIIPolicy.from_manifest(
        [raw], unmatched="replicate", salt_file=salt_path
    )


@pytest.mark.parametrize(
    ("column", "descriptor", "value", "output"),
    [
        ("text", TEXT, "unused", "PostgreSQL text output"),
        ("int", INT, 42, "42"),
        ("date", DATE, "unused", "2026-08-29"),
        ("uuid", UUID, "unused", "550e8400-e29b-41d4-a716-446655440000"),
        ("json", JSON, {"a": 1}, '{"a": 1}'),
        ("jsonb", JSONB, {"a": 1}, '{"a":1}'),
        ("bytea", BYTEA, b"abc", "\\x616263"),
        ("inet", INET, "unused", "192.0.2.1/32"),
        ("cidr", CIDR, "unused", "192.0.2.0/24"),
        ("array", ARRAY, ["a"], "{a}"),
        ("range", RANGE, "unused", "[1,10)"),
        ("composite", COMPOSITE, {"id": 1, "note": "x"}, "(1,x)"),
        ("money", MONEY, "unused", "$12.34"),
        ("xml", XML, "unused", "<a>value</a>"),
    ],
)
def test_hash_uses_only_the_explicit_postgresql_output_proof(
    tmp_path, column, descriptor, value, output
):
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"unit-salt")
    salt_path.chmod(0o600)
    policy = _policy("hash", column, salt_path=salt_path)
    sanitized = policy.sanitize_mapping(
        "app.pii",
        {column: value},
        {column: descriptor},
        output_texts={
            column: PostgreSQLOutputText(output, descriptor.output_function_oid)
        },
    )
    expected = hmac.new(
        b"unit-salt", output.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    assert sanitized == {column: expected}


def test_exclusion_and_masking_do_not_need_or_retain_source_output(tmp_path):
    excluded = _policy("exclude", "email").sanitize_mapping(
        "app.pii", {"email": "secret@example.invalid"}, {"email": TEXT}
    )
    masked = _policy("mask", "email", replacement="[REDACTED]").sanitize_mapping(
        "app.pii", {"email": "secret@example.invalid"}, {"email": TEXT}
    )
    assert excluded == {}
    assert masked == {"email": "[REDACTED]"}
    assert "secret@example.invalid" not in repr(masked)


def test_per_column_regex_truncation_is_unicode_and_not_global():
    policy = PIIPolicy.from_manifest(
        [
            {
                "column_regex": r"^app\.pii\.notes$",
                "action": "truncate",
                "max_chars": 4,
            },
            {"column_regex": r"^app\.pii\.id$", "action": "replicate"},
        ],
        unmatched="exclude",
    )
    result = policy.sanitize_mapping(
        "app.pii",
        {"notes": "😀é漢字XYZ", "id": 7},
        {"notes": TEXT, "id": INT},
        output_texts={"notes": PostgreSQLOutputText("😀é漢字XYZ", TEXT.output_function_oid)},
    )
    assert result == {"notes": "😀é漢字", "id": 7}


def test_unproven_non_text_policy_input_refuses_instead_of_synthesizing_text(tmp_path):
    salt = tmp_path / "salt"
    salt.write_bytes(b"unit-salt")
    salt.chmod(0o600)
    policy = _policy("hash", "quantity", salt_path=salt)
    # A descriptor alone is not proof that the Python object is PostgreSQL output.
    with pytest.raises(PolicyValueRefused, match="OUTPUT proof"):
        policy.sanitize_mapping(
            "app.pii", {"quantity": 17}, {"quantity": INT}
        )


def test_null_does_not_fabricate_policy_text(tmp_path):
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"unit-salt")
    salt_path.chmod(0o600)
    policy = _policy("hash", "notes", salt_path=salt_path)
    assert policy.sanitize_mapping(
        "app.pii", {"notes": None}, {"notes": TEXT}
    ) == {"notes": None}


@pytest.mark.parametrize("action", ["exclude", "mask", "hash", "truncate"])
def test_policy_refusal_seals_key_and_after_before_the_seam_returns(tmp_path, action):
    """A policy refusal from the post-decode gate cannot retain source images."""
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"unit-salt")
    salt_path.chmod(0o600)
    raw_rule = {
        "column_regex": r"^app\.pii_refusal\.secret$",
        "action": action,
    }
    if action == "hash":
        raw_rule.update({"algorithm": "HMAC-SHA-256", "salt_id": "test-v1"})
    if action == "mask":
        raw_rule["replacement"] = "[MASKED]"
    if action == "truncate":
        raw_rule["max_chars"] = 4
    policy = PIIPolicy.from_manifest(
        [raw_rule], unmatched="replicate", salt_file=salt_path
    )
    box = Lab(tmp_path / f"{action}.duckdb", pii_policy=policy)
    try:
        record = data(
            "refusal-txn",
            1,
            101,
            table="pii_refusal",
            key={"secret": "KEY_SOURCE_SENTINEL"},
            after={"secret": "AFTER_SOURCE_SENTINEL"},
        )
        descriptor = SourceTypeDescriptor(
            25,
            "pg_catalog.text",
            "text",
            output_function_oid=1009,
        )
        box._fixture_descriptor_map[record.qualified_table] = {"secret": descriptor}

        # Before this regression fix, sanitize() raises out of _sanitize_record and
        # these assertions are never reached.  The seam must return a sealed record.
        returned = box.applier._sanitize_record(record)
        assert returned is record
        assert isinstance(record.admission_refusal, SchemaEvolutionRefused)
        assert record.key is None
        assert record.before is None
        assert record.after is None
        assert record.typed_key is None
        assert record.typed_before is None
        assert record.typed_after is None
        assert record.output_texts == {}
        assert record.sanitized is True
        assert isinstance(record.raw, AcknowledgementHandle)
        assert "KEY_SOURCE_SENTINEL" not in repr(record)
        assert "AFTER_SOURCE_SENTINEL" not in repr(record)
    finally:
        box.close()


def _boundary_failure_record(box):
    record = data(
        "boundary-failure",
        1,
        201,
        table="pii_boundary",
        key={"secret": "KEY_SOURCE_SENTINEL"},
        before={"secret": "BEFORE_SOURCE_SENTINEL"},
        after={"secret": "AFTER_SOURCE_SENTINEL"},
    )
    descriptor = SourceTypeDescriptor(
        25,
        "pg_catalog.text",
        "text",
        output_function_oid=1009,
    )
    box._fixture_descriptor_map[record.qualified_table] = {"secret": descriptor}
    record.key_descriptors = {"secret": descriptor}
    record.before_descriptors = {"secret": descriptor}
    record.after_descriptors = {"secret": descriptor}
    record.typed_key = TypedImage.from_mapping(record.key, record.key_descriptors)
    record.typed_before = TypedImage.from_mapping(record.before, record.before_descriptors)
    record.typed_after = TypedImage.from_mapping(record.after, record.after_descriptors)
    record.output_texts = {
        "after": {
            "secret": PostgreSQLOutputText("OUTPUT_SOURCE_SENTINEL", 1009),
        }
    }
    record.value_schema = {"source": "VALUE_SCHEMA_SENTINEL"}
    record.key_schema = {"source": "KEY_SCHEMA_SENTINEL"}
    record.before_schema = {"source": "BEFORE_SCHEMA_SENTINEL"}
    record.after_schema = {"source": "AFTER_SCHEMA_SENTINEL"}
    return record


def _boundary_refusal_policy():
    return PIIPolicy.from_manifest(
        [
            {
                "column_regex": r"^app\.pii_boundary\.secret$",
                "action": "exclude",
            }
        ],
        unmatched="replicate",
    )


def _raise_original_boundary_failure():
    raise RuntimeError("original boundary failure")


def _assert_boundary_payload_is_stripped(record, original_raw, *, schemas=True):
    assert record.key is None
    assert record.before is None
    assert record.after is None
    assert record.key_descriptors == {}
    assert record.before_descriptors == {}
    assert record.after_descriptors == {}
    assert record.typed_key is None
    assert record.typed_before is None
    assert record.typed_after is None
    if schemas:
        assert record.value_schema is None
        assert record.key_schema is None
        assert record.before_schema is None
        assert record.after_schema is None
    assert record.output_texts == {}
    assert isinstance(record.raw, AcknowledgementHandle)
    assert record.raw is not original_raw


def test_policy_boundary_sealer_failure_strips_payload_and_reraises(tmp_path, monkeypatch):
    box = Lab(tmp_path / "sealer-failure.duckdb", pii_policy=_boundary_refusal_policy())
    try:
        record = _boundary_failure_record(box)
        original_raw = record.raw

        def fail_inside_sealer(_event, _refusal):
            raise RuntimeError("injected sealer failure")

        monkeypatch.setattr(
            box.applier.policy_gate, "seal_refusal", fail_inside_sealer
        )
        with pytest.raises(RuntimeError, match="injected sealer failure"):
            box.applier._sanitize_record(record)
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


def test_policy_boundary_cleanup_setter_failure_preserves_original_and_clears_payload(
    tmp_path, monkeypatch
):
    box = Lab(tmp_path / "cleanup-setter-failure.duckdb")
    try:
        record = _boundary_failure_record(box)
        original_raw = record.raw

        def fail_if_called(*_args, **_kwargs):
            raise SystemExit("cleanup setter must not be called")

        monkeypatch.setattr(applier_module, "_set_policy_boundary_field", fail_if_called)
        monkeypatch.setattr(
            box.applier,
            "_activate_delete_policy_at_boundary",
            _raise_original_boundary_failure,
        )
        with pytest.raises(RuntimeError, match="original boundary failure"):
            box.applier._sanitize_record(record)
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


def test_policy_boundary_ack_constructor_failure_still_replaces_raw_source(
    tmp_path, monkeypatch
):
    box = Lab(tmp_path / "ack-constructor-failure.duckdb")
    try:
        record = _boundary_failure_record(box)
        original_raw = record.raw

        class FailingAcknowledgementHandle(AcknowledgementHandle):
            def __init__(self, _delegate):
                raise SystemExit("ack constructor must not be called")

        monkeypatch.setattr(
            applier_module, "AcknowledgementHandle", FailingAcknowledgementHandle
        )
        monkeypatch.setattr(
            box.applier,
            "_activate_delete_policy_at_boundary",
            _raise_original_boundary_failure,
        )
        with pytest.raises(RuntimeError, match="original boundary failure"):
            box.applier._sanitize_record(record)
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


def _unusual_image_record(box, shape):
    record = data(
        "unusual-image",
        1,
        301,
        table="pii_boundary",
        key=None,
        before=None,
        after=None,
    )
    descriptor = TEXT
    if shape == "empty_dict":
        record.after = {}
    elif shape == "non_dict":
        record.after = "NOT_A_MAPPING"
    record.after_descriptors = {"secret": descriptor}
    record.typed_after = TypedImage.from_mapping(
        {"secret": "IMAGE_SOURCE_SENTINEL"}, {"secret": descriptor}
    )
    record.output_texts = {
        "after": {
            "secret": PostgreSQLOutputText(
                "OUTPUT_SOURCE_SENTINEL", descriptor.output_function_oid
            )
        }
    }
    return record


@pytest.mark.parametrize("shape", ["none", "empty_dict", "non_dict"])
def test_policy_boundary_image_shapes_leave_no_typed_source_state(tmp_path, shape):
    box = Lab(tmp_path / f"image-shape-{shape}.duckdb")
    try:
        record = _unusual_image_record(box, shape)
        original_raw = record.raw
        if shape == "non_dict":
            with pytest.raises(AttributeError):
                box.applier._sanitize_record(record)
        else:
            assert box.applier._sanitize_record(record) is record
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


class _RaisingDescriptor:
    kind = "text"

    @property
    def output_function_oid(self):
        raise KeyboardInterrupt("descriptor output identity access failure")

    @property
    def metadata(self):
        return ()


def test_policy_boundary_raising_descriptor_leaves_no_typed_source_state(tmp_path):
    salt_path = tmp_path / "descriptor-salt"
    salt_path.write_bytes(b"unit-salt")
    salt_path.chmod(0o600)
    policy = PIIPolicy.from_manifest(
        [
            {
                "column_regex": r"^app\.pii_boundary\.secret$",
                "action": "hash",
                "algorithm": "HMAC-SHA-256",
                "salt_id": "test-v1",
            }
        ],
        unmatched="replicate",
        salt_file=salt_path,
    )
    box = Lab(tmp_path / "raising-descriptor.duckdb", pii_policy=policy)
    try:
        record = _unusual_image_record(box, "empty_dict")
        descriptor = _RaisingDescriptor()
        record.after = {"secret": "IMAGE_SOURCE_SENTINEL"}
        record.after_descriptors = {"secret": descriptor}
        original_raw = record.raw
        with pytest.raises(KeyboardInterrupt, match="descriptor output identity"):
            box.applier._sanitize_record(record)
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


def test_policy_boundary_activation_failure_strips_payload_and_reraises(
    tmp_path, monkeypatch
):
    box = Lab(tmp_path / "activation-failure.duckdb")
    try:
        record = _boundary_failure_record(box)
        original_raw = record.raw

        class InjectedAdmission(AdmissionError):
            pass

        def fail_before_boundary():
            raise InjectedAdmission("injected activation failure")

        monkeypatch.setattr(
            box.applier, "_activate_delete_policy_at_boundary", fail_before_boundary
        )
        with pytest.raises(InjectedAdmission, match="injected activation failure"):
            box.applier._sanitize_record(record)
        _assert_boundary_payload_is_stripped(record, original_raw)
    finally:
        box.close()


def test_policy_boundary_ordinary_refusal_still_returns_sealed_record(tmp_path):
    box = Lab(tmp_path / "ordinary-refusal.duckdb", pii_policy=_boundary_refusal_policy())
    try:
        record = _boundary_failure_record(box)
        original_raw = record.raw

        returned = box.applier._sanitize_record(record)

        assert returned is record
        assert isinstance(record.admission_refusal, SchemaEvolutionRefused)
        assert record.sanitized is True
        _assert_boundary_payload_is_stripped(record, original_raw, schemas=False)
    finally:
        box.close()
