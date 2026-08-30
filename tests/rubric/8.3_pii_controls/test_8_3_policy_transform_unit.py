"""Rubric §8.3 transforms, provenance, and fail-closed output handling."""

from __future__ import annotations

import hashlib
import hmac

import pytest

from cdc_flight.policy import (
    PIIPolicy,
    PolicyGate,
    PolicyValueRefused,
    PostgreSQLOutputText,
)
from cdc_flight.typed_types import SourceTypeDescriptor


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
    gate = PolicyGate(policy)
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
    gate = PolicyGate(policy)
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
