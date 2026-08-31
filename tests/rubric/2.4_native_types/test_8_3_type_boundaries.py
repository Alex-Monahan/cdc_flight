"""§8.3 policy controls do not turn PostgreSQL money/xml into blockers."""

from __future__ import annotations

import hashlib
import hmac

import pytest

from cdc_flight.policy import PIIPolicy, PolicyGate, PostgreSQLOutputText
from cdc_flight.typed_types import SourceTypeDescriptor, native_type

MONEY = SourceTypeDescriptor(
    790, "pg_catalog.money", "money", output_function_oid=790,
)
XML = SourceTypeDescriptor(
    142, "pg_catalog.xml", "xml", output_function_oid=142,
)


def _policy(tmp_path, action: str, column: str, **extra) -> PIIPolicy:
    salt = tmp_path / "money-xml-salt"
    salt.write_bytes(b"money-xml-only-salt")
    salt.chmod(0o600)
    rule = {
        "column_regex": rf"^app\.money_xml\.{column}$",
        "action": action,
        **extra,
    }
    if action == "hash":
        rule.update(algorithm="HMAC-SHA-256", salt_id="money-xml-v1")
    return PIIPolicy.from_manifest(
        [rule], unmatched="exclude", salt_file=salt if action == "hash" else None
    )


@pytest.mark.parametrize(
    ("action", "extra"),
    [
        ("exclude", {}),
        ("mask", {"replacement": "[M]"}),
        ("hash", {}),
        ("truncate", {"max_chars": 4}),
        ("replicate", {}),
    ],
)
@pytest.mark.parametrize("column, descriptor, output", [
    ("money", MONEY, "$12.34"),
    ("xml", XML, "<secret>value</secret>"),
])
def test_money_and_xml_controls_are_varchar_and_nonblocking(
    tmp_path, action, extra, column, descriptor, output
):
    assert native_type(descriptor).kind == "VARCHAR"
    policy = _policy(tmp_path, action, column, **extra)
    gate = PolicyGate(policy)
    values = {column: output}
    output_texts = (
        {column: PostgreSQLOutputText(output, descriptor.output_function_oid)}
        if action in {"hash", "truncate"}
        else None
    )
    sanitized = gate.sanitize_mapping(
        "app.money_xml",
        values,
        {column: descriptor},
        output_texts=output_texts,
    )
    if action == "exclude":
        assert sanitized == {}
    elif action == "mask":
        assert sanitized == {column: "[M]"}
    elif action == "hash":
        expected = hmac.new(
            b"money-xml-only-salt", output.encode(), hashlib.sha256
        ).hexdigest()
        assert sanitized == {column: expected}
    elif action == "truncate":
        assert sanitized == {column: output[:4]}
    else:
        assert sanitized == values


def test_unproven_money_and_xml_transform_inputs_are_omitted_not_table_refusals(
    tmp_path,
):
    for column, descriptor in (("money", MONEY), ("xml", XML)):
        policy = _policy(tmp_path, "hash", column)
        result = policy.sanitize_mapping(
            "app.money_xml", {column: "connector-text"}, {column: descriptor}
        )
        if column == "money":
            assert result == {}
        else:
            # xml's catalog typoutput identity plus its stock text transport is
            # the explicit text-like proof accepted by the gate.
            assert set(result) == {column}
