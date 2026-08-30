"""All four policy controls at a real MotherDuck destination."""

from __future__ import annotations

import contextlib
import hashlib
import hmac
from pathlib import Path

import pytest
from support.applier_lab import Lab, data, end
from support.motherduck_probe import connect

from cdc_flight.policy import PIIPolicy, PostgreSQLOutputText
from cdc_flight.typed_types import SourceTypeDescriptor


pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def test_all_controls_and_money_xml_contract_on_motherduck(motherduck_case, tmp_path):
    salt = tmp_path / "md-policy-salt"
    salt.write_bytes(b"md-policy-private-salt")
    salt.chmod(0o600)
    policy = PIIPolicy.from_manifest(
        [
            {"column_regex": r"^app\.md_pii\.id$", "action": "replicate"},
            {"column_regex": r"^app\.md_pii\.email$", "action": "hash", "algorithm": "HMAC-SHA-256", "salt_id": "md-v1"},
            {"column_regex": r"^app\.md_pii\.phone$", "action": "mask", "replacement": "[PHONE]"},
            {"column_regex": r"^app\.md_pii\.notes$", "action": "truncate", "max_chars": 4},
            {"column_regex": r"^app\.md_pii\.money$", "action": "exclude"},
            {"column_regex": r"^app\.md_pii\.xml_value$", "action": "mask", "replacement": "[XML]"},
        ],
        unmatched="exclude",
        salt_file=salt,
        epoch=8,
    )
    con = connect(motherduck_case["token"], motherduck_case["database"])
    box = Lab(
        Path("motherduck-pii-lab.duckdb"),
        connection=con,
        dataset=motherduck_case["dataset"],
        control_schema=motherduck_case["control_schema"],
        pipeline="p8_md_pii",
        namespace="p8-md-pii",
        pii_policy=policy,
    )
    try:
        text = SourceTypeDescriptor(25, "pg_catalog.text", "text", output_function_oid=25)
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4", output_function_oid=43)
        money = SourceTypeDescriptor(790, "pg_catalog.money", "money", output_function_oid=790)
        xml = SourceTypeDescriptor(142, "pg_catalog.xml", "xml", output_function_oid=142)
        event = data(
            "pii", 1, 100, table="md_pii", key={"id": 1},
            after={
                "id": 1,
                "email": "md-email-original",
                "phone": "md-phone-original",
                "notes": "md-notes-original",
                "money": "$12.34",
                "xml_value": "<original>xml</original>",
            },
        )
        event.after_descriptors = {
            "id": integer,
            "email": text,
            "phone": text,
            "notes": text,
            "money": money,
            "xml_value": xml,
        }
        event.output_texts = {
            "after": {
                "email": PostgreSQLOutputText("md-email-output", 25),
                "notes": PostgreSQLOutputText("md-notes-output", 25),
            }
        }
        box.run([event, end("pii", 1, 110, {"app.md_pii": 1})])
        target = box.target("md_pii")
        qualified = f'"{motherduck_case["database"]}"."{motherduck_case["dataset"]}"."{target}"'
        assert con.execute(
            f"SELECT id, email, phone, notes, xml_value FROM {qualified}"
        ).fetchall() == [
            (
                1,
                hmac.new(b"md-policy-private-salt", b"md-email-output", hashlib.sha256).hexdigest(),
                "[PHONE]",
                "md-",
                "[XML]",
            )
        ]
        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [motherduck_case["dataset"], target],
            ).fetchall()
        }
        assert "money" not in columns
        durable = repr(
            con.execute(
                f'SELECT * FROM "{motherduck_case["control_schema"]}".policy_alerts'
            ).fetchall()
        )
        for secret in ("md-email-original", "md-phone-original", "md-notes-original", "md-policy-private-salt"):
            assert secret not in durable
    finally:
        with contextlib.suppress(Exception):
            con.execute(f'DROP SCHEMA IF EXISTS "{motherduck_case["dataset"]}" CASCADE')
        box.close()
