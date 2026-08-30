"""Schema-default acquisition and persistence stay behind the PII gate."""

from __future__ import annotations

import hashlib
import hmac

import duckdb
import pytest

from cdc_flight import destination
from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.catalog import CatalogChange, SourceRelation
from cdc_flight.catalog_apply import CatalogAction, CatalogCoordinator, CatalogPlan
from cdc_flight.catalog_support import CATALOG_SQL, missing_value_from_output
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.policy import PIIPolicy, PolicyGate, PostgreSQLOutputText
from cdc_flight.schema_evolution import COLUMN_ADDED, ColumnChange, SourceColumn
from cdc_flight.source_relations import upsert_source_relation
from cdc_flight.typed_types import SourceTypeDescriptor

SECRET_OUTPUT = "RAW_DEFAULT_SENTINEL"
SECRET_DESCRIPTOR = SourceTypeDescriptor(
    25,
    "pg_catalog.text",
    "text",
    output_function_oid=1009,
)


def _policy(action: str, salt_path) -> PIIPolicy:
    rule = {
        "column_regex": r"^app\.p8_defaults\.secret$",
        "action": action,
    }
    if action == "mask":
        rule["replacement"] = "[MASKED]"
    elif action == "truncate":
        rule["max_chars"] = 8
    elif action == "hash":
        rule.update({"algorithm": "HMAC-SHA-256", "salt_id": "default-v1"})
    return PIIPolicy.from_manifest(
        [rule], unmatched="replicate", salt_file=salt_path
    )


def _relation() -> SourceRelation:
    return SourceRelation(
        schema="app",
        table="p8_defaults",
        oid=8001,
        published=True,
        replica_identity="d",
        columns=(
            SourceColumn(
                attnum=1,
                name="id",
                type_oid=23,
                type_name="integer",
                descriptor=SourceTypeDescriptor(
                    23, "pg_catalog.int4", "int4", output_function_oid=42
                ),
            ),
            SourceColumn(
                attnum=2,
                name="secret",
                type_oid=25,
                type_name="text",
                has_missing_default=True,
                missing_value=PostgreSQLOutputText(
                    SECRET_OUTPUT, SECRET_DESCRIPTOR.output_function_oid
                ),
                descriptor=SECRET_DESCRIPTOR,
            ),
        ),
    )


class _EmptySource:
    def read_columns(self, relation, key_columns, value_columns):
        return []


@pytest.mark.parametrize("action", ["exclude", "mask", "hash", "truncate"])
def test_add_default_backfill_and_control_row_are_policy_sanitized(
    tmp_path, action
):
    """Every schema-default control either removes or transforms the source value."""
    salt_path = tmp_path / "salt"
    salt_path.write_bytes(b"schema-default-private-salt")
    salt_path.chmod(0o600)
    policy = _policy(action, salt_path)
    relation = _relation()
    change = ColumnChange(
        kind=COLUMN_ADDED,
        attnum=2,
        new_name="secret",
        type_oid=25,
        type_name="text",
        new_descriptor=SECRET_DESCRIPTOR,
    )
    con = duckdb.connect(str(tmp_path / f"{action}.duckdb"))
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "p8_defaults",
            columns={"id": "INTEGER", "secret": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute('INSERT INTO "cdc_raw"."p8_defaults" (id) VALUES (1)')
        coordinator = CatalogCoordinator(
            catalog=_EmptySource(),
            pipeline="p8-defaults",
            topic_prefix="cdcflight",
            drop_mode="replicate",
            registry_of=lambda: registry,
            policy_gate=PolicyGate(policy),
        )
        planned_changes = coordinator._policy_changes(
            relation.qualified, (change,)
        )
        if action == "exclude":
            assert planned_changes == ()
        else:
            assert len(planned_changes) == 1
            assert planned_changes[0].type_name == "character varying"
            assert planned_changes[0].new_descriptor.kind == "varchar"
        coordinator.backfill_schema(
            con,
            CatalogPlan(
                actions=(
                    CatalogAction(
                        change=CatalogChange(
                            kind="schema_changed",
                            schema="app",
                            table="p8_defaults",
                            detected_lsn=100,
                            new_relation=relation,
                            column_changes=(change,),
                        ),
                        target="p8_defaults",
                        destructive=False,
                    ),
                ),
            ),
        )
        actual = con.execute(
            'SELECT secret FROM "cdc_raw"."p8_defaults"'
        ).fetchall()
        if action == "exclude":
            expected = [(None,)]
        elif action == "mask":
            expected = [("[MASKED]",)]
        elif action == "truncate":
            expected = [(SECRET_OUTPUT[:8],)]
        elif action == "hash":
            expected = [(
                hmac.new(
                    b"schema-default-private-salt",
                    SECRET_OUTPUT.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest(),
            )]
        else:  # pragma: no cover - parametrization is exhaustive
            raise AssertionError(action)
        assert actual == expected

        upsert_source_relation(
            con,
            pipeline="p8-defaults",
            source_schema="app",
            source_table="p8_defaults",
            relation_oid=relation.oid,
            published=True,
            replica_identity="d",
            columns=relation.columns,
        )
        control_json = con.execute(
            "SELECT columns_json FROM _cdc_flight.source_relations "
            "WHERE pipeline = 'p8-defaults'"
        ).fetchone()[0]
        assert SECRET_OUTPUT not in control_json
        assert "missing_value_text" not in control_json
    finally:
        con.close()


@pytest.mark.parametrize(
    ("wire", "kind", "expected"),
    [
        ("{RAW_DEFAULT_SENTINEL}", "text", SECRET_OUTPUT),
        ("{$12.34}", "money", "$12.34"),
        ("{<a>value</a>}", "xml", "<a>value</a>"),
        ("{{1,2}}", "array", "{1,2}"),
        (r'{"[1,10)"}', "range", "[1,10)"),
        (r'{"(1,note)"}', "composite", "(1,note)"),
        (r'{"{\"a\": 1}"}', "jsonb", '{"a": 1}'),
        ("{192.0.2.1}", "inet", "192.0.2.1"),
        ("{(1,1),(0,0)}", "box", "(1,1),(0,0)"),
    ],
)
def test_missing_default_parser_preserves_type_output_text(wire, kind, expected):
    descriptor = SourceTypeDescriptor(
        9000,
        f"app.{kind}",
        kind,
        output_function_oid=9010,
    )
    value = missing_value_from_output(
        wire, descriptor, delimiter=";" if kind == "box" else ","
    )
    assert isinstance(value, PostgreSQLOutputText)
    assert str(value) == expected
    assert value.output_function_oid == 9010


def test_missing_default_without_output_identity_is_refused():
    descriptor = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    with pytest.raises(SchemaEvolutionRefused, match="OUTPUT identity"):
        missing_value_from_output("{42}", descriptor)


def test_catalog_default_query_uses_output_function_envelope_not_text_cast():
    assert "a.attmissingval::text" not in CATALOG_SQL
    assert "'missing_value_output'" in CATALOG_SQL
    assert "'type_delimiter'" in CATALOG_SQL
    assert "format(chr(37) || 's', a.attmissingval)" in CATALOG_SQL
