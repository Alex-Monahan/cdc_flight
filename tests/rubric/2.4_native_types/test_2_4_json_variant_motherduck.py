"""Real MotherDuck JSON/VARIANT and recursive shadow-swap evidence."""

from __future__ import annotations

import pytest
from support.motherduck_probe import assert_runtime, connect, scratch_database

from cdc_flight.apply_sql import SchemaRegistry, insert_rows
from cdc_flight.config import motherduck_token
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_motherduck_json_variant_nested_round_trip_and_union_shadow():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    text = _source("text", 25)
    json_source = _source("json", 114)
    jsonb_source = _source("jsonb", 3802)
    jsonb_array = SourceTypeDescriptor(
        3803,
        "app.jsonb_array",
        "array",
        array_element=jsonb_source,
    )
    row = SourceTypeDescriptor(
        9000,
        "app.payload",
        "composite",
        composite_fields=(("kept", text), ("jsonb_value", jsonb_source)),
    )
    row_array = SourceTypeDescriptor(
        9001,
        "app.payload_array",
        "array",
        array_element=row,
    )
    attrs = SourceTypeDescriptor(
        9002,
        "public.hstore",
        "map",
        map_key=text,
        map_value=text,
    )

    with scratch_database(token, "cdc_p2b_json_variant") as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "native_values",
                columns={
                    "id": integer,
                    "json_value": json_source,
                    "jsonb_value": jsonb_source,
                    "jsonb_array": jsonb_array,
                    "payload": row,
                    "payloads": row_array,
                    "attrs": attrs,
                },
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("native_values"),
                ["id", "json_value", "jsonb_value", "jsonb_array", "payload", "payloads", "attrs"],
                [
                    [
                        1,
                        '{"b":[1,{"unicode":"é🙂"}],"a":1}',
                        '{"b":1,"a":2,"a":3}',
                        ['{"b":1,"a":2,"a":3}', "null", None],
                        {"kept": None, "jsonb_value": '{"b":1,"a":2,"a":3}', "dropped": "ignored"},
                        [{"kept": "first", "jsonb_value": '{"z":0}'}, {"kept": None, "jsonb_value": "null"}],
                        {"ü": "värde", "null_value": None},
                    ],
                    [2, '{"empty":true}', '{"empty":true}', [], None, [], {}],
                    [3, '{"sql_null":true}', None, None, None, None, None],
                ],
            )

            types = dict(
                con.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='typed' AND table_name='native_values'"
                ).fetchall()
            )
            assert types["json_value"] == "JSON"
            assert types["jsonb_value"] == "VARIANT"
            assert types["jsonb_array"] == "VARIANT[]"
            assert types["payload"].startswith("STRUCT(")
            assert "jsonb_value VARIANT" in types["payload"]
            assert types["payloads"].endswith("[]")
            assert types["attrs"] == "MAP(VARCHAR, VARCHAR)"

            rows = con.execute(
                "SELECT id, json_value, jsonb_value, variant_typeof(jsonb_value), "
                "jsonb_array, payload, payloads, attrs "
                "FROM typed.native_values ORDER BY id"
            ).fetchall()
            assert rows[0][1] == '{"b":[1,{"unicode":"é🙂"}],"a":1}'
            assert rows[0][2] == {"a": 3, "b": 1}
            assert rows[0][3] == "OBJECT(a, b)"
            assert rows[0][4][1] is None
            assert rows[0][5]["kept"] is None
            assert rows[0][5]["jsonb_value"] == {"a": 3, "b": 1}
            assert "dropped" not in rows[0][5]
            assert rows[0][6][1]["jsonb_value"] is None
            assert rows[0][7] == {"ü": "värde", "null_value": None}
            assert rows[1][4] == [] and rows[1][6] == [] and rows[1][7] == {}
            assert rows[2][2] is None and rows[2][4] is None and rows[2][5] is None

            con.execute(
                "CREATE TABLE typed.jsonb_nulls (id INTEGER, value VARIANT)"
            )
            con.execute(
                "INSERT INTO typed.jsonb_nulls VALUES "
                "(1, CAST(JSON 'null' AS VARIANT)), (2, NULL)"
            )
            assert con.execute(
                "SELECT variant_typeof(value), value IS NULL "
                "FROM typed.jsonb_nulls ORDER BY id"
            ).fetchall() == [("VARIANT_NULL", True), ("VARIANT_NULL", True)]

            registry.ensure_typed(
                "changes",
                columns={"id": integer, "value": json_source},
                key_columns=("id",),
            )
            insert_rows(con, registry.get("changes"), ["id", "value"], [[1, '{"old":1}']])
            registry.convert_column_to_union("changes", "value", json_source, jsonb_source)
            insert_rows(
                con,
                registry.get("changes"),
                ["id", "value"],
                [[2, '{"new":2,"a":1}'], [3, None]],
            )
            physical = con.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema='typed' AND table_name='changes' AND column_name='value'"
            ).fetchone()[0]
            assert "JSON" in physical and "VARIANT" in physical
            changed = con.execute(
                "SELECT union_tag(value), value FROM typed.changes ORDER BY id"
            ).fetchall()
            assert changed[0][1] == '{"old":1}'
            assert changed[1][1] == {"a": 1, "new": 2}
            assert changed[2][1] is None
        finally:
            con.close()
