"""Local DuckDB JSON/VARIANT mapping and recursive bind evidence."""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight.apply_sql import SchemaRegistry, insert_rows
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect as connect_destination
from cdc_flight.typed_types import (
    FieldValue,
    InvalidTypedValue,
    JsonbNull,
    SourceTypeDescriptor,
    TypedImage,
    encode_value,
    native_type,
)

_LOCAL_CONFIG = {"storage_compatibility_version": "v1.5.0"}


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def _native_table(con):
    integer = _source("int4", 23)
    text = _source("text", 25)
    json_source = _source("json", 114)
    jsonb_source = _source("jsonb", 3802)
    row = SourceTypeDescriptor(
        9000,
        "app.payload",
        "composite",
        composite_fields=(
            ("kept", text),
            ("jsonb_value", jsonb_source),
        ),
    )
    jsonb_array = SourceTypeDescriptor(
        3803,
        "app.jsonb_array",
        "array",
        array_element=jsonb_source,
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
    return registry, {
        "json": json_source,
        "jsonb": jsonb_source,
        "jsonb_array": jsonb_array,
        "row": row,
        "row_array": row_array,
        "attrs": attrs,
    }


def test_local_json_jsonb_recursive_round_trip_and_physical_types():
    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry, _sources = _native_table(con)
        large = "é🙂" * 2_000
        json_text = '{"b":[1,{"unicode":"é🙂"}],"a":1}'
        jsonb_text = '{"b":1,"a":2,"a":3}'
        insert_rows(
            con,
            registry.get("native_values"),
            ["id", "json_value", "jsonb_value", "jsonb_array", "payload", "payloads", "attrs"],
            [
                [
                    1,
                    json_text,
                    jsonb_text,
                    [jsonb_text, "null", None],
                    {"kept": None, "jsonb_value": jsonb_text, "dropped": "ignored"},
                    [
                        {"kept": "first", "jsonb_value": '{"z":0}'},
                        {"kept": None, "jsonb_value": "null"},
                    ],
                    {"ü": large, "null_value": None},
                ]
            ],
        )

        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'typed' AND table_name = 'native_values'"
            ).fetchall()
        )
        assert types["json_value"] == "JSON"
        assert types["jsonb_value"] == "VARIANT"
        assert types["jsonb_array"] == "VARIANT[]"
        assert types["payload"].startswith("STRUCT(")
        assert "jsonb_value VARIANT" in types["payload"]
        assert types["payloads"].startswith("STRUCT(") and types["payloads"].endswith("[]")
        assert types["attrs"] == "MAP(VARCHAR, VARCHAR)"

        row = con.execute(
            "SELECT json_value, jsonb_value, variant_typeof(jsonb_value), "
            "jsonb_array, payload, payloads, attrs, jsonb_value IS NULL "
            "FROM typed.native_values"
        ).fetchone()
        assert row[0] == json_text
        assert row[1] == {"a": 3, "b": 1}
        assert row[2] == "OBJECT(a, b)"
        assert row[3][1] is None
        assert row[4]["kept"] is None
        assert row[4]["jsonb_value"] == {"a": 3, "b": 1}
        assert "dropped" not in row[4]
        assert row[5][1]["jsonb_value"] is None
        assert row[6]["ü"] == large
        assert row[6]["null_value"] is None
        assert row[7] is False
    finally:
        con.close()


def test_local_json_jsonb_edge_states_and_multidimensional_lists():
    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        integer = _source("int4", 23)
        text = _source("text", 25)
        json_source = _source("json", 114)
        jsonb_source = _source("jsonb", 3802)
        jsonb_array = SourceTypeDescriptor(
            3803, "app.jsonb_array", "array", array_element=jsonb_source
        )
        jsonb_matrix = SourceTypeDescriptor(
            3804, "app.jsonb_matrix", "array", array_element=jsonb_array
        )
        attrs = SourceTypeDescriptor(
            9002, "public.hstore", "map", map_key=text, map_value=text
        )
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "edge_values",
            columns={
                "id": integer,
                "json_value": json_source,
                "jsonb_value": jsonb_source,
                "jsonb_array": jsonb_array,
                "jsonb_matrix": jsonb_matrix,
                "attrs": attrs,
            },
            key_columns=("id",),
        )
        large = "é🙂" * 2_000
        deep = '{"level1":{"level2":{"level3":[{"level4":"深"}]}}}'
        large_json = '{"payload":"' + large + '"}'
        large_variant = '{"payload":"' + large + '","deep":' + deep + '}'
        insert_rows(
            con,
            registry.get("edge_values"),
            ["id", "json_value", "jsonb_value", "jsonb_array", "jsonb_matrix", "attrs"],
            [
                [1, large_json, large_variant, [deep, "null", None], [[deep], []], {"ü": large, "null": None}],
                [2, '[{"x":1},null,[]]', "{}", [], [], {}],
                [3, "{}", "[]", None, None, {}],
                [4, "[]", "null", None, None, None],
                [5, None, None, None, None, None],
            ],
        )

        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema='typed' AND table_name='edge_values'"
            ).fetchall()
        )
        assert types["json_value"] == "JSON"
        assert types["jsonb_value"] == "VARIANT"
        assert types["jsonb_array"] == "VARIANT[]"
        assert types["jsonb_matrix"] == "VARIANT[][]"
        rows = con.execute(
            "SELECT id, json_value, jsonb_value, variant_typeof(jsonb_value), "
            "jsonb_array, jsonb_matrix, attrs FROM typed.edge_values ORDER BY id"
        ).fetchall()
        assert rows[0][1] == large_json
        assert len(rows[0][2]["payload"]) == len(large)
        assert rows[0][2]["deep"]["level1"]["level2"]["level3"][0]["level4"] == "深"
        assert rows[0][3] == "OBJECT(deep, payload)"
        assert rows[0][4][1] is None and rows[0][5][1] == []
        assert rows[0][6]["ü"] == large and rows[0][6]["null"] is None
        assert rows[1][1] == '[{"x":1},null,[]]' and rows[1][2] == {}
        assert rows[1][4] == [] and rows[1][5] == [] and rows[1][6] == {}
        assert rows[2][1] == "{}" and rows[2][2] == [] and rows[2][4] is None
        assert rows[3][1] == "[]" and rows[3][2] is None and rows[3][6] is None
        assert rows[4][1] is None and rows[4][2] is None
    finally:
        con.close()


def test_local_destination_connection_creates_persistent_variant_storage(tmp_path):
    con = connect_destination(
        DestinationConfig(kind="duckdb", duckdb_path=tmp_path / "native.duckdb")
    )
    try:
        con.execute("CREATE TABLE values_table (value VARIANT)")
        con.execute(
            "INSERT INTO values_table VALUES "
            "(CAST(JSON '{\"ok\":true}' AS VARIANT))"
        )
        assert con.execute("SELECT typeof(value), value FROM values_table").fetchone() == (
            "VARIANT",
            {"ok": True},
        )
    finally:
        con.close()


def test_jsonb_null_spill_and_backfill_keep_the_typed_boundary():
    jsonb_source = _source("jsonb", 3802)
    encoded = encode_value("null", jsonb_source)
    assert isinstance(encoded, JsonbNull)
    image = TypedImage((
        ("payload", FieldValue.of(encoded, jsonb_source)),
    ))
    restored = TypedImage.from_dict(image.to_dict())
    assert isinstance(restored.field("payload").value, JsonbNull)

    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        integer = _source("int4", 23)
        registry.ensure_typed("backfill", columns={"id": integer}, key_columns=("id",))
        insert_rows(con, registry.get("backfill"), ["id"], [[1]])
        registry.ensure_typed(
            "backfill", columns={"payload": jsonb_source}, key_columns=("id",)
        )
        registry.backfill_columns(
            "backfill",
            key_columns=("id",),
            value_columns=("payload",),
            rows=[(1, '{"backfilled":true}')],
        )
        assert con.execute(
            "SELECT typeof(payload), payload FROM typed.backfill"
        ).fetchone() == ("VARIANT", {"backfilled": True})
    finally:
        con.close()


def test_local_jsonb_key_order_is_structural_but_json_order_is_textual():
    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        assert con.execute(
            "SELECT CAST('{\"a\":1,\"b\":2}' AS JSON) = "
            "CAST('{\"b\":2,\"a\":1}' AS JSON)"
        ).fetchone() == (False,)
        assert con.execute(
            "SELECT CAST(CAST('{\"a\":1,\"b\":2}' AS JSON) AS VARIANT) = "
            "CAST(CAST('{\"b\":2,\"a\":1}' AS JSON) AS VARIANT)"
        ).fetchone() == (True,)
    finally:
        con.close()


def test_local_variant_null_behavior_is_explicit_and_machine_checked():
    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        rows = con.execute(
            "SELECT variant_typeof(CAST(JSON 'null' AS VARIANT)), "
            "CAST(JSON 'null' AS VARIANT) IS NULL, "
            "variant_typeof(CAST(NULL AS VARIANT)), "
            "CAST(NULL AS VARIANT) IS NULL, "
            "CAST(JSON 'null' AS VARIANT) IS NOT DISTINCT FROM "
            "CAST(NULL AS VARIANT)"
        ).fetchone()
        assert rows == ("VARIANT_NULL", True, "VARIANT_NULL", True, True)
    finally:
        con.close()


@pytest.mark.parametrize(
    ("source", "value"),
    [
        (_source("json", 114), "{malformed"),
        (_source("jsonb", 3802), "{malformed"),
    ],
)
def test_malformed_json_payloads_are_refused_before_native_bind(source, value):
    with pytest.raises(InvalidTypedValue):
        encode_value(value, source)


def test_recursive_resolver_refuses_incomplete_nested_descriptors():
    with pytest.raises(Exception) as exc_info:
        native_type(SourceTypeDescriptor(9003, "app.bad_array", "array"))
    assert "element descriptor" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        native_type(SourceTypeDescriptor(9004, "app.bad_map", "map"))
    assert "key/value" in str(exc_info.value)


def test_local_json_jsonb_shadow_swap_keeps_both_native_union_members():
    con = duckdb.connect(":memory:", config=_LOCAL_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        integer = _source("int4", 23)
        json_source = _source("json", 114)
        jsonb_source = _source("jsonb", 3802)
        registry = SchemaRegistry(con, "typed")
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
        rows = con.execute(
            "SELECT id, union_tag(value), value FROM typed.changes ORDER BY id"
        ).fetchall()
        assert rows[0][1].startswith("m_")
        assert rows[0][2] == '{"old":1}'
        assert rows[1][2] == {"a": 1, "new": 2}
        assert rows[2][2] is None
    finally:
        con.close()
