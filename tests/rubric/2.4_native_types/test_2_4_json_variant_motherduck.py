"""Real MotherDuck JSON/VARIANT and recursive shadow-swap evidence."""

from __future__ import annotations

import pytest
from support.motherduck_probe import assert_runtime, connect, scratch_database

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.config import DestinationConfig, motherduck_token
from cdc_flight.destination import connect as destination_connect
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_motherduck_accepts_json_as_a_jsonb_primary_key():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    jsonb = _source("jsonb", 3802)
    with scratch_database(token, "cdc_p2b_json_key") as database:
        con = connect(token, database)
        try:
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "jsonb_key",
                columns={"id": jsonb, "tenant": integer, "payload": jsonb},
                key_columns=("id", "tenant"),
            )
            table = registry.get("jsonb_key")
            assert table.raw_types["id"] == "JSON"
            assert table.raw_types["payload"] == "VARIANT"
            insert_rows(
                con,
                table,
                ["id", "tenant", "payload"],
                [['{"account": 7}', 1, '{"body": true}']],
            )
            assert con.execute(
                'SELECT "id", "payload" FROM typed."jsonb_key"'
            ).fetchone() == ('{"account":7}', {"body": True})
        finally:
            con.close()


def test_motherduck_probe_connections_share_the_production_configuration():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    with scratch_database(token, "cdc_p2b_probe_config") as database:
        probe = connect(token, database)
        try:
            production = destination_connect(
                DestinationConfig(kind="motherduck", motherduck_database=database)
            )
            production.close()
        finally:
            probe.close()


def test_motherduck_jsonb_key_gain_and_loss_use_the_same_shadow_resolver():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    jsonb = _source("jsonb", 3802)
    with scratch_database(token, "cdc_p2b_json_key_transition") as database:
        con = connect(token, database)
        try:
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "transitions",
                columns={"id": integer, "json_key": jsonb, "payload": jsonb},
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("transitions"),
                ["id", "json_key", "payload"],
                [[1, '{"a":1}', '{"body":true}']],
            )
            registry.ensure_typed(
                "transitions",
                columns={"id": integer, "json_key": jsonb, "payload": jsonb},
                key_columns=("id", "json_key"),
            )
            assert registry.get("transitions").raw_types["json_key"] == "JSON"
            registry.ensure_typed(
                "transitions",
                columns={"id": integer, "json_key": jsonb, "payload": jsonb},
                key_columns=("id",),
            )
            table = registry.get("transitions")
            assert table.raw_types["json_key"] == "VARIANT"
            assert table.primary_key_columns == ("id",)
            assert con.execute(
                'SELECT "payload" FROM typed."transitions"'
            ).fetchone() == ({"body": True},)
        finally:
            con.close()


def test_motherduck_nested_composite_jsonb_key_deletes_existing_row():
    """A recursive key descriptor must use JSON on the physical shadow table."""
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    text = _source("text", 25)
    jsonb = _source("jsonb", 3802)
    inner = SourceTypeDescriptor(
        9100,
        "app.inner_key",
        "composite",
        composite_fields=(("doc", jsonb), ("n", integer)),
    )
    outer = SourceTypeDescriptor(
        9101,
        "app.outer_key",
        "composite",
        composite_fields=(("inner_key", inner), ("note", text)),
    )

    with scratch_database(token, "cdc_p2b_nested_json_key") as database:
        con = connect(token, database)
        try:
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "nested_key",
                columns={"id": integer, "compound": outer, "payload": text},
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("nested_key"),
                ["id", "compound", "payload"],
                [[1, {"inner_key": {"doc": '{"a":1}', "n": 7}, "note": "x"}, "kept"]],
            )
            registry.ensure_typed(
                "nested_key",
                columns={"id": integer, "compound": outer, "payload": text},
                key_columns=("id", "compound"),
            )
            table = registry.get("nested_key")
            assert "doc JSON" in table.raw_types["compound"]
            delete_keys(
                con,
                table,
                ("id", "compound"),
                [(1, {"inner_key": {"doc": '{"a":1}', "n": 7}, "note": "x"})],
            )
            assert con.execute('SELECT count(*) FROM typed."nested_key"').fetchone()[0] == 0
        finally:
            con.close()


def test_motherduck_bytea_key_changed_to_union_deletes_existing_row():
    """The SQL and Python bytea identity renderings must be byte-for-byte equal."""
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    bytea = _source("bytea", 17)
    text = _source("text", 25)
    with scratch_database(token, "cdc_p2b_bytea_union_key") as database:
        con = connect(token, database)
        try:
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "bytea_union_key",
                columns={"id": bytea, "payload": text},
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("bytea_union_key"),
                ["id", "payload"],
                [[b"abc", "kept"]],
            )
            registry.convert_column_to_union("bytea_union_key", "id", bytea, text)
            table = registry.get("bytea_union_key")
            delete_keys(con, table, ("id",), [(b"abc",)])
            assert con.execute('SELECT count(*) FROM typed."bytea_union_key"').fetchone()[0] == 0
        finally:
            con.close()


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


def test_motherduck_json_variant_edge_states_and_multidimensional_lists():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

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

    with scratch_database(token, "cdc_p2b_json_variant_edges") as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute("CREATE SCHEMA typed")
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
