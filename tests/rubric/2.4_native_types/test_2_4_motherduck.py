"""Real MotherDuck nested-type and numeric-special evidence for rubric 2.4."""

from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest
from support.motherduck_probe import assert_runtime, connect

from cdc_flight.apply_sql import SchemaRegistry, insert_rows
from cdc_flight.naming import quote
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_motherduck_accepts_native_nested_types_and_specials(motherduck_case):
    token = motherduck_case["token"]
    typed = motherduck_case["control_schema"]
    quoted_typed = quote(typed)

    integer = _source("int4", 23)
    text = _source("text", 25)
    numeric = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=12, scale=4
    )
    array = SourceTypeDescriptor(1007, "pg_catalog._int4", "array", array_element=integer)
    nested_array = SourceTypeDescriptor(
        9001, "app._int4_array", "array", array_element=array
    )
    payload = SourceTypeDescriptor(
        9000,
        "app.payload",
        "composite",
        composite_fields=(("amount", numeric), ("values", array)),
    )
    attrs = SourceTypeDescriptor(
        9002, "public.hstore", "map", map_key=text, map_value=text
    )
    enum = SourceTypeDescriptor(
        9003, "app.mood", "enum", enum_labels=("calm", "alert")
    )

    with contextlib.nullcontext(motherduck_case["database"]) as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute(f"CREATE SCHEMA {quoted_typed}")
            registry = SchemaRegistry(con, typed)
            registry.ensure_typed(
                "native_values",
                columns={
                    "id": integer,
                    "payload": payload,
                    "attrs": attrs,
                    "nested": nested_array,
                    "mood": enum,
                    "double_value": _source("float8", 701),
                },
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("native_values"),
                ["id", "payload", "attrs", "nested", "mood", "double_value"],
                [[
                    1,
                    {"amount": "12.3400", "values": [1, 2]},
                    {"site": "hq", "tier": None},
                    [[1, 2], []],
                    "calm",
                    float("inf"),
                ]],
            )

            types = dict(
                con.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = 'native_values'",
                    [typed],
                ).fetchall()
            )
            assert types["payload"].startswith("STRUCT(")
            assert "UNION(" in types["payload"]
            assert types["attrs"].startswith("MAP(")
            assert types["nested"].startswith("INTEGER[][]")
            assert types["mood"].startswith("ENUM(")
            assert types["double_value"] == "DOUBLE"
            row = con.execute(
                f"SELECT payload.amount, payload.values, attrs['site'], "
                f"nested, mood, isinf(double_value) FROM {quoted_typed}.native_values"
            ).fetchone()
            assert row[0] == Decimal("12.3400")
            assert row[1] == [1, 2]
            assert row[2] == "hq"
            assert row[3] == [[1, 2], []]
            assert row[4:] == ("calm", True)
            assert con.execute(
                f"SELECT union_tag(payload.amount) FROM {quoted_typed}.native_values"
            ).fetchone() == ("finite",)
        finally:
            con.close()
