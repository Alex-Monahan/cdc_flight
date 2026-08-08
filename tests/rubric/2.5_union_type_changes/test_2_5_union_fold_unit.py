"""Default-lane tests for the single typed UNION conversion path."""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.typed_types import SourceTypeDescriptor, union_member_name, union_type


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid=oid, qualified_name=f"pg_catalog.{kind}", kind=kind)


def test_member_names_are_fingerprinted_and_reused():
    integer = _source("int4", 23)
    text = _source("text", 25)
    assert union_member_name(integer) == union_member_name(integer)
    assert union_member_name(integer) != union_member_name(text)
    target = union_type((integer, text))
    assert target.sql.startswith("UNION(")
    assert target.sql.count(",") == 1


def test_shadow_conversion_preserves_old_values_and_tags():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    text = _source("text", 25)
    registry.ensure_typed(
        "items",
        columns={"id": integer, "value": integer},
        key_columns=("id",),
    )
    con.execute('INSERT INTO d."items" VALUES (1, 7)')

    registry.convert_column_to_union("items", "value", integer, text)
    con.execute(
        'INSERT INTO d."items" ("id", "value") VALUES (2, union_value(m_%s := \'new\'))'
        % union_member_name(text)
    )

    rows = con.execute('SELECT id, value, union_tag(value) FROM d."items" ORDER BY id').fetchall()
    assert rows == [(1, 7, union_member_name(integer)), (2, "new", union_member_name(text))]
    physical = con.execute(
        "SELECT data_type FROM information_schema.columns WHERE table_schema='d' AND table_name='items' AND column_name='value'"
    ).fetchone()[0]
    assert physical.startswith("UNION(")
    assert physical.count(",") == 1


def test_union_key_uses_internal_identity_instead_of_a_failed_primary_key():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    registry.ensure_typed(
        "keyed",
        columns={"key": _source("int4", 23), "value": _source("text", 25)},
        key_columns=("key",),
    )
    assert registry.get("keyed").primary_key_columns == ("key",)
    registry.convert_column_to_union("keyed", "key", _source("int4", 23), _source("text", 25))
    table = registry.get("keyed")
    assert table.primary_key_columns == ("cdcf_internal_id",)
    assert "cdcf_internal_id" in table.columns


@pytest.mark.parametrize("members", [
    (_source("int4", 23), _source("text", 25)),
    (_source("int4", 23), _source("int8", 20), _source("text", 25)),
])
def test_union_sql_is_deterministic_for_replay(members):
    assert union_type(members).sql == union_type(tuple(reversed(tuple(reversed(members))))).sql

