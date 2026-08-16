"""Real MotherDuck UNION/shadow/identity evidence for rubric 2.5."""

from __future__ import annotations

import contextlib

import pytest
from support.motherduck_probe import assert_runtime, connect

from cdc_flight.apply_sql import SchemaRegistry, _union_members, insert_rows
from cdc_flight.naming import quote
from cdc_flight.typed_types import SourceTypeDescriptor, union_member_name

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_motherduck_union_history_reversion_and_key_identity(motherduck_case):
    token = motherduck_case["token"]
    typed = motherduck_case["control_schema"]
    quoted_typed = quote(typed)

    integer = _source("int4", 23)
    text = _source("text", 25)
    boolean = _source("bool", 16)

    with contextlib.nullcontext(motherduck_case["database"]) as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute(f"CREATE SCHEMA {quoted_typed}")
            registry = SchemaRegistry(con, typed)
            registry.ensure_typed(
                "history",
                columns={"id": integer, "value": integer},
                key_columns=("id",),
            )
            insert_rows(con, registry.get("history"), ["id", "value"], [[1, 7], [2, None]])
            registry.convert_column_to_union("history", "value", integer, text)
            insert_rows(con, registry.get("history"), ["id", "value"], [[3, "new"]])
            registry.convert_column_to_union("history", "value", text, boolean)
            insert_rows(con, registry.get("history"), ["id", "value"], [[4, True]])
            registry.convert_column_to_union("history", "value", boolean, text)
            insert_rows(con, registry.get("history"), ["id", "value"], [[5, "again"]])

            physical = con.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema=? AND table_name='history' AND column_name='value'",
                [typed],
            ).fetchone()[0]
            members = _union_members(physical)
            assert physical.startswith("UNION(")
            assert len(members) == 3
            assert {name for name, _ in members} == {
                union_member_name(integer),
                union_member_name(text),
                union_member_name(boolean),
            }
            rows = con.execute(
                f"SELECT id, value, union_tag(value) FROM {quoted_typed}.history ORDER BY id"
            ).fetchall()
            assert [row[2] for row in rows] == [
                union_member_name(integer),
                union_member_name(integer),
                union_member_name(text),
                union_member_name(boolean),
                union_member_name(text),
            ]
            assert rows[1][1] is None

            # A UNION cannot be a primary key.  The registry chooses the internal
            # length-prefixed identity in the same shadow swap instead of catching a
            # failed CREATE and leaving the visible source key unconstrained.
            registry.ensure_typed(
                "keyed",
                columns={"key": integer, "value": text},
                key_columns=("key",),
            )
            registry.convert_column_to_union("keyed", "key", integer, text)
            table = registry.get("keyed")
            assert table.primary_key_columns == ("cdcf_internal_id",)
            assert table.source_key_columns == ("key",)
            assert con.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema=? AND table_name='keyed' "
                "AND column_name='key'",
                [typed],
            ).fetchone()[0].startswith("UNION(")
        finally:
            con.close()
