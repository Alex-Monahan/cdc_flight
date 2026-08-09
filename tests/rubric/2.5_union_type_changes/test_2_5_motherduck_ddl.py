"""Real MotherDuck UNION/shadow/identity evidence for rubric 2.5."""

from __future__ import annotations

import pytest
from support.motherduck_probe import assert_runtime, connect, scratch_database

from cdc_flight import faults
from cdc_flight.apply_sql import SchemaRegistry, _union_members, delete_keys, insert_rows
from cdc_flight.config import motherduck_token
from cdc_flight.identity_codec import identity_value
from cdc_flight.typed_types import SourceTypeDescriptor, union_member_name

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_motherduck_union_history_reversion_and_key_identity():
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    text = _source("text", 25)
    boolean = _source("bool", 16)

    with scratch_database(token, "cdc_p2b_25") as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
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
                "WHERE table_schema='typed' AND table_name='history' AND column_name='value'"
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
                "SELECT id, value, union_tag(value) FROM typed.history ORDER BY id"
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
                "WHERE table_schema='typed' AND table_name='keyed' "
                "AND column_name='key'"
            ).fetchone()[0].startswith("UNION(")
        finally:
            con.close()


def test_motherduck_range_and_multirange_equality_classes_gain_keys():
    """MotherDuck preserves the source range identity through STRUCT readback."""
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    text = _source("text", 25)
    range_type = SourceTypeDescriptor(
        3904, "pg_catalog.int4range", "range", range_subtype=integer
    )
    multirange_type = SourceTypeDescriptor(
        4451,
        "pg_catalog.int4multirange",
        "multirange",
        range_subtype=range_type,
    )
    cases = (
        ("range_key", range_type, "[1,3]", "[1,4)"),
        (
            "multirange_key",
            multirange_type,
            ["[10,12)", "[1,3]", "[3,5)"],
            ["[1,5)", "[10,12)"],
        ),
    )
    with scratch_database(token, "cdc_p2b_range_identity") as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            for name, descriptor, source_value, equivalent_value in cases:
                table, _ = registry.ensure_typed(
                    name,
                    columns={"key": descriptor, "payload": text},
                    key_columns=("key",),
                )
                insert_rows(con, table, ["key", "payload"], [[source_value, "kept"]])
                readback = con.execute(
                    f'SELECT "key" FROM typed."{name}"'
                ).fetchone()[0]
                assert identity_value(table, (source_value,), key_columns=("key",)) == identity_value(
                    table, (readback,), key_columns=("key",)
                )
                delete_keys(con, table, ("key",), [(equivalent_value,)])
                assert con.execute(
                    f'SELECT count(*) FROM typed."{name}"'
                ).fetchone() == (0,)
        finally:
            con.close()


def test_motherduck_typed_swap_faults_restart_and_converges_across_repeated_cycles(
    monkeypatch,
):
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")

    integer = _source("int4", 23)
    text = _source("text", 25)
    boolean = _source("bool", 16)
    with scratch_database(token, "cdc_p2b_swap_faults") as database:
        con = connect(token, database)
        try:
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            registry.ensure_typed(
                "fault_cycles",
                columns={"id": integer, "value": integer, "other": integer},
                key_columns=("id",),
            )
            insert_rows(
                con,
                registry.get("fault_cycles"),
                ["id", "value", "other"],
                [[1, 7, 9]],
            )

            monkeypatch.setenv(faults.ENV_VAR, "swap:1:raise")
            faults.refresh()
            con.execute("BEGIN")
            with pytest.raises(faults.InjectedFault):
                registry.convert_column_to_union(
                    "fault_cycles", "value", integer, text
                )
            con.rollback()
            assert con.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema='typed' AND table_name='fault_cycles' "
                "AND column_name='value'"
            ).fetchone() == ("INTEGER",)

            monkeypatch.delenv(faults.ENV_VAR, raising=False)
            faults.refresh()
            registry.convert_column_to_union("fault_cycles", "value", integer, text)
            insert_rows(con, registry.get("fault_cycles"), ["id", "value", "other"], [[2, "new", 10]])

            # The first failed attempt and the first successful restart both
            # crossed the same process-local anchor.  The second cycle is the
            # third typed swap observed by this registry.
            monkeypatch.setenv(faults.ENV_VAR, "swap:3:raise")
            faults.refresh()
            con.execute("BEGIN")
            with pytest.raises(faults.InjectedFault):
                registry.convert_column_to_union(
                    "fault_cycles", "other", integer, boolean
                )
            con.rollback()
            assert con.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema='typed' AND table_name='fault_cycles' "
                "AND column_name='other'"
            ).fetchone() == ("INTEGER",)

            monkeypatch.delenv(faults.ENV_VAR, raising=False)
            faults.refresh()
            registry.convert_column_to_union("fault_cycles", "other", integer, boolean)
            insert_rows(
                con,
                registry.get("fault_cycles"),
                ["id", "value", "other"],
                [[3, "last", True]],
            )
            rows = con.execute(
                "SELECT id, value, union_tag(value), other, union_tag(other) "
                "FROM typed.fault_cycles ORDER BY id"
            ).fetchall()
            assert rows[0][0] == 1 and rows[0][1] == 7
            assert rows[1][1] == "new" and rows[2][1] == "last"
            assert rows[2][3] is True
            assert all(row[2] == union_member_name(text) for row in rows[1:])
            assert rows[0][4] == union_member_name(integer)
            assert rows[2][4] == union_member_name(boolean)
        finally:
            monkeypatch.delenv(faults.ENV_VAR, raising=False)
            faults.refresh()
            con.close()
