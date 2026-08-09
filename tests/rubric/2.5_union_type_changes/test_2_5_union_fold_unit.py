"""Default-lane tests for the single typed UNION conversion path."""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight import faults
from cdc_flight.apply_sql import (
    SchemaRegistry,
    _union_members,
    delete_keys,
    insert_rows,
    update_rows,
)
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.row_patch import RowPatch
from cdc_flight.table_work import TableWork, _key_value, collect, end_transaction, write
from cdc_flight.typed_types import (
    FieldValue,
    SourceTypeDescriptor,
    union_member_name,
    union_type,
)


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
        f'INSERT INTO d."items" ("id", "value") VALUES '
        f'(2, union_value({union_member_name(text)} := \'new\'))'
    )
    insert_rows(con, registry.get("items"), ["id", "value"], [[3, None]])

    rows = con.execute('SELECT id, value, union_tag(value) FROM d."items" ORDER BY id').fetchall()
    assert rows[0] == (1, 7, union_member_name(integer))
    assert rows[1] == (2, "new", union_member_name(text))
    assert rows[2][0] == 3 and rows[2][1] is None and rows[2][2] == union_member_name(text)
    physical = con.execute(
        "SELECT data_type FROM information_schema.columns WHERE table_schema='d' AND table_name='items' AND column_name='value'"
    ).fetchone()[0]
    assert physical.startswith("UNION(")
    assert len(_union_members(physical)) == 2


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


def test_type_changed_source_key_delete_addresses_the_old_union_member():
    """A post-DDL delete must remove a row carrying the pre-DDL key tag."""

    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    widened = _source("int8", 20)
    text = _source("text", 25)
    registry.ensure_typed(
        "changed_keys",
        columns={"id": integer, "value": text},
        key_columns=("id",),
    )
    insert_rows(con, registry.get("changed_keys"), ["id", "value"], [[1, "old"]])
    registry.convert_column_to_union("changed_keys", "id", integer, widened)

    table = registry.get("changed_keys")
    delete_keys(con, table, ("id",), [(_key_value(table, "id", 1),)])
    assert con.execute('SELECT count(*) FROM d."changed_keys"').fetchone() == (0,)


def test_type_changed_key_move_uses_old_identity_then_writes_new_member():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    widened = _source("int8", 20)
    text = _source("text", 25)
    registry.ensure_typed(
        "moved_keys",
        columns={"id": integer, "value": text},
        key_columns=("id",),
    )
    insert_rows(con, registry.get("moved_keys"), ["id", "value"], [[1, "old"]])
    registry.convert_column_to_union("moved_keys", "id", integer, widened)

    item = TableWork(
        target="moved_keys",
        key_columns=("id",),
        identified=True,
        source_schema="app",
        source_table="moved_keys",
    )
    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="app.moved_keys",
        nbytes=1,
        op="u",
        schema="app",
        table="moved_keys",
        key={"id": 2},
        before={"id": 1, "value": "old"},
        after={"id": 2, "value": "moved"},
        before_descriptors={"id": integer, "value": text},
        after_descriptors={"id": widened, "value": text},
    )
    patch = RowPatch(
        {
            "id": FieldValue.of(2, widened),
            "value": FieldValue.of("moved", text),
        },
        complete=True,
    )

    class Probe:
        def start_exists(self, _item, _key):
            return True

        def start_matches(self, _item, _key, _image):
            return True

    collect(item, event, patch.encoded_values(), "move", probe=Probe(), patch=patch)
    end_transaction(item)
    write(con, registry, item, set())
    assert con.execute(
        'SELECT id, value, union_tag(id) FROM d."moved_keys"'
    ).fetchone() == (2, "moved", union_member_name(widened))


def test_internal_identity_and_old_key_descriptor_survive_registry_restart():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    integer = _source("int4", 23)
    widened = _source("int8", 20)
    text = _source("text", 25)
    first = SchemaRegistry(con, "d")
    first.ensure_typed(
        "restart_keys",
        columns={"id": integer, "value": text},
        key_columns=("id",),
    )
    insert_rows(con, first.get("restart_keys"), ["id", "value"], [[1, "old"]])
    first.convert_column_to_union("restart_keys", "id", integer, widened)

    restarted = SchemaRegistry(con, "d")
    restarted.ensure_typed(
        "restart_keys",
        columns={"id": widened, "value": text},
        key_columns=("id",),
    )
    table = restarted.get("restart_keys")
    assert table.source_key_columns == ("id",)
    # The shadow copy rewrote the row to the one current canonical identity;
    # no historical descriptor ledger is needed after a restart.
    assert not hasattr(table, "identity_descriptors")
    assert update_rows(
        con, table, ("id",), [((1,), {"id": 2, "value": "moved"})]
    ) == 1
    assert con.execute(
        'SELECT id, value, union_tag(id) FROM d."restart_keys"'
    ).fetchone() == (2, "moved", union_member_name(widened))


def test_typed_union_shadow_swap_uses_the_declared_fault_anchor(monkeypatch):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    text = _source("text", 25)
    registry.ensure_typed("faulted", columns={"id": integer, "value": integer}, key_columns=("id",))
    insert_rows(con, registry.get("faulted"), ["id", "value"], [[1, 7]])
    monkeypatch.setenv("CDC_FAULT_INJECT", "swap:1:raise")
    faults.refresh()
    try:
        con.execute("BEGIN")
        with pytest.raises(faults.InjectedFault):
            registry.convert_column_to_union("faulted", "value", integer, text)
        con.rollback()
    finally:
        monkeypatch.delenv("CDC_FAULT_INJECT", raising=False)
        faults.refresh()
    assert con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema='d' AND table_name='faulted' AND column_name='value'"
    ).fetchone() == ("INTEGER",)


def test_repeated_changes_append_once_and_typed_rows_use_the_current_member():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    text = _source("text", 25)
    boolean = _source("bool", 16)
    registry.ensure_typed(
        "history",
        columns={"id": integer, "value": integer},
        key_columns=("id",),
    )
    con.execute('INSERT INTO d."history" VALUES (1, 7)')
    registry.convert_column_to_union("history", "value", integer, text)
    registry.convert_column_to_union("history", "value", integer, text)
    from cdc_flight.apply_sql import insert_rows

    insert_rows(con, registry.get("history"), ["id", "value"], [[2, "new"]])
    registry.convert_column_to_union("history", "value", text, boolean)
    table = registry.get("history")
    assert table.raw_types["value"].count(",") == 2

    insert_rows(con, table, ["id", "value"], [[3, True]])
    rows = con.execute(
        'SELECT id, value, union_tag(value) FROM d."history" ORDER BY id'
    ).fetchall()
    assert rows == [
        (1, 7, union_member_name(integer)),
        (2, "new", union_member_name(text)),
        (3, True, union_member_name(boolean)),
    ]


def test_numeric_inner_union_becomes_one_outer_history_union_and_rehydrates():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    numeric8 = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=8, scale=4
    )
    numeric18 = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=18, scale=4
    )
    registry.ensure_typed(
        "numeric_history",
        columns={"id": integer, "value": numeric8},
        key_columns=("id",),
    )
    insert_rows(
        con,
        registry.get("numeric_history"),
        ["id", "value"],
        [[1, "1.2300"], [2, "NaN"], [3, None]],
    )
    registry.convert_column_to_union("numeric_history", "value", numeric8, numeric18)
    insert_rows(
        con,
        registry.get("numeric_history"),
        ["id", "value"],
        [[4, "2.3400"], [5, None]],
    )

    physical = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'd' AND table_name = 'numeric_history' "
        "AND column_name = 'value'"
    ).fetchone()[0]
    assert physical.startswith("UNION(")
    assert len(_union_members(physical)) == 2
    assert union_member_name(numeric8) in physical
    assert union_member_name(numeric18) in physical
    tags = con.execute(
        "SELECT id, union_tag(value) FROM d.numeric_history ORDER BY id"
    ).fetchall()
    assert tags == [
        (1, union_member_name(numeric8)),
        (2, union_member_name(numeric8)),
        (3, union_member_name(numeric8)),
        (4, union_member_name(numeric18)),
        (5, union_member_name(numeric18)),
    ]
    assert con.execute(
        f"SELECT isnan(union_extract(value, '{union_member_name(numeric8)}').special) "
        "FROM d.numeric_history WHERE id = 2"
    ).fetchone() == (True,)

    # Re-read the physical declaration through a fresh cache.  No type-history
    # ledger is allowed to be necessary for the next post-restart write.
    registry.forget("numeric_history")
    registry.ensure_typed(
        "numeric_history", columns={"value": numeric18}, key_columns=("id",)
    )
    insert_rows(
        con,
        registry.get("numeric_history"),
        ["id", "value"],
        [[6, "3.4500"]],
    )
    assert con.execute(
        "SELECT union_tag(value) FROM d.numeric_history WHERE id = 6"
    ).fetchone() == (union_member_name(numeric18),)


def test_tagged_nulls_and_non_indexable_key_predicates_are_native():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    text = _source("text", 25)
    registry.ensure_typed(
        "union_keys",
        columns={"key": integer, "value": text},
        key_columns=("key",),
    )
    insert_rows(con, registry.get("union_keys"), ["key", "value"], [[1, "before"]])
    registry.convert_column_to_union("union_keys", "key", integer, text)
    table = registry.get("union_keys")
    insert_rows(con, table, ["key", "value"], [["2", None]])
    delete_keys(con, table, ("key",), [(_key_value(table, "key", "2"),)])
    assert con.execute(
        "SELECT union_tag(key), value FROM d.union_keys"
    ).fetchall() == [(union_member_name(integer), "before")]


def test_added_typed_column_is_physically_added_to_an_existing_table():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = _source("int4", 23)
    text = _source("text", 25)
    registry.ensure_typed("rows", columns={"id": integer}, key_columns=("id",))
    con.execute('INSERT INTO d."rows" VALUES (1)')
    registry.ensure_typed("rows", columns={"note": text}, key_columns=("id",))
    assert con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema='d' AND table_name='rows' AND column_name='note'"
    ).fetchone()[0] == "VARCHAR"
    con.execute('INSERT INTO d."rows" ("id", "note") VALUES (2, \'ok\')')
    assert con.execute('SELECT id, note FROM d."rows" ORDER BY id').fetchall() == [
        (1, None),
        (2, "ok"),
    ]


@pytest.mark.parametrize("members", [
    (_source("int4", 23), _source("text", 25)),
    (_source("int4", 23), _source("int8", 20), _source("text", 25)),
])
def test_union_sql_is_deterministic_for_replay(members):
    assert union_type(members).sql == union_type(tuple(reversed(tuple(reversed(members))))).sql
