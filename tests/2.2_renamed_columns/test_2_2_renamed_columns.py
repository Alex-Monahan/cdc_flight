"""Rubric 2.2: a PostgreSQL attnum-preserving rename is a true rename."""

from __future__ import annotations

import duckdb

from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.schema_evolution import (
    COLUMN_ADDED,
    COLUMN_DROPPED,
    COLUMN_RENAMED,
    ColumnChange,
    SourceColumn,
    apply_column_changes,
    diff_columns,
)


def col(attnum: int, name: str, type_oid: int = 25, type_name: str = "text"):
    return SourceColumn(attnum, name, type_oid, type_name, True)


def test_same_attnum_and_type_is_logical_continuity_not_drop_plus_add():
    changes = diff_columns(
        (col(1, "id", 20, "bigint"), col(2, "name")),
        (col(1, "id", 20, "bigint"), col(2, "full_name")),
    )

    assert len(changes) == 1
    assert changes[0].kind == COLUMN_RENAMED
    assert (changes[0].old_name, changes[0].new_name) == ("name", "full_name")


def test_rename_combined_with_add_and_drop_keeps_each_attnum_identity():
    changes = diff_columns(
        (
            col(1, "id", 20, "bigint"),
            col(2, "name"),
            col(3, "obsolete"),
        ),
        (
            col(1, "id", 20, "bigint"),
            col(2, "full_name"),
            col(4, "profile_note"),
        ),
    )

    assert [(change.kind, change.attnum) for change in changes] == [
        (COLUMN_RENAMED, 2),
        (COLUMN_DROPPED, 3),
        (COLUMN_ADDED, 4),
    ]


def test_rename_preserves_data_and_destination_identity(tmp_path):
    con = duckdb.connect(str(tmp_path / "rename.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute('INSERT INTO "cdc_raw"."customers" VALUES (1, \'Ada\')')

        apply_column_changes(
            registry,
            "customers",
            [
                ColumnChange(
                    kind=COLUMN_RENAMED,
                    attnum=2,
                    old_name="name",
                    new_name="full_name",
                    type_oid=25,
                    type_name="text",
                )
            ],
        )

        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("full_name",)]
        assert con.execute(
            'SELECT id, full_name FROM "cdc_raw"."customers"'
        ).fetchall() == [(1, "Ada")]
    finally:
        con.close()


def test_late_rename_is_idempotent_when_the_new_name_already_arrived(tmp_path):
    con = duckdb.connect(str(tmp_path / "late-rename.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute('INSERT INTO "cdc_raw"."customers" VALUES (1, \'Ada\')')
        # A new-name row can be decoded before the catalog polling thread observes the
        # rename.  The schema action must merge it, not leave duplicate logical columns.
        con.execute('ALTER TABLE "cdc_raw"."customers" ADD COLUMN full_name VARCHAR')
        con.execute('UPDATE "cdc_raw"."customers" SET full_name = name')

        apply_column_changes(
            registry,
            "customers",
            [ColumnChange(COLUMN_RENAMED, 2, "name", "full_name", 25, "text", True)],
        )

        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("full_name",)]
        assert con.execute('SELECT full_name FROM "cdc_raw"."customers"').fetchall() == [
            ("Ada",)
        ]
    finally:
        con.close()
