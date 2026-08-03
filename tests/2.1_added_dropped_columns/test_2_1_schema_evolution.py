"""Rubric 2.1: source column add/drop must keep the destination coherent.

The unit cases are deliberately written before the implementation.  They pin the
contract that a catalog observation is a column identity change, not merely another
row-shape inference: a dropped source column is dropped from the current-state
destination in the same commit as its audit marker.
"""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.schema_evolution import (
    COLUMN_ADDED,
    COLUMN_DROPPED,
    ColumnChange,
    SourceColumn,
    apply_column_changes,
    diff_columns,
    dlt_table_columns,
)


def column(attnum: int, name: str, type_oid: int = 25, type_name: str = "text"):
    return SourceColumn(
        attnum=attnum,
        name=name,
        type_oid=type_oid,
        type_name=type_name,
        nullable=True,
    )


def test_added_and_dropped_columns_are_explicit_identity_changes():
    changes = diff_columns(
        (column(1, "id", 20, "bigint"), column(2, "old_name")),
        (column(1, "id", 20, "bigint"), column(3, "new_name")),
    )

    assert [(change.kind, change.attnum) for change in changes] == [
        (COLUMN_DROPPED, 2),
        (COLUMN_ADDED, 3),
    ]
    assert changes[0].old_name == "old_name"
    assert changes[1].new_name == "new_name"


def test_the_dlt_schema_model_is_the_normalisation_boundary():
    model = dlt_table_columns(
        (column(1, "id", 20, "bigint"), column(2, "payload", 3802, "jsonb"))
    )

    assert model == {
        "id": {"name": "id", "nullable": True, "data_type": "bigint"},
        "payload": {"name": "payload", "nullable": True, "data_type": "json"},
    }


def test_dropped_column_is_removed_from_current_state_without_losing_other_rows(tmp_path):
    con = duckdb.connect(str(tmp_path / "schema.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "old_name": "VARCHAR", "kept": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw"."customers" VALUES (1, \'before\', \'yes\')'
        )

        apply_column_changes(
            registry,
            "customers",
            [
                ColumnChange(
                    kind=COLUMN_DROPPED,
                    attnum=2,
                    old_name="old_name",
                    type_oid=25,
                    type_name="text",
                )
            ],
        )

        columns = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall()
        assert [row[0] for row in columns] == ["id", "kept"]
        assert con.execute('SELECT id, kept FROM "cdc_raw"."customers"').fetchall() == [
            (1, "yes")
        ]
    finally:
        con.close()


def test_added_column_backfill_agrees_with_current_source_values(tmp_path):
    con = duckdb.connect(str(tmp_path / "add-backfill.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw"."customers" VALUES (1, \'Ada\'), (2, \'Grace\')'
        )
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR", "tier": "VARCHAR"},
            key_columns=("id",),
        )

        registry.backfill_columns(
            "customers",
            key_columns=("id",),
            value_columns=("tier",),
            rows=[(1, "gold"), (2, "bronze")],
        )

        assert con.execute(
            'SELECT id, tier FROM "cdc_raw"."customers" ORDER BY id'
        ).fetchall() == [(1, "gold"), (2, "bronze")]
    finally:
        con.close()


def test_keyless_added_column_uses_uniform_source_value_without_inventing_identity(tmp_path):
    con = duckdb.connect(str(tmp_path / "keyless-backfill.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "readings",
            columns={"value": "VARCHAR", "tier": "VARCHAR", "cdcf_event_id": "VARCHAR"},
            key_columns=("cdcf_event_id",),
        )
        con.execute(
            "INSERT INTO \"cdc_raw\".readings VALUES "
            "('same', NULL, 'e1'), ('same', NULL, 'e2')"
        )

        registry.backfill_constant_columns(
            "readings", value_columns=("tier",), rows=[("bronze",), ("bronze",)]
        )

        assert con.execute(
            'SELECT tier FROM "cdc_raw".readings ORDER BY cdcf_event_id'
        ).fetchall() == [("bronze",), ("bronze",)]
    finally:
        con.close()


def test_keyless_non_uniform_added_values_are_refused(tmp_path):
    con = duckdb.connect(str(tmp_path / "keyless-nonuniform.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "readings",
            columns={"tier": "VARCHAR", "cdcf_event_id": "VARCHAR"},
            key_columns=("cdcf_event_id",),
        )
        with pytest.raises(ValueError, match="no stable row identity"):
            registry.backfill_constant_columns(
                "readings", value_columns=("tier",), rows=[("bronze",), ("silver",)]
            )
    finally:
        con.close()
