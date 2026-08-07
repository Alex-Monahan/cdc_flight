"""Rubric 2.2: a PostgreSQL attnum-preserving rename is a true rename."""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import data

from cdc_flight import destination
from cdc_flight.applier import Applier
from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.assembler import UNIT_TXN, CompleteUnit
from cdc_flight.catalog import CHANGE_SCHEMA, CatalogChange, CatalogWatcher, SourceRelation
from cdc_flight.errors import SchemaEvolutionRefused, SchemaShapeUnexplained
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


def test_rename_then_drop_of_the_old_destination_name_preserves_the_rename(tmp_path):
    """BLOCKER reproduction: a same-group rename/drop is identity ordered."""

    con = duckdb.connect(str(tmp_path / "rename-name-reuse.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "first_name": "VARCHAR", "last_name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw"."customers" VALUES (1, \'Ada\', \'Lovelace\')'
        )

        changes = diff_columns(
            (col(1, "id", 20, "bigint"), col(2, "first_name"), col(3, "last_name")),
            (col(1, "id", 20, "bigint"), col(2, "last_name")),
        )
        apply_column_changes(registry, "customers", changes)

        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("last_name",)]
        assert con.execute(
            'SELECT id, last_name FROM "cdc_raw"."customers"'
        ).fetchall() == [(1, "Ada")]
    finally:
        con.close()


def test_late_rename_preserves_an_explicit_null_new_name(tmp_path):
    """The presence journal distinguishes explicit NULL from an absent field."""

    con = duckdb.connect(str(tmp_path / "rename-null.duckdb"))
    try:
        destination.ensure_control_schema(con)
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={
                "id": "BIGINT",
                "name": "VARCHAR",
                "full_name": "VARCHAR",
                "cdcf_event_id": "VARCHAR",
            },
            key_columns=("id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw"."customers" VALUES (1, \'Ada\', NULL, \'e1\')'
        )
        destination.write_column_presence(
            con,
            target_dataset="cdc_raw",
            target_table="customers",
            event_id="e1",
            column_name="full_name",
        )

        apply_column_changes(
            registry,
            "customers",
            [ColumnChange(COLUMN_RENAMED, 2, "name", "full_name", 25, "text", True)],
        )

        assert con.execute(
            'SELECT full_name FROM "cdc_raw"."customers"'
        ).fetchall() == [(None,)]
    finally:
        con.close()


def test_late_rename_rebinds_a_primary_key_to_the_new_source_identity(tmp_path):
    """A PK rename changes destination identity, not just the display name."""
    con = duckdb.connect(str(tmp_path / "rename-primary-key.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute('INSERT INTO "cdc_raw".customers VALUES (1, \'Ada\')')
        con.execute('ALTER TABLE "cdc_raw".customers ADD COLUMN customer_id BIGINT')
        con.execute('UPDATE "cdc_raw".customers SET customer_id = id')

        registry.rename_column("customers", "id", "customer_id")

        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("name",), ("customer_id",)]
        assert con.execute(
            "SELECT k.column_name FROM information_schema.key_column_usage k "
            "JOIN information_schema.table_constraints t "
            "  ON t.constraint_schema = k.constraint_schema "
            " AND t.constraint_name = k.constraint_name "
            " AND t.table_name = k.table_name "
            "WHERE k.table_schema = 'cdc_raw' AND k.table_name = 'customers' "
            "  AND t.constraint_type = 'PRIMARY KEY'"
        ).fetchall() == [("customer_id",)]
        assert con.execute(
            'SELECT customer_id, name FROM "cdc_raw".customers'
        ).fetchall() == [(1, "Ada")]
    finally:
        con.close()


def test_late_primary_key_rename_refuses_a_non_unique_new_identity(tmp_path):
    """A PK rebind that cannot preserve uniqueness rolls back as a refusal."""
    con = duckdb.connect(str(tmp_path / "rename-primary-key-refusal.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw".customers VALUES (1, \'Ada\'), (2, \'Grace\')'
        )
        con.execute('ALTER TABLE "cdc_raw".customers ADD COLUMN customer_id BIGINT')
        con.execute('UPDATE "cdc_raw".customers SET customer_id = 10')
        con.execute("BEGIN")
        with pytest.raises(SchemaEvolutionRefused, match="not unique"):
            registry.rename_column("customers", "id", "customer_id")
        con.execute("ROLLBACK")
        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("name",), ("customer_id",)]
    finally:
        con.close()


def test_hidden_intermediate_column_shape_is_refused_instead_of_folded(tmp_path):
    """A->B->C between polls cannot be safely represented by an A->C diff."""
    old_columns = (col(1, "id", 20, "bigint"), col(2, "name"))
    new_columns = (col(1, "id", 20, "bigint"), col(2, "full_name"))
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        include={"app.customers"},
        poll_seconds=0,
        known={"app.customers": SourceRelation("app", "customers", 1, True, "d", old_columns)},
    )
    watcher.queue(
        CatalogChange(
            kind=CHANGE_SCHEMA,
            schema="app",
            table="customers",
            detected_lsn=100,
            new_relation=SourceRelation("app", "customers", 1, True, "d", new_columns),
            column_changes=(
                ColumnChange(COLUMN_RENAMED, 2, "name", "full_name", 25, "text", True),
            ),
        )
    )
    unit = SimpleNamespace(
        events=[
            data(
                "hidden-history",
                1,
                101,
                key={"id": 1},
                after={"id": 1, "nickname": "Ada"},
            )
        ]
    )
    with pytest.raises(SchemaShapeUnexplained, match="intermediate DDL history"):
        watcher.observe_unit(unit)


def test_hidden_shape_refusal_at_applier_callback_is_durable(tmp_path, monkeypatch):
    """The callback boundary must leave a rebuild obligation, not only an exception."""
    from applier_lab import Lab

    old_columns = (col(1, "id", 20, "bigint"), col(2, "name"))
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        include={"app.customers"},
        poll_seconds=0,
        known={
            "app.customers": SourceRelation(
                "app", "customers", 1, True, "d", old_columns
            )
        },
    )
    watcher.queue(
        CatalogChange(
            kind=CHANGE_SCHEMA,
            schema="app",
            table="customers",
            detected_lsn=100,
            new_relation=SourceRelation(
                "app", "customers", 1, True, "d", old_columns
            ),
            column_changes=(
                ColumnChange(COLUMN_RENAMED, 2, "name", "full_name", 25, "text", True),
            ),
        )
    )
    box = Lab(tmp_path / "hidden-shape-applier.duckdb", catalog=watcher)
    try:
        event = data(
            "hidden-history",
            1,
            101,
            key={"id": 1},
            after={"id": 1, "nickname": "Ada"},
        )
        unit = CompleteUnit(
            kind=UNIT_TXN,
            events=[event],
            records=[event],
            txn_id="hidden-history",
            last_lsn=101,
            commit_lsn=101,
        )
        monkeypatch.setattr(box.applier.assembler, "feed", lambda _record: [unit])
        monkeypatch.setattr(
            "cdc_flight.applier.decode",
            lambda _raw, topic_prefix, want_offsets=False: event,
        )
        monkeypatch.setattr(
            "cdc_flight.applier.decode_notification",
            lambda _raw, topic_prefix: None,
        )

        with pytest.raises(SchemaShapeUnexplained, match="intermediate DDL history"):
            box.applier.handle_batch([object()], box.committer)

        pending = destination.pending_schema_refusals(box.con, "lab")
        assert len(pending) == 1
        assert pending[0][0:2] == ("app", "customers")
        assert "intermediate DDL history" in pending[0][2]
        assert box.scalar(
            "SELECT snapshot_state FROM _cdc_flight.table_state "
            "WHERE pipeline = 'lab' AND source_schema = 'app' "
            "AND source_table = 'customers'"
        ) == "awaiting_snapshot"
    finally:
        box.close()


def test_one_source_unit_with_pre_and_post_schema_shapes_is_refused():
    """A whole transaction cannot be split around DDL after the fact."""
    action = SimpleNamespace(
        change=CatalogChange(
            kind=CHANGE_SCHEMA,
            schema="app",
            table="customers",
            detected_lsn=100,
            column_changes=(
                ColumnChange(COLUMN_RENAMED, 2, "name", "full_name", 25, "text", True),
            ),
        )
    )
    events = [
        data("mixed-old", 1, 101, key={"id": 1}, after={"id": 1, "name": "Ada"}),
        data(
            "mixed-new",
            1,
            102,
            key={"id": 2},
            after={"id": 2, "full_name": "Grace"},
        ),
    ]
    with pytest.raises(SchemaEvolutionRefused, match="both sides of a schema fence"):
        Applier._refuse_mixed_schema_epoch(events, [action])


def test_rename_with_unsupported_type_change_is_refused_before_baseline_persists(tmp_path):
    con = duckdb.connect(str(tmp_path / "rename-type-refusal.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute('INSERT INTO "cdc_raw".customers VALUES (1, \'Ada\')')
        con.execute("BEGIN")
        with pytest.raises(SchemaEvolutionRefused, match="safe widening lattice"):
            apply_column_changes(
                registry,
                "customers",
                [
                    ColumnChange(
                        COLUMN_RENAMED,
                        2,
                        "name",
                        "full_name",
                        20,
                        "bigint",
                        True,
                        True,
                    )
                ],
            )
        con.execute("ROLLBACK")
        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("name",)]
    finally:
        con.close()
