"""Rubric 2.1: source column add/drop must keep the destination coherent.

The unit cases are deliberately written before the implementation.  They pin the
contract that a catalog observation is a column identity change, not merely another
row-shape inference: a dropped source column is dropped from the current-state
destination in the same commit as its audit marker.
"""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight import destination, resnapshot, resnapshot_projection, table_lifecycle
from cdc_flight.apply_sql import SchemaRegistry
from cdc_flight.catalog import CatalogChange, SourceRelation
from cdc_flight.catalog_apply import CatalogAction, CatalogCoordinator, CatalogPlan
from cdc_flight.errors import SchemaBackfillRefused, SchemaEvolutionRefused
from cdc_flight.schema_evolution import (
    COLUMN_ADDED,
    COLUMN_DROPPED,
    ColumnChange,
    SourceColumn,
    apply_column_changes,
    diff_columns,
    dlt_table_columns,
)
from cdc_flight.snapshot import SnapshotCoordinator


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


def test_dropping_a_primary_key_without_a_replacement_is_a_durable_refusal(tmp_path):
    con = duckdb.connect(str(tmp_path / "drop-primary-key-refusal.duckdb"))
    try:
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "customers",
            columns={"id": "BIGINT", "name": "VARCHAR"},
            key_columns=("id",),
        )
        con.execute("BEGIN")
        with pytest.raises(SchemaEvolutionRefused, match="replacement identity"):
            apply_column_changes(
                registry,
                "customers",
                [ColumnChange(COLUMN_DROPPED, 1, old_name="id")],
            )
        con.execute("ROLLBACK")
        assert con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'customers' "
            "ORDER BY ordinal_position"
        ).fetchall() == [("id",), ("name",)]
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
        with pytest.raises(SchemaBackfillRefused, match="no stable row identity"):
            registry.backfill_constant_columns(
                "readings", value_columns=("tier",), rows=[("bronze",), ("silver",)]
            )
    finally:
        con.close()


def test_keyless_default_is_proven_from_catalog_metadata_when_source_is_empty(tmp_path):
    con = duckdb.connect(str(tmp_path / "keyless-default-empty.duckdb"))
    try:
        destination.ensure_control_schema(con)
        con.execute("CREATE SCHEMA cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        registry.ensure(
            "readings",
            columns={"tier": "VARCHAR", "cdcf_event_id": "VARCHAR"},
            key_columns=("cdcf_event_id",),
        )
        con.execute(
            'INSERT INTO "cdc_raw".readings VALUES (NULL, \'e1\'), (NULL, \'e2\')'
        )

        class EmptySource:
            def read_columns(self, relation, key_columns, value_columns):
                return []

        relation = SourceRelation(
            "app",
            "readings",
            1,
            True,
            "n",
            (
                SourceColumn(
                    1,
                    "tier",
                    25,
                    "text",
                    True,
                    True,
                    "bronze",
                ),
            ),
        )
        coordinator = CatalogCoordinator(
            catalog=EmptySource(),
            pipeline="p",
            topic_prefix="cdcflight",
            drop_mode="replicate",
            registry_of=lambda: registry,
        )
        change = CatalogChange(
            kind="schema_changed",
            schema="app",
            table="readings",
            detected_lsn=100,
            new_relation=relation,
            column_changes=(
                # The physical column already exists in this focused backfill test.
                ColumnChange(
                    COLUMN_ADDED, 1, new_name="tier", type_oid=25, type_name="text"
                ),
            ),
        )
        coordinator.backfill_schema(
            con,
            CatalogPlan(
                actions=(CatalogAction(change, "readings", False),),
            ),
        )
        assert con.execute(
            'SELECT tier FROM "cdc_raw".readings ORDER BY cdcf_event_id'
        ).fetchall() == [("bronze",), ("bronze",)]
    finally:
        con.close()


def test_nonuniform_keyless_backfill_records_a_durable_refusal_and_is_idempotent(tmp_path):
    con = duckdb.connect(str(tmp_path / "keyless-refusal.duckdb"))
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        destination.register_table(
            con,
            pipeline="p",
            source_schema="app",
            source_table="readings",
            target_table="cdcflight_app_readings",
        )
        reason = "source values are not uniform and no stable row identity exists"
        destination.record_schema_refusal(
            con,
            pipeline="p",
            source_schema="app",
            source_table="readings",
            target_table="cdcflight_app_readings",
            detected_lsn=100,
            reason=reason,
        )
        destination.record_schema_refusal(
            con,
            pipeline="p",
            source_schema="app",
            source_table="readings",
            target_table="cdcflight_app_readings",
            detected_lsn=100,
            reason=reason,
        )
        assert destination.pending_schema_refusals(con, "p") == [
            ("app", "readings", reason)
        ]
        assert con.execute(
            "SELECT snapshot_state FROM _cdc_flight.table_state "
            "WHERE pipeline = 'p' AND source_table = 'readings'"
        ).fetchone()[0] == "awaiting_snapshot"
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.table_events "
            "WHERE pipeline = 'p' AND event = 'schema_refusal'"
        ).fetchone()[0] == 1
        con.execute("BEGIN")
        table_lifecycle.transition(
            con,
            pipeline="p",
            source_schema="app",
            source_table="readings",
            to=table_lifecycle.COMPLETE,
            reason="the replacement snapshot completed",
            snapshot_lsn=101,
        )
        assert destination.resolve_schema_refusal(
            con, pipeline="p", source_schema="app", source_table="readings"
        ) is True
        con.execute("COMMIT")
        assert destination.pending_schema_refusals(con, "p") == []
        assert con.execute(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE pipeline = 'p' AND source_table = 'readings'"
        ).fetchone()[0] == "resolved"
    finally:
        con.close()


def test_snapshot_swap_audit_is_in_the_same_transaction_as_the_image(tmp_path):
    """A rollback after swap must also roll back the discovery completion audit."""
    con = duckdb.connect(str(tmp_path / "snapshot-audit-atomic.duckdb"))
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        registry = SchemaRegistry(con, "cdc_raw")
        destination.write_resume_point(
            con,
            pipeline="p",
            namespace="main",
            point=destination.ResumePoint(snapshot_epoch=2),
            commit_id=0,
            offset_blob=None,
            offset_key_blob=None,
        )
        audit_calls = []

        def audit(state, snapshot_lsn, commit_id):
            audit_calls.append((state.schema, state.table, snapshot_lsn, commit_id))
            resnapshot._record_snapshot_swap_audit(
                con,
                pipeline="p",
                state=state,
                snapshot_lsn=snapshot_lsn,
                commit_id=commit_id,
                reason="same transaction",
                new_relations={"app.arrival"},
                namespace="main",
                snapshot_epoch=7,
            )

        coordinator = SnapshotCoordinator(
            con,
            dataset="cdc_raw",
            pipeline="p",
            topic_prefix="cdcflight",
            created_in_txn=lambda: set(),
            get_registry=lambda: registry,
            epoch=0,
            transactional_ddl=True,
            on_swap=audit,
        )
        con.execute("BEGIN")
        state = coordinator.state_for("app", "arrival")
        con.execute(
            'CREATE TABLE "cdc_raw"."cdcflight_app_arrival__cdcf_tmp" (id BIGINT)'
        )
        coordinator.swap(state, commit_id=7, snapshot_lsn=123)
        assert audit_calls == [("app", "arrival", 123, 7)]
        con.execute("ROLLBACK")

        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.snapshot_audits"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.table_state "
            "WHERE pipeline = 'p' AND source_table = 'arrival'"
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT snapshot_epoch FROM _cdc_flight.debezium_offsets "
            "WHERE pipeline = 'p' AND namespace = 'main'"
        ).fetchone()[0] == 2
    finally:
        con.close()


def test_all_resnapshot_completion_paths_use_one_projection_component(monkeypatch, tmp_path):
    con = duckdb.connect(str(tmp_path / "shared-projection.duckdb"))
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        calls = []
        real_project = resnapshot_projection.project_snapshot_completion

        def recording_project(*args, **kwargs):
            calls.append(kwargs["events"])
            return real_project(*args, **kwargs)

        monkeypatch.setattr(
            resnapshot_projection,
            "project_snapshot_completion",
            recording_project,
        )

        state = type(
            "SnapshotState",
            (),
            {"schema": "app", "table": "swapped", "target": "cdcflight_app_swapped"},
        )()
        con.execute("BEGIN")
        resnapshot._record_snapshot_swap_audit(
            con,
            pipeline="p",
            state=state,
            snapshot_lsn=101,
            commit_id=7,
            reason="callback",
            new_relations={"app.swapped"},
        )
        con.execute("COMMIT")

        destination.request_snapshot(
            con,
            pipeline="p",
            tables=[("app", "empty", "cdcflight_app_empty")],
            detail="empty projection",
        )
        assert resnapshot.finish_verified_empty_tables(
            con,
            pipeline="p",
            dataset="cdc_raw",
            tables=[("app", "empty", "cdcflight_app_empty")],
            done=set(),
            evidence=resnapshot.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={"app.empty": 0},
                wal_lsn=103,
            ),
            new_relations={"app.empty"},
        ) == ["app.empty"]

        assert len(calls) == 2
    finally:
        con.close()


def test_verified_empty_discovery_emits_canonical_new_table_event(tmp_path):
    con = duckdb.connect(str(tmp_path / "empty-discovery-audit.duckdb"))
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        tables = [("app", "empty", "cdcflight_app_empty")]
        destination.request_snapshot(
            con, pipeline="p", tables=tables, detail="new empty table"
        )

        assert resnapshot.finish_verified_empty_tables(
            con,
            pipeline="p",
            dataset="cdc_raw",
            tables=tables,
            done=set(),
            evidence=resnapshot.EmptinessEvidence(
                snapshot_phase_ended=True,
                tables_seen=set(),
                source_empty_at={"app.empty": 0},
                wal_lsn=123,
            ),
            new_relations={"app.empty"},
        ) == ["app.empty"]

        assert con.execute(
            "SELECT event FROM _cdc_flight.table_events "
            "WHERE pipeline = 'p' AND source_table = 'empty' ORDER BY seq"
        ).fetchall() == [("resnapshot_empty",), ("new",)]
    finally:
        con.close()
