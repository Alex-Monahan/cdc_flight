"""Round-3 regressions for source-semantic identity and strict type authority."""

from __future__ import annotations

import json
from types import SimpleNamespace

import duckdb
import pytest

from cdc_flight import catalog as catalog_coordinator
from cdc_flight import destination
from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.catalog_poll import _column_descriptor
from cdc_flight.catalog_state import read_known_relations
from cdc_flight.typed_types import SourceTypeDescriptor, native_type


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def _jsonb_compound() -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        9300,
        "app.identity_compound",
        "composite",
        composite_fields=(
            ("doc", _source("jsonb", 3802)),
            ("note", _source("text", 25)),
        ),
    )


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ('{"doc":{"v":1},"note":"same"}', '{"doc":{"v":1.0},"note":"same"}'),
        ('{"doc":{"v":1e2},"note":"same"}', '{"doc":{"v":100},"note":"same"}'),
        ('{"doc":{"v":1.2300},"note":"same"}', '{"doc":{"v":1.23},"note":"same"}'),
        ('{"doc":{"v":-0.0},"note":"same"}', '{"doc":{"v":0},"note":"same"}'),
        ('{"doc":{"a":1,"a":2},"note":"same"}', '{"doc":{"a":2},"note":"same"}'),
        ('{"doc":{"b":[1,{"z":1,"a":2}]},"note":"same"}', '{"doc":{"b":[1.0,{"a":2,"z":1.0}]},"note":"same"}'),
    ],
)
def test_postgres_jsonb_equality_classes_delete_one_row(left, right):
    """The internal key must follow PostgreSQL JSONB equality, not Python numbers."""
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        compound = _jsonb_compound()
        registry.ensure_typed(
            "jsonb_identity",
            columns={"compound": compound, "payload": _source("text", 25)},
            key_columns=("compound",),
        )
        table = registry.get("jsonb_identity")
        assert table.internal_identity
        left_value = json.loads(left)
        right_value = json.loads(right)
        insert_rows(con, table, ["compound", "payload"], [[left_value, "kept"]])
        delete_keys(con, table, ("compound",), [(right_value,)])
        assert con.execute('SELECT count(*) FROM typed."jsonb_identity"').fetchone()[0] == 0
    finally:
        con.close()


def test_interval_is_a_valid_source_key_but_uses_internal_identity():
    interval = _source("interval", 1186)
    assert native_type(interval).indexable is False
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "interval_key",
            columns={"key": interval, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        table = registry.get("interval_key")
        assert table.internal_identity
        insert_rows(con, table, ["key", "payload"], [["P1Y2M3DT4H5M6S", "kept"]])
        delete_keys(con, table, ("key",), [("P1Y2M3DT4H5M6S",)])
        assert con.execute('SELECT count(*) FROM typed."interval_key"').fetchone()[0] == 0
    finally:
        con.close()


def test_interval_key_creation_migration_and_replay_keep_one_identity_path():
    interval = _source("interval", 1186)
    text = _source("text", 25)
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "interval_lifecycle",
            columns={"key": interval, "payload": text},
            key_columns=("key",),
        )
        table = registry.get("interval_lifecycle")
        value = "P1Y2M3DT4H5M6S"

        # Replay uses the same source-key identity for the delete and the next
        # insert; it must never need a destination INTERVAL index.
        insert_rows(con, table, ["key", "payload"], [[value, "first"]])
        delete_keys(con, table, ("key",), [(value,)])
        assert con.execute('SELECT count(*) FROM typed."interval_lifecycle"').fetchone()[0] == 0
        insert_rows(con, table, ["key", "payload"], [[value, "replayed"]])
        before = con.execute(
            'SELECT "cdcf_internal_id" FROM typed."interval_lifecycle"'
        ).fetchone()[0]

        # A current-version type change must carry the existing ID across the
        # shadow copy instead of deriving it from a readback INTERVAL/UNION value.
        registry.convert_column_to_union("interval_lifecycle", "key", interval, text)
        after = con.execute(
            'SELECT "cdcf_internal_id" FROM typed."interval_lifecycle"'
        ).fetchone()[0]
        assert after == before
        delete_keys(con, registry.get("interval_lifecycle"), ("key",), [(value,)])
        assert con.execute('SELECT count(*) FROM typed."interval_lifecycle"').fetchone()[0] == 0
    finally:
        con.close()


def test_live_catalog_descriptor_miss_fails_closed_instead_of_guessing():
    with pytest.raises(Exception, match="descriptor authority"):
        _column_descriptor(
            {"type_oid": 999999, "type_name": "jsonb", "nullable": True}, {}
        )


def test_persisted_catalog_descriptor_miss_fails_closed_instead_of_guessing():
    raw_columns = json.dumps([
        {
            "attnum": 1,
            "name": "payload",
            "type_oid": 999999,
            "type_name": "jsonb",
            "nullable": True,
        }
    ])

    class Result:
        def fetchall(self):
            return [(
                "app", "missing_descriptor", 42, 1000, 100,
                True, "d", None, raw_columns, "external",
            )]

    class Connection:
        def execute(self, *_args, **_kwargs):
            return Result()

    with pytest.raises(Exception, match="descriptor authority"):
        read_known_relations(Connection(), "pipeline")


def test_persisted_descriptor_refusal_routes_to_durable_automatic_resnapshot():
    con = duckdb.connect(":memory:")
    try:
        destination.ensure_control_schema(con)
        destination.ensure_dataset(con, "cdc_raw")
        destination.register_table(
            con,
            pipeline="pipeline",
            source_schema="app",
            source_table="descriptor_wait",
            target_table="cdcflight_app_descriptor_wait",
        )
        # Use the durable writer's raw-column contract directly.  SourceColumn's
        # constructor intentionally fills a live catalog descriptor; this fixture is
        # the persisted-state case where that serialized authority is absent.
        missing = SimpleNamespace(
            attnum=1,
            name="payload",
            type_oid=999999,
            type_name="jsonb",
            typmod=None,
            attstorage=None,
            descriptor=None,
            nullable=True,
            has_missing_default=False,
            missing_value=None,
        )
        destination.upsert_source_relation(
            con,
            pipeline="pipeline",
            source_schema="app",
            source_table="descriptor_wait",
            relation_oid=42,
            relation_filenode=100,
            relation_type_oid=1000,
            published=True,
            replica_identity="f",
            columns=(missing,),
        )
        assert catalog_coordinator.read_known_relations(con, "pipeline") == {}
        assert destination.pending_schema_refusals(con, "pipeline")[0][0:2] == (
            "app",
            "descriptor_wait",
        )
        assert con.execute(
            "SELECT snapshot_state FROM _cdc_flight.table_state "
            "WHERE pipeline='pipeline' AND source_table='descriptor_wait'"
        ).fetchone()[0] == "awaiting_snapshot"
    finally:
        con.close()
