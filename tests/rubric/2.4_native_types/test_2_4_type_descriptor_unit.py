"""Default-lane contract tests for rubric 2.4 native type handling."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import duckdb
import pytest
from support.applier_lab import Lab, data
from support.type_matrix import nested_matrix, scalar_matrix

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows, update_rows
from cdc_flight.catalog_descriptors import CatalogDescriptorReader, RelationDescriptorProvider
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.destination import (
    DUCKDB_CONNECT_CONFIG,
    assert_runtime_capabilities,
    pending_schema_refusals,
    tables_awaiting_snapshot,
)
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.planner import GroupPlan
from cdc_flight.spill import SpillBuffer, StagedEvent
from cdc_flight.typed_types import (
    FieldState,
    FieldValue,
    SourceTypeDescriptor,
    TypedImage,
    UnsupportedType,
    encode_value,
    native_type,
    numeric_value,
)


def test_json_and_jsonb_have_distinct_native_targets():
    json_source = SourceTypeDescriptor(114, "pg_catalog.json", "json")
    jsonb_source = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")

    assert native_type(json_source).kind == "JSON"
    assert native_type(json_source).sql == "JSON"
    assert native_type(jsonb_source).kind == "VARIANT"
    assert native_type(jsonb_source).sql == "VARIANT"


def test_jsonb_key_uses_json_while_non_key_jsonb_stays_variant():
    """JSONB is a VARIANT value, but JSON is the lossless key representation."""

    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA d")
        registry = SchemaRegistry(con, "d")
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
        jsonb = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")
        registry.ensure_typed(
            "jsonb_keys",
            columns={"id": jsonb, "tenant": integer, "payload": jsonb},
            key_columns=("id", "tenant"),
        )
        table = registry.get("jsonb_keys")
        assert table.raw_types["id"] == "JSON"
        assert table.raw_types["tenant"] == "INTEGER"
        assert table.raw_types["payload"] == "VARIANT"
        assert table.primary_key_columns == ("id", "tenant")
        insert_rows(
            con,
            table,
            ["id", "tenant", "payload"],
            [['{"account": 7}', 1, '{"body": true}']],
        )
        assert con.execute(
            'SELECT "id", "payload" FROM d."jsonb_keys"'
        ).fetchone() == ('{"account":7}', {"body": True})
    finally:
        con.close()


def test_jsonb_key_gain_rebinds_a_composite_identity_without_variant_pk():
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA d")
        registry = SchemaRegistry(con, "d")
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
        jsonb = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")
        registry.ensure_typed(
            "key_gain",
            columns={"id": integer, "json_key": jsonb},
            key_columns=("id",),
        )
        insert_rows(con, registry.get("key_gain"), ["id", "json_key"], [[1, '{"a": 1}']])
        registry.ensure_typed(
            "key_gain",
            columns={"id": integer, "json_key": jsonb},
            key_columns=("id", "json_key"),
        )
        table = registry.get("key_gain")
        assert table.raw_types["json_key"] == "JSON"
        assert table.primary_key_columns == ("id", "json_key")
    finally:
        con.close()


def test_jsonb_primary_key_rebuild_self_heals_a_legacy_variant_identity():
    """A queued rebuild cannot repeat the old VARIANT-primary-key failure."""
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA d")
        con.execute(
            "CREATE TABLE d.legacy_variant_key ("
            "id VARIANT, payload VARIANT, cdcf_internal_id VARCHAR PRIMARY KEY)"
        )
        con.execute(
            "INSERT INTO d.legacy_variant_key VALUES "
            "(CAST(CAST('{\"account\":7}' AS JSON) AS VARIANT), "
            "CAST(CAST('{\"body\":true}' AS JSON) AS VARIANT), 'legacy-row')"
        )
        registry = SchemaRegistry(con, "d")
        jsonb = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")
        registry.ensure_typed(
            "legacy_variant_key",
            columns={"id": jsonb, "payload": jsonb},
            key_columns=("id",),
        )
        table = registry.get("legacy_variant_key")
        assert table.raw_types["id"] == "JSON"
        assert table.primary_key_columns == ("id",)
        assert "cdcf_internal_id" not in table.columns
        assert con.execute(
            'SELECT "id", "payload" FROM d."legacy_variant_key"'
        ).fetchone() == ('{"account":7}', {"body": True})
    finally:
        con.close()


def test_jsonb_key_loss_uses_a_typed_shadow_transition_to_variant():
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA d")
        registry = SchemaRegistry(con, "d")
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
        jsonb = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")
        registry.ensure_typed(
            "key_loss",
            columns={"id": integer, "json_key": jsonb, "payload": jsonb},
            key_columns=("id", "json_key"),
        )
        insert_rows(
            con,
            registry.get("key_loss"),
            ["id", "json_key", "payload"],
            [[1, '{"a": 1}', '{"body": true}']],
        )
        registry.ensure_typed(
            "key_loss",
            columns={"id": integer, "json_key": jsonb, "payload": jsonb},
            key_columns=("id",),
        )
        table = registry.get("key_loss")
        assert table.raw_types["json_key"] == "VARIANT"
        assert table.primary_key_columns == ("id",)
        assert con.execute('SELECT "payload" FROM d."key_loss"').fetchone() == (
            {"body": True},
        )
    finally:
        con.close()


def test_nested_composite_jsonb_key_uses_key_native_types_and_one_identity_encoder():
    """A key gain must rebind nested JSONB to JSON and delete the old row."""
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA d")
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
        text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
        jsonb = SourceTypeDescriptor(3802, "pg_catalog.jsonb", "jsonb")
        inner = SourceTypeDescriptor(
            9100,
            "app.inner_key",
            "composite",
            composite_fields=(("doc", jsonb), ("n", integer)),
        )
        outer = SourceTypeDescriptor(
            9101,
            "app.outer_key",
            "composite",
            composite_fields=(("inner_key", inner), ("note", text)),
        )
        registry = SchemaRegistry(con, "d")
        registry.ensure_typed(
            "nested_key",
            columns={"id": integer, "compound": outer, "payload": text},
            key_columns=("id",),
        )
        insert_rows(
            con,
            registry.get("nested_key"),
            ["id", "compound", "payload"],
            [[1, {"inner_key": {"doc": '{"a":1}', "n": 7}, "note": "x"}, "kept"]],
        )
        registry.ensure_typed(
            "nested_key",
            columns={"id": integer, "compound": outer, "payload": text},
            key_columns=("id", "compound"),
        )
        table = registry.get("nested_key")
        assert "doc JSON" in table.raw_types["compound"]
        delete_keys(
            con,
            table,
            ("id", "compound"),
            [(1, {"inner_key": {"doc": '{"a":1}', "n": 7}, "note": "x"})],
        )
        assert con.execute('SELECT count(*) FROM d."nested_key"').fetchone()[0] == 0
    finally:
        con.close()


def test_bytea_key_changed_to_union_uses_the_same_identity_for_existing_rows():
    """The old BLOB member must be addressable after the typed UNION swap."""
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA d")
        bytea = SourceTypeDescriptor(17, "pg_catalog.bytea", "bytea")
        text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
        registry = SchemaRegistry(con, "d")
        registry.ensure_typed(
            "bytea_union_key",
            columns={"id": bytea, "payload": text},
            key_columns=("id",),
        )
        insert_rows(con, registry.get("bytea_union_key"), ["id", "payload"], [[b"abc", "kept"]])
        registry.convert_column_to_union("bytea_union_key", "id", bytea, text)
        table = registry.get("bytea_union_key")
        delete_keys(con, table, ("id",), [(b"abc",)])
        assert con.execute('SELECT count(*) FROM d."bytea_union_key"').fetchone()[0] == 0
    finally:
        con.close()


@pytest.mark.parametrize("shape", ["missing", "incomplete"], ids=["missing", "incomplete"])
def test_relation_descriptor_provider_refuses_non_authoritative_catalog_shape(
    monkeypatch, shape
):
    """A strict one-shot provider cannot infer around a missing/incomplete tree."""
    oid = 9102 if shape == "incomplete" else 3802

    class CatalogConnection:
        def execute(self, _sql, _params):
            return self

        def fetchall(self):
            return [("app", "rows", "payload", oid, -1, "jsonb" if oid == 3802 else "app.payload")]

    if shape == "missing":
        resolved = {}
    else:
        resolved = {
            oid: SourceTypeDescriptor(oid, "app.payload", "composite", composite_fields=())
        }
    monkeypatch.setattr(CatalogDescriptorReader, "resolve", lambda _reader, _oids: resolved)
    with pytest.raises(SchemaEvolutionRefused, match="descriptor"):
        RelationDescriptorProvider.from_tables(
            CatalogConnection(), [("app", "rows", "cdcflight_app_rows")]
        )


def test_spill_descriptor_failure_rolls_back_and_records_a_durable_refusal(tmp_path):
    box = Lab(tmp_path / "spill-refusal.duckdb", unit_spill_events=1, unit_spill_bytes=1)
    try:
        def failing_provider(_qualified):
            raise OSError("catalog unavailable")

        box.applier.descriptor_provider = failing_provider
        event = data("spill-refusal", 1, 10, table="spill_rows", key={"id": 1}, after={"id": 1})
        with pytest.raises(SchemaEvolutionRefused, match="descriptor"):
            box.applier._spill_events([event], unit_seq=1)
        assert box.applier.group.txn_open is False
        assert pending_schema_refusals(box.con, "lab")
        assert [
            f"{schema}.{table}"
            for schema, table, _target in tables_awaiting_snapshot(box.con, "lab")
        ] == ["app.spill_rows"]
        assert box.con.execute(
            "SELECT count(*) FROM _cdc_flight.spill_events"
        ).fetchone()[0] == 0
    finally:
        box.close()


def test_production_typed_path_fails_closed_when_catalog_descriptors_are_unavailable():
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="app.rows",
        nbytes=1,
        schema="app",
        table="rows",
        key={"id": 1},
        after={"id": 1, "payload": "{}"},
        after_descriptors={"id": integer},
    )
    plan = object.__new__(GroupPlan)
    plan.descriptor_provider = lambda _qualified: (_ for _ in ()).throw(
        OSError("catalog unavailable")
    )
    plan._catalog_descriptor_cache = {}
    with pytest.raises(SchemaEvolutionRefused, match="catalog descriptor"):
        plan._enrich_descriptors(event)


def test_tablework_numeric_adapter_is_idempotent_for_all_bounded_specials():
    from cdc_flight.table_work import _typed_value
    from cdc_flight.typed_types import UnionValue

    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    numeric = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=12, scale=4
    )
    registry.ensure_typed("adapter_numbers", columns={"id": integer, "value": numeric}, key_columns=("id",))
    table = registry.get("adapter_numbers")
    for raw in ("NaN", "Infinity", "-Infinity", "1.2500", None):
        encoded = encode_value(raw, numeric)
        adapted = _typed_value(table, "value", encoded)
        assert adapted == encoded
        if encoded is not None:
            assert isinstance(adapted, UnionValue)


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity"])
def test_numeric_specials_update_through_the_typed_assignment_seam(raw):
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA d")
        integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
        numeric = SourceTypeDescriptor(
            1700, "pg_catalog.numeric", "numeric", precision=12, scale=4
        )
        registry = SchemaRegistry(con, "d")
        registry.ensure_typed(
            "assignment_numbers", columns={"id": integer, "value": numeric}, key_columns=("id",)
        )
        insert_rows(con, registry.get("assignment_numbers"), ["id", "value"], [[1, "1.2500"]])
        changed = update_rows(
            con,
            registry.get("assignment_numbers"),
            ("id",),
            [((1,), {"value": encode_value(raw, numeric)})],
        )
        assert changed == 1
        assert con.execute('SELECT union_tag("value") FROM d."assignment_numbers"').fetchone()[0] == "special"
    finally:
        con.close()


def test_runtime_capability_guard_rejects_an_effective_setting_mismatch():
    class WrongSettings:
        def execute(self, _sql, _params):
            return self

        def fetchall(self):
            return [
                ("storage_compatibility_version", "v1.4.0"),
                ("variant_minimum_shredding_size", "-1"),
            ]

    with pytest.raises(RuntimeError, match="required VARIANT settings"):
        assert_runtime_capabilities(WrongSettings())


def test_descriptor_is_recursive_and_has_stable_fingerprint():
    original = nested_matrix()[2]
    restored = SourceTypeDescriptor.from_dict(original.to_dict())

    assert restored == original
    assert restored.fingerprint == original.fingerprint
    assert restored.array_element is None
    assert restored.map_key.qualified_name == "pg_catalog.text"


@pytest.mark.parametrize("source", scalar_matrix())
def test_every_core_scalar_resolves_to_a_native_destination(source):
    target = native_type(source)
    assert target.sql
    assert target.kind not in {"VARCHAR_FALLBACK", "JSON_FALLBACK"}


@pytest.mark.parametrize("source", nested_matrix())
def test_nested_values_are_not_stringified(source):
    target = native_type(source)
    assert target.kind in {"LIST", "STRUCT", "MAP"}
    assert "JSON" not in target.sql.upper()
    assert target.sql != "VARCHAR"


def _matrix_sql_cases():
    scalar_sql = (
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "VARCHAR",
        "BLOB",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "TIMETZ",
        "INTERVAL",
        "UUID",
        "JSON",
        "VARIANT",
        "UNION(finite DECIMAL(12,4),special DOUBLE)",
        "STRUCT(coefficient BIGNUM,scale INTEGER,special DOUBLE)",
        "ENUM('pending','paid')",
        "VARCHAR",
        "VARCHAR",
    )
    nested_sql = (
        "INTEGER[]",
        "INTEGER[][]",
        "MAP(VARCHAR,VARCHAR)",
        'STRUCT("id" INTEGER,"label" VARCHAR)',
        "INTEGER[]",
    )
    return tuple(zip(scalar_matrix(), scalar_sql, strict=True)) + tuple(
        zip(nested_matrix(), nested_sql, strict=True)
    )


@pytest.mark.parametrize(
    ("source", "expected_sql"),
    _matrix_sql_cases(),
    ids=lambda case: case[0].qualified_name if isinstance(case, tuple) else str(case),
)
def test_scalar_and_nested_matrix_sql_is_pinned(source, expected_sql):
    assert native_type(source).sql == expected_sql


@pytest.mark.parametrize("value, expected", [
    ("NaN", "special"),
    ("Infinity", "special"),
    ("-Infinity", "special"),
    ("12.3400", "finite"),
])
def test_numeric_specials_use_the_declared_numeric_union(value, expected):
    source = SourceTypeDescriptor(
        oid=1700,
        qualified_name="pg_catalog.numeric",
        kind="numeric",
        precision=12,
        scale=4,
    )
    encoded = numeric_value(value, source)
    assert encoded.member == expected
    if expected == "finite":
        assert encoded.value == Decimal("12.3400")
    else:
        assert encoded.value in {float("inf"), float("-inf")} or encoded.value != encoded.value


@pytest.mark.parametrize("value", [date(2026, 8, 7), time(1, 2, 3), datetime(2026, 8, 7, 1, 2, 3)])
def test_temporal_values_remain_temporal(value):
    kind = {date: "date", time: "time", datetime: "timestamp"}[type(value)]
    target = native_type(SourceTypeDescriptor(oid=1, qualified_name=f"pg_catalog.{kind}", kind=kind))
    assert encode_value(value, target.source or target) == value


def test_typed_image_distinguishes_null_from_absent_and_round_trips():
    integer = SourceTypeDescriptor(oid=23, qualified_name="pg_catalog.int4", kind="int4")
    image = TypedImage.from_mapping({"a": None, "b": 3}, {"a": integer, "b": integer})

    assert image.field("a").state is FieldState.EXPLICIT_NULL
    assert image.field("b").state is FieldState.VALUE
    assert image.field("c").state is FieldState.ABSENT
    assert TypedImage.from_dict(image.to_dict()) == image
    assert FieldValue.unchanged_toast().state is FieldState.UNCHANGED_TOAST


def test_typed_spill_round_trip_preserves_descriptor_and_raw_toast_like_string():
    con = duckdb.connect()
    ensure_control_schema(con)
    integer = SourceTypeDescriptor(oid=23, qualified_name="pg_catalog.int4", kind="int4")
    text = SourceTypeDescriptor(oid=25, qualified_name="pg_catalog.text", kind="text")
    placeholder = "__debezium_unavailable_value"
    event = PendingRecord(
        raw=object(),
        kind=KIND_DATA,
        topic="app.events",
        nbytes=1,
        op="u",
        schema="app",
        table="events",
        lsn=11,
        txn_id="7",
        total_order=1,
        key={"id": 1},
        before={"id": 1, "payload": placeholder},
        after={"id": 1, "payload": placeholder},
        key_descriptors={"id": integer},
        before_descriptors={"id": integer, "payload": text},
        after_descriptors={"id": integer, "payload": text},
        typed_key=TypedImage.from_mapping({"id": 1}, {"id": integer}),
        typed_before=TypedImage.from_mapping(
            {"id": 1, "payload": placeholder},
            {"id": integer, "payload": text},
        ),
        typed_after=TypedImage.from_mapping(
            {"id": 1, "payload": placeholder},
            {"id": integer, "payload": text},
        ),
    )
    spill = SpillBuffer(con)
    spill.stage(
        commit_id=1,
        unit_seq=1,
        prepared=[StagedEvent(event=event, event_id="11:7:1", target="events", seq=1)],
    )
    restored = spill.load(commit_id=1, unit_seq=1)[0].event

    assert restored.after == event.after
    assert restored.typed_after is not None
    assert restored.typed_after.field("payload").state is FieldState.VALUE
    assert restored.typed_after.field("payload").value == placeholder
    assert restored.after_descriptors["id"].fingerprint == integer.fingerprint
    assert restored.after_descriptors["payload"].fingerprint == text.fingerprint


def test_unknown_types_fail_closed_instead_of_becoming_text():
    with pytest.raises(UnsupportedType):
        native_type(SourceTypeDescriptor(oid=999999, qualified_name="ext.secret", kind="unknown"))


def test_recursive_struct_map_and_numeric_union_write_native_values():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    bounded_numeric = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=12, scale=4
    )
    row = SourceTypeDescriptor(
        9000,
        "app.row_type",
        "composite",
        composite_fields=(("n", integer), ("amount", bounded_numeric)),
    )
    attrs = SourceTypeDescriptor(
        9001, "public.hstore", "map", map_key=text, map_value=text
    )
    registry.ensure_typed(
        "native_rows",
        columns={"id": integer, "payload": row, "attrs": attrs},
        key_columns=("id",),
    )
    insert_rows(
        con,
        registry.get("native_rows"),
        ["id", "payload", "attrs"],
        [[1, {"n": 7, "amount": "12.3400"}, {"site": "hq", "tier": None}]],
    )

    types = dict(
        con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'd' AND table_name = 'native_rows'"
        ).fetchall()
    )
    assert types["payload"].startswith("STRUCT(")
    assert "JSON" not in types["payload"].upper()
    assert types["attrs"].startswith("MAP(")
    assert con.execute(
        "SELECT payload.n, union_tag(payload.amount), attrs['site'], attrs['tier'] "
        "FROM d.native_rows"
    ).fetchone() == (7, "finite", "hq", None)


def test_keyless_event_identity_remains_the_destination_primary_key():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    registry.ensure_typed(
        "changelog",
        columns={"cdcf_event_id": "VARCHAR", "value": text},
        key_columns=("cdcf_event_id",),
    )
    assert registry.get("changelog").primary_key_columns == ("cdcf_event_id",)


def test_bounded_numeric_null_and_specials_are_explicitly_tagged():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    registry = SchemaRegistry(con, "d")
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    numeric = SourceTypeDescriptor(
        1700, "pg_catalog.numeric", "numeric", precision=12, scale=4
    )
    registry.ensure_typed("numbers", columns={"id": integer, "value": numeric}, key_columns=("id",))
    insert_rows(
        con,
        registry.get("numbers"),
        ["id", "value"],
        [[1, None], [2, "NaN"], [3, "Infinity"], [4, "-Infinity"], [5, "1.2500"]],
    )
    rows = con.execute(
        "SELECT id, union_tag(value), value FROM d.numbers ORDER BY id"
    ).fetchall()
    assert [(row[0], row[1]) for row in rows] == [
        (1, "finite"),
        (2, "special"),
        (3, "special"),
        (4, "special"),
        (5, "finite"),
    ]
    assert rows[0][2] is None
    assert rows[4][2] == Decimal("1.2500")
    assert con.execute(
        "SELECT isnan(value.special), isinf(value.special), "
        "isinf(value.special) AND value.special < 0 FROM d.numbers "
        "WHERE id IN (2, 3, 4) ORDER BY id"
    ).fetchall() == [(True, False, False), (False, True, False), (False, True, True)]
