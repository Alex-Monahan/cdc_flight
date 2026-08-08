"""Rubric 2.6 unit coverage.

The matrix in this module is deliberately expressed in terms of the declared
RowPatch state machine.  A marker is a disposition, never a value, and a table
with a residual TOAST field is a fallback table even when another field is
structurally safe.
"""

from __future__ import annotations

import json
import threading
from types import SimpleNamespace

import pytest
from support.applier_lab import Lab, data, end

from cdc_flight.catalog_poll import _ensure_toast_policies
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.catalog_support import observe_unit
from cdc_flight.errors import AmbiguousDelete
from cdc_flight.row_patch import RowPatch
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.spill import _image_from_json, _image_json
from cdc_flight.table_work import TOAST_PLACEHOLDER
from cdc_flight.toast import (
    STRUCTURAL_MARKER,
    ToastRoute,
    classify_column,
    classify_relation,
    field_value,
    is_structural_marker,
)
from cdc_flight.typed_types import FieldState, SourceTypeDescriptor, TypedImage


def source(name: str, kind: str, *, oid: int = 25, element=None):
    return SourceTypeDescriptor(
        oid=oid,
        qualified_name=name,
        kind=kind,
        array_element=element,
    )


TEXT = source("pg_catalog.text", "text")
VARCHAR = source("pg_catalog.varchar", "varchar", oid=1043)
JSONB = source("pg_catalog.jsonb", "jsonb", oid=3802)
CHAR = source("pg_catalog.bpchar", "char", oid=1042)
JSON = source("pg_catalog.json", "json", oid=114)
XML = source("pg_catalog.xml", "xml", oid=142)
BYTEA = source("pg_catalog.bytea", "bytea", oid=17)
INT4 = source("pg_catalog.int4", "int4", oid=23)
HSTORE = source("public.hstore", "map", oid=9999)
TEXT_ARRAY = source("pg_catalog.text[]", "array", oid=1009, element=TEXT)
UUID_ARRAY = source(
    "pg_catalog.uuid[]", "array", oid=2951,
    element=source("pg_catalog.uuid", "uuid", oid=2950),
)
INT_ARRAY = source(
    "pg_catalog.int4[]", "array", oid=1007,
    element=INT4,
)
BYTEA_ARRAY = source(
    "pg_catalog.bytea[]", "array", oid=1001,
    element=BYTEA,
)
HSTORE_ARRAY = source(
    "public.hstore[]", "array", oid=9998,
    element=HSTORE,
)


@pytest.mark.parametrize("descriptor", [TEXT, VARCHAR, CHAR, JSON, JSONB, XML])
def test_nul_is_a_type_gated_unchanged_toast_disposition(descriptor):
    value = field_value(STRUCTURAL_MARKER, descriptor)
    assert value.state is FieldState.UNCHANGED_TOAST
    assert value.value is None
    assert is_structural_marker(STRUCTURAL_MARKER, descriptor)


@pytest.mark.parametrize(
    "descriptor,value",
    [
        (SourceTypeDescriptor(oid=1009, qualified_name="text[]", kind="array", array_element=TEXT), [STRUCTURAL_MARKER]),
        (SourceTypeDescriptor(oid=1015, qualified_name="varchar[]", kind="array", array_element=VARCHAR), [STRUCTURAL_MARKER]),
        (SourceTypeDescriptor(oid=3807, qualified_name="jsonb[]", kind="array", array_element=JSONB), [STRUCTURAL_MARKER]),
        (HSTORE, {STRUCTURAL_MARKER: STRUCTURAL_MARKER}),
        (HSTORE, json.dumps({STRUCTURAL_MARKER: STRUCTURAL_MARKER})),
    ],
)
def test_structural_array_and_hstore_forms_are_recognized(descriptor, value):
    assert field_value(value, descriptor).state is FieldState.UNCHANGED_TOAST


@pytest.mark.parametrize(
    "descriptor,value",
    [
        (TEXT, "hex:00"),
        (VARCHAR, "hex:00"),
        (CHAR, "hex:00"),
        (JSON, "hex:00"),
        (JSONB, "hex:00"),
        (XML, "hex:00"),
        (TEXT, TOAST_PLACEHOLDER),
        (VARCHAR, TOAST_PLACEHOLDER),
        (CHAR, TOAST_PLACEHOLDER),
        (JSON, TOAST_PLACEHOLDER),
        (JSONB, TOAST_PLACEHOLDER),
        (XML, TOAST_PLACEHOLDER),
        (TEXT, r"\\u0000"),
        (BYTEA, b"\x00"),
        (BYTEA, b"ordinary"),
        (INT4, 0),
    ],
)
def test_equal_looking_genuine_values_are_not_generic_marker_matches(descriptor, value):
    assert field_value(value, descriptor).state is FieldState.VALUE


@pytest.mark.parametrize(
    "mode,expected",
    [
        ("bytes", ToastRoute.FALLBACK),
        ("base64", ToastRoute.FALLBACK),
        ("base64-url-safe", ToastRoute.FALLBACK),
        ("hex", ToastRoute.STRUCTURAL),
    ],
)
def test_every_binary_mode_is_classified_for_bytea(mode, expected):
    policy = classify_column("payload", BYTEA, attstorage="x", binary_mode=mode)
    assert policy.route is expected
    assert policy.reason


@pytest.mark.parametrize(
    "mode,value,state",
    [
        ("bytes", b"\x00", FieldState.VALUE),
        ("base64", None, FieldState.EXPLICIT_NULL),
        ("base64-url-safe", None, FieldState.EXPLICIT_NULL),
        ("hex", STRUCTURAL_MARKER, FieldState.UNCHANGED_TOAST),
    ],
)
def test_binary_wire_dispositions_are_mode_gated(mode, value, state):
    assert field_value(value, BYTEA, binary_mode=mode).state is state


@pytest.mark.parametrize("mode", ["bytes", "base64", "base64-url-safe", "hex"])
def test_configured_placeholder_text_is_a_genuine_binary_value_in_every_mode(mode):
    assert field_value("hex:00", BYTEA, binary_mode=mode).state is FieldState.VALUE


@pytest.mark.parametrize(
    "descriptor,value",
    [
        (TEXT_ARRAY, ["hex:00"]),
        (HSTORE, {"hex:00": "hex:00"}),
        (HSTORE, json.dumps({"hex:00": "hex:00"})),
    ],
)
def test_configured_placeholder_is_genuine_in_string_containers(descriptor, value):
    assert field_value(value, descriptor).state is FieldState.VALUE


@pytest.mark.parametrize("descriptor", [UUID_ARRAY, INT_ARRAY, BYTEA_ARRAY, HSTORE_ARRAY])
def test_residual_array_shapes_are_not_structural(descriptor):
    assert classify_column("payload", descriptor, attstorage="x").route is ToastRoute.FALLBACK


def test_text_array_is_the_only_array_shape_in_the_allowlist():
    assert classify_column("payload", TEXT_ARRAY, attstorage="x").route is ToastRoute.STRUCTURAL


def test_scalar_fixed_storage_is_not_a_toast_risk():
    policy = classify_column("id", INT4, attstorage="p")
    assert policy.route is ToastRoute.NONE


def test_one_residual_field_switches_the_whole_table_to_fallback():
    policy = classify_relation(
        "app.events",
        [
            ("body", TEXT, "x"),
            ("payload", BYTEA, "x"),
        ],
        replica_identity="d",
    )
    assert policy.route is ToastRoute.FALLBACK
    assert policy.residual_columns == ("payload",)
    assert policy.structural_columns == ("body",)


def test_replica_identity_full_is_the_verified_residual_route():
    policy = classify_relation(
        "app.events", [("payload", BYTEA, "x")], replica_identity="f"
    )
    assert policy.route is ToastRoute.REPLICA_IDENTITY_FULL


class _PolicyConnection:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))
        if self.fail and sql.startswith("ALTER TABLE"):
            raise RuntimeError("permission denied")
        return self

    def fetchone(self):
        return ("f",)


def _residual_relation(identity="d"):
    return SourceRelation(
        schema="app",
        table="events",
        oid=42,
        published=True,
        replica_identity=identity,
        columns=(
            SourceColumn(
                1,
                "payload",
                BYTEA.oid,
                "bytea",
                descriptor=BYTEA,
                attstorage="x",
            ),
        ),
    )


def test_residual_admission_attempts_and_verifies_table_full_before_compare():
    relation = _residual_relation()
    watcher = SimpleNamespace(binary_handling_mode="base64", hstore_handling_mode="map")
    con = _PolicyConnection()
    observed = _ensure_toast_policies(watcher, con, {relation.qualified: relation})
    assert observed[relation.qualified].replica_identity == "f"
    assert any(sql.startswith('ALTER TABLE "app"."events"') for sql, _ in con.sql)


def test_failed_full_admission_stays_on_automatic_fallback_route():
    relation = _residual_relation()
    watcher = SimpleNamespace(binary_handling_mode="base64", hstore_handling_mode="map")
    observed = _ensure_toast_policies(
        watcher, _PolicyConnection(fail=True), {relation.qualified: relation}
    )
    assert observed[relation.qualified].toast_policy.route is ToastRoute.FALLBACK


def test_schema_type_change_is_repolled_before_same_name_event_admission():
    relation = SourceRelation(
        schema="app",
        table="events",
        oid=42,
        published=True,
        replica_identity="d",
        columns=(SourceColumn(1, "payload", TEXT.oid, "text", descriptor=TEXT),),
    )
    watcher = SimpleNamespace(
        dsn="source",
        known={relation.qualified: relation},
        _lock=threading.Lock(),
        polled=0,
    )

    def poll_quietly():
        watcher.polled += 1
        return []

    watcher.poll_quietly = poll_quietly
    watcher.allowed_event_fields = lambda name: {"payload", "id"}
    event = data("1", 1, 10, key={"id": 1}, after={"payload": "x"})
    event.after_descriptors = {"payload": BYTEA}
    unit = SimpleNamespace(events=[event])
    observe_unit(watcher, unit)
    assert watcher.polled == 1


def test_row_patch_composes_real_null_marker_and_absent_without_writing_marker():
    first = RowPatch(
        {"body": field_value("old", TEXT), "touch": field_value(1, INT4)},
        absent=("missing",),
    )
    second = RowPatch(
        {
            "body": field_value(STRUCTURAL_MARKER, TEXT),
            "touch": field_value(None, INT4),
        }
    )
    composed = first.compose(second)
    assert composed.field("body").state is FieldState.VALUE
    assert composed.field("body").value == "old"
    assert composed.field("touch").state is FieldState.EXPLICIT_NULL
    assert "missing" in composed.absent
    assert STRUCTURAL_MARKER not in composed.bindable_values().values()
    assert composed.digest != RowPatch({"body": field_value("old", TEXT), "touch": field_value(None, INT4)}).digest


def test_typed_image_spill_shape_preserves_field_state():
    image = TypedImage(
        (
            ("body", field_value(STRUCTURAL_MARKER, TEXT)),
            ("touch", field_value(None, INT4)),
        )
    )
    restored = TypedImage.from_dict(json.loads(json.dumps(image.to_dict())))
    assert restored.field("body").state is FieldState.UNCHANGED_TOAST
    assert restored.field("touch").state is FieldState.EXPLICIT_NULL


def test_absent_fields_are_persisted_and_marker_is_not_in_spill_raw_image():
    serialized = _image_json(
        {"body": STRUCTURAL_MARKER, "touch": 1},
        None,
        {"body": TEXT, "touch": INT4, "later": TEXT},
    )
    assert serialized is not None
    assert STRUCTURAL_MARKER not in serialized
    raw, typed = _image_from_json(serialized)
    assert raw == {"touch": 1}
    assert typed is not None
    assert typed.field("body").state is FieldState.UNCHANGED_TOAST
    assert typed.field("later").state is FieldState.ABSENT


def _txn(number: str, events: list) -> list:
    counts = {}
    for event in events:
        name = f"{event.schema}.{event.table}"
        counts[name] = counts.get(name, 0) + 1
    return [*events, end(number, len(events), max(e.lsn or 0 for e in events) + 1, counts)]


@pytest.fixture
def lab(tmp_path):
    boxes = []

    def make(**config):
        box = Lab(tmp_path / f"toast-{len(boxes)}.duckdb", **config)
        boxes.append(box)
        return box

    yield make
    for box in boxes:
        box.close()


@pytest.mark.parametrize("spill", [False, True], ids=["memory", "spill"])
def test_sparse_updates_keep_untouched_toast_body_and_compose_multiple_updates(lab, spill):
    box = lab(**({"unit_spill_events": 1, "unit_spill_bytes": 1} if spill else {}))
    body = "incompressible-body"
    initial = data(
        "1", 1, 10, key={"id": 1},
        after={"id": 1, "body": body, "touch": 0},
    )
    initial.after_descriptors = {"body": TEXT, "touch": INT4, "id": INT4}
    box.run(_txn("1", [initial]))
    marker = data(
        "2", 1, 20, op="u", key={"id": 1},
        after={"id": 1, "body": STRUCTURAL_MARKER, "touch": 1},
    )
    marker.after_descriptors = {"body": TEXT, "touch": INT4, "id": INT4}
    real = data(
        "2", 2, 21, op="u", key={"id": 1},
        after={"id": 1, "body": STRUCTURAL_MARKER, "touch": None},
    )
    real.after_descriptors = {"body": TEXT, "touch": INT4, "id": INT4}
    box.run(_txn("2", [marker, real]))
    assert box.q(
        'SELECT body, touch FROM "cdc_raw"."cdcflight_app_customers" WHERE id=1'
    ) == [(body, None)]
    if spill:
        assert box.applier.spilled_events > 0


@pytest.mark.parametrize("spill", [False, True], ids=["memory", "spill"])
def test_delete_dominates_a_prior_sparse_patch(lab, spill):
    box = lab(**({"unit_spill_events": 1, "unit_spill_bytes": 1} if spill else {}))
    initial = data("1", 1, 10, key={"id": 1}, after={"id": 1, "body": "old"})
    initial.after_descriptors = {"id": INT4, "body": TEXT}
    box.run(_txn("1", [initial]))
    update = data("2", 1, 20, op="u", key={"id": 1}, after={"id": 1, "body": STRUCTURAL_MARKER})
    update.after_descriptors = {"id": INT4, "body": TEXT}
    delete = data("2", 2, 21, op="d", key={"id": 1}, before={"id": 1, "body": STRUCTURAL_MARKER})
    delete.before_descriptors = {"id": INT4, "body": TEXT}
    box.run(_txn("2", [update, delete]))
    assert box.q('SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers"') == [(0,)]


@pytest.mark.parametrize("spill", [False, True], ids=["memory", "spill"])
def test_update_after_delete_refuses_missing_base_and_routes_to_recovery(lab, spill):
    box = lab(**({"unit_spill_events": 1, "unit_spill_bytes": 1} if spill else {}))
    box.run(_txn("1", [data("1", 1, 10, key={"id": 1}, after={"id": 1, "body": "old"})]))
    delete = data("2", 1, 20, op="d", key={"id": 1}, before={"id": 1})
    update = data("2", 2, 21, op="u", key={"id": 1}, after={"id": 1, "body": STRUCTURAL_MARKER})
    update.after_descriptors = {"id": INT4, "body": TEXT}
    with pytest.raises(AmbiguousDelete, match=r"base|resnapshot|snapshot"):
        box.run(_txn("2", [delete, update]))
    assert box.q('SELECT id, body FROM "cdc_raw"."cdcflight_app_customers"') == [(1, "old")]


@pytest.mark.parametrize("spill", [False, True], ids=["memory", "spill"])
def test_sparse_physical_key_move_preserves_body_and_changes_key(lab, spill):
    box = lab(**({"unit_spill_events": 1, "unit_spill_bytes": 1} if spill else {}))
    initial = data("1", 1, 10, key={"id": 1}, after={"id": 1, "body": "old", "touch": 0})
    initial.after_descriptors = {"id": INT4, "body": TEXT, "touch": INT4}
    box.run(_txn("1", [initial]))
    moved = data(
        "2", 1, 20, op="u", key={"id": 2},
        before={"id": 1, "body": STRUCTURAL_MARKER, "touch": 0},
        after={"id": 2, "body": STRUCTURAL_MARKER, "touch": 1},
    )
    moved.before_descriptors = {"id": INT4, "body": TEXT, "touch": INT4}
    moved.after_descriptors = {"id": INT4, "body": TEXT, "touch": INT4}
    box.run(_txn("2", [moved]))
    assert box.q(
        'SELECT id, body, touch FROM "cdc_raw"."cdcflight_app_customers"'
    ) == [(2, "old", 1)]
