"""Merge-only coverage for native types crossed with the RowPatch state machine.

The TOAST implementation and the JSON/VARIANT correction were each tested in
isolation.  These tests keep the cross-product explicit: every declared field
disposition is realized for every recursive native shape, and the two states
that must never bind are exercised through the real SQL update boundary.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import Lab, data, end

from cdc_flight import apply_sql
from cdc_flight.apply_sql import SchemaRegistry, insert_rows, update_rows
from cdc_flight.config import DestinationConfig
from cdc_flight.destination import DUCKDB_CONNECT_CONFIG
from cdc_flight.destination import connect as connect_destination
from cdc_flight.physical_row_matrix import declared_cells, exercise_cell, exercise_cells
from cdc_flight.planner import GroupPlan
from cdc_flight.row_patch import RowPatch
from cdc_flight.spill import _image_from_json, _image_json
from cdc_flight.table_work import _distinguishing
from cdc_flight.toast import STRUCTURAL_MARKER
from cdc_flight.typed_types import (
    FieldState,
    FieldValue,
    SourceTypeDescriptor,
    TypedImage,
    native_type,
    union_member_name,
)


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


INTEGER = _source("int4", 23)
TEXT = _source("text", 25)
JSON = _source("json", 114)
JSONB = _source("jsonb", 3802)
STRUCT = SourceTypeDescriptor(
    9000,
    "app.payload",
    "composite",
    composite_fields=(("payload", JSONB), ("note", TEXT)),
)
LIST = SourceTypeDescriptor(9001, "app.payload[]", "array", array_element=JSONB)
MAP = SourceTypeDescriptor(
    9002,
    "app.payload_map",
    "map",
    map_key=TEXT,
    map_value=JSONB,
)

NATIVE_CASES = (
    ("json", JSON, '{"old":1}', '{"new":2}', "JSON", '{"old":1}', '{"new":2}'),
    ("variant", JSONB, '{"old":1}', '{"new":2}', "VARIANT", {"old": 1}, {"new": 2}),
    (
        "struct",
        STRUCT,
        {"payload": '{"old":1}', "note": "old"},
        {"payload": '{"new":2}', "note": "new"},
        "STRUCT",
        {"payload": {"old": 1}, "note": "old"},
        {"payload": {"new": 2}, "note": "new"},
    ),
    ("list", LIST, ['{"old":1}', None], ['{"new":2}'], "LIST", [{"old": 1}, None], [{"new": 2}]),
    ("map", MAP, {"old": '{"n":1}'}, {"new": '{"n":2}'}, "MAP", {"old": {"n": 1}}, {"new": {"n": 2}}),
)


def test_declared_physical_row_product_realizes_or_refuses_every_cell():
    """The full operation/field/base/storage/outcome/identity/epoch product is closed."""
    cells = declared_cells()
    results = exercise_cells(cells)
    assert len(cells) == 4 * 4 * 3 * 2 * 5 * 2 * 3
    assert len({result.cell for result in results}) == len(cells)
    assert all(result.kind in {"exercised", "refused"} for result in results)
    assert all(result.reason for result in results)
    assert {result.kind for result in results} == {"exercised", "refused"}
    # Every cell was sent through the real owner. A requested outcome may differ from
    # the outcome that owner can safely provide, but that is a classified refusal,
    # never a reachability predicate or an unhandled exception.
    assert sum(result.covered for result in results) >= 1000
    refusal_outcomes = {
        "ambiguous_delete": "AmbiguousDelete",
        "toast_base_missing": "ToastBaseMissing",
        "schema_refusal": "SchemaEvolutionRefused",
        "swap_fault": "InjectedFault",
    }
    for result in results:
        assert result.owner
        if result.actual_outcome == "commit":
            assert result.kind == "exercised"
            assert result.owner == "destination_commit"
            assert result.durable_rows is not None
            continue
        assert result.kind == "refused"
        assert result.actual_outcome in refusal_outcomes
        assert result.owner == refusal_outcomes[result.actual_outcome]
        assert result.reason.startswith("owner_refusal:")
    assert all(result.rollback_clean for result in results)
    assert all(
        result.actual_outcome in {
            "commit", "ambiguous_delete", "toast_base_missing", "schema_refusal", "swap_fault"
        }
        for result in results
    )


def test_commit_cell_reports_real_destination_transaction_evidence():
    """A commit cell must not be satisfied by the old generic RowPatch stub."""
    cell = next(
        item
        for item in declared_cells()
        if item == type(item)("insert", "value", "start", "memory", "commit", "keyed", "pre")
    )
    result = exercise_cell(cell)
    assert result.kind == "exercised"
    assert result.owner == "destination_commit"
    assert result.actual_outcome == "commit"
    assert result.durable_rows == 2
    assert result.rollback_clean
    assert result.state_transition == "destination_commit"


def test_schema_refusal_uses_the_real_applier_spill_refusal_seam():
    """Refusal recovery must exercise Applier -> spill_refusal, not a test writer."""
    cell = next(
        item
        for item in declared_cells()
        if item == type(item)(
            "insert", "value", "start", "spill", "schema_refusal", "keyed", "pre"
        )
    )
    result = exercise_cell(cell)
    assert result.kind == "refused"
    assert result.covered
    assert result.owner == "SchemaEvolutionRefused"
    assert result.actual_outcome == "schema_refusal"
    assert result.rollback_clean
    assert result.state_transition == "schema_refusal->awaiting_snapshot"
    assert result.durable_rows == 1


def test_keyless_event_identity_cells_reach_the_real_owner():
    """A keyless changelog uses cdcf_event_id for every physical row operation."""
    cells = (
        type(declared_cells()[0])(
            "update", "absent", "in_group", "spill", "commit", "keyless", "post"
        ),
        type(declared_cells()[0])(
            "delete", "value", "start", "memory", "commit", "keyless", "pre"
        ),
        type(declared_cells()[0])(
            "key_move", "value", "start", "memory", "commit", "keyless", "post"
        ),
    )
    for cell in cells:
        result = exercise_cell(cell)
        assert result.kind == "exercised", result
        assert result.covered
        assert result.owner == "destination_commit"
        assert result.actual_outcome == "commit"
        assert result.durable_rows is not None
        assert result.rollback_clean


@pytest.mark.parametrize(
    ("name", "descriptor", "old", "new", "native_kind", "old_read", "new_read"),
    NATIVE_CASES,
    ids=[case[0] for case in NATIVE_CASES],
)
@pytest.mark.parametrize("state", list(FieldState), ids=lambda state: state.value)
def test_every_row_patch_disposition_reaches_the_native_update_boundary(
    name, descriptor, old, new, native_kind, old_read, new_read, state
):
    """VALUE/NULL bind; marker/ABSENT produce no native assignment at all."""

    assert native_type(descriptor).kind == native_kind
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "rows", columns={"id": INTEGER, "payload": descriptor}, key_columns=("id",)
        )
        insert_rows(con, registry.get("rows"), ["id", "payload"], [[1, old]])

        field = {
            FieldState.VALUE: FieldValue.of(new, descriptor),
            FieldState.EXPLICIT_NULL: FieldValue.explicit_null(descriptor),
            FieldState.UNCHANGED_TOAST: FieldValue.unchanged_toast(descriptor),
            FieldState.ABSENT: FieldValue.absent(descriptor),
        }[state]
        patch = RowPatch({"payload": field})
        restored = RowPatch.from_dict(json.loads(json.dumps(patch.to_dict())))
        assert restored.field("payload").state is state
        assert restored.digest == patch.digest

        bindable = patch.bindable_values()
        if state in {FieldState.VALUE, FieldState.EXPLICIT_NULL}:
            assert "payload" in bindable
            assert update_rows(con, registry.get("rows"), ("id",), [((1,), bindable)]) == 1
            expected = new_read if state is FieldState.VALUE else None
        else:
            assert "payload" not in bindable
            # An empty assignment group is machine-refused as a write: no SQL,
            # Arrow value, or destination NULL is synthesized for a disposition.
            assert update_rows(con, registry.get("rows"), ("id",), [((1,), bindable)]) == 0
            expected = old_read

        actual = con.execute('SELECT "payload" FROM typed."rows"').fetchone()[0]
        assert actual == expected, (name, state, actual, expected)
    finally:
        con.close()


@pytest.mark.parametrize(
    ("name", "descriptor"),
    [(case[0], case[1]) for case in NATIVE_CASES],
    ids=[case[0] for case in NATIVE_CASES],
)
def test_invalid_marker_value_is_refused_for_each_native_shape(name, descriptor):
    """A disposition cannot carry a value that could leak into a bind path."""

    with pytest.raises(ValueError, match="both a marker disposition and a value"):
        RowPatch(
            {
                "payload": FieldValue(
                    FieldState.UNCHANGED_TOAST, STRUCTURAL_MARKER, descriptor
                )
            }
        )


@pytest.mark.parametrize(
    ("name", "descriptor"),
    [(case[0], case[1]) for case in NATIVE_CASES],
    ids=[case[0] for case in NATIVE_CASES],
)
@pytest.mark.parametrize(
    "state", [FieldState.UNCHANGED_TOAST, FieldState.ABSENT], ids=lambda state: state.value
)
def test_native_dispositions_are_filtered_before_duplicate_key_probe(
    monkeypatch, name, descriptor, state
):
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "rows", columns={"id": INTEGER, "payload": descriptor}, key_columns=("id",)
        )
        table_work_item = SimpleNamespace(key_columns=("id",), descriptors={})
        typed = TypedImage(
            (("payload", FieldValue(state, None, descriptor)),)
        )
        image = _distinguishing(
            table_work_item,
            {"payload": STRUCTURAL_MARKER},
            descriptors={"payload": descriptor},
            typed=typed,
        )
        assert image == {}, (name, state)

        planner = object.__new__(GroupPlan)
        planner.created_in_txn = set()
        planner._registry_of = lambda: registry
        planner.con = con

        real_typed_assignment = apply_sql._typed_assignment

        def fail_if_payload_bound(table, column, value):
            if column == "payload":
                raise AssertionError("a TOAST disposition reached the duplicate-key binder")
            return real_typed_assignment(table, column, value)

        monkeypatch.setattr(apply_sql, "_typed_assignment", fail_if_payload_bound)
        probe_item = SimpleNamespace(snapshot=False, target="rows", key_columns=("id",))
        assert planner.start_matches(probe_item, (1,), image) is None
    finally:
        con.close()


def _txn(number: str, events: list) -> list:
    counts = {}
    for event in events:
        table = f"{event.schema}.{event.table}"
        counts[table] = counts.get(table, 0) + 1
    return [*events, end(number, len(events), max(event.lsn or 0 for event in events) + 1, counts)]


@pytest.mark.parametrize("spill", [False, True], ids=["memory", "spill"])
def test_native_marker_and_absent_survive_spill_replay_without_destination_writes(
    tmp_path, spill
):
    descriptors = {
        "json_value": JSON,
        "variant_value": JSONB,
        "struct_value": STRUCT,
        "list_value": LIST,
        "map_value": MAP,
        "absent_value": JSONB,
    }
    old = {
        "json_value": '{"old":1}',
        "variant_value": '{"old":2}',
        "struct_value": {"payload": '{"old":3}', "note": "old"},
        "list_value": ['{"old":4}'],
        "map_value": {"old": '{"n":5}'},
        "absent_value": '{"old":6}',
    }
    box = Lab(
        tmp_path / f"native-dispositions-{spill}.duckdb",
        **({"unit_spill_events": 1, "unit_spill_bytes": 1} if spill else {}),
    )
    try:
        initial = data("1", 1, 10, key={"id": 1}, after={"id": 1, **old})
        initial.after_descriptors = {"id": INTEGER, **descriptors}
        box.run(_txn("1", [initial]))

        marker_after = {
            "id": 1,
            **{name: STRUCTURAL_MARKER for name in descriptors if name != "absent_value"},
        }
        update = data("2", 1, 20, op="u", key={"id": 1}, after=marker_after)
        update.after_descriptors = {"id": INTEGER, **descriptors}
        update.typed_after = TypedImage(
            tuple(
                [
                    (name, FieldValue.unchanged_toast(descriptor))
                    for name, descriptor in descriptors.items()
                    if name != "absent_value"
                ]
                + [("absent_value", FieldValue.absent(JSONB))]
            )
        )
        box.run(_txn("2", [update]))

        assert box.q(
            'SELECT "json_value", "variant_value", "struct_value", '
            '"list_value", "map_value", "absent_value" '
            'FROM "cdc_raw"."cdcflight_app_customers"'
        ) == [
            (
                old["json_value"],
                {"old": 2},
                {"payload": {"old": 3}, "note": "old"},
                [{"old": 4}],
                {"old": {"n": 5}},
                {"old": 6},
            )
        ]
        assert box.scalar(
            'SELECT "dbz_op" FROM "cdc_raw"."cdcflight_app_customers"'
        ) == "u"
        if spill:
            assert box.applier.spilled_events > 0
    finally:
        box.close()


def test_native_marker_spill_sidecar_preserves_digest_and_filters_raw_payload():
    descriptors = {case[0]: case[1] for case in NATIVE_CASES}
    fields = {
        name: FieldValue.unchanged_toast(descriptor)
        for name, descriptor in descriptors.items()
    }
    fields["absent"] = FieldValue.absent(JSONB)
    patch = RowPatch(fields)
    typed = TypedImage(tuple(fields.items()))
    raw = {name: STRUCTURAL_MARKER for name in descriptors}

    serialized = _image_json(raw, typed, {**descriptors, "absent": JSONB})
    assert serialized is not None
    assert STRUCTURAL_MARKER not in serialized
    restored_raw, restored_typed = _image_from_json(serialized)
    assert restored_raw == {}
    assert restored_typed is not None
    restored_patch = RowPatch.from_image(
        restored_raw, {**descriptors, "absent": JSONB}, typed=restored_typed
    )
    assert restored_patch.digest == patch.digest
    assert all(
        restored_patch.field(name).state is FieldState.UNCHANGED_TOAST
        for name in descriptors
    )
    assert restored_patch.field("absent").state is FieldState.ABSENT


def test_json_to_jsonb_shadow_union_accepts_native_update_beside_toast_marker():
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "history",
            columns={"id": INTEGER, "value": JSON, "toast_value": JSONB},
            key_columns=("id",),
        )
        insert_rows(
            con,
            registry.get("history"),
            ["id", "value", "toast_value"],
            [[1, '{"old":true}', '{"body":"kept"}']],
        )
        registry.convert_column_to_union("history", "value", JSON, JSONB)
        table = registry.get("history")

        patch = RowPatch(
            {
                "value": FieldValue.of('{"new":true}', JSONB),
                "toast_value": FieldValue.unchanged_toast(JSONB),
            }
        )
        assert update_rows(
            con, table, ("id",), [((1,), patch.bindable_values())]
        ) == 1

        physical = con.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema='typed' AND table_name='history' AND column_name='value'"
        ).fetchone()[0]
        assert "JSON" in physical and "VARIANT" in physical
        value_tag, value, toast_value = con.execute(
            'SELECT union_tag("value"), "value", "toast_value" FROM typed."history"'
        ).fetchone()
        assert value_tag == union_member_name(JSONB)
        assert value == {"new": True}
        assert toast_value == {"body": "kept"}
    finally:
        con.close()


def test_local_destination_connection_uses_both_variant_safety_settings(tmp_path):
    con = connect_destination(
        DestinationConfig(kind="duckdb", duckdb_path=tmp_path / "settings.duckdb")
    )
    try:
        settings = dict(
            con.execute(
                "SELECT name, value FROM duckdb_settings() "
                "WHERE name IN ('storage_compatibility_version', "
                "'variant_minimum_shredding_size')"
            ).fetchall()
        )
        assert settings == DUCKDB_CONNECT_CONFIG
    finally:
        con.close()
