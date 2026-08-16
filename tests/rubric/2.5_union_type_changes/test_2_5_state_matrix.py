"""Generated schema-fence/refusal state coverage for rubric 2.5."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdc_flight import machines, state_matrix
from cdc_flight.envelope import PendingRecord
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.schema_epoch import refuse_mixed_schema_epoch
from cdc_flight.schema_evolution import (
    COLUMN_RENAMED,
    COLUMN_TYPE_CHANGED,
    ColumnChange,
)
from cdc_flight.typed_types import SourceTypeDescriptor


def test_union_epoch_matrix_uses_declared_owners_and_real_gates():
    """Exercise the generated schema/refusal product, including error cells."""

    pairs = tuple(
        pair
        for pair in machines.INTERACTING_MACHINE_PAIRS
        if "catalog_change" in pair or "schema_refusal" in pair
    )
    cells = state_matrix.cells(pairs)
    assert cells
    for pair, left, right in cells:
        result = state_matrix.exercise_cell(pair, left, right)
        assert result.kind in {"exercised", "refused"}
        assert result.reason


def test_mixed_epoch_data_is_a_durable_refusal_route():
    change = ColumnChange(
        kind=COLUMN_RENAMED,
        attnum=2,
        old_name="value",
        new_name="value_new",
        old_type_oid=23,
        old_type_name="integer",
        type_oid=25,
        type_name="text",
    )
    action = SimpleNamespace(
        change=SimpleNamespace(
            qualified="app.items",
            column_changes=(change,),
            detected_lsn=1,
        )
    )
    event = PendingRecord(
        raw=None,
        kind="data",
        topic="app.items",
        nbytes=1,
        schema="app",
        table="items",
        before={"id": 1, "value": 7},
        after={"id": 1, "value_new": "seven"},
    )
    with pytest.raises(SchemaEvolutionRefused, match="contains both sides"):
        refuse_mixed_schema_epoch([event], [action])


def test_same_name_type_change_fences_pre_and_post_images():
    old = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    new = SourceTypeDescriptor(20, "pg_catalog.int8", "int8")
    change = ColumnChange(
        kind=COLUMN_TYPE_CHANGED,
        attnum=2,
        old_name="value",
        new_name="value",
        old_type_oid=23,
        old_type_name="integer",
        type_oid=20,
        type_name="bigint",
        old_descriptor=old,
        new_descriptor=new,
    )
    action = SimpleNamespace(
        change=SimpleNamespace(
            qualified="app.items",
            column_changes=(change,),
            detected_lsn=1,
        )
    )
    before = PendingRecord(
        raw=None,
        kind="data",
        topic="app.items",
        nbytes=1,
        schema="app",
        table="items",
        before={"id": 1, "value": 7},
        after={"id": 1, "value": 8},
        before_descriptors={"id": old, "value": old},
        after_descriptors={"id": old, "value": old},
    )
    after = PendingRecord(
        raw=None,
        kind="data",
        topic="app.items",
        nbytes=1,
        schema="app",
        table="items",
        before={"id": 1, "value": 8},
        after={"id": 1, "value": 9},
        before_descriptors={"id": new, "value": new},
        after_descriptors={"id": new, "value": new},
    )
    with pytest.raises(SchemaEvolutionRefused, match="both sides"):
        refuse_mixed_schema_epoch([before, after], [action])
