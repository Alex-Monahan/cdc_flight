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
    ColumnChange,
)


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
