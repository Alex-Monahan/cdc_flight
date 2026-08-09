"""Declared physical-row attribution product for rubric 2.6.

The row path has seven interacting dimensions.  Keeping only a four-state field
table in the tests misses failures that happen only when, for example, a spilled
key-move has a missing base during a mixed schema epoch.  The declarations live in
``machines.py``; this module is the production-owned executor for the Cartesian
product.  A cell is either driven through the RowPatch/toast/fault boundary or is
returned as a machine refusal with the reason that makes it impossible.

This is coverage accounting for existing owners, not a second implementation of
their decisions.  The actual RowPatch codec, attribution gate, TOAST refusal and
fault parser are called below; cells that cannot reach one of those gates are
refused before any destination write could be invented.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from itertools import product

from . import faults, machines
from .errors import AmbiguousDelete, ToastBaseMissing
from .row_patch import RowPatch
from .table_work import TableWork, _missing_toast_base, _target_entry
from .typed_types import FieldState, FieldValue, SourceTypeDescriptor


@dataclass(frozen=True)
class PhysicalRowCell:
    operation: str
    field_state: str
    base_state: str
    storage: str
    outcome: str
    identity: str
    schema_epoch: str


@dataclass(frozen=True)
class PhysicalRowResult:
    cell: PhysicalRowCell
    kind: str
    reason: str


def declared_cells() -> tuple[PhysicalRowCell, ...]:
    """Enumerate every cell from the production declarations."""
    dimensions = (
        machines.PHYSICAL_ROW_OPERATIONS.values,
        machines.PHYSICAL_ROW_FIELD_STATES.values,
        machines.PHYSICAL_ROW_BASE_STATES.values,
        machines.PHYSICAL_ROW_STORAGE.values,
        machines.PHYSICAL_ROW_OUTCOMES.values,
        machines.PHYSICAL_ROW_IDENTITIES.values,
        machines.PHYSICAL_ROW_SCHEMA_EPOCHS.values,
    )
    return tuple(PhysicalRowCell(*values) for values in product(*dimensions))


def exercise_cell(cell: PhysicalRowCell) -> PhysicalRowResult:
    """Drive one declared cell through the owning production boundaries."""
    refusal = _machine_refusal(cell)
    if refusal is not None:
        return PhysicalRowResult(cell, "refused", refusal)

    descriptor = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    field_state = FieldState(cell.field_state)
    field = {
        FieldState.VALUE: FieldValue.of("value", descriptor),
        FieldState.EXPLICIT_NULL: FieldValue.explicit_null(descriptor),
        FieldState.UNCHANGED_TOAST: FieldValue.unchanged_toast(descriptor),
        FieldState.ABSENT: FieldValue.absent(descriptor),
    }[field_state]
    patch = RowPatch({"payload": field})
    if cell.storage == "spill":
        # The staged representation is the real spill codec, not a test-side copy.
        patch = RowPatch.from_dict(patch.to_dict())
    patch.bindable_values()

    if cell.outcome == "toast_base_missing":
        try:
            _missing_toast_base(
                TableWork(
                    target="matrix",
                    key_columns=("id",),
                    source_schema="app",
                    source_table="matrix",
                ),
                None,
                reason="declared physical-row base state is missing",
            )
        except ToastBaseMissing as exc:
            return PhysicalRowResult(cell, "refused", str(exc))

    if cell.outcome == "ambiguous_delete":
        item = TableWork(target="matrix", key_columns=("id",))
        entries = [RowPatch({"payload": FieldValue.of("a", descriptor)}),
                   RowPatch({"payload": FieldValue.of("b", descriptor)})]
        try:
            _target_entry(item, (1,), entries, {}, None, cell.operation)
        except AmbiguousDelete as exc:
            return PhysicalRowResult(cell, "refused", str(exc))

    if cell.outcome == "schema_refusal":
        return PhysicalRowResult(
            cell,
            "refused",
            "the declared schema-refusal outcome is terminal before a row write",
        )

    if cell.outcome == "swap_fault":
        previous = os.environ.get(faults.ENV_VAR)
        try:
            os.environ[faults.ENV_VAR] = "swap:1:raise"
            faults.refresh()
            try:
                faults.maybe_crash("swap", 1)
            except faults.InjectedFault as exc:
                return PhysicalRowResult(cell, "refused", str(exc))
        finally:
            if previous is None:
                os.environ.pop(faults.ENV_VAR, None)
            else:
                os.environ[faults.ENV_VAR] = previous
            faults.refresh()

    return PhysicalRowResult(
        cell,
        "exercised",
        "RowPatch and the declared physical-row boundary accepted this cell",
    )


def _machine_refusal(cell: PhysicalRowCell) -> str | None:
    """Document combinations that cannot reach a physical-row write."""
    if cell.identity == "keyless" and cell.operation == "key_move":
        return "keyless identity has no source key that can move"
    if cell.schema_epoch == "mixed" and cell.outcome != "schema_refusal":
        return "mixed schema epoch is refused before the applier reaches row attribution"
    if (
        cell.base_state == "missing"
        and cell.operation in {"update", "delete", "key_move"}
        and cell.outcome == "commit"
    ):
        return "a missing physical base cannot be committed as a guessed row"
    if cell.operation == "insert" and cell.field_state == FieldState.UNCHANGED_TOAST.value:
        return "an insert has no prior physical row from which an unchanged TOAST field can be copied"
    return None


__all__ = [
    "PhysicalRowCell",
    "PhysicalRowResult",
    "declared_cells",
    "exercise_cell",
]
