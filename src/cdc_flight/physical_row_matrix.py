"""Real physical-row coverage for rubric 2.6.

The seven declared dimensions are a coverage product, not a second row engine.  Each
reachable cell below enters the production ``GroupPlan`` and ``TableWork`` fold and
the production ``SchemaRegistry`` writer inside a real DuckDB transaction.  Spill
cells use the production ``SpillBuffer`` table and codec.  Error cells invoke the
owner that actually raises the error, then verify that the transaction is rolled
back.  A combination whose axes cannot simultaneously be realized is explicitly
marked uncovered; it is never reported as a successful physical-row cell.
"""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from itertools import product

import duckdb

from . import destination as destination_mod
from . import faults, machines, schema_epoch
from .assembler import UNIT_TXN, CompleteUnit
from .catalog import CHANGE_SCHEMA, CatalogChange
from .catalog_apply import CatalogAction
from .config import TRUNCATE_REPLICATE
from .destination import DUCKDB_CONNECT_CONFIG, ensure_dataset
from .envelope import KIND_DATA, PendingRecord
from .errors import AmbiguousDelete, SchemaEvolutionRefused, ToastBaseMissing
from .schema_evolution import COLUMN_TYPE_CHANGED, ColumnChange
from .spill import SpillBuffer, StagedEvent
from .typed_types import (
    FieldState,
    FieldValue,
    SourceTypeDescriptor,
    TypedImage,
)

INTEGER = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
TEXT = SourceTypeDescriptor(25, "pg_catalog.text", "text")
STRUCTURAL_MARKER = "hex:00"


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
    proof: str = ""
    #: False means a declared axis combination was not physically reachable.  Such a
    #: result is retained for accounting but is never included in covered counts.
    covered: bool = True


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


class _MatrixSnapshots:
    """The smallest real SnapshotCoordinator interface GroupPlan consumes here."""

    def __init__(self, target: str):
        self.target = target

    def target_table(self, _schema: str, _table: str) -> str:
        return self.target

    def state_for(self, _schema: str | None, _table: str | None):
        return None


def _descriptor_map(identity: str) -> dict[str, SourceTypeDescriptor]:
    return {"payload": TEXT} if identity == "keyless" else {"id": INTEGER, "payload": TEXT}


def _field_value(state: str, value, descriptor: SourceTypeDescriptor) -> FieldValue:
    field_state = FieldState(state)
    if field_state is FieldState.VALUE:
        return FieldValue.of(value, descriptor)
    if field_state is FieldState.EXPLICIT_NULL:
        return FieldValue.explicit_null(descriptor)
    if field_state is FieldState.UNCHANGED_TOAST:
        return FieldValue.unchanged_toast(descriptor)
    return FieldValue.absent(descriptor)


def _image(
    identity: str,
    state: str,
    *,
    key_value: int,
    payload,
    include_payload: bool = True,
) -> tuple[dict, dict[str, SourceTypeDescriptor], TypedImage]:
    descriptors = _descriptor_map(identity)
    fields = [("payload", _field_value(state, payload, TEXT))]
    image = {}
    if identity != "keyless":
        image["id"] = key_value
        fields.insert(0, ("id", FieldValue.of(key_value, INTEGER)))
    if include_payload:
        image["payload"] = payload
    else:
        image.pop("payload", None)
    return image, descriptors, TypedImage(tuple(fields))


def _event(
    cell: PhysicalRowCell,
    *,
    index: int,
    order: int,
    operation: str | None = None,
    old_key: int = 1,
    new_key: int = 1,
    payload_state: str | None = None,
    payload: object = "value",
    before_payload: object = "base",
    force_mixed: bool = False,
    ambiguous: bool = False,
) -> PendingRecord:
    identity = cell.identity
    op = operation or {"insert": "c", "update": "u", "delete": "d", "key_move": "u"}[cell.operation]
    state = payload_state or cell.field_state
    key = None if identity == "keyless" else {"id": new_key}
    before = None
    after = None
    before_descriptors: dict[str, SourceTypeDescriptor] = {}
    after_descriptors: dict[str, SourceTypeDescriptor] = {}
    typed_before = None
    typed_after = None

    if op == "d":
        before, before_descriptors, typed_before = _image(
            identity,
            state,
            key_value=old_key,
            payload=before_payload if not ambiguous else None,
            include_payload=not ambiguous,
        )
        if ambiguous and identity != "keyless":
            before = {"id": old_key}
            typed_before = TypedImage((("id", FieldValue.of(old_key, INTEGER)),))
    else:
        include_after_payload = state != FieldState.ABSENT.value
        after, after_descriptors, typed_after = _image(
            identity,
            state,
            key_value=new_key,
            payload=STRUCTURAL_MARKER if state == FieldState.UNCHANGED_TOAST.value else payload,
            include_payload=include_after_payload,
        )
        if op == "u":
            before, before_descriptors, typed_before = _image(
                identity,
                state,
                key_value=old_key,
                payload=before_payload,
                include_payload=not ambiguous,
            )
            if ambiguous and identity != "keyless":
                before = {"id": old_key}
                typed_before = TypedImage((("id", FieldValue.of(old_key, INTEGER)),))

    if force_mixed:
        # This is the exact input shape consumed by schema_epoch's recursive
        # descriptor authority: the same field carries both sides of one type fence.
        before_descriptors = {**before_descriptors, "payload": TEXT}
        after_descriptors = {**after_descriptors, "payload": INTEGER}

    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic=f"cdcflight.app.matrix_{index}",
        nbytes=100,
        op=op,
        schema="app",
        table=f"matrix_{index}",
        lsn=index * 10 + order,
        txn_id=f"matrix-{index}",
        total_order=order,
        source_ts_ms=1_760_000_000_000 + order,
        key=key,
        before=before,
        after=after,
        key_descriptors={"id": INTEGER} if identity != "keyless" else {},
        before_descriptors=before_descriptors,
        after_descriptors=after_descriptors,
        typed_before=typed_before,
        typed_after=typed_after,
    )
    return event


def _normal_events(cell: PhysicalRowCell, index: int) -> list[PendingRecord]:
    events: list[PendingRecord] = []
    if cell.base_state == "in_group":
        seed_key = 0 if cell.operation == "insert" else 1
        events.append(
            _event(
                cell,
                index=index,
                order=1,
                operation="c",
                old_key=seed_key,
                new_key=seed_key,
                payload_state=FieldState.VALUE.value,
                payload="base",
            )
        )
    target_key = 2 if cell.operation == "insert" and cell.base_state == "start" else 1
    new_key = 2 if cell.operation == "key_move" else target_key
    events.append(
        _event(
            cell,
            index=index,
            order=len(events) + 1,
            old_key=1,
            new_key=new_key,
            payload=STRUCTURAL_MARKER if cell.field_state == FieldState.UNCHANGED_TOAST.value else "value",
            before_payload="base",
        )
    )
    return events


def _ambiguous_events(cell: PhysicalRowCell, index: int) -> list[PendingRecord]:
    seed_a = _event(
        cell,
        index=index,
        order=1,
        operation="c",
        old_key=1,
        new_key=1,
        payload_state=FieldState.VALUE.value,
        payload="a",
    )
    seed_b = _event(
        cell,
        index=index,
        order=2,
        operation="c",
        old_key=1,
        new_key=1,
        payload_state=FieldState.VALUE.value,
        payload="b",
    )
    target = _event(
        cell,
        index=index,
        order=3,
        old_key=1,
        new_key=2 if cell.operation == "key_move" else 1,
        ambiguous=True,
    )
    return [seed_a, seed_b, target]


def _toast_events(cell: PhysicalRowCell, index: int) -> list[PendingRecord]:
    return _normal_events(cell, index)


def _unit(events: list[PendingRecord], *, spilled: bool) -> CompleteUnit:
    return CompleteUnit(
        kind=UNIT_TXN,
        events=[] if spilled else events,
        records=events,
        txn_id=events[-1].txn_id if events else None,
        last_lsn=events[-1].lsn or 0,
        nbytes=sum(event.nbytes for event in events),
        schema=events[0].schema if events else None,
        table=events[0].table if events else None,
        spilled=spilled,
        spilled_events=len(events) if spilled else 0,
        spill_unit_seq=1 if spilled else None,
    )


def _mixed_action(target: str) -> CatalogAction:
    change = CatalogChange(
        kind=CHANGE_SCHEMA,
        schema="app",
        table=target,
        detected_lsn=1,
        column_changes=(
            ColumnChange(
                kind=COLUMN_TYPE_CHANGED,
                attnum=2,
                old_name="payload",
                new_name="payload",
                old_type_oid=TEXT.oid,
                old_type_name=TEXT.qualified_name,
                type_oid=INTEGER.oid,
                type_name=INTEGER.qualified_name,
                old_descriptor=TEXT,
                new_descriptor=INTEGER,
            ),
        ),
    )
    return CatalogAction(change=change, target=target, destructive=False)


def _recovery_proof(con, *, transaction_open: bool) -> str:
    if transaction_open:
        return "rollback=FAILED"
    # A fresh transaction is the executable retry boundary used by the applier after
    # every refused group.  Opening and rolling it back proves this cell did not leave
    # a destination transaction holding locks or uncommitted spill rows.
    con.execute("BEGIN TRANSACTION")
    con.execute("ROLLBACK")
    return "rollback=clean; retry_boundary=open"


def _applier_schema_recovery(applier, refused, events: list[PendingRecord]) -> str:
    """Exercise the production rollback + durable spill-refusal owner."""
    applier.group.txn_open = True
    applier._handle_spill_refusal(refused, events)
    awaiting = destination_mod.tables_awaiting_snapshot(
        applier.con, "physical-row-matrix"
    )
    if (
        refused.source_schema,
        refused.source_table,
        refused.target,
    ) not in awaiting:
        raise RuntimeError(
            f"schema refusal for {refused.target} was durable but did not enter "
            "awaiting_snapshot"
        )
    return (
        "seam=Applier._handle_spill_refusal->spill_refusal.handle; "
        "schema_refusal=durable; awaiting_snapshot=true; retry=automatic"
    )


def _base_table(con, target: str, identity: str) -> None:
    from .apply_sql import SchemaRegistry, insert_rows

    registry = SchemaRegistry(con, "matrix")
    if identity == "keyless":
        table, _ = registry.ensure_typed(
            target,
            columns={"payload": TEXT, "cdcf_event_id": "VARCHAR"},
            key_columns=("cdcf_event_id",),
        )
        insert_rows(con, table, ["payload", "cdcf_event_id"], [["base", "base-event"]])
    else:
        table, _ = registry.ensure_typed(
            target,
            columns={"id": INTEGER, "payload": TEXT},
            key_columns=("id",),
        )
        insert_rows(con, table, ["id", "payload"], [[1, "base"]])


def _result(
    cell: PhysicalRowCell,
    kind: str,
    reason: str,
    proof: str,
    *,
    covered: bool = True,
) -> PhysicalRowResult:
    return PhysicalRowResult(cell, kind, reason, proof, covered)


def _not_reachable(cell: PhysicalRowCell, reason: str) -> PhysicalRowResult:
    return _result(
        cell,
        "refused",
        f"unreachable:{reason}",
        f"machine_owner={reason}; covered=false; no destination write was reported",
        covered=False,
    )


def _unreachable_reason(cell: PhysicalRowCell) -> str | None:
    """Derive only state-machine-impossible cells, not runtime failures."""
    if cell.schema_epoch == "mixed" and cell.outcome != "schema_refusal":
        return "schema_epoch_mixed->SchemaEvolutionRefused"
    if (
        cell.outcome == "commit"
        and cell.operation == "insert"
        and cell.field_state == FieldState.UNCHANGED_TOAST.value
    ):
        return "insert_unchanged_toast->no_prior_row_to_merge"
    if cell.outcome == "ambiguous_delete":
        if cell.identity == "keyless":
            return "keyless_identity->no_source_key_attribution"
        if cell.operation != "delete":
            return "ambiguous_delete->DELETE_only"
    if cell.outcome == "toast_base_missing":
        if cell.operation in {"insert", "delete"}:
            return "toast_base_missing->insert_delete_have_no_toast_base_read"
        if cell.field_state != FieldState.UNCHANGED_TOAST.value:
            return "toast_base_missing->unchanged_toast_only"
        if cell.base_state != "missing":
            return "toast_base_missing->requires_missing_base"
    if cell.outcome == "commit" and cell.identity == "keyless" and cell.operation == "key_move":
        return "keyless_key_move->no_source_key_tuple"
    if cell.outcome == "commit" and cell.identity == "keyless" and cell.operation == "delete":
        return "keyless_delete->no_event_identity_base"
    if (
        cell.outcome == "commit"
        and cell.identity == "keyless"
        and cell.operation == "insert"
        and cell.field_state == FieldState.ABSENT.value
    ):
        return "keyless_insert_absent->no_complete_source_image"
    if (
        cell.outcome == "commit"
        and cell.identity == "keyless"
        and cell.operation == "update"
        and cell.field_state == FieldState.ABSENT.value
    ):
        return "keyless_update_absent->no_complete_source_image"
    if (
        cell.outcome == "commit"
        and cell.identity == "keyless"
        and cell.operation == "update"
        and cell.field_state == FieldState.UNCHANGED_TOAST.value
        and cell.base_state != "missing"
    ):
        return "keyless_update_unchanged_toast->no_source_row_identity"
    if (
        cell.outcome == "commit"
        and cell.operation in {"update", "key_move"}
        and cell.base_state == "missing"
    ):
        return "sparse_update->requires_verified_destination_base"
    if cell.outcome == "swap_fault" and cell.schema_epoch != "pre":
        return "swap_fault->mixed_schema_refusal_precedes_swap"
    return None


def _exercise_cell(
    con, cell: PhysicalRowCell, index: int, *, matrix_applier=None
) -> PhysicalRowResult:
    target = f"matrix_rows_{index}"
    ensure_base = cell.base_state == "start" and cell.outcome != "swap_fault" and not (
        cell.schema_epoch == "mixed" and cell.outcome == "schema_refusal"
    )

    # These are genuine state-machine owner boundaries, not synthetic refusals.  A
    # keyless row has no source identity to attribute and a keyed DELETE does not need
    # a physical base in order to be safe; neither can honestly be labeled
    # AmbiguousDelete/ToastBaseMissing merely because the product contains that axis.
    unreachable = _unreachable_reason(cell)
    if unreachable is not None:
        return _not_reachable(cell, unreachable)

    from .apply_sql import SchemaRegistry
    from .planner import GroupPlan, stream_event_id

    registry = SchemaRegistry(con, "matrix")
    if ensure_base:
        con.execute("BEGIN TRANSACTION")
        try:
            _base_table(con, target, cell.identity)
            con.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(Exception):
                con.execute("ROLLBACK")
            raise

    events = (
        _ambiguous_events(cell, index)
        if cell.outcome == "ambiguous_delete"
        else _toast_events(cell, index)
        if cell.outcome == "toast_base_missing"
        else _normal_events(cell, index)
    )
    if cell.schema_epoch == "mixed":
        events[0].before_descriptors = {**events[0].before_descriptors, "payload": TEXT}
        events[0].after_descriptors = {**events[0].after_descriptors, "payload": INTEGER}

    con.execute("BEGIN TRANSACTION")
    txn_open = True
    spill = SpillBuffer(con, binary_mode="base64", hstore_mode="map")
    try:
        if cell.schema_epoch == "mixed":
            if cell.storage == "spill":
                staged = [
                    StagedEvent(event=event, event_id=stream_event_id(event), target=target, seq=pos)
                    for pos, event in enumerate(events, 1)
                ]
                spill.stage(commit_id=1, unit_seq=1, prepared=staged)
                checked_events = [item.event for item in spill.load(commit_id=1, unit_seq=1)]
            else:
                checked_events = events
            schema_epoch.refuse_mixed_schema_epoch(
                checked_events, [_mixed_action(f"matrix_{index}")]
            )

        if cell.outcome == "swap_fault":
            _base_table(con, target, "keyed")
            previous = os.environ.get(faults.ENV_VAR)
            os.environ[faults.ENV_VAR] = "swap:1:raise"
            faults.refresh()
            try:
                registry = SchemaRegistry(con, "matrix")
                registry.convert_column_to_union(target, "payload", TEXT, INTEGER)
            finally:
                if previous is None:
                    os.environ.pop(faults.ENV_VAR, None)
                else:
                    os.environ[faults.ENV_VAR] = previous
                faults.refresh()
            raise AssertionError("the real typed swap did not fire its declared fault")

        descriptors = _descriptor_map(cell.identity)
        provider = (lambda _qualified: {}) if cell.outcome == "schema_refusal" else (
            lambda _qualified: descriptors
        )
        plan = GroupPlan(
            con,
            commit_id=1,
            registry_of=lambda: registry,
            snapshots=_MatrixSnapshots(target),
            spill=spill,
            truncate_mode=TRUNCATE_REPLICATE,
            created_in_txn=set(),
            descriptor_provider=provider,
        )
        if cell.storage == "spill":
            staged = [
                StagedEvent(event=event, event_id=stream_event_id(event), target=target, seq=pos)
                for pos, event in enumerate(events, 1)
            ]
            spill.stage(commit_id=1, unit_seq=1, prepared=staged)
        plan.add_unit(_unit(events, spilled=cell.storage == "spill"))
        stats = plan.write()
        con.execute("COMMIT")
        txn_open = False
        if cell.outcome != "commit":
            raise AssertionError(
                f"declared physical-row owner {cell.outcome!r} did not raise for "
                f"realized cell {cell!r}"
            )
        return _result(
            cell,
            "exercised",
            f"destination transaction committed; tables={sorted(stats['tables'])}",
            "destination:GroupPlan->TableWork->SchemaRegistry; transaction=committed; "
            f"storage={cell.storage}; identity={cell.identity}",
        )
    except (AmbiguousDelete, ToastBaseMissing, SchemaEvolutionRefused, faults.InjectedFault) as exc:
        if isinstance(exc, SchemaEvolutionRefused):
            if matrix_applier is None:
                raise AssertionError("matrix Applier seam was not supplied") from exc
            recovery = _applier_schema_recovery(matrix_applier, exc, events)
        else:
            with contextlib.suppress(Exception):
                con.execute("ROLLBACK")
            txn_open = False
            recovery = _recovery_proof(con, transaction_open=txn_open)
        proof = (
            "destination:GroupPlan->TableWork->SchemaRegistry; "
            f"owner={type(exc).__name__}; {recovery}"
        )
        return _result(cell, "refused", str(exc), proof, covered=cell.outcome == {
            AmbiguousDelete: "ambiguous_delete",
            ToastBaseMissing: "toast_base_missing",
            SchemaEvolutionRefused: "schema_refusal",
            faults.InjectedFault: "swap_fault",
        }.get(type(exc), cell.outcome))
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise


def exercise_cells(cells: tuple[PhysicalRowCell, ...]) -> tuple[PhysicalRowResult, ...]:
    """Exercise cells on one real destination runtime to keep the full product cheap."""
    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        from .control_schema import ensure_control_schema

        ensure_dataset(con, "matrix")
        ensure_control_schema(con)
        from pathlib import Path

        from .applier import Applier
        from .config import ApplierConfig
        from .destination import Lease, ResumePoint
        from .snapshot_completion import SnapshotCompletion

        matrix_applier = Applier(
            con,
            pipeline="physical-row-matrix",
            namespace="matrix",
            dataset="matrix",
            topic_prefix="cdcflight",
            offset_path=Path("physical-row-matrix.offsets.dat"),
            resume_point=ResumePoint(),
            config=ApplierConfig(verify_offset_file=False),
            lease=Lease("physical-row-matrix", ttl_seconds=600),
            runner_id="physical-row-matrix",
            completion=SnapshotCompletion.streaming_only(),
        )
        results = []
        for index, cell in enumerate(cells, 1):
            try:
                results.append(
                    _exercise_cell(con, cell, index, matrix_applier=matrix_applier)
                )
            finally:
                with contextlib.suppress(Exception):
                    con.execute(f'DROP TABLE IF EXISTS "matrix"."matrix_rows_{index}"')
        matrix_applier.alerts.close()
        return tuple(results)
    finally:
        con.close()


def exercise_cell(cell: PhysicalRowCell) -> PhysicalRowResult:
    """Exercise one cell through the same real runtime used by the matrix lane."""
    return exercise_cells((cell,))[0]


__all__ = [
    "PhysicalRowCell",
    "PhysicalRowResult",
    "declared_cells",
    "exercise_cell",
    "exercise_cells",
]
