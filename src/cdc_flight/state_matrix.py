"""Machine-derived state/product probes used by the 1.9 coverage suite.

The matrix is intentionally an execution harness, not a second safety table.  It
starts each owner from a declared initial state, takes real production transitions to
the requested state, and invokes the owner gate that makes the interacting decision.
An expected refusal is returned only when that production gate raises its own closed
protocol error; no test-side disposition can make an unsafe cell look successful.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace

from . import catalog_admission, catalog_reporting, destination, state_interactions, table_lifecycle
from .catalog import CatalogWatcher
from .catalog_state import CHANGE_NEW, CHANGE_SCHEMA, CatalogChange, SourceRelation
from .control_schema import ensure_control_schema
from .machines import (
    ADMISSION_ABSENT,
    CATALOG_CHANGE,
    CATALOG_SCHEMA_LIVENESS,
    LIFECYCLE_ABSENT,
    PUBLICATION_ADMISSION,
    SCHEMA_REFUSAL,
    SNAPSHOT_AWAITING_CALLBACKS,
    SNAPSHOT_CALLBACKS_COMPLETE,
    SNAPSHOT_CALLBACKS_STARTED,
    SNAPSHOT_NOT_REQUIRED,
    SNAPSHOT_STREAMING,
)
from .ownership import DestinationOwnership
from .snapshot_completion import SnapshotCompletion
from .states import Machine


class MachineCellRefusal(RuntimeError):
    """A production gate refused a combination or no legal owner path exists."""


@dataclass(frozen=True)
class CellResult:
    kind: str
    reason: str


def cells(pairs) -> list[tuple[tuple[str, str], str, str]]:
    """Enumerate reachable product cells from the supplied machine declarations."""
    declared = _declared_machines()
    return [
        (pair, left, right)
        for pair in pairs
        for left in sorted(declared[pair[0]].reachable_states())
        for right in sorted(declared[pair[1]].reachable_states())
    ]


def transitions() -> list[tuple[str, str, str]]:
    """Enumerate every declared edge, including edges out of error states."""
    return [
        (machine.name, before, after)
        for machine in _declared_machines().values()
        for before, after in sorted(machine.edges)
    ]


def exercise_transition(machine_name: str, before: str, after: str) -> None:
    """Drive a declared edge through its owner when it has a product probe.

    The seven machines outside the schema/discovery product are independently owned by
    the run, recovery, or filesystem lifecycle tests; they have no cross-machine
    product callback to construct here.  Their edge is still checked against the
    production declaration, while every interacting machine below is driven through
    its real owner.
    """
    machine = _declared_machines()[machine_name]
    machine.check(before, after)
    if machine_name in {
        "catalog_change",
        "publication_admission",
        "catalog_schema_liveness",
        "schema_refusal",
        "table_lifecycle",
        "snapshot_completion",
        "destination_ownership",
    }:
        _exercise_owner(machine_name, after)


def exercise_cell(pair: tuple[str, str], left: str, right: str) -> CellResult:
    """Exercise both owners and their production interaction gate."""
    declared = _declared_machines()
    left_machine = declared[pair[0]]
    right_machine = declared[pair[1]]
    left_machine.parse(left)
    right_machine.parse(right)
    left_owner = _exercise_owner(pair[0], left)
    right_owner = _exercise_owner(pair[1], right)
    try:
        _exercise_interaction(pair, left, right, left_owner, right_owner)
    except MachineCellRefusal as refused:
        return CellResult("refused", str(refused))
    return CellResult("exercised", "the declared owners accepted the real gate")


def _declared_machines() -> dict[str, Machine]:
    from . import machines

    return machines.declared_machines()


def _path(machine: Machine, target: str) -> list[str]:
    machine.parse(target)
    queue = deque((initial, [initial]) for initial in machine.initial_states)
    seen = set(machine.initial_states)
    while queue:
        current, path = queue.popleft()
        if current == target:
            return path
        for successor in sorted(machine.successors(current)):
            if successor not in seen:
                seen.add(successor)
                queue.append((successor, [*path, successor]))
    raise MachineCellRefusal(
        f"{machine.name}:{target} has no path from declared initial state(s) "
        f"{machine.initial_states}"
    )


def _exercise_owner(machine_name: str, target: str):
    if machine_name == "catalog_change":
        return _catalog_change(target)
    if machine_name == "publication_admission":
        return _publication_admission(target)
    if machine_name == "catalog_schema_liveness":
        return _schema_liveness(target)
    if machine_name == "schema_refusal":
        return _schema_refusal(target)
    if machine_name == "table_lifecycle":
        return _table_lifecycle(target)
    if machine_name == "snapshot_completion":
        return _snapshot_completion(target)
    if machine_name == "destination_ownership":
        return _destination_ownership(target)
    raise MachineCellRefusal(f"no production owner probe for {machine_name}")


def _catalog_change(target: str):
    change = CatalogChange(
        kind=CHANGE_SCHEMA,
        schema="public",
        table="customers",
        detected_lsn=1,
    )
    path = _path(CATALOG_CHANGE, target)
    for state in path[1:]:
        change.to(state)
    change.context()
    change.can(change.state)
    return change


def _watcher(*, known=None, replicated=None, auto_discover=True) -> CatalogWatcher:
    return CatalogWatcher(
        dsn="",
        publication="cdc_pub",
        schema="public",
        schemas={"public"},
        include={"public.customers"},
        auto_discover=auto_discover,
        known=known,
        replicated=replicated,
        poll_seconds=0,
    )


def _base_relation(admission_state: str = ADMISSION_ABSENT) -> SourceRelation:
    return SourceRelation(
        schema="public",
        table="customers",
        oid=1,
        published=False,
        replica_identity="d",
        admission_state=admission_state,
    )


def _publication_admission(target: str):
    watcher = _watcher()
    change = CatalogChange(
        kind=CHANGE_NEW,
        schema="public",
        table="customers",
        detected_lsn=1,
    )
    relation = _base_relation()
    change.new_relation = relation
    watcher.known[relation.qualified] = relation
    for state in _path(PUBLICATION_ADMISSION, target)[1:]:
        relation = replace(relation, admission_state=state)
        catalog_admission._record(watcher, change, relation, error=None)
    watcher.pending_admission()
    watcher.summary()
    return watcher


def _schema_liveness(target: str):
    watcher = _watcher(
        known={"public.customers": _base_relation("external")},
        replicated={"public.customers"},
        auto_discover=False,
    )
    CATALOG_SCHEMA_LIVENESS.parse(target)
    watcher._schema_liveness["public"] = target
    # This is the real absence gate used by catalog polling.  A non-visible state
    # must not manufacture a destructive change from an empty observation.
    watcher._compare({}, 1)
    catalog_reporting.summary(watcher)
    return watcher


def _new_connection():
    import duckdb

    con = duckdb.connect(":memory:")
    ensure_control_schema(con)
    return con


def _schema_refusal(target: str):
    con = _new_connection()
    pipeline = "matrix"
    kwargs = {
        "pipeline": pipeline,
        "source_schema": "public",
        "source_table": "customers",
    }
    if target != SCHEMA_REFUSAL.initial:
        destination.record_schema_refusal(
            con,
            **kwargs,
            target_table="customers",
            detected_lsn=1,
            reason="matrix schema refusal probe",
        )
    if target == "resolved":
        con.execute("BEGIN TRANSACTION")
        destination.resolve_schema_refusal(con, **kwargs)
        con.execute("COMMIT")
    destination.pending_schema_refusals(con, pipeline)
    con.close()
    return target


def _table_lifecycle(target: str):
    con = _new_connection()
    pipeline = "matrix"
    kwargs = {
        "pipeline": pipeline,
        "source_schema": "public",
        "source_table": "customers",
    }
    path = _path(table_lifecycle.TABLE_LIFECYCLE, target)
    current = LIFECYCLE_ABSENT
    for state in path[1:]:
        if state == LIFECYCLE_ABSENT:
            table_lifecycle.forget(con, **kwargs, reason="matrix lifecycle probe")
        else:
            table_lifecycle.transition(
                con,
                **kwargs,
                to=state,
                reason="matrix lifecycle probe",
                target_table="customers",
            )
        current = state
    table_lifecycle.read(con, **kwargs)
    con.close()
    return current


def _snapshot_completion(target: str):
    if target in {SNAPSHOT_NOT_REQUIRED, SNAPSHOT_STREAMING}:
        completion = SnapshotCompletion.streaming_only()
        if target == SNAPSHOT_STREAMING:
            completion.enter_streaming()
        return completion

    completion = SnapshotCompletion.full_snapshot(expected_tables=("public.customers",))
    if target == SNAPSHOT_AWAITING_CALLBACKS:
        return completion
    completion.observe_notification("STARTED", {})
    if target == SNAPSHOT_CALLBACKS_STARTED:
        return completion
    rows = "0" if target == SNAPSHOT_CALLBACKS_COMPLETE else "1"
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "status": "SUCCEEDED",
            "scanned_collection": "public.customers",
            "total_rows_scanned": rows,
        },
    )
    completion.observe_notification("COMPLETED", {})
    return completion


class _MatrixApplier:
    callback_quiesced = True

    def __init__(self):
        self.alerts = type("Alerts", (), {"close": lambda self: None})()

    def shutdown(self, *, reason):
        del reason


def _destination_ownership(target: str):
    owner = DestinationOwnership()
    applier = _MatrixApplier()
    if target != "available":
        owner.attach(applier)
    if target in {"active", "callback_owned"}:
        owner.activate(applier)
    if target == "callback_owned":
        owner.transfer_to_callback(applier)
    owner.owns(applier)
    return owner


def _exercise_interaction(pair, left, right, left_owner, right_owner) -> None:
    decision = state_interactions.evaluate(
        pair,
        left,
        right,
        left_owner=left_owner,
        right_owner=right_owner,
    )
    if decision.kind == "refused":
        raise MachineCellRefusal(decision.reason)
