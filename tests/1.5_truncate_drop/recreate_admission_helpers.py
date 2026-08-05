"""Small fixtures and state constructors shared by the 1.5 fold and recreate tests."""

from __future__ import annotations

from applier_lab import DATASET, Lab, data, end, keyed

from cdc_flight import table_lifecycle
from cdc_flight.catalog import (
    CHANGE_RECREATED,
    CatalogChange,
    CatalogWatcher,
    SourceRelation,
)
from cdc_flight.machines import (
    CHANGE_MARKED,
    LIFECYCLE_AWAITING,
    LIFECYCLE_COMPLETE,
    LIFECYCLE_IN_PROGRESS,
)

CUSTOMERS = "cdcflight_app_customers"
ORDERS = "cdcflight_app_orders"


def _watcher(*, present: dict[str, int] | None = None, **kw) -> CatalogWatcher:
    """A polling-disabled watcher whose source OID read is supplied by the test."""
    watcher = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0, **kw
    )
    oids = dict(present or {})
    watcher.relation_oids = lambda names: {  # type: ignore[method-assign]
        f"{schema}.{table}": oids.get(f"{schema}.{table}")
        for schema, table in names
    }
    return watcher


def txn(number: str, events: list, per_table: dict[str, int] | None = None) -> list:
    counts: dict[str, int] = {}
    for event in events:
        qualified = f"{event.schema}.{event.table}"
        counts[qualified] = counts.get(qualified, 0) + 1
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, per_table or counts)]


def preload(box: Lab, *, customers=(1, 2, 3), orders=(7, 8)) -> None:
    events = [
        keyed("1", i + 1, 10 + i, ident, f"c{ident}")
        for i, ident in enumerate(customers)
    ]
    events += [
        data(
            "1", len(customers) + i + 1, 20 + i, table="orders",
            key={"id": ident}, after={"id": ident, "note": f"o{ident}"},
        )
        for i, ident in enumerate(orders)
    ]
    box.run(txn("1", events))


def rows(box: Lab, table: str) -> list[tuple]:
    return box.q(f'SELECT id FROM "{DATASET}"."{table}" ORDER BY id')


def markers(box: Lab) -> list[tuple]:
    return box.q(
        "SELECT event, source_table, applied, rows_removed FROM _cdc_flight.table_events "
        "ORDER BY commit_id, seq"
    )


def _queue(watcher: CatalogWatcher, change: CatalogChange) -> None:
    watcher.queue(change)


def _catalog_relation(table: str, oid: int) -> SourceRelation:
    return SourceRelation(
        schema="app", table=table, oid=oid, published=True, replica_identity="d"
    )


def _queue_recreated(
    watcher: CatalogWatcher,
    relation: SourceRelation,
    *,
    state: str = CHANGE_MARKED,
    detected_lsn: int = 150,
) -> None:
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_RECREATED,
            schema=relation.schema,
            table=relation.table,
            detected_lsn=detected_lsn,
            old_oid=16384,
            new_oid=relation.oid,
            new_relation=relation,
            state=state,
        ),
    )


def _assert_recreated_boundary(box: Lab, relation: SourceRelation) -> None:
    assert not box.exists(CUSTOMERS), "the stale image is quarantined until re-snapshot"
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE pipeline = 'lab' AND source_table = 'customers'"
    ) == [("awaiting_snapshot",)]
    assert box.q(
        "SELECT relation_oid FROM _cdc_flight.source_relations "
        "WHERE pipeline = 'lab' AND source_table = 'customers'"
    ) == [(relation.oid,)]


def _set_current_relation_oid(watcher: CatalogWatcher, relation: SourceRelation) -> None:
    watcher.relation_oids = lambda names: {  # type: ignore[method-assign]
        f"{schema}.{table}": relation.oid for schema, table in names
    }


def _realize_recreate_lifecycle(box: Lab, state: str) -> None:
    """Materialize a declared retained-image state rather than only naming it."""
    if state == LIFECYCLE_COMPLETE:
        table_lifecycle.transition(
            box.con,
            pipeline="lab",
            source_schema="app",
            source_table="customers",
            to=LIFECYCLE_IN_PROGRESS,
            reason="recreate admission matrix setup",
            target_table=box.target("customers"),
        )
        table_lifecycle.transition(
            box.con,
            pipeline="lab",
            source_schema="app",
            source_table="customers",
            to=LIFECYCLE_COMPLETE,
            reason="recreate admission matrix setup",
            target_table=box.target("customers"),
            snapshot_lsn=25,
            last_commit_id=1,
        )
    elif state == LIFECYCLE_AWAITING:
        table_lifecycle.transition(
            box.con,
            pipeline="lab",
            source_schema="app",
            source_table="customers",
            to=LIFECYCLE_AWAITING,
            reason="recreate admission matrix setup",
            target_table=box.target("customers"),
        )
    else:  # pragma: no cover - the caller derives states from TABLE_LIFECYCLE
        raise AssertionError(f"unhandled recreate lifecycle state {state!r}")
