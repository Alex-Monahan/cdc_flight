"""Rubric 1.5 — TRUNCATE and DROP through the shipped applier (no JVM, no Postgres).

`TRUNCATE` is a *transactional* statement in Postgres and pgoutput carries it: one
`op="t"` event per relation of a `TRUNCATE a, b CASCADE`, all inside one
transaction, all counted in the transaction's `END.event_count`. The applier
therefore gets it for free from the commit protocol — what it has to get right is
the *fold*:

* everything the group planned for that table before the truncate is gone (Postgres
  removes rows the same transaction inserted, too);
* rows collected **after** the truncate survive (`TRUNCATE t; INSERT …` in one
  transaction leaves the inserted rows);
* the destination table is emptied with a `DELETE FROM` inside the group's
  transaction, so a rolled-back group leaves every row in place;
* a truncate carries **no message key**, which must not be read as "this table is
  keyless" for the rest of the group.

`DROP TABLE` is not in the stream at all (`cdc_flight.catalog` explains why), so
what is tested here is the *application* of a detected drop: the LSN fence, the
destination DDL, and the marker row. The detection itself is
`test_1_5_catalog_detection.py`.
"""

from __future__ import annotations

import duckdb
import pytest
from recreate_admission_helpers import (
    CUSTOMERS,
    ORDERS,
    _assert_recreated_boundary,
    _catalog_relation,
    _queue,
    _queue_recreated,
    _watcher,
    markers,
    preload,
    rows,
    txn,
)
from support.applier_lab import DATASET, Lab, heartbeat, keyed, truncate

from cdc_flight import catalog_baseline, faults, table_lifecycle
from cdc_flight.catalog import (
    CHANGE_DROPPED,
    CHANGE_RECREATED,
    DESTRUCTIVE,
    CatalogChange,
    SourceRelation,
)
from cdc_flight.config import DROP_IGNORE, DROP_LOG, DROP_MODES, DROP_REPLICATE
from cdc_flight.destination import upsert_source_relation
from cdc_flight.machines import (
    CATALOG_BASELINE,
    CHANGE_MARKED,
)


@pytest.fixture
def lab(tmp_path):
    boxes: list[Lab] = []

    def _make(**cfg) -> Lab:
        box = Lab(tmp_path / f"lab{len(boxes)}.duckdb", **cfg)
        boxes.append(box)
        return box

    yield _make
    for box in boxes:
        box.close()


# --------------------------------------------------------------------------- #
# truncate
# --------------------------------------------------------------------------- #
def test_a_truncate_empties_the_destination_table(lab):
    box = lab()
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200)]))
    assert rows(box, CUSTOMERS) == []
    assert rows(box, ORDERS) == [(7,), (8,)], "only the truncated table is emptied"


def test_a_truncate_records_a_marker_with_what_it_removed(lab):
    """Rubric 1.5 wants faithful current state; history must not just evaporate.

    The current-state table is emptied because Postgres emptied it, and
    `_cdc_flight.table_events` records the truncate, its LSN, its transaction and the
    number of rows the destination lost - in the SAME transaction as the delete.
    """
    box = lab()
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200)]))
    assert markers(box) == [("truncate", "customers", True, 3)]


def test_a_multi_table_truncate_is_one_atomic_group(lab):
    """`TRUNCATE app.customers, app.orders CASCADE` arrives as one event per table
    inside one transaction, so both tables are emptied by one COMMIT."""
    box = lab()
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200), truncate("2", 2, 200, table="orders")]))
    assert rows(box, CUSTOMERS) == []
    assert rows(box, ORDERS) == []
    assert [(m[0], m[1], m[3]) for m in markers(box)] == [
        ("truncate", "customers", 3),
        ("truncate", "orders", 2),
    ]
    # One commit group, so one commit_log row lists both tables.
    logged = box.q(
        "SELECT tables_touched FROM _cdc_flight.commit_log ORDER BY commit_id DESC LIMIT 1"
    )
    assert sorted(logged[0][0]) == [CUSTOMERS, ORDERS]


def test_rows_inserted_after_a_truncate_in_the_same_transaction_survive(lab):
    box = lab()
    preload(box)
    box.run(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                keyed("2", 2, 201, 50, "after-truncate"),
                keyed("2", 3, 202, 51, "also-after"),
            ],
        )
    )
    assert rows(box, CUSTOMERS) == [(50,), (51,)]


def test_rows_inserted_before_a_truncate_in_the_same_transaction_do_not(lab):
    """Postgres removes them, so the destination must too."""
    box = lab()
    preload(box)
    box.run(
        txn(
            "2",
            [
                keyed("2", 1, 200, 60, "doomed"),
                truncate("2", 2, 201),
                keyed("2", 3, 202, 61, "survivor"),
            ],
        )
    )
    assert rows(box, CUSTOMERS) == [(61,)]


def test_a_truncate_does_not_make_a_keyed_table_keyless(lab):
    """The truncate event has no message key. Reading that as "keyless" would give
    every later event in the group a `cdcf_event_id` identity and turn the
    current-state table into an append-only log."""
    box = lab()
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 5, "v1"), keyed("2", 3, 202, 5, "v2", op="u")]))
    assert rows(box, CUSTOMERS) == [(5,)], "one row per key, not one row per event"
    assert box.q(f'SELECT name FROM "{DATASET}"."{CUSTOMERS}"') == [("v2",)]


def test_a_truncate_of_a_table_the_destination_never_held_creates_nothing(lab):
    box = lab()
    box.run(txn("2", [truncate("2", 1, 200, table="never_seen")]))
    assert not box.exists("cdcflight_app_never_seen")
    assert markers(box) == [("truncate", "never_seen", True, 0)]


def test_a_truncate_that_rolls_back_leaves_every_row_in_place(lab, monkeypatch):
    box = lab()
    preload(box)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", [truncate("2", 1, 200)]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert markers(box) == [], "the marker must not outlive the apply it describes"
    # `drain_on_shutdown()` is what a real run does after a failed group: the buffered
    # unit is deferred (Invariant O says nothing about it was acknowledged) and the
    # next run replays it from the durable resume point.
    box.applier.drain_on_shutdown()
    box.run(txn("2", [truncate("2", 1, 200)]))
    assert rows(box, CUSTOMERS) == []
    assert markers(box) == [("truncate", "customers", True, 3)]


def test_truncate_mode_log_keeps_the_rows_and_still_records_the_marker(lab):
    """The rubric's "tombstones / soft delete" behaviour, for a destination whose
    consumers treat the table as an append-only log."""
    box = lab(truncate_mode="log")
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200)]))
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert markers(box) == [("truncate", "customers", False, None)]


def test_a_spilled_truncate_still_empties_the_table(lab):
    """A truncate that had to be staged in `_cdc_flight.spill_events` must come back
    out as a truncate; restored as a data event it would be folded as a row."""
    box = lab(unit_spill_events=1, unit_spill_bytes=1)
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 70, "after")]))
    assert box.applier.spilled_events >= 1, "the test did not actually spill"
    assert rows(box, CUSTOMERS) == [(70,)]


def test_an_unknown_truncate_mode_is_refused(tmp_path):
    with pytest.raises(ValueError, match="CDC_TRUNCATE_MODE"):
        Lab(tmp_path / "bad.duckdb", truncate_mode="tombstone")


# --------------------------------------------------------------------------- #
# applying a detected drop
# --------------------------------------------------------------------------- #


def test_a_dropped_table_is_dropped_at_the_destination(lab):
    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=100, old_oid=16384, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert not box.exists(CUSTOMERS), "the destination table must be gone"
    assert box.exists(ORDERS)
    assert markers(box) == [("dropped", "customers", True, None)]
    assert box.applier.tables_dropped == 1


def test_a_drop_is_not_applied_before_the_destination_reaches_the_detected_lsn(lab):
    """The fence. A drop detected at LSN X must not be applied while the destination
    is still behind X: an in-flight event for that table would re-create it as a
    zombie holding pre-drop rows."""
    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=10_000, old_oid=16384, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert box.exists(CUSTOMERS), "applied too early"
    assert markers(box) == []
    assert len(watcher.pending()) == 1

    # A record past the detected LSN opens the fence - here a heartbeat, in a live
    # run the WAL marker the watcher emits.
    box.run([heartbeat(10_500)])
    assert not box.exists(CUSTOMERS)
    assert markers(box) == [("dropped", "customers", True, None)]


def test_a_dropped_table_raises_an_alert(lab):
    """"Your destination table is gone" is the one signal an operator must not have
    to go looking for, and `alerts` is deliberately written outside the commit
    group's transaction (ADR §9.1)."""
    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=100, old_oid=1, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    alerts = box.q("SELECT severity, code FROM _cdc_flight.alerts")
    assert alerts == [("warning", "table_dropped")]


def test_a_dropped_table_forgets_its_table_state_row(lab):
    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    # No manual INSERT: streaming DML that creates a destination table now registers
    # the ownership itself, inside the same transaction (Codex 5). That row is what
    # makes a drop detectable after a restart, and it is what the drop removes.
    assert box.q(
        "SELECT target_table FROM _cdc_flight.table_state WHERE source_table = 'customers'"
    ) == [(CUSTOMERS,)]
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=100, old_oid=1, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert box.q(
        "SELECT count(*) FROM _cdc_flight.table_state WHERE source_table = 'customers'"
    ) == [(0,)]


def test_drop_mode_log_keeps_the_destination_table(lab):
    watcher = _watcher()
    box = lab(catalog=watcher, drop_mode="log")
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=100, old_oid=1, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert box.exists(CUSTOMERS)
    assert markers(box) == [("dropped", "customers", False, None)]


def _baseline_state_for_matrix(box: Lab, relation: SourceRelation, state: str):
    """Reach a declared baseline state, then return the next run's mark.

    The invalidated cell models a previous run that rebuilt its table but died before
    confirming the baseline: the source identity and complete lifecycle are restored,
    while the durable baseline remains invalidated until this run proves it again.
    """
    kwargs = {"pipeline": "lab", "dataset": DATASET}
    if state == catalog_baseline.ABSENT:
        upsert_source_relation(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            relation_oid=relation.oid,
            published=relation.published,
            replica_identity=relation.replica_identity,
            columns=relation.columns,
        )
    elif state == catalog_baseline.STALE:
        upsert_source_relation(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            relation_oid=relation.oid,
            published=relation.published,
            replica_identity=relation.replica_identity,
            columns=relation.columns,
        )
        catalog_baseline.mark_unconfirmed(box.con, **kwargs)
    elif state == catalog_baseline.VALID:
        upsert_source_relation(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            relation_oid=relation.oid,
            published=relation.published,
            replica_identity=relation.replica_identity,
            columns=relation.columns,
        )
        prior = catalog_baseline.mark_unconfirmed(box.con, **kwargs)
        catalog_baseline.confirm(
            box.con, **kwargs, check=prior, successful_polls=1
        )
    elif state == catalog_baseline.INVALIDATED:
        prior = catalog_baseline.mark_unconfirmed(box.con, **kwargs)
        assert prior.state == catalog_baseline.INVALIDATED
        table_lifecycle.transition(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            to=table_lifecycle.IN_PROGRESS,
            reason="matrix repair",
            target_table=box.target(relation.table),
        )
        table_lifecycle.transition(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            to=table_lifecycle.COMPLETE,
            reason="matrix repair",
            target_table=box.target(relation.table),
        )
        upsert_source_relation(
            box.con,
            pipeline="lab",
            source_schema=relation.schema,
            source_table=relation.table,
            relation_oid=relation.oid,
            published=relation.published,
            replica_identity=relation.replica_identity,
            columns=relation.columns,
        )
    else:  # pragma: no cover - the parameter list comes from the machine
        raise AssertionError(f"unhandled baseline state {state!r}")

    return catalog_baseline.mark_unconfirmed(
        box.con,
        **kwargs,
        reconcile=box.config.drop_mode != DROP_IGNORE,
    )


# Keep this matrix coupled to the declared destructive catalog domain. If a new
# destructive lifecycle is added, the drop-mode matrix must grow with it.
DROP_CHANGE_KINDS = tuple(DESTRUCTIVE)
DROP_BASELINE_CELLS = tuple(
    (drop_mode, baseline_state, change_kind)
    for drop_mode in DROP_MODES
    for baseline_state in sorted(CATALOG_BASELINE.reachable_states())
    for change_kind in DROP_CHANGE_KINDS
)


@pytest.mark.parametrize("drop_mode, baseline_state, change_kind", DROP_BASELINE_CELLS)
def test_drop_mode_and_baseline_confirmation_matrix(
    lab, drop_mode, baseline_state, change_kind
):
    """Every baseline/change cell has an explicit outcome for every drop mode.

    ``ignore`` has no catalog watcher in the real pipeline, so its confirmation cells
    are deliberate machine refusals (zero successful polls), not an untested allow path.
    ``replicate`` destroys the destination for both destructive changes; ``log`` keeps a
    plain drop's identity and destination, but a recreate quarantines the stale image
    while its new lifecycle is durably owed a re-snapshot.
    """
    old_relation = _catalog_relation("customers", 16384)
    new_relation = _catalog_relation("customers", 16385)
    watcher = _watcher(
        present={new_relation.qualified: new_relation.oid}
        if change_kind == CHANGE_RECREATED
        else None
    )
    box = lab(catalog=watcher, drop_mode=drop_mode)
    box.run(txn("1", [keyed("1", 1, 10, 1, "before")]))
    check = _baseline_state_for_matrix(box, old_relation, baseline_state)

    if drop_mode == DROP_IGNORE:
        box.run([heartbeat(200)])
        refused = catalog_baseline.confirm(
            box.con,
            pipeline="lab",
            dataset=DATASET,
            check=check,
            successful_polls=0,
        )
        assert not refused.valid
        assert refused.state == catalog_baseline.STALE
        assert box.exists(CUSTOMERS)
        assert box.q(
            "SELECT relation_oid FROM _cdc_flight.source_relations "
            "WHERE pipeline = 'lab' AND source_table = 'customers'"
        ) == [(old_relation.oid,)]
        return

    if change_kind == CHANGE_RECREATED:
        _queue_recreated(watcher, new_relation)
    else:
        _queue(
            watcher,
            CatalogChange(
                kind=CHANGE_DROPPED,
                schema="app",
                table="customers",
                detected_lsn=100,
                old_oid=old_relation.oid,
                new_relation=old_relation,
                state=CHANGE_MARKED,
            ),
        )
    box.run([heartbeat(200)])

    confirmed = catalog_baseline.confirm(
        box.con,
        pipeline="lab",
        dataset=DATASET,
        check=check,
        successful_polls=1,
    )
    if change_kind == CHANGE_RECREATED:
        assert not confirmed.valid, (drop_mode, baseline_state, confirmed.reason)
        assert confirmed.state == catalog_baseline.INVALIDATED
        _assert_recreated_boundary(box, new_relation)
    else:
        assert confirmed.valid, (drop_mode, baseline_state, confirmed.reason)

    if drop_mode == DROP_LOG:
        if change_kind == CHANGE_DROPPED:
            assert box.exists(CUSTOMERS)
            assert box.q(
                "SELECT relation_oid FROM _cdc_flight.source_relations "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [(old_relation.oid,)]
            assert box.q(
                "SELECT count(*) FROM _cdc_flight.table_state "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [(1,)]
    else:
        assert drop_mode == DROP_REPLICATE
        if change_kind == CHANGE_DROPPED:
            assert not box.exists(CUSTOMERS)
            assert box.q(
                "SELECT count(*) FROM _cdc_flight.source_relations "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [(0,)]
            assert box.q(
                "SELECT count(*) FROM _cdc_flight.table_state "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [(0,)]
        else:
            assert box.exists(CUSTOMERS)
            assert box.q(
                "SELECT relation_oid FROM _cdc_flight.source_relations "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [(new_relation.oid,)]
            assert box.q(
                "SELECT snapshot_state FROM _cdc_flight.table_state "
                "WHERE pipeline = 'lab' AND source_table = 'customers'"
            ) == [("awaiting_snapshot",)]
    assert markers(box) == [
        (
            change_kind,
            "customers",
            drop_mode == DROP_REPLICATE or change_kind == CHANGE_RECREATED,
            None,
        )
    ]


def test_drop_mode_log_persists_identity_and_confirms_after_restart(lab):
    """A logged drop is durable catalog history, not a half-applied deletion."""
    watcher = _watcher()
    customers = _catalog_relation("customers", 16384)
    orders = _catalog_relation("orders", 16385)
    watcher._dirty.update({customers.qualified: customers, orders.qualified: orders})
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    assert box.q(
        "SELECT source_schema, source_table, relation_oid "
        "FROM _cdc_flight.source_relations WHERE pipeline = 'lab' "
        "ORDER BY source_table"
    ) == [("app", "customers", 16384), ("app", "orders", 16385)]

    check = catalog_baseline.mark_unconfirmed(
        box.con, pipeline="lab", dataset=DATASET
    )
    assert check.was == catalog_baseline.ABSENT
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED,
            schema="app",
            table="customers",
            detected_lsn=100,
            old_oid=customers.oid,
            new_relation=customers,
            state=CHANGE_MARKED,
        ),
    )
    box.run([heartbeat(200)])
    assert box.exists(CUSTOMERS)
    assert box.q(
        "SELECT count(*) FROM _cdc_flight.table_state "
        "WHERE pipeline = 'lab' AND source_table = 'customers'"
    ) == [(1,)]
    assert box.q(
        "SELECT relation_oid FROM _cdc_flight.source_relations "
        "WHERE pipeline = 'lab' AND source_table = 'customers'"
    ) == [(16384,)]
    assert markers(box) == [("dropped", "customers", False, None)]

    confirmed = catalog_baseline.confirm(
        box.con,
        pipeline="lab",
        dataset=DATASET,
        check=check,
        successful_polls=1,
    )
    assert confirmed.valid, confirmed.reason
    box.close()

    restarted = duckdb.connect(str(box.path))
    try:
        after_restart = catalog_baseline.mark_unconfirmed(
            restarted, pipeline="lab", dataset=DATASET
        )
        after_restart = catalog_baseline.confirm(
            restarted,
            pipeline="lab",
            dataset=DATASET,
            check=after_restart,
            successful_polls=1,
        )
        assert after_restart.valid, after_restart.reason
        assert restarted.execute(
            "SELECT relation_oid FROM _cdc_flight.source_relations "
            "WHERE pipeline = 'lab' AND source_table = 'customers'"
        ).fetchall() == [(16384,)]
        assert restarted.execute(
            f'SELECT count(*) FROM "{DATASET}"."{CUSTOMERS}"'
        ).fetchone() == (3,)
    finally:
        restarted.close()


def test_an_unpublished_table_is_never_dropped(lab):
    """A table that left the publication still HAS its rows in Postgres. Dropping the
    destination table would destroy data the source holds, so it is a marker and an
    operator decision, not a replication action."""
    from cdc_flight.catalog import CHANGE_UNPUBLISHED

    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_UNPUBLISHED, schema="app", table="customers",
            detected_lsn=100, old_oid=1, new_oid=1, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert markers(box) == [("unpublished", "customers", False, None)]


def test_a_rolled_back_drop_stays_pending(lab, monkeypatch):
    """A crash between the DDL and the COMMIT must leave the change to be applied
    again, not forget it - the destination table is still there."""
    watcher = _watcher()
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED, schema="app", table="customers",
            detected_lsn=100, old_oid=1, state=CHANGE_MARKED,
        ),
    )
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert box.exists(CUSTOMERS), "the DDL rolled back with the group"
    assert len(watcher.pending()) == 1, "and the change is still to be applied"

    box.run(txn("3", [keyed("3", 1, 400, 9, "unrelated", table="orders")]))
    assert not box.exists(CUSTOMERS)
