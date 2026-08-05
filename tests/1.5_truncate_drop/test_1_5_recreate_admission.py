"""Rubric 2.3 / R6 — structural replacement admission and lifecycle matrix."""

from __future__ import annotations

import random

import pytest
from applier_lab import Lab, data, heartbeat, keyed
from recreate_admission_helpers import (
    CUSTOMERS,
    ORDERS,
    _assert_recreated_boundary,
    _catalog_relation,
    _queue,
    _queue_recreated,
    _realize_recreate_lifecycle,
    _set_current_relation_oid,
    _watcher,
    preload,
    rows,
    txn,
)

from cdc_flight import catalog_generation, table_lifecycle
from cdc_flight.catalog import CHANGE_DROPPED, CatalogChange
from cdc_flight.config import DROP_LOG, DROP_REPLICATE
from cdc_flight.machines import (
    CATALOG_CHANGE,
    CHANGE_APPLIED,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_OBSERVED,
    CHANGE_PENDING,
    CHANGE_SUPERSEDED,
    CHANGE_UNCONFIRMED,
    LIFECYCLE_AWAITING,
    LIFECYCLE_COMPLETE,
    TABLE_LIFECYCLE,
)
from cdc_flight.snapshot_completion import SnapshotObservationError


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


def test_a_recreated_table_drops_the_destination_and_says_why(lab):
    """A same-name replacement gets an absent target and an owed snapshot state."""
    watcher = _watcher(present={"app.customers": 99999})
    box = lab(catalog=watcher)
    preload(box)
    _queue(
        watcher,
        CatalogChange(
            kind="recreated", schema="app", table="customers",
            detected_lsn=50, old_oid=16384, new_oid=99999, state=CHANGE_MARKED,
        ),
    )
    box.run(txn("2", [keyed("2", 1, 300, 9, "unrelated", table="orders")]))
    assert not box.exists(CUSTOMERS)
    detail = box.q(
        "SELECT detail FROM _cdc_flight.table_events WHERE event = 'recreated'"
    )[0][0]
    assert "re-snapshot" in detail and "99999" in detail
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state WHERE source_table = 'customers'"
    ) == [("awaiting_snapshot",)]
    assert box.applier.stats()["tables_awaiting_snapshot"] == ["app.customers"]


def test_log_mode_recreate_refuses_new_relation_stream_until_resnapshot(lab):
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={new.qualified: new.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    _queue(
        watcher,
        CatalogChange(
            kind=CHANGE_DROPPED,
            schema="app",
            table="customers",
            detected_lsn=100,
            old_oid=old.oid,
            new_relation=old,
            state=CHANGE_MARKED,
        ),
    )
    box.run([heartbeat(125)])
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]

    _queue_recreated(watcher, new)
    box.run([heartbeat(175)])
    _assert_recreated_boundary(box, new)

    with pytest.raises(SnapshotObservationError, match="awaiting_snapshot"):
        box.run(txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")]))
    assert not box.exists(CUSTOMERS)


def test_log_mode_recreate_same_group_fences_before_catalog_apply(lab):
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={new.qualified: new.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, new)
    box.run(txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")]))
    _assert_recreated_boundary(box, new)
    assert box.applier.fenced_units == 1
    assert box.q(
        "SELECT event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(0,)]


def test_log_mode_recreate_spilled_same_group_fences_before_apply(lab):
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={new.qualified: new.oid})
    watcher._dirty[old.qualified] = old
    box = lab(
        catalog=watcher,
        drop_mode=DROP_LOG,
        unit_spill_events=1,
        unit_spill_bytes=1,
    )
    preload(box)
    _queue_recreated(watcher, new)
    box.feed(txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")]))
    unit = box.applier.group.units[-1]
    assert unit.spilled_events >= 1
    assert unit.tables_touched() == {"app.customers"}
    box.commit()
    _assert_recreated_boundary(box, new)
    assert unit.fenced
    assert box.applier.fenced_spilled_events >= 1


def test_log_mode_recreate_pre_detection_tail_is_fenced_before_catalog_poll(lab):
    """The new relation cannot enter a valid image before the watcher observes it."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    _set_current_relation_oid(watcher, new)
    box.run(txn("2", [keyed("2", 1, 130, 4, "new-lifecycle")]))
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.applier.fenced_units == 1

    _queue_recreated(watcher, new, detected_lsn=150)
    box.run([heartbeat(151)])
    _assert_recreated_boundary(box, new)


def test_log_mode_pre_detection_spill_only_group_is_not_counted_as_data(lab):
    """Fenced spill rows do not advance data-group or fault-anchor accounting."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    box = lab(
        catalog=watcher,
        drop_mode=DROP_LOG,
        unit_spill_events=1,
        unit_spill_bytes=1,
    )
    preload(box)
    data_groups_before = box.applier.data_commit_groups

    _set_current_relation_oid(watcher, new)
    box.feed(txn("2", [keyed("2", 1, 130, 4, "new-lifecycle")]))
    unit = box.applier.group.units[-1]
    assert unit.spilled_events >= 1
    box.commit()

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert unit.fenced
    assert box.applier.data_commit_groups == data_groups_before
    assert box.applier.fenced_spilled_events >= 1
    assert box.q(
        "SELECT fenced_units, event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(1, 0)]


def test_log_mode_recreate_same_group_fences_strict_predicate_bypass(lab):
    """A low-LSN replacement unit cannot bypass a due plan via ordering."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    _set_current_relation_oid(watcher, new)
    _queue_recreated(watcher, new, detected_lsn=150)
    box.run(
        txn("2", [keyed("2", 1, 130, 4, "new-lifecycle")])
        + txn(
            "3",
            [
                data(
                    "3", 1, 200, table="orders", key={"id": 9},
                    after={"id": 9, "note": "unrelated"},
                )
            ],
        )
    )
    assert not box.exists(CUSTOMERS)
    assert rows(box, ORDERS) == [(7,), (8,), (9,)]
    assert box.applier.fenced_units == 1
    assert box.q(
        "SELECT fenced_units, event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(1, 1)]


# The matrix is derived from both machines. The other lifecycle states are not retained
# images: absent/none are registration or initial-snapshot work, and in_progress is owned
# by snapshot completion/recovery. They are documented here and tested by their owning
# state-machine suites rather than relabelled as complete/awaiting setup.
RECREATE_IMAGE_LIFECYCLE_STATES = tuple(
    state
    for state in sorted(TABLE_LIFECYCLE.reachable_states())
    if state in {LIFECYCLE_COMPLETE, LIFECYCLE_AWAITING}
)
RECREATE_OUT_OF_SCOPE_LIFECYCLE_STATES = frozenset(
    TABLE_LIFECYCLE.reachable_states() - set(RECREATE_IMAGE_LIFECYCLE_STATES)
)
RECREATE_PLAN_STATES = (None, *sorted(CATALOG_CHANGE.reachable_states()))
RECREATE_NO_PLAN_CATALOG_STATES = frozenset(
    CATALOG_CHANGE.reachable_states() - {CHANGE_DUE}
)
RECREATE_GENERATION_PLAN_STATES = tuple(
    state for state in RECREATE_PLAN_STATES if state == CHANGE_DUE
)
RECREATE_GENERATION_MACHINE_REFUSED_STATES = frozenset(
    state for state in RECREATE_PLAN_STATES if state != CHANGE_DUE
)
RECREATE_ADMISSION_CELLS = tuple(
    (lifecycle, catalog_plan_state, spilled)
    for lifecycle in RECREATE_IMAGE_LIFECYCLE_STATES
    for catalog_plan_state in RECREATE_PLAN_STATES
    for spilled in (False, True)
)


def test_recreate_matrix_documents_non_image_lifecycle_ownership():
    assert {"absent", "none", "in_progress"} == RECREATE_OUT_OF_SCOPE_LIFECYCLE_STATES
    assert CHANGE_DUE not in RECREATE_NO_PLAN_CATALOG_STATES
    assert RECREATE_GENERATION_PLAN_STATES == (CHANGE_DUE,)
    assert (
        RECREATE_NO_PLAN_CATALOG_STATES | {None}
    ) == RECREATE_GENERATION_MACHINE_REFUSED_STATES


@pytest.mark.parametrize(
    "lifecycle_state, catalog_plan_state, spilled", RECREATE_ADMISSION_CELLS
)
def test_log_recreate_admission_catalog_plan_spill_matrix(
    lab, lifecycle_state, catalog_plan_state, spilled
):
    """Every retained-image x catalog-state x spill cell realizes its state."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation(
        "customers", 16385 if lifecycle_state == LIFECYCLE_COMPLETE else 16386
    )
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    spill_config = {"unit_spill_events": 1, "unit_spill_bytes": 1} if spilled else {}
    box = lab(catalog=watcher, drop_mode=DROP_LOG, **spill_config)
    preload(box)
    _realize_recreate_lifecycle(box, lifecycle_state)
    assert table_lifecycle.read(
        box.con, pipeline="lab", source_schema="app", source_table="customers"
    ) == lifecycle_state
    _set_current_relation_oid(watcher, replacement)

    change = None
    if catalog_plan_state is not None:
        _queue_recreated(
            watcher,
            replacement,
            state=catalog_plan_state,
            detected_lsn=150 if catalog_plan_state == CHANGE_DUE else 1_000,
        )
        expected_state = (
            CHANGE_PENDING
            if catalog_plan_state in {CHANGE_OBSERVED, CHANGE_UNCONFIRMED}
            else catalog_plan_state
        )
        change = watcher._changes[-1]
        assert change.state == expected_state

    stream = txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")])
    if lifecycle_state == LIFECYCLE_AWAITING:
        with pytest.raises(SnapshotObservationError, match="awaiting_snapshot"):
            box.run(stream)
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
        if change is not None:
            assert change.state == expected_state
        return

    box.run(stream)
    if catalog_plan_state == CHANGE_DUE:
        _assert_recreated_boundary(box, replacement)
        assert change is not None and change.state == CHANGE_APPLIED
        assert change not in watcher.pending(), "the applied plan must be resolved"
    else:
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
        assert box.applier.fenced_units == 1
        if change is not None:
            if catalog_plan_state in {CHANGE_APPLIED, CHANGE_SUPERSEDED}:
                # Terminal machine states are deliberately no-plan cells: the watcher
                # refuses to re-admit them, so they cannot become a catalog action.
                assert change.state == catalog_plan_state
            else:
                # Every live non-due state is held below the fence, with the exact
                # declared transition exercised by CatalogWatcher.due().
                assert change.state == CHANGE_DEFERRED


def test_log_mode_recreate_while_flight_is_stopped_keeps_the_boundary(lab):
    """The quarantine and refusal survive a restart between drop and recreation."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    first_watcher = _watcher()
    first_watcher._dirty[old.qualified] = old
    box = lab(catalog=first_watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue(
        first_watcher,
        CatalogChange(
            kind=CHANGE_DROPPED,
            schema="app",
            table="customers",
            detected_lsn=100,
            old_oid=old.oid,
            new_relation=old,
            state=CHANGE_MARKED,
        ),
    )
    box.run([heartbeat(125)])
    path = box.path
    box.lease.release(box.con)
    box.close()

    restarted_watcher = _watcher(
        present={new.qualified: new.oid},
        known={old.qualified: old},
        replicated={old.qualified},
    )
    restarted = Lab(path, catalog=restarted_watcher, drop_mode=DROP_LOG, resume_lsn=125)
    try:
        _queue_recreated(restarted_watcher, new)
        restarted.run([heartbeat(175)])
        _assert_recreated_boundary(restarted, new)
        with pytest.raises(SnapshotObservationError, match="awaiting_snapshot"):
            restarted.run(txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")]))
        assert not restarted.exists(CUSTOMERS)
    finally:
        restarted.close()


def _mutable_source_oids(watcher, current: dict[str, int | None]) -> None:
    watcher.relation_oids = lambda names: {  # type: ignore[method-assign]
        f"{schema}.{table}": current.get(f"{schema}.{table}")
        for schema, table in names
    }


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_queued_intermediate_recreate_is_superseded_by_the_final_generation(
    lab, drop_mode
):
    """A queued B generation cannot fence or apply ahead of a newer C generation."""
    old = _catalog_relation("customers", 16384)
    replacement_b = _catalog_relation("customers", 16385)
    replacement_c = _catalog_relation("customers", 16386)
    orders = _catalog_relation("orders", 16390)
    current = {old.qualified: old.oid}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    _mutable_source_oids(watcher, current)
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    current[old.qualified] = replacement_b.oid
    queued_b = watcher._compare(
        {old.qualified: replacement_b, orders.qualified: orders}, lsn=100
    )[0]
    current[old.qualified] = replacement_c.oid
    added_c = watcher._compare(
        {old.qualified: replacement_c, orders.qualified: orders}, lsn=110
    )

    assert [change.kind for change in added_c] == ["recreated"]
    assert added_c[0].new_oid == replacement_c.oid
    assert queued_b.state == CHANGE_SUPERSEDED
    assert [
        (change.qualified, change.new_oid)
        for change in watcher.pending_destructive()
        if change.qualified == old.qualified
    ] == [(old.qualified, replacement_c.oid)]

    box.run(txn("2", [keyed("2", 1, 300, 4, "c-generation")]))
    _assert_recreated_boundary(box, replacement_c)
    assert not box.exists(CUSTOMERS)
    assert box.applier.fenced_units == 1


def test_stale_recreate_after_source_drop_is_logged_without_quarantining_log_image(lab):
    """A final drop reclassifies A->B instead of deleting the retained A image."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    current = {old.qualified: None}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    _mutable_source_oids(watcher, current)
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, replacement)

    box.run([heartbeat(175)])

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.q(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers'"
    ) == [("dropped", False)]
    assert box.q(
        "SELECT relation_oid FROM _cdc_flight.source_relations "
        "WHERE source_table = 'customers'"
    ) == [(old.oid,)]
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    ) != [(LIFECYCLE_AWAITING,)]


def test_replacement_identity_uses_bounded_plan_and_final_proofs(lab):
    """Admission and plan share the same last-moment proof, not a stale cache."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    current = {old.qualified: old.oid}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    calls: list[set[tuple[str, str]]] = []

    def relation_oids(names):
        calls.append(set(names))
        return {f"{schema}.{table}": current.get(f"{schema}.{table}") for schema, table in names}

    watcher.relation_oids = relation_oids  # type: ignore[method-assign]
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    current[old.qualified] = replacement.oid
    _queue_recreated(watcher, replacement)

    box.run(
        txn("2", [keyed("2", 1, 300, 4, "first")])
        + txn("3", [keyed("3", 1, 310, 5, "second")])
    )

    assert len(calls) == 2
    assert calls[0] == {("app", "customers")}
    assert box.applier.fenced_units == 2


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_source_flip_after_cached_identity_fences_normal_tail(lab, drop_mode):
    """R9-M1(a): A->B after the cached read must not append B rows to A."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    current = {old.qualified: old.oid}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    cached = False

    def read_after_cached(names):
        nonlocal cached
        result = {
            f"{schema}.{table}": current.get(f"{schema}.{table}")
            for schema, table in names
        }
        if not cached:
            cached = True
            current[old.qualified] = replacement.oid
        return result

    watcher.relation_oids = read_after_cached  # type: ignore[method-assign]
    box.run(txn("2", [keyed("2", 1, 300, 4, "new-lifecycle")]))

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.applier.fenced_units == 1


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_short_lived_replacement_that_is_absent_at_read_is_fenced(lab, drop_mode):
    """R9-M1(b): A->B->absent before polling cannot make a tail belong to A."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    lifecycle = [replacement.oid, None]
    current = {old.qualified: lifecycle[-1]}
    watcher = _watcher(
        present={old.qualified: old.oid},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    def read_after_short_lived_replacement(names):
        return {
            f"{schema}.{table}": current.get(f"{schema}.{table}")
            for schema, table in names
        }

    watcher.relation_oids = read_after_short_lived_replacement  # type: ignore[method-assign]
    box.run(txn("2", [keyed("2", 1, 300, 4, "new-lifecycle")]))

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.applier.fenced_units == 1


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_final_proof_cannot_quarantine_after_queued_b_disappears(lab, drop_mode):
    """R9-M1(c): a B->absent flip before apply must retain the A image."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    current = {old.qualified: replacement.oid}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    _queue_recreated(watcher, replacement)

    cached = False

    def read_then_drop(names):
        nonlocal cached
        result = {
            f"{schema}.{table}": current.get(f"{schema}.{table}")
            for schema, table in names
        }
        if not cached:
            cached = True
            current[old.qualified] = None
        return result

    watcher.relation_oids = read_then_drop  # type: ignore[method-assign]
    box.run([heartbeat(175)])

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.q(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers'"
    ) == []


def _with_filenode(relation: object, filenode: int):
    """Attach the R9 stronger generation token to a test relation."""
    object.__setattr__(relation, "relfilenode", filenode)
    return relation


def _with_partition_type(relation: object, type_oid: int):
    """Complete the generation token for a partitioned parent (relfilenode=0)."""
    object.__setattr__(relation, "relfilenode", 0)
    object.__setattr__(relation, "relation_type_oid", type_oid)
    return relation


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m2_same_oid_different_filenode_is_a_recreate(lab, drop_mode):
    """R9-M2: OID reuse with a new relfilenode cannot be current-generation data."""
    old = _with_filenode(_catalog_relation("customers", 16384), 90001)
    replacement = _with_filenode(_catalog_relation("customers", 16384), 90002)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={
            old.qualified: (replacement.oid, replacement.relfilenode),
            orders.qualified: orders.oid,
        },
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    added = watcher._compare(
        {replacement.qualified: replacement, orders.qualified: orders}, lsn=100
    )

    customer_changes = [change for change in added if change.qualified == old.qualified]
    assert [change.kind for change in customer_changes] == ["recreated"]
    box.run(txn("2", [keyed("2", 1, 300, 4, "recreated-lifecycle")]))
    _assert_recreated_boundary(box, replacement)


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m2_same_oid_partitioned_parent_type_is_a_recreate(lab, drop_mode):
    """A partitioned parent has relfilenode=0; its row type still fences OID reuse."""
    old = _with_partition_type(_catalog_relation("customers", 16384), 91001)
    replacement = _with_partition_type(_catalog_relation("customers", 16384), 91002)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={old.qualified: replacement, orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    added = watcher._compare(
        {replacement.qualified: replacement, orders.qualified: orders}, lsn=100
    )

    assert [change.kind for change in added] == ["recreated"]
    box.run(txn("2", [keyed("2", 1, 300, 4, "recreated-partitioned-parent")]))
    _assert_recreated_boundary(box, replacement)


RECREATE_GENERATION_OUTCOMES = catalog_generation.GENERATION_PROOF_STATES
RECREATE_GENERATION_MODES = (DROP_LOG, DROP_REPLICATE)
RECREATE_GENERATION_CELLS = tuple(
    (lifecycle, plan_state, generation, drop_mode, spilled)
    for lifecycle in (LIFECYCLE_COMPLETE,)
    for plan_state in RECREATE_GENERATION_PLAN_STATES
    for generation in RECREATE_GENERATION_OUTCOMES
    for drop_mode in RECREATE_GENERATION_MODES
    for spilled in (False, True)
)


@pytest.mark.parametrize(
    "lifecycle_state, plan_state, generation, drop_mode, spilled",
    RECREATE_GENERATION_CELLS,
)
def test_recreate_generation_supersession_drop_mode_matrix(
    lab, lifecycle_state, plan_state, generation, drop_mode, spilled
):
    """Every feasible due-generation x drop-mode x spill cell realizes its state."""
    old = _catalog_relation("customers", 16384)
    replacement_b = _catalog_relation("customers", 16385)
    replacement_c = _catalog_relation("customers", 16386)
    expected = {
        "current": replacement_b,
        "newer": replacement_c,
        "absent": None,
        "unknown": None,
        "ambiguous": replacement_b,
        "boundary_unproven": replacement_b,
    }[generation]
    if generation == "boundary_unproven":
        # Make the identity itself complete so the first (non-final) plan can be
        # current; only the missing source-WAL coverage should refuse the final plan.
        object.__setattr__(replacement_b, "relfilenode", 90001)
    current = {
        old.qualified: {
            "current": replacement_b.oid,
            "newer": replacement_c.oid,
            "absent": None,
            "unknown": catalog_generation.UNKNOWN,
            "ambiguous": (replacement_b.oid, 90002),
            "boundary_unproven": catalog_generation.GenerationProof(
                identity=catalog_generation.RelationIdentity(
                    replacement_b.oid, replacement_b.relfilenode
                ),
                source_lsn=None,
                legacy=False,
            ),
        }[generation]
    }
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    _mutable_source_oids(watcher, current)
    watcher._dirty[old.qualified] = old
    box = lab(
        catalog=watcher,
        drop_mode=drop_mode,
        **({"unit_spill_events": 1, "unit_spill_bytes": 1} if spilled else {}),
    )
    preload(box)
    _queue_recreated(
        watcher,
        replacement_b,
        state=plan_state,
        detected_lsn=150,
    )

    box.run(txn("2", [keyed("2", 1, 300, 4, "generation-matrix")]))

    if generation in {"current", "newer"}:
        assert expected is not None
        _assert_recreated_boundary(box, expected)
        assert not box.exists(CUSTOMERS)
        assert watcher.pending() == []
    elif generation == "absent" and drop_mode == DROP_LOG:
        assert box.exists(CUSTOMERS)
        # An absent final proof is ambiguous for admission, so the tail is fenced;
        # only the retained A image remains in the drop marker path.
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
        assert box.q(
            "SELECT event, applied FROM _cdc_flight.table_events "
            "WHERE source_table = 'customers'"
        ) == [("dropped", False)]
        assert box.q(
            "SELECT relation_oid FROM _cdc_flight.source_relations "
            "WHERE source_table = 'customers'"
        ) == [(old.oid,)]
        assert watcher.pending() == []
    elif generation == "absent":
        assert not box.exists(CUSTOMERS)
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.source_relations "
            "WHERE source_table = 'customers'"
        ) == [(0,)]
        assert watcher.pending() == []
    else:
        # UNKNOWN, AMBIGUOUS and BOUNDARY_UNPROVEN are explicit fail-closed cells:
        # the unit is fenced and no quarantine/reclassification is allowed. The
        # refused change remains the automatic watcher obligation.
        assert box.exists(CUSTOMERS)
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
        assert box.q(
            "SELECT event FROM _cdc_flight.table_events "
            "WHERE source_table = 'customers'"
        ) == []
        assert watcher.pending() != []
    if spilled:
        assert box.applier.spilled_events >= 1


def test_log_mode_rapid_recreate_drop_sequences_converge_without_image_loss(lab):
    """A deterministic bounded rapid sequence leaves only the final source fact live."""
    rng = random.Random(0xA81)  # fixed seed for a bounded reproducible sequence
    old = _catalog_relation("customers", 16384)
    current = {old.qualified: old.oid}
    watcher = _watcher(
        present=current,
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    _mutable_source_oids(watcher, current)
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    next_oid = 16385
    for step in range(12):
        if rng.randrange(4) == 0:
            current[old.qualified] = None
            observed = {}
        else:
            current[old.qualified] = next_oid
            observed = {
                old.qualified: _catalog_relation("customers", next_oid)
            }
            next_oid += 1
        watcher._compare(observed, lsn=100 + step * 10)

    # Make the terminal source fact a drop so the property includes DROP_LOG's
    # no-data-loss branch after several queued replacement generations.
    current[old.qualified] = None
    watcher._compare({}, lsn=300)
    box.run([heartbeat(400)])

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.q(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers' ORDER BY commit_id, seq"
    )[-1:] == [("dropped", False)]
    assert watcher.pending() == []
    assert watcher.superseded >= 1
