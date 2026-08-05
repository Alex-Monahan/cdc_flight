"""Rubric 2.3 / R6 — structural replacement admission and lifecycle matrix."""

from __future__ import annotations

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

from cdc_flight import table_lifecycle
from cdc_flight.catalog import CHANGE_DROPPED, CatalogChange
from cdc_flight.config import DROP_LOG
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
RECREATE_ADMISSION_CELLS = tuple(
    (lifecycle, catalog_plan_state, spilled)
    for lifecycle in RECREATE_IMAGE_LIFECYCLE_STATES
    for catalog_plan_state in RECREATE_PLAN_STATES
    for spilled in (False, True)
)


def test_recreate_matrix_documents_non_image_lifecycle_ownership():
    assert {"absent", "none", "in_progress"} == RECREATE_OUT_OF_SCOPE_LIFECYCLE_STATES
    assert CHANGE_DUE not in RECREATE_NO_PLAN_CATALOG_STATES


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
