"""Rubric 2.3 / R6 — structural replacement admission and lifecycle matrix."""

from __future__ import annotations

import random

import pytest
from recreate_admission_helpers import (
    CUSTOMERS,
    ORDERS,
    _assert_recreated_boundary,
    _catalog_relation,
    _queue,
    _queue_recreated,
    _watcher,
    preload,
    rows,
    txn,
)
from support.applier_lab import Lab, data, heartbeat, keyed, truncate

from cdc_flight.catalog import CHANGE_DROPPED, CatalogChange, SourceRelation
from cdc_flight.config import DROP_LOG, DROP_REPLICATE
from cdc_flight.machines import (
    CATALOG_CHANGE,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_SUPERSEDED,
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


def test_a_recreated_table_quarantines_the_destination_and_says_why(lab):
    """A same-name replacement keeps the retained image and owes a snapshot."""
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
    assert box.exists(CUSTOMERS)
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
    assert box.exists(CUSTOMERS)


def test_log_mode_recreate_same_group_is_quarantined_after_group_dml(lab):
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={new.qualified: new.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, new)
    box.run(txn("3", [keyed("3", 1, 300, 4, "new-lifecycle")]))
    _assert_recreated_boundary(box, new)
    assert box.applier.fenced_units == 0
    assert box.q(
        "SELECT event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(1,)]


def test_log_mode_recreate_spilled_same_group_is_quarantined_after_group_dml(lab):
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
    assert not unit.fenced
    assert box.applier.fenced_spilled_events == 0


def test_log_mode_recreate_pre_detection_tail_converges_after_catalog_poll(lab):
    """The stale interval is honest: the image is repaired after observation."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    box.run(txn("2", [keyed("2", 1, 130, 4, "new-lifecycle")]))
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    assert box.applier.fenced_units == 0

    _queue_recreated(watcher, new, detected_lsn=150)
    box.run([heartbeat(151)])
    _assert_recreated_boundary(box, new)


def test_log_mode_pre_detection_spill_group_converges_as_data(lab):
    """Spill does not change the stale-image convergence contract."""
    old = _catalog_relation("customers", 16384)
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

    box.feed(txn("2", [keyed("2", 1, 130, 4, "new-lifecycle")]))
    unit = box.applier.group.units[-1]
    assert unit.spilled_events >= 1
    box.commit()

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    assert not unit.fenced
    assert box.applier.data_commit_groups == data_groups_before + 1
    assert box.applier.fenced_spilled_events == 0
    assert box.q(
        "SELECT fenced_units, event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(0, 1)]


def test_log_mode_recreate_same_group_quarantine_wins_over_low_lsn_ordering(lab):
    """A due plan removes transient rows after the complete group is applied."""
    old = _catalog_relation("customers", 16384)
    new = _catalog_relation("customers", 16385)
    watcher = _watcher(present={old.qualified: old.oid})
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

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
    assert box.exists(CUSTOMERS)
    assert rows(box, ORDERS) == [(7,), (8,), (9,)]
    assert box.applier.fenced_units == 0
    assert box.q(
        "SELECT fenced_units, event_count FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(0, 2)]


# The convergence matrix is made from the real watcher and destination machines.
# It intentionally has no synthetic proof dimension: the source observer is either
# late, has queued durable work, or has queued it immediately before apply.  The
# same cells are exercised in memory and after assembler spill for both policies.
RECREATE_CONVERGENCE_CELLS = tuple(
    (drop_mode, timing, spilled)
    for drop_mode in (DROP_LOG, DROP_REPLICATE)
    for timing in ("before_detection", "queued", "at_apply")
    for spilled in (False, True)
)


def test_recreate_matrix_documents_non_image_lifecycle_ownership():
    assert {"absent", "none", "in_progress"} == (
        TABLE_LIFECYCLE.reachable_states()
        - {LIFECYCLE_COMPLETE, LIFECYCLE_AWAITING}
    )
    assert CATALOG_CHANGE.reachable_states() >= {CHANGE_MARKED, CHANGE_DUE}


@pytest.mark.parametrize("drop_mode, timing, spilled", RECREATE_CONVERGENCE_CELLS)
def test_recreate_convergence_matrix_uses_real_watcher_states(
    lab, drop_mode, timing, spilled
):
    old = _with_filenode(_catalog_relation("customers", 16384), 90001)
    replacement = _with_filenode(_catalog_relation("customers", 16385), 90002)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={replacement.qualified: replacement, orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(
        catalog=watcher,
        drop_mode=drop_mode,
        **({"unit_spill_events": 1, "unit_spill_bytes": 1} if spilled else {}),
    )
    preload(box)
    stream = txn("2", [keyed("2", 1, 300, 4, "new-lifecycle")])

    if timing == "before_detection":
        box.run(stream)
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
        changed = watcher._compare(
            {replacement.qualified: replacement, orders.qualified: orders}, lsn=302
        )
        assert any(
            change.qualified == replacement.qualified and change.kind == "recreated"
            for change in changed
        )
        box.run([heartbeat(400)])
    elif timing == "queued":
        _queue_recreated(watcher, replacement, detected_lsn=150)
        box.run(stream)
    else:
        box.feed(stream)
        _queue_recreated(watcher, replacement, detected_lsn=150)
        box.commit()

    _assert_recreated_boundary(box, replacement)
    assert watcher.pending() == []
    assert box.applier.resume_point.last_lsn >= 301


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
        assert restarted.exists(CUSTOMERS)
    finally:
        restarted.close()


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_queued_intermediate_recreate_is_superseded_by_the_final_generation(
    lab, drop_mode
):
    """A queued B generation cannot fence or apply ahead of a newer C generation."""
    old = _catalog_relation("customers", 16384)
    replacement_b = _catalog_relation("customers", 16385)
    replacement_c = _catalog_relation("customers", 16386)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    queued_b = watcher._compare(
        {old.qualified: replacement_b, orders.qualified: orders}, lsn=100
    )[0]
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
    assert box.exists(CUSTOMERS)
    assert box.applier.fenced_units == 0


def test_stale_recreate_after_source_drop_is_logged_without_quarantining_log_image(lab):
    """A later absent observation cannot cancel B before its quarantine is durable."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, replacement)
    # The source is now absent before apply. This is not yet the final source fact for
    # DROP_LOG/DROP_REPLICATE; the recreate obligation remains until re-snapshot.
    changed = watcher._compare({}, lsn=160)
    assert [
        change.kind for change in changed if change.qualified == old.qualified
    ] == []
    assert [
        change.kind
        for change in watcher.pending_destructive()
        if change.qualified == old.qualified
    ] == ["recreated"]

    box.run([heartbeat(175)])

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.q(
        "SELECT event FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers'"
    ) == [("recreated",)]
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    ) == [(LIFECYCLE_AWAITING,)]


def test_stale_recreate_after_final_observation_preserves_drop_log_image(lab):
    """A source disappearance after the last poll cannot erase retained history."""
    old = _with_filenode(_catalog_relation("customers", 16384), 91001)
    replacement = _with_filenode(_catalog_relation("customers", 16385), 91002)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    added = watcher._compare(
        {replacement.qualified: replacement}, lsn=150
    )
    assert [
        change.kind for change in added if change.qualified == replacement.qualified
    ] == ["recreated"]
    # This is the review's interleaving: B is the final observation, then the source
    # disappears before the stale B plan is applied. There is deliberately no poll of
    # the absent source between queueing and applying.
    box.run([*txn("2", [keyed("2", 1, 130, 4, "replacement")]), heartbeat(175)])

    _assert_recreated_boundary(box, replacement)
    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_drop_seen_while_quarantine_is_owed_waits_for_resnapshot_policy(lab, drop_mode):
    """A post-quarantine absence cannot destroy the retained image on a stale plan."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    watcher._compare({old.qualified: replacement}, lsn=150)
    box.run([heartbeat(175)])
    _assert_recreated_boundary(box, replacement)

    # The replacement is now quarantined and owed. A later absence must be deferred
    # until the re-snapshot reads the final source fact, even in DROP_REPLICATE mode.
    watcher._compare({}, lsn=180)
    box.run([heartbeat(200)])

    assert box.exists(CUSTOMERS)
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    ) == [(LIFECYCLE_AWAITING,)]
    assert [
        (change.kind, change.state)
        for change in watcher.pending_destructive()
        if change.qualified == old.qualified
    ] == [("dropped", CHANGE_DEFERRED)]


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_pending_type_recreate_survives_same_lifecycle_rewrite(lab, drop_mode):
    """A relfilenode-only rewrite updates the token without canceling the rebuild."""
    old = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=91001,
        relation_type_oid=92001,
    )
    replacement_b = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=91002,
        relation_type_oid=92002,
    )
    replacement_c = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=91003,
        relation_type_oid=92002,
    )
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    queued = watcher._compare({old.qualified: replacement_b}, lsn=150)[0]
    assert queued.kind == "recreated"
    # B's WAL fence is still closed, so its row is admitted to the retained image.
    box.run(txn("2", [keyed("2", 1, 120, 4, "b-generation")]))

    watcher._compare({old.qualified: replacement_c}, lsn=160)
    assert queued.state != CHANGE_SUPERSEDED
    assert queued.new_identity == (
        queued.new_identity.__class__(16384, 91003, 92002)
    )

    box.run(
        txn(
            "3",
            [truncate("3", 1, 200), keyed("3", 2, 201, 5, "c-generation")],
        )
    )
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    ) == [(LIFECYCLE_AWAITING,)]
    assert box.q(
        "SELECT relation_oid, relation_filenode, relation_type_oid "
        "FROM _cdc_flight.source_relations WHERE source_table = 'customers'"
    ) == [(16384, 91003, 92002)]


def test_settle_absorbs_superseded_due_change_and_keeps_newer_pending(lab):
    """A watcher poll between due and settle cannot fail after acknowledgement."""
    old = _catalog_relation("customers", 16384)
    replacement_b = _catalog_relation("customers", 16385)
    replacement_c = _catalog_relation("customers", 16386)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)

    planned_b = watcher._compare({old.qualified: replacement_b}, lsn=100)[0]
    plan = box.applier.catalog_coordinator.plan(100)
    assert plan.actions and plan.actions[0].change is planned_b
    watcher._compare({old.qualified: replacement_c}, lsn=110)
    assert planned_b.state == CHANGE_SUPERSEDED

    box.applier.catalog_coordinator.settle(plan, set())
    assert planned_b.state == "applied"
    assert [
        (change.qualified, change.new_oid)
        for change in watcher.pending_destructive()
    ] == [(old.qualified, replacement_c.oid)]


def test_replacement_uses_durable_watcher_state_without_commit_source_reads(lab):
    """Catalog state is enough; no bounded plan/final proof is needed."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    assert not hasattr(watcher, "relation_oids")
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, replacement)

    box.run(
        txn("2", [keyed("2", 1, 300, 4, "first")])
        + txn("3", [keyed("3", 1, 310, 5, "second")])
    )

    _assert_recreated_boundary(box, replacement)
    assert box.applier.fenced_units == 0


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_source_flip_after_cached_identity_converges(lab, drop_mode):
    """R9-M1(a): A->B after an old read is repaired by the next observation."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={replacement.qualified: replacement, orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    box.run(txn("2", [keyed("2", 1, 300, 4, "new-lifecycle")]))
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]

    changed = watcher._compare(
        {replacement.qualified: replacement, orders.qualified: orders}, lsn=320
    )
    assert any(
        change.qualified == old.qualified and change.kind == "recreated"
        for change in changed
    )
    box.run([heartbeat(400)])

    _assert_recreated_boundary(box, replacement)
    assert box.applier.resume_point.last_lsn == 400


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_short_lived_replacement_that_is_absent_at_read_converges(
    lab, drop_mode
):
    """R9-M1(b): an unseen short-lived B is handled by the observed final drop."""
    old = _catalog_relation("customers", 16384)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={old.qualified: old, orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    box.run(txn("2", [keyed("2", 1, 300, 4, "new-lifecycle")]))
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    changed = watcher._compare({orders.qualified: orders}, lsn=320)
    assert any(
        change.qualified == old.qualified and change.kind == "dropped"
        for change in changed
    )
    box.run([heartbeat(400)])

    if drop_mode == DROP_LOG:
        assert box.exists(CUSTOMERS)
        assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
        assert box.q(
            "SELECT event, applied FROM _cdc_flight.table_events "
            "WHERE source_table = 'customers'"
        )[-1:] == [("dropped", False)]
    else:
        assert not box.exists(CUSTOMERS)


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_r9_m1_queued_b_disappears_and_policy_decides_the_retained_image(
    lab, drop_mode
):
    """R9-M1(c): a missing B keeps the recreate obligation until re-snapshot."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)
    _queue_recreated(watcher, replacement)
    changed = watcher._compare({orders.qualified: orders}, lsn=160)
    assert changed == []
    assert [change.kind for change in watcher.pending_destructive()] == ["recreated"]
    box.run([heartbeat(175)])

    _assert_recreated_boundary(box, replacement)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]


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


@pytest.mark.parametrize("point", ["begin", "mid_apply", "pre_commit"])
def test_recreate_crash_rolls_back_the_boundary_and_retries(lab, monkeypatch, point):
    """Crash is a whole MD transaction state, not a proof state."""
    old = _catalog_relation("customers", 16384)
    replacement = _catalog_relation("customers", 16385)
    watcher = _watcher(
        present={replacement.qualified: replacement},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)
    _queue_recreated(watcher, replacement, detected_lsn=150)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"{point}:{nth}:raise")
    from cdc_flight import faults

    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", [keyed("2", 1, 300, 4, "replacement")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert watcher.pending() != []

    box.run([heartbeat(400)])
    _assert_recreated_boundary(box, replacement)
    assert watcher.pending() == []


def test_log_mode_rapid_recreate_drop_sequences_converge_without_image_loss(lab):
    """A deterministic bounded rapid sequence leaves only the final source fact live."""
    rng = random.Random(0xA81)  # fixed seed for a bounded reproducible sequence
    old = _catalog_relation("customers", 16384)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    next_oid = 16385
    for step in range(12):
        if rng.randrange(4) == 0:
            observed = {}
        else:
            observed = {
                old.qualified: _catalog_relation("customers", next_oid)
            }
            next_oid += 1
        watcher._compare(observed, lsn=100 + step * 10)

    # Make the terminal source fact a drop so the property includes DROP_LOG's
    # no-data-loss branch after several queued replacement generations.
    watcher._compare({}, lsn=300)
    box.run([heartbeat(400)])

    assert box.exists(CUSTOMERS)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,)]
    assert box.q(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers' ORDER BY commit_id, seq"
    )[-1:] == [("recreated", True)]
    assert box.q(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    ) == [(LIFECYCLE_AWAITING,)]
    assert watcher.pending() == []
    assert watcher.superseded >= 1


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
def test_seeded_convergence_does_not_poll_intermediate_generations_before_apply(
    lab, drop_mode
):
    """The final observed replacement is applied without a stale absence poll."""
    rng = random.Random(0xB11)
    old = _with_filenode(_catalog_relation("customers", 16384), 91001)
    generations = [
        _with_filenode(_catalog_relation("customers", 16385 + index), 91002 + index)
        for index in range(5)
    ]
    final = generations[rng.randrange(len(generations))]
    watcher = _watcher(
        present={final.qualified: final},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=drop_mode)
    preload(box)

    added = watcher._compare({final.qualified: final}, lsn=150)
    assert [
        change.kind for change in added if change.qualified == final.qualified
    ] == ["recreated"]
    # The other seeded generations are deliberately not passed to _compare. They are
    # the review's unpolled intermediate states between the last observation and apply.
    box.run([*txn("2", [keyed("2", 1, 130, 4, "replacement")]), heartbeat(175)])

    _assert_recreated_boundary(box, final)
    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    assert watcher.pending() == []


# --------------------------------------------------------------------------- #
# R10 lease deletion contract (these tests intentionally fail on b21ec30)
# --------------------------------------------------------------------------- #
def test_r10_m1_source_proof_error_cannot_acknowledge_and_lose_a_source_txn(lab):
    """A source-proof outage is no longer an admission state.

    R10-M1 reproduced the old lease path: the source read became UNKNOWN, the unit
    was fenced, but the resume point still advanced.  The convergence design has no
    source read in the commit protocol, so an ordinary unit remains durable.
    """
    old = _catalog_relation("customers", 16384)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    assert not hasattr(watcher, "relation_oids")
    box.run(txn("2", [keyed("2", 1, 300, 4, "ordinary-source-txn")]))

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    assert box.applier.fenced_units == 0
    assert box.applier.resume_point.last_lsn == 301
    assert box.q(
        "SELECT event_count, fenced_units FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    ) == [(1, 0)]


def test_r10_m2_dead_source_backend_cannot_hold_a_trusted_commit_lease(lab):
    """The MD commit has no source backend whose released lock can reopen a TOCTOU."""
    old = _catalog_relation("customers", 16384)
    watcher = _watcher(
        present={old.qualified: old},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_REPLICATE)
    preload(box)

    assert not hasattr(watcher, "generation_proof_lease")
    box.run(txn("2", [keyed("2", 1, 300, 4, "ordinary-source-txn")]))

    assert rows(box, CUSTOMERS) == [(1,), (2,), (3,), (4,)]
    assert box.applier.fenced_units == 0


def test_r10_m3_relation_appearing_after_an_absent_observation_converges(lab):
    """A relation appearing after an absent read is handled by watcher state, not a
    partial initial/final lock set.
    """
    old = _with_filenode(_catalog_relation("customers", 16384), 90001)
    replacement = _with_filenode(_catalog_relation("customers", 16385), 90002)
    watcher = _watcher(
        present={replacement.qualified: replacement},
        known={old.qualified: old},
        replicated={old.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(catalog=watcher, drop_mode=DROP_LOG)
    preload(box)

    # Poll 1 has no relation; poll 2 sees the replacement before the queued action is
    # applied.  The watcher supersedes the drop and queues the complete recreate.
    first = watcher._compare({}, lsn=100)
    assert [change.kind for change in first if change.qualified == old.qualified] == [
        "dropped"
    ]
    second = watcher._compare(
        {replacement.qualified: replacement}, lsn=110
    )
    assert [change.kind for change in second if change.qualified == old.qualified] == [
        "recreated"
    ]

    assert not hasattr(watcher, "relation_oids")
    box.run(txn("2", [keyed("2", 1, 300, 4, "replacement")]))

    _assert_recreated_boundary(box, replacement)
    assert watcher.pending() == []
    assert box.applier.resume_point.last_lsn == 301


@pytest.mark.parametrize("drop_mode", [DROP_LOG, DROP_REPLICATE])
@pytest.mark.parametrize("spilled", [False, True])
def test_r10_m4_truncate_cannot_authorize_a_same_oid_generation(
    lab, drop_mode, spilled
):
    """TRUNCATE evidence is not a generation token, in memory or after spill."""
    old = _with_filenode(_catalog_relation("customers", 16384), 90001)
    replacement = _with_filenode(_catalog_relation("customers", 16384), 90002)
    orders = _catalog_relation("orders", 16390)
    watcher = _watcher(
        present={replacement.qualified: replacement, orders.qualified: orders},
        known={old.qualified: old, orders.qualified: orders},
        replicated={old.qualified, orders.qualified},
        confirm_polls=1,
    )
    watcher._dirty[old.qualified] = old
    box = lab(
        catalog=watcher,
        drop_mode=drop_mode,
        **({"unit_spill_events": 1, "unit_spill_bytes": 1} if spilled else {}),
    )
    preload(box)
    added = watcher._compare(
        {replacement.qualified: replacement, orders.qualified: orders}, lsn=100
    )
    assert [change.kind for change in added if change.qualified == old.qualified] == [
        "recreated"
    ]

    box.run(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                keyed("2", 2, 201, 4, "new-generation"),
            ],
        )
    )

    _assert_recreated_boundary(box, replacement)
    assert box.applier.fenced_units == 0
    assert box.applier.resume_point.last_lsn == 202
