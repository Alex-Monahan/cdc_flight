"""§7.3 Round A — catalog-owned partition topology observation.

These tests stop at the observation boundary. They prove the edge identity and
fence facts, but deliberately do not claim that any destination row was repaired.
"""

from __future__ import annotations

from dataclasses import replace

import duckdb
import pytest

from cdc_flight import catalog_support, destination
from cdc_flight.catalog import (
    CHANGE_PARTITION_ATTACHED,
    CHANGE_PARTITION_DETACHED,
    CHANGE_PARTITION_DROPPED,
    CatalogWatcher,
    PartitionEdge,
    PartitionTopologyObservation,
    SourceRelation,
)
from cdc_flight.catalog_apply import CatalogCoordinator, CatalogPlan


def _relation(
    table: str,
    oid: int,
    filenode: int,
    type_oid: int,
    *,
    is_partition: bool = False,
) -> SourceRelation:
    return SourceRelation(
        schema="app",
        table=table,
        oid=oid,
        relfilenode=filenode,
        relation_type_oid=type_oid,
        published=True,
        replica_identity="d",
        is_partition=is_partition,
    )


def _facts(*, edge_lsn: int = 100, epoch: int = 1) -> tuple[SourceRelation, SourceRelation, PartitionEdge]:
    parent = _relation("events", 1001, 0, 1002)
    child = _relation("events_2026_01", 2001, 2002, 2003, is_partition=True)
    edge = PartitionEdge(
        parent_schema="app",
        parent_table="events",
        parent_oid=1001,
        parent_relfilenode=0,
        parent_relation_type_oid=1002,
        child_schema="app",
        child_table="events_2026_01",
        child_oid=2001,
        child_relfilenode=2002,
        child_relation_type_oid=2003,
        # This string is intentionally PostgreSQL-shaped input. The production
        # query obtains it only through pg_get_expr; Python never renders it.
        bound="FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
        attachment_state="attached",
        attachment_epoch="xmin-attach-41",
        attachment_sequence=1,
        parent_published=True,
        child_published=True,
        publication_all_tables=False,
        parent_publication_member=True,
        child_publication_member=False,
        observed_lsn=edge_lsn,
        observation_epoch=epoch,
    )
    return parent, child, edge


def _watcher() -> CatalogWatcher:
    return CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        include={"app.events"},
        poll_seconds=0,
    )


def _observation(edges, *, lsn: int, epoch: int, complete: bool = True, reason=None):
    return PartitionTopologyObservation(
        edges=tuple(edges),
        complete=complete,
        detection_lsn=lsn,
        observation_epoch=epoch,
        reason=reason,
    )


@pytest.mark.parametrize(
    ("transition", "expected_kind"),
    (
        ("attach", CHANGE_PARTITION_ATTACHED),
        ("detach", CHANGE_PARTITION_DETACHED),
        ("drop", CHANGE_PARTITION_DROPPED),
    ),
)
def test_two_complete_observations_produce_exact_generation_aware_edge_facts(
    transition, expected_kind
):
    parent, child, edge = _facts()
    w = _watcher()
    all_relations = {parent.qualified: parent, child.qualified: child}

    if transition == "attach":
        assert w._compare_partitions(
            _observation((), lsn=100, epoch=1), {parent.qualified: parent}
        ) == []
        first = {parent.qualified: parent, child.qualified: child}
        assert w._compare_partitions(_observation((edge,), lsn=200, epoch=2), first) == []
        changes = w._compare_partitions(
            _observation((replace(edge, observed_lsn=201, observation_epoch=3),), lsn=201, epoch=3),
            first,
        )
    else:
        assert w._compare_partitions(_observation((edge,), lsn=100, epoch=1), all_relations) == []
        current = all_relations if transition == "detach" else {parent.qualified: parent}
        assert w._compare_partitions(_observation((), lsn=200, epoch=2), current) == []
        changes = w._compare_partitions(_observation((), lsn=201, epoch=3), current)

    assert len(changes) == 1
    change = changes[0]
    assert change.kind == expected_kind
    assert change.detected_lsn == 201
    assert change.observation_epoch == 3
    assert change.old_identity is not None or change.new_identity is not None
    observed_edge = change.new_partition_edge or change.old_partition_edge
    assert observed_edge is not None
    assert observed_edge.parent_generation == (1001, 0, 1002)
    assert observed_edge.child_generation == (2001, 2002, 2003)
    assert observed_edge.bound == "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
    assert observed_edge.attachment_state == "attached"
    assert observed_edge.attachment_epoch == "xmin-attach-41"
    assert observed_edge.attachment_sequence == 1
    assert observed_edge.publication_facts == {
        "parent_published": True,
        "child_published": True,
        "publication_all_tables": False,
        "parent_publication_member": True,
        "child_publication_member": False,
    }
    if transition == "attach":
        assert change.old_partition_edge is None
        assert change.new_partition_edge is not None
        assert change.new_identity.oid == 2001
    else:
        assert change.old_partition_edge is not None
        assert change.new_partition_edge is None
        assert change.old_identity.oid == 2001
    assert not change.fenced


def test_partition_bound_uses_postgres_output_and_query_has_no_python_renderer():
    assert "pg_get_expr(child.relpartbound, child.oid)" in catalog_support.PARTITION_SQL
    assert "AS partition_bound" in catalog_support.PARTITION_SQL
    assert "FOR VALUES FROM" not in catalog_support.PARTITION_SQL


def test_incomplete_or_empty_projection_cannot_confirm_a_missing_child():
    parent, child, edge = _facts()
    w = _watcher()
    relations = {parent.qualified: parent, child.qualified: child}
    assert w._compare_partitions(_observation((edge,), lsn=10, epoch=1), relations) == []

    assert w._compare_partitions(
        _observation((), lsn=20, epoch=2, complete=False, reason="permission denied"),
        relations,
    ) == []
    assert edge.edge_key in w._snapshot_partitions
    assert w._compare_partitions(_observation((), lsn=21, epoch=3), relations) == []
    change = w._compare_partitions(_observation((), lsn=22, epoch=4), relations)
    assert [item.kind for item in change] == [CHANGE_PARTITION_DETACHED]
    assert change[0].detected_lsn == 22


def test_oid_reuse_is_fail_closed_and_does_not_become_drop_or_detach():
    parent, child, edge = _facts()
    reused = _relation("events_2026_01", 3001, 3002, 3003, is_partition=False)
    w = _watcher()
    assert w._compare_partitions(
        _observation((edge,), lsn=10, epoch=1),
        {parent.qualified: parent, child.qualified: child},
    ) == []
    current = {parent.qualified: parent, reused.qualified: reused}
    for lsn, epoch in ((20, 2), (21, 3), (22, 4)):
        assert w._compare_partitions(_observation((), lsn=lsn, epoch=epoch), current) == []
    assert edge.edge_key in w._snapshot_partitions
    assert not w.pending()
    assert "generation reuse" in (w._partition_observation_reason or "")


def test_in_progress_detach_is_not_a_lifecycle_observation():
    parent, child, edge = _facts()
    w = _watcher()
    relations = {parent.qualified: parent, child.qualified: child}
    assert w._compare_partitions(_observation((edge,), lsn=10, epoch=1), relations) == []
    pending_edge = replace(edge, attachment_state="detach_pending")
    assert w._compare_partitions(
        _observation((pending_edge,), lsn=20, epoch=2, complete=False), relations
    ) == []
    assert w._compare_partitions(_observation((), lsn=21, epoch=3), relations) == []
    change = w._compare_partitions(_observation((), lsn=22, epoch=4), relations)
    assert [item.kind for item in change] == [CHANGE_PARTITION_DETACHED]


def test_edge_transition_is_superseded_when_the_positive_edge_returns():
    parent, child, edge = _facts()
    w = _watcher()
    relations = {parent.qualified: parent, child.qualified: child}
    w._compare_partitions(_observation((edge,), lsn=10, epoch=1), relations)
    w._compare_partitions(_observation((), lsn=20, epoch=2), relations)
    changes = w._compare_partitions(_observation((), lsn=21, epoch=3), relations)
    assert len(changes) == 1
    detached = changes[0]
    assert detached.state == "pending"

    assert w._compare_partitions(_observation((edge,), lsn=30, epoch=4), relations) == []
    assert detached.state == "superseded"
    assert detached not in w.pending()


def test_partition_fact_waits_for_the_source_wal_fence_and_uses_the_existing_marker():
    parent, child, edge = _facts()
    w = _watcher()
    relations = {parent.qualified: parent, child.qualified: child}
    w._compare_partitions(_observation((edge,), lsn=10, epoch=1), relations)
    w._compare_partitions(_observation((), lsn=20, epoch=2), relations)
    change = w._compare_partitions(_observation((), lsn=21, epoch=3), relations)[0]

    class SourceMarkerConnection:
        def execute(self, _sql, _params=None):
            class Cursor:
                def fetchone(self):
                    return (900,)

            return Cursor()

    w._emit_marker(SourceMarkerConnection(), [change])
    assert change.fenced
    assert w.due(change.detected_lsn - 1) == []
    assert change.state == "deferred"
    assert w.due(change.detected_lsn) == [change]
    assert change.state == "due"


def test_catalog_apply_persists_observation_not_destination_repair():
    parent, child, edge = _facts()
    w = _watcher()
    relations = {parent.qualified: parent, child.qualified: child}
    w._compare_partitions(_observation((edge,), lsn=10, epoch=1), relations)
    w._compare_partitions(_observation((), lsn=20, epoch=2), relations)
    change = w._compare_partitions(_observation((), lsn=21, epoch=3), relations)[0]
    con = duckdb.connect(":memory:")
    try:
        destination.ensure_control_schema(con)
        coordinator = CatalogCoordinator(
            catalog=w,
            pipeline="p73_unit",
            topic_prefix="cdcflight",
            drop_mode="log",
            registry_of=lambda: None,
        )
        plan = CatalogPlan(
            partition_events=(change,),
            partition_edges=(),
            partition_epoch=3,
            durable_lsn=21,
        )
        con.execute("BEGIN TRANSACTION")
        coordinator.apply(con, plan, {"tables": set()}, commit_id=7)
        con.execute("COMMIT")
        fact = con.execute(
            "SELECT transition, parent_oid, parent_relfilenode, "
            "parent_relation_type_oid, child_oid, child_relfilenode, "
            "child_relation_type_oid, partition_bound, attachment_epoch, "
            "detection_lsn, durable_lsn, state "
            "FROM _cdc_flight.partition_events WHERE pipeline = ?",
            ["p73_unit"],
        ).fetchall()
        assert fact == [
            (
                CHANGE_PARTITION_DETACHED,
                1001,
                0,
                1002,
                2001,
                2002,
                2003,
                "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
                "xmin-attach-41",
                21,
                21,
                "applied",
            )
        ]
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.partition_edges WHERE pipeline = ?",
            ["p73_unit"],
        ).fetchone()[0] == 0
        assert con.execute(
            "SELECT state FROM _cdc_flight.partition_observation_state "
            "WHERE pipeline = ?",
            ["p73_unit"],
        ).fetchone() == ("complete",)
    finally:
        con.close()
