"""Durable storage for catalog-owned partition topology facts.

This module has no source connection and no polling loop.  ``CatalogWatcher`` remains
the only source observer; ``CatalogCoordinator`` calls these writers while the
applier's already-open destination transaction is active.  Keeping the storage seam
separate makes it difficult to accidentally turn the catalog poll into a second
destination or source-signal writer.
"""

from __future__ import annotations

import json

from .catalog_state import CatalogChange, PartitionEdge
from .config import resolve_control_schema
from .naming import control_table

_EDGE_COLUMNS = (
    "parent_schema, parent_table, parent_oid, parent_relfilenode, "
    "parent_relation_type_oid, child_schema, child_table, child_oid, "
    "child_relfilenode, child_relation_type_oid, partition_bound, "
    "attachment_state, attachment_epoch, attachment_sequence, "
    "parent_published, child_published, publication_all_tables, "
    "parent_publication_member, child_publication_member, observed_lsn, "
    "observation_epoch"
)


def _table(control_schema: str | None, name: str) -> str:
    return control_table(resolve_control_schema(control_schema), name)


def _edge_values(edge: PartitionEdge) -> list:
    return [
        edge.parent_schema,
        edge.parent_table,
        edge.parent_oid,
        edge.parent_relfilenode,
        edge.parent_relation_type_oid,
        edge.child_schema,
        edge.child_table,
        edge.child_oid,
        edge.child_relfilenode,
        edge.child_relation_type_oid,
        edge.bound,
        edge.attachment_state,
        edge.attachment_epoch,
        edge.attachment_sequence,
        edge.parent_published,
        edge.child_published,
        edge.publication_all_tables,
        edge.parent_publication_member,
        edge.child_publication_member,
        edge.observed_lsn,
        edge.observation_epoch,
    ]


def write_partition_snapshot(
    con,
    *,
    pipeline: str,
    edges: tuple[PartitionEdge, ...] | list[PartitionEdge],
    observed_lsn: int = 0,
    observation_epoch: int = 0,
    control_schema: str | None = None,
) -> None:
    """Replace one stable snapshot and its complete-observation marker atomically."""
    table = _table(control_schema, "partition_edges")
    con.execute(f"DELETE FROM {table} WHERE pipeline = ?", [pipeline])
    values = [[pipeline, _edge_key(edge), *_edge_values(edge)] for edge in edges]
    if values:
        placeholders = ",".join("?" for _ in range(2 + len(_edge_values(edges[0]))))
        con.executemany(
            f"INSERT INTO {table} (pipeline, edge_key, {_EDGE_COLUMNS}) "
            f"VALUES ({placeholders})",
            values,
        )
    state_table = _table(control_schema, "partition_observation_state")
    from .destination import now

    con.execute(
        f"INSERT INTO {state_table} "
        "(pipeline, observed_lsn, observation_epoch, state, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (pipeline) DO UPDATE SET "
        "observed_lsn = excluded.observed_lsn, "
        "observation_epoch = excluded.observation_epoch, "
        "state = excluded.state, updated_at = excluded.updated_at",
        [pipeline, int(observed_lsn), int(observation_epoch), "complete", now()],
    )


def _edge_key(edge: PartitionEdge) -> str:
    return json.dumps(edge.edge_key, separators=(",", ":"), default=str)


def _event_edge(change: CatalogChange) -> PartitionEdge | None:
    return change.new_partition_edge or change.old_partition_edge


def write_partition_event(
    con,
    *,
    pipeline: str,
    change: CatalogChange,
    commit_id: int,
    durable_lsn: int,
    control_schema: str | None = None,
) -> None:
    """Record one confirmed transition atomically with its destination commit."""
    event_id = change.partition_event_id
    edge = _event_edge(change)
    if event_id is None or edge is None:
        raise ValueError("partition event requires a transition identity and edge")
    table = _table(control_schema, "partition_events")
    from .destination import now

    columns = (
        "pipeline, event_id, transition, edge_key, "
        + _EDGE_COLUMNS
        + ", detection_lsn, durable_lsn, commit_id, state, recorded_at"
    )
    values = [
        pipeline,
        event_id,
        change.kind,
        _edge_key(edge),
        *_edge_values(edge),
        change.detected_lsn,
        durable_lsn,
        commit_id,
        "applied",
        now(),
    ]
    placeholders = ",".join("?" for _ in values)
    con.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        "ON CONFLICT (pipeline, event_id) DO NOTHING",
        values,
    )


def _edge_from_row(row) -> PartitionEdge:
    return PartitionEdge(
        parent_schema=str(row[0]),
        parent_table=str(row[1]),
        parent_oid=(int(row[2]) if row[2] is not None else None),
        parent_relfilenode=(int(row[3]) if row[3] is not None else None),
        parent_relation_type_oid=(int(row[4]) if row[4] is not None else None),
        child_schema=str(row[5]),
        child_table=str(row[6]),
        child_oid=(int(row[7]) if row[7] is not None else None),
        child_relfilenode=(int(row[8]) if row[8] is not None else None),
        child_relation_type_oid=(int(row[9]) if row[9] is not None else None),
        bound=row[10],
        attachment_state=row[11],
        attachment_epoch=row[12],
        attachment_sequence=(int(row[13]) if row[13] is not None else None),
        parent_published=bool(row[14]),
        child_published=bool(row[15]),
        publication_all_tables=bool(row[16]),
        parent_publication_member=bool(row[17]),
        child_publication_member=bool(row[18]),
        observed_lsn=int(row[19] or 0),
        observation_epoch=int(row[20] or 0),
    )


def read_partition_edges(
    con, pipeline: str, *, control_schema: str | None = None
) -> dict[tuple, PartitionEdge] | None:
    """Load a committed topology snapshot, or ``None`` when no baseline exists."""
    table = _table(control_schema, "partition_edges")
    rows = con.execute(
        f"SELECT {_EDGE_COLUMNS} FROM {table} WHERE pipeline = ?",
        [pipeline],
    ).fetchall()
    edges = [_edge_from_row(row) for row in rows]
    state_table = _table(control_schema, "partition_observation_state")
    state = con.execute(
        f"SELECT state FROM {state_table} WHERE pipeline = ?", [pipeline]
    ).fetchall()
    if not state and not edges:
        return None
    return {edge.edge_key: edge for edge in edges}
