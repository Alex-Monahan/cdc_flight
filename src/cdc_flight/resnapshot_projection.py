"""The one durable completion projection shared by every re-snapshot path.

The image-producing path, the replayable compatibility path, and the verified-empty
path have different transaction owners and different table-event shapes.  They do not
have different meanings: each must publish the table watermark, snapshot audit,
table-event history, refusal discharge, and main snapshot epoch as one destination
projection.  This module owns that projection and deliberately assumes its caller has
already opened the transaction that also owns the image or emptying operation.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import destination as dest_mod
from .destination import CONTROL_SCHEMA
from .errors import EngineFailure


@dataclass(frozen=True)
class ProjectionEvent:
    """One audit fact and, optionally, its canonical table-event representation."""

    audit_event: str
    audit_detail: str
    table_event: str | None = None
    table_event_detail: str | None = None
    seq: int = 0
    rows_removed: int | None = None
    applied: bool = True


def project_snapshot_completion(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    snapshot_lsn: int | None,
    commit_id: int,
    events: tuple[ProjectionEvent, ...],
    namespace: str | None = None,
    snapshot_epoch: int | None = None,
) -> None:
    """Publish one complete snapshot projection inside the caller's transaction.

    ``events`` is the only path-specific input.  A non-empty swap supplies
    ``resnapshot`` and optionally ``new`` table events; a verified-empty completion
    supplies ``resnapshot_empty`` and optionally ``new``.  Audit and table-event
    idempotency are checked independently so a historical partial projection cannot
    suppress the missing canonical row on replay.
    """
    if snapshot_lsn is None:
        raise EngineFailure(
            f"the snapshot for {source_schema}.{source_table} has no source LSN; "
            "refusing to publish a completion projection without a fence"
        )

    con.execute(
        f"UPDATE {CONTROL_SCHEMA}.table_state SET snapshot_lsn = ? "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [snapshot_lsn, pipeline, source_schema, source_table],
    )
    for event in events:
        _write_audit(
            con,
            pipeline=pipeline,
            source_schema=source_schema,
            source_table=source_table,
            target_table=target_table,
            snapshot_lsn=snapshot_lsn,
            event=event,
        )
        if event.table_event is not None:
            _write_table_event(
                con,
                pipeline=pipeline,
                commit_id=commit_id,
                source_schema=source_schema,
                source_table=source_table,
                target_table=target_table,
                snapshot_lsn=snapshot_lsn,
                event=event,
            )
    dest_mod.resolve_schema_refusal(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
    )
    advance_snapshot_epoch(
        con,
        pipeline=pipeline,
        namespace=namespace,
        snapshot_epoch=snapshot_epoch,
    )


def _write_audit(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    snapshot_lsn: int,
    event: ProjectionEvent,
) -> None:
    exists = con.execute(
        f"SELECT count(*) FROM {CONTROL_SCHEMA}.snapshot_audits "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ? "
        "AND snapshot_lsn = ? AND event = ?",
        [pipeline, source_schema, source_table, snapshot_lsn, event.audit_event],
    ).fetchone()[0]
    if exists:
        return
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.snapshot_audits "
        "(pipeline, source_schema, source_table, snapshot_lsn, event, "
        "target_table, detail, recorded_at) VALUES (?,?,?,?,?,?,?,?)",
        [
            pipeline,
            source_schema,
            source_table,
            snapshot_lsn,
            event.audit_event,
            target_table,
            event.audit_detail,
            dest_mod.now(),
        ],
    )


def _write_table_event(
    con,
    *,
    pipeline: str,
    commit_id: int,
    source_schema: str,
    source_table: str,
    target_table: str,
    snapshot_lsn: int,
    event: ProjectionEvent,
) -> None:
    table_event = event.table_event
    assert table_event is not None
    exists = con.execute(
        f"SELECT count(*) FROM {CONTROL_SCHEMA}.table_events "
        "WHERE pipeline = ? AND commit_id = ? AND seq = ? AND event = ? "
        "AND source_schema = ? AND source_table = ?",
        [
            pipeline,
            commit_id,
            event.seq,
            table_event,
            source_schema,
            source_table,
        ],
    ).fetchone()[0]
    if exists:
        return
    dest_mod.write_table_event(
        con,
        pipeline=pipeline,
        commit_id=commit_id,
        seq=event.seq,
        event=table_event,
        source_schema=source_schema,
        source_table=source_table,
        target_table=target_table,
        applied=event.applied,
        lsn=snapshot_lsn,
        rows_removed=event.rows_removed,
        detail=event.table_event_detail or event.audit_detail,
    )


def advance_snapshot_epoch(
    con,
    *,
    pipeline: str,
    namespace: str | None,
    snapshot_epoch: int | None,
) -> None:
    """Advance the main image identity in the same transaction as its projection."""
    if namespace is None or snapshot_epoch is None:
        return
    con.execute(
        f"UPDATE {CONTROL_SCHEMA}.debezium_offsets SET snapshot_epoch = "
        "greatest(snapshot_epoch, ?) WHERE pipeline = ? AND namespace = ?",
        [snapshot_epoch, pipeline, namespace],
    )
