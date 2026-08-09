"""Replayable compatibility projection for already-swapped re-snapshots.

Production swaps use the callback owned by ``SnapshotCoordinator``.  Older callers and
recovery code can still ask which complete lifecycle rows need their projection
discharged; this module keeps that compatibility transaction separate from the engine
orchestration while using the same canonical projection component.
"""

from __future__ import annotations

from . import resnapshot_projection as projection
from . import table_lifecycle
from .resnapshot_projection import ProjectionEvent


def completed_tables(
    con,
    pipeline: str,
    tables: list[tuple[str, str, str]],
    consistent_lsn: int,
    *,
    reason: str = "",
    new_relations: set[str] | None = None,
    write_audit: bool = True,
    control_schema: str | None = None,
) -> list[str]:
    """Project requested tables whose shadow has already reached ``complete``.

    This compatibility path owns its own transaction because the shadow swap happened in
    an earlier transaction.  Its state/audit/table-event/refusal/epoch writes still all
    go through ``project_snapshot_completion`` so replay and the live callback cannot
    drift apart.
    """
    done: list[str] = []
    discovered = new_relations or set()
    for schema, table, target in tables:
        state = table_lifecycle.read(
            con,
            pipeline=pipeline,
            source_schema=schema,
            source_table=table,
            control_schema=control_schema,
        )
        if state != table_lifecycle.COMPLETE:
            continue
        if not write_audit:
            done.append(f"{schema}.{table}")
            continue
        resnapshot_detail = (
            f"re-snapshotted at consistent point {consistent_lsn} ({reason}). "
            "The table holds exact current state; change events of transactions "
            "that committed before this LSN are fenced rather than applied, so "
            "per-event history for that span is the snapshot image and not the "
            "individual events (rubric 8.2's changelog is discontinuous here)."
        )
        events = [
            ProjectionEvent(
                "resnapshot",
                resnapshot_detail,
                table_event="resnapshot",
                seq=0,
            )
        ]
        if f"{schema}.{table}" in discovered:
            events.append(
                ProjectionEvent(
                    "new",
                    "new source relation discovered by the catalog watcher and "
                    "snapshotted before streaming",
                    table_event="new",
                    seq=1,
                )
            )
        con.execute("BEGIN TRANSACTION")
        try:
            projection.project_snapshot_completion(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                target_table=target,
                snapshot_lsn=consistent_lsn,
                commit_id=0,
                events=tuple(events),
                namespace=None,
                snapshot_epoch=None,
                control_schema=control_schema,
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        done.append(f"{schema}.{table}")
    return done
