"""Admission of proven whole units into an applier commit group."""

from __future__ import annotations

import logging
import time

from . import table_lifecycle
from .assembler import UNIT_SNAPSHOT_CHUNK, UNIT_TXN
from .config import DROP_LOG
from .envelope import KIND_SNAPSHOT_BOUNDARY
from .errors import SchemaShapeUnexplained
from .snapshot_completion import SnapshotObservationError

log = logging.getLogger("cdc_flight.unit_admission")


def add_unit(applier, unit) -> None:
    if discard_resnapshot_unit(applier, unit):
        return
    refuse_log_recreate_tail(applier, unit)
    if applier.catalog is not None:
        try:
            applier.catalog.observe_unit(unit)
        except SchemaShapeUnexplained as refused:
            refused.detected_lsn = (
                refused.detected_lsn or unit.commit_lsn or unit.last_lsn or None
            )
            applier._rollback_quietly()
            applier._record_schema_refusal(refused)
            raise
    is_snapshot = is_snapshot_unit(unit)
    was_snapshot = applier.group.is_snapshot
    if not is_snapshot:
        if (
            applier.group.units
            and applier.group.is_snapshot
            and has_snapshot_boundary(applier.group.units)
        ):
            result = applier.commit_group("snapshot_chunk")
            if result.value != "committed":
                applier.snapshot_completion.check_streaming_admission()
                raise SnapshotObservationError(
                    "cannot cross the snapshot phase boundary with commit result "
                    f"{result.value}"
                )
        else:
            applier.snapshot_completion.check_streaming_admission()
    if applier.group.units and is_snapshot != applier.group.is_snapshot:
        result = applier.commit_group(
            "snapshot_chunk" if was_snapshot else "phase"
        )
        if result.value != "committed":
            raise SnapshotObservationError(
                "cannot cross the snapshot phase boundary with commit result "
                f"{result.value}"
            )
    if not is_snapshot:
        applier.snapshot_completion.enter_streaming()
    append_unit(applier, unit, is_snapshot=is_snapshot)


def refuse_log_recreate_tail(applier, unit) -> None:
    """Do not stream into a retained image that owes a replacement snapshot.

    ``CatalogCoordinator`` records a log-mode recreate after the fenced group has
    applied. A later whole Postgres transaction must not be allowed to append the new
    relation's rows to that old image before the next run's re-snapshot. Reading the
    durable lifecycle here makes the boundary survive a restart; the whole unit is
    refused before it enters a commit group, so no source transaction is partially
    admitted.
    """
    if applier.cfg.drop_mode != DROP_LOG or unit.kind != UNIT_TXN:
        return
    owing = set(table_lifecycle.owing_work(applier.con, applier.pipeline))
    blocked = sorted(unit.tables_touched() & owing)
    if blocked:
        raise SnapshotObservationError(
            "streaming admission refused for relation(s) awaiting_snapshot: "
            + ", ".join(blocked)
            + "; a replacement snapshot must complete before the new lifecycle streams"
        )


def append_unit(applier, unit, *, is_snapshot: bool) -> None:
    if not applier.group.units:
        applier.group.is_snapshot = is_snapshot
        applier.group.opened_at = time.monotonic()

    if (
        unit.kind == UNIT_TXN
        and unit.last_lsn
        and unit.last_lsn <= applier.resume_point.last_lsn
    ):
        unit.fenced = True
        applier.fenced_units += 1
        applier.fenced_events += unit.event_count
        log.info(
            "fencing already-durable transaction %s (lsn %s <= durable %s)",
            unit.txn_id,
            unit.last_lsn,
            applier.resume_point.last_lsn,
        )

    if not applier.cfg.ack_every_record and len(unit.records) > 1:
        for record in unit.records[:-1]:
            record.raw = None
        unit.records = [unit.records[-1]]
    applier.group.units.append(unit)
    applier.group.events += unit.event_count
    applier.group.nbytes += unit.nbytes


def discard_resnapshot_unit(applier, unit) -> bool:
    """Discard throwaway-slot streaming after the assembler proved it whole."""
    if not applier.cfg.resnapshot or unit.kind != UNIT_TXN:
        return False
    unit.fenced = True
    applier.fenced_units += 1
    applier.fenced_events += unit.event_count
    applier.resnapshot_discarded_events += unit.event_count
    applier._pending_discarded_records.extend(unit.records)
    if unit.spilled_events:
        applier.fenced_spilled_events += unit.spilled_events
    log.debug(
        "discarding %s streaming events from throwaway re-snapshot transaction %s",
        unit.event_count,
        unit.txn_id,
    )
    return True


def is_snapshot_unit(unit) -> bool:
    return unit.kind == UNIT_SNAPSHOT_CHUNK or any(
        record.kind == KIND_SNAPSHOT_BOUNDARY for record in unit.records
    )


def has_snapshot_boundary(units) -> bool:
    return any(
        record.kind == KIND_SNAPSHOT_BOUNDARY
        for unit in units
        for record in unit.records
    )
