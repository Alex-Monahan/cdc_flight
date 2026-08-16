"""Admission of complete PostgreSQL units into an applier commit group."""

from __future__ import annotations

import logging
import time

from . import table_lifecycle
from .assembler import UNIT_SNAPSHOT_CHUNK, UNIT_TXN
from .config import DROP_LOG
from .envelope import KIND_SNAPSHOT_BOUNDARY
from .errors import AdmissionError, as_schema_refusal
from .snapshot_completion import SnapshotObservationError

log = logging.getLogger("cdc_flight.unit_admission")


def add_unit(applier, unit) -> None:
    """Admit one complete unit, with ONE containment boundary around all of it.

    Round 13 (review r12, BLOCKER R12-2): the boundary used to wrap only
    ``observe_unit``, so an admission refusal raised anywhere else in this
    function — the DROP_LOG tail guard, the snapshot phase edge — escaped
    uncaught and killed the run.  The boundary now covers the whole admission
    and catches the common ``AdmissionError`` base, so a future admission
    sibling is contained by construction rather than by having been listed.
    """
    if discard_resnapshot_unit(applier, unit):
        return
    try:
        _admit_unit(applier, unit)
    except AdmissionError as error:
        refused = as_schema_refusal(
            error,
            refusal_origin="catalog_shape",
            detected_lsn=unit.commit_lsn or unit.last_lsn or None,
        )
        refused.detected_lsn = (
            refused.detected_lsn or unit.commit_lsn or unit.last_lsn or None
        )
        # A refusal that NAMES a relation can be discharged against it: roll the
        # group back and write the durable refusal.  One that names none cannot —
        # discarding buffered snapshot work on its behalf would destroy evidence
        # for a condition that belongs to no table (it is the snapshot phase edge:
        # `tests/rubric/1.3_atomic_batches` pins that the buffered snapshot chunk
        # survives it).  It still records — `spill_refusal.record_schema_refusal`
        # turns an unscoped refusal into a `critical` alert — and still stops the
        # run.  This is a rule about the refusal's SCOPE, not about its class.
        if refused.source_schema and refused.source_table:
            applier._rollback_quietly()
        applier._record_schema_refusal(refused)
        raise


def _admit_unit(applier, unit) -> None:
    hold_log_owed_tail(applier, unit)
    if applier.catalog is not None:
        applier.catalog.observe_unit(unit)
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


def hold_log_owed_tail(applier, unit) -> None:
    """Hold a DROP_LOG tail out of the retained image WITHOUT stopping the run.

    DROP_REPLICATE may transiently receive rows into a newly materialized partial
    target; that target remains untrusted and baseline refuses it until the complete
    resnapshot swap. DROP_LOG retains its old image, so a new lifecycle's rows must
    not enter that retained image until the swap completes.

    Round 13 (review r12, BLOCKER R12-2).  This used to *raise*, and the raise was
    an ``AdmissionError``-shaped refusal that no boundary caught: with
    ``CDC_DROP_MODE=log`` one relation awaiting a replacement snapshot killed
    every subsequent run — measured over four consecutive runs with both
    ``restart_lsn`` and ``confirmed_flush_lsn`` frozen, retained WAL growing
    monotonically, and a healthy co-published peer receiving nothing.  Refusing
    the whole unit was never necessary to protect the retained image: the
    relation already owes a FULL replacement snapshot, whose per-table watermark
    fences everything before it, so holding just that relation's rows out of the
    group is exactly as safe and is what the planner already does for a
    quarantined table (``planner.GroupPlan._collect``).  The peer's rows in the
    same PostgreSQL transaction still commit, the slot still advances, and the
    hold is alerted once per relation per blocking condition.
    """
    if applier.catalog is None or unit.kind != UNIT_TXN:
        return
    if applier.cfg.drop_mode != DROP_LOG:
        return
    owing = set(
        table_lifecycle.owing_work(
            applier.con,
            applier.pipeline,
            control_schema=applier.control_schema,
        )
    )
    held = sorted(unit.tables_touched() & owing)
    if held:
        applier.hold_streaming_tail(held)


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
