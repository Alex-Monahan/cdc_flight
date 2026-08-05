"""Admission of proven whole units into an applier commit group."""

from __future__ import annotations

import logging
import time

from . import catalog_generation, table_lifecycle
from .assembler import UNIT_SNAPSHOT_CHUNK, UNIT_TXN
from .catalog_state import CHANGE_RECREATED
from .config import DROP_LOG
from .control_schema import CONTROL_SCHEMA
from .envelope import KIND_SNAPSHOT_BOUNDARY
from .errors import SchemaShapeUnexplained
from .snapshot_completion import SnapshotObservationError

log = logging.getLogger("cdc_flight.unit_admission")


def add_unit(applier, unit) -> None:
    if discard_resnapshot_unit(applier, unit):
        return
    reject_log_owed_tail(applier, unit)
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


def reject_log_owed_tail(applier, unit) -> None:
    """Reject a log-mode tail already known to require a replacement snapshot.

    This pre-admission ownership check is deliberately separate from the source
    identity fence below. It preserves the existing DROP_LOG fail-fast behavior for
    an already durable ``awaiting_snapshot`` row without reading source identity
    early; the complete generation check runs once, at commit-group planning time,
    for every drop mode.
    """
    if applier.catalog is None or unit.kind != UNIT_TXN:
        return
    owing = set(table_lifecycle.owing_work(applier.con, applier.pipeline))
    blocked = sorted(unit.tables_touched() & owing)
    if blocked and applier.cfg.drop_mode == DROP_LOG:
        raise SnapshotObservationError(
            "streaming admission refused for relation(s) awaiting_snapshot: "
            + ", ".join(blocked)
            + "; a replacement snapshot must complete before the new lifecycle streams"
        )


def refuse_log_recreate_tail(applier, unit, *, catalog_plan=None) -> None:
    """Refuse or fence a replacement tail before it can reach the row planner.

    A replacement is a *relation identity* boundary, not a WAL-watermark boundary.
    The Debezium envelope currently exposes the qualified table but not pgoutput's
    relation OID, so the admission seam asks the catalog watcher for the current OID
    and compares it with the durable ``source_relations`` identity. A replacement
    transaction cannot be emitted before its DDL commits; therefore every unit from
    the new lifecycle sees the new OID, even if the watcher has not yet queued a
    ``CHANGE_RECREATED`` observation.

    The fence's identity signal is PostgreSQL's four-byte relation OID.  OID
    wraparound/reuse can make a drop-and-recreate reuse the same OID, which this model
    cannot distinguish from the original lifecycle; that is an explicit supported-
    lifetime limitation, not a claim of database-wide uniqueness.  See ADR 0001 A72
    and ``reviews/p2_review_r8.md`` R8-M3 for the contract boundary.

    ``CatalogCoordinator`` also records a recreate after the fenced group has applied.
    Once that plan is due, every touched unit is fenced regardless of LSN in either drop
    mode.
    Reading the durable lifecycle here makes the post-detection boundary survive a
    restart; the whole unit is refused before it enters a commit group, so no source
    transaction is partially admitted.

    A commit group can contain the catalog plan and the replacement tail before the
    catalog phase writes its durable lifecycle mark. In that ordering the plan is a
    second, durable identity boundary. Such a unit is fenced (rather than raising and
    rolling back the plan), so the catalog obligation and the source resume point
    commit atomically; the next invocation's re-snapshot owns the rows.
    """
    if applier.catalog is None or unit.kind != UNIT_TXN:
        return
    if unit.fenced:
        return
    prepare_source_identity_cache(applier)
    owing = set(table_lifecycle.owing_work(applier.con, applier.pipeline))
    blocked = sorted(unit.tables_touched() & owing)
    if blocked and applier.cfg.drop_mode == DROP_LOG:
        raise SnapshotObservationError(
            "streaming admission refused for relation(s) awaiting_snapshot: "
            + ", ".join(blocked)
            + "; a replacement snapshot must complete before the new lifecycle streams"
        )
    identity_blocked = _replacement_identity_mismatches(applier, unit.tables_touched())
    planned = sorted(
        action.change.qualified
        for action in getattr(catalog_plan, "actions", ())
        if (
            action.change.kind == CHANGE_RECREATED
            and action.change.qualified in unit.tables_touched()
        )
    )
    fenced = sorted(set(identity_blocked) | set(planned))
    if blocked:
        # Replicate mode must not create an untrusted partial table after quarantine;
        # its final image is owned by the automatic snapshot just like log mode's.
        fenced = sorted(set(fenced) | set(blocked))
    if not fenced:
        return
    unit.fenced = True
    applier.fenced_units += 1
    applier.fenced_events += unit.event_count
    log.info(
        "fencing replacement stream unit %s for relation(s) %s before row apply; "
        "the catalog phase will durably queue a re-snapshot",
        unit.txn_id,
        ", ".join(fenced),
    )


def _replacement_identity_mismatches(applier, names: set[str]) -> list[str]:
    """Return names whose current source relation is a different lifecycle.

    Only a *present* relation with a different OID is a replacement proof.  ``None``
    means the relation is currently absent, which remains under the ordinary DROP
    detector's WAL fence so a log-mode drop does not newly discard an old-lifecycle
    tail.  A source query error is fail-closed: admitting an unknown relation identity
    would turn an unavailable catalog into permission to append to a retained image.
    """
    if not names or applier.catalog is None:
        return []
    prepare_source_identity_cache(applier)
    durable = {
        qualified: oid
        for qualified, oid in _durable_source_oids(applier).items()
        if qualified in names
    }
    if not durable:
        return []
    if applier.group.source_identity_error is not None:
        raise SnapshotObservationError(
            "cannot establish source relation identity before streaming admission; "
            "the unit is not safe to append to a retained image"
        )
    current = applier.group.source_identity_oids or {}
    return sorted(
        qualified
        for qualified, old_oid in durable.items()
        if current.get(qualified) is not None
        and current.get(qualified) is not catalog_generation.UNKNOWN
        and int(current[qualified]) != old_oid
    )


def _durable_source_oids(applier) -> dict[str, int]:
    return {
        f"{schema}.{table}": int(oid)
        for schema, table, oid in applier.con.execute(
            f"SELECT source_schema, source_table, relation_oid "
            f"FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?",
            [applier.pipeline],
        ).fetchall()
    }


def prepare_source_identity_cache(applier) -> None:
    """Read source identities once for this commit group, fail closed on errors.

    Admission and catalog planning deliberately share this snapshot.  It keeps the
    synchronous source dependency outside the MotherDuck transaction where possible,
    avoids one source connection/query per unit, and still makes an unreadable source
    an explicit ``SnapshotObservationError`` for a touched durable relation.
    """
    if applier.catalog is None or applier.group.source_identity_oids is not None:
        return
    durable = _durable_source_oids(applier)
    names = set(durable)
    names.update(
        change.qualified
        for change in applier.catalog.pending()
        if change.kind == CHANGE_RECREATED
    )
    if not names:
        # Leave the cache uninitialised. A first group can materialise its first
        # source-relations rows; a later catalog plan in the same lifecycle must still
        # be able to discover a newly queued recreate and read its identity.
        return
    relation_oids = getattr(applier.catalog, "relation_oids", None)
    if relation_oids is None:
        applier.group.source_identity_oids = dict.fromkeys(names, catalog_generation.UNKNOWN)
        applier.group.source_identity_error = "catalog watcher has no identity reader"
        return
    pairs = {tuple(qualified.split(".", 1)) for qualified in names}
    try:
        current = relation_oids(pairs)
    except Exception as exc:
        applier.group.source_identity_oids = dict.fromkeys(
            names, catalog_generation.UNKNOWN
        )
        applier.group.source_identity_error = str(exc)
        return
    applier.group.source_identity_oids = dict(current)


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
