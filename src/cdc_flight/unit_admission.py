"""Admission of proven whole units into an applier commit group."""

from __future__ import annotations

import logging
import time

from . import catalog_generation, table_lifecycle
from .assembler import UNIT_SNAPSHOT_CHUNK, UNIT_TXN
from .catalog_state import CHANGE_RECREATED
from .config import DROP_LOG
from .control_schema import CONTROL_SCHEMA
from .envelope import KIND_DATA, KIND_SNAPSHOT_BOUNDARY, KIND_TRUNCATE
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


def refuse_log_recreate_tail(
    applier, unit, *, catalog_plan=None, generation_proof=None
) -> None:
    """Fence a unit unless the final source-generation proof is current.

    The proof is acquired by the catalog coordinator inside the destination commit
    protocol. This function only consumes that value; it never performs an out-of-band
    source read of its own. A missing, changed, absent, ambiguous, or WAL-uncovered
    proof is a fence, so no unit can append a new lifecycle to a retained image.
    """
    if applier.catalog is None or unit.kind != UNIT_TXN:
        return
    if unit.fenced:
        return
    owing = set(table_lifecycle.owing_work(applier.con, applier.pipeline))
    blocked = sorted(unit.tables_touched() & owing)
    if blocked and applier.cfg.drop_mode == DROP_LOG:
        raise SnapshotObservationError(
            "streaming admission refused for relation(s) awaiting_snapshot: "
            + ", ".join(blocked)
            + "; a replacement snapshot must complete before the new lifecycle streams"
        )
    identity_blocked = _replacement_identity_mismatches(
        applier,
        unit.tables_touched(),
        unit.last_lsn,
        generation_proof=generation_proof,
        truncate_names=generation_unit_evidence(applier, unit)[0],
    )
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


def _replacement_identity_mismatches(
    applier,
    names: set[str],
    minimum_lsn: int | None,
    *,
    generation_proof=None,
    truncate_names: set[str] | None = None,
) -> list[str]:
    """Return durable names not proven current at this unit's WAL boundary."""
    if not names or applier.catalog is None:
        return []
    durable = {
        qualified: identity
        for qualified, identity in _durable_source_identities(applier).items()
        if qualified in names
    }
    if not durable:
        return []
    current = generation_proof
    if current is None:
        current = applier.group.source_generation_final or {}
    ignored = truncate_names or set()
    return sorted(
        qualified
        for qualified, expected in durable.items()
        if qualified not in ignored
        if catalog_generation.check(
            expected,
            current.get(qualified, catalog_generation.UNKNOWN),
            minimum_lsn=minimum_lsn,
        ).state
        != catalog_generation.GENERATION_CURRENT
    )


def _unit_stream_events(applier, unit):
    events = list(unit.events)
    if unit.spill_unit_seq is not None:
        events.extend(
            staged.event
            for staged in applier.spill.load(
                commit_id=applier.group.spill_commit_id or applier._next_commit_id,
                unit_seq=unit.spill_unit_seq,
            )
        )
    return events


def generation_unit_evidence(applier, unit) -> tuple[set[str], set[str]]:
    """Return generation evidence for one unit, including its spilled prefix."""
    truncates: set[str] = set()
    normal_rows: set[str] = set()
    for record in _unit_stream_events(applier, unit):
        qualified = record.qualified_table
        if not qualified:
            continue
        if record.kind == KIND_TRUNCATE:
            truncates.add(qualified)
        elif record.kind == KIND_DATA:
            normal_rows.add(qualified)
    return truncates, normal_rows


def generation_stream_evidence(applier) -> tuple[set[str], set[str]]:
    """Return committed and current ``(truncates, normal_rows)`` evidence.

    A same-OID relfilenode change is ambiguous in the source catalog because PostgreSQL
    also rewrites a table's physical file for TRUNCATE.  The decoded transaction is
    the ordered discriminator: a TRUNCATE authorizes refreshing the durable token,
    while ordinary rows prove that a replacement generation is trying to enter.
    """
    remembered = getattr(applier, "_generation_stream_evidence", {})
    truncates: set[str] = set(remembered.get("truncates", ()))
    normal_rows: set[str] = set(remembered.get("normal_rows", ()))
    for unit in applier.group.units:
        if unit.kind != UNIT_TXN:
            continue
        unit_truncates, unit_rows = generation_unit_evidence(applier, unit)
        truncates.update(unit_truncates)
        normal_rows.update(unit_rows)
    # Catalog polling can observe a relfilenode change after the transaction carrying
    # its TRUNCATE/rows has already committed. Keep the evidence attached to this
    # group until COMMIT; rollback then discards it with the rest of the group.
    applier.group._generation_stream_evidence = (set(truncates), set(normal_rows))
    return truncates, normal_rows


def settle_generation_stream_evidence(applier, group, plan=None) -> None:
    """Retain committed stream evidence until its catalog rewrite is settled."""
    observed = getattr(group, "_generation_stream_evidence", (set(), set()))
    remembered = getattr(
        applier, "_generation_stream_evidence", {"truncates": set(), "normal_rows": set()}
    )
    remembered.setdefault("truncates", set()).update(observed[0])
    remembered.setdefault("normal_rows", set()).update(observed[1])
    if plan is not None:
        resolved = {
            action.change.qualified
            for action in plan.actions
            if action.change.kind in {"dropped", CHANGE_RECREATED}
        }
        resolved.update(change.qualified for change in plan.generation_refreshes)
        remembered["truncates"].difference_update(resolved)
        remembered["normal_rows"].difference_update(resolved)
    applier._generation_stream_evidence = remembered


def _durable_source_identities(applier) -> dict[str, catalog_generation.RelationIdentity]:
    return {
        f"{schema}.{table}": catalog_generation.RelationIdentity(
            int(oid),
            int(relfilenode) if relfilenode is not None else None,
            int(relation_type_oid) if relation_type_oid is not None else None,
        )
        for schema, table, oid, relfilenode, relation_type_oid in applier.con.execute(
            f"SELECT source_schema, source_table, relation_oid, relation_filenode, "
            f"relation_type_oid "
            f"FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?",
            [applier.pipeline],
        ).fetchall()
    }


def source_generation_names(applier, catalog_plan=None) -> set[str]:
    """Names for the plan read and the final proof lease.

    Every durable relation touched by a streaming unit is included, even when the
    catalog action is a plain drop. That is what makes an unobserved A->B transition
    fail closed; the drop revalidation switch only controls whether a plain drop is
    allowed to use the proof for its DDL decision.
    """
    if applier.catalog is None:
        return set()
    names = set(_durable_source_identities(applier))
    names.update(
        change.qualified
        for change in applier.catalog.pending()
        if change.kind == CHANGE_RECREATED
        or (change.kind == "dropped" and applier.cfg.drop_revalidate)
    )
    if catalog_plan is not None:
        names.update(
            action.change.qualified
            for action in catalog_plan.actions
            if action.change.kind == CHANGE_RECREATED
            or (
                action.change.kind == "dropped"
                and applier.cfg.drop_revalidate
            )
        )
    return names


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
