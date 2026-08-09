"""Destination spill staging protocol for the applier."""

from __future__ import annotations

import logging

from .envelope import PendingRecord
from .errors import SchemaEvolutionRefused
from .faults import maybe_crash
from .planner import stream_event_id
from .spill import StagedEvent

log = logging.getLogger("cdc_flight.spill_protocol")


def stage_events(
    applier,
    events: list[PendingRecord],
    *,
    unit_seq: int,
    snapshot: tuple[str | None, str | None] | None = None,
) -> int:
    """Stage one complete unit's memory prefix inside its commit transaction."""
    if not events:
        return 0
    if applier.cfg.resnapshot and snapshot is None:
        log.debug(
            "discarding %s throwaway re-snapshot events before destination spill",
            len(events),
        )
        return len(events)
    if not applier.group.txn_open:
        applier.con.execute("BEGIN TRANSACTION")
        applier.group.txn_open = True
        applier.group.spill_commit_id = applier._reserve_commit_id()
    commit_id = applier.group.spill_commit_id or applier._next_commit_id
    state = (
        applier.snapshots.state_for(*snapshot) if snapshot is not None else None
    )

    prepared: list[StagedEvent] = []
    for event in events:
        if not event.schema or not event.table:
            continue
        _enrich_descriptors(applier, event)
        if state is not None:
            prepared.append(
                StagedEvent(
                    event=event,
                    event_id=applier.snapshots.event_id(event),
                    target=state.shadow,
                    seq=event.snapshot_ordinal,
                )
            )
        else:
            prepared.append(
                StagedEvent(
                    event=event,
                    event_id=stream_event_id(event),
                    target=applier.snapshots.target_table(event.schema, event.table),
                    seq=event.total_order,
                )
            )
    staged = applier.spill.stage(
        commit_id=commit_id, unit_seq=unit_seq, prepared=prepared
    )
    applier.spilled_events += staged
    maybe_crash("spill", applier.data_commit_groups + 1)
    return len(events)


def _enrich_descriptors(applier, event: PendingRecord) -> None:
    """Apply the catalog epoch before a raw image crosses the spill boundary."""
    provider = applier.descriptor_provider or (
        getattr(applier.catalog, "descriptors_for", None)
        if applier.catalog is not None
        else None
    )
    if not event.qualified_table:
        return
    if provider is None:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is unavailable for {event.qualified_table}; "
            "the spilled source unit is held for automatic retry",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
        )
    try:
        descriptors = provider(event.qualified_table)
    except Exception as exc:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority failed for {event.qualified_table}; "
            "the spilled source unit is held for automatic retry",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
        ) from exc
    if not descriptors:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is incomplete for {event.qualified_table}; "
            "the spilled source unit is held for automatic retry",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
        )
    for attribute in ("key_descriptors", "before_descriptors", "after_descriptors"):
        getattr(event, attribute).update(descriptors)
