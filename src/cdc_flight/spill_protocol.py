"""Destination spill staging protocol for the applier."""

from __future__ import annotations

import logging

from . import catalog_support
from .envelope import PendingRecord
from .errors import AdmissionError, SchemaEvolutionRefused
from .faults import maybe_crash
from .planner import stream_event_id
from .spill import StagedEvent
from .typed_types import native_type

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
            refusal_origin="spill_protocol",
        )
    # This is a source catalog/control read, before any destination-table DML.
    # Admission errors produced by the provider itself remain explicit schema
    # refusals; driver/session/network failures must stay run-level and cannot be
    # relabelled as a table problem here.
    descriptors = provider(event.qualified_table)
    if not descriptors:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is incomplete for {event.qualified_table}; "
            "the spilled source unit is held for automatic retry",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
            refusal_origin="spill_protocol",
        )
    for name, descriptor in descriptors.items():
        try:
            native_type(descriptor)
        except (AdmissionError, ValueError, TypeError) as exc:
            raise SchemaEvolutionRefused(
                f"source catalog descriptor for {event.qualified_table}.{name} is not "
                f"deliverable through the strict native authority: {exc}",
                source_schema=event.schema,
                source_table=event.table,
                target=event.qualified_table,
                detected_lsn=event.lsn,
                refusal_origin="spill_protocol",
            ) from exc
    watcher = getattr(provider, "__self__", None)
    if watcher is not None and hasattr(watcher, "event_shape_missing"):
        missing = watcher.event_shape_missing(event, set(descriptors))
    elif not catalog_support.has_event_schema(event):
        missing = ()
    else:
        missing = tuple(
            sorted(set(descriptors) - catalog_support.delivered_event_fields(event))
        )
    if missing:
        raise SchemaEvolutionRefused(
            f"source catalog/event shape is incomplete for {event.qualified_table}; "
            f"the connector delivered no field(s) {list(missing)!r}; refusing the "
            "spill boundary rather than staging a partial row",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
            detected_lsn=event.lsn,
            refusal_origin="spill_protocol",
        )
    for attribute in ("key_descriptors", "before_descriptors", "after_descriptors"):
        getattr(event, attribute).update(descriptors)
