"""Ordered schema-fence planning for one destination commit group.

The epoch policy is deliberately separate from :mod:`applier`: it answers only which
whole source units belong before or after each catalog fence.  It never owns the
transaction, the acknowledgement window, or durable state.  The caller supplies those
operations as narrow callbacks, so a schema decision cannot quietly grow a second commit
protocol beside the real one.
"""

from __future__ import annotations

from collections.abc import Callable

from .catalog import CHANGE_SCHEMA
from .catalog_apply import CatalogPlan
from .errors import SchemaEvolutionRefused
from .schema_evolution import COLUMN_ADDED, COLUMN_DROPPED, COLUMN_RENAMED, COLUMN_TYPE_CHANGED


def _type_change_epoch(event, change, table: str) -> str | None:
    """Classify a same-name type change from recursive source fingerprints."""
    old = change.old_descriptor
    new = change.new_descriptor
    old_identity = old.fingerprint if old is not None else (
        f"{change.old_type_oid}:{str(change.old_type_name or '').lower()}"
    )
    new_identity = new.fingerprint if new is not None else (
        f"{change.type_oid}:{str(change.type_name or '').lower()}"
    )
    observed: set[str] = set()
    for attribute in ("key_descriptors", "before_descriptors", "after_descriptors"):
        for descriptor in getattr(event, attribute, {}).values():
            fingerprint = descriptor.fingerprint
            if fingerprint == old_identity:
                observed.add("pre")
            if fingerprint == new_identity:
                observed.add("post")
    if not observed:
        raise SchemaEvolutionRefused(
            f"type-change evidence for {table}.{change.destination_new_name or change.destination_old_name} "
            "has no catalog descriptor fingerprint; the source unit is held rather "
            "than inferred from a same-name field",
            source_schema=event.schema,
            source_table=event.table,
            target=table,
            detected_lsn=None,
            refusal_origin="schema_epoch",
        )
    if len(observed) > 1:
        return "mixed"
    return next(iter(observed))


def events_for_schema_check(spill, unit, commit_id: int) -> list:
    """Return one whole unit's in-memory events plus its staged prefix."""
    events = list(unit.events)
    if unit.spill_unit_seq is not None:
        events.extend(
            staged.event
            for staged in spill.load(
                commit_id=commit_id, unit_seq=unit.spill_unit_seq
            )
        )
    return events


def refuse_mixed_schema_epoch(events: list, actions: list) -> None:
    """Refuse one whole source unit that contains both sides of a schema fence."""
    changes_by_table: dict[str, tuple] = {}
    for action in actions:
        changes_by_table.setdefault(action.change.qualified, ())
        changes_by_table[action.change.qualified] += tuple(action.change.column_changes)
    epochs: dict[str, set[str]] = {}
    for event in events:
        if not event.schema or not event.table:
            continue
        table = f"{event.schema}.{event.table}"
        changes = changes_by_table.get(table)
        if not changes:
            continue
        fields = set()
        for image in (event.before, event.after, event.key):
            if image:
                fields.update(image)
        for change in changes:
            old = change.destination_old_name
            new = change.destination_new_name
            epoch = None
            if change.kind == COLUMN_TYPE_CHANGED or (
                change.kind == COLUMN_RENAMED and change.type_changed
            ):
                epoch = _type_change_epoch(event, change, table)
                if epoch == "mixed":
                    raise SchemaEvolutionRefused(
                        f"row shape for {table} contains both old and new recursive "
                        "type descriptors for the same schema fence",
                        source_schema=event.schema,
                        source_table=event.table,
                        target=table,
                        detected_lsn=min(action.change.detected_lsn for action in actions),
                        refusal_origin="schema_epoch",
                    )
                if epoch is not None:
                    epochs.setdefault(table, set()).add(epoch)
                    continue
            if change.kind == COLUMN_ADDED and new:
                epoch = "post" if new in fields else "pre"
            elif change.kind == COLUMN_DROPPED and old:
                epoch = "pre" if old in fields else "post"
            elif change.kind == COLUMN_RENAMED and old and new:
                if old in fields and new in fields:
                    raise SchemaEvolutionRefused(
                        f"row shape for {table} contains both sides of the "
                        f"rename {old!r} -> {new!r}; the source transaction "
                        "cannot be ordered safely around the schema fence",
                        source_schema=event.schema,
                        source_table=event.table,
                        target=table,
                        detected_lsn=min(
                            action.change.detected_lsn for action in actions
                        ),
                        refusal_origin="schema_epoch",
                    )
                if old in fields:
                    epoch = "pre"
                elif new in fields:
                    epoch = "post"
            if epoch is not None:
                epochs.setdefault(table, set()).add(epoch)
    mixed = sorted(table for table, values in epochs.items() if len(values) > 1)
    if not mixed:
        return
    table = mixed[0]
    schema, _, source_table = table.partition(".")
    raise SchemaEvolutionRefused(
        f"source unit for {table} contains row images from both sides of a "
        "schema fence; a whole source transaction cannot be safely canonicalized, "
        "so it is refused for a replacement snapshot",
        source_schema=schema,
        source_table=source_table,
        target=table,
        detected_lsn=min(action.change.detected_lsn for action in actions),
        refusal_origin="schema_epoch",
    )


def unit_is_post_schema_epoch(spill, unit, actions: list, commit_id: int) -> bool:
    """Whether an applicable unit's row shape proves it is post-fence."""
    if unit.fenced:
        return False
    changes_by_table: dict[str, tuple] = {}
    for action in actions:
        changes_by_table.setdefault(action.change.qualified, ())
        changes_by_table[action.change.qualified] += tuple(action.change.column_changes)
    for event in events_for_schema_check(spill, unit, commit_id):
        if not event.schema or not event.table:
            continue
        changes = changes_by_table.get(f"{event.schema}.{event.table}")
        if not changes:
            continue
        fields = set()
        for image in (event.before, event.after, event.key):
            if image:
                fields.update(image)
        if not fields:
            continue
        for change in changes:
            old = change.destination_old_name
            new = change.destination_new_name
            if change.kind == COLUMN_ADDED and new and new in fields:
                return True
            if change.kind == COLUMN_DROPPED and old and old not in fields:
                return True
            if change.kind == COLUMN_TYPE_CHANGED or (
                change.kind == COLUMN_RENAMED and change.type_changed
            ):
                epoch = _type_change_epoch(event, change, f"{event.schema}.{event.table}")
                if epoch == "post":
                    return True
            if (
                change.kind == COLUMN_RENAMED
                and new
                and new in fields
                and old not in fields
            ):
                return True
    return False


def empty_apply_stats() -> dict:
    return {
        "events": 0,
        "tables": set(),
        "first_txn_id": None,
        "last_txn_id": None,
        "first_lsn": None,
        "last_lsn": None,
        "max_source_ts": None,
    }


def merge_apply_stats(total: dict | None, part: dict) -> dict:
    if total is None:
        return part
    total["events"] += part["events"]
    total["tables"].update(part["tables"])
    if total["first_txn_id"] is None:
        total["first_txn_id"] = part["first_txn_id"]
    if part["last_txn_id"] is not None:
        total["last_txn_id"] = part["last_txn_id"]
    if total["first_lsn"] is None:
        total["first_lsn"] = part["first_lsn"]
    if part["last_lsn"] is not None:
        total["last_lsn"] = part["last_lsn"]
    if part["max_source_ts"] is not None:
        total["max_source_ts"] = max(
            total["max_source_ts"] or 0, part["max_source_ts"]
        )
    for name in ("fold_sec", "event_ledger_sec", "destination_write_sec"):
        if name in part:
            total[name] = total.get(name, 0.0) + part[name]
    return total


class SchemaEpochCoordinator:
    """Apply whole units and catalog schema phases in source order."""

    def __init__(
        self,
        *,
        spill,
        apply_units: Callable,
        apply_catalog_phase: Callable,
        backfill_schema: Callable,
        clear_spill: Callable,
    ) -> None:
        self.spill = spill
        self._apply_units = apply_units
        self._apply_catalog_phase = apply_catalog_phase
        self._backfill_schema = backfill_schema
        self._clear_spill = clear_spill

    def apply(
        self,
        group,
        commit_id: int,
        *,
        has_data: bool,
        catalog_plan: CatalogPlan | None,
        catalog_stats: dict,
        created_in_txn: set[str],
    ) -> dict:
        actions = sorted(
            (
                action
                for action in (catalog_plan.actions if catalog_plan else ())
                if action.change.kind == CHANGE_SCHEMA
            ),
            key=lambda action: (action.change.detected_lsn, action.change.qualified),
        )
        if not actions:
            return self._apply_units(group, commit_id, has_data=has_data)

        total: dict | None = None
        cursor = 0
        index = 0
        while index < len(actions):
            boundary = actions[index].change.detected_lsn
            same_boundary: list = []
            while (
                index < len(actions)
                and actions[index].change.detected_lsn == boundary
            ):
                same_boundary.append(actions[index])
                index += 1
            for unit in group:
                if unit.fenced:
                    # Replay units are not applicable evidence for an epoch.  This
                    # also prevents a fenced mixed-shape unit from forcing a rebuild.
                    continue
                refuse_mixed_schema_epoch(
                    events_for_schema_check(self.spill, unit, commit_id), same_boundary
                )
            end = cursor
            while end < len(group) and (group[end].last_lsn or 0) <= boundary:
                end += 1
            # The catalog observation LSN is a discovery fence, not necessarily the
            # source DDL event LSN; a post-DDL row shape is stronger evidence.
            for position in range(cursor, end):
                if unit_is_post_schema_epoch(
                    self.spill, group[position], same_boundary, commit_id
                ):
                    end = position
                    break
            if end > cursor:
                part = self._apply_units(
                    group[cursor:end],
                    commit_id,
                    has_data=has_data,
                    clear_spill=False,
                    created_in_txn=created_in_txn,
                )
                total = merge_apply_stats(total, part)
                cursor = end

            names = {action.change.qualified for action in same_boundary}
            phase = CatalogPlan(
                actions=tuple(same_boundary),
                relations=tuple(
                    relation
                    for relation in (catalog_plan.relations if catalog_plan else ())
                    if relation.qualified in names
                ),
                policy_alerts=tuple(
                    alert
                    for alert in (catalog_plan.policy_alerts if catalog_plan else ())
                    if (
                        f"{alert.get('source_schema', '')}."
                        f"{alert.get('source_table', '')}"
                    ) in names
                ),
            )
            self._apply_catalog_phase(
                commit_id, phase, catalog_stats, schema_only=True
            )
            self._backfill_schema(phase)

        if cursor < len(group):
            part = self._apply_units(
                group[cursor:],
                commit_id,
                has_data=has_data,
                clear_spill=True,
                created_in_txn=created_in_txn,
            )
            total = merge_apply_stats(total, part)
        elif any(unit.spill_unit_seq is not None for unit in group):
            self._clear_spill(commit_id)
        return total or empty_apply_stats()
