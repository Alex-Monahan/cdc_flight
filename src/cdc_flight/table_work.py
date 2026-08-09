"""The apply plan for one commit group: one `TableWork` per destination table.

This is the layer where the two table shapes become one mechanism (ADR D6):

| table | identity | effect of a group |
|---|---|---|
| keyed | the source key columns | delete every key the group touched, insert the final row per key |
| keyless | `cdcf_event_id` | delete that event id if present, insert - so a replay cannot duplicate and two byte-identical source rows stay two rows |

**The fold models physical rows, not keys** (ADR §18/A35). That is the correction the
1.4/1.5 review round forced, and it is the whole of the design:

    live[key] = [entry, ...]      # the rows that currently wear `key`
    entry     = START | row       # START is the row the destination already held

A key is not a row. Inside one Postgres transaction a *deferred* unique constraint
lets several rows wear one key at once, and across the transactions of one commit
group a key can be freed and re-taken; a plan indexed by key alone cannot express
either, which is exactly how the previous fold lost rows. Every event is therefore
one physical operation on that list:

| event | operation |
|---|---|
| `c` / `r`, or the new-key half of a key change | append a row |
| `u` with an unchanged key | replace the entry the before-image identifies |
| `d`, or the old-key half of a key change | remove the entry the before-image identifies |
| `t` (TRUNCATE) | every entry is gone, **including** `START` |

Attribution needs no notion of "before this commit group" at all: a row an earlier
transaction of the group placed is a concrete entry in the list, and only `START` -
one entry, whose existence and content live at the destination - is ever probed.
Where two entries could be the one a delete removes, the delete's before-image
decides; where the before-image cannot decide but only one row can really wear the
key (no deferred constraint ⇒ no full before-image ⇒ at most one row), the key
simply ends empty; and where it genuinely cannot be decided, `AmbiguousDelete` fails
the group. Silent loss is worse than an error (the rubric's own scale), and a failed
group rolls back and replays for free (Invariant O).

At group end each key holds at most one row - the source enforces that at every
transaction boundary - and the three cases are:

| `live[key]` | destination |
|---|---|
| `[row]`, `[START, row]` | delete the key, insert `row` |
| `[]` | delete the key |
| `[START]` | **leave it alone**: the row the destination holds is the row the source holds |

`live` is a dict, so insertion order *is* source order and membership is O(1). It
used to be a dict paired with an `order` list and an `if key not in order` scan,
which is linear per event: MEASURED 458 s for one 200 000-event transaction, 1.6 s
after the change.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from . import apply_sql, naming
from .envelope import KIND_TRUNCATE, PendingRecord
from .errors import (
    AmbiguousDelete,
    DestinationIdentityCollision,
    SchemaEvolutionRefused,
    ToastBaseMissing,
)
from .naming import CDCF_COMMIT_ID, CDCF_EVENT_ID, CDCF_TOTAL_ORDER
from .row_patch import RowPatch
from .toast import is_structural_marker
from .typed_types import FieldState, FieldValue

log = logging.getLogger("cdc_flight.table_work")

DBZ_COLUMN_TYPES = {
    "dbz_op": apply_sql.VARCHAR,
    "dbz_lsn": apply_sql.BIGINT,
    "dbz_tx_id": apply_sql.BIGINT,
    "dbz_schema": apply_sql.VARCHAR,
    "dbz_table": apply_sql.VARCHAR,
    "dbz_source_ts_ms": apply_sql.BIGINT,
}
#: The applier's own columns have KNOWN types; they are declared, never inferred.
#: Widening them against a group in which they all happened to be NULL is how
#: `cdcf_total_order` silently became VARCHAR.
APPLIER_COLUMN_TYPES = {
    CDCF_COMMIT_ID: apply_sql.BIGINT,
    CDCF_EVENT_ID: apply_sql.VARCHAR,
    CDCF_TOTAL_ORDER: apply_sql.BIGINT,
    **DBZ_COLUMN_TYPES,
}

class _Start:
    """The row the destination already held under a key, before this group.

    A single sentinel rather than a fetched row: its *content* is only ever needed
    to answer "is this the row that delete removed?", which the destination answers
    far more reliably than a Python comparison of values that have been through
    DuckDB's type system - and when it survives, it is never rewritten at all.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "START"


START = _Start()


@dataclass
class RowMove:
    """A sparse key move that can update the old physical row in place."""

    source_key: tuple
    target_key: tuple
    patch: RowPatch


@dataclass
class TableWork:
    """Everything one destination table needs from one commit group."""

    target: str
    key_columns: tuple[str, ...] = ()
    keyless: bool = False
    columns: dict[str, str] = field(default_factory=dict)
    #: Source descriptors observed on the schema-bearing envelope. Values in
    #: `columns` are retained for the applier's fixed metadata fields; source
    #: creation and binding use the native descriptor map.
    descriptors: dict[str, Any] = field(default_factory=dict)
    native_columns: dict[str, Any] = field(default_factory=dict)
    #: Native descriptors are recursive and immutable; resolve each catalog
    #: fingerprint once per table/group instead of once per event field.
    native_fingerprints: dict[str, str] = field(default_factory=dict)
    #: identity key -> the rows that currently wear it, in source order. See the
    #: module docstring: this is the whole fold.
    live: dict[tuple, list] = field(default_factory=dict)
    #: Fold token -> raw source key and the descriptor epoch that emitted it.
    #: Fingerprinting the token keeps an int4 key and an int8 key with the same
    #: rendered value from sharing one physical-row slot during a migration.
    key_values: dict[tuple, tuple] = field(default_factory=dict)
    key_descriptors: dict[tuple, dict[str, Any]] = field(default_factory=dict)
    #: keys currently holding two or more CONCRETE rows. Never legal at a source
    #: transaction boundary, so `end_transaction` refuses on it.
    multi: set[tuple] = field(default_factory=set)
    #: cache of "does the destination hold a row under this key?", so `START` is
    #: probed at most once per key and only where attribution needs it.
    start_present: dict[tuple, bool] = field(default_factory=dict)
    snapshot: bool = False
    events: int = 0
    #: rubric 1.5: the group truncated this table, so every row that predates the
    #: truncate is gone - including the destination's own, which is why no `START`
    #: is created afterwards.
    truncated: bool = False
    #: how many `op="t"` events the group carried for this table (a `TRUNCATE a, b`
    #: sends one per relation, so this is 1 per table in the normal case).
    truncates: int = 0
    #: rows the plan itself dropped at each truncate, in order. The FIRST truncate of
    #: a group is also the one whose `DELETE FROM` removes the destination's own rows,
    #: so `write` adds that count to element 0 and nowhere else - which is what stops
    #: two truncates in one transaction from reporting the same number (Codex 2).
    truncate_marks: list[int] = field(default_factory=list)
    #: filled in by `write`: rows removed per truncate, positionally, immutable.
    truncate_rows_removed: tuple[int | None, ...] = ()
    #: filled in by `write`: how many destination rows the `DELETE FROM` removed.
    rows_removed: int | None = None
    #: True when the identity (`key_columns` / `keyless`) has been established from an
    #: identity-bearing event. A truncate carries no key and must not establish one.
    identified: bool = False
    #: the source relation this target belongs to, recorded so the applier can persist
    #: destination ownership in the same transaction that first creates the table
    #: (Codex 5).
    source_schema: str | None = None
    source_table: str | None = None
    #: A DELETE makes a key unavailable for a later sparse UPDATE in the same source
    #: transaction.  A following INSERT clears the tombstone and is a legal physical
    #: key reuse; an UPDATE must never silently reuse the destination's START row.
    deleted_keys: set[tuple] = field(default_factory=set)


def work_for(
    work: dict[str, TableWork], target: str, event: PendingRecord, snapshot: bool,
) -> TableWork:
    """The `TableWork` for `target`, created on first sight.

    One map per commit group, shared by the in-memory and the staged path, so the
    merge sees the whole group at once and in source order (Opus B-1).

    The identity is taken from the first event that HAS one. A truncate event carries
    no message key (Debezium sends truncates to the table topic with a null key
    schema, `EventDispatcher.java:526`), and reading that absent key as "this table is
    keyless" would give a keyed table the keyless identity for the whole group.
    """
    item = work.get(target)
    if item is None:
        item = TableWork(
            target=target,
            snapshot=snapshot,
        )
        work[target] = item
    if item.source_schema is None and event.schema:
        item.source_schema = event.schema
        item.source_table = event.table
    if not item.identified and event.kind != KIND_TRUNCATE:
        item.identified = True
        item.keyless = event.key is None
        item.key_columns = (
            tuple(naming.normalize(k) for k in event.key)
            if event.key
            else (CDCF_EVENT_ID,)
        )
    return item


def _key_token(
    item: TableWork,
    values: tuple,
    descriptors: dict[str, Any] | None,
    *,
    update_native: bool = True,
) -> tuple:
    descriptors = descriptors or {}
    token: list[str] = []
    resolved_descriptors: dict[str, Any] = {}
    for column, value in zip(item.key_columns, values, strict=True):
        descriptor = descriptors.get(column) or item.descriptors.get(column)
        if descriptor is not None:
            resolved_descriptors[column] = descriptor
            fingerprint = descriptor.fingerprint
        else:
            raise SchemaEvolutionRefused(
                f"{item.target}: catalog descriptor is missing for key column "
                f"{column!r}; the source unit is held for automatic retry",
                source_schema=item.source_schema,
                source_table=item.source_table,
                target=item.target,
            )
        try:
            rendered = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        except (TypeError, ValueError):
            rendered = repr(value)
        token.append(f"{fingerprint}:{rendered}")
        if update_native and descriptor is not None and (
            column not in item.native_columns or column in item.key_columns
        ):
            from .typed_types import native_type

            item.descriptors[column] = descriptor
            item.native_columns[column] = native_type(
                descriptor, for_key=column in item.key_columns
            )
            item.native_fingerprints[column] = descriptor.fingerprint
            item.columns[column] = item.native_columns[column].sql
    result = tuple(token)
    item.key_values[result] = tuple(values)
    item.key_descriptors[result] = resolved_descriptors
    return result


def _raw_key(item: TableWork, key: tuple) -> tuple:
    return getattr(item, "key_values", {}).get(key, key)


def row_for(
    event: PendingRecord, commit_id: int, event_id: str, *, snapshot: bool
) -> dict[str, Any]:
    """The bindable compatibility view of one typed sparse image."""
    return patch_for(event, commit_id, event_id, snapshot=snapshot).encoded_values()


def patch_for(
    event: PendingRecord,
    commit_id: int,
    event_id: str,
    *,
    snapshot: bool,
    binary_mode: str = "base64",
    hstore_mode: str = "map",
) -> RowPatch:
    """Decode an event into a RowPatch before any destination conversion.

    The patch retains marker/absent state for folding and spill digests, while its
    bindable view omits unchanged TOAST fields entirely.
    """
    return RowPatch.from_event(
        event,
        commit_id=commit_id,
        event_id=event_id,
        snapshot=snapshot,
        binary_mode=binary_mode,
        hstore_mode=hstore_mode,
    )


# --------------------------------------------------------------------------- #
# the fold
# --------------------------------------------------------------------------- #
def collect(
    item: TableWork,
    event: PendingRecord,
    row: dict[str, Any],
    event_id: str,
    *,
    probe=None,
    patch: RowPatch | None = None,
) -> None:
    """Fold one event's physical operation into the table's plan.

    `probe` answers the only two questions the event stream cannot: whether the
    destination holds a row under a key (`probe.start_exists`) and whether the
    destination's row under that key is the one a delete's before-image describes
    (`probe.start_matches`). Both are asked only where two entries compete for one
    key, so a group with no key reuse issues no extra queries at all.
    """
    if event.kind == KIND_TRUNCATE:
        truncate(item)
        return
    patch = patch or patch_for(event, 0, event_id, snapshot=item.snapshot)
    descriptors = event.before_descriptors if event.op == "d" else event.after_descriptors
    for column, field_value_item in patch.fields.items():
        source_name = column
        descriptor = field_value_item.descriptor or descriptors.get(source_name)
        if (
            descriptor is not None
            and field_value_item.state is not FieldState.ABSENT
            and source_name not in APPLIER_COLUMN_TYPES
        ):
            from .typed_types import native_type

            fingerprint = descriptor.fingerprint
            if item.native_fingerprints.get(column) != fingerprint:
                item.descriptors[column] = descriptor
                item.native_columns[column] = native_type(
                    descriptor, for_key=column in item.key_columns
                )
                item.native_fingerprints[column] = fingerprint
            item.columns[column] = item.native_columns[column].sql
        elif source_name in APPLIER_COLUMN_TYPES:
            item.columns[column] = APPLIER_COLUMN_TYPES[source_name]
        elif field_value_item.state in {FieldState.VALUE, FieldState.EXPLICIT_NULL}:
            raise SchemaEvolutionRefused(
                f"{item.target}: catalog descriptor is missing for source column "
                f"{source_name!r}; the source unit is held for automatic retry",
                source_schema=item.source_schema,
                source_table=item.source_table,
                target=item.target,
            )
    item.events += 1
    if patch.has_marker() and event.op in {"c", "r"}:
        _missing_toast_base(item, event)
    if item.keyless:
        # A keyless table is a changelog (ADR §15/A12): one row per event, identified
        # by `cdcf_event_id`, and a delete is a row like any other. The identity is
        # unique per event, so no attribution question can arise and no `START` is
        # needed - the keyed DELETE of that one event id is what makes a replay
        # idempotent.
        if patch.has_marker():
            _missing_toast_base(item, event, reason="keyless sparse change has no safe base")
        item.live[(event_id,)] = [patch.encoded_values()]
        return

    current_values = tuple(
        (event.key or {}).get(column) for column in item.key_columns
    )
    current_descriptors = dict(event.after_descriptors or {})
    current_descriptors.update(event.key_descriptors or {})
    key = _key_token(item, current_values, current_descriptors)
    if event.op == "d":
        before_values = tuple(
            (event.before or {}).get(column) for column in item.key_columns
        )
        key = _key_token(
            item,
            before_values,
            event.before_descriptors,
            update_native=False,
        )
        _remove(
            item,
            key,
            event.before,
            probe,
            descriptors=event.before_descriptors,
            typed=event.typed_before,
        )
        return

    if event.op == "u":
        # A primary-key UPDATE that reaches us as a single `u` (not Postgres - see
        # rubric 1.4's README - but other connectors and older versions do this)
        # carries the OLD key in `before`. The old key's row has to be REMOVED FROM
        # THE PLAN, not just deleted at the destination: a row this same group placed
        # under it would otherwise be re-inserted and the destination would hold the
        # row twice.
        if event.before and all(k in event.before for k in event.key):
            old_values = tuple(event.before.get(column) for column in item.key_columns)
            old = _key_token(
                item,
                old_values,
                event.before_descriptors,
                update_native=False,
            )
            if old != key:
                removed = _remove(
                    item,
                    old,
                    event.before,
                    probe,
                    descriptors=event.before_descriptors,
                    typed=event.typed_before,
                )
                if isinstance(removed, RowMove):
                    _place(
                        item,
                        key,
                        patch.encoded_values(),
                        patch=removed.patch.compose(patch),
                        move_from=removed.source_key,
                    )
                elif removed is not None and removed is not START:
                    source = removed if isinstance(removed, RowPatch) else RowPatch(
                        {name: FieldValue.of(value) for name, value in removed.items()},
                        complete=True,
                    )
                    combined = source.compose(patch)
                    _place(item, key, combined.encoded_values(), patch=combined)
                else:
                    _place(item, key, patch.encoded_values(), patch=patch, move_from=old)
                return
        # An `u` with an unchanged key REPLACES a row; it never adds one, whether or
        # not a before-image came with it. Treating an image-less `u` as a place is
        # how three updates of one key inside one transaction became three live rows.
        _update(
            item, key, patch.encoded_values(), event.before, probe,
            patch=patch, descriptors=event.before_descriptors, typed=event.typed_before,
        )
        return
    _place(item, key, patch.encoded_values(), patch=patch)


def truncate(item: TableWork) -> None:
    """Fold one `op="t"` event: every row that wore any key is gone.

    Postgres semantics, and they are exact: `TRUNCATE` inside a transaction removes
    every row that existed when it ran, including rows the same transaction inserted
    before it. So the plan drops every live entry - including `START`, which is the
    fact the previous fold never recorded and the reason a post-truncate key reuse
    asked the destination a question whose truthful answer had become "no" (Opus
    BLOCKER-1). Rows collected *after* the truncate survive, and the `DELETE FROM`
    that `write` issues covers whatever the destination still held.
    """
    item.truncated = True
    item.truncates += 1
    item.events += 1
    item.truncate_marks.append(sum(_concrete(entries) for entries in item.live.values()))
    item.live.clear()
    item.multi.clear()
    item.start_present.clear()
    item.deleted_keys.clear()


def end_transaction(item: TableWork) -> None:
    """Assert the source-transaction invariant the fold relies on.

    A unique key admits exactly one row per key at every transaction *boundary* -
    a deferred constraint relaxes it only *inside* a transaction. Two concrete rows
    left wearing one key therefore mean the fold's attribution went wrong or the
    stream is not what it claims, and either way committing it would durably record
    a duplicate. Called at every unit boundary, so the group fails at the boundary
    that broke rather than at the write.
    """
    if not item.multi:
        return
    keys = sorted(item.multi, key=repr)
    raise AmbiguousDelete(
        f"{item.target}: {len(keys)} identity key(s) end a source transaction wearing "
        f"two or more rows (first: {keys[0]!r}). A unique key admits one row per key "
        "at a transaction boundary, so this fold would durably commit a duplicate "
        "(ADR 0001 §18/A35).",
        source_schema=item.source_schema,
        source_table=item.source_table,
        target=item.target,
    )


def _entries(item: TableWork, key: tuple) -> list:
    """The live rows under `key`, created on first sight.

    First sight seeds `START`: the destination may hold a row under this key, and
    which one a later delete removes depends on it. After a truncate it cannot,
    so nothing is seeded.
    """
    entries = item.live.get(key)
    if entries is None:
        entries = [] if item.truncated else [START]
        item.live[key] = entries
    return entries


def _place(
    item: TableWork,
    key: tuple,
    row: dict,
    *,
    patch: RowPatch | None = None,
    move_from: tuple | None = None,
) -> None:
    item.deleted_keys.discard(key)
    entries = _entries(item, key)
    entries.append(
        RowMove(move_from, key, patch)
        if move_from is not None and patch is not None
        else (patch if patch is not None else row)
    )
    _track(item, key, entries)


def _update(
    item: TableWork,
    key: tuple,
    row: dict,
    before,
    probe,
    *,
    patch: RowPatch | None = None,
    descriptors=None,
    typed=None,
) -> None:
    """An UPDATE with an unchanged key: the same physical row, with new values."""
    if key in item.deleted_keys:
        _missing_toast_base(item, None, reason="UPDATE follows DELETE for the same physical key")
    entries = _entries(item, key)
    if entries == [START]:
        _resolve_start(item, key, entries, probe)
    if not entries:
        _missing_toast_base(item, None, reason="UPDATE has no destination base row")
    index = _target_entry(
        item, key, entries, before, probe, "update", descriptors=descriptors, typed=typed
    )
    patch = patch or RowPatch({name: FieldValue.of(value) for name, value in row.items()})
    if index is None:
        entries[:] = [patch]
    else:
        current = entries[index]
        if isinstance(current, RowPatch):
            entries[index] = current.compose(patch)
        elif current is START:
            entries[index] = patch
        elif isinstance(current, RowMove):
            current.patch = current.patch.compose(patch)
        else:
            current.update(row)
    _track(item, key, entries)


def _remove(item: TableWork, key: tuple, before, probe, *, descriptors=None, typed=None):
    """Rubric 1.4's hard case: which of the rows wearing `key` did this delete take?

    The two orderings below are byte-identical event streams with opposite answers,
    and what separates them is not "did the key exist before" but *which row* the
    delete's before-image describes:

    | one transaction | events | truth |
    |---|---|---|
    | `UPDATE t SET id = id + 1` over rows 1,2 (DEFERRABLE key) | `d(1,a) c(2,a) d(2,b) c(3,b)` | `{2:a, 3:b}` |
    | `UPDATE … id=2 WHERE id=1; UPDATE … id=3 WHERE id=2` | `d(1,a) c(2,a) d(2,a) c(3,a)` | `{3:a}` |

    In the permutation `d(2)` describes row `b` - the row that wore key 2 before -
    so the row that just became 2 survives; in the chain it describes row `a`, the
    row the transaction itself put there.
    """
    entries = _entries(item, key)
    if entries == [START]:
        _resolve_start(item, key, entries, probe)
    index = _target_entry(
        item, key, entries, before, probe, "delete", descriptors=descriptors, typed=typed
    )
    removed = None
    if index is None:
        # Nothing left under the key that we know of. The keyed DELETE still runs, so
        # a destination row we never modelled is removed either way.
        entries.clear()
    else:
        removed = entries.pop(index)
    if not entries:
        item.deleted_keys.add(key)
    _track(item, key, entries)
    return removed


def _target_entry(
    item, key: tuple, entries: list, before, probe, what: str, *, descriptors=None, typed=None
) -> int | None:
    """Index of the entry `before` describes, or None for "collapse the key".

    Raises `AmbiguousDelete` when two entries could be it and nothing can choose.
    """
    if len(entries) > 1:
        _resolve_start(item, key, entries, probe)
    if not entries:
        return None
    if len(entries) == 1:
        return 0
    image = _distinguishing(item, before, descriptors=descriptors, typed=typed)
    concrete = _concrete(entries)
    descriptor_map = descriptors or {}
    collapsed_variant_null = any(
        _variant_null_value(value, descriptor_map.get(column) or item.descriptors.get(column))
        for column, value in (before or {}).items()
        if naming.normalize(column) not in item.key_columns
    )
    if collapsed_variant_null and concrete >= 1:
        raise _unattributable(
            item,
            key,
            entries,
            before,
            what,
            "the runtime collapses SQL NULL and JSONB root null into one null value",
        )
    if not image:
        # Nothing in the before-image separates the candidates. That is only ever the
        # case under `REPLICA IDENTITY DEFAULT` (or a fully TOASTed row), and a
        # non-deferrable key means at most one row can really wear this key - so
        # whichever entry this operation named, the key ends empty.
        if concrete <= 1:
            return None
        raise _unattributable(item, key, entries, before, what, "no distinguishing columns")
    matched = [
        index
        for index, entry in enumerate(entries)
        if _matches(item, key, entry, image, probe)
    ]
    if len(matched) == 1:
        return matched[0]
    if matched:
        # Byte-identical rows: every attribution leaves the same observable state, so
        # prefer `START` and let the destination's copy be the one that goes.
        for index in matched:
            if entries[index] is START:
                return index
        return matched[0]
    raise _unattributable(item, key, entries, before, what, "no candidate matches")


def _variant_null_value(value, descriptor) -> bool:
    if descriptor is None or str(descriptor.kind).lower() != "jsonb":
        return False
    from .typed_types import JsonbNull

    return value is None or isinstance(value, JsonbNull) or (
        isinstance(value, str) and value.strip().lower() == "null"
    )


def _resolve_start(item, key, entries: list, probe) -> None:
    """Drop `START` when the destination has no row under `key`.

    This is the one destination query the fold makes, and it runs during the fold -
    before the group has issued any DELETE or INSERT for the table - so what it reads
    is genuinely the pre-group state.
    """
    if not entries or entries[0] is not START:
        return
    if item.truncated:  # pragma: no cover - `_entries` never seeds START after one
        entries.pop(0)
        return
    present = item.start_present.get(key)
    if present is None:
        present = bool(probe is not None and probe.start_exists(item, key))
        item.start_present[key] = present
    if not present:
        entries.pop(0)


def _matches(item, key, entry, image: dict, probe) -> bool:
    if entry is START:
        # Compared at the destination, with each value bound to the destination
        # column's own type: a Python comparison of a Debezium JSON value against a
        # value that has been through DuckDB's type system is not a comparison.
        answer = probe.start_matches(item, key, image) if probe is not None else None
        if answer is None:
            return False
        return answer
    if isinstance(entry, RowMove):
        entry = entry.patch
    if isinstance(entry, RowPatch):
        for column, value in image.items():
            observed = entry.field(column)
            if observed.state is FieldState.EXPLICIT_NULL:
                if value is not None:
                    return False
            elif observed.state is FieldState.VALUE:
                if observed.value != value:
                    return False
            else:
                return False
        return True
    return all(entry.get(column) == value for column, value in image.items())


def _distinguishing(item: TableWork, before, *, descriptors=None, typed=None) -> dict:
    """The before-image columns that can tell two rows under one key apart."""
    if not before:
        return {}
    out = {}
    descriptors = descriptors or {}
    typed_fields = dict(typed.fields) if typed is not None else {}
    for column, value in before.items():
        name = naming.normalize(column)
        descriptor = descriptors.get(column) or descriptors.get(name) or item.descriptors.get(name)
        disposition = typed_fields.get(column) or typed_fields.get(name)
        if disposition is not None and disposition.state in {
            FieldState.UNCHANGED_TOAST,
            FieldState.ABSENT,
        }:
            # A typed sidecar is authoritative even when the legacy raw image still
            # contains a connector marker.  The marker is a no-op, not a value that
            # may be bound into the destination probe for attribution.
            continue
        if name in item.key_columns or is_structural_marker(value, descriptor):
            continue
        out[name] = value
    return out


def _unattributable(item, key, entries, before, what, why) -> AmbiguousDelete:
    return AmbiguousDelete(
        f"{item.target}: cannot attribute a {what} of identity {key!r} - {why}. "
        f"{len(entries)} row(s) wear that key inside this source transaction and the "
        f"before-image {before!r} does not say which one this event describes. "
        "Folding a guess would durably commit a wrong answer, so the commit group is "
        "refused and replays instead (ADR 0001 §18/A35). A deferred unique constraint "
        "requires REPLICA IDENTITY FULL, which supplies the image this needs.",
        source_schema=item.source_schema,
        source_table=item.source_table,
        target=item.target,
    )


def _missing_toast_base(item: TableWork, event: PendingRecord | None, *, reason: str = "") -> None:
    detail = reason or "a sparse TOAST patch has no verified physical-row base"
    raise ToastBaseMissing(
        f"{item.target}: {detail}; refusing the commit group so automatic "
        "table-scoped refetch/resnapshot can repair the base",
        source_schema=item.source_schema,
        source_table=item.source_table,
        target=item.target,
    )


def _concrete(entries: list) -> int:
    if not entries:
        return 0
    return len(entries) - 1 if entries[0] is START else len(entries)


def _track(item: TableWork, key: tuple, entries: list) -> None:
    if len(entries) > 1 and _concrete(entries) > 1:
        item.multi.add(key)
    elif item.multi:
        item.multi.discard(key)


# --------------------------------------------------------------------------- #
# writing the plan
# --------------------------------------------------------------------------- #
def write(con, registry, item: TableWork, created_in_txn: set[str]) -> None:
    """Apply one table's plan: `ensure` the shape, delete the touched keys, insert.

    Runs on the caller's connection and never opens or closes a transaction: the
    commit group owns that (principle 4).
    """
    if item.truncated and not item.live and not registry.get(item.target).exists:
        # A truncate of a table this destination has never held. There is nothing to
        # empty and nothing to insert, and CREATEing an empty table from a truncate
        # would invent a shape out of an event that carries no columns at all.
        item.rows_removed = 0
        _finish_truncate_audit(item)
        return
    # Every source column must have a catalog descriptor. Only the applier's own
    # control columns have fixed types here; source shape is never inferred from a
    # Python value.
    columns = {col: ctype or apply_sql.VARCHAR for col, ctype in item.columns.items()}
    columns.update(APPLIER_COLUMN_TYPES)
    for column in item.key_columns:
        columns.setdefault(column, apply_sql.VARCHAR)

    if item.native_columns:
        typed_columns = {**columns, **item.native_columns}
        try:
            table, created = registry.ensure_typed(
                item.target, columns=typed_columns, key_columns=item.key_columns
            )
        except SchemaEvolutionRefused as refused:
            refused.source_schema = refused.source_schema or item.source_schema
            refused.source_table = refused.source_table or item.source_table
            refused.target = refused.target or item.target
            raise
    elif item.truncated and registry.get(item.target).exists:
        # A pure TRUNCATE carries no row image from which a source descriptor could be
        # obtained. Reuse the already-known destination shape for the table-level
        # operation; this is not a schema-creation or value-inference path.
        table, created = registry.get(item.target), False
    else:
        raise SchemaEvolutionRefused(
            f"{item.target}: catalog-authoritative descriptors are incomplete; "
            "the production typed path cannot create or write an untyped table",
            source_schema=item.source_schema,
            source_table=item.source_table,
            target=item.target,
        )
    if created:
        created_in_txn.add(item.target)
    # A table this transaction created is empty, so the DELETE half of the merge
    # cannot match anything: skipping it turns a snapshot into a pure bulk insert
    # instead of N key probes against a growing table.
    fresh = item.target in created_in_txn

    column_order = [c for c in table.columns if c in columns] + [
        c for c in columns if c not in table.columns
    ]
    column_order = list(dict.fromkeys(column_order))

    delete_keys, rows, partial_updates, moves = _plan(item)

    if item.truncated:
        # Rubric 1.5, "replicated just like Postgres handles them": the destination
        # table is emptied, and the rows this group collected *after* the truncate are
        # inserted below. `DELETE FROM` rather than `TRUNCATE`: it is unambiguously
        # transactional on both DuckDB and MotherDuck, and the whole point is that a
        # rolled-back commit group leaves the rows in place.
        item.rows_removed = 0 if fresh else _delete_all(con, table)
    elif not item.snapshot and not fresh and delete_keys:
        keys = [
            tuple(
                _key_value(table, col, value)
                for col, value in zip(item.key_columns, _raw_key(item, key), strict=False)
            )
            for key in delete_keys
        ]
        apply_sql.delete_keys(con, table, item.key_columns, keys)
    _finish_truncate_audit(item)

    updates: list[tuple[tuple, dict[str, Any]]] = []
    for key, patch in partial_updates:
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, _raw_key(item, key), strict=False)
        )
        updates.append((source_key, patch.bindable_values()))
    for move in moves:
        assignments = move.patch.bindable_values()
        target_key = _raw_key(item, move.target_key)
        source_key_values = _raw_key(item, move.source_key)
        for column, value in zip(item.key_columns, target_key, strict=False):
            assignments[column] = value
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, source_key_values, strict=False)
        )
        updates.append((source_key, assignments))
    if updates:
        affected = 0 if fresh else apply_sql.update_rows(
            con, table, item.key_columns, updates
        )
        if fresh or affected != len(updates):
            _missing_toast_base(
                item,
                None,
                reason=(
                    f"sparse update matched {affected} of {len(updates)} "
                    "destination base row(s)"
                ),
            )

    apply_sql.insert_rows(
        con,
        table,
        column_order,
        [
            [
                (
                    _typed_value(table, col, row.get(col))
                    if table.native_types and col in table.native_types
                    else apply_sql.bind(row.get(col), table.columns.get(col, apply_sql.VARCHAR))
                )
                for col in column_order
            ]
            for row in rows
        ],
    )
    # A no-op when the destination accepted the PRIMARY KEY on the identity columns
    # (it then rejects a duplicate on the INSERT itself). Where it could not, this is
    # what keeps "duplication is impossible" enforced by the destination rather than
    # asserted by us (Opus M-2).
    try:
        apply_sql.assert_identity_is_unique(con, table)
    except DestinationIdentityCollision as collision:
        # Rubric 4.7: the same permanent-loop shape as `AmbiguousDelete`. The group must
        # roll back, and then it replays into the same collision for ever unless somebody
        # rebuilds the table. Naming the source relation is what lets the applier queue
        # that rebuild automatically.
        collision.source_schema = item.source_schema
        collision.source_table = item.source_table
        collision.target = item.target
        raise


def _typed_value(table, column: str, value):
    """Bind a source value to the current physical native declaration."""
    from .typed_types import adapt_value

    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if native is None or source is None:
        return value
    return adapt_value(value, native)


def _key_value(table, column: str, value):
    """Encode a key using the same source descriptor as the row path."""
    from .typed_types import adapt_value

    if table.internal_identity:
        # Source keys are resolved to the deterministic cdcf_internal_id by
        # apply_sql. Keeping the source value raw here lets that resolver try the
        # descriptor that emitted the old UNION member as well as the current one.
        return value

    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if native is None or source is None:
        return apply_sql.bind(value, table.columns.get(column, apply_sql.VARCHAR))
    return adapt_value(value, native)


def _plan(item: TableWork) -> tuple[
    list[tuple],
    list[dict],
    list[tuple[tuple, RowPatch]],
    list[RowMove],
]:
    """Read the physical fold into full inserts, sparse updates, and key moves.

    A key whose only live entry is `START` is deliberately **absent from both**: the
    row the destination holds is the row the source holds, nothing in the group
    changed it, and deleting it would be the loss Opus BLOCKER-2 measured.
    """
    delete_keys: list[tuple] = []
    rows: list[dict] = []
    partial_updates: list[tuple[tuple, RowPatch]] = []
    moves: list[RowMove] = []
    for key, entries in item.live.items():
        if len(entries) == 1 and entries[0] is START:
            continue
        if not entries:
            delete_keys.append(key)
            continue
        if len(entries) == 1:
            entry = entries[0]
            if isinstance(entry, RowMove):
                moves.append(entry)
            elif isinstance(entry, RowPatch) and not entry.complete:
                partial_updates.append((key, entry))
            else:
                delete_keys.append(key)
                rows.append(entry.encoded_values() if isinstance(entry, RowPatch) else entry)
        elif entries:
            concrete = [entry for entry in entries if entry is not START]
            if len(concrete) > 1:  # pragma: no cover - `end_transaction` catches it
                raise AmbiguousDelete(
                    f"{item.target}: identity {key!r} would be written twice by one "
                    "commit group (ADR 0001 §18/A35)",
                    source_schema=item.source_schema,
                    source_table=item.source_table,
                    target=item.target,
                )
            for entry in concrete:
                if isinstance(entry, RowPatch) and not entry.complete:
                    partial_updates.append((key, entry))
                elif isinstance(entry, RowMove):
                    moves.append(entry)
                else:
                    delete_keys.append(key)
                    rows.append(
                        entry.encoded_values() if isinstance(entry, RowPatch) else entry
                    )
    move_sources = {move.source_key for move in moves}
    delete_keys = [key for key in delete_keys if key not in move_sources]
    return delete_keys, rows, partial_updates, moves


def _finish_truncate_audit(item: TableWork) -> None:
    """Freeze what each truncate of this group removed (Codex 2).

    Positional and immutable, because the markers are written after every table has
    been applied: holding a reference to the mutable plan and reading one field made
    two truncates in one transaction report the same number.
    """
    counts: list[int | None] = list(item.truncate_marks)
    if counts:
        counts[0] = None if item.rows_removed is None else counts[0] + item.rows_removed
    item.truncate_rows_removed = tuple(counts)


def _delete_all(con, table) -> int | None:
    """Empty one destination table inside the caller's transaction.

    Returns the number of rows removed when the destination reports it (DuckDB and
    MotherDuck both return a `Count` for a DELETE), so the truncate marker in
    `_cdc_flight.table_events` records what the destination actually lost. `None`
    when it cannot be read — the marker then says "unknown" rather than "0".
    """
    result = con.execute(f"DELETE FROM {table.qualified}")
    try:
        row = result.fetchone()
    except Exception:  # pragma: no cover - a destination that returns no result set
        return None
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):  # pragma: no cover
        return None


def live_names(tables: set[str]) -> set[str]:
    """Report the table an operator knows about, not the shadow it landed in."""
    return {
        t[: -len(naming.SHADOW_SUFFIX)] if t.endswith(naming.SHADOW_SUFFIX) else t
        for t in tables
    }


def _as_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
