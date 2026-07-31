"""The apply plan for one commit group: one `TableWork` per destination table.

Extracted from `applier.py` unchanged in behaviour (Codex 8), which leaves that
module to be the commit protocol - `BEGIN -> apply -> state -> COMMIT -> ack` -
and nothing else.

This is the layer where the two table shapes become one mechanism (ADR D6):

| table | identity | effect of a group |
|---|---|---|
| keyed | the source key columns | delete every key the group touched, insert the final row per key |
| keyless | `cdcf_event_id` | delete that event id if present, insert - so a replay cannot duplicate and two byte-identical source rows stay two rows |

`final` is a dict, so insertion order *is* source order and membership is O(1). It
used to be a dict paired with an `order` list and an `if key not in order` scan,
which is linear per event: MEASURED 458 s for one 200 000-event transaction, 1.6 s
after the change. `touched` carries the old key of a primary-key UPDATE as well as
the new one, which is what makes rubric 1.4 fall out of the normal path instead of
needing a special case.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from . import apply_sql, naming
from .envelope import KIND_TRUNCATE, PendingRecord
from .naming import CDCF_COMMIT_ID, CDCF_EVENT_ID, CDCF_TOTAL_ORDER

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


@dataclass
class TableWork:
    """Everything one destination table needs from one commit group."""

    target: str
    key_columns: tuple[str, ...] = ()
    keyless: bool = False
    columns: dict[str, str] = field(default_factory=dict)
    #: ordered, deduplicated identity keys touched by the group
    touched: dict[tuple, None] = field(default_factory=dict)
    #: identity key -> final row (None when the key's last event is a delete)
    final: dict[tuple, dict | None] = field(default_factory=dict)
    snapshot: bool = False
    events: int = 0
    #: keys whose PRE-GROUP row has already been consumed by a delete (or by the
    #: old-key half of a key change) inside this group. See `_remove`.
    absented: set[tuple] = field(default_factory=set)
    #: keys a NEW row moved onto during this group (an insert, or the new-key half
    #: of a key change). An UPDATE of a row that already wore the key is not one:
    #: that is the same row, and a later delete of it is unambiguous.
    acquired: set[tuple] = field(default_factory=set)
    #: cache of "did this key exist at the destination before the group?", so the
    #: probe runs at most once per ambiguous key.
    pre_existing: dict[tuple, bool] = field(default_factory=dict)
    #: rubric 1.5: the group truncated this table, so every row that predates the
    #: truncate is gone and only the rows collected *after* it survive.
    truncated: bool = False
    #: how many `op="t"` events the group carried for this table (a `TRUNCATE a, b`
    #: sends one per relation, so this is 1 per table in the normal case).
    truncates: int = 0
    #: filled in by `write`: how many destination rows the truncate removed, for the
    #: `_cdc_flight.table_events` marker.
    rows_removed: int | None = None
    #: True when the identity (`key_columns` / `keyless`) has been established from an
    #: identity-bearing event. A truncate carries no key and must not establish one.
    identified: bool = False


def work_for(
    work: dict[str, TableWork], target: str, event: PendingRecord, snapshot: bool
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
        item = TableWork(target=target, snapshot=snapshot)
        work[target] = item
    if not item.identified and event.kind != KIND_TRUNCATE:
        item.identified = True
        item.keyless = event.key is None
        item.key_columns = (
            tuple(naming.normalize(k) for k in event.key)
            if event.key
            else (CDCF_EVENT_ID,)
        )
    return item


def row_for(
    event: PendingRecord, commit_id: int, event_id: str, *, snapshot: bool
) -> dict[str, Any]:
    """The destination row for one change event, plus the applier's own columns."""
    image = event.after if event.op != "d" else event.before
    row: dict[str, Any] = {}
    for column, value in (image or {}).items():
        row[naming.normalize(column)] = value
    row[CDCF_COMMIT_ID] = commit_id
    row[CDCF_EVENT_ID] = event_id
    # A snapshot record has no transaction, so it has no ordinal. Leaving it NULL is
    # what tells a consumer "this identity is not txn-derived".
    row[CDCF_TOTAL_ORDER] = None if snapshot else event.total_order
    row["dbz_op"] = event.op
    row["dbz_lsn"] = event.lsn
    row["dbz_tx_id"] = None if snapshot else _as_int(event.txn_id)
    row["dbz_schema"] = event.schema
    row["dbz_table"] = event.table
    row["dbz_source_ts_ms"] = event.source_ts_ms
    return row


def collect(
    item: TableWork,
    event: PendingRecord,
    row: dict[str, Any],
    event_id: str,
    *,
    probe=None,
) -> None:
    """Fold one event's row into the table's plan.

    `probe(item, key) -> bool` answers "did this key exist at the destination before
    this commit group?". It is called only for the one ambiguous shape described in
    `_remove`, so a group with no key reuse issues no extra queries at all.
    """
    if event.kind == KIND_TRUNCATE:
        _truncate(item)
        return
    for column, value in row.items():
        item.columns[column] = apply_sql.widen(
            item.columns.get(column), apply_sql.sql_type(value)
        )
    item.events += 1
    if item.keyless:
        # A keyless table is a changelog (ADR §15/A12): one row per event, identified
        # by `cdcf_event_id`, and a delete is a row like any other.
        key: tuple = (event_id,)
        item.touched.setdefault(key, None)
        item.final[key] = row
        return

    key = tuple(event.key[k] for k in event.key)
    if event.op == "d":
        item.touched.setdefault(key, None)
        _remove(item, key, probe)
        return

    # A primary-key UPDATE that reaches us as a single `u` (not Postgres - see
    # rubric 1.4's README - but other connectors and older versions do this) carries
    # the OLD key in `before`. The old key has to be REMOVED FROM THE PLAN, not just
    # added to `touched`: a row this same group inserted under it would otherwise be
    # re-inserted and the destination would hold the row under both keys.
    key_changed = False
    if event.before and all(k in event.before for k in event.key):
        old = tuple(event.before[k] for k in event.key)
        if old != key:
            key_changed = True
            item.touched.setdefault(old, None)
            _remove(item, old, probe)
    item.touched.setdefault(key, None)
    if event.op != "u" or key_changed or key in item.absented:
        # A row that was not under this key before now is: an insert, the new-key
        # half of a key change, or a re-insert after this group deleted what was
        # here. That is what makes a *later* delete of this key ambiguous.
        item.acquired.add(key)
    item.final[key] = row


def _truncate(item: TableWork) -> None:
    """Fold one `op="t"` event: everything this group planned so far is gone.

    Postgres semantics, and they are exact: `TRUNCATE` inside a transaction removes
    every row that existed when it ran, including rows the same transaction inserted
    before it. So the plan drops its accumulated rows *and* its per-key bookkeeping —
    the `DELETE FROM <table>` that `write` issues instead of the keyed delete covers
    every key the group had touched — and rows collected *after* the truncate survive
    (`TRUNCATE t; INSERT …` in one transaction leaves the inserted rows).
    """
    item.truncated = True
    item.truncates += 1
    item.events += 1
    item.final.clear()
    item.touched.clear()
    item.absented.clear()
    item.acquired.clear()
    item.pre_existing.clear()


def _remove(item: TableWork, key: tuple, probe) -> None:
    """Record that `key` no longer holds the row it held. Rubric 1.4's hard case.

    The easy reading - "set `final[key] = None`" - is wrong exactly when this group
    has already *inserted* a row under `key`, because then two different rows have
    worn that key inside one transaction and the event stream does not say which one
    is being removed. Postgres does, and the two answers differ:

    | one transaction | events | truth |
    |---|---|---|
    | `UPDATE t SET id = id + 1` over rows 1,2 (DEFERRABLE key) | `d(1) c(2) d(2) c(3)` | `{2, 3}` |
    | `UPDATE … id=2 WHERE id=1; UPDATE … id=3 WHERE id=2` | `d(1) c(2) d(2) c(3)` | `{3}` |

    Byte-identical streams. What separates them is whether key 2 existed *before*
    the transaction: in the permutation the `d(2)` removes the pre-transaction row 2
    (and the row that just became 2 survives), in the chain it removes the row the
    transaction itself created.

    So: if the group inserted a row under `key` and a pre-group row under `key` is
    still unconsumed, this removal takes the **pre-group** row - which the merge's
    `DELETE … WHERE key IN touched` already performs - and the in-group row stays.
    Otherwise it takes the in-group row, and the plan drops it.

    A snapshot chunk cannot reach the ambiguous branch: it writes into a shadow
    table this transaction created and carries no deletes.
    """
    pending = item.final.get(key)
    ambiguous = (
        pending is not None
        and key in item.acquired
        and key not in item.absented
        and probe is not None
    )
    if not ambiguous:
        # Either there is no in-group row under this key, or the row under it is the
        # pre-group row itself (an UPDATE of it), or the pre-group row is already
        # spoken for. In every one of those the key simply ends the group absent.
        item.absented.add(key)
        item.final[key] = None
        return
    if item.pre_existing.get(key) is None:
        item.pre_existing[key] = bool(probe(item, key))
    item.absented.add(key)
    if not item.pre_existing[key]:
        item.final[key] = None


def write(con, registry, item: TableWork, created_in_txn: set[str]) -> None:
    """Apply one table's plan: `ensure` the shape, delete the touched keys, insert.

    Runs on the caller's connection and never opens or closes a transaction: the
    commit group owns that (principle 4).
    """
    if item.truncated and not item.final and not registry.get(item.target).exists:
        # A truncate of a table this destination has never held. There is nothing to
        # empty and nothing to insert, and CREATEing an empty table from a truncate
        # would invent a shape out of an event that carries no columns at all.
        item.rows_removed = 0
        return
    # A column every event left NULL tells us nothing about its type; VARCHAR is the
    # honest placeholder and `widen()` upgrades it the moment a real value shows up
    # (rubric 2.1/2.5 own the better answer).
    columns = {col: ctype or apply_sql.VARCHAR for col, ctype in item.columns.items()}
    columns.update(APPLIER_COLUMN_TYPES)
    for column in item.key_columns:
        columns.setdefault(column, apply_sql.VARCHAR)

    table, created = registry.ensure(
        item.target, columns=columns, key_columns=item.key_columns
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

    if item.truncated:
        # Rubric 1.5, "replicated just like Postgres handles them": the destination
        # table is emptied, and the rows this group collected *after* the truncate are
        # inserted below. `DELETE FROM` rather than `TRUNCATE`: it is unambiguously
        # transactional on both DuckDB and MotherDuck, and the whole point is that a
        # rolled-back commit group leaves the rows in place.
        item.rows_removed = 0 if fresh else _delete_all(con, table)
    elif not item.snapshot and not fresh:
        keys = [
            tuple(
                apply_sql.bind(value, table.columns.get(col, apply_sql.VARCHAR))
                for col, value in zip(item.key_columns, key, strict=False)
            )
            for key in item.touched
        ]
        apply_sql.delete_keys(con, table, item.key_columns, keys)

    rows: list[list] = []
    for row in item.final.values():
        if row is None:
            continue
        rows.append(
            [
                apply_sql.bind(row.get(col), table.columns.get(col, apply_sql.VARCHAR))
                for col in column_order
            ]
        )
    apply_sql.insert_rows(con, table, column_order, rows)
    # A no-op when the destination accepted the PRIMARY KEY on the identity columns
    # (it then rejects a duplicate on the INSERT itself). Where it could not, this is
    # what keeps "duplication is impossible" enforced by the destination rather than
    # asserted by us (Opus M-2).
    apply_sql.assert_identity_is_unique(con, table)


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
