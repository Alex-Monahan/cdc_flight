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
from .envelope import PendingRecord
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


def work_for(
    work: dict[str, TableWork], target: str, event: PendingRecord, snapshot: bool
) -> TableWork:
    """The `TableWork` for `target`, created on first sight.

    One map per commit group, shared by the in-memory and the staged path, so the
    merge sees the whole group at once and in source order (Opus B-1).
    """
    item = work.get(target)
    if item is None:
        item = TableWork(target=target, keyless=event.key is None, snapshot=snapshot)
        item.key_columns = (
            tuple(naming.normalize(k) for k in event.key)
            if event.key
            else (CDCF_EVENT_ID,)
        )
        work[target] = item
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


def collect(item: TableWork, event: PendingRecord, row: dict[str, Any], event_id: str) -> None:
    """Fold one event's row into the table's plan."""
    for column, value in row.items():
        item.columns[column] = apply_sql.widen(
            item.columns.get(column), apply_sql.sql_type(value)
        )
    item.events += 1
    if item.keyless:
        key: tuple = (event_id,)
    else:
        key = tuple(event.key[k] for k in event.key)
        # A primary-key UPDATE under REPLICA IDENTITY FULL arrives as one event whose
        # `before` carries the OLD key. Touching both keys is what makes "delete old,
        # insert new" fall out of the normal path (rubric 1.4).
        if event.before and all(k in event.before for k in event.key):
            old = tuple(event.before[k] for k in event.key)
            if old != key:
                item.touched.setdefault(old, None)
    item.touched.setdefault(key, None)
    item.final[key] = None if (event.op == "d" and not item.keyless) else row


def write(con, registry, item: TableWork, created_in_txn: set[str]) -> None:
    """Apply one table's plan: `ensure` the shape, delete the touched keys, insert.

    Runs on the caller's connection and never opens or closes a transaction: the
    commit group owns that (principle 4).
    """
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

    if not (fresh or item.snapshot):
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
