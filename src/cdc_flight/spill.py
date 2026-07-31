"""`_cdc_flight.spill_events` — the staging buffer for a unit too big for memory.

Extracted from `applier.py` unchanged in behaviour (Codex 8). The reason it is its
own module is the shape of the bug it used to have: the staging code reached into
the applier's snapshot state to *infer* whether a record was a snapshot record, and
that state is initialised by a different part of the same file, later. On the first
spilled chunk of every snapshot it therefore concluded "streaming" and staged the
rows into the **live** table with a streaming identity (Codex 1).

So the buffer takes `StagedEvent`s: an event, the identity it will carry, the table
it belongs in, and its ordinal within the unit. It infers **nothing**. Every one of
those four is decided by the caller, which is the only place that knows.

Staging and drain happen inside the commit group's own transaction (ADR §3.4), so
nothing is ever visible early and a crash rolls the staged rows back with
everything else - there is no orphan cleanup problem, and no separate durability
story to reason about.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from . import apply_sql
from .destination import CONTROL_SCHEMA
from .envelope import KIND_DATA, PendingRecord

log = logging.getLogger("cdc_flight.spill")

_COLUMNS = [
    "commit_id", "unit_seq", "event_seq", "target_table", "source_schema",
    "source_table", "lsn", "txn_id", "total_order", "cdcf_event_id", "op",
    "source_ts_ms", "before_json", "after_json", "key_json",
]
_TYPES = [
    apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.BIGINT, apply_sql.VARCHAR,
    apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BIGINT, apply_sql.VARCHAR,
    apply_sql.BIGINT, apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.BIGINT,
    apply_sql.VARCHAR, apply_sql.VARCHAR, apply_sql.VARCHAR,
]


@dataclass
class StagedEvent:
    """One event on its way to (or back from) the staging table.

    `seq` orders the events *within one unit* and is the connector's own ordinal -
    `transaction.total_order` for a streaming event, the snapshot arrival ordinal
    for a snapshot record. It is never a locally generated sequence: substituting
    one for missing connector identity is what would let a replay recompute a
    different `cdcf_event_id` (Codex 4).
    """

    event: PendingRecord
    event_id: str
    target: str
    seq: int | None = None


class SpillBuffer:
    """Reads and writes `_cdc_flight.spill_events`. Owns no policy."""

    def __init__(self, con):
        self.con = con
        #: rows currently staged for the open commit group
        self.rows = 0

    def stage(self, *, commit_id: int, unit_seq: int, prepared: list[StagedEvent]) -> int:
        """Insert `prepared` for `(commit_id, unit_seq)`. Returns rows written."""
        if not prepared:
            return 0
        rows = [
            [
                commit_id, unit_seq, staged.seq, staged.target,
                staged.event.schema, staged.event.table, staged.event.lsn,
                staged.event.txn_id, staged.event.total_order, staged.event_id,
                staged.event.op, staged.event.source_ts_ms,
                _json(staged.event.before), _json(staged.event.after), _json(staged.event.key),
            ]
            for staged in prepared
        ]
        apply_sql.bulk_insert(
            self.con, f"{CONTROL_SCHEMA}.spill_events", _COLUMNS, rows, _TYPES
        )
        self.rows += len(rows)
        return len(rows)

    def load(self, *, commit_id: int, unit_seq: int) -> list[StagedEvent]:
        """One unit's staged rows, in source order.

        `ORDER BY event_seq` within a unit is source order, and the caller loads a
        unit's prefix immediately before collecting that unit's in-memory tail, so
        the whole group ends up applied in one totally ordered pass (Opus B-1).
        """
        staged = self.con.execute(
            f"SELECT target_table, source_schema, source_table, lsn, txn_id, total_order, "
            "       cdcf_event_id, op, source_ts_ms, before_json, after_json, key_json, "
            "       event_seq "
            f"FROM {CONTROL_SCHEMA}.spill_events WHERE commit_id = ? AND unit_seq = ? "
            "ORDER BY event_seq",
            [commit_id, unit_seq],
        ).fetchall()
        out: list[StagedEvent] = []
        for row in staged:
            (
                target, schema, table, lsn, txn_id, total_order, event_id, op,
                source_ts_ms, before_json, after_json, key_json, event_seq,
            ) = row
            out.append(
                StagedEvent(
                    event=PendingRecord(
                        raw=None, kind=KIND_DATA, topic="", nbytes=0, op=op, schema=schema,
                        table=table, lsn=lsn, txn_id=txn_id, total_order=total_order,
                        source_ts_ms=source_ts_ms,
                        key=json.loads(key_json) if key_json else None,
                        before=json.loads(before_json) if before_json else None,
                        after=json.loads(after_json) if after_json else None,
                    ),
                    event_id=event_id,
                    target=target,
                    seq=event_seq,
                )
            )
        return out

    def clear(self, commit_id: int) -> None:
        """Delete every staged row of the group, applied or fenced.

        A fenced unit's prefix is never *loaded* (Codex 5), so this is also what
        discards it - inside the same transaction, so it cannot outlive a rollback
        either way.
        """
        self.con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.spill_events WHERE commit_id = ?", [commit_id]
        )
        self.rows = 0


def _json(value) -> str | None:
    return json.dumps(value, default=str) if value else None
