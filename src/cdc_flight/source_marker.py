"""The one component that WRITES to the source: a transactional WAL marker.

cdc_flight otherwise only reads Postgres. Two features need a write, and they need
the *same* write (Opus Q3, ADR D9 / §18/A36):

| reason | who | why a WAL record has to appear |
|---|---|---|
| `catalog_fence` | rubric 1.5, `cdc_flight.catalog` | a `DROP TABLE` is detected out of band, and the action may only be applied once the destination has consumed everything before it. On a quiet source nothing would ever advance the durable LSN past the detection point. |
| `idle_heartbeat` | rubric 4.4 / 7.2, ADR D9 | a slot that only publishes a subset of tables never advances while other tables are busy, so the WAL it pins grows without bound. |
| `completion_watermark` | `cdc_flight.completion_watermark` | a run must END on a POSITION rather than on a timer. Nothing in the source's own WAL is guaranteed to be delivered to *this* slot, so the run writes the position it wants to reach. |

This is deliberately a small, complete component rather than one call inlined into
the catalog poller: 4.4 extends it (one more reason, its own cadence, a rate limit
shared with this one) rather than building a second writer with its own capability
probe, its own error state and its own routing to the primary.

**Transactional** (`pg_logical_emit_message(true, …)`), and that is a measured
decision rather than a preference. A non-transactional message looks like the obvious
choice — it carries no transaction id, so `TransactionMonitor.dataEvent` returns
early and it stays out of every `END.event_count` — but it does not end Debezium's
WAL-position search after a restart: `WalPositionLocator.resumeFromLsn` only stops
searching on a **COMMIT** whose LSN is past the stored one (`case MESSAGE:` logs and
falls through), and while it is searching `skipMessage()` drops every record.
MEASURED, 2026-07-31: a quiet run whose only new WAL was a non-transactional marker
delivered `records=0` with the slot 770 KB behind and never applied the drop, while
the same code with a committed transaction after the marker applied it in ~1 s.
That measurement is a constraint on D9 itself, not a local detail: a
non-transactional heartbeat cannot be relied on to advance an idle slot.

A transactional message arrives as BEGIN + `op="m"` + END, which is exactly the shape
D9's source heartbeat specifies and the assembler already proves whole (its
`data_collections` pseudo-entry is covered by `message_count`).

## The interface 4.4 extends

    marker = SourceMarker(prefix="cdcf", max_writes=60)
    marker.emit(conn, SourceMarker.CATALOG_FENCE, {"changes": [...]})   # -> bool
    marker.capable        # None until first attempt, then True/False
    marker.last_error     # the reason a write failed, kept until one succeeds
    marker.summary()      # counters for the run summary

`emit` never raises: a source that cannot be written to is an operational condition
(a read-only replica, a missing privilege), and the caller's job is to *not act* and
to say so — not to crash. It returns False so the caller can do exactly that.
"""

from __future__ import annotations

import json
import logging
import threading

log = logging.getLogger("cdc_flight.source_marker")

#: `pg_logical_emit_message` prefixes, one per reason, so a consumer can tell them
#: apart without parsing the payload.
CATALOG_FENCE = "catalog_fence"
IDLE_HEARTBEAT = "idle_heartbeat"
#: `cdc_flight.completion_watermark`: the position a run must reach before it may
#: say it is finished. The LSN PostgreSQL assigns this record IS the watermark.
COMPLETION_WATERMARK = "completion_watermark"
REASONS = (CATALOG_FENCE, IDLE_HEARTBEAT, COMPLETION_WATERMARK)


class SourceMarker:
    """Emits transactional logical-decoding messages on a source connection."""

    CATALOG_FENCE = CATALOG_FENCE
    IDLE_HEARTBEAT = IDLE_HEARTBEAT
    COMPLETION_WATERMARK = COMPLETION_WATERMARK

    def __init__(
        self,
        *,
        prefix: str = "cdcf",
        enabled: bool = True,
        max_writes: int | None = None,
    ):
        self.prefix = prefix
        self.enabled = enabled
        #: Opus MINOR-1: a fence that never opens would otherwise write one WAL record
        #: per poll for ever against a source we otherwise only read. `None` = no cap.
        self.max_writes = max_writes
        self.writes = 0
        self.suppressed = 0
        self.capable: bool | None = None
        self.last_error: str | None = None
        #: The LSN PostgreSQL assigned the most recent successful write, as a
        #: plain integer offset from `0/0`. `pg_logical_emit_message` returns it,
        #: so the caller does not have to guess a position from a second query
        #: whose answer a co-tenant database could have moved (review r12 R12-3).
        self.last_lsn: int | None = None
        self._lock = threading.Lock()

    def prefix_for(self, reason: str) -> str:
        return f"{self.prefix}_{reason}"

    @property
    def exhausted(self) -> bool:
        return self.max_writes is not None and self.writes >= self.max_writes

    def emit(self, conn, reason: str, payload: dict | None = None) -> bool:
        """Write one marker. Returns True only if the source accepted it."""
        if reason not in REASONS:  # pragma: no cover - programming error
            raise ValueError(f"unknown source-marker reason {reason!r}")
        if not self.enabled:
            return False
        with self._lock:
            if self.exhausted:
                self.suppressed += 1
                if self.suppressed == 1 or self.suppressed % 60 == 0:
                    log.error(
                        "source marker suppressed: %s markers already written for %s and "
                        "the condition has not cleared. Something downstream is not "
                        "consuming (see catalog_pending / replication lag); this process "
                        "will not keep writing to the source.",
                        self.writes, reason,
                    )
                self.last_error = (
                    f"marker budget exhausted after {self.writes} writes; the "
                    f"{reason} condition never cleared"
                )
                return False
        body = json.dumps(payload or {}, separators=(",", ":"), default=str)
        try:
            row = conn.execute(
                "SELECT (pg_logical_emit_message(true, %s, %s) - '0/0')::BIGINT",
                (self.prefix_for(reason), body),
            ).fetchone()
        except Exception as exc:
            self.capable = False
            self.last_error = f"{type(exc).__name__}: {exc}"
            log.error(
                "could not emit the %s marker (%s). Nothing that depends on a WAL "
                "record past this point can be applied; see rubric 1.5 / ADR D9.",
                reason, self.last_error,
            )
            return False
        with self._lock:
            self.writes += 1
            self.last_lsn = int(row[0]) if row and row[0] is not None else None
        self.capable = True
        self.last_error = None
        return True

    def summary(self) -> dict:
        return {
            "source_markers": self.writes,
            "source_marker_capable": self.capable,
            "source_marker_error": self.last_error,
            "source_markers_suppressed": self.suppressed,
        }
