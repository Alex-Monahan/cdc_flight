"""The run's completion watermark: the one fact that ends a run.

Why this exists (measured, `codex_logs/slowlane_rootcause.md`, 2026-08-13)
--------------------------------------------------------------------------
`run_engine_bounded` used to end a *successful* run on a **timer**: `--idle-seconds`
of silence, corroborated against `pg_replication_slots`. Silence is not a fact about
delivery, so the window had to be long enough to out-wait Debezium's 10 s
retriable-restart backoff (`source_health` explains why), and **every** run paid it
whether or not anything was still owed. One instrumented slow lane measured

    1,640.1 s — 37.8 % of the whole lane — inside that quiet window,
    across 218 runs that had nothing left to deliver

plus 572.7 s of runs that could never satisfy the predicate and burned their whole
`--max-seconds`. The measured floor of one pipeline run was `3.90 s + idle_seconds`,
so roughly four fifths of a typical run was waiting.

What replaces it
----------------
A **position**. The run writes one transactional marker to the source, takes the LSN
PostgreSQL assigned it, and ends at the instant the destination's durable resume
point is at or past that LSN.

    arm  ──►  write `pg_logical_emit_message(true, 'cdcf_completion_watermark', …)`
              PostgreSQL returns L, the position of that record in the WAL
    stop ──►  when `applier.resume_point.last_lsn >= L`

Why that is a proof and not an optimisation, in four steps:

1. **L is in the past of every later commit.** WAL is append-only, so any source
   transaction whose COMMIT record was written *before* we asked has an LSN < L.
2. **Logical decoding hands over whole transactions in commit order.** Every
   transaction with a commit LSN < L is therefore delivered to this run before any
   record at or past L.
3. **The applier applies whole units and commits groups in order**, and
   `resume_point` is assigned only *after* the destination `COMMIT` returns
   (`commit_protocol.commit_group`). So `resume_point.last_lsn >= L` means some
   record at or past L is durable, which means every transaction before it is
   durable too. That is Invariant O read forwards.
4. **L is reachable.** This is the reason for the marker, and the reason a bare
   `pg_current_wal_lsn()` will not do: nothing guarantees that an arbitrary WAL
   position is ever *delivered* to this slot — a publication that captures a subset
   of tables never sees most of it — and `pg_current_wal_lsn()` is cluster-wide, so
   an unrelated co-tenant database moves it (review r12 R12-3 measured that exact
   mistake costing every bounded run its entire `--max-seconds`: 60.44 s against
   10.55 s). The marker is OUR record on OUR slot, so it is guaranteed to arrive
   and no neighbour can move it.

Straddling transactions
-----------------------
**Whole transactions only, and the boundary is the COMMIT.** A transaction that began
before the watermark and commits after it has a commit LSN > L, so it is decoded
after the marker: it is entirely *outside* this run's watermark and is delivered by
the next run. It is never half-applied — the assembler only ever emits complete units
and the un-ENDed tail is discarded at shutdown under Invariant O.

If such a transaction is delivered anyway (it committed while we were waiting), the
watermark is **invalidated** rather than honoured: a run whose source is still
producing has not reached a quiescent point, so it takes a new position once the
stream is quiet again. That is what keeps this identical to the old timer for a live
writer while removing the wait for everyone else.

What still falls back to `--idle-seconds`
-----------------------------------------
A watermark needs one source write. When that is impossible the run has **no**
position to reach, so it keeps exactly the source-corroborated quiet window it always
had — `may_declare_idle` and its freshness confirmation, unchanged — and says
`completion_watermark: "unavailable"` in its summary. The cases are:

* the source cannot be written to: a hot standby, a role without permission on
  `pg_logical_emit_message`, or `CDC_COMPLETION_WATERMARK=0` / `CDC_CATALOG_MARKER=0`;
* the marker budget for this run is exhausted (`CDC_WATERMARK_MAX_WRITES`);
* there is no `SourceHealth` at all — the re-snapshot engine and the streaming-only
  fakes. A re-snapshot stops on `stop_when` (its last shadow is swapped in) and its
  slot is a throwaway whose offsets nobody reads, so a watermark there would be a
  position on a stream that is deliberately discarded.

What it is NOT
--------------
It is not a shortcut past any durability rule. The run still may not stop while the
applier is busy, before `min_records`, or before the snapshot phase has ended; the
slot-acknowledgement hand-off in the supervisor's `finally` still has to prove
`confirmed_flush_lsn` reached the durable position; and `--max-seconds` is still the
safety ceiling that stops this from ever becoming an unbounded wait (rubric 4.5).
A run that arms a watermark and never reaches it does **not** report success: it
fails loudly, which is the same B5 shape the timer's corroboration existed for, now
proved by arithmetic on a position instead of by a continuity window.
"""

from __future__ import annotations

import logging
import time

from .machines import (
    COMPLETION_WATERMARK,
    WATERMARK_ARMED,
    WATERMARK_REACHED,
    WATERMARK_UNARMED,
    WATERMARK_UNAVAILABLE,
)
from .source_marker import COMPLETION_WATERMARK as WATERMARK_REASON
from .source_marker import SourceMarker

log = logging.getLogger("cdc_flight.completion_watermark")

#: How long the stream must have been quiet before the run asks the source for a
#: position. It is NOT a completion timer: arming early is free, because a
#: watermark the source overtakes is discarded and retaken. It only stops a run
#: from writing a marker between two batches of a burst.
DEFAULT_QUIET_SECONDS = 0.5


class CompletionWatermark:
    """May this run stop yet? One object, one declared state, one answer.

    The supervision loop asks `reached(handler, elapsed)` once per tick and does
    nothing else with the question. Everything that used to be spread across the
    loop — the quiet timer, the source corroboration, the sampler-freshness
    confirmation window and their three `continue`s — lives here, behind
    `machines.COMPLETION_WATERMARK`.
    """

    def __init__(
        self,
        health,
        run,
        *,
        completion=None,
        marker=None,
        quiet_seconds: float = DEFAULT_QUIET_SECONDS,
    ):
        self.health = health
        self.run = run
        self.completion = completion
        self.marker = marker
        #: Never longer than the fallback window it replaces: a caller that asks
        #: for a sub-second idle window gets a sub-second arming delay too.
        self.quiet_seconds = max(0.0, min(quiet_seconds, run.idle_seconds))
        self.target_lsn: int | None = None
        self.arms = 0
        self.invalidations = 0
        self._state = COMPLETION_WATERMARK.initial
        self._armed_data_batches = 0
        self._idle_candidate_since: float | None = None
        self._blocked = 0

    @classmethod
    def for_run(cls, health, run, *, completion=None, prefix: str = "cdcf"):
        """The production constructor: one marker budget of this run's own.

        Deliberately NOT the catalog watcher's marker. A source that keeps
        committing while we watch it can consume watermark writes, and a fence
        that cannot be written is a `DROP TABLE` the destination never applies.
        """
        return cls(
            health,
            run,
            completion=completion,
            marker=SourceMarker(
                prefix=prefix,
                enabled=run.watermark_enabled,
                max_writes=run.watermark_max_writes or None,
            ),
            quiet_seconds=run.watermark_quiet_seconds,
        )

    # -- state -------------------------------------------------------------- #
    @property
    def state(self) -> str:
        return self._state

    def _to(self, state: str) -> None:
        COMPLETION_WATERMARK.check(self._state, state)
        self._state = state

    # -- the question ------------------------------------------------------- #
    def reached(self, handler, elapsed: float) -> bool:
        """True only when this run has a *completed delivery it can prove*."""
        if self._state == WATERMARK_REACHED:
            return True
        if not self._may_stop(handler):
            self._idle_candidate_since = None
            return False
        if self._state == WATERMARK_ARMED:
            if handler.data_batch_count != self._armed_data_batches:
                # A whole source transaction committed PAST this watermark, so
                # the watermark no longer describes a finished delivery. Take a
                # new one once the stream is quiet again.
                self.invalidations += 1
                self._to(WATERMARK_UNARMED)
                return False
            if self._durable(handler) >= (self.target_lsn or 0):
                self._to(WATERMARK_REACHED)
                log.info(
                    "the destination is durably past the run's completion watermark "
                    "(lsn=%s); stopping now", self.target_lsn,
                )
                return True
            return False
        if self._state == WATERMARK_UNARMED:
            if not self._markable():
                self._to(WATERMARK_UNAVAILABLE)
                log.warning(
                    "this run has no way to establish a completion watermark; "
                    "falling back to the --idle-seconds quiet window"
                )
            elif not self._ready_to_arm(handler):
                return False
            else:
                self.target_lsn = self._arm(handler)
                if self.target_lsn is not None:
                    self._armed_data_batches = handler.data_batch_count
                    self.arms += 1
                    self._to(WATERMARK_ARMED)
                    return False
                self._to(WATERMARK_UNAVAILABLE)
                log.warning(
                    "the source refused this run's completion watermark (%s); "
                    "falling back to the --idle-seconds quiet window",
                    getattr(self.marker, "last_error", None),
                )
        return self._quiet_window(handler, elapsed)

    # -- the watermark ------------------------------------------------------ #
    def _may_stop(self, handler) -> bool:
        """The preconditions no completion route may ever skip."""
        return (
            handler.record_count >= self.run.min_records
            and not handler.busy
            and (self.completion is None or self.completion.phase_ended)
        )

    def _markable(self) -> bool:
        """Can this run write to the source at all? A permanent property."""
        return (
            self.health is not None
            and self.marker is not None
            and self.marker.enabled
        )

    def _ready_to_arm(self, handler) -> bool:
        """Is now the moment to ask? A transient one.

        A marker written before the slot exists is WAL the slot never carries, so
        the run could never reach it. `ever_streamed` is the cheapest honest proof
        that our slot already exists: a walsender has been observed attached to
        it, and Debezium only attaches after creating it, so the record we are
        about to write is behind the slot's start position.
        """
        return (
            handler.seconds_since_last_batch >= self.quiet_seconds
            and self.health.ever_streamed
        )

    def _arm(self, handler) -> int | None:
        return self.health.emit_marker(
            self.marker,
            WATERMARK_REASON,
            {
                "slot": getattr(self.health, "slot_name", None),
                "delivered_lsn": getattr(handler, "highest_source_lsn", None),
            },
        )

    @staticmethod
    def _durable(handler) -> int:
        """The destination's durable position. Invariant O, read forwards."""
        return int(getattr(getattr(handler, "resume_point", None), "last_lsn", 0) or 0)

    # -- the declared fallback ---------------------------------------------- #
    def _quiet_window(self, handler, elapsed: float) -> bool:
        """`--idle-seconds` of silence the SOURCE corroborates. Unchanged.

        This is the whole of the old completion condition, moved rather than
        rewritten, because it is still the only answer available to a run whose
        source cannot be marked. Its three parts are (a) the quiet window itself,
        (b) `may_declare_idle`, which refuses to call a stream idle while the
        walsender has not been continuously attached for that window (Opus B5),
        and (c) a confirmation window of two sampler intervals, because the
        asynchronous sampler's last `active` observation can race a walsender
        termination at the exact idle boundary.
        """
        if handler.seconds_since_last_batch < self.run.idle_seconds:
            self._idle_candidate_since = None
            return False
        # Never stop before the connector has had a chance to start.
        if elapsed < min(self.run.idle_seconds, 5.0):
            return False
        if self.health is None:
            return True
        if not self._source_agrees(handler):
            self._idle_candidate_since = None
            self._blocked += 1
            if self._blocked % 20 == 1:
                log.warning(
                    "stream quiet for %.1fs but the source disagrees it is idle: %s",
                    handler.seconds_since_last_batch, self.health.summary(),
                )
            return False
        interval = getattr(self.health, "interval", None)
        if interval is None:
            return True
        now = time.monotonic()
        if self._idle_candidate_since is None:
            self._idle_candidate_since = now
            return False
        latest_at = getattr(getattr(self.health, "last", None), "at", None)
        if (
            now - self._idle_candidate_since < max(float(interval) * 2.0, 1.0)
            or latest_at is None
            or latest_at < self._idle_candidate_since
        ):
            return False
        # Re-run the actual proof at the end of the candidate window: freshness
        # alone cannot turn a walsender kill during that window into an idle
        # verdict.
        if not self._source_agrees(handler):
            self._idle_candidate_since = None
            return False
        return True

    def _source_agrees(self, handler) -> bool:
        return self.health.may_declare_idle(
            min_seconds=self.run.idle_seconds,
            # The per-slot backlog reference (round 13, review r12 R12-3): what
            # the connector delivered to THIS run, not what the cluster wrote.
            received_high_water=getattr(handler, "highest_source_lsn", None),
        )

    # -- observability ------------------------------------------------------ #
    def as_dict(self) -> dict:
        """What this run did to its source, reported rather than inferred.

        `arms` is always present because it is the *cost* of the mechanism: each
        one is a whole transaction the Flight wrote to a source it otherwise only
        reads, and Debezium delivers each of them back as three records
        (BEGIN, the message, END) that appear in `records`.
        """
        summary = {
            "completion_watermark": self._state,
            "completion_watermark_arms": self.arms,
        }
        if self.target_lsn is not None:
            summary["completion_watermark_lsn"] = self.target_lsn
        if self.invalidations:
            summary["completion_watermark_invalidations"] = self.invalidations
        return summary
