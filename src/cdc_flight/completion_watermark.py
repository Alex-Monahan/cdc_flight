"""The run's completion watermark: the one fact that ends a run.

Why this exists (measured, `codex_logs/slowlane_rootcause.md`, 2026-08-13)
--------------------------------------------------------------------------
`run_engine_bounded` used to end a *successful* run on a **timer**: `--idle-seconds`
of silence, corroborated against `pg_replication_slots`. Silence is not a fact about
delivery, so the window had to out-wait Debezium's 10 s retriable-restart backoff
(`source_health` explains why), and **every** run paid it whether or not anything was
still owed. One instrumented slow lane measured **1,640.1 s — 37.8 % of the whole
lane — inside that quiet window across 218 runs that had nothing left to deliver**,
plus 572.7 s of runs that could never satisfy the predicate and burned their whole
`--max-seconds`. The floor of one pipeline run was `3.90 s + idle_seconds`, so
roughly four fifths of a typical run was waiting.

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
   `pg_current_wal_lsn()` will not do: nothing guarantees an arbitrary WAL position
   is ever *delivered* to this slot — a publication capturing a subset of tables
   never sees most of it — and `pg_current_wal_lsn()` is cluster-wide, so a
   co-tenant database moves it (review r12 R12-3 measured that mistake costing every
   bounded run its entire `--max-seconds`: 60.44 s against 10.55 s). The marker is
   OUR record on OUR slot: guaranteed to arrive, and no neighbour can move it.

Straddling transactions, and what `stop_reason: "idle"` now means
-----------------------------------------------------------------
**Whole transactions only, and the boundary is the COMMIT.** A transaction that began
before the watermark and commits after it has a commit LSN > L, so it is decoded
after the marker: it is entirely *outside* this run's watermark and is delivered by
the next run. It is never half-applied — the assembler only ever emits complete units
and the un-ENDed tail is discarded at shutdown under Invariant O.

A position, once PostgreSQL has assigned it, is never withdrawn. An earlier version
invalidated an armed watermark when a data batch landed after it, claiming that kept a
live writer behaving exactly as it did under the timer. **Measured, that claim is
false** (review MINOR-3): a source writing every 1.5 s still produces quiet ticks,
still arms, and still ends in ~5.5 s mid-write. All the edge did was take a second
position when a batch landed in the ~100 ms between arming and durability. It is gone,
with its re-arm budget; `arms` is now 0 or 1 per run.

So state the claim honestly instead: a reached watermark says **"the destination is
complete as of position L"**, NOT "the source has stopped writing". `stop_reason:
"idle"` keeps its old spelling; the field carrying the real claim is
`completion_watermark` — `reached` on the position path, `unavailable` on the
quiet-window path. A caller needing "the source was quiescent" must read that one.

What still falls back to `--idle-seconds`
-----------------------------------------
A watermark needs one source write. When that is impossible the run has **no**
position to reach, so it keeps exactly the source-corroborated quiet window it always
had — `may_declare_idle` and its freshness confirmation, unchanged — and says
`completion_watermark: "unavailable"` in its summary. The cases are:

* the source cannot be written to: a hot standby, a role without permission on
  `pg_logical_emit_message`, or `CDC_COMPLETION_WATERMARK=0` — which is the ONLY
  knob that keeps a run read-only against its source. `CDC_CATALOG_MARKER=0` is
  **not** one: it governs the DDL fence, not the completion decision.
* there is no `SourceHealth` at all — the re-snapshot engine and the streaming-only
  fakes. A re-snapshot stops on `stop_when` (its last shadow is swapped in) and its
  slot is a throwaway whose offsets nobody reads, so a watermark there would be a
  position on a stream that is deliberately discarded.

What it is NOT
--------------
It is not a shortcut past any durability rule. The run still may not stop while the
applier is busy, before `min_records`, or before the snapshot phase has ended, and
the slot-acknowledgement hand-off in the supervisor's `finally` still has to prove
`confirmed_flush_lsn` reached the durable position.

`--max-seconds` remains the bound that stops this becoming an unbounded wait
(rubric 4.5) — and it is a CEILING, NOT AN EXIT PATH. On any run that could take a
position (`armed` and unreached, or `unarmed` because the source never stopped
committing) reaching the ceiling raises `EngineFailure` in
`supervisor.run_engine_bounded`. That closes the defect a review measured directly:
against a continuously-writing source the run returned `ok: true, stop_reason:
max_seconds, completion_watermark: unarmed` with 28 committed source rows absent
from the destination. Only `unavailable` keeps the older, weaker connector-health
rule, because a run that never had a position cannot be judged against one.
"""

from __future__ import annotations

import logging
import time

from . import faults
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

#: How long the stream must have been quiet before the run asks for a position.
#: Not a completion timer (it is capped by `--idle-seconds`) but load-bearing —
#: see `CompletionWatermark._ready_to_arm`.
DEFAULT_QUIET_SECONDS = 0.5


class CompletionWatermark:
    """May this run stop yet? One object, one declared state, one answer.

    The supervision loop asks `reached(handler, elapsed)` once per tick. Everything
    that used to be spread across it — the quiet timer, the source corroboration,
    the sampler-freshness window and their three `continue`s — lives here, behind
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
        clock_started_at: float | None = None,
    ):
        self.health = health
        self.run = run
        self.completion = completion
        self.marker = marker
        self._clock_started_at = (
            time.monotonic() if clock_started_at is None else clock_started_at
        )
        #: Never longer than the fallback window it replaces: a caller that asks
        #: for a sub-second idle window gets a sub-second arming delay too.
        self.quiet_seconds = max(0.0, min(quiet_seconds, run.idle_seconds))
        self.target_lsn: int | None = None
        self.arms = 0
        self._state = COMPLETION_WATERMARK.initial
        self._idle_candidate_since: float | None = None
        self._blocked = 0
        #: These are decision events, not a second copy of the state machine.  The
        #: reached timestamp says when the durable position became true; the stop
        #: timestamp is recorded by the supervisor only when it actually accepts the
        #: completion predicate.  Their interval excludes pipeline/JVM work before
        #: the watermark and therefore does not turn host load into a completion
        #: verdict.
        self._watermark_reached_at: float | None = None
        self._stop_at: float | None = None
        self._stop_condition: str | None = None
        self._watermark_to_stop_seconds: float | None = None
        self._idle_window_seconds: float | None = None

    @classmethod
    def for_run(
        cls,
        health,
        run,
        *,
        completion=None,
        prefix: str = "cdcf",
        clock_started_at: float | None = None,
    ):
        """The production constructor: a marker of this run's own.

        Deliberately NOT the catalog watcher's marker: a fence that cannot be
        written is a `DROP TABLE` the destination never applies, and the two
        must not share a failure. No write budget, because a run takes at most
        ONE position — `armed` is only ever left for `reached`.
        """
        return cls(
            health,
            run,
            completion=completion,
            marker=SourceMarker(prefix=prefix, enabled=run.watermark_enabled),
            quiet_seconds=run.watermark_quiet_seconds,
            clock_started_at=clock_started_at,
        )

    # -- state -------------------------------------------------------------- #
    @property
    def state(self) -> str:
        return self._state

    def _to(self, state: str) -> None:
        COMPLETION_WATERMARK.check(self._state, state)
        self._state = state
        if state == WATERMARK_REACHED:
            self._watermark_reached_at = time.monotonic()
        faults.runtime_state(watermark=state)
        if state == WATERMARK_ARMED:
            faults.matrix_crash("watermark_armed")
        elif state == WATERMARK_REACHED:
            faults.matrix_crash("watermark_reached")

    # -- the question ------------------------------------------------------- #
    def reached(self, handler, elapsed: float) -> bool:
        """True only when this run has a *completed delivery it can prove*."""
        if self._state == WATERMARK_REACHED:
            return True
        if not self._may_stop(handler):
            self._idle_candidate_since = None
            return False
        if self._state == WATERMARK_ARMED:
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

    def record_stop_decision(self, handler) -> None:
        """Record the completion predicate the supervisor actually accepted.

        ``completion_watermark`` is the state reached by the mechanism. This is a
        separate observation of the terminal decision: it distinguishes a position
        that became durable from a supervisor that did (or did not) wait for the
        fallback's quiet interval before breaking its loop.
        """
        if self._stop_at is not None:
            return
        self._stop_at = time.monotonic()
        if self._state == WATERMARK_REACHED:
            self._stop_condition = "watermark"
            if self._watermark_reached_at is not None:
                self._watermark_to_stop_seconds = (
                    self._stop_at - self._watermark_reached_at
                )
            return
        if self._state == WATERMARK_UNAVAILABLE:
            self._stop_condition = "idle_window"
            self._idle_window_seconds = handler.seconds_since_last_batch
            return
        raise RuntimeError(
            "the completion predicate returned true without a terminal watermark "
            f"state: {self._state!r}"
        )

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
            # A resnapshot and a read-only embedding caller deliberately have no
            # primary write route.  Keep their documented quiet-window fallback
            # even when the sampler cannot corroborate a watermark; a normal
            # streaming run always supplies the configured primary DSN and must
            # stay unarmed while the source is reconnecting.
            and getattr(self.health, "primary_dsn", None) is not None
        )

    def _ready_to_arm(self, handler) -> bool:
        """Is now the moment to ask? A transient one, and the ONLY one.

        A marker written before the slot exists is WAL the slot never carries, so
        the run could never reach it. `ever_streamed` is the cheapest honest proof
        that our slot already exists: a walsender has been observed attached to
        it, and Debezium only attaches after creating it, so the record we are
        about to write is behind the slot's start position.

        The quiet term is what makes the removed `armed -> unarmed` edge
        unnecessary: a position is taken only from a stream that has stopped
        handing batches over, so there is nothing to withdraw it for. Callback
        silence alone is not enough, though: the source sampler must corroborate
        the same quiet interval, so a walsender restart/backoff cannot arm a
        position in front of an undelivered backlog. A source that never stops
        committing therefore never gets a position at all, and such a run fails
        on its ceiling rather than reporting success.
        """
        return (
            handler.seconds_since_last_batch >= self.quiet_seconds
            and self.health.ever_streamed
            and self.health.may_declare_idle(
                min_seconds=self.quiet_seconds,
                received_high_water=getattr(handler, "highest_source_lsn", None),
            )
        )

    def _arm(self, handler) -> int | None:
        target = self.health.emit_marker(
            self.marker,
            WATERMARK_REASON,
            {
                "slot": getattr(self.health, "slot_name", None),
                "delivered_lsn": getattr(handler, "highest_source_lsn", None),
            },
        )
        if target is not None:
            faults.runtime_state(
                completion_marker_state="written", completion_marker_lsn=target
            )
            faults.matrix_crash("completion_marker_written")
        return target

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

        `arms` is always present because it is the *cost* of the mechanism: it is
        a whole transaction the Flight wrote to a source it otherwise only reads,
        and Debezium delivers it back as three records (BEGIN, the message, END)
        that appear in `records`. It is 0 or 1, never more.
        """
        summary = {
            "completion_watermark": self._state,
            "completion_watermark_arms": self.arms,
            "completion_stop_condition": self._stop_condition,
            "completion_watermark_reached_at_sec": (
                round(self._watermark_reached_at - self._clock_started_at, 3)
                if self._watermark_reached_at is not None
                else None
            ),
            "completion_stop_at_sec": (
                round(self._stop_at - self._clock_started_at, 3)
                if self._stop_at is not None
                else None
            ),
            "completion_watermark_to_stop_sec": (
                round(self._watermark_to_stop_seconds, 3)
                if self._watermark_to_stop_seconds is not None
                else None
            ),
            "completion_idle_window_sec": (
                round(self._idle_window_seconds, 3)
                if self._idle_window_seconds is not None
                else None
            ),
        }
        if self.target_lsn is not None:
            summary["completion_watermark_lsn"] = self.target_lsn
        return summary
