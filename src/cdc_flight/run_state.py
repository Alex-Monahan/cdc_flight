"""Where a run is, and why it stopped — as two machines (rubric 1.9, ADR §20/A55).

Two states that were previously derived expressions over unnamed variables:

**`RunPhase`.** `pipeline.run()` is a 470-line function whose phases were readable only
as source-line position — `connecting → control_schema_ready → lease_held → …` lived in
local variables and in nothing durable at all. ADR §4.8 declared
`_cdc_flight.heartbeat.phase` and the table was created only in the 1.6—1.8 round; it
still had no writer. An operator could not ask the destination where a live run was.

**`RunOutcome`.** `stop_reason` was a nine-valued string assigned by plain `=` in eight
places, including inside a `finally`, with the precedence rule implemented as two copies
of `if stop_reason not in ("source_dark", "engine_error")` (`supervisor.py:180`, `:186`).
A49 recorded the exact defect that produced: a dark source makes `engine.close()` hang,
so the `finally` overwrote the **cause** with the **symptom** and `last_run.json`
reported `hung` for a blackholed Postgres. `RunOutcome` keeps the highest-precedence
value it has been given, so the overwrite is not a rule anyone has to remember — it is
an edge `machines.RUN_OUTCOME` does not have.

## What this is NOT

It is not the liveness/lag heartbeat writer. That is rubric 4.4/6.1's, with its own
cadence, its own lag arithmetic and its own source-side WAL heartbeat; this writes one
row per run and updates it on phase transitions. Splitting them keeps 1.9 from
pre-empting a design decision that belongs to 4.4.

## Where it is written

On an **independent connection** (`con.cursor()`, the property `AlertSink` establishes
and verifies), never inside the commit group's transaction and never inside the
commit→ack window. The binding principle is that the window between `COMMIT` and the
acknowledgement contains nothing but the acknowledgement; an observability write there
would be exactly the unrelated work that principle excludes. Every write is wrapped:
a heartbeat that cannot be written must never fail a run that is otherwise correct.

Two parts of that used to be aspiration rather than mechanism (Codex r1 MAJOR-3):

* **the window is now a wall-clock exclusion, not a program-order one.** The phase
  writer runs on the *supervisor's* thread, and `max_seconds` / engine error /
  source-dark all break out of the supervision loop and write `draining` without asking
  whether the engine thread happens to be between `COMMIT` and `markBatchFinished()`.
  `COMMIT_ACK` is entered by the applier around exactly that interval, through a gate
  the writer also holds for its check-and-write, so no write can be in flight when the
  window opens and none can begin while it is open. A phase write that arrives inside it
  is **dropped**; the next transition rewrites the whole row, so nothing is lost but a
  timestamp. The gate has no escape hatch — see `_CommitAckWindow`, and Codex r3
  MAJOR-2, which reproduced SQL running inside the window through the escape the first
  cut had.
* **there is no fallback to the primary connection.** `con.cursor()` failing used to set
  `_sink = con`, so a destination without cursors got phase writes on the applier's own
  connection, inside its open transaction, from another thread. The honest degradation
  is no row at all.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from contextlib import contextmanager

from .control_schema import CONTROL_SCHEMA
from .machines import (
    CONNECTION_RETIREMENT,
    OUTCOME_FAILURES,
    PHASE_FAILED,
    PHASE_STARTING,
    PHASE_STOPPED,
    RUN_OUTCOME,
    RUN_PHASE,
)
from .states import IllegalTransition

log = logging.getLogger("cdc_flight.run_state")

__all__ = ["COMMIT_ACK", "RunOutcome", "RunPhaseWriter"]


class _CommitAckWindow:
    """The interval between `COMMIT` and Debezium's acknowledgement, as a real gate.

    The first cut was a bare boolean, and a bare boolean cannot carry this claim
    (Codex r2 MAJOR-1). `RunPhaseWriter._write()` read the flag, then built a
    timestamp, then executed SQL — and a database call releases the GIL, so the applier
    could enter the window in between and the write landed inside the exact interval the
    ADR says excludes it. A two-thread barrier reproduced it.

    The protocol that does carry it is one mutex used asymmetrically:

    * an independent writer holds `_gate` for **check and write together**, so a write
      that starts before the window can be waited for, and a write that starts after it
      opens sees `_active` and drops;
    * the applier takes `_gate` in `enter()`, which happens **before `COMMIT`**. Waiting
      there costs nothing the principle protects: the window has not opened yet, and
      this is the same place `write_resume_point` and the offset fingerprint already
      run. It only delays *opening* the window until any write already in flight is
      finished.
    * `leave()` is a plain assignment, so the acknowledgement path itself takes no lock.

    **There is no escape.** The first cut let `enter()` give up after five seconds,
    open the window anyway and merely *count* the overlap — and the reviewer duly held
    `_execute()` past the bound and ran SQL inside the window (Codex r3 MAJOR-2). An
    instrumented violation of an absolute principle is still a violation, so the wait is
    unbounded here and the applier wraps it in the **commit watchdog** instead: an
    observability connection wedged long enough to threaten the commit path now kills the
    run loudly, with the same `EX_TEMPFAIL` a wedged `COMMIT` produces, rather than
    quietly overlapping. A bounded, loud death is an acceptable failure mode; a silent
    overlap is not.

    The **terminal** phase write is the one place a bounded wait is right: it runs after
    the applier has been shut down, it has no next transition to restore it, and blocking
    the run's own teardown on a stalled observability cursor would be the same mistake in
    the other direction. It waits `GATE_TIMEOUT`, then writes ungated and says so.
    """

    __slots__ = ("_active", "_gate", "dropped_writes", "ungated_terminal_writes")

    #: How long the TERMINAL phase write waits for the gate before writing without it.
    #: Never used by `enter()` — see the class docstring.
    GATE_TIMEOUT = 5.0

    def __init__(self) -> None:
        self._active = False
        self._gate = threading.Lock()
        #: how many observability writes this process declined because of the window.
        #: Surfaced in the run summary, so "we never wrote inside it" is measured.
        self.dropped_writes = 0
        #: how many terminal writes went ahead without the gate. Must be 0 on any
        #: healthy run; reported when it is not.
        self.ungated_terminal_writes = 0

    def enter(self) -> None:
        """Open the window. Blocks until no independent write is in flight.

        Called by the applier immediately BEFORE `COMMIT`, inside the commit watchdog,
        so this wait is bounded by `CDC_COMMIT_TIMEOUT` and a violation of that bound is
        a loud death rather than a silent overlap.
        """
        with self._gate:
            self._active = True

    def leave(self) -> None:
        self._active = False

    @contextmanager
    def excluded(self, timeout: float | None = None):
        """Hold the gate for one independent write. Yields whether it must be dropped.

        `timeout` is for the terminal write only: on expiry the write proceeds WITHOUT
        the gate and the fact is counted, because a terminal row that never lands is
        worse than a theoretical overlap after the applier has already stopped.
        """
        if timeout is None:
            with self._gate:
                yield self._active
            return
        if self._gate.acquire(timeout=timeout):
            try:
                yield self._active
            finally:
                self._gate.release()
            return
        self.ungated_terminal_writes += 1
        log.error(
            "the observability gate did not come free in %.1fs; writing the terminal "
            "phase without it rather than losing the terminal row", timeout,
        )
        yield False

    @property
    def active(self) -> bool:
        return self._active

    def wait_until_closed(self, timeout: float = GATE_TIMEOUT) -> bool:
        """Block until the window is shut, for a write that must not be dropped.

        Only the TERMINAL phase write uses this: a dropped `stopped`/`failed` write
        would leave the durable heartbeat non-terminal for ever, which is the one phase
        row an operator actually needs (Codex r2 MAJOR-1). It is safe to wait here
        because the applier has already been shut down by the time it is called.
        """
        deadline = time.monotonic() + timeout
        while self._active and time.monotonic() < deadline:
            time.sleep(0.01)
        return not self._active

    def reset(self) -> None:
        """Test seam."""
        self._active = False
        self.dropped_writes = 0
        self.ungated_terminal_writes = 0


#: Process-global because the applier and the supervisor are different threads of the
#: same run and there is exactly one applier per run.
COMMIT_ACK = _CommitAckWindow()


class RunOutcome:
    """`stop_reason` with a declared precedence. Cause before symptom, structurally."""

    __slots__ = ("_value", "history", "refusals")

    def __init__(self, initial: str = "max_seconds") -> None:
        self._value = RUN_OUTCOME.parse(initial)
        #: every assignment that was refused because it would have downgraded the
        #: diagnosis. Surfaced in the run summary: "we nearly reported the symptom" is
        #: operationally interesting, and it is the evidence A49's guard now works.
        self.refusals: list[tuple[str, str]] = []
        self.history: list[str] = [self._value]

    @property
    def value(self) -> str:
        return self._value

    def __str__(self) -> str:  # so `f"{outcome}"` and `str(outcome)` read as before
        return self._value

    def __eq__(self, other) -> bool:
        return self._value == (other.value if isinstance(other, RunOutcome) else other)

    def __hash__(self) -> int:
        return hash(self._value)

    @property
    def failed(self) -> bool:
        return self._value in OUTCOME_FAILURES

    def record(self, reason: str) -> bool:
        """Take `reason` if it is at least as severe as what we already have.

        Returns True if the outcome changed. A downgrade is **refused and logged**, not
        raised: the callers are `finally` blocks and shutdown paths where raising would
        replace a real failure with a bookkeeping one. The *machine* still calls it
        illegal — `RUN_OUTCOME.check('source_dark', 'hung')` raises — and that is what
        `tests/1.9_state_machines/` asserts against, so the rule is checked in one place
        rather than trusted in eight.
        """
        reason = RUN_OUTCOME.parse(reason)
        if reason == self._value:
            return False
        if RUN_OUTCOME.allows(self._value, reason):
            self._value = reason
            self.history.append(reason)
            return True
        self.refusals.append((self._value, reason))
        log.info(
            "run outcome stays %r rather than becoming %r: the second is a consequence "
            "of the first, and reporting it would lose the diagnosis (A49)",
            self._value, reason,
        )
        return False


class RunPhaseWriter:
    """One `_cdc_flight.heartbeat` row per run, moved through `RUN_PHASE`.

    Degrades to a no-op that still tracks the phase in memory when the destination
    cannot give an independent connection or the write fails; the machine's edge check
    happens either way, so an illegal phase order is a test failure even where the row
    is not written.

    ## The sink has ONE owner, and it is retired with a bound

    Codex r5 MAJOR-1, reproduced against a real serialized DuckDB cursor. Round 5 bounded
    how long `_execute_bounded()` *waits* by moving the database call onto a throwaway
    thread — and then `pipeline.run()` called `close()` on the very cursor that thread
    was still executing on. `cursor.close()` blocks behind the statement in flight
    (measured: it returned only when the query did), so the bound applied to one wait
    site and the run's teardown was still unbounded. Abandoning a worker while keeping no
    handle to it is not a bound; it is the same wait one stack frame later, plus a race.

    So ownership of the cursor is explicit and moves in one direction:

    * the writer owns it for every ordinary phase write, on the caller's thread;
    * for the **terminal** write it hands the cursor to one named worker thread and
      waits `TERMINAL_WRITE_TIMEOUT`;
    * `close()` **retires** it: join the worker for `RETIRE_TIMEOUT`, then either close
      the cursor (the worker is gone, so nobody else holds it) or *release* it —
      `CONNECTION_RETIREMENT` records which. A released cursor is closed by the worker
      if it ever finishes, and dies with the process if it does not. It is a daemon
      thread, so it can never hold the process open.

    Losing a heartbeat row is bad. Hanging a run's teardown on a heartbeat is worse, and
    calling `close()` on a cursor another thread is mid-statement on is worse again.
    """

    #: How long the terminal write's worker waits for the commit->ack exclusion
    #: before writing without it. The one bounded escape, and it is on the terminal
    #: write only, after the applier has already stopped.
    TERMINAL_GATE_TIMEOUT = _CommitAckWindow.GATE_TIMEOUT
    #: How long the terminal DATABASE CALL gets before the run stops waiting for it.
    #: The caller waits this PLUS `TERMINAL_GATE_TIMEOUT`, because the worker may spend
    #: the latter waiting for the exclusion before it issues any SQL at all.
    TERMINAL_WRITE_TIMEOUT = 5.0
    #: ...and how long an ORDINARY phase write gets. Shorter, because losing one costs
    #: a timestamp — the next transition rewrites the whole row — while blocking on one
    #: costs the run. `stopping` is written by `pipeline.run()`'s own `finally`, so an
    #: unbounded write here stalls a teardown before the terminal bound is ever reached.
    PHASE_WRITE_TIMEOUT = 2.0
    #: How long `close()` waits for an abandoned terminal write before releasing the
    #: cursor unclosed. Short on purpose: by here the run has its verdict and the only
    #: thing left to lose is a timestamp.
    RETIRE_TIMEOUT = 2.0

    def __init__(
        self, con, *, pipeline: str, runner_id: str, outcome: RunOutcome | None = None
    ) -> None:
        self.pipeline = pipeline
        self.runner_id = runner_id
        self.phase = PHASE_STARTING
        #: The run's ONE outcome. `pipeline.run()` hands the same object to
        #: `supervisor.run_engine_bounded`, so `last_run.json`'s `stop_reason` and the
        #: destination heartbeat's `terminal_reason` are two projections of one value
        #: rather than two objects that were free to disagree (Codex r1 MAJOR-2).
        self.outcome = outcome if outcome is not None else RunOutcome()
        self.transitions: list[str] = [PHASE_STARTING]
        self.independent = False
        self._sink = None
        self._row = False
        #: The one worker the terminal write may be handed to, and the lock that makes
        #: the hand-off and the retirement a single decision rather than a check-then-act.
        self._terminal_worker: threading.Thread | None = None
        self._sink_lock = threading.Lock()
        #: True once the writer has given the cursor up. The worker reads it to decide
        #: whether IT is now responsible for closing.
        self._released = False
        #: Phase writes this run stopped waiting for, terminal or not.
        self.phase_writes_abandoned = 0
        #: A terminal write the run stopped waiting for. Distinct from
        #: `COMMIT_ACK.ungated_terminal_writes`, which means something else entirely
        #: ("it was written without the gate") — one attempt used to increment that
        #: counter twice, once per bound (Codex r5 MAJOR-1, subordinate note).
        self.terminal_write_abandoned = False
        #: One value from `machines.CONNECTION_RETIREMENT`, carried into `last_run.json`.
        self.sink_retirement = "never_opened"
        try:
            self._sink = con.cursor()
            self.independent = True
        except Exception:  # pragma: no cover - a destination without cursors
            # NOT `self._sink = con`. Writing phases on the applier's own connection
            # puts an observability statement inside its open transaction, on another
            # thread, which is what the transaction discipline forbids (Codex r1
            # MAJOR-3). No independent connection means no row; the phase is still
            # tracked (and edge-checked) in memory.
            log.warning(
                "could not open an independent connection for the run-phase heartbeat; "
                "the phase will be tracked in memory only and NOT written",
                exc_info=True,
            )
            self._sink = None
        self._write(PHASE_STARTING, insert=True)

    # -- phases ------------------------------------------------------------- #
    def to(self, phase: str, *, detail: str | None = None) -> None:
        """Move to `phase`, asserting the edge. Raises on an undeclared transition.

        Deliberately strict, unlike `RunOutcome.record`: a phase order nobody declared
        means the run did something the design does not describe, and there is no
        conservative fallback that is more informative than the failure.
        """
        RUN_PHASE.check(self.phase, phase)
        self.phase = phase
        self.transitions.append(phase)
        log.info("run phase -> %s%s", phase, f" ({detail})" if detail else "")
        self._write(phase)

    def ensure(self, phase: str, *, detail: str | None = None) -> None:
        """`to(phase)`, unless we are already there. For phases a run can enter by more
        than one route (`reconciling` is reached directly and again after arming a
        recovery), where "already there" is not a transition at all."""
        if self.phase != phase:
            self.to(phase, detail=detail)

    def finish(self, *, ok: bool, reason: str | None = None) -> None:
        """Terminal: `stopped` when the run succeeded, `failed` when it did not."""
        if reason is not None:
            try:
                self.outcome.record(reason)
            except Exception:  # pragma: no cover - an outcome outside the domain
                log.warning("run outcome %r is not a declared value", reason)
        terminal = PHASE_STOPPED if ok else PHASE_FAILED
        if RUN_PHASE.is_terminal(self.phase):
            return
        if not RUN_PHASE.allows(self.phase, terminal):
            # `stopped` is only reachable through `stopping`; a run that ended in a
            # phase it could not leave cleanly is a failure, which is the honest answer.
            terminal = PHASE_FAILED
        try:
            self.to(terminal)
        except IllegalTransition:  # pragma: no cover - the fallback above is total
            log.debug("could not record the terminal run phase", exc_info=True)

    # -- the row ------------------------------------------------------------ #
    def _write(self, phase: str, *, insert: bool = False) -> None:
        if self._sink is None:
            return
        terminal = RUN_PHASE.is_terminal(phase)
        if terminal and not COMMIT_ACK.wait_until_closed():
            # A terminal row is the one an operator actually needs, and dropping it
            # would leave the heartbeat non-terminal for ever (Codex r2 MAJOR-1).
            # Waiting is safe here: `pipeline.run()` shuts the applier down before it
            # terminalises, so the window is closed or the applier is gone.
            log.warning(
                "the commit->ack window was still open when the terminal phase was "
                "recorded; writing anyway rather than losing the terminal row"
            )
        # EVERY write is bounded, not only the terminal one. `pipeline.run()`'s `finally`
        # writes `stopping` *before* it terminalises, and that write went straight down
        # the unbounded path — so against a wedged sink the teardown blocked one
        # statement EARLIER than either the round-5 or the round-6 finding, before the
        # bounded terminal write was ever reached. Found by driving the whole `finally`
        # block in a real process rather than the one call each finding had named.
        #
        # Dropping a non-terminal write costs a timestamp: the next transition rewrites
        # the whole row. Blocking a run on one costs the run.
        #
        # **The bound is on the WAIT, never on the exclusion.** The gate is acquired
        # inside the worker and held across its SQL, so the caller giving up cannot let
        # a statement run inside the commit->ack window (Codex r2 MAJOR-1 / r3 MAJOR-2 —
        # an instrumented violation of an absolute principle is still a violation). A
        # worker that cannot get the gate simply never writes; a worker that holds it
        # past the applier's patience is what the commit watchdog exists to kill.
        self._execute_bounded(
            phase,
            insert=insert,
            terminal=terminal,
            # The terminal worker may spend up to `GATE_TIMEOUT` waiting for the
            # exclusion before it even issues SQL, so the caller has to allow for
            # both or a contended gate abandons every terminal write by arithmetic.
            timeout=(
                self.TERMINAL_GATE_TIMEOUT + self.TERMINAL_WRITE_TIMEOUT if terminal
                else self.PHASE_WRITE_TIMEOUT
            ),
        )

    def _execute_bounded(
        self, phase: str, *, insert: bool, terminal: bool, timeout: float
    ) -> None:
        """One phase write, with a bound on the DATABASE call, not just the gate.

        Giving up on the Python gate and then calling `_execute()` anyway put the write
        straight back into an unbounded wait: DuckDB serialises calls on one connection,
        so the terminal statement simply queued behind the stalled writer's statement
        with no timeout at all (Codex r4 MAJOR-1, measured still alive at 8 s). The
        observability sink cannot be given a statement timeout, so the bound goes where
        it can: the call runs on ONE NAMED, OWNED worker and the run stops waiting for it.

        The worker is *kept*, not thrown away (Codex r5 MAJOR-1). A thread nobody holds
        a handle to cannot be joined, cannot be waited on with a bound, and cannot be
        told that the cursor it is using has been given up — so `close()` closed that
        cursor underneath it and blocked behind its statement while doing so.
        """
        done = threading.Event()

        def _run() -> None:
            try:
                # The gate is held for the CHECK AND THE WRITE TOGETHER, on the thread
                # that does both. Reading a flag and then executing SQL is a
                # check-then-act, and a database call releases the GIL, so the applier
                # could open the window in between (Codex r2 MAJOR-1, reproduced with a
                # two-thread barrier). Holding it here cannot delay the acknowledgement:
                # the applier takes the same gate BEFORE `COMMIT`.
                with COMMIT_ACK.excluded(
                    timeout=self.TERMINAL_GATE_TIMEOUT if terminal else None
                ) as inside_window:
                    if inside_window and not terminal:
                        COMMIT_ACK.dropped_writes += 1
                        log.debug(
                            "dropped the %r phase write: the applier is inside the "
                            "commit->ack window", phase,
                        )
                        return
                    self._execute(phase, insert=insert)
            finally:
                done.set()
                # If the run has already retired the sink, this thread is now its only
                # owner and closing it is this thread's job. Under the lock, so
                # "has it been released?" and "close it" are one decision.
                with self._sink_lock:
                    if self._released and self._sink is not None:
                        sink, self._sink = self._sink, None
                    else:
                        sink = None
                if sink is not None:
                    try:
                        sink.close()
                    except Exception:  # pragma: no cover - closing a wedged cursor
                        log.debug("the abandoned heartbeat sink would not close",
                                  exc_info=True)

        worker = threading.Thread(target=_run, name="cdc-phase-write", daemon=True)
        with self._sink_lock:
            # ONE owner at a time. A previous write that never returned still holds the
            # cursor, so a second worker would queue behind it on the same handle and
            # the bound would be a bound on starting a thread rather than on the write.
            busy = self._terminal_worker is not None and self._terminal_worker.is_alive()
            if not busy:
                self._terminal_worker = worker
        if busy:
            self.phase_writes_abandoned += 1
            if terminal:
                # The terminal row is the one an operator needs, so "an earlier write
                # still owns the sink" is exactly as bad as "this one did not return":
                # either way the durable heartbeat never terminalises and
                # `last_run.json` is the only record. One fact, one name.
                self.terminal_write_abandoned = True
            log.error(
                "dropped the %r phase write: an earlier one still owns the heartbeat "
                "sink and has not returned", phase,
            )
            return
        worker.start()
        if not done.wait(timeout):
            # NOT `ungated_terminal_writes`: that counter means "written without the
            # commit->ack gate", and incrementing it here made one attempt look like two
            # separate ungated writes. This is a different fact and it gets its own name.
            self.phase_writes_abandoned += 1
            if RUN_PHASE.is_terminal(phase):
                self.terminal_write_abandoned = True
            log.error(
                "the %r phase write did not complete within %.1fs; this run stops "
                "waiting for it. The heartbeat sink is now owned by the "
                "'cdc-phase-write' worker and will be RELEASED rather than closed",
                phase, timeout,
            )

    def _execute(self, phase: str, *, insert: bool) -> None:
        try:
            from .destination import now

            stamp = now()
            if insert:
                self._sink.execute(
                    f"DELETE FROM {CONTROL_SCHEMA}.heartbeat "
                    "WHERE pipeline = ? AND runner_id = ?",
                    [self.pipeline, self.runner_id],
                )
                self._sink.execute(
                    f"INSERT INTO {CONTROL_SCHEMA}.heartbeat "
                    "(pipeline, runner_id, beat_at, phase, phase_since, "
                    " terminal_reason, phase_history) VALUES (?,?,?,?,?,?,?)",
                    [
                        self.pipeline, self.runner_id, stamp, phase, stamp, None,
                        json.dumps(self.transitions),
                    ],
                )
                self._row = True
                return
            if not self._row:  # the INSERT failed; do not silently write nothing
                return
            self._sink.execute(
                f"UPDATE {CONTROL_SCHEMA}.heartbeat SET phase = ?, phase_since = ?, "
                "beat_at = ?, terminal_reason = ?, phase_history = ? "
                "WHERE pipeline = ? AND runner_id = ?",
                [
                    phase, stamp, stamp,
                    self.outcome.value if RUN_PHASE.is_terminal(phase) else None,
                    json.dumps(self.transitions), self.pipeline, self.runner_id,
                ],
            )
        except Exception:  # pragma: no cover - observability must never fail a run
            log.debug("could not write the run-phase heartbeat row", exc_info=True)

    def summary(self) -> dict:
        out = {
            "run_phase": self.phase,
            "run_phases": list(self.transitions),
            "run_outcome": self.outcome.value,
            "heartbeat_independent": self.independent,
        }
        if COMMIT_ACK.dropped_writes:
            # Evidence, not decoration: it is the count of times the commit->ack
            # exclusion actually fired.
            out["phase_writes_dropped_in_commit_ack"] = COMMIT_ACK.dropped_writes
        if COMMIT_ACK.ungated_terminal_writes:
            # The one bounded escape, and it is on the terminal write only, after the
            # applier has stopped. Must be 0 on a healthy run; reported when it is not.
            out["ungated_terminal_phase_writes"] = COMMIT_ACK.ungated_terminal_writes
        if self.phase_writes_abandoned:
            out["phase_writes_abandoned"] = self.phase_writes_abandoned
        if self.terminal_write_abandoned:
            # An abandoned terminal write means the durable heartbeat may never have
            # reached its terminal phase. `last_run.json` is then the ONLY record that
            # this run terminalised at all, so it has to say so (Codex r5 MAJOR-1).
            out["terminal_phase_write_abandoned"] = True
        if self.independent:
            out["heartbeat_sink_retirement"] = CONNECTION_RETIREMENT.parse(
                self.sink_retirement
            )
        if self.outcome.refusals:
            # A49's guard, as evidence rather than as a comment.
            out["outcome_downgrades_refused"] = [
                f"{a}->{b}" for a, b in self.outcome.refusals
            ]
        return out

    def close(self) -> None:
        """Retire the sink under a bound. Never blocks the process indefinitely.

        The three outcomes are the whole of `machines.CONNECTION_RETIREMENT`:

        * `never_opened` — there was no independent connection to give up;
        * `closed` — nobody else held the cursor, so it was closed properly;
        * `abandoned` — a terminal write still owns it. It is **released, not closed**,
          because `cursor.close()` blocks behind the statement in flight and closing a
          cursor another thread is executing on is the race, not the fix. The worker
          closes it if it ever returns; it is a daemon, so it cannot hold the process.
        """
        with self._sink_lock:
            worker = self._terminal_worker
            if self._sink is None:
                # Never opened, already closed, or released and since closed by the
                # worker. Every path that clears `_sink` sets the retirement with it, so
                # there is nothing to decide here and a second `close()` is a no-op.
                return
        if worker is not None and worker.is_alive():
            worker.join(self.RETIRE_TIMEOUT)
        with self._sink_lock:
            sink = self._sink
            if worker is not None and worker.is_alive():
                # Hand ownership over rather than racing it. `_released` is what tells
                # the worker that closing is now its job.
                self._released = True
                self.sink_retirement = "abandoned"
                log.error(
                    "the heartbeat sink is still owned by the terminal-phase write "
                    "after %.1fs; RELEASING it unclosed rather than blocking this "
                    "run's teardown behind a statement that has not returned",
                    self.RETIRE_TIMEOUT,
                )
                return
            self._sink = None
        if sink is None:  # pragma: no cover - the worker closed it between the checks
            self.sink_retirement = "abandoned"
            return
        try:
            sink.close()
        except Exception:  # pragma: no cover
            log.debug("closing the heartbeat connection failed", exc_info=True)
        self.sink_retirement = "closed"
