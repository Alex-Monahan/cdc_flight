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
  `COMMIT_ACK` is entered by the applier around exactly that interval; a phase write
  that lands inside it is **dropped**, never deferred-with-a-lock and never blocked,
  because the one thing an observability writer must not do is make the acknowledgement
  wait. The next transition rewrites the whole row, so nothing is lost but a timestamp.
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

    The wait in `enter()` is **bounded**. An observability connection that has stalled
    must never be able to stall the commit path, so after `GATE_TIMEOUT` the applier
    proceeds and records `overlaps`. That counter is the honest edge of the guarantee:
    it is asserted to be zero, and if it is ever non-zero the claim is measurably false
    rather than quietly false.
    """

    __slots__ = ("_active", "_gate", "dropped_writes", "overlaps")

    #: How long the applier will wait for an in-flight phase write before opening the
    #: window anyway. Generous relative to a single-row UPDATE on a local cursor, tiny
    #: relative to `CDC_COMMIT_TIMEOUT`.
    GATE_TIMEOUT = 5.0

    def __init__(self) -> None:
        self._active = False
        self._gate = threading.Lock()
        #: how many observability writes this process declined because of the window.
        #: Surfaced in the run summary, so "we never wrote inside it" is measured.
        self.dropped_writes = 0
        #: how many times the applier opened the window without proving no write was in
        #: flight. Must be 0; reported when it is not.
        self.overlaps = 0

    def enter(self) -> None:
        if self._gate.acquire(timeout=self.GATE_TIMEOUT):
            try:
                self._active = True
            finally:
                self._gate.release()
            return
        self.overlaps += 1
        self._active = True
        log.error(
            "opened the commit->ack window without acquiring the observability gate "
            "after %.1fs; an independent write may overlap it", self.GATE_TIMEOUT,
        )

    def leave(self) -> None:
        self._active = False

    @contextmanager
    def excluded(self):
        """Hold the gate for one independent write. Yields whether it must be dropped."""
        with self._gate:
            yield self._active

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
        self.overlaps = 0


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
    """

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
        if RUN_PHASE.is_terminal(phase) and not COMMIT_ACK.wait_until_closed():
            # A terminal row is the one an operator actually needs, and dropping it
            # would leave the heartbeat non-terminal for ever (Codex r2 MAJOR-1).
            # Waiting is safe here: `pipeline.run()` shuts the applier down before it
            # terminalises, so the window is closed or the applier is gone.
            log.warning(
                "the commit->ack window was still open when the terminal phase was "
                "recorded; writing anyway rather than losing the terminal row"
            )
        # The gate is held for the CHECK AND THE WRITE TOGETHER. Reading a flag and
        # then executing SQL is a check-then-act, and a database call releases the GIL,
        # so the applier could open the window in between and the write landed inside
        # the exact interval the ADR excludes (Codex r2 MAJOR-1, reproduced with a
        # two-thread barrier). Holding it here cannot delay the acknowledgement: the
        # applier takes the same gate BEFORE `COMMIT`, with a bounded wait.
        with COMMIT_ACK.excluded() as inside_window:
            if inside_window and not RUN_PHASE.is_terminal(phase):
                COMMIT_ACK.dropped_writes += 1
                log.debug(
                    "dropped the %r phase write: the applier is inside the "
                    "commit->ack window", phase,
                )
                return
            self._execute(phase, insert=insert)

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
        if COMMIT_ACK.overlaps:
            # The honest edge of the guarantee: the applier opened the window without
            # proving no independent write was in flight, because the gate did not come
            # free inside its bounded wait. Must be 0; reported loudly when it is not.
            out["commit_ack_gate_overlaps"] = COMMIT_ACK.overlaps
        if self.outcome.refusals:
            # A49's guard, as evidence rather than as a comment.
            out["outcome_downgrades_refused"] = [
                f"{a}->{b}" for a, b in self.outcome.refusals
            ]
        return out

    def close(self) -> None:
        if self.independent and self._sink is not None:
            try:
                self._sink.close()
            except Exception:  # pragma: no cover
                log.debug("closing the heartbeat connection failed", exc_info=True)
        self._sink = None
