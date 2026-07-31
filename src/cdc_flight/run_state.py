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
"""

from __future__ import annotations

import json
import logging

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

__all__ = ["RunOutcome", "RunPhaseWriter"]


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

    def __init__(self, con, *, pipeline: str, runner_id: str) -> None:
        self.pipeline = pipeline
        self.runner_id = runner_id
        self.phase = PHASE_STARTING
        self.outcome = RunOutcome()
        self.transitions: list[str] = [PHASE_STARTING]
        self.independent = False
        self._sink = None
        self._row = False
        try:
            self._sink = con.cursor()
            self.independent = True
        except Exception:  # pragma: no cover - a destination without cursors
            log.warning(
                "could not open an independent connection for the run-phase heartbeat; "
                "the phase will be tracked in memory only", exc_info=True,
            )
            self._sink = con
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
