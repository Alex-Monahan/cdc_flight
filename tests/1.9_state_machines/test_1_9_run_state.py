"""Rubric 1.9 — the run phase reaches the destination, and the outcome has a precedence.

Two things that did not exist:

* **`_cdc_flight.heartbeat` had no writer.** ADR §4.8 declared `phase` and the table was
  created (empty) one round ago; an operator could not ask the destination where a live
  run was, because the only durable trace of a run's progress was `last_run.json` on the
  machine that ran it.
* **`stop_reason` was a string with a hand-written precedence.** A49 measured the cost:
  a dark source makes `engine.close()` hang, so a `finally` block replaced the diagnosis
  with the symptom and a blackholed Postgres was reported as `hung`.

In-process, DuckDB in a tmp dir, no engine.
"""

from __future__ import annotations

import contextlib
import json
import sys
import threading
import time
from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import machines as m
from cdc_flight.run_state import RunOutcome, RunPhaseWriter
from cdc_flight.states import IllegalTransition

PIPELINE = "run_state"
RUNNER = "runner-1"


@pytest.fixture
def con(tmp_path):
    connection = duckdb.connect(str(tmp_path / "dest.duckdb"))
    dest_mod.ensure_control_schema(connection)
    try:
        yield connection
    finally:
        connection.close()


def _row(con):
    return con.execute(
        "SELECT phase, terminal_reason, phase_history FROM _cdc_flight.heartbeat "
        "WHERE pipeline = ? AND runner_id = ?", [PIPELINE, RUNNER]
    ).fetchone()


# --------------------------------------------------------------------------- #
# RunOutcome — the precedence
# --------------------------------------------------------------------------- #
def test_a_symptom_does_not_overwrite_the_cause():
    """A49, reproduced against the type rather than against two copies of a tuple."""
    outcome = RunOutcome()
    assert outcome.record("source_dark") is True
    assert outcome.record("hung") is False
    assert outcome.value == "source_dark"
    assert outcome.refusals == [("source_dark", "hung")]


def test_the_same_guard_holds_for_engine_error():
    outcome = RunOutcome()
    outcome.record("engine_error")
    outcome.record("hung")
    outcome.record("catalog_unresolved")
    assert outcome.value == "engine_error"


def test_an_escalation_is_taken():
    outcome = RunOutcome()
    assert outcome.value == "max_seconds"
    assert outcome.record("idle") is True
    assert outcome.record("catalog_unresolved") is True
    assert outcome.record("engine_error") is True
    assert outcome.history == ["max_seconds", "idle", "catalog_unresolved", "engine_error"]


def test_recording_the_same_outcome_twice_is_not_a_transition():
    outcome = RunOutcome()
    assert outcome.record("max_seconds") is False
    assert outcome.history == ["max_seconds"]


def test_an_outcome_outside_the_domain_is_refused():
    from cdc_flight.states import UnknownState

    with pytest.raises(UnknownState):
        RunOutcome().record("probably_fine")


def test_the_failure_set_is_derived_from_the_precedence():
    assert "engine_error" in m.OUTCOME_FAILURES
    assert "source_dark" in m.OUTCOME_FAILURES
    assert "idle" not in m.OUTCOME_FAILURES
    assert "max_seconds" not in m.OUTCOME_FAILURES


def test_the_supervisor_no_longer_assigns_the_outcome_by_hand():
    """A49's defect stated exactly: `stop_reason` was assigned by plain `=` in eight
    places, one of them inside a `finally`, with the precedence written out as two
    copies of a literal tuple. Parsed rather than grepped, so the prose above (which
    quotes the old guard on purpose) is not mistaken for the guard.
    """
    import ast
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "src" / "cdc_flight" / "supervisor.py"
    tree = ast.parse(path.read_text())
    assignments = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id == "stop_reason"
    ]
    assert not assignments, (
        f"`stop_reason` is assigned by hand at line(s) {assignments}; the outcome is a "
        "`RunOutcome` and `record()` is what enforces cause-before-symptom"
    )
    assert "RunOutcome" in path.read_text()


# --------------------------------------------------------------------------- #
# RunPhaseWriter — the durable row
# --------------------------------------------------------------------------- #
def test_the_run_writes_its_phase_where_an_operator_can_read_it(con):
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        assert _row(con)[0] == "starting"
        phases.to(m.PHASE_RECONCILING)
        assert _row(con)[0] == "reconciling"
        phases.to(m.PHASE_STREAMING)
        phases.to(m.PHASE_DRAINING)
        phases.ensure(m.PHASE_STOPPING)
        phases.finish(ok=True)
        phase, terminal, history = _row(con)
        assert phase == "stopped"
        assert terminal == "max_seconds"
        assert json.loads(history) == [
            "starting", "reconciling", "streaming", "draining", "stopping", "stopped",
        ]
    finally:
        phases.close()


def test_a_failed_run_lands_on_failed_with_its_reason(con):
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        phases.to(m.PHASE_STREAMING)
        phases.outcome.record("source_dark")
        phases.finish(ok=False, reason="hung")  # the symptom arrives second
        phase, terminal, _history = _row(con)
        assert phase == "failed"
        assert terminal == "source_dark", "the diagnosis, not the consequence"
    finally:
        phases.close()


def test_an_undeclared_phase_order_is_refused(con):
    """Strict, unlike the outcome: a phase order nobody declared means the run did
    something the design does not describe, and there is no conservative fallback that
    is more informative than the failure."""
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        with pytest.raises(IllegalTransition):
            phases.to(m.PHASE_STOPPED)
        assert phases.phase == "starting"
    finally:
        phases.close()


def test_the_row_is_written_on_an_independent_connection(con):
    """It must survive a rolled-back commit group: this is the property `AlertSink`
    already establishes, and an observability signal that dies with the apply it was
    reporting on is not one."""
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        assert phases.independent is True
        con.execute("BEGIN TRANSACTION")
        con.execute(
            "INSERT INTO _cdc_flight.alerts (pipeline, raised_at, severity, code, message) "
            "VALUES (?, now(), 'info', 'x', 'y')", [PIPELINE],
        )
        phases.to(m.PHASE_RECONCILING)
        con.execute("ROLLBACK")
        assert _row(con)[0] == "reconciling"
    finally:
        phases.close()


def test_a_heartbeat_that_cannot_be_written_never_fails_the_run(con):
    """Observability must not be able to break a run that is otherwise correct."""
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        con.execute("DROP TABLE _cdc_flight.heartbeat")
        phases.to(m.PHASE_RECONCILING)  # must not raise
        assert phases.phase == "reconciling"
    finally:
        phases.close()


def test_the_summary_reports_the_phases_and_any_refused_downgrade(con):
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        phases.outcome.record("source_dark")
        phases.outcome.record("hung")
        summary = phases.summary()
        assert summary["run_phase"] == "reconciling"
        assert summary["run_phases"] == ["starting", "reconciling"]
        assert summary["run_outcome"] == "source_dark"
        assert summary["outcome_downgrades_refused"] == ["source_dark->hung"]
    finally:
        phases.close()


def test_the_heartbeat_table_carries_the_run_phase_columns(con):
    columns = {
        str(row[0])
        for row in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
        ).fetchall()
    }
    assert {"phase", "phase_since", "terminal_reason", "phase_history"} <= columns


def test_the_columns_are_added_to_a_heartbeat_that_predates_them(tmp_path):
    """`CREATE TABLE IF NOT EXISTS` cannot add a column, and the table shipped one round
    ago — including into the shared MotherDuck development database."""
    path = str(tmp_path / "old.duckdb")
    old = duckdb.connect(path)
    old.execute("CREATE SCHEMA _cdc_flight")
    old.execute(
        "CREATE TABLE _cdc_flight.heartbeat (pipeline VARCHAR, runner_id VARCHAR, "
        "beat_at TIMESTAMPTZ, phase VARCHAR)"
    )
    old.close()

    fresh = duckdb.connect(path)
    try:
        dest_mod.ensure_control_schema(fresh)
        columns = {
            str(row[0])
            for row in fresh.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
            ).fetchall()
        }
        assert {"phase_since", "terminal_reason", "phase_history"} <= columns
    finally:
        fresh.close()


def test_the_terminal_reason_reaches_the_heartbeat_row(con):
    """`heartbeat.terminal_reason` has to say something, or the column is decoration.

    `pipeline.run()` hands the run's own `stop_reason` to `finish()`; the precedence
    refuses a downgrade there too, so a shutdown-path reason cannot overwrite a
    diagnosis the engine already recorded.
    """
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        phases.to(m.PHASE_STREAMING)
        phases.to(m.PHASE_DRAINING)
        phases.ensure(m.PHASE_STOPPING)
        phases.finish(ok=False, reason="catalog_unresolved")
        phase, terminal, _history = _row(con)
        assert phase == "failed"
        assert terminal == "catalog_unresolved"
    finally:
        phases.close()


def test_a_reason_outside_the_domain_is_logged_rather_than_raised(con):
    """`finish()` runs from the outermost `finally`. A future `stop_reason` nobody
    added to the precedence must not replace the run's real failure with a
    bookkeeping one."""
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.ensure(m.PHASE_STOPPING)
        phases.finish(ok=True, reason="something_nobody_declared")  # must not raise
        phase, terminal, _history = _row(con)
        assert phase == "stopped"
        assert terminal == "max_seconds", "it kept the value it had"
    finally:
        phases.close()


# --------------------------------------------------------------------------- #
# Codex r1 MAJOR-2 / MAJOR-3 / MINOR-1
# --------------------------------------------------------------------------- #
def test_one_outcome_object_is_shared_with_whoever_supervises_the_run(con):
    """`stop_reason` and `run_outcome` are two projections of ONE value.

    They used to be two objects: `run_engine_bounded` built one and `RunPhaseWriter`
    built another, so ordinary successful runs shipped `stop_reason="idle"` beside
    `run_outcome="max_seconds"`, and a severe supervisor result could be published as
    the mild phase-writer default (Codex r1 MAJOR-2).
    """
    shared = RunOutcome()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER, outcome=shared)
    try:
        assert phases.outcome is shared
        shared.record("source_dark")
        assert phases.summary()["run_outcome"] == "source_dark"
        phases.to(m.PHASE_RECONCILING)
        phases.finish(ok=False)
        assert _row(con)[0] == "failed"
        assert _row(con)[1] == "source_dark"
    finally:
        phases.close()


def test_engine_finished_is_not_called_a_failure_by_the_precedence(con):
    """It is a SUCCESS for a terminating snapshot mode and a failure otherwise, and
    severity alone cannot decide that — `run_engine_bounded` knows which run it is."""
    outcome = RunOutcome()
    outcome.record("engine_finished")
    assert outcome.failed is False
    outcome.record("hung")
    assert outcome.failed is True


def test_a_phase_write_inside_the_commit_ack_window_is_dropped_not_performed(con):
    """The binding principle, in WALL CLOCK rather than program order.

    The supervisor writes `draining` on its own thread the moment `max_seconds`, an
    engine error or source-dark breaks the loop — without asking whether the engine
    thread is between `COMMIT` and `markBatchFinished()`. The callback's instruction
    sequence was clean; "never in the window" was still false (Codex r1 MAJOR-3).
    """
    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        assert _row(con)[0] == "reconciling"

        COMMIT_ACK.enter()
        try:
            phases.to(m.PHASE_STREAMING)
        finally:
            COMMIT_ACK.leave()
        # The machine still moved — an illegal phase order is a failure either way —
        # but the destination was NOT written to inside the window.
        assert phases.phase == "streaming"
        assert _row(con)[0] == "reconciling"
        assert COMMIT_ACK.dropped_writes == 1
        assert phases.summary()["phase_writes_dropped_in_commit_ack"] == 1

        # ... and the next transition restores the whole row, so nothing is lost.
        phases.to(m.PHASE_DRAINING)
        assert _row(con)[0] == "draining"
    finally:
        COMMIT_ACK.reset()
        phases.close()


def test_the_applier_really_holds_that_window_around_commit_to_ack():
    """The flag is only worth anything if the applier is the thing that sets it."""
    source = (
        Path(__file__).resolve().parents[2] / "src" / "cdc_flight" / "applier.py"
    ).read_text()
    commit = source.index('self.con.execute("COMMIT")')
    ack = source.index("self._committer.markBatchFinished()")
    assert source.rindex("COMMIT_ACK.enter()", 0, commit) > commit - 800, (
        "the window must be entered immediately before COMMIT"
    )
    assert source.index("COMMIT_ACK.leave()", ack) - ack < 700, (
        "the window must be left immediately after the acknowledgement"
    )


def test_no_independent_connection_means_no_row_rather_than_the_primary_one(con):
    """`con.cursor()` failing used to set `_sink = con`, so phase writes landed on the
    applier's own connection, inside its open transaction, from another thread — which
    is precisely what the transaction discipline forbids (Codex r1 MAJOR-3)."""

    class _NoCursors:
        def __init__(self, real):
            self._real = real

        def cursor(self):
            raise RuntimeError("this destination has no cursors")

        def execute(self, *a, **k):  # pragma: no cover - must never be reached
            raise AssertionError("the phase writer borrowed the primary connection")

    phases = RunPhaseWriter(_NoCursors(con), pipeline=PIPELINE, runner_id=RUNNER)
    try:
        assert phases.independent is False
        phases.to(m.PHASE_RECONCILING)  # must not raise, must not write
        assert phases.phase == "reconciling"
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.heartbeat WHERE runner_id = ?", [RUNNER]
        ).fetchone()[0] == 0
    finally:
        phases.close()


def test_a_migration_that_cannot_be_shown_to_have_happened_is_loud(tmp_path):
    """Every `ALTER` exception used to be read as "a concurrent runner won the race",
    with no re-check, so a permission or DDL failure looked like success and the writer
    that depends on the column failed silently for ever (Codex r1 MINOR-1)."""
    from cdc_flight import control_schema

    path = str(tmp_path / "old.duckdb")
    old = duckdb.connect(path)
    old.execute("CREATE SCHEMA _cdc_flight")
    old.execute(
        "CREATE TABLE _cdc_flight.heartbeat (pipeline VARCHAR, runner_id VARCHAR, "
        "beat_at TIMESTAMPTZ, phase VARCHAR)"
    )
    old.close()

    fresh = duckdb.connect(path)

    class _RefusesAlters:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **k):
            if "ADD COLUMN" in str(sql):
                raise RuntimeError("permission denied")
            return self._real.execute(sql, *a, **k)

    try:
        with pytest.raises(control_schema.ControlSchemaFailed) as failure:
            dest_mod.ensure_control_schema(_RefusesAlters(fresh))
        assert "phase_since" in str(failure.value)
    finally:
        fresh.close()


def test_a_migration_that_lost_the_race_is_accepted_after_re_reading(tmp_path):
    """The one benign reading, VERIFIED rather than assumed."""

    path = str(tmp_path / "raced.duckdb")
    old = duckdb.connect(path)
    old.execute("CREATE SCHEMA _cdc_flight")
    old.execute(
        "CREATE TABLE _cdc_flight.heartbeat (pipeline VARCHAR, runner_id VARCHAR, "
        "beat_at TIMESTAMPTZ, phase VARCHAR)"
    )
    old.close()

    fresh = duckdb.connect(path)

    class _RacedBy:
        """Adds the column behind our back, then reports the ALTER as failed."""

        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **k):
            if "ADD COLUMN" in str(sql):
                self._real.execute(sql)
                raise RuntimeError("a concurrent runner added it first")
            return self._real.execute(sql, *a, **k)

    try:
        dest_mod.ensure_control_schema(_RacedBy(fresh))  # must not raise
        columns = {
            str(row[0])
            for row in fresh.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
            ).fetchall()
        }
        assert {"phase_since", "terminal_reason", "phase_history"} <= columns
    finally:
        fresh.close()


# --------------------------------------------------------------------------- #
# Codex r2 MAJOR-1 — the commit->ack window as a gate, not an observation
# --------------------------------------------------------------------------- #
def test_a_write_that_starts_before_the_window_cannot_run_inside_it(con):
    """The adversarial interleaving, as the regression it should always have been.

    The first cut read `COMMIT_ACK.active`, then built a timestamp, then executed SQL.
    A database call releases the GIL, so the applier could open the window in between
    and the write landed inside the exact interval the ADR excludes; a two-thread
    barrier reproduced it (Codex r2 MAJOR-1). The gate is now held for the check AND
    the write, so the applier's `enter()` — which happens BEFORE `COMMIT` — waits for an
    in-flight write instead of racing it.
    """
    import threading

    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        in_write = threading.Event()
        release = threading.Event()
        active_during_sql: list[bool] = []

        original = phases._execute

        def _slow_execute(phase, *, insert):
            in_write.set()
            release.wait(5)
            active_during_sql.append(COMMIT_ACK.active)
            return original(phase, insert=insert)

        phases._execute = _slow_execute
        writer = threading.Thread(target=lambda: phases.to(m.PHASE_STREAMING))
        writer.start()
        assert in_write.wait(5), "the phase write never started"

        entered = threading.Event()

        def _enter():
            COMMIT_ACK.enter()
            entered.set()

        applier_thread = threading.Thread(target=_enter)
        applier_thread.start()
        # The applier must NOT be able to open the window while the write is in flight.
        assert not entered.wait(0.5), (
            "COMMIT_ACK.enter() opened the window while an independent write was "
            "already executing: that is the check-then-act race"
        )
        release.set()
        writer.join(10)
        applier_thread.join(10)
        assert active_during_sql == [False], (
            f"SQL executed with the window open: {active_during_sql}"
        )
    finally:
        COMMIT_ACK.reset()
        phases.close()


def test_the_gate_has_no_five_second_escape(con):
    """The escape the first gate shipped with, and the reproduction that killed it.

    `enter()` used to give up after `GATE_TIMEOUT`, open the window anyway and merely
    COUNT the overlap; holding `_execute()` past the bound then ran SQL with
    `COMMIT_ACK.active is True` and `dropped_writes == 0` (Codex r3 MAJOR-2). An
    instrumented violation of an absolute principle is still a violation. `enter()` now
    waits without a bound of its own, and the applier wraps it in the commit watchdog so
    a wedged observability cursor is a loud, bounded death instead.
    """
    import threading

    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        in_write = threading.Event()
        release = threading.Event()
        active_during_sql: list[bool] = []
        original = phases._execute

        def _very_slow_execute(phase, *, insert):
            in_write.set()
            release.wait(30)
            active_during_sql.append(COMMIT_ACK.active)
            return original(phase, insert=insert)

        phases._execute = _very_slow_execute
        writer = threading.Thread(target=lambda: phases.to(m.PHASE_STREAMING))
        writer.start()
        assert in_write.wait(5)

        entered = threading.Event()
        threading.Thread(target=lambda: (COMMIT_ACK.enter(), entered.set())).start()
        # WELL past the old five-second bound.
        assert not entered.wait(COMMIT_ACK.GATE_TIMEOUT + 3), (
            "the window opened while an independent write was still executing; the "
            "five-second escape is back"
        )
        release.set()
        writer.join(30)
        assert entered.wait(10)
        assert active_during_sql == [False], active_during_sql
    finally:
        COMMIT_ACK.reset()
        phases.close()


def test_a_stalled_gate_cannot_block_the_terminal_write_for_ever(con):
    """The other direction, and it is bounded rather than absolute on purpose.

    Blocking a run's own teardown on a stalled observability cursor is the same mistake
    as overlapping the window (Codex r3 MAJOR-2, which held the writer and watched
    terminalisation hang). By the time the terminal phase is written the applier has
    stopped, so the terminal write waits `GATE_TIMEOUT` and then writes WITHOUT the gate,
    counting the fact.
    """
    import threading
    import time as _time

    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    holder_release = threading.Event()
    try:
        phases.to(m.PHASE_RECONCILING)

        def _hold_the_gate():
            with COMMIT_ACK.excluded():
                holder_release.wait(30)

        threading.Thread(target=_hold_the_gate, daemon=True).start()
        _time.sleep(0.2)
        started = _time.monotonic()
        phases.finish(ok=False)
        elapsed = _time.monotonic() - started
        assert elapsed < COMMIT_ACK.GATE_TIMEOUT + 3, (
            f"terminalisation blocked for {elapsed:.1f}s on the observability gate"
        )
        assert _row(con)[0] == "failed", "the terminal row was lost"
        assert COMMIT_ACK.ungated_terminal_writes == 1
    finally:
        holder_release.set()
        COMMIT_ACK.reset()
        phases.close()


def test_the_terminal_phase_write_is_never_dropped(con):
    """A dropped `stopped`/`failed` write leaves the heartbeat non-terminal for ever.

    Every other phase write can be dropped because the next transition rewrites the
    whole row; the terminal one has no next transition (Codex r2 MAJOR-1). It waits for
    the window instead, which is safe because the applier is shut down before
    `pipeline.run()` terminalises.
    """
    import threading

    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    try:
        phases.to(m.PHASE_RECONCILING)
        COMMIT_ACK.enter()
        threading.Timer(0.3, COMMIT_ACK.leave).start()
        phases.finish(ok=False)
        assert _row(con)[0] == "failed", "the terminal row was dropped"
    finally:
        COMMIT_ACK.reset()
        phases.close()


def test_the_window_is_left_even_when_the_acknowledgement_raises():
    """`markProcessed`/`markBatchFinished` can raise; a window left open would drop
    every later phase write silently (Codex r2 MAJOR-1)."""
    source = (
        Path(__file__).resolve().parents[2] / "src" / "cdc_flight" / "applier.py"
    ).read_text()
    ack = source.index("self._committer.markBatchFinished()")
    tail = source[ack : ack + 600]
    assert "finally:" in tail and "COMMIT_ACK.leave()" in tail, (
        "the acknowledgement block must leave the window in a `finally`"
    )


def test_metadata_that_cannot_be_read_is_loud_rather_than_skipped(tmp_path):
    """"I could not read the metadata" is not "the table is fine" (Codex r2 MINOR-1)."""
    from cdc_flight import control_schema

    class _NoIntrospection:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **k):
            if "information_schema.columns" in str(sql):
                raise RuntimeError("catalog unavailable")
            return self._real.execute(sql, *a, **k)

    fresh = duckdb.connect(str(tmp_path / "blind.duckdb"))
    try:
        with pytest.raises(control_schema.ControlSchemaFailed):
            dest_mod.ensure_control_schema(_NoIntrospection(fresh))
    finally:
        fresh.close()


def test_an_old_heartbeat_with_a_key_and_data_keeps_both_through_the_migration(tmp_path):
    """The exact prior DDL, with its constraint and its rows (Codex r2 MINOR-1)."""
    path = str(tmp_path / "prior.duckdb")
    old = duckdb.connect(path)
    old.execute("CREATE SCHEMA _cdc_flight")
    old.execute(
        "CREATE TABLE _cdc_flight.heartbeat ("
        " pipeline VARCHAR NOT NULL, runner_id VARCHAR NOT NULL,"
        " beat_at TIMESTAMPTZ NOT NULL, phase VARCHAR NOT NULL,"
        " last_event_at TIMESTAMPTZ, last_commit_id BIGINT, lag_seconds DOUBLE,"
        " PRIMARY KEY (pipeline, runner_id, beat_at))"
    )
    old.execute(
        "INSERT INTO _cdc_flight.heartbeat (pipeline, runner_id, beat_at, phase, "
        "last_commit_id) VALUES ('p', 'r', now(), 'streaming', 7)"
    )
    old.close()

    fresh = duckdb.connect(path)
    try:
        dest_mod.ensure_control_schema(fresh)
        columns = {
            str(row[0])
            for row in fresh.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
            ).fetchall()
        }
        assert {"phase_since", "terminal_reason", "phase_history"} <= columns
        kept = fresh.execute(
            "SELECT phase, last_commit_id FROM _cdc_flight.heartbeat WHERE pipeline = 'p'"
        ).fetchall()
        assert kept == [("streaming", 7)], "the migration lost the existing row"
        # The key survives too: the migration adds columns, it does not rebuild.
        keys = fresh.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = '_cdc_flight' AND table_name = 'heartbeat' "
            "AND constraint_type = 'PRIMARY KEY'"
        ).fetchall()
        assert keys and set(keys[0][0]) == {"pipeline", "runner_id", "beat_at"}
    finally:
        fresh.close()


# --------------------------------------------------------------------------- #
# Codex r2 MAJOR-2 — a PRE-ENGINE failure projects the same phase and outcome
# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_pre_engine_failure_agrees_with_the_heartbeat(tmp_path_factory, postgres_cluster):
    """`last_run.json` and the destination heartbeat, on a route that never reaches
    the engine at all.

    Fixing the *normal* path left this one behind: `reported` was populated only on the
    inner engine success/`EngineFailure` paths, so a lease refusal wrote heartbeat
    `failed/error` while `main()` built its summary from the exception and shipped no
    `run_phase` and no `run_outcome` at all (Codex r2 MAJOR-2). The escaping exception
    now carries the one projection, and the outer `finally` fills it in after the
    terminal transitions.
    """
    import json
    import uuid as _uuid

    from conftest import Sandbox

    box = Sandbox("pre_engine", tmp_path_factory.mktemp("sbx_pre_engine"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)

        # A live incumbent lease held by a runner whose pid is this (very much alive)
        # test process, so `Lease.acquire` cannot reclaim it.
        pipeline = box.env["CDC_PIPELINE_NAME"]
        con = duckdb.connect(str(box.duckdb_path))
        try:
            con.execute("DELETE FROM _cdc_flight.lease WHERE pipeline = ?", [pipeline])
            con.execute(
                "INSERT INTO _cdc_flight.lease (pipeline, owner_id, host, pid, "
                "acquired_at, renewed_at, expires_at) VALUES (?,?,?,?, now(), now(), "
                "now() + INTERVAL 1 HOUR)",
                [pipeline, _uuid.uuid4().hex, "not-this-host", 1],
            )
        finally:
            con.close()

        refused = box.run(max_seconds=60, timeout=120, expect_success=False)
        assert refused["returncode"] != 0, refused
        summary = json.loads((box.state_dir / "last_run.json").read_text())
        assert summary["ok"] is False
        assert summary["error_type"] == "LeaseLost", summary
        # The two projections agree, which is the whole point.
        assert summary["run_outcome"] == "error", summary
        assert summary["run_phase"] == "failed", summary
        assert summary["stop_reason"] == "error", summary

        con = duckdb.connect(str(box.duckdb_path))
        try:
            row = con.execute(
                "SELECT phase, terminal_reason FROM _cdc_flight.heartbeat "
                "WHERE pipeline = ? ORDER BY beat_at DESC LIMIT 1", [pipeline],
            ).fetchone()
        finally:
            con.close()
        assert row == (summary["run_phase"], summary["run_outcome"]), (
            f"the heartbeat says {row} and last_run.json says "
            f"{(summary['run_phase'], summary['run_outcome'])}"
        )
    finally:
        box.cleanup()
        box.reseed()


# --------------------------------------------------------------------------- #
# the heartbeat sink has ONE owner, and it is retired with a bound
# (Codex r5 MAJOR-1, reproduced against a real serialized DuckDB cursor)
# --------------------------------------------------------------------------- #
class _WedgedCursor:
    """A cursor that wedges once armed, and records every call made on it.

    Stands in for the shape the reviewer measured against a real serialized DuckDB
    connection: once a statement is in flight, the next statement queues behind it with
    no bound at all — and `close()` queues behind it too rather than cancelling it. Both
    facts are reproduced, because a `close()` that returned instantly would make the old
    code look bounded when it was not.

    It is armed rather than born wedged so the run can reach its terminal phase the way
    a real one does: the ordinary phase writes happen on a healthy sink, and the sink
    goes bad while the run is tearing down.
    """

    def __init__(self, release: threading.Event) -> None:
        self._release = release
        self._armed = threading.Event()
        self.closed = False
        self.close_calls = 0
        self.statements: list[str] = []

    def arm(self) -> None:
        self._armed.set()

    def execute(self, sql, *_a, **_k):
        self.statements.append(str(sql).split()[0].upper())
        if self._armed.is_set():
            self._release.wait(30)
        return self

    def close(self):
        self.close_calls += 1
        if self._armed.is_set():
            self._release.wait(30)
        self.closed = True


class _WedgedConnection:
    def __init__(self, release):
        self.cursor_obj = _WedgedCursor(release)

    def cursor(self):
        return self.cursor_obj


@pytest.fixture
def wedged():
    """A writer whose sink is healthy until the run starts tearing down."""
    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    release = threading.Event()
    con = _WedgedConnection(release)
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    phases.TERMINAL_GATE_TIMEOUT = 0.2
    phases.TERMINAL_WRITE_TIMEOUT = 0.3
    phases.RETIRE_TIMEOUT = 0.2
    phases.to(m.PHASE_RECONCILING)
    con.cursor_obj.arm()
    try:
        yield phases, con.cursor_obj, release
    finally:
        release.set()
        COMMIT_ACK.reset()


def test_the_whole_teardown_is_bounded_not_just_the_terminal_wait(wedged):
    """`finish()` **and** `close()`, against a sink that never answers.

    Round 5's fix bounded one wait site by moving the database call to a throwaway
    thread — and then `pipeline.run()` called `close()` on the very cursor that thread
    was still executing on, which blocked behind it. The bound has to cover the
    lifecycle, not the call: a run must be able to finish tearing down while a
    heartbeat statement is still in flight for ever.
    """
    phases, _cursor, _release = wedged
    started = time.monotonic()
    phases.finish(ok=False)
    phases.close()
    elapsed = time.monotonic() - started
    assert elapsed < 3.0, (
        f"teardown took {elapsed:.1f}s against a sink that never answers; the bound "
        "must cover finish() AND close(), not one wait site"
    )


def test_an_abandoned_sink_is_released_rather_than_closed(wedged):
    """Closing a cursor another thread is mid-statement on is the race, not the fix.

    `close()` on a busy DuckDB cursor blocks behind the statement (measured). So the
    retirement hands ownership over instead: the worker closes it if it ever returns,
    and it is a daemon, so it can never hold the process open.
    """
    phases, cursor, release = wedged
    phases.finish(ok=False)
    phases.close()

    assert phases.sink_retirement == "abandoned"
    assert cursor.close_calls == 0, (
        "the teardown called close() on a cursor a live worker still owns"
    )
    # ...and the worker takes it over once the statement finally returns.
    release.set()
    for _ in range(300):
        if cursor.closed:
            break
        time.sleep(0.02)
    assert cursor.closed, "nobody ever closed the released cursor"


def test_an_abandoned_terminal_write_is_recorded_in_the_summary(wedged):
    """`last_run.json` is then the ONLY record that this run terminalised at all.

    And it is one fact with one name: the database-call bound used to increment
    `ungated_terminal_writes`, which means "written without the commit->ack gate", so a
    single attempt looked like two separate ungated writes (Codex r5, subordinate note).
    """
    from cdc_flight.run_state import COMMIT_ACK

    phases, _cursor, _release = wedged
    phases.finish(ok=False)
    phases.close()

    summary = phases.summary()
    assert summary["terminal_phase_write_abandoned"] is True
    assert summary["heartbeat_sink_retirement"] == "abandoned"
    assert COMMIT_ACK.ungated_terminal_writes == 0, (
        "the database-call bound is not an ungated write; one attempt must not "
        "increment two different counters"
    )


def test_an_ordinary_run_closes_its_sink_and_says_so(con):
    """The healthy path still closes properly — the retirement is not a leak licence."""
    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    phases = RunPhaseWriter(con, pipeline=PIPELINE, runner_id=RUNNER)
    phases.to(m.PHASE_RECONCILING)
    phases.to(m.PHASE_STOPPING)
    phases.finish(ok=True)
    phases.close()
    assert phases.sink_retirement == "closed"
    assert phases.summary()["heartbeat_sink_retirement"] == "closed"
    assert phases.summary().get("terminal_phase_write_abandoned") is None
    assert _row(con)[0] == "stopped"
    COMMIT_ACK.reset()


def test_the_pipeline_reports_the_retirement_after_it_has_happened():
    """Ordering, asserted on the source: `close()` then `summary()`.

    How the sink was retired is only known once it has been retired, and an abandoned
    terminal write is exactly the case where the summary is the only surviving record.
    Sampling the summary first would drop the one fact that matters.
    """
    source = (
        Path(__file__).resolve().parents[2] / "src" / "cdc_flight" / "pipeline.py"
    ).read_text()
    tail = source[source.index("phases.finish(ok=run_ok)"):]
    assert tail.index("phases.close()") < tail.index("reported.update(phases.summary())"), (
        "the summary is sampled before the sink is retired, so an abandoned terminal "
        "write cannot reach last_run.json"
    )


def test_the_retirement_values_are_one_declared_domain_for_both_handles():
    """One vocabulary for the cursor AND its parent connection (Codex r6 MAJOR-1).

    Bounding the child and then closing the parent one statement later is the same
    unbounded wait one level out, so both retirements answer the same question and
    report the same values."""
    assert set(m.CONNECTION_RETIREMENT.values) == {"never_opened", "closed", "abandoned"}


@pytest.mark.slow
def test_a_real_serialized_duckdb_sink_cannot_hang_the_teardown(tmp_path):
    """The reviewer's own probe, executable: `finish()` THEN `close()`, on a real cursor.

    The fake above proves the ownership protocol; this proves the protocol is aimed at
    the right thing. DuckDB serialises statements on one connection, so a terminal write
    issued while another statement is in flight on that cursor queues behind it with no
    bound — and `cursor.close()` queues behind it too. Round 5 measured `finish()`
    returning at 10.013 s and `close()` still alive two seconds later, released only by
    an explicit interrupt.

    Production bounds on purpose (5 s + 2 s): a test that shrank them would prove the
    arithmetic and not the schedule.
    """
    from cdc_flight.run_state import COMMIT_ACK

    COMMIT_ACK.reset()
    connection = duckdb.connect(str(tmp_path / "dest.duckdb"))
    connection.execute("PRAGMA threads=1")
    dest_mod.ensure_control_schema(connection)
    phases = RunPhaseWriter(connection, pipeline=PIPELINE, runner_id=RUNNER)
    phases.to(m.PHASE_RECONCILING)

    # Occupy THE SINK — the same cursor the terminal write has to use.
    occupied = threading.Event()

    def _hog():
        occupied.set()
        with contextlib.suppress(Exception):  # the cursor may be released under us
            phases._sink.execute("SELECT max(md5(i::VARCHAR)) FROM range(400000000) t(i)")

    hog = threading.Thread(target=_hog, daemon=True)
    hog.start()
    occupied.wait(5)
    time.sleep(1.0)

    started = time.monotonic()
    phases.finish(ok=False)
    phases.close()
    elapsed = time.monotonic() - started

    bound = (
        phases.TERMINAL_GATE_TIMEOUT + phases.TERMINAL_WRITE_TIMEOUT
        + phases.RETIRE_TIMEOUT + 3.0
    )
    assert elapsed < bound, (
        f"teardown took {elapsed:.2f}s behind a statement in flight on the heartbeat "
        f"sink (bound {bound:.1f}s). This is the r5 schedule: finish() bounded, "
        "close() not"
    )
    assert phases.sink_retirement == "abandoned"
    assert phases.summary()["terminal_phase_write_abandoned"] is True
    hog.join(60)
    COMMIT_ACK.reset()
    with contextlib.suppress(Exception):
        connection.close()


# --------------------------------------------------------------------------- #
# the WHOLE teardown, in a real process, in production order (Codex r6 MAJOR-1)
# --------------------------------------------------------------------------- #
TEARDOWN_DRIVER = Path(__file__).resolve().parents[1] / "teardown_order_driver.py"


def _teardown_child(tmp_path, outcome: str):
    """Run the driver and return `(returncode, elapsed, summary)`. Never hangs the suite."""
    import os
    import subprocess

    summary_path = tmp_path / f"summary_{outcome}.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(Path(__file__).resolve().parents[2] / "src"), env.get("PYTHONPATH", "")]
    )
    started = time.monotonic()
    proc = subprocess.run(
        [
            sys.executable, str(TEARDOWN_DRIVER), str(tmp_path / f"{outcome}.duckdb"),
            str(summary_path), outcome,
        ],
        env=env, capture_output=True, text=True, timeout=90, check=False,
    )
    elapsed = time.monotonic() - started
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else None
    return proc, elapsed, summary


@pytest.mark.slow
@pytest.mark.parametrize("outcome", ["ok", "fail"])
def test_the_whole_teardown_exits_behind_a_wedged_sink(tmp_path, outcome):
    """The assertion neither earlier test could make: **the process exits**.

    Round 5 bounded `finish()` and production then called `close()`. Round 6 bounded
    `close()` and production then called `con.close()` on the parent handle — and the
    reviewer measured the writer retiring correctly at 7.005 s while the process was
    still alive with no exit code at 12 s. Each test stopped at the boundary it was
    written for; this one runs `pipeline.run()`'s whole `finally` block, in order, in a
    real process, against a real serialized DuckDB sink that never answers.

    Both outcomes are parametrised because the exit code has to be the one the RUN
    earned. A heartbeat that could not be written must not turn a successful run into a
    failure, and must not turn a failing one into a success.
    """
    proc, elapsed, _summary = _teardown_child(tmp_path, outcome)

    assert proc.returncode == (0 if outcome == "ok" else 1), (
        f"exit {proc.returncode} for an otherwise-{outcome} run\n"
        f"stdout={proc.stdout[-1500:]}\nstderr={proc.stderr[-1500:]}"
    )
    bound = (
        RunPhaseWriter.PHASE_WRITE_TIMEOUT            # the `stopping` write
        + RunPhaseWriter.TERMINAL_GATE_TIMEOUT
        + RunPhaseWriter.TERMINAL_WRITE_TIMEOUT       # the terminal write
        + RunPhaseWriter.RETIRE_TIMEOUT               # the cursor
        + 5.0                                         # the parent connection
        + 15.0                                        # interpreter start-up and slack
    )
    assert elapsed < bound, f"the process took {elapsed:.1f}s to exit (bound {bound:.0f}s)"


@pytest.mark.slow
def test_the_persisted_summary_records_what_happened_to_BOTH_handles(tmp_path):
    """`last_run.json` is the only surviving record when the heartbeat could not be written.

    It is written after `run()` returns, so a teardown that never returns cannot produce
    it — which is why the round-5 abandonment fields never reached the file they were
    added for until the parent handle was bounded too.
    """
    _proc, _elapsed, summary = _teardown_child(tmp_path, "ok")

    assert summary is not None, "the summary was never written, so nothing recorded this run"
    assert summary["terminal_phase_write_abandoned"] is True
    assert summary["heartbeat_sink_retirement"] == "abandoned"
    assert summary["destination_connection_release"] == "abandoned", (
        "the parent connection closed cleanly behind a live statement, which means the "
        "sink was not actually wedged and this test proved nothing"
    )
    assert summary["run_phase"] == "stopped"
