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

import json

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
