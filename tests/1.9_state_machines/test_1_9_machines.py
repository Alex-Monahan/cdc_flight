"""Rubric 1.9 — the machines themselves, and the past bugs they make illegal.

The rubric grades *no state machines = 1, one big state machine = 3, an appropriate
number (over 1) = 5*. "Appropriate" is the load-bearing word and it is not a count you
can assert, so what this file asserts instead is the property the count is a proxy for:
**each consistency-affecting state has exactly one owner, one declared domain and one
declared edge set, and the transitions that produced this project's measured
regressions are edges that do not exist.**

Every test here is in the default suite and runs in milliseconds: no JVM, no Postgres,
no subprocess. A guard that only runs when somebody remembers to ask for it is not a
guard, and a guard that costs six minutes is not one either.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from cdc_flight import machines as m
from cdc_flight.states import Domain, IllegalTransition, Machine, UnknownState, ranked


# --------------------------------------------------------------------------- #
# the mechanism
# --------------------------------------------------------------------------- #
def test_a_machine_refuses_an_undeclared_edge():
    machine = Machine("t_basic", states=("a", "b", "c"), edges=(("a", "b"),))
    machine.check("a", "b")
    with pytest.raises(IllegalTransition) as raised:
        machine.check("b", "c")
    assert "t_basic" in str(raised.value)
    assert "'b' -> 'c'" in str(raised.value)


def test_a_machine_refuses_a_state_outside_its_domain():
    machine = Machine("t_domain", states=("a", "b"), edges=(("a", "b"),))
    assert machine.parse("a") == "a"
    with pytest.raises(UnknownState):
        machine.parse("banana")
    with pytest.raises(UnknownState):
        machine.check("a", "banana")


def test_a_machine_cannot_declare_an_edge_to_a_state_it_does_not_have():
    with pytest.raises(ValueError):
        Machine("t_bad", states=("a",), edges=(("a", "z"),))


def test_the_transition_table_is_data_not_prose():
    machine = Machine(
        "t_table", states=("a", "b"), edges=(("a", "b"),), terminal=("b",),
        durable="somewhere",
    )
    assert machine.table() == [
        {"machine": "t_table", "from": "a", "to": "b", "terminal": True,
         "durable": "somewhere"}
    ]


def test_the_cells_with_no_edge_are_enumerable():
    """The mechanism behind the 4.7 inventory's UNDEFINED bucket.

    Today's nine UNDEFINED failure modes were found by reviewers reading code for
    a day. A cell of `states x states` with no declared edge is findable by `pytest`.
    """
    machine = Machine("t_cells", states=("a", "b"), edges=(("a", "b"),))
    assert machine.unreachable_cells() == [("b", "a")]


def test_a_ranked_machine_allows_escalation_and_only_escalation():
    machine = ranked("t_rank", order=("low", "mid", "high"))
    machine.check("low", "high")
    machine.check("mid", "high")
    with pytest.raises(IllegalTransition):
        machine.check("high", "mid")


def test_a_domain_is_frozen_and_validated():
    domain = Domain("t_vals", values=("x", "y"))
    assert "x" in domain
    assert "z" not in domain
    assert domain.parse("y") == "y"
    with pytest.raises(UnknownState):
        domain.parse("z")


# --------------------------------------------------------------------------- #
# the shape of the answer to rubric 1.9
# --------------------------------------------------------------------------- #
DECLARED = {
    "table_lifecycle",
    "run_phase",
    "run_outcome",
    "acquisition_recovery",
    "catalog_change",
    # rev 14. The architecture review's initial set covered only the states then
    # *visible*; this one was found by reproducing the inconsistency it allows, which is a
    # better argument for a machine than any of the original four had (Codex r5
    # BLOCKER-1). "Can an observed relation identity be adopted as history for rows the
    # destination already holds?" was a derived expression over a registry row, a table
    # lifecycle, a destination row count and an in-process poll counter.
    "catalog_baseline",
}


def test_the_declared_machines_are_the_ones_the_review_said_to_build():
    """The consistency-affecting states, plus the outcome precedence. Not one, not ten.

    The architecture review said yes to five machines and no to seven other candidates;
    `machines.py`'s module docstring carries the argument for each `no`. This asserts
    the *set*, so adding a machine (or quietly dropping one) is a decision somebody has
    to make in this file rather than a diff nobody notices.
    """
    assert set(_declared()) == DECLARED


def _declared() -> dict[str, Machine]:
    """Only the machines `cdc_flight/machines.py` itself declares.

    The global registry also holds whatever a test constructed, which is the right
    behaviour for a registry and the wrong basis for "these are the system's machines".
    """
    return {
        value.name: value
        for value in vars(m).values()
        if isinstance(value, Machine)
    }


def _published_inventory(path: Path) -> dict[str, tuple[int, int]]:
    """Current markdown inventory as ``machine -> (states, edges)``."""
    rows: dict[str, tuple[int, int]] = {}
    for line in path.read_text().splitlines():
        if not line.startswith("| `"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 5:
            continue
        name = cells[0].strip("`")
        if name not in DECLARED:
            continue
        edge_count = re.match(r"\d+", cells[3])
        if cells[2].isdigit() and edge_count is not None:
            rows[name] = (int(cells[2]), int(edge_count.group()))
    return rows


def test_current_machine_inventories_are_generated_from_the_declarations():
    """Round 8 MINOR-2: state/edge counts cannot become hand-maintained claims."""
    root = Path(__file__).resolve().parents[2]
    expected = {
        name: (len(machine.states), len(machine.edges))
        for name, machine in _declared().items()
    }
    for relative in ("docs/adr/0001-transactional-applier.md", "RUBRIC_STATUS.md"):
        published = _published_inventory(root / relative)
        assert published == expected, (
            f"{relative}'s current machine inventory differs from machines.py; "
            "regenerate the state and edge counts from the declarations"
        )


def test_more_than_one_machine_and_each_owns_exactly_one_state():
    declared = _declared()
    assert len(declared) > 1, "the rubric's 3-band is 'only 1 big state machine'"
    # No two machines share a durable location: one state, one owner, one column.
    durable = [mm.durable for mm in declared.values() if mm.durable]
    assert len(durable) == len(set(durable)), durable


def test_the_commit_group_is_deliberately_not_a_machine():
    """Invariant O is why, and saying so is part of the 1.9 answer.

    A durable machine around the commit group would suggest it has recoverable
    intermediate states, which is the opposite of the claim the whole design rests on.
    `applier.OpenGroup` is the alternative: a plain object, replaced wholesale.
    """
    assert "commit_group" not in _declared()
    from cdc_flight.applier import OpenGroup

    assert not hasattr(OpenGroup, "check")


def test_the_assembler_is_not_reimplemented_as_a_machine():
    """It already is one, with its own error type, and it has never produced a blocker."""
    from cdc_flight.assembler import TransactionAssemblyError

    assert issubclass(TransactionAssemblyError, Exception)
    assert "transaction_assembly" not in _declared()


# --------------------------------------------------------------------------- #
# the measured past bugs, as edges that do not exist
# --------------------------------------------------------------------------- #
def test_A49_a_symptom_cannot_overwrite_the_diagnosis():
    """The bug, three review rounds running: a dark source makes `close()` hang.

    `supervisor.py`'s `finally` overwrote `stop_reason='source_dark'` with `'hung'`, so
    `last_run.json` reported the consequence and lost the cause. The fix at the time was
    `if stop_reason not in ("source_dark", "engine_error")`, written out at two call
    sites; a tenth outcome had to remember both.
    """
    with pytest.raises(IllegalTransition):
        m.RUN_OUTCOME.check("source_dark", "hung")
    with pytest.raises(IllegalTransition):
        m.RUN_OUTCOME.check("engine_error", "hung")
    # ... and the escalations that DO happen are declared.
    m.RUN_OUTCOME.check("max_seconds", "hung")
    m.RUN_OUTCOME.check("idle", "catalog_unresolved")
    m.RUN_OUTCOME.check("hung", "engine_error")


def test_A49_the_precedence_puts_every_cause_above_its_symptom():
    order = m.OUTCOME_ORDER
    assert order.index("hung") < order.index("source_dark")
    assert order.index("hung") < order.index("engine_error")
    assert order.index("max_seconds") == 0, "the base value must be the least severe"


def test_a_recovery_cannot_skip_a_destructive_step():
    """`requested -> armed` would claim a slot was dropped that never was.

    A45: Debezium only pairs a snapshot with an exact WAL position when it creates the
    slot itself, so a re-snapshot against a surviving slot resumes past the snapshot's
    consistent point — the loss window rubric 1.8 exists to close.
    """
    with pytest.raises(IllegalTransition):
        m.ACQUISITION_RECOVERY.check("requested", "armed")
    with pytest.raises(IllegalTransition):
        m.ACQUISITION_RECOVERY.check("offsets_file_deleted", "armed")


def test_a_recovery_can_only_be_cleared_once_it_is_armed():
    """Clearing earlier discards the record of a half-done destructive sequence."""
    m.ACQUISITION_RECOVERY.check("armed", "absent")
    for phase in ("requested", "offsets_file_deleted", "resume_point_deleted"):
        with pytest.raises(IllegalTransition):
            m.ACQUISITION_RECOVERY.check(phase, "absent")


def test_a_table_cannot_become_complete_without_having_been_owed_or_snapshotted():
    """`none -> complete` and `absent -> complete` are the "it just looks healthy" edges.

    Codex B1 / Opus BLOCKER-1 was exactly this shape at one remove: completion was
    inferred across four modules and a live table's rows were deleted on a claim nothing
    had checked. A table reaches `complete` from a swapped shadow or from having been
    PROVEN empty at the source, and from nowhere else.
    """
    with pytest.raises(IllegalTransition):
        m.TABLE_LIFECYCLE.check("none", "complete")
    with pytest.raises(IllegalTransition):
        m.TABLE_LIFECYCLE.check("absent", "complete")
    m.TABLE_LIFECYCLE.check("in_progress", "complete")
    m.TABLE_LIFECYCLE.check("awaiting_snapshot", "complete")


def test_a_second_snapshot_cannot_start_over_a_durable_half_finished_one():
    """`in_progress -> in_progress` is the residue the architecture review found.

    A process killed inside a snapshot leaves the row `in_progress`. If a later run
    could simply open a second shadow over it, the fact that a previous image was never
    finished would leave no trace anywhere — which is how a table came to be owed work
    and selected by no queue. Start-up promotion (`in_progress -> awaiting_snapshot`) is
    the declared route, and it is the only one.
    """
    with pytest.raises(IllegalTransition):
        m.TABLE_LIFECYCLE.check("in_progress", "in_progress")
    m.TABLE_LIFECYCLE.check("in_progress", "awaiting_snapshot")
    m.TABLE_LIFECYCLE.check("awaiting_snapshot", "in_progress")


def test_owing_work_is_derived_from_the_terminal_set_not_restated():
    """A second literal list of "which states mean owed" is a second thing to forget."""
    assert frozenset({"in_progress", "awaiting_snapshot"}) == m.LIFECYCLE_OWING_WORK
    for state in m.LIFECYCLE_OWING_WORK:
        assert not m.TABLE_LIFECYCLE.is_terminal(state)


def test_a_catalog_change_cannot_be_applied_without_becoming_due():
    """The fence is `durable_lsn >= detected_lsn`; nothing may route around it."""
    for state in ("observed", "pending", "marked", "deferred", "refused"):
        with pytest.raises(IllegalTransition):
            m.CATALOG_CHANGE.check(state, "applied")
    m.CATALOG_CHANGE.check("due", "applied")


def test_a_superseded_or_applied_change_is_terminal():
    assert m.CATALOG_CHANGE.is_terminal("applied")
    assert m.CATALOG_CHANGE.is_terminal("superseded")
    assert m.CATALOG_CHANGE.successors("applied") == set()
    assert m.CATALOG_CHANGE.successors("superseded") == set()


def test_a_run_cannot_reach_stopped_without_stopping():
    """`stopping` is where the lease is released; skipping it is a leaked lease."""
    for phase in ("starting", "reconciling", "streaming", "draining"):
        with pytest.raises(IllegalTransition):
            m.RUN_PHASE.check(phase, "stopped")
    m.RUN_PHASE.check("stopping", "stopped")


def test_every_run_phase_can_fail():
    """A phase that cannot fail is a phase whose failure has no name."""
    for phase in m.RUN_PHASE.states:
        if m.RUN_PHASE.is_terminal(phase):
            continue
        m.RUN_PHASE.check(phase, "failed")


# --------------------------------------------------------------------------- #
# the frozen domains
# --------------------------------------------------------------------------- #
def test_the_slot_verdict_domain_matches_what_check_slot_can_actually_return():
    """`RESNAPSHOT_DECISIONS` and `FORGET_CATALOG_DECISIONS` were declared and consumed
    only by a test; the verdict strings they name have to be in the frozen domain or
    the two vocabularies have already drifted."""
    from cdc_flight import reconcile

    for decision in reconcile.RESNAPSHOT_DECISIONS:
        assert decision in m.SLOT_VERDICTS, decision
    for decision in reconcile.FORGET_CATALOG_DECISIONS:
        assert decision in m.SLOT_VERDICTS, decision


def test_the_reconciliation_decision_domain_is_frozen():
    assert "resume" in m.RECONCILE_DECISIONS
    assert "orphan_accepted_resnapshot" in m.RECONCILE_DECISIONS
    with pytest.raises(UnknownState):
        m.RECONCILE_DECISIONS.parse("probably_fine")


def test_the_source_health_domain_names_the_fail_open_state():
    """A51 row 50: `unknown_never_sampled` was the state with no name at all."""
    assert "unknown_never_sampled" in m.SOURCE_HEALTH_STATES
    assert "dark" in m.SOURCE_HEALTH_STATES


# --------------------------------------------------------------------------- #
# SourceHealth: a fold with a declared classification, NOT a machine
# --------------------------------------------------------------------------- #
def test_source_health_classifies_itself_into_the_declared_domain():
    """The classification was written out three times and its most important value
    (`unknown_never_sampled`, A51 row 50's fail-open) had no name at all."""
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://unused", slot_name="s")
    assert health.state() == "unsampled"

    health._last = SlotSample(at=0.0, error="no route to host")
    health._unknown_since = 0.0
    assert health.state() == "unknown_never_sampled", (
        "a source that was dark before we ever looked is the fail-open, and it is now "
        "a named state rather than a fall-through"
    )

    health._ever_sampled = True
    assert health.state() == "unknown"
    assert health.state(dark_after=0.001) == "dark"

    health._last = SlotSample(at=0.0, exists=True, active=True, lag_bytes=0)
    assert health.state() in ("streaming", "not_streaming")
    assert health.state() in m.SOURCE_HEALTH_STATES


def test_source_health_is_not_a_machine():
    """It is a fold over observations: no durable state, no transition anybody can cut.
    Adding a machine here would be ceremony, and the review said so."""
    assert "source_health" not in _declared()
    assert "source_health" in {d.name for d in m.__dict__.values() if isinstance(d, Domain)}
