"""Rubric 1.9 (SM-D) — one per-relation state instead of four containers.

`CatalogChange.fenced` is the canonical decayed state flag in this codebase: its
docstring claimed it gated the destructive action, it never did, and it took a review
round to find out (Opus MINOR-2). The cause is that "where is this change" was spread
over `_unconfirmed`, `_pending`, `refused`, `awaiting_snapshot`, plus three counters on
the change itself, so no single thing could be wrong — or right.

Memory only, deliberately: a lost pending change is re-detected on the next poll, which
is correct, so persisting it would buy nothing and would need a new durable domain. What
it buys is that the observe → confirm → fence → apply pipeline is one readable value,
and that `applied` is reachable only through `due`.
"""

from __future__ import annotations

import pytest

from cdc_flight.catalog import (
    CHANGE_DROPPED,
    CHANGE_NEW,
    CatalogChange,
    CatalogWatcher,
)
from cdc_flight.machines import (
    CHANGE_APPLIED,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_OBSERVED,
    CHANGE_PENDING,
    CHANGE_REFUSED,
    CHANGE_SUPERSEDED,
)
from cdc_flight.states import IllegalTransition


def _change(kind: str = CHANGE_DROPPED, lsn: int = 100) -> CatalogChange:
    return CatalogChange(kind=kind, schema="app", table="customers", detected_lsn=lsn)


def _watcher(**kw) -> CatalogWatcher:
    return CatalogWatcher(
        dsn="postgresql://unused",
        publication="pub",
        schema="app",
        include={"app.customers"},
        known={},
        replicated={"app.customers"},
        poll_seconds=0,
        emit_marker=False,
        **kw,
    )


def test_a_change_starts_observed():
    assert _change().state == CHANGE_OBSERVED


def test_membership_of_the_pending_list_is_the_pending_state():
    watcher = _watcher()
    change = _change()
    watcher.queue(change)
    watcher.due(durable_lsn=0)
    assert change.state != CHANGE_OBSERVED


def test_the_fence_is_what_makes_a_change_due():
    watcher = _watcher()
    change = _change(lsn=100)
    watcher.queue(change)

    assert watcher.due(durable_lsn=50) == []
    assert change.state == "deferred"
    assert change.deferrals == 1

    assert watcher.due(durable_lsn=100) == [change]
    assert change.state == CHANGE_DUE


def test_a_non_destructive_change_is_due_immediately():
    """Nothing is removed for a `new` change, so there is nothing for the fence to
    protect; fencing them anyway kept the watcher writing marker records for no reason."""
    watcher = _watcher()
    change = _change(kind=CHANGE_NEW, lsn=999)
    watcher.queue(change)
    assert watcher.due(durable_lsn=0) == [change]
    assert change.state == CHANGE_DUE


def test_a_superseded_change_is_terminal_and_says_so():
    watcher = _watcher()
    change = _change()
    watcher.queue(change)
    watcher.due(durable_lsn=0)
    watcher._supersede("app.customers", CHANGE_DROPPED)
    assert change.state == CHANGE_SUPERSEDED
    with pytest.raises(IllegalTransition):
        change.to(CHANGE_DUE)


def test_applied_is_reachable_only_through_due():
    """The fence is `durable_lsn >= detected_lsn` and nothing may route around it."""
    change = _change()
    change.to(CHANGE_PENDING)
    with pytest.raises(IllegalTransition):
        change.to(CHANGE_APPLIED)
    change.to(CHANGE_DUE)
    change.to(CHANGE_APPLIED)
    assert change.state == CHANGE_APPLIED


def test_a_superseded_due_change_can_be_settled_as_applied():
    """Settlement records a committed stale plan without deleting newer work."""
    from cdc_flight.machines import CATALOG_CHANGE

    assert CATALOG_CHANGE.allows(CHANGE_SUPERSEDED, CHANGE_APPLIED)


def test_a_refused_change_can_come_back_round():
    """A refusal is not terminal: the mass-drop breaker and the staleness guard both
    leave the change pending so a later poll can resolve it."""
    change = _change()
    change.to(CHANGE_PENDING)
    change.to(CHANGE_DUE)
    change.to(CHANGE_REFUSED)
    change.to(CHANGE_DUE)
    change.to(CHANGE_APPLIED)


def test_moving_to_the_state_it_is_already_in_is_a_no_op():
    change = _change()
    change.to(CHANGE_OBSERVED)
    assert change.state == CHANGE_OBSERVED


def test_the_state_reaches_the_alert_context_and_fenced_is_derived_from_it():
    """`context()` is what an operator sees on a `table_dropped` alert.

    `fenced` used to be a second field somebody had to remember to set, and the review
    round that added SM-D left both representations in place. It is now READ OFF the
    state history: `marked` is the state that means "a marker was written", so there is
    nothing left that could disagree with it (Codex r1 MAJOR-1).
    """
    change = _change()
    change.to(CHANGE_PENDING)
    assert change.fenced is False, "no marker has been emitted yet"
    change.to(CHANGE_MARKED)
    assert change.context()["state"] == CHANGE_MARKED
    assert change.context()["fenced"] is True

    # ... and it stays true once the fence has opened and the change is applied.
    change.to(CHANGE_DUE)
    assert change.fenced is True
    assert change.history == [
        CHANGE_OBSERVED, CHANGE_PENDING, CHANGE_MARKED, CHANGE_DUE
    ]


def test_a_change_that_reached_due_without_a_marker_is_not_claimed_fenced():
    """The grace path: `durable_lsn` never caught up and the marker never landed.

    `catalog_apply` labels such an action "applied without a WAL fence marker", and it
    can only do that if `fenced` is false for a change that never passed through
    `marked`. A flag anybody could set could not carry that.
    """
    change = _change()
    change.to(CHANGE_PENDING)
    change.to(CHANGE_DEFERRED)
    change.to(CHANGE_DUE)
    assert change.fenced is False


# --------------------------------------------------------------------------- #
# Codex r1 MAJOR-1: the live, undeclared `due -> marked` event
# --------------------------------------------------------------------------- #
class _FakeMarker:
    """Stands in for `SourceMarker`: the emit always succeeds, nothing is written."""

    def __init__(self) -> None:
        self.enabled = True
        self.writes = 0
        self.last_error = None
        self.capable = True

    def emit(self, conn, reason, payload) -> bool:
        self.writes += 1
        return True


def test_a_poll_that_overlaps_the_applier_does_not_walk_a_due_change_backwards():
    """The reproduction. `due()` leaves the change in the live list until the COMMIT.

    The applier asks what is due and holds the change while it plans and applies; the
    polling thread then emits its next fence marker and used to move EVERY live change
    to `marked`, including that one — an edge `machines.CATALOG_CHANGE` does not declare
    and which is meaningless (its fence is already open). `poll_quietly` caught the
    `IllegalTransition`, wrote it to `last_error`, and let the run report success, so
    A51 row 51's promise of a loud failure was not kept either (Codex r1 MAJOR-1).
    """
    watcher = _watcher()
    watcher.marker = _FakeMarker()
    change = _change(lsn=100)
    watcher.queue(change)

    # 1. the applier takes it through the fence
    assert watcher.due(durable_lsn=100) == [change]
    assert change.state == CHANGE_DUE

    # 2. the polling thread emits another marker while the applier still holds it
    watcher._emit_marker(object(), [change])

    assert change.state == CHANGE_DUE, (
        "a change whose fence has already opened must not be walked back to `marked`"
    )
    assert watcher.machine_error is None
    # And a change that IS still waiting is marked by the same emit.
    waiting = _change(lsn=10_000)
    watcher.queue(waiting)
    watcher._emit_marker(object(), [waiting])
    assert waiting.state == CHANGE_MARKED


def test_an_undeclared_transition_during_a_poll_is_loud_not_swallowed():
    """A51 row 51's actual policy: the run must not be reported successful.

    `poll_quietly` fails soft on the SOURCE — a poll that could not connect is
    transient and the next one fixes it — and it used to fail soft on the machine too,
    which is a DDL fact that moved along a path nobody declared.
    """
    watcher = _watcher()

    def explode():
        raise IllegalTransition("catalog_change: 'due' -> 'marked' is not declared")

    watcher.poll = explode
    assert watcher.poll_quietly() == []
    assert watcher.machine_error is not None
    assert "due" in watcher.machine_error
    assert watcher.summary()["catalog_machine_error"] == watcher.machine_error
    # and the transient channel is untouched, so the two policies stay separable
    assert watcher.last_error is None


def test_the_confirmation_streak_advances_one_object_through_its_declared_edges():
    """`unconfirmed -> unconfirmed -> pending` is an edge set an OBJECT now takes.

    The first observation's object used to be discarded and the confirming poll built a
    new one that went `observed -> pending`, so the declared `unconfirmed -> pending`
    edge described nothing production ever did (Codex r1 MAJOR-1).
    """
    watcher = _watcher(confirm_polls=3)
    first = _change(lsn=10)
    assert watcher._confirm("app.customers", first) is None
    assert first.state == "unconfirmed"

    assert watcher._confirm("app.customers", _change(lsn=20)) is None
    assert watcher._unconfirmed["app.customers"] is first, "the same object carries on"
    assert first.confirmations == 2
    assert first.detected_lsn == 20, "the fence has to clear the LATEST agreeing poll"

    queued = watcher._confirm("app.customers", _change(lsn=30))
    assert queued is first
    assert first.history == ["observed", "unconfirmed"]
    watcher.queue(first)
    assert first.state == "pending"
