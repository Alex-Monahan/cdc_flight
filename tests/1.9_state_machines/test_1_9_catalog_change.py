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
    watcher._pending.append(change)
    watcher.due(durable_lsn=0)
    assert change.state != CHANGE_OBSERVED


def test_the_fence_is_what_makes_a_change_due():
    watcher = _watcher()
    change = _change(lsn=100)
    watcher._pending.append(change)

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
    watcher._pending.append(change)
    assert watcher.due(durable_lsn=0) == [change]
    assert change.state == CHANGE_DUE


def test_a_superseded_change_is_terminal_and_says_so():
    watcher = _watcher()
    change = _change()
    watcher._pending.append(change)
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


def test_the_state_reaches_the_alert_context():
    """`context()` is what an operator sees on a `table_dropped` alert. `fenced` alone
    told them about a marker; the state tells them where the change actually is."""
    change = _change()
    change.to(CHANGE_PENDING)
    change.to(CHANGE_MARKED)
    assert change.context()["state"] == CHANGE_MARKED
    assert change.context()["fenced"] is False, (
        "the marker flag and the state are different facts, and conflating them is "
        "exactly the decay Opus MINOR-2 found"
    )
