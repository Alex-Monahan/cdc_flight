"""A rolled-back commit group must leave NOTHING behind in the process (Opus M-1).

`_reset_group()` used to be called only after a successful `COMMIT`. On the exception
path `_rollback_quietly()` cleared the markers, the catalog lists and the registry
cache — the author had clearly thought about post-rollback state — but left
`self._group`, `_group_events`, `_group_bytes`, `_created_in_txn` and
`_spill_commit_id` populated. Any later `commit_group` on the same `Applier` therefore
folded the rolled-back units a **second** time, in the same fold, alongside whatever
had arrived since.

For an idempotent shape (a plain PK move) double-folding is harmless, which is exactly
why the existing fault tests passed through a model this defect invalidates. For a
key-reuse shape it is not, and it was **measured** to lose a row:

```
clear_group=False  stale_units=1  replay=[(3,'b')]           *** LOST ROW ***
clear_group=True   stale_units=1  replay=[(2,'a'),(3,'b')]   CORRECT
```

Three of the leftovers are dangerous on their own, independently of the fold:
`_created_in_txn` surviving a rollback makes `write()` compute `fresh=True` and skip
the DELETE half of the merge (a duplication path), and it makes the fold's
destination probe answer "no row here" unconditionally.

The ADR's own rule is that a rolled-back group replays *from the source*. These tests
pin that this is true of the process as well as of the offset store.

**Rubric 1.9 made the defect unrepresentable rather than merely fixed.** The sixteen
fields are now one `applier.OpenGroup`, created at BEGIN and *replaced* at COMMIT and at
ROLLBACK, so "reset five of them and forget the other nine" is not something anybody can
write. These assertions therefore read against `applier.group` — and the strongest of
them is that the object itself is a different one.
"""

from __future__ import annotations

import pytest
from applier_lab import Lab, data, end

from cdc_flight import faults

TABLE = "cdcflight_app_customers"


@pytest.fixture
def lab(tmp_path):
    boxes: list[Lab] = []

    def _make(**cfg) -> Lab:
        box = Lab(tmp_path / f"lab{len(boxes)}.duckdb", **cfg)
        boxes.append(box)
        return box

    yield _make
    for box in boxes:
        box.close()


def _row(ident: int, name: str) -> dict:
    return {"id": ident, "name": name}


def insert(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, after=_row(ident, name), op="c")


def delete(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, before=_row(ident, name), op="d")


def txn(number: str, events: list) -> list:
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, {"app.customers": len(events)})]


def permutation(number: str, base_lsn: int) -> list:
    """The deferred `UPDATE t SET id = id + 1` over keys {1,2}: `{1,2} -> {2,3}`."""
    return txn(
        number,
        [
            delete(number, 1, base_lsn, 1, "a"),
            insert(number, 2, base_lsn, 2, "a"),
            delete(number, 3, base_lsn + 1, 2, "b"),
            insert(number, 4, base_lsn + 1, 3, "b"),
        ],
    )


def state(box: Lab) -> list[tuple]:
    return box.rows(TABLE, "id, name")


def preload(box: Lab) -> None:
    box.run(txn("1", [insert("1", 1, 10, 1, "a"), insert("1", 2, 11, 2, "b")]))


@pytest.mark.parametrize("point", ["begin", "mid_apply", "pre_commit"])
def test_a_rolled_back_group_is_not_folded_a_second_time(lab, monkeypatch, point):
    """The measured case. Fault, then re-deliver the same transaction."""
    box = lab()
    preload(box)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"{point}:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(permutation("2", 200))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()

    assert state(box) == [(1, "a"), (2, "b")], "the rollback left the old state"
    assert box.applier.group.units == [], "the failed group must not stay buffered"
    assert box.applier.group.created_in_txn == set()
    assert box.applier.group.spill_commit_id is None
    assert box.applier.group.txn_open is False

    # Debezium replays the transaction from the durable resume point.
    box.run(permutation("2", 200))
    assert state(box) == [(2, "a"), (3, "b")], "the replay must not lose a row"


def test_a_rolled_back_group_does_not_contaminate_a_different_transaction(lab, monkeypatch):
    """The stale units used to be folded *alongside* whatever arrived next."""
    box = lab()
    preload(box)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(permutation("2", 200))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()

    # A completely unrelated transaction arrives before the replay does.
    box.run(txn("3", [insert("3", 1, 400, 9, "z")]))
    assert state(box) == [(1, "a"), (2, "b"), (9, "z")], (
        "the unrelated group must not carry the rolled-back one's events"
    )
    # No replay assertion here on purpose: this group has advanced the durable resume
    # point past lsn 200, so a re-delivery of transaction 2 is (correctly) fenced. In a
    # real run that ordering cannot occur - Debezium delivers in order and the offset it
    # would resume from still precedes transaction 2 - which is why the *first*
    # assertion is the one that matters: the fold of transaction 3 was clean.
    assert box.applier.fenced_units == 0


def test_the_rolled_back_group_object_is_replaced_not_edited(lab, monkeypatch):
    """The 1.9 claim: a partially-reset group has no representation.

    Not "every field was reset" — that is the assertion that was true of the success
    path and false of the failure path for a whole review round. The object identity is
    the thing: whatever the discarded group held, nothing holds it now.
    """
    box = lab()
    preload(box)
    before = box.applier.group
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(permutation("2", 200))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert box.applier.group is not before
    # And a fresh one, field for field, without anybody having enumerated the fields.
    from cdc_flight.applier import OpenGroup

    fresh = OpenGroup()
    for name in fresh.__dataclass_fields__:
        if name == "opened_at":
            continue  # a timestamp; "is a fresh one" is the assertion above
        assert getattr(box.applier.group, name) == getattr(fresh, name), name


def test_the_deferral_of_a_failed_group_is_counted(lab, monkeypatch):
    """A run that discards whole transactions must say so (Opus MINOR-9's rule)."""
    box = lab()
    preload(box)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(permutation("2", 200))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    stats = box.applier.stats()
    assert stats["deferred_units"] == 1
    assert stats["deferred_events"] == 4


def test_a_rolled_back_group_that_created_a_table_does_not_skip_the_merge(lab, monkeypatch):
    """`_created_in_txn` surviving a rollback is a duplication path in its own right.

    The CREATE is gone with the transaction, so a stale entry makes the next group
    compute `fresh=True` and skip the DELETE half of the merge entirely.
    """
    box = lab()
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("1", [insert("1", 1, 10, 1, "a")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert box.applier.group.created_in_txn == set()
    assert not box.exists(TABLE)

    box.run(txn("1", [insert("1", 1, 10, 1, "a")]))
    box.run(txn("2", [insert("2", 1, 200, 1, "a2")]))
    assert state(box) == [(1, "a2")], "one row per key, not two"
