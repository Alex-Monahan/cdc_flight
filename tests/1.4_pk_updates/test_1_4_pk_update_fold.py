"""Rubric 1.4 — a primary-key update, folded by the shipped applier.

Debezium never emits a key-changing UPDATE as an `u` for Postgres: whenever the
old key is available (which it is under `REPLICA IDENTITY DEFAULT` *and* `FULL` —
pgoutput sends the old key when the key changes) `RelationalChangeRecordEmitter`
splits it into `d(old key)` + `c(new key)` inside the same transaction
(`emitUpdateAsPrimaryKeyChangeRecord`, verified in the vendored 3.6 source). Both
events therefore land in one `CompleteUnit`, and a commit group holds whole units,
so "delete the old key and insert the new one" is atomic **for free** — that part
of 1.4 is a property of the commit protocol, not of a special case, and the first
test here is the proof rather than an assumption.

What is *not* free is the fold: a group collapses many events per key into one
final row, and a key can be **reused inside one transaction**. Two shapes broke:

| shape | Postgres truth | what the fold produced |
|---|---|---|
| `UPDATE t SET id = id + 1` over two rows with a DEFERRABLE primary key: `d(1) c(2) d(3=old 2) c(3)` | `{2, 3}` | `{3}` — a **lost row** |
| a key-changing `u` (the defensive non-Postgres path) after an insert of the old key in the same group | `{2}` | `{1, 2}` — **duplication**, the rubric's own `=2` |

Both are decided by one question the fold could not previously ask: *does the key
this event removes belong to a row that existed before this commit group, or to a
row this group itself inserted?* Postgres answers it — the second `d` of an
`id = id + 1` permutation targets the pre-transaction tuple, while the second `d`
of a chained `1->2->3` targets the row the transaction just created — and the
answer is in the destination, so `TableWork` asks it (once per ambiguous key) and
caches it.

Driven through `tests/applier_lab.py`: the real `TransactionAssembler`, the real
`Applier`, a real DuckDB file. Milliseconds, and the interleavings are exact.
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


# --------------------------------------------------------------------------- #
# event shapes
# --------------------------------------------------------------------------- #
def _row(ident: int, name: str) -> dict:
    return {"id": ident, "name": name}


def insert(txn: str, order: int, lsn: int, ident: int, name: str):
    return data(txn, order, lsn, key={"id": ident}, after=_row(ident, name), op="c")


def delete(txn: str, order: int, lsn: int, ident: int, name: str = "x"):
    return data(txn, order, lsn, key={"id": ident}, before=_row(ident, name), op="d")


def update(txn: str, order: int, lsn: int, ident: int, name: str, was: str = "x"):
    return data(
        txn, order, lsn, key={"id": ident}, before=_row(ident, was),
        after=_row(ident, name), op="u",
    )


def key_change_as_one_update(txn: str, order: int, lsn: int, old: int, new: int, name: str):
    """The `u` shape whose `before.key != after.key`.

    Postgres never produces it (see the module docstring); other connectors do, and
    the applier has always claimed to handle it, so it is pinned here.
    """
    return data(
        txn, order, lsn, key={"id": new}, before=_row(old, name),
        after=_row(new, name), op="u",
    )


def pk_change(txn: str, order: int, lsn: int, old: int, new: int, name: str):
    """What Debezium actually emits: `d(old)` then `c(new)`, one shared LSN."""
    return [delete(txn, order, lsn, old, name), insert(txn, order + 1, lsn, new, name)]


def txn(number: str, events: list, *, table: str = "app.customers") -> list:
    """Wrap events in the END marker the assembler demands (no BEGIN needed:
    Debezium suppresses it after a restart and the assembler opens implicitly).

    The END's LSN is one past the last event's, so a later transaction with higher
    LSNs is never mistaken for a replay by the idempotency fence.
    """
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, {table: len(events)})]


def state(lab: Lab) -> list[tuple]:
    return lab.rows(TABLE, "id, name")


def preload(lab: Lab, rows: dict[int, str]) -> None:
    events = [insert("1", i + 1, 10 + i, ident, name) for i, (ident, name) in enumerate(rows.items())]
    lab.run(txn("1", events))


# --------------------------------------------------------------------------- #
# the part the commit protocol already gives us (rubric 1.4's core)
# --------------------------------------------------------------------------- #
def test_a_primary_key_update_lands_under_the_new_key_only(lab):
    """`UPDATE app.customers SET id = 9001 WHERE id = 1` — the rubric's own case."""
    box = lab()
    preload(box, {1: "ada"})
    box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "ada")))
    assert state(box) == [(9001, "ada")], "the row must exist under the new key only"


def test_the_delete_and_the_insert_cannot_be_split_across_commit_groups(lab):
    """No consumer can ever see the row under both keys, or under neither.

    The `d` and the `c` are two events of one Postgres transaction, and the
    assembler only emits *whole* transactions, so there is no sequence of commit
    triggers that separates them. Asserted by driving the applier with commit
    triggers set to fire on **every single event** and counting the groups that
    could see an intermediate state.
    """
    box = lab(commit_max_events=1, commit_max_bytes=1, commit_max_age=0.0)
    preload(box, {1: "ada"})
    before = box.applier.commit_groups
    box.feed(pk_change("2", 1, 200, 1, 9001, "ada"))
    # Both events are buffered inside one incomplete unit: nothing may commit yet.
    assert box.applier.commit_groups == before
    assert state(box) == [(1, "ada")]
    box.feed([end("2", 2, 210, {"app.customers": 2})])
    box.commit("triggers")
    assert box.applier.commit_groups == before + 1, "one group, not two"
    assert state(box) == [(9001, "ada")]


def test_a_primary_key_update_mixed_with_other_changes_to_the_same_row(lab):
    """One transaction: update the row, change its key, then update it again."""
    box = lab()
    preload(box, {1: "a"})
    box.run(
        txn(
            "2",
            [
                update("2", 1, 200, 1, "b", was="a"),
                *pk_change("2", 2, 201, 1, 9001, "b"),
                update("2", 4, 202, 9001, "c", was="b"),
            ],
        )
    )
    assert state(box) == [(9001, "c")]


def test_a_primary_key_update_whose_new_key_was_just_freed(lab):
    """`DELETE FROM t WHERE id = 2; UPDATE t SET id = 2 WHERE id = 1;` in one txn.

    This is how a collision is resolved without a deferrable constraint, and it is
    the direction the fold has always got right: the delete of key 2 precedes the
    insert of key 2.
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 2, "b"),
                *pk_change("2", 2, 201, 1, 2, "a"),
            ],
        )
    )
    assert state(box) == [(2, "a")]


def test_a_chained_key_change_keeps_only_the_final_key(lab):
    """`UPDATE SET id=2 WHERE id=1; UPDATE SET id=3 WHERE id=2;` — truth is `{3}`.

    Byte-identical event stream to the permutation below, and the opposite answer.
    What separates them is whether key 2 existed *before* the transaction.
    """
    box = lab()
    preload(box, {1: "a"})
    box.run(
        txn(
            "2",
            [*pk_change("2", 1, 200, 1, 2, "a"), *pk_change("2", 3, 201, 2, 3, "a")],
        )
    )
    assert state(box) == [(3, "a")]


def test_an_insert_and_delete_of_one_key_inside_a_transaction_leaves_nothing(lab):
    box = lab()
    box.run(txn("2", [insert("2", 1, 200, 5, "a"), delete("2", 2, 201, 5, "a")]))
    assert state(box) == []


def test_delete_reinsert_delete_of_a_pre_existing_key_leaves_nothing(lab):
    """The pre-transaction row is consumed by the FIRST delete, so the second one
    can only be targeting the row the transaction inserted."""
    box = lab()
    preload(box, {2: "orig"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 2, "orig"),
                insert("2", 2, 201, 2, "new"),
                delete("2", 3, 202, 2, "new"),
            ],
        )
    )
    assert state(box) == []


# --------------------------------------------------------------------------- #
# the two shapes that were wrong (both fixed by `table_work._remove`)
# --------------------------------------------------------------------------- #
def test_a_deferred_primary_key_permutation_keeps_both_rows(lab):
    """`UPDATE t SET id = id + 1` with a DEFERRABLE PRIMARY KEY: `{1,2} -> {2,3}`.

    Postgres allows this (the uniqueness check is deferred to COMMIT) and emits
    `d(1) c(2) d(2) c(3)`. The `d(2)` targets the *pre-transaction* row 2, not the
    row this transaction just gave key 2 — which is why collapsing by key alone
    dropped a row that Postgres still holds.
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [*pk_change("2", 1, 200, 1, 2, "a"), *pk_change("2", 3, 201, 2, 3, "b")],
        )
    )
    assert state(box) == [(2, "a"), (3, "b")]


def test_a_key_changing_update_after_an_insert_of_the_old_key_does_not_duplicate(lab):
    """The defensive `u` path: `c(1)` then `u(before id=1, after id=2)`.

    The old key has to be *removed from the plan*, not merely deleted from the
    destination table: the row inserted under key 1 by the same group was still in
    the plan and was re-inserted, so the destination held the row under both keys —
    the rubric's `duplication=2`, reachable through the shape ADR §6.1 claims to
    normalise.
    """
    box = lab()
    box.run(
        txn(
            "2",
            [insert("2", 1, 200, 1, "a"), key_change_as_one_update("2", 2, 201, 1, 2, "a")],
        )
    )
    assert state(box) == [(2, "a")]


def test_a_key_changing_update_of_a_pre_existing_row_moves_it(lab):
    box = lab()
    preload(box, {1: "a"})
    box.run(txn("2", [key_change_as_one_update("2", 1, 200, 1, 2, "a")]))
    assert state(box) == [(2, "a")]


def test_a_key_changing_update_permutation_keeps_both_rows(lab):
    """The `u`-shaped equivalent of the deferred permutation."""
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [
                key_change_as_one_update("2", 1, 200, 1, 2, "a"),
                key_change_as_one_update("2", 2, 201, 2, 3, "b"),
            ],
        )
    )
    assert state(box) == [(2, "a"), (3, "b")]


# --------------------------------------------------------------------------- #
# composite keys, and the group boundary
# --------------------------------------------------------------------------- #
def _composite(txn_id: str, order: int, lsn: int, ident: int, day: str, actor: str, op="c"):
    key = {"id": ident, "occurred_at": day}
    row = {"id": ident, "occurred_at": day, "actor": actor}
    kwargs = {"after": row} if op != "d" else {"before": row}
    return data(txn_id, order, lsn, table="audit_log", key=key, op=op, **kwargs)


def test_a_composite_primary_key_update_moves_the_whole_key(lab):
    """`app.audit_log`'s key is `(id, occurred_at)`; moving a row to another
    partition changes the second component."""
    box = lab()
    box.run(
        [
            _composite("1", 1, 100, 7, "2026-07-01", "ada"),
            end("1", 1, 101, {"app.audit_log": 1}),
        ]
    )
    box.run(
        [
            _composite("2", 1, 200, 7, "2026-07-01", "ada", op="d"),
            _composite("2", 2, 200, 7, "2026-08-02", "ada"),
            end("2", 2, 201, {"app.audit_log": 2}),
        ]
    )
    assert box.rows("cdcflight_app_audit_log", "id, occurred_at") == [(7, "2026-08-02")]


def test_two_transactions_in_one_group_each_move_one_key(lab):
    """A commit group holds several whole transactions; the fold must keep each
    transaction's key moves independent."""
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.feed(txn("2", pk_change("2", 1, 200, 1, 11, "a")))
    box.feed(txn("3", pk_change("3", 1, 300, 2, 12, "b")))
    box.commit("two-units")
    assert state(box) == [(11, "a"), (12, "b")]


def test_a_primary_key_update_spread_over_a_spilled_unit(lab):
    """The `d` staged to `_cdc_flight.spill_events`, the `c` still in memory.

    A unit that spills keeps accumulating an in-memory tail, so a PK update can be
    split across the two storage representations. `unit_spill_events=1` forces it.
    """
    box = lab(unit_spill_events=1, unit_spill_bytes=1)
    preload(box, {1: "a"})
    box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "a")))
    assert state(box) == [(9001, "a")]
    assert box.applier.spilled_events >= 1, "the test did not actually spill"


# --------------------------------------------------------------------------- #
# faults around a PK-update unit
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("point", ["begin", "mid_apply", "pre_commit"])
def test_a_fault_while_applying_a_pk_update_leaves_the_old_state(lab, monkeypatch, point):
    """Every anchor before COMMIT must leave the destination on the OLD key.

    The group rolls back and Debezium replays it, so the row must not be half-moved
    (present under both keys, or under neither).
    """
    box = lab()
    preload(box, {1: "a"})
    # `<nth>` counts the data-carrying commit groups of the process, and the
    # preload was one of them.
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"{point}:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "a")))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert state(box) == [(1, "a")], "a rolled-back PK update must not be half-applied"

    # ... and the replay lands it exactly once.
    box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "a")))
    assert state(box) == [(9001, "a")]


def test_replaying_a_pk_update_after_the_ack_window_is_fenced(lab):
    """A crash between COMMIT and the acknowledgement replays the transaction.

    The idempotency fence drops it (its LSN is at or below the durable resume
    point), and even without the fence the merge is idempotent — the assertion is
    that the row exists once under the new key, never twice.
    """
    box = lab()
    preload(box, {1: "a"})
    box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "a")))
    box.run(txn("2", pk_change("2", 1, 200, 1, 9001, "a")))
    assert state(box) == [(9001, "a")]
    assert box.applier.fenced_units >= 1
