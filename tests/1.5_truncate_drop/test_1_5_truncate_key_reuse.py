"""Rubric 1.5 x 1.4 — TRUNCATE crossed with key reuse, which nothing tested.

Both reviews landed here independently. `tests/1.5_truncate_drop/test_1_5_truncate_fold.py`
covers rows-before / rows-after / keyless / spilled / multi-table / rolled-back
truncates, and `tests/1.4_pk_updates/` covers every key-reuse shape — but no test
crossed the two features, and the crossing is where the fold was wrong:

* a truncate cleared the plan's per-key bookkeeping but never recorded that the
  destination table's *pre-group image* is gone, so a later delete under a re-used
  key asked the destination "did this key exist?" and got `True` from a row the
  truncate had already logically removed (the `DELETE FROM` that empties the table
  is issued later, at write time);
* folding across source-transaction boundaries let a delete in transaction B be
  attributed to a pre-group row that transaction A's truncate had already removed.

`TRUNCATE staging; INSERT …; DELETE FROM staging WHERE <bad>;` in one transaction is
a stock ETL idiom, so shape A below is an ordinary case, not an exotic one.
"""

from __future__ import annotations

import pytest
from applier_lab import DATASET, Lab, data, end, keyed, truncate

CUSTOMERS = "cdcflight_app_customers"


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


def txn(number: str, events: list) -> list:
    counts: dict[str, int] = {}
    for event in events:
        name = f"{event.schema}.{event.table}"
        counts[name] = counts.get(name, 0) + 1
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, counts)]


def _row(ident: int, name: str) -> dict:
    return {"id": ident, "name": name}


def insert(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, after=_row(ident, name), op="c")


def delete(txn_id: str, order: int, lsn: int, ident: int, name: str):
    return data(txn_id, order, lsn, key={"id": ident}, before=_row(ident, name), op="d")


def preload(box: Lab, rows: dict[int, str]) -> None:
    events = [
        keyed("1", i + 1, 10 + i, ident, name) for i, (ident, name) in enumerate(rows.items())
    ]
    box.run(txn("1", events))


def state(box: Lab) -> list[tuple]:
    return box.q(f'SELECT id, name FROM "{DATASET}"."{CUSTOMERS}" ORDER BY id')


# --------------------------------------------------------------------------- #
# Opus BLOCKER-1: one transaction, truncate + key reuse
# --------------------------------------------------------------------------- #
def test_opus_b1_shape_a_truncate_insert_delete_of_the_same_key(lab):
    """A **spurious row** that never healed.

    ```sql
    BEGIN; TRUNCATE app.tr; INSERT INTO app.tr VALUES (5,'new');
           DELETE FROM app.tr WHERE id = 5; COMMIT;
    ```
    Verified emitted stream (`test_decoding`): `TRUNCATE`, `INSERT id=5`,
    `DELETE id=5`. Ordinary non-deferrable primary key, `REPLICA IDENTITY DEFAULT`.

    Postgres afterwards: 0 rows.  Measured before the fix: `{(5,'new')}`.
    """
    box = lab()
    preload(box, {5: "old", 6: "other"})
    box.run(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                insert("2", 2, 201, 5, "new"),
                delete("2", 3, 202, 5, "new"),
            ],
        )
    )
    assert state(box) == []


def test_opus_b1_shape_a_does_not_heal_or_reappear_later(lab):
    """The spurious row persisted across later unrelated groups; assert it is gone
    both immediately and after the table is used again."""
    box = lab()
    preload(box, {5: "old"})
    box.run(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                insert("2", 2, 201, 5, "new"),
                delete("2", 3, 202, 5, "new"),
            ],
        )
    )
    assert state(box) == []
    box.run(txn("3", [insert("3", 1, 300, 7, "later")]))
    assert state(box) == [(7, "later")]


def test_opus_b1_shape_b_truncate_then_a_key_move_off_a_pre_group_key(lab):
    """**Duplication** — the rubric's own `=2`, with an ordinary primary key.

    ```sql
    BEGIN; INSERT INTO app.tr VALUES (3,'c'); TRUNCATE app.tr;
           INSERT INTO app.tr VALUES (1,'z'); UPDATE app.tr SET id = 2 WHERE id = 1;
    COMMIT;
    ```
    Emitted: `INSERT 3`, `TRUNCATE`, `INSERT 1`, then the key change as `d(1)` + `c(2)`.

    Postgres afterwards: `{(2,'z')}`.  Measured before the fix: `{(1,'z'), (2,'z')}`
    — one source row present under two keys.
    """
    box = lab()
    preload(box, {1: "pre", 9: "other"})
    box.run(
        txn(
            "2",
            [
                insert("2", 1, 200, 3, "c"),
                truncate("2", 2, 201),
                insert("2", 3, 202, 1, "z"),
                delete("2", 4, 203, 1, "z"),
                insert("2", 5, 203, 2, "z"),
            ],
        )
    )
    assert state(box) == [(2, "z")]


def test_a_truncate_after_a_key_reuse_still_empties_everything(lab):
    """The other order: reuse a key, then truncate. Everything before the truncate
    is gone, including the plan's own rows."""
    box = lab()
    preload(box, {5: "old"})
    box.run(
        txn(
            "2",
            [
                insert("2", 1, 200, 5, "mid"),
                delete("2", 2, 201, 5, "mid"),
                truncate("2", 3, 202),
                insert("2", 4, 203, 5, "final"),
            ],
        )
    )
    assert state(box) == [(5, "final")]


# --------------------------------------------------------------------------- #
# Codex finding 3: the truncate and the delete are in DIFFERENT transactions of
# one commit group
# --------------------------------------------------------------------------- #
def test_codex_3_a_cross_transaction_truncate_leaves_no_zombie_row(lab):
    """A **zombie row** Postgres had deleted.

    ```
    destination/source: old row at key 1
    T1: TRUNCATE t; INSERT (1, 'new');
    T2: DELETE FROM t WHERE id = 1;
    ```
    Both transactions land in one destination commit group. T2's delete saw T1's row
    as an in-group acquisition and the pre-*group* probe still saw the old key-1 row,
    so the delete was read as consuming the old row and T1's row was preserved.

    Postgres afterwards: empty.  Measured before the fix: `[(1, 'new')]`.
    """
    box = lab()
    preload(box, {1: "old"})
    box.feed(txn("2", [truncate("2", 1, 200), insert("2", 2, 201, 1, "new")]))
    box.feed(txn("3", [delete("3", 1, 300, 1, "new")]))
    box.commit("two-units")
    assert state(box) == []


def test_a_cross_transaction_truncate_with_a_later_reinsert(lab):
    """T1 truncates and inserts, T2 deletes and re-inserts the same key."""
    box = lab()
    preload(box, {1: "old"})
    box.feed(txn("2", [truncate("2", 1, 200), insert("2", 2, 201, 1, "new")]))
    box.feed(
        txn("3", [delete("3", 1, 300, 1, "new"), insert("3", 2, 301, 1, "newest")])
    )
    box.commit("two-units")
    assert state(box) == [(1, "newest")]


def test_a_truncate_in_a_later_transaction_of_the_group_wins(lab):
    """T1 writes rows, T2 truncates: the destination ends empty."""
    box = lab()
    preload(box, {1: "old"})
    box.feed(txn("2", [insert("2", 1, 200, 4, "t1")]))
    box.feed(txn("3", [truncate("3", 1, 300)]))
    box.commit("two-units")
    assert state(box) == []


def test_the_cross_transaction_truncate_case_under_spill(lab):
    box = lab(unit_spill_events=1, unit_spill_bytes=1)
    preload(box, {1: "old"})
    box.feed(txn("2", [truncate("2", 1, 200), insert("2", 2, 201, 1, "new")]))
    box.feed(txn("3", [delete("3", 1, 300, 1, "new")]))
    box.commit("two-units")
    assert box.applier.spilled_events >= 1, "the test did not actually spill"
    assert state(box) == []


def test_the_cross_transaction_truncate_case_over_two_tables(lab):
    """`TRUNCATE a, b` in T1, deletes for both in T2."""
    box = lab()
    preload(box, {1: "old"})
    box.run(
        txn(
            "0",
            [
                data("0", 1, 60, table="orders", key={"id": 1}, after={"id": 1, "note": "old"}),
            ],
        )
    )
    box.feed(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                truncate("2", 2, 200, table="orders"),
                insert("2", 3, 201, 1, "new"),
                data("2", 4, 201, table="orders", key={"id": 1}, after={"id": 1, "note": "new"}),
            ],
        )
    )
    box.feed(
        txn(
            "3",
            [
                delete("3", 1, 300, 1, "new"),
                data("3", 2, 300, table="orders", key={"id": 1},
                     before={"id": 1, "note": "new"}, op="d"),
            ],
        )
    )
    box.commit("two-units")
    assert state(box) == []
    assert box.q(f'SELECT id FROM "{DATASET}"."cdcflight_app_orders"') == []
