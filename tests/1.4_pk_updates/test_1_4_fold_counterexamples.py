"""Rubric 1.4 — the orderings the 1.4/1.5 review round *reproduced* against the
shipped applier, plus the ones it proved correct and which must stay correct.

Every case in the first section was executed by a reviewer, against a real
Postgres 18.1 cluster on `:15432` for the source truth and against the shipped
`Applier` for the destination answer, and the two disagreed. They are all one
defect family, and Codex named the root:

> the `TableWork` fold is GROUP-wide, but the delete-attribution ambiguity is
> per-SOURCE-TRANSACTION.

`table_work` now folds *physical rows*, not keys: `live[key]` is the list of rows
that currently wear that key, `START` stands for the row the destination already
held, and a delete removes **one** entry chosen by its before-image. That is the
model Postgres itself has, which is why the per-transaction ambiguity disappears
rather than being special-cased — a row a previous transaction of the same group
placed is simply a concrete entry in the list, and the "did it exist before?"
question is asked of the destination only for `START`.

Source truth for each case is stated as the `Postgres:` line and was verified with
`pg_logical_slot_get_changes` / `test_decoding` by the reviewers; the event streams
below are what Debezium 3.6 + pgoutput emits for them.
"""

from __future__ import annotations

import pytest
from applier_lab import Lab, data, end

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


def delete(txn: str, order: int, lsn: int, ident: int, name: str):
    """A delete under `REPLICA IDENTITY FULL`: the before-image is the whole row.

    This is the only identity a DEFERRABLE primary key can have (a deferrable
    unique index is not a valid replica identity, so `UPDATE` on the published
    table fails until `REPLICA IDENTITY FULL` is set — verified on the cluster),
    which is precisely why the disambiguating information is always present in the
    shapes that need it.
    """
    return data(txn, order, lsn, key={"id": ident}, before=_row(ident, name), op="d")


def delete_key_only(txn: str, order: int, lsn: int, ident: int):
    """A delete under `REPLICA IDENTITY DEFAULT`: the before-image is the key.

    Reachable only where the key is *not* deferrable, so at most one row can wear
    the key at any instant and no attribution question can arise.
    """
    return data(txn, order, lsn, key={"id": ident}, before={"id": ident}, op="d")


def txn(number: str, events: list, *, table: str = "app.customers") -> list:
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, {table: len(events)})]


def state(box: Lab) -> list[tuple]:
    return box.rows(TABLE, "id, name")


def preload(box: Lab, rows: dict[int, str]) -> None:
    events = [
        insert("1", i + 1, 10 + i, ident, name) for i, (ident, name) in enumerate(rows.items())
    ]
    box.run(txn("1", events))


# --------------------------------------------------------------------------- #
# the reproduced counterexamples
# --------------------------------------------------------------------------- #
def test_codex_1a_a_key_inserted_by_an_earlier_transaction_of_the_same_group(lab):
    """Codex finding 1, first reproduced case — a **lost row**.

    ```
    destination/source: row a at key 1
    T1: INSERT (2, 'b')
    T2: UPDATE t SET id = id + 1     -- deferred, over keys {1,2}
    ```
    T2 emits `d(1,a) c(2,a) d(2,b) c(3,b)`. For T2, key 2 existed *at transaction
    start* because T1 committed first — but the shipped probe answered "did key 2
    exist before this whole commit **group**?", where it does not, so `d(2)` was
    read as removing the row T2 itself had just put at key 2.

    Postgres: `{(2,'a'), (3,'b')}`.  Measured before the fix: `[(3, 'b')]`.
    """
    box = lab()
    preload(box, {1: "a"})
    box.feed(txn("2", [insert("2", 1, 200, 2, "b")]))
    box.feed(
        txn(
            "3",
            [
                delete("3", 1, 300, 1, "a"),
                insert("3", 2, 300, 2, "a"),
                delete("3", 3, 301, 2, "b"),
                insert("3", 4, 301, 3, "b"),
            ],
        )
    )
    box.commit("two-units")
    assert state(box) == [(2, "a"), (3, "b")]


def test_codex_1b_two_in_group_rows_wearing_one_key_and_one_is_deleted(lab):
    """Codex finding 1, second reproduced case — a **lost row**, one transaction.

    ```
    source: a at key 1, b at key 2
    BEGIN;  -- DEFERRABLE PRIMARY KEY, REPLICA IDENTITY FULL
      UPDATE t SET id = 3 WHERE id = 1;   -- a -> 3
      UPDATE t SET id = 3 WHERE id = 2;   -- b -> 3   (two rows transiently at 3)
      DELETE FROM t WHERE id = 3 AND name = 'a';
    COMMIT;
    ```
    Events: `d(1,a) c(3,a) d(2,b) c(3,b) d(3,a)`. Two *different* rows wear key 3,
    and a key-indexed `final` map can only remember one of them, so the last
    delete emptied the plan.

    Postgres: `{(3,'b')}`.  Measured before the fix: `[]`.
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 3, "a"),
                delete("2", 3, 201, 2, "b"),
                insert("2", 4, 201, 3, "b"),
                delete("2", 5, 202, 3, "a"),
            ],
        )
    )
    assert state(box) == [(3, "b")]


def test_opus_b2_a_pre_group_row_and_an_in_group_row_wearing_one_key(lab):
    """Opus BLOCKER-2 — a **lost row and a duplicated row**.

    ```
    source: a at key 1, b at key 2
    BEGIN;
      UPDATE t SET id = 2 WHERE id = 1;                -- two rows transiently at 2
      UPDATE t SET id = 5 WHERE id = 2 AND name = 'a'; -- move the in-group row off 2
    COMMIT;
    ```
    Real WAL, verbatim from `pg_logical_slot_get_changes`:

    ```
    table app.k: UPDATE: old-key: id:1 name:'a'  new-tuple: id:2 name:'a'
    table app.k: UPDATE: old-key: id:2 name:'a'  new-tuple: id:5 name:'a'
    ```
    which Debezium splits into `d(1,a) c(2,a) d(2,a) c(5,a)`. The `d(2,a)` removes
    the row *this transaction* moved onto key 2, so the destination's own row `b`
    survives under key 2 — and the group must therefore leave key 2 alone rather
    than delete it as a touched key.

    Postgres: `{(2,'b'), (5,'a')}`.  Measured before the fix: `[(2,'a'), (5,'a')]`.
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 2, "a"),
                delete("2", 3, 201, 2, "a"),
                insert("2", 4, 201, 5, "a"),
            ],
        )
    )
    assert state(box) == [(2, "b"), (5, "a")]


def test_the_surviving_destination_row_keeps_its_original_provenance(lab):
    """The row that survives under a touched key is the destination's own row.

    It is not deleted and re-inserted: nothing in the source changed it, so its
    `cdcf_commit_id` still names the group that actually wrote it. That is what
    makes "the pre-group row survives" expressible without re-reading and
    re-binding a whole destination row (whose types have been through DuckDB).
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    first = box.applier.last_commit_id
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 2, "a"),
                delete("2", 3, 201, 2, "a"),
                insert("2", 4, 201, 5, "a"),
            ],
        )
    )
    provenance = dict(box.q(f'SELECT id, cdcf_commit_id FROM "cdc_raw"."{TABLE}" ORDER BY id'))
    assert provenance[2] == first, "the untouched row must not be rewritten"
    assert provenance[5] == box.applier.last_commit_id


def test_an_unattributable_delete_fails_the_group_instead_of_folding_silently(lab):
    """Where the stream genuinely cannot say which row a delete removed, refuse.

    Two *different* in-group rows wear one key and the delete's before-image
    matches neither. That is not a shape Postgres can produce (a deferrable key
    forces `REPLICA IDENTITY FULL`, so the image is complete), so reaching it means
    the input is not what the fold is entitled to assume — and per the rubric's own
    scale a loud error beats a silently wrong fold.
    """
    from cdc_flight.errors import AmbiguousDelete

    box = lab()
    preload(box, {1: "a", 2: "b"})
    with pytest.raises(AmbiguousDelete, match="cannot attribute"):
        box.run(
            txn(
                "2",
                [
                    delete("2", 1, 200, 1, "a"),
                    insert("2", 2, 200, 3, "a"),
                    delete("2", 3, 201, 2, "b"),
                    insert("2", 4, 201, 3, "b"),
                    delete("2", 5, 202, 3, "ghost"),
                ],
            )
        )
    # Nothing was committed, so the destination still holds the pre-group state and
    # the transaction replays (Invariant O).
    assert state(box) == [(1, "a"), (2, "b")]


def test_a_key_only_delete_of_a_key_this_group_also_inserted(lab):
    """`REPLICA IDENTITY DEFAULT`: `INSERT (5,…); DELETE WHERE id = 5;` in one txn.

    The before-image carries only the key, so it cannot attribute anything — but it
    does not have to: without a deferrable constraint at most one row wears key 5 at
    a time, so whichever row this delete removed, key 5 ends the transaction empty.
    """
    box = lab()
    preload(box, {1: "a"})
    box.run(
        txn(
            "2",
            [insert("2", 1, 200, 5, "new"), delete_key_only("2", 2, 201, 5)],
        )
    )
    assert state(box) == [(1, "a")]


# --------------------------------------------------------------------------- #
# regression guards: orderings the reviews proved CORRECT and that the rewrite
# must not break (Opus verified all of these independently)
# --------------------------------------------------------------------------- #
def test_a_three_ring_rotation_is_handled(lab):
    """`RUBRIC_STATUS`'s recorded falsifier, which Opus showed already works.

    `UPDATE t SET id = (id % 3) + 1` over keys {1,2,3} with a deferred constraint:
    `d(1,a) c(2,a) d(2,b) c(3,b) d(3,c) c(1,c)`.
    """
    box = lab()
    preload(box, {1: "a", 2: "b", 3: "c"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 2, "a"),
                delete("2", 3, 201, 2, "b"),
                insert("2", 4, 201, 3, "b"),
                delete("2", 5, 202, 3, "c"),
                insert("2", 6, 202, 1, "c"),
            ],
        )
    )
    assert state(box) == [(1, "c"), (2, "a"), (3, "b")]


def test_a_four_ring_rotation_is_handled(lab):
    box = lab()
    preload(box, {1: "a", 2: "b", 3: "c", 4: "d"})
    events = []
    order = 0
    for source, target, name in ((1, 2, "a"), (2, 3, "b"), (3, 4, "c"), (4, 1, "d")):
        events.append(delete("2", order + 1, 200 + order, source, name))
        events.append(insert("2", order + 2, 200 + order, target, name))
        order += 2
    box.run(txn("2", events))
    assert state(box) == [(1, "d"), (2, "a"), (3, "b"), (4, "c")]


def test_a_swap_through_a_temporary_key(lab):
    """`1 -> 99, 2 -> 1, 99 -> 2` — the non-deferrable way to swap two keys."""
    box = lab()
    preload(box, {1: "a", 2: "b"})
    events = []
    order = 0
    for source, target, name in ((1, 99, "a"), (2, 1, "b"), (99, 2, "a")):
        events.append(delete("2", order + 1, 200 + order, source, name))
        events.append(insert("2", order + 2, 200 + order, target, name))
        order += 2
    box.run(txn("2", events))
    assert state(box) == [(1, "b"), (2, "a")]


def test_a_delete_matching_two_transiently_identical_rows_removes_both(lab):
    """Two byte-identical rows wear one key and both are deleted.

    Either attribution gives the same observable answer, and both deletes together
    must leave the key empty.
    """
    box = lab()
    preload(box, {1: "a", 2: "a"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 9, "a"),
                delete("2", 3, 201, 2, "a"),
                insert("2", 4, 201, 9, "a"),
                delete("2", 5, 202, 9, "a"),
                delete("2", 6, 203, 9, "a"),
            ],
        )
    )
    assert state(box) == []


def test_the_ambiguous_shape_is_idempotent_under_replay(lab):
    """Re-folding Opus BLOCKER-2's shape with fresh LSNs must converge.

    The fence normally drops a replayed transaction; this drives the fold itself
    twice with LSNs the fence cannot help with, because a fold that is only correct
    once is not correct.
    """
    box = lab()
    preload(box, {1: "a", 2: "b"})
    shape = [
        delete("2", 1, 200, 1, "a"),
        insert("2", 2, 200, 2, "a"),
        delete("2", 3, 201, 2, "a"),
        insert("2", 4, 201, 5, "a"),
    ]
    box.run(txn("2", shape))
    assert state(box) == [(2, "b"), (5, "a")]
    replay = [
        delete("3", 1, 400, 1, "a"),
        insert("3", 2, 400, 2, "a"),
        delete("3", 3, 401, 2, "a"),
        insert("3", 4, 401, 5, "a"),
    ]
    box.run(txn("3", replay))
    assert state(box) == [(2, "b"), (5, "a")], "a second fold must not add or lose a row"


def test_the_ambiguous_shape_under_spill(lab):
    """The same shape with every event staged in `_cdc_flight.spill_events`."""
    box = lab(unit_spill_events=1, unit_spill_bytes=1)
    preload(box, {1: "a", 2: "b"})
    box.run(
        txn(
            "2",
            [
                delete("2", 1, 200, 1, "a"),
                insert("2", 2, 200, 2, "a"),
                delete("2", 3, 201, 2, "a"),
                insert("2", 4, 201, 5, "a"),
            ],
        )
    )
    assert box.applier.spilled_events >= 1, "the test did not actually spill"
    assert state(box) == [(2, "b"), (5, "a")]


def test_two_tables_do_not_interfere(lab):
    """The same ambiguous shape on two tables inside one commit group."""
    box = lab()
    preload(box, {1: "a", 2: "b"})
    others = [
        data("9", i + 1, 50 + i, table="orders", key={"id": ident}, after={"id": ident, "note": note})
        for i, (ident, note) in enumerate({1: "a", 2: "b"}.items())
    ]
    box.run(txn("9", others, table="app.orders"))

    def moves(txn_id: str, base_lsn: int, table: str, column: str) -> list:
        def row(ident: int, value: str) -> dict:
            return {"id": ident, column: value}

        return [
            data(txn_id, 1, base_lsn, table=table, key={"id": 1}, before=row(1, "a"), op="d"),
            data(txn_id, 2, base_lsn, table=table, key={"id": 2}, after=row(2, "a"), op="c"),
            data(txn_id, 3, base_lsn + 1, table=table, key={"id": 2}, before=row(2, "a"), op="d"),
            data(txn_id, 4, base_lsn + 1, table=table, key={"id": 5}, after=row(5, "a"), op="c"),
        ]

    box.feed(txn("2", moves("2", 200, "customers", "name"), table="app.customers"))
    box.feed(txn("3", moves("3", 300, "orders", "note"), table="app.orders"))
    box.commit("two-tables")
    assert state(box) == [(2, "b"), (5, "a")]
    assert box.rows("cdcflight_app_orders", "id, note") == [(2, "b"), (5, "a")]
