# tests/1.4_pk_updates — "if a primary key is updated, correctly handle it"

Rubric 1.4: `error=1, duplication=2, primary key row correctly deleted and
inserted or updated=5`.

## What Postgres and Debezium actually emit

A key-changing `UPDATE` never reaches us as an `u`. `RelationalChangeRecordEmitter`
splits it into `d(old key)` + `c(new key)` whenever the old key is available
(`emitUpdateAsPrimaryKeyChangeRecord`), and pgoutput sends the old key whenever the
key changes — under `REPLICA IDENTITY DEFAULT` as well as `FULL`. Both events carry
the same `txId` and the same event LSN.

That is why the *atomicity* half of 1.4 is free: the pair is inside one
`CompleteUnit`, a commit group only ever holds whole units, and the destination
merge deletes every key the group touched before inserting the group's final rows.
`test_the_delete_and_the_insert_cannot_be_split_across_commit_groups` drives the
applier with a commit trigger on **every event** and shows it still cannot split
them.

## What was not free: **a key is not a row**

The first attempt indexed the plan by key and asked the destination one question —
*did this key exist before this commit group?* Two independent reviews then reproduced
five orderings where that is the wrong question, three of them losing a row:

| ordering | Postgres | the key-indexed fold |
|---|---|---|
| T1 inserts key 2; T2 permutes `{1,2} -> {2,3}`; one commit group | `{2:a, 3:b}` | `{3:b}` — lost row |
| one txn `d(1,a) c(3,a) d(2,b) c(3,b) d(3,a)` (two rows on key 3) | `{3:b}` | `{}` — lost row |
| one txn `d(1,a) c(2,a) d(2,a) c(5,a)` (the destination's row `b` on key 2) | `{2:b, 5:a}` | `{2:a, 5:a}` — lost `b`, duplicated `a` |
| one txn `TRUNCATE; INSERT 5; DELETE 5` | `{}` | `{5}` — spurious row |
| T1 `TRUNCATE; INSERT 1`; T2 `DELETE 1`; one group | `{}` | `{1}` — zombie row |

A key can be worn by several rows at once inside a transaction (a **deferred** unique
constraint), and freed and re-taken across the transactions of one commit group. So no
question about a *key* — at any scope — decides what a delete removed. What decides it
is which physical **row** the delete's before-image describes.

`table_work` therefore holds `live[key] = [entry, …]`, where an entry is a row or
`START` (the row the destination already held), and every event is one physical
operation on that list: `c`/`r` append, `u` replaces the entry its before-image
identifies, `d` removes it, `t` discards every entry **including `START`**. At group
end each key holds at most one row, and `[START]` alone means *leave the destination's
row completely alone* — the case the key-indexed plan could not express at all.

Where two entries compete and the before-image cannot choose, the group is **refused**
(`AmbiguousDelete`) rather than folded on a guess: the rubric's own scale puts an error
above silent loss, and a rolled-back group replays for free. See ADR 0001 §18/A35–A37.

## Two Postgres facts worth knowing

* A `DEFERRABLE` primary key is **not** a replica identity. `UPDATE` on such a
  published table fails with *"cannot update table … because it does not have a
  replica identity and publishes updates"*, so the deferred-permutation collision
  is only reachable with `REPLICA IDENTITY FULL` (or another non-deferrable unique
  index). The message key still comes from the primary key. **This is load-bearing for
  the fold**: it is exactly why the disambiguating full before-image is always present
  in the only configuration where the ambiguity is reachable.
* `app.orders` references `app.customers (id)` with `ON DELETE CASCADE` and no
  `ON UPDATE`, so Postgres refuses a key update on a customer that has orders. The
  e2e scenario moves customer 3, which has none.

## Files

| file | suite | what it proves |
|---|---|---|
| `test_1_4_fold_counterexamples.py` | default | the orderings both reviews **reproduced** against the shipped applier (the table above), each asserting equality with Postgres rather than mere uniqueness — plus the ones they verified as *correct* and which the rewrite must not break: 3-ring and 4-ring rotations, a swap through a temporary key, a delete matching two transiently identical rows, the ambiguous shape under spill, over two tables, and re-folded with fresh LSNs so the idempotency fence cannot help |
| `test_1_4_pk_update_fold.py` | default | every fold shape, through the shipped `Applier` and a real DuckDB file: the plain move, mixed with other changes to the same row, the freed-key collision, the chained move, the deferred permutation, composite keys, a spilled unit, two units in one group, and a fault at `begin` / `mid_apply` / `pre_commit` around the move |
| `test_1_4_pk_update_e2e.py` | default (`e2e`) | the same properties through real Postgres + Debezium in one 18 s scenario, plus "no error", the array-column table shape, and row-for-row agreement with the source |
| `test_1_4_pk_update_crash.py` | `slow` | a real `SIGKILL`-equivalent in the commit→ack window of the group that carries a PK update, then recovery |
