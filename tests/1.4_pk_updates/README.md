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

## What was not free

Two shapes were wrong when 1.4 was picked up, both measured:

* `UPDATE t SET id = id + 1` over two rows (only legal with a `DEFERRABLE` primary
  key) emits `d(1) c(2) d(2) c(3)`. Collapsing by key made the `d(2)` delete the row
  the `c(2)` had just created: Postgres held `{2, 3}`, the destination held `{3}` —
  a **lost row**, reproduced end to end in `test_gap_a_deferred_key_permutation_*`.
* a key-changing `u` (the defensive non-Postgres path) after an insert of the old
  key in the same group left the row under **both** keys — the rubric's
  `duplication=2` exactly.

Both are the same question: does the key this event removes belong to a row that
existed *before* this commit group, or to a row the group itself inserted? The
event stream cannot answer it (`d(1) c(2) d(2) c(3)` is byte-identical for the
permutation, whose answer is `{2,3}`, and for a chained `1->2->3`, whose answer is
`{3}`) — but the destination can, so the fold asks it, once per ambiguous key.

## Two Postgres facts worth knowing

* A `DEFERRABLE` primary key is **not** a replica identity. `UPDATE` on such a
  published table fails with *"cannot update table … because it does not have a
  replica identity and publishes updates"*, so the deferred-permutation collision
  is only reachable with `REPLICA IDENTITY FULL` (or another non-deferrable unique
  index). The message key still comes from the primary key.
* `app.orders` references `app.customers (id)` with `ON DELETE CASCADE` and no
  `ON UPDATE`, so Postgres refuses a key update on a customer that has orders. The
  e2e scenario moves customer 3, which has none.

## Files

| file | suite | what it proves |
|---|---|---|
| `test_1_4_pk_update_fold.py` | default | every fold shape, through the shipped `Applier` and a real DuckDB file: the plain move, mixed with other changes to the same row, the freed-key collision, the chained move, the deferred permutation, composite keys, a spilled unit, two units in one group, and a fault at `begin` / `mid_apply` / `pre_commit` around the move |
| `test_1_4_pk_update_e2e.py` | default (`e2e`) | the same properties through real Postgres + Debezium in one 18 s scenario, plus "no error", the array-column table shape, and row-for-row agreement with the source |
| `test_1_4_pk_update_crash.py` | `slow` | a real `SIGKILL`-equivalent in the commit→ack window of the group that carries a PK update, then recovery |
