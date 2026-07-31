# tests/1.5_truncate_drop — "truncate table and drop table propagate"

Rubric 1.5: `silently ignored=1, logged=2, tombstones/soft delete=3, replicated
just like Postgres handles them=5`.

## TRUNCATE

pgoutput carries it and Postgres makes it transactional, so the only thing standing
between the baseline's 1 and a faithful replication was **Debezium's own default**:
`skipped.operations` defaults to `"t"`, and the pgoutput decoder then drops the `'T'`
message before it is ever decoded (`PgOutputMessageDecoder.isTruncateEventsIncluded`).
That is why the baseline's `skipped` counter did not even increment.
`CDC_TRUNCATE_MODE=ignore` restores that default on demand, and
`test_1_5_drop_recreate.py::test_ignore_mode_reproduces_the_baseline_gap` uses it to
reproduce the gap against a live cluster.

Once the events arrive, three things matter:

* a truncate is a **counted** event (Debezium sends it through the same
  `changeRecord` path, so it is in `END.event_count`, it occupies a
  `transaction.total_order` ordinal and it gets a `data_collections` entry). Feeding
  it as anything else makes every truncating transaction fail the completeness rule;
* it carries **no message key**, which must not be read as "this table is keyless";
* the fold must drop what the group planned *before* it and keep what came *after*
  it — `TRUNCATE t; INSERT …` in one transaction leaves the inserted rows, and
  Postgres also removes rows the same transaction inserted before the truncate.

`TRUNCATE a, b CASCADE` is one transaction, so it is one commit group and one
`COMMIT`: no observer sees the parent empty while the child is still full.

## DROP TABLE

Not in the replication stream at all — pgoutput has no DDL and the Postgres connector
has no DDL event source. `cdc_flight.catalog.CatalogWatcher` polls the source catalog
(default every 10 s) for the two facts logical decoding cannot give us: the relation
`oid` and publication membership. Four outcomes: `dropped`, `recreated` (same name,
new oid), `unpublished` (never destructive — Postgres still holds the rows) and `new`
(rubric 2.3's hook).

**The fence.** A drop is discovered after the fact, so applying it immediately could
delete rows that an in-flight event would then re-add — leaving a zombie table. Each
change carries the `pg_current_wal_lsn()` of its poll, and the applier holds it until
its durable resume point reaches that LSN. On a quiet source nothing would ever
advance it, so the watcher emits a **transactional** logical-decoding message past
the change.

That the marker is transactional is a measured decision. A non-transactional one is
the obvious choice (it stays out of every `END.event_count`), but
`WalPositionLocator.resumeFromLsn` only stops searching for the restart position on a
**COMMIT** past the stored LSN, and while it searches `skipMessage()` drops every
record — including the marker. MEASURED 2026-07-31: with a non-transactional marker a
quiet run delivered `records=0`, sat 770 KB behind the source and never applied the
drop; with a transactional one it applies in about a second.

## Files

| file | suite | what it proves |
|---|---|---|
| `test_1_5_truncate_fold.py` | default | every truncate fold shape and the application of a detected drop, through the shipped `Applier` and a real DuckDB file: multi-table atomicity, rows before/after the truncate, the keyless trap, the marker, `truncate_mode=log`, a spilled truncate, the LSN fence, `recreated`, `unpublished`, `drop_mode=log`, and a rolled-back drop staying pending |
| `test_1_5_catalog_detection.py` | default | the comparison and the fence in isolation, including the restart case (a replicated table with no persisted oid that is gone **is** a drop) and that a marker failure leaves the change unapplied rather than forced |
| `test_1_5_truncate_drop_e2e.py` | default (`e2e`) | one 33 s scenario: real `TRUNCATE parent CASCADE` + inserts in one transaction, a real `DROP TABLE`, the audit trail, and that the fence marker does not break the assembler |
| `test_1_5_motherduck.py` | `motherduck` | the truncate and the drop against real MotherDuck, including that `DELETE FROM` reports its row count there (the marker would otherwise say "unknown") |
| `test_1_5_drop_recreate.py` | `slow` | the live gap under `CDC_TRUNCATE_MODE=ignore`, a `SIGKILL`-equivalent in the commit→ack window of a truncating group, and drop-then-recreate with a different schema |

## Policy switches

| variable | default | meaning |
|---|---|---|
| `CDC_TRUNCATE_MODE` | `replicate` | `replicate` empties the destination table (=5); `log` keeps the rows and records the marker (=3); `ignore` restores Debezium's skip |
| `CDC_DROP_MODE` | `replicate` | `replicate` drops the destination table (=5); `log` records the marker only; `ignore` disables catalog polling |
| `CDC_CATALOG_POLL_SECONDS` | `10` | catalog poll interval (also rubric 2.3's discovery interval) |
| `CDC_CATALOG_MARKER` | `1` | emit the WAL fence marker on the source |
| `CDC_CATALOG_GRACE` | `0` | never apply a DDL action the fence has not cleared |
