# tests/1.5_truncate_drop — "truncate table and drop table propagate"

Rubric 1.5: `silently ignored=1, logged=2, tombstones/soft delete=3, replicated
just like Postgres handles them=5`.

## TRUNCATE

pgoutput carries it and Postgres makes it transactional. The pipeline pins
`skipped.operations` to `"none"` so the ordered `'T'` event reaches the generation
fence; `CDC_TRUNCATE_MODE=ignore` remains a destination no-op, preserving the baseline
rows/marker behavior while retaining the raw event internally. The
`test_1_5_drop_recreate.py::test_ignore_mode_reproduces_the_baseline_gap` test verifies
that external gap against a live cluster.

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
| `test_1_5_truncate_key_reuse.py` | default | rubric 1.5 crossed with 1.4, which nothing tested before and where the fold was wrong: both reproduced Opus BLOCKER-1 shapes (a spurious row that never healed; one source row present under two keys with an **ordinary** primary key), Codex 3's cross-transaction zombie, and the same cases under spill and over two tables |
| `test_1_5_truncate_storage_modes.py` | default | a `{memory, spill} x {replicate, log}` matrix over rows / marker / counters / `rows_removed`, because storage used to change *semantics*: `truncate_mode=log` under spill emptied the table and neither mode wrote a marker. Plus two truncates in one transaction reporting what each removed, and a fault at `spill` / `mid_apply` / `pre_commit` around a **staged** truncate |
| `test_1_5_catalog_guards.py` | default | the guards between "the table is gone" and `DROP TABLE`: confirmation, supersession, durable observation/quarantine, the mass-drop circuit breaker, the zero-relations guard, and that an alert about a *refusal* survives a rollback while an alert about an *applied* drop does not |
| `test_1_5_ownership_and_honesty.py` | default | `table_state` as the canonical source-to-destination registry (written by whoever first creates the table, kept by `--reset-state`), the synchronous final catalog poll, and that a run with an unresolved destructive change **fails** instead of reporting `ok: true` |
| `test_1_5_truncate_fold.py` | default | every truncate fold shape and the application of a detected drop, through the shipped `Applier` and a real DuckDB file: multi-table atomicity, rows before/after the truncate, the keyless trap, the marker, `truncate_mode=log`, a spilled truncate, the LSN fence, `recreated`, `unpublished`, `drop_mode=log` with durable identity and baseline confirmation across restart, the declared drop-mode/baseline-state matrix, and a rolled-back drop staying pending |
| `test_1_5_catalog_detection.py` | default | the comparison and the fence in isolation, including the restart case (a replicated table with no persisted oid that is gone **is** a drop) and that a marker failure leaves the change unapplied rather than forced |
| `test_1_5_truncate_drop_e2e.py` | default (`e2e`) | one 33 s scenario: real `TRUNCATE parent CASCADE` + inserts in one transaction, a real `DROP TABLE`, the audit trail, and that the fence marker does not break the assembler |
| `test_1_5_motherduck.py` | `motherduck` | the truncate and the drop against real MotherDuck: that `DELETE FROM` reports its row count there (the marker would otherwise say "unknown"), that the alert sink really is an independent connection on a server-side transaction implementation, and a second scenario injecting `pre_commit` on a truncating group (every row kept, no marker, then a replay landing it exactly once) |
| `test_1_5_drop_recreate.py` | `slow` | the live gap under `CDC_TRUNCATE_MODE=ignore`, a `SIGKILL`-equivalent in the commit→ack window of a truncating group, and drop-then-recreate with a different schema |

## Policy switches

| variable | default | meaning |
|---|---|---|
| `CDC_TRUNCATE_MODE` | `replicate` | `replicate` empties the destination table (=5); `log` keeps the rows and records the marker (=3); `ignore` is a destination no-op while the raw event remains decoded |
| `CDC_DROP_MODE` | `replicate` | `replicate` drops the destination table (=5); `log` records the marker only; `ignore` disables catalog polling |
| `CDC_CATALOG_POLL_SECONDS` | `10` | catalog poll interval (also rubric 2.3's discovery interval) |
| `CDC_CATALOG_MARKER` | `1` | emit the WAL fence marker on the source |
| `CDC_DROP_CONFIRM_POLLS` | `2` | consecutive polls that must agree before a destructive change is queued |
| `CDC_DROP_MAX_PER_POLL` | `1` | how many relations one commit group may destroy; the **whole set** is refused above it |
| `CDC_DROP_ALLOW_MASS` | `0` | authorise a mass drop (an operator who really is replicating a `DROP SCHEMA`) |
| `CDC_CATALOG_MARKER_MAX` | `60` | cap on fence markers written to the source per run while a change stays unresolved |
| `CDC_CATALOG_DRAIN_SECONDS` | `30` | how long a quiet run holds the engine open for a change its final poll just queued |
| `CDC_CATALOG_GRACE` | `0` | never apply a DDL action the fence has not cleared. **A non-zero value is excluded from the structural correctness guarantee** (ADR §18/A38) |
