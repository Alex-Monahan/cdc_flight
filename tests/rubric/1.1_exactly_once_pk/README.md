# tests/rubric/1.1_exactly_once_pk

**Rubric 1.1** — *Delivery guarantees for tables with a primary key.*
`at-most-once=1, at-least-once=3, exactly-once=5`. Baseline score: **3**.
**Status: the target tests pass** — the transactional applier landed (ADR 0001).

## The gap

Debezium's offset lives in `.cdc_state/offsets.dat`, flushed on its own schedule
(`offset.flush.interval.ms=1000`), entirely outside the destination write. The
handler calls `dlt_pipeline.run()` and pydbzengine then calls
`committer.markProcessed()` / `markBatchFinished()`
(`repos/pydbzengine/pydbzengine/_jvm.py:121-124`). Between "destination
committed" and "offset durable" there is a window; a crash inside it replays the
batch, and `write_disposition="append"` turns the replay into a permanent
duplicate.

`probes/p13_offset_replay.py` measured both halves: a real `kill -9` during a
400 000-row load left **402 048 rows / 400 000 distinct ids** — exactly one
`max.batch.size` batch duplicated, zero rows lost. Textbook at-least-once.

## Fault injection used here

The default suite crashes the process **for real**, but deterministically. `src/cdc_flight/faults.py` exposes crash points via the `CDC_FAULT_INJECT`
environment variable; it is inert unless that variable is set. The points are
named after the **transactional protocol** — `pre_commit`,
`post_commit_pre_ack`, `post_ack` — so they survive the ADR 0001 refactor, and
`<nth>` counts **data** batches only (a metadata-only first batch would
otherwise silently move every fault point by one). The suite uses
`post_commit_pre_ack:1` — the destination transaction has committed and the process dies
*before* `markProcessed()` / `markBatchFinished()` run, so Debezium's offset file
still points before the batch. That is exactly the window a `kill -9` hits,
without racing it.

Two things were measured while building this, and both are why the naive
approaches were rejected:

* rolling the **offset file** back instead does not reliably replay: Postgres
  will not stream from before the slot's `restart_lsn`, which has already
  advanced, so only the tail of the batch comes back. Observed: a 50-customer +
  60-reading pair of transactions replayed the readings and *not* the customers.
* racing a real `SIGKILL` needs a huge workload to win — `probes/p07` lost the
  race at 60 k rows; `probes/p13` only won it at 400 k.

The scenario is the session-scoped `crash_replay` fixture in `tests/support/fixtures.py`,
shared with `tests/rubric/1.2_exactly_once_nopk/`.

`test_slow_real_sigkill_loses_nothing` (marked `slow`, deselected by `make test`)
does the real thing — a 200 000-row transaction, `kill -9` mid-load, restart —
but asserts **no loss**, not duplication. Whether a SIGKILL duplicates depends on
where it lands relative to the offset flush, and that race is not winnable
reliably — two consecutive runs of *this same test* at 200 000 rows gave:

```
run 1: SIGKILL after ~200 000 rows: 200 000 rows / 200 000 distinct =>     0 duplicates
run 2: SIGKILL after  151 552 rows: 205 706 rows / 200 000 distinct => 5 706 duplicates
```

(`probes/p07` lost the race at 60 k, `probes/p13` won it at 400 k.) Requiring
duplication here would make the suite flaky for no gain; the duplication claim
rests on the deterministic scenario instead. The observed duplicate count is
printed either way, and **no loss** is asserted in both cases.

## What the tests assert

| test | status |
|---|---|
| ~~`test_gap_replay_duplicates_pk_rows`~~ | DELETED — it asserted duplication, which no longer happens |
| ~~`test_gap_some_ids_are_delivered_twice`~~ | DELETED — same reason |
| ~~`test_gap_duplicates_span_a_contiguous_prefix`~~ | DELETED — same reason |
| `test_scenario_crashed_after_commit_and_recovered` | passes — the guard, rewritten (see below) |
| `test_no_rows_are_lost` | passes |
| `test_target_change_event_ledger_balances` | **passes** (marker removed) |
| `test_target_exactly_once_pk` | **passes** (marker removed) |
| `test_target_no_duplicate_change_events` | **passes** (marker removed) |
| `test_target_slot_never_outruns_the_destination` | **passes** (marker removed) |

### The guard had to change, and that is the result

`test_scenario_actually_replayed` used to assert `replayed["records"] > 0` —
"the crash caused a replay, so the duplication assertions are not vacuous".
Under Invariant O there is nothing to replay: the resume point went into the same
destination transaction as the rows, so start-up reconciliation rebuilds
`offsets.dat` from it and the connector resumes *after* the committed batch.
"No replay happened" is the improvement, so it cannot also be the guard. The
guard is now: the fault really fired (exit 137), the restart really succeeded,
and the committed rows really are there.

## Two more test modules landed with the applier

* `test_1_1_fault_matrix.py` — crashes at **all five** commit-group anchors
  (`begin`, `mid_apply`, `pre_commit`, `post_commit_pre_ack`, `post_ack`) and
  asserts no loss, no duplicates and the Invariant-O guard at each. The keyless
  table is the decisive half: a merge on a primary key can hide a double
  delivery, an append keyed on event identity cannot.
* `test_1_1_reconciliation.py` — ADR §4.5's decision table, including the row
  that must **refuse to start** (offsets file present, destination row missing),
  and a run with `CDC_OFFSET_FILE_REPAIR=0` proving that correctness does not
  depend on the file repair at all.

### Why row counts are not enough (review finding)

For a primary-keyed table, `count(*) == 50 AND count(DISTINCT id) == 50` is
satisfied by **any** implementation that merges on `id`, even one that delivered
and applied the same change event twice. Row-shape assertions therefore cannot
distinguish exactly-once *delivery* from idempotent *application*.
`test_target_change_event_ledger_balances` compares the number of change events
in the destination against the number the source produced; an append-only
changelog cannot fake that.

`test_target_slot_never_outruns_the_destination` asserts ADR 0001 §4.7's
Invariant-O guard, `slot.confirmed_flush_lsn <= debezium_offsets.last_lsn`. It is
the only detector for the class of bug that produced ADR revision 2 (a Debezium
lifecycle path confirming an LSN MotherDuck never committed), so it lands with
the applier rather than after it.

Conventions (gap pin vs `test_target_*` xfail-strict vs `slow`) are described in
`tests/rubric/1.0_engine_error_propagation/README.md`.
