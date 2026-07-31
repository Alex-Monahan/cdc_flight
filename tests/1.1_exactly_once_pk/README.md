# tests/1.1_exactly_once_pk

**Rubric 1.1** — *Delivery guarantees for tables with a primary key.*
`at-most-once=1, at-least-once=3, exactly-once=5`. Baseline score: **3**.

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

The default suite crashes the process **for real**, but deterministically. A new
module, `src/cdc_flight/faults.py`, exposes two crash points via the
`CDC_FAULT_INJECT` environment variable; it is inert unless that variable is set
and the only thing it can do is `os._exit` the process. The suite uses
`after_load:1` — the destination transaction has committed and the process dies
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

The scenario is the session-scoped `crash_replay` fixture in `tests/conftest.py`,
shared with `tests/1.2_exactly_once_nopk/`.

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

| test | today | after the applier lands |
|---|---|---|
| `test_gap_replay_duplicates_pk_rows` | **passes** — the 50 replayed customers exist twice | starts failing; delete it |
| `test_no_rows_are_lost` | passes | must keep passing |
| `test_target_exactly_once_pk` | **xfail(strict)** | must pass, then drop the marker |

Conventions (gap pin vs `test_target_*` xfail-strict vs `slow`) are described in
`tests/1.0_engine_error_propagation/README.md`.
