# Rubric test index

This is the canonical navigation index for the rubric suites. The six historical
suite-local README bodies that existed before the file-structure refactor are
retained verbatim below under stable anchors; their evidence, measurements, and
links are preserved while paths point at this index.

## Suites

| suite | description |
|---|---|
| [`1.0_engine_error_propagation/`](#rubric-1-0-engine-error-propagation) | engine failures must not exit 0 — historical README below |
| [`1.1_exactly_once_pk/`](#rubric-1-1-exactly-once-pk) | exactly-once delivery for tables with a primary key — historical README below |
| [`1.2_exactly_once_nopk/`](#rubric-1-2-exactly-once-nopk) | exactly-once delivery for tables without a primary key — historical README below |
| [`1.3_atomic_batches/`](#rubric-1-3-atomic-batches) | multi-table transactional atomicity — historical README below |
| [`1.4_pk_updates/`](#rubric-1-4-pk-updates) | primary-key updates — historical README below |
| [`1.5_truncate_drop/`](#rubric-1-5-truncate-drop) | TRUNCATE and DROP TABLE — historical README below |
| `1.6_snapshot_consistency/` | snapshot consistency and recovery |
| `1.7_fault_injection/` | fault injection and recovery anchors |
| `1.8_slot_mismatch/` | slot mismatch recovery |
| `1.9_state_machines/` | durable state machines |
| `2.1_added_dropped_columns/` | added and dropped columns |
| `2.2_renamed_columns/` | renamed columns |
| `2.3_new_table_discovery/` | new-table discovery |
| `4.7_self_healing/` | self-healing inventory |

All suites use the fixtures and drivers in `tests/support/`; pytest keeps
`tests/conftest.py` as the common fixture boundary. The default, slow, and
MotherDuck lanes remain the Makefile selectors documented in the repository
README.

## Historical suite evidence

<a id="rubric-1-0-engine-error-propagation"></a>

# tests/rubric/1.0_engine_error_propagation

**TODO item 1.0(b).** Not a rubric item on its own; it *gates the measurement* of
rubric 1.8, 4.1, 4.2, 4.3 and 6.2, because every one of those failure modes
currently exits 0.

## The defect

`DebeziumJsonEngine.run()` (pydbzengine) builds the engine with
`DebeziumEngine.create(...).using(props).notifying(consumer).build()` — it never
registers a `CompletionCallback`. Debezium 3.6's `AsyncEmbeddedEngine` reports a
startup or streaming failure by calling
`completionCallback.handle(false, message, error)`
(`repos/debezium/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java:796-804`)
and then returning normally from `run()`. Nothing is thrown on the caller's
thread, so `run_engine_bounded`'s `error_box` stays empty, the loop falls out of
`while thread.is_alive()` into the `else:` branch (`stop_reason:
"engine_finished"`), and the process prints a success summary and exits 0.

Probe `probes/p11_dropped_slot_logs.py` proved it: with the replication slot
dropped, the engine logs

```
WARN  BaseSourceTask - Last recorded offset is no longer available on the server.
ERROR AsyncEmbeddedEngine - 1 task(s) out of 1 failed to start.
```

while the pipeline reports `{"records": 0, "stop_reason": "engine_finished",
"returncode": 0}`.

## What these tests assert

| test | asserts |
|---|---|
| `test_healthy_run_reports_success` | a healthy run still exits 0 with a summary — guards against an over-eager failure detector. Asserts a *scenario-specific* row count, not the seed's global total, so an unrelated seed change or another writer cannot break it. |
| `test_engine_death_surfaces_as_a_failure` | the connector cannot start (the **publication** is dropped) ⇒ non-zero exit and `ok: false` |
| `test_failure_carries_the_debezium_error_message` | the run summary and stderr carry Debezium's own *distinctive* text for a missing publication, and `stop_reason: "engine_error"` |
| `test_failed_run_does_not_claim_records` | `records: 0` alongside `ok: false`, the shape that made p04/p11 invisible |
| `test_a_dropped_slot_is_now_recovered_rather_than_fatal` | a dropped slot no longer reaches the engine at all — rubric 1.8 detects and repairs it before start-up |
| `test_corrupt_offset_is_repaired_rather_than_fatal` (`slow`) | a corrupted offset file is rebuilt from the destination rather than being fatal |

**The subject changed when rubric 1.8 landed, and the README did not** (Codex m2). This
file used to drop the **replication slot** to kill the connector; 1.8's slot check now
detects a missing slot before start-up and repairs it with an automatic re-snapshot, so
keeping that trigger would have turned a supervisor test into a recovery test. Dropping
the **publication** breaks connector start-up the same way and is not something 1.8 can
or should repair. The old coverage is not lost: the slot case has its own test above, and
1.8's own suite proves the repair end to end.

The sentinel matters as much as the subject. It was briefly the bare word `"publication"`,
matched case-insensitively against the whole output, which our own log lines satisfy
(Opus MINOR-6). It is now Debezium's distinctive phrase, so a run that fails for an
unrelated start-up reason cannot pass this test.

These are **target-behaviour** tests for a defect that is fixed in the same
commit range, so they are expected to *pass*. They are not xfail-marked.

## 1.0(feedback) — the two failure paths 1.0(b) did **not** cover

`test_1_0_supervisor_liveness.py` (`slow`) and `test_1_0_supervisor_unit.py`
(fast, no JVM, no Postgres) were added after the dual review of TODO 1.0.

`1.0(b)` fixed *startup* failures. Two other ways to exit 0 on a broken run
survived it:

| test | defect |
|---|---|
| `test_walsender_kill_never_reports_ok_on_partial_delivery` (`slow`) | a **streaming** failure. Killing the walsender puts Debezium into a 10 s retriable-restart backoff; the 8 s idle timer fires first and the run reports `ok: true` with `57 344 / 60 000` rows and `EXIT=0` (measured on the pre-fix code). Idle is now corroborated against `pg_replication_slots`: the slot must have been continuously held for the whole quiet window. |
| `test_engine_that_returns_on_its_own_is_a_failure_in_streaming_mode` | the engine thread returning on its own (`stop_reason: engine_finished`) used to be `ok: true`, even when Debezium had swallowed a `StopEngineException`. Only a snapshot mode that is designed to terminate may end that way. |
| `test_noise_filter_*` | the shutdown-noise filter was armed on **every** close (`close()` runs in a `finally`), so it degenerated into "discard any failure whose text contains `interrupted`" — which is exactly how a handler error propagates. It is now armed only by an *intentional* close, and a suppressed message still reaches the run summary. |
| `test_verifier_*` | `markBatchFinished()` can return normally without flushing, because Debezium discards `commitOffsets()`'s boolean. `OffsetFlushVerifier` makes that fatal. |
| `test_fault_spec_*` / `test_malformed_fault_spec_is_rejected` | a malformed `CDC_FAULT_INJECT` used to leave the process running normally, so a fault test could pass vacuously. It is parsed and validated once at start-up. |

## Conventions used by all `tests/rubric/<rubric item>_*/` suites

1. **Gap pins** — plain tests whose assertions encode *today's broken*
   behaviour, named `test_gap_*`. They pass now. When the fix lands they start
   failing, which is the signal to delete them and flip the matching target
   test.
2. **Target behaviour** — tests named `test_target_*`, marked
   `@pytest.mark.xfail(reason=..., strict=True)`. `strict=True` means an
   unexpected pass is a *failure*, so the moment the applier implements the
   behaviour, CI forces the marker (and the paired gap pin) to be removed.
   Nothing silently drifts.
3. **Cost** — the scenario (several 20-30 s pipeline runs) is built once per
   module in a module-scoped fixture; individual tests only query it.
4. **`slow`** — real `kill -9` / long fault-injection runs carry
   `@pytest.mark.slow` and are deselected by `make test`. Run them with
   `make test-slow`. Every slow test has a fast deterministic counterpart in the
   default suite so regressions are still caught.


<a id="rubric-1-1-exactly-once-pk"></a>

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
`tests/rubric/README.md#rubric-1-0-engine-error-propagation`.


<a id="rubric-1-2-exactly-once-nopk"></a>

# tests/rubric/1.2_exactly_once_nopk

**Rubric 1.2** — *Delivery guarantees for tables WITHOUT a primary key.*
`at-most-once=1, at-least-once=3, exactly-once=5`. Baseline score: **3**.
**Status: the target tests pass** — the transactional applier landed (ADR 0001).

Table under test: `app.sensor_readings` (no PK, `REPLICA IDENTITY FULL`).

## Why this is a separate item from 1.1

The delivery machinery is identical, so the same offset-flush-window crash
duplicates rows here too. What is *different* is that there is **no key to
deduplicate on afterwards**: a replayed batch of sensor readings is
indistinguishable from six genuinely identical readings. Any fix that leans on
`INSERT … ON CONFLICT` or a merge key therefore does not apply, and a downstream
`SELECT DISTINCT` cannot rescue the data either — it would also collapse
legitimately identical readings, which is *data loss*.
`test_gap_dedup_would_destroy_real_data` pinned both halves of that at once; it
was deleted when the applier landed, because it asserted the *broken* behaviour.

The applier must give keyless tables a **stable synthetic identity**. Review
correction: it cannot be built from `source.sequence` — that field is
`[lastCommitLsn, currentLsn]`, not an event ordinal
(`repos/debezium/.../postgresql/SourceInfo.java:180-196`), and several events can
share one LSN. The ordinal is `transaction.total_order` from the envelope's
transaction block, available once `provide.transaction.metadata=true`
(`AbstractTransactionStructMaker.java:41-43`). See
`docs/adr/0001-transactional-applier.md` §6.

## The decisive case

The scenario inserts **two byte-identical rows on purpose** — same `sensor_id`,
`reading_at`, `value` and `unit`. Every other assertion in this directory is
satisfiable by a `SELECT DISTINCT`; this one is not:

* both real rows must survive (`test_identical_source_rows_are_never_lost`, which
  is **not** xfail — it must hold of every implementation, including today's);
* and after a crash+replay there must be exactly two of them
  (`test_target_identical_source_rows_both_survive`).

An implementation that deduplicates by row content fails the first; today's
at-least-once pipeline fails the second. Only exactly-once delivery passes both.

## Fault injection used here

Shared with 1.1 via the session-scoped `crash_replay` fixture in
`tests/support/fixtures.py` (see `tests/rubric/README.md#rubric-1-1-exactly-once-pk` for what it does
and why the deterministic offset rollback is equivalent to the `kill -9` that
`probes/p13_offset_replay.py` recorded).

## What the tests assert

| test | status |
|---|---|
| ~~`test_gap_replay_duplicates_keyless_rows`~~ | DELETED — it asserted duplication, which no longer happens |
| ~~`test_gap_dedup_would_destroy_real_data`~~ | DELETED — same reason |
| `test_no_readings_are_lost` | passes |
| `test_identical_source_rows_are_never_lost` | passes — the anti-dedup guard |
| `test_target_identical_source_rows_both_survive` | **passes** (marker removed) |
| `test_target_change_event_ledger_balances` | **passes** (marker removed) |
| `test_target_exactly_once_nopk` | **passes** (marker removed) |
| `test_target_synthetic_key_is_present_and_unique` | **passes** (marker removed) |
| `test_target_event_identity_is_derived_not_random` | **passes** (marker removed) |

## How it is met

Keyless tables are keyed on `cdcf_event_id` =
`"<event lsn>:<source.txId>:<transaction.total_order>"` and applied as an
append-only changelog, so:

* two genuinely identical source rows are two different *events* and keep two
  different ids — deduplication by content is impossible by construction;
* a replayed event recomputes the *same* id, and the applier's unit-level fence
  drops the whole replayed unit before it is ever applied.

ADR 0001 §15/A1 records the correction that made this work: Debezium 3.6's
envelope `transaction.id` is `"<txId>:<lsn>"` and changes per event, so the
stable transaction identifier is `source.txId`. `transaction.total_order` is a
genuine 1-based ordinal and is used as specified.

Conventions are described in `tests/rubric/README.md#rubric-1-0-engine-error-propagation`.


<a id="rubric-1-3-atomic-batches"></a>

# tests/rubric/1.3_atomic_batches

**Rubric 1.3** — *CDC changes should be atomic in MotherDuck.*
`no Postgres transactional boundaries respected=1, single-table transactional
batches respected=3, multi-table transactional batches=5`. Baseline score: **1**.
**Status: the local target tests pass**; the MotherDuck visibility proof is in
`test_1_3_motherduck_atomicity.py` (marker `motherduck`).

## The gap

Postgres transaction boundaries are never consulted. A Debezium batch is a fixed
window of up to `max.batch.size=2048` records
(`src/cdc_flight/debezium_props.py`), each batch becomes one `dlt_pipeline.run()`
(`src/cdc_flight/handler.py`), and inside a load package dlt opens **one
transaction per table**
(`repos/dlt/dlt/destinations/insert_job_client.py:21-29`). So:

1. a Postgres transaction larger than 2 048 events is **split across several
   destination commits** — a reader can see half of it;
2. two tables written by the *same* Postgres transaction are committed
   **separately** — a reader can see the parent without the child.

`probes/p06` recorded 25 batches for one 50 000-row `INSERT`; `probes/p13`
recorded 174 batches for one 400 000-row transaction; `probes/p12` recorded one
MotherDuck load package per batch per table.

## Scenario

One Postgres `BEGIN … COMMIT` inserting 1 500 `app.customers` **and** 1 500
`app.orders` (3 000 change events, one `dbz_tx_id`). 3 000 > 2 048, so Debezium
must cut it into at least two batches, and the cut necessarily falls inside the
transaction and between the two tables.

## The `_dlt_load_id` gap tests were deleted

`test_gap_pg_transaction_is_split_across_commits` and
`test_gap_torn_transaction_is_observable` read `_dlt_load_id`. ADR 0001 D10
removed dlt from the apply path, so that column no longer exists and both tests
would have failed with a DuckDB `CatalogException` rather than a clean assertion
failure. They were deleted, as this README said they should be.

## Observing atomicity without a concurrent reader

DuckDB is single-writer: while the pipeline holds the write lock on the file, a
second process cannot open it even read-only, so a polling observer is not
possible here. Instead the tests reconstruct the sequence of destination commits
**after the fact** from `_dlt_load_id` (dlt writes one load package per
`dlt.run()`, and load ids sort chronologically). If the events of one Postgres
transaction span more than one load id, then there was a point in time at which
the earlier package was committed and the later one was not — i.e. a torn
transaction was visible. `test_gap_torn_transaction_is_observable` computes that
intermediate state explicitly.

## What the tests assert

| test | status |
|---|---|
| `test_scenario_is_one_postgres_transaction` | passes |
| ~~`test_gap_pg_transaction_is_split_across_commits`~~ | DELETED (keyed off `_dlt_load_id`) |
| ~~`test_gap_torn_transaction_is_observable`~~ | DELETED (same) |
| `test_target_pg_transaction_lands_in_one_commit` | **passes** (marker removed) |
| `test_target_commit_group_metadata_is_present` | **passes** (marker removed) |
| `test_target_commit_log_accounts_for_every_row` | **passes** (marker removed) |

### The MotherDuck variant is where the real proof lives

`test_1_3_motherduck_atomicity.py` (marker `motherduck`, deselected by
`make test`, run with `make test-md`) is the answer to the review's central
objection: *metadata equality is not proof*. An implementation could stamp the
same `cdcf_commit_id` in two separate commits and pass every assertion in this
file. Only a **visibility** assertion is proof, and it needs a concurrent reader,
which DuckDB's single-writer file lock makes impossible locally but MotherDuck
makes trivial.

That module streams one multi-table Postgres transaction into MotherDuck while a
second MotherDuck connection samples both tables. Every observation must be
either `(0, 0)` or `(N, N)`; anything else is a torn transaction that a consumer
could have seen. Rubric 1.3 asks for atomicity *in MotherDuck*, so 1.3 is not
scored 5 until that test passes. Its `test_gap_a_torn_transaction_is_observable_in_motherduck`
counterpart was deleted for the same reason as the local gap pins. It is kept deliberately small (one 3 000-event
transaction, no large loads) to keep MotherDuck usage light.

Conventions are described in `tests/rubric/README.md#rubric-1-0-engine-error-propagation`.

## Note on the target shape

The target tests assert on `cdcf_commit_id` — the MotherDuck commit-group
identifier defined in `docs/adr/0001-transactional-applier.md`. A commit group is
allowed to contain *many whole* Postgres transactions (MotherDuck sustains only
~100 txn/s), but never part of one. So the invariant is
"`count(DISTINCT cdcf_commit_id) = 1` per `dbz_tx_id`", **not** the reverse.


<a id="rubric-1-4-pk-updates"></a>

# tests/rubric/1.4_pk_updates — "if a primary key is updated, correctly handle it"

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


<a id="rubric-1-5-truncate-drop"></a>

# tests/rubric/1.5_truncate_drop — "truncate table and drop table propagate"

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
