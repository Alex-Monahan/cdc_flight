# RUBRIC_STATUS — baseline scoring (Phase 0.6)

Scores every item of `cdc_tool_decision_matrix_v3_for_swarm.md` against the
**Phase-0 baseline** of this repo, i.e.

```
PostgreSQL 18.1 (project-local cluster, :15432, wal_level=logical)
  -> Debezium 3.6.0.Final embedded engine (pydbzengine 3.6.0.0 / JPype, plugin=pgoutput)
  -> ExtractNewRecordState (unwrap, delete.tombstone.handling.mode=rewrite)
  -> dlt 1.29.1, write_disposition="append"
  -> DuckDB 1.5.4 file  /  MotherDuck (cdc_flight_dev)
```

Target for every item is **5**. The scale is the rubric's own; where the rubric
defines only some of 1–5 (e.g. `1 / 2 / 5`) an intermediate score is used and the
mapping is justified in the item's notes.

> **Item count.** The rubric's own preamble says "42 rubric items", but the
> document contains **40** numbered items (8 + 6 + 7 + 6 + 4 + 2 + 4 + 3).
> `TODO.md`'s Phase 1–8 task list also enumerates 40. All 40 are scored below;
> no item is skipped. If two items were lost in the v2→v3 re-categorisation they
> need to be restored before Phase 9.2 sign-off.

### How these scores were produced

* A score is only assigned from **observed behaviour**: an existing test, a probe
  under [`probes/`](probes/), or the exact default read out of the vendored
  Debezium / dlt / pydbzengine sources in `../repos/`.
* **Scoring is deliberately conservative.** Where the rubric mapping is ambiguous,
  or where the happy path is proven but the failure path is not, the *lower*
  score is taken. Anything resting on assumption rather than an executed
  experiment is scored as if the worst plausible behaviour were true, and the
  note names the experiment that would raise it. An inflated baseline score
  hides work; a deflated one only costs a later agent one probe.
* Items whose evidence is incomplete are marked **provisional** with the missing
  evidence named explicitly.

### Reproducing the evidence

```bash
make up && make seed
uv run python probes/p01_dml_edge_cases.py     # etc.
```

Each probe writes JSON to `probes/.out/<name>.json`. Probes are evidence scripts,
not tests: several deliberately break the source schema, and every one reseeds it
first. They use their own replication slot, offset file, dlt state directory and
DuckDB file, so they never disturb `make pipeline` or the pytest suite.

| Probe | Answers |
|---|---|
| `p01_dml_edge_cases.py` | 1.4 PK update, 1.5 TRUNCATE, 7.4 logical messages |
| `p02_schema_evolution.py` | 2.1 add/drop, 2.2 rename, 2.5 type change |
| `p03_table_lifecycle.py` | 2.3 new tables, 1.5 DROP TABLE, 7.3 partitions |
| `p04_offset_mismatch.py` | 1.8 externally-advanced slot |
| `p05_concurrent_instances.py` | 4.2 concurrent Flights |
| `p06_perf_latency_memory.py` | 5.1 / 5.2 / 5.3 / 5.4 |
| `p07_crash_duplication.py` | 1.1 / 1.2 / 1.7 — SIGKILL landed too late, inconclusive; superseded by p13 |
| `p08_snapshot_consistency.py` | 1.6 snapshot/CDC consistency, 3.1, 3.3 |
| `p09_replica.py` | 7.2 read from a hot standby |
| `p10_slot_and_offset_failures.py` | 4.1 lost slot, 4.3 bad offset |
| `p11_dropped_slot_logs.py` | 4.1 / 4.3 / 6.2 — the *silent* failure mode |
| `p12_motherduck_throughput.py` | 5.1 / 5.3 / 1.3 against the real destination |
| `p13_offset_replay.py` | 1.1 / 1.2 / 1.7 — deterministic duplication |

---

## Changes since the baseline scoring

The scores below are the **Phase-0 baseline** and are deliberately left
unchanged until each item is re-measured. Two things have moved underneath them:

* **TODO 1.0(b) — engine failures no longer exit 0.** `SupervisedDebeziumEngine`
  (`src/cdc_flight/engine.py`) registers a Debezium `CompletionCallback`, so a
  connector that fails to start now raises `EngineFailure`, exits non-zero and
  writes `last_run.json` with `ok: false` and the Debezium message. A hang
  detected by the watchdog is also a non-zero exit now. This does **not** change
  any score by itself — 4.1/4.3 still have no recovery, and 6.2 still has no
  alerts — but it is the precondition for measuring 1.8, 4.1, 4.2, 4.3 and 6.2 at
  all. Pinned by `tests/1.0_engine_error_propagation/`.
* **Deterministic fault injection exists** (`src/cdc_flight/faults.py`,
  `CDC_FAULT_INJECT`). 1.7's "robust injection of failures in testing" now has
  machinery behind it; the score stays at 1 until the applier makes duplication
  impossible, which is what `tests/1.1_*` and `tests/1.2_*` measure.
  A real `kill -9` at 200 000 rows reproduced the at-least-once behaviour again
  under the new harness: **205 706 rows / 200 000 distinct = 5 706 duplicates**,
  zero rows lost (`tests/1.1_exactly_once_pk::test_slow_real_sigkill_loses_nothing`).
  An immediately preceding run of the same test killed outside the flush window
  and produced 0 duplicates — which is precisely why the default suite relies on
  the deterministic crash point rather than the race.
* **The architecture that closes §1 is decided** in
  [`docs/adr/0001-transactional-applier.md`](docs/adr/0001-transactional-applier.md).
  **Revision 2** (after the dual review of TODO 1.0) withdrew the ordering
  argument that the first revision's exactly-once proof rested on and replaced it
  with **Invariant O** — Debezium's offset store must never contain an offset
  that is not already durable in MotherDuck. See the ADR's revision history.
* **TODO 1.0(feedback) — three correctness defects found by review were fixed in
  code**, all of which change what a *measurement* of §4/§6 means:
  1. `markBatchFinished()` can return normally without flushing (Debezium
     discards `commitOffsets()`'s boolean). `src/cdc_flight/consumer.py` now
     replaces pydbzengine's `ChangeConsumer` and raises `OffsetFlushFailed` if
     `offsets.dat` did not move. `offset.flush.interval.ms` is 0 so a flush is
     always expected. (`offset.commit.policy=…$AlwaysCommitOffsetPolicy` does
     **not** work — Debezium requires a `(Properties)` constructor that class
     does not have; measured.)
  2. **Engine death still exited 0** on the retriable-restart path. Reproduced
     under fault injection: walsender killed mid-stream ⇒ `ok: true`,
     `records: 57 344 / 60 000`, `stop_reason: idle`, `EXIT=0`. The 8 s idle
     timer is shorter than Debezium's 10 s restart backoff, so a reconnect looks
     exactly like an idle stream. `src/cdc_flight/source_health.py` now requires
     the slot to have been *continuously* held for the whole quiet window before
     a run may be called idle. Pinned by
     `tests/1.0_engine_error_propagation/test_1_0_supervisor_liveness.py`
     (`slow`), which fails on the pre-fix code and passes on the fixed code.
  3. An engine thread that returns on its own in streaming mode is now a
     non-zero exit rather than `stop_reason: engine_finished, ok: true`.
* **Measured slot signals** (60 000-row stream, 2026-07-30), recorded because the
  idle detector and 6.1's lag metric both depend on them:
  `pg_current_wal_lsn() - confirmed_flush_lsn` settles at **328–384 bytes** on a
  healthy run, but **freezes ~1.8 MB behind and never recovers** after a
  connector reconnect, even once every row has been delivered. So slot lag is a
  usable *health* signal and a poor *completion* signal.
  `pg_stat_replication.sent_lsn` is useless for progress here: it read 0–48 bytes
  behind current WAL while `confirmed_flush_lsn` was 19 MB behind, because the
  walsender had already pushed everything into Debezium's in-memory queue.

Two measurements made while writing those tests are worth recording because they
correct assumptions in the notes below:

1. Rolling `offsets.dat` back does **not** reliably force a replay: Postgres will
   not stream from before the slot's `restart_lsn`. In a two-transaction test
   only the tail replayed. `probes/p13` case A worked only because its rollback
   happened to stay inside the retained window.
2. `snapshot.mode=never` does not exist in Debezium 3.6 (the valid values are
   `always, initial_only, configuration_based, when_needed, initial, custom,
   no_data`). The old CLI help text advertised `never`; before 1.0(b) that
   misconfiguration exited **0** with `records: 0`.

## Summary

| # | Item | Score | One-line gap |
|---|---|---|---|
| 1.1 | Delivery guarantees, tables WITH a primary key | 3 | At-least-once, **proven**: SIGKILL mid-load left 2 048 duplicate rows on restart; `append` makes them permanent. |
| 1.2 | Delivery guarantees, tables WITHOUT a primary key | 3 | Same machinery, and with no key there is nothing to dedupe on afterwards. |
| 1.3 | CDC changes atomic in MotherDuck | 1 | Batches are 2048-record windows, not Postgres transactions; each table is its own dlt transaction. |
| 1.4 | Primary-key update handled correctly | 2 | Debezium emits delete+insert correctly, but the append-only destination keeps both rows and the delete image is fabricated zeros/empty strings. |
| 1.5 | TRUNCATE / DROP propagate | 1 | `skipped.operations` defaults to `t`, so truncate never reaches us; DROP TABLE is silently ignored. |
| 1.6 | Snapshot/backfill consistent with CDC | 3 | Consistent on the healthy path (proven); an interrupted snapshot restarts from scratch and the partial snapshot is already appended. |
| 1.7 | Failures do not cause correctness issues | 1 | Zero fault injection in the suite; a real `kill -9` mid-load produced 2 048 duplicate rows (`p13` case B). |
| 1.8 | Externally-advanced slot detected → backfill | 1 | **Proven silent data loss**: 31 change events skipped, run reported `records: 0`, exit 0. |
| 2.1 | Added / dropped columns handled | 2 | Adds work correctly; a dropped column silently lingers and reads NULL, indistinguishable from a real NULL. |
| 2.2 | Renamed columns handled well | 1 | Rename lands as "new column + old column silently goes NULL". No tombstone, no linkage. |
| 2.3 | New tables and schemas auto-discovered | 1 | Needs `ALTER PUBLICATION` + config change + restart, and pre-existing rows are silently never snapshotted. |
| 2.4 | Postgres types → native MotherDuck types | 1 | numeric→base64 VARCHAR, date/time/timestamp/interval→BIGINT, NaN/Inf degrade the column to VARCHAR, `col_numeric_nan` dropped entirely. |
| 2.5 | Data type changes supported | 3 | dlt adds a `__v_text` variant column beside the old one; no widening logic, no UNION type. |
| 2.6 | TOAST columns handled well | 1 | Unchanged TOAST arrives as the literal `__debezium_unavailable_value` — silent corruption, not an error. |
| 3.1 | Backfill scalable / parallelized | 3 | 120 k rows in ~28 s single-threaded (`snapshot.max.threads=1`); works but does not scale, untested past 120 k. |
| 3.2 | Backfills atomic | 1 | Snapshot rows are appended straight into the live table; no shadow table, no swap. |
| 3.3 | Existing tables keep receiving CDC during snapshot | 1 | Debezium's initial snapshot blocks streaming entirely; everything goes stale for the snapshot's duration. |
| 3.4 | Snapshot an arbitrary set of tables | 1 | Only the global `snapshot.mode`; no `signal.data.collection`, so no ad-hoc/incremental snapshots. |
| 3.5 | Per-table CDC / full refresh / incremental refresh | 3 | CDC only. |
| 3.6 | Backfill when CDC falls too far behind | 1 | Lag is never measured, so nothing can trigger on it. |
| 3.7 | Failed backfill resumes midway | 1 | Debezium restarts the initial snapshot from scratch, and the partial snapshot is already appended. |
| 4.1 | Recover from failed / lost slot | 1 | **Proven**: slot dropped → engine fails to start, process exits **0** in 1 s, slot never recreated, permanent silent no-op. |
| 4.2 | Concurrent Flight instances | 1 | Two simultaneous runs both exit 0; same-slot runs silently no-op, different-slot runs silently duplicate into the same tables. |
| 4.3 | Recover from problematic WAL / offset state | 1 | A bad offset kills the engine; there is no backfill, no retry, and the failure is reported as success. |
| 4.4 | Idle-slot heartbeat | 1 | `heartbeat.interval.ms` unset, no `heartbeat.action.query`. |
| 4.5 | Errors must not hang or lock | 2 | Bounded runner + JVM watchdog make hangs survivable (one observed in p09), but nothing systematic prevents them. |
| 4.6 | Detect silently-dead Postgres connection | 1 | Only `database.tcpKeepAlive=true` at OS defaults (~2 h). No client read timeout, no heartbeat, never tested. |
| 5.1 | CDC fast on large changes | 3 | 50 k-row transaction absorbed at ~3.5 k rows/s into local DuckDB; no failure, but a full `dlt.run()` per 2048-row batch is the ceiling. |
| 5.2 | Low latency on small changes | 1 | Capture latency is 83 ms, but the deliverable is a bounded batch job with no defined cadence — end-to-end latency is the schedule interval. |
| 5.3 | Keep up with high Postgres TPS | 2 | ~1 k events/s inside the engine, but ~17 s of per-run JVM/connect overhead drops the shipped bounded job to ~157 events/s to MotherDuck. |
| 5.4 | Bounded memory / spill to disk | 1 | `max.queue.size.in.bytes=0` (disabled): the bound is on record count only. A 2048-row batch of 64 kB TOAST bodies is ~128 MB of JSON before parsing. |
| 6.1 | Detailed logs in MotherDuck incl. replication lag | 1 | Nothing lands in MotherDuck: one local JSON summary and a log4j file. |
| 6.2 | Alerts and warnings | 1 | No alerting, and the worst failure modes exit 0 — the pipeline actively lies about its health. |
| 7.1 | No Postgres extension required | 5 | `plugin.name=pgoutput` with a version-controlled `PUBLICATION`. |
| 7.2 | Read from a Postgres replica | 1 | Snapshot from a hot standby proven to work, but streaming was never exercised and the run hung on shutdown. |
| 7.3 | Partitioned tables handled gracefully | 3 | One logical table via `publish_via_partition_root`; DETACH/DROP PARTITION silently ignored; no per-partition or DuckLake option. |
| 7.4 | Capture `pg_logical_emit_message` | 3 | Proven to land, but incidentally: base64 payload, raw un-unwrapped envelope, no test, no consumer. |
| 8.1 | Hard and soft delete options | 1 | Soft delete only (`deleted='true'` rows), and not even a current-state view. |
| 8.2 | Change history / SCD2 | 1 | Not supported in any form. |
| 8.3 | PII controls | 1 | None: no column exclusion, masking, hashing or truncation. |

**Average: 66 / 40 = 1.65 out of 5.** Items already at 5: **1 of 40** (7.1).
Distribution: **26 items at 1**, 4 at 2, 9 at 3, 0 at 4, 1 at 5.
Distance to target: **134 rubric points**.

---

## 1. Delivery Guarantees & Correctness

### 1.1 Delivery guarantees for tables WITH a primary key — **3 / 5**

`at-most-once=1, at-least-once=3, exactly-once=5`

**Evidence.** `repos/pydbzengine/pydbzengine/_jvm.py:121-124` calls
`committer.markProcessed()` / `markBatchFinished()` *after* `handleJsonBatch()`
returns, and `offset.flush.interval.ms=1000`
(`src/cdc_flight/debezium_props.py:77`). So the offset is never ahead of the
destination write — losses are impossible on this path, replays are not. Write
disposition is `append` (`src/cdc_flight/handler.py:62`), so a replay is a
permanent duplicate.

`probes/p13_offset_replay.py` proves both halves:

* **Case A (deterministic).** 1 000 rows loaded, then the previous
  `offsets.dat` restored — exactly the state a SIGKILL in the flush window
  leaves behind. The next run reloaded the same 1 000 records:
  `2000 rows / 1000 distinct ids`, **1 000 duplicated ids**.
* **Case B (real SIGKILL).** 400 000-row transaction, process `kill -9`'d 14 s
  in with 47 104 rows loaded. After restart: `402 048 rows / 400 000 distinct`
  — **2 048 duplicated ids, exactly one `max.batch.size` batch**, and **zero
  rows lost**. Textbook at-least-once.

`research/NOTES.md` records the same thing happening unprompted against
MotherDuck. (`probes/p07_crash_duplication.py` tried to catch the window with a
timed SIGKILL against only 60 k rows and lost the race — kept as a record of the
failed attempt.)

**Gap to 5.** Offsets must be committed inside the same destination transaction
as the rows, or the write must be idempotent. Concretely, one of:
(a) store the Debezium offset in a MotherDuck table written in the same
`BEGIN/COMMIT` as the batch, and make the handler read it back on start; or
(b) key every row by `(dbz_lsn, dbz_tx_id, table, pk)` and switch from `append`
to a `merge`/`INSERT … ON CONFLICT` that is a no-op on replay. (a) is required
anyway for 1.3.

**Pointers.** `src/cdc_flight/handler.py`, `src/cdc_flight/debezium_props.py`
(`offset.storage*`), `repos/pydbzengine/pydbzengine/_jvm.py`,
`tests/test_e2e_duckdb.py::test_second_run_is_incremental`.

### 1.2 Delivery guarantees for tables WITHOUT a primary key — **3 / 5**

Same mechanism, same score. `app.sensor_readings` has `REPLICA IDENTITY FULL`,
so Debezium *does* deliver complete before-images for updates and deletes
(`tests/test_e2e_duckdb.py` asserts `{"r": 4, "c": 6, "u": 4, "d": 2}`), but
there is no key to deduplicate on afterwards — a replayed batch is
indistinguishable from six genuinely identical readings.

**Gap to 5.** Same transactional-offset fix as 1.1, plus a synthetic identity for
keyless tables (`(dbz_lsn, dbz_tx_id, ordinal-within-transaction)` is unique and
comes free in the envelope). Note that `REPLICA IDENTITY FULL` is currently set
in `sql/01_schema.sql`; a real source may not have it, and Postgres refuses to
decode UPDATE/DELETE without it — that case is untested.

### 1.3 CDC changes should be atomic in MotherDuck — **1 / 5**

`no transactional boundaries=1, single-table transactional batches=3, multi-table=5`

**Evidence.** Postgres transaction boundaries are never consulted. A Debezium
batch is a fixed window of up to `max.batch.size=2048` records
(`src/cdc_flight/debezium_props.py:79`), so a single PG transaction of 50 000
rows is split across 25 batches — observed in `p06`
(`A_bulk_50k_one_txn.batches == 25` for one `INSERT … generate_series(1,50000)`),
and in `p13` a single 400 000-row transaction became **174 batches**.
Each batch becomes one `dlt.run()`, and inside a load package dlt runs **one
transaction per table**: `repos/dlt/dlt/destinations/insert_job_client.py:21-29`
wraps a single table's insert file in `begin_transaction()`. So even
single-table PG transactions are not respected, and multi-table ones certainly
are not. Against MotherDuck the same shape holds: `p12` recorded
`md_load_packages == 4` for 4 runs' worth of batches, one commit per batch per
table.

**Gap to 5.** The handler must buffer whole Postgres transactions (`dbz_tx_id`
changes are the boundary; Debezium's transaction-metadata topic gives exact
`event_count` per transaction) and emit one MotherDuck `BEGIN … COMMIT`
containing an integral number of PG transactions across all tables. That means
bypassing `dlt.run()`-per-batch and driving the destination connection directly,
or teaching dlt to load a multi-table package in one transaction.

**Architectural note.** MotherDuck sustains roughly 100 transactions/s, so
batches must span many PG transactions — the commit policy needs both a size and
a time trigger, and the offset write (1.1) has to ride in the same transaction.

**Pointers.** `src/cdc_flight/handler.py:93-126`,
`repos/dlt/dlt/destinations/insert_job_client.py`,
`repos/dlt/dlt/destinations/job_client_impl.py`.

### 1.4 Primary-key update handled correctly — **2 / 5**

`error=1, duplication=2, correctly deleted and inserted or updated=5`

**Evidence** (`probes/p01_dml_edge_cases.py`). `UPDATE app.customers SET id=9001
WHERE id=1` produced exactly two events in one transaction (`dbz_lsn=47290944`,
`dbz_tx_id=1497`): a `d` for `id=1` and a `c` for `id=9001`. The change *stream*
is correct. The **destination** is not: `cdcflight_app_customers` now contains
the original `r` row for `id=1`, a delete marker for `id=1`, and a `c` row for
`id=9001` — a consumer doing `SELECT * FROM cdcflight_app_customers` sees the
customer twice, and nothing in the repo resolves it.

Worse, the delete event carries **fabricated values**, not NULLs:

```
{'id': 1, 'external_ref': '00000000-0000-0000-0000-000000000000', 'name': '',
 'email': '', 'signup_at': 1970-01-01T00:00:00Z, 'lifetime_value': 'AA==' (=0),
 'is_active': True, 'prefs': '{}', 'deleted': 'true', 'dbz_op': 'd'}
```

Root cause is visible in `logs/pydbzengine.log`: `JsonConverterConfig …
replace.null.with.default = true`. With `REPLICA IDENTITY DEFAULT` only the key
is present in the before-image, and the converter fills every other field with
its schema default. A naive last-write-wins merge would resurrect the row with
zeroed data.

**Gap to 5.** (a) Set `value.converter.replace.null.with.default=false` so
missing before-image fields stay NULL. (b) Materialise current state — a merge
that applies `d` rows — so the old key actually disappears. Shared with 8.1/8.2.

### 1.5 TRUNCATE and DROP TABLE propagate — **1 / 5**

`silently ignored=1, logged=2, tombstones/soft delete=3, replicated faithfully=5`

**Evidence.** `TRUNCATE TABLE app.orders` in `p01`: the destination still shows
`{'r': 5}` for orders, `any_op_t == 0`, and the run's `skipped` counter is **0** —
the event never even reached the handler. Cause is upstream and exact:
`repos/debezium/debezium-connector-common/.../CommonConnectorConfig.java:865-875`
— `skipped.operations` `.withDefault("t")`, "By default, only truncate
operations will be skipped". Our publication does include `truncate`
(`sql/01_schema.sql:150`), so this is purely a Debezium default we never
overrode.

`DROP TABLE app.documents` in `p03`: no error, subsequent CDC keeps flowing
(`runE_drop_table` loaded the following customer insert), and
`cdcflight_app_documents` keeps its 2 rows forever
(`documents_rows_after_drop == [["r", 2]]`). DROP is not in logical decoding at
all.

**Gap to 5.** (a) `skipped.operations=none` plus handling for `op='t'` — emit a
truncate marker and actually `DELETE FROM`/`TRUNCATE` the destination table
inside the batch transaction. (b) DROP needs an out-of-band detector: an event
trigger writing to a signal table, or a periodic catalog diff, feeding the same
propagation path. Both need a policy switch (replicate vs. tombstone) because
"faithfully replicated" destroys destination data.

### 1.6 Snapshot/backfill consistent with CDC — **3 / 5** (provisional)

`inconsistent=1, consistent=5`

**Evidence** (`probes/p08_snapshot_consistency.py`). 120 005 preloaded rows,
then 300 rows inserted *during* the snapshot at ~30/s. Result: 120 000 preload
rows present with 120 000 distinct ids, and all 300 concurrent rows present
exactly once, all as `c` (streaming) events — no gap, no overlap, no duplicate.
Debezium's exported-snapshot + slot-LSN coordination works.

**Why not 5.** The failure path is not consistent and is not tested here. The
Postgres connector docs state plainly: *"If the connector stops during a
snapshot, the connector begins a new snapshot when it restarts"*
(`repos/debezium/documentation/.../postgresql.adoc:109`). With
`write_disposition="append"` the abandoned partial snapshot is already in the
destination, so the restarted snapshot duplicates every row it re-reads.

**Evidence that would raise this.** A probe that SIGKILLs the process partway
through a large initial snapshot and shows the destination afterwards contains
each source row exactly once.

**Gap to 5.** Snapshot into a shadow table (3.2) and swap atomically, so a
failed snapshot leaves nothing behind; and record the snapshot's start LSN so the
swap and the stream hand over at a known point.

### 1.7 Failures do not cause correctness issues — **1 / 5**

`duplication possible due to crash=1, impossible but not well tested=3, robust fault injection=5`

**Evidence.** There is no fault injection in `tests/` at all. The crash probes
written for this evaluation are the first, and they land the rubric's 1
squarely: `p13` case B `kill -9`'d the process mid-load and the restart left
**2 048 duplicate rows** in the destination (`402 048 rows / 400 000 distinct`).
Duplication is not a theoretical risk here, it is the measured behaviour.

**Gap to 5.** Fix 1.1/1.3 first, then build a fault-injection harness that is
part of `make test`: kill mid-snapshot, kill mid-load, kill between load and
offset flush, kill the Postgres backend, drop the slot, sever the connection —
each asserting exact row counts at the destination.

### 1.8 Externally-advanced slot detected → backfill — **1 / 5**

`silent data loss=1, process exits=4, automatic backfill=5`

**Evidence** (`probes/p04_offset_mismatch.py`). Snapshot run, then 31 change
events generated, then
`pg_replication_slot_advance(slot, pg_current_wal_lsn())`. The next run:

```json
{"records": 0, "batches": 0, "stop_reason": "idle", "returncode": 0}
```

Thirty-one changes gone, exit code 0, no warning. Root cause confirmed in the
engine log (`p11`): `Using offset mismatch strategy 'no_validation': Connector
will not validate slot position`.
`repos/debezium/.../PostgresConnectorConfig.java:677-688` — `offset.mismatch.strategy`
defaults to `NO_VALIDATION`, and the docs note that on Postgres 15+ the server
silently starts from `confirmed_flush_lsn` instead of erroring.

**Gap to 5.** Set `offset.mismatch.strategy=trust_offset` so the mismatch is
detected and raised (that alone is worth 4), then convert the raised condition
into an automatic re-snapshot of the affected tables (needs 3.2/3.4). Note the
property is marked Technology Preview upstream, so we should also validate the
slot ourselves on acquisition: compare `confirmed_flush_lsn` against the stored
offset before starting the engine.

---

## 2. Schema Evolution & Type Handling

### 2.1 Added or dropped columns must be handled — **2 / 5**

`no=1, yes=5`

**Evidence** (`probes/p02_schema_evolution.py`).
*Add*: `ALTER TABLE app.customers ADD COLUMN loyalty_tier text DEFAULT 'bronze'`
→ `loyalty_tier VARCHAR` appears at the destination and carries correct values
(`'gold'` on the insert, `'silver'` on the update). Clean.
*Drop*: `ALTER TABLE app.customers DROP COLUMN is_active` → the destination
column **stays** and subsequent rows read `is_active = NULL`
(`valuesB_is_active` shows `('c', null, 1)` after the drop alongside
`('c', true, 1)` before it). Nothing marks the column as dropped.

Scored 2 rather than 5 because half the item is not handled, and rather than 1
because adds provably are. A dropped column silently reading NULL is
indistinguishable from a genuinely NULL value — a correctness problem, not just
cosmetics.

**Gap to 5.** Detect the drop (the Debezium schema change / relation message has
it) and either drop the destination column or mark it dropped in a schema-history
table with the LSN at which it disappeared, so consumers can tell "no longer
exists" from "NULL". Add a test pinning both directions.

### 2.2 Renamed columns must be handled well — **1 / 5**

`no=1, old column renamed with tombstone=3, seamless rename=5`

**Evidence** (`p02`). `ALTER TABLE app.customers RENAME COLUMN name TO
full_name` → the destination gains `full_name VARCHAR`, keeps `name VARCHAR`,
and rows written after the rename have `name = NULL`, `full_name = 'Renamed
Col'`. Rows written before have the opposite. No tombstone, no mapping, no
warning. Score 1: this is exactly the "add + stale column" non-handling.

**Gap to 5.** pgoutput does not expose renames as renames — the relation message
just has different column names at the same attribute numbers. Seamless rename
therefore needs attribute-number tracking (`pg_attribute.attnum` via a catalog
diff or an event trigger writing to a signal table) so old and new names can be
identified as the same column, then a destination `ALTER TABLE … RENAME COLUMN`
and backfill of the historical rows.

### 2.3 New tables and schemas auto-discovered — **1 / 5**

`no=1, infrequently or requiring restart=4, automatically on short interval=5`

**Evidence** (`probes/p03_table_lifecycle.py`). Three stages:

| stage | action | records |
|---|---|---|
| A | `CREATE TABLE app.newcomer` + 2 rows (not published, not in include list) | 0 |
| B | `ALTER PUBLICATION … ADD TABLE app.newcomer` + 1 row (still not in include list) | 0 |
| C | `CDC_TABLES` widened + process restart | 2 |

Stage C delivered rows 3 and 4 (row 3 was replayed from the WAL once the table
was included), but rows **1 and 2 are permanently lost** — no snapshot is ever
taken for a newly included table, so all pre-existing data is silently missing.
Discovery requires a `PUBLICATION` change *and* a config change *and* a restart,
and is lossy. That is 1, not 4.

**Gap to 5.** Poll `pg_class`/`pg_publication_tables` on a short interval; add
new tables to the publication and the include list automatically; trigger a
targeted snapshot for each new table (3.4) so pre-existing rows arrive. New
*schemas* are equally undiscovered — `schema.include.list` is a single value in
`src/cdc_flight/debezium_props.py:68`.

### 2.4 Postgres types accurately converted to native MotherDuck types — **1 / 5**

`most types text/json=1, core scalars well (nested as text)=3, (nested as json)=4, full=5`

**Evidence** (`p02` `colsD_wide_types`, `tests/test_e2e_duckdb.py::test_documented_baseline_gaps`).
Observed destination types for `app.wide_types`:

| Postgres | destination | verdict |
|---|---|---|
| `numeric(30,10)` | `VARCHAR` (base64 of the unscaled bytes) | broken |
| `numeric` holding `NaN` (`col_numeric_nan`) | **column absent entirely** | silent data loss |
| `date`, `time`, `timestamp`, `interval` | `BIGINT` (epoch days / micros) | broken |
| `timestamptz` | `TIMESTAMP WITH TIME ZONE` | correct |
| `timetz` | `VARCHAR` | broken |
| `bytea` | `VARCHAR` (base64) | broken |
| `double precision` holding `Infinity` / `NaN` | `VARCHAR` — the whole column degrades | broken |
| `json` / `jsonb` | `VARCHAR`, not DuckDB `JSON` | below par |
| `int[]`, `text[]`, `numeric[]` | dlt **child tables**, not `LIST` | below par |
| `point` | three columns `__x`, `__y`, `__wkb` | below par |
| `money`, `inet`, `cidr`, `macaddr`, `bit`, `int4range`, enum | `VARCHAR` | acceptable per rubric |
| `smallint`/`integer`/`bigint`/`real`/`double`/`bool`/`char`/`varchar`/`text`/`uuid` | native | correct |

`numeric` and the date/time family are core scalar types and they are all wrong,
so this cannot reach the rubric's "3". The dropped `col_numeric_nan` column is
the most serious individual finding in this section: an entire column of source
data vanishes without an error.

**Gap to 5.** Almost certainly means abandoning `ExtractNewRecordState` +
`JsonConverter` and consuming the full Debezium envelope with its Connect schema,
so the semantic type (`io.debezium.time.MicroTimestamp`,
`org.apache.kafka.connect.data.Decimal`, …) is available at mapping time. Cheaper
partial wins to sequence first: `decimal.handling.mode=string`,
`time.precision.mode=connect` (or `isodatetime`), `binary.handling.mode=hex`,
`interval.handling.mode=string`, plus explicit dlt column hints. NaN/Infinity
need a JSON encoding decision (DuckDB `DOUBLE` supports both natively).
Arrays must become DuckDB `LIST`, `json`/`jsonb` must become `JSON`.

### 2.5 Data type changes supported — **3 / 5**

`error=1, drop and add=3, widening automatic=4, MotherDuck UNION types=5`

**Evidence** (`p02` step D). `ALTER COLUMN col_smallint TYPE text` then inserting
`'now-a-string'` → dlt created a **variant column**: `col_smallint BIGINT` (old
values) alongside `col_smallint__v_text VARCHAR` (new values). No error, no data
loss, but the consumer must know to coalesce two columns.
`ALTER COLUMN col_integer TYPE bigint` was invisible because dlt already maps
every Postgres integer width to `BIGINT` — that is a coincidence, not widening
logic, so it does not earn the rubric's 4.

**Gap to 5.** Represent the column as a MotherDuck/DuckDB `UNION(bigint BIGINT,
str VARCHAR)` so both representations live in one column with their types
intact, and migrate the existing values into the union on the DDL event. Needs a
dlt destination-level type hint or a post-load `ALTER TABLE`, and a decision on
what happens to downstream views.

### 2.6 TOAST columns handled well — **1 / 5**

`errors on TOAST=1, handled but inefficiently=4, handled efficiently=5`

**Evidence.** `tests/test_e2e_duckdb.py::test_documented_baseline_gaps` asserts
that after an UPDATE that does not touch the TOASTed `body` column, the
destination row contains the literal string `__debezium_unavailable_value`.

Scored 1 rather than 4: it does not error, but it is *worse* than an error —
it writes a plausible-looking string over real data with no marker, so the
destination silently disagrees with the source. "Handled inefficiently" implies
the correct value eventually arrives; here it never does.

**Gap to 5.** Options, cheapest first: (a) `REPLICA IDENTITY FULL` on TOAST-heavy
tables — correct but multiplies WAL volume; (b) detect the placeholder in the
handler and carry forward the previous value from the destination (requires
current-state materialisation, 8.2); (c) re-read the row from Postgres on demand
(costs a point lookup per affected row, and is racy against later updates).
Whatever is chosen must be configurable per table and must never write the
placeholder string.

---

## 3. Backfill & Refresh Modes

### 3.1 Backfill scalable and performant (parallelized) — **3 / 5** (provisional)

`fails on large tables=1, slow=3, fast=5`

**Evidence** (`p08`). 120 320 rows snapshotted in 40.5 s wall (59 batches),
≈ 28 s excluding JVM start and the 12 s idle tail → **~4 300 rows/s**, entirely
single-threaded: `snapshot.max.threads` defaults to 1
(`repos/debezium/.../CommonConnectorConfig.java:936-944`) and we never set it.
At that rate a 100 M-row table takes ~6.5 hours with no parallelism and no
resumability (3.7).

**Evidence that would raise or lower this.** A snapshot of ≥10 M rows,
and the same against MotherDuck rather than a local DuckDB file. If it fails or
OOMs at that size the score is 1.

**Gap to 5.** Parallel chunked backfill: split each table by primary-key range,
run N readers concurrently, write into per-chunk shadow tables, and use
MotherDuck's bulk path rather than `INSERT … VALUES` per 2048 rows.
Debezium's incremental snapshot (with `signal.data.collection`) already chunks;
`snapshot.max.threads > 1` parallelises across tables but not within one.

### 3.2 Backfills must be atomic — **1 / 5**

`clear and repopulate=1, alternate table + rename swap=4, fully atomic=5`

**Evidence.** `src/cdc_flight/handler.py:62` — every resource is
`write_disposition="append"`, and the snapshot writes into the same destination
table consumers read. `p08` shows the snapshot arriving as 59 separate load
packages, so a consumer querying mid-backfill sees a partially populated table,
and a re-snapshot doubles the rows instead of replacing them.

**Gap to 5.** The mandated design: snapshot into `<table>_tmp` shadow tables,
then one MotherDuck transaction that renames every `_tmp` into place (or
`CREATE OR REPLACE TABLE … AS SELECT`). Must cover the dlt child tables for
arrays (`cdcflight_app_customers__tags` etc.) in the same transaction.

### 3.3 Existing tables continue to receive CDC during a healthy snapshot — **1 / 5**

`go stale=1, continue but with complexity/errors=2, simple and elegant=5`

**Evidence** (`p08`). All 300 rows inserted during the 28-second snapshot arrived
as `c` events — but only *after* the snapshot completed. Debezium's initial
snapshot is blocking: streaming does not start until it finishes. Every
replicated table was therefore stale for the entire snapshot window. Scaled to
the 6.5-hour snapshot implied by 3.1, that is a 6.5-hour outage for every other
table.

**Gap to 5.** Use Debezium's *incremental* snapshot (signal-table driven,
watermark-based) which interleaves snapshot chunks with the live stream, so
adding or re-snapshotting one table never pauses the others. Requires
`signal.data.collection` (a table in Postgres) and
`incremental.snapshot.watermarking.strategy`.

### 3.4 Snapshot an arbitrary set of tables while others keep streaming — **1 / 5**

`whole database only=1, one table=4, any arbitrary set=5`

**Evidence.** The only control is the global `snapshot.mode`
(`src/cdc_flight/debezium_props.py:70`, exposed as `--snapshot-mode`). There is
no `signal.data.collection`, no signalling channel, no way to ask for a
re-snapshot of `app.orders` alone. `p03` stage C shows the consequence: a newly
included table gets no snapshot at all.

**Gap to 5.** Create a signal table in Postgres, set `signal.data.collection`,
and expose an `execute-snapshot` API (CLI + a MotherDuck-side control table) that
takes a list of tables and optional row filters. Pairs with 3.2's shadow tables.

### 3.5 Per-table CDC / scheduled full refresh / scheduled incremental refresh — **3 / 5**

`CDC only=3, CDC and full refresh=4, all three=5`

**Evidence.** `src/cdc_flight/config.py` has one global `CDC_TABLES` list and one
`snapshot_mode`. There is no per-table configuration of any kind and no
scheduler. CDC only.

**Gap to 5.** A per-table mode declaration (`cdc` | `full_refresh` | `incremental`)
with a cursor column and schedule for the refresh modes, plus a scheduler that
runs them. Full refresh reuses 3.2's shadow-table swap; incremental refresh needs
a watermark column and merge semantics.

### 3.6 Auto-backfill when CDC falls too far behind — **1 / 5**

`falls further behind=1, size trigger=4, size or time trigger=5`

**Evidence.** Replication lag is never computed. Nothing in `src/cdc_flight/`
queries `pg_replication_slots`, and the run summary
(`src/cdc_flight/pipeline.py:141-148`) reports only records/batches/elapsed.
With no measurement there can be no trigger.

**Gap to 5.** Measure both dimensions each run —
`pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` for bytes and
`now() - dbz_source_ts_ms` for time — publish them (6.1), and when either crosses
a threshold, abandon the stream and trigger a targeted backfill (3.4) that
advances the slot past the backlog.

### 3.7 Failed backfill resumes midway — **1 / 5**

`restarts from the beginning=1, restarts incomplete tables=4, resumes per table=5`

**Evidence.** Debezium's own documentation:
*"If the connector stops during a snapshot, the connector begins a new snapshot
when it restarts"* and *"If the connector fails … upon restart the connector
begins a new snapshot"*
(`repos/debezium/documentation/modules/ROOT/pages/connectors/postgresql.adoc:109,197`).
Combined with `append`, the abandoned partial snapshot stays in the destination,
so the restart is not just slow — it duplicates.

**Gap to 5.** Chunked backfill with per-table, per-chunk progress persisted in a
MotherDuck control table, so a restart resumes at the last completed chunk. Falls
out of the same work as 3.1/3.2.

---

## 4. Failure Detection & Recovery

### 4.1 Recover from a failed or lost slot — **1 / 5**

`error or restart required=1, recovers with backfill=3, failover slot + backfill=5`

**Evidence** (`probes/p10_slot_and_offset_failures.py`,
`probes/p11_dropped_slot_logs.py`). Slot dropped externally while the offset file
still points at an old LSN. Debezium's log:

```
WARN  BaseSourceTask - Last recorded offset is no longer available on the server.
ERROR AsyncEmbeddedEngine - 1 task(s) out of 1 failed to start.
ERROR AsyncEmbeddedEngine - Engine has failed with
      DebeziumException: The connector is trying to read change stream starting at …
```

The pipeline's own summary for the same run:

```json
{"records": 0, "batches": 0, "stop_reason": "engine_finished", "returncode": 0}
```

`slot_recreated == 0`: the slot is never recreated, so **every subsequent run is
a one-second no-op that reports success**. Rows inserted before and after the
drop never arrive (`rows_landed == 0`, `rows_landed_after_second == 0`).

This is two defects. The Debezium-level one is the missing recovery. The
repo-level one is worse and is a bug in `run_engine_bounded`
(`src/cdc_flight/pipeline.py:92-148`): the async engine reports its failure
through its completion callback, not by raising out of `engine.run()`, so
`error_box` stays empty and the runner reports success. **Fix this first — it
currently masks every failure mode in this section.**

**Gap to 5.** (a) Register a Debezium `CompletionCallback` / check
`AsyncEmbeddedEngine` completion state and fail the process loudly. (b) On
"offset no longer available", drop the stale offset and trigger a full re-snapshot
into shadow tables. (c) Support failover slots (`slot.failover=true`,
`CREATE_FAIL_OVER_SLOT` exists in the connector — `PostgresConnectorConfig`) plus
`synchronized_standby_slots` on the primary, so a promoted standby keeps the slot.

### 4.2 Handle or prevent concurrent Flight instances — **1 / 5**

`hangs / poorly handled=1, fail unpredictably=3, fail predictably=5`

**Evidence** (`probes/p05_concurrent_instances.py`). Two runs launched ~1 s
apart:

* **same slot, same offset file, same DuckDB file** — both processes exited **0**.
  Postgres permits only one active connection per slot, so one of them cannot
  have streamed; nothing surfaced that.
* **different slots, same destination** — both exited 0 and the second one loaded
  its own full 20-row snapshot into the same tables the first was writing, i.e.
  silent duplication.

No lock, no lease, no error. This is the rubric's 1 ("other bad consequences"),
not 3 — "unpredictable failure" would at least be visible.

**Gap to 5.** A destination-side lease (a row in a MotherDuck control table with
an owner id and heartbeat-updated expiry, taken in the same transaction as the
offset write) so a second instance fails immediately with a clear message. The
Postgres slot's own `active` flag is a useful second check on acquisition.

### 4.3 Recover from an unhandled or problematic WAL message — **1 / 5**

`hangs or stuck=0, restart required for backfill=2, automatic backfill=5`

**Evidence** (`p10` case B, `p11`). A corrupted / far-future offset produces the
same shape as 4.1: engine fails to start, `records: 0`, exit 0, no recovery, no
backfill, and the state is permanent. It does not hang — the bounded runner
returns in ~1 s — but it does not recover either, and it does not even tell you.
(Case B ran after case A had already removed the slot, so it is not a clean
isolation of "bad offset alone"; the score does not depend on that distinction
because the outcome is identical and the engine-failure-swallowing bug is
common to both.)

**Evidence that would raise this.** A probe that corrupts the offset with a
healthy slot present and shows a clean, loud failure.

**Gap to 5.** Same as 4.1: surface engine failure, classify it, and route
"cannot resume from stored position" to an automatic re-snapshot.

### 4.4 Idle-slot heartbeat to advance the slot — **1 / 5**

`no heartbeat=1, heartbeat present=5`

**Evidence.** `src/cdc_flight/debezium_props.py:104-106` documents the deliberate
omission: `heartbeat.interval.ms` unset, `heartbeat.action.query` unset. With
`table.include.list` covering only `app.*`, activity in any other schema
advances the WAL without producing events, so `confirmed_flush_lsn` stalls and
Postgres retains WAL indefinitely.

**Gap to 5.** `heartbeat.interval.ms` (e.g. 10 000) plus
`heartbeat.action.query = SELECT pg_logical_emit_message(false, 'cdc_flight_hb',
now()::text)` — which also gives the heartbeat a visible trace via 7.4 — and the
handler must acknowledge heartbeat events so the offset advances even when no
business rows moved. Verify with
`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` staying flat while an
unreplicated table is hammered.

### 4.5 Errors must not cause hanging or locking — **2 / 5**

`hanging hard to recover=1, hanging recoverable=2, systematically prevented=5`

**Evidence.** Hangs are real but survivable. `p09` produced
`stop_reason: "hung"` — the Debezium engine thread did not stop within 60 s of
`engine.close()`, and only the `os._exit` watchdog
(`src/cdc_flight/pipeline.py:212-232`) got the process out. The bounded runner
also caps every run at `max_seconds`. So: recoverable (2), but nothing
*prevents* the hang, and the watchdog's exit code was **0**, so the hang was
invisible to any scheduler.

**Gap to 5.** A liveness heartbeat inside the CDC loop (last-progress timestamp
updated by the engine, watched by the supervisor), bounded socket-level timeouts
on the Postgres connection, and a non-zero exit whenever the watchdog rather than
a clean shutdown ends the process.

### 4.6 Detect failure of a Postgres node with a silently-dead connection — **1 / 5**

`unable to detect=1, TCP keepalives under 30 min=3, full heartbeat under 1 h=5`

**Evidence.** Debezium sets `database.tcpKeepAlive` `.withDefault(true)`
(`repos/debezium/.../PostgresConnectorConfig.java:1096-1104`) but the OS defaults
govern the timing — ~2 hours idle on macOS/Linux — and we override nothing.
`status.update.interval.ms` defaults to 10 s
(`PostgresConnectorConfig.java:998-1006`), which does put writes on the socket,
but the resulting TCP retransmission timeout is neither configured nor measured.
The `wal_sender_timeout = 60s` in `scripts/pg.sh` protects the *server* from a
dead client, not us from a dead server. No test exists.

**Evidence that would raise this.** A probe that severs the connection (packet
filter or SIGSTOP on the postmaster) and measures time-to-detection.

**Gap to 5.** Set explicit keepalive parameters on the JDBC connection
(`database.tcpKeepAlive` plus OS-level `tcp_keepidle`/`tcp_keepintvl` via socket
options), add a connection-level read timeout, and add the application heartbeat
from 4.4 as the authoritative liveness signal with a detection budget well under
an hour. Then measure it.

---

## 5. Performance, Latency & Scale

All numbers below are from `probes/p06_perf_latency_memory.py` and
`probes/p08_snapshot_consistency.py` on an M-series Mac, local DuckDB
destination, unless stated otherwise.

### 5.1 CDC scalable and performant for large changes — **3 / 5**

`fails on large changes=1, slow=3, fast=5`

**Evidence.** One transaction inserting 50 000 rows (Postgres committed it in
0.23 s) was absorbed in 24.2 s of engine time across **25 batches**, ≈ 14 s
excluding the 10 s idle tail → **~3 500 rows/s**. No failure, no memory error.
The snapshot path in `p08` was similar (~4 300 rows/s for 120 k rows).

Against **MotherDuck** (`probes/p12_motherduck_throughput.py`, deliberately
light): 5 000 rows in 3 batches, 15.1 s of engine time (≈ 5 s excluding the 10 s
idle tail) → ~1 000 rows/s, 4 dlt load packages, `md_row_count == 5005` with no
duplication. Per-run fixed cost is the striking number: `wall_sec == 31.9` for
15.1 s of engine time, i.e. **~17 s of JVM start + MotherDuck connect before any
work happens**.

The ceiling is structural: every batch is a full `dlt_pipeline.run()`
(`src/cdc_flight/handler.py:116`) — schema resolution, normalisation, a load
package on disk, `INSERT … VALUES` fragments, then `complete_load`. That is
several hundred milliseconds of fixed cost per 2048 rows even against a local
file.

**Evidence that would raise or lower this.** The same test at 10 M rows, and a
sustained (≥100 k row) MotherDuck run rather than the 5 k smoke test.

**Gap to 5.** Stop calling `dlt.run()` per batch. Write Arrow/Parquet and use
DuckDB/MotherDuck's bulk ingest inside one transaction per commit group, with the
schema resolved once per run instead of once per batch.

### 5.2 CDC low latency on small changes — **1 / 5**

`minutes=1, 30-60 s=4, consistently under 30 s=5`

**Evidence.** Capture latency is excellent: a single row inserted while the
engine was live had `dbz_ts_ms - dbz_source_ts_ms = 83 ms`, and 100 ms from the
`INSERT` call to Debezium processing it.

But the delivered artifact is a **bounded batch job**: `make pipeline` runs the
engine for at most 90 s and stops after 8 s of quiet
(`src/cdc_flight/config.py:88-98`). There is no scheduler and no long-running
mode, so end-to-end latency is "whenever the next run happens" — minutes, by
construction, for any realistic Flight schedule. Scored at the floor because the
90 ms figure measures a component, not the product.

**Gap to 5.** Either a continuously running mode (engine stays up, commits on a
size-or-time trigger — which 1.3 needs anyway), or a demonstrated sub-30 s run
cadence with the JVM start amortised. Then measure commit-in-Postgres →
visible-in-MotherDuck end to end, not `dbz_ts_ms`.

### 5.3 Keep up with high Postgres TPS — **2 / 5** (provisional)

`<100=1, 100-300=2, 300-1000=3, 1000-2000=4, >2000=5`

**Evidence.** 5 000 single-row autocommit transactions were committed by
Postgres in 0.39 s (**~12 950 TPS** at the source) and absorbed by the pipeline in
~5 s of engine time across **3 batches** → **~1 000 rows/s** sustained; the
50 k-row burst reached ~3 500 rows/s. Batching itself works well (5 000
single-row transactions collapsed into 3 dlt loads).

Against MotherDuck (`p12`) the *engine-time* rate is comparable (~1 000 rows/s
for 5 000 rows in 3 batches), but the delivered artifact is a bounded job that
pays ~17 s of JVM + connect overhead per run: amortised over the whole process
the same 5 000 rows landed at **157 events/s**, which is the rubric's band 2.
Scored on that number, not the in-engine one, because process restarts are the
shipped behaviour. It is also a 5 000-row smoke test, not a sustained load, and
the batches are not transaction-aligned — once 1.3 forces whole-transaction
commit groups, MotherDuck's ~100 transactions/s becomes the binding constraint
rather than row throughput.

**Evidence that would raise this.** A sustained MotherDuck run (≥100 k events)
with events/s *and* commits/s reported, from a long-running engine rather than a
cold start.

**Gap to 5.** 1.3's commit-group design (many PG transactions per MotherDuck
transaction) plus 5.1's bulk ingest. Then re-measure against MotherDuck.

### 5.4 Well-managed low memory use and/or spill to disk — **1 / 5**

`all in memory, no guardrails=1, in memory with guardrails=5, disk backed/spill=5`

**Evidence.** Max RSS (`/usr/bin/time -l`): 318 MB idle (JVM + Python floor),
481 MB for the 5 000-row run, **629 MB** for the 50 000-row burst. Bounded in
practice for narrow rows.

But the only bound is on **record count**: `max.batch.size=2048`,
`max.queue.size=8192` (`src/cdc_flight/debezium_props.py:79-81`), while
`max.queue.size.in.bytes` defaults to **0 = disabled**
(`repos/debezium/.../CommonConnectorConfig.java:649,747-755`). A batch of 2048
rows from `app.documents` (64 kB TOASTed bodies) is ~128 MB of JSON text before
`json.loads` triples it, and the handler materialises the whole batch as Python
dicts (`src/cdc_flight/handler.py:93-107`) before dlt ever sees it. Scored at the
floor because the byte-bounded case is the one that matters and it is both
unbounded and untested.

**Evidence that would raise this.** A burst of thousands of TOAST-heavy rows with
RSS measured; if it stays flat, this is a 3–5.

**Gap to 5.** Set `max.queue.size.in.bytes`, stream events into the destination
writer instead of building a full Python list per batch, and add a memory
watermark that forces an early commit. dlt already spills its load packages to
disk; the Python-side buffer does not.

---

## 6. Observability & Alerting

### 6.1 Detailed logs, consumable in MotherDuck, including replication lag — **1 / 5**

`can't see slot health=1, detailed logs in Postgres=2, non-detailed in MotherDuck=3, detailed incl. lag=5`

**Evidence.** The only outputs are `.cdc_state/last_run.json`
(`src/cdc_flight/pipeline.py:273-275` — stop reason, elapsed, records, batches,
per-table counts) and `logs/pydbzengine.log` (raw log4j). **Nothing is written to
MotherDuck.** No replication lag, no slot state, no per-table freshness, no error
history. `probes/_common.py:slot_info()` written for this evaluation is the only
code in the repo that has ever queried `pg_replication_slots`.

**Gap to 5.** A `_cdc_flight` schema in MotherDuck with, at minimum: `runs`
(start, end, outcome, exit reason, error text), `table_stats` (rows by op, last
source LSN, last source timestamp), and `slot_health` (`restart_lsn`,
`confirmed_flush_lsn`, retained WAL bytes, seconds behind), all written inside
the batch transaction so they cannot disagree with the data.

### 6.2 Alerts and warnings for issues — **1 / 5**

`not enough logs for alerts=1, enough logs but no alerts=2, alerts built in=5`

**Evidence.** There is no alerting, and — the reason this is 1 rather than 2 —
the pipeline's health signal is actively wrong. Every one of these exits **0**:

| condition | probe | reported |
|---|---|---|
| replication slot dropped, engine fails to start | `p11` | `records: 0`, exit 0 |
| slot advanced externally, 31 events lost | `p04` | `records: 0`, exit 0 |
| two concurrent instances, one cannot stream | `p05` | both exit 0 |
| engine thread hung, killed by the watchdog | `p09` | `stop_reason: "hung"`, exit 0 |

An alert built on today's signals would never fire.

**Gap to 5.** Fix the exit-code bug in `run_engine_bounded` first (see 4.1), then
land 6.1's tables, then evaluate alert rules in-process (lag over threshold, zero
records when the slot shows a backlog, engine failure, lease conflict, schema
drift) and write them to an `alerts` table with severity — plus a non-zero exit
so the Flight scheduler itself notices.

---

## 7. Postgres Features to Support

### 7.1 Requires a Postgres extension — **5 / 5** ✅

`obscure plugin=1, wal2json=2, pgoutput=5`

**Evidence.** `src/cdc_flight/debezium_props.py:60` sets
`plugin.name=pgoutput` — built into Postgres, no extension. The publication is
version-controlled in `sql/01_schema.sql:142-150` and
`publication.autocreate.mode=disabled` prevents Debezium from inventing one. The
whole test suite runs against a stock Homebrew `postgresql@18` with only
`wal_level=logical`.

**Keep it that way.** Do not let a later phase reach for `wal2json` to work
around a type-mapping problem (2.4).

### 7.2 Able to read from a Postgres replica — **1 / 5** (provisional)

`primary only=1, replica but disrupts primary=3, replica with light primary workload=5`

**Evidence** (`probes/p09_replica.py`). A hot standby was built with
`pg_basebackup` on :15433 (`hot_standby_feedback=on`) and the pipeline pointed at
it. It **worked**: `pg_is_in_recovery() = t`, the run snapshotted 21 records
including a row inserted on the primary, and the logical slot was created on the
standby — `primary_slots` shows only the physical `probe_standby_slot`. Debezium
handles standbys explicitly
(`PostgresConnection.java:594` chooses `pg_last_wal_receive_lsn()` when
`pg_is_in_recovery()`).

Scored at the floor anyway, per the conservative rule:

* the run ended with `stop_reason: "hung"` — the engine thread never stopped and
  the watchdog had to kill the process;
* only the **snapshot** path was exercised; streaming from the standby was never
  tested, and logical slots on a standby can be invalidated by recovery conflicts;
* `hot_standby_feedback=on` is required and does hold back vacuum on the primary,
  which is exactly the "disrupts the primary" caveat in the rubric's 3;
* nothing in the repo configures, documents or tests replica mode.

**Evidence that would raise this.** A probe that streams live changes from the
standby for several minutes, shuts down cleanly, and shows the primary's vacuum
horizon unaffected.

**Gap to 5.** Fix the shutdown hang, add streaming coverage, handle slot
invalidation on recovery conflict by re-snapshotting, and document the required
primary settings (`synchronized_standby_slots`, `hot_standby_feedback`).

### 7.3 Handle partitioned Postgres tables gracefully — **3 / 5**

`cannot handle=1, handles except detach/drop partition=3, options for per-partition or one table=4, +DuckLake=5`

**Evidence.** `app.audit_log` (range-partitioned by month) arrives as one logical
table thanks to `publish_via_partition_root = true` (`sql/01_schema.sql:150`);
`tests/test_e2e_duckdb.py` asserts `{"r": 3, "c": 2}` on it. In `p03`:

* `ALTER TABLE app.audit_log DETACH PARTITION app.audit_log_2026_06` — a row
  inserted into the detached table afterwards did **not** arrive (correct, since
  it left the publication) but nothing signalled the change and the destination
  keeps the detached partition's historical rows forever;
* `DROP TABLE app.audit_log_2026_08` — no error, subsequent parent inserts kept
  flowing, and the dropped partition's rows stay in the destination indefinitely.

That is exactly the rubric's 3: partitioned tables work, detach and drop do not.
The score is capped at 3 regardless because there is **no option** — one large
table is the only behaviour available.

**Gap to 5.** A per-table setting choosing `partition_root` (today),
`per_partition` (topic-per-partition, `publish_via_partition_root=false` plus
naming), or a partitioned DuckLake target; plus handling for
DETACH/DROP PARTITION that removes or tombstones the affected rows at the
destination.

### 7.4 Capture `pg_logical_emit_message` messages — **3 / 5**

`not supported=1, supported=5`

**Evidence** (`p01`). Both a transactional and a non-transactional message were
captured and landed in a `cdcflight_message` table:

```
('m', 'cdc_flight', 'dHJhbnNhY3Rpb25hbC1oZWxsbw==',        lsn=47288704, tx_id=1495)
('m', 'cdc_flight', 'bm9udHJhbnNhY3Rpb25hbC1oZWxsbw==',    lsn=47288840, tx_id=NULL)
```

So the capability is real and this contradicts the informal note in
`research/NOTES.md` §4 §7.4.

Scored 3, not 5, because it works by accident rather than by design: the message
records are **not** unwrapped by `ExtractNewRecordState` (the table carries the
raw envelope — `op`, `ts_ms`, `source__*` — not the `dbz_*` metadata every other
table has), the payload is base64, `resolve_table_name()`
(`src/cdc_flight/handler.py:28-42`) falls back to the topic name for them, and
there is no test. A later change to the SMT config or the internal-topic filter
in `src/cdc_flight/pipeline.py:42` would silently drop them.

**Gap to 5.** Decide the message shape deliberately: decode the content, give it
the same `dbz_*` metadata columns, use `logical_decoding_message.prefix.include.list`
to separate heartbeat messages (4.4) from application messages, and pin it with a
test.

---

## 8. Specific CDC Features

### 8.1 Hard and soft delete options — **1 / 5**

`soft delete only=1, hard delete only=4, both=5`

**Evidence.** `transforms.unwrap.delete.tombstone.handling.mode=rewrite`
(`src/cdc_flight/debezium_props.py:99`) rewrites deletes into rows carrying
`deleted='true'`, and `write_disposition="append"` keeps them. There is no hard
delete, no configuration switch, and no current-state view — the destination is
purely a changelog, so a "deleted" row is still present as its earlier insert
(pinned by `tests/test_e2e_duckdb.py::test_documented_baseline_gaps`).

**Gap to 5.** Materialise current state (a merge keyed on the source PK) and
offer, per table, `hard` (delete the row) or `soft` (keep it with a
`_deleted_at`), with the changelog retained separately (8.2).

### 8.2 Change history / SCD2 mode — **1 / 5**

`not supported=1, global flag=3, per table=4, per table with current state + changelog=5`

**Evidence.** No SCD2, no validity intervals, no current-state table — and no
option to ask for any of it. The append-only changelog is the only shape
available; it is not an SCD2 table (no `valid_from`/`valid_to`, no surrogate key,
no ordering guarantee within a batch).

**Gap to 5.** Per-table mode producing both a current-state table (merge, hard or
soft delete per 8.1) and a changelog/SCD2 table with
`valid_from`/`valid_to`/`is_current` derived from `dbz_source_ts_ms` and
`dbz_lsn`. Needs 1.3's transaction boundaries so the SCD2 intervals are
consistent across tables.

### 8.3 PII controls — **1 / 5**

`all columns always replicated=1, exclusion only=3, exclusion + masking + salted hash + truncation w/ per-column regex=5`

**Evidence.** `grep -ri 'mask\|hash\|exclude\|pii' src/` returns nothing but
`hashlib` in the test data generator. Every column of every included table is
replicated verbatim; `src/cdc_flight/config.py` has table-level selection only.

**Gap to 5.** Debezium supplies most primitives — `column.exclude.list`,
`column.mask.with.<n>.chars`, `column.mask.hash.<algo>.with.salt.<salt>`,
`column.truncate.to.<n>.chars` — all regex-matched on
`schema.table.column`. Wire them into `config.py` as a declarative per-column
policy, add a test proving excluded columns never reach the destination (not even
in dlt's schema files or load packages on disk), and decide whether masking
happens in the JVM (safer: the plaintext never enters Python) or in the handler.

---

## What to fix first

Ordered by how much other work they unblock, not by rubric number.

1. **`run_engine_bounded` swallows engine failures** (`src/cdc_flight/pipeline.py:92-148`).
   Not a rubric item on its own, but it makes 4.1, 4.2, 4.3, 1.8 and 6.2 all look
   like successes. Nothing in §4 or §6 can be measured until this is fixed.
2. **Transactional offsets + commit groups** (1.1, 1.2, 1.3, 1.7). One design
   decision — write the Debezium offset inside the same MotherDuck transaction as
   the batch — buys exactly-once and multi-table atomicity together.
3. **Type mapping** (2.4). Currently the largest *silent* correctness gap: a
   whole column disappears, `numeric` is base64, dates are integers. Likely
   requires dropping `ExtractNewRecordState`, which also unblocks 2.6 and 7.4.
4. **Shadow-table backfills + incremental snapshots** (3.1–3.4, 3.7, 1.6, 2.3).
   All six items are the same piece of machinery.
5. **Heartbeats** (4.4, 4.5, 4.6, and half of 6.1). Cheap relative to the above,
   and 4.4 comes almost free with `heartbeat.action.query` +
   `pg_logical_emit_message`, which 7.4 already proves lands.
