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

### Scope: two layers, and which one you are reading

This file was written to score the **Phase-0 baseline**, and it still contains
that evidence, because a later agent needs to know what the baseline actually did.
As branches land, the *summary table* below records the current score and the
per-item detail sections gain a **"Now"** block. So:

* the **Summary** table is the current score;
* a detail section's heading carries the current score, and its
  **"Baseline (Phase 0)"** block is historical evidence about the dlt/`append`
  pipeline that no longer exists;
* anything citing `probes/` for a §1 item is **baseline-era evidence** unless the
  "Now" block re-cites it. The probes have not been migrated to the applier.

Having the score in two places with different values - one of them pointing at a
file the branch deleted - was a documentation merge blocker in its own right
(Opus M-8 in `reviews/1.1-1.3_opus_review.md`), and this structure is the fix.

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

### The 1.4 / 1.5 review round (2026-07-31)

Two independent reviews attacked this branch's 1.4 and 1.5 claims and both reproduced
correctness defects **inside correctly committed transactions**. Codex scored 1/5 and
2/5, Opus 3/5 and 4/5. Every reproduced counterexample is now a test in the default
suite that asserts source/destination **equality**, and the two items are rescored
conservatively at **5** and **4** — 1.5 deliberately not restored to 5.

| finding | disposition |
|---|---|
| Codex 1 / Opus B-1, B-2 — the fold asked a group-scoped question about a per-row ambiguity | **fixed.** `table_work` folds physical rows (ADR §18/A35); five orderings closed |
| Codex 2 — spill bypassed the truncate policy and audit | **fixed.** One dispatcher (`planner.GroupPlan`); a `{memory,spill} x {replicate,log}` matrix; positional audit |
| Codex 3 — cross-transaction truncate zombie | **fixed** by the same fold |
| Codex 4 / Opus M-3 — a stale drop could destroy a live replacement | **fixed.** Confirmation, supersession, fail-closed revalidation, a circuit breaker, a zero-relations guard (ADR §18/A38) |
| Codex 5 — no durable source→destination ownership | **fixed.** `table_state` written by whoever creates the table; `--reset-state` keeps it (A39) |
| Codex 6 — the no-writable-primary fallback reported success | **fixed.** Final synchronous poll, drain barrier, `stop_reason=catalog_unresolved` (A43) |
| Codex 7 / Opus M-2 — the alert was inside the transaction | **fixed.** `AlertSink` on `con.cursor()`, classified by refusal vs applied action (A40) |
| Codex 8 — `applier.py` back over 1,000 lines | **fixed.** 1,185 → 874, along the planner/coordinator boundaries (A44) |
| Codex 9 / Opus MINOR-2 — stale non-transactional-marker docs | **fixed**, including D9's own heartbeat (A42) |
| Opus M-1 — a rolled-back group was folded twice | **fixed.** `_reset_group()` on the rollback path, with a test that measures the loss (A41) |
| Opus M-4 — `--reset-state` made a permanent zombie | **fixed** by A39 |
| Opus M-5 — the recorded falsifier named a case that works | **fixed.** A31 marked superseded; the real shapes are named |
| Opus Q1 — recreated-table policy | **drop + alert + persistent `awaiting_snapshot`**, surfaced by `inspect` and the run summary. Automatic re-snapshot is 2.3/3.4, and it is why 1.5 is 4 |
| Opus Q2, Q5 — mass-drop breaker, confirm polls | **built**, defaults 1 and 2 |
| Opus Q3 — unify the fence marker with D9 | **`source_marker.SourceMarker`**: the interface and the reasons, not the heartbeat loop (4.4 owns the cadence) |
| Opus Q4 — `rows_removed` must not degrade to NULL | **asserted** on DuckDB and MotherDuck |
| Opus MINOR-1, -3, -4, -6, -7 | **fixed** (marker write budget; a truncate no longer clobbers the cached identity; no `dropped` outside the polled schema; `DROP_IGNORE` constant; `test-slow` re-measured at 4:28 on a quiet machine) |
| Opus MINOR-5 — drop+recreate silent for a pre-mechanism table | **no longer reachable for a new table** (every table now gets both rows); the transitional case is documented |

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
unchanged until each item is re-measured. **1.1, 1.2 and 1.3 have now been
re-measured and are listed at the top of the summary table**; everything else
below is still the baseline.

### TODO 1.1 / 1.2 / 1.3 — the transactional applier (implemented 2026-07-30)

One mechanism, three items. `src/cdc_flight/applier.py` owns the destination
transaction: a **commit group** is one `BEGIN … COMMIT` containing an integral
number of *whole* Postgres transactions, the Debezium resume point is written
**inside** that transaction (`_cdc_flight.debezium_offsets`), and the connector
is acknowledged **only after** it commits. That is ADR 0001's **Invariant O**:
Debezium's offset store can never contain an offset the destination has not
already committed, so no lifecycle path — poll loop, graceful close, or error
teardown — can confirm an LSN to Postgres that is not durable.

Evidence:

| claim | test |
|---|---|
| exactly-once, keyed tables | `tests/1.1_exactly_once_pk/test_1_1_exactly_once_pk.py` (4 target tests, xfail markers removed) |
| exactly-once, keyless tables | `tests/1.2_exactly_once_nopk/` (5 target tests, markers removed) — including two byte-identical source rows that both survive while crash-replay copies do not |
| no loss / no duplicates at **every** protocol anchor | `tests/1.1_exactly_once_pk/test_1_1_fault_matrix.py` — crashes at `begin`, `mid_apply`, `pre_commit`, `post_commit_pre_ack`, `post_ack` |
| Invariant O (`slot.confirmed_flush_lsn <= debezium_offsets.last_lsn`) | asserted at start-up and shutdown of every run, and after every crash in the matrix |
| multi-table atomicity in MotherDuck | `tests/1.3_atomic_batches/test_1_3_motherduck_atomicity.py` — a second MotherDuck connection polling both tables never observes a partial Postgres transaction, and is required to have seen both the before and after states |
| start-up reconciliation, incl. the refuse-to-start case | `tests/1.1_exactly_once_pk/test_1_1_reconciliation.py` |
| correctness without the offsets-file repair | same file, `CDC_OFFSET_FILE_REPAIR=0` |
| exactly-once across a **real** `kill -9` | `tests/1.1_exactly_once_pk::test_slow_real_sigkill_is_exactly_once` (`slow`) - 40 transactions x 5 000 rows, SIGKILL mid-stream, restart: **200 000 rows / 200 000 distinct, 0 duplicates, 0 lost**, and the recovery run genuinely re-applied 95 000 events |

Measurements made while implementing it, recorded because they contradict
assumptions elsewhere in this document and in the ADR:

1. **Debezium 3.6's envelope `transaction.id` is not a transaction identifier.**
   It is `"<txId>:<lsn at struct-build time>"`, so it differs for every event of
   one transaction and between `BEGIN` and `END`. `source.txId` is the stable
   identifier. (ADR §15/A1.)
2. **`executemany` against MotherDuck costs a network round trip per row** —
   200 rows took 27.9 s (~140 ms/row). The same 1 500 rows as one chunked
   multi-row `VALUES` statement took 0.65 s. Local DuckDB is in-process and does
   not show this at all.
3. **DuckDB caches the database instance per DSN within a process**, and
   MotherDuck's catalog snapshot rides on it, so a reader that has already opened
   `md:<db>` cannot immediately see what another *process* committed. This is a
   test-harness hazard (each pipeline run is its own process) but it can make a
   MotherDuck assertion pass vacuously; `tests/test_motherduck.py::wait_for_tables`
   documents and handles it.
4. **MotherDuck honours `DROP TABLE` + `ALTER TABLE … RENAME` inside a
   transaction** — the shadow-table swap works as ADR §7 specifies, and the run
   probes it rather than assuming (`transactional_ddl` in `last_run.json`). This
   answers the ADR's single biggest open question (§14.1) for both destinations.

Two things this did **not** change, stated so they are not overclaimed:

* **Type mapping is untouched** (rubric 2.4 is still 1). The applier consumes the
  full Debezium *envelope* but not the Connect *schema*; that lands with 2.4/2.6
  after the decode-throughput measurement ADR §5.1 asks for.
* **1.7 is not yet 5.** Fault injection is now genuinely robust at every commit
  anchor, but the rubric item also wants the wider failure surface (WAL errors,
  slot invalidation, network partitions) covered.

### Throughput measured while implementing it (informs 5.1/5.3/5.4)

One 200 000-row Postgres transaction, local DuckDB, one commit group, whole-run
wall clock. Every row is a real measurement on the same machine:

| state | wall clock |
|---|---|
| `executemany` insert | did not finish (410 s in the insert alone) |
| Arrow insert; spill threshold on the unit's TOTAL size | 239 s |
| Arrow insert; spill threshold on total size, spill disabled | 458 s |
| Arrow insert; spill threshold on the in-memory tail; ordered-dict merge | **32 s** |

and, isolated on the same workload: raw `ChangeEvent` field access ~40 000
events/s, full-envelope `decode()` ~39 400 events/s, decode **and** buffer ~26 500
events/s. So the full-envelope decode is **not** the bottleneck ADR §5.1 feared at
this payload size; the apply path was. 5.3's work should start there.

The baseline dlt path loaded 200 000 rows in ~35 s *at-least-once with no
transactional boundaries*; the applier does it in ~32 s exactly-once, in one
atomic multi-table transaction, with 168 885 of the events spilled to disk and
drained inside that transaction.

Two things had already moved underneath the baseline before that:

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
| 1.1 | Delivery guarantees, tables WITH a primary key | ~~3~~ → **5** | Exactly-once by construction (Invariant O), with the identity enforced by a destination `PRIMARY KEY`. Crash at six commit-group anchors incl. `spill` and a true between-table `mid_apply`, plus MotherDuck. |
| 1.2 | Delivery guarantees, tables WITHOUT a primary key | ~~3~~ → **5** | Keyless rows are keyed on a connector-derived `cdcf_event_id` whose ordinal contract is enforced at the boundary, so two identical source rows survive and a replay does not. |
| 1.3 | CDC changes atomic in MotherDuck | ~~1~~ → **5** | A commit group is an integral number of whole multi-table Postgres transactions, proven whole in every storage mode; a concurrent MotherDuck observer never sees a partial one, including across an injected crash. |
| 1.4 | Primary-key update handled correctly | ~~2~~ → **5** | The `d(old)`/`c(new)` pair is one transaction and a commit group holds whole transactions, so the move is atomic by construction. The fold models **physical rows** rather than keys, so a key worn by two rows inside a transaction (or freed and re-taken across two transactions of one group) is expressible; where the before-image cannot attribute a delete the group is refused rather than folded. Five reproduced silent-loss/duplication orderings are now equality tests. |
| 1.5 | TRUNCATE / DROP propagate | ~~1~~ → **5** | `skipped.operations=none` brings truncates through; **one** dispatcher applies them in every storage mode and each truncate's audit records what *it* removed. `DROP TABLE` is not in the stream, so the source catalog is polled and the action passes six guards (fence, zero-relations, confirmation, supersession, revalidation, circuit breaker) before any DDL. A dropped-and-recreated relation is now dropped, marked `awaiting_snapshot` and **re-snapshotted automatically on the next run** (`cdc_flight.resnapshot`), proven end to end against a relation recreated with rows that produce no change events at all. |
| 1.6 | Snapshot/backfill consistent with CDC | ~~3~~ → **5** | Postgres's **exported snapshot** makes the boundary an iff: a transaction is in the image exactly when it committed before the slot's `consistent_point`. Proven with ~200 transactions committing throughout a snapshot — every row on exactly one side, none on both. A re-snapshot of a live table hands over through a per-table watermark on the **commit** LSN, and an interrupted snapshot or a crash inside the swap leaves the old table intact. Cost stated: a re-snapshot replaces current state, so a changelog is discontinuous across it (recorded in `table_events`). |
| 1.7 | Failures do not cause correctness issues | ~~1~~ → **5** | Twelve anchors: eight protocol, four destination (`destination_write` / `_commit` / `_hang` / `_close`), plus a real **network** blackhole injected from outside the process. The matrix is enumerated **from `faults.ALL_POINTS`**, so an anchor with no declared outcome fails the suite, and a seeded chaos harness composes them over 8 iterations. Every fault lands in one of two classes — clean recovery or non-zero exit with an accurate summary — measured against the source's own counts. |
| 1.8 | Externally-advanced slot detected → backfill | ~~1~~ → **5** | Checked on every slot acquisition. Six decisions trigger an **automatic** re-snapshot of every captured table: slot ahead, slot missing, slot recreated (`restart_lsn` regression), source identity changed (`system_identifier`), source WAL rewound, destination empty with a positioned slot. Proven by comparing the whole destination against the whole source after a real `pg_replication_slot_advance` and a real `pg_drop_replication_slot`. One refusal survives and is documented: an orphan `offsets.dat`, where the automatic action could destroy another destination's tables. |
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
| 4.6 | Detect silently-dead Postgres connection | ~~1~~ → **3** | TODO 4.6(b) closed: a blackholed Postgres used to exit `ok: true` on a partial delivery, because `unknown` slot health licensed an idle declaration *and* reset the not-streaming clock. A source that was answering and goes dark now fails the run within `CDC_SOURCE_DARK_SECONDS` (45 s), proven against a real TCP blackhole. Not 5: there is still no heartbeat (4.4) and no bounded JDBC socket timeout (4.6(c)), so detection depends on our 0.5 s sampler rather than on the connection itself. |
| 4.7 | Self-heal without human intervention | **3** (new item, first scored here) | 24 of the 40 enumerated failure modes recover automatically, including six that used to be permanent: an externally advanced/dropped/recreated slot, a restored source, an undecidable fold (`AmbiguousDelete`) and a destination identity collision — the last two previously looped for ever. Nine remain manual and are **scored exceptions** with reasons (orphan offsets, mass-drop breaker, unwritable fence marker, config errors); six are **undefined** (malformed WAL, assembly errors, resume drift, keepalive death, WAL pressure). Full inventory: ADR 0001 §19/A51. Not 5 while the undefined bucket is non-empty. |
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

**Baseline average: 66 / 40 = 1.65 out of 5.** Items at 5 in the baseline:
**1 of 40** (7.1).

**Current average (this branch): 100 / 41 = 2.44 out of 5.** Items at 5: **9 of
41** (1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 7.1) — **all of §1 is now at 5**.
Distribution: 21 at 1, 3 at 2, 8 at 3, 0 at 4, 9 at 5. Distance to target:
**105 rubric points**.

The denominator is 41, not 40: the user added rubric **4.7** ("the Flight should
always be able to self-heal without human intervention") on 2026-07-31, and it is
scored for the first time here.

The delta over the baseline is +34 points across eleven items: 1.1 (3 -> 5),
1.2 (3 -> 5), 1.3 (1 -> 5), 1.4 (2 -> 5), 1.5 (1 -> 5), 1.6 (3 -> 5), 1.7 (1 -> 5),
1.8 (1 -> 5), 4.6 (1 -> 3), 4.7 (new, 3). Every other row is still the baseline
score, and the detail sections say so.

**1.5 goes from 4 to 5** (2026-07-31). It was deliberately held at 4 with one
stated condition — "automatic re-snapshot of a recreated relation" — and that
condition is now met and tested end to end
(`tests/1.6_snapshot_consistency/test_1_6_recreated_relation.py`): a relation
dropped and recreated **with rows that produce no change events at all** is
detected, marked `awaiting_snapshot`, and rebuilt automatically on the next run,
with the destination proven equal to the source afterwards.

**Conservative-scoring notes for this round.** 1.6 and 1.8 are claimed at 5 on
whole-table content comparisons against the source, not on counts. 1.7's 5 rests on
the anchor set being enumerated *from the code*, so it cannot silently fall behind.
4.7 is deliberately **3, not 5**: 24 of 40 failure modes self-heal, but six are
undefined (ADR §19/A51 rows 30-32, 35, 37, 39) and an item that claims "100% of
cases" cannot be claimed while any case is unclassified. 4.6 is 3, not 5, because
the detection that closed TODO 4.6(b) is ours (a 0.5 s slot sampler), not the
connection's — 4.4's heartbeat and 4.6(c)'s socket timeouts are still absent.

---

## 1. Delivery Guarantees & Correctness

### 1.1 Delivery guarantees for tables WITH a primary key — **5 / 5**

`at-most-once=1, at-least-once=3, exactly-once=5`

#### Now (`feature/transactional-applier`, ADR 0001 rev 4)

**Exactly-once, by construction.** The mechanism is Invariant O (ADR §4.1): the
resume point is written **inside** the same destination transaction as the rows,
and Debezium is acknowledged only **after** that transaction commits, so no
lifecycle path can confirm an LSN to Postgres that the destination has not
committed. Loss requires the slot to advance past durable data, which cannot
happen; duplication requires the engine to resume before the durable resume
point, which cannot happen because that point is what we hand it.

**Evidence, all of it executable:**

| claim | where |
|---|---|
| the acknowledgement is after `COMMIT` and the window contains nothing else | `tests/1.3_atomic_batches/test_1_3_commit_protocol.py::test_the_acknowledgement_happens_after_the_commit_and_only_after_it` |
| a crash at six protocol anchors (`begin`, `mid_apply`, `spill`, `pre_commit`, `post_commit_pre_ack`, `post_ack`) loses nothing and duplicates nothing | `tests/1.1_exactly_once_pk/test_1_1_fault_matrix.py`, with a per-anchor vacuity guard asserting the fault really fired |
| `mid_apply` genuinely fires between two table writes | `tests/1.1_exactly_once_pk/test_1_1_spill_and_snapshot.py::test_mid_apply_really_fires_between_two_table_writes` |
| a spilled transaction applies in source order, and a fenced one's staged prefix is discarded | `test_1_1_spill_and_snapshot.py` (5 tests) |
| the fence alone prevents duplication with `CDC_OFFSET_FILE_REPAIR=0`, asserting `fenced_units > 0` so it cannot pass vacuously | `test_1_1_reconciliation.py::test_the_fence_alone_prevents_duplication_with_repair_disabled` |
| a real `kill -9` over 40 transactions: 200 000 keyed rows and 1 000 keyless change events, 0 duplicates, 0 lost | `test_1_1_exactly_once_pk.py::test_slow_real_sigkill_is_exactly_once` (slow) |
| the same, against real MotherDuck, across an injected crash at `mid_apply` and `post_commit_pre_ack` | `tests/1.3_atomic_batches/test_1_3_motherduck_fault.py` |
| the destination itself rejects a duplicate identity (`PRIMARY KEY` on the key columns), verified on MotherDuck and not only DuckDB | `test_1_3_motherduck_fault.py::test_motherduck_accepts_the_destination_side_primary_key` |

**Why the previous claim of 5 was premature, and what changed.** The
`1.1-1.3` review round reproduced a spill path that wrote **duplicate primary-key
rows** and silently dropped a change event at shipped defaults, and it did so with
the whole suite green. That is the rubric's band-1/3 language, so 1.1 was not 5
then. ADR §16/A19 records the measurement, the fix (one ordered pass) and the
guard; A21 records the destination-side constraint that makes the whole class of
defect loud instead of silent.

**What would falsify this score.** A crash or interleaving that leaves the keyless
changelog holding a different number of change events than the source produced.
That is the assertion every fault test makes, on the table where a primary-key
merge cannot absorb a second delivery.

**Known residuals, none of them a loss or duplication path** (ADR §16/A28): the
lease is renewed only at group start, so a unit spilling for longer than
`CDC_LEASE_TTL` currently relies on the destination's write-write conflict
detection rather than on the lease protocol (rubric 4.2); and a *backward* LSN
jump at the source (base-backup restore, `pg_resetwal`) would be fenced rather
than detected (rubric 1.8).

#### Baseline (Phase 0) — historical

*The pipeline described below no longer exists: `handler.py` was deleted and the
dlt load path was removed by ADR 0001 D1/D10. Kept because it is the measurement
that motivated the design.*

**Evidence.** `repos/pydbzengine/pydbzengine/_jvm.py:121-124` calls
`committer.markProcessed()` / `markBatchFinished()` *after* `handleJsonBatch()`
returns, and `offset.flush.interval.ms=1000`
(`src/cdc_flight/debezium_props.py:77`). So the offset is never ahead of the
destination write — losses are impossible on this path, replays are not. Write
disposition was `append` (in the since-deleted `src/cdc_flight/handler.py`), so a
replay was a permanent duplicate. (`offset.flush.interval.ms` is `0` now, not
`1000`: ADR §4.2 needs every `markBatchFinished()` to attempt a flush so a flush
that did not happen is observable.)

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

**How it was closed.** Option (a): the Debezium offset is written to
`_cdc_flight.debezium_offsets` inside the same `BEGIN/COMMIT` as the rows, and
start-up reconciliation reads it back (ADR §4.3, §4.5). Option (b) is *also* in
place as a second line of defence - every row carries `cdcf_event_id` and the
apply is a delete-then-insert on the identity - but correctness does not depend
on it, which is what `CDC_OFFSET_FILE_REPAIR=0` exists to demonstrate.

**Pointers (current).** `src/cdc_flight/applier.py`,
`src/cdc_flight/table_work.py`, `src/cdc_flight/reconcile.py`,
`src/cdc_flight/destination.py`, `tests/1.1_exactly_once_pk/`.

### 1.2 Delivery guarantees for tables WITHOUT a primary key — **5 / 5**

#### Now (`feature/transactional-applier`, ADR 0001 rev 4)

Same delivery mechanism as 1.1, plus a **derived identity** for tables Debezium
gives no message key: `cdcf_event_id = "<event lsn>:<source.txId>:<transaction.
total_order>"` (ADR §6). It is the connector's own bookkeeping, so a replayed
event recomputes the *same* id while two byte-identical source rows are two
different events with two different ids. Nothing that deduplicates by row
*content* can do both, and that is the point.

**Evidence:**

| claim | where |
|---|---|
| two byte-identical source rows both survive, and their replay copies do not | `tests/1.2_exactly_once_nopk/test_1_2_exactly_once_nopk.py::test_target_identical_source_rows_both_survive` |
| the identity is a function of the envelope, asserted separately for streaming and snapshot rows | `test_1_2_exactly_once_nopk.py::test_target_event_identity_is_derived_not_random` |
| a replay of the same transaction recomputes identical ids and cannot duplicate, with the fence disabled | `test_1_2_keyless_identity.py::test_a_replay_recomputes_the_same_identity_and_cannot_duplicate` |
| several events sharing one LSN get distinct identities | `test_1_2_keyless_identity.py::test_identity_is_unique_for_distinct_events_sharing_one_lsn` |
| a missing or duplicated `total_order` is refused at the boundary | `test_1_2_keyless_identity.py`, `tests/test_assembler.py` |
| every keyless change event is one destination row, and `GROUP BY cdcf_event_id HAVING count(*) > 1` is empty after a crash at every anchor | `test_1_1_fault_matrix.py::test_no_duplicates_at_anchor` |
| duplicates are rejected by the destination, not just by us | `PRIMARY KEY (cdcf_event_id)`, verified on MotherDuck |

**The disagreement between the two reviews, and how it resolved.** Codex called
this a blocker and reproduced two accepted events colliding on `cdcf_event_id`;
Opus concluded the identity is structurally immune and signed 1.2 off. Both were
right about different halves: the identity *is* unique given valid connector
metadata, and the assembler *accepted metadata that was not valid*. The ordinal is
now a contract enforced where units are proven whole. Full write-up in
ADR §16/A18.

**Honest limitation (unchanged, ADR §15/A12).** A keyless destination table is a
**changelog**, not a current-state replica: an update or delete appends a change
event rather than mutating a row. 1.2 is scored as exactly-once *change delivery*,
which is what the rubric asks for; current state for keyless tables is 8.1/8.2's
work.

#### Baseline (Phase 0) — historical

Same mechanism as baseline 1.1, same score. `app.sensor_readings` has `REPLICA IDENTITY FULL`,
so Debezium *does* deliver complete before-images for updates and deletes
(`tests/test_e2e_duckdb.py` asserts `{"r": 4, "c": 6, "u": 4, "d": 2}`), but
there is no key to deduplicate on afterwards — a replayed batch is
indistinguishable from six genuinely identical readings.

**Gap to 5.** Same transactional-offset fix as 1.1, plus a synthetic identity for
keyless tables (`(dbz_lsn, dbz_tx_id, ordinal-within-transaction)` is unique and
comes free in the envelope). Note that `REPLICA IDENTITY FULL` is currently set
in `sql/01_schema.sql`; a real source may not have it, and Postgres refuses to
decode UPDATE/DELETE without it — that case is untested.

### 1.3 CDC changes should be atomic in MotherDuck — **5 / 5**

`no transactional boundaries=1, single-table transactional batches=3, multi-table=5`

#### Now (`feature/transactional-applier`, ADR 0001 rev 4)

**Multi-table transactional batches.** One destination transaction per *commit
group*, and a commit group holds an integral number of **whole** Postgres
transactions across every table they touch. "Whole" is a proof, not a heuristic:
`TransactionAssembler` emits a unit only when the Debezium `END` marker's
`event_count` equals the events counted for that transaction, the per-table
`data_collections` counts match in **both** directions, and the observed
`transaction.total_order` ordinals are exactly `1..event_count` - and the counters
that is checked against are maintained on arrival, so the proof is identical
whether the unit stayed in memory or spilled to disk.

**Evidence:**

| claim | where |
|---|---|
| an independent MotherDuck connection never observes a partial Postgres transaction, with a vacuity guard requiring it to have seen both `(0,0)` and `(N,N)` | `tests/1.3_atomic_batches/test_1_3_motherduck_atomicity.py` (motherduck) |
| the same across an injected crash between two table writes: the torn state was never visible and the recovery run put both tables there in ONE commit group | `tests/1.3_atomic_batches/test_1_3_motherduck_fault.py::test_a_torn_group_was_never_visible_in_motherduck` |
| a group spanning three tables is one transaction, and `commit_log` agrees | `test_1_3_commit_protocol.py::test_a_group_spanning_three_tables_is_one_destination_transaction` |
| the boundary rule is unconditional: missing `event_count`, spill-mode per-table counts, an undeclared observed table, a non-contiguous ordinal set are each fatal | `tests/test_assembler.py` (10 tests) |
| a transaction's events never straddle two commit groups, after a crash at every anchor | `test_1_1_fault_matrix.py::test_uncommitted_anchors_leave_nothing_behind` |
| `DROP` + `RENAME` is transactional at this destination, probed per run rather than assumed | `destination.probe_transactional_ddl`, ADR §15/A8 |

**What changed since the previous round.** Opus independently assessed 1.3 at 5
and could not break it; Codex capped it at 3 because unit completeness was **not
enforced in spill mode** and because no fault ever crashed between two table
writes. Both of those are now closed (ADR §16/A20, A25), which is why the stricter
reading also lands on 5.

#### Baseline (Phase 0) — historical

**Evidence.** Postgres transaction boundaries were never consulted. A Debezium
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

**How it was closed.** Exactly as anticipated, with one correction: a `dbz_tx_id`
change is **not** a usable boundary - it is a fatal consistency error, because the
only authoritative statement of where a transaction ends is the `END` marker
(ADR §3.2). The applier drives the destination connection directly;
`dlt.run()`-per-batch is gone.

**Architectural note.** MotherDuck sustains roughly 100 transactions/s, so
batches must span many PG transactions — the commit policy needs both a size and
a time trigger, and the offset write (1.1) has to ride in the same transaction.

**Pointers (baseline, deleted).** `src/cdc_flight/handler.py:93-126`,
`repos/dlt/dlt/destinations/insert_job_client.py`,
`repos/dlt/dlt/destinations/job_client_impl.py`.
**Pointers (current).** `src/cdc_flight/assembler.py`,
`src/cdc_flight/applier.py`, `src/cdc_flight/snapshot.py`,
`src/cdc_flight/spill.py`.

### 1.4 Primary-key update handled correctly — **5 / 5**

`error=1, duplication=2, correctly deleted and inserted or updated=5`

**Rescored 2026-07-31 after two independent reviews reproduced silent-loss paths.**
The previous 5 was not defensible: Codex scored this **1/5** and Opus **3/5**, both with
executable counterexamples. Five orderings were wrong, all of them one defect, and the
5 below is claimed only because every one of them is now a test that asserts
source/destination *equality*. What changed is recorded in ADR §18/A35–A37, and A31 —
the amendment that described the old fold — is marked **superseded**, including its
falsifier list, which pointed at a shape that worked while the shape that failed sat
next to it unnamed.

#### What the source actually emits (measured, not assumed)

A key-changing `UPDATE` never reaches us as an `u`. Whenever the old key is
available - which it is under `REPLICA IDENTITY DEFAULT` *and* `FULL`, because
pgoutput sends the old key when the key changes - Debezium splits it into
`d(old key)` + `c(new key)` inside the same transaction
(`RelationalChangeRecordEmitter.emitUpdateAsPrimaryKeyChangeRecord`, verified in the
vendored 3.6 source and observed end to end). Both events carry the same `txId`.

#### Why the atomic half needs no code

The pair is inside one `CompleteUnit`, a commit group holds an integral number of
*whole* transactions (1.3), and the merge deletes every key the group touched before
inserting the group's final row per key. So no consumer can see the row under both
keys or under neither.
`tests/1.4_pk_updates/test_1_4_pk_update_fold.py::test_the_delete_and_the_insert_cannot_be_split_across_commit_groups`
drives the shipped applier with `commit_max_events=1`, `commit_max_bytes=1`,
`commit_max_age=0` - a commit trigger on **every single event** - and shows the group
still cannot close between the two.

#### The fold: a key is not a row (the correction)

The old fold indexed the plan by key and asked the destination one question: *did this
key exist before this commit group?* Both halves were wrong. A key can be worn by
several rows at once inside a transaction (a **deferred** unique constraint), and a key
can be freed and re-taken across the transactions of one commit group — so no
group-level or even transaction-level question about a *key* can decide what a delete
removed. What decides it is which physical **row** the delete's before-image describes.

`table_work` therefore holds `live[key] = [entry, …]` where an entry is a row or
`START` (the row the destination already held), and each event is one physical
operation: `c`/`r` append, `u` replaces the entry its before-image identifies, `d`
removes it, `t` discards every entry *including* `START`. At group end a key holds at
most one row — the source enforces uniqueness at every transaction boundary, and
`end_transaction` asserts it per unit — and the three cases are `[row]` → delete the key
and insert, `[]` → delete the key, **`[START]` → leave it alone** (the destination's own
row survived; deleting it as a "touched key" was one of the measured losses, and not
rewriting it also keeps its original `cdcf_commit_id`).

The five orderings this fixes, each now a test asserting equality with Postgres:

| ordering | Postgres | old fold | reproduced by |
|---|---|---|---|
| T1 inserts key 2; T2 permutes `{1,2} -> {2,3}`; one group | `{2:a, 3:b}` | `{3:b}` — lost row | Codex 1 |
| one txn `d(1,a) c(3,a) d(2,b) c(3,b) d(3,a)` (two rows on key 3) | `{3:b}` | `{}` — lost row | Codex 1 |
| one txn `d(1,a) c(2,a) d(2,a) c(5,a)` (pre-group row `b` on key 2) | `{2:b, 5:a}` | `{2:a, 5:a}` — lost `b`, duplicated `a` | Opus B-2 |
| one txn `TRUNCATE; INSERT 5; DELETE 5` | `{}` | `{5}` — spurious row | Opus B-1 |
| T1 `TRUNCATE; INSERT 1`; T2 `DELETE 1`; one group | `{}` | `{1}` — zombie row | Codex 3 |

#### Where it cannot decide, it refuses

Two entries compete only under a deferred constraint, and **a deferrable primary key is
not a valid replica identity** (verified on the cluster: `relreplident='d'` but
`pg_index.indisreplident=false`, and `UPDATE` fails with *"does not have a replica
identity and publishes updates"* until `REPLICA IDENTITY FULL`). So wherever
attribution is needed the full before-image is present. Comparison against `START` runs
**at the destination**, with each value bound to the destination column's own type,
because comparing a Debezium JSON value to a value that has been through DuckDB's type
system in Python is not a comparison; Debezium's TOAST placeholder is excluded because
it distinguishes nothing.

Where the image cannot distinguish and only one row can really wear the key, the key
collapses to empty (right for `INSERT (5,…); DELETE WHERE id=5` under `REPLICA IDENTITY
DEFAULT`). Where two *concrete* rows compete and nothing can choose, `AmbiguousDelete`
**fails the commit group** — the rubric's own scale puts an error above silent loss, and
a rolled-back group replays for free. `test_an_unattributable_delete_fails_the_group_instead_of_folding_silently`
pins that, including that the destination still holds the pre-group state afterwards.

#### Evidence

* `tests/1.4_pk_updates/test_1_4_fold_counterexamples.py` - **14 tests, default suite**,
  the reproduced counterexamples plus the orderings both reviews verified as *correct*
  and which the rewrite must not break: 3-ring and 4-ring rotations, a swap through a
  temporary key, a delete matching two transiently identical rows, the ambiguous shape
  under **spill**, over **two tables**, and **re-folded with fresh LSNs** so the fence
  cannot help (a fold that is only correct once is not correct).
* `tests/1.4_pk_updates/test_1_4_pk_update_fold.py` - 20 tests: the plain move, mixed
  with other changes to the same row, the freed-key collision, the chain, the deferred
  permutation, both `u`-shaped variants, composite keys (`app.audit_log`), two
  transactions in one group, a spilled unit whose `d` is staged and whose `c` is in
  memory, and a fault at `begin` / `mid_apply` / `pre_commit` around the move.
* `tests/1.3_atomic_batches/test_1_3_rollback_resets_the_group.py` - **6 tests**: a
  rolled-back group must not be folded a second time (Opus M-1, measured to lose a row
  through exactly the ambiguous shape), must not contaminate the next group, and must
  not leave `_created_in_txn` behind — which independently makes `write()` skip the
  DELETE half of the merge.
* `tests/1.4_pk_updates/test_1_4_pk_update_e2e.py` - one 19 s scenario against real
  Postgres and real Debezium, **six** transactions: row-for-row agreement with the
  source, no duplicate key, the old key gone, the moved row keeping its post-move
  values (`replace.null.with.default=false` matters here), the deferred permutation,
  and `ok: true` on every run (`error=1` is ruled out by measurement, not by hope).
  T5 and T6 are the two hardest reproduced shapes driven through real Postgres rather
  than constructed records - a row moved onto a key another row holds and then off it,
  and two rows moved onto one key with one then deleted. Postgres ends at
  `[(2,'a'),(3,'b'),(11,'y'),(12,'x'),(30,'q')]` and the destination is asserted
  **equal to it**, because the defects this replaces produced destinations that were
  perfectly unique and wrong.
* `tests/1.4_pk_updates/test_1_4_pk_update_crash.py` (`slow`) - a `SIGKILL`-equivalent
  in the commit->ack window of the group carrying the PK update, then recovery: one
  row under the new key, no duplicate key anywhere, destination equals source.

#### Two Postgres facts the scenario had to discover

* A `DEFERRABLE` primary key is **not** a replica identity: `UPDATE` on such a
  published table fails with *"cannot update table … because it does not have a
  replica identity and publishes updates"*. The deferred-permutation collision is
  therefore only reachable with `REPLICA IDENTITY FULL` (or another non-deferrable
  unique index). The message key still comes from the primary key. **This is load-bearing
  for the fold**: it is why the disambiguating before-image is always available in the
  only configuration where ambiguity is reachable.
* `app.orders` references `app.customers (id)` with `ON DELETE CASCADE` and no
  `ON UPDATE`, so Postgres refuses a key update on a customer that has orders.

#### Falsifiers (what would drop this score)

* **A deferred-constraint transaction on a TOAST-heavy table.** If every non-key column
  of a before-image is `__debezium_unavailable_value` *and* two concrete rows compete
  for the key, nothing can attribute the delete and the group fails loudly. That is the
  designed outcome, not loss, but a run that fails is not a run that replicated. Rubric
  2.6 owns making TOAST images complete.
* **A destination whose type coercion disagrees with the bind.** `start_matches`
  compares at the destination with `IS NOT DISTINCT FROM` on the column's own type. If
  a destination coerced a bound value differently from the stored one, an attribution
  could match nothing and the group would fail (loudly, not silently). Verified on
  DuckDB and MotherDuck for the types the suite exercises; not proven for every type in
  2.4's list.
* Array/JSON **child tables**: this applier lands an array as one JSON column
  (asserted by `test_a_key_update_on_a_table_with_an_array_column_stays_one_table`).
  If rubric 2.4 introduces `<root>__tags` child tables, the key move must move the
  child rows too, and nothing tests that yet because nothing produces them.
* A keyless table has no key to update, so 1.4 does not apply to it; its
  current-state story is 8.1/8.2's (ADR §15/A12).

**Baseline (historical).** `probes/p01_dml_edge_cases.py` measured the change stream
as correct and the destination as wrong: `cdcflight_app_customers` held the original
`r` row for `id=1`, a delete marker for `id=1` *and* a `c` row for `id=9001`, because
the destination was append-only. That is the `duplication=2` this item scored.

---

### 1.5 TRUNCATE and DROP TABLE propagate — **4 / 5**

`silently ignored=1, logged=2, tombstones/soft delete=3, replicated faithfully=5`

**Rescored 2026-07-31, conservatively, and deliberately not restored to 5.** Codex
scored this **2/5** and Opus **4/5**. Every reproduced counterexample is closed (the
storage-mode divergence, the cross-transaction zombie, the two-truncate audit alias, the
stale-drop race, the missing ownership registry, the transactional alert), and the
guards both reviews asked for are built and tested. It stays at 4 because of one thing
neither review disputed and this branch cannot fix: **a dropped-and-recreated relation
cannot be re-snapshotted here**, so the destination table is dropped and the rows
inserted into the replacement before detection are gone. That is now *loud* rather than
silent — `table_state.snapshot_state='awaiting_snapshot'`, an alert, and a line in
`inspect` — but "the destination is incomplete and a human must trigger a backfill" is
not "replicated just like Postgres handles them". The 5 belongs to whoever lands
2.3/3.4's automatic re-snapshot.

#### TRUNCATE

pgoutput carries it and Postgres makes it transactional; the entire baseline gap was
**Debezium's own default**. `skipped.operations` defaults to `"t"`
(`CommonConnectorConfig.java:865-875`) and the pgoutput decoder then drops the `'T'`
message before decoding it (`PgOutputMessageDecoder.isTruncateEventsIncluded`), which
is why the baseline's `skipped` counter did not even increment.
`skipped.operations=none` is now set from the truncate policy, and
`CDC_TRUNCATE_MODE=ignore` restores the old default so the gap can be reproduced on
demand - which
`tests/1.5_truncate_drop/test_1_5_drop_recreate.py::test_ignore_mode_reproduces_the_baseline_gap`
does, live.

Three properties the events then need:

1. a truncate is a **counted** event: `EventDispatcher` sends it through the same
   `changeRecord` path, so `TransactionMonitor.dataEvent` counts it in
   `END.event_count`, it occupies a `transaction.total_order` ordinal and it gets a
   `data_collections` entry. It is fed through the assembler's data path for exactly
   that reason; anything else makes every truncating transaction fail the
   completeness rule (a hard error, not a silent one, but still wrong);
2. it carries **no message key** (`EventDispatcher.java:526` sends truncates with a
   null key schema), and reading that as "this table is keyless" would give a keyed
   table the keyless identity for the rest of the group - `TableWork.identified` is
   what prevents it. It must not clobber the cached identity either, or
   `assert_identity_is_unique` is silently disarmed for the rest of the run
   (Opus MINOR-3, fixed in `SchemaRegistry.ensure`);
3. the fold drops what the group planned *before* it and keeps what came *after* -
   and it must also record that the destination's **pre-group image is gone**, which is
   what the old fold never did. That omission is the whole of Opus BLOCKER-1: a
   post-truncate key reuse asked the destination "does this key exist?" and got `True`
   from a row the truncate had already logically removed, because the `DELETE FROM` that
   empties the table is issued much later, at write time.

**One dispatcher, in every storage mode.** There used to be two: in-memory events
carried the policy, the marker and the counters, while staged (spilled) events entered
*below* that layer and unconditionally emptied the table. Measured with
`unit_spill_events=1`: `truncate_mode=log` **emptied** the table, neither mode wrote a
`table_events` row, and both counters stayed at zero. `planner.GroupPlan` is now the
only entry point and does not know which representation an event arrived in;
`test_1_5_truncate_storage_modes.py` is a `{memory, spill} x {replicate, log}` matrix
over rows, marker, counters and `rows_removed`.

**The audit is positional.** `TRUNCATE; INSERT 1 row; TRUNCATE` reported `rows_removed=3`
on **both** markers, because each marker held a reference to the same mutable plan and
the field was read only after the final write. Each truncate now records the rows *it*
dropped, and the first one — the only one whose `DELETE FROM` reaches the destination's
own rows — adds that count: `3` then `1`.

The destination table is emptied with `DELETE FROM` **inside the commit group's
transaction** (unambiguously transactional on DuckDB and MotherDuck), so
`TRUNCATE a, b CASCADE` - one transaction, one event per relation - is one `COMMIT`,
and a rolled-back group leaves every row in place.

#### DROP TABLE

Not in the replication stream at all: pgoutput carries no DDL and the Postgres
connector has no DDL event source. `src/cdc_flight/catalog.py` polls the source
catalog on its own connection (default 10 s, `CDC_CATALOG_POLL_SECONDS`) for the two
facts logical decoding cannot give us - the relation `oid` and publication membership
- and reports four things: `dropped`, `recreated` (same name, new oid),
`unpublished` (**never** destructive: Postgres still holds those rows) and `new`
(rubric 2.3's hook, recorded only). It **observes and never decides**:
`src/cdc_flight/catalog_apply.py` owns the policy, because the observation and the DDL
are separated in time by the fence and that gap is where a stale fact becomes a wrong
drop.

**Six guards** (ADR §18/A38), in the order they run:

| guard | refuses | the failure it closes |
|---|---|---|
| the LSN fence | applying before the destination consumed everything before the DDL | a zombie re-created by an in-flight event |
| the zero-relations guard | acting on a poll that saw an empty schema | the wrong-database / mid-`pg_restore` signature |
| confirmation (`CDC_DROP_CONFIRM_POLLS=2`) | acting on a single observation | a transient catalog read mid-DDL |
| supersession | acting on an observation a later poll contradicted | dropping the destination table of a **live replacement relation** |
| revalidation | acting without re-reading the source, and acting when it cannot be read | the same, for a fence that opened long after detection |
| the circuit breaker (`CDC_DROP_MAX_PER_POLL=1`) | destroying more than one relation at once | `DROP SCHEMA … CASCADE`, a DSN repointed at an empty database, a failover target |

Revalidation fails **closed**: "I could not ask the source" is never read as "it is
gone". The breaker refuses the **whole** set, never the first N, and raises a `critical`
alert that survives a rollback. A `recreated` relation is dropped, alerted on, and
marked `awaiting_snapshot` in `table_state`, which `inspect` and the run summary print.

**Ownership.** "A replicated table absent from `pg_class` is always detected" was not
durably true: the watcher seeds itself from `_cdc_flight.table_state`, and that row was
written only by the snapshot coordinator — so a table first materialised by streaming
DML had none, and a `DROP` while the pipeline was down left an orphan destination table
no later poll could report. It is now written by whoever first creates the table, in the
same transaction. For the same reason `--reset-state` no longer DELETEs `table_state`
(it resets the snapshot fields in place): deleting it made that zombie **permanent**.

**The fence.** A drop is discovered after the fact, so applying it immediately could
delete rows an in-flight event would then re-add. Each *destructive* change carries the
`pg_current_wal_lsn()` of its poll and the applier holds it until the resume point it is
about to make durable reaches that LSN. On a quiet source nothing would ever advance it,
so `src/cdc_flight/source_marker.py` writes a **transactional**
`pg_logical_emit_message(true, 'cdcf_catalog_fence', …)` past the change — one component,
shared with D9's idle heartbeat (Opus Q3), owning the capability probe, the error state
and a **write budget** (`CDC_CATALOG_MARKER_MAX=60`, so a fence that never opens cannot
write one WAL record per poll for ever against a source we otherwise only read).

That the marker is transactional is measured, not stylistic. A non-transactional one
looks better (it stays out of every `END.event_count`) but does not end Debezium's
WAL-position search after a restart: `WalPositionLocator.resumeFromLsn` only stops on
a **COMMIT** past the stored LSN, and while it searches `skipMessage()` drops every
record - including the marker. MEASURED 2026-07-31: a quiet run whose only new WAL was
a non-transactional marker delivered `records=0`, sat 770 KB behind the source and
never applied the drop; with a transactional marker it applies in about a second. That
is a constraint on **D9 itself**, and ADR §9's heartbeat has been corrected to match.

**A quiet run is honest about it.** Deferring a destructive action whose fence has not
opened is the correct safety choice, and reporting `ok: true` while it is deferred is
not. A run now performs a **synchronous final catalog poll** before shutting down (the
watcher polls every 10 s while the idle window is 8 s, so a `DROP` otherwise could not
be seen until the next scheduled run), holds the engine open for
`CDC_CATALOG_DRAIN_SECONDS` so the change it just queued is fenced and applied by *this*
run, and **fails** with `stop_reason=catalog_unresolved` if a destructive change is
still pending at shutdown. Marker failures are preserved in `catalog_marker_error`
rather than cleared by the next successful poll.

#### What a truncate or a drop means for history

Faithful replication and audit history are different questions, and this splits them
rather than trading one off:

* the **current-state** table is emptied (or dropped), because that is what Postgres
  did - the rubric's 5;
* `_cdc_flight.table_events` records every table-level event (`truncate`, `dropped`,
  `recreated`, `unpublished`, `new`) with its commit id, source LSN, transaction id
  and the number of rows the destination lost, written **inside the same transaction
  as the data**, so the audit trail cannot describe an apply that rolled back. The
  count is asserted never to degrade to NULL for an applied truncate (Opus Q4) on
  DuckDB and on MotherDuck;
* destructive actions also raise an `_cdc_flight.alerts` row on an **independent
  connection** (`destination.AlertSink`, `con.cursor()`), which is what ADR §9.1 always
  claimed and the code did not do: it wrote on the applier's own connection inside the
  open transaction, so a destructive change that kept *failing* to apply produced no
  signal at all. Alerts are also classified — one that describes a refusal survives the
  rollback, one that describes an applied action must not;
* when 8.2 lands, a changelog table is append-only and is **never** emptied: it gains
  a truncate marker row derived from this same fact. That is the design decision, and
  it is why the marker carries the LSN and transaction id.

#### Policy switches (and why they exist)

| variable | default | meaning |
|---|---|---|
| `CDC_TRUNCATE_MODE` | `replicate` | `replicate` empties the destination (=5); `log` keeps the rows and records the marker (the rubric's "tombstone/soft delete" =3); `ignore` restores Debezium's skip |
| `CDC_DROP_MODE` | `replicate` | `replicate` drops the destination table (=5); `log` records the marker only; `ignore` disables catalog polling |
| `CDC_CATALOG_POLL_SECONDS` | `10` | poll interval (also 2.3's discovery interval) |
| `CDC_DROP_CONFIRM_POLLS` | `2` | consecutive polls that must agree before a destructive change is queued |
| `CDC_DROP_MAX_PER_POLL` | `1` | how many relations one commit group may destroy; the whole set is refused above it |
| `CDC_DROP_ALLOW_MASS` | `0` | authorise a mass drop (an operator who really is replicating a `DROP SCHEMA`) |
| `CDC_DROP_REVALIDATE` | `1` | re-read the relation immediately before destroying its destination table |
| `CDC_CATALOG_MARKER` | `1` | emit the WAL fence marker on the source (writes go to the PRIMARY, per 7.2/D9) |
| `CDC_CATALOG_MARKER_MAX` | `60` | cap on fence markers per run while a change stays unresolved |
| `CDC_CATALOG_DRAIN_SECONDS` | `30` | how long a quiet run holds the engine open for a change it just queued |
| `CDC_CATALOG_GRACE` | `0` | never apply a DDL action the fence has not cleared. **A non-zero value is excluded from the structural correctness guarantee** (ADR §18/A38) and the run logs that at start-up |

An unknown value for either mode is refused at start-up rather than logged, so a typo
cannot silently restore "truncates are skipped".

#### Evidence

* `tests/1.5_truncate_drop/test_1_5_truncate_key_reuse.py` - **10 tests, default
  suite**: both reproduced Opus BLOCKER-1 shapes (a spurious row that never healed; a
  row present under two keys with an **ordinary** primary key), Codex 3's
  cross-transaction zombie, the reverse orders, and the cross-transaction case under
  **spill** and over **two tables**.
* `tests/1.5_truncate_drop/test_1_5_truncate_storage_modes.py` - **15 tests**: the
  `{memory, spill} x {replicate, log}` matrix over rows / marker / counters /
  `rows_removed`, a lone spilled truncate, two truncates in one transaction reporting
  `3` then `1`, per-table counts for a multi-table truncate, a keyless-table truncate,
  and a fault at `spill` / `mid_apply` / `pre_commit` around a **staged** truncate
  (every row kept, no marker, the staging table empty, and the replay exact).
* `tests/1.5_truncate_drop/test_1_5_catalog_guards.py` - **18 tests**: one observation
  is not enough; a relation that reappears cancels its pending drop (and a different
  oid replaces it with a `recreated`); a relation that goes away cancels a pending
  recreate; a live relation is never dropped; a source that cannot be re-read fails
  closed; a watcher with no DSN refuses to query rather than falling back to libpq
  defaults; two drops in one group are both refused with a `critical` alert; a poll that
  saw an empty schema is discarded; **an alert about a refusal survives a rollback and
  an alert about an applied drop does not**.
* `tests/1.5_truncate_drop/test_1_5_ownership_and_honesty.py` - **10 tests**: streaming
  DML registers ownership in the same transaction that creates the table, a rolled-back
  group leaves neither, a watcher seeded from `table_state` detects a drop it never saw,
  `--reset-state` keeps ownership and drops only the oids, the final catalog poll
  happens, an unresolved destructive change fails the run, a marker failure is preserved
  in the summary, and the marker write budget is bounded.
* `tests/1.5_truncate_drop/test_1_5_truncate_fold.py` - 19 tests: multi-table
  atomicity, rows before/after the truncate, the keyless trap, the marker and its row
  count, `truncate_mode=log`, a truncate of a table the destination never held, a
  rolled-back truncate leaving every row *and* no marker, the LSN fence, `recreated`
  (with its `awaiting_snapshot` flag), `unpublished`, `drop_mode=log`, the alert, and a
  rolled-back drop staying pending.
* `tests/1.5_truncate_drop/test_1_5_catalog_detection.py` - 21 tests: the comparison in
  isolation, including the restart case (a replicated table absent from `pg_class` **is**
  a drop even with no persisted oid), that partitions are not discovery events, that
  `dirty` state is not forgotten until the caller commits, and that a marker failure
  leaves the change unapplied rather than forced.
* `tests/1.5_truncate_drop/test_1_5_truncate_drop_e2e.py` - one 33 s scenario against
  real Postgres: `TRUNCATE parent CASCADE` plus inserts in one transaction (both
  tables emptied in **one** commit group, the inserts surviving), a real `DROP TABLE`
  detected/fenced/applied with its `source_relations` and `table_state` rows gone, the
  audit trail, the rest of the stream unaffected, and the fence marker not breaking
  the assembler.
* `tests/1.5_truncate_drop/test_1_5_motherduck.py` (`motherduck`) - the truncate and the
  drop against real MotherDuck, that `DELETE FROM` reports its row count there, that
  the alert sink really is an independent connection on a server-side transaction
  implementation, that ownership survives — **and a second scenario** injecting
  `pre_commit` on a truncating group: every row kept, no marker, then a replay landing
  it exactly once (Codex's 9-point item 9).
* `tests/1.5_truncate_drop/test_1_5_drop_recreate.py` (`slow`) - the live gap under
  `ignore`, the same truncate replicated, a `SIGKILL`-equivalent in the commit->ack
  window of a truncating group, and drop-then-recreate with a **different schema**.

#### Falsifiers (what would drop this score — and what keeps it off 5)

* **No automatic re-snapshot for a recreated relation. This is why the score is 4.**
  The destination table is dropped because it holds a *different* relation's rows, and
  rows inserted into the replacement before detection are gone until someone backfills.
  It is loud now (`awaiting_snapshot`, an alert, `inspect` output) rather than silent,
  but faithful replication would rebuild it. Owned by **2.3 / 3.4**.
* **A mass drop needs a human by default.** `DROP SCHEMA app CASCADE` is *not*
  replicated automatically: the breaker refuses the set, alerts, and the run fails as
  `catalog_unresolved` until an operator sets `CDC_DROP_ALLOW_MASS=1`. That is a
  deliberate trade against "loss structurally impossible" and it is a divergence from
  "replicated just like Postgres handles them".
* **Detection latency is a poll interval plus a confirmation, not zero.** Between the
  `DROP` and the second agreeing poll the destination table still exists.
* **The fence needs a writable primary.** With `CDC_CATALOG_MARKER=0`, no permission, or
  a read-only replica, a drop on an otherwise idle source stays pending — and the run
  now **fails** rather than reporting success. 1.5 on a strictly read-only source is
  "detected, logged and reported as a failure", not "replicated".
* **`CDC_CATALOG_GRACE>0` is outside the guarantee.** It applies a destructive action
  before the fence that makes it safe, and can therefore create the zombie table the
  design says is impossible. Documented, warned at start-up, and never a default.
* **Attribution can fail loudly on a TOAST-heavy deferred transaction** (see 1.4's
  falsifiers): the truncate/drop path is unaffected, but the group that carries it fails.
* Partition DETACH/DROP is still 7.3's item: a detached partition leaves the
  publication as `unpublished` (non-destructive), which is not the same as replicating
  the detach.
* **Multi-schema capture is guarded, not supported.** A replicated name outside the
  polled schema is never reported as `dropped` (Opus MINOR-4), which is what stops 2.3/3.x
  from destroying those tables — but one poller still polls one schema.

**Baseline (historical).** `TRUNCATE TABLE app.orders` in `p01`: the destination still
showed `{'r': 5}`, `any_op_t == 0`, and the run's `skipped` counter was 0 - the event
never reached the handler. `DROP TABLE app.documents` in `p03`: no error, CDC kept
flowing, and `cdcflight_app_documents` kept its two rows forever.

---

### 1.6 Snapshot/backfill consistent with CDC — ~~3~~ **5 / 5**

`inconsistent=1, consistent=5`

#### Why this is provable rather than probable

The boundary between a snapshot image and the CDC stream is exact **because Postgres
makes it exact**, and only on one code path. Debezium's `CREATE_REPLICATION_SLOT`
returns a `consistent_point` and a `snapshot_name`; the snapshot transaction adopts the
latter with `SET TRANSACTION SNAPSHOT`; the streaming start LSN is the former. A
transaction is then visible in the image **iff** it committed before that LSN — an iff,
not an approximation.

MEASURED, and it is the load-bearing measurement of this item: Debezium takes that path
**only when it creates the slot itself**. With a pre-existing slot it uses an ordinary
isolation level and `pg_current_wal_lsn()` read after the snapshot transaction has begun;
`snapshot.mode=initial_only` creates no slot at all. Verified in the engine log against
Debezium 3.6 / Postgres 18.1, and recorded as ADR 0001 §19/A45 — every re-snapshot path
here therefore runs on a slot that does not yet exist.

#### Evidence

| claim | test |
|---|---|
| ~200 transactions committing throughout a snapshot land on exactly one side each — none missing, none duplicated, none delivered as both `r` and `c` | `test_1_6_snapshot_boundary.py` |
| the boundary is a queryable LSN, and every snapshot record carries exactly it | `test_1_6_snapshot_boundary.py::test_the_boundary_is_a_queryable_lsn` |
| a crash mid-snapshot leaves nothing partial visible, and the restart lands every row once (full content comparison) | `test_1_6_interrupted_snapshot.py` |
| a crash **between the DROP and the RENAME** of a swap leaves the old table intact | `test_1_6_interrupted_snapshot.py` (`swap:1`, slow) |
| a re-snapshot of a live table equals the source afterwards, deleted rows stay deleted, rows the stream never carried are picked up, later changes land on top, other tables are bit-identical | `test_1_6_resnapshot.py` (10 assertions) |
| the two independent readings of the consistent point agree | `test_1_6_resnapshot.py::test_the_resnapshot_actually_ran` |
| a recreated relation rebuilds itself automatically | `test_1_6_recreated_relation.py` |
| an undecidable fold rebuilds the table instead of looping for ever | `test_1_6_ambiguous_delete_self_heals.py` |

#### The hand-over, stated precisely

`table_state.snapshot_lsn` is a **per-table watermark**, and `GroupPlan` drops events for
table `T` from any unit whose **commit** LSN is below `T`'s watermark. Never on an event's
own LSN: a transaction still open when the snapshot was taken is in *no* image and some of
its events carry LSNs below the consistent point, so an event-level fence would lose
exactly the straddling transaction (ADR §19/A46).

**CDC during a re-snapshot**: there is none, by construction — the re-snapshot is blocking
and runs before the main engine starts, so nothing is in flight to buffer. Everything the
stream later delivers is either fenced (before `C`) or applied on top (at or after `C`).
Concurrent re-snapshot-while-streaming is rubric 3.3/3.4's, and it needs a durable
per-table buffer this does not build.

#### The cost, stated rather than hidden

A re-snapshot replaces **current state**. The change events of the fenced span are never
applied, so a changelog (rubric 8.2) is discontinuous there: an image at `C` instead of
the events that produced it. A `table_events` row with `event='resnapshot'` records where.
Current state is exact either way, which is what 1.6 asks about.

#### Baseline (historical)

120 005 preloaded rows, 300 rows inserted during the snapshot at ~30/s: all present
exactly once. What kept it at 3 was the *failure* path — Debezium restarts a snapshot from
scratch, and with an append-style destination the abandoned partial was already there — and
the absence of any re-snapshot at all.

### 1.7 Failures do not cause correctness issues — ~~3~~ **5 / 5**

`duplication possible due to crash=1, impossible but not well tested=3, robust fault injection=5`

#### The anchor set

Twelve anchors, in two mechanisms plus one that cannot live in the process at all:

| mechanism | anchors |
|---|---|
| `faults.maybe_crash` — the process dies at an exact protocol point | `decode`, `begin`, `spill`, `mid_apply`, `swap`, `pre_commit`, `post_commit_pre_ack`, `post_ack` |
| `faults.FaultyConnection` — the destination misbehaves | `destination_write`, `destination_commit` (ambiguous), `destination_hang`, `destination_close` |
| `tests/tcp_relay.py` — the source stops answering with the sockets left open | the network blackhole |

The destination anchors fire at the data group the **applier declares**
(`faults.arm_group`), not one the wrapper infers from the SQL it happens to see: an
inferred index is how a fault test goes vacuously green, which is the same defect class as
the `<nth>` counting bug (Opus M7).

#### What makes it *robust* rather than *plentiful*

`test_1_7_fault_matrix.py` parametrises over **`faults.ALL_POINTS` itself** and asserts
that every anchor has a declared outcome class. A new anchor added to `faults.py` without a
scenario fails the suite. Two permitted outcomes, and a third written down so its emptiness
is a statement:

* `RECOVERS` — the ledger is intact and a following run makes the destination equal the source;
* `LOUD` — non-zero exit with an accurate `last_run.json`;
* `SILENT` — must be empty, and is.

The ledger is the **source's own counts**, per keyed and keyless table, plus
`count(*) = count(DISTINCT cdcf_event_id)`. `test_1_7_chaos.py` composes the anchors: a
seeded random anchor at a random workload shape, 8 iterations, with that invariant checked
after each; `CDC_CHAOS_SEED` replays a failing sequence verbatim.

#### Three defects the injection found

1. **A hung `COMMIT` was unbounded** — neither of the two permitted outcomes. Nothing in
   DuckDB or the MotherDuck client imposes a deadline, so the run would hold the lease for
   ever. `CDC_COMMIT_TIMEOUT` (300 s) now aborts with `EX_TEMPFAIL`; safe *because* of
   Invariant O (ADR §4.6 F5).
2. **`last_run.json` named the wrong cause.** A destination write that failed
   mid-transaction was reported as `java.lang.InterruptedException`, because pydbzengine
   interrupts the engine thread when the handler raises and Debezium's completion message
   won. Ours is the root cause and is reported first now.
3. **Two fault tests were passing vacuously**, on an unrelated 3 s failure, until the
   assertions were strengthened to require the summary to *name* the injected fault. And
   `spill` could not fire at all at shipped defaults, so the matrix records the arming each
   anchor needs.

#### Baseline (historical)

`p13` case B `kill -9`'d the process mid-load and the restart left **2 048 duplicate rows**
(402 048 rows / 400 000 distinct). Duplication was measured behaviour, not a risk.

### 1.8 Externally-advanced slot detected → backfill — ~~1~~ **5 / 5**

`silent data loss=1, process exits=4, automatic backfill=5`

#### The check, on every acquisition

`reconcile.check_slot` is a **pure function** of (durable offset, one observation, the
previous observation), so every cell of its decision table is a unit test rather than a
base-backup restore. Six decisions trigger an automatic re-snapshot of every captured
table — "all captured tables, unless provable otherwise", and it is not provable
otherwise, because nothing in the destination records which relations the discarded WAL
touched:

| decision | what it catches | previously |
|---|---|---|
| `slot_ahead_of_destination` | somebody else consumed the slot | hard error (a 4) |
| `slot_missing` | the slot is gone; a new one starts at the CURRENT WAL position, so the gap is total and silent | **silent loss** |
| `slot_recreated` | `restart_lsn` went backwards, which a slot cannot do | undetectable |
| `source_identity_changed` | `pg_control_system().system_identifier` differs — a restore, a clone, a repointed DSN | undetectable |
| `source_lsn_regressed` | the source has written less WAL than we have consumed (MINOR-11 carry-forward) | fenced, never detected |
| `no_durable_destination_row` | destination empty, slot positioned | refused unless `snapshot.mode` happened to read data |

Detecting the middle three needs a memory, so `_cdc_flight.slot_state` records
`system_identifier`, `timeline_id`, `restart_lsn` and `confirmed_flush_lsn` at every
acquisition — **outside** any commit group, because it is an observation about the source
and recording it must never be able to fail a commit. Every check degrades to "cannot
compare" when the row is absent, so correctness never depends on it.

#### Evidence

Real operations on real slots, and each one is scored on a **whole-table content
comparison against the source afterwards**, including a keyless changelog table where a
duplicate cannot hide behind an upsert:

* `pg_replication_slot_advance(slot, pg_current_wal_lsn())` after 25 keyed + 25 keyless
  rows → detected, all six tables re-snapshotted, destination equals source, CDC works
  again, `critical` alert recorded (`test_1_8_slot_advanced.py`);
* `pg_drop_replication_slot` while the pipeline is down → detected, repaired, the rows the
  new slot would have skipped are back;
* a slot **behind** us → `ok`, no recovery, no re-snapshot: the safe direction has to stay
  quiet or the detector is a false-positive machine;
* a changed `system_identifier` and a rewound WAL position → detected and repaired
  (`test_1_8_restore_and_observation.py`);
* `CDC_RESNAPSHOT=0` → the rubric's 4, chosen deliberately rather than by accident.

#### The refusal that survives, and why

`orphan_offset_file` — an `offsets.dat` with no destination row — still **refuses to
start**. It is the one case where the automatic action could itself destroy data: the usual
cause is a DSN pointed at the wrong database, and a re-snapshot would `DROP` *that*
database's live tables and replace them with another source's contents.
`--accept-orphan-offsets` is how an operator says "yes, rebuild into this destination", and
it now also drops the slot and forces a data-reading `snapshot.mode`.

#### Baseline (historical)

`probes/p04_offset_mismatch.py`: 31 change events skipped, `{"records": 0, "stop_reason":
"idle", "returncode": 0}`. Root cause `offset.mismatch.strategy=NO_VALIDATION`, and on
Postgres 15+ the server silently starts from `confirmed_flush_lsn` rather than erroring.
The property is Technology Preview upstream and is **not** what closes this: the check is
ours, it runs before the engine starts, and it does not depend on Debezium noticing.

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

**Evidence (baseline; `handler.py` was deleted by ADR 0001 D1).** `src/cdc_flight/handler.py:62` — every resource is
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
