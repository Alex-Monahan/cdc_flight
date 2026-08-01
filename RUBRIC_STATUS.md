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

### The 1.6 / 1.7 / 1.8 review round (2026-07-31)

Two independent reviews attacked this branch's 1.5-1.8 and 4.6-4.7 claims. Both agreed
the narrow `BEGIN -> data + durable resume state -> COMMIT -> markProcessed/
markBatchFinished` protocol still preserves Invariant O and found no new commit path that
acknowledges data before durability. Both then found the **recovery layer around it** was
where loss now lived. Codex recorded 6 BLOCKER / 6 MAJOR / 3 MINOR and scored
1.5/1.6/1.7/1.8 at 2/1/1/1; Opus recorded 2 BLOCKER / 6 MAJOR / 9 MINOR and scored them
4/4/4/4. Every reproduced defect below is now a test that fails on the previous
implementation.

| finding | disposition |
|---|---|
| **Codex B1 = Opus BLOCKER-1** — re-snapshot completion meant "all tables seen so far"; an unreached table was classified empty and its live destination rows deleted; the `still_owed` guard was dead code | **fixed.** Completion is Debezium's own end-of-snapshot marker; "empty" needs three independent facts; `still_owed` is reachable and raises. `test_1_6_resnapshot_completion.py` (13 tests; the 10 that address the reported defect all fail on the old code) + `test_1_6_resnapshot_multi_table.py` (4 tables, keyless, empty, concurrent writer, crash-after-first-swap). ADR §19/A52 |
| **Codex B2** — the all-empty consistent point was a polled race; `_agree` took the `min()` and only logged | **fixed.** A verified-empty table is fenced at a WAL position sampled *before* the emptiness check, which cannot be ahead of the image; a disagreement between the two readings of `C` is fatal (both reviewers' Q1 answer). Both `_agree` branches tested |
| **Codex B3 = Opus MAJOR-1** — the recovery was not crash-recoverable; the documented order stranded a crash-between as a permanent `orphan_offset_file`; the forced snapshot mode did not survive a crash | **fixed.** `cdc_flight.recovery`: a durable journal written before any mutation, idempotent re-entrant phases, file deleted before the row, snapshot mode persisted. `test_1_8_recovery_state_machine.py` cuts at every phase boundary. ADR §19/A53, and A50's order claim is corrected |
| **Codex B4 = Opus MAJOR-2/Q4a** — a failed drop of the load-bearing slot was logged and stepped over, and the LSN baseline was cleared as if it had succeeded; failed re-snapshots leaked `_rs` slots | **fixed.** `RecoveryFailed` with the journal intact, in both the automatic path and `--accept-orphan-offsets`; `try/finally` around the whole engine section plus an unconditional start-up sweep; the shared cluster's leaked slots were dropped and the suite now sweeps stale slots at session start |
| **Opus BLOCKER-2** — `no_durable_destination_row` rebuilt a healthy populated destination; a safety regression against `main` | **fixed.** `check_slot` takes the destination's actual contents; a populated destination refuses with the orphan cell's own justification |
| **Codex B5** — the persisted timeline never participated in the decision | **fixed.** `source_timeline_changed`, ordered after identity and before LSN regression, with the catalog discarded (which also closes **Codex M1**) |
| **Codex B6** — the four giant modules regrew | **fixed, and re-fixed at Codex r1 MINOR-5.** The 1.6-1.8 round split `applier_config.py`, `self_heal.py`, `supervisor.py` and `control_schema.py` out; the 1.9 rounds put the growth back, so `OpenGroup` is now `commit_group.py` and the four pre-engine decisions are `acquisition.py`. Measured after round 3: `applier.py` 922, `pipeline.py` 782, `destination.py` 952, `reconcile.py` 611, `catalog.py` 829, `recovery.py` 609, `resnapshot.py` 860. `destination.py` and `resnapshot.py` are the ones to watch and are a **carry-forward** |
| **Codex M2 / M3, Opus MAJOR-5 / MAJOR-6, MINOR-1** — the fault matrix proved declared labels, not outcomes; four tautologies; the hung-commit test accepted any death; `hang_seconds` was the exit code; the chaos harness did not compose | **fixed.** Every anchor writes a fsynced `fault_fired.json`; the outcome class is derived from the run; exact exit codes; `CDC_FAULT_HANG_SECONDS`; `destination_commit_late` for genuine ambiguity; chaos injects during recovery over a shuffled cover with a per-iteration fired assertion. ADR §19/A54 |
| **Codex M5, Opus MAJOR-3** — the A51 counts were arithmetically false and the inventory incomplete | **fixed.** 54 rows, one failure and one class each, **34/12/8**, with `tests/4.7_self_healing/test_4_7_inventory.py` parsing the table so the headline cannot drift again. The missing modes (`CDC_RESNAPSHOT=0`, the unqueueable folds, drop-failure, `C` disagreement, the startup-dark fail-open, and more) are rows |
| **Codex M6, Opus MINOR-7** — `write_slot_state` was a non-atomic DELETE+INSERT | **fixed.** One transaction. "Typed record" is a **carry-forward**: it is still a dict |
| **Opus MAJOR-4** — `RUBRIC_STATUS` contradicted itself on 1.5 and 4.6 and had no 4.7 detail | **fixed.** Headings reconciled, 1.5's stale falsifier struck through with what replaced it, 4.6's "no test exists" replaced by what the test measures, 4.7 has a detail section |
| **Opus Q5, Codex m3** — 57 of 91 new tests and 10 of 12 fault anchors were slow-only | **fixed.** Every anchor and every reconciliation cell now has a default-suite guard; see "Suite partition" below |
| **Opus MINOR-4** — `slot_recreated` false-positives after an unclean Postgres restart | **fixed.** Both positions must regress, with the checkpoint-artefact case logged and tested |
| **Opus MINOR-6 / Q3** — the 1.0 sentinel was weakened to the bare word "publication" | **fixed.** Debezium's distinctive phrase, plus a non-zero-exit assertion; the stale README rows are corrected (**Codex m2**) |
| **Opus MINOR-5** — the 45 s dark-source bound was claimed, not measured | **fixed.** `stop_reason == "source_dark"` and a measured elapsed bound below the deadline |
| **Opus MINOR-8** — A47 needed `C > L` strictly | **fixed** in the ADR |
| **Codex m1** — startup-dark fail-open | **documented** as A51 row 50 with its rationale, and named in 4.6's detail. Closing it belongs with 4.4's heartbeat |
| **Opus MINOR-2** — `SILENT` and `RECOVERS` are both empty | **stated** in 1.7's detail rather than papered over |
| **Opus MINOR-9** — two pipelines sharing a dataset + `topic_prefix` | **carry-forward, 4.2.** Not touched here |
| **Opus MINOR-3** — `_agree` untested | **fixed** by the hard-fail plus both-branch tests |
| **Architecture review, finding 1** — `snapshot_state='in_progress'` is durable, non-terminal, and selected by no durable queue; its only recovery was an `except BaseException` that `os._exit`/`SIGKILL` skip, so the journal could report "recovery COMPLETE" over a half-built table | **fixed.** `promote_interrupted_snapshots()` at start-up, and the journal-clear test uses the same `SNAPSHOT_STATES_OWING_WORK` predicate as the queue |
| **Architecture review, finding 2** — ADR §4.8's declared `snapshot_state` domain omitted `awaiting_snapshot` and included a `failed` nothing writes; nothing validated a read | **fixed.** Frozen in `destination.SNAPSHOT_STATES`, validated by `read_snapshot_states()`, ADR corrected |
| **Architecture review, finding 3** — `recovery.PHASES` was declared and never enforced, so an unknown phase made `resume()` a silent no-op that logged ARMED | **fixed.** An unrecognised phase is `RecoveryFailed` |
| **Architecture review, finding 4** — `--reset-state` and `--accept-orphan-offsets` are unjournalled multi-step durable mutations, the same shape as B3 | **both journalled** (ADR §A58.1/§A58.2). The 1.9 round journalled only the orphan hatch and argued reset convergent; the review reproduced the counter-example (a positioned slot over a populated destination refuses with `no_durable_destination_row` before `will_snapshot_everything` is computed, and repeating the flag does not drop that slot), so reset is now `recovery.begin(decision='operator_reset')` like any other. Both routes' `--help` names every destructive surface, including the slot drop |
| **Architecture review, finding 5** — `_cdc_flight.heartbeat` is declared in ADR §4.8/D9.1 and never created | **fixed.** The table is created; the writer belongs to 4.4/6.1 and is still a carry-forward |
| **Codex M4** — the re-snapshot tests avoided the cases the proof depends on | **fixed** for multi-table, empty-with-concurrent-insert, keyless, straddling and the `C` disagreement; `CompleteUnit.commit_lsn` vs `last_lsn` is a **carry-forward** (the assembler constructs `last_lsn >= commit_lsn`, so no regression, but the alias should go) |

### Suite partition (2026-07-31, after the 1.6-1.8 review round)

Opus Q5 and Codex m3 both said the same thing about the *shape* of the suite rather than
its size: **57 of the 91 tests this branch added, and 10 of its 12 fault anchors, ran only
under `-m slow`.** A guard outside the gate that runs on every change is not a guard, and
"the default suite is at 8:21 of a 10-minute budget" was being spent on the wrong things.

The fix was not to move every crash/recovery cycle into the default lane — each costs
25-40 s and the budget is the whole reason the split exists. It was to put a guard for
each *behaviour class* in the default lane at the cheapest level that can actually fail:

| what moved IN (default) | why it is the right representative |
|---|---|
| `test_1_7_anchor_guards.py` (25 tests, **0.8 s**) | every one of the thirteen anchors, in-process: it parses, it fires where it says it fires, and it produces its own mechanism. The commit watchdog's exit 75 is proved in a subprocess because the thing under test is `os._exit` |
| `test_1_6_resnapshot_completion.py` (13 tests, **0.9 s**) | the completion semantics and both `C`-agreement branches, deterministically. The ten that address the reported defect all fail on the previous implementation |
| `test_1_8_recovery_state_machine.py` (12 tests, **0.9 s**) | a crash at **every** phase boundary of the acquisition recovery, plus the fatal slot-drop failure |
| `test_1_6_resnapshot_multi_table.py` (7 tests, **70 s**) | the one expensive addition, and the one the branch's worst defect needed: four tables at once, a keyless one, a genuinely empty one, a concurrent writer |
| `test_4_7_inventory.py` (6 tests, **0 s**) | the A51 counts, parsed rather than recalled |
| matrix `post_commit_pre_ack` (was `pre_commit`) | the at-least-once window is the most dangerous protocol anchor, so it is the one the default lane should hold |
| `destination_commit_late` | the genuinely ambiguous commit — the only anchor that exercises recovery from a *durable* group the offset store never heard about |

| what moved OUT (slow) | why nothing is lost |
|---|---|
| `test_1_6_resnapshot.py` (10 tests, ~38 s) | the single-table original. The multi-table module covers a strict superset of its claims — keyless, empty, concurrent writer, hand-over — more cheaply per claim |
| `test_1_5_truncate_drop_e2e.py` (12 tests, ~34 s) | 1.5 keeps **98** deterministic tests in the default lane; what moved is the three-run end-to-end scenario the unit tests already pin |
| matrix `pre_commit`, `destination_commit` | superseded in the default lane by the two representatives above; both still run under `-m slow` |

Nothing was deleted. Every anchor and every `check_slot` decision cell now has at least
one default-suite guard:

| coverage | default-suite guard |
|---|---|
| 8 protocol anchors | `test_1_1_fault_matrix.py` fires six end to end; `test_1_7_anchor_guards.py` covers `decode` and `swap` in-process |
| 5 destination anchors | `test_1_7_anchor_guards.py` fires all five against a real connection wrapper; two also run end to end |
| the network blackhole | the unit half of `test_1_7_source_blackhole.py` (the real relay stays slow — it costs 106 s) |
| 9 `check_slot` cells | `test_1_8_decision_table.py`, 25 pure-function tests, plus the advanced-slot recovery end to end |
| 4 recovery phases | `test_1_8_recovery_state_machine.py` |

**Measured: 441 passed in 8:46** (was 384 in 8:21). Fifty-seven more tests, every anchor
guarded, twenty-five seconds more, still inside the 10-minute budget. `make test-slow` is
78 passed in 17:45 and `make test-md` is 22 passed in 4:50, both green. The single largest
remaining default cost is `test_1_1_fault_matrix.py`'s module fixture at 75 s — six real
crash/recovery cycles — and it stays, because it is the exactly-once evidence for 1.1 and
1.2 and it is deterministic protocol coverage rather than an environmental scenario.

Two structural notes for whoever takes the suite-optimisation task: `dest_fault_box` and
`matrix_box` are module-scoped fixtures shared across the default/slow boundary, so
`-m "not slow"` still pays their full baseline run to execute two or three cases each;
and every e2e module serialises on the session `flock`, so partitioning across processes
buys nothing until per-worker databases land.

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
| 1.5 | TRUNCATE / DROP propagate | ~~1~~ → **5** | `skipped.operations=none` brings truncates through; **one** dispatcher applies them in every storage mode and each truncate's audit records what *it* removed. `DROP TABLE` is not in the stream, so the source catalog is polled and the action passes six guards before any DDL. A dropped-and-recreated relation is dropped, marked `awaiting_snapshot` and **re-snapshotted automatically on the next run**. Held at 4 through the 1.6-1.8 review because the rebuild machinery could delete a live destination table it had merely not reached; that is closed with positive-evidence emptiness and multi-table proof (ADR §19/A52). A mass drop still needs an operator, deliberately. |
| 1.6 | Snapshot/backfill consistent with CDC | ~~3~~ → **5** | Postgres's **exported snapshot** makes the boundary an iff, and the fence is on the transaction's **commit** LSN, so a transaction straddling `C` is applied in full rather than lost. Proven with ~200 transactions committing throughout a snapshot — every row on exactly one side. A re-snapshot is complete only when **every requested table** reaches a terminal state: swapped, or verified empty on three independent facts (Debezium's own end-of-snapshot marker, zero records for that table, and a source count of zero). A disagreement between the two readings of `C` is fatal. Proven against a four-table re-snapshot with a keyless table, a genuinely empty table and a concurrent writer. |
| 1.7 | Failures do not cause correctness issues | ~~1~~ ~~4~~ → **5** | **Eighteen** anchors: eight protocol, five destination (including `destination_commit_late`, the genuinely ambiguous *committed-then-lost-the-answer* case), **five at the acquisition recovery's durable boundaries**, plus a real **network** blackhole injected from outside the process. The matrix is enumerated **from `faults.ALL_POINTS`**, every anchor writes a machine-readable fired record so the outcome class is *derived* from the run rather than read back out of the table, and the chaos harness injects **during recovery** over a shuffled cover of the anchor set. The 4 was an honest hold on one stated gap — the recovery's crash cuts were proven through a test seam rather than an injected `kill -9` — and that gap is closed with a default-lane guard per anchor (0.3 s) and a slow-lane run that kills a real process at `recovery_armed` against a real slot and then compares the whole destination against the whole source. |
| 1.8 | Externally-advanced slot detected → backfill | ~~1~~ → **5** | Checked on every slot acquisition. Seven decisions trigger an **automatic** re-snapshot: slot ahead, slot missing, slot recreated, source identity changed, **source timeline forked**, source WAL rewound, and an empty destination with a positioned slot. The recovery is a **journalled state machine**: the intent is durable before any mutation, every step is idempotent and re-entrant after a crash at any phase, and a slot that will not drop fails the recovery rather than being logged and stepped over. A **populated** destination with no resume point refuses instead of being rebuilt. Proven by comparing the whole destination against the whole source after a real `pg_replication_slot_advance` and a real `pg_drop_replication_slot`, and by cutting the recovery at every phase boundary. |
| 1.9 | Consistency-affecting state managed with state machines | **5** (new item) | Four machines and one precedence, each owning exactly one state, declared in one readable file (`cdc_flight/machines.py`) and enforced by a ~230-line `states.py` with no dependencies: `table_lifecycle` (durable, `table_state.snapshot_state`, now with a **single writer** the suite greps for), `run_phase` (durable, `_cdc_flight.heartbeat`, the writer ADR §4.8 has specified since rev 1 and never had), `run_outcome` (a **precedence**, so A49's cause-before-symptom rule stops being two copies of a literal tuple), `acquisition_recovery` (durable, edge-checked on top of rev 8's domain check) and `catalog_change` (memory only). Seven further candidates are declined **with the argument written down** — the commit group stays memory-only because a durable machine there would weaken Invariant O, and its sixteen hand-reset fields become one `OpenGroup` instead, which makes Opus MAJOR-1's partial reset unrepresentable. Four measured past bugs are now edges that do not exist. |
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
| 4.7 | Self-heal without human intervention | **1** (new item; claimed 3, rescored 1) | The rubric's 1-band is a **count**: "more than 2 cases that cause manual human intervention". The corrected inventory (ADR §19/A51, 54 rows, parsed by `tests/4.7_self_healing/test_4_7_inventory.py`) is **34 AUTO / 12 MANUAL / 8 UNDEFINED**. Twelve is more than two, so it is a 1 under every defensible reading. The direction is right — eight previously-permanent failures became automatic this round, including the Flight's own half-finished recovery — and enumerating them properly *found four more manual cases*. See the detail section. |
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

**Current average (this branch): 99 / 42 = 2.36 out of 5.** Items at 5: **10 of 42**
(1.1-1.9 and 7.1) — **all of §1 is now at 5**. Distribution: 22 at 1, 3 at 2, 7 at 3,
0 at 4, 10 at 5. Distance to target: **111 rubric points**.

**Correction (2026-07-31).** This paragraph previously read `100 / 41 = 2.44` with a
distribution of `21/3/8/0/9`, and neither matched the summary table above it — the block
was not updated when 1.7 fell from 5 to 4 and 4.7 from 3 to 1 in the previous round. The
numbers here are now **parsed from the table** by
`tests/4.7_self_healing/test_4_7_inventory.py`'s sibling reasoning: read the Score column,
take the last bolded digit, sum. It is the same class of defect as MAJOR-3 (a headline
drifting from the rows it summarises) and it is recorded rather than quietly fixed.

The denominator is 42, not 40: the user added rubric **4.7** ("the Flight should always be
able to self-heal without human intervention") and rubric **1.9** ("any state that can
affect consistency is managed with a state machine approach") on 2026-07-31.

The delta over the baseline is +33 points across eleven items: 1.1 (3 -> 5), 1.2 (3 -> 5),
1.3 (1 -> 5), 1.4 (2 -> 5), 1.5 (1 -> 5), 1.6 (3 -> 5), 1.7 (1 -> 5), 1.8 (1 -> 5),
4.6 (1 -> 3), plus 4.7 as a new item scored **1** and 1.9 as a new item scored **5**.
Every other row is still the baseline score, and the detail sections say so.

**Rescored 2026-07-31 after the 1.6-1.8 review round**, in both directions and
conservatively. 1.5 rose from 4 to 5 (its stated condition is met and the machinery it
rests on is now safe); 1.7 fell from a claimed 5 to 4 (the acquisition recovery has no
fault anchors); 4.7 fell from a claimed 3 to 1 (the band is a count, and the corrected
count is twelve). Where the two reviewers disagreed, the lower reading was taken unless
the specific defect each of them named is closed and tested — which is why 1.6 and 1.8
are 5 despite Codex scoring them 1: every reason given is reproduced, fixed, and guarded
by a test that fails on the previous implementation.

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
4.7 is **1**, rescored down from a claimed 3 after both reviewers pointed at the same
thing: the band is a literal count of manual-intervention cases, the corrected
inventory has twelve of them, and twelve is more than two. Scoring the remainder after
setting the manual rows aside as "exceptions" is scoring a hand-selected subset.
**1.7 goes from 4 to 5** (2026-07-31, with 1.9). It was held at 4 with one stated
condition — the acquisition recovery had no fault anchors of its own, so its crash cuts
rested on a test seam rather than an injected `kill -9` — and that condition is met: five
anchors at the recovery's durable boundaries, each with a declared outcome in the
enumerated matrix, a default-lane guard, and a slow-lane run that kills a real process at
`recovery_armed` against a real Postgres slot and then compares the whole destination
against the whole source. **1.9 is claimed at 5** on the rubric's own text (*an
appropriate number of state machines, over 1*): four machines plus one precedence, each
owning one state, with the seven declined candidates argued individually rather than
omitted. 4.6 is 3, not 5, because
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

### 1.5 TRUNCATE and DROP TABLE propagate — ~~1~~ **5 / 5**

`silently ignored=1, logged=2, tombstones/soft delete=3, replicated faithfully=5`

**Rescored 2026-07-31 to 5, after the 1.6-1.8 review round closed the last defect.**
The history is worth keeping because the score moved twice and both moves were
conditional:

* the 1.4/1.5 round scored it **4**, with one stated condition — "no automatic
  re-snapshot for a recreated relation" — and that condition was met by
  `cdc_flight.resnapshot` (`tests/1.6_snapshot_consistency/test_1_6_recreated_relation.py`);
* the 1.6-1.8 round then found that the *machinery* the new 5 rested on could delete a
  live destination table: `_finish_empty_tables` inferred "the source relation is empty"
  from "our engine did not reach this table", and the guard meant to catch a partial
  re-snapshot was dead code (Codex B1 scored this **2/5**; Opus BLOCKER-1 held it at
  **4/5** for the same reason). Both reviewers named that one defect and nothing else;
* it is now closed with positive evidence on three independent facts and proven against
  a real multi-table re-snapshot including a genuinely empty table (ADR §19/A52,
  `test_1_6_resnapshot_completion.py`, `test_1_6_resnapshot_multi_table.py`).

What keeps the 5 honest rather than absolute is written in the falsifiers below: a mass
drop still needs an operator, deliberately.

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

* ~~**No automatic re-snapshot for a recreated relation. This is why the score is 4.**~~
  **Closed 2026-07-31.** A recreated relation is dropped, marked `awaiting_snapshot` and
  rebuilt automatically on the next run through `cdc_flight.resnapshot`, proven end to
  end against a relation recreated with rows that produce no change events at all
  (`test_1_6_recreated_relation.py`). The stale text that still argued for a 4 lived here
  and in the section heading while the summary table said 5 — the file's own preamble
  calls that a documentation merge blocker, and it was one (Opus MAJOR-4).
* **The rebuild machinery must never classify an unreached table as empty.** This is the
  falsifier that replaced the one above, because it is where the risk actually is: the
  re-snapshot may only empty a destination table on positive evidence (end-of-snapshot
  marker + zero records for that table + a source count of zero), and a table it did not
  reach must stay `awaiting_snapshot` with its destination untouched. Guarded in the
  default suite by `test_1_6_resnapshot_completion.py` and end to end by
  `test_1_6_resnapshot_multi_table.py`.
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

#### What the 1.6-1.8 review round found here, and what closed it

Both reviewers reproduced the same defect from different directions and it is the reason
this item was claimed at 5 while the machinery behind it could destroy a live table
(Codex B1 scored 1.6 **1/5**; Opus BLOCKER-1 held it at **4/5**).

* **Completion meant "no table is currently mid-snapshot".** Debezium closes a table's
  snapshot chunk when a record for the *next* table arrives, so at a batch boundary in
  that gap both halves of the old condition are true and the next table has not been
  scanned. Every unreached table was then classified empty and `DELETE FROM` ran against
  its live destination table, with an audit row asserting an emptiness nothing checked.
  The `still_owed` guard was provably dead code. Completion is now Debezium's own
  end-of-snapshot marker, "empty" needs three independent facts, and `still_owed` is
  reachable and tested.
* **The all-empty consistent point was a race.** With no snapshot records there was no
  `source.lsn`, so `C` came from the first polled `confirmed_flush_lsn` of the throwaway
  slot — which for an all-empty capture set can be sampled *after* the engine has entered
  streaming and advanced it. A verified-empty table is now fenced at a WAL position we
  sample ourselves, immediately before the emptiness check, which cannot be ahead of the
  image.
* **A disagreement between the two readings of `C` took the `min()`.** That knowingly
  duplicates a keyless table, which breaks rubric 1.2's exactly-once claim: it did not
  avoid a correctness violation, it chose a different one. It is now fatal, the tables
  stay owed, and the next run takes a fresh `C`.

Evidence: `test_1_6_resnapshot_completion.py` (default suite, deterministic, no engine —
the ten that predate the empty-capture-set case all fail on the previous implementation) and
`test_1_6_resnapshot_multi_table.py` (four tables at once including a keyless one and a
genuinely empty one, a concurrent writer throughout, and a `swap`-anchor crash after the
first table in the slow lane). Full reasoning in ADR §19/A52.

#### Baseline (historical)

120 005 preloaded rows, 300 rows inserted during the snapshot at ~30/s: all present
exactly once. What kept it at 3 was the *failure* path — Debezium restarts a snapshot from
scratch, and with an append-style destination the abandoned partial was already there — and
the absence of any re-snapshot at all.

### 1.7 Failures do not cause correctness issues — ~~1~~ ~~4~~ **5 / 5**

`duplication possible due to crash=1, impossible but not well tested=3, robust fault injection=5`

#### The anchor set

**Rescored 2026-07-31 from 4 to 5, after the one stated gap was closed** (rubric 1.9's
task). The history: Codex scored this **1/5** and Opus **4/5** in the 1.6-1.8 round; all
of that is closed. It was then held at **4** by this project rather than by a reviewer,
for one reason written down in advance — the acquisition recovery had no fault anchors of
its own — and that is what changed.

**Eighteen anchors**, in three mechanisms plus one that cannot live in the process at all:

| mechanism | anchors |
|---|---|
| `faults.maybe_crash` — the process dies at an exact protocol point | `decode`, `begin`, `spill`, `mid_apply`, `swap`, `pre_commit`, `post_commit_pre_ack`, `post_ack` |
| `faults.FaultyConnection` — the destination misbehaves | `destination_write`, `destination_commit`, `destination_commit_late` (the genuinely ambiguous one), `destination_hang`, `destination_close` |
| `faults.maybe_crash` at a **durable recovery boundary** (new) | `recovery_requested`, `recovery_offsets_file_deleted`, `recovery_resume_point_deleted`, `recovery_armed`, `table_rebuild_queued` |
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

#### What the 1.6-1.8 review round found here, and what closed it

The item whose whole premise is "not vacuously green" had four more assertions that could
not fail, and the mechanism for proving *which* fault ended a run did not exist
(ADR §19/A54):

* `test_no_anchor_is_allowed_to_be_silent()` asserted that a hand-written dictionary
  contained no `SILENT` string. It could only fail if somebody edited the dictionary — and
  it was the test the "the SILENT bucket is empty" claim pointed at. The outcome class is
  now **derived from the run**: an armed anchor that left no fired record is `SILENT` and
  fails the test, whatever the exit code said.
* the matrix accepted any non-zero exit without establishing that the *selected* fault had
  fired, so a run that died of an unrelated start-up problem passed.
* `test_a_hung_commit_...` accepted `returncode in (75, -9, 137, 1)`: 75 is the commit
  watchdog's own `EX_TEMPFAIL` and the entire point of the test, and the other three are
  "something killed it". It now requires exactly 75, and the hang duration is longer than
  the watchdog so nothing else can have ended the run.
* `destination_hang`'s duration was `<action>` reinterpreted as seconds, so
  `destination_hang:1` hung for **137** seconds — undocumented, and *shorter* than the
  shipped 300 s watchdog it exists to exercise. It is now `CDC_FAULT_HANG_SECONDS`.
* `destination_commit` raised *before* `COMMIT` ran, which is an ordinary uncommitted
  failure wearing an ambiguous name. `destination_commit_late` executes the `COMMIT` and
  then raises: the destination committed and we cannot know it.
* the chaos harness gave its recovery runs **no** fault environment, so a fault could
  never fire during another fault's recovery — the composition its own docstring claimed.
  It now injects during recovery, plans a shuffled **cover** of the anchor set, asserts
  every iteration's anchor actually fired, and runs more than one seed.

Every anchor now writes `$CDC_STATE_DIR/fault_fired.json` before it does anything else,
fsynced, so even `os._exit` leaves the evidence, and every fault assertion rests on it.

#### What closed the 4 (2026-07-31, with rubric 1.9)

The hold was stated precisely, so the closure can be checked against it: *the acquisition
recovery's crash cuts are proven through a **test seam** (`recovery.resume(on_phase=...)`
raising a Python exception), not through an injected fault, and there is no end-to-end
`kill -9` in the middle of the recovery against a real Postgres.*

Why that mattered rather than being pedantry: a raised exception unwinds `finally` blocks,
closes the destination connection and flushes the JVM. `os._exit` does none of that, and
the claim under test is precisely that **durable state alone** is enough to resume. The
gap was not theoretical — the architecture review's finding 1 was an
`except BaseException` handler that `os._exit` steps straight over, with a measured
consequence (a table owed work and selected by no queue).

Five anchors now sit at the boundaries, one per durable mutation plus one inside the
queue write (ADR §20/A57):

| anchor | fires | what must survive |
|---|---|---|
| `recovery_requested` | after the journal row and the to-do list commit | the journal at `requested`; nothing destroyed |
| `recovery_offsets_file_deleted` | after `offsets.dat` is unlinked, before the phase is recorded | `file absent / row present` → reconciliation rebuilds it |
| `recovery_resume_point_deleted` | after the resume point is deleted, before the phase is recorded | the slot survives; the next run re-runs the drop |
| `recovery_armed` | after the slot is dropped, before the phase is recorded | the forced `snapshot.mode`, which used to live only in a local variable (Codex B3) |
| `table_rebuild_queued` | inside `begin()`'s transaction, mid-write of the to-do list | neither the journal nor the marking — they are one transaction |

Each meets the same three requirements the rest of the matrix does, which is what makes
this a closure rather than a claim:

1. **a declared outcome** — `test_1_7_fault_matrix.py` enumerates `faults.ALL_POINTS` and
   fails on an anchor with no entry. All five are declared `LOUD`. They are excluded from
   the *generic* scenario (they need a slot the check declares unusable, the way `swap`
   needs a shadow), and a new test asserts that every excluded anchor names the module
   that does prove it **and that the module exists** — a comment would not have failed if
   the file were renamed;
2. **a default-suite guard** — `tests/1.7_fault_injection/test_1_7_recovery_anchors.py`,
   13 tests in **0.3 s**: each anchor parses, fires where it says it fires, writes its
   fsynced record, and leaves a journal the next attempt finishes from. `<nth>` is
   per-boundary-arrival, and a test drives a second recovery in one process to prove the
   index addresses it;
3. **an exact-count recovery proof** — `tests/1.8_slot_mismatch/test_1_8_recovery_crash_e2e.py`
   (slow lane) advances a **real** slot, kills a **real** `cdc-flight` process at
   `recovery_armed` with `os._exit`, and then asserts the fired record names that anchor,
   the next run resumed *from `resume_point_deleted`*, the journal was cleared, no table is
   left owing work, and the destination equals the source **exactly** on both a keyed and
   a keyless table.

#### Why 5 and what is still true against it

The rubric's 5 is "robust injection of failures in testing". The claim is that the anchor
set is now **enumerated from the code and complete over the durable state machines**: the
matrix derives itself from `ALL_POINTS`, the `SILENT` bucket is empty *by derivation from
the run* rather than by spelling, and every durable multi-step sequence in the tree — the
commit protocol, the destination, the network, and now the acquisition recovery — has an
anchor at each of its boundaries.

Two residual honesty notes: `RECOVERS` and `SILENT` are both empty in practice — every
injected fault ends in a non-zero exit — so "two permitted outcome classes" is really one
class plus two that must stay empty; and `swap` is exercised end to end only in the slow
lane, with a deterministic default-suite guard standing in for it
(`test_1_7_anchor_guards.py`).

#### What the first Codex review rejected here, and what round 2 changed

The first submission was told to **retain the interim 4/5 hold**, on four specific
grounds. Each is now closed by a test rather than by an argument (ADR §A58.7):

| finding | what it was | what it is now |
|---|---|---|
| the four cuts used **exception unwinding**, not hard death | `test_1_7_recovery_anchors.py` armed `:raise`, so `recovery_requested`, offsets-file-deleted and resume-row-deleted were proven by a Python exception that unwinds `finally`, closes the connection and lets the interpreter tidy up. Only `recovery_armed` had an `os._exit` pairing, in the slow lane | all four are cut by a real `os._exit` in a **child process** (`tests/recovery_crash_driver.py`) against the same DuckDB file and the same offsets file — milliseconds, no JVM, no Postgres, and the fired record's `pid` is asserted not to be the test runner's. The `:raise` variants remain as the *error-teardown* lifecycle, which is a different path (§1.2) |
| `table_rebuild_queued` fired **before the first table write** despite being documented "mid-write" | it proved a pre-write rollback | it fires after the FIRST captured table has taken its `-> awaiting_snapshot` edge and before the second has, so the queue really is torn when the process dies |
| the composed chaos fault was **allowed not to fire** | armed at `<nth>=2` so the random walk terminates, and the shuffled-cover assertion could be satisfied entirely by the first faults — so "the anchors compose" rested on nothing that had to happen | the seeded harness keeps its terminating draw, and a **bounded** scenario now requires one: a hard death at `post_commit_pre_ack`, then `pre_commit:1` during the recovery run, which the replay of the un-acknowledged group necessarily reaches |
| the two **operator routes** had no anchors at all | `--accept-orphan-offsets` and `--reset-state` were durable multi-step sequences absent from the enumeration, so `ALL_POINTS` could not prove completeness | both are journalled recoveries and therefore reach the same five anchors. `test_1_8_operator_route_crash_e2e.py` kills a real process mid-sequence on each, restarts **without repeating the flag** under `--snapshot-mode no_data`, and asserts exact source/destination equality. `no_data` is load-bearing: a run that forgot the obligation would stream instead of rebuild |

And the nondeterministic assertion the review asked to remove is removed:
`close_hung is True` was a race the test happened to win most of the time — the repository
had already recorded it flaking while every correctness assertion passed. What the
blackhole test asserts now is the contract: non-zero exit, `source_dark` **by name**, a
measured detection bound, and — if a hang is reported at all — that the symptom did not
replace the diagnosis, which is the only thing A49 needs from it.

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

#### What the 1.6-1.8 review round found here, and what closed it

The *detection* survived both reviews intact. The **recovery** did not (Codex B3/B4
scored 1.8 **1/5**; Opus BLOCKER-2 + MAJOR-1 held it at **4/5**):

* **One cell rebuilt a healthy populated destination.** `no_durable_destination_row` is
  documented — here and in the ADR — as "destination *empty*, slot positioned", and the
  code tested only that the control row was missing. A state directory pointed at an
  existing warehouse silently dropped and rebuilt every captured table. That was a safety
  **regression** against `main`, where the same cell refused. It refuses again, on the
  destination's actual contents.
* **The recovery could strand itself for ever.** Four independent durable mutations, no
  journal: a crash between deleting the resume row and deleting `offsets.dat` left the
  exact state the Flight diagnoses as `orphan_offset_file`, and it then refused to start
  across three consecutive restarts until a human passed a CLI flag. A crash after the
  slot drop lost the forced `snapshot.mode` entirely. There is now a durable journal
  written **before** any mutation, every step is idempotent, and the file goes before the
  row.
* **A failed slot drop was logged and stepped over.** A45 is the reason it may not be: a
  re-snapshot against a surviving slot resumes the stream past the snapshot's consistent
  point. It now raises with the journal intact, and so does `--accept-orphan-offsets`.
* **The persisted timeline never participated in the decision.** A promotion or a PITR
  keeps the `system_identifier` and forks the timeline, and Postgres reuses WAL positions
  across a fork, so every scalar comparison could look healthy. `source_timeline_changed`
  is now a decision, and a fork or a rewind discards the recorded catalog as well.

Evidence: `test_1_8_recovery_state_machine.py` cuts at **every** phase boundary and proves
the next attempt finishes the job, `test_1_8_decision_table.py` covers the populated-
destination refusal and the forked timeline, and the end-to-end `pg_replication_slot_advance`
scenario now also asserts the journal was cleared. Full reasoning in ADR §19/A53.

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


### 1.9 Consistency-affecting state managed with state machines — **5 / 5**

`no state machines=1, only 1 big state machine=3, an appropriate number (over 1)=5`

*(Added to the rubric by the user on 2026-07-31. Scored here for the first time.)*

#### Why this item exists, in this codebase's own evidence

Every named regression this project has recorded is the same shape: **a state that exists
in the design, is represented as a derived expression over two or more variables, and is
therefore mutated by a path the design did not enumerate.**

| finding | the state | how it was represented | what it cost |
|---|---|---|---|
| Opus MAJOR-1 (1.4/1.5) | "this group is being committed" | 16 fields + an implicit success path | `_reset_group()` on success only ⇒ a rolled-back group folded twice ⇒ **a measured lost row** |
| Opus B5 / A49 | "why the run stopped" | a last-writer-wins string, precedence in two copies of a tuple | a blackholed Postgres exited `ok: true`, **three rounds running** |
| Codex B1 / Opus BLOCKER-1 | "the re-snapshot completed" | `swaps > 0 and not active` across 3 modules | a **live table's rows deleted** on a claim nothing had checked |
| Codex B3 / Opus MAJOR-1 (1.6-1.8) | "a recovery is in progress" | 4 unjournalled mutations + a local variable | **permanent refusal to start**, three restarts running |
| architecture review, finding 1 | "this table's image is trustworthy" | `snapshot_state`, unvalidated, `in_progress` in no queue | a half-snapshotted table owed work and **belonged to no queue** |

Explicit machines do not make the system correct. They make the **unenumerated path** a
run-time error instead of a review finding.

#### What was built — four machines and one precedence

| machine | owns | states | edges | persistence |
|---|---|---|---|---|
| `table_lifecycle` | is this destination table a trustworthy image, and who owes the work | 5 | 21 | `_cdc_flight.table_state.snapshot_state` |
| `run_phase` | where is this run, readable from the destination *while it runs* | 9 | 23 | `_cdc_flight.heartbeat.phase` |
| `run_outcome` | why did this run stop — cause before symptom | 9 | 36 (escalations only) | `heartbeat.terminal_reason`, `last_run.json` |
| `acquisition_recovery` | what has this destructive recovery already done | 5 | 9 | `_cdc_flight.recovery_state.phase` |
| `catalog_change` | where is one DDL fact in observe → confirm → fence → apply | 9 | 30 | **memory only** |

Style, deliberately minimal: `cdc_flight/states.py` is 293 lines with **no dependencies**
— plain-`str` states (they are already durable strings in `VARCHAR` columns, in
`last_run.json` and in a hundred test literals, so an `enum.Enum` would need a migration
and would break every existing SQL comparison for no gain), `machine.check(from, to)`
raising `IllegalTransition`, `machine.parse()` raising `UnknownState`, and
`machine.table()` emitting the transition table as data. `cdc_flight/machines.py` is the
one file a reviewer reads to see every consistency-affecting state in the system.

#### Four measured bugs are now edges that do not exist

Each of these is a test in `tests/1.9_state_machines/`, named after the finding:

* `RUN_OUTCOME.check("source_dark", "hung")` **raises**. A dark source makes
  `engine.close()` hang almost by definition; the old `finally` overwrote the diagnosis
  with the symptom, and the fix was `if stop_reason not in ("source_dark",
  "engine_error")` written out at two call sites. `supervisor.py` no longer assigns
  `stop_reason` at all — asserted by parsing its AST, not by grepping.
* `TABLE_LIFECYCLE.check("none", "complete")` **raises**. A table reaches `complete` from
  a swapped shadow or from having been *proven* empty at the source, and from nowhere
  else — which is Codex B1's shape as a missing edge.
* `TABLE_LIFECYCLE.check("in_progress", "in_progress")` **raises**. A second shadow
  opened over a durable half-finished snapshot is how that residue stayed invisible; the
  declared route is start-up promotion to `awaiting_snapshot`, and it is the only one.
* `ACQUISITION_RECOVERY.check("requested", "armed")` **raises**, and `-> absent` is
  reachable only from `armed`. No caller can claim a slot was dropped that never was, and
  no caller can clear a journal that still describes a half-done destructive sequence.

#### The behavioural changes, not just the checks

* **One writer.** There is now exactly one `UPDATE ... SET snapshot_state` and one
  `INSERT ... snapshot_state` in `src/`, both in `cdc_flight/table_lifecycle.py`, and a
  test greps the shipped source for a second one. A machine with two writers is a machine
  with one writer and one bug pending.
* **The owed queue selects every non-terminal state**, not the one literal
  `awaiting_snapshot`. The start-up promotion still runs — "owed" and "owed and
  mid-snapshot" should not be two different durable answers — but the queue no longer
  *depends* on somebody having called it.
* **`_cdc_flight.heartbeat` has a writer.** ADR §4.8 declared `phase` at rev 1 and nothing
  ever wrote it, so "where is this run" was a source-line position in a 470-line function.
  One row per run, updated on each transition, on the **independent** connection, never
  inside a commit group and never inside the commit→ack window. `phase_since`,
  `terminal_reason` and `phase_history` are added by a migration, because
  `CREATE TABLE IF NOT EXISTS` cannot add a column and the table shipped one round ago.
* **The commit group is one object.** Sixteen fields reset by name in *two* functions that
  had to stay in sync become `applier.OpenGroup`, created at BEGIN and **replaced** at
  COMMIT and ROLLBACK. A partially-reset group is unrepresentable: there are no fields to
  forget. The test asserts the object *identity* changed rather than enumerating the
  fields — which is exactly the enumeration that diverged.

#### The seven candidates that are deliberately NOT machines

This is the part of the claim that makes "an appropriate number" an argument rather than
a count. Full table in ADR §20/A55.

* **the commit group** — crash ⇒ discard and replay is the *whole* story under Invariant
  O. A durable machine would advertise recoverable intermediate states that do not exist,
  which weakens the design's central claim. `OpenGroup` instead;
* **the transaction assembler** — *already* a guarded machine, with an error type naming
  every rule it enforces; the one component that has produced no correctness blocker in
  four review rounds. Untouched;
* **the spill unit** — staging is inside the group's own transaction, so `DISCARDED` is
  what `ROLLBACK` does for free. Recorded: *if* separately-committed staging is ever
  adopted, this becomes a durable machine and must be built as one;
* **the lease** — already explicit and durable, `LeaseLost` on every illegal transition;
* **`SourceHealth`** — a **fold**, not a machine. What was missing was a declared
  *classification*, now `machines.SOURCE_HEALTH_STATES` and one `state()` property, which
  finally names `unknown_never_sampled` (A51 row 50's fail-open, previously a
  fall-through with no name);
* **`check_slot`** and **offset reconciliation** — decision tables over *external*
  configuration. Their outputs are frozen **domains**, so the inventory, the run summary
  and the tests share one vocabulary; they are not states anything moves through.

#### Evidence

| what | where | cost |
|---|---|---|
| the mechanism, and the four bugs as illegal edges | `tests/1.9_state_machines/test_1_9_machines.py` | ms |
| one writer, the `in_progress` residue, the owed queue, `--reset-state`, alerts | `tests/1.9_state_machines/test_1_9_table_lifecycle.py` | ms |
| the durable phase row, the precedence, the migration, the independent connection | `tests/1.9_state_machines/test_1_9_run_state.py` | ms |
| the per-relation state and the fence | `tests/1.9_state_machines/test_1_9_catalog_change.py` | ms |
| `OpenGroup` — the object is replaced, not edited | `tests/1.3_atomic_batches/test_1_3_rollback_resets_the_group.py` | ms |
| the ADR's transition tables are generated from `machine.table()` | `tests/4.7_self_healing/test_4_7_inventory.py` | ms |

78 tests, well under a second, all in the default lane; the five recovery anchors add 17
more (four of them a real `os._exit` in a child process) in 0.8 s. That is deliberate: a
guard that only runs when somebody asks for it is not a guard. Measured after round 2:
**544 default / 8:51**, against a 10-minute budget.

#### Why 5, and what a reviewer should push on

The rubric's 5 is *an appropriate number of state machines (over 1)*. Five machines, each
owning exactly one state, no two sharing a durable location (asserted), and seven declined
candidates each with a written argument. The 3-band ("only 1 big state machine") is
clearly not this, and the 1-band is clearly not this.

#### What the first Codex review rejected, and what round 2 changed

The first submission of this item was scored **3, not 5**, and the rejection was
specific: several good machines with material gaps, so the rubric's word **any** was not
satisfied. Every gap it named is closed here, and each one is a behaviour change rather
than a documentation change.

| finding | what it was | what it is now |
|---|---|---|
| **BLOCKER-1** — `--accept-orphan-offsets` journalled AFTER destroying its evidence | `offset_reconcile` dropped the slot and unlinked `offsets.dat`, then `pipeline.run()` wrote the journal. A hard exit in that gap left no row, no file, no slot and no journal, which the next run reads as an ordinary `fresh_start`: under a configured non-data `snapshot.mode` it streams onto a destination nobody rebuilt (B3/A53, recreated) | `reconcile()` **classifies and mutates nothing**. `recovery.begin()` makes the intent and the whole table obligation durable first; the one idempotent `resume()` ladder removes the file, deletes the row and drops the slot, anchored at every boundary. Proved by a real `os._exit` at `recovery_requested` and a restart that does **not** repeat the flag, under `--snapshot-mode no_data`, ending in exact source/destination equality |
| **MAJOR-1** — `catalog_change` was a shadow model with a real missing edge | `CatalogChange.state` sat *beside* `_unconfirmed`, `_pending` and a `fenced` flag; the confirming poll built a second object, so `unconfirmed -> pending` described nothing; and a live `due -> marked` event (poll overlapping `due()`) raised `IllegalTransition`, which `poll_quietly` wrote to `last_error` and stepped over | The machine **owns** the representation: `_unconfirmed` holds the object whose state says `unconfirmed`, `fenced` is derived from the state history, `pending()` filters on state, `queue()` is the `observed -> pending` edge, and `_emit_marker` never walks a `due` change backwards. An undeclared edge on the polling thread sets `machine_error` and **fails the run** (A51 row 51a) |
| **MAJOR-2** — two `RunOutcome`s, contradictory summaries | `run_engine_bounded` built one and `RunPhaseWriter` another, so successful runs shipped `stop_reason="idle"` beside `run_outcome="max_seconds"`, and the summary was sampled while the run was still `draining` | **One** `RunOutcome` per run, constructed in `pipeline.run()` and handed to both. The returned dict is updated in the outer `finally` **after** the terminal transitions, so `last_run.json` and the destination heartbeat are two projections of one state. Outer failures record on the same object |
| **MAJOR-3** — the heartbeat could overlap commit→ack and could borrow the primary connection | true in program order, false in wall clock: the supervisor writes `draining` on its own thread the moment `max_seconds`/engine error/source-dark breaks the loop, and `con.cursor()` failing set `_sink = con` | `run_state.COMMIT_ACK` is entered by the applier immediately before `COMMIT` and left immediately after `markBatchFinished()`; a phase write inside it is **dropped** (never deferred behind a lock, never blocking), counted, and reported. No independent connection now means **no row**, not the applier's connection |
| **MAJOR-4** — `--reset-state` neither atomic nor convergent | five independent durable mutations plus a process-local `snapshot.mode='initial'`. The convergence argument was false: a positioned slot over a populated destination makes the next run refuse with `no_durable_destination_row` before `will_snapshot_everything` is computed, and repeating the flag does not drop that slot | a journalled recovery (`decision='operator_reset'`) like any other: intent and table reset in one transaction, then state directory, resume row and **slot**, each idempotent. Proved by an `os._exit` mid-sequence and a restart **without the flag** under `no_data` |
| **MAJOR-5** — ownership gaps | `SLOT_VERDICTS` / `RECONCILE_DECISIONS` were referenced only by tests; the recovery-clear predicate lived in `pipeline.py` and a false predicate still reported `ok: true`; the captured obligation was not persisted | both domains are parsed in `__post_init__` on the production types; `recovery.complete_if_ready()` owns the predicate, validates the **journalled** captured set, performs `armed -> absent` itself and returns a typed `Completion`; an uncleared recovery raises `EngineFailure` and the run exits non-zero. `slot_state` persists `verdict`, `verdict_message`, `verdict_at` in the observation's own transaction |
| **MINOR-1/2/3** | migration failures silently accepted; the `OpenGroup` claim overstated; `in_progress -> in_progress` checked after a side effect | a failed `ALTER` is **re-read** and raises `ControlSchemaFailed` unless the column is now present; the `OpenGroup` claim is narrowed to what is true and the one legitimate partial mutation is `discard_units()`; `SnapshotCoordinator.state_for` calls `table_lifecycle.check_transition()` **before** it drops the shadow |

Held-open items a reviewer should still treat as the honest edges of the claim:

1. **`catalog_change` is memory-only and still does not gate the fence.** The behavioural
   fence is `durable_lsn >= detected_lsn`, as it must be — it is a fact about the
   destination, not about this object. What the state now owns is the *queue*: which
   changes are live, which are fenced, and whether a poll may touch one the applier is
   holding. That is a behaviour change; gating the LSN comparison would not be.
2. **Two new MANUAL rows, and a third.** Making an undeclared transition and an
   out-of-domain durable value into loud refusals *created* two manual-intervention cases
   (A51 rows 51, 52) where there had been two silent-corruption paths; splitting the
   catalog machine's policy out honestly (51a) adds a third. Right for correctness, wrong
   for 4.7's count, and all of it recorded rather than half of it.
3. **`states.py` is 293 lines, not "~230".** The number above is corrected; the module
   still has no dependencies.


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

### 4.6 Detect failure of a Postgres node with a silently-dead connection — ~~1~~ **3 / 5**

`unable to detect=1, TCP keepalives under 30 min=3, full heartbeat under 1 h=5`

**Evidence.** Debezium sets `database.tcpKeepAlive` `.withDefault(true)`
(`repos/debezium/.../PostgresConnectorConfig.java:1096-1104`) but the OS defaults
govern the timing — ~2 hours idle on macOS/Linux — and we override nothing.
`status.update.interval.ms` defaults to 10 s
(`PostgresConnectorConfig.java:998-1006`), which does put writes on the socket,
but the resulting TCP retransmission timeout is neither configured nor measured.
The `wal_sender_timeout = 60s` in `scripts/pg.sh` protects the *server* from a
dead client, not us from a dead server.

**Now (2026-07-31) — 3/5.** TODO 4.6(b) is closed and the probe the baseline asked for
exists. `tests/tcp_relay.py` is an out-of-process TCP relay that accepts the pipeline's
connections and then stops forwarding packets with the sockets left open — a real
silently-dead source, not a killed process. What it found: `unknown` slot health used to
license an idle declaration *and* reset the not-streaming clock, so a blackholed Postgres
exited `ok: true` on a partial delivery (ADR §19/A49). A source that was answering and
goes dark now forbids the idle declaration and fails the run within
`CDC_SOURCE_DARK_SECONDS` (45 s), with `stop_reason='source_dark'`.

The test that carries the 45-second claim now **measures** it. It used to assert only
`returncode != 0` plus `"source" in json.dumps(summary).lower()` — which `source_schema`
and half a dozen other keys satisfy — while running with `--max-seconds 70`, so the run
could equally have been ended by the deadline (Opus MINOR-5). It now asserts
`stop_reason == "source_dark"` and an elapsed bound below the deadline.

**Not 5, and the honest reason.** Detection is *ours* — a 0.5 s slot sampler on a second
connection — not the connection's. There is still no application heartbeat (4.4) and no
bounded JDBC socket timeout (4.6(c)), so a source that is dark from the **first** sample
(no privilege, no psycopg, dark before we ever looked) still degrades to the timer-only
path and can report success on a delivery that never started. That fail-open is
deliberate and it is now written down as an inventory row rather than left implicit
(ADR §19/A51 row 50, Codex m1).

**Evidence that would raise this.** The 4.4 heartbeat as the authoritative liveness
signal, plus an explicit socket read timeout, with time-to-detection measured against
both.

**Gap to 5.** Set explicit keepalive parameters on the JDBC connection
(`database.tcpKeepAlive` plus OS-level `tcp_keepidle`/`tcp_keepintvl` via socket
options), add a connection-level read timeout, and add the application heartbeat
from 4.4 as the authoritative liveness signal with a detection budget well under
an hour. Then measure it.

---

### 4.7 The Flight self-heals without human intervention — **1 / 5**

`more than 2 manual-intervention cases=1, human needed only in a catastrophic scenario=3, self-heals in 100% of cases=5`

*Added 2026-07-31 by user directive, and it is the only scored item whose band is a
literal count rather than a judgement. That is why it is scored the way it is.*

#### The number, and where it comes from

ADR §19/A51 enumerates every raise site, fatal log, refusal and `stop_reason` in the
tree: **54 rows, 34 AUTO / 12 MANUAL / 8 UNDEFINED**. Twelve manual cases is more than
two, so the 1-band's test is met on our own evidence.

The count is not recalled: `tests/4.7_self_healing/test_4_7_inventory.py` re-parses the
ADR table and fails if the headline stops matching the rows, or if any row carries two
terminal classes in one cell. That test exists because the previous headline —
`24 AUTO / 9 MANUAL / 6 UNDEFINED` — totalled 39 against a 40-row table and matched no
reading of the class column, and **both** reviewers had to count the rows by hand to find
that out (Codex M5, Opus MAJOR-3). A number a score rests on is code.

#### Why it was claimed at 3, and why that was wrong

The 3-band describes a Flight that needs a human only in a *catastrophic* scenario. The
manual cases are not catastrophic: an orphan `offsets.dat`, a `DROP SCHEMA … CASCADE`, a
read-only replica that cannot take a fence marker, a typo'd `CDC_TRUNCATE_MODE`, a full
destination. Those are routine. The previous 3 was reached by treating the manual rows as
"scored exceptions" and scoring the remainder — which is scoring a hand-selected subset,
not the item.

#### The manual cases, and why each is manual

| why | rows |
|---|---|
| protecting a destination from an automatic action that could destroy it | 17b (populated destination, no resume point), 25 (orphan offsets), 26 (mass-drop breaker), 27 (unwritable fence marker), 38 (destination full), 44b (a slot that never frees) |
| configuration a human wrote and only a human can fix | 28 (malformed env), 29 (missing token / unreachable destination), 49 (topic collision) |
| a deliberate opt-out of automation | 41 (`CDC_RESNAPSHOT=0`), 42 (`CDC_AMBIGUOUS_RESNAPSHOT=0` and the two unqueueable folds) |
| a source-side misconfiguration | 33 (publication dropped / privileges revoked) |

Six of the twelve are *correct* engineering decisions — refusing to auto-destroy a
destination is the right call — and the rubric scores outcomes, not intentions.

#### What this round actually moved

Eight failure modes that used to require a human are now automatic:

* an externally advanced slot, a dropped slot, a recreated slot (rubric 1.8);
* a restored/cloned source, and now a **forked timeline** as well;
* a rewound source, with the stale catalog discarded so the mass-drop breaker no longer
  fires spuriously on our own bookkeeping;
* an undecidable fold (`AmbiguousDelete`) and a `DestinationIdentityCollision`, which
  previously failed identically for ever (A47);
* **the Flight's own half-finished recovery** — which this round *introduced* as a
  permanent manual case and then closed with a durable journal (A53).

And enumerating the inventory honestly *found four more manual cases* than the previous
count admitted. That is the shape of real progress on this item: the number goes up
before it goes down, because the first thing self-healing needs is an accurate list.

#### The one fail-open, stated

If the slot sampler has **never** succeeded — no privilege, no psycopg, a source that was
dark before we ever looked — the run degrades to the timer-only idle path and can report
success on a delivery that never started (A51 row 50, Codex m1). Once the sampler has
succeeded, a source that goes dark forbids the idle declaration and fails the run within
45 s (A49). The fail-open is deliberate and is classified `UNDEFINED` rather than `AUTO`,
because "reported success and delivered nothing" is not a recovery.

#### What would raise this

Not more argument: fewer rows. The 3-band needs the routine manual cases gone —
`no_durable_destination_row` and `orphan_offset_file` become automatic once durable
source/destination *ownership* proves the file and the target belong together (both
reviewers agree that proof does not exist today); the mass-drop breaker needs a
confirmation channel that is not a human; the unwritable fence marker needs 7.2's
primary-side write path. The 5-band additionally needs the `UNDEFINED` bucket empty,
which is rubric 4.3's work on malformed WAL and assembly errors.


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
