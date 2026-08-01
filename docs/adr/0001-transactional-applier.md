# ADR 0001 — The transactional applier

* **Status:** accepted (revision 8, 2026-07-31 — implemented; §15 records the amendments the implementation forced, §16 those the 1.1–1.3 review round forced, §17 those 1.4/1.5 forced, §18 the 1.4/1.5 review round, §19 the 1.6-1.8 work and its review round)
* **Date:** 2026-07-30
* **Task:** TODO 1.0(a); revised under TODO 1.0(feedback)
* **Decides rubric items:** 1.1, 1.2, 1.3, 1.7 (directly), and 1.4, 1.6, 1.8, 3.2,
  3.3, 4.2, 4.4, 7.2, 7.4, 8.2 (mechanism only — each still needs its own task).
  *Revision 2 trimmed this list: 2.4, 2.6, 3.1, 4.5, 4.6, 5.1, 5.3, 5.4, 6.1, 6.2
  and 8.1 were previously listed as "shaped by" this ADR but are only gestured at
  here; treating them as settled would be wrong (Opus M10).*
* **Supersedes:** the dlthub blog's `DltChangeHandler` shape
  (`research/dlthub_debezium_and_dlt.md`). The rubric outranks the prior art.

## Revision history

| rev | date | change |
|---|---|---|
| 1 | 2026-07-30 | original |
| 2 | 2026-07-30 | **P2 withdrawn** and replaced by **Invariant O** (§4.1). Crash matrix rebuilt over all three engine lifecycle paths **and** the snapshot phase (§4.6). Transaction assembly made a state machine with one boundary rule (§3.2). Triggers restated as soft group-close requests plus a hard spill threshold (§3.3). Start-up reconciliation decision table added (§4.5). Keyless event identity moved off `source.sequence` (§6). D10 rewritten: dlt demoted to a **library**, not removed (§10). Throughput risk of D5 recorded as a measurement task (§5). |
| 3 | 2026-07-30 | **Amendments from the implementation** (§15): the apply path must insert through Arrow - `executemany` is 300x slower and makes a large commit group unfinishable (A14); `transaction.id` is not a transaction identifier (A1); the Connect schema stays off, so rubric 2.4 is untouched (A2); `verify_offset_file` becomes a rebuild plus a one-directional assertion (A4); a drained batch closes the group (A5); a provably-dead lease is reclaimed (A6); §14.1 answered for DuckDB (A8); §10's dlt exit criterion evaluated (A10). |
| 4 | 2026-07-31 | **Amendments from the 1.1-1.3 review round** (§16). The boundary rule made unconditional in every storage mode (A20); the ordinal contract enforced, which is how the keyless-identity disagreement between the two reviews resolves (A18); spill made one ordered pass with explicit identity and a fence that covers staged rows (A19); the destination enforces the identity with a PRIMARY KEY (A21); reconciliation compares the whole typed offset map and gains §4.5's missing "slot exists / no durable row" row (A22); `lsn.flush.mode` pinned (A23); the commit->ack window emptied (A24); the fault anchors corrected and extended (A25); `commit_id` scoped per pipeline (A26); the applier decomposed (A29); deferrals stated (A28). |
| 6 | 2026-07-31 | **Amendments from the 1.4/1.5 review round** (§18). The fold rebuilt around **physical rows**, which supersedes A31 and closes five reproduced silent-loss/duplication orderings (A35); where it cannot attribute, it refuses rather than guessing (A36); Invariant O restated as bounding *ordering only*, so the fold is the second half of the exactly-once claim (A37); four guards added between detection and destruction, and `CDC_CATALOG_GRACE` excluded from the guarantee (A38); `table_state` made the canonical ownership registry, which `--reset-state` no longer deletes (A39); the destructive-DDL alert genuinely moved out of the transaction and classified by whether it describes a refusal or an applied action (A40); a rolled-back group discarded in the process too (A41); one source writer shared with D9 (A42); a run with an unresolved destructive change is not a success (A43); module boundaries restored (A44). |
| 7 | 2026-07-31 | **Amendments from 1.6 / 1.7 / 1.8 and the mid-task addition of rubric 4.7** (§19): the re-snapshot mechanism, and why Debezium only pairs a snapshot with an exact LSN on a slot it creates itself (A45); the hand-over fenced per table on the COMMIT LSN, with the changelog cost stated (A46); undecidable folds self-heal instead of failing identically for ever (A47); destination and network fault injection, the commit watchdog and the mis-reported cause (A48); `unknown` source health no longer licenses an idle declaration (A49); rubric 1.8's decision table and the one refusal that survives it (A50); the failure-mode inventory rubric 4.7 needs (A51). |
| 5 | 2026-07-31 | **Amendments from 1.4 / 1.5** (§17): a key-changing UPDATE is always `d`+`c` for Postgres, so 1.4's atomicity is a corollary of §3.2/§3.3 (A30); the merge cannot collapse a group by key alone when one key is worn by two rows in one transaction (A31); TRUNCATE is a counted, key-less data event and `skipped.operations` was the whole gap (A32); DROP TABLE needs a catalog poller whose WAL fence marker must be **transactional** (A33 — which also constrains D9); what a table-level event means for history (A34). |
| 8 | 2026-07-31 | **Amendments from the 1.6-1.8 review round** (§19, continued). Re-snapshot completion means *every requested table*, and "empty" needs positive evidence rather than the absence of our own records (A52 — which also corrects A45's `min()` resolution of a disagreeing `C` to a hard failure). The acquisition recovery becomes a journalled, idempotent, crash-re-entrant state machine, and A50's claim that its *order* made every intermediate state recoverable is withdrawn as false (A53). A fault test must name the anchor that fired, and three fault-injection accidents are fixed (A54). A50 gains a timeline decision and a destination-emptiness input. A51 is recounted (the old headline did not add up), split to one failure and one class per row, extended with the modes this branch exposed, and made machine-checked. |
| 9 | 2026-07-31 | **Rubric 1.9 — explicit state machines** (§20). Four machines and one precedence, each owning exactly one consistency-affecting state, declared in `cdc_flight/machines.py` and enforced by a dependency-free `cdc_flight/states.py` with no dependencies (A55). `table_state.snapshot_state` gets a single writer and a transition table, and the owed queue selects every non-terminal state rather than one literal value; `_cdc_flight.heartbeat` gets the run-phase writer ADR §4.8 has specified since rev 1; `stop_reason` becomes a declared precedence, so A49's cause-before-symptom rule stops being two copies of a literal tuple; `recovery_state.phase` gains edge checks on top of rev 8's domain check; catalog changes get one per-relation state instead of four containers. The commit group deliberately stays memory-only — a durable machine there would weaken Invariant O — and its sixteen hand-reset fields become one `OpenGroup`, so neither reset path can forget a field (A56, narrowed at A58.6: the mutable type does not make partial mutation impossible, and does not claim to). The acquisition recovery gains five fault anchors of its own, closing rubric 1.7's honest hold (A57). A51 is rewritten as states × transitions with generated transition tables. |

---

## 1. Context

The Phase-0 baseline is

```
Postgres 18.1 → Debezium 3.6 embedded engine (pydbzengine/JPype)
  → ExtractNewRecordState → dlt 1.29.1 (write_disposition="append")
  → DuckDB file / MotherDuck
```

and scores **1.65 / 5** across the rubric (`RUBRIC_STATUS.md`). Five measurements
from Phase 0 constrain every design choice below:

| Measurement | Where | Consequence |
|---|---|---|
| A `kill -9` mid-load left **2 048 duplicate rows** (402 048 rows / 400 000 distinct) | `probes/p13` case B | at-least-once, and `append` makes replays permanent |
| One 400 000-row PG transaction became **174 Debezium batches**; the baseline calls `dlt.run()` **once per Debezium batch** (`handler.py`), and dlt then opens **one transaction per table** inside each load package | `probes/p13`, `probes/p06`, `repos/dlt/dlt/destinations/insert_job_client.py:24` | Postgres transaction boundaries are invisible at the destination. *(rev 2: the "one load package per 2048 rows" is our handler's choice, not a dlt property — Opus m7.)* |
| **~17 s** of JVM start + MotherDuck connect per process (wall 31.9 s for 15.1 s of engine time) | `probes/p12` | a per-run process model cannot meet 5.2 (<30 s) or 5.3 (>2000 TPS) |
| Slot dropped / advanced externally / offset corrupted ⇒ `{"records": 0}`, **exit 0** | `probes/p04`, `p10`, `p11` | failures were invisible; partly fixed by TODO 1.0(b), fully by TODO 1.0(feedback) (see §11) |
| `numeric`→base64, dates→BIGINT, a NaN column **dropped entirely**, TOAST→`__debezium_unavailable_value` | `probes/p02`, `tests/test_e2e_duckdb.py` | the `ExtractNewRecordState` + JSON payload is lossy before we ever see it |

### 1.1 The user's binding design principles

Stated 2026-07-30 and binding on every decision below:

1. **Data loss must be structurally impossible, by construction** — not achieved
   by careful ordering, not mitigated by a handshake.
2. **The replication slot may only advance once the data is durable (committed)
   in MotherDuck.**
3. **The MD-commit → slot-advance window must be as short as possible, with no
   other operations in between.**
4. **Data commits and state-management commits must be atomic with one another**
   (same MotherDuck transaction).

Principle (1) is what forced the revision in rev 2: rev 1's argument (P2) was an
*ordering* argument, and an ordering argument is conditional on a schedule.

### 1.2 What the vendored Debezium sources actually say

Read out of `repos/debezium` (3.6.0.Final) rather than assumed. Paths below are
relative to `repos/debezium/`.

* `RecordCommitter.markBatchFinished()` is the **only** thing that ever flushes an
  offset from the poll path — there is no background flush thread
  (`debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java:1369-1382`,
  `:894-932`). `offset.flush.interval.ms` merely gates it via
  `OffsetCommitPolicy`.
* **`markBatchFinished()` can silently not flush.** `commitOffsets(...)` returns
  `false` when there is nothing to flush, when `doFlush` returns `null`, or when
  *any* non-`TimeoutException` exception is thrown (`:894-932`, note the
  `catch (Exception e) { … return false; }`). `markBatchFinished()` **discards the
  boolean** (`:1369-1382`). Only a `TimeoutException` is rethrown. So "the offset
  file is now current" is not something `markBatchFinished()` returning normally
  can be taken to mean. (Opus B2.)
* The poll loop calls `committer.markBatchFinished()` **on every empty poll**
  (`AsyncEmbeddedEngine.java:1320-1325`). Any design that relies on *us* being the
  only caller is already wrong.
* The Postgres connector's `performCommit()` **re-reads the offset from the offset
  store** and confirms *that* LSN to Postgres
  (`debezium-connector-postgres/…/PostgresConnectorTask.java:470-497` →
  `PostgresStreamingChangeEventSource.java:504-535` →
  `PostgresReplicationConnection.java:1032-1046`).
* **There are three paths to `performCommit()`, not one** (this is the finding
  that withdrew P2 — Codex 1 / Opus B1):

  | # | Path | Thread | Waits for our `handle_batch`? |
  |---|---|---|---|
  | **L1** | poll loop: `poll()` → `if (shouldPerformCommit.getAndSet(false)) performCommit()` (`BaseSourceTask.java:353-361`) | the poll thread — the same thread that runs `handle_batch` | **yes** |
  | **L2** | graceful shutdown: `engine.close()` → `stopConnector()` (`AsyncEmbeddedEngine.java:777-783`) → `stopSourceTasks()` (`:701-712`) → `commitOffsets(offsetWriter,…)` **and** `task.connectTask().stop()` → `BaseSourceTask.stop()` (`:504-513`) → `performCommit()` | the `taskService` pool | **no** |
  | **L3** | error teardown: `closeEngineWithException()` (`:318-332`) → the *same* `close(state)` → `stopConnector()` → `stopSourceTasks()` | the engine thread / `taskService` pool | **no** — and this is the path taken when *our own applier raises* |

  `stopSourceTasks()` also calls `commitOffsets()` **itself** before stopping the
  task (`:704-710`), so it flushes anything `markProcessed()` left pending in the
  `OffsetStorageWriter`. The dangerous window therefore opens at the **first
  `markProcessed()`**, not at `markBatchFinished()`.
* `lsn.flush.mode` exists in 3.6 (`PostgresConnectorConfig.java:1264-1284`,
  `:1348-1359`; supersedes the deprecated `flush.lsn.source`). With `manual`,
  `isFlushLsnOnSource()` is false (`:1522-1524`) and `flushLsn()` becomes a
  **no-op on every path** (`PostgresReplicationConnection.java:1032-1046`). The
  pgjdbc keepalive flush is also off, because that needs `connector_and_driver`.
* `source.sequence` is **not an event ordinal**: it is
  `[lastCommitLsn, currentLsn]` serialised as JSON
  (`debezium-connector-postgres/…/SourceInfo.java:180-196`). Several events can
  share one LSN; Debezium keeps a separate `lsn_proc` offset field for exactly
  that reason (`PostgresOffsetContext.java:38-39`, `:98-101`, `:248-249`).
* With `provide.transaction.metadata=true`, every data event's envelope carries a
  `transaction` block of `{id, total_order, data_collection_order}`
  (`debezium-connector-common/…/txmetadata/TransactionStructMaker.java:20-28`,
  `AbstractTransactionStructMaker.java:41-43`). `total_order` is
  `transactionContext.getTotalEventCount()` — a 1-based ordinal **within the
  transaction**. The `END` marker carries `event_count` and per-`data_collection`
  counts (`AbstractTransactionStructMaker.java:51-64`).
* `TransactionMonitor.dataEvent()` returns early when the event has **no
  transaction id** and emits `END` only when the txId *changes* or on
  `transactionCommittedEvent` (`TransactionMonitor.java:74-108`). Snapshot records
  therefore produce **no `BEGIN`/`END` at all** — see §3.5 (Opus B3).
* pydbzengine's `PythonChangeConsumer.handleBatch` calls `markProcessed()` /
  `markBatchFinished()` **for** us and never hands the committer to the Python
  handler (`repos/pydbzengine/pydbzengine/_jvm.py:109-130`). It also swallows the
  flush outcome.
* `user directive, 2026-07-30`: **rubric item 7.2 (read from a Postgres replica,
  light workload on the primary) stands and must be met.** The idle-slot
  heartbeat therefore may not be a write on the connection Debezium streams
  from — see the dual-connection topology in §9.2.

## 2. Decision, in brief

1. **D1** — Drive MotherDuck directly. The apply path is a hand-written
   applier over one DuckDB/MotherDuck connection, not `dlt.pipeline.run()`.
2. **D2** — The unit of work is a **commit group**: one MotherDuck
   `BEGIN … COMMIT` containing an integral number of *whole* Postgres
   transactions (or, during a snapshot, whole snapshot chunks).
3. **D3** — **Invariant O**: Debezium's offset store must never contain an offset
   that is not already durable in MotherDuck. `markProcessed()` /
   `markBatchFinished()` run **after** the MotherDuck `COMMIT`; our own canonical
   resume point is written **inside** it. Exactly-once follows from a structural
   impossibility, not from an ordering argument and not from deduplication.
4. **D4** — The engine runs **long**, not once per batch: one JVM, one Postgres
   connection, one MotherDuck connection, many commit groups.
5. **D5** — Consume the **full Debezium envelope** with its Connect schema.
   `ExtractNewRecordState` is dropped. *(Throughput risk: §5.1.)*
6. **D6** — Every event gets a stable synthetic identity derived from the complete
   source identity; keyless tables use it as their row identity.
7. **D7** — Backfills go to `<table>__cdcf_tmp` shadow tables and are swapped in
   one transaction, together with the resume point.
8. **D8** — Per-table output shape: current state, changelog, and/or SCD2, all
   written inside the same commit group.
9. **D9** — Two heartbeats: a **destination** heartbeat for liveness and
   observability, and a **source** heartbeat that emits a logical message on a
   *separate connection to the primary*, so streaming can read from a replica
   (7.2) while the slot still advances (4.4).
10. **D10** — dlt is demoted from **framework** to **library**. The pipeline/load
    path and dlt's state tables leave the apply path; `dlt.common.schema`, the
    naming normalizers and `dlt.destinations.sql_jobs`' SQL generators are
    retained as callables inside *our* transaction. See §10.

---

## 3. D1/D2 — The applier and the commit group

### 3.1 Why not dlt's load path

`dlt.pipeline.run()` cannot host the resume point in the destination
transaction, cannot span tables in one transaction, and costs a schema
resolution + load package + `complete_load` per call. Verified in the vendored
dlt 1.29.x:

1. **One transaction per file, i.e. per table.**
   `repos/dlt/dlt/destinations/insert_job_client.py:24` —
   `with self._sql_client.begin_transaction():` inside `InsertValuesLoadJob.run()`.
2. **`complete_load()` runs on a *different connection*.**
   `repos/dlt/dlt/load/load.py:637-647` — `complete_package()` does
   `with self.get_destination_client(schema) as job_client:` and only then calls
   `job_client.complete_load(load_id)`; `SqlJobClientBase.__enter__`
   (`job_client_impl.py:444-446`) opens a **new** connection. The data
   transactions are already committed and closed by then, so even the
   "put the offset in `complete_load`" trick cannot join them.
3. **Jobs run in a worker pool with independent clients**, so serialising them
   into one transaction defeats dlt's own concurrency model.

There is no supported hook (`on_before_commit`, shared-connection context) to
inject a statement into a data job's transaction, and a custom
`@dlt.destination` sink is still invoked per job/file. Three rubric items (1.1,
1.3, 5.1) are therefore structurally unreachable through the dlt **load path**.
That is the whole reason for D1; it is not a performance preference, and it is
*not* an argument against dlt's other layers (§10).

### 3.2 Transaction assembly — one rule, and a state machine

Rev 1 gave two contradictory rules: §3.2 offered a `txId`-change fallback "for the
final transaction before a shutdown", while §3.4's pseudocode advanced only on
`END` (Codex 2 / Opus M2). Both were wrong in opposite directions. Rev 2 states
**one** rule:

> **Boundary rule.** A Postgres transaction is *complete*, and therefore eligible
> for a commit group, **only** when its authoritative Debezium `END` marker has
> been received **and** the marker's `event_count` equals the number of events
> buffered for that transaction id (and each `data_collections[].event_count`
> equals the per-table count). Nothing else ever marks a transaction complete.

Consequences, stated so they cannot be re-litigated by accident:

* A `txId` change **without** an intervening `END` is a **fatal consistency
  error**, not a fallback. It means transaction metadata is broken or filtered;
  the applier raises rather than guessing. (`provide.transaction.metadata=true`
  is therefore mandatory, and start-up asserts the transaction topic is
  reachable.)
* **At shutdown, the un-`END`ed tail is discarded** and replays on the next run.
  It is safe to discard precisely because nothing about it has been acknowledged
  — under Invariant O the offset store still points before it. "Drain to a group
  boundary" means *the last verified `END`*, never "assume the tail is done".
  This removes rev 1's shutdown heuristic, which could commit a partial Postgres
  transaction (Codex 2).
* An event count mismatch is fatal for the same reason: a commit group must
  contain only transactions we can *prove* whole.

The implementation shape follows from that (Codex 2's "code-judo" note, adopted):
a `TransactionAssembler` consumes `PendingRecord`s and emits typed, already
validated `CompleteUnit` objects. A commit group is a list of `CompleteUnit`s by
construction, so no boundary conditionals survive into `commit_group()`.

```python
@dataclass(frozen=True)
class PendingRecord:          # one Debezium SourceRecord, decoded once
    raw:            Any       # the Java ChangeEvent, for markProcessed
    kind:           Literal["data", "txn_begin", "txn_end", "heartbeat",
                            "logical_message", "schema_change", "snapshot"]
    source_offset:  dict      # Debezium's own sourcePartition/sourceOffset pair
    txn_id:         str | None
    total_order:    int | None   # transaction.total_order (1-based, per txn)
    lsn:            int | None
    nbytes:         int
    payload:        dict | None

@dataclass(frozen=True)
class CompleteUnit:
    """A whole PG transaction, a whole snapshot chunk, or a control unit."""
    kind:           Literal["txn", "snapshot_chunk", "control"]
    events:         list[PendingRecord]   # data events only; may be empty
    terminal_raw:   Any                   # the LAST raw record of the unit
    terminal_offset: dict                 # its Debezium source offset
    last_lsn:       int
    txn_id:         str | None
```

`terminal_raw` is the single field that removes rev 1's parallel-array indexing
(`buffer` / `pending` / `complete_upto` / `upto_raw`, Codex 6): acknowledging a
unit means `markProcessed(terminal_raw)` — and, for safety on connectors that do
not treat a partition offset as a high-water mark, every raw record in the unit
in order. The correlated-slice arithmetic disappears.

A **control unit** (`kind="control"`, `events=[]`) is how heartbeats and
offset-only records participate. This fixes rev 1's dead branch, where a
heartbeat-only group could never commit because the guard was
`complete_upto > 0` over the *data* buffer (Codex 5 / Opus M3), **and** the
related hazard that an unconditional heartbeat branch could declare a partially
buffered transaction complete (Opus M3):

> A control unit is emitted **only when no transaction is open**. If a heartbeat
> or logical message arrives between a `BEGIN` and its `END`, it is buffered with
> the transaction and carried by that transaction's `terminal_raw`.

### 3.3 Trigger policy — soft group-close requests, one hard threshold

Rev 1 presented 5 s / 200 000 events / 256 MB as guardrails. They are not:
Invariant B (never split a Postgres transaction) always wins, so for a single
5 M-row transaction none of them bound anything (Codex 4 / Opus M4). Rev 2
separates the two concepts.

**Soft group-close requests.** These ask the assembler to close the group *at the
next unit boundary*. They can never split a unit.

| request | default | why |
|---|---|---|
| `COMMIT_MAX_AGE` | 5 s | 5 s ≪ rubric 5.2's 30 s even with a slow commit; 0.2 commits/s is 0.2 % of MotherDuck's ~100 txn/s budget. Evaluated by an independent timer, not only on batch arrival. |
| `COMMIT_MAX_EVENTS` | 200 000 | keeps a *typical* group's commit latency bounded |
| `COMMIT_MAX_BYTES` | 256 MB | keeps a *typical* group's memory bounded |
| `COMMIT_ON_DDL` | — | a schema change closes the group before the DDL, so no group straddles two schemas |
| `COMMIT_ON_SHUTDOWN` | — | close at the last verified `END`; discard the tail (§3.2) |

Because these are soft, **the true memory bound of the in-memory path is the
largest single Postgres transaction.** That is stated plainly here because rubric
5.4 depends on it and rev 1 obscured it.

**The hard threshold** is therefore a different mechanism, and it changes
*storage representation*, never *visibility*:

| threshold | default | effect |
|---|---|---|
| `UNIT_SPILL_BYTES` | 64 MB | the *current unit* exceeds this in memory ⇒ enter spill mode for that unit |
| `UNIT_SPILL_EVENTS` | 500 000 | ditto, by count |

### 3.4 Spill mode — explicit states, explicit drain

Spill is a state machine on the *current unit*, not a paragraph.

```
BUFFERING ──(unit bytes > UNIT_SPILL_BYTES)──▶ SPILLING ──(END verified)──▶ SPILLED
    │                                              │                          │
    └──(END verified)──▶ COMPLETE_IN_MEMORY        └──(rollback / crash)──▶ DISCARDED
```

* **Entering `SPILLING`** opens (if needed) `_cdc_flight.spill_events`, a single
  staging table for *all* units, inside the group's own transaction:

  ```sql
  CREATE TABLE IF NOT EXISTS _cdc_flight.spill_events (
      commit_id      BIGINT   NOT NULL,   -- the group this belongs to
      unit_seq       BIGINT   NOT NULL,   -- ordinal of the unit inside the group
      event_seq      BIGINT   NOT NULL,   -- total_order within the unit
      target_table   VARCHAR  NOT NULL,
      lsn            BIGINT   NOT NULL,
      txn_id         VARCHAR,
      cdcf_event_id  VARCHAR  NOT NULL,
      op             VARCHAR  NOT NULL,
      before         JSON,
      after          JSON
  );
  ```

  One heterogeneous staging table keyed by `target_table` answers Opus M4's
  "what schema for a multi-table transaction": the payloads stay JSON until the
  drain, where they are projected per target table.
* **Memory retained while `SPILLING`** is the minimal terminal state only: the
  unit's `terminal_raw`, its `terminal_offset`, running counts and `last_lsn`.
  Decoded events are written through and dropped; earlier raw records are dropped
  as soon as they are staged. (Codex 4: "retain only the minimal terminal raw
  record". Whether marking only the terminal record is sufficient for Postgres is
  an **open question**, §14.6 — until it is answered, spill mode retains raw
  records too, and that is the conservative default.)
* **The drain happens in the same transaction, before `COMMIT`:** one
  `INSERT … SELECT … FROM _cdc_flight.spill_events WHERE commit_id = ?` per target
  table (or the D8 merge/SCD2 statement reading from the same source), then
  `DELETE FROM _cdc_flight.spill_events WHERE commit_id = ?`. Because staging and
  drain are inside the one transaction, **nothing is ever visible early**, so
  rubric 1.3 is not weakened. This is the answer to rev 1's misleading claim that
  spill was an "exception to Invariant B" — it is not an exception at all
  (Codex 4).
* **Crash during spill** rolls the whole transaction back, staging rows included,
  and the unit replays. There is no orphan cleanup problem *because staging is
  never committed separately*. If measurement later shows an in-transaction
  staging table is too expensive and a separately-committed staging table is
  needed, that variant requires a deterministic staging identity and a start-up
  `DELETE FROM spill_events WHERE commit_id > last_committed_commit_id` — recorded
  here so the trade is explicit, not adopted.
* **Interaction with D8.** Merge/SCD2 need events ordered per identity key; the
  drain therefore reads `ORDER BY unit_seq, event_seq`, which is exactly the
  source order. `event_seq` comes from `transaction.total_order` (§6), so the
  ordering is the connector's, not ours.
* **What is still unbounded** is the *destination's* uncommitted transaction
  size. That is the thing that actually breaks MotherDuck, and it is
  §14.2's open question, not something this ADR resolves.

### 3.5 Snapshot mode (Opus B3)

Snapshot records carry no transaction id, so `TransactionMonitor.dataEvent()`
returns early and **no `BEGIN`/`END` is ever emitted during a snapshot**
(`TransactionMonitor.java:80-95`). Under rev 1's algorithm `complete_upto` would
never advance and the entire snapshot would buffer. Rev 2 gives the snapshot its
own unit boundary:

* **The unit is a snapshot chunk.** A chunk ends at whichever comes first:
  `SNAPSHOT_CHUNK_EVENTS` (default 50 000), `SNAPSHOT_CHUNK_BYTES`
  (default 64 MB), a change of source table, or the
  `snapshot="last"` / `snapshot_completed` marker in the envelope's `source`
  block. A chunk is a `CompleteUnit(kind="snapshot_chunk")` and is committed like
  any other unit, so buffering is bounded and the same spill machinery applies.
* **Snapshot units are never mixed with streaming units in one group**, so
  `commit_log.trigger` unambiguously says `snapshot_chunk` or a streaming
  trigger.
* **Crash and re-snapshot.** A crash mid-snapshot means Debezium re-runs the
  snapshot from the beginning (`snapshot.mode=initial`). Under D7 that is safe by
  construction and *only* under D7: **the initial snapshot always lands in
  `<table>__cdcf_tmp` shadow tables and becomes visible through the single
  swap transaction (§7).** A re-snapshot truncates the shadow table and starts
  again; nothing partially-snapshotted is ever visible, and no snapshot row ever
  merges into a live table. This — not identity stability — is what makes the
  snapshot idempotent.
* **Snapshot event identity** must nevertheless be stable enough not to poison a
  changelog. `cdcf_event_id` for a snapshot row is
  `f"snap:{snapshot_epoch}:{schema}.{table}:{ordinal}"`, where `snapshot_epoch`
  is a value **read from `table_state.snapshot_epoch`** (incremented once when a
  snapshot starts, recorded in the same transaction) rather than the swap's
  commit id. Rev 1 keyed it on `commit_id_of_swap`, which is not knowable while
  the snapshot is running (Opus B3). Two runs of the *same* snapshot epoch —
  which is what a crash-and-resume produces once §14.4 is answered — therefore
  produce identical ids; a deliberate re-snapshot produces a new epoch, which is
  correct, because it replaces the table wholesale.
* `table_state.snapshot_state` is `in_progress` for the whole snapshot and only
  becomes `complete` in the swap transaction, so 1.6's "where the backfill ends
  and the stream begins" stays a queryable fact.

### 3.6 Algorithm

```text
# ---- state -----------------------------------------------------------------
assembler       : TransactionAssembler        # emits CompleteUnit only
group           : list[CompleteUnit] = []
group_bytes     : int   = 0
group_events    : int   = 0
group_opened_at : float = now()
resume_point    : ResumePoint                 # loaded from MotherDuck at start
close_requested : bool  = False               # set by the timer thread / DDL / shutdown

# ---- Debezium calls this on the single poll thread -------------------------
def handle_batch(records, committer):
    for raw in records:
        rec = decode(raw)                      # full envelope + Connect schema
        for unit in assembler.feed(rec):       # 0..n complete units
            if unit.last_lsn <= resume_point.last_lsn:
                continue                       # idempotency fence, §4.4
            group.append(unit)
            group_events += len(unit.events)
            group_bytes  += unit.nbytes
    if group and (close_requested or soft_trigger_hit()):
        commit_group(committer)

def soft_trigger_hit():
    return (group_events >= COMMIT_MAX_EVENTS
         or group_bytes  >= COMMIT_MAX_BYTES
         or now() - group_opened_at >= COMMIT_MAX_AGE)

# ---- the transaction -------------------------------------------------------
def commit_group(committer):
    commit_id = next_commit_id()
    new_point = ResumePoint(
        partition   = group[-1].terminal_offset["partition"],
        offset      = group[-1].terminal_offset["offset"],
        last_lsn    = group[-1].last_lsn,
        last_txn_id = group[-1].txn_id,
        total_order = group[-1].events[-1].total_order if group[-1].events else None,
    )
    con.execute("BEGIN TRANSACTION")
    try:
        renew_lease(con)                       # 4.2: fail fast if we lost it
        apply_units(con, group, commit_id)     # §3.4 drain, §5, §7, §8
        write_commit_log(con, commit_id, stats(group))
        con.execute(
            "INSERT OR REPLACE INTO _cdc_flight.debezium_offsets "
            "  (pipeline, namespace, resume_json, offset_blob, commit_id, "
            "   last_lsn, last_txn_id, last_total_order, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,now())",
            [PIPELINE, NAMESPACE, new_point.to_json(), None, commit_id,
             new_point.last_lsn, new_point.last_txn_id, new_point.total_order])
        fault_point("pre_commit")
        con.execute("COMMIT")                  # ── (4) data ∧ state atomic ✔
        fault_point("post_commit_pre_ack")
    except BaseException:
        rollback_quietly(con)                  # never mask the original error (m5)
        raise                                  # -> EngineFailure -> non-zero exit

    # ─────────── the ONLY window that matters, and it contains nothing else ──
    for unit in group:
        for rec in unit.records_in_order():
            committer.markProcessed(rec.raw)
    flush_ok = committer.mark_batch_finished_checked()   # see §4.2
    fault_point("post_ack")
    # next poll() -> performCommit() -> flushLsn(new)    ── (3) nothing between ─

    verify_offset_file(new_point, tolerate_stale=not flush_ok)   # P1', §4.3
    resume_point = new_point
    reset_group_state()
```

Three things about this ordering, because the whole correctness argument rests on
them, and they are *different* from rev 1's:

* **The acknowledgement happens after the commit, not inside it.** Nothing can
  put an offset into Debezium's store before MotherDuck has committed. That is
  Invariant O (§4.1), and it is what makes principles (1) and (2) structural.
* **The resume point is ours, written by us, inside our transaction.** We no
  longer copy Debezium's serialised bytes *as the source of truth* (rev 1's P1).
  We keep the bytes as a redundant column and compare them every group (§4.3), so
  format drift is caught continuously rather than assumed away.
* **Principle (3) is satisfied better than in rev 1**: between `COMMIT` and the
  slot advancing there is one local file write and one poll iteration, and the
  *dangerous* window (Debezium holding an unconfirmed offset) is **zero-length**.

### 3.7 Getting the committer

pydbzengine's `PythonChangeConsumer` calls `markProcessed`/`markBatchFinished`
itself, does not pass the committer to the handler, and discards the flush
outcome (`repos/pydbzengine/pydbzengine/_jvm.py:109-130`). The applier therefore
replaces the consumer: `DebeziumJsonEngine.consumer` is a `cached_property` and
`SupervisedDebeziumEngine` already overrides `engine`, so we supply our own
`@jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")` object.
No fork of pydbzengine is needed. **This landed early** (TODO 1.0(feedback),
`src/cdc_flight/consumer.py`) precisely so the discarded flush boolean (Opus B2)
stops being invisible before the applier is written.

---

## 4. D3 — Invariant O, and the exactly-once argument

### 4.1 The invariant

> **Invariant O.** At every instant, every offset reachable through Debezium's
> `OffsetStorageReader` / `OffsetStorageWriter` — and therefore every offset any
> of L1/L2/L3 could confirm to Postgres — corresponds to data already **committed**
> in MotherDuck.

If Invariant O holds, it does not matter who flushes, when, or on which thread:
Debezium is *incapable* of confirming an LSN that MotherDuck has not committed.
Principles (1) and (2) hold by construction rather than by schedule. Note also
that the poll loop's own `markBatchFinished()` on empty polls
(`AsyncEmbeddedEngine.java:1320-1325`) becomes harmless — which is the tell that
the property is structural.

**Why rev 1's P2 was withdrawn rather than amended.** P2 asserted that the
standby status update "happens on the next `poll()`, on this same thread, which
cannot run until `commit_group` has returned". That is true of L1 and false of L2
and L3 (§1.2). It was load-bearing for every "no / no" in rev 1's matrix, and its
failure mode is *loss*, not duplication: `markProcessed()` inside the open
transaction + an apply failure ⇒ L3 flushes and confirms `W′` while MotherDuck
holds only `W`. Rev 1's own revisit trigger ("if the single-poll-thread invariant
is ever violated…") had therefore already fired, in the present tense.

**Option A — invert the order (chosen).** `markProcessed()` /
`markBatchFinished()` run only after the MotherDuck `COMMIT` returns (§3.6). Pure
Python, no Java build step, and the slot keeps advancing continuously so
principle (3) stays tight. Its one cost — we must produce the resume point
ourselves — is answered by the continuous verification in §4.3.

**Option B — a Java `io.debezium.spi.storage.OffsetStore` backed by the
MotherDuck table (live fallback, no longer "rejected").** `set()` runs inline on
the calling thread (`DefaultOffsetStorageWriter.java:134`), the engine blocks on
the returned future (`AsyncEmbeddedEngine.java:918`), and a failure aborts
cleanly via `cancelFlush()` (`:926-930`). It makes principle (4) *literal*: the
offset write happens on our connection inside our transaction, and reads
(`getPreviousOffsets()` on every path) return only committed rows, so Invariant O
is enforced by the database rather than by us. The objection is packaging:
Debezium instantiates the store reflectively by class name
(`AsyncEmbeddedEngine.createAndStartOffsetStore`, `:844-891`), and a JPype proxy
has no Java class name, so it needs a compiled `.class` shipped inside what must
be a pure-Python Flight (9.1).

Rev 1 rejected Option B on that packaging cost. Rev 2 does **not**: principle (1)
does not permit trading a correctness property for packaging convenience. Option
B is selected the moment Option A's verification (§4.3) proves unreliable, and it
is ~120 lines with `JdbcOffsetBackingStore` as a template.

**`lsn.flush.mode=manual` — the containment switch.** With `manual`,
`flushLsn()` is a no-op on *every* path and *every* thread
(`PostgresConnectorConfig.java:1522-1524`,
`PostgresReplicationConnection.java:1032-1046`). The slot then only advances when
*we* advance it, from the value in the committed
`_cdc_flight.debezium_offsets` row. It is not the default because
`pg_replication_slot_advance()` refuses to act on an **active** slot, so the
external advance can only happen at `--window` boundaries under D4 — which
trades bounded extra WAL retention (one window, default 600 s) against principle
(3). It is:

* the switch to set the instant the §4.7 guard test ever trips;
* the mode to use during any operation where Invariant O cannot be guaranteed
  (a backfill swap that spans an engine restart, a schema migration);
* an explicit, tested configuration (`CDC_LSN_FLUSH_MODE`), not a hypothetical.

### 4.2 Detecting a silently-failed flush (Opus B2)

`markBatchFinished()` returning normally does **not** mean the offset file
advanced (§1.2). Under Invariant O this can no longer cause loss — the file only
ever lags the truth — but it must not be invisible, because it silently converts
"resume at `W′`" into "resume at `W`", i.e. an avoidable replay, and because it
is the canary for a broken offset store.

`mark_batch_finished_checked()` therefore:

1. snapshots `(size, mtime_ns, sha256)` of `offsets.dat` before the call;
2. calls `markBatchFinished()`;
3. re-reads the file, and if `offset.commit.policy` is
   `AlwaysCommitOffsetPolicy` (which we set, so a flush is always expected after
   at least one `markProcessed`) and the file did not change, raises
   `OffsetFlushFailed`.

Because this runs **after** the `COMMIT`, raising here is safe: the data and our
resume point are already durable, the process exits non-zero, and start-up
rebuilds the file from the table (§4.5). Rev 1 put this check before the offset
insert and used it to *roll back*; under Invariant O rolling back would throw
away already-durable work.

`FileOffsetBackingStore` is also not an atomic writer, so a crash *during* the
write can leave a truncated file. Under §4.5 that is a "file is corrupt" case and
resolves to "rebuild from the table".

### 4.3 P1′ — the resume point is ours, the bytes are the check

Rev 1's P1 was *"we copy Debezium's authentic bytes, so there is no format
drift"*. Rev 2 inverts the roles:

* `resume_json` — **our** serialisation of `{partition, offset}` plus decoded
  `last_lsn` / `last_txn_id` / `last_total_order`. This is the source of truth
  and what §4.5 rebuilds `offsets.dat` from.
* `offset_blob` — the verbatim `offsets.dat` bytes *as observed after the
  acknowledgement*, stored on the **next** group's transaction (it does not exist
  yet when the current group commits). Redundant, kept for forensics.
* `verify_offset_file(new_point)` — after every acknowledgement, deserialise
  `offsets.dat` and assert it equals `serialize(new_point)` byte-for-byte.
  A mismatch is fatal (`ResumePointDrift`).

This turns rev 1's assumption into a **continuously-checked assertion**, which is
strictly stronger. A Debezium upgrade that changes the offset map fails loudly on
the first commit group instead of silently on the first restart.

### 4.4 The idempotency fence

Correctness does not depend on it; two cheap fences are kept as defence in depth:

1. Debezium itself discards messages at or below the last processed LSN when it
   resumes, because Postgres restarts the stream at `restart_lsn`, which is
   behind `confirmed_flush_lsn`.
2. The applier drops any **unit** whose `last_lsn` is `<=` the loaded
   `resume_point.last_lsn`. Unit granularity (not event granularity) is
   deliberate: replay always restarts at a unit boundary, so a partially-fenced
   unit is not a state that can occur. Where an exact event comparison is needed
   the persisted `(last_lsn, last_txn_id, last_total_order)` triple is the
   complete key — rev 1 stored only `last_lsn`/`last_tx_id` while its prose
   compared `(lsn, seq)` (Codex 3).

### 4.5 Start-up reconciliation — the decision table (Opus M5)

**Rule: `offsets.dat` is never a source of truth.** It is a scratch
serialisation buffer that Debezium happens to require on disk.

| `offsets.dat` | `_cdc_flight.debezium_offsets` row | Decision | Why |
|---|---|---|---|
| absent | absent | **fresh start**: snapshot per `snapshot.mode` | nothing durable anywhere |
| absent | present | **write the file from `resume_json`**, resume at `last_lsn` | the table is the truth |
| present, decodes, == table | present | resume | the normal case |
| present, decodes, **ahead of** table | present | **overwrite the file from the table**, resume at the table's `last_lsn`; log `warning offset_file_ahead` | the extra offset was never durable (F3/F4/F8) |
| present, decodes, **behind** table | present | overwrite from the table, resume at the table's `last_lsn` | a lagging flush (§4.2); the later position *is* durable |
| present, corrupt/truncated | present | overwrite from the table | crash during the non-atomic file write |
| present (any state) | **absent** | **REFUSE TO START.** `critical` alert `orphan_offset_file`; the operator must either point at the right MotherDuck database or pass `--accept-orphan-offsets`, which **forces a re-snapshot** and deletes the file | the file may be arbitrarily ahead of anything in MotherDuck ⇒ trusting it is silent loss. This is exactly what happens the first time the applier runs on a machine that already has a baseline `.cdc_state/offsets.dat` — and this repo has one in the working tree today. |
| absent | absent, **but the slot exists** | **re-snapshot** (1.8): the slot has a `confirmed_flush_lsn` we cannot account for | |
| any | present, but `slot.confirmed_flush_lsn > resume_point.last_lsn` | **loss has already happened** ⇒ `critical` alert `slot_ahead_of_destination`, route to 1.8's automatic re-snapshot | this is the Invariant-O guard (§4.7) evaluated at start-up |

No cell in that table is "trust the file".

### 4.6 Crash / failure matrix, rebuilt on Invariant O

`W` = the durable resume point in `_cdc_flight.debezium_offsets`;
`W′` = the resume point this group would establish. "Replay" means the events are
re-delivered by Postgres and re-applied; because the destination transaction that
would have contained them was rolled back, re-applying is not a duplicate.

**The proof is one line, and it is the same line for every row:** nothing enters
Debezium's offset store until after `COMMIT`, so on every path L1/L2/L3 the
offset store is a subset of what MotherDuck has committed; loss requires the slot
to advance past durable data (impossible by Invariant O) and duplication requires
the engine to resume before `W` (impossible because `W` is what we hand it).

#### Streaming phase

| # | Failure point | MotherDuck | `debezium_offsets` | `offsets.dat` | Slot `confirmed_flush` | On restart | Dup | Loss |
|---|---|---|---|---|---|---|---|---|
| F1 | after decode, before `BEGIN` | unchanged | `W` | `W` | `≤ W` | resume at `W` | no | no |
| F2 | mid-`apply_units`, transaction open | rolled back | `W` | `W` | `≤ W` | resume at `W`, replay | no | no |
| F3 | during spill staging | rolled back (staging included) | `W` | `W` | `≤ W` | resume at `W`, replay | no | no |
| F4 | after the `debezium_offsets` INSERT, before `COMMIT` | rolled back | `W` | `W` | `≤ W` | resume at `W` | no | no |
| F5 | **during** `COMMIT` (ambiguous) | all-or-nothing | atomic with the data | `W` | `≤ W` | table says `W` or `W′`; whichever it says is what is durable | no | no |
| F6 | after `COMMIT`, before the first `markProcessed()` | `W′` | `W′` | `W` (stale) | `≤ W` | file rebuilt from the table (§4.5 row 5) ⇒ resume at `W′` | no | no |
| F7 | between `markProcessed()` and `markBatchFinished()` | `W′` | `W′` | `W` | `≤ W` | as F6 | no | no |
| F8 | after `markBatchFinished()`, before the next `poll()` | `W′` | `W′` | `W′` | `≤ W` | resume at `W′` | no | no |
| F9 | after the slot is confirmed | `W′` | `W′` | `W′` | `W′` | nothing to redo | no | no |
| F10 | **apply fails / MotherDuck rejects the COMMIT** ⇒ our exception escapes ⇒ **L3 error teardown** | rolled back | `W` | `W` | `≤ W` | resume at `W`, replay | no | no | 
| F11 | **graceful `engine.close()` mid-group (L2)** — poll thread interrupted, transaction rolls back | rolled back | `W` | `W` | `≤ W` | resume at `W`, replay | no | no |
| F12 | `markBatchFinished()` silently fails to flush | `W′` | `W′` | `W` | `≤ W` | `OffsetFlushFailed` ⇒ non-zero exit; restart rebuilds the file from the table ⇒ resume at `W′` | no | no |
| F13 | crash *during* the `offsets.dat` write (non-atomic) | `W′` | `W′` | truncated | `≤ W` | corrupt-file row of §4.5 ⇒ rebuild ⇒ resume at `W′` | no | no |
| F14 | Postgres slot dropped / offset unusable | unchanged | `W` | `W` | n/a | engine fails to start ⇒ **non-zero exit** ⇒ 1.8 routes it to a re-snapshot | no | no |
| F15 | second instance starts | — | — | — | — | lease renewal inside the group fails ⇒ the loser exits non-zero before writing (4.2) | no | no |
| F16 | slot advanced externally between runs | unchanged | `W` | `W` | `> W` | start-up guard (§4.5 last row) ⇒ `critical` + re-snapshot (1.8) | no | no* |

\* F16 is the one row where loss is *possible in the world* — someone else moved
the slot — and the guarantee is detection plus automatic recovery, which is what
rubric 1.8 asks for.

F10 and F11 are the rows rev 1 got wrong. Under rev 1 they were **loss**
cases, because `markProcessed()` had already run inside the open transaction and
L2/L3 would flush and confirm it. Under Invariant O the offset store is untouched
at that point, so the teardown flush has nothing to make durable.

#### Snapshot phase (Opus B3)

| # | Failure point | Destination | On restart | Dup | Loss |
|---|---|---|---|---|---|
| S1 | mid-chunk, transaction open | shadow table rolled back to the last committed chunk | Debezium re-snapshots; the shadow table is truncated and refilled from scratch under the same `snapshot_epoch` | no | no |
| S2 | between chunk commits | shadow table holds whole chunks only | as S1 | no | no |
| S3 | after the last chunk, before the swap | shadow table complete, live table untouched | `table_state.snapshot_state='in_progress'` ⇒ re-snapshot (or resume, §14.4) | no | no |
| S4 | during the swap transaction | all-or-nothing (§7) | if committed: `snapshot_state='complete'`, stream on; else as S3 | no | no |
| S5 | crash after swap, before ack | live table swapped, resume point durable | file rebuilt from the table (§4.5) | no | no |

The snapshot's safety comes from **D7, not from event identity**: no
partially-snapshotted state is ever visible, so a re-snapshot cannot duplicate
anything.

### 4.7 The guard test

Invariant O is a claim about Debezium's behaviour, so it gets a continuously
evaluated assertion, not a one-off test:

```sql
-- sampled on every destination heartbeat, and at start-up and shutdown
SELECT s.confirmed_flush_lsn, o.last_lsn
FROM pg_replication_slots s, _cdc_flight.debezium_offsets o
WHERE s.slot_name = ? AND o.pipeline = ?;
-- INVARIANT: confirmed_flush_lsn <= last_lsn
```

A violation raises a `critical` alert `slot_ahead_of_destination`, routes to
1.8's re-snapshot, and is the trigger for switching to
`lsn.flush.mode=manual` and/or Option B. Under Option A it should be
unfalsifiable; the point of measuring it continuously is that it turns a silent
loss into a loud one if any of the above is ever wrong. **It must land in the
same task as the applier (TODO 1.1/1.2/1.3), not later** — it is the only
detector for the class of bug that produced this revision.

### 4.8 Schema

```sql
CREATE SCHEMA IF NOT EXISTS _cdc_flight;

-- Our canonical resume point. Written INSIDE the data transaction.
CREATE TABLE IF NOT EXISTS _cdc_flight.debezium_offsets (
    pipeline          VARCHAR     NOT NULL,
    namespace         VARCHAR     NOT NULL,   -- Debezium engine name
    resume_json       VARCHAR     NOT NULL,   -- OUR serialisation: {partition, offset}
    offset_blob       BLOB,                   -- verbatim offsets.dat, redundant (§4.3)
    commit_id         BIGINT      NOT NULL,
    last_lsn          BIGINT      NOT NULL,
    last_txn_id       VARCHAR,
    last_total_order  BIGINT,                 -- transaction.total_order (§6)
    updated_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline, namespace)
);

CREATE SEQUENCE IF NOT EXISTS _cdc_flight.commit_id_seq START 1;

-- One row per MotherDuck transaction. The audit trail for 1.3 / 1.7 / 6.1.
CREATE TABLE IF NOT EXISTS _cdc_flight.commit_log (
    commit_id       BIGINT      PRIMARY KEY,
    pipeline        VARCHAR     NOT NULL,
    runner_id       VARCHAR     NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL,
    trigger         VARCHAR     NOT NULL,  -- events|bytes|time|ddl|shutdown|snapshot_chunk|swap
    unit_count      BIGINT      NOT NULL,  -- whole PG txns (or snapshot chunks)
    event_count     BIGINT      NOT NULL,
    spilled         BOOLEAN     NOT NULL DEFAULT false,
    first_txn_id    VARCHAR,
    last_txn_id     VARCHAR,
    first_lsn       BIGINT,
    last_lsn        BIGINT,
    max_source_ts   TIMESTAMPTZ,           -- feeds end-to-end lag (5.2, 6.1)
    tables_touched  VARCHAR[]
);

-- Single-writer lease (4.2). Renewed inside every commit group.
CREATE TABLE IF NOT EXISTS _cdc_flight.lease (
    pipeline        VARCHAR     PRIMARY KEY,
    owner_id        VARCHAR     NOT NULL,
    host            VARCHAR,
    pid             BIGINT,
    acquired_at     TIMESTAMPTZ NOT NULL,
    renewed_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);

-- Destination-side heartbeat / health (4.5, 4.6, 6.1, 6.2). Written on a
-- SEPARATE connection, deliberately outside the commit group: an observability
-- signal must survive a rolled-back apply.
CREATE TABLE IF NOT EXISTS _cdc_flight.heartbeat (
    pipeline                 VARCHAR     NOT NULL,
    runner_id                VARCHAR     NOT NULL,
    beat_at                  TIMESTAMPTZ NOT NULL,
    phase                    VARCHAR     NOT NULL,  -- rev 9: `machines.RUN_PHASE`, validated
                                            -- starting|reconciling|recovering|snapshotting|
                                            -- streaming|draining|stopping|stopped|failed
                                            -- (the rev-1 vocabulary was never implemented;
                                            --  this one is, by `cdc_flight.run_state`)
    phase_since              TIMESTAMPTZ,           -- rev 9
    terminal_reason          VARCHAR,               -- rev 9: `machines.RUN_OUTCOME`, a PRECEDENCE
    phase_history            VARCHAR,               -- rev 9: the phases this run passed through
    last_event_at            TIMESTAMPTZ,
    last_commit_id           BIGINT,
    last_commit_at           TIMESTAMPTZ,
    buffered_events          BIGINT,
    buffered_bytes           BIGINT,
    connector_state          VARCHAR,               -- RUNNING|RESTARTING|STOPPED (4.x, B5)
    slot_active              BOOLEAN,
    slot_restart_lsn         BIGINT,
    slot_confirmed_flush_lsn BIGINT,
    slot_retained_bytes      BIGINT,
    source_heartbeat_at      TIMESTAMPTZ,           -- §9.2 (was referenced, never declared)
    source_heartbeat_error   VARCHAR,               -- §9.2
    lag_seconds              DOUBLE,
    PRIMARY KEY (pipeline, runner_id, beat_at)
);

-- Per-table configuration and progress (3.2, 3.4, 3.5, 3.7, 8.1, 8.2).
CREATE TABLE IF NOT EXISTS _cdc_flight.table_state (
    pipeline        VARCHAR     NOT NULL,
    source_schema   VARCHAR     NOT NULL,
    source_table    VARCHAR     NOT NULL,
    refresh_mode    VARCHAR     NOT NULL,   -- cdc|full_refresh|incremental
    delete_mode     VARCHAR     NOT NULL,   -- hard|soft
    history_mode    VARCHAR     NOT NULL,   -- none|changelog|scd2|both
    key_strategy    VARCHAR     NOT NULL,   -- pk|synthetic
    key_columns     VARCHAR[],
    snapshot_state  VARCHAR     NOT NULL,   -- none|in_progress|complete|awaiting_snapshot
                                            -- rev 9: this is `machines.TABLE_LIFECYCLE`,
                                            --  with `absent` as the pseudo-state for "no
                                            --  row". ONE writer (`table_lifecycle.py`),
                                            --  every write edge-checked, every read
                                            --  parsed. See §20/A55 and §A51.1.
                                            -- (rev 8: `failed` withdrawn - nothing ever
                                            --  wrote it; `awaiting_snapshot` added - the
                                            --  whole re-snapshot queue runs on it and it
                                            --  was never in the declared domain. Frozen in
                                            --  `destination.SNAPSHOT_STATES` and validated
                                            --  on read.)
    snapshot_epoch  BIGINT      NOT NULL DEFAULT 0,  -- §3.5 event identity
    snapshot_lsn    BIGINT,                 -- 1.6: where the snapshot hands over
    last_commit_id  BIGINT,
    PRIMARY KEY (pipeline, source_schema, source_table)
);

-- Resumable backfill chunks (3.1, 3.7).
CREATE TABLE IF NOT EXISTS _cdc_flight.backfill_chunks (
    pipeline        VARCHAR     NOT NULL,
    source_schema   VARCHAR     NOT NULL,
    source_table    VARCHAR     NOT NULL,
    chunk_id        BIGINT      NOT NULL,
    low_key         VARCHAR,
    high_key        VARCHAR,
    state           VARCHAR     NOT NULL,   -- pending|running|done
    rows_loaded     BIGINT,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline, source_schema, source_table, chunk_id)
);

-- Alert evaluations (6.2).
CREATE TABLE IF NOT EXISTS _cdc_flight.alerts (
    pipeline        VARCHAR     NOT NULL,
    raised_at       TIMESTAMPTZ NOT NULL,
    severity        VARCHAR     NOT NULL,   -- info|warning|critical
    code            VARCHAR     NOT NULL,
    message         VARCHAR     NOT NULL,
    context         JSON
);
```

### 4.9 Columns added to every replicated table

| column | source | purpose |
|---|---|---|
| `cdcf_commit_id` | applier | which MotherDuck transaction made this row visible (1.3) |
| `cdcf_event_id` | applier | stable event identity, unique per change event (§6) |
| `cdcf_total_order` | envelope `transaction.total_order` | ordinal within the Postgres transaction (ordering, SCD2) |
| `dbz_op`, `dbz_lsn`, `dbz_tx_id`, `dbz_schema`, `dbz_table`, `dbz_source_ts_ms` | envelope | unchanged from the baseline, deliberately — existing tests and probes key off them |

`cdcf_*` is applier-owned, `dbz_*` is source-derived. Rev 1 called the ordinal
column `cdcf_seq` and sourced it from "the envelope"; no such field exists
(Opus m8), hence the rename and the explicit provenance.

### 4.10 Debezium settings this decision fixes

```properties
provide.transaction.metadata      = true     # §3.2 — mandatory, not optional
offset.commit.policy              = io.debezium.engine.spi.OffsetCommitPolicy$AlwaysCommitOffsetPolicy
offset.flush.timeout.ms           = 5000     # see below
task.management.timeout.ms        = 30000    # >= offset.flush.timeout.ms  (m10)
lsn.flush.mode                    = connector  # 'manual' is the containment switch, §4.1
```

Rev 1 raised `offset.flush.timeout.ms` to 60 000 "so the engine never abandons a
flush that is waiting on MotherDuck". Two things changed: (a) under Invariant O
the flush no longer waits on MotherDuck at all — it is a local file write — so a
long timeout buys nothing; (b) `stopSourceTasks()` waits only
`task.management.timeout.ms` before `taskService.shutdownNow()`
(`AsyncEngineConfig.java:25`, `:76-80`), so a 60 s flush timeout would be
hard-killed mid-write during shutdown (Opus m10). The two are now aligned, with
the task-management timeout the larger of the pair.

---

## 5. D5 — Consume the full envelope

`transforms=unwrap` (`ExtractNewRecordState`) is removed, and
`value.converter.schemas.enable` becomes `true`, so each record carries its
Connect schema. This is decided **now** because it fixes the shape the applier
consumes; retrofitting it later would rewrite the applier.

What the flattened payload makes impossible today:

| Need | Rubric | Why unwrap blocks it |
|---|---|---|
| `numeric`, `date`, `time`, `interval`, `bytea` mapped natively | 2.4 | the semantic type lives in the Connect schema (`io.debezium.time.MicroTimestamp`, `org.apache.kafka.connect.data.Decimal`), not in the JSON value |
| Distinguish "TOAST not shipped" from a real value | 2.6 | needs `before` vs `after` and the `unavailable.value.placeholder`, both discarded |
| `TRUNCATE` (`op='t'`) | 1.5 | has no `after` image, so the SMT drops it and the handler skips the null payload |
| `pg_logical_emit_message` (`op='m'`) | 7.4 | same — it only survives today by accident, un-unwrapped |
| PK update: old key **and** new key | 1.4 | `before` is discarded |
| Keyless update/delete before-image | 1.2 | `before` is discarded |
| **Transaction metadata** (`transaction.total_order`) | 1.2, 1.3 | the SMT does not add it, and §3.2/§6 both depend on it |

Companion settings: `replace.null.with.default=false` (the p01 finding — the
delete image was fabricated zeros and empty strings, not NULLs),
`skipped.operations=none` (truncate is skipped by default —
`CommonConnectorConfig.java:865-875`), `provide.transaction.metadata=true`
(§3.2), and `max.queue.size.in.bytes` set (5.4).

### 5.1 The throughput risk, stated rather than waved away (Opus M8)

Rev 1 claimed the larger payload "is paid for by the byte-bounded commit trigger
and by not building a Python dict per row twice". That does not address
**per-event parse cost**, which is the dominant term above 2000 events/s.
Re-serialising the Connect schema into every record inflates the JSON by roughly
3–5×, and every one of those bytes is parsed by Python in `handleJsonBatch`. The
measured baseline is ~1 000 rows/s end to end (`RUBRIC_STATUS.md`, p12/p13).

**This is now an explicit measurement task owned by TODO 5.3**, listed in §14.7,
with three candidate mitigations to compare:

1. `orjson` instead of stdlib `json`;
2. `value.converter.schemas.enable=false` plus a per-topic schema fetched once
   (the schema is invariant per topic between DDL changes);
3. the Avro/binary converter, if it can be driven without a schema registry.

If none of them clears 2000 events/s, D5 stands (it is a *correctness*
prerequisite for 2.4/2.6/1.4/1.5/7.4) and 5.3's answer becomes parallel decode,
which is a different decision and a different ADR.

---

## 6. D6 — Event identity, and tables with no key

Rev 1 defined the streaming identity as `f"{lsn}:{tx_id}:{seq}"` with `seq` from
`source.sequence`. That is wrong: `source.sequence` is `[lastCommitLsn, lsn]`,
not an ordinal (`SourceInfo.java:180-196`), and several events can share one LSN.
Two distinct keyless events could then receive the same `cdcf_event_id`, which
makes 1.2 = 5 unreachable (Codex 3).

**Rev 2 identity.**

* **streaming**: `f"{txn_id}:{total_order}"` — from the envelope's `transaction`
  block (`{id, total_order, data_collection_order}`,
  `AbstractTransactionStructMaker.java:41-43`). `total_order` is the connector's
  own 1-based event ordinal within the transaction, restored from the offset on
  restart, so it is stable across a replay of the same WAL. Postgres `txid`s are
  32-bit and wrap, so the stored form is
  `f"{lsn_of_txn_commit}:{txn_id}:{total_order}"` — the commit LSN
  disambiguates a wrapped `txid` and is monotonic.
* **snapshot**: `f"snap:{snapshot_epoch}:{schema}.{table}:{ordinal}"` (§3.5).
* **logical message / heartbeat**: `f"msg:{lsn}:{prefix}"`; these carry no data
  row and exist only in the changelog when 7.4 asks for them.

Because the identity is derived from the connector's own transaction bookkeeping
rather than from anything we count, replay stability does not depend on our
buffer surviving a restart.

Per-table identity comes from `table_state.key_strategy`:

* `pk` — the Postgres primary key (or replica-identity index). Used for
  current-state merges, SCD2 keys, and hard deletes.
* `synthetic` — no PK. `cdcf_event_id` is the row identity. The consequence is
  explicit and must be documented for users: a keyless table can have a faithful
  **changelog**, and a current-state table only if `REPLICA IDENTITY FULL` is
  set, in which case the full before-image is the matching key. Postgres refuses
  to decode UPDATE/DELETE on a keyless table without it, so the applier detects
  that case at start-up (`pg_class.relreplident`) and forces
  `history_mode='changelog'` for the table, raising a `warning` alert rather
  than silently producing a wrong current-state table.

**The decisive keyless test** (Opus M6, Codex 8) is not "no duplicates": it is
that two *genuinely identical* source rows both survive while a crash-replay copy
does not. `tests/1.2_exactly_once_nopk/` now contains that case explicitly
(`test_target_identical_source_rows_both_survive`), because every count/DISTINCT
assertion is otherwise satisfiable by a `SELECT DISTINCT` that is *wrong* for
keyless tables.

### 6.1 Primary-key updates (1.4)

With the full envelope a PK update is unambiguous: either two events in one
transaction (`d` on the old key, `c` on the new one — what `probes/p01`
observed), or one `u` whose `before.key != after.key` under
`REPLICA IDENTITY FULL`. The applier normalises both to *delete old, insert new*
and applies them **inside one commit group**, so no consumer ever sees the row
under two keys — which is exactly the "duplication=2" the baseline scores.
`replace.null.with.default=false` stops the delete image resurrecting the row
with zeroed columns.

---

## 7. D7 — Backfills as shadow tables

A backfill writes to `<table>__cdcf_tmp` and never to the live table. The swap is
one MotherDuck transaction:

```sql
BEGIN TRANSACTION;
  -- one statement per member of the swap set, see below
  DROP TABLE IF EXISTS cdcflight_app_customers;
  ALTER TABLE cdcflight_app_customers__cdcf_tmp RENAME TO cdcflight_app_customers;
  -- ... child tables ...
  INSERT INTO _cdc_flight.commit_log (...) VALUES (..., 'swap', ...);
  UPDATE _cdc_flight.table_state SET snapshot_state='complete', snapshot_lsn=?;
  INSERT OR REPLACE INTO _cdc_flight.debezium_offsets (...) VALUES (...);
COMMIT;
```

Three details that are easy to get wrong:

1. **The swap set is not one table.** Until rubric 2.4 turns Postgres arrays into
   DuckDB `LIST`s, child tables exist —
   `cdcflight_app_customers__tags`, `cdcflight_app_orders__quantities`, and so
   on. The swap must move the root **and every table whose name starts with
   `<root>__`**, enumerated from `information_schema.tables` *inside* the
   transaction so the set cannot change underneath it. Once 2.4 lands, the set
   collapses to the root table and the same code keeps working.
2. **CDC does not stop.** Events that arrive during the backfill are applied to
   the `__cdcf_tmp` table (the applier resolves the target through
   `table_state.snapshot_state`), so at swap time the shadow table is already
   caught up and the switch is instantaneous. This is what makes 3.3 "simple and
   elegant": there is one write path, and only the *name* it resolves to changes.
3. **The resume point rides in the swap transaction** for the same reason it
   rides in every other commit group: a crash mid-swap must leave neither a
   half-swapped table nor an advanced offset. And per §3.5 this is what makes the
   snapshot phase idempotent at all.

`snapshot_lsn` is what makes 1.6 provable: the snapshot's exported-snapshot LSN
is recorded in the same transaction as the swap, so "where the backfill ends and
the stream begins" is a queryable fact rather than an assumption.

If MotherDuck turns out not to support transactional `DROP`/`RENAME`, the
fallback inside the same transaction is
`CREATE OR REPLACE TABLE <t> AS SELECT * FROM <t>__cdcf_tmp` — the rubric's
"BEGIN/COMMIT transactionality fine too" wording explicitly allows it. **This
must be verified before 3.2 is implemented; it is the single biggest unknown in
this ADR.** Note that dlt already encodes a per-destination answer to this
question via `capabilities.supports_ddl_transactions` and `maybe_transaction`
(`repos/dlt/dlt/destinations/job_client_impl.py:110-123`) — see §10.

---

## 8. D8 — Output shapes: current state, changelog, SCD2

Per table, `table_state.history_mode` selects any combination of:

| shape | physical table | written how |
|---|---|---|
| current state | `<table>` | merge on the identity key; `delete_mode=hard` removes the row, `soft` sets `cdcf_deleted_at` |
| changelog | `<table>__changelog` | append-only, one row per change event, full `cdcf_*`/`dbz_*` metadata |
| SCD2 | `<table>__scd2` | `valid_from`, `valid_to`, `is_current`, plus the identity key |

All three are written **inside the same commit group transaction**, which is
what makes 8.2's "both current state and changelog" consistent: a reader can
join them at any instant and they agree. The changelog is also the *evidence*
rubric 1.1 needs — an idempotent current-state `MERGE` can mask duplicate
delivery, an append-only changelog cannot.

SCD2 mechanics: within a group, events are ordered by
`(lsn, txn_id, total_order)`; for each identity key the applier closes the open
interval and opens a new one. Rev 1 keyed the interval end on the *next event's
source timestamp*; Debezium's source timestamp is millisecond-resolution, so two
events on one key inside one millisecond produce zero-length or overlapping
intervals (Opus m6). **Intervals are therefore keyed on
`(lsn, txn_id, total_order)` and the timestamp is carried as an attribute.**

A key touched N times in one group produces N intervals, not one — the group is a
commit boundary, not a compaction boundary. Because groups contain whole
Postgres transactions (Invariant A), SCD2 intervals never split a transaction, so
a multi-table point-in-time query is consistent. That is why 8.2 depends on 1.3
and not the other way round.

`dlt.destinations.sql_jobs.gen_scd2_sql` already implements this shape and is
tested; §10 keeps it on the table rather than hand-writing it.

---

## 9. D9 — Two heartbeats

The rubric asks for heartbeats to do two unrelated jobs, and conflating them is
why the baseline has neither. They are separated:

### 9.1 Destination heartbeat — liveness and observability (4.5, 4.6, 6.1, 6.2)

A supervisor thread writes a row to `_cdc_flight.heartbeat` every
`HEARTBEAT_INTERVAL` (default 5 s) **on its own MotherDuck connection**, outside
the commit-group transaction. Each beat carries the phase, the connector state,
the newest source timestamp seen, buffered depth, and the slot's `restart_lsn` /
`confirmed_flush_lsn` / retained bytes read from `pg_replication_slots`.

Consequences:

* **4.5 (hangs)** — the beat is the watchdog input. If the applier has been in
  `applying` for more than `HANG_TIMEOUT`, or the poll thread has not advanced
  `last_event_at` while the slot shows a backlog, the supervisor tears the
  process down with a **non-zero** exit.
* **4.6 (silently-dead Postgres)** — the beat's slot query runs against Postgres
  on a connection with an explicit socket timeout; a dead node fails the beat
  within one interval, so detection is seconds, not the ~2 h of OS TCP defaults.
* **6.1 / 6.2** — lag (`now() - max_source_ts`) and retained WAL are already in
  MotherDuck, so alert rules are `SELECT`s over `_cdc_flight.heartbeat` written
  to `_cdc_flight.alerts`.
* **"idle" is a source-corroborated condition, not a timer** (Opus B5). The
  measured failure was: kill the walsender, Debezium enters a 10 s retriable
  restart backoff, no batches arrive, the 8 s idle detector fires, and the run
  reports `ok:true` with 118 785 of 250 000 rows. A timer alone cannot tell
  "nothing left to do" from "not currently connected". The supervisor therefore
  requires, in addition to the quiet timer, that the slot is **active** and that
  `pg_current_wal_lsn() - confirmed_flush_lsn` is below a small threshold. This
  landed early (TODO 1.0(feedback), `src/cdc_flight/source_health.py`) because
  every §4/§6 measurement depends on it.

Deliberately **not** transactional with the data: a health signal that
disappears when the apply rolls back is exactly the signal you need most.

### 9.2 Source heartbeat — advancing an idle slot (4.4) without disturbing 7.2

When only a subset of tables is captured and the rest of the cluster is busy, the
WAL advances but our stream produces no events, so no offset is committed and
`confirmed_flush_lsn` stalls — Postgres then retains WAL indefinitely. Debezium
documents this precisely (`postgresql.adoc:4601-4616`).

The obvious implementation, `heartbeat.action.query`, is **rejected**: Debezium
executes that query on its own connection, which is the connection it streams
from. If that connection points at a hot standby (rubric 7.2) the write fails and
the connector errors. Building the heartbeat on it would make 4.4 and 7.2
mutually exclusive.

#### Dual-connection topology

```
                     ┌───────────────────────────────┐
   PRIMARY ──WAL──▶  │  REPLICA (hot standby)        │
      ▲              │  logical slot cdc_flight_slot │
      │              └───────────────┬───────────────┘
      │                              │  START_REPLICATION (pgoutput)
      │ heartbeat write              │  read-only, no writes ever
      │ pg_logical_emit_message      ▼
      │                    ┌───────────────────────┐
      └────────────────────┤  cdc_flight process   │
        separate psycopg   │  supervisor + applier │
        connection, ours,  └───────────┬───────────┘
        not Debezium's                 │ commit groups
                                       ▼
                                  MotherDuck
```

The complete source topology is therefore **three** connections, not two
(Codex 5): Debezium's replication connection to the replica, our psycopg
*monitoring* connection to the replica (the slot lives there, so
`pg_replication_slots` must be read there), and our psycopg *heartbeat*
connection to the primary.

* **Streaming connection** — owned by Debezium, configured by
  `database.hostname` / `database.port`. Strictly read-only: no
  `heartbeat.action.query`, no signal-table writes, and
  `publication.autocreate.mode=disabled` so Debezium never attempts DDL there.
* **Monitoring connection** — ours, to the host that owns the slot (the replica
  in replica mode). Feeds §9.1 and §4.7.
* **Heartbeat connection** — ours, to the **primary**. Every
  `HEARTBEAT_INTERVAL_MS` it runs exactly one statement:

  ```sql
  SELECT pg_logical_emit_message(true, 'cdcf_idle_heartbeat', <payload>);
  ```

  **Corrected in rev 6 (§18/A42): `true`, not `false`.** A33's measurement is a
  constraint on this heartbeat, not a local detail of the catalog fence — a
  non-transactional message does not end `WalPositionLocator.resumeFromLsn`'s search
  after a restart, so it cannot be relied on to advance an idle slot. It is emitted
  through the one shared `cdc_flight.source_marker`, which already owns the capability
  probe, the error state and the write budget. The message is still a single tiny WAL
  record (a BEGIN/message/COMMIT rather than one record) — no table, no rows, no
  bloat, no vacuum load. It replicates to the standby physically, is decoded
  there by our logical slot, and arrives on our stream as an `op='m'` event.
  `probes/p01` already proved such messages land, which is why 4.4 and 7.4 are
  one piece of work.
* **In replica mode the primary endpoint is required, not defaulted.** Rev 1
  defaulted `CDC_PRIMARY_*` to the streaming source; in replica mode that
  silently degrades to `passive` and therefore below 4.4 = 5 (Codex 5). Rev 2:
  `CDC_PRIMARY_HOST` defaults to the streaming source **only when
  `pg_is_in_recovery()` is false on that host**; if the streaming host is a
  standby and no primary is configured, start-up fails with
  `heartbeat_primary_required`.

Debezium config:

```properties
heartbeat.interval.ms = 10000        # keeps the connector's own status updates flowing
# heartbeat.action.query intentionally NOT set - see above
logical_decoding_message.prefix.include.list = cdcf_hb,app_.*
```

New settings in `config.py`:

| setting | default | meaning |
|---|---|---|
| `CDC_PRIMARY_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DBNAME` | the streaming source, iff it is not in recovery | where heartbeat writes go |
| `CDC_HEARTBEAT_INTERVAL_MS` | 10000 | source heartbeat cadence |
| `CDC_HEARTBEAT_PREFIX` | `cdcf_hb` | logical-message prefix, so 7.4's application messages stay separable |
| `CDC_HEARTBEAT_MODE` | `primary_write` | `primary_write` \| `passive` |
| `CDC_SLOT_RETENTION_WARN_BYTES` / `_CRITICAL_BYTES` | 1 GiB / 8 GiB | when stalled slot advancement becomes an alert |

#### Why this scores 7.2 at 5

The primary receives **one WAL-record-emitting statement every 10 s** and nothing
else — no table, no index, no autovacuum consequence. The standby's
`hot_standby_feedback=on`, which Postgres requires for a logical slot on a
standby, remains the larger of the two costs, and it is a Postgres requirement
rather than something this design adds.

#### Applier handling

* A `cdcf_hb` message is **offset-only**: it becomes a `CompleteUnit` of
  `kind="control"` with no data events — but only when no transaction is open
  (§3.2). It updates `heartbeat.source_heartbeat_at` on the next destination
  beat.
* A commit group whose only content is control units still commits, on the time
  trigger, because the guard is `if group and …`, not `if data_events and …`.
  The resume point advances, the acknowledgement runs, the next poll confirms the
  new LSN — the slot moves with zero business changes. That is 4.4. (Rev 1's
  guard made this branch dead — Codex 5.)
* Round-trip latency between "emitted on the primary" and "seen on our stream" is
  itself a useful metric: it is replica lag plus decode time, and it lands in
  `_cdc_flight.heartbeat` for 6.1.

#### Failure behaviour when the primary heartbeat connection is unavailable

Losing the heartbeat degrades *WAL retention*; it does not threaten correctness.
CDC streaming is **never** stopped because of it.

| condition | response |
|---|---|
| a single emit fails | retry with exponential backoff (1 s → 30 s cap), reconnect on the next attempt; recorded as `heartbeat.source_heartbeat_error` |
| failing for > 3 intervals | `warning` alert `source_heartbeat_unavailable`; streaming continues |
| failing **and** `slot_retained_bytes > CDC_SLOT_RETENTION_WARN_BYTES` | `warning` escalates to describe the real harm (WAL growth) |
| failing **and** `slot_retained_bytes > CDC_SLOT_RETENTION_CRITICAL_BYTES` | `critical` alert; with `CDC_ON_SLOT_RETENTION_CRITICAL=exit` (opt-in, default `alert`) the process exits non-zero |
| primary unreachable but the replica keeps streaming | expected during a failover; the writer keeps retrying and re-resolves `CDC_PRIMARY_HOST` each attempt |
| the heartbeat target is itself in recovery | detected via `pg_is_in_recovery()` on connect; `critical` alert `heartbeat_target_is_standby`, writer degrades to `passive` |

`CDC_HEARTBEAT_MODE=passive` disables the source write entirely. It is the
documented degradation for sources where we have no write access anywhere, and
it is explicitly **not** good enough for a 5 on 4.4.

**Residual edge cases, stated rather than hidden.**

1. *Logical slot on a standby can be invalidated by a recovery conflict.*
   Invalidation surfaces as an engine failure (non-zero, per 1.0(b)) and must
   route to a re-snapshot under 4.1.
2. *Failover.* The slot on a standby does not survive promotion unless
   `slot.failover=true` and the primary lists it in `synchronized_standby_slots`.
   That belongs to 4.1.
3. *Privileges for `pg_logical_emit_message` on Postgres 18* must be verified and
   documented when 4.4 is implemented (§14.3).

---

## 10. D10 — dlt is demoted to a library, not removed

**User decision, 2026-07-30**, adopting the Opus review's recommendation over rev
1's "the honest expectation is that dlt is dropped completely".

Rev 1's structural argument (§3.1) is correct and was verified independently
against the vendored dlt: the **load path** cannot host our transaction. But that
argument applies to `dlt.pipeline.run()`, not to dlt's lower layers, and rev 1
kept exactly the wrong hedge — it retained *type inference*, which D5 genuinely
obsoletes, while discarding the **tested SQL-generation layer**, which D7, D8,
1.4 and Phase 2 would otherwise re-implement by hand (Opus M9).

### Removed from the apply path

`dlt.pipeline.run()`, extract/normalize/load packages, dlt's worker pool, dlt's
per-table transactions, the `_dlt_load_id` / `_dlt_id` columns, and the
`_dlt_loads` / `_dlt_version` / `_dlt_pipeline_state` tables — including, and
especially, **`_dlt_pipeline_state` / `state_sync`**. Its purpose is to keep
resumption state in the destination *outside* the data transaction, which is
precisely the bug this ADR exists to fix; two sources of truth for "where are we"
is worse than one. dlt's array→child-table normalisation also goes, replaced by
2.4's `LIST` mapping.

### Retained as a library, called from inside our transaction

| dlt component | Vendored path | Used for |
|---|---|---|
| `dlt.common.schema` — schema object, evolution diff, **contracts** (`evolve`/`freeze`/`discard_row`/`discard_value`), version hash | `repos/dlt/dlt/common/schema/` | 2.1, 2.2, 2.5. Contracts are a tested vocabulary for "a column changed type mid-stream" |
| `dlt.common.normalizers.naming` | `repos/dlt/dlt/common/normalizers/naming/` | destination identifier stability. The current names (`cdcflight_app_customers`, `…__tags`) are dlt-normalised and D7's swap enumeration depends on the `<root>__<child>` convention; re-deriving it by hand is how silent renames happen |
| `dlt.destinations.sql_jobs` — `SqlMergeFollowupJob.gen_merge_sql`, `gen_scd2_sql` | `repos/dlt/dlt/destinations/sql_jobs.py:186-187`, `:913` | D8 (8.2), 1.4. These are classmethods returning SQL strings from a table chain + a sql_client, so they can be invoked directly against our own connection with no dlt pipeline involved |
| `capabilities.supports_ddl_transactions` / `maybe_transaction` | `repos/dlt/dlt/destinations/job_client_impl.py:110-123` | §14.1's open question already has a per-destination answer and a fallback path here |

### Not retained

Type inference from values (`dlt.common.schema.schema`) — D5's Connect schema is
strictly better, and inference over JSON values is the root cause of half of
2.4's failures. `_dlt_loads` lineage — `_cdc_flight.commit_log` is strictly
richer. `dlt.extract.incremental` — we have a WAL, not a cursor column.

### Exit criterion

The retention is evaluated **before Phase 2 starts**, with an explicit test:
*if calling the generators requires more than ~100 lines of adapter, or if the
adapter has to reach into dlt internals that are not part of its public surface,
drop them and hand-write the SQL.* That decision is recorded in a follow-up ADR
either way. Until then the dependency stays in `pyproject.toml`, and this ADR
does **not** claim dlt will be removed.

What is kept from the blog's design, because it was right: pgoutput with a
version-controlled publication, deterministic destination naming from
`dbz_schema`/`dbz_table`, and a guaranteed process exit.

---

## 11. D4 — Long-running engine, and what it means for the Flight

`probes/p12` measured ~17 s of JVM start + MotherDuck connect before any work
happens, against 15.1 s of actual engine time. A per-run model therefore spends
more than half its life starting up, which puts 5.2 (<30 s end to end) and 5.3
(>2000 TPS; the amortised rate was **157 events/s**) out of reach by
construction, not by tuning.

**Decision.** The engine stays up across commit groups. The shipped process has
two modes:

* `--window <seconds>` (default 600) — the scheduled shape. One JVM, one
  connection pair, many commit groups. The process drains to the **last verified
  `END`** (§3.2), discards the un-`END`ed tail, and exits 0.
* `--forever` — a daemon, for a hosted deployment.

Both are safe to kill at any instant: that is what §4.6 is for. Both take the
`_cdc_flight.lease` before starting the engine and renew it inside every commit
group, so back-to-back scheduled windows that overlap fail predictably (4.2)
instead of silently double-writing (what `probes/p05` observed).

Note that a `--window` boundary makes the L2 graceful-shutdown path the *normal*
termination, which is exactly why F11 had to be a first-class row in §4.6 rather
than an exotic case.

**Consequences for Flight packaging (9.1).** The Flight is a long-window job, not
a per-batch job: the run must be re-entrant (state lives in MotherDuck, not on
local disk — `offsets.dat` is rebuilt from `_cdc_flight.debezium_offsets` at
start, §4.5), it must exit non-zero on failure so the scheduler notices, and it
must tolerate being killed at the window boundary. Anything the Flight runtime
does *not* let us do — hold a JVM for ten minutes, keep a Postgres replication
connection open — becomes a blocking question for 9.1 (§14.4).

---

## 12. Consequences

**Positive.**

* 1.1, 1.2, 1.3 and 1.7 are solved by one mechanism instead of four, and the
  proof is one sentence (Invariant O) instead of a ten-row case analysis.
* 1.6, 3.2, 3.3, 3.7 collapse into the shadow-table swap, which is the same
  transaction machinery — and §3.5 shows the swap is also what makes the snapshot
  phase crash-safe.
* 5.1/5.3 become bulk-ingest problems (one large transaction per 5 s) rather than
  per-batch overhead problems — *subject to §5.1's decode measurement*.
* 6.1/6.2 get their data for free: `commit_log` and `heartbeat` are written by
  the applier itself, so the observability can never disagree with the data.

**Negative / accepted costs.**

* We own the apply SQL that dlt's *load path* used to own. §10 keeps dlt's
  generation layer, so the burden is DDL orchestration and type mapping, not
  MERGE/SCD2 construction.
* We now own the resume-point serialisation (§4.3) rather than copying Debezium's
  bytes. The mitigation is a per-group byte comparison, which is strictly
  stronger than rev 1's untested assumption — but it is code we maintain.
* The in-memory bound is "the largest single Postgres transaction" until spill
  engages (§3.3). That is stated rather than dressed up as a guardrail.
* Buffering whole transactions and a spill state machine are complexity a naive
  batcher does not have.
* A second and third Postgres connection now exist (§9.2). Each is another thing
  that can fail and another credential to configure. The alternative
  (`heartbeat.action.query`) is cheaper but makes 4.4 and 7.2 mutually exclusive.

**Rejected alternatives.**

| Alternative | Why rejected |
|---|---|
| Keep `dlt.run()` and dedupe afterwards with a `merge` on `(lsn, tx_id, pk)` | Does not give 1.3 at all, and keyless tables (1.2) have no merge key. Deduplication also cannot fix a *torn* transaction, and it silently collapses legitimately identical keyless rows. |
| **Rev 1's ordering argument (P2)** | False on 2 of 3 engine lifecycle paths, and its failure mode is loss. **Withdrawn**, not amended. |
| A supervisor↔applier handshake so `close()` cannot run mid-group | Mitigation, not construction — fails principle (1), and cannot cover L3 at all, because there the teardown is initiated by the engine in response to *our* exception. |
| Custom Java `OffsetStore` | **No longer rejected** — it is the named fallback (§4.1, Option B), selected if §4.3's verification proves fragile. |
| `lsn.flush.mode=manual` + `pg_replication_slot_advance()` as the *default* | Postgres refuses to advance an **active** slot, so it only works at window boundaries; that is in tension with principle (3). Retained as the **containment switch** (§4.1), not as the default. |
| A `txId`-change fallback for the final transaction at shutdown | Can commit a partial Postgres transaction — violates Invariant B. Replaced by "discard the un-`END`ed tail". |
| Per-batch process, scheduled every 30 s | ~17 s of the 30 s is JVM start; cannot meet 5.3 and barely meets 5.2. |
| Heartbeat table on the source instead of `pg_logical_emit_message` | Table bloat, vacuum load, and a schema to own — for strictly less capability. |
| `heartbeat.action.query` | Runs on the streaming connection, so it fails against a hot standby. Would make 4.4 and 7.2 mutually exclusive. |

---

## 13. Implementation order

Each step must be able to land with the suite green.

1. **1.0(b)** — engine failures exit non-zero. *(done)*
2. **1.0(feedback)** — Invariant-O groundwork that must exist before the applier:
   our own `ChangeConsumer` with `mark_batch_finished_checked()` (§4.2),
   source-corroborated idle (§9.1), protocol-anchored fault points
   (`pre_commit` / `post_commit_pre_ack` / `post_ack`), and the strengthened
   1.1/1.2/1.3 target tests. *(done)*
3. `_cdc_flight` schema + the start-up reconciliation table (§4.5) + the
   Invariant-O guard test (§4.7). **The guard test lands here, not later.**
4. The `TransactionAssembler` and commit groups behind a flag, DuckDB only,
   append-only output — turns 1.1/1.2/1.3 green.
5. Long-running mode + lease (4.2, 5.2, 5.3), measured against MotherDuck.
6. Full envelope (D5) — **preceded by §5.1's decode measurement** — unblocks 2.4,
   2.6, 1.5, 7.4, 1.4.
7. Shadow-table backfills (D7) + snapshot chunking (§3.5) — 3.2, 3.3, 1.6, 3.7.
8. Heartbeats (D9) — 4.4, 4.5, 4.6, 7.2, and the data for 6.1/6.2.
9. Output shapes (D8) — 8.1, 8.2.
10. Spill (§3.4) — 5.4, and the >200k/>256MB cases of 1.3.

## 14. Open questions that block later phases

1. Does MotherDuck honour `DROP TABLE` / `ALTER TABLE … RENAME` inside a
   transaction? Blocks 3.2. Fallback in §7; dlt has a per-destination answer
   (§10).
2. Measured MotherDuck commit latency **and uncommitted-transaction memory
   behaviour** for a 200 000-row / >256 MB group — sets `COMMIT_MAX_EVENTS`,
   `UNIT_SPILL_BYTES` and `offset.flush.timeout.ms` for real. Blocks 5.3 and
   §3.4.
3. Exact privileges required for `pg_logical_emit_message` on Postgres 18.
   Blocks 4.4's documentation.
4. Whether a MotherDuck Flight can hold a JVM and a replication connection for a
   10-minute window. Blocks 9.1 and, if the answer is no, forces a re-read of D4.
5. End-to-end latency of the primary→standby→our-slot heartbeat round trip, and
   whether a logical slot on a Postgres 18 standby is stable enough over a long
   window (`probes/p09` saw a shutdown hang and only exercised the snapshot
   path). Blocks 7.2 and 4.4's replica story.
6. **(new)** Is marking only a unit's *terminal* raw record sufficient to move the
   Postgres partition offset, or must every record be marked? Determines how much
   memory spill mode can actually release (§3.4). Needs an integration test.
7. **(new)** Full-envelope decode throughput (§5.1). Blocks D5's landing and
   therefore 2.4/2.6/1.4/1.5/7.4. Owned by TODO 5.3.
8. **(new)** Can Debezium resume a *partially completed* snapshot after a crash
   (incremental snapshot / `snapshot.mode=when_needed` semantics), or is a full
   re-snapshot always required? Determines whether §3.5's `snapshot_epoch` ever
   has to survive a restart, and whether 3.7 ("resume midway per table") can use
   the connector's own snapshot at all.

---

## 15. Amendments from the implementation (rev 3, 2026-07-30)

Written while implementing TODO 1.1/1.2/1.3. Each entry is a place where
reality forced a deviation from rev 2, recorded here rather than silently
diverging. Everything not listed below was implemented as specified.

### A1 — `transaction.id` is NOT a transaction identifier (corrects §3.2, §6)

Rev 2 read `AbstractTransactionStructMaker.addTransactionBlock` and concluded
that the envelope's `transaction.id` identifies the transaction. **Measured
against the shipped Debezium 3.6.0.Final + pgoutput, it does not.** It is
`"<txId>:<the LSN at the moment that struct was built>"`, so it differs for
every event of one transaction *and* between `BEGIN` and `END`:

```
BEGIN  {"status":"BEGIN","id":"11115:937926432"}
data   {"transaction":{"id":"11115:937926432","total_order":1,...}}
data   {"transaction":{"id":"11115:937926736","total_order":2,...}}
data   {"transaction":{"id":"11115:937927016","total_order":3,...}}
END    {"status":"END","id":"11115:937927152","event_count":3,
        "data_collections":[{"data_collection":"app.customers","event_count":2},
                            {"data_collection":"app.sensor_readings","event_count":1}]}
```

Taken literally, every multi-event transaction looks like "the txId changed
without an `END`" — which §3.2 correctly declares fatal, so the applier died on
its first real transaction. The stable identifier is the **prefix**, which
equals `source.txId` and equals the offset's `transaction_id`. The assembler
therefore keys on `source.txId` for data events and on the prefix of
`transaction.id` for the markers. `envelope._txn_id()` is that one line, and
`tests/test_assembler.py` pins the behaviour.

Everything else §3.2 relies on was confirmed exactly as documented:
`event_count`, per-`data_collection` counts, `total_order` as a 1-based ordinal,
and snapshot records carrying `"transaction": null`.

### A2 — the Connect schema stays OFF; only the envelope lands here

D5 says `value.converter.schemas.enable` becomes `true`. It is still `false`.
The applier needs the **envelope** (`before` / `after` / `source` / `transaction`
/ `op`) — that is what rubric 1.1/1.2/1.3/1.4 depend on, and dropping
`ExtractNewRecordState` delivers it. It does *not* need the **Connect schema**,
which is what rubric 2.4/2.6 depend on, and which §5.1 flags as an unmeasured
3–5× payload inflation owned by 5.3.

Consequence, stated plainly rather than buried: **type mapping is unchanged from
the baseline and rubric 2.4 is still a 1.** `timestamptz` lands as `VARCHAR`
(the baseline got `TIMESTAMP` only because dlt *inferred* it from the string —
inference the applier deliberately does not do, per §10). `numeric` is still
base64, `date`/`interval` still integers. Two things did improve for free:
arrays are a native `JSON` column instead of a dlt child table, and the all-NaN
`numeric` column that dlt dropped entirely now exists.
`tests/test_e2e_duckdb.py::test_documented_type_gaps` pins all of it.

### A3 — `cdcf_event_id` uses the event's own LSN, not the commit LSN

§6 specifies `"<lsn_of_txn_commit>:<txn_id>:<total_order>"`. The implementation
uses the **event's** LSN. Both are monotonic and both are deterministic on
replay, so the disambiguation §6 wanted (a wrapped 32-bit `txid`) is equally
served. The reason for the change is spill mode: events are staged to
`_cdc_flight.spill_events` *before* the `END` marker exists, so the commit LSN
is not knowable at the moment an identity has to be assigned, and an identity
that differs between the spilled and in-memory paths would be worse than either.

### A4 — `verify_offset_file` becomes "rebuild + one-directional assertion"

§4.3 asks for a byte-for-byte comparison of `offsets.dat` against
`serialize(new_point)` after every acknowledgement. Two problems surfaced:

1. The file legitimately **lags** — `markBatchFinished()` may flush nothing
   (§4.2), and the poll loop flushes on its own schedule — so byte equality
   false-fires on a healthy pipeline.
2. Only one direction signals a bug. The file being *behind* is expected; the
   file being *ahead* of the durable resume point is an Invariant-O violation.

The implementation therefore (a) writes the file itself when reconciliation says
so, byte-compatibly with Kafka's `FileOffsetBackingStore` — the format was read
off a live file and is round-tripped byte-identically in
`tests/test_offset_file.py` — and (b) asserts after every acknowledgement that
`file_lsn <= durable_lsn`, raising `ResumePointDrift` otherwise. That is
strictly the Invariant-O direction and cannot false-fire on a lagging flush.

A related correctness detail that cost a debugging cycle: the Connect offset map
must be round-tripped with its **Java types preserved**. `transaction_id` and
`messageType` are `String`, `lsn`/`txId`/`ts_usec` are `Long`. Coercing
`"11115"` to an integer makes the connector die on start-up with
`ClassCastException: java.lang.Long cannot be cast to java.lang.String`.

### A5 — a drained batch closes the group (adds to §3.3)

§3.6's pseudocode closes a group only on a soft trigger. That deadlocks a quiet
stream: Debezium calls `committer.markBatchFinished()` **directly** on an empty
poll (`AsyncEmbeddedEngine.java:1320-1325`) and never calls the consumer, so a
group holding complete units would sit in memory until the run ended and then be
discarded. The added rule: a batch smaller than `max.batch.size` means the
connector's queue drained, so the group closes now; a full batch means more is
already queued, so accumulation continues up to the soft triggers. This keeps
batching under load and bounds latency at idle, and it can still never split a
unit.

### A6 — a lease whose owner is provably dead is reclaimed (refines §4.6 F15)

F15 assumes the loser of a lease race exits. It does not cover the case that
matters far more often: a runner that was `SIGKILL`ed never released its lease,
so **crash recovery** — the normal path this design exists to make safe — would
have to wait out the TTL. `Lease.acquire` now reclaims a lease whose recorded
host is this host and whose pid no longer exists, and logs a warning when it
does. A lease from any other host is still only released by its TTL, because
there we cannot prove anything.

### A7 — `--reset-state` also clears the destination resume point

With §4.5 in place, deleting only `offsets.dat` produces "absent file, present
row", which correctly resumes instead of re-snapshotting — so `--reset-state`
silently did nothing. It now deletes the `debezium_offsets`, `table_state` and
`lease` rows for the pipeline as well.

### A8 — §14.1 answered for DuckDB, and probed at run time

`destination.probe_transactional_ddl()` runs a real `DROP` + `ALTER … RENAME`
inside a rolled-back transaction at start-up and records the answer in the run
summary. **Local DuckDB 1.5.4: transactional (`transactional_ddl: true`).** The
swap uses `DROP` + `RENAME` when the probe says yes and falls back to
`CREATE OR REPLACE TABLE … AS SELECT` when it says no, which is the fallback §7
already specified and which the rubric explicitly allows. §14.1 stays open only
for MotherDuck until the `motherduck`-marked run confirms it.

### A9 — correctness must not depend on the offsets-file repair

`CDC_OFFSET_FILE_REPAIR=0` disables §4.5's rebuild. The suite runs the
`post_commit_pre_ack` crash both ways: with the repair (no replay at all) and
without it (Postgres re-delivers, and the §4.4 fence drops every re-delivered
unit). If the second case ever failed, exactly-once would have quietly become an
ordering argument again.

### A10 — §10's dlt exit criterion, evaluated

* **Retained:** `dlt.common.normalizers.naming.snake_case`. Three calls, no
  adapter. It keeps every destination identifier byte-identical across this
  migration, which every existing probe and `RUBRIC_STATUS.md` entry depends on.
* **Not retained:** `dlt.destinations.sql_jobs.gen_merge_sql`. Calling it needs a
  dlt `Schema`, a `TTableSchema` chain, a staging dataset and a `SqlClientBase`
  wrapper — comfortably more than §10's "~100 lines of adapter" budget, for a
  merge that is two statements here. This is §10's own exit criterion firing, not
  a change of mind; `gen_scd2_sql` is re-evaluated when 8.2 is implemented.
* **`dlt.common.schema`** is untouched by this task and is re-evaluated before
  Phase 2, as §10 says.

### A11 — snapshot phase details §3.5 did not specify

* A table's shadow is dropped and recreated when its first snapshot record of a
  run arrives, which is what makes a re-snapshot after a crash idempotent.
* A table whose snapshot ends without a `last` marker (Debezium emits
  `last_in_data_collection` per table and `last` only once) is swapped on its
  own marker; **the first non-snapshot record closes the snapshot phase and
  swaps whatever is left.** Without that rule a table could stay behind its
  shadow forever.
* Snapshot units are never fenced by LSN. Every snapshot record of a run carries
  the *same* `source.lsn`, so an LSN fence would drop the entire snapshot after
  the first chunk. Snapshot idempotence comes from D7, exactly as §3.5 says.

### A12 — keyless tables are a changelog, not current state

§6 notes that a keyless table can have a current-state form under
`REPLICA IDENTITY FULL`. This task implements the changelog form only: keyless
tables are keyed on `cdcf_event_id`, so every change event is a row. That is
what makes rubric 1.2 measurable at all (a merge can hide a double delivery; an
append-keyed-on-event-identity table cannot). Keyless current state belongs to
8.1/8.2.

### A13 — fault anchors extended (Codex 9 carry-forward)

`decode`, `begin`, `spill` and `mid_apply` join `pre_commit`,
`post_commit_pre_ack` and `post_ack`; `<nth>` now counts **data-carrying commit
groups** rather than data batches, because the commit group is the unit the
protocol is about. `tests/1.1_exactly_once_pk/test_1_1_fault_matrix.py` crashes
at all five commit-group anchors and asserts no loss, no duplicates and
Invariant O at each.

### A14 — the apply path inserts through Arrow, and this is a correctness matter

The ADR never says how rows reach the destination, and the obvious answer is
wrong by two orders of magnitude. Measured, 200 000 rows x 19 columns into local
DuckDB inside one transaction:

| strategy | time |
|---|---|
| `con.executemany("INSERT … VALUES (?,…)", rows)` | **410 s** |
| chunked multi-row `INSERT … VALUES (…),(…),…` | **> 7 min**, abandoned |
| register a `pyarrow.Table` + `INSERT … SELECT` | **1.37 s** |

and against MotherDuck, 1 500 rows: `executemany` **27.9 s** (a network round
trip *per row*), multi-row `VALUES` 0.65 s, Arrow 1.87 s.

This is not a performance footnote. A commit group holds an integral number of
*whole* Postgres transactions (Invariant B), so a single 200 000-row transaction
is a single group and a single `COMMIT`; at 410 s that group cannot finish inside
any sane deadline, and the run is killed with the transaction open. The first
symptom was `tests/1.1_exactly_once_pk::test_slow_real_sigkill_loses_nothing`
timing out at 300 s, and the second was
`tests/1.3_atomic_batches/test_1_3_motherduck_atomicity.py` reaching its deadline
with **zero** commit groups. `pyarrow` is therefore a hard dependency, and
§14.2's "measured MotherDuck commit latency for a 200 000-row group" is now
partly answered: the write path, not the commit, was the cost.

### A15 — MotherDuck's client caches the catalog per process (affects verification only)

`duckdb.connect()` caches the database instance per DSN inside a process, and
MotherDuck's catalog snapshot rides on that instance. A process that has already
opened `md:<db>` — even on a connection it has since closed — can therefore not
immediately see what another *process* committed. The applier is unaffected (each
run is its own process), but a test that verifies a MotherDuck write from the
parent process will read an empty schema and conclude nothing was written. It is
recorded here because the same trap will catch anyone writing 6.1's observability
queries. `tests/test_motherduck.py::wait_for_tables` handles it.

### A16 — the throughput bugs the whole-transaction design exposes

A commit group holds *whole* Postgres transactions, so one 200 000-event
transaction is one group. That turns anything super-linear in the per-event path
from a slow query into a **run that cannot finish**, and this ADR's design is what
makes that failure mode reachable at all. Three separate defects were found and
fixed by measuring one 200 000-row transaction end to end (local DuckDB, one
commit group, wall clock for the whole run):

| state | wall clock |
|---|---|
| `executemany` insert (A14) | did not finish (410 s in the insert alone) |
| + Arrow insert, spill threshold on the unit's TOTAL size | **239 s** |
| + Arrow insert, spill threshold on total size, spill disabled | **458 s** |
| + all three fixed | **32 s** (spill engaged: 168 885 events staged and drained) |

1. **A14's `executemany`.**
2. **The spill threshold tested the unit's total size, not what was still in
   memory.** Once a large transaction crossed `UNIT_SPILL_BYTES` the threshold
   stayed crossed for every remaining record, so each record became its own
   `INSERT INTO spill_events`. Measured: 12 500 events/s for the first ~88 000,
   then ~1 000 events/s. The threshold now means "spill each time the in-memory
   tail exceeds X", which is what §3.4 intended.
3. **`_TableWork` kept an `order` list and tested `if key not in order` per
   event** — a linear scan, so O(n²) over the group. This was the dominant term:
   458 s → 1.6 s of apply. The ordered `final` dict is both the ordering and the
   membership test.

Two design points worth recording alongside them:

* **Retention.** A unit retains only its most recent record for the
  acknowledgement and releases the Java reference on every earlier one, which is
  safe because `markProcessed()` is a last-write-wins map put
  (`AsyncEmbeddedEngine.java:1361-1366`). That **answers §14.6**: marking only the
  terminal record is sufficient, and `CDC_ACK_EVERY_RECORD=1` restores the
  conservative behaviour for anyone who wants to re-test the claim.
* **Isolated throughput measurements** (200 000-event transaction, same machine),
  which bound where the remaining time goes and partly answer §5.1: raw
  `ChangeEvent` field access ≈ 40 000 events/s; full-envelope `decode()` ≈ 39 400
  events/s; decode **and buffer** ≈ 26 500 events/s. So the full-envelope decode
  is *not* the bottleneck ADR §5.1 feared - at least not at this payload size -
  and 5.3's work should start with the apply path rather than the converter.

### A17 — spill re-opens an Invariant-B hole that §3.4 does not mention

§3.4 says staging and drain happen "in the same transaction, before `COMMIT`", and
concludes that spill therefore weakens nothing. That is true for the unit being
spilled and false for the *group*: a commit group can already hold several whole
transactions when a large one starts spilling, and the group's soft triggers would
then close it and drain the staging table — applying events from a transaction
whose `END` has not arrived. The group itself cannot detect this, because by
construction it contains only whole units; the partial transaction is in the
staging table, not in the group.

The applier therefore refuses to close a group while
`TransactionAssembler.open_unit_has_spilled` is true, and
`tests/test_assembler.py::test_an_open_unit_that_has_spilled_blocks_the_group_from_closing`
pins it. The cost is that the destination transaction stays open until the large
unit completes, which is inherent to §3.4's in-transaction staging and is the
trade §3.4 already records.

---

## 16. Amendments from the 1.1–1.3 review round (rev 4, 2026-07-31)

Written while implementing the union of `reviews/1.1-1.3_codex_review.md`
(4 BLOCKER / 6 MAJOR) and `reviews/1.1-1.3_opus_review.md`
(2 BLOCKER / 8 MAJOR / 16 MINOR). Every entry below corrects something the
reviews *reproduced by running the shipped classes*, not something they
speculated about. Where the two reviews overlapped, the stricter reading won.

The uncomfortable fact this section exists to record: **all four blockers
coexisted with 110 default, 3 slow and 5 MotherDuck tests passing, and lint
clean.** The suite could not see them because each needs a specific interleaving
of assembler and applier state. That is why every fix below ships with a
default-suite guard driven through `tests/applier_lab.py`, which runs the real
`Applier` against a real DuckDB file with a faked `ChangeEvent` and
`RecordCommitter` — the interleaving becomes an argument to a function instead of
a race to win.

### A18 — the ordinal contract, and the keyless-identity disagreement resolved

The two reviews disagreed. Codex 4 called keyless identity a **blocker** and
reproduced two accepted events colliding on `cdcf_event_id`; Opus's attack log
concluded keyless identity is **structurally immune** and signed 1.2 off. The
decisive tests are
`tests/1.2_exactly_once_nopk/test_1_2_keyless_identity.py`, and they show both
reviews were right about different halves of the question:

* **Opus is right about the identity.** `<event lsn>:<source.txId>:<total_order>`
  *is* unique and replay-stable given valid connector metadata:
  `total_order` is a 1-based per-transaction ordinal, the event LSN separates
  transactions, and a replayed transaction renumbers from 1 and recomputes
  *identical* ids — because a resume point can only ever sit on a transaction
  boundary. `test_a_replay_recomputes_the_same_identity_and_cannot_duplicate`
  executes that with the fence disabled, so the merge on `cdcf_event_id` is what
  has to hold, and it does. Opus's reason for it is also better than the one rev 2
  wrote: it is the transaction-boundary property, not the offset restoring
  `TransactionContext`.
* **Codex is right about the input.** The assembler validated only that `txn_id`
  existed. It never required `total_order`, never checked it was positive, and
  never rejected a duplicate — so a stream with missing or repeated ordinals was
  *accepted*, and `test_two_events_that_share_an_ordinal_are_refused` shows two
  same-LSN events both producing `100:7:1` before the fix. Spill made it look
  plausible by substituting a local sequence for `event_seq` while
  `cdcf_event_id` still contained `None`.

So the resolution is neither "it is fine" nor "change the identity". The ordinal
is now a **contract enforced at the boundary**: non-null, integral, ≥ 1, no
duplicates within a transaction, and the observed set exactly `1..event_count`.
`TransactionAssembler` is the only producer of units, so nothing that could
collide can reach the identity builder, and the uniqueness is structural rather
than conventional. `_stream_event_id`'s docstring says so, and says what it
depends on.

Snapshot identity moved with it: the arrival ordinal is assigned **in the
assembler**, when the record arrives, so it is arrival order whether the record is
later spilled or kept in memory. It used to be a counter on the applier's snapshot
state that the spill path incremented separately — see A19.

### A19 — spill: one ordered pass, explicit identity, and the fence (corrects §3.4)

Three findings, one root cause. §3.4 described spill as a change of *storage
representation* that changes nothing about visibility or order. The
implementation did not deliver that.

**Ordering (Opus B-1, reproduced).** `_apply_units` was two passes: write every
in-memory `TableWork`, then drain the staging table. A unit keeps accumulating an
in-memory **tail** after it spills, so its staged rows are *earlier* in source
order than its own tail — and reordering the two passes cannot fix it either,
because a group can hold `unit1 (spilled + tail), unit2 (wholly in memory)` whose
correct order interleaves the two representations. Measured, one PG transaction of
three UPDATEs of one primary key (`a -> b -> c`) with `CDC_UNIT_SPILL_EVENTS=2`:

```
no spill (control)                -> [(1, 'c')]                ok
spill, target table pre-existing  -> [(1, 'b')]                ORDER INVERTED
spill, table created in this txn  -> [(1, 'c'), (1, 'b')]      DUPLICATE PRIMARY KEY
```

The first is silent wrong-final-state *plus* the loss of a change event; the
second is a direct 1.1 violation, and it happens because `fresh` is true in both
passes so both skip the DELETE half of the merge. Reachability was not
theoretical: the headline 200 000-row measurement in A16 ran this exact path with
168 885 events spilled and 31 115 left as the tail, and produced the right answer
only because the workload never touched a key twice.

It is now **one ordered pass**: walk the units in group order and, for each,
load its staged prefix into the *shared* `work` map before collecting its
in-memory tail. One write per destination table, source order preserved end to
end, and the merge sees the whole group at once. This also means the drain is no
longer a separate code path that can drift from the in-memory one — which is what
had left it not updating `table_counts` or `max_source_ts`.

**Routing and identity (Codex 1, reproduced).** `_spill_events` inferred whether
a record was a snapshot record by looking in a mapping that `_apply_units`
populates *later*. On the first spilled chunk of every snapshot that mapping is
empty, so it staged the rows into the **live** table with a `<lsn>:None:None`
streaming identity; a consumer could see a partial snapshot, and the swap then
replaced the live table with a shadow holding only the later chunks. Measured:
`[3, 6]` where `[1, 2, 3, 4, 5, 6]` was expected. The spill callback is now told
the unit identity and the snapshot phase **explicitly**, and resolving the shadow
goes through `SnapshotCoordinator.state_for()`, which creates the shadow, its
`table_state` row and the epoch before anything can be staged.

**The fence (Codex 5).** Rows are staged while the unit is still open; the resume
fence is set at its `END`. Draining unconditionally therefore re-applied the
prefix of a transaction the destination already held, which made A9's "the fence
alone prevents duplication" false for every spilled unit. Staged rows now carry a
`unit_seq`, a fenced unit's prefix is never loaded (and is deleted with the rest,
inside the same transaction), and `has_data` is no longer forced true by rows a
fence is about to discard — which had shifted every `<nth>`-indexed fault anchor
by one.

### A20 — the boundary rule is unconditional (corrects §3.2)

§3.2 says a transaction is complete "only when the marker's `event_count`
**equals** the number of events buffered". Three things made that conditional:

1. a **missing** `event_count` skipped the check entirely and the unit was
   emitted as whole (`declared is not None and ...`). `None` equals nothing;
2. the per-table `data_collections` check was disabled wholesale as soon as any
   event spilled, and the claimed "the drain re-derives them" had no
   corresponding comparison anywhere;
3. the per-table comparison ran in one direction only, so an **observed** table
   the marker never declared was accepted — which is exactly what a misrouted or
   mis-named event looks like.

All three are closed, and the counters the rule is checked against (`count`,
`per_table`, `orders`) are maintained *as records arrive* and never touched by
spilling. So the proof is identical in memory and on disk: nothing about
completeness is conditional on the storage representation any more.

`envelope.decode` also failed open in the one direction that skips the check:
`kind = KIND_TXN_BEGIN if status == "BEGIN" else KIND_TXN_END` turned **any**
unrecognised payload on the transaction topic into an `END` with no
`event_count`, terminating the open transaction with no completeness check at
all. An unrecognised `status` and a malformed payload now raise
`EnvelopeDecodeError`. The module docstring's claim that decode "never raises for
an unexpected payload shape" was both untrue and the wrong goal.

**Transactional logical-decoding messages** are counted now (Opus M-5). Verified
against the vendored source: `LogicalDecodingMessageMonitor.java:106` calls
`transactionMonitor.dataEvent(...)`, so an `op="m"` event *is* in
`END.event_count`, occupies an ordinal, and gets its own `data_collections`
pseudo-entry. It is counted toward the total and the ordinal set, carries no row
of ours, and its declared collection is tolerated by an explicit allowance rather
than by weakening the per-table check. This matters because ADR D9's source
heartbeat is specified as exactly this mechanism, so the assembler had to stop
being fatal for it before D9 lands.

**Incremental snapshots are refused** rather than mishandled (Opus M-7, cheap
half). `snapshot_last` — which swaps *every* shadow over its live table — was set
by any non-snapshot record, and is now set only when Debezium actually said
`last`. Per-table swaps still happen on `snapshot_last_for_table`, so nothing is
lost today; what is removed is a live-table-destruction path that opens the moment
incremental snapshots are enabled. `source.snapshot = "incremental"` is refused
with a message pointing at rubric 3.3, because those records interleave with
streaming events, never carry a `last` marker and carry no `txId`/`lsn` at all.
Full incremental-snapshot support is 3.3's work, not this ADR's.

### A21 — the destination enforces the identity (Opus M-2)

Generated tables carried no `PRIMARY KEY` or `UNIQUE`, so exactly-once was
enforced *procedurally* by the applier and a duplicate identity was not an error.
That is why A19's defects corrupted silently instead of failing. Every table the
applier creates now carries a `PRIMARY KEY` on its identity columns — the source
key columns for a keyed table, `cdcf_event_id` for a keyless one — so principle
(1) is a property of the destination rather than an assertion of ours, and the
whole class of apply-path defect becomes a failed transaction. A failed
transaction is safe: it rolls back and the events replay.

Measured on DuckDB 1.5.4 before committing to it: 200 000 rows through Arrow into
a table with a `PRIMARY KEY` takes 0.03 s, and `DELETE` then `INSERT` of the same
key inside one transaction is accepted, so the merge path is unaffected. Verified
enforced by **MotherDuck** too, not only DuckDB
(`test_motherduck_accepts_the_destination_side_primary_key`). Where a destination
cannot express the constraint, `apply_sql.assert_identity_is_unique` runs inside
the commit group as the documented fallback, and `CDC_DESTINATION_CONSTRAINTS=0`
selects it deliberately.

### A22 — reconciliation compares the whole typed offset map (corrects §4.5)

§4.5's decision table was implemented against a **scalar LSN**.
`offset_file.lsn_of()` returns the first of `("lsn", "lsn_proc", "lsn_commit")`
that is present, and several events share one commit LSN, so a file at
`{lsn: 100, lsn_proc: 999}` and a durable `{lsn: 100, lsn_proc: 1}` produced
`decision="resume"` — the file genuinely ahead within that LSN, and the guard
saying it agreed. Only `parsed[0]` was consulted, so a second entry was invisible,
and the decoded key was never checked against the expected namespace/partition
even though Kafka looks the partition up by exact `ByteBuffer`.

The destination's full partition + typed offset map is canonical now: exactly one
entry, exactly the expected key, and every typed field equal, or the file is
rewritten from the destination (`file_offset_mismatch_rebuilt`, with the
differing fields in the message).

**The row §4.5 named and the code did not have** is also in: *offsets absent,
destination row absent, but the slot exists and has advanced.* That returned
`ok=True` from `check_invariant_o` because `durable is None`. It now refuses to
start unless the configured `snapshot.mode` re-reads every captured table's data
in full, and even then it is reported as its own decision
(`no_durable_row_full_snapshot`) rather than as Invariant-O healthy. With a
non-backfilling mode the connector would stream from the slot's confirmed position
and every change before it would be silently gone.

Related, and the same failure shape: `envelope.offsets_of()` returns
`(None, None)` for every bridge failure, after which `_resume_point_for` paired a
**newer** `last_lsn` with the **previous** offset map. Debezium would resume from
the older offset while our fence claimed the newer LSN was durable, so the replay
would be fenced away — silent loss. A group that would advance `last_lsn` without
a readable terminal Connect offset is now refused; a rollback replays, which is
free.

The codec tests keep a **real Debezium-written `offsets.dat`** as a committed
fixture (`tests/fixtures/offsets_debezium_3.6.dat`). The previous
"byte identical to one Debezium wrote" test created both files with our own
writer, so it proved only that our writer is deterministic.

### A23 — `lsn.flush.mode` is pinned, not inherited (adds to §4.10)

Invariant O holds because `PostgresConnectorTask.performCommit()` re-reads the
offset *backing store* rather than the task's in-memory offset context. Opus B-2
traced the one bypass: with `lsn.flush.mode=connector_and_driver`,
`PostgresReplicationConnection.java:1114-1123` sets `.withAutomaticFlush(true)`
and the shipped pgjdbc then advances the flushed LSN to the **server-supplied**
`lastServerLSN` on keepalives, never consulting the offset store. Debezium's
default is `connector`, which is safe — and "the default happens to be safe" is
precisely the conditional argument rev 2 exists to eliminate. It is pinned, and
`provide.transaction.metadata`, `offset.flush.interval.ms` and it now **refuse**
an override rather than warning about one.

Worth recording as a fragility rather than a finding, per Opus: Invariant O rests
on a **Postgres-connector-specific override**. The generic
`BaseSourceTask.performCommit()` path would violate it. A different connector
would need this re-derived from scratch.

### A24 — the commit→ack window contains nothing else (principle 3, Codex 7)

The post-commit ordering was safe with respect to Invariant O but did not satisfy
the binding principle as implemented: between `COMMIT` and the next poll it ran a
fault lookup that re-read and re-parsed an environment variable, all the
`markProcessed()` calls, `verifier.before()` (which `stat`s and `sha256`s
`offsets.dat`), `markBatchFinished()`, and `verifier.after()` (which hashes it
again).

Now: the fingerprint is taken **before** `COMMIT` — it is a forensic baseline and
never needed to be in the window — and the comparison runs on the *next batch*,
once Debezium has had its poll/commit opportunity, or at shutdown for the last
group. The check is not weakened by deferring it: `markBatchFinished()` on an
empty poll comes from an independent committer that never marked a record, so
`beginFlush()` finds nothing and does not rewrite the file. Only our own
acknowledgement can have moved it. The fault spec is parsed once and cached.

The window now contains the `markProcessed()` calls and `markBatchFinished()`,
and `test_the_acknowledgement_happens_after_the_commit_and_only_after_it` asserts
exactly that sequence.

### A25 — the fault anchors, and what the "22-test matrix" actually was

`mid_apply` was documented as "some tables written, others not" and fired
*before* the table-write loop, so it could not detect a transaction torn between
table A and table B — the one interleaving rubric 1.3 is about (Codex 6). It
fires after the first table write now, and
`test_mid_apply_really_fires_between_two_table_writes` observes the torn state
inside the still-open transaction, immediately before the rollback.

`spill` and `decode` were declared anchors with no test behind them, and A13's
claim that the anchors "bracket every state the commit group passes through" was
therefore false for the two states where the blockers lived. Coverage now:

| where | what |
|---|---|
| default matrix | `begin`, `mid_apply`, `spill`, `pre_commit`, `post_commit_pre_ack`, `post_ack`, each a hard exit plus a recovery run |
| `test_1_1_fault_interleavings.py` (slow) | `decode`; the `raise` action before **and** after `COMMIT` (Debezium's L3 teardown, not process death); a between-table crash whose recovery replays a *spilled* transaction; a crash during the **snapshot** phase; a genuinely unwritable `offsets.dat` |
| `test_1_3_motherduck_fault.py` (motherduck) | `mid_apply` and `post_commit_pre_ack` against real MotherDuck, with exactly-once measured on the keyless changelog |
| `test_1_1_spill_and_snapshot.py`, `test_1_3_commit_protocol.py` (default) | the interleavings themselves, in process |

The headline count is deliberately gone from the claims. What matters is which
states are bracketed, not how many parametrised assertions run over them.

### A26 — `commit_id` is scoped to the pipeline (corrects §4.8)

`commit_log.commit_id` was globally unique and allocated as `max(commit_id) + 1`,
which cannot be atomic on this destination, while leases are per pipeline. Two
*different, valid* pipelines therefore raced into a primary-key failure: the loser
rolled back safely, so it was never a loss hole, but a destination hosting more
than one pipeline could not operate, and a global id was acting as a coordination
mechanism with no global lease (Codex 9). The key is `(pipeline, commit_id)` now,
allocated monotonically per pipeline — the same scope the lease already
guarantees — with a migration for destinations that already carry the global key,
which the shared MotherDuck development database did.

### A27 — a hard crash leaves the lease row locked at MotherDuck (new, measured)

Found by the new MotherDuck fault tests, and not visible before because no
MotherDuck test had ever crashed the pipeline. After a hard crash the dead process
leaves an **uncommitted server-side transaction** that had already touched
`_cdc_flight.lease`, so the next runner's `DELETE` fails with
`TransactionContext Error: Conflict on tuple deletion!` even though the lease is
correctly reclaimable (the pid is provably gone). Safe — the run exits non-zero
having applied nothing — but it made crash *recovery* fail, which is the path this
whole design exists to make routine. The lease write retries a conflict for up to
30 s; the statements are idempotent and run before any data is written.

### A28 — deferrals, stated explicitly

These are the review findings **not** fixed here, with the rubric item that owns
each. None of them is a loss or duplication path.

| finding | owner | why deferred |
|---|---|---|
| Opus M-6 — the lease is renewed only at group start, so a unit that spills for longer than `CDC_LEASE_TTL` lets a second runner `acquire()` | 4.2 | safety currently rests on the destination detecting a write-write conflict on `_cdc_flight.lease` at `COMMIT`, which holds for DuckDB/MotherDuck MVCC but is *emergent, not designed*. Recorded rather than fixed because the fix (renew on a timer inside the open transaction, or derive the TTL from `commit_max_age` + the spill ceiling) belongs with 4.2's concurrency work. |
| Opus M-7 second half — actually supporting incremental snapshots | 3.3 | the guard is in (A20); the support is a design question about interleaved snapshot windows that 3.3 owns. |
| Opus MINOR-11 — the resume point carries no source identity (`system_identifier`/timeline), so a source restored from a base backup or `pg_resetwal` can reuse LSNs below `last_lsn` and genuinely new events would be fenced | 1.8 | `check_invariant_o` detects the slot being *ahead*, not a backward jump. 1.8 owns slot/source divergence and the automatic re-snapshot it routes to. |
| Opus MINOR-15 / the `timestamptz -> VARCHAR` regression | 2.4 / 2.5 | the honest consequence of dropping type inference (A2); `test_documented_type_gaps` pins it. `ensure()` now refuses to ALTER a column whose destination type is outside the widening lattice, so it can no longer narrow one by accident. |
| Keyless tables are a changelog, not current state (A12) | 8.1 / 8.2 | unchanged, and it is what makes 1.2 measurable at all. |
| Spill throughput against MotherDuck | 5.3 | correctness first; the local 200 000-event measurement stands (A16). |
| Probes not migrated to the applier | — | they are baseline-era evidence and `RUBRIC_STATUS.md` now labels them as such where it cites them for §1. |
| Codex's note that `naming.destination_table()` is not injective (two source tables can normalise to one destination name) | 2.3 | not reachable with the captured schema, and a collision registry belongs with automatic table discovery. The narrower case Codex asked to pin - a captured table whose topic collides with `<prefix>.transaction` - is a start-up assertion now (`assert_no_internal_topic_collision`), so the silent-loss shape is closed even though the general injectivity question is not. |
| Keyless update/delete acceptance tests beyond op-mix coverage | 8.1 / 8.2 | `test_e2e_duckdb.py` pins the op mix (`{"r":4,"c":6,"u":4,"d":2}`) and A12 states plainly that the table is a changelog; what "correct update/delete" *means* for a keyless table is the current-state question 8.1/8.2 own. |

### A29 — module decomposition (Codex 8)

`applier.py` reached 1 032 lines owning the Debezium callbacks, the destination
protocol, resume-point capture, PK/keyless apply planning, snapshot epochs and
swaps, spill staging and drain, and the counters. The snapshot-spill blocker was
a direct consequence of those boundaries, so the extraction follows that fault
line rather than a line count:

* `snapshot.py` — `SnapshotCoordinator`: epochs, shadow targets, snapshot
  identity, the swap. `state_for()` is the only entry into the snapshot phase.
* `spill.py` — `SpillBuffer` + `StagedEvent`: takes the identity, target and
  ordinal as **inputs** and infers nothing, which makes the Codex 1 defect
  unexpressible rather than merely fixed.
* `table_work.py` — the apply plan and the one merge mechanism for both table
  shapes.
* `applier.py` (881 lines) — the commit protocol, and only that.

Still on the large side, and stated plainly rather than claimed as small: what
changed is that each module has one owner, and the exactly-once argument can be
read in `applier.py` without following state into three other concerns.

---

## 17. Amendments from 1.4 / 1.5 (rev 5, 2026-07-31)

Written while implementing rubric 1.4 (primary-key updates) and 1.5 (TRUNCATE and
DROP TABLE). §6.1 and the 1.5 gap notes turned out to be *nearly* right; what
follows is what the source and the destination actually required.

### A30 — a key-changing UPDATE is always `d` + `c` for Postgres (corrects §6.1)

§6.1 said a PK update arrives as "either two events in one transaction, or one `u`
whose `before.key != after.key` under `REPLICA IDENTITY FULL`". The second shape does
not exist for this connector: `RelationalChangeRecordEmitter.emitUpdateRecord` splits
the update into `d(old key)` + `c(new key)` whenever `oldKey != null`, and pgoutput
sends the old key whenever the key changed — under `DEFAULT` as well as `FULL`. The
`u` path is kept and tested because other connectors do produce it, and because it is
the shape the applier's own docstring claimed to normalise; it is not the Postgres
path.

Consequence: **the atomicity half of 1.4 is a corollary of §3.2/§3.3 and needs no
mechanism.** The pair is one transaction, a commit group holds whole transactions, and
the merge deletes every touched key before inserting the group's final rows. The
guard test proves it by setting every commit trigger to fire on a single event and
showing the group still cannot close mid-pair.

### A31 — the fold cannot collapse by key alone (new; corrects D6's merge rule)

> **SUPERSEDED by §18/A35 (rev 6).** The probe described below answers "did this key
> exist before this commit **group**?", and the ambiguity is not group-scoped. Worse,
> the falsifier this amendment records is not a hole (three-ring rotations were
> verified to work) while the shape that *was* broken — **one** pre-group row and
> **one** in-group row wearing a key, two rows not three — is not named here at all.
> A falsifier list that points the next agent at a non-hole while the real hole sits
> next to it is worse than no list. Read A35/A36 instead.

D6 defines the keyed apply as "delete every touched key, then insert the final row per
key". That is wrong when **one key is worn by two different rows inside one Postgres
transaction**, which `DEFERRABLE` constraints make legal:

| one transaction | events | truth | collapse-by-key |
|---|---|---|---|
| `UPDATE t SET id = id + 1` over rows 1,2 | `d(1) c(2) d(2) c(3)` | `{2, 3}` | `{3}` — a lost row |
| `UPDATE … id=2 WHERE id=1; UPDATE … id=3 WHERE id=2` | `d(1) c(2) d(2) c(3)` | `{3}` | `{3}` ✓ |

The event streams are byte-identical and the answers differ. The distinguishing fact
is not in the stream: it is whether key 2 existed **before** the transaction. The
destination holds that fact, so `table_work._remove` asks it — at most once per
ambiguous key, cached on the `TableWork`, and never in a group that does not re-use a
key. `TableWork.acquired` is the other half: an `UPDATE` of the row already wearing
the key is the *same* row, so a later delete of it is unambiguous. Reading every
non-None `final[key]` as "a new row moved here" made a plain `UPDATE … ; DELETE …`
leave the row behind, which was measured and fixed before it shipped.

What this does **not** solve, stated plainly: three rows rotating through one key
under a deferred constraint, where two *in-group* rows compete for the same key. That
needs row-level identity we do not have (a full before-image would give it; rubric
2.6/8.2 territory). The probe answers "pre-group or in-group", not "which in-group
row".

### A32 — TRUNCATE is a counted, key-less data event (fills in §3.2 and 1.5)

`skipped.operations` defaults to `"t"`, and the pgoutput decoder drops the `'T'`
message before decoding it, so the *entire* baseline gap for truncate was one
connector default. With `skipped.operations=none`:

* a truncate goes through `EventDispatcher`'s normal `changeRecord` path, so
  `TransactionMonitor.dataEvent` counts it in `END.event_count`, it occupies a
  `transaction.total_order` ordinal and it gets a `data_collections` entry. §3.2's
  boundary rule therefore applies to it unchanged, and it must be fed through the
  data path or every truncating transaction fails the completeness check;
* it carries **no message key**, so `work_for` must not derive identity from it —
  otherwise a keyed table becomes keyless for the rest of the group;
* the fold clears what the group had planned for that table and keeps what follows,
  because that is what `TRUNCATE` does inside a Postgres transaction;
* the destination table is emptied with `DELETE FROM` inside the group's transaction.
  `TRUNCATE a, b CASCADE` is one transaction, so it is one `COMMIT` — 1.3 applied to
  1.5 — and a rolled-back group leaves every row in place.

Policy: `CDC_TRUNCATE_MODE=replicate|log|ignore`, defaulting to `replicate` because
that is what the rubric's 5 asks for. `log` is the rubric's own `=3` behaviour and
exists for destinations whose consumers treat the table as an append-only log.

### A33 — DROP TABLE needs a catalog poller, and the poller needs a WAL fence

Logical decoding carries no DDL, so `catalog.py` polls `pg_class` + `pg_publication_rel`
for the two facts the stream cannot carry: the relation `oid` and publication
membership. Four outcomes — `dropped`, `recreated`, `unpublished`, `new` — of which
only the first two are destructive, and `unpublished` is *never* destructive because
Postgres still holds those rows. The same poll is the mechanism rubric 2.3 will
generalise (its `new` outcome is already recorded); what 2.3 adds is snapshotting the
new table, a collision registry for `naming.destination_table`, and per-table
include/exclude evolution.

Two things the design turns on:

1. **The fence.** A drop is discovered after the fact, so the action carries the
   `pg_current_wal_lsn()` of its poll and the applier holds it until the resume point
   it is about to make durable reaches that LSN. Applying earlier could delete rows
   that an in-flight event then re-adds — a zombie table. `CDC_CATALOG_GRACE=0` (the
   default) means an unfenced action is never forced.
2. **The marker must be transactional.** On a quiet source nothing advances the
   resume point, so the watcher emits `pg_logical_emit_message` past the change (D9's
   mechanism, one poll early). MEASURED 2026-07-31: with `transactional => false` a
   quiet run delivered `records=0`, sat 770 KB behind the source and never applied the
   drop, because `WalPositionLocator.resumeFromLsn` only ends its restart search on a
   **COMMIT** past the stored LSN (`case MESSAGE:` falls through) and `skipMessage()`
   drops everything while searching. With `transactional => true` the same drop applies
   in about a second. This also matters to D9 itself: **a non-transactional source
   heartbeat cannot be relied on to advance an idle slot after a restart.**

Persisted state (`_cdc_flight.source_relations`) is written inside the commit group's
transaction and is never allowed to run ahead of a destructive action it implies —
otherwise the next run would agree with the source and never notice the drop.
Detection also must not *depend* on a persisted oid: a table we replicate that is
simply absent from `pg_class` is a drop, which is what makes a `DROP` performed while
the pipeline was down detectable at all. `--reset-state` clears `source_relations`
too, or "start over" would be read as "every table was recreated".

### A34 — what a table-level event means for history (refines D8)

D8 gives every table up to three shapes (current state, changelog, SCD2). 1.5 has to
say what a TRUNCATE or a DROP does to each, and the answer separates replication from
audit rather than trading them off:

* the **current-state** table is emptied or dropped, because that is what Postgres
  did. Faithful replication is the rubric's 5 and it wins;
* `_cdc_flight.table_events` records every table-level event with its commit id,
  source LSN, transaction id and the number of rows the destination lost, **inside the
  same transaction as the data**, so the audit trail can never describe an apply that
  rolled back;
* a destructive action additionally raises an `_cdc_flight.alerts` row *outside* that
  transaction (§9.1's rule: a signal that vanishes with the rollback is the one you
  need most). **This was a claim, not an implementation, until rev 6** — the row was
  written on the applier's own connection with `BEGIN TRANSACTION` open, so it was
  fully transactional and a rolled-back apply discarded it. See §18/A40;
* when 8.2 lands, a **changelog table is append-only and is never emptied**: it gains
  a truncate marker row derived from this same fact. That is why the marker carries
  the LSN and the transaction id rather than just a timestamp.

Keyless tables are a changelog today (A12), so a truncate does empty them. That is
the honest consequence of having only one materialisation: the rows are gone at the
source, the marker records what was lost, and 8.2 is where "current state *and*
changelog" makes the distinction physical.

---

## 18. Amendments from the 1.4 / 1.5 review round (rev 6, 2026-07-31)

Two independent reviews reproduced silent-loss and duplication paths inside correctly
committed transactions. Codex named the root, and it is a modelling error rather than a
missing branch: **the fold was group-wide, but the ambiguity it was resolving is
per-source-transaction — and neither scope is the right one, because the ambiguity is
about physical rows.** These amendments record what replaced it, and they supersede
A31 (which was both incomplete and wrong about which case it could not handle).

### A35 — the fold models physical rows, not keys (supersedes A31, corrects D6)

A key is not a row. Two facts make that load-bearing:

* inside one Postgres transaction a **deferred** unique constraint lets several rows
  wear one key at once (`UPDATE t SET id = id + 1`);
* across the transactions of one **commit group** a key can be freed and re-taken by a
  different row, and a group holds several whole transactions.

A plan indexed by key cannot express either, and A31's "did this key exist before the
group?" probe answers a question about the wrong scope. Measured consequences, all
reproduced against the shipped applier:

| ordering | Postgres | old fold |
|---|---|---|
| T1 inserts key 2; T2 permutes `{1,2} -> {2,3}`; one group | `{2:a, 3:b}` | `{3:b}` — lost row |
| one txn: `d(1,a) c(3,a) d(2,b) c(3,b) d(3,a)` (two rows on key 3) | `{3:b}` | `{}` — lost row |
| one txn: `d(1,a) c(2,a) d(2,a) c(5,a)` (pre-group row `b` on key 2) | `{2:b, 5:a}` | `{2:a, 5:a}` — lost `b`, duplicated `a` |
| one txn: `TRUNCATE; INSERT 5; DELETE 5` | `{}` | `{5}` — spurious row |
| T1 `TRUNCATE; INSERT 1`; T2 `DELETE 1`; one group | `{}` | `{1}` — zombie row |

`table_work` now holds, per destination table:

    live[key] = [entry, ...]      # the rows that currently wear `key`
    entry     = START | row       # START is the row the destination already held

and every event is one physical operation on that list: `c`/`r` and the new-key half of
a key change **append**; `u` with an unchanged key **replaces** the entry its
before-image identifies; `d` and the old-key half of a key change **remove** it; `t`
discards every entry **including `START`**. Attribution needs no notion of scope at
all — a row an earlier transaction of the group placed is a concrete entry — and the
destination is consulted only about `START`, only where two entries compete, and at
most once per key.

At group end each key holds at most one row (the source enforces uniqueness at every
transaction boundary, which `table_work.end_transaction` asserts per unit), and the
three cases are: `[row]` → delete the key and insert; `[]` → delete the key;
**`[START]` → leave it alone**. That last case is the one A31 could not express, and it
is what the third row of the table above needs: the destination's own row survives
under a key the group touched, so the key is excluded from the merge's DELETE rather
than being deleted and re-inserted. It also keeps its original `cdcf_commit_id`, which
is honest — nothing in the source changed it.

### A36 — where the fold cannot decide, it refuses (new)

Two entries can compete only under a deferred unique constraint, and **a deferrable
primary key is not a valid replica identity** — verified on the cluster:
`relreplident='d'` but `pg_index.indisreplident=false`, and `UPDATE` on the published
table fails with *"does not have a replica identity and publishes updates"* until
`REPLICA IDENTITY FULL` is set. So in every shape where attribution is *needed*, the
full before-image that answers it is *present*. Comparison against `START` is done at
the destination with each value bound to the destination column's own type, because a
Python comparison of a Debezium JSON value against a value that has been through
DuckDB's type system is not a comparison; Debezium's TOAST placeholder is excluded,
because it distinguishes nothing.

Where the before-image cannot distinguish and at most one row can really wear the key
(no deferred constraint ⇒ no ambiguity), the key collapses to empty — that is the
right answer for `INSERT (5,…); DELETE WHERE id=5` under `REPLICA IDENTITY DEFAULT`.
Where two *concrete* rows compete and nothing can choose, `AmbiguousDelete` **fails the
commit group**. The rubric's own scale puts an error (=1) above silent loss, the group
rolls back, and the transaction replays for free under Invariant O. This is the one
place in the design that deliberately prefers a failed run to a committed answer.

### A37 — Invariant O bounds ordering, not semantics (sharpens §4.1)

Invariant O proves the slot cannot advance past data the destination has not committed.
It says nothing about whether what was committed is *right*, and a durably committed
wrong fold advances the slot exactly as happily as a right one. The exactly-once claim
therefore has two halves, and only one of them was ever proven:

1. **ordering** — Invariant O, which the 1.4/1.5 diff did not regress;
2. **the fold** — that the state each group commits is the state Postgres has.

A green crash suite cannot falsify (2): a deterministic fold commits the same wrong
answer before and after a replay. What falsifies it is adversarial *orderings*, which
is why the counterexample suites (`tests/1.4_pk_updates/test_1_4_fold_counterexamples.py`,
`tests/1.5_truncate_drop/test_1_5_truncate_key_reuse.py`,
`test_1_5_truncate_storage_modes.py`) assert source/destination equality rather than
mere uniqueness, and why the module boundaries below exist: every blocker of the last
two review rounds was a second code path doing one job.

### A38 — the four guards between detection and destruction (extends A33)

A33 gave the drop a WAL fence. The fence is necessary and it is not sufficient: the
observation and the DDL are separated in time, and on a quiet source that gap is
unbounded, so the code has to ask whether the fact still holds.

| guard | refuses | reachable failure it closes |
|---|---|---|
| the fence | applying before the destination consumed everything before the DDL | a zombie re-created by an in-flight event |
| **confirmation** (`CDC_DROP_CONFIRM_POLLS`, default 2) | acting on a single observation | a transient catalog read mid-DDL |
| **supersession** | acting on an observation a later poll contradicted | dropping the destination table of a **live replacement relation** |
| **revalidation** | acting without re-reading the source, and acting when it cannot be read | the same, for a fence that opened long after detection |
| **the circuit breaker** (`CDC_DROP_MAX_PER_POLL`, default 1) | destroying more than one relation at once | `DROP SCHEMA … CASCADE`, a DSN repointed at an empty database, a failover target, a source mid-`pg_restore` |
| **the zero-relations guard** | acting on a poll that saw an empty schema | the wrong-database signature, which can never mean "drop everything" |

Revalidation fails **closed**: "I could not ask" is never read as "it is gone". The
circuit breaker refuses the **whole** set, never the first N. And `CDC_CATALOG_GRACE>0`
applies a destructive action *before* the fence that makes it safe, so it is
**explicitly excluded from the structural correctness guarantee** — the run logs that
at start-up.

`recreated` is the one case where the source relation exists and the destination table
is still wrong, because it holds a *different* relation's rows. Keeping them presents
pre-drop data as current; dropping them and letting ordinary CDC rebuild a partial
table is worse, because the destination then looks healthy while being silently
incomplete. So: drop, alert, and persist `table_state.snapshot_state='awaiting_snapshot'`
which the run summary and `inspect` surface. Rubric 2.3/3.4 own clearing it.

### A39 — `table_state` is the canonical ownership registry (new)

"A replicated table absent from `pg_class` is always detected" was not durably true.
The watcher learns which names it owns from `_cdc_flight.table_state`, and that row was
written only by the snapshot coordinator — so a table first materialised by streaming
DML had **no** durable row, and a `DROP TABLE` while the pipeline was down left an
orphan destination table that no later poll could ever report. Ownership is now
persisted by whoever first creates the table, in the same transaction, whatever the
origin.

For the same reason `--reset-state` no longer DELETEs `table_state`; it resets the
*snapshot* bookkeeping in place. Deleting it produced a **permanent** zombie: a
source-dropped table produces no events, so nothing re-teaches the watcher that the
name is ours, and detection was disabled for it for ever.

### A40 — the alert really is out of transaction (corrects §9.1's implementation)

§9.1 says a destructive-DDL alert must survive the rollback of the apply it describes.
The code said so in a comment and then wrote the row on the applier's **own**
connection with `BEGIN TRANSACTION` open, so it was fully transactional: measured,
inject `pre_commit:raise` after a detected drop and the DDL correctly rolls back while
`_cdc_flight.alerts` is empty — the exact case §9.1 exists for.

`destination.AlertSink` holds `con.cursor()`, a separate connection to the same
database with its own transaction context (VERIFIED on DuckDB 1.5.4: an INSERT on the
cursor while the parent holds an open write transaction succeeds and survives the
parent's ROLLBACK). Alerts are also now **classified**: one that describes a refusal
("I did not destroy your warehouse") survives the rollback; one that describes an
applied action ("your table was dropped") must not outlive the rollback that undid it.
If a destination cannot provide an independent connection the sink degrades to the
caller's and says so in the row itself, rather than labelling a same-connection insert
non-transactional.

### A41 — a rolled-back group is discarded, in the process too (fills a gap in §3.3)

`_reset_group()` ran only after a successful COMMIT. On the exception path the markers,
catalog lists and registry cache were cleared — but `_group`, `_created_in_txn` and
`_spill_commit_id` were not, so the next `commit_group` folded the rolled-back units a
**second** time alongside whatever arrived after them. Harmless for an idempotent
shape, which is why the fault suite passed; measured to lose a row for a key-reuse
shape. `_created_in_txn` surviving a rollback is independently dangerous: it makes
`write()` compute `fresh=True` and skip the DELETE half of the merge. The ADR's rule is
that a rolled-back group replays *from the source*; that is now true of the process as
well as of the offset store, and the discarded units are counted as deferrals.

### A42 — one source writer, shared with D9 (answers §17's open question 3)

The catalog fence marker and D9's idle-slot heartbeat are the same write to the same
place for the same reason, so they are one component: `cdc_flight.source_marker`
emits `pg_logical_emit_message(true, '<prefix>_<reason>', …)` with `reason` in
`{catalog_fence, idle_heartbeat}`, and owns the capability probe, the error state and
the **write budget** (`CDC_CATALOG_MARKER_MAX`, default 60 — a fence that never opens
must not write one WAL record per poll for ever against a source we otherwise only
read). 4.4/7.2 extend it with the heartbeat cadence and the primary routing rather than
building a second writer. A33's measurement is a constraint on D9 itself: a
non-transactional heartbeat cannot be relied on to advance an idle slot after a restart.

### A43 — a run with an unresolved destructive change is not a success (extends §9.1)

Deferring a destructive action whose fence has not opened is the correct *safety*
choice. It is not faithful propagation, and the run used to report `ok: true` with
`catalog_error: null` and `catalog_pending > 0` — because `poll()` cleared `last_error`
unconditionally and the alert was raised only when a change was *applied*. Three
changes: marker failures are preserved in `catalog_marker_error`; a quiet run performs
a **synchronous final catalog poll** and then holds the engine open for
`CDC_CATALOG_DRAIN_SECONDS` so a change it just queued is fenced and applied by *this*
run rather than the next one; and a destructive change still unresolved at shutdown
makes the run **fail** with `stop_reason=catalog_unresolved`.

### A44 — module boundaries (extends A29)

A29 left `applier.py` owning "the commit protocol, and only that". The 1.4/1.5 work put
catalog policy, destructive DDL, audit construction, truncate dispatch and
source-catalog persistence back into it, and that is exactly where the duplicated
truncate dispatcher came from. Restored, with the *semantics* moved rather than
redistributed:

| module | owns |
|---|---|
| `applier.py` | the commit protocol: `BEGIN -> apply -> state -> COMMIT -> ack`, the group triggers, the fence, spill staging |
| `planner.py` | `GroupPlan`: one canonical event dispatcher for both storage modes, the truncate policy and audit, the destination probe |
| `table_work.py` | the physical-row fold and the merge for one table |
| `catalog.py` | observing the source catalog. It never decides |
| `catalog_apply.py` | the destructive-DDL policy and the four guards, as an immutable plan |
| `resume.py` | the resume point and the `offsets.dat` forensics |
| `source_marker.py` | the only writes cdc_flight makes to the source |

---

## 19. Amendments from 1.6 / 1.7 / 1.8, and from their review round (rev 7-8, 2026-07-31)

Written while implementing TODO 1.6 (snapshot/CDC consistency), 1.7 (fault injection)
and 1.8 (externally-advanced slot), plus the mid-task addition of rubric **4.7**
("the Flight should always be able to self-heal without human intervention").

### A45 — the re-snapshot mechanism, and why a *fresh slot* is load-bearing

Three items needed "rebuild this table from the source" and none of them had it. The
mechanism is a **blocking re-snapshot through a short-lived Debezium engine with its own
fresh replication slot, before the main stream starts** (`cdc_flight.resnapshot`).

The load-bearing measurement, taken against Debezium 3.6 + Postgres 18.1 and recorded
here because the whole consistency claim rests on it: Debezium pairs the snapshot with an
exact WAL position **only when it creates the slot itself**.

| slot | isolation statement | streaming start LSN |
|---|---|---|
| created by Debezium in this start-up | `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;` **+ `SET TRANSACTION SNAPSHOT '<exported>'`** | `slotCreatedInfo.startLsn()` — the slot's `consistent_point` |
| already existed | the configured isolation mode, no exported snapshot | `pg_current_wal_lsn()`, read after the snapshot transaction has begun |

(`PostgresSnapshotChangeEventSource.snapshotTransactionIsolationLevelStatement`,
`getTransactionStartLsn`; the upstream comment on the first row is "crucial so that if any
SQL operations occur mid-snapshot they'll be properly captured when streaming begins;
otherwise they'll be lost".) `snapshot.mode=initial_only` creates **no slot at all** —
verified in the engine log — so it takes the second path and is unusable for this.

Consequences that are now design rules:

1. the re-snapshot engine uses `snapshot.mode=initial` on a slot name that does not
   exist, and is stopped the moment the snapshot completes (`stop_when`);
2. rubric 1.8's recovery **drops** the pipeline's slot rather than reusing it, for the
   same reason plus a second one: a slot whose `confirmed_flush_lsn` we cannot account for
   makes the stream resume *past* the snapshot's consistent point;
3. `--accept-orphan-offsets` drops the slot too — same argument, previously missing.

`C` (the consistent point) is read two independent ways — every snapshot record's
`source.lsn` (which *is* `slotCreatedInfo.startLsn()`), and the first
`confirmed_flush_lsn` the throwaway slot ever shows — and they must agree.

**CORRECTED at rev 8 (Codex B2, Opus Q1).** This section used to say a disagreement takes
the **minimum** and logs an error, on the argument that fencing too low can only
re-apply. The argument is true and insufficient: re-applying **duplicates** on a keyless
table, which violates rubric 1.2's exactly-once claim, so `min` did not avoid a
correctness violation — it chose a different one. And a disagreement has already
falsified the assumption that either reading identifies the exported snapshot, so neither
value is a boundary anything may rest on. It is now fatal; the tables stay
`awaiting_snapshot` and the next run takes a fresh `C`. The polled slot value is a
**corroboration only** and is never the sole basis for `C`: for an all-empty capture set
it was a race with no upper bound, and that case is handled by A52's own construction
instead.

### A46 — the hand-over is fenced per table, on the COMMIT LSN

Postgres's exported snapshot makes visibility an **iff**: a transaction is in the image
exactly when it committed before `C`. So `table_state.snapshot_lsn` becomes a per-table
watermark and `GroupPlan` drops events for table `T` from any unit whose **commit** LSN is
below `T`'s watermark.

Never on an event's own LSN. A transaction that was still open when the snapshot was taken
is in **no** image, and some of its events carry LSNs below `C`; fencing those would be
silent loss of exactly the straddling transaction. The watermark is read for *every* table
with a complete image, not only re-snapshotted ones — after an ordinary initial snapshot
the same rule holds and is a no-op, and a fence armed only on some paths is one nobody can
reason about.

**CDC during a re-snapshot** therefore has one-line semantics: there is none, because the
main stream has not started; and everything the main stream later delivers for the table is
either fenced (before `C`) or applied on top (at or after `C`). Concurrent
re-snapshot-while-streaming is rubric 3.3/3.4's, and it needs a durable per-table buffer
this does not build.

**The honest cost.** A re-snapshot replaces *current state*. The change events of the
fenced span are never applied, so a changelog (rubric 8.2) is discontinuous there: an image
at `C` instead of the events that produced it. A `table_events` row with
`event='resnapshot'` records where, so 8.2 can *find* the discontinuity rather than be told
about it in a docstring.

### A47 — an undecidable fold self-heals (rubric 4.7)

`AmbiguousDelete` (A36) and `DestinationIdentityCollision` (A21) were the right immediate
answer and the wrong eventual one: the group rolls back, Invariant O replays the
transaction, the same condition is hit, the run fails again — **for ever**. That is a
permanent manual-intervention case.

Both exceptions now carry the relation they could not fold, and the applier records an
`awaiting_snapshot` request on the **independent** connection (the one `AlertSink`
introduced, whose survival of a parent `ROLLBACK` is verified) so the request outlives the
rollback that must still happen. The next run rebuilds that table.

Termination is an argument, not a hope: the offending transaction had already been
delivered, so its commit LSN was already in WAL when the re-snapshot starts; `C` is taken
after that; so A46's watermark fences it. **One re-snapshot, always.** The run still exits
non-zero — no human action is required, but an operator should know — and
`CDC_AMBIGUOUS_RESNAPSHOT=0` restores the permanent failure for anyone who wants it.

**The inequality the termination argument needs, stated (Opus MINOR-8).** "One
re-snapshot, always" requires `C > L` **strictly**, where `L` is the commit LSN of the
transaction that could not be folded: the fence is `commit_lsn >= mark -> not fenced`
(`planner.GroupPlan._below_watermark`), so `C == L` would leave the offending transaction
on the stream side, replay it, fail to fold it again and re-queue — a loop, not a
termination. In practice `C > L` holds because creating a replication slot forces a
`LogStandbySnapshot` WAL record after the offending transaction is already durable, so
the exported snapshot's consistent point is strictly ahead of it. The argument is sound;
it was asserted without its inequality.

### A48 — destination and network faults, and the two outcome classes

Rubric 1.7's anchors were all places *we* stand, which cannot express "the destination
refused the write". Two mechanisms are added:

* `faults.FaultyConnection` wraps the single destination connection and injects
  `destination_write` / `destination_commit` / `destination_hang` / `destination_close` at
  a data group the **applier declares** (`faults.arm_group`) rather than one the wrapper
  infers from the SQL it sees — an inferred index is how a fault test goes vacuously green;
* `tests/tcp_relay.py` blackholes the *source* from outside the process: bytes stop being
  forwarded and both sockets stay open, which no in-process anchor can simulate and which
  is the shape rubric 4.6 calls "silently-dead-connection".

Every anchor must land in one of exactly two classes — a clean recovery with the ledger
intact, or a non-zero exit with an accurate `last_run.json` — and
`tests/1.7_fault_injection/test_1_7_fault_matrix.py` enumerates them **from
`faults.ALL_POINTS`**, so a new anchor with no declared outcome fails the suite.

Two defects this found:

* a hung `COMMIT` was unbounded, which is neither class. `CDC_COMMIT_TIMEOUT` (300 s)
  aborts the process with `EX_TEMPFAIL`. Safe *because* of Invariant O: the commit is
  ambiguous and nothing was acknowledged, so the next run resumes from whatever the
  destination actually holds (§4.6 F5);
* `last_run.json` named the wrong cause. A destination write that failed mid-transaction
  was reported as `java.lang.InterruptedException`, because pydbzengine interrupts the
  engine thread when the handler raises and Debezium's completion message won the
  comparison. Ours is the root cause and is now reported first.

### A49 — `unknown` is not a reason to declare a stream idle (closes TODO 4.6(b))

`SourceHealth` reported `unknown` when the slot could not be queried, and
`may_declare_idle()` returned **True** for it *and* the sampler loop reset the
not-streaming clock. A blackholed Postgres therefore exited `ok: true` on a partial
delivery — measured, and the residual half of review finding B5.

The fix keeps the distinction the fail-soft existed for. A sampler that has **never**
succeeded (no `psycopg`, no privilege, a firewall that was always there) still degrades to
timer-only idle detection; one that *was* answering and has gone dark forbids idle, counts
as not-streaming, and after `CDC_SOURCE_DARK_SECONDS` (45 s) fails the run outright — which
makes detection bounded rather than "whenever `--max-seconds` happens to expire".

**Amended at rev 8 (Opus MINOR-5).** Two things the blackhole scenario exposed once the
test stopped accepting `"source" in json.dumps(summary)` as evidence:

* **the shutdown symptom was replacing the diagnosis.** A source that has gone dark makes
  `engine.close()` hang almost by definition — the connector is waiting on a socket that
  will never answer — and the supervisor overwrote `stop_reason='source_dark'` with
  `'hung'` in its `finally`. The cause is now preserved and `close_hung: true` is recorded
  alongside it, which is the same cause-before-symptom rule the `EngineFailure` path
  already followed.
* **the 45-second claim was unmeasurable from outside.** Time-to-process-exit includes
  tearing down a JVM whose connector is blocked on a dead socket — another minute — so it
  measures the shutdown path, not the detector. The summary now carries
  `source_dark_detected_after_sec`, taken at the instant of detection, and the test
  asserts the interval from the blackhole to that instant.

### A50 — rubric 1.8's decision table, and the one refusal that survives it

`reconcile.check_slot` is a **pure function** of (durable offset, one observation, the
previous observation), so every cell is a unit test rather than a base-backup restore.
Seven decisions trigger an automatic re-snapshot of every captured table:
`slot_ahead_of_destination`, `slot_missing`, `slot_recreated`, `source_identity_changed`,
`source_timeline_changed`, `source_lsn_regressed`, and `no_durable_destination_row`
**when the destination is empty**.

`source_timeline_changed` is new at rev 8. `timeline_id` was persisted from the first cut
of `slot_state` and never consulted, which made the documented pair
`system_identifier + timeline_id` a claim rather than a check: a promoted standby or a
point-in-time restore keeps the system identifier and forks the timeline, and Postgres
**reuses WAL positions across a fork**, so every scalar comparison in the table can look
healthy while the destination's offset names a point on a history that no longer exists. A
probe with the same system id, previous timeline 1 and current timeline 2 returned `ok`
(Codex B5). A fork also invalidates the recorded catalog, so `FORGET_CATALOG_DECISIONS`
now covers timeline change and LSN regression as well as identity change (Codex M1).

`no_durable_destination_row` takes a fourth input at rev 8: **what the destination
actually holds**. This section and `RUBRIC_STATUS` both described the cell as "destination
*empty*, slot positioned" while the code tested only that the control row was missing, so
a healthy populated warehouse reached through a fresh state directory was silently rebuilt
from whatever source the DSN named — a safety *regression* against `main`, where the same
cell refused (Opus BLOCKER-2). A populated destination now refuses, with the justification
the orphan-file refusal already carries word for word: a durable resume point is what
proves a destination belongs to this pipeline, and this cell is defined by its absence.

Detecting a recreated slot or a restored cluster needs a memory, so `_cdc_flight.slot_state`
records `system_identifier`, `timeline_id`, `restart_lsn` and `confirmed_flush_lsn` at every
acquisition. It is written **outside** any commit group: it is an observation about the
source, not a fact about the data, and recording it must never be able to fail a commit.
Every check degrades to "cannot compare" when the row is absent, so correctness never
depends on it — it only makes the detectable set larger.

**CORRECTED at rev 8 (Codex B3 / Opus MAJOR-1).** This section used to claim that the
recovery order — mark the tables, *then* delete the resume point, *then* the offsets file,
*then* drop the slot — made every intermediate state recoverable. **That claim was false,
and in both directions.** Deleting the row before the file leaves `row-gone + file-present`,
which is exactly the `orphan_offset_file` refusal: the Flight diagnosed its own
half-finished recovery as an operator's mistake and refused to start for ever (reproduced
across three consecutive restarts; only `--accept-orphan-offsets` recovered it). And a
crash after the slot drop lost the forced `snapshot.mode='initial'` entirely, because it
lived in a local variable — the next run saw no row, no file and no slot and called that an
ordinary fresh start.

The sequence is now a **journalled state machine** (`cdc_flight.recovery`, A53) and the
file is deleted *before* the row. See A53 for the phases and the crash-cut table.

**The refusal that survives.** `orphan_offset_file` — an `offsets.dat` with no destination
row — still refuses to start, and it is the one place where automatic recovery could itself
destroy data: the usual cause is a DSN pointed at the wrong database, and a re-snapshot
would `DROP` *that* database's live tables and replace them with another source's contents.
`--accept-orphan-offsets` is how an operator says "yes, rebuild into this destination", and
it now also drops the slot (A45) and forces a data-reading `snapshot.mode`.

### A51 — failure-mode inventory, as states × transitions (rubric 4.7's baseline)

**REWRITTEN at rev 9** (rubric 1.9). The previous inventory was a hand-authored flat
list of 54 numbered rows. Machine-checking its arithmetic (rev 8) closed one failure
mode — a headline that did not add up — and left the one that matters open: **a failure
mode with no row at all**. Opus MAJOR-3 found eight of those by reading code for a day,
and nothing in the tree could have found the ninth.

Every row now names the **machine and the edge** it belongs to, and the machines'
transition tables below are *generated from `machine.table()`* rather than transcribed
(`tests/4.7_self_healing/test_4_7_inventory.py` regenerates them and fails on any
difference — MAJOR-3's arithmetic drift, one level up). That makes three things
mechanical rather than remembered:

1. an inventory row naming an edge that does not exist fails the suite;
2. `states × states` minus the declared edges is a computable set, so "a cell nobody has
   thought about" is printable (`Machine.unreachable_cells()`, §A51.4);
3. `AUTO` / `MANUAL` / `UNDEFINED` are aggregations over the rows, not a number anybody
   types.

Rows whose edge is `—` are **not transitions**: pre-conditions a human wrote (28, 29,
49), decode-level refusals rubric 4.3 owns (30, 31, 48), internal invariants (32, 32b,
51, 52), things not detected here at all (37, 39), and the commit group — which is
memory-only **by design**, because under Invariant O the whole group is uncommitted
until one `COMMIT` and "crash ⇒ discard and replay" is the entire correctness story
(rows 1–7). Keeping them separate is honest: A51 used to mix "a human wrote a bad env
var" with "the slot was externally advanced", and the rubric's band is a *count*.

#### A51.1 — the transition tables (generated from `cdc_flight.machines`)

**`table_lifecycle`** — Does this destination table hold a trustworthy image of its source relation, and if not, who owes the work?

persistence: `_cdc_flight.table_state.snapshot_state` · initial: `absent` · terminal: `absent`, `complete`

| from | to | terminal |
|---|---|---|
| `absent` | `awaiting_snapshot` | no |
| `absent` | `in_progress` | no |
| `absent` | `none` | no |
| `awaiting_snapshot` | `absent` | yes |
| `awaiting_snapshot` | `awaiting_snapshot` | no |
| `awaiting_snapshot` | `complete` | yes |
| `awaiting_snapshot` | `in_progress` | no |
| `awaiting_snapshot` | `none` | no |
| `complete` | `absent` | yes |
| `complete` | `awaiting_snapshot` | no |
| `complete` | `complete` | yes |
| `complete` | `in_progress` | no |
| `complete` | `none` | no |
| `in_progress` | `absent` | yes |
| `in_progress` | `awaiting_snapshot` | no |
| `in_progress` | `complete` | yes |
| `in_progress` | `none` | no |
| `none` | `absent` | yes |
| `none` | `awaiting_snapshot` | no |
| `none` | `in_progress` | no |
| `none` | `none` | no |

**`run_phase`** — Where is this run right now, readable from the destination while it runs?

persistence: `_cdc_flight.heartbeat.phase` · initial: `starting` · terminal: `failed`, `stopped`

| from | to | terminal |
|---|---|---|
| `draining` | `failed` | yes |
| `draining` | `stopping` | no |
| `reconciling` | `failed` | yes |
| `reconciling` | `recovering` | no |
| `reconciling` | `snapshotting` | no |
| `reconciling` | `stopping` | no |
| `reconciling` | `streaming` | no |
| `recovering` | `failed` | yes |
| `recovering` | `reconciling` | no |
| `recovering` | `stopping` | no |
| `snapshotting` | `failed` | yes |
| `snapshotting` | `stopping` | no |
| `snapshotting` | `streaming` | no |
| `starting` | `failed` | yes |
| `starting` | `reconciling` | no |
| `starting` | `recovering` | no |
| `starting` | `stopping` | no |
| `stopping` | `failed` | yes |
| `stopping` | `stopped` | yes |
| `stopping` | `stopping` | no |
| `streaming` | `draining` | no |
| `streaming` | `failed` | yes |
| `streaming` | `stopping` | no |

**`run_outcome`** — Why did this run stop? Cause before symptom, by construction.

persistence: `_cdc_flight.heartbeat.terminal_reason (also last_run.json stop_reason)` · initial: `max_seconds` · terminal: (none)

| from | to | terminal |
|---|---|---|
| `catalog_unresolved` | `engine_error` | no |
| `catalog_unresolved` | `error` | no |
| `catalog_unresolved` | `recovery_uncleared` | no |
| `catalog_unresolved` | `source_dark` | no |
| `engine_error` | `error` | no |
| `engine_finished` | `catalog_unresolved` | no |
| `engine_finished` | `engine_error` | no |
| `engine_finished` | `error` | no |
| `engine_finished` | `hung` | no |
| `engine_finished` | `recovery_uncleared` | no |
| `engine_finished` | `source_dark` | no |
| `hung` | `catalog_unresolved` | no |
| `hung` | `engine_error` | no |
| `hung` | `error` | no |
| `hung` | `recovery_uncleared` | no |
| `hung` | `source_dark` | no |
| `idle` | `catalog_unresolved` | no |
| `idle` | `engine_error` | no |
| `idle` | `engine_finished` | no |
| `idle` | `error` | no |
| `idle` | `hung` | no |
| `idle` | `recovery_uncleared` | no |
| `idle` | `source_dark` | no |
| `idle` | `work_done` | no |
| `max_seconds` | `catalog_unresolved` | no |
| `max_seconds` | `engine_error` | no |
| `max_seconds` | `engine_finished` | no |
| `max_seconds` | `error` | no |
| `max_seconds` | `hung` | no |
| `max_seconds` | `idle` | no |
| `max_seconds` | `recovery_uncleared` | no |
| `max_seconds` | `source_dark` | no |
| `max_seconds` | `work_done` | no |
| `recovery_uncleared` | `engine_error` | no |
| `recovery_uncleared` | `error` | no |
| `recovery_uncleared` | `source_dark` | no |
| `source_dark` | `engine_error` | no |
| `source_dark` | `error` | no |
| `work_done` | `catalog_unresolved` | no |
| `work_done` | `engine_error` | no |
| `work_done` | `engine_finished` | no |
| `work_done` | `error` | no |
| `work_done` | `hung` | no |
| `work_done` | `recovery_uncleared` | no |
| `work_done` | `source_dark` | no |

**`acquisition_recovery`** — What has this destructive recovery already done, if the process died mid-way?

persistence: `_cdc_flight.recovery_state.phase` · initial: `absent` · terminal: `absent`

| from | to | terminal |
|---|---|---|
| `absent` | `requested` | no |
| `armed` | `absent` | yes |
| `armed` | `requested` | no |
| `offsets_file_deleted` | `requested` | no |
| `offsets_file_deleted` | `resume_point_deleted` | no |
| `requested` | `offsets_file_deleted` | no |
| `requested` | `requested` | no |
| `resume_point_deleted` | `armed` | no |
| `resume_point_deleted` | `requested` | no |

**`catalog_change`** — Where in the observe -> confirm -> fence -> apply pipeline is one DDL fact about one relation? Memory only: a lost pending change is re-detected, which is correct, so persisting it would buy nothing.

persistence: **memory only** · initial: `observed` · terminal: `applied`, `superseded`

| from | to | terminal |
|---|---|---|
| `deferred` | `deferred` | no |
| `deferred` | `due` | no |
| `deferred` | `marked` | no |
| `deferred` | `refused` | no |
| `deferred` | `superseded` | yes |
| `due` | `applied` | yes |
| `due` | `deferred` | no |
| `due` | `refused` | no |
| `due` | `superseded` | yes |
| `marked` | `deferred` | no |
| `marked` | `due` | no |
| `marked` | `marked` | no |
| `marked` | `refused` | no |
| `marked` | `superseded` | yes |
| `observed` | `pending` | no |
| `observed` | `superseded` | yes |
| `observed` | `unconfirmed` | no |
| `pending` | `deferred` | no |
| `pending` | `due` | no |
| `pending` | `marked` | no |
| `pending` | `refused` | no |
| `pending` | `superseded` | yes |
| `refused` | `deferred` | no |
| `refused` | `due` | no |
| `refused` | `marked` | no |
| `refused` | `refused` | no |
| `refused` | `superseded` | yes |
| `unconfirmed` | `pending` | no |
| `unconfirmed` | `superseded` | yes |
| `unconfirmed` | `unconfirmed` | no |

**`catalog_baseline`** — Can the relation identities this run observes be related to the rows the destination already holds, or must they be reconciled before they are adopted?

persistence: `_cdc_flight.catalog_baseline.state` · initial: `absent` · terminal: *none — a confirmed baseline becomes unconfirmed again the moment the next run starts, which is why `valid` is not terminal*

| from | to | terminal |
|---|---|---|
| `absent` | `stale` | no |
| `invalidated` | `absent` | no |
| `invalidated` | `invalidated` | no |
| `invalidated` | `stale` | no |
| `invalidated` | `valid` | no |
| `stale` | `absent` | no |
| `stale` | `invalidated` | no |
| `stale` | `stale` | no |
| `stale` | `valid` | no |
| `valid` | `absent` | no |
| `valid` | `stale` | no |

There is deliberately **no `absent -> valid`** and no `valid -> valid`. Every
catalog-enabled run marks the baseline unconfirmed *before the engine starts*, so
`valid` is only ever reachable from a mark this run has to discharge with evidence: a
catalog it actually read, and no relation left holding rows under an identity it cannot
relate. A run that could assert `valid` without passing through the mark is precisely
the run that reported success over an unchecked catalog.

#### A51.2 — the inventory, anchored to those edges

| # | machine · edge | failure mode | detection | recovery | crash cut | class |
|---|---|---|---|---|---|---|
| 1 | — | crash at any of the 8 protocol anchors | process death | next run resumes from the durable resume point | before: nothing committed, replays · after: acknowledged, nothing owed | AUTO |
| 2 | — | destination write fails mid-transaction | exception | group rolls back, replays | n/a — one COMMIT is the only durable write | AUTO |
| 3 | — | destination `COMMIT` raises (ambiguous) | exception | next run reads whichever point is durable | before: replays · after: the resume point committed with the data | AUTO |
| 4 | — | destination `COMMIT` hangs | `CDC_COMMIT_TIMEOUT` watchdog | process aborts (75), next run resumes | as 3 | AUTO |
| 5 | — | destination connection severed | exception | as 2 | as 2 | AUTO |
| 6 | — | MotherDuck lease row locked by an abandoned txn | conflict on DELETE | 30 s bounded retry (A27) | n/a — the lease is TTL-bounded either way | AUTO |
| 7 | — | second concurrent runner | lease renewal inside the group | loser exits before writing; retry later | n/a | AUTO |
| 8 | reconcile_decision (domain) | `offsets.dat` missing / corrupt / ahead / behind | start-up reconciliation | rebuilt from the destination | the file is never a source of truth | AUTO |
| 9 | — | `markBatchFinished()` did not flush | `OffsetFlushVerifier` | non-zero exit; next run rebuilds the file | n/a | AUTO |
| 10 | run_outcome: max_seconds -> engine_error | walsender killed / connector restart backoff | `SourceHealth` streaming clock | run fails; next run resumes | n/a (memory) | AUTO |
| 11 | run_outcome: max_seconds -> source_dark | source blackholed (silently-dead connection) | `unknown` + `CDC_SOURCE_DARK_SECONDS` | run fails; next run resumes | n/a (memory) — and `hung` can no longer overwrite it | AUTO |
| 12 | acquisition_recovery: absent -> requested | slot advanced externally | `check_slot` | automatic full re-snapshot | journalled; see 45 | AUTO |
| 13 | acquisition_recovery: absent -> requested | slot dropped | `check_slot` | automatic full re-snapshot | journalled; see 45 | AUTO |
| 14 | acquisition_recovery: absent -> requested | slot recreated at the same name | `slot_state.restart_lsn` regression | automatic full re-snapshot | journalled; see 45 | AUTO |
| 15 | acquisition_recovery: absent -> requested | source restored / cloned / DSN repointed | `system_identifier` change | automatic full re-snapshot + catalog forgotten | journalled; see 45 | AUTO |
| 16 | acquisition_recovery: absent -> requested | source WAL rewound | `pg_current_wal_lsn() < durable` | automatic full re-snapshot | journalled; see 45 | AUTO |
| 17 | acquisition_recovery: absent -> requested | destination **empty**, slot positioned | `check_slot` + a destination row count | automatic full re-snapshot | journalled; see 45 | AUTO |
| 17b | slot_verdict (domain) | destination **populated**, slot positioned, no resume point | `check_slot` + a destination row count | **refuses**: no resume point means no proof the destination is ours (A50) | nothing is mutated, so there is no cut | **MANUAL** (scored exception) |
| 18 | table_lifecycle: in_progress -> awaiting_snapshot | crash mid-snapshot | process death; the durable state is `in_progress` | the shadow is dropped and rebuilt; start-up promotes the row to owed work | before: the row still says `in_progress`, which the owed queue now SELECTS · after: `awaiting_snapshot` | AUTO |
| 19 | table_lifecycle: in_progress -> complete | crash between a swap's DROP and RENAME | process death | transactional DDL rolls back; the table is still owed | before: old table intact, still `in_progress` · after: the swap and the state committed together | AUTO |
| 20 | table_lifecycle: in_progress -> awaiting_snapshot | crash mid-re-snapshot | process death | every non-terminal table is re-asserted `awaiting_snapshot`; next run redoes it | as 18 | AUTO |
| 21 | table_lifecycle: complete -> awaiting_snapshot | source relation dropped and recreated | catalog poller | destination dropped, `awaiting_snapshot`, then auto re-snapshot | the drop and the marking are in one commit group | AUTO |
| 22 | table_lifecycle: complete -> awaiting_snapshot | undecidable fold (`AmbiguousDelete`) | fold refuses | auto re-snapshot of that table, terminating (A47) | the request is written on the independent connection, so it survives the rollback | AUTO |
| 23 | table_lifecycle: complete -> awaiting_snapshot | destination identity collision | post-apply assertion | as 22 | as 22 | AUTO |
| 24 | table_lifecycle: awaiting_snapshot -> complete | source table emptied at the source during a re-snapshot | end-of-snapshot marker **and** zero records for the table **and** a source count of zero | destination table emptied + audited, fenced at the WAL position sampled before the count | before: still owed · after: emptied and fenced in one transaction | AUTO |
| 24b | table_lifecycle: in_progress -> awaiting_snapshot | a requested table the re-snapshot did not reach | it is neither swapped nor verified-empty | run fails; the table is re-marked `awaiting_snapshot`, its destination untouched | as 18 | AUTO |
| 25 | reconcile_decision (domain) | orphan `offsets.dat` (no destination row) | reconciliation | **refuses to start** | nothing is mutated, so there is no cut | **MANUAL** (scored exception) |
| 26 | catalog_change: due -> refused | >1 destination table would be destroyed at once | mass-drop circuit breaker | **refuses**, the changes stay pending | nothing destroyed | **MANUAL** (owned by 4.7's own task) |
| 27 | run_outcome: idle -> catalog_unresolved | destructive catalog change unresolved at shutdown (fence marker unwritable, e.g. read-only source) | `catalog_unresolved` | run fails; **repeats** while the source cannot be written to | n/a (memory) | **MANUAL** |
| 28 | — | `CDC_FAULT_INJECT` / `CDC_TRUNCATE_MODE` / `CDC_DROP_MODE` malformed, `INVARIANT_O_PINS` overridden | start-up validation | refuses | nothing started | **MANUAL** (correctly) |
| 29 | — | `motherduck_token` absent / destination unreachable at connect | connect raises | refuses; retry once fixed | nothing written | **MANUAL** (correctly) |
| 30 | — | malformed / unknown WAL message (`EnvelopeDecodeError`) | decode refuses | run fails and **repeats** on the same record | nothing wrong is written | **UNDEFINED** — rubric 4.3 owns "handles backfill automatically" |
| 31 | — | inconsistent Debezium transaction metadata (`TransactionAssemblyError`) | the assembler's own guarded state machine refuses | as 30 | as 30 | **UNDEFINED** — 4.3 |
| 32 | — | `ResumePointDrift`: the offsets file disagrees with the durable point after COMMIT | assertion | run fails; the next run's reconciliation rebuilds the file from the destination | the data is already durable | AUTO |
| 32b | — | `ResumePointDrift`: a snapshot record with no arrival ordinal | assertion | none — an internal invariant, not a repairable state | nothing wrong is written | **UNDEFINED** |
| 33 | — | publication dropped / privileges revoked at the source | connector fails to start | run fails and repeats | nothing wrong is written | **MANUAL** (correctly — but 4.1 may want auto-recreate) |
| 35 | table_lifecycle: in_progress -> awaiting_snapshot | re-snapshot yields no consistent point on ONE attempt | `resnapshot` refuses | run fails; tables stay owed; next run retries | nothing swapped | AUTO |
| 35b | table_lifecycle: in_progress -> awaiting_snapshot | re-snapshot **persistently** yields no consistent point | the same failure every run | none; it repeats for ever | nothing swapped | **UNDEFINED** |
| 36 | run_outcome: max_seconds -> hung | engine `close()` hangs / engine thread will not stop | `close_timeout`, 60 s join | run fails; process exits via the JVM watchdog | n/a (memory) — and it can no longer overwrite `source_dark` | AUTO |
| 37 | — | Debezium keepalive thread dies silently | **not detected** | — | — | **UNDEFINED** — TODO 4.6(b) carry-forward |
| 38 | — | destination disk full / MotherDuck quota | exception mid-apply | as 2, then repeats until space exists | as 2 | **MANUAL** (correctly) |
| 39 | — | WAL retained until the slot is consumed (source disk pressure) | not detected here | — | — | **UNDEFINED** — 3.6/4.4 |
| 40 | — | throwaway `_rs` slot leaked by ANY failure of a re-snapshot | swept by name at **every** start-up of the owning pipeline, plus a `try/finally` | dropped | no WAL held beyond the next run of that pipeline | AUTO |
| 41 | slot_verdict (domain) | `CDC_RESNAPSHOT=0` | the operator set it | rows 12-17 and 21-24 stop being automatic and raise instead | nothing mutated | **MANUAL** (deliberate: the rubric's 4 instead of its 5) |
| 42 | table_lifecycle: complete -> awaiting_snapshot | `CDC_AMBIGUOUS_RESNAPSHOT=0`, or an undecidable fold that did not name a table, or a re-snapshot request that could not be recorded | the fold refuses and the queue write fails or is disabled | none; the same transaction replays and fails identically for ever | nothing wrong is written | **MANUAL** |
| 43 | table_lifecycle: in_progress -> awaiting_snapshot | the two readings of the re-snapshot's consistent point disagree | `agree_on_consistent_point` | run fails; the tables are re-marked `awaiting_snapshot`; next run takes a fresh `C` | nothing swapped, nothing fenced | AUTO |
| 44 | acquisition_recovery: resume_point_deleted -> armed | the load-bearing slot cannot be dropped on ONE attempt (another backend holds it) | `drop_slot` neither returns `dropped` nor `absent` | `RecoveryFailed`; the journal is intact and the next run retries the same phase | before: the journal stays at `resume_point_deleted` and the slot survives · after: `armed` | AUTO |
| 44b | acquisition_recovery: resume_point_deleted -> armed | the load-bearing slot can **never** be dropped (the holder never lets go) | the same `RecoveryFailed` every run | none; a human has to free the slot | nothing snapshotted against a surviving slot | **MANUAL** |
| 45 | acquisition_recovery: requested -> offsets_file_deleted | crash at any phase of an acquisition recovery | `_cdc_flight.recovery_state`, validated against the declared phase domain | the next acquisition resumes from the recorded phase; every step is idempotent | **INJECTED at all four boundaries** (`recovery_requested`, `recovery_offsets_file_deleted`, `recovery_resume_point_deleted`, `recovery_armed`) — before: the effect happened and the journal does not know, which the resume ladder re-runs for free · after: the phase advances | AUTO |
| 46 | acquisition_recovery: absent -> requested | source timeline forked (promotion / PITR) | `slot_state.timeline_id` change | automatic full re-snapshot + catalog forgotten | journalled; see 45 | AUTO |
| 47 | catalog_change: due -> refused | stale catalog after a rewind/fork makes the mass-drop breaker refuse the whole capture set | `source_relations` oids vs the new source | none by itself — but rows 15/16/46 discard `source_relations`, so it no longer arises from our own bookkeeping | destination untouched | AUTO (was the cause of row 26 firing spuriously) |
| 48 | — | `source.snapshot='incremental'` record reaches the assembler | `TransactionAssemblyError` | run fails and repeats; rubric 3.3 owns the mechanism | nothing partial swapped | **UNDEFINED** |
| 49 | — | a captured table's topic collides with a Debezium internal topic | `assert_no_internal_topic_collision` at start-up | refuses | n/a — a human chose the names | **MANUAL** (correctly) |
| 50 | source_health (domain) | the source is dark from the FIRST sample and never answers | `SourceHealth.ever_sampled` is false | the run falls back to timer-only idle and can report success on a partial delivery | n/a (memory) | **UNDEFINED** — **fail-open**, see the note below |
| 51 | — | an **undeclared state transition** is attempted in a DURABLE machine (`table_lifecycle`, `run_phase`, `acquisition_recovery`) | `Machine.check(from, to)` at the one writer of that state | refused; the previous (more conservative) state is kept, a `critical` alert is raised on the independent connection, and the exception propagates: the run fails | nothing wrong is written, so there is no cut | **MANUAL** (rev 9 — it means a defect in the Flight, and only a code change fixes it. It replaces a SILENT wrong state, which is strictly safer and strictly worse for this count.) |
| 51a | — | an undeclared transition in the MEMORY-ONLY catalog machine, on the polling thread | `CatalogChange.to()` raises; `CatalogWatcher.poll_quietly` records it in `machine_error` rather than in the transient `last_error` | the poll returns nothing, the destructive action is not applied, and `run_engine_bounded` raises `EngineFailure` at shutdown: **the run is not successful** | the polling thread owns no destination state, so nothing is half-written | **MANUAL** (rev 10 — corrected. Rev 9's row 51 promised a loud failure for *any* machine; the polling thread caught the exception, wrote it to `last_error` and let the run report success, and a live `due -> marked` event reached it. Codex r1 MAJOR-1.) |
| 52 | — | a durable state value **outside its frozen domain** is read (`table_state.snapshot_state`, `recovery_state.phase`) | `Machine.parse()` on every read | refused; the run does not start. A value in no domain belongs to no queue and no recovery path | n/a — reading is not a mutation | **MANUAL** (new at rev 9 — same trade as 51: it was silent before) |
| 53 | run_phase: starting -> reconciling | the run-phase heartbeat row cannot be written (the table is missing, the independent connection is refused) | the write raises and is caught | the phase is tracked in memory, the machine still checks every edge, and the run is unaffected: observability must never fail a run that is otherwise correct | the row is not load-bearing for any decision | AUTO (new at rev 9) |
| 54 | catalog_baseline: valid -> stale | a run cannot be assumed to have confirmed the source-catalog baseline | it has not finished; the mark is written BEFORE the engine starts, unconditionally | the next run reconciles from the durable mark instead of trusting an observation | `catalog_baseline_marked` — the mark is durable, nothing else has happened; the next run reconciles | AUTO (rev 14) |
| 55 | catalog_baseline: stale -> invalidated | a relation holds destination rows, has no recorded source identity, and the baseline was never confirmed | `catalog_baseline.unrelatable_relations`, from durable state alone | the observed identity is NOT adopted; the relation is queued as `recreated`, fenced, and left `awaiting_snapshot` for rubric 1.6's rebuild | as 54 — the names are in the row, so the obligation survives the process that found it | AUTO (rev 14 — this is the r5 BLOCKER, and it used to be silent adoption) |
| 56 | catalog_baseline: stale -> valid | — (the discharge) | ≥1 real catalog comparison **and** nothing unrelatable, recomputed after the flush | n/a: this is the healthy transition | `catalog_baseline_pre_valid` — the learned relations are durable and the promotion is not; the next run reaches the same verdict from durable state, so it is idempotent rather than one-shot | AUTO (rev 14) |
| 57 | catalog_baseline: invalidated -> valid | — (the discharge, after a rebuild was queued) | the relation is now `awaiting_snapshot`, so `table_lifecycle` owes its image and the baseline is no longer protecting it | n/a | as 56 | AUTO (rev 14) |
| 58 | catalog_baseline: valid -> absent | the recorded source catalog is forgotten (`--reset-state`, a source identity change) | `recovery.begin(forget_catalog=True)` | the claim is deleted in the SAME transaction as `source_relations`: a claim about a registry that no longer exists would suppress the reconciliation of the one replacing it | one transaction, so there is no cut | AUTO (rev 14) |
| 59 | heartbeat_sink_retirement (domain) | the terminal run-phase write never returns (a wedged observability cursor) | the write is bounded on a named worker; `close()` joins it with a bound and RELEASES the cursor rather than closing it under a live statement | the run tears down within the bound and exits on its own verdict; `last_run.json` carries `terminal_phase_write_abandoned` | n/a — the heartbeat is not load-bearing for any decision | **UNDEFINED** (rev 14 — honest: nothing clears the non-terminal heartbeat row that abandoned runner left behind, so an operator reading `_cdc_flight.heartbeat` alone sees a phase that never terminalised. `last_run.json` is the terminal record. Bounding the teardown was the merge-blocking half; sweeping the stale row is 4.4/6.1's) |

**The counts, parsed from the rows above rather than recalled.** 64 rows, one
failure and one terminal class each.

| class | count | rows |
|---|---:|---|
| `AUTO` | **40** | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 24b, 32, 35, 36, 40, 43, 44, 45, 46, 47, 53, 54, 55, 56, 57, 58 |
| `MANUAL` | **15** | 17b, 25, 26, 27, 28, 29, 33, 38, 41, 42, 44b, 49, 51, 51a, 52 |
| `UNDEFINED` | **9** | 30, 31, 32b, 35b, 37, 39, 48, 50, 59 |

**What rev 9 changed, and it is not flattering.** Three rows are new and two of them are
`MANUAL`: making an undeclared transition (51) and an out-of-domain durable value (52)
into loud refusals *created* two manual-intervention cases where there had previously
been two silent-corruption paths. That is the right trade for correctness and the wrong
direction for a rubric whose band is a count, and both facts are stated rather than one
of them.

**What rev 10 changed, and it is worse.** Row 51 claimed that policy for *any* machine
and the memory-only catalog machine did not implement it: `poll_quietly` caught the
`IllegalTransition`, wrote it to the same `last_error` a network failure uses, and let
the run report success — and a real, reachable `due -> marked` event was hitting it
(Codex r1 MAJOR-1). Row 51a is that case, split out with the policy the code now has: the
run fails. A fourteenth manual case, created by admitting one that was being under-counted
as automatic. 4.7 stays at **1** either way — its 1-band is "more than 2 cases that cause
manual intervention" and 15 is more than 2.

**The manual cases, and why each one is manual.** Six protect a destination from an
automatic action that could destroy it (17b, 25, 26, 27, 38, and 44's terminal form);
three are configuration a human wrote and only a human can fix (28, 29, 49); two are
deliberate opt-outs of automation (41, 42); one is a source-side misconfiguration (33);
and two are defects in the Flight itself, which no automatic route can fix (51, 52).
The branch's honest position is unchanged from rev 8: it converted eight
previously-permanent failures into automatic ones and *found more manual cases while
enumerating them properly*.

**The startup-dark fail-open (row 50), stated rather than buried** (Codex m1). Once the
slot sampler has succeeded, a source that goes dark forbids an idle declaration and fails
the run within `CDC_SOURCE_DARK_SECONDS` (A49). If the sampler has *never* succeeded —
no psycopg, no privilege, a source that was dark before we ever looked — `ever_sampled`
is false and the run degrades to the timer-only path, which can declare a quiet stream
idle and report success on a delivery that never started. That is deliberate: an
environment where the slot cannot be read at all is not one where refusing to run is
obviously safer, and 4.6's score of 3 does not rest on it. It is recorded as `UNDEFINED`
rather than `AUTO` because "the run reported success and delivered nothing" is not a
recovery, and closing it belongs with 4.4's heartbeat. `machines.SOURCE_HEALTH_STATES`
now at least gives it a name — `unknown_never_sampled` — which is what it never had.

#### A51.3 — the frozen decision domains

Classifications, not states anything moves through, and deliberately **not** dressed up
as machines. They exist so the inventory, the run summary and the tests share one
vocabulary; `RESNAPSHOT_DECISIONS` and `RECONCILE_DECISIONS` were previously declared and
consumed only by a test.

* **`slot_verdict`** (11 values) — What did the last acquisition conclude about the slot?
  `ok`, `fresh_start`, `source_unobservable`, `slot_ahead_of_destination`, `slot_missing`, `slot_recreated`, `source_identity_changed`, `source_timeline_changed`, `source_lsn_regressed`, `no_durable_destination_row`, `no_durable_row_full_snapshot`
* **`reconcile_decision`** (10 values) — What did `offsets.dat` versus the durable resume point turn out to be?
  `fresh_start`, `resume`, `file_missing_rebuilt`, `file_missing_no_durable_offset`, `file_missing_repair_disabled`, `file_corrupt_rebuilt`, `file_ahead_rebuilt`, `file_behind_rebuilt`, `file_offset_mismatch_rebuilt`, `orphan_accepted_resnapshot`
* **`source_health`** (6 values) — What is the source connector doing, as one named value rather than six timers?
  `unsampled`, `streaming`, `not_streaming`, `unknown`, `unknown_never_sampled`, `dark`
* **`heartbeat_sink_retirement`** (3 values) — Who owned the run-phase heartbeat cursor
  when the run tore down, and was it closed or abandoned?
  `never_opened`, `closed`, `abandoned`

  A domain rather than a machine **by this file's own test**: a crash never leaves
  durable state in an intermediate configuration here — the cursor dies with the
  process. What the r5 finding actually needed was an *owner* and a *bound*, not a
  durable machine, and dressing an in-process hand-off up as one would assert exactly
  the recoverable intermediate state it does not have (rev 14).


#### A51.4 — how UNDEFINED is found now

`Machine.unreachable_cells()` returns every `(from, to)` pair in `states × states` with
no declared edge. Most are nonsense (`live → live`), which is exactly why the *count* is
not the interesting output — the interesting output is that the set is **computable**, so
a genuinely missing edge cannot hide in prose. The test prints the cells for every
machine and asserts that every inventory row's edge exists, which closes the loop in the
direction that matters: a row can no longer name a transition the code does not have, and
a machine can no longer grow an edge no row accounts for without somebody looking at the
printed set.

What this does **not** do, stated plainly so a reviewer does not have to find it: it
cannot invent a row for a failure mode that is not a transition at all. Rows 28–31, 37,
39, 48, 51 and 52 are found by reading, as before. The mechanisation covers the durable
state machines, which is where every measured regression in this project has lived.

### A52 — re-snapshot completion, and the two things it is not

**The defect.** `Applier.snapshot_completed` became true when "at least one shadow has
swapped and no table is currently mid-snapshot". Debezium closes a table's snapshot chunk
the moment a record for the *next* table arrives, so at a batch boundary in that gap both
halves are true and the next table has not been scanned. The re-snapshot supervisor
stopped there, and `_finish_empty_tables()` then treated every requested table that had
not swapped as "the source relation held no rows", ran `DELETE FROM` against its **live**
destination table and wrote an audit row asserting an emptiness nothing had checked. The
`still_owed` guard that was supposed to catch a partial re-snapshot was provably dead
code: the same function appended every pending table to `emptied` unconditionally, so
`still_owed` was `[]` for every possible input (Codex B1 / Opus BLOCKER-1, reproduced
against a populated table).

**The rule now.** A re-snapshot is complete when **every requested table** reaches one of
exactly two terminal states:

* `swapped` — a shadow was built and atomically renamed over the live table. `C` is the
  snapshot records' own `source.lsn`, which is `slotCreatedInfo.startLsn()` (A45);
* `verified_empty` — three independent facts agree: Debezium emitted its own
  end-of-snapshot marker (`CompleteUnit.snapshot_last`, which the assembler had been
  decoding all along and the applier was not using), this table produced **zero** snapshot
  records, and a source count taken afterwards returns zero.

Anything else leaves the table untouched, re-asserts `awaiting_snapshot`, and fails the
run. `still_owed` is now reachable and tested.

**`C` for a verified-empty table is not `C` for a swapped one.** An empty table emits no
snapshot records, so it has no `source.lsn`. Polling the throwaway slot for one is a race
with no upper bound — for an all-empty capture set the engine can finish the image, enter
streaming and advance `confirmed_flush_lsn` before the first poll lands, and fencing above
the image is silent loss (Codex B2). A verified-empty table is instead fenced at
`pg_current_wal_lsn()` sampled **before** the emptiness check, on its own statement,
followed by a `REPEATABLE READ` count. Every transaction with a commit LSN below the
sample is visible to that count, so a count of zero proves no transaction below the sample
left a row behind; every transaction at or above it is not fenced and is applied on top.
Neither direction can lose.

**A disagreement between the two readings of `C` is now FATAL.** It used to take the
`min()`, on the argument that fencing too low can only re-apply. True, and insufficient:
re-applying **duplicates** on a keyless table, which violates rubric 1.2's exactly-once
claim — so `min` did not avoid a correctness violation, it chose a different one. And once
the two readings disagree, neither can be shown to identify the exported snapshot at all.
Both reviewers reached hard-fail independently (Codex B2, Opus Q1). Cost: one extra run.

### A53 — the acquisition recovery is a journalled state machine

Rubric 1.8's recovery mutates four independent durable things — the to-do list,
`offsets.dat`, the durable resume point, the replication slot — and nothing outside one
destination transaction can make two of them atomic. A50 used to claim the *order* made
every intermediate state recoverable. It did not (see the correction in A50).

The intent is now written **first**, durably, in one transaction with the table marking
and the catalog invalidation: a `_cdc_flight.recovery_state` row carrying the decision,
the phase, the slot name, the offsets path and the **forced `snapshot.mode`**. Every later
step is idempotent and re-entrant from the recorded phase, and the file is deleted before
the row.

| phase reached before the crash | what the next acquisition sees | what it does |
|---|---|---|
| `requested` | journal + owed tables; file, row and slot all still there | deletes the file, then the row, then the slot |
| `offsets_file_deleted` | file gone, row present — reconciliation calls this `file_missing_rebuilt` | deletes the row, then the slot |
| `resume_point_deleted` | file and row gone, slot present | drops the slot |
| `armed` | nothing durable left to undo | forces the journal's `snapshot.mode` and rebuilds |

The journal is cleared only when the work it asked for is done: no table owes a snapshot
and the destination has a resume point again. Clearing it earlier would discard the forced
snapshot mode the rest of the rebuild depends on — the exact cut that used to turn a
recovery into an ordinary fresh start (Codex B3).

**Dropping the slot may not be stepped over.** A45 measured that Debezium only pairs the
snapshot with an exact WAL position when it creates the slot itself, so a re-snapshot
against a surviving slot resumes the stream from a `confirmed_flush_lsn` we cannot account
for — past the snapshot's consistent point, which is the loss window rubric 1.8 exists to
close. A drop that neither returns `dropped` nor proves the slot `absent` now raises
`RecoveryFailed` with the journal intact. It used to be caught, recorded as the string
`drop_failed: ...` and stepped over, while the caller *also* erased the recorded LSN
baseline (Codex B4). `--accept-orphan-offsets` gets the same treatment: it drops the slot
first and refuses to delete anything if the drop fails, because the operator authorised
rebuilding a destination and not an uncoordinated image/stream boundary.

**Continuous WAL retention.** Codex B3 asks for at least one slot retaining WAL from the
snapshot's `C` through main-stream takeover, and the precise condition is now checked
rather than argued.

The throwaway `_rs` re-snapshot is safe **iff a durable resume point exists**: the main
slot is then retaining WAL continuously from that point and is never dropped, so the
image at `C` hands over to a stream that never stopped. With no durable resume point
there is no such guarantee — the main slot may not exist yet, and a transaction
committing between the throwaway slot's lifetime and the main slot's creation would be
retained by neither. `pipeline.run` therefore refuses to run a throwaway re-snapshot when
tables are owed, the resume point is absent, and `snapshot.mode` does not read data. That
combination is reachable (`--snapshot-mode no_data` on a fresh state directory that still
owes tables) and it is the only shape that opens the window.

After a *recovery* it cannot arise: the journal forces a data-reading mode and the resume
point is gone, so `will_snapshot_everything` is true and the **main** engine's own
coordinated snapshot is the rebuild — Debezium creates the slot and exports the snapshot
in one operation, with no gap at all.

**Five facts a parallel state-machine architecture review supplied, folded in here
rather than left for the refactor that will own them.**

1. **`in_progress` is durable, non-terminal, and belonged to no queue.** It is written
   the instant a table's first snapshot record arrives and cleared only by the swap, so
   a process that dies inside a snapshot leaves it behind — and the only thing that ever
   recovered from it was the applier's `except BaseException`, which `os._exit` (the
   fault injector, the commit watchdog's `EX_TEMPFAIL`) and `SIGKILL` step straight
   over. The journal's "no table owes a snapshot any more" test could therefore pass
   over a half-built table and the run could log that every captured table had a fresh
   image. `promote_interrupted_snapshots()` runs once at start-up — nothing is
   mid-snapshot then, by definition — and the journal-clear test uses the same
   `SNAPSHOT_STATES_OWING_WORK` predicate the queue does.
2. **The `snapshot_state` domain is frozen and validated on read.** §4.8 declared
   `none|in_progress|complete|failed`; `failed` was never written by anything and
   `awaiting_snapshot`, which the entire re-snapshot machinery runs on, was not in the
   declared set. A value outside `destination.SNAPSHOT_STATES` now raises rather than
   silently belonging to no queue.
3. **`recovery.PHASES` is enforced.** It was declared and never checked: `read()`
   accepted any string, `resume()` matched none of its branches, fell through every
   `if`, and logged "recovery is ARMED" having done nothing — a silent no-op wearing a
   success message. An unrecognised phase is now `RecoveryFailed`.
4. **`--accept-orphan-offsets` is journalled; `--reset-state` deliberately is not.**
   The first is the same shape as the acquisition recovery — drop the slot, delete the
   file, force a data-reading snapshot — and its forced mode lived only in a local
   variable, so a crash lost it exactly the way B3 described. The second needs no
   journal because **every** intermediate state converges on the outcome that was asked
   for rather than on a refusal (state-dir gone + row present is `file_missing_rebuilt`;
   row gone is a full snapshot anyway), and the one hole — a non-data `snapshot.mode`
   turning "start over" into "stream onto tables we just cleared" — is closed by forcing
   the mode rather than by persisting an intent. The reasoning is written at the code.
5. **`_cdc_flight.heartbeat` now exists.** §4.8 and D9.1 declared it and nothing ever
   created it, so the observability surface rubrics 4.5/4.6/6.1/6.2 are scored against
   was absent. The writer belongs to 4.4/6.1 and is still absent; creating the table now
   means that task lands a writer rather than a migration.

**The throwaway slot is swept.** It leaked on every failure route out of `resnapshot.run`
(there was no `try/finally`), and a leaked logical slot holds WAL on the source for ever
and counts against `max_replication_slots` — it leaked twice on the shared development
cluster in a single day, from two independent review sessions, and the second one made a
later probe fail with "all replication slots are in use" (Opus MAJOR-2). There is now a
`try/finally` around the whole engine section **and** an unconditional start-up sweep of
the one `_rs` name this pipeline derives from its own slot.

### A54 — a fault test must name the anchor that fired

Rubric 1.7's claim is that a *named* anchor produces a *named* outcome, and the suite
could not carry that. Both reviewers found the same shape from different directions:

* `test_no_anchor_is_allowed_to_be_silent()` asserted that a hand-written dictionary
  contained no `SILENT` string. It could only fail if somebody edited the dictionary, and
  it was the test the "the SILENT bucket is empty" claim pointed at (Opus MINOR-1);
* the parametrised matrix accepted any non-zero exit without establishing that the
  **selected** fault had fired, so a run that died of an unrelated start-up problem passed
  (Codex M2);
* `test_a_hung_commit_is_bounded...` accepted `returncode in (75, -9, 137, 1)`. 75 is the
  commit watchdog's own `EX_TEMPFAIL` and the entire point of the test; `-9` is the
  harness giving up, `137` is the injector's default, `1` is anything. It passed if the
  run died of anything at all (Opus MAJOR-5).

Every anchor now writes `$CDC_STATE_DIR/fault_fired.json` — point, `<nth>`, action, pid —
**before** it does anything else and fsynced, so even `os._exit` leaves it. The outcome
class is then *derived* from the run (`_observed_outcome`): an armed anchor that left no
record is `SILENT` and fails, whatever the exit code said. `destination_hang` must exit
exactly 75.

Three more corrections came with it:

* `hang_seconds` was `<action>` reinterpreted as a duration, so `destination_hang:1` hung
  for **137** seconds — the default exit code — which is undocumented and, against the
  shipped `CDC_COMMIT_TIMEOUT` of 300 s, *shorter than the watchdog it exists to test*.
  It is now `CDC_FAULT_HANG_SECONDS`, defaulting to an hour.
* `destination_commit` raises **before** `COMMIT` runs, which is an ordinary uncommitted
  failure wearing an ambiguous name. `destination_commit_late` executes the `COMMIT` and
  *then* raises, which is the genuinely ambiguous shape §4.6 F5 is about: the destination
  committed and we cannot know it. Both are kept and both must recover exactly.
* the chaos harness gave its recovery runs **no** fault environment, so a fault could
  never fire during another fault's recovery — the composition its own docstring claimed.
  Recovery runs now carry a fault, the plan is a shuffled **cover** of the anchor set
  rather than a uniform sample, every iteration asserts its anchor fired (an iteration in
  which nothing fired is a missing case, not "a perfectly good data point"), and more than
  one seed runs.

---

## 20. Rubric 1.9 — explicit state machines

### A55 — four machines, one precedence, and the seven candidates that are not

Rubric 1.9 (added 2026-07-31) asks that *any state that can affect consistency is managed
with a state machine approach*: **no state machines = 1, only one big state machine = 3,
an appropriate number (over 1) = 5.**

The argument for doing it is not aesthetic. Every named regression this project has
recorded is one shape: **a state that exists in the design, is represented as a derived
expression over two or more variables, and is therefore mutated by a path the design did
not enumerate.**

| finding | the state | how it was represented | the bug |
|---|---|---|---|
| Opus MAJOR-1 (1.4/1.5) | "this group is being committed" | `_group` list + implicit success path | `_reset_group()` only on success ⇒ rolled-back group re-folded ⇒ **measured row loss** |
| Opus B5 / A49 | "the connector is streaming" / "why the run stopped" | timers, then a last-writer-wins string | a blackholed Postgres exited `ok: true`, three rounds running |
| Codex B1 / Opus BLOCKER-1 | "the re-snapshot completed" | `swaps > 0 and not active` across 3 modules | true at a batch boundary ⇒ a live table's rows deleted |
| Codex B3 / Opus MAJOR-1 (1.6-1.8) | "a recovery is in progress" | four unjournalled mutations + a local variable | permanent refusal to start, three restarts running |
| Opus MINOR-2 (1.5) | "this catalog change is fenced" | `CatalogChange.fenced` bool | documented as gating the action; gated nothing |
| architecture review, finding 1 | "this table's image is trustworthy" | `snapshot_state`, unvalidated, `in_progress` in no queue | a half-snapshotted table owed work and belonged to no queue |

Explicit machines do not make the system correct. They make the **unenumerated path** a
run-time error instead of a review finding.

**The distinguishing test for whether a state needs a machine** is: *can a crash leave
durable state in an intermediate configuration?* If no, the implicit style is fine and a
machine is ceremony — worse than ceremony, because it advertises recoverable intermediate
states that do not exist. If yes, the state needs a name, a persisted value and a
transition table.

#### What was built (four machines + one precedence)

| machine | owns | states | edges | persistence |
|---|---|---|---|---|
| `table_lifecycle` | is this destination table a trustworthy image, and who owes the work | 5 | 21 | `_cdc_flight.table_state.snapshot_state` |
| `run_phase` | where is this run, readable from the destination while it runs | 9 | 23 | `_cdc_flight.heartbeat.phase` |
| `run_outcome` | why did this run stop — cause before symptom | 9 | 36 (a **precedence**: escalations only) | `heartbeat.terminal_reason`, `last_run.json` |
| `acquisition_recovery` | what has this destructive recovery already done | 5 | 9 | `_cdc_flight.recovery_state.phase` |
| `catalog_change` | where is one DDL fact in observe → confirm → fence → apply | 9 | 30 | **memory only** |

Generated transition tables: §A51.1. Declarations: `cdc_flight/machines.py`, which is one
file an operator or a reviewer can read to see every consistency-affecting state in the
system. Mechanism: `cdc_flight/states.py` — plain-`str` states (they are already durable
strings in `VARCHAR` columns, in `last_run.json` and in a hundred test literals, so an
`enum.Enum` would need a migration and would break every existing SQL comparison for no
gain), `machine.check(from, to)` raising `IllegalTransition`, `machine.parse()` raising
`UnknownState`, and `machine.table()` emitting the transition table as data.

#### What each one closed

* **`table_lifecycle`.** One durable column had five writers with their own SQL. There is
  now exactly one `UPDATE ... SET snapshot_state` and one `INSERT ... snapshot_state` in
  `src/`, both in `cdc_flight/table_lifecycle.py`, and a test greps the tree for a second
  one. Two edges are deliberately **absent** and both are past bugs: `none -> complete`
  (a table that just looks healthy — Codex B1's shape) and `in_progress -> in_progress`
  (a second shadow opened over a durable half-finished snapshot, which is how the residue
  stayed invisible). `tables_awaiting_snapshot()` now selects every **non-terminal** state
  rather than the one literal `awaiting_snapshot`, so the queue no longer depends on
  somebody having called the start-up promotion. An undeclared edge is refused, keeps the
  previous (more conservative) state, and raises a `critical` alert on the independent
  connection.
* **`run_phase`.** ADR §4.8 has declared `_cdc_flight.heartbeat.phase` since rev 1 and
  nothing ever wrote it, so "where is this run" was a source-line position in a 470-line
  function and a `last_run.json` on the machine that ran. One row per run, updated on
  each transition, on the independent connection, never inside a commit group and never
  inside the commit→ack window. `phase_since`, `terminal_reason` and `phase_history` are
  added by a migration, because `CREATE TABLE IF NOT EXISTS` cannot add a column and the
  table shipped one round ago — including into the shared MotherDuck database. **This is
  the phase writer only**: the periodic liveness/lag heartbeat and the source-side
  WAL-advancing heartbeat remain 4.4/6.1's, with their own cadence and their own design.
* **`run_outcome`.** A49's defect was a `finally` overwriting `stop_reason='source_dark'`
  with `'hung'` — a dark source makes `engine.close()` hang almost by definition, so the
  symptom replaced the diagnosis. The fix at the time was
  `if stop_reason not in ("source_dark", "engine_error")`, written out at
  `supervisor.py:180` **and** `:186`; a tenth outcome had to remember both. The
  precedence is now `max_seconds < idle < work_done < engine_finished < hung <
  catalog_unresolved < source_dark < engine_error < error`, and `hung` sitting below
  `source_dark` *is* the rule. `RunOutcome.record()` keeps the most severe value it has
  been given and counts the refusals; the run summary reports them
  (`outcome_downgrades_refused`), because "we nearly reported the symptom" is
  operationally interesting.
* **`acquisition_recovery`.** Rev 8 landed the journal and rev 8's fix round added the
  domain check (`PHASES` had been declared and never consumed, so `read()` accepted any
  string and `resume()` fell through every branch and logged success). Rev 9 adds the
  **edges**: `requested -> armed` is not a transition, so no future caller can claim a
  slot was dropped that never was, and `-> absent` (clearing the journal) is reachable
  only from `armed`, so a clear cannot discard a half-done destructive sequence.
* **`catalog_change`.** Seven per-relation states were spread over four containers and
  three counters, which is how `fenced` came to be documented as gating an action it never
  gated (Opus MINOR-2). One `state` field, memory only — a lost pending change is
  re-detected, which is correct, so persistence would buy nothing — and `applied` is
  reachable only from `due`, which is the LSN fence.

#### The seven candidates that are deliberately NOT machines

Recorded so the "appropriate number" claim is an argument rather than a count.

| candidate | why not |
|---|---|
| commit-group assembly | crash ⇒ discard and replay is the whole story under Invariant O; a durable machine would advertise recoverable intermediate states that do not exist. Refactored to one `OpenGroup` instead — see A56 |
| transaction assembly (`assembler.py`) | **already** a guarded machine with its own error type naming every rule it enforces; the only component that has produced no correctness blocker in four review rounds. Do not touch |
| spill unit | staging happens inside the group's own transaction and is never committed separately, so `DISCARDED` is what `ROLLBACK` does for free. *If the separately-committed-staging variant is ever adopted, this becomes a durable machine and must be built as one* |
| lease | already explicit and durable, with `LeaseLost` on every illegal transition and a provably-dead-owner reclaim |
| `SourceHealth` | a **fold** over observations, not a machine. What was missing was a declared *classification*, which is now `machines.SOURCE_HEALTH_STATES` — and it finally names `unknown_never_sampled`, A51 row 50's fail-open, which had no name at all |
| `check_slot` | a pure decision function over an external configuration with a documented table; the eleven outcomes are a frozen **domain**, not states anything moves through |
| offset-file reconciliation | likewise: ten decisions against a documented table, frozen as a domain so the inventory, the summary and the tests share one vocabulary |

#### Composition, and the one nesting rule

```
RunPhase (per process, heartbeat row)
 ├── AcquisitionRecovery (per pipeline+namespace, recovery_state row)  [0..1, spans runs]
 ├── TableLifecycle (per pipeline+schema+table, table_state row)       [N, spans runs]
 ├── CatalogChangeState (per relation, memory)                        [N, per run]
 └── OpenGroup (memory, no persistence — Invariant O)                 [1 at a time]
```

**A run may not clear an `AcquisitionRecovery` while any `TableLifecycle` is
non-terminal.** That single invariant subsumes A52's `still_owed` guard and the
recovery-clear predicate, and it is one query — which is the interpretability claim in
concrete form: "where is this pipeline" stops being re-derived from `stop_reason` plus
`snapshot_state` plus `slot_check` plus three in-memory sets.

### A56 — the commit group becomes one object, and that is not a state machine

Opus MAJOR-1 (1.4/1.5) measured a lost row: `_reset_group()` was called only on the
success path, so a group whose `COMMIT` failed stayed buffered and was folded a second
time alongside whatever had arrived since. Idempotent shapes survived, which is why the
fault tests passed; a key-reuse shape did not. The fix at the time was a **second** reset
function, `_reset_after_rollback()`, which has to stay in sync with the first.

The sixteen fields are now one `commit_group.OpenGroup` dataclass, created at BEGIN and
**replaced** at COMMIT and at ROLLBACK. "Reset" is `self.group = OpenGroup()`, on both
paths, so neither can forget a field — which is the defect that was measured. The test
asserts the object *identity* changed rather than enumerating the fields, which is exactly
the enumeration that diverged.

**Narrowed at A58.6, and it stays narrowed** (Codex r3 MINOR-1 found this paragraph still
overclaiming). `OpenGroup` is a mutable dataclass with public collections: a partial
*mutation* is representable and one is deliberate — `discard_units()`, which shutdown uses
to give up a tail Invariant O guarantees will replay. The guarantee is about the reset
paths, not about the type.

Two consequences worth stating:

* `SnapshotCoordinator` takes `created_in_txn` as a **callable**, not as the set. A
  coordinator that had captured the set would keep writing into a discarded group's copy,
  which is the same bug arriving through a captured reference instead of a missed reset.
* This is emphatically **not** a durable state machine, and the reason is Invariant O.
  Adding one would suggest the group has recoverable intermediate states; the entire
  correctness argument is that it does not.

### A57 — the acquisition recovery gets fault anchors of its own (rubric 1.7 → 5)

Rubric 1.7 was held at 4 for one stated reason: the acquisition-recovery crash cuts were
proven through a **test seam** (`recovery.resume(on_phase=...)`), not through injected
faults. A raised Python exception is not a crash. It unwinds `finally` blocks, closes the
destination connection and flushes the JVM; `os._exit` does none of that, and the claim
under test is precisely that *durable state alone* is enough. The gap is not theoretical:
the architecture review's finding 1 was an `except BaseException` handler that `os._exit`
steps straight over.

Five anchors are added to `faults.ALL_POINTS`, one per boundary of the recovery plus one
inside the queue write:

| anchor | fires | what must survive |
|---|---|---|
| `recovery_requested` | after the journal row and the to-do list commit | the journal, at `requested`; nothing destroyed |
| `recovery_offsets_file_deleted` | after `offsets.dat` is unlinked, before the phase is recorded | `file absent / row present` — reconciliation rebuilds it |
| `recovery_resume_point_deleted` | after the durable resume point is deleted, before the phase is recorded | the slot survives; the next run re-runs the drop |
| `recovery_armed` | after the slot is dropped, before the phase is recorded | the forced `snapshot.mode`, which used to live only in a local variable (Codex B3) |
| `table_rebuild_queued` | inside `begin()`'s transaction, while the to-do list is being written | neither the journal nor the marking — they are one transaction |

`<nth>` for these is **per boundary arrival**, not per commit group: a recovery boundary
is not a commit group, and an index that is a function of the workload is one that
silently stops firing (Opus M7). The matrix (`test_1_7_fault_matrix.py`) enumerates them
from `ALL_POINTS` and requires a declared outcome for each; the default-suite guard is
`test_1_7_recovery_anchors.py` (milliseconds, injectable slot drop); the slow lane kills a
**real** `cdc-flight` process at `recovery_armed` against a real Postgres slot and then
compares the whole destination against the whole source
(`test_1_8_recovery_crash_e2e.py`).

### A58 — rev 10: the operator routes are journalled too, and one owner per fact

The first Codex review of rev 9 scored 1.9 a **3**, not a 5, and it was right for a
reason that is easy to state: the machines were real but the **word "any" was not
satisfied**. Three consistency-affecting sequences still lived outside them, two facts
still had two owners, and one machine was a shadow of the containers it was supposed to
replace. Rev 10 is the answer, and every item below is a behaviour change.

#### A58.1 — `--accept-orphan-offsets` journals before it destroys (BLOCKER)

`offset_reconcile.reconcile()` dropped the replication slot and unlinked `offsets.dat`,
and `pipeline.run()` wrote the recovery journal **after** it returned. The comment saying
so called the placement deliberate. It is the B3/A53 shape recreated on the one route an
operator reaches for when something has already gone wrong:

1. the operator authorises a rebuild;
2. the slot is dropped and `offsets.dat` is unlinked;
3. the process dies before `recovery.begin()` commits;
4. the next run sees no resume row, no offsets file, no slot and **no journal**, and
   classifies that as an ordinary `fresh_start`;
5. under a configured non-data `snapshot.mode` it streams onto the old destination
   without rebuilding the baseline the operator explicitly asked to rebuild.

A crash between the drop and the unlink instead strands the operation behind the orphan
refusal. Both outcomes contradict the journal's purpose.

`reconcile()` now **classifies and mutates nothing**; `dsn` and `slot_name` are accepted
and ignored so a caller that passes them is not silently mis-wired. `recovery.begin()`
writes the intent and the whole captured obligation in one transaction, and the one
idempotent `resume()` ladder performs the file, the row and the slot. The route is
therefore anchored at every boundary it already had, and the evidence is a real `os._exit`
at `recovery_requested` followed by a restart that does **not** repeat the flag, under
`--snapshot-mode no_data`, ending in exact source/destination equality
(`test_1_8_operator_route_crash_e2e.py`).

#### A58.2 — `--reset-state` is a journalled recovery (MAJOR)

Rev 9 argued reset needed no journal because every intermediate state converges. Two
steps of that argument are false:

* after the resume row is deleted, `check_slot()` runs **before**
  `will_snapshot_everything` is computed. With a positioned slot over a populated
  destination it returns the deliberate `no_durable_destination_row` refusal — and
  repeating `--reset-state` does not drop that slot, so it does not necessarily finish
  the reset either;
* the forced `props['snapshot.mode'] = 'initial'` was a local variable, so a crash after
  the file and the row were gone let the next ordinary run start fresh under a configured
  `no_data` mode.

Reset is now `recovery.begin(decision='operator_reset', ...)`: the table bookkeeping goes
back to `none` **inside the journal's transaction** (through `TableLifecycle`, as before),
`source_relations` is discarded through the existing `forget_catalog` flag, and the ladder
clears the state directory, deletes the resume row and **drops the slot**. Dropping the
slot is not incidental — it is what makes the sequence converge, and it is required for
correctness anyway, because Debezium only pairs a snapshot with an exact WAL position when
it creates the slot itself (A45). The lease row is still deleted outside the journal,
before the lease is acquired: it destroys no data and records no obligation.

#### A58.3 — one `RunOutcome` per run, and terminalise before reporting (MAJOR)

`run_engine_bounded()` built one `RunOutcome` and `RunPhaseWriter` built another. The
supervisor returned the first as `stop_reason`; the phase writer published the second as
`run_outcome`. Successful end-to-end runs therefore shipped

```text
ok=true, stop_reason="idle", run_outcome="max_seconds", run_phase="draining"
```

while the destination heartbeat correctly held `phase='stopped', terminal_reason='idle'`.
A severe result could be represented by the untouched mild default, which is A49's class
arriving through duplicate ownership instead of last-writer-wins.

There is now one `RunOutcome`, constructed in `pipeline.run()` and passed to both. The
returned summary dict is the very dict `main()` prints, and the outer `finally` updates it
**after** `stopping`/`stopped`/`failed` — so `last_run.json` and the heartbeat are two
projections of one state. A failure that unwinds outside the engine records `error` on the
same object, but only when nothing more specific has been diagnosed, because `error` is
the most severe value in the precedence and would otherwise bury `engine_error`.
`OUTCOME_FAILURES` no longer contains `engine_finished`: that is a success for a
terminating snapshot mode and a failure otherwise, and severity alone cannot decide it.

#### A58.4 — the commit→ack window is a wall-clock exclusion (MAJOR)

The callback's synchronous instruction sequence was clean, and that is not the claim the
ADR made. On `max_seconds`, engine error or source-dark the supervisor breaks the loop
without checking `handler.busy` and writes `draining` on its own thread — while the engine
thread can be between `COMMIT` and `markBatchFinished()`. "Never in the window" was false.

`run_state.COMMIT_ACK` is a flag the applier enters immediately before `COMMIT` and leaves
immediately after the acknowledgement. Two plain attribute assignments, no lock and no
allocation: taking a mutex inside the one sequence that must contain nothing else would be
exactly the unrelated work the principle excludes. A phase write that arrives while it is
set is **dropped** — never deferred behind a lock, never blocking — and counted into the
run summary, because every write states the whole row and the next transition restores it.

Separately, `con.cursor()` failing used to set `_sink = con`, which put observability
statements on the applier's own connection, inside its open transaction, from another
thread. It is `None` now: no independent connection means no row.

#### A58.5 — the catalog machine owns the catalog (MAJOR)

`CatalogChange.state` was added *beside* `_unconfirmed`, `_pending`, `fenced`,
`deferrals` and `confirmations` rather than replacing them, and it showed:

* the first observation's object went `observed -> unconfirmed` and was **discarded**;
  the confirming poll constructed a new one that went `observed -> pending`. The declared
  `unconfirmed -> pending` edge described no object production ever advanced;
* `fenced` was a second representation of `marked` that a caller had to remember to set;
* there was a **live, reachable undeclared edge**. `due()` leaves a change in the list
  until the COMMIT resolves it; a poll that overlaps the applier then called
  `_queued(change).to(marked)` over every live change, and `due -> marked` is not
  declared. `poll_quietly()` caught the `IllegalTransition`, wrote it to the same
  `last_error` a network failure uses, and let the run report success — so A51 row 51's
  promise of a loud failure was not kept for this machine.

Now: `_unconfirmed` holds the object whose state *says* `unconfirmed` and carries the
streak on it; `fenced` is derived from the state history; `pending()` filters on state;
`queue()` is the `observed -> pending` edge and the only way in; `_emit_marker` marks only
changes from which `-> marked` is declared. An undeclared edge on the polling thread sets
`machine_error`, and `run_engine_bounded` raises `EngineFailure` on it. A51 row 51 is
split, with 51a stating the policy this machine actually has.

#### A58.6 — ownership: domains, the completion predicate, the slot verdict (MAJOR)

* `SLOT_VERDICTS` and `RECONCILE_DECISIONS` were referenced only by tests, so they froze
  nothing. `SlotVerdict` and `Reconciliation` parse them in `__post_init__`.
* The recovery-clear predicate lived in `pipeline.py`, re-derived the obligation from
  *all* current lifecycle rows, called `clear()` directly, and — when false — only added a
  summary key while the run still reported `ok: true`. It is `recovery.complete_if_ready()`
  now: it validates the **journalled** captured set, performs `armed -> absent` itself, and
  returns a typed `Completion`. An uncleared recovery raises `EngineFailure`, so the run
  exits non-zero. `run_outcome` gains `recovery_uncleared` for it.
* `recovery_state` persists `captured_json` (the obligation) and `state_dir` (reset only).
* `slot_state` persists `verdict`, `verdict_message` and `verdict_at` in the observation's
  own transaction, so the destination can explain why a rebuild started. A typed
  classification, not a new machine.

#### A58.7 — the 1.7 evidence, made non-vacuous (MAJOR)

* All four recovery boundaries are now cut by a **real `os._exit`** in a child process
  (`tests/recovery_crash_driver.py`), not by exception unwinding; the `:raise` variants
  stay as the *error-teardown* path, which is a different lifecycle (§1.2).
* `table_rebuild_queued` fires after the **first** captured table has taken its
  `-> awaiting_snapshot` edge, so it proves a torn queue rather than a pre-write rollback.
* The chaos harness's composed fault is still armed at `<nth>=2` and still allowed not to
  fire, which is right for a terminating random walk and wrong as evidence. A bounded
  scenario now *requires* one: a hard death at `post_commit_pre_ack`, then `pre_commit:1`
  during the replay run, which the replay necessarily reaches.
* The two operator routes have end-to-end crash coverage of their own (A58.1, A58.2).
* `close_hung is True` is gone from the blackhole test: whether the JVM finishes `close()`
  inside 30 s is a race, not a correctness property, and it was recorded flaking. What is
  asserted is that a reported hang did **not** replace the diagnosis, which is all A49
  needs.

#### A58.8 — decomposition, again

`applier.py` and `pipeline.py` were back within a hundred lines of the 1,000-line
threshold once this round's changes landed on top of `b7f7cb7`'s 967 and 953.
`OpenGroup` moves to `cdc_flight/commit_group.py` and the four pre-engine decisions move
to `cdc_flight/acquisition.py`. Both seams are argued rather than arbitrary: `OpenGroup`
has no dependency on the applier, and the acquisition decisions are testable against a
DuckDB file with no JVM. Sizes after rev 11 are in `RUBRIC_STATUS`, measured rather than
recalled; no file crosses the threshold.

### A59 — rev 11: what round 2's review found in round 1's fixes

The re-review confirmed the orphan-route ordering, the `due -> marked` catalog edge, the
hard-death anchors, the composed-fault evidence and the `OpenGroup` narrowing, and found
that four of the round-1 fixes were incomplete and one was actively wrong. Everything
below is a reproduction the reviewer ran, not a reading.

#### A59.1 — a journal may only clear over POSITIVE terminal evidence (BLOCKER)

Journalling `--reset-state` introduced a worse defect than the one it closed. Reset's
table action was `reset_all()` — every captured table to `none` — and it recorded
`tables_marked = 0`. Completion then asked only whether a captured state was in
`LIFECYCLE_OWING_WORK`, and `none` is not; nor is a **missing** lifecycle row. The
`tables_marked > 0` guard also exempted every reset from the resume-point requirement.
The obligation was therefore satisfied by doing nothing at all.

That is reachable, and the reviewer reached it. A source relation with **zero rows**
emits no Debezium snapshot records, so `SnapshotCoordinator` never opens a shadow for it
and never swaps one in: the destination table keeps exactly what it had. Truncate
`app.documents` at the source without running CDC, then `--reset-state`, and the run
returned `ok=true / idle`, cleared its own journal, and left two rows the source no
longer had. The operator action whose entire purpose is a clean baseline certified stale
data as success.

Three changes, and all three are needed:

1. **the obligation is real.** `recovery.begin()` still resets the per-table snapshot
   bookkeeping for `operator_reset` — that is what "start over" means for `snapshot_epoch`,
   `snapshot_lsn` and `last_commit_id` — and then marks **every captured table
   `awaiting_snapshot`**, exactly as every other recovery does, with a truthful
   `tables_marked`;
2. **completion demands `complete`.** `not in LIFECYCLE_OWING_WORK` was wrong in both
   directions; `complete` is the one state that says the destination table holds a
   trustworthy image. A resume point is required of every recovery, reset included;
3. **an empty source table reaches `complete` positively.** The blocking re-snapshot has
   always closed this with `EmptinessEvidence` — end-of-snapshot marker, zero records for
   this table, a source count of zero, fenced at a WAL position sampled before the count.
   `resnapshot.finish_empty_tables_after_main_snapshot()` asks the same three questions
   after the **main** engine's snapshot, so a reset over an emptied table converges in one
   run instead of failing and self-healing on the next. A table that fails any of the
   three is left untouched and stays owed, which fails the run — the conservative half is
   unchanged.

#### A59.2 — the commit→ack window is a gate, not an observation (MAJOR)

A58.4 replaced a false claim with a weaker false claim. `RunPhaseWriter._write()` read
`COMMIT_ACK.active`, then built a timestamp, then executed SQL — and a database call
releases the GIL, so the applier could open the window in between. A two-thread barrier
reproduced the `UPDATE` running with the flag true and `dropped_writes == 0`.

The protocol that carries the claim is one mutex used asymmetrically. An independent
writer holds it for **the check and the write together**. The applier takes it in
`enter()`, which happens **before `COMMIT`** — so waiting there costs nothing the
principle protects; it only delays *opening* the window until an in-flight write has
finished. `leave()` is a plain assignment, so the acknowledgement path itself takes no
lock, and it is in a `finally` because `markProcessed`/`markBatchFinished` can raise.

Two honest edges, both measured rather than asserted: the wait in `enter()` is bounded at
5 s so a stalled observability connection can never stall the commit path, and the
`overlaps` counter records every time that bound is hit; and the **terminal** phase write
waits for the window rather than being dropped, because a dropped `stopped`/`failed` row
leaves the heartbeat non-terminal for ever and there is no next transition to restore it.

#### A59.3 — a pre-engine failure carries the same projection (MAJOR)

Fixing the normal path left the failure path behind: `reported` was populated only on the
inner engine success and `EngineFailure` routes, so a lease refusal wrote heartbeat
`failed/error` while `main()` built its summary from the exception and shipped no
`run_phase` and no `run_outcome` at all. The escaping exception now carries the one
projection, and the outer `finally` fills it in after the terminal transitions — so
`last_run.json` and the heartbeat agree on a route that never reaches the engine.

#### A59.4 — the catalog verdict is taken on a quiesced watcher (MAJOR)

`run_engine_bounded()` checked `catalog.machine_error` once and could return success
while the polling thread was stopped only later, in `pipeline.run()`'s `finally`. A poll
already in flight could take an undeclared transition after the check, and nobody re-read
the field: the same "success over an undeclared edge" policy A58.5 claims is impossible,
moved into a timing gap. The supervisor now calls `catalog.stop()` — which sets the event
and joins — before any verdict is taken.

#### A59.5 — `connect_timeout` does not bound a query on a live socket (MAJOR)

The mandated network-blackhole proof was timing-dependent: it failed on `stop_reason ==
'source_dark'` in one run and passed in the next. The cause is that `SourceHealth`'s
sampler bounded only the **handshake**. A relay that blackholes packets after the socket
is established leaves the query blocked on a recv that never returns, so the sampler
stops publishing entirely — `unknown` is never recorded, `unknown_for` never reaches
`CDC_SOURCE_DARK_SECONDS`, and the run dies of the shutdown symptom with the diagnosis
never formed. The precedence cannot preserve a diagnosis nothing recorded.

The sampler now sets `statement_timeout` (the server's bound) plus keepalives and
`tcp_user_timeout` (the client's, and the one that actually fires against a blackhole),
both well under the dark threshold.

#### A59.6 — the minors

* `_table_columns()` caught an introspection failure and returned `None`, and the caller
  silently returned — which recreates the silent-writer-failure the new
  `ControlSchemaFailed` exists to prevent, one step earlier. It raises. The migration test
  now uses the exact prior DDL, with its primary key and a row in it.
* The `in_progress -> in_progress` regression test drove the lifecycle writer directly, so
  it would have stayed green if `SnapshotCoordinator.state_for()` regressed to
  side-effect-first ordering. It drives the real coordinator now and asserts the shadow,
  the registry and `created_in_txn` are untouched by a refused edge.
* `--reset-state` and `--accept-orphan-offsets` name **every** destructive surface in
  `--help`, including the replication-slot drop.

### A60 — rev 12: what round 3's review found in round 2's fixes

Round 3 confirmed the empty-table reset fix, pre-engine reporting, the migration
introspection, the coordinator side-effect test, the destructive `--help`, and the
blackhole determinism (two consecutive green runs). It found one reproduced blocker and
three majors, all of them the same shape: **a guarantee that held on the path the test
took and not on the path production takes.**

#### A60.1 — learned catalog state must survive a run that commits nothing (BLOCKER)

A59 removed `CatalogCoordinator.plan()`'s early return so a plan with no due action still
carries the watcher's learned relations. That only helps if the applier commits a group.
A quiet stream with **zero records** never reaches the plan/apply path at all, so
`source_relations` — the only thing that makes a `DROP` or a drop-and-recreate detectable
across a restart — was still empty at shutdown.

The consequence was measured end to end: a populated destination, a quiet run under the
default `replicate` mode (`ok=true`, zero records, zero commit groups, zero persisted
relations), then an offline drop-and-recreate with one replacement row. The next run
applied the insert, reported `catalog_changes_applied=0`, and persisted the **new** oid as
though it had always owned that relation — leaving the old relation's two rows beside the
replacement's one, permanently, because from then on the persisted oid agrees with the
source. A successful, silent, self-concealing inconsistency.

`destination.flush_learned_relations()` now persists them once per run, in its own
transaction, with the same `exclude` guard the commit path uses (a row carrying the new
oid of a relation whose destructive action is still pending would make the next run agree
with the source and never notice the drop). The order is fixed and it is the order the
review asked for: **quiesce, validate, flush, report.**

#### A60.2 — an all-empty capture set needs a durable handoff point (MAJOR)

Requiring a resume point of every recovery was right. But an entirely empty capture set
emits zero Debezium records, so the applier commits zero groups and writes no resume
point — and `--reset-state` then failed with `recovery_uncleared`, as did every run after
it, because no new fact could ever produce a commit group. Deterministically
non-convergent: it fails closed, which is not the old blocker, but it makes the
destructive recovery unusable for a legitimate source shape and breaks 4.7's convergence
claim.

`resnapshot.record_empty_handoff()` writes the durable position from the fence the
emptiness was proven at, and the argument is exact: that fence is `pg_current_wal_lsn()`
sampled on its own statement **before** the counts, and every captured relation then
counted zero under `REPEATABLE READ`, so no transaction at or below it left a row
anywhere in the capture set. There is nothing below that position for the destination to
be missing. It is written with an **empty offset map**, because we have not observed one:
reconciliation reads that as "no durable offset" and lets the connector resume from its
own flushed `offsets.dat`. It refuses to overwrite an existing resume point and refuses
without a fence.

#### A60.3 — the commit→ack gate has no escape (MAJOR)

A59.2's gate closed the check-then-act race only while the writer finished inside five
seconds. On timeout it counted an `overlaps` and opened the window anyway — and the
reviewer duly held `_execute()` past the bound and ran SQL with `COMMIT_ACK.active` true.
An *instrumented* violation of an absolute principle is still a violation.

`enter()` now waits without a bound of its own, and the applier takes it **inside the
commit watchdog**. So the exclusion is absolute, and the failure mode it used to hide
becomes the same loud, bounded `EX_TEMPFAIL` death a wedged `COMMIT` already produces: an
observability cursor wedged long enough to threaten the commit path kills the run instead
of quietly overlapping it. The reviewer's other half — terminalisation blocking
indefinitely on the same gate — is fixed in the opposite direction, because it is the
opposite trade: the **terminal** write waits `GATE_TIMEOUT` and then writes ungated,
counting the fact, since by then the applier has stopped and a terminal row that never
lands is worse than a theoretical overlap.

#### A60.4 — `stop()` must mean quiesced (MAJOR)

A59.4 moved `catalog.stop()` before the verdict, which is the right ordering only if
`stop()` really stops the thread. It joined for `max(1, poll_seconds)` and returned
whatever happened, and catalog queries had only a handshake `connect_timeout` — so a poll
blocked on an established socket outlived the join, and the reviewer watched
`run_engine_bounded()` return `ok=true` while the thread was alive and then set
`machine_error`.

Three changes: catalog connections get the same bounds `SourceHealth` got in A59.5
(`statement_timeout`, keepalives, `tcp_user_timeout`); `stop()` returns whether the thread
is **actually dead**; and the supervisor raises `EngineFailure` when it is not, because
every check after that point — the undeclared-transition check, the unresolved-change
check, and the caller's flush of learned relations — reads state a live poller can still
mutate. The shipped test drives a real `CatalogWatcher` thread held at a barrier rather
than a fake whose `stop()` sets the error synchronously.

#### A60.5 — decomposition, again (the third time this branch)

`destination.py` crossed 1,000 lines when the learned-relation flush landed on it. The
source-relation registry — three functions, one table, one job — is
`cdc_flight/source_relations.py`, re-exported from `destination` so no caller changes.
It is a coherent seam rather than an arbitrary cut: `_cdc_flight.source_relations` is the
only thing that makes a drop-and-recreate detectable across a restart, and A60.1 is
entirely about who is responsible for making it durable. Current sizes are in
`RUBRIC_STATUS`, measured; `applier.py` (928) and `resnapshot.py` (926) are the two to
watch next.

### A61 — rev 13: the fix that reintroduced duplication, and two others

Round 4 confirmed the zero-commit catalog baseline, immediate all-empty reset
convergence, the removal of the five-second gate escape, real watcher quiescence and the
corrected `OpenGroup` wording. It also reproduced **duplication** — the thing this whole
design exists to make impossible — introduced by A60.2, and that is worth stating plainly
rather than burying: a fix for a convergence stall created an exactly-once defect.

#### A61.1 — a synthetic resume point is not a resumable offset (BLOCKER)

A60.2 wrote a durable resume row for an all-empty capture set: `last_lsn = fence`, with
an **empty offset map**. The LSN claim was true. The row was still wrong, because the
resume point is not only a claim about durability — it is the offset the connector is
handed. An empty map means "no offset", so the next run started Debezium fresh, took an
`initial` snapshot, and did it **against the slot the previous run had left behind**. That
is precisely the uncoordinated image/stream boundary A45 measured: with a writer running
across it, two committed keyless rows landed twice, once as `r` and once as `c`, and the
run reported success.

The row is gone. What replaces it is the honest state: **no resume point at all**, and a
completion predicate that accepts its absence only when every journalled relation was
discharged by *verified-empty evidence on this run*. The next run is then a real
`fresh_start`: the slot check sees a positioned slot over an empty destination, arms a
recovery, and that recovery **drops the slot**, so Debezium creates its own and the
boundary is exact. It costs a recovery per run for as long as the source stays entirely
empty, which is noisy and correct — and the moment one row exists, an ordinary commit
group writes a real resume point and the noise stops.

The regression is the reviewer's own shape: an all-empty reset, then a writer inserting
into a keyless table across the next run's boundary, asserting destination rows equal
source rows and every `cdcf_event_id` is distinct.

#### A61.2 — a run may not succeed over a catalog it never read (BLOCKER)

A60.4 proved the poller was **dead**. It did not prove the poller had ever **spoken**.
`poll_quietly()` catches a query failure into `last_error` — right for a transient
source — and the supervisor reported it and could still return success, while
`flush_learned_relations()` had nothing to persist. The four-second catalog
`statement_timeout` A60.4 added makes that a normal production path rather than a curiosity.

Measured: with every poll timing out and source health perfectly healthy, a quiet run
returned `ok=true` and durably knew zero relations. An offline drop-and-recreate then left
the old relation's two rows beside the replacement's one, permanently, because the
following runs adopted the replacement oid as the baseline they had never had.

`CatalogWatcher.successful_polls` counts polls that actually read and compared the
catalog, and a run with zero of them raises `EngineFailure`. Reporting success on an
unchecked catalog says "I looked and nothing was dropped" when nothing was looked at.

#### A61.3 — the terminal write is bounded at the DATABASE call (MAJOR)

A60.3 bounded the terminal write's wait for the *Python gate* and then called `_execute()`
anyway — on the same sink the stalled writer holds. DuckDB serialises calls on one
connection, so the statement simply queued behind the stalled one with no bound at all,
and the reviewer watched terminalisation still alive at 8 s. The observability sink cannot
be given a statement timeout, so the bound goes where it can: the terminal call runs on a
throwaway thread and the run stops waiting for it. Losing the terminal row is bad;
hanging a run's teardown on a heartbeat is worse.

The commit watchdog also reported "the commit is AMBIGUOUS" when it fired while the
applier was still waiting for the observability gate — before `COMMIT` had been issued at
all. It now names the stage, because "the transaction was never committed and will replay
in full" and "the commit may or may not be durable" send an operator to different places.

### A62 — the review loop did not converge, and the branch is not merged

`feature/1.9-state-machines` ran a fix → re-review loop against an independent
thermo-nuclear reviewer: one initial review and four re-reviews
(`reviews/1.9_codex_review.md`, `_r2`, `_r3`, `_r4`, `_r5`). Round 5 returned
`SATISFIED: no` with **1 BLOCKER and 1 MAJOR**, so the loop's stop condition was reached
and the branch was **not merged**. 1.9 is published at **3/5** and 1.7 at **3/5** — the
reviewer's conservative bands, not the claims.

What the loop bought is worth stating, because it is the argument for running it: across
five rounds it found and forced fixes for one destructive-ordering blocker
(`--accept-orphan-offsets` destroying evidence before journalling it), a reset that
cleared its own journal over a table it had not rebuilt, a heartbeat that could write
inside the commit→ack window, a run verdict taken over a live catalog poller, a catalog
baseline that vanished on a quiet run, and — most instructively — **a duplication that one
of the fixes itself introduced** (A61.1). Four of those were reproduced against a real
cluster by the reviewer rather than found by the suite.

#### What is still open

1. **Catalog-baseline validity is not durable (BLOCKER).** A run in which every catalog
   poll fails now fails loudly, but leaves no durable record that a baseline was never
   established. A later healthy run therefore adopts the currently observed
   `relation_oid` as history; if the relation was dropped and recreated in between, the
   old rows survive beside the new ones and every subsequent run reports success.
   `successful_polls` is process memory. The fix is a **durable** baseline-valid /
   baseline-invalid state with its own edges, and a forced rebuild of destination-owned
   relations when it is invalid — which is 1.9 work, not a patch, and is exactly the
   "consistency-affecting state outside a machine" the item is scored against.
2. **The terminal heartbeat write does not bound the whole teardown (MAJOR).** The
   database call is abandoned on a daemon thread after a bound, but `pipeline.run()` then
   calls `RunPhaseWriter.close()` on the same sink; against a real serialized DuckDB
   cursor the reviewer measured `close()` blocking behind the abandoned statement. The
   worker needs an owner and a retirement policy (close the sink from the worker, or
   discard the connection rather than closing it), and a test that covers
   `finish()` → `close()` on a genuinely serialized sink.

Neither is a loss path: Invariant O is untouched, the commit→ack ordering is unchanged,
and no crash/replay duplication was reproduced in round 5. The first is a *silent
inconsistency after an unchecked catalog*, the second is a *teardown stall*. Both are
carried forward with the branch.

### A63 — rev 14: the two findings the loop stopped on, closed

Round 5 (`reviews/1.9_codex_review_r5.md`) returned `SATISFIED: no` with 1 BLOCKER and
1 MAJOR, and A62 recorded both as carried forward. They are not carried forward any
more. Neither was a patch: the first added a machine, the second added an owner.

#### A63.1 — catalog-baseline validity is durable state with declared edges (BLOCKER)

The reviewer's sequence, reproduced against a real cluster: a destination populated with
no relation registry, a run in which **every** catalog poll failed, a drop-and-recreate
while the pipeline was down, and then two healthy runs that both reported `ok=true`
while the destination permanently held the old relation's rows *beside* the new one's.

A61.2's fix was real and insufficient. `CatalogWatcher.successful_polls` proves the
poller spoke; it is **process memory**, so it can reject the run that failed and it
cannot carry that run's *obligation* across the failure into the next one. The next run
sees a relation it has no oid for, and "first sight, record the oid" is indistinguishable
from "adopt a replacement as though we had always owned it".

The state that was missing has a name now — `machines.CATALOG_BASELINE`, SM-E,
`_cdc_flight.catalog_baseline.state`, four states and eleven declared edges — and one
writer (`cdc_flight/catalog_baseline.py`). It works the way the acquisition recovery
works, which is the only shape that survives a hard death:

1. **the mark comes first.** Every catalog-enabled run writes `-> stale` *before the
   engine starts*, unconditionally, in one transaction. A `SIGKILL`, an `os._exit` from a
   fault anchor, an unreadable catalog and a clean refusal therefore all leave the same
   durable statement. There is deliberately no `absent -> valid` edge and no
   `valid -> valid`: `valid` is reachable only from a mark this run has to discharge.
2. **an untrusted baseline reconciles instead of adopting.** `unrelatable_tables()` asks
   three questions of durable state alone — does `table_state` claim a trustworthy
   image, is there no `source_relations` row, and does the destination table actually
   hold rows — and every relation that answers yes to all three is *unrelatable*. Each
   one is marked `awaiting_snapshot`, **before the owed queue is read**, so rubric 1.6's
   blocking re-snapshot rebuilds it from the source in this same run, before the main
   stream, and swaps a complete fenced image over it in one transaction.

   **Marked, not dropped, and that is a correction the first cut earned.** The first cut
   queued a `recreated` change per unrelatable relation — the destructive route the
   round-5 work order named. Measured against the real cluster: a destination built
   without a registry has *every* captured relation unrelatable at once, so the mass-drop
   circuit breaker (`CDC_DROP_MAX_PER_POLL=1`, and it is right to be there) refused all
   of them and the pipeline wedged on `catalog_unresolved` until a human intervened.
   Destroying is also the wrong action for the fact: the relation **exists** at the
   source; what cannot be done is relating the rows we hold to it. `awaiting_snapshot`
   says exactly that, the data stays queryable-stale-and-flagged until a complete image
   replaces it, and there is no DDL nobody asked for, no fence to wait on and no breaker
   to trip. The destructive route survives as a **fail-safe** on the watcher, and it is
   keyed on `BaselineCheck.unmarked` — the unrelatable relations this run could not put
   in the owed queue, read back from `table_lifecycle.owing_work()` rather than from an
   affected-row count. Not on a recomputation: by the time the watcher is built the
   blocking re-snapshot has already rebuilt those relations from the *current* source
   relation, so adopting their oid is now the correct thing to do, while the question
   "is this relation unrelatable?" would still answer yes for the wrong reason — the
   registry row is written by the flush at the end of this very run. Measured: the first
   attempt at the fail-safe sent freshly rebuilt tables down the destructive path and
   wedged the pipeline on the breaker a second time.
3. **the promotion is evidence, and the run cannot succeed without it.** `-> valid`
   requires at least one real catalog comparison and nothing unrelatable, recomputed
   after the learned relations are flushed; `pipeline.run()` raises `EngineFailure`
   otherwise. So "eventual source/destination equality **or** a persistent non-success
   verdict" is structural rather than hoped for.

**`absent` is trusted, on purpose.** A destination that has never made a claim carries no
evidence of an unchecked window, and treating every one as suspect would rebuild every
existing destination on upgrade. Only a mark this pipeline wrote forbids adoption.

Two crash cuts across the new edges are real `os._exit` anchors —
`catalog_baseline_marked` and `catalog_baseline_pre_valid` — and the promotion is
idempotent rather than one-shot, so the second cut costs a recomputation and nothing
else. A third anchor, `catalog_poll`, makes "this run read the source catalog zero
times" an *executable* state instead of a monkeypatch: it is a repeating fault rather
than a one-shot, because one failed poll out of six is not the state that matters.

Inventory rows 54–58; transition table in §A51.1.

#### A63.2 — the heartbeat sink has one owner and a bounded retirement (MAJOR)

A61.3 moved the terminal write's database call onto a throwaway thread and stopped
waiting for it. `pipeline.run()` then called `RunPhaseWriter.close()` on the very cursor
that thread was still executing on — and `cursor.close()` **queues behind the statement
in flight** rather than cancelling it (measured directly: a close on a busy DuckDB cursor
returned only when the query did). So the bound applied to one wait site and the run's
teardown was still unbounded. Abandoning a worker while keeping no handle to it is not a
bound; it is the same wait one stack frame later, plus a race against a live cursor.

Ownership is explicit now and moves in one direction: the writer owns the cursor for
ordinary phase writes, hands it to one named worker for the terminal write, and
`close()` **retires** it — join the worker for `RETIRE_TIMEOUT`, then either close the
cursor (nobody else holds it) or *release* it unclosed, in which case the worker closes
it if it ever returns and the process is unaffected either way because the worker is a
daemon. The outcome is one value from the `heartbeat_sink_retirement` domain, and
`pipeline.run()` now samples the summary **after** `close()` rather than before, so an
abandoned terminal write reaches `last_run.json` — which is then the only record that
the run terminalised at all.

The subordinate metric confusion goes with it: the database-call bound no longer
increments `ungated_terminal_writes` (which means "written without the commit→ack
gate"), so one attempt stops looking like two ungated writes. It sets
`terminal_phase_write_abandoned`, which is a different fact with a different name.

Row 59 is **UNDEFINED**, and deliberately: bounding the teardown was the merge-blocking
half, but nothing sweeps the non-terminal heartbeat row an abandoned runner leaves
behind. An operator reading `_cdc_flight.heartbeat` alone would see a phase that never
terminalised. `last_run.json` carries the verdict; the sweep belongs to 4.4/6.1.

