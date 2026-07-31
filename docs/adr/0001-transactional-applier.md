# ADR 0001 — The transactional applier

* **Status:** accepted (revision 3, 2026-07-30 — implemented; see §15 for the amendments the implementation forced)
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
    phase                    VARCHAR     NOT NULL,  -- starting|snapshot|stream|applying|idle|retrying|stopping
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
    snapshot_state  VARCHAR     NOT NULL,   -- none|in_progress|complete|failed
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
  SELECT pg_logical_emit_message(false, 'cdcf_hb', <runner_id>||':'||now()::text);
  ```

  The message is a single non-transactional WAL record — no table, no rows, no
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
