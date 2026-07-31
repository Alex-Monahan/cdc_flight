# ADR 0001 — The transactional applier

* **Status:** accepted
* **Date:** 2026-07-30
* **Task:** TODO 1.0(a)
* **Shapes rubric items:** 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.4, 2.6, 3.2,
  3.3, 4.2, 4.4, 4.5, 4.6, 5.1, 5.2, 5.3, 5.4, 6.1, 6.2, 7.2, 7.4, 8.1, 8.2
* **Supersedes:** the dlthub blog's `DltChangeHandler` shape
  (`research/dlthub_debezium_and_dlt.md`). The rubric outranks the prior art.

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
| One 400 000-row PG transaction became **174 Debezium batches**, each its own `dlt.run()`; dlt opens **one transaction per table** inside a load package | `probes/p13`, `probes/p06`, `repos/dlt/dlt/destinations/insert_job_client.py:21-29` | Postgres transaction boundaries are invisible at the destination |
| **~17 s** of JVM start + MotherDuck connect per process (wall 31.9 s for 15.1 s of engine time) | `probes/p12` | a per-run process model cannot meet 5.2 (<30 s) or 5.3 (>2000 TPS) |
| Slot dropped / advanced externally / offset corrupted ⇒ `{"records": 0}`, **exit 0** | `probes/p04`, `p10`, `p11` | failures were invisible; fixed by TODO 1.0(b) (see §11) |
| `numeric`→base64, dates→BIGINT, a NaN column **dropped entirely**, TOAST→`__debezium_unavailable_value` | `probes/p02`, `tests/test_e2e_duckdb.py` | the `ExtractNewRecordState` + JSON payload is lossy before we ever see it |

Additional inputs to this decision, read out of the vendored sources rather than
assumed:

* `RecordCommitter.markBatchFinished()` is the **only** thing that ever flushes an
  offset — there is no background flush thread
  (`repos/debezium/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java:1369-1377`,
  `:901-932`). `offset.flush.interval.ms` merely gates it via
  `OffsetCommitPolicy`.
* The Postgres connector's `performCommit()` **re-reads the offset from the
  offset store** and confirms *that* LSN to Postgres
  (`repos/debezium/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorTask.java:472-500` →
  `PostgresStreamingChangeEventSource.java:504-535` →
  `PostgresReplicationConnection.java:1032-1046`). The slot can therefore never
  advance past whatever the offset store says is durable.
* `task.commit()` only sets a flag; the confirmation happens on the **next
  `poll()`** (`BaseSourceTask.java:360-361`), and the poll loop, the record
  processor and the user's `handleBatch` all run on **one thread**
  (`AsyncEmbeddedEngine.java:1304-1327`, `AbstractRecordProcessor.java:54-77`).
  The Postgres connector has exactly one task.
* pydbzengine's `PythonChangeConsumer.handleBatch` calls `markProcessed()` /
  `markBatchFinished()` **for** us and never hands the committer to the Python
  handler (`repos/pydbzengine/pydbzengine/_jvm.py:109-130`).
* `user directive, 2026-07-30`: **rubric item 7.2 (read from a Postgres replica,
  light workload on the primary) stands and must be met.** The idle-slot
  heartbeat therefore may not be a write on the connection Debezium streams
  from — see the dual-connection topology in §9.2.

## 2. Decision, in brief

1. **D1** — Drive MotherDuck directly. The apply path is a hand-written
   applier over one DuckDB/MotherDuck connection, not `dlt.pipeline.run()`.
2. **D2** — The unit of work is a **commit group**: one MotherDuck
   `BEGIN … COMMIT` containing an integral number of *whole* Postgres
   transactions, closed by whichever of a size, byte or **time** trigger fires
   first.
3. **D3** — The Debezium offset is written **inside that same transaction**, and
   the engine is only allowed to confirm an LSN to Postgres after it has
   committed. Exactly-once follows from the ordering, not from deduplication.
4. **D4** — The engine runs **long**, not once per batch: one JVM, one Postgres
   connection, one MotherDuck connection, many commit groups.
5. **D5** — Consume the **full Debezium envelope** with its Connect schema.
   `ExtractNewRecordState` is dropped.
6. **D6** — Every table gets a synthetic event identity; keyless tables use it as
   their primary identity.
7. **D7** — Backfills go to `<table>__cdcf_tmp` shadow tables and are swapped in
   one transaction, together with the offset write.
8. **D8** — Per-table output shape: current state, changelog, and/or SCD2, all
   written inside the same commit group.
9. **D9** — Two heartbeats: a **destination** heartbeat for liveness and
   observability, and a **source** heartbeat that emits a logical message on a
   *separate connection to the primary*, so streaming can read from a replica
   (7.2) while the slot still advances (4.4).
10. **D10** — dlt is removed from the apply path entirely; it survives, if at
    all, as a schema-inference helper. See §10.

---

## 3. D1/D2 — The applier and the commit group

### 3.1 Why not dlt

`dlt.pipeline.run()` cannot host the offset write in the destination
transaction, cannot span tables in one transaction, and costs a schema
resolution + load package + `complete_load` per call. Three rubric items (1.1,
1.3, 5.1) are structurally unreachable through it. That is the whole reason for
D1; it is not a performance preference.

### 3.2 Transaction boundaries

A Postgres transaction boundary is visible in the envelope
(`source.txId` changes, and `source.lsn`/`source.sequence` order events within
it). Debezium's transaction-metadata topic (`provide.transaction.metadata=true`)
additionally emits `BEGIN`/`END` markers with an exact `event_count`, which lets
the applier know a transaction is *complete* rather than inferring it from the
next transaction's first event. We enable it and use the `END` marker as the
authoritative boundary; the `txId`-change heuristic remains as a fallback for
the final transaction before a shutdown.

**Invariant A.** A commit group contains only whole Postgres transactions.
**Invariant B.** A Postgres transaction is never split across commit groups.

Invariant B has one legitimate exception, documented rather than hidden: a single
Postgres transaction larger than the memory guardrail (§3.4) cannot be buffered.
In that case the applier switches that transaction to *spill mode* — it writes
its events to a staging table `_cdc_flight.spill_<commit_id>` in the same
transaction and only makes them visible at the group's `COMMIT`, so atomicity is
preserved even though the buffer is not in memory. Spill mode is recorded in
`commit_log.trigger = 'spill'`.

### 3.3 Trigger policy

MotherDuck sustains roughly **100 transactions/s**, and rubric 5.2 wants
end-to-end latency consistently under 30 s. Those two numbers set the window:

| Trigger | Default | Why |
|---|---|---|
| `COMMIT_MAX_AGE` | 5 s | 5 s ≪ 30 s even with a slow commit; 0.2 commits/s is 0.2 % of MotherDuck's budget |
| `COMMIT_MAX_EVENTS` | 200 000 | bounds a single commit's work so one huge group cannot blow the latency budget |
| `COMMIT_MAX_BYTES` | 256 MB | the memory guardrail rubric 5.4 asks for; `max.queue.size.in.bytes` is set to the same order |
| `COMMIT_ON_DDL` | — | a schema change closes the group before the DDL is applied, so no group straddles two schemas |
| `COMMIT_ON_SHUTDOWN` | — | a graceful stop drains the buffer to a group boundary |

At 2 000 PG TPS with 5 s groups, each group holds ~10 000 whole transactions —
one MotherDuck transaction per 10 000 Postgres ones. Rubric 5.3's ">2000 TPS"
is therefore a *bulk-ingest* problem, not a transaction-rate problem, which is
exactly what a columnar destination is good at.

### 3.4 Algorithm

```text
# state
buffer          : list[Event]              # decoded, not yet applied
pending         : list[ChangeEvent]        # the Java objects, for markProcessed
complete_upto   : int   = 0                # index after the last complete PG txn
group_opened_at : float = now()
group_bytes     : int   = 0

# --- Debezium calls this on the single poll thread -----------------------
def handle_batch(records, committer):
    for raw in records:
        ev = decode(raw)                       # full envelope + Connect schema
        pending.append(raw)
        if ev.is_transaction_marker:
            if ev.marker == "END":
                complete_upto = len(buffer)    # everything up to here is whole
            continue
        if ev.is_heartbeat:                    # offset-only, no data
            complete_upto = len(buffer)
            continue
        if ev.lsn <= durable_watermark:        # idempotency fence, §4.4
            continue
        buffer.append(ev)
        group_bytes += ev.nbytes

    if complete_upto > 0 and should_commit():
        commit_group(committer, upto=complete_upto)

def should_commit():
    return (len(buffer)   >= COMMIT_MAX_EVENTS
         or group_bytes   >= COMMIT_MAX_BYTES
         or now() - group_opened_at >= COMMIT_MAX_AGE
         or shutdown_requested
         or ddl_pending)

# --- the transaction ------------------------------------------------------
def commit_group(committer, upto):
    commit_id = next_commit_id()               # monotonic, from the sequence
    con.execute("BEGIN TRANSACTION")
    try:
        renew_lease(con)                       # 4.2: fail fast if we lost it
        apply_events(con, buffer[:upto], commit_id)     # §5, §7, §8
        write_commit_log(con, commit_id, stats(buffer[:upto]))

        # (1) Debezium serialises its own offset. markBatchFinished() runs the
        #     flush synchronously on THIS thread (AsyncEmbeddedEngine:901-932),
        #     so by the time it returns, offsets.dat holds the new position.
        for raw in pending[:upto_raw]:
            committer.markProcessed(raw)
        committer.markBatchFinished()

        # (2) that authentic byte string becomes durable inside OUR transaction
        con.execute(
            "INSERT OR REPLACE INTO _cdc_flight.debezium_offsets VALUES (?,?,?,?,?,now())",
            [pipeline, namespace, OFFSET_KEY, read_bytes(offset_file), commit_id])

        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise                                  # -> EngineFailure -> non-zero exit

    durable_watermark = last_lsn(buffer[:upto])
    reset_group_state()
    # (3) control returns to Debezium's poll loop, which now confirms the LSN to
    #     Postgres (BaseSourceTask:360-361). It cannot have done so earlier: the
    #     poll loop is this thread.
```

Three properties of that ordering deserve to be called out, because the whole
correctness argument rests on them:

* **P1 — the offset bytes are authentic.** We do not reconstruct Debezium's
  offset map; we let Debezium serialise it and copy the result. A test asserts
  that the round-trip (`table → offsets.dat → engine`) resumes at the expected
  LSN, so a format change upstream fails loudly instead of silently.
* **P2 — the slot cannot outrun the data.** `task.commit()` only sets a flag; the
  standby status update happens on the next `poll()`, on this same thread, which
  cannot run until `commit_group` has returned. So Postgres is never told about
  an LSN that MotherDuck has not committed.
* **P3 — the engine cannot resume ahead of the data.** At process start the
  supervisor **overwrites `offsets.dat` from `_cdc_flight.debezium_offsets`**.
  The file is a scratch serialisation buffer; the MotherDuck table is the truth.

`offset.commit.policy` is set to
`io.debezium.engine.spi.OffsetCommitPolicy$AlwaysCommitOffsetPolicy` so
`markBatchFinished()` always flushes, and `offset.flush.timeout.ms` is raised to
60 000 so the engine never abandons a flush that is waiting on MotherDuck.

### 3.5 Getting the committer

pydbzengine's `PythonChangeConsumer` calls `markProcessed`/`markBatchFinished`
itself and does not pass the committer to the handler
(`repos/pydbzengine/pydbzengine/_jvm.py:109-130`). The applier therefore replaces
the consumer: `DebeziumJsonEngine.consumer` is a `cached_property`, and
`SupervisedDebeziumEngine` (added in TODO 1.0(b),
`src/cdc_flight/engine.py`) already overrides `engine`, so we supply our own
`@jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")` object.
No fork of pydbzengine is needed.

---

## 4. D3 — Offsets, metadata and the exactly-once argument

### 4.1 Why not a custom Debezium `OffsetStore`

The "obvious" design is a custom `io.debezium.spi.storage.OffsetStore` whose
`set()` writes into the applier's open transaction — and it would work:
`set()` runs inline on the calling thread
(`DefaultOffsetStorageWriter.java:134`), the engine blocks on the returned
future (`AsyncEmbeddedEngine.java:918`), and a failure cleanly aborts via
`cancelFlush()` (`:926-930`).

It is rejected because **Debezium instantiates the store reflectively by class
name** (`AsyncEmbeddedEngine.createAndStartOffsetStore`, `:844-891`). A JPype
proxy has no Java class name, so this design requires shipping a compiled Java
class — a `javac` build step inside what must be a pure-Python MotherDuck Flight
(9.1). The chosen design gets the same guarantee from ordering (P1–P3) with no
Java.

**Revisit trigger:** if the single-poll-thread invariant (P2) is ever violated —
the guard test in §4.5 fails — implement the Java `OffsetStore`. It is ~120
lines and `JdbcOffsetBackingStore` is a working template.

### 4.2 Schema

```sql
CREATE SCHEMA IF NOT EXISTS _cdc_flight;

-- Debezium's own offset bytes, durable only when the data is.
CREATE TABLE IF NOT EXISTS _cdc_flight.debezium_offsets (
    pipeline        VARCHAR     NOT NULL,
    namespace       VARCHAR     NOT NULL,   -- Debezium engine name
    offset_blob     BLOB        NOT NULL,   -- verbatim offsets.dat contents
    commit_id       BIGINT      NOT NULL,
    last_lsn        BIGINT      NOT NULL,   -- decoded, for humans and for the fence
    last_tx_id      BIGINT,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pipeline, namespace)
);

CREATE SEQUENCE IF NOT EXISTS _cdc_flight.commit_id_seq START 1;

-- One row per MotherDuck transaction. The audit trail for 1.3 / 6.1.
CREATE TABLE IF NOT EXISTS _cdc_flight.commit_log (
    commit_id       BIGINT      PRIMARY KEY,
    pipeline        VARCHAR     NOT NULL,
    runner_id       VARCHAR     NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL,
    trigger         VARCHAR     NOT NULL,   -- size|bytes|time|ddl|shutdown|spill|swap
    pg_txn_count    BIGINT      NOT NULL,
    event_count     BIGINT      NOT NULL,
    first_tx_id     BIGINT,
    last_tx_id      BIGINT,
    first_lsn       BIGINT,
    last_lsn        BIGINT,
    max_source_ts   TIMESTAMPTZ,            -- feeds end-to-end lag (5.2, 6.1)
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
    phase                    VARCHAR     NOT NULL,  -- starting|snapshot|stream|applying|idle|stopping
    last_event_at            TIMESTAMPTZ,           -- source ts of the newest event seen
    last_commit_id           BIGINT,
    last_commit_at           TIMESTAMPTZ,
    buffered_events          BIGINT,
    buffered_bytes           BIGINT,
    slot_active              BOOLEAN,
    slot_restart_lsn         BIGINT,
    slot_confirmed_flush_lsn BIGINT,
    slot_retained_bytes      BIGINT,
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
    code            VARCHAR     NOT NULL,   -- engine_failure|lag|slot_gone|lease_lost|...
    message         VARCHAR     NOT NULL,
    context         JSON
);
```

### 4.3 Columns added to every replicated table

| column | source | purpose |
|---|---|---|
| `cdcf_commit_id` | applier | which MotherDuck transaction made this row visible (1.3) |
| `cdcf_event_id` | applier | synthetic event identity, unique per change event (1.2) |
| `cdcf_seq` | envelope | ordinal within the Postgres transaction (ordering, SCD2) |
| `dbz_op`, `dbz_lsn`, `dbz_tx_id`, `dbz_schema`, `dbz_table`, `dbz_source_ts_ms` | envelope | unchanged from the baseline, deliberately — existing tests and probes key off them |

`cdcf_*` is applier-owned, `dbz_*` is source-derived. Keeping the `dbz_*` names
across the `ExtractNewRecordState` removal (D5) avoids a gratuitous breaking
change for consumers and for the existing suite.

### 4.4 The idempotency fence

Correctness does not depend on it, but two cheap fences are kept as defence in
depth:

1. Debezium itself discards messages at or below the last processed LSN when it
   resumes, because Postgres restarts the stream at `restart_lsn`, which is
   behind `confirmed_flush_lsn`.
2. The applier drops any event whose `(lsn, seq)` is `<=` the
   `durable_watermark` loaded from `_cdc_flight.debezium_offsets` at start-up.
   O(1), no index, no dedupe query.

### 4.5 Crash / failure matrix

`W` = durable watermark in `_cdc_flight.debezium_offsets`. "replay" means the
events are re-delivered by Postgres and re-applied; because the destination
transaction that would have contained them was rolled back, re-applying is not a
duplicate.

| # | Crash / failure point | MotherDuck data | `debezium_offsets` | `offsets.dat` | Slot `confirmed_flush` | On restart | Duplication | Loss |
|---|---|---|---|---|---|---|---|---|
| F1 | after decode, before `BEGIN` | unchanged | `W` | `W` | `≤ W` | resume at `W` | no | no |
| F2 | mid-`apply_events`, transaction open | rolled back | `W` | `W` | `≤ W` | resume at `W`, replay | no | no |
| F3 | after `markBatchFinished()`, before our `INSERT` | rolled back | `W` | **`W′` (ahead)** | `≤ W` | supervisor **overwrites the file from the table** (P3) → resume at `W` | no | no |
| F4 | after the offset `INSERT`, before `COMMIT` | rolled back | `W` | `W′` | `≤ W` | as F3 | no | no |
| F5 | during `COMMIT` | atomic — all or nothing | atomic with the data | `W′` | `≤ W` | if committed resume at `W′`; else as F3 | no | no |
| F6 | after `COMMIT`, before the next `poll()` confirms the LSN | `W′` applied | `W′` | `W′` | `≤ W` | resume at `W′`; Postgres re-sends from `restart_lsn`, Debezium and the fence discard `≤ W′` | no | no |
| F7 | after the LSN is confirmed | `W′` applied | `W′` | `W′` | `W′` | nothing to redo | no | no |
| F8 | MotherDuck rejects the commit | rolled back | `W` | `W′` | `≤ W` | `EngineFailure` → non-zero exit → as F3 | no | no |
| F9 | Postgres slot dropped / offset unusable | unchanged | `W` | `W` | n/a | engine fails to start → **non-zero exit** (1.0(b)) → 1.8 routes it to a re-snapshot | no | no |
| F10 | second instance starts | — | — | — | — | lease renewal in the group fails → the loser exits non-zero before writing (4.2) | no | no |

The single reason every row says "no / no" is that
`data ∧ offset` commit atomically, and the slot is only ever told about an offset
that a `COMMIT` has already made durable (P2). Losing is impossible because the
slot never advances past `W`; duplicating is impossible because the engine never
resumes before `W`.

**P2 is an assumption about Debezium's threading, so it gets its own test.**
`tests/1.1_exactly_once_pk/` will gain an invariant test asserting that at every
observed moment `slot.confirmed_flush_lsn <= debezium_offsets.last_lsn`. If that
ever fails, §4.1's revisit trigger fires.

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

Companion settings: `replace.null.with.default=false` (the p01 finding — the
delete image was fabricated zeros and empty strings, not NULLs),
`skipped.operations=none` (truncate is skipped by default —
`CommonConnectorConfig.java:865-875`), `provide.transaction.metadata=true`
(§3.2), and `max.queue.size.in.bytes` set (5.4).

The cost is a larger payload per event. That is paid for by the byte-bounded
commit trigger (§3.3) and by not building a Python `dict` per row twice, which
the dlt path did.

---

## 6. D6 — Keys, and tables that have none

`cdcf_event_id` is assigned to every event:

* **streaming**: `f"{lsn}:{tx_id}:{seq}"` — unique by construction, and stable
  across a replay of the same WAL, so a re-applied event is recognisable.
* **snapshot**: `f"snap:{commit_id_of_swap}:{table}:{ordinal}"` — a re-snapshot
  produces different ids, which is correct, because a re-snapshot replaces the
  table wholesale via the shadow swap (§7) rather than merging into it.

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

`app.sensor_readings` is the test bed for all of this
(`tests/1.2_exactly_once_nopk/`).

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
  INSERT INTO _cdc_flight.commit_log ... trigger = 'swap';
  UPDATE _cdc_flight.table_state SET snapshot_state='complete', snapshot_lsn=?;
  INSERT OR REPLACE INTO _cdc_flight.debezium_offsets ...;   -- the offset rides along
COMMIT;
```

Three details that are easy to get wrong:

1. **The swap set is not one table.** Until rubric 2.4 turns Postgres arrays into
   DuckDB `LIST`s, dlt-style child tables exist —
   `cdcflight_app_customers__tags`, `cdcflight_app_orders__quantities`,
   `cdcflight_app_wide_types__col_int_array`, and so on. The swap must move the
   root **and every table whose name starts with `<root>__`**, enumerated from
   `information_schema.tables` *inside* the transaction so the set cannot change
   underneath it. Once 2.4 lands, the set collapses to the root table and the
   same code keeps working — no second implementation.
2. **CDC does not stop.** Events that arrive during the backfill are applied to
   the `__cdcf_tmp` table (the applier resolves the target through
   `table_state.snapshot_state`), so at swap time the shadow table is already
   caught up and the switch is instantaneous. This is what makes 3.3 "simple and
   elegant" rather than "increased complexity": there is one write path, and only
   the *name* it resolves to changes.
3. **The offset rides in the swap transaction** for the same reason it rides in
   every other commit group: a crash mid-swap must leave neither a half-swapped
   table nor an advanced offset.

`snapshot_lsn` is what makes 1.6 provable: the snapshot's exported-snapshot LSN
is recorded in the same transaction as the swap, so "where the backfill ends and
the stream begins" is a queryable fact rather than an assumption.

If MotherDuck turns out not to support transactional `DROP`/`RENAME`, the
fallback inside the same transaction is
`CREATE OR REPLACE TABLE <t> AS SELECT * FROM <t>__cdcf_tmp` — the rubric's
"BEGIN/COMMIT transactionality fine too" wording explicitly allows it. **This
must be verified before 3.2 is implemented; it is the single biggest unknown in
this ADR.**

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
join them at any instant and they agree.

SCD2 mechanics: within a group, events are ordered by `(lsn, seq)`; for each
identity key the applier closes the open interval
(`valid_to = <next event's source_ts>`, `is_current = false`) and opens a new one.
A key touched N times in one group produces N intervals, not one — the group is a
commit boundary, not a compaction boundary. Because groups contain whole
Postgres transactions (Invariant A), SCD2 intervals never split a transaction,
so a multi-table point-in-time query is consistent. That is why 8.2 depends on
1.3 and not the other way round.

---

## 9. D9 — Two heartbeats

The rubric asks for heartbeats to do two unrelated jobs, and conflating them is
why the baseline has neither. They are separated:

### 9.1 Destination heartbeat — liveness and observability (4.5, 4.6, 6.1, 6.2)

A supervisor thread writes a row to `_cdc_flight.heartbeat` every
`HEARTBEAT_INTERVAL` (default 5 s) **on its own MotherDuck connection**, outside
the commit-group transaction. Each beat carries the phase, the newest source
timestamp seen, buffered depth, and the slot's `restart_lsn` /
`confirmed_flush_lsn` / retained bytes read from `pg_replication_slots`.

Consequences:

* **4.5 (hangs)** — the beat is the watchdog input. If the applier has been in
  `applying` for more than `HANG_TIMEOUT`, or the poll thread has not advanced
  `last_event_at` while the slot shows a backlog, the supervisor tears the
  process down with a **non-zero** exit. The baseline's watchdog exited 0, which
  is why `probes/p09`'s hang was invisible.
* **4.6 (silently-dead Postgres)** — the beat's slot query runs against Postgres
  on a connection with an explicit socket timeout; a dead node fails the beat
  within one interval, so detection is seconds, not the ~2 h of OS TCP defaults.
* **6.1 / 6.2** — lag (`now() - max_source_ts`) and retained WAL are already in
  MotherDuck, so alert rules are `SELECT`s over `_cdc_flight.heartbeat` written
  to `_cdc_flight.alerts`.

Deliberately **not** transactional with the data: a health signal that
disappears when the apply rolls back is exactly the signal you need most.

### 9.2 Source heartbeat — advancing an idle slot (4.4) without disturbing 7.2

When only a subset of tables is captured and the rest of the cluster is busy, the
WAL advances but our stream produces no events, so no offset is committed and
`confirmed_flush_lsn` stalls — Postgres then retains WAL indefinitely. Debezium
documents this precisely (`postgresql.adoc:4601-4616`): `heartbeat.interval.ms`
alone "enables the connector to send the latest retrieved LSN", and a *write* to
the source is the mechanism for the case where the captured tables themselves are
quiet.

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

* **Streaming connection** — owned by Debezium, configured by
  `database.hostname` / `database.port`. Points at the replica in replica mode.
  Strictly read-only: no `heartbeat.action.query`, no signal-table writes, and
  `publication.autocreate.mode=disabled` (already the case) so Debezium never
  attempts DDL there.
* **Heartbeat connection** — owned by *our* supervisor, a plain `psycopg`
  connection to the **primary**. Every `HEARTBEAT_INTERVAL_MS` it runs exactly
  one statement:

  ```sql
  SELECT pg_logical_emit_message(false, 'cdcf_hb', <runner_id>||':'||now()::text);
  ```

  The message is a single non-transactional WAL record — no table, no rows, no
  bloat, no vacuum load. It replicates to the standby physically, is decoded
  there by our logical slot, and arrives on our stream as an `op='m'` event.
  `probes/p01` already proved such messages land, which is why 4.4 and 7.4 are
  one piece of work.
* When the source is a single node, `CDC_PRIMARY_*` defaults to the streaming
  source and the topology collapses to one host — **one code path, no special
  case**.

Debezium config:

```properties
heartbeat.interval.ms = 10000        # keeps the connector's own status updates flowing
# heartbeat.action.query intentionally NOT set - see above
logical_decoding_message.prefix.include.list = cdcf_hb,app_.*
```

New settings in `config.py`:

| setting | default | meaning |
|---|---|---|
| `CDC_PRIMARY_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DBNAME` | the streaming source | where heartbeat writes go |
| `CDC_HEARTBEAT_INTERVAL_MS` | 10000 | source heartbeat cadence |
| `CDC_HEARTBEAT_PREFIX` | `cdcf_hb` | logical-message prefix, so 7.4's application messages stay separable |
| `CDC_HEARTBEAT_MODE` | `primary_write` | `primary_write` \| `passive` |
| `CDC_SLOT_RETENTION_WARN_BYTES` / `_CRITICAL_BYTES` | 1 GiB / 8 GiB | when stalled slot advancement becomes an alert |

#### Why this scores 7.2 at 5

The rubric's 5 is "replica enabled and light workload on primary". The primary
receives **one WAL-record-emitting statement every 10 s** and nothing else — no
table, no index, no autovacuum consequence. The standby's
`hot_standby_feedback=on`, which Postgres requires for a logical slot on a
standby, remains the larger of the two costs, and it is a Postgres requirement
rather than something this design adds.

#### Applier handling

* A `cdcf_hb` message is **offset-only**: it advances `complete_upto` and
  contributes to the commit group's offset, but writes no data row. It updates
  `heartbeat.source_heartbeat_at` on the next destination beat.
* A commit group whose only content is heartbeats still commits, on the time
  trigger. The offset advances, `markBatchFinished()` runs, the next poll
  confirms the new LSN — the slot moves with zero business changes. That is 4.4.
* Round-trip latency between "emitted on the primary" and "seen on our stream"
  is itself a useful metric: it is replica lag plus decode time, and it lands in
  `_cdc_flight.heartbeat` for 6.1.

#### Failure behaviour when the primary heartbeat connection is unavailable

Losing the heartbeat degrades *WAL retention*; it does not threaten correctness.
The response is graduated, and CDC streaming is **never** stopped because of it:

| condition | response |
|---|---|
| a single emit fails | retry with exponential backoff (1 s → 30 s cap), reconnect on the next attempt; recorded as `heartbeat.source_heartbeat_error` |
| failing for > 3 intervals | `warning` alert `source_heartbeat_unavailable` in `_cdc_flight.alerts`; streaming continues |
| failing **and** `slot_retained_bytes > CDC_SLOT_RETENTION_WARN_BYTES` | `warning` escalates to describe the real harm (WAL growth), not just the symptom |
| failing **and** `slot_retained_bytes > CDC_SLOT_RETENTION_CRITICAL_BYTES` | `critical` alert; with `CDC_ON_SLOT_RETENTION_CRITICAL=exit` (opt-in, default `alert`) the process exits non-zero so a scheduler escalates |
| primary is unreachable but the replica keeps streaming | expected during a failover; the heartbeat writer keeps retrying and re-resolves `CDC_PRIMARY_HOST` each attempt, so a promoted standby is picked up automatically |
| the heartbeat connection points at a host that is itself in recovery | detected via `pg_is_in_recovery()` on connect; `critical` alert `heartbeat_target_is_standby`, and the writer degrades to `passive` rather than erroring in a loop |

`CDC_HEARTBEAT_MODE=passive` disables the source write entirely and relies on
`heartbeat.interval.ms` alone plus an empty commit group on the time trigger.
That advances the slot whenever Debezium has received *any* LSN, which covers
everything except a cluster generating no WAL at all — in which case there is
nothing to retain. It is the documented degradation for sources where we have no
write access anywhere, and it is explicitly **not** good enough for a 5 on 4.4,
which is why `primary_write` is the default.

**Residual edge cases, stated rather than hidden.**

1. *Logical slot on a standby can be invalidated by a recovery conflict.* Not
   caused by this design, but it interacts: invalidation surfaces as an engine
   failure (now non-zero, per 1.0(b)) and must route to a re-snapshot under 4.1.
2. *Failover.* The slot on a standby does not survive promotion unless
   `slot.failover=true` and the primary lists it in `synchronized_standby_slots`.
   That belongs to 4.1 and is called out here because the heartbeat writer's
   automatic re-resolution of the primary is only half of a failover story.
3. *Privileges for `pg_logical_emit_message` on Postgres 18* must be verified and
   documented when 4.4 is implemented (see §14).

---

## 10. D10 — What happens to dlt

**Removed from the apply path entirely**: `dlt.pipeline.run()`, load packages,
dlt's per-table transactions, dlt pipeline state, the `_dlt_load_id` / `_dlt_id`
columns, `_dlt_loads` / `_dlt_version` / `_dlt_pipeline_state` tables, and dlt's
array→child-table normalisation (2.4 replaces it with `LIST`).

**Retained, conditionally**: dlt's schema inference and DDL generation *may* be
used to create destination tables on first sight, if it proves cheaper than
mapping the Connect schema ourselves. But D5 gives us the Connect schema, which
is a strictly better type source than inference over JSON values — which is the
root cause of half of 2.4's failures. The honest expectation is therefore that
**dlt is dropped completely** during Phase 2, and this ADR does not pretend
otherwise. The dependency stays in `pyproject.toml` until 2.4 is implemented, at
which point a follow-up ADR records the removal (or the reprieve).

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
  connection pair, many commit groups; the 17 s start-up is amortised to <3 %,
  and latency *inside* the window is the commit-group age (5 s), comfortably
  under 30 s. The process drains to a group boundary and exits 0.
* `--forever` — a daemon, for a hosted deployment.

Both are safe to kill at any instant: that is what the F1–F10 matrix is for. Both
take the `_cdc_flight.lease` before starting the engine and renew it inside every
commit group, so back-to-back scheduled windows that overlap fail predictably
(4.2) instead of silently double-writing (what `probes/p05` observed).

**Consequences for Flight packaging (9.1).** The Flight is a long-window job, not
a per-batch job. Concretely: the run must be re-entrant (state lives in
MotherDuck, not on local disk — `offsets.dat` is rebuilt from
`_cdc_flight.debezium_offsets` at start, §3.4 P3), it must exit non-zero on
failure so the scheduler notices (1.0(b)), and it must tolerate being killed at
the window boundary. Anything the Flight runtime does *not* let us do — hold a
JVM for ten minutes, keep a Postgres replication connection open — becomes a
blocking question for 9.1 and is flagged here rather than discovered there.

---

## 12. Consequences

**Positive.**

* 1.1, 1.2, 1.3, 1.7 are solved by one mechanism instead of four.
* 1.6, 3.2, 3.3, 3.7 collapse into the shadow-table swap, which is the same
  transaction machinery.
* 5.1/5.3 become bulk-ingest problems (one large transaction per 5 s) rather
  than per-batch overhead problems.
* 6.1/6.2 get their data for free: `commit_log` and `heartbeat` are written by
  the applier itself, so the observability can never disagree with the data.

**Negative / accepted costs.**

* We now own code that dlt used to own: DDL generation, type mapping, schema
  evolution, `INSERT`/`MERGE` construction. That is a real maintenance burden and
  is the price of items dlt structurally cannot deliver.
* Correctness rests on a threading property of `AsyncEmbeddedEngine` (P2). It is
  documented, tested, and has a named fallback (§4.1) — but it is an upstream
  behaviour, not a contract.
* Buffering whole transactions costs memory; the byte trigger and spill mode
  bound it, but they add complexity that a naive batcher does not have.
* A second Postgres connection (to the primary) now exists purely for the
  heartbeat (§9.2). It is one statement every 10 s, but it is another thing that
  can fail, another credential to configure, and another failure mode to test.
  The alternative — `heartbeat.action.query` — is cheaper but makes 4.4 and 7.2
  mutually exclusive, which is not a trade the rubric allows.

**Rejected alternatives.**

| Alternative | Why rejected |
|---|---|
| Keep `dlt.run()` and dedupe afterwards with a `merge` on `(lsn, tx_id, pk)` | Does not give 1.3 at all, and keyless tables (1.2) have no merge key. Deduplication also cannot fix a *torn* transaction. |
| Custom Java `OffsetStore` | Cleanest semantics, but needs a `javac` build inside a pure-Python Flight. Kept as the documented fallback (§4.1). |
| `flush.lsn.source=false` + `pg_replication_slot_advance()` after commit | Postgres refuses to advance an **active** slot, and ours is active for the life of the engine. Non-starter. |
| Reconstruct Debezium's offset map ourselves instead of copying its bytes | Format drift across Debezium versions would be a silent correctness bug. Copying the bytes it just wrote is authentic by construction (P1). |
| Per-batch process, scheduled every 30 s | ~17 s of the 30 s is JVM start; it cannot meet 5.3 and barely meets 5.2. |
| Heartbeat table on the source instead of `pg_logical_emit_message` | Table bloat, vacuum load, and a schema to own — for strictly less capability. |
| `heartbeat.action.query` (Debezium writes the heartbeat itself) | Runs on the streaming connection, so it fails against a hot standby. Would make 4.4 and 7.2 mutually exclusive. Replaced by the dual-connection topology (§9.2). |

---

## 13. Implementation order

Each step must be able to land with the suite green.

1. **1.0(b)** — engine failures exit non-zero. *(done: `src/cdc_flight/engine.py`,
   `tests/1.0_engine_error_propagation/`)*
2. **Fault injection** — `CDC_FAULT_INJECT` crash points. *(done:
   `src/cdc_flight/faults.py`; it is what makes 1.1/1.2 testable at all)*
3. `_cdc_flight` schema + the offset round-trip (P1) and its guard test.
4. The applier and commit groups behind a flag, DuckDB only, append-only output —
   turns 1.1/1.2/1.3 green.
5. Long-running mode + lease (4.2, 5.2, 5.3), measured against MotherDuck.
6. Full envelope (D5) — unblocks 2.4, 2.6, 1.5, 7.4, 1.4.
7. Shadow-table backfills (D7) — 3.2, 3.3, 1.6, 3.7.
8. Heartbeats (D9) — 4.4, 4.5, 4.6, 7.2, and the data for 6.1/6.2. The replica
   topology is exercised by promoting `probes/p09_replica.py` into a test:
   stream from the standby while the heartbeat writes only to the primary, and
   assert `restart_lsn` advances while the standby stays read-only.
9. Output shapes (D8) — 8.1, 8.2.

## 14. Open questions that block later phases

1. Does MotherDuck honour `DROP TABLE` / `ALTER TABLE … RENAME` inside a
   transaction? Blocks 3.2. Fallback in §7.
2. Measured MotherDuck commit latency for a 200 000-row group — sets
   `COMMIT_MAX_EVENTS` and `offset.flush.timeout.ms` for real. Blocks 5.3.
3. Exact privileges required for `pg_logical_emit_message` on Postgres 18.
   Blocks 4.4's documentation.
4. Whether a MotherDuck Flight can hold a JVM and a replication connection for a
   10-minute window. Blocks 9.1 and, if the answer is no, forces a re-read of D4.
5. End-to-end latency of the primary→standby→our-slot heartbeat round trip, and
   whether a logical slot on a Postgres 18 standby is stable enough over a long
   window (`probes/p09` saw a shutdown hang and only exercised the snapshot
   path). Blocks 7.2 and 4.4's replica story.
