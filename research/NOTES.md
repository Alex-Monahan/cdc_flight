# Research notes — baseline (Phase 0)

Everything here is measured against the running baseline in this repo, not guessed.
Source material: [`dlthub_debezium_and_dlt.md`](dlthub_debezium_and_dlt.md) (saved copy of
the dlthub post) and the forks catalogued in `FORKS.md`.

**The rubric outranks the blog.** Where the blog's design would score below 5 on
`cdc_tool_decision_matrix_v3_for_swarm.md`, the blog is wrong and gets replaced in a later
phase. The baseline keeps the blog's *shape* so the gaps are measurable, not because the
shape is right.

---

## 1. Versions actually used

| Thing | Blog (2025) | Here (2026-07-30) |
|---|---|---|
| Debezium | 3.0.0.Final (via `debezium/example-postgres` image) | **3.6.0.Final** (vendored in pydbzengine 3.6.0.0) |
| pydbzengine | PyPI `pydbzengine[dev]` | **3.6.0.0 from GitHub** — no longer published to PyPI |
| dlt | ≥1.5 | **1.29.1** |
| DuckDB | unpinned | **1.5.4** (see deviation D7) |
| Postgres | `debezium/example-postgres:3.0.0.Final` in Docker | **18.1**, Homebrew, project-local cluster, no Docker |
| Java | unspecified | **OpenJDK 23.0.2** (pydbzengine needs 17+) |
| Python | unspecified | **3.13** via uv |

## 2. Deviations from the blog (and why)

| # | Blog | Here | Reason |
|---|---|---|---|
| D1 | `testcontainers` + `debezium/example-postgres` Docker image | native Homebrew Postgres 18 cluster in `CDC_TEST_PGDATA` (default `./.pgdata`) on `CDC_TEST_PGPORT` (default :15432), managed by `scripts/pg.sh` | Project constraint: no Docker. Also gives us a real `postgresql.conf` we control and a cluster we can crash/restart for fault injection later (rubric 1.7, 4.x). |
| D2 | `table.whitelist` / `schema.whitelist` / `database.whitelist` | `table.include.list` / `schema.include.list` | The `*.whitelist` properties are **removed** in Debezium 2.x+. The blog's config is silently ignored on 3.6 — it captures *everything*. Bitrot. |
| D3 | `transforms.unwrap.delete.handling.mode=rewrite` | `transforms.unwrap.delete.tombstone.handling.mode=rewrite` | Renamed in Debezium 3.x; the old key is ignored (upstream's own example file was already updated). |
| D4 | default `plugin.name` | explicit `plugin.name=pgoutput`, explicit `slot.name`, explicit `publication.name`, `publication.autocreate.mode=disabled` | Rubric 7.1 wants pgoutput with no extension; an autocreated publication is invisible infrastructure, so `sql/01_schema.sql` owns it. |
| D5 | `replica.identity.autoset.values=inventory.*:FULL` | REPLICA IDENTITY set in DDL (`FULL` on the no-PK table only) | Debezium silently rewriting `REPLICA IDENTITY` on the source is a production footgun (it rewrites the table's catalog and inflates WAL for every table). Being selective and explicit also lets us *observe* the DEFAULT-identity behaviour we need to fix for rubric 1.2/2.6. |
| D6 | `max.batch.size=5`, `poll.interval.ms=10000` | `max.batch.size=2048`, `poll.interval.ms=500` | Blog values are demo-scale: batches of 5 with a 10 s poll is ~0.5 rows/s and would fail rubric 5.2/5.3 by three orders of magnitude. Every batch is a full `dlt_pipeline.run()`, so tiny batches are catastrophic. |
| D7 | unpinned `duckdb` | `duckdb>=1.4,<1.5.5` | MotherDuck rejects DuckDB versions it has not shipped an extension for. 1.5.5 fails at connect with *"Your DuckDB version (v1.5.5) is not yet supported by MotherDuck"*; 1.5.4 works. |
| D8 | `Utils.run_engine_async(engine, timeout_sec=60)` — fixed wall clock | bounded runner with idle detection + in-flight guard (`pipeline.run_engine_bounded`) | A fixed timeout either wastes time or truncates the stream. It also has a real bug when the destination is slow: `Utils` closes the engine mid-batch, which interrupts `RecordCommitter.markBatchFinished()` and throws `InterruptedException` — **we hit exactly this against MotherDuck** before adding the `handler.busy` guard. |
| D9 | default `transforms.unwrap.add.fields` prefix `__` | prefix `dbz_` | dlt's snake_case naming convention strips leading underscores, so `__op`/`__table`/`__schema` land as bare `op`/`table`/`schema` — reserved words that can collide with real source columns. `dbz_` survives normalisation. (Note: `__deleted` is *not* covered by that prefix property and still lands as `deleted`.) |
| D10 | table name = topic with dots replaced | table name built from `dbz_schema` + `dbz_table`, topic as fallback | Debezium 3.6's default topic for this connector is `<prefix>.<table>` — **the source schema is not in the topic**. The blog's `destination().replace(".","_")` therefore collides `app.customers` with `billing.customers`. |
| D11 | process just ends | explicit `jpype.shutdownJVM()` behind a watchdog, then `os._exit` | Debezium leaves non-daemon JVM threads behind; without this the CLI hangs forever *after* completing its work. Observed on the first run: exit never happened. |
| D12 | prints tables at the end | machine-readable run summary (`.cdc_state/last_run.json` + stdout JSON) | Needed by the test harness and by rubric §6 later. |

## 3. What the baseline actually does (verified)

One run = Debezium snapshot (`snapshot.mode=initial`) then streaming until idle. Verified
with real queries against `cdc_flight.duckdb`:

* snapshot → 20 rows, all `dbz_op='r'`
* after one datagen wave (30 DML row-changes) → exactly 30 change events, split
  `c`/`u`/`d` per table as expected
* `app.sensor_readings` (**no primary key**, `REPLICA IDENTITY FULL`) → updates and deletes
  captured with complete old row images
* `app.audit_log` (**partitioned**) → arrives as one logical table thanks to
  `publish_via_partition_root = true`
* second run with no source changes → 0 records (offsets persist)
* MotherDuck (`cdc_flight_dev`) → same code path, verified with a live query

## 4. Baseline gaps, mapped to rubric items

Informal — the formal scoring is task 0.6. Items are numbered `<section>.<item>`.

### §1 Delivery guarantees & correctness
* **1.1 / 1.2 — at-least-once at best.** Debezium offsets live in `offsets.dat` and are
  flushed on their own schedule, entirely outside the dlt load transaction. A crash between
  "dlt committed" and "offset flushed" replays; a crash the other way loses. Write
  disposition is `append`, so replays become duplicates. *Observed for real*: the
  MotherDuck run that died in `markBatchFinished()` had already committed its rows, and the
  retry duplicated all 7 customer rows.
* **1.3 — no transactional boundaries.** Each Debezium batch is one or more `dlt.run()`
  calls; a Postgres transaction spanning tables can be split across loads. Nothing groups
  whole PG transactions into one MotherDuck `BEGIN/COMMIT`.
* **1.4 — PK updates.** Append-only changelog: a PK update shows up as `u` with the new key
  and nothing reconciles the old one.
* **1.5 — TRUNCATE / DROP.** The publication includes `truncate`, but `ExtractNewRecordState`
  drops truncate events (no `after` image) and the handler skips null payloads. DROP is not
  in logical decoding at all.
* **1.6 — snapshot/CDC consistency.** Relies entirely on Debezium's `initial` snapshot; no
  independent verification, no LSN coordination on our side.
* **1.7 — no fault injection.** Zero crash tests today.
* **1.8 — no `offset.mismatch.strategy` handling.** If the slot is advanced externally we
  silently skip data.

### §2 Schema evolution & types
* **2.1 add/drop columns** — dlt's default `evolve` contract adds columns; dropped columns
  linger and go NULL. Untested here.
* **2.2 renames** — will appear as add + stale column. No rename handling.
* **2.3 new tables** — `table.include.list` is static and the publication is explicit, so a
  new table needs a config + DDL change and a restart.
* **2.4 type mapping — the biggest measured gap.** Everything below is *observed* in
  `cdc_raw.cdcflight_app_wide_types`:
  * `numeric` → **base64 VARCHAR** (`decimal.handling.mode=precise` emits `bytes`)
  * `date`, `time`, `timestamp` (no tz), `interval` → **BIGINT** (epoch days / micros)
  * `timestamptz` → correct `TIMESTAMP WITH TIME ZONE` ✅
  * `bytea` → base64 VARCHAR
  * `double precision` holding `Infinity` / `NaN` → **VARCHAR** (JSON has no such literals),
    so the whole column degrades to text
  * an unconstrained `numeric` holding `NaN` (`col_numeric_nan`) is **dropped entirely** —
    the column does not exist at the destination
  * arrays (`int[]`, `text[]`, `numeric[]`) → **dlt child tables**, not DuckDB `LIST`
  * `point` → three columns (`__x`, `__y`, `__wkb`)
  * `json`/`jsonb` → VARCHAR, not DuckDB `JSON`
  * `money`, `inet`, `cidr`, `macaddr`, `bit`, `int4range`, enum → VARCHAR (acceptable per
    the rubric for genuinely obscure types, but `money`/`inet` are listed as the *allowed*
    text cases, so this part is fine)
* **2.5 type changes** — dlt creates a variant column; not a MotherDuck UNION.
* **2.6 TOAST** — an update that does not touch a TOASTed column delivers the literal string
  `__debezium_unavailable_value`. Verified. Nothing reconstructs the real value.

### §3 Backfill & refresh
* No control over snapshotting beyond Debezium's `snapshot.mode`. No shadow `_tmp` tables,
  no atomic swap, no per-table modes, no resumability, no lag-triggered backfill, no
  ad-hoc re-snapshot of an arbitrary table set. (3.1–3.7 all uncovered.)

### §4 Failure detection & recovery
* **4.1** no failover-slot support, no recovery path from a failed slot.
* **4.2** two concurrent runs against the same slot → the second gets a "replication slot is
  active" error from Postgres. Predictable-ish but untested and unhandled.
* **4.3** an unhandled WAL message stops the connector; nothing triggers a backfill.
* **4.4 / 4.5 / 4.6** **no heartbeat at all** — deliberately omitted from the baseline
  (`heartbeat.interval.ms` unset, no `heartbeat.action.query`). An idle slot on a busy
  database will retain WAL indefinitely, and a silently-dead connection is only bounded by
  `wal_sender_timeout`. The bounded runner does at least prevent an *infinite* hang, and
  D11's watchdog guarantees process exit.

### §5 Performance & latency
* **5.1 / 5.3** every batch triggers a full `dlt_pipeline.run()` — schema resolution,
  normalisation, load package, commit. That is a heavy per-batch cost and the dominant
  bottleneck. Untested above ~30 events.
* **5.2** latency is whatever the scheduler gives us; the bounded runner adds an
  `idle_seconds` tail (default 8 s). Fine, but not designed for it.
* **5.4** the whole batch is materialised in Python lists; no memory guardrails, no spill.

### §6 Observability
* Run summary JSON + Debezium's log4j file. **Nothing lands in MotherDuck**, no replication
  lag metric, no alerts. (6.1, 6.2 uncovered.)

### §7 Postgres features
* **7.1 ✅ pgoutput, no extension** — the one item the baseline already scores 5 on.
* **7.2** never tested against a replica.
* **7.3** partitioned tables work via `publish_via_partition_root`, but there is no *option*
  for per-partition tables or a partitioned DuckLake.
* **7.4** `pg_logical_emit_message` events are not captured (the unwrap SMT has no `after`
  image for them, and the handler skips them).

### §8 CDC features
* **8.1** soft delete only (`deleted='true'` rows). No hard-delete mode.
* **8.2** no SCD2 / change-history mode. The destination is *only* a changelog — there is
  not even a current-state view.
* **8.3** no PII controls of any kind: exclusion, masking, hashing, truncation all absent.

## 5. Design decisions to carry forward

Things the baseline already got right and later phases should not regress:

1. **pgoutput + a version-controlled `PUBLICATION`** (rubric 7.1). Never `publication.autocreate`.
2. **Bounded, restartable runs** rather than a daemon — a MotherDuck Flight is scheduled.
3. **Deterministic destination naming from `dbz_schema`/`dbz_table`**, not from the topic.
4. **A guaranteed process exit** (watchdog + `os._exit`) — rubric 4.5 in miniature.
5. **`handler.busy` in-flight guard** — never tear the engine down mid-batch.
6. **A source schema that is hostile on purpose**: no-PK table, TOAST table, 34-type table,
   partitioned table. Gaps show up as test failures instead of surprises in production.

## 6. Open questions for later phases

* Can we reach Debezium's `RecordCommitter` from Python to commit offsets *inside* the same
  transaction as the destination write? (`pydbzengine/_jvm.py` calls `markBatchFinished()`
  for us — we may need to fork the handler.) Prerequisite for rubric 1.1/1.2.
* Is `decimal.handling.mode=string` + explicit dlt column hints enough for 2.4, or do we
  need to bypass `ExtractNewRecordState` and consume the full Debezium envelope with its
  schema so we can map types properly?
* Debezium's incremental snapshot / signalling channel looks like the right primitive for
  §3.3–3.7 — needs a signal data collection in Postgres.
* `heartbeat.action.query` + `pg_logical_emit_message` covers 4.4 and 7.4 together.
