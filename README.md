# cdc_flight

Change Data Capture from **PostgreSQL** to **MotherDuck** (and local DuckDB), built on the
**Debezium embedded engine** driven from Python plus a **transactional applier** that owns
the destination transaction.

The starting point was the dlthub post
[*Real-time Data Replication with Debezium and Python*](https://dlthub.com/blog/debezium-and-dlt),
but this repo goes a long way past it: the target is 5/5 on every item of a Postgres-CDC
decision matrix (delivery guarantees, schema evolution, backfills, failure recovery,
latency, observability, PII controls).

**Where it is now.** Rubric §1.1 (exactly-once, keyed tables), §1.2 (exactly-once, keyless
tables) and §1.3 (multi-table atomic batches) are implemented and their target tests pass.
The mechanism is one commit group = one destination `BEGIN … COMMIT` containing an integral
number of *whole* Postgres transactions, with the Debezium resume point written **inside**
that transaction and the connector acknowledged **only after** it commits. See
[`docs/adr/0001-transactional-applier.md`](docs/adr/0001-transactional-applier.md).
`dlt.pipeline.run()` is no longer in the apply path — it structurally cannot host the
resume point in the destination transaction — but dlt remains as a *library*
(`cdc_flight/naming.py`).

## Architecture

```
                  ┌──────────────────────────────────────────── one Python process ──┐
 PostgreSQL 18    │                                                                  │
 (local cluster,  │  JVM (JPype)                       Python                         │
  :15432,         │  ┌────────────────────┐  full      ┌──────────────────────┐       │
  wal_level=      │  │ Debezium 3.6       │  envelope  │ TransactionAssembler │       │
  logical)        │  │ PostgresConnector  ├───────────▶│   -> whole units     │       │
    │  WAL        │  │ plugin=pgoutput    │  + BEGIN/  └──────────┬───────────┘       │
    └────────────▶│  │ txn metadata ON    │    END                │ commit group      │
   replication    │  └────────────────────┘                       ▼                   │
   slot +         │        ▲   ▲                        ┌──────────────────────┐      │
   publication    │        │   └── offsets.dat          │ Applier: BEGIN .. .. │      │
                  │        │       (scratch, rebuilt    │  apply, commit_log,  │      │
                  │        │        from the table)     │  resume point COMMIT │      │
                  │        │                            └──────────┬───────────┘      │
                  │        └── markProcessed() AFTER the COMMIT    ▼                   │
                  │            (Invariant O)             DuckDB file / MotherDuck     │
                  └──────────────────────────────────────────────────────────────────┘
```

There is **no Kafka, no Kafka Connect, no Docker**. Debezium runs as an embedded engine
inside the Python process via JPype; Postgres is a project-local Homebrew cluster.

## Requirements

| Component | Version used | Notes |
|---|---|---|
| PostgreSQL | **18.1** (Homebrew `postgresql@18`) | latest GA major; project-local cluster on **:15432** |
| Java (JDK) | **OpenJDK 23.0.2** (Homebrew `openjdk`) | pydbzengine needs JDK **17+** on `PATH` |
| Debezium | **3.6.0.Final** | bundled inside `pydbzengine` 3.6.0.0 |
| pydbzengine | **3.6.0.0** | installed from GitHub (not on PyPI any more) |
| dlt | **1.19.x** | `dlt[duckdb,motherduck]` |
| DuckDB | **1.4.x** | |
| Python | **3.13** | managed by `uv` |

```bash
brew install postgresql@18 openjdk
brew install uv           # if not already installed
```

The project-local cluster lives in `./.pgdata` and **never touches** a Homebrew
`postgresql@N` service you may already run on :5432.

## Quick start

```bash
make install       # uv venv + editable install (pulls the ~340 MB pydbzengine repo once)
make up            # initdb (first time) + start Postgres on :15432, create cdc_source
make seed          # apply sql/01_schema.sql and sql/02_seed.sql
make pipeline      # Debezium snapshot + stream → ./cdc_flight.duckdb
make changes       # generate inserts/updates/deletes in Postgres
make pipeline      # run again → the new change events land in DuckDB
make query         # show row counts + a sample of what landed
make down          # stop Postgres
```

Full reset (fresh cluster, fresh offsets, fresh DuckDB):

```bash
make reset
```

### What each step actually does

* **`make up`** → `scripts/pg.sh start`. First run does `initdb` into `./.pgdata` and appends
  `wal_level=logical`, `max_replication_slots=20`, `max_wal_senders=20`, `port=15432`,
  `unix_socket_directories=<pgdata>` to `postgresql.conf`. Then `pg_ctl start` and
  `createdb cdc_source`.
* **`make seed`** → `sql/01_schema.sql` builds schema `app` (6 tables: PK tables, a **no-PK**
  table with `REPLICA IDENTITY FULL`, a **TOAST**-heavy `documents` table, a `wide_types`
  table covering ~34 Postgres types, and a **partitioned** `audit_log`) and creates
  `PUBLICATION cdc_flight_pub`. `sql/02_seed.sql` inserts deterministic starting rows.
* **`make pipeline`** → `cdc-flight`. Builds Debezium properties (see
  `src/cdc_flight/debezium_props.py`), starts the embedded engine on a background thread,
  and loads every batch through `dlt` into `cdc_flight.duckdb`, dataset `cdc_raw`.
  The run is **bounded**: it stops once the stream has been quiet for `--idle-seconds`
  (default 8) or after `--max-seconds` (default 90) — the shape a scheduled Flight needs.
* **`make changes`** → `cdc-datagen changes`. One deterministic wave of ~25 DML statements
  across all tables, including an update that leaves the TOASTed `body` column untouched
  and update/delete against the no-PK table.

### Running against MotherDuck

`motherduck_token` must be exported in your shell (lowercase or `MOTHERDUCK_TOKEN`).

```bash
make pipeline-md        # loads into MotherDuck database `cdc_flight_dev`, dataset `cdc_raw`
```

The database is created on first connect by MotherDuck. Keep this light — the local DuckDB
path is the fast dev loop; MotherDuck is a smoke test that the same code path works.

## Destination shape

One destination table per source table, named `<topic_prefix>_<schema>_<table>`:

| DuckDB table | source |
|---|---|
| `cdc_raw.cdcflight_app_customers` | `app.customers` |
| `cdc_raw.cdcflight_app_orders` | `app.orders` |
| `cdc_raw.cdcflight_app_sensor_readings` | `app.sensor_readings` |
| `cdc_raw.cdcflight_app_documents` | `app.documents` |
| `cdc_raw.cdcflight_app_wide_types` | `app.wide_types` |
| `cdc_raw.cdcflight_app_audit_log` | `app.audit_log` (all partitions, via `publish_via_partition_root`) |

Every row is the event's `after` image (or, for a delete, its `before` image) plus:

| column | source | meaning |
|---|---|---|
| `cdcf_commit_id` | applier | which destination transaction made this row visible |
| `cdcf_event_id` | applier | stable event identity, `"<lsn>:<txId>:<total_order>"` (or `"snap:<epoch>:<schema>.<table>:<n>"`) |
| `cdcf_total_order` | envelope | ordinal within the Postgres transaction; NULL for snapshot rows |
| `dbz_op` | envelope | `r` snapshot read, `c` insert, `u` update, `d` delete |
| `dbz_table`, `dbz_schema` | envelope | source identifiers |
| `dbz_lsn`, `dbz_tx_id` | envelope | WAL position / Postgres transaction id |
| `dbz_source_ts_ms` | envelope | commit time |

**Two write shapes, chosen per table by whether Debezium emits a message key:**

* **keyed tables** (a primary key or unique index exists) are **current state**. Within a
  commit group every touched key is deleted and the group's final row per key is inserted,
  which makes a primary-key update fall out as delete-old + insert-new with no special
  case, and a delete a real delete.
* **keyless tables** are an **append-only changelog keyed on `cdcf_event_id`**. Two
  genuinely identical source rows are two different *events*, so both survive; a replayed
  event recomputes the same id and is dropped. Nothing that deduplicates by row *content*
  can do both, which is exactly what rubric 1.2 asks for.

Alongside the data, the applier owns a `_cdc_flight` schema: `debezium_offsets` (the
resume point, written inside the data transaction), `commit_log` (one row per destination
transaction), `lease` (single-writer, rubric 4.2), `table_state`, `spill_events` and
`alerts`.

## Testing

```bash
make test        # default suite, local only (Postgres + DuckDB), no cloud, no slow tests
make test-md     # MotherDuck smoke test only        (marker: motherduck)
make test-slow   # slow fault injection only         (marker: slow)
make test-all    # everything
```

`pytest` is configured with `--durations=20`, so every run prints a timing report.
MotherDuck tests carry the `motherduck` marker and long fault-injection tests carry
`slow`; both are deselected by `make test`.

Measured on an M-series Mac. Only executed runs are reported here; see
`RUBRIC_STATUS.md` for the per-item evidence.

| suite | result | wall clock | measured |
|---|---|---|---|
| `make test` (local only) | **106 passed, 0 xfail** | **422 s** (7:01) | 2026-07-30, after the applier |
| `make test-md` | **5 passed** | **97 s** | 2026-07-30, after the applier |
| `make test-slow` | see `RUBRIC_STATUS.md` | — | 2026-07-30, after the applier |

The xfail count is now zero because the 1.1/1.2/1.3 target tests pass; the gap pins they
superseded were deleted, as each suite's README said they should be.

Budget is 10 minutes for the default suite. The dominant cost is JVM startup plus the
Debezium idle tail per pipeline invocation, so the suite is optimised by *sharing
scenarios*, not by running fewer assertions: the rubric gap suites build their scenario
once per module (or, for `crash_replay`, once per session) and then interrogate it with
many cheap tests.

**One session at a time.** Every sandbox has its own slot, offsets and DuckDB file, but
they all share the `app` schema and publication on :15432, and reseeding drops and
recreates both. Two concurrent `make test` runs used to corrupt each other - that is what
produced the 1.0 review's `assert 110 == 0`. A session-wide `flock` on
`.pytest-source.lock` now serialises whole sessions; a second run waits and says so.

### Test layout and conventions

Rubric work lives in `tests/<item>_<slug>/`, each with a README explaining the gap:

| directory | rubric item |
|---|---|
| `tests/1.0_engine_error_propagation/` | TODO 1.0(b) — engine failures must not exit 0 |
| `tests/1.1_exactly_once_pk/` | 1.1 delivery guarantees, tables with a PK |
| `tests/1.2_exactly_once_nopk/` | 1.2 delivery guarantees, tables without a PK |
| `tests/1.3_atomic_batches/` | 1.3 multi-table transactional atomicity |

Three naming conventions carry meaning:

* `test_gap_*` — pins **today's broken behaviour**. It passes now; when the fix lands it
  starts failing, which is the signal to delete it.
* `test_target_*` — the **desired behaviour**, marked
  `@pytest.mark.xfail(..., strict=True)`. Strict means an unexpected pass fails the
  suite, so implementing the feature forces the marker (and its paired gap pin) to be
  removed. Nothing drifts silently.
* `@pytest.mark.slow` — real `kill -9` / large-workload fault injection, deselected by
  `make test`. Every slow test has a fast deterministic counterpart in the default suite.

### Fault injection

`src/cdc_flight/faults.py` makes crash points exact instead of racing a `kill -9`
(`probes/p07` lost that race outright). It is inert unless `CDC_FAULT_INJECT` is set:

```bash
CDC_FAULT_INJECT=post_commit_pre_ack:1 uv run cdc-flight --destination duckdb
```

Fault points are named after the **transactional protocol**, not the current
implementation, so they survive the ADR 0001 refactor:

| point | meaning |
|---|---|
| `pre_commit` | applied to the destination inside an open transaction, not committed |
| `post_commit_pre_ack` | destination committed, Debezium **not** acknowledged — the at-least-once window |
| `post_ack` | `offsets.dat` flushed, the replication slot not yet confirmed |

`<nth>` counts **data** batches only: batches containing nothing but Debezium heartbeats
or transaction-metadata markers are not counted, because otherwise enabling
`provide.transaction.metadata` would silently move every fault point by one and disarm
the whole 1.1/1.2 suite. The action defaults to `os._exit(137)`; `:raise` instead raises,
which exercises Debezium's error-teardown path. The spec is parsed and validated once at
start-up, so a typo fails the run rather than leaving a fault test vacuously green.
Legacy names `before_load` / `after_load` still work.

The tests start the Postgres cluster themselves if it is not already up (session fixture
`postgres_cluster`), reseed the schema, and give every test its own replication slot,
Debezium offset file, dlt state directory and DuckDB file under `tmp_path`. The cluster is
left running afterwards — `make down` stops it.

## Layout

```
cdc_flight/
├── Makefile                      # up/down/test/pipeline + pg lifecycle
├── scripts/pg.sh                 # project-local Postgres cluster (initdb/pg_ctl)
├── sql/01_schema.sql             # source schema + publication
├── sql/02_seed.sql               # deterministic seed rows
├── src/cdc_flight/
│   ├── config.py                 # env-driven config dataclasses
│   ├── debezium_props.py         # Debezium engine properties
│   ├── engine.py                 # Debezium engine that cannot fail silently
│   ├── errors.py                 # EngineFailure
│   ├── faults.py                 # deterministic crash injection (test-only)
│   ├── envelope.py               # decode the full Debezium envelope
│   ├── assembler.py              # TransactionAssembler: whole units only
│   ├── applier.py                # commit groups, fence, snapshot swap, spill
│   ├── apply_sql.py              # merge/DDL SQL inside the caller's transaction
│   ├── destination.py            # _cdc_flight control schema + lease
│   ├── offset_file.py            # read/write Debezium's offsets.dat
│   ├── reconcile.py              # start-up decision table + Invariant-O guard
│   ├── naming.py                 # dlt-as-a-library identifier normalisation
│   ├── pipeline.py               # bounded runner + CLI entrypoint
│   ├── datagen.py                # deterministic change generator
│   └── inspect.py                # `make query`
├── docs/adr/                     # architecture decision records
├── tests/                        # pytest e2e harness + rubric gap suites
├── probes/                       # rubric evidence experiments (see RUBRIC_STATUS.md)
├── RUBRIC_STATUS.md              # all 40 rubric items scored, with evidence
└── research/                     # blog capture + deviation notes
```

## Rubric status

[`RUBRIC_STATUS.md`](RUBRIC_STATUS.md) scores this baseline against every item of
the 40-item Postgres-CDC decision matrix, with the evidence for each score.
**Baseline average: 1.65 / 5; one item (7.1, pgoutput) is already at 5.**

The evidence comes from [`probes/`](probes/) — small, reproducible experiments
(`uv run python probes/p01_dml_edge_cases.py`, output in `probes/.out/`). They
are *not* tests: several deliberately break the source schema, and each reseeds
it first and uses its own replication slot, offset file and DuckDB file.

The single most important finding was a bug rather than a rubric item:
`run_engine_bounded` reported **exit 0** when the Debezium engine failed to start
(dropped slot, unusable offset), so several catastrophic failure modes looked like
successful no-op runs. **Fixed** — see `src/cdc_flight/engine.py` and
`tests/1.0_engine_error_propagation/`. Debezium reports such failures through a
`CompletionCallback` and then returns normally from `run()`; the engine now
registers one, and the CLI exits non-zero with the Debezium message and writes a
`last_run.json` carrying `ok: false` and `error`.

The architecture that closes §1 of the rubric is decided in
[`docs/adr/0001-transactional-applier.md`](docs/adr/0001-transactional-applier.md).

## Known baseline weaknesses

Summarised in [`research/NOTES.md`](research/NOTES.md) and scored in
[`RUBRIC_STATUS.md`](RUBRIC_STATUS.md); the headline ones:

* ~~**At-least-once at best.**~~ **FIXED** by the transactional applier (rubric 1.1/1.2).
* ~~**No transactional boundaries.**~~ **FIXED** — a commit group is an integral number of
  whole Postgres transactions (rubric 1.3).
* **`numeric` arrives base64-encoded** (Debezium `decimal.handling.mode=precise`) and
  temporal types arrive as integers (`time.precision.mode=adaptive`) — left at Debezium
  defaults on purpose so the type-mapping gap is measurable.
* **Unchanged TOAST columns arrive as `__debezium_unavailable_value`.**
* **No heartbeat**, so an idle slot on a busy database retains WAL.
* **No snapshot/CDC coordination, no backfill controls, no schema-change handling beyond
  dlt's `evolve` contract, no observability tables, no PII controls.**

Every one of those is a numbered rubric item and gets its own phase.
