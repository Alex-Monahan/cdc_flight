# cdc_flight

Change Data Capture from **PostgreSQL** to **MotherDuck** (and local DuckDB), built on the
**Debezium embedded engine** driven from Python plus **dlt** for loading.

The starting point is the dlthub post
[*Real-time Data Replication with Debezium and Python*](https://dlthub.com/blog/debezium-and-dlt),
but this repo is meant to go a long way past it: the target is 5/5 on every item of a
42-item Postgres-CDC decision matrix (delivery guarantees, schema evolution, backfills,
failure recovery, latency, observability, PII controls). **This commit is the *baseline***
— faithful to the blog, cleaned up and made testable, with the gaps documented rather
than fixed. See [`research/NOTES.md`](research/NOTES.md) for the honest list.

## Architecture

```
                  ┌──────────────────────────────────────────── one Python process ──┐
 PostgreSQL 18    │                                                                  │
 (local cluster,  │  JVM (JPype)                        Python                        │
  :15432,         │  ┌────────────────────┐   batch of  ┌───────────────────┐         │
  wal_level=      │  │ Debezium 3.6       │   JSON      │ DltChangeHandler  │         │
  logical)        │  │ PostgresConnector  ├────────────▶│  → dlt.pipeline   │         │
    │  WAL        │  │ plugin=pgoutput    │   events    └─────────┬─────────┘         │
    └────────────▶│  │ ExtractNewRecord   │                       │                   │
   replication    │  └────────────────────┘                       ▼                   │
   slot +         │        ▲                              DuckDB file  /  MotherDuck  │
   publication    │        └── offsets.dat (file-backed)                              │
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

Every row is the **flattened `after` image** plus Debezium metadata columns added by
`ExtractNewRecordState`:

| column | meaning |
|---|---|
| `dbz_op` | `r` snapshot read, `c` insert, `u` update, `d` delete |
| `deleted` | `'true'` on delete rows (`delete.tombstone.handling.mode=rewrite`) |
| `dbz_table`, `dbz_schema` | source identifiers |
| `dbz_lsn`, `dbz_tx_id` | WAL position / Postgres transaction id |
| `dbz_source_ts_ms`, `dbz_ts_ms` | commit time / Debezium processing time |

(Debezium's own default prefix is `__`, but dlt strips leading underscores, which would
land the metadata as bare `op` / `table` / `schema`. We set
`transforms.unwrap.add.fields.prefix=dbz_` instead. `deleted` is the one field Debezium
does not apply that prefix to.)

Write disposition is **append** — the destination is a raw changelog, not a mirror.
Collapsing it into current state is Phase 1/8 work.

## Testing

```bash
make test        # default suite, local only (Postgres + DuckDB), no cloud
make test-md     # MotherDuck smoke test only  (marker: motherduck)
make test-all    # everything
```

`pytest` is configured with `--durations=20`, so every run prints a timing report.
MotherDuck tests carry the `motherduck` marker and are deselected by `make test`.

Measured on an M-series Mac (2026-07-30):

| suite | tests | wall clock |
|---|---|---|
| `make test` (local only) | 8 | **142 s** |
| `make test-md` | 2 | **35 s** |
| everything | 10 | **~3 min** |

Budget is 10 minutes for the whole suite, so there is plenty of headroom. The dominant
cost is JVM startup (~10 s) plus the Debezium idle tail per pipeline invocation; each e2e
test runs the pipeline twice.

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
│   ├── handler.py                # Debezium batch → dlt resources
│   ├── pipeline.py               # bounded runner + CLI entrypoint
│   ├── datagen.py                # deterministic change generator
│   └── inspect.py                # `make query`
├── tests/                        # pytest e2e harness
└── research/                     # blog capture + deviation notes
```

## Known baseline weaknesses

Summarised in [`research/NOTES.md`](research/NOTES.md); the headline ones:

* **At-least-once at best.** Debezium offsets live in `offsets.dat`, committed independently
  of the dlt load. A crash between the two duplicates rows, and `append` means duplicates stay.
* **No transactional boundaries.** Each Debezium batch becomes one or more dlt loads;
  Postgres transactions are split arbitrarily.
* **`numeric` arrives base64-encoded** (Debezium `decimal.handling.mode=precise`) and
  temporal types arrive as integers (`time.precision.mode=adaptive`) — left at Debezium
  defaults on purpose so the type-mapping gap is measurable.
* **Unchanged TOAST columns arrive as `__debezium_unavailable_value`.**
* **No heartbeat**, so an idle slot on a busy database retains WAL.
* **No snapshot/CDC coordination, no backfill controls, no schema-change handling beyond
  dlt's `evolve` contract, no observability tables, no PII controls.**

Every one of those is a numbered rubric item and gets its own phase.
