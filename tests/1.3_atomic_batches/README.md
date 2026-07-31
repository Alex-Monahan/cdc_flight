# tests/1.3_atomic_batches

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

Conventions are described in `tests/1.0_engine_error_propagation/README.md`.

## Note on the target shape

The target tests assert on `cdcf_commit_id` — the MotherDuck commit-group
identifier defined in `docs/adr/0001-transactional-applier.md`. A commit group is
allowed to contain *many whole* Postgres transactions (MotherDuck sustains only
~100 txn/s), but never part of one. So the invariant is
"`count(DISTINCT cdcf_commit_id) = 1` per `dbz_tx_id`", **not** the reverse.
