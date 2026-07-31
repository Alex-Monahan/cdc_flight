"""Rubric 1.3 - atomicity as an *observer of MotherDuck* would experience it.

Why this file exists (Codex 7)
------------------------------
`test_1_3_atomic_batches.py` runs against local DuckDB and reconstructs the
sequence of destination commits after the fact. That is good gap evidence, but
it is the wrong *target* evidence for two reasons:

1. rubric 1.3 asks for atomicity **in MotherDuck**, and DuckDB-on-a-file is a
   different transaction implementation;
2. metadata equality (`all rows share one cdcf_commit_id`) is satisfiable by an
   implementation that stamps the same id in two separate commits. Only a
   *visibility* assertion - a second connection that never sees a prefix - is
   proof.

DuckDB's single-writer file lock makes a concurrent observer impossible locally;
MotherDuck's server-side storage makes it trivial. So the visibility proof lives
here, marked `motherduck`, and is deliberately kept **small**: one Postgres
transaction of `2 * N` events across two tables, one observer thread sampling
both tables, no large loads.

Deselected by `make test`; run with `make test-md`.
"""

from __future__ import annotations

import threading
import time
import uuid

import duckdb
import pytest

from cdc_flight.config import motherduck_token

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

MD_DATABASE = "cdc_flight_dev"
N = 1500  # per table; 2 * N = 3000 events > max.batch.size (2048)

TARGET = (
    "rubric 1.3: whole-transaction commit groups need the custom MotherDuck "
    "applier with BEGIN/COMMIT (ADR 0001 D1/D2)"
)


@pytest.fixture(scope="module")
def md_token() -> str:
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    return token


@pytest.fixture(scope="module")
def md_observed_txn(sandbox, md_token) -> dict:
    """Stream one multi-table PG transaction into MotherDuck, watching from outside.

    The observer polls both tables from a *separate* MotherDuck connection while
    the pipeline writes, and records every `(customers, orders)` pair it sees.
    An atomic implementation can only ever be observed at `(0, 0)` or `(N, N)`.
    """
    dataset = f"cdc_atomic_{uuid.uuid4().hex[:8]}"
    dsn = f"md:{MD_DATABASE}?motherduck_token={md_token}"
    env = {
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": MD_DATABASE,
        "MOTHERDUCK_TOKEN": md_token,
        "motherduck_token": md_token,
    }

    bootstrap = duckdb.connect(f"md:?motherduck_token={md_token}")
    bootstrap.execute(f'CREATE DATABASE IF NOT EXISTS "{MD_DATABASE}"')
    bootstrap.close()

    sandbox.reseed()
    sandbox.run(
        reset_state=True, destination="motherduck", max_seconds=300,
        timeout=600, extra_env=env,
    )

    sandbox.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            "'mdatomic-c-' || i, 'mdatomic-c-' || i || '@example.com' "
            f"FROM generate_series(1, {N}) i",
            "INSERT INTO app.orders (customer_id, total_amount) "
            "SELECT id, 10.00 FROM app.customers WHERE name LIKE 'mdatomic-c-%'",
        ],
        one_transaction=True,
    )

    observations: list[tuple[int, int]] = []
    stop = threading.Event()

    def _observe():
        con = duckdb.connect(dsn)
        try:
            while not stop.is_set():
                try:
                    row = con.execute(
                        f'SELECT (SELECT count(*) FROM "{dataset}"."cdcflight_app_customers" '
                        "        WHERE name LIKE 'mdatomic-c-%'), "
                        f'       (SELECT count(*) FROM "{dataset}"."cdcflight_app_orders" o '
                        f'        WHERE EXISTS (SELECT 1 FROM "{dataset}"."cdcflight_app_customers" c '
                        "                      WHERE c.id = o.customer_id "
                        "                        AND c.name LIKE 'mdatomic-c-%'))"
                    ).fetchone()
                    observations.append((int(row[0]), int(row[1])))
                except duckdb.Error:
                    observations.append((0, 0))  # tables not created yet
                stop.wait(0.25)
        finally:
            con.close()

    watcher = threading.Thread(target=_observe, name="md-observer", daemon=True)
    watcher.start()
    try:
        streamed = sandbox.run(
            destination="motherduck", max_seconds=400, idle_seconds=10,
            timeout=700, extra_env=env,
        )
    finally:
        time.sleep(1.0)
        stop.set()
        watcher.join(timeout=15)

    con = duckdb.connect(dsn)
    try:
        yield {
            "box": sandbox,
            "con": con,
            "dataset": dataset,
            "streamed": streamed,
            "observations": observations,
            "n": N,
        }
    finally:
        con.execute(f'DROP SCHEMA IF EXISTS "{MD_DATABASE}"."{dataset}" CASCADE')
        con.close()


def test_scenario_reached_motherduck(md_observed_txn):
    """Guard: without this, every assertion below is vacuous."""
    con, dataset, n = md_observed_txn["con"], md_observed_txn["dataset"], md_observed_txn["n"]
    landed = con.execute(
        f'SELECT count(*) FROM "{dataset}"."cdcflight_app_customers" '
        "WHERE name LIKE 'mdatomic-c-%'"
    ).fetchone()[0]
    assert landed == n, md_observed_txn["streamed"]
    assert md_observed_txn["observations"], "the observer never sampled MotherDuck"


def test_gap_a_torn_transaction_is_observable_in_motherduck(md_observed_txn):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands.

    A reader on another MotherDuck connection saw a state in which part of one
    Postgres transaction was visible and the rest was not.
    """
    n = md_observed_txn["n"]
    torn = [pair for pair in md_observed_txn["observations"] if pair not in {(0, 0), (n, n)}]
    assert torn, (
        "no torn intermediate state was observed; either the observer was too slow "
        "or atomicity has been fixed - update RUBRIC_STATUS. observations="
        f"{md_observed_txn['observations'][:20]}"
    )


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_no_observer_ever_sees_a_partial_transaction(md_observed_txn):
    """TARGET BEHAVIOUR - the actual proof of rubric 1.3.

    Every observation from an independent MotherDuck connection must be either
    "the transaction is not there yet" or "the whole transaction is there".
    Nothing in between may ever be visible, in either table.
    """
    n = md_observed_txn["n"]
    torn = [pair for pair in md_observed_txn["observations"] if pair not in {(0, 0), (n, n)}]
    assert not torn, (
        f"{len(torn)} observations saw a partial Postgres transaction in MotherDuck, "
        f"e.g. {torn[:10]}"
    )


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_one_commit_group_per_pg_transaction_in_motherduck(md_observed_txn):
    """TARGET BEHAVIOUR - and the metadata agrees with what the observer saw."""
    con, dataset = md_observed_txn["con"], md_observed_txn["dataset"]
    commits = con.execute(
        f'SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_customers" '
        "WHERE name LIKE 'mdatomic-c-%' "
        f'UNION SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_orders" o '
        f'WHERE EXISTS (SELECT 1 FROM "{dataset}"."cdcflight_app_customers" c '
        "              WHERE c.id = o.customer_id AND c.name LIKE 'mdatomic-c-%')"
    ).fetchall()
    assert len(commits) == 1, f"PG transaction split across {len(commits)} commit groups"
