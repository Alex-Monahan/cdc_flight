"""Rubric 1.3 - a Postgres transaction must land in MotherDuck atomically.

See README.md for the failure mode, the observation technique and the test
conventions.
"""

from __future__ import annotations

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
ORDERS = '"cdc_raw"."cdcflight_app_orders"'
N = 1500  # per table; 2 * N = 3000 events > max.batch.size (2048)

TARGET = (
    "rubric 1.3: whole-transaction commit groups need the custom MotherDuck "
    "applier with BEGIN/COMMIT (ADR 0001)"
)


@pytest.fixture(scope="module")
def multi_table_txn(sandbox) -> dict:
    """One Postgres transaction writing two tables, larger than one batch."""
    sandbox.reseed()
    # A normal initial snapshot first, so the slot exists and the seeded rows are
    # already accounted for (they arrive as dbz_op='r' with the snapshot's tx id,
    # which is why every assertion below keys off the *streamed* transaction).
    sandbox.run(reset_state=True, max_seconds=150)

    sandbox.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            "'atomic-c-' || i, 'atomic-c-' || i || '@example.com' "
            f"FROM generate_series(1, {N}) i",
            "INSERT INTO app.orders (customer_id, total_amount) "
            "SELECT id, 10.00 FROM app.customers WHERE name LIKE 'atomic-c-%'",
        ],
        one_transaction=True,
    )
    streamed = sandbox.run(max_seconds=200, idle_seconds=8)

    tx_ids = [
        t
        for (t,) in sandbox.duck_query(
            f"SELECT DISTINCT dbz_tx_id FROM {CUSTOMERS} "
            "WHERE name LIKE 'atomic-c-%' AND dbz_op = 'c'"
        )
    ]
    return {"box": sandbox, "streamed": streamed, "tx_ids": tx_ids}


def _load_ids(box, tx_id: int) -> list[str]:
    rows = box.duck_query(
        f"SELECT _dlt_load_id FROM {CUSTOMERS} WHERE dbz_tx_id = {tx_id} "
        f"UNION SELECT _dlt_load_id FROM {ORDERS} WHERE dbz_tx_id = {tx_id} "
        "ORDER BY 1"
    )
    return [r[0] for r in rows]


def test_scenario_is_one_postgres_transaction(multi_table_txn):
    """Guard: both tables really were written by a single PG transaction."""
    box = multi_table_txn["box"]
    tx_ids = multi_table_txn["tx_ids"]
    assert len(tx_ids) == 1, f"expected one dbz_tx_id, got {tx_ids}"
    tx_id = tx_ids[0]
    customers = box.scalar(f"SELECT count(*) FROM {CUSTOMERS} WHERE dbz_tx_id = {tx_id}")
    orders = box.scalar(f"SELECT count(*) FROM {ORDERS} WHERE dbz_tx_id = {tx_id}")
    assert customers == N and orders == N, (customers, orders)


def test_gap_pg_transaction_is_split_across_commits(multi_table_txn):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands.

    One Postgres transaction, more than one destination commit.
    """
    box = multi_table_txn["box"]
    loads = _load_ids(box, multi_table_txn["tx_ids"][0])
    assert len(loads) > 1, (
        "expected the PG transaction to be split across several dlt load "
        f"packages (max.batch.size=2048 vs {2 * N} events); got {loads}"
    )


def test_gap_torn_transaction_is_observable(multi_table_txn):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands.

    Reconstruct what a reader saw after the first destination commit: part of
    one Postgres transaction visible, the rest not.
    """
    box = multi_table_txn["box"]
    tx_id = multi_table_txn["tx_ids"][0]
    first = _load_ids(box, tx_id)[0]

    seen_customers = box.scalar(
        f"SELECT count(*) FROM {CUSTOMERS} WHERE dbz_tx_id = {tx_id} "
        f"AND _dlt_load_id <= '{first}'"
    )
    seen_orders = box.scalar(
        f"SELECT count(*) FROM {ORDERS} WHERE dbz_tx_id = {tx_id} "
        f"AND _dlt_load_id <= '{first}'"
    )
    assert (seen_customers, seen_orders) != (N, N), (
        "the first destination commit already contained the whole transaction; "
        "atomicity may have been fixed - update RUBRIC_STATUS"
    )
    assert seen_customers + seen_orders > 0, "nothing landed in the first commit"


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_pg_transaction_lands_in_one_commit(multi_table_txn):
    """TARGET BEHAVIOUR - all rows of a PG transaction share one commit group.

    A commit group may contain *many whole* PG transactions; it may never
    contain part of one.
    """
    box = multi_table_txn["box"]
    tx_id = multi_table_txn["tx_ids"][0]
    commits = box.duck_query(
        f"SELECT DISTINCT cdcf_commit_id FROM {CUSTOMERS} WHERE dbz_tx_id = {tx_id} "
        f"UNION SELECT DISTINCT cdcf_commit_id FROM {ORDERS} WHERE dbz_tx_id = {tx_id}"
    )
    assert len(commits) == 1, f"PG transaction split across {len(commits)} commit groups"


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_commit_group_metadata_is_present(multi_table_txn):
    """TARGET BEHAVIOUR - every applied row is attributable to a commit group."""
    box = multi_table_txn["box"]
    for table in (CUSTOMERS, ORDERS):
        nulls = box.scalar(f"SELECT count(*) FROM {table} WHERE cdcf_commit_id IS NULL")
        assert nulls == 0, f"{table} has {nulls} rows with no commit group"
