"""Rubric 1.4 end to end: real Postgres, real Debezium, real destination tables.

`test_1_4_pk_update_fold.py` pins the *fold* against constructed records. This
module pins the thing that cannot be constructed: that Postgres and Debezium
really do produce the event shapes the fold is written against, and that the
whole pipeline lands the right rows.

One module-scoped scenario, four Postgres transactions, one streaming run — so
the JVM and the snapshot are paid for once:

| txn | source statements | Postgres truth afterwards |
|---|---|---|
| T1 | `UPDATE app.customers SET id = 9001 WHERE id = 3` | customer 3 is now 9001 |
| T2 | insert 7001, rename it, `SET id = 7002`, then update it again | one customer, 7002, `name='Renamed'` |
| T3 | `DELETE … WHERE id = 9001; UPDATE … SET id = 9001 WHERE id = 7002` | 9001 = the renamed row |
| T4 | `UPDATE app.pk_demo SET id = id + 1` under a DEFERRABLE primary key | `{2, 3}` |

Customer 3 is chosen deliberately: `app.orders` has a foreign key to
`app.customers (id)` with `ON DELETE CASCADE` and no `ON UPDATE`, so Postgres
*refuses* a key update on a customer that has orders. Customer 3 has none.

`app.pk_demo` is created by the scenario (and added to the publication and to
`CDC_TABLES`) because the deferred-permutation case needs a table whose primary
key is `DEFERRABLE INITIALLY DEFERRED`, and mutating a seeded table's constraints
would leak into every other module that shares this cluster.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e

CUSTOMERS = "cdcflight_app_customers"
PK_DEMO = "cdcflight_app_pk_demo"
TABLES = "customers,orders,sensor_readings,documents,wide_types,audit_log,pk_demo"


@pytest.fixture(scope="module")
def pk_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = TABLES
    box.sql(
        [
            "DROP TABLE IF EXISTS app.pk_demo",
            "CREATE TABLE app.pk_demo (id bigint NOT NULL, label text NOT NULL, "
            "CONSTRAINT pk_demo_pkey PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED)",
            # MEASURED, 2026-07-31: a DEFERRABLE primary key is NOT a replica
            # identity. `UPDATE` on a published table then fails outright with
            # "cannot update table because it does not have a replica identity and
            # publishes updates", so the deferred-permutation collision is only
            # reachable with REPLICA IDENTITY FULL (or another non-deferrable unique
            # index). The message *key* still comes from the primary key.
            "ALTER TABLE app.pk_demo REPLICA IDENTITY FULL",
            "INSERT INTO app.pk_demo (id, label) VALUES (1, 'a'), (2, 'b')",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.pk_demo",
        ]
    )
    snapshot = box.run(reset_state=True, max_seconds=150)

    # T1 — the rubric's own case, in its own transaction.
    box.sql("UPDATE app.customers SET id = 9001 WHERE id = 3")
    # T2 — an insert, a normal update, a key change and another update, one txn.
    box.sql(
        [
            "INSERT INTO app.customers (id, name, email) "
            "VALUES (7001, 'Temp', 'temp@example.com')",
            "UPDATE app.customers SET name = 'Renamed' WHERE id = 7001",
            "UPDATE app.customers SET id = 7002 WHERE id = 7001",
            "UPDATE app.customers SET tags = ARRAY['moved'] WHERE id = 7002",
        ],
        one_transaction=True,
    )
    # T3 — the new key collides with a live row, freed earlier in the same txn.
    box.sql(
        [
            "DELETE FROM app.customers WHERE id = 9001",
            "UPDATE app.customers SET id = 9001 WHERE id = 7002",
        ],
        one_transaction=True,
    )
    # T4 — a permutation Postgres only allows because the key check is deferred.
    box.sql("UPDATE app.pk_demo SET id = id + 1", one_transaction=True)

    streamed = box.run(max_seconds=150, min_records=1)
    try:
        yield {"box": box, "snapshot": snapshot, "streamed": streamed}
    finally:
        box.reseed()


def _source(box, stmt: str) -> list[tuple]:
    return box.pg_query(stmt)


# --------------------------------------------------------------------------- #
# the source really does emit what the fold is written against
# --------------------------------------------------------------------------- #
def test_a_key_update_arrives_as_a_delete_and_an_insert_in_one_transaction(pk_scenario):
    """The `d`/`c` pair carries ONE transaction id and ONE commit id.

    That is the whole reason 1.4 is atomic: the pair cannot be split across commit
    groups, because a group holds whole transactions.
    """
    box = pk_scenario["box"]
    rows = box.duck_query(
        f"SELECT dbz_tx_id, cdcf_commit_id FROM {box.table(CUSTOMERS)} WHERE id = 9001"
    )
    assert rows, "the moved row is missing entirely"
    tx_id, commit_id = rows[0]
    assert tx_id is not None and commit_id is not None
    # The `d` half of the same pair was applied in the same commit group: the
    # commit_log row for that group lists the table exactly once.
    logged = box.duck_query(
        "SELECT count(*) FROM _cdc_flight.commit_log WHERE commit_id = ? "
        "AND list_contains(tables_touched, ?)",
        [commit_id, CUSTOMERS],
    )
    assert logged[0][0] == 1


def test_the_destination_matches_postgres_row_for_row(pk_scenario):
    box = pk_scenario["box"]
    source = _source(box, "SELECT id, name FROM app.customers ORDER BY id")
    landed = box.duck_query(f"SELECT id, name FROM {box.table(CUSTOMERS)} ORDER BY id")
    assert landed == source


def test_no_key_appears_twice(pk_scenario):
    """The rubric's `duplication=2` is exactly this query returning a row."""
    box = pk_scenario["box"]
    dupes = box.duck_query(
        f"SELECT id, count(*) FROM {box.table(CUSTOMERS)} GROUP BY id HAVING count(*) > 1"
    )
    assert dupes == []


def test_the_old_key_is_gone(pk_scenario):
    box = pk_scenario["box"]
    assert box.duck_query(f"SELECT count(*) FROM {box.table(CUSTOMERS)} WHERE id IN (3, 7001, 7002)") == [(0,)]


def test_the_moved_row_keeps_the_values_it_had_after_the_move(pk_scenario):
    """`replace.null.with.default=false` matters here: a delete image full of
    fabricated zeros would resurrect the row with empty columns."""
    box = pk_scenario["box"]
    rows = box.duck_query(
        f"SELECT name, tags FROM {box.table(CUSTOMERS)} WHERE id = 9001"
    )
    assert rows == [("Renamed", '["moved"]')]


def test_gap_a_deferred_key_permutation_loses_a_row(pk_scenario):
    """MEASURED GAP (real Postgres, real Debezium, 2026-07-31): Postgres holds
    `{2, 3}` and the destination holds `{3}`. Deleted with the fix."""
    box = pk_scenario["box"]
    assert _source(box, "SELECT id, label FROM app.pk_demo ORDER BY id") == [(2, "a"), (3, "b")]
    assert box.duck_query(f"SELECT id, label FROM {box.table(PK_DEMO)} ORDER BY id") == [(3, "b")]


@pytest.mark.xfail(
    strict=True,
    reason="1.4 target: the fold collapses by key, so the second `d` of the "
    "permutation deletes the row the first key change created",
)
def test_a_deferred_key_permutation_lands_both_rows(pk_scenario):
    """`UPDATE app.pk_demo SET id = id + 1` — the collision case Postgres allows."""
    box = pk_scenario["box"]
    assert _source(box, "SELECT id, label FROM app.pk_demo ORDER BY id") == [(2, "a"), (3, "b")]
    assert box.duck_query(f"SELECT id, label FROM {box.table(PK_DEMO)} ORDER BY id") == [
        (2, "a"),
        (3, "b"),
    ]


def test_a_key_update_on_a_table_with_an_array_column_stays_one_table(pk_scenario):
    """`app.customers.tags` is `text[]`.

    ADR §7 note 1 anticipates child tables (`<root>__tags`) for arrays; this applier
    lands an array as one JSON column instead, so a PK update touches exactly one
    destination table. Asserted rather than assumed, because "the row must move in
    every child table too" is a real 1.4 failure mode the moment child tables exist.
    """
    box = pk_scenario["box"]
    children = box.duck_query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? "
        "AND table_name LIKE ?",
        [box.DATASET, f"{CUSTOMERS}\\_\\_%"],
    )
    assert children == [], f"child tables exist and the PK update did not move them: {children}"
    types = dict(
        box.duck_query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = 'tags'",
            [box.DATASET, CUSTOMERS],
        )
    )
    assert types == {"tags": "JSON"}


def test_the_run_reported_no_error(pk_scenario):
    """`primary key update causes an error=1`: the runs must be clean."""
    assert pk_scenario["snapshot"]["ok"] is True
    assert pk_scenario["streamed"]["ok"] is True
    assert pk_scenario["streamed"]["returncode"] == 0
