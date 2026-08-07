"""Ongoing CDC around a real ADD COLUMN and DROP COLUMN."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


@pytest.fixture(scope="module")
def add_drop_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = "customers"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        box.run(reset_state=True, max_seconds=180)

        # CDC is live before the DDL, so this is not a snapshot-only assertion.
        box.sql("UPDATE app.customers SET name = 'before-ddl' WHERE id = 1")
        box.run(max_seconds=120, min_records=1)

        box.sql("ALTER TABLE app.customers ADD COLUMN evolution_marker text DEFAULT 'bronze'")
        box.sql(
            [
                "UPDATE app.customers SET evolution_marker = 'gold' WHERE id = 1",
                "INSERT INTO app.customers (name, email, evolution_marker) "
                "VALUES ('after-add', 'after-add@example.com', 'silver')",
            ],
            one_transaction=True,
        )
        after_add = box.run(max_seconds=150, min_records=1)
        source_after_add = box.pg_query(
            "SELECT id, evolution_marker FROM app.customers ORDER BY id"
        )
        target_after_add = box.duck_query(
            f"SELECT id, evolution_marker FROM {box.table('cdcflight_app_customers')} "
            "ORDER BY id"
        )

        box.sql("ALTER TABLE app.customers DROP COLUMN evolution_marker")
        box.sql("UPDATE app.customers SET name = 'after-drop' WHERE id = 1")
        after_drop = box.run(max_seconds=150, min_records=1)
        settled = box.run(max_seconds=120)
        yield {
            "box": box,
            "after_add": after_add,
            "after_add_source": source_after_add,
            "after_add_target": target_after_add,
            "after_drop": after_drop,
            "settled": settled,
        }
    finally:
        box.reseed()


def test_add_and_drop_runs_complete(add_drop_scenario):
    assert add_drop_scenario["after_add"]["ok"] is True
    assert add_drop_scenario["after_drop"]["ok"] is True


def test_added_column_matches_postgres_existing_and_new_rows(add_drop_scenario):
    assert add_drop_scenario["after_add_target"] == add_drop_scenario["after_add_source"]


def test_dropped_column_is_gone_and_non_evolved_data_kept(add_drop_scenario):
    box = add_drop_scenario["box"]
    assert box.duck_query(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? AND column_name = 'evolution_marker'",
        [box.DATASET, "cdcflight_app_customers"],
    ) == [(0,)]
    assert box.duck_query(
        f"SELECT id, name FROM {box.table('cdcflight_app_customers')} ORDER BY id"
    ) == box.pg_query("SELECT id, name FROM app.customers ORDER BY id")


def test_schema_changes_are_one_auditable_event_each(add_drop_scenario):
    box = add_drop_scenario["box"]
    events = box.duck_query(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers' AND event IN ('column_added', 'column_dropped') "
        "ORDER BY commit_id, seq"
    )
    assert events == [("column_added", True), ("column_dropped", True)]
