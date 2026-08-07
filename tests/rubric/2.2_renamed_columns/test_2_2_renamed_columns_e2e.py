"""A live pgoutput stream around an attnum-preserving PostgreSQL rename."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


@pytest.fixture(scope="module")
def rename_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = "customers"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        box.run(reset_state=True, max_seconds=180)
        box.sql("UPDATE app.customers SET name = 'pre-rename' WHERE id = 1")
        box.run(max_seconds=120, min_records=1)

        box.sql("ALTER TABLE app.customers RENAME COLUMN name TO full_name")
        box.sql(
            [
                "UPDATE app.customers SET full_name = 'post-rename' WHERE id = 1",
                "INSERT INTO app.customers (full_name, email) "
                "VALUES ('new-after-rename', 'rename@example.com')",
            ],
            one_transaction=True,
        )
        streamed = box.run(max_seconds=150, min_records=1)
        settled = box.run(max_seconds=120)
        yield {"box": box, "streamed": streamed, "settled": settled}
    finally:
        box.reseed()


def test_rename_stream_completed(rename_scenario):
    assert rename_scenario["streamed"]["ok"] is True


def test_destination_has_one_continuous_logical_column(rename_scenario):
    box = rename_scenario["box"]
    columns = box.duck_query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? "
        "AND column_name IN ('name', 'full_name') ORDER BY ordinal_position",
        [box.DATASET, "cdcflight_app_customers"],
    )
    assert columns == [("full_name",)]
    expected = box.pg_query(
        "SELECT id, full_name FROM app.customers "
        "WHERE id = 1 OR full_name = 'new-after-rename' ORDER BY id"
    )
    assert box.duck_query(
        f"SELECT id, full_name FROM {box.table('cdcflight_app_customers')} "
        "WHERE id = 1 OR full_name = 'new-after-rename' ORDER BY id"
    ) == expected


def test_rename_has_no_drop_add_history(rename_scenario):
    box = rename_scenario["box"]
    assert box.duck_query(
        "SELECT event, applied, detail FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers' AND event LIKE 'column_%' "
        "ORDER BY commit_id, seq"
    ) == [("column_renamed", True, "name -> full_name")]
