"""Slow real-PostgreSQL/Debezium evidence for rubric 2.5."""

from __future__ import annotations

import os
from types import SimpleNamespace

import psycopg
import pytest

from cdc_flight.apply_sql import _union_members
from cdc_flight.catalog import CHANGE_SCHEMA, CatalogWatcher
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.schema_epoch import refuse_mixed_schema_epoch

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_real_postgres_type_change_keeps_old_and_new_members(sandbox):
    """The source DDL/data epoch crosses one atomic typed shadow swap."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    sandbox.reseed()
    initial = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert initial["ok"] is True, initial

    # Let the catalog fence marker itself become durable before the first
    # post-change row.  A DDL+row transaction is deliberately a mixed-epoch
    # refusal (covered by the unit gate); the production round trip below proves
    # the normal ordered DDL -> post-boundary data path.
    sandbox.sql(
        "ALTER TABLE app.customers ALTER COLUMN lifetime_value "
        "TYPE double precision USING lifetime_value::double precision"
    )
    observed = sandbox.run(max_seconds=150, idle_seconds=6)
    assert observed["ok"] is True, observed
    sandbox.sql(
        "UPDATE app.customers SET lifetime_value = lifetime_value + 0.125 "
        "WHERE id = 1"
    )
    streamed = sandbox.run(max_seconds=180, idle_seconds=6)
    assert streamed["ok"] is True, streamed
    assert (
        observed["catalog_changes_applied"] + streamed["catalog_changes_applied"] >= 1
    ), (observed, streamed)
    assert streamed["catalog_pending_schema"] == 0, streamed

    physical = sandbox.duck_query(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'cdc_raw' AND table_name = 'cdcflight_app_customers' "
        "AND column_name = 'lifetime_value'"
    )[0][0]
    members = _union_members(physical)
    assert physical.startswith("UNION(")
    assert len(members) == 2, physical
    assert any("DECIMAL(18,4)" in member_type for _, member_type in members)
    assert any(member_type == "DOUBLE" for _, member_type in members)

    tags = dict(
        sandbox.duck_query(
            "SELECT id, union_tag(lifetime_value) "
            "FROM cdc_raw.cdcflight_app_customers ORDER BY id"
        )
    )
    assert len(set(tags.values())) == 2, tags
    assert tags[1] != tags[2]


def test_real_postgres_ddl_and_dml_one_transaction_is_a_mixed_epoch_refusal(sandbox):
    """Catalog evidence from one real transaction reaches the schema fence first."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = "cdc_flight_pub"
    qualified = "app.p2b_epoch"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS app.p2b_epoch")
        con.execute(
            "CREATE TABLE app.p2b_epoch (id integer PRIMARY KEY, value integer)"
        )
        con.execute("INSERT INTO app.p2b_epoch VALUES (1, 7)")
        con.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")

    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        primary_dsn=sandbox.source.dsn,
        publication=publication,
        schema="app",
        schemas={"app"},
        include={qualified},
        emit_marker=False,
        confirm_polls=1,
    )
    try:
        watcher.poll()
        with psycopg.connect(sandbox.source.dsn, autocommit=False) as con:
            con.execute(
                "ALTER TABLE app.p2b_epoch ALTER COLUMN value TYPE bigint "
                "USING value::bigint"
            )
            con.execute("UPDATE app.p2b_epoch SET value = 8 WHERE id = 1")
            con.commit()
        watcher.poll()
        schema_change = next(
            change for change in watcher.pending() if change.kind == CHANGE_SCHEMA
        )
        type_change = next(
            column
            for column in schema_change.column_changes
            if column.destination_new_name == "value"
        )
        assert type_change.old_descriptor is not None
        assert type_change.new_descriptor is not None
        event = PendingRecord(
            raw=None,
            kind=KIND_DATA,
            topic=qualified,
            nbytes=1,
            op="u",
            schema="app",
            table="p2b_epoch",
            key={"id": 1},
            before={"id": 1, "value": 7},
            after={"id": 1, "value": 8},
            before_descriptors={"value": type_change.old_descriptor},
            after_descriptors={"value": type_change.new_descriptor},
        )
        with pytest.raises(SchemaEvolutionRefused, match="both old and new"):
            refuse_mixed_schema_epoch(
                [event],
                [
                    SimpleNamespace(
                        change=schema_change,
                    )
                ],
            )
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
            con.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            con.execute("DROP TABLE IF EXISTS app.p2b_epoch")
