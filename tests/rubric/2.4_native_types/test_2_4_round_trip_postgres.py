"""Slow real-PostgreSQL/Debezium evidence for rubric 2.4."""

from __future__ import annotations

import os

import psycopg
import pytest

from cdc_flight.catalog import CatalogWatcher
from cdc_flight.catalog_descriptors import RelationDescriptorProvider
from cdc_flight.catalog_state import CHANGE_SCHEMA
from cdc_flight.config import ReplicationConfig
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.planner import GroupPlan

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_real_postgres_native_arrays_specials_and_obscure_text(sandbox):
    """A real schema-bearing stream reaches native nested destinations."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    sandbox.reseed()
    initial = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert initial["ok"] is True, initial

    sandbox.sql(
        [
            "UPDATE app.wide_types SET "
            "col_int_array = ARRAY[]::integer[], "
            "col_text_array = ARRAY[]::text[], "
            "col_numeric_array = ARRAY[]::numeric(12,2)[] "
            "WHERE id = 1",
            "UPDATE app.wide_types SET "
            "col_double_inf = '-Infinity'::double precision, "
            "col_double_nan = 'NaN'::double precision "
            "WHERE id = 1",
        ],
        one_transaction=True,
    )
    streamed = sandbox.run(max_seconds=150, idle_seconds=6)
    assert streamed["ok"] is True, streamed

    types = dict(
        sandbox.duck_query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'cdcflight_app_wide_types'"
        )
    )
    assert types["col_int_array"] == "INTEGER[]"
    assert types["col_text_array"] == "VARCHAR[]"
    assert types["col_numeric_array"].endswith("[]")
    assert types["col_numeric_array"].startswith("UNION(")
    assert types["col_jsonb"] == "VARIANT"
    assert types["col_inet"] == "VARCHAR"
    assert types["col_money"] == "VARCHAR"

    row = sandbox.duck_query(
        "SELECT col_int_array, col_text_array, col_numeric_array, "
        "isinf(col_double_inf), isnan(col_double_nan) "
        "FROM cdc_raw.cdcflight_app_wide_types WHERE id = 1"
    )[0]
    assert row[:3] == ([], [], [])
    assert row[3:] == (True, True)


def test_real_catalog_watcher_refreshes_mutable_enum_and_composite_facts(sandbox):
    """OID reuse is not treated as type immutability across catalog epochs."""
    publication = ReplicationConfig().publication_name
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS app.p2b_mutable")
        conn.execute("DROP TYPE IF EXISTS app.p2b_address CASCADE")
        conn.execute("DROP TYPE IF EXISTS app.p2b_mood CASCADE")
        conn.execute("CREATE TYPE app.p2b_mood AS ENUM ('happy', 'sad')")
        conn.execute("CREATE TYPE app.p2b_address AS (street text)")
        conn.execute(
            "CREATE TABLE app.p2b_mutable (id integer PRIMARY KEY, "
            "mood app.p2b_mood, address app.p2b_address)"
        )
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE app.p2b_mutable")

    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        primary_dsn=sandbox.source.dsn,
        publication=publication,
        schema="app",
        schemas={"app"},
        include={"app.p2b_mutable"},
        emit_marker=False,
        confirm_polls=1,
    )
    try:
        watcher.poll()
        initial = watcher.known["app.p2b_mutable"]
        initial_by_name = {column.name: column for column in initial.columns}
        initial_enum = initial_by_name["mood"].descriptor
        initial_composite = initial_by_name["address"].descriptor

        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute("ALTER TYPE app.p2b_mood ADD VALUE 'sleepy'")
            conn.execute("ALTER TYPE app.p2b_address ADD ATTRIBUTE postal_code text")

        watcher.poll()
        current = watcher.known["app.p2b_mutable"]
        current_by_name = {column.name: column for column in current.columns}
        assert current_by_name["mood"].descriptor.enum_labels == (
            "happy", "sad", "sleepy"
        )
        assert tuple(name for name, _ in current_by_name["address"].descriptor.composite_fields) == (
            "street", "postal_code"
        )
        assert current_by_name["mood"].descriptor.fingerprint != initial_enum.fingerprint
        assert current_by_name["address"].descriptor.fingerprint != initial_composite.fingerprint
        assert any(change.kind == CHANGE_SCHEMA for change in watcher.pending())

        # The same refreshed tree is what the planner enriches into a row envelope,
        # and what the resnapshot path constructs through RelationDescriptorProvider.
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            provider = RelationDescriptorProvider.from_tables(
                conn, [("app", "p2b_mutable", "cdcflight_app_p2b_mutable")]
            ).descriptors_for
        event = PendingRecord(
            raw=None,
            kind=KIND_DATA,
            topic="app.p2b_mutable",
            nbytes=1,
            schema="app",
            table="p2b_mutable",
            key={"id": 1},
            after={"mood": "sleepy", "address": {"street": "x", "postal_code": "y"}},
        )
        plan = object.__new__(GroupPlan)
        plan.descriptor_provider = provider
        plan._catalog_descriptor_cache = {}
        plan._enrich_descriptors(event)
        assert event.after_descriptors["mood"].enum_labels[-1] == "sleepy"
        assert tuple(
            name for name, _ in event.after_descriptors["address"].composite_fields
        ) == ("street", "postal_code")
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute("ALTER PUBLICATION " + publication + " DROP TABLE app.p2b_mutable")
            conn.execute("DROP TABLE IF EXISTS app.p2b_mutable")
            conn.execute("DROP TYPE IF EXISTS app.p2b_address CASCADE")
            conn.execute("DROP TYPE IF EXISTS app.p2b_mood CASCADE")
