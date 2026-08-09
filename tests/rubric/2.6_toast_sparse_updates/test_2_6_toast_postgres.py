"""Real PostgreSQL admission/proof checks for rubric 2.6.

The Debezium wire scenarios live behind the existing slow/e2e fixtures.  These
tests keep the catalog and PostgreSQL NUL invariants independently executable in
the slow lane, using only the project-local port selected by CDC_TEST_PGPORT.
"""

from __future__ import annotations

import os

import psycopg
import pytest

from cdc_flight.catalog import CatalogWatcher
from cdc_flight.catalog_poll import _ensure_toast_policies
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.debezium_props import UNAVAILABLE_VALUE_PLACEHOLDER
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _dsn():
    return (
        f"host=127.0.0.1 port={os.environ.get('CDC_TEST_PGPORT', '15434')} "
        "dbname=cdc_source user=postgres password=postgres"
    )


def test_postgres_event_before_full_activation_is_fenced_from_current_policy(sandbox):
    """A real pre-FULL bytea event cannot be admitted as an explicit NULL."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = "cdc_flight_pub"
    qualified = "app.p2b_toast_race"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS app.p2b_toast_race")
        con.execute(
            "CREATE TABLE app.p2b_toast_race "
            "(id integer PRIMARY KEY, payload bytea)"
        )
        con.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")
        con.execute(
            "INSERT INTO app.p2b_toast_race VALUES (1, decode('00ff','hex'))"
        )
        con.execute(
            "UPDATE app.p2b_toast_race SET payload = decode('010203','hex') WHERE id = 1"
        )
        event_lsn = int(
            con.execute(
                "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
            ).fetchone()[0]
        )
        relation_oid = int(
            con.execute(
                "SELECT c.oid FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='app' AND c.relname='p2b_toast_race'"
            ).fetchone()[0]
        )
        relation = SourceRelation(
            schema="app",
            table="p2b_toast_race",
            oid=relation_oid,
            published=True,
            replica_identity="d",
            columns=(
                SourceColumn(
                    attnum=1,
                    name="id",
                    type_oid=23,
                    type_name="integer",
                    descriptor=SourceTypeDescriptor(23, "pg_catalog.int4", "int4"),
                    attstorage="p",
                ),
                SourceColumn(
                    attnum=2,
                    name="payload",
                    type_oid=17,
                    type_name="bytea",
                    descriptor=SourceTypeDescriptor(17, "pg_catalog.bytea", "bytea"),
                    attstorage="x",
                ),
            ),
        )
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
        observed = _ensure_toast_policies(
            watcher,
            con,
            {qualified: relation},
            activation_lsn=event_lsn + 1,
        )
        policy = observed[qualified].toast_policy
        assert policy.full_activation_lsn == event_lsn + 1
        assert policy.accepts_event(event_lsn) is False
        assert policy.accepts_event(event_lsn + 1) is True
        assert con.execute(
            "SELECT payload FROM app.p2b_toast_race WHERE id = 1"
        ).fetchone()[0] == b"\x01\x02\x03"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
        con.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
        con.execute("DROP TABLE IF EXISTS app.p2b_toast_race")


def test_postgres_rejects_nul_for_structural_string_types_and_accepts_bytea():
    with psycopg.connect(_dsn(), autocommit=True) as con:
        cases = (
            "text", "varchar", "character(4)", 'pg_catalog."char"',
            "json", "jsonb", "xml",
        )
        for cast in cases:
            with pytest.raises(psycopg.Error):
                con.execute(f"SELECT %s::{cast}", ("prefix\x00suffix",))
        assert con.execute("SELECT decode('00ff','hex')::bytea").fetchone()[0] == b"\x00\xff"


def test_catalog_query_exposes_column_storage_facts():
    with psycopg.connect(_dsn(), autocommit=True) as con:
        rows = con.execute(
            "SELECT a.attname, a.attstorage, format_type(a.atttypid,a.atttypmod) "
            "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='app' AND c.relname='documents' AND a.attnum > 0 "
            "AND NOT a.attisdropped ORDER BY a.attnum"
        ).fetchall()
    assert rows
    assert any(name == "body" and storage != "p" for name, storage, _ in rows)


def test_structural_marker_is_the_configured_debezium_value():
    assert UNAVAILABLE_VALUE_PLACEHOLDER == "hex:00"
