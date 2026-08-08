"""Real PostgreSQL admission/proof checks for rubric 2.6.

The Debezium wire scenarios live behind the existing slow/e2e fixtures.  These
tests keep the catalog and PostgreSQL NUL invariants independently executable in
the slow lane, using only the project-local port selected by CDC_TEST_PGPORT.
"""

from __future__ import annotations

import os

import psycopg
import pytest

from cdc_flight.debezium_props import UNAVAILABLE_VALUE_PLACEHOLDER

pytestmark = pytest.mark.slow


def _dsn():
    return (
        f"host=127.0.0.1 port={os.environ.get('CDC_TEST_PGPORT', '15432')} "
        "dbname=cdc_source user=postgres password=postgres"
    )


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
