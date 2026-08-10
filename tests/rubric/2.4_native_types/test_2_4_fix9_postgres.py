"""Real stock-Debezium FIX ROUND 9 opaque-type probes on local DuckDB."""

from __future__ import annotations

import os

import psycopg
import pytest
from support.fix9_opaque import (
    EXACT_CORPUS,
    STOCK_UNLOSSLESS_TEXT_TYPES,
    capture_environment,
    create_corpus,
    drop_corpus,
    populate_corpus,
    source_connector_text,
)
from support.fixtures import Sandbox

from cdc_flight.catalog_descriptors import CatalogDescriptorReader
from cdc_flight.typed_types import InvalidTypedValue, UnsupportedType, adapt_value, native_type

# This is a source-generated correctness guard, not a fault-injection timing
# proof. Keep it in the default lane so it cannot starve the slow lane's
# six-worker JVM/replication stress pool.
pytestmark = [pytest.mark.e2e]


def test_postgresql_generated_opaque_corpus_is_lossless_or_refused_on_local_duckdb(
    tmp_path, postgres_cluster
):
    sandbox = Sandbox("fix9_pg_generated", tmp_path / "sandbox", postgres_cluster)
    try:
        assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
        sandbox.reseed()
        tables = create_corpus(sandbox, EXACT_CORPUS)
        capture = capture_environment(tables)
        try:
            baseline = sandbox.run(reset_state=True, extra_env=capture, max_seconds=180)
            assert baseline["ok"] is True, baseline
            populate_corpus(sandbox)
            # Values were generated and rendered by PostgreSQL above; no expected
            # row value is hand-written below.
            # The non-lossless stock adapters are separate source tables/rows.  A
            # run reaches the next durable refusal only after the previous table is
            # quarantined, so four attempts are expected: money, inet, int2vector,
            # then the healthy remainder can advance and finish.
            for attempt in range(4):
                result = sandbox.run(
                    extra_env=capture,
                    max_seconds=180,
                    expect_success=attempt == 3,
                )
                if attempt < 3:
                    assert result["ok"] is False, result
                else:
                    assert result["ok"] is True, result
            for name in (*STOCK_UNLOSSLESS_TEXT_TYPES, "int2vector"):
                assert sandbox.duck_query(
                    "SELECT state FROM _cdc_flight.schema_refusals "
                    "WHERE source_schema='app' AND source_table=?",
                    [f"p2b_r9_{name}"],
                ) == [("quarantined",)]
                assert sandbox.duck_query(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='cdc_raw' AND table_name=?",
                    [f"cdcflight_app_p2b_r9_{name}"],
                ) == []
            for name in EXACT_CORPUS:
                if name == "int2vector" or name in STOCK_UNLOSSLESS_TEXT_TYPES:
                    continue
                source = source_connector_text(sandbox, name)
                destination = sandbox.duck_query(
                    f'SELECT "id", "value" FROM cdc_raw."cdcflight_app_p2b_r9_{name}" '
                    'ORDER BY "id"'
                )
                assert destination == source, name
            third = sandbox.run(extra_env=capture, max_seconds=180)
            assert third["ok"] is True, third
        finally:
            drop_corpus(sandbox, EXACT_CORPUS)
    finally:
        sandbox.cleanup()


def test_source_catalog_sweep_has_an_explicit_decision_for_every_builtin_type(
    postgres_cluster,
):
    """Every PG18 builtin base type has a direct native/refusal decision.

    Array OIDs are covered recursively through their element descriptors.  The
    non-array catalog rows are queried from PostgreSQL rather than maintained as
    a hand-written list of the review's named examples.
    """
    expected_allowed = {
        "bool", "bytea", "char", "name", "int8", "int2", "int2vector", "int4",
        "text", "oid", "xid", "json", "xml", "point", "cidr", "float4", "float8",
        "macaddr8", "money", "macaddr", "inet", "bpchar", "varchar", "date", "time",
        "timestamp", "timestamptz", "interval", "timetz", "bit", "varbit", "numeric",
        "uuid", "pg_lsn", "tsvector", "tsquery", "jsonb", "jsonpath",
        "int4multirange", "nummultirange", "tsmultirange", "tstzmultirange",
        "datemultirange", "int8multirange",
    }
    opaque_allowed = {
        "int2vector", "xml", "cidr", "macaddr8", "money", "macaddr", "inet",
        "pg_lsn", "tsvector", "tsquery", "jsonpath", "int4multirange",
        "nummultirange", "tsmultirange", "tstzmultirange", "datemultirange",
        "int8multirange",
    }
    value_refused = {"money": "12.34", "inet": "192.0.2.1"}
    with psycopg.connect(postgres_cluster.dsn, autocommit=True) as con:
        rows = con.execute(
            "SELECT t.oid::bigint, t.typname FROM pg_type t "
            "JOIN pg_namespace n ON n.oid=t.typnamespace "
            "WHERE n.nspname='pg_catalog' AND t.typtype IN ('b', 'm') "
            "AND t.typname NOT LIKE '\\_%' ESCAPE '\\' ORDER BY t.oid",
        ).fetchall()
        descriptors = CatalogDescriptorReader(con).resolve([oid for oid, _name in rows])
    names = {name for _oid, name in rows}
    assert names == expected_allowed | {
        "regproc", "tid", "cid", "oidvector", "pg_node_tree", "lseg", "path", "box",
        "polygon", "line", "circle", "aclitem", "refcursor", "regprocedure", "regoper",
        "regoperator", "regclass", "regtype", "txid_snapshot", "pg_ndistinct",
        "pg_dependencies", "gtsvector", "regconfig", "regdictionary", "regnamespace",
        "regrole", "regcollation", "pg_brin_bloom_summary", "pg_brin_minmax_multi_summary",
        "pg_mcv_list", "pg_snapshot", "xid8",
    }
    for oid, name in rows:
        descriptor = descriptors[oid]
        if name in expected_allowed:
            resolved = native_type(descriptor)
            assert resolved.sql
            if name in opaque_allowed:
                assert resolved.sql == "VARCHAR", name
            if name in value_refused:
                with pytest.raises(InvalidTypedValue):
                    adapt_value(value_refused[name], resolved)
        else:
            with pytest.raises(UnsupportedType):
                native_type(descriptor)
