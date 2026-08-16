"""Round-6 range identity evidence against PostgreSQL's own operator."""

from __future__ import annotations

import os

import duckdb
import pytest
from support.range_evidence import postgres_range_equality_classes

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.destination import DUCKDB_CONNECT_CONFIG
from cdc_flight.identity_codec import _identity_tree
from cdc_flight.typed_types import (
    SourceTypeDescriptor,
    mark_canonical_range_text,
)

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def _range_descriptors() -> dict[str, SourceTypeDescriptor]:
    timestamp = _source("timestamp", 1114)
    numeric = _source("numeric", 1700)
    integer = _source("int4", 23)
    numrange = SourceTypeDescriptor(
        3906, "pg_catalog.numrange", "range", range_subtype=numeric
    )
    return {
        "timestamp special endpoint": SourceTypeDescriptor(
            3908, "pg_catalog.tsrange", "range", range_subtype=timestamp
        ),
        "timestamp special versus unbounded": SourceTypeDescriptor(
            3908, "pg_catalog.tsrange", "range", range_subtype=timestamp
        ),
        "continuous multirange reordered and merged": SourceTypeDescriptor(
            4532, "pg_catalog.nummultirange", "multirange", range_subtype=numrange
        ),
        "continuous multirange overlapping merge": SourceTypeDescriptor(
            4532, "pg_catalog.nummultirange", "multirange", range_subtype=numrange
        ),
        "continuous multirange adjacent merge": SourceTypeDescriptor(
            4532, "pg_catalog.nummultirange", "multirange", range_subtype=numrange
        ),
        "discrete range closed versus canonical": SourceTypeDescriptor(
            3904,
            "pg_catalog.int4range",
            "range",
            range_subtype=integer,
        ),
        "discrete empty spelling": SourceTypeDescriptor(
            3904,
            "pg_catalog.int4range",
            "range",
            range_subtype=integer,
        ),
    }


def test_real_postgres_equality_classes_match_local_identity_and_storage(postgres_cluster):
    """The source operator supplies the class; local storage only round-trips it."""

    assert postgres_cluster.port == int(os.environ["CDC_TEST_PGPORT"])
    evidence = postgres_range_equality_classes(postgres_cluster.dsn)
    descriptors = _range_descriptors()

    for equality_class in evidence:
        descriptor = descriptors[equality_class.name]
        left = mark_canonical_range_text(equality_class.left_text, descriptor)
        right = mark_canonical_range_text(equality_class.right_text, descriptor)
        same_identity = _identity_tree(left, descriptor) == _identity_tree(right, descriptor)
        assert same_identity is equality_class.postgres_equal, equality_class

    con = duckdb.connect(":memory:", config=DUCKDB_CONNECT_CONFIG)
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        cases = (
            evidence[0],
            evidence[2],
            evidence[5],
        )
        for equality_class in cases:
            descriptor = descriptors[equality_class.name]
            table_name = equality_class.name.replace(" ", "_")
            table, _ = registry.ensure_typed(
                table_name,
                columns={"key": descriptor, "payload": _source("text", 25)},
                key_columns=("key",),
            )
            left = mark_canonical_range_text(equality_class.left_text, descriptor)
            right = mark_canonical_range_text(equality_class.right_text, descriptor)
            insert_rows(con, table, ["key", "payload"], [[left, "kept"]])
            stored = con.execute(
                f'SELECT "key" FROM typed."{table_name}"'
            ).fetchone()[0]
            assert stored is not None
            delete_keys(con, table, ("key",), [(right,)])
            assert con.execute(
                f'SELECT count(*) FROM typed."{table_name}"'
            ).fetchone() == (0,)
    finally:
        con.close()


__all__ = ["test_real_postgres_equality_classes_match_local_identity_and_storage"]
