"""Round-6 range identity evidence on MotherDuck, with PostgreSQL as oracle."""

from __future__ import annotations

import os

import pytest
from support.motherduck_probe import assert_runtime, connect, scratch_database
from support.range_evidence import postgres_range_equality_classes

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.config import motherduck_token
from cdc_flight.identity_codec import _identity_tree
from cdc_flight.typed_types import SourceTypeDescriptor, mark_canonical_range_text

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


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
            3904, "pg_catalog.int4range", "range", range_subtype=integer
        ),
        "discrete empty spelling": SourceTypeDescriptor(
            3904, "pg_catalog.int4range", "range", range_subtype=integer
        ),
    }


def test_motherduck_range_equality_classes_match_postgres_and_delete_keys(postgres_cluster):
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    assert postgres_cluster.port == int(os.environ["CDC_TEST_PGPORT"])

    evidence = postgres_range_equality_classes(postgres_cluster.dsn)
    descriptors = _range_descriptors()
    for equality_class in evidence:
        descriptor = descriptors[equality_class.name]
        left = mark_canonical_range_text(equality_class.left_text, descriptor)
        right = mark_canonical_range_text(equality_class.right_text, descriptor)
        same_identity = _identity_tree(left, descriptor) == _identity_tree(right, descriptor)
        assert same_identity is equality_class.postgres_equal, equality_class

    with scratch_database(token, "cdc_p2b_range_source_equality") as database:
        con = connect(token, database)
        try:
            assert_runtime(con)
            con.execute("CREATE SCHEMA typed")
            registry = SchemaRegistry(con, "typed")
            for equality_class in (evidence[0], evidence[2], evidence[5]):
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
                assert con.execute(
                    f'SELECT "key" FROM typed."{table_name}"'
                ).fetchone()[0] is not None
                delete_keys(con, table, ("key",), [(right,)])
                assert con.execute(
                    f'SELECT count(*) FROM typed."{table_name}"'
                ).fetchone() == (0,)
        finally:
            con.close()


__all__ = ["test_motherduck_range_equality_classes_match_postgres_and_delete_keys"]
