"""Real stock-Debezium FIX ROUND 8 opaque-type probes."""

from __future__ import annotations

import os
import shutil

import psycopg
import pytest
from support.fixtures import Sandbox

from cdc_flight.config import ReplicationConfig

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


@pytest.fixture(scope="module")
def opaque_sandbox(request, tmp_path_factory, postgres_cluster):
    """Give each include mode an independent destination/control history."""
    mode = str(request.param)
    box = Sandbox(
        f"fix8_{mode}",
        tmp_path_factory.mktemp(f"sbx_fix8_{mode}"),
        postgres_cluster,
    )
    try:
        yield box
    finally:
        box.cleanup()
        shutil.rmtree(box.state_dir, ignore_errors=True)

TABLE_TRUE = "app.p2b_opaque_ten_true"
TABLE_FALSE = "app.p2b_opaque_ten_false"
TARGET_TRUE = "cdcflight_app_p2b_opaque_ten_true"
TARGET_FALSE = "cdcflight_app_p2b_opaque_ten_false"
VALUE_COLUMNS = (
    "tsquery_value",
    "jsonpath_value",
    "pg_lsn_value",
    "tsvector_value",
    "xml_value",
    "money_value",
    "inet_value",
    "cidr_value",
    "macaddr_value",
    "macaddr8_value",
)


def _capture(include_unknown: str, table: str) -> dict[str, str]:
    return {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": table.removeprefix("app."),
        "CDC_INCLUDE_UNKNOWN_DATATYPES": include_unknown,
    }


def _create_probe(sandbox, table: str) -> None:
    publication = ReplicationConfig().publication_name
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(
            f"CREATE TABLE {table} ("
            "id integer PRIMARY KEY, "
            "tsquery_value tsquery, "
            "jsonpath_value jsonpath, "
            "pg_lsn_value pg_lsn, "
            "tsvector_value tsvector, "
            "xml_value xml, "
            "money_value money, "
            "inet_value inet, "
            "cidr_value cidr, "
            "macaddr_value macaddr, "
            "macaddr8_value macaddr8)"
        )
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {table}")


def _drop_probe(sandbox, *tables: str) -> None:
    publication = ReplicationConfig().publication_name
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        for table in tables:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {table}")
            conn.execute(f"DROP TABLE IF EXISTS {table}")


def _insert(sandbox, table: str, *, updated: bool = False) -> None:
    if not updated:
        values = (
            "'''fat'' & ''rat'''::tsquery",
            "'$.\"a\"'::jsonpath",
            "'0/16B6A0'::pg_lsn",
            "'''fat'':1 ''rat'':2'::tsvector",
            "'<a>fat</a>'::xml",
            "'12.34'::money",
            "'192.0.2.1'::inet",
            "'192.0.2.0/24'::cidr",
            "'08:00:2b:01:02:03'::macaddr",
            "'08:00:2b:01:02:03:04:05'::macaddr8",
        )
    else:
        values = (
            "'''blue'''::tsquery",
            "'$.\"b\"'::jsonpath",
            "'0/16B6A1'::pg_lsn",
            "'''blue'':1'::tsvector",
            "'<b>blue</b>'::xml",
            "'23.45'::money",
            "'198.51.100.2'::inet",
            "'198.51.100.0/24'::cidr",
            "'08:00:2b:04:05:06'::macaddr",
            "'08:00:2b:04:05:06:07:08'::macaddr8",
        )
    sandbox.sql(
        f"INSERT INTO {table} VALUES (1, {', '.join(values)})"
        if not updated
        else f"UPDATE {table} SET "
        + ", ".join(f"{column} = {value}" for column, value in zip(VALUE_COLUMNS, values, strict=True))
        + " WHERE id = 1"
    )


def _source_canonical(sandbox, table: str) -> tuple:
    # The stock connector's text converters intentionally omit money's locale
    # currency symbol and inet's redundant /32 host mask.  These are still the
    # source type's canonical semantic text forms for this probe.
    expressions = {
        **{column: f"{column}::text" for column in VALUE_COLUMNS},
        "money_value": "money_value::numeric::text",
        "inet_value": "host(inet_value)",
    }
    columns = ", ".join(expressions[column] for column in VALUE_COLUMNS)
    return sandbox.pg_query(f"SELECT {columns} FROM {table} WHERE id = 1")[0]


def _destination_row(sandbox, target: str) -> tuple:
    columns = ", ".join(f'"{column}"' for column in VALUE_COLUMNS)
    return sandbox.duck_query(
        f'SELECT {columns} FROM cdc_raw."{target}" WHERE "id" = 1'
    )[0]


@pytest.mark.parametrize(
    ("opaque_sandbox", "include_unknown"),
    [("true", "true"), ("false", "false")],
    indirect=["opaque_sandbox"],
    ids=["include-true", "include-false"],
)
def test_stock_ten_type_probe_runs_under_both_unknown_modes(
    opaque_sandbox, include_unknown
):
    """The stock connector either lands canonical text or refuses loudly."""
    sandbox = opaque_sandbox
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    sandbox.reseed()
    table = TABLE_TRUE if include_unknown == "true" else TABLE_FALSE
    target = TARGET_TRUE if include_unknown == "true" else TARGET_FALSE
    _create_probe(sandbox, table)
    capture = _capture(include_unknown, table)
    try:
        empty = sandbox.run(reset_state=True, extra_env=capture)
        assert empty["ok"] is True, empty

        _insert(sandbox, table)
        inserted = sandbox.run(
            extra_env=capture,
            expect_success=include_unknown == "true",
        )
        if include_unknown == "true":
            assert inserted["ok"] is True, inserted
            assert _destination_row(sandbox, target) == _source_canonical(sandbox, table)

            _insert(sandbox, table, updated=True)
            updated = sandbox.run(extra_env=capture)
            assert updated["ok"] is True, updated
            assert _destination_row(sandbox, target) == _source_canonical(sandbox, table)
        else:
            assert inserted["ok"] is False, inserted
            output = inserted.get("output", "").lower()
            assert all(name in output for name in ("tsquery", "jsonpath", "pg_lsn")), inserted
            assert sandbox.duck_query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'cdc_raw' AND "
                "table_name = 'cdcflight_app_p2b_opaque_ten_false'"
            ) == []
            assert sandbox.duck_query(
                "SELECT state FROM _cdc_flight.schema_refusals "
                "WHERE source_schema = 'app' AND source_table = 'p2b_opaque_ten_false'"
            ) == [("pending",)]
            assert sandbox.duck_query(
                "SELECT snapshot_state FROM _cdc_flight.table_state "
                "WHERE source_schema = 'app' AND source_table = 'p2b_opaque_ten_false'"
            ) == [("awaiting_snapshot",)]
    finally:
        _drop_probe(sandbox, table)
