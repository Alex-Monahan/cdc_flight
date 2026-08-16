"""FIX ROUND 14 scalar temporal infinity regression.

This is deliberately a real stock-Debezium/PostgreSQL sequence.  The temporal
columns are scalar values (not range endpoints), and every source transaction
also writes a healthy peer so a table-local failure cannot hide behind a failed
whole-transaction apply.
"""

from __future__ import annotations

import math
import os
import shutil

import psycopg
import pytest

from cdc_flight.config import ReplicationConfig

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_scalar_temporal_infinity_round_trips_and_does_not_starve_peer(sandbox):
    """Five scalar infinity transactions must deliver both signs and the peer."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = ReplicationConfig().publication_name
    temporal = "app.scalar_temporal_infinity_probe"
    peer = "app.scalar_temporal_infinity_peer"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "scalar_temporal_infinity_probe,scalar_temporal_infinity_peer",
    }
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {temporal}")
        conn.execute(f"DROP TABLE IF EXISTS {peer}")
        conn.execute(
            f"CREATE TABLE {temporal} ("
            "id integer PRIMARY KEY, "
            "tsz timestamptz NOT NULL, "
            "ts timestamp NOT NULL, "
            "d date NOT NULL, "
            "note text NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE {peer} (id integer PRIMARY KEY, note text NOT NULL)"
        )
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {temporal}, {peer}")
        # Put both signs in the initial image as well as in later DML.  This is
        # the original regression shape: the scalar value must survive the real
        # snapshot/backfill Arrow materializer before streaming is even started.
        conn.execute(
            f"INSERT INTO {temporal} VALUES "
            "(-1, '-infinity'::timestamptz, '-infinity'::timestamp, "
            "'-infinity'::date, 'snapshot-negative'), "
            "(0, 'infinity'::timestamptz, 'infinity'::timestamp, "
            "'infinity'::date, 'snapshot-positive')"
        )

    try:
        empty = sandbox.run(reset_state=True, extra_env=capture)
        assert empty["ok"] is True, empty

        for ident in range(1, 6):
            sign = "infinity" if ident % 2 else "-infinity"
            sandbox.sql(
                [
                    (
                        f"INSERT INTO {temporal} VALUES "
                        f"({ident}, '{sign}'::timestamptz, '{sign}'::timestamp, "
                        f"'{sign}'::date, 'temporal-{ident}')"
                    ),
                    f"INSERT INTO {peer} VALUES ({ident}, 'peer-{ident}')",
                ],
                one_transaction=True,
            )
            result = sandbox.run(
                max_seconds=240,
                timeout=360,
                extra_env=capture,
            )
            assert result["ok"] is True, result

        assert sandbox.duck_query(
            'SELECT "id", CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), '
            'CAST("d" AS VARCHAR), "note" '
            'FROM cdc_raw.cdcflight_app_scalar_temporal_infinity_probe ORDER BY 1'
        ) == [
            (-1, "-infinity", "-infinity", "-infinity", "snapshot-negative"),
            (0, "infinity", "infinity", "infinity", "snapshot-positive"),
            (1, "infinity", "infinity", "infinity", "temporal-1"),
            (2, "-infinity", "-infinity", "-infinity", "temporal-2"),
            (3, "infinity", "infinity", "infinity", "temporal-3"),
            (4, "-infinity", "-infinity", "-infinity", "temporal-4"),
            (5, "infinity", "infinity", "infinity", "temporal-5"),
        ]
        assert sandbox.duck_query(
            'SELECT "id", "note" FROM cdc_raw.cdcflight_app_scalar_temporal_infinity_peer '
            'ORDER BY 1'
        ) == [(ident, f"peer-{ident}") for ident in range(1, 6)]
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {temporal}, {peer}")
            conn.execute(f"DROP TABLE IF EXISTS {temporal}")
            conn.execute(f"DROP TABLE IF EXISTS {peer}")


def test_scalar_special_value_matrix_reaches_the_real_materializer(sandbox):
    """The complete 2.4 five-band matrix is scalar and source-backed."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = ReplicationConfig().publication_name
    table = "app.fix14_scalar_specials"
    peer = "app.fix14_scalar_specials_peer"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "fix14_scalar_specials,fix14_scalar_specials_peer",
    }
    # This is an independent source-backed scenario in the same module as the
    # temporal probe.  The preceding probe drops its temporary relations after its
    # final run; consuming those DROP events would make the test needlessly depend
    # on another pipeline invocation.  Start this scenario with a fresh destination
    # and state directory so no prior catalog epoch can become an unresolved
    # lifecycle obligation.
    sandbox.drop_slot()
    if sandbox.duckdb_path.exists():
        sandbox.duckdb_path.unlink()
    shutil.rmtree(sandbox.state_dir, ignore_errors=True)
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"DROP TABLE IF EXISTS {peer}")
        conn.execute(
            f"CREATE TABLE {table} ("
            "id integer PRIMARY KEY, real_value real, double_value double precision, "
            "numeric_value numeric, tsz timestamptz, ts timestamp, date_value date, note text)"
        )
        conn.execute(f"CREATE TABLE {peer} (id integer PRIMARY KEY, note text)")
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {table}, {peer}")
        conn.execute(
            f"INSERT INTO {table} VALUES "
            "(1, 'NaN'::real, 'NaN'::double precision, 'NaN'::numeric, NULL, NULL, NULL, 'nan-snapshot'), "
            "(2, 'Infinity'::real, 'Infinity'::double precision, 'Infinity'::numeric, "
            "'infinity'::timestamptz, 'infinity'::timestamp, 'infinity'::date, 'positive-snapshot'), "
            "(3, '-Infinity'::real, '-Infinity'::double precision, '-Infinity'::numeric, "
            "'-infinity'::timestamptz, '-infinity'::timestamp, '-infinity'::date, 'negative-snapshot')"
        )
    try:
        baseline = sandbox.run(reset_state=True, extra_env=capture, max_seconds=240, timeout=360)
        assert baseline["ok"] is True, baseline
        sandbox.sql(
            [
                f"INSERT INTO {table} VALUES "
                "(4, 'NaN'::real, 'NaN'::double precision, 'NaN'::numeric, NULL, NULL, NULL, 'nan-stream'), "
                "(5, 'Infinity'::real, 'Infinity'::double precision, 'Infinity'::numeric, "
                "'infinity'::timestamptz, 'infinity'::timestamp, 'infinity'::date, 'positive-stream'), "
                "(6, '-Infinity'::real, '-Infinity'::double precision, '-Infinity'::numeric, "
                "'-infinity'::timestamptz, '-infinity'::timestamp, '-infinity'::date, 'negative-stream')",
                f"INSERT INTO {peer} SELECT id, 'peer-' || id FROM generate_series(4, 6) id",
            ],
            one_transaction=True,
        )
        streamed = sandbox.run(extra_env=capture, max_seconds=240, timeout=360)
        assert streamed["ok"] is True, streamed
        rows = sandbox.duck_query(
            "SELECT id, isnan(real_value), isinf(real_value), real_value < 0, "
            "isnan(double_value), isinf(double_value), double_value < 0, "
            "numeric_value.special, CAST(tsz AS VARCHAR), CAST(ts AS VARCHAR), "
            "CAST(date_value AS VARCHAR), note "
            "FROM cdc_raw.cdcflight_app_fix14_scalar_specials ORDER BY id"
        )
        assert [row[:7] + row[8:] for row in rows] == [
            (1, True, False, False, True, False, False, None, None, None, "nan-snapshot"),
            (2, False, True, False, False, True, False, "infinity", "infinity", "infinity", "positive-snapshot"),
            (3, False, True, True, False, True, True, "-infinity", "-infinity", "-infinity", "negative-snapshot"),
            (4, True, False, False, True, False, False, None, None, None, "nan-stream"),
            (5, False, True, False, False, True, False, "infinity", "infinity", "infinity", "positive-stream"),
            (6, False, True, True, False, True, True, "-infinity", "-infinity", "-infinity", "negative-stream"),
        ]
        assert math.isnan(rows[0][7]) and math.isnan(rows[3][7])
        assert math.isinf(rows[1][7]) and rows[1][7] > 0
        assert math.isinf(rows[2][7]) and rows[2][7] < 0
        assert math.isinf(rows[4][7]) and rows[4][7] > 0
        assert math.isinf(rows[5][7]) and rows[5][7] < 0
        assert sandbox.duck_query(
            "SELECT id, note FROM cdc_raw.cdcflight_app_fix14_scalar_specials_peer ORDER BY id"
        ) == [(4, "peer-4"), (5, "peer-5"), (6, "peer-6")]
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {table}, {peer}")
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(f"DROP TABLE IF EXISTS {peer}")
