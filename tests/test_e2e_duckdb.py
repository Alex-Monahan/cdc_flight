"""End-to-end baseline test.

    Postgres (native, :15432)
      -> Debezium embedded engine (snapshot, then streaming)
      -> dlt
      -> local DuckDB

Runs two pipeline invocations against one Postgres cluster:
1. snapshot of the seeded rows,
2. streaming of a generated wave of inserts / updates / deletes.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.e2e

TABLES = [
    "cdcflight_app_customers",
    "cdcflight_app_orders",
    "cdcflight_app_sensor_readings",
    "cdcflight_app_documents",
    "cdcflight_app_wide_types",
    "cdcflight_app_audit_log",
]

TOAST_PLACEHOLDER = "__debezium_unavailable_value"


def _rows(con, dataset, table, cols="*", where="", order=""):
    sql = f'SELECT {cols} FROM "{dataset}"."{table}"'
    if where:
        sql += f" WHERE {where}"
    if order:
        sql += f" ORDER BY {order}"
    return con.execute(sql).fetchall()


def test_baseline_end_to_end(fresh_seed, run_pipeline, generate_changes, duck, dataset):
    # ---------------------------------------------------------------- snapshot
    snap = run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    assert snap["stop_reason"] in {"idle", "engine_finished"}, snap
    assert snap["records"] == 20, snap  # 5+5+4+2+1+3 seeded rows

    con = duck()
    try:
        landed = {
            t
            for (t,) in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [dataset],
            ).fetchall()
        }
        assert set(TABLES) <= landed, sorted(landed)

        # Snapshot rows are op='r'.
        assert _rows(con, dataset, "cdcflight_app_customers", "count(*)")[0][0] == 5
        assert {op for (op,) in _rows(con, dataset, "cdcflight_app_customers", "dbz_op")} == {"r"}

        # A real value round-trips.
        names = {r[0] for r in _rows(con, dataset, "cdcflight_app_customers", "name")}
        assert "Ada Lovelace" in names

        # Nested/array columns become dlt child tables in the baseline.
        assert "cdcflight_app_customers__tags" in landed
    finally:
        con.close()

    # --------------------------------------------------------------- streaming
    changes = generate_changes(scale=1, seed=42)
    assert changes["total"] == 30, changes

    stream = run_pipeline(max_seconds=120, idle_seconds=6)
    assert stream["records"] == 30, stream

    con = duck()
    try:
        # Every operation type is represented.
        cust_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_customers" '
                "GROUP BY 1"
            ).fetchall()
        )
        assert cust_ops == {"r": 5, "c": 3, "u": 2, "d": 1}, cust_ops

        # Deletes are rewritten as rows flagged `deleted`, not dropped.
        deletes = _rows(
            con, dataset, "cdcflight_app_customers", "id, deleted", where="dbz_op = 'd'"
        )
        assert len(deletes) == 1
        assert deletes[0][1] == "true"

        # A table with NO primary key still yields updates and deletes because
        # REPLICA IDENTITY is FULL.
        sensor_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_sensor_readings" '
                "GROUP BY 1"
            ).fetchall()
        )
        assert sensor_ops == {"r": 4, "c": 6, "u": 4, "d": 2}, sensor_ops

        # Partitioned table arrives as one logical table (publish_via_partition_root).
        audit_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_audit_log" '
                "GROUP BY 1"
            ).fetchall()
        )
        assert audit_ops == {"r": 3, "c": 2}, audit_ops

        # CDC metadata is present and usable.
        meta = con.execute(
            "SELECT dbz_lsn, dbz_tx_id, dbz_source_ts_ms, dbz_schema, dbz_table "
            f'FROM "{dataset}"."cdcflight_app_customers" WHERE dbz_op = \'c\' LIMIT 1'
        ).fetchone()
        assert meta[0] > 0 and meta[1] > 0 and meta[2] > 0
        assert meta[3] == "app" and meta[4] == "customers"
    finally:
        con.close()


def test_documented_baseline_gaps(fresh_seed, run_pipeline, generate_changes, duck, dataset):
    """Pin the *known* baseline weaknesses so later phases can prove they closed them.

    These assertions encode current (bad) behaviour on purpose - if one starts
    failing, something improved and the rubric status should be updated.
    """
    run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    generate_changes(scale=1, seed=7)
    run_pipeline(max_seconds=120, idle_seconds=6)

    con = duck()
    try:
        # GAP (rubric 2.6): unchanged TOAST columns arrive as a placeholder string.
        toast = con.execute(
            f'SELECT count(*) FROM "{dataset}"."cdcflight_app_documents" '
            f"WHERE body = '{TOAST_PLACEHOLDER}'"
        ).fetchone()[0]
        assert toast >= 1, "expected at least one unchanged-TOAST placeholder"

        # GAP (rubric 2.4): numeric arrives base64-encoded, not as DECIMAL.
        dtype = con.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = 'cdcflight_app_customers' "
            "AND column_name = 'lifetime_value'",
            [dataset],
        ).fetchone()[0]
        assert dtype == "VARCHAR", dtype

        # GAP (rubric 2.4): date/time/interval arrive as raw integers.
        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = 'cdcflight_app_wide_types'",
                [dataset],
            ).fetchall()
        )
        assert types["col_date"] == "BIGINT"
        assert types["col_interval"] == "BIGINT"
        # ... while timestamptz *is* mapped natively.
        assert types["col_timestamptz"].startswith("TIMESTAMP")

        # GAP (rubric 2.4): an unconstrained numeric holding NaN is dropped entirely.
        assert "col_numeric_nan" not in types

        # GAP (rubric 8.1/1.x): the destination is an append-only changelog, so a
        # deleted row is still present as an earlier insert row.
        total = con.execute(
            f'SELECT count(*) FROM "{dataset}"."cdcflight_app_customers"'
        ).fetchone()[0]
        distinct_ids = con.execute(
            f'SELECT count(DISTINCT id) FROM "{dataset}"."cdcflight_app_customers"'
        ).fetchone()[0]
        assert total > distinct_ids, "append-only changelog expected"
    finally:
        con.close()


def test_second_run_is_incremental(fresh_seed, run_pipeline, duck, dataset):
    """Offsets persist: a second run with no source changes loads nothing new."""
    first = run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    assert first["records"] == 20

    second = run_pipeline(max_seconds=40, idle_seconds=6)
    assert second["records"] == 0, second

    con = duck()
    try:
        assert (
            con.execute(
                f'SELECT count(*) FROM "{dataset}"."cdcflight_app_customers"'
            ).fetchone()[0]
            == 5
        )
    finally:
        con.close()
