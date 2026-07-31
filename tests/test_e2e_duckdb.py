"""End-to-end test of the applier path.

    Postgres (native, :15432)
      -> Debezium embedded engine, FULL envelope (snapshot, then streaming)
      -> the transactional applier
      -> local DuckDB

Migrated from the dlt-path baseline when ADR 0001 D1/D10 removed
`dlt.pipeline.run()` from the apply path. The coverage is deliberately the same
or better; what *changed* is the destination semantics, and each change is
asserted here rather than left implicit:

| baseline (dlt, append) | applier |
|---|---|
| every table append-only, so a delete left the old row behind | keyed tables are **current state** (merge on the Debezium message key), keyless tables are append-keyed on `cdcf_event_id` |
| `_dlt_load_id` / `_dlt_id` | `cdcf_commit_id` / `cdcf_event_id`, plus `_cdc_flight.commit_log` |
| arrays exploded into `<table>__tags` child tables | arrays land as DuckDB `JSON` in the row |
| deletes rewritten to a `deleted='true'` row by the SMT | hard delete (rubric 8.1 adds the soft option) |
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
    assert snap["applied_events"] == 20, snap
    # ADR 0001 §3.5 / D7: the snapshot lands in `<table>__cdcf_tmp` and becomes
    # visible through one swap, which is what makes a crash mid-snapshot safe.
    assert snap["snapshot_swaps"] == 6, snap

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
        # Every shadow table was swapped away; none may survive the run.
        assert not [t for t in landed if t.endswith("__cdcf_tmp")], sorted(landed)

        assert _rows(con, dataset, "cdcflight_app_customers", "count(*)")[0][0] == 5
        assert {op for (op,) in _rows(con, dataset, "cdcflight_app_customers", "dbz_op")} == {"r"}

        names = {r[0] for r in _rows(con, dataset, "cdcflight_app_customers", "name")}
        assert "Ada Lovelace" in names

        # Arrays are a JSON column now, not a dlt child table.
        assert "cdcflight_app_customers__tags" not in landed
        tags_type = con.execute(
            "SELECT data_type FROM information_schema.columns WHERE table_schema = ? "
            "AND table_name = 'cdcflight_app_customers' AND column_name = 'tags'",
            [dataset],
        ).fetchone()[0]
        assert tags_type == "JSON", tags_type
    finally:
        con.close()

    # --------------------------------------------------------------- streaming
    changes = generate_changes(scale=1, seed=42)
    assert changes["total"] == 30, changes

    stream = run_pipeline(max_seconds=120, idle_seconds=6)
    assert stream["applied_events"] == 30, stream

    con = duck()
    try:
        # A keyed table is CURRENT STATE: 5 snapshot rows + 3 inserts - 1 delete.
        total, distinct = con.execute(
            f'SELECT count(*), count(DISTINCT id) FROM "{dataset}"."cdcflight_app_customers"'
        ).fetchone()
        assert (total, distinct) == (7, 7), (total, distinct)
        # ... and the row carries the op of the LAST event that touched it.
        cust_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_customers" GROUP BY 1'
            ).fetchall()
        )
        assert cust_ops == {"r": 3, "c": 2, "u": 2}, cust_ops
        # The deleted customer is GONE, not tombstoned (rubric 8.1 adds soft delete).
        assert (
            con.execute(
                f"SELECT count(*) FROM \"{dataset}\".\"cdcflight_app_customers\" "
                "WHERE dbz_op = 'd'"
            ).fetchone()[0]
            == 0
        )

        # A table with NO primary key is an append-only changelog keyed on the
        # synthetic event identity, so every change event is a row - which is
        # exactly what rubric 1.2 needs to be measurable at all.
        sensor_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_sensor_readings" '
                "GROUP BY 1"
            ).fetchall()
        )
        assert sensor_ops == {"r": 4, "c": 6, "u": 4, "d": 2}, sensor_ops
        rows, ids = con.execute(
            f'SELECT count(*), count(DISTINCT cdcf_event_id) '
            f'FROM "{dataset}"."cdcflight_app_sensor_readings"'
        ).fetchone()
        assert rows == ids == 16, (rows, ids)

        # Partitioned table arrives as one logical table (publish_via_partition_root).
        audit_ops = dict(
            con.execute(
                f'SELECT dbz_op, count(*) FROM "{dataset}"."cdcflight_app_audit_log" GROUP BY 1'
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


def test_commit_log_accounts_for_the_whole_run(fresh_seed, run_pipeline, generate_changes, duck):
    """`_cdc_flight.commit_log` is the audit trail rubric 1.7 and 6.1 rest on."""
    run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    generate_changes(scale=1, seed=11)
    run_pipeline(max_seconds=120, idle_seconds=6)

    con = duck()
    try:
        rows = con.execute(
            "SELECT commit_id, trigger, unit_count, event_count, last_lsn "
            "FROM _cdc_flight.commit_log ORDER BY commit_id"
        ).fetchall()
        assert rows, "no commit groups were recorded"
        # commit ids are dense and increasing; LSNs never go backwards.
        assert [r[0] for r in rows] == list(range(1, len(rows) + 1))
        lsns = [r[4] for r in rows if r[4] is not None]
        assert lsns == sorted(lsns), lsns
        assert rows[0][1] == "snapshot_chunk"
        assert sum(r[3] for r in rows) == 50  # 20 snapshot + 30 streamed

        # The resume point is a single row and agrees with the last commit group.
        offsets = con.execute(
            "SELECT commit_id, last_lsn FROM _cdc_flight.debezium_offsets"
        ).fetchall()
        assert len(offsets) == 1
        assert offsets[0][0] == rows[-1][0]
    finally:
        con.close()


def test_documented_type_gaps(fresh_seed, run_pipeline, generate_changes, duck, dataset):
    """Pin the *known* type-mapping weaknesses so rubric 2.4 can prove it closed them.

    These are deliberately still open: ADR 0001 D5 lands the full envelope here,
    but keeps `value.converter.schemas.enable=false`, so the Connect schema that
    carries the semantic type (`io.debezium.time.Date`,
    `org.apache.kafka.connect.data.Decimal`) is still not available. Turning it on
    inflates every payload 3-5x, which ADR §5.1 flags as an unmeasured throughput
    risk owned by rubric 5.3. If one of these starts failing, something improved.
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

        types = dict(
            con.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = 'cdcflight_app_wide_types'",
                [dataset],
            ).fetchall()
        )
        # GAP (rubric 2.4): numeric arrives base64-encoded, dates as raw integers.
        assert types["col_numeric"] == "VARCHAR", types["col_numeric"]
        assert types["col_date"] == "BIGINT"
        assert types["col_interval"] == "BIGINT"
        # GAP (rubric 2.4): timestamptz arrives as an ISO string. The baseline
        # mapped it natively only because dlt *inferred* from the value; the
        # applier deliberately does not infer, so this is a knowing regression
        # that 2.4 fixes properly from the Connect schema.
        assert types["col_timestamptz"] == "VARCHAR"
        # IMPROVEMENT over the baseline: the all-NaN numeric column no longer
        # disappears (dlt dropped it), and arrays are native JSON.
        assert "col_numeric_nan" in types
        assert types["col_int_array"] == "JSON"
    finally:
        con.close()


def test_second_run_is_incremental(fresh_seed, run_pipeline, duck, dataset):
    """Offsets persist: a second run with no source changes loads nothing new.

    This assertion is about *our* offsets, but its literal form ("the second run
    saw zero change events") is a statement about the whole shared cluster, so
    any other writer on :15432 breaks it with a baffling message. Sessions are
    serialised by the `exclusive_source` lock; this fingerprint check catches
    anything that still slips past and says so plainly.
    """
    from conftest import source_fingerprint

    first = run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    assert first["records"] == 20

    before = source_fingerprint(fresh_seed)
    second = run_pipeline(max_seconds=40, idle_seconds=6)
    after = source_fingerprint(fresh_seed)

    if before != after:
        pytest.fail(
            "the shared Postgres source was modified by something other than this "
            "test while it ran, so 'a second run loads nothing new' cannot be "
            f"evaluated.\n  before: {before}\n  after:  {after}\n  run: {second}"
        )
    assert second["applied_events"] == 0, second
    assert second["reconciliation"] == "resume", second

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
