"""Local DuckDB proofs for scalar temporal special values and path coverage."""

from __future__ import annotations

import math

import pytest
from support.applier_lab import Lab, data, end, snap

from cdc_flight import typed_materialization
from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.identity_codec import _identity_tree
from cdc_flight.typed_types import PostgresInfinity, SourceTypeDescriptor

INT4 = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
TEXT = SourceTypeDescriptor(25, "pg_catalog.text", "text")
DATE = SourceTypeDescriptor(1082, "pg_catalog.date", "date")
TIMESTAMP = SourceTypeDescriptor(1114, "pg_catalog.timestamp", "timestamp")
TIMESTAMPTZ = SourceTypeDescriptor(1184, "pg_catalog.timestamptz", "timestamptz")
FLOAT4 = SourceTypeDescriptor(700, "pg_catalog.float4", "float4")
FLOAT8 = SourceTypeDescriptor(701, "pg_catalog.float8", "float8")
NUMERIC = SourceTypeDescriptor(
    1700, "pg_catalog.numeric", "numeric", precision=30, scale=10
)
NUMRANGE = SourceTypeDescriptor(
    3906, "pg_catalog.numrange", "range", range_subtype=NUMERIC
)
NUMMULTIRANGE = SourceTypeDescriptor(
    4532, "pg_catalog.nummultirange", "multirange", range_subtype=NUMRANGE
)


def _temporal_descriptors():
    return {
        "id": INT4,
        "tsz": TIMESTAMPTZ,
        "ts": TIMESTAMP,
        "d": DATE,
        "note": TEXT,
    }


def _temporal_event(*, txn: str, order: int, lsn: int, ident: int, sign: str):
    value = PostgresInfinity(sign == "infinity")
    event = data(
        txn,
        order,
        lsn,
        table="customers",
        key={"id": ident},
        after={"id": ident, "tsz": value, "ts": value, "d": value, "note": sign},
    )
    event.key_descriptors = {"id": INT4}
    event.after_descriptors = _temporal_descriptors()
    return event


def test_scalar_temporal_infinities_round_trip_through_local_native_materializer():
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "temporal_specials",
            columns={"id": INT4, "tsz": TIMESTAMPTZ, "ts": TIMESTAMP, "d": DATE},
            key_columns=("id",),
        )
        insert_rows(
            con,
            registry.get("temporal_specials"),
            ["id", "tsz", "ts", "d"],
            [
                [1, PostgresInfinity(True), PostgresInfinity(True), PostgresInfinity(True)],
                [2, PostgresInfinity(False), PostgresInfinity(False), PostgresInfinity(False)],
            ],
        )
        assert con.execute(
            'SELECT id, CAST(tsz AS VARCHAR), CAST(ts AS VARCHAR), CAST(d AS VARCHAR) '
            'FROM typed.temporal_specials ORDER BY id'
        ).fetchall() == [
            (1, "infinity", "infinity", "infinity"),
            (2, "-infinity", "-infinity", "-infinity"),
        ]
    finally:
        con.close()


def test_scalar_special_value_five_band_is_native_for_every_supported_type():
    """NaN/+infinity/-infinity all cross the real local materializer.

    Temporal types have no NaN spelling; their two infinity cells are asserted
    separately.  This is intentionally scalar coverage, not the existing range
    endpoint/text corpus.
    """
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "special_matrix",
            columns={
                "id": INT4,
                "real_value": FLOAT4,
                "double_value": FLOAT8,
                "numeric_value": NUMERIC,
                "tsz": TIMESTAMPTZ,
                "ts": TIMESTAMP,
                "d": DATE,
            },
            key_columns=("id",),
        )
        insert_rows(
            con,
            registry.get("special_matrix"),
            ["id", "real_value", "double_value", "numeric_value", "tsz", "ts", "d"],
            [
                [1, float("nan"), float("nan"), "NaN", None, None, None],
                [2, float("inf"), float("inf"), "Infinity", PostgresInfinity(True), PostgresInfinity(True), PostgresInfinity(True)],
                [3, float("-inf"), float("-inf"), "-Infinity", PostgresInfinity(False), PostgresInfinity(False), PostgresInfinity(False)],
            ],
        )
        rows = con.execute(
            "SELECT id, isnan(real_value), isinf(real_value), real_value < 0, "
            "isnan(double_value), isinf(double_value), double_value < 0, "
            "numeric_value.special, CAST(tsz AS VARCHAR), CAST(ts AS VARCHAR), "
            "CAST(d AS VARCHAR) FROM typed.special_matrix ORDER BY id"
        ).fetchall()
        assert rows[0][:7] == (1, True, False, False, True, False, False)
        assert math.isnan(rows[0][7]) and rows[0][8:] == (None, None, None)
        assert rows[1] == (
            2, False, True, False, False, True, False,
            float("inf"), "infinity", "infinity", "infinity",
        )
        assert rows[2] == (
            3, False, True, True, False, True, True,
            float("-inf"), "-infinity", "-infinity", "-infinity",
        )
    finally:
        con.close()


def test_temporal_infinity_is_a_real_key_and_delete_reuses_the_same_identity():
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "temporal_keys",
            columns={"key": TIMESTAMPTZ, "payload": TEXT},
            key_columns=("key",),
        )
        table = registry.get("temporal_keys")
        insert_rows(
            con,
            table,
            ["key", "payload"],
            [[PostgresInfinity(True), "positive"], [PostgresInfinity(False), "negative"]],
        )
        delete_keys(con, table, ("key",), [(PostgresInfinity(True),)])
        assert con.execute(
            'SELECT CAST("key" AS VARCHAR), payload FROM typed.temporal_keys'
        ).fetchall() == [("-infinity", "negative")]
    finally:
        con.close()


def test_temporal_infinity_survives_typed_shadow_key_rebind():
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "shadow_temporal",
            columns={"key": TIMESTAMP, "payload": TEXT},
            key_columns=("key",),
        )
        insert_rows(
            con,
            registry.get("shadow_temporal"),
            ["key", "payload"],
            [[PostgresInfinity(True), "kept"]],
        )
        registry.ensure_typed(
            "shadow_temporal",
            columns={"key": TIMESTAMP, "payload": TEXT},
            key_columns=("key", "payload"),
        )
        assert con.execute(
            'SELECT CAST("key" AS VARCHAR), payload FROM typed.shadow_temporal'
        ).fetchall() == [("infinity", "kept")]
    finally:
        con.close()


def test_multirange_identity_carries_arbitrary_source_text():
    """The VARCHAR multirange identity path never invents or parses source text."""
    source_text = "opaque-source-spelling"
    assert _identity_tree(source_text, NUMMULTIRANGE) == {
        "multirange_text": source_text,
    }


def test_temporal_infinity_survives_snapshot_spill_and_replay(tmp_path):
    boxes: list[Lab] = []
    try:
        snapshot_box = Lab(
            tmp_path / "temporal-snapshot.duckdb",
            full_snapshot=True,
            unit_spill_events=1,
            snapshot_chunk_events=1,
        )
        boxes.append(snapshot_box)
        snapshot = snap("customers", 50, ident=1, marker="last")
        snapshot.after = {
            "id": 1,
            "tsz": PostgresInfinity(True),
            "ts": PostgresInfinity(True),
            "d": PostgresInfinity(True),
            "note": "snapshot",
        }
        snapshot.after_descriptors = _temporal_descriptors()
        snapshot.key_descriptors = {"id": INT4}
        snapshot_box.run([snapshot])
        assert snapshot_box.rows(
            snapshot_box.target("customers"),
            'id, CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), CAST("d" AS VARCHAR)',
        ) == [(1, "infinity", "infinity", "infinity")]

        box = Lab(tmp_path / "temporal-stream.duckdb", unit_spill_events=1)
        boxes.append(box)
        first = _temporal_event(txn="stream-1", order=1, lsn=100, ident=1, sign="infinity")
        second = _temporal_event(txn="stream-1", order=2, lsn=101, ident=2, sign="-infinity")
        box.run([first, second, end("stream-1", 2, 102, {"app.customers": 2})])
        assert box.rows(
            box.target("customers"),
            'id, CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), '
            'CAST("d" AS VARCHAR)',
            "id",
        ) == [
            (1, "infinity", "infinity", "infinity"),
            (2, "-infinity", "-infinity", "-infinity"),
        ]
        assert box.applier.spilled_events >= 1

        # A second arrival of the exact source transaction is the replay path.  The
        # keyed fold/delete identity must leave one row per source key.
        box.run([first, second, end("stream-1", 2, 102, {"app.customers": 2})])
        assert box.scalar(
            f'SELECT count(*) FROM "cdc_raw"."{box.target("customers")}"'
        ) == 2
    finally:
        for box in boxes:
            box.close()


def test_synthetic_builtin_failure_is_contained_to_one_table(tmp_path, monkeypatch):
    """The generic boundary catches a failure that is not an AdmissionError."""
    original = typed_materialization._bulk_insert_typed_rows

    def fail_only_bad(con, table, columns, rows):
        if table.name.endswith("contained_bad"):
            raise ValueError("synthetic third-party materializer failure")
        return original(con, table, columns, rows)

    monkeypatch.setattr(typed_materialization, "_bulk_insert_typed_rows", fail_only_bad)
    box = Lab(tmp_path / "contained.duckdb")
    try:
        for ident in range(1, 5):
            box.run(
                [
                    data(
                        f"contained-{ident}",
                        1,
                        100 + ident,
                        table="contained_bad",
                        key={"id": ident},
                        after={"id": ident, "name": f"bad-{ident}"},
                    ),
                    data(
                        f"contained-{ident}",
                        2,
                        200 + ident,
                        table="contained_peer",
                        key={"id": ident},
                        after={"id": ident, "name": f"peer-{ident}"},
                    ),
                    end(
                        f"contained-{ident}",
                        2,
                        300 + ident,
                        {
                            "app.contained_bad": 1,
                            "app.contained_peer": 1,
                        },
                    ),
                ]
            )
        assert box.rows(box.target("contained_peer"), "id, name", "id") == [
            (1, "peer-1"),
            (2, "peer-2"),
            (3, "peer-3"),
            (4, "peer-4"),
        ]
        assert not box.exists(box.target("contained_bad"))
        assert box.q(
            "SELECT state, reason FROM _cdc_flight.schema_refusals "
            "WHERE source_table = 'contained_bad'"
        )[0][0] == "quarantined"
        assert box.scalar(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE code = 'table_exception_contained'"
        ) == 1
        assert box.applier._contained_failures
        assert "ValueError" in box.applier._contained_failures[0]["exception_type"]
    finally:
        box.close()


@pytest.mark.slow
@pytest.mark.e2e
def test_real_slot_advances_when_a_builtin_materializer_failure_is_injected(sandbox, tmp_path):
    """Four real runs contain a synthetic ValueError and preserve the healthy peer."""
    import os

    import psycopg

    publication = "cdc_flight_pub"
    bad = "app.fix14_any_exception_bad"
    peer = "app.fix14_any_exception_peer"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "fix14_any_exception_bad,fix14_any_exception_peer",
    }
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {bad}")
        conn.execute(f"DROP TABLE IF EXISTS {peer}")
        conn.execute(f"CREATE TABLE {bad} (id integer PRIMARY KEY, name text)")
        conn.execute(f"CREATE TABLE {peer} (id integer PRIMARY KEY, name text)")
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {bad}, {peer}")

    # The hook is test-only and lives outside the checkout.  It raises a builtin
    # ValueError at the real PyArrow materializer for one target, after which the
    # production boundary must be the component that contains it.
    hook = tmp_path / "sitecustomize.py"
    hook.write_text(
        "from cdc_flight import typed_materialization as _tm\n"
        "_real = _tm._bulk_insert_typed_rows\n"
        "def _fail_one(con, table, columns, rows):\n"
        "    if 'fix14_any_exception_bad' in table.name:\n"
        "        raise ValueError('synthetic third-party materializer failure')\n"
        "    return _real(con, table, columns, rows)\n"
        "_tm._bulk_insert_typed_rows = _fail_one\n",
        encoding="utf-8",
    )
    pipeline_env = {
        **capture,
        "PYTHONPATH": os.pathsep.join(
            item for item in (str(tmp_path), os.environ.get("PYTHONPATH", "")) if item
        ),
    }

    def slot_metrics():
        sandbox.sql("CHECKPOINT")
        return sandbox.pg_query(
            "SELECT restart_lsn::text, confirmed_flush_lsn::text, "
            "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
            "(restart_lsn - '0/0')::bigint, "
            "(confirmed_flush_lsn - '0/0')::bigint "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (sandbox.slot,),
        )[0]

    try:
        baseline = sandbox.run(
            reset_state=True,
            extra_env=pipeline_env,
            max_seconds=35,
            timeout=120,
        )
        assert baseline["ok"] is True, baseline
        runs = []
        metrics = []
        for ident in range(1, 5):
            sandbox.sql(
                [
                    f"INSERT INTO {bad} VALUES ({ident}, 'bad-{ident}')",
                    f"INSERT INTO {peer} VALUES ({ident}, 'peer-{ident}')",
                ],
                one_transaction=True,
            )
            run = sandbox.run(
                extra_env=pipeline_env,
                max_seconds=45,
                timeout=150,
                expect_success=False,
            )
            assert run["ok"] is False, run
            assert run["error_cause_type"] == "SchemaEvolutionRefused", run
            # The first run carries the raw builtin in the run cause.  Once the
            # durable table is quarantined, later runs fail closed on the retained
            # refusal before re-entering the materializer; the original third-party
            # type/message must remain durable and attributable there.
            assert "ValueError" in sandbox.duck_query(
                "SELECT reason FROM _cdc_flight.schema_refusals "
                "WHERE source_table = 'fix14_any_exception_bad'"
            )[0][0], run
            runs.append(run)
            metrics.append(slot_metrics())

        restarts = [int(row[3]) for row in metrics]
        confirms = [int(row[4]) for row in metrics]
        assert restarts == sorted(restarts) and len(set(restarts)) == 4, metrics
        assert confirms == sorted(confirms) and len(set(confirms)) == 4, metrics
        assert sandbox.duck_query(
            "SELECT id, name FROM cdc_raw.cdcflight_app_fix14_any_exception_peer "
            "ORDER BY id"
        ) == [(ident, f"peer-{ident}") for ident in range(1, 5)]
        assert sandbox.duck_query(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE source_table = 'fix14_any_exception_bad'"
        ) == [("quarantined",)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE code = 'table_exception_contained'"
        ) == [(1,)]
        print("FIX14 generic any-exception runs:", runs)
        print("FIX14 generic any-exception slot metrics:", metrics)
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {bad}, {peer}")
            conn.execute(f"DROP TABLE IF EXISTS {bad}")
            conn.execute(f"DROP TABLE IF EXISTS {peer}")
