"""Local DuckDB proofs for scalar temporal special values and path coverage."""

from __future__ import annotations

import math
import shutil
from datetime import date, timedelta

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
NUMERIC = SourceTypeDescriptor(1700, "pg_catalog.numeric", "numeric", precision=30, scale=10)
NUMRANGE = SourceTypeDescriptor(3906, "pg_catalog.numrange", "range", range_subtype=NUMERIC)
NUMMULTIRANGE = SourceTypeDescriptor(
    4532, "pg_catalog.nummultirange", "multirange", range_subtype=NUMRANGE
)


def _fresh_real_sandbox(sandbox) -> None:
    """Separate module-scoped real-source scenarios by deleting only their temp sink."""
    sandbox.drop_slot()
    shutil.rmtree(sandbox.state_dir, ignore_errors=True)
    sandbox.duckdb_path.unlink(missing_ok=True)


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
            "SELECT id, CAST(tsz AS VARCHAR), CAST(ts AS VARCHAR), CAST(d AS VARCHAR) "
            "FROM typed.temporal_specials ORDER BY id"
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
                [
                    2,
                    float("inf"),
                    float("inf"),
                    "Infinity",
                    PostgresInfinity(True),
                    PostgresInfinity(True),
                    PostgresInfinity(True),
                ],
                [
                    3,
                    float("-inf"),
                    float("-inf"),
                    "-Infinity",
                    PostgresInfinity(False),
                    PostgresInfinity(False),
                    PostgresInfinity(False),
                ],
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
            2,
            False,
            True,
            False,
            False,
            True,
            False,
            float("inf"),
            "infinity",
            "infinity",
            "infinity",
        )
        assert rows[2] == (
            3,
            False,
            True,
            True,
            False,
            True,
            True,
            float("-inf"),
            "-infinity",
            "-infinity",
            "-infinity",
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


def test_large_delete_key_staging_keeps_temporal_infinity_on_typed_path():
    """The >2,000-key DELETE path must not send PostgreSQL infinity through Arrow."""
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "large_temporal_keys",
            columns={"key": DATE, "payload": TEXT},
            key_columns=("key",),
        )
        table = registry.get("large_temporal_keys")
        finite = [date(2000, 1, 1) + timedelta(days=index) for index in range(2001)]
        keys = [(PostgresInfinity(True),), *[(value,) for value in finite]]
        insert_rows(
            con,
            table,
            ["key", "payload"],
            [[key[0], str(index)] for index, key in enumerate(keys)],
        )
        assert con.execute("SELECT count(*) FROM typed.large_temporal_keys").fetchone() == (2002,)
        delete_keys(con, table, ("key",), keys)
        assert con.execute("SELECT count(*) FROM typed.large_temporal_keys").fetchone() == (0,)
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
            'id, CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), CAST("d" AS VARCHAR)',
            "id",
        ) == [
            (1, "infinity", "infinity", "infinity"),
            (2, "-infinity", "-infinity", "-infinity"),
        ]
        assert box.applier.spilled_events >= 1

        # A second arrival of the exact source transaction is the replay path.  The
        # keyed fold/delete identity must leave one row per source key.
        box.run([first, second, end("stream-1", 2, 102, {"app.customers": 2})])
        assert box.scalar(f'SELECT count(*) FROM "cdc_raw"."{box.target("customers")}"') == 2
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
        assert (
            box.q(
                "SELECT state, reason FROM _cdc_flight.schema_refusals "
                "WHERE source_table = 'contained_bad'"
            )[0][0]
            == "quarantined"
        )
        assert (
            box.scalar(
                "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
            )
            == 1
        )
        assert box.applier._contained_failures
        assert "ValueError" in box.applier._contained_failures[0]["exception_type"]
    finally:
        box.close()


def test_materializer_failure_after_table_delete_rolls_back_the_whole_group(tmp_path, monkeypatch):
    """A Python failure after DML cannot commit a torn table image."""
    original = typed_materialization._bulk_insert_typed_rows
    box = Lab(tmp_path / "torn-write.duckdb")
    try:
        box.run(
            [
                data(
                    "torn-baseline",
                    1,
                    100,
                    table="torn_bad",
                    key={"id": 1},
                    after={"id": 1, "name": "old"},
                ),
                data(
                    "torn-baseline",
                    2,
                    101,
                    table="torn_bad",
                    key={"id": 2},
                    after={"id": 2, "name": "keep"},
                ),
                end("torn-baseline", 2, 102, {"app.torn_bad": 2}),
            ]
        )

        def delete_then_raise(con, table, columns, rows):
            if rows and table.name.endswith("torn_bad"):
                con._connection.execute(f'DELETE FROM {table.qualified} WHERE "id" = 1')
                raise ValueError("synthetic post-delete materializer failure")
            return original(con, table, columns, rows)

        monkeypatch.setattr(typed_materialization, "_bulk_insert_typed_rows", delete_then_raise)
        box.run(
            [
                data(
                    "torn-change",
                    1,
                    200,
                    table="torn_bad",
                    op="d",
                    key={"id": 1},
                    before={"id": 1, "name": "old"},
                ),
                data(
                    "torn-change",
                    2,
                    201,
                    table="torn_bad",
                    key={"id": 1},
                    after={"id": 1, "name": "new"},
                ),
                data(
                    "torn-change",
                    3,
                    202,
                    table="torn_peer",
                    key={"id": 1},
                    after={"id": 1, "name": "healthy"},
                ),
                end(
                    "torn-change",
                    3,
                    203,
                    {"app.torn_bad": 2, "app.torn_peer": 1},
                ),
            ]
        )
        # The first attempt deleted the old row before the injected failure.  The
        # commit owner must roll that whole group back before replaying the healthy
        # peer with torn_bad excluded.
        assert box.rows(box.target("torn_bad"), "id, name", "id") == [
            (1, "old"),
            (2, "keep"),
        ]
        assert box.rows(box.target("torn_peer"), "id, name", "id") == [(1, "healthy")]
        assert box.q(
            "SELECT state FROM _cdc_flight.schema_refusals WHERE source_table = 'torn_bad'"
        ) == [("pending",)]
        assert box.applier.error is not None
        assert "ValueError" in box.applier._contained_failures[0]["exception_type"]
    finally:
        box.close()


def test_an_explicit_quarantine_ack_is_loudly_recorded_but_does_not_unblock(tmp_path, monkeypatch):
    """An operator acknowledgement suppresses only the repeated run failure."""
    original = typed_materialization._bulk_insert_typed_rows

    def fail_only_bad(con, table, columns, rows):
        if rows and table.name.endswith("ack_bad"):
            raise ValueError("synthetic acknowledged materializer failure")
        return original(con, table, columns, rows)

    monkeypatch.setattr(typed_materialization, "_bulk_insert_typed_rows", fail_only_bad)
    path = tmp_path / "acknowledged-quarantine.duckdb"
    first = Lab(path)
    try:
        for ident in (1, 2):
            first.run(
                [
                    data(
                        f"ack-{ident}",
                        1,
                        300 + ident,
                        table="ack_bad",
                        key={"id": ident},
                        after={"id": ident, "name": f"bad-{ident}"},
                    ),
                    data(
                        f"ack-{ident}",
                        2,
                        400 + ident,
                        table="ack_peer",
                        key={"id": ident},
                        after={"id": ident, "name": f"peer-{ident}"},
                    ),
                    end(
                        f"ack-{ident}",
                        2,
                        500 + ident,
                        {"app.ack_bad": 1, "app.ack_peer": 1},
                    ),
                ]
            )
        assert first.q(
            "SELECT state FROM _cdc_flight.schema_refusals WHERE source_table = 'ack_bad'"
        ) == [("quarantined",)]
    finally:
        first.lease.release(first.con)
        first.close()

    acknowledged = Lab(
        path,
        acknowledged_quarantines=frozenset({"app.ack_bad"}),
    )
    try:
        acknowledged.run(
            [
                data(
                    "ack-replay",
                    1,
                    600,
                    table="ack_bad",
                    key={"id": 3},
                    after={"id": 3, "name": "still-blocked"},
                ),
                data(
                    "ack-replay",
                    2,
                    601,
                    table="ack_peer",
                    key={"id": 3},
                    after={"id": 3, "name": "peer-3"},
                ),
                end("ack-replay", 2, 602, {"app.ack_bad": 1, "app.ack_peer": 1}),
            ]
        )
        assert acknowledged.applier.error is None
        assert acknowledged.applier._acknowledged_quarantines == {"app.ack_bad"}
        assert acknowledged.rows(acknowledged.target("ack_peer"), "id, name", "id") == [
            (1, "peer-1"),
            (2, "peer-2"),
            (3, "peer-3"),
        ]
        assert acknowledged.q(
            "SELECT state FROM _cdc_flight.schema_refusals WHERE source_table = 'ack_bad'"
        ) == [("quarantined",)]
        assert not acknowledged.exists(acknowledged.target("ack_bad"))
    finally:
        acknowledged.close()


def test_destination_programming_error_is_loud_not_a_table_quarantine(tmp_path, monkeypatch):
    """A malformed destination statement stays an engine/programming failure."""
    import duckdb

    original = typed_materialization._bulk_insert_typed_rows

    def fail_with_bad_sql(con, table, columns, rows):
        if table.name.endswith("programming_bad"):
            con._connection.execute(
                "SELECT definitely_missing_column FROM definitely_missing_table"
            )
        return original(con, table, columns, rows)

    monkeypatch.setattr(typed_materialization, "_bulk_insert_typed_rows", fail_with_bad_sql)
    box = Lab(tmp_path / "programming-error.duckdb")
    try:
        with pytest.raises(duckdb.CatalogException):
            box.run(
                [
                    data(
                        "programming",
                        1,
                        100,
                        table="programming_bad",
                        key={"id": 1},
                        after={"id": 1, "name": "bad"},
                    ),
                    end("programming", 1, 101, {"app.programming_bad": 1}),
                ]
            )
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.schema_refusals "
            "WHERE source_table = 'programming_bad'"
        ) == [(0,)]
    finally:
        box.close()


def test_transaction_control_exception_is_not_a_table_quarantine(tmp_path, monkeypatch):
    """FIX16 reproduction: a control failure must escape the table boundary."""
    import duckdb

    original = typed_materialization._bulk_insert_typed_rows

    def begin_inside_destination_transaction(con, table, columns, rows):
        if table.name.endswith("txn_exception_bad"):
            # This is the reviewer's probe: it is a transaction-control operation,
            # not a rejection of a source value or row.
            con._connection.execute("BEGIN TRANSACTION")
        return original(con, table, columns, rows)

    monkeypatch.setattr(
        typed_materialization,
        "_bulk_insert_typed_rows",
        begin_inside_destination_transaction,
    )
    box = Lab(tmp_path / "txn-exception-reproduction.duckdb")
    try:
        with pytest.raises(duckdb.TransactionException):
            box.run(
                [
                    data(
                        "txn-control",
                        1,
                        100,
                        table="txn_exception_bad",
                        key={"id": 1},
                        after={"id": 1, "name": "bad"},
                    ),
                    data(
                        "txn-control",
                        2,
                        101,
                        table="txn_exception_peer",
                        key={"id": 1},
                        after={"id": 1, "name": "healthy"},
                    ),
                    end(
                        "txn-control",
                        2,
                        102,
                        {"app.txn_exception_bad": 1, "app.txn_exception_peer": 1},
                    ),
                ]
            )
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.schema_refusals "
            "WHERE source_table = 'txn_exception_bad'"
        ) == [(0,)]
        assert box.q(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(0,)]
    finally:
        box.close()


def test_destination_classifier_is_a_closed_data_boundary():
    """Only explicit value/row rejections cross the table-DML capability boundary."""
    import inspect

    import duckdb

    from cdc_flight import destination_failure

    assert destination_failure.DATA_REJECTION_EXCEPTION_NAMES == (
        "ConversionException",
        "ConstraintException",
        "InvalidInputException",
        "NotImplementedException",
        "OutOfRangeException",
        "TypeMismatchException",
    )
    assert "TransactionException" not in destination_failure.DATA_REJECTION_EXCEPTION_NAMES
    driver_errors = {
        name
        for name in dir(duckdb)
        if inspect.isclass(getattr(duckdb, name))
        and issubclass(getattr(duckdb, name), duckdb.Error)
        and name != "Error"
    }
    assert driver_errors == {
        *destination_failure.DATA_REJECTION_EXCEPTION_NAMES,
        *destination_failure.NON_DATA_EXCEPTION_NAMES,
    }

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t_int (value INTEGER)")
    facade = destination_failure.MaterializationConnection(
        con,
        destination_failure._mint_table_data_provenance("app", "typed", "t_int"),
    )
    try:
        with pytest.raises(duckdb.ConversionException):
            con.execute("SELECT CAST('not-an-integer' AS INTEGER)")
        with pytest.raises(destination_failure.DestinationDataRejection) as rejected:
            destination_failure.execute_table_dml(
                facade, "INSERT INTO t_int VALUES (?)", ["not-an-integer"]
            )
        assert isinstance(rejected.value.original, duckdb.ConversionException)
        assert rejected.value.provenance.qualified_source == "app.typed"

        con.execute("BEGIN TRANSACTION")
        with pytest.raises(duckdb.TransactionException):
            con.execute("BEGIN TRANSACTION")
        con.execute("ROLLBACK")
    finally:
        con.close()


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
    _fresh_real_sandbox(sandbox)
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {bad}")
        conn.execute(f"DROP TABLE IF EXISTS {peer}")
        conn.execute(f"CREATE TABLE {bad} (id integer PRIMARY KEY, name text, payload jsonb)")
        conn.execute(f"CREATE TABLE {peer} (id integer PRIMARY KEY, name text)")
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {bad}, {peer}")

    # The hook is test-only and lives outside the checkout.  It raises a builtin
    # ValueError at the real PyArrow materializer for one target, after which the
    # production boundary must be the component that contains it.
    hook = tmp_path / "sitecustomize.py"
    hook.write_text(
        "from cdc_flight import typed_materialization as _tm\n"
        "from cdc_flight.destination_failure import execute_table_dml\n"
        "_real_bulk = _tm.bulk_insert\n"
        "_real_typed = _tm.insert_typed_rows\n"
        "def _bad(target):\n"
        "    return 'fix14_any_exception_bad' in str(target)\n"
        "def _fail_bulk(con, target, columns, rows, types=None, **kwargs):\n"
        "    if rows and _bad(target):\n"
        "        raise ValueError('synthetic third-party materializer failure')\n"
        "    return _real_bulk(con, target, columns, rows, types, **kwargs)\n"
        "def _fail_typed(con, table, columns, rows, native_types, **kwargs):\n"
        "    if rows and _bad(table.qualified):\n"
        "        raise ValueError('synthetic third-party materializer failure')\n"
        "    return _real_typed(con, table, columns, rows, native_types, **kwargs)\n"
        "_tm.bulk_insert = _fail_bulk\n"
        "_tm.insert_typed_rows = _fail_typed\n",
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
                    f"INSERT INTO {bad} VALUES ({ident}, 'bad-{ident}', '{{\"kind\": \"bad\"}}')",
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
            assert run["ok"] is False, run.get("output", run)
            assert run["error_cause_type"] == "SchemaEvolutionRefused", run
            # The first run carries the raw builtin in the run cause.  Once the
            # durable table is quarantined, later runs fail closed on the retained
            # refusal before re-entering the materializer; the original third-party
            # type/message must remain durable and attributable there.
            assert (
                "ValueError"
                in sandbox.duck_query(
                    "SELECT reason FROM _cdc_flight.schema_refusals "
                    "WHERE source_table = 'fix14_any_exception_bad'"
                )[0][0]
            ), run
            runs.append(run)
            metrics.append(slot_metrics())

        restarts = [int(row[3]) for row in metrics]
        confirms = [int(row[4]) for row in metrics]
        assert restarts == sorted(restarts) and len(set(restarts)) == 4, metrics
        assert confirms == sorted(confirms) and len(set(confirms)) == 4, metrics
        assert sandbox.duck_query(
            "SELECT id, name FROM cdc_raw.cdcflight_app_fix14_any_exception_peer ORDER BY id"
        ) == [(ident, f"peer-{ident}") for ident in range(1, 5)]
        assert sandbox.duck_query(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE source_table = 'fix14_any_exception_bad'"
        ) == [("quarantined",)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(1,)]
        print("FIX14 generic any-exception runs:", runs)
        print("FIX14 generic any-exception slot metrics:", metrics)
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {bad}, {peer}")
            conn.execute(f"DROP TABLE IF EXISTS {bad}")
            conn.execute(f"DROP TABLE IF EXISTS {peer}")


@pytest.mark.slow
@pytest.mark.e2e
def test_real_destination_error_is_contained_over_four_runs_with_wal_metrics(sandbox, tmp_path):
    """A real DuckDB statement error is loud, table-scoped, and slot-safe."""
    import os

    import psycopg

    publication = "cdc_flight_pub"
    bad_tables = [f"app.r15_destination_error_bad_{index}" for index in range(1, 5)]
    peer = "app.r15_destination_error_peer"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": ",".join([table.rsplit(".", 1)[1] for table in [*bad_tables, peer]]),
    }
    _fresh_real_sandbox(sandbox)
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS " + ", ".join([*bad_tables, peer]))
        for table in bad_tables:
            conn.execute(f"CREATE TABLE {table} (id integer PRIMARY KEY, name text, payload jsonb)")
        conn.execute(f"CREATE TABLE {peer} (id integer PRIMARY KEY, name text)")
        conn.execute(
            "ALTER PUBLICATION " + publication + " ADD TABLE " + ", ".join([*bad_tables, peer])
        )
    hook = tmp_path / "sitecustomize.py"
    hook.write_text(
        "from cdc_flight import typed_materialization as _tm\n"
        "from cdc_flight.destination_failure import execute_table_dml\n"
        "_real_bulk = _tm.bulk_insert\n"
        "_real_typed = _tm.insert_typed_rows\n"
        "def _bad(target):\n"
        "    return 'r15_destination_error_bad_' in str(target)\n"
        "def _fail_bulk(con, target, columns, rows, types=None, **kwargs):\n"
        "    if rows and _bad(target):\n"
        "        execute_table_dml(con, f'INSERT INTO {target} (\"id\") VALUES (?)', ['bad-int'])\n"
        "    return _real_bulk(con, target, columns, rows, types, **kwargs)\n"
        "def _fail_typed(con, table, columns, rows, native_types, **kwargs):\n"
        "    if rows and _bad(table.qualified):\n"
        "        execute_table_dml(con, f'INSERT INTO {table.qualified} (\"id\") VALUES (?)', ['bad-int'])\n"
        "    return _real_typed(con, table, columns, rows, native_types, **kwargs)\n"
        "_tm.bulk_insert = _fail_bulk\n"
        "_tm.insert_typed_rows = _fail_typed\n",
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
            max_seconds=150,
            timeout=300,
        )
        assert baseline["ok"] is True, baseline
        metrics = []
        for index, bad in enumerate(bad_tables, start=1):
            sandbox.sql(
                [
                    f"INSERT INTO {bad} VALUES ({index}, 'bad-{index}', '{{\"kind\": \"bad\"}}')",
                    f"INSERT INTO {peer} VALUES ({index}, 'healthy-{index}')",
                ],
                one_transaction=True,
            )
            failed = sandbox.run(
                extra_env=pipeline_env,
                expect_success=False,
                max_seconds=150,
                timeout=300,
            )
            assert failed["ok"] is False, failed.get("output", failed)
            assert failed["error_cause_type"] == "SchemaEvolutionRefused", failed
            metrics.append(slot_metrics())
            reason = sandbox.duck_query(
                "SELECT reason FROM _cdc_flight.schema_refusals WHERE source_table = ?",
                [bad.rsplit(".", 1)[1]],
            )[0][0]
            assert "ConversionException" in reason, reason

        restart = [int(row[3]) for row in metrics]
        confirmed = [int(row[4]) for row in metrics]
        retained = [int(row[2]) for row in metrics]
        assert restart == sorted(restart) and len(set(restart)) == 4, metrics
        assert confirmed == sorted(confirmed) and len(set(confirmed)) == 4, metrics
        assert all(value >= 0 for value in retained), metrics
        assert all(int(row[3]) <= int(row[4]) for row in metrics), metrics
        assert sandbox.duck_query(
            "SELECT id, name FROM cdc_raw.cdcflight_app_r15_destination_error_peer ORDER BY id"
        ) == [(index, f"healthy-{index}") for index in range(1, 5)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(4,)]
        print("FIX15 destination-raised LSN/WAL metrics:", metrics)
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(
                "ALTER PUBLICATION " + publication + " DROP TABLE " + ", ".join([*bad_tables, peer])
            )
            conn.execute("DROP TABLE IF EXISTS " + ", ".join([*bad_tables, peer]))


@pytest.mark.slow
@pytest.mark.e2e
def test_real_transaction_control_failure_fails_run_without_table_attribution(sandbox, tmp_path):
    """A real PostgreSQL run keeps DuckDB transaction-control errors run-scoped."""
    import os

    import psycopg

    publication = "cdc_flight_pub"
    bad = "app.r16_txn_control_bad"
    peer = "app.r16_txn_control_peer"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "r16_txn_control_bad,r16_txn_control_peer",
    }
    _fresh_real_sandbox(sandbox)
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {bad}, {peer}")
        conn.execute(f"CREATE TABLE {bad} (id integer PRIMARY KEY, name text)")
        conn.execute(f"CREATE TABLE {peer} (id integer PRIMARY KEY, name text)")
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {bad}, {peer}")

    hook = tmp_path / "sitecustomize.py"
    hook.write_text(
        "from cdc_flight import typed_materialization as _tm\n"
        "_real = _tm._bulk_insert_typed_rows\n"
        "def _control(con, table, columns, rows):\n"
        "    if rows and str(table.name).endswith('r16_txn_control_bad'):\n"
        "        con._connection.execute('BEGIN TRANSACTION')\n"
        "    return _real(con, table, columns, rows)\n"
        "_tm._bulk_insert_typed_rows = _control\n",
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
            max_seconds=90,
            timeout=180,
        )
        assert baseline["ok"] is True, baseline
        before = slot_metrics()
        sandbox.sql(
            [
                f"INSERT INTO {bad} VALUES (1, 'bad')",
                f"INSERT INTO {peer} VALUES (1, 'healthy')",
            ],
            one_transaction=True,
        )
        source_lsn = sandbox.pg_query("SELECT (pg_current_wal_lsn() - '0/0')::bigint")[0][0]
        failed = sandbox.run(
            extra_env=pipeline_env,
            expect_success=False,
            max_seconds=90,
            timeout=180,
        )
        after = slot_metrics()
        assert failed["ok"] is False, failed
        assert failed["error_cause_type"] == "TransactionException", failed
        assert "cannot start a transaction within a transaction" in failed["error"].lower(), failed
        assert sandbox.duck_query(
            "SELECT count(*) FROM _cdc_flight.schema_refusals WHERE source_table IN (?, ?)",
            [bad.rsplit(".", 1)[1], peer.rsplit(".", 1)[1]],
        ) == [(0,)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM _cdc_flight.alerts WHERE code = 'table_exception_contained'"
        ) == [(0,)]
        assert int(after[4]) == int(before[4]), (before, after)
        assert int(after[4]) < int(source_lsn), (after, source_lsn)
        assert int(after[3]) <= int(after[4])
        assert int(after[2]) >= 0
        print(
            "FIX16 transaction-control failure LSN/WAL evidence:",
            {"before": before, "after": after, "source_lsn": source_lsn},
        )
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {bad}, {peer}")
            conn.execute(f"DROP TABLE IF EXISTS {bad}, {peer}")
