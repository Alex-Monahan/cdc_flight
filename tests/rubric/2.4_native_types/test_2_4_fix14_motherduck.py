"""FIX ROUND 14 scalar temporal infinity paths on the real MotherDuck runtime."""

from __future__ import annotations

import contextlib

import pytest
from support.applier_lab import Lab, data, end, snap
from support.motherduck_probe import connect

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.naming import quote
from cdc_flight.typed_types import PostgresInfinity, SourceTypeDescriptor

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]


INT4 = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
TEXT = SourceTypeDescriptor(25, "pg_catalog.text", "text")
DATE = SourceTypeDescriptor(1082, "pg_catalog.date", "date")
TIMESTAMP = SourceTypeDescriptor(1114, "pg_catalog.timestamp", "timestamp")
TIMESTAMPTZ = SourceTypeDescriptor(1184, "pg_catalog.timestamptz", "timestamptz")


def _temporal_descriptors():
    return {
        "id": INT4,
        "tsz": TIMESTAMPTZ,
        "ts": TIMESTAMP,
        "d": DATE,
        "note": TEXT,
    }


def _event(txn: str, order: int, lsn: int, ident: int, positive: bool):
    value = PostgresInfinity(positive)
    record = data(
        txn,
        order,
        lsn,
        table="customers",
        key={"id": ident},
        after={"id": ident, "tsz": value, "ts": value, "d": value, "note": str(value)},
    )
    record.key_descriptors = {"id": INT4}
    record.after_descriptors = _temporal_descriptors()
    return record


def test_motherduck_temporal_infinity_snapshot_stream_spill_replay_shadow_and_key(
    tmp_path, motherduck_case
):
    token = motherduck_case["token"]
    database = motherduck_case["database"]
    dataset = motherduck_case["dataset"]
    control_schema = motherduck_case["control_schema"]
    boxes: list[Lab] = []
    try:
        snapshot_con = connect(token, database)
        snapshot_box = Lab(
            tmp_path / "motherduck-temporal-snapshot",
            connection=snapshot_con,
            dataset=dataset,
            pipeline=f"fix14-md-snapshot-{dataset}",
            namespace=f"fix14-md-snapshot::{dataset}",
            control_schema=control_schema,
            full_snapshot=True,
            unit_spill_events=1,
            snapshot_chunk_events=1,
        )
        boxes.append(snapshot_box)
        snapshot = snap("customers", 50, ident=10, marker="true")
        snapshot.after = {
            "id": 10,
            "tsz": PostgresInfinity(True),
            "ts": PostgresInfinity(True),
            "d": PostgresInfinity(True),
            "note": "snapshot",
        }
        snapshot.after_descriptors = _temporal_descriptors()
        snapshot.key_descriptors = {"id": INT4}
        snapshot_negative = snap("customers", 51, ident=11, marker="last")
        snapshot_negative.after = {
            "id": 11,
            "tsz": PostgresInfinity(False),
            "ts": PostgresInfinity(False),
            "d": PostgresInfinity(False),
            "note": "snapshot-negative",
        }
        snapshot_negative.after_descriptors = _temporal_descriptors()
        snapshot_negative.key_descriptors = {"id": INT4}
        snapshot_box.run([snapshot, snapshot_negative])
        assert snapshot_box.rows(
            snapshot_box.target("customers"),
            'id, CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), CAST("d" AS VARCHAR)',
        ) == [
            (10, "infinity", "infinity", "infinity"),
            (11, "-infinity", "-infinity", "-infinity"),
        ]

        stream_con = connect(token, database)
        stream_box = Lab(
            tmp_path / "motherduck-temporal-stream",
            connection=stream_con,
            dataset=dataset,
            pipeline=f"fix14-md-stream-{dataset}",
            namespace=f"fix14-md-stream::{dataset}",
            control_schema=control_schema,
            unit_spill_events=1,
        )
        boxes.append(stream_box)
        positive = _event("md-stream", 1, 100, 12, True)
        negative = _event("md-stream", 2, 101, 13, False)
        stream_box.run([positive, negative, end("md-stream", 2, 102, {"app.customers": 2})])
        stream_box.run([positive, negative, end("md-stream", 2, 102, {"app.customers": 2})])
        assert stream_box.applier.spilled_events >= 1
        assert stream_box.rows(
            stream_box.target("customers"),
            'id, CAST("tsz" AS VARCHAR), CAST("ts" AS VARCHAR), CAST("d" AS VARCHAR)',
        ) == [
            (10, "infinity", "infinity", "infinity"),
            (11, "-infinity", "-infinity", "-infinity"),
            (12, "infinity", "infinity", "infinity"),
            (13, "-infinity", "-infinity", "-infinity"),
        ]

        registry = SchemaRegistry(stream_box.con, dataset)
        registry.ensure_typed(
            "temporal_keys",
            columns={"key": TIMESTAMPTZ, "payload": TEXT},
            key_columns=("key",),
        )
        table = registry.get("temporal_keys")
        insert_rows(
            stream_box.con,
            table,
            ["key", "payload"],
            [[PostgresInfinity(True), "positive"], [PostgresInfinity(False), "negative"]],
        )
        delete_keys(stream_box.con, table, ("key",), [(PostgresInfinity(True),)])
        assert stream_box.con.execute(
            f"SELECT CAST(\"key\" AS VARCHAR), payload FROM {quote(dataset)}.temporal_keys"
        ).fetchall() == [("-infinity", "negative")]

        registry.ensure_typed(
            "shadow_temporal",
            columns={"key": TIMESTAMP, "payload": TEXT},
            key_columns=("key",),
        )
        insert_rows(
            stream_box.con,
            registry.get("shadow_temporal"),
            ["key", "payload"],
            [[PostgresInfinity(True), "kept"]],
        )
        registry.ensure_typed(
            "shadow_temporal",
            columns={"key": TIMESTAMP, "payload": TEXT},
            key_columns=("key", "payload"),
        )
        assert stream_box.con.execute(
            f"SELECT CAST(\"key\" AS VARCHAR), payload FROM {quote(dataset)}.shadow_temporal"
        ).fetchall() == [("infinity", "kept")]
    finally:
        for box in boxes:
            with contextlib.suppress(Exception):
                box.close()
        cleanup = None
        try:
            cleanup = connect(token, database)
            cleanup.execute(f"DROP SCHEMA IF EXISTS {quote(dataset)} CASCADE")
        finally:
            if cleanup is not None:
                cleanup.close()
