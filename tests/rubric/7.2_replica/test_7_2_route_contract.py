"""Rubric 7.2's route, heartbeat, WAL, and shutdown contracts.

These tests deliberately use connection spies rather than a standalone connector
probe. They exercise the same route objects and lifecycle seams used by the
production pipeline; the live post-snapshot proof is in the slow and MotherDuck
modules beside this file.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from cdc_flight import catalog_poll
from cdc_flight.catalog import CatalogWatcher, SourceRelation
from cdc_flight.config import ReplicationConfig, SourceConfig
from cdc_flight.debezium_props import (
    HEARTBEAT_ACTION_QUERY,
    STANDBY_HEARTBEAT_DISABLED_PROPERTY,
    build_properties,
)
from cdc_flight.errors import UnsafeDebeziumProperty
from cdc_flight.pipeline import run_engine_bounded
from cdc_flight.source_health import (
    _SLOT_SQL,
    _SLOT_SQL_FAST,
    SlotSample,
    SourceHealth,
    assert_recovery_safe_wal_sql,
)
from cdc_flight.source_marker import SourceMarker
from cdc_flight.source_routes import SourceRoutePolicy

PRIMARY = "postgresql://primary:15432/cdc_source"
REPLICA = "postgresql://postgres:postgres@standby:15435/cdc_source"


def _standby_source(monkeypatch) -> SourceConfig:
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    monkeypatch.setenv("CDC_PRIMARY_DSN", PRIMARY)
    return SourceConfig(
        host="standby",
        port=15435,
        dbname="cdc_source",
        primary_dsn_override=PRIMARY,
    )


def _relation(*, published=True, relfilenode=10, relation_type_oid=11):
    return SourceRelation(
        schema="app",
        table="customers",
        oid=1,
        relfilenode=relfilenode,
        relation_type_oid=relation_type_oid,
        published=published,
        replica_identity="d",
    )


class _Result:
    def __init__(self, *, rows=(), row=None):
        self.rows = list(rows)
        self.row = row

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row


class _CatalogConnection:
    def __init__(self, dsn, *, published=True):
        self.dsn = dsn
        self.published = published
        self.closed = False
        self.sql: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
        return False

    def close(self):
        self.closed = True

    def execute(self, sql, _params=None):
        text = str(sql)
        self.sql.append(text)
        if "relation_count" in text:
            return _Result(rows=[("app", 1)])
        if "FROM pg_inherits" in text:
            return _Result()
        if "CATALOG" in text:
            return _Result()
        if "pg_current_wal_lsn" in text or "pg_last_wal_receive_lsn" in text:
            return _Result(row=(777,))
        if "information_schema.columns" in text:
            return _Result(rows=[])
        if "ALTER PUBLICATION" in text:
            return _Result()
        return _Result(
            rows=[
                (
                    "app",
                    "customers",
                    1,
                    10,
                    11,
                    "d",
                    self.published,
                    False,
                    False,
                    [],
                )
            ]
        )


def test_standby_route_policy_has_three_noninterchangeable_routes(monkeypatch):
    source = _standby_source(monkeypatch)
    routes = source.route_policy

    assert routes == SourceRoutePolicy(
        role="standby",
        read_replication_dsn=REPLICA,
        source_write_dsn=PRIMARY,
        slot_owner_dsn=REPLICA,
    )
    assert routes.read_dsn == REPLICA
    assert routes.source_write_dsn != routes.slot_owner_dsn
    assert routes.slot_owner_scope == "standby"

    watcher = CatalogWatcher(
        dsn="ignored-by-policy",
        primary_dsn="also-ignored-by-policy",
        routes=routes,
        publication="cdc_flight_pub",
        schema="app",
        include={"app.customers"},
        poll_seconds=0,
    )
    assert watcher.dsn == REPLICA
    assert watcher.primary_dsn == PRIMARY
    assert watcher.routes.slot_owner_dsn == REPLICA


def test_missing_primary_route_is_rejected_before_pipeline_mutation(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    monkeypatch.delenv("CDC_PRIMARY_DSN", raising=False)
    touched = []

    class MissingPrimary:
        @property
        def route_policy(self):
            raise ValueError("CDC_PRIMARY_DSN is required")

    import cdc_flight.pipeline as pipeline

    monkeypatch.setattr(pipeline, "SourceConfig", MissingPrimary)
    monkeypatch.setattr(
        pipeline,
        "ReplicationConfig",
        lambda: touched.append("replication") or pytest.fail("late route validation"),
    )
    monkeypatch.setattr(
        pipeline,
        "DestinationConfig",
        lambda **_kwargs: touched.append("destination")
        or pytest.fail("late route validation"),
    )

    with pytest.raises(ValueError, match="CDC_PRIMARY_DSN"):
        pipeline.run()
    assert touched == []


def test_catalog_reads_replica_and_opens_primary_only_for_real_publication_change(
    monkeypatch,
):
    source = _standby_source(monkeypatch)
    routes = source.route_policy
    seen: list[str] = []
    connections: dict[str, _CatalogConnection] = {}

    def connect(watcher, *, dsn=None, **_kwargs):
        dsn = dsn or watcher.dsn
        seen.append(dsn)
        connection = _CatalogConnection(
            dsn,
            published=True,
        )
        connections.setdefault(dsn, connection)
        return connection

    monkeypatch.setattr(catalog_poll, "connect", connect)
    watcher = CatalogWatcher(
        dsn=REPLICA,
        primary_dsn=PRIMARY,
        routes=routes,
        publication="cdc_flight_pub",
        schema="app",
        include={"app.customers"},
        known={"app.customers": _relation()},
        replicated={"app.customers"},
        poll_seconds=0,
    )
    assert watcher.poll() == []
    assert seen == [REPLICA]

    seen.clear()
    def connect_unpublished(watcher, *, dsn=None, **_kwargs):
        dsn = dsn or watcher.dsn
        seen.append(dsn)
        connection = _CatalogConnection(dsn, published=False)
        connections[dsn] = connection
        return connection

    monkeypatch.setattr(catalog_poll, "connect", connect_unpublished)
    discovered = CatalogWatcher(
        dsn=REPLICA,
        primary_dsn=PRIMARY,
        routes=routes,
        publication="cdc_flight_pub",
        schema="app",
        include={"app.customers"},
        auto_discover=True,
        poll_seconds=0,
    )
    assert discovered.poll()
    assert seen == [REPLICA, PRIMARY]
    assert any("ALTER PUBLICATION" in sql for sql in connections[PRIMARY].sql)


def test_source_marker_heartbeat_uses_source_write_route(monkeypatch):
    source = _standby_source(monkeypatch)
    routes = source.route_policy
    seen: list[str] = []

    class MarkerConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, _params=None):
            assert "pg_logical_emit_message" in sql
            assert self is not None
            return _Result(row=(991,))

    import psycopg

    def connect(dsn, **_kwargs):
        seen.append(dsn)
        return MarkerConnection()

    monkeypatch.setattr(psycopg, "connect", connect)
    health = SourceHealth(
        dsn=routes.read_dsn,
        source_write_dsn=routes.source_write_dsn,
        slot_name="local_slot",
        primary_dsn=routes.source_write_dsn,
        source_marker=SourceMarker(prefix="cdc_flight"),
        standby_heartbeat=True,
        heartbeat_interval=1,
    )
    health._ingest(
        SlotSample(
            at=1,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    health._maybe_emit_standby_heartbeat()
    assert seen == [PRIMARY]
    assert health.summary()["standby_heartbeat_writes"] == 1


def test_standby_cannot_retain_stock_write_heartbeat_action(monkeypatch, tmp_path):
    source = _standby_source(monkeypatch)
    replication = ReplicationConfig(state_dir=tmp_path)
    props = build_properties(source, replication, routes=source.route_policy)
    assert props["heartbeat.action.query"] == ""
    assert props[STANDBY_HEARTBEAT_DISABLED_PROPERTY] == "true"
    assert props["heartbeat.interval.ms"] == "5000"
    with pytest.raises(UnsafeDebeziumProperty, match=r"heartbeat\.action\.query"):
        build_properties(
            source,
            replication,
            routes=source.route_policy,
            overrides={"heartbeat.action.query": HEARTBEAT_ACTION_QUERY},
        )

    primary = replace(source, primary_dsn_override=None)
    monkeypatch.setenv("CDC_SOURCE_ROLE", "primary")
    primary_props = build_properties(
        primary,
        ReplicationConfig(state_dir=tmp_path / "primary"),
        routes=primary.route_policy,
    )
    assert primary_props["heartbeat.action.query"] == HEARTBEAT_ACTION_QUERY
    assert primary_props[STANDBY_HEARTBEAT_DISABLED_PROPERTY] == "false"


def test_standby_wal_sql_has_no_unconditional_current_lsn():
    for sql in (_SLOT_SQL, _SLOT_SQL_FAST):
        assert_recovery_safe_wal_sql(sql)
        assert "CASE WHEN pg_is_in_recovery()" in " ".join(sql.split())
    with pytest.raises(ValueError, match="guard"):
        assert_recovery_safe_wal_sql("SELECT pg_current_wal_lsn()")


def test_local_slot_loss_is_a_stop_witness_after_a_real_stream_only():
    health = SourceHealth(
        dsn=REPLICA,
        slot_name="local_slot",
        detect_local_slot_failure=True,
    )
    health._ingest(
        SlotSample(at=1, exists=True, active=True, confirmed_pos=100)
    )
    assert health.local_slot_failure is None
    health._ingest(SlotSample(at=2, exists=False, active=False))
    witness = health.local_slot_failure
    assert witness is not None
    assert witness["kind"] == "lost"
    assert witness["recovery_required"] == "local_slot_repair_and_fenced_full_resnapshot"

    health._ingest(
        SlotSample(
            at=3,
            exists=True,
            active=False,
            wal_status="lost",
            invalidation_reason="rows_removed",
        )
    )
    assert health.local_slot_failure["kind"] == "invalidated"


def test_shutdown_order_seals_then_quiesces_then_retires_before_close():
    events: list[str] = []

    class Handler:
        record_count = 0
        batch_count = 0
        data_batch_count = 0
        skipped_count = 0
        busy = False
        error = None
        seconds_since_last_batch = 100
        resume_point = type("Resume", (), {"last_lsn": 0})()
        highest_source_lsn = 0
        cfg = type("Cfg", (), {"resnapshot": False})()

        def request_drain(self):
            events.append("drain_intent")

        def shutdown(self, *, reason):
            events.append("seal")

        def wait_for_quiescence(self, timeout):
            events.append("quiescent")
            return True

        def wait_for_internal_teardown(self, timeout):
            events.append("executor_retired")
            return True

        def drain_on_shutdown(self):
            return 0

        def snapshot_counts(self):
            return {}

        def stats(self):
            return {}

    class Engine:
        failure = None
        completed_success = True
        suppressed_message = None
        offset_flushes_verified = 0

        def run(self):
            return None

        def close(self, *, intentional):
            events.append("engine_closed")

    summary = run_engine_bounded(
        Engine(),
        Handler(),
        type(
            "Run",
            (),
            {
                    "min_records": 0,
                    "watermark_enabled": True,
                    "watermark_quiet_seconds": 0.01,
                    "idle_seconds": 0,
                "max_seconds": 2,
                "close_timeout": 1,
                "engine_thread_timeout": 1,
                "source_dark_seconds": 10,
                "source_probe_startup_seconds": 1,
                "engine_start_timeout": 1,
            },
        )(),
        engine_terminates_normally=True,
    )
    assert summary["ok"] is True
    assert events == ["drain_intent", "seal", "quiescent", "executor_retired", "engine_closed"]
    history = summary["shutdown_sequence_history"]
    assert history.index("callbacks_quiescent") < history.index("engine_closing")
