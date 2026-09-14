"""§7.3 Round A slow proof against the real PostgreSQL catalog.

The source DDL is committed while one stock Debezium child is active. The
destination assertions are limited to the catalog-owned fact; no child DML
absence is treated as evidence of a topology transition.
"""

from __future__ import annotations

import contextlib
import subprocess
import time
import uuid

import pytest

from cdc_flight import catalog_support
from cdc_flight.naming import quote

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _child_output(process) -> tuple[str, str]:
    if process.poll() is None:
        return "", ""
    stdout, stderr = process.communicate()
    return (stdout or "")[-5000:], (stderr or "")[-8000:]


def _wait_for_sentinel(sandbox, process, table: str, sentinel: str, *, timeout: float) -> None:
    """Complete the real post-snapshot data handshake before the DDL run."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = _child_output(process)
            rows = sandbox.duck_query(
                f"SELECT name FROM {table} WHERE name = ?", [sentinel]
            )
            if process.returncode != 0 or not rows:
                raise AssertionError(
                    "NEVER_ARMED: post-snapshot sentinel was not durably delivered; "
                    f"returncode={process.returncode}, rows={rows}, "
                    f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
                )
            return
        time.sleep(0.2)
    if process.poll() is None:
        process.terminate()
    stdout, stderr = process.communicate(timeout=60)
    raise AssertionError(
        "NEVER_ARMED: child did not reach the post-snapshot data-sentinel "
        f"boundary; returncode={process.returncode}\nstdout={stdout}\nstderr={stderr}"
    )


def _wait_for_exit(process, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = _child_output(process)
            if process.returncode != 0:
                raise AssertionError(
                    "the topology-observation child failed before its durable fact "
                    f"could be checked: returncode={process.returncode}\n"
                    f"stdout={stdout}\nstderr={stderr}"
                )
            return
        time.sleep(0.2)
    process.terminate()
    stdout, stderr = process.communicate(timeout=60)
    raise AssertionError(
        "the topology-observation child did not quiesce before the durable-fact "
        f"check; returncode={process.returncode}\nstdout={stdout}\nstderr={stderr}"
    )


def _partition_row(sandbox, parent: str, child: str) -> tuple:
    rows = sandbox.pg_query(
        catalog_support.PARTITION_SQL,
        ("cdc_flight_pub", ["app"], ["app"]),
    )
    matching = [row for row in rows if row[1] == parent and row[6] == child]
    assert len(matching) == 1, (parent, child, rows)
    return matching[0]


def test_committed_attach_detach_and_child_drop_are_catalog_facts(sandbox):
    sandbox.reseed()
    token = uuid.uuid4().hex[:10]
    parent = f"p73_events_{token}"
    base_child = f"{parent}_base"
    detach_child = f"{parent}_detach"
    drop_child = f"{parent}_drop"
    # SourceConfig.tables owns the schema prefix; CDC_TABLES receives bare names.
    captured_tables = parent
    common_env = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": captured_tables,
        "CDC_CATALOG_POLL_SECONDS": "1",
        "CDC_CATALOG_DRAIN_SECONDS": "30",
        "CDC_CATALOG_MARKER": "1",
    }
    target = sandbox.table(f"cdcflight_app_{parent}")
    precondition_process = None
    topology_process = None
    observed_facts = []

    def finish_transition(child_name: str, transition: str) -> tuple:
        nonlocal topology_process
        assert topology_process is not None
        _wait_for_exit(topology_process, timeout=180)
        topology_process = None
        rows = sandbox.duck_query(
            "SELECT transition, parent_oid, parent_relfilenode, "
            "parent_relation_type_oid, child_table, child_oid, child_relfilenode, "
            "child_relation_type_oid, partition_bound, attachment_epoch, "
            "detection_lsn, durable_lsn, state "
            "FROM _cdc_flight.partition_events "
            "WHERE pipeline = ? AND transition = ? AND child_table = ? "
            "ORDER BY detection_lsn",
            [sandbox.env["CDC_PIPELINE_NAME"], transition, child_name],
        )
        assert len(rows) == 1, (transition, child_name, rows, sandbox.last_summary())
        observed_facts.extend(rows)
        return rows[0]

    def start_topology_child() -> None:
        nonlocal topology_process
        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        topology_process = sandbox.spawn(
            max_seconds=180,
            idle_seconds=8,
            snapshot_mode="no_data",
            extra_env=common_env,
            capture=True,
        )
        sandbox.wait_for_slot_active(process=topology_process, timeout=74)

    try:
        qparent = quote(parent)
        qbase = quote(base_child)
        qdetach = quote(detach_child)
        qdrop = quote(drop_child)
        sandbox.sql(
            [
                f"CREATE TABLE app.{qparent} (id integer, name text, "
                "happened_at timestamptz, PRIMARY KEY (id, happened_at)) "
                "PARTITION BY RANGE (happened_at)",
                f"CREATE TABLE app.{qbase} (LIKE app.{qparent} INCLUDING ALL)",
                f"ALTER TABLE app.{qparent} ATTACH PARTITION app.{qbase} "
                "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{qparent}",
            ],
            one_transaction=True,
        )

        baseline = sandbox.run(
            reset_state=True,
            max_seconds=180,
            idle_seconds=8,
            extra_env=common_env,
        )
        assert baseline["returncode"] == 0, baseline

        sentinel = f"p73-sentinel-{token}"
        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        precondition_process = sandbox.spawn(
            max_seconds=180,
            idle_seconds=8,
            snapshot_mode="no_data",
            extra_env=common_env,
            capture=True,
        )
        sandbox.wait_for_slot_active(process=precondition_process, timeout=74)
        sandbox.sql(
            f"INSERT INTO app.{qparent} (id, name, happened_at) VALUES "
            f"(91001, '{sentinel}', '2026-01-15T00:00:00+00:00')"
        )
        _wait_for_sentinel(sandbox, precondition_process, target, sentinel, timeout=135)
        precondition_process = None

        # Start from the proven post-snapshot offset. Each committed transition is
        # discharged by the existing catalog watcher before the next DDL is issued;
        # local DuckDB's writer lock is released only at that durable boundary.
        start_topology_child()
        sandbox.sql(f"CREATE TABLE app.{qdetach} (LIKE app.{qparent} INCLUDING ALL)")
        sandbox.sql(
            f"ALTER TABLE app.{qparent} ATTACH PARTITION app.{qdetach} "
            "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"
        )
        attach_source = _partition_row(sandbox, parent, detach_child)
        attach_fact = finish_transition(detach_child, "partition_attached")

        start_topology_child()
        sandbox.sql(f"ALTER TABLE app.{qparent} DETACH PARTITION app.{qdetach}")
        assert sandbox.pg_query(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'app' AND c.relname = %s",
            (detach_child,),
        )
        assert sandbox.pg_query(
            "SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'app' AND c.relname = %s",
            (detach_child,),
        ) == []
        detach_fact = finish_transition(detach_child, "partition_detached")

        start_topology_child()
        sandbox.sql(f"CREATE TABLE app.{qdrop} (LIKE app.{qparent} INCLUDING ALL)")
        sandbox.sql(
            f"ALTER TABLE app.{qparent} ATTACH PARTITION app.{qdrop} "
            "FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')"
        )
        drop_source = _partition_row(sandbox, parent, drop_child)
        drop_attach_fact = finish_transition(drop_child, "partition_attached")

        start_topology_child()
        sandbox.sql(f"DROP TABLE app.{qdrop}")
        drop_fact = finish_transition(drop_child, "partition_dropped")

        pipeline = sandbox.env["CDC_PIPELINE_NAME"]
        facts = sandbox.duck_query(
            "SELECT transition, parent_oid, parent_relfilenode, "
            "parent_relation_type_oid, child_table, child_oid, child_relfilenode, "
            "child_relation_type_oid, partition_bound, attachment_epoch, "
            "detection_lsn, durable_lsn, state "
            "FROM _cdc_flight.partition_events "
            "WHERE pipeline = ? ORDER BY detection_lsn, child_table",
            [pipeline],
        )
        assert facts == observed_facts
        assert [row[0] for row in facts] == [
            "partition_attached",
            "partition_detached",
            "partition_attached",
            "partition_dropped",
        ], facts
        for fact in (attach_fact, detach_fact, drop_attach_fact, drop_fact):
            assert fact[1:4] == attach_source[2:5]
            assert fact[-1] == "applied"
            assert fact[-3] > 0
            assert fact[-2] >= fact[-3]
        assert attach_fact[5:8] == attach_source[7:10]
        assert attach_fact[8] == attach_source[10]
        assert attach_fact[9] == attach_source[12]
        assert drop_attach_fact[5:8] == drop_source[7:10]
        assert drop_attach_fact[8] == drop_source[10]
        assert drop_attach_fact[9] == drop_source[12]
        assert detach_fact[5:9] == attach_fact[5:9]
        assert detach_fact[9] == attach_fact[9]
        assert drop_fact[5:10] == drop_attach_fact[5:10]

        assert sandbox.pg_query(
            "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'app' AND c.relname = %s",
            (drop_child,),
        ) == []
        remaining = sandbox.pg_query(
            catalog_support.PARTITION_SQL,
            ("cdc_flight_pub", ["app"], ["app"]),
        )
        assert {row[6] for row in remaining if row[1] == parent} == {base_child}
    finally:
        for process in (topology_process, precondition_process):
            if process is not None and process.poll() is None:
                process.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.communicate(timeout=60)
        with contextlib.suppress(Exception):
            sandbox.sql(f"DROP TABLE IF EXISTS app.{quote(parent)} CASCADE")
