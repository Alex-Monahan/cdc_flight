"""§7.3 Round A MotherDuck durability lane."""

from __future__ import annotations

import contextlib
import subprocess
import time
import uuid

import duckdb
import pytest

from cdc_flight import catalog_support
from cdc_flight.naming import quote

pytestmark = [
    pytest.mark.motherduck,
    pytest.mark.e2e,
    pytest.mark.xdist_group("md_7_3"),
]


def _wait_for_md_sentinel(sandbox, process, con, table: str, sentinel: str, *, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                "NEVER_ARMED: MotherDuck topology child exited before the "
                f"ordinary data sentinel was durable: {sandbox.last_summary()}"
            )
        try:
            con.execute("FORCE CHECKPOINT")
            if con.execute(
                f"SELECT name FROM {table} WHERE name = ?", [sentinel]
            ).fetchall():
                return
        except duckdb.Error:
            pass
        time.sleep(0.5)
    if process.poll() is None:
        process.terminate()
    stdout, stderr = process.communicate(timeout=60)
    raise AssertionError(
        "NEVER_ARMED: MotherDuck ordinary data sentinel did not become visible; "
        f"returncode={process.returncode}, stdout={stdout[-4000:]}, stderr={stderr[-7000:]}"
    )


def _wait_for_exit(process, *, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise AssertionError(
                    "MotherDuck topology child failed before its fact was checked: "
                    f"returncode={process.returncode}, stdout={stdout[-4000:]}, "
                    f"stderr={stderr[-7000:]}"
                )
            return
        time.sleep(0.2)
    process.terminate()
    stdout, stderr = process.communicate(timeout=60)
    raise AssertionError(
        "MotherDuck topology child did not quiesce before its durable-fact check: "
        f"returncode={process.returncode}, stdout={stdout[-4000:]}, stderr={stderr[-7000:]}"
    )


def _source_partition_row(sandbox, parent: str, child: str) -> tuple:
    rows = sandbox.pg_query(
        catalog_support.PARTITION_SQL,
        ("cdc_flight_pub", ["app"], ["app"]),
    )
    matching = [row for row in rows if row[1] == parent and row[6] == child]
    assert len(matching) == 1, (parent, child, rows)
    return matching[0]


def _wait_for_md_fact(
    sandbox, process, con, control: str, pipeline: str, child: str, *, timeout: float
) -> tuple:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                "NEVER_ARMED: MotherDuck child exited before the catalog-owned "
                f"{child} attach fact was visible: {sandbox.last_summary()}"
            )
        try:
            con.execute("FORCE CHECKPOINT")
            rows = con.execute(
                f"SELECT transition, parent_oid, parent_relfilenode, "
                f"parent_relation_type_oid, child_table, child_oid, "
                f"child_relfilenode, child_relation_type_oid, partition_bound, "
                f"attachment_epoch, detection_lsn, durable_lsn, state "
                f"FROM {control}.partition_events "
                "WHERE pipeline = ? AND child_table = ? AND transition = ?",
                [pipeline, child, "partition_attached"],
            ).fetchall()
            if rows:
                return rows[0]
        except duckdb.Error:
            pass
        time.sleep(0.5)
    raise AssertionError(
        "NEVER_ARMED: MotherDuck never exposed the catalog-owned attach fact "
        f"for {child!r}; summary={sandbox.last_summary()}"
    )


def test_motherduck_partition_fact_is_applied_or_explicitly_pending(
    sandbox, motherduck_module_case
):
    case = motherduck_module_case
    token = uuid.uuid4().hex[:10]
    parent = f"p73_md_events_{token}"
    base_child = f"{parent}_base"
    drop_child = f"{parent}_drop"
    qparent = quote(parent)
    qbase = quote(base_child)
    qdrop = quote(drop_child)
    pipeline = sandbox.env["CDC_PIPELINE_NAME"]
    dsn = f"md:{case['database']}?motherduck_token={case['token']}"
    control = quote(case["control_schema"])
    target = f'{quote(case["dataset"])}.cdcflight_app_{parent}'
    common_env = {
        "CDC_DATASET": case["dataset"],
        "CDC_MD_DATABASE": case["database"],
        "CDC_CONTROL_SCHEMA": case["control_schema"],
        "MOTHERDUCK_TOKEN": case["token"],
        "motherduck_token": case["token"],
        "CDC_AUTO_DISCOVERY": "0",
        # SourceConfig.tables owns the schema prefix; CDC_TABLES receives bare names.
        "CDC_TABLES": parent,
        "CDC_CATALOG_POLL_SECONDS": "1",
        "CDC_CATALOG_DRAIN_SECONDS": "30",
        "CDC_CATALOG_MARKER": "1",
    }
    precondition_process = None
    topology_process = None
    try:
        sandbox.reseed()
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
            destination="motherduck",
            reset_state=True,
            max_seconds=300,
            idle_seconds=8,
            timeout=600,
            extra_env=common_env,
        )
        assert baseline["returncode"] == 0, baseline

        sentinel = f"p73-md-sentinel-{token}"
        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        precondition_process = sandbox.spawn(
            destination="motherduck",
            max_seconds=240,
            idle_seconds=45,
            snapshot_mode="no_data",
            extra_env=common_env,
            capture=True,
        )
        sandbox.wait_for_slot_active(process=precondition_process, timeout=90)
        sandbox.sql(
            f"INSERT INTO app.{qparent} (id, name, happened_at) VALUES "
            f"(92001, '{sentinel}', '2026-01-15T00:00:00+00:00')"
        )
        with duckdb.connect(dsn) as md:
            _wait_for_md_sentinel(
                sandbox, precondition_process, md, target, sentinel, timeout=150
            )
        precondition_process.terminate()
        precondition_process.communicate(timeout=60)
        precondition_process = None

        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        topology_process = sandbox.spawn(
            destination="motherduck",
            max_seconds=300,
            idle_seconds=90,
            snapshot_mode="no_data",
            extra_env=common_env,
            capture=True,
        )
        sandbox.wait_for_slot_active(process=topology_process, timeout=90)
        sandbox.sql(f"CREATE TABLE app.{qdrop} (LIKE app.{qparent} INCLUDING ALL)")
        sandbox.sql(
            f"ALTER TABLE app.{qparent} ATTACH PARTITION app.{qdrop} "
            "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"
        )
        source_edge = _source_partition_row(sandbox, parent, drop_child)
        with duckdb.connect(dsn) as md:
            attach_fact = _wait_for_md_fact(
                sandbox,
                topology_process,
                md,
                control,
                pipeline,
                drop_child,
                timeout=180,
            )
        sandbox.sql(f"DROP TABLE app.{qdrop}")
        _wait_for_exit(topology_process, timeout=240)
        topology_process = None

        with duckdb.connect(dsn) as md:
            md.execute("FORCE CHECKPOINT")
            facts = md.execute(
                f"SELECT transition, parent_oid, parent_relfilenode, "
                f"parent_relation_type_oid, child_table, child_oid, "
                f"child_relfilenode, child_relation_type_oid, "
                f"partition_bound, attachment_epoch, "
                f"detection_lsn, durable_lsn, state FROM {control}.partition_events "
                "WHERE pipeline = ? AND child_table = ? ORDER BY detection_lsn",
                [pipeline, drop_child],
            ).fetchall()
            if facts:
                assert [row[0] for row in facts] == [
                    "partition_attached",
                    "partition_dropped",
                ], facts
                attached = facts[0]
                assert attached == attach_fact
                assert attached[1:4] == source_edge[2:5]
                assert attached[5:8] == source_edge[7:10]
                assert attached[8] == source_edge[10]
                assert attached[9] == source_edge[12]
                assert all(row[-1] == "applied" for row in facts)
                assert all(row[-3] > 0 and row[-2] >= row[-3] for row in facts)
            else:
                summary = sandbox.last_summary()
                pending = summary.get("catalog_partition_pending") or []
                observation = summary.get("catalog_partition_observation") or {}
                assert pending or observation.get("state") == "pending", (
                    "MotherDuck topology fact disappeared without an applied row or "
                    f"explicit pending state: summary={summary}"
                )
    finally:
        for process in (topology_process, precondition_process):
            if process is not None and process.poll() is None:
                process.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    process.communicate(timeout=60)
        with contextlib.suppress(Exception):
            sandbox.sql(f"DROP TABLE IF EXISTS app.{quote(parent)} CASCADE")
