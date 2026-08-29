"""Rubric 1.7: committed-process crash cells and adversarial compositions.

This module is deliberately slow.  Every cell starts a real ``cdc-flight`` child and
the crash is either the production injector's ``os._exit`` or a parent-issued
``SIGKILL``.  A Python exception, a monkeypatched state value, and a mocked destination
are not acceptable evidence here.

The state journal is written by the running production objects only when this matrix is
armed.  It records the last observed recovery, ownership, marker, and completion-
watermark states before the child dies.  The assertions below still read the surviving
PostgreSQL slot, offsets file, destination, and control rows independently; the journal
is evidence of where the child died, not a substitute for those checks.
"""

from __future__ import annotations

import json
import os
import signal
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from support.fixtures import Sandbox
from support.motherduck_probe import connect as connect_motherduck
from support.tcp_relay import TcpRelay

from cdc_flight import faults

ROWS = 8
MATRIX_STATE = "crash_matrix_state.json"
NAMESPACE = "cdc-flight-engine"


@dataclass(frozen=True)
class Cell:
    name: str
    proves: str
    cut: str
    expected_recovery: str
    expected_ownership: str
    expected_marker: str
    expected_watermark: str
    recovery: bool = False
    inject: str | None = None
    compose: dict[str, str] | None = None
    max_seconds: float = 120
    prior_recovery: bool = False
    expected_shutdown: str = "open"
    expected_interruption_marker: str = "absent"


@dataclass(frozen=True)
class Destination:
    kind: str
    env: dict[str, str]
    dataset: str = "cdc_raw"
    control_schema: str = "_cdc_flight"


# One row is one committed crash cell.  The rows intentionally overlap the axes: the
# recovery rows prove the durable ladder while the ownership/marker/watermark rows prove
# the in-process boundaries that surround it.  No state is assigned by the test; the
# expected values are checked against the production state journal and surviving stores.
CELLS = (
    Cell(
        "recovery_requested",
        "journal intent and the full table obligation commit before destruction",
        "recovery_requested_recorded",
        "requested",
        "available",
        "none",
        "unarmed",
        recovery=True,
    ),
    Cell(
        "recovery_offsets_file_deleted",
        "the offsets file may disappear while the durable journal still names the work",
        "recovery_offsets_file_deleted_recorded",
        "offsets_file_deleted",
        "available",
        "none",
        "unarmed",
        recovery=True,
    ),
    Cell(
        "recovery_resume_point_deleted",
        "the destination resume row is deleted before the slot is touched",
        "recovery_resume_point_deleted_recorded",
        "resume_point_deleted",
        "available",
        "none",
        "unarmed",
        recovery=True,
    ),
    Cell(
        "recovery_armed",
        "the slot is gone only after the journal can resume the forced snapshot",
        "recovery_armed_recorded",
        "armed",
        "available",
        "none",
        "unarmed",
        recovery=True,
    ),
    Cell(
        "ownership_available",
        "a pre-engine crash leaves no destination callback owner to reclaim",
        "ownership_available",
        "absent",
        "available",
        "none",
        "unarmed",
    ),
    Cell(
        "ownership_attached",
        "consumer construction is protected by an attached destination owner",
        "ownership_attached",
        "absent",
        "attached",
        "none",
        "unarmed",
    ),
    Cell(
        "ownership_active",
        "only an activated owner may receive Debezium callbacks",
        "ownership_active",
        "absent",
        "active",
        "none",
        "unarmed",
    ),
    Cell(
        "ownership_callback_owned",
        "failed callback quiescence transfers terminal ownership before teardown",
        "ownership_callback_owned",
        "absent",
        "callback_owned",
        "none",
        "unarmed",
        inject="destination_hang:1",
        compose={"CDC_COMMIT_TIMEOUT": "300", "CDC_FAULT_HANG_SECONDS": "600"},
        max_seconds=20,
        expected_shutdown="callback_owned",
    ),
    Cell(
        "completion_marker_written",
        "a completion marker can be written without being mistaken for a durable destination commit",
        "completion_marker_written",
        "absent",
        "active",
        "written",
        "unarmed",
    ),
    Cell(
        "watermark_armed",
        "the run cannot claim completion merely because its source position was armed",
        "watermark_armed",
        "absent",
        "active",
        "written",
        "armed",
    ),
    Cell(
        "watermark_reached",
        "reached means the destination resume point is durably past the source marker",
        "watermark_reached",
        "absent",
        "active",
        "written",
        "reached",
    ),
    Cell(
        "shutdown_idle_marker_written",
        "the shutdown marker is durable on PostgreSQL before its slot acknowledgement",
        "shutdown_idle_marker_written",
        "absent",
        "active",
        "shutdown_idle_written",
        "reached",
        expected_shutdown="ack_pending",
    ),
    Cell(
        "shutdown_idle_marker_acknowledged",
        "the destination position is acknowledged before shutdown teardown proceeds",
        "shutdown_idle_marker_acknowledged",
        "absent",
        "active",
        "shutdown_idle_acknowledged",
        "reached",
        expected_shutdown="ack_pending",
    ),
)


# The durable recovery journal is deliberately retained after a real child death.  These
# cells start the next real child from that armed journal and then cut the later lifecycle
# edges which the one-dimensional rows cannot reach.  They are local because the cloud
# fixture already proves each destination edge independently; the cross-state reachability
# proof is a PostgreSQL/source ordering property shared by both destinations.
CROSS_STATE_CELLS = (
    Cell(
        "armed_recovery_ownership_available",
        "an armed recovery journal survives a pre-engine ownership cut",
        "ownership_available",
        "armed",
        "available",
        "none",
        "unarmed",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_ownership_attached",
        "an armed recovery journal survives applier attachment",
        "ownership_attached",
        "armed",
        "attached",
        "none",
        "unarmed",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_ownership_active",
        "an armed recovery journal survives callback activation",
        "ownership_active",
        "armed",
        "active",
        "none",
        "unarmed",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_completion_marker_written",
        "an armed recovery journal survives completion-marker publication",
        "completion_marker_written",
        "armed",
        "active",
        "written",
        "unarmed",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_watermark_armed",
        "an armed recovery journal survives completion-watermark arming",
        "watermark_armed",
        "armed",
        "active",
        "written",
        "armed",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_watermark_reached",
        "an armed recovery journal survives a reached completion watermark",
        "watermark_reached",
        "armed",
        "active",
        "written",
        "reached",
        prior_recovery=True,
    ),
    Cell(
        "armed_recovery_shutdown_marker_written",
        "an armed recovery journal survives a written shutdown idle marker",
        "shutdown_idle_marker_written",
        "armed",
        "active",
        "shutdown_idle_written",
        "reached",
        prior_recovery=True,
        expected_shutdown="ack_pending",
    ),
    Cell(
        "armed_recovery_shutdown_marker_acknowledged",
        "an armed recovery journal survives acknowledgement of the shutdown marker",
        "shutdown_idle_marker_acknowledged",
        "armed",
        "active",
        "shutdown_idle_acknowledged",
        "reached",
        prior_recovery=True,
        expected_shutdown="ack_pending",
    ),
)


def _state_path(box: Sandbox) -> Path:
    return box.state_dir / MATRIX_STATE


def _add_rows(box: Sandbox, tag: str) -> None:
    box.sql(
        [
            "SET synchronous_commit = on",
            "INSERT INTO app.customers (name, email) SELECT "
            f"'{tag}-c-' || i, '{tag}-c-' || i || '@example.com' "
            f"FROM generate_series(1, {ROWS}) i",
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
            f"'{tag.upper()}', i * 1.5, 'C' FROM generate_series(1, {ROWS}) i",
        ],
        one_transaction=True,
    )


def _destination_query(
    box: Sandbox,
    destination: Destination | None,
    statement: str,
    params: list | None = None,
    connection=None,
) -> list[tuple]:
    if destination is None:
        return box.duck_query(statement, params)
    owned = connection is None
    if owned:
        connection = connect_motherduck(
            destination.env["MOTHERDUCK_TOKEN"], destination.env["CDC_MD_DATABASE"]
        )
    try:
        return connection.execute(statement, params or []).fetchall()
    finally:
        if owned:
            connection.close()


def _source_query(
    box: Sandbox,
    statement: str,
    params: tuple | None = None,
    connection=None,
) -> list[tuple]:
    owned = connection is None
    if owned:
        connection = psycopg.connect(box.source.dsn, autocommit=True)
    try:
        return connection.execute(statement, params).fetchall()
    finally:
        if owned:
            connection.close()


def _destination_table(box: Sandbox, destination: Destination | None, name: str) -> str:
    if destination is None:
        return box.table(name)
    return f'"{destination.dataset}"."{name}"'


def _control_table(destination: Destination | None, name: str) -> str:
    schema = destination.control_schema if destination is not None else "_cdc_flight"
    return f'"{schema}"."{name}"'


def _advance_slot_past_new_rows(
    box: Sandbox,
    destination: Destination | None = None,
    connection=None,
    source_connection=None,
) -> None:
    durable_rows = _destination_query(
        box, destination,
        f"SELECT last_lsn FROM {_control_table(destination, 'debezium_offsets')} "
        "WHERE pipeline = ? AND namespace = ?",
        [box.env["CDC_PIPELINE_NAME"], NAMESPACE],
        connection=connection,
    )
    durable = int(durable_rows[0][0])
    _source_query(
        box,
        "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, pg_current_wal_lsn())",
        (box.slot,),
        source_connection,
    )
    confirmed = _source_query(
        box,
        "SELECT (confirmed_flush_lsn - '0/0')::bigint "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
        source_connection,
    )
    assert confirmed and int(confirmed[0][0]) > durable, (
        "the recovery cell did not create a real slot-ahead-of-destination state: "
        f"confirmed={confirmed!r}, durable={durable}"
    )


def _run_with_cut(
    box: Sandbox, cell: Cell, destination: Destination | None = None
) -> dict:
    env = {
        "CDC_CRASH_MATRIX_CUT": cell.cut,
        "CDC_CRASH_MATRIX_STATE": MATRIX_STATE,
        **(destination.env if destination is not None else {}),
    }
    if cell.inject:
        env["CDC_FAULT_INJECT"] = cell.inject
    if cell.compose:
        env.update(cell.compose)
    return box.run(
        destination=destination.kind if destination is not None else "duckdb",
        max_seconds=cell.max_seconds,
        timeout=190,
        expect_success=False,
        extra_env=env,
        matrix_arm=True,
    )


def _probe_survivor(
    box: Sandbox,
    tag: str,
    destination: Destination | None = None,
    connection=None,
    source_connection=None,
) -> dict:
    if destination is None:
        return _probe_survivor_details(
            box, tag, destination, None, source_connection
        )
    owned = connection is None
    if owned:
        connection = connect_motherduck(
            destination.env["MOTHERDUCK_TOKEN"], destination.env["CDC_MD_DATABASE"]
        )
    try:
        return _probe_survivor_details(
            box, tag, destination, connection, source_connection
        )
    finally:
        if owned:
            connection.close()


def _motherduck_probe_snapshot(
    box: Sandbox, tag: str, destination: Destination, connection
) -> dict:
    """Collect one cloud snapshot instead of paying one round trip per field."""
    pipeline = box.env["CDC_PIPELINE_NAME"]
    recovery_table = _control_table(destination, "recovery_state")
    offsets_table = _control_table(destination, "debezium_offsets")
    lease_table = _control_table(destination, "lease")
    customers_table = _destination_table(
        box, destination, "cdcflight_app_customers"
    )
    readings_table = _destination_table(
        box, destination, "cdcflight_app_sensor_readings"
    )
    statement = f"""
        SELECT
            (SELECT phase FROM {recovery_table}
             WHERE pipeline = ? AND namespace = ?
             ORDER BY updated_at DESC LIMIT 1),
            (SELECT last_lsn FROM {offsets_table}
             WHERE pipeline = ? AND namespace = ?
             ORDER BY updated_at DESC LIMIT 1),
            (SELECT count(*) FROM {recovery_table}
             WHERE pipeline = ? AND namespace = ?),
            (SELECT count(*) FROM {offsets_table}
             WHERE pipeline = ? AND namespace = ?),
            (SELECT count(*) FROM {lease_table}
             WHERE pipeline = ?),
            (SELECT count(*) FROM {customers_table}
             WHERE name LIKE ?),
            (SELECT count(*) FROM {readings_table}
             WHERE sensor_id = ?),
            (SELECT list(struct_pack(id := id, name := name, email := email)
                         ORDER BY id)
             FROM {customers_table} WHERE name LIKE ?),
            (SELECT list(struct_pack(
                         sensor_id := sensor_id,
                         value := CAST(value AS DOUBLE),
                         unit := unit) ORDER BY sensor_id, value, unit)
             FROM {readings_table} WHERE sensor_id = ?),
            (SELECT count(*) FROM {readings_table}
             WHERE sensor_id = ?),
            (SELECT count(DISTINCT cdcf_event_id) FROM {readings_table}
             WHERE sensor_id = ?)
    """
    params = [
        pipeline,
        NAMESPACE,
        pipeline,
        NAMESPACE,
        pipeline,
        NAMESPACE,
        pipeline,
        NAMESPACE,
        pipeline,
        f"{tag}-c-%",
        tag.upper(),
        f"{tag}-c-%",
        tag.upper(),
        tag.upper(),
        tag.upper(),
    ]
    row = _destination_query(
        box, destination, statement, params, connection=connection
    )[0]
    customer_values = [
        (item["id"], item["name"], item["email"])
        for item in (row[7] or [])
    ]
    reading_values = [
        (item["sensor_id"], item["value"], item["unit"])
        for item in (row[8] or [])
    ]
    return {
        "recovery": [(row[0],)] if row[0] is not None else [],
        "durable": [(row[1],)] if row[1] is not None else [],
        "control_rows": {
            "recovery_state": row[2],
            "debezium_offsets": row[3],
            "lease": row[4],
        },
        "destination_customers": row[5],
        "destination_readings": row[6],
        "destination_customer_values": customer_values,
        "destination_reading_values": reading_values,
        "event_ids": (row[9], row[10]),
    }


def _probe_survivor_details(
    box: Sandbox,
    tag: str,
    destination: Destination | None,
    connection,
    source_connection=None,
) -> dict:
    state = {}
    path = _state_path(box)
    if path.exists():
        state = json.loads(path.read_text())
    destination_snapshot = (
        _motherduck_probe_snapshot(box, tag, destination, connection)
        if destination is not None
        else None
    )
    if destination_snapshot is not None:
        recovery = destination_snapshot["recovery"]
        durable = destination_snapshot["durable"]
        control_rows = destination_snapshot["control_rows"]
    else:
        recovery = _destination_query(
            box, destination,
            f"SELECT phase FROM {_control_table(destination, 'recovery_state')} "
            "WHERE pipeline = ? AND namespace = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            [box.env["CDC_PIPELINE_NAME"], NAMESPACE],
            connection=connection,
        )
        durable = _destination_query(
            box, destination,
            f"SELECT last_lsn FROM {_control_table(destination, 'debezium_offsets')} "
            "WHERE pipeline = ? AND namespace = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            [box.env["CDC_PIPELINE_NAME"], NAMESPACE],
            connection=connection,
        )
    pipeline = box.env["CDC_PIPELINE_NAME"]
    if destination_snapshot is None:
        control_rows = {}
        for table in ("recovery_state", "debezium_offsets", "lease"):
            scope = "pipeline = ?"
            params = [pipeline]
            if table != "lease":
                scope += " AND namespace = ?"
                params.append(NAMESPACE)
            control_rows[table] = _destination_query(
                box,
                destination,
                f"SELECT count(*) FROM {_control_table(destination, table)} "
                f"WHERE {scope}",
                params,
                connection=connection,
            )[0][0]
    slot = _source_query(
        box,
        "SELECT (restart_lsn - '0/0')::bigint, "
        "(confirmed_flush_lsn - '0/0')::bigint, active "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
        source_connection,
    )
    source_customers = _source_query(
        box,
        "SELECT count(*) FROM app.customers WHERE name LIKE %s",
        (f"{tag}-c-%",),
        source_connection,
    )[0][0]
    if destination_snapshot is not None:
        destination_customers = destination_snapshot["destination_customers"]
    else:
        destination_customers = _destination_query(
            box, destination,
            f"SELECT count(*) FROM {_destination_table(box, destination, 'cdcflight_app_customers')} "
            "WHERE name LIKE ?",
            [f"{tag}-c-%"],
            connection=connection,
        )[0][0]
    source_readings = _source_query(
        box,
        "SELECT count(*) FROM app.sensor_readings WHERE sensor_id = %s",
        (tag.upper(),),
        source_connection,
    )[0][0]
    if destination_snapshot is not None:
        destination_readings = destination_snapshot["destination_readings"]
    else:
        destination_readings = _destination_query(
            box, destination,
            f"SELECT count(*) FROM "
            f"{_destination_table(box, destination, 'cdcflight_app_sensor_readings')} "
            "WHERE sensor_id = ?",
            [tag.upper()],
            connection=connection,
        )[0][0]
    source_customer_values = _source_query(
        box,
        "SELECT id, name, email FROM app.customers WHERE name LIKE %s ORDER BY id",
        (f"{tag}-c-%",),
        source_connection,
    )
    if destination_snapshot is not None:
        destination_customer_values = destination_snapshot[
            "destination_customer_values"
        ]
    else:
        destination_customer_values = _destination_query(
            box,
            destination,
            f"SELECT id, name, email FROM "
            f"{_destination_table(box, destination, 'cdcflight_app_customers')} "
            "WHERE name LIKE ? ORDER BY id",
            [f"{tag}-c-%"],
            connection=connection,
        )
    source_reading_values = _source_query(
        box,
        "SELECT sensor_id, value::double precision, unit "
        "FROM app.sensor_readings WHERE sensor_id = %s "
        "ORDER BY sensor_id, value, unit",
        (tag.upper(),),
        source_connection,
    )
    if destination_snapshot is not None:
        destination_reading_values = destination_snapshot[
            "destination_reading_values"
        ]
        event_ids = destination_snapshot["event_ids"]
    else:
        destination_reading_values = _destination_query(
            box,
            destination,
            f"SELECT sensor_id, CAST(value AS DOUBLE), unit FROM "
            f"{_destination_table(box, destination, 'cdcflight_app_sensor_readings')} "
            "WHERE sensor_id = ? ORDER BY sensor_id, value, unit",
            [tag.upper()],
            connection=connection,
        )
        event_ids = _destination_query(
            box, destination,
            f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
            f"{_destination_table(box, destination, 'cdcflight_app_sensor_readings')} "
            "WHERE sensor_id = ?",
            [tag.upper()],
            connection=connection,
        )[0]
    return {
        "state": state,
        "recovery_phase": recovery[0][0] if recovery else "absent",
        "durable_recovery_phase": recovery[0][0] if recovery else "absent",
        "offset_file": box.offset_file.exists(),
        "offset_file_bytes": box.offset_file.stat().st_size if box.offset_file.exists() else 0,
        "durable_lsn": durable[0][0] if durable else None,
        "control_rows": control_rows,
        "slot": slot[0] if slot else None,
        "source_customers": source_customers,
        "destination_customers": destination_customers,
        "source_readings": source_readings,
        "destination_readings": destination_readings,
        "destination_event_ids": event_ids,
        "identities": {
            "customers": {
                "source": [row[0] for row in source_customer_values],
                "destination": [row[0] for row in destination_customer_values],
            },
            # A keyless source has no primary-key identity. Its complete source row is
            # the identity set; the destination additionally carries cdcf_event_id,
            # checked separately below for replay uniqueness.
            "sensor_readings": {
                "source": [tuple(row) for row in source_reading_values],
                "destination": [tuple(row) for row in destination_reading_values],
            },
        },
        "values": {
            "customers": [tuple(row) for row in source_customer_values],
            "destination_customers": [tuple(row) for row in destination_customer_values],
            "sensor_readings": [tuple(row) for row in source_reading_values],
            "destination_sensor_readings": [
                tuple(row) for row in destination_reading_values
            ],
        },
    }


def _recover_and_probe(
    box: Sandbox,
    tag: str,
    destination: Destination | None = None,
    connection=None,
    source_connection=None,
) -> dict:
    recovered = box.run(
        destination=destination.kind if destination is not None else "duckdb",
        max_seconds=180,
        timeout=260,
        expect_success=False,
        extra_env=destination.env if destination is not None else None,
    )
    after = _probe_survivor(
        box, tag, destination, connection, source_connection
    )
    return {"run": recovered, "after": after}


def _wait_for_state(
    box: Sandbox, predicate, timeout: float = 90, filename: str = MATRIX_STATE
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        path = box.state_dir / filename
        if path.exists():
            state = json.loads(path.read_text())
            if predicate(state):
                return state
        time.sleep(0.05)
    raise AssertionError(
        f"timed out waiting for crash-matrix state in {box.state_dir / filename}"
    )


def _run_commit_window(
    box: Sandbox, tag: str, destination: Destination | None = None
) -> dict:
    """Crash after the destination commit and before Debezium can advance the slot."""
    _add_rows(box, tag)
    _state_path(box).unlink(missing_ok=True)
    crashed = box.run(
        destination=destination.kind if destination is not None else "duckdb",
        max_seconds=300 if destination is not None else 120,
        timeout=600 if destination is not None else 190,
        expect_success=False,
        extra_env={
            **(destination.env if destination is not None else {}),
            "CDC_CRASH_MATRIX_STATE": MATRIX_STATE,
            "CDC_FAULT_INJECT": "post_commit_pre_ack:1",
        },
        matrix_arm=True,
    )
    fired = box.fired_fault()
    survivor = _probe_survivor(box, tag, destination)
    resumed = _recover_and_probe(box, tag, destination)
    return {
        "crashed": crashed,
        "fired": fired,
        "survivor": survivor,
        "resumed": resumed,
    }


def _run_compositions(tmp_path_factory, postgres_cluster) -> dict[str, dict]:
    """Run overlapping real processes once and retain every survivor observation."""
    box = Sandbox(
        "real_crash_compositions",
        tmp_path_factory.mktemp("sbx_real_crash_compositions"),
        postgres_cluster,
    )
    relay = None
    results: dict[str, dict] = {}
    try:
        box.reseed()
        results["baseline"] = box.run(reset_state=True, max_seconds=150)

        # A second hard death is taken while the first crash's recovery obligation is
        # still durable.  This is the exact composition the single-anchor seam could
        # not reach: the second child starts from the first child's real survivor.
        tag = "r17_comp_recovery"
        _add_rows(box, tag)
        _advance_slot_past_new_rows(box)
        first = _run_with_cut(box, CELLS[1])
        first_fired = box.fired_fault()
        box.clear_fired_fault()
        first_survivor = _probe_survivor(box, tag)
        second = _run_with_cut(box, CELLS[2])
        second_fired = box.fired_fault()
        second_survivor = _probe_survivor(box, tag)
        resumed = _recover_and_probe(box, tag)
        results["crash_during_recovery"] = {
            "first": first,
            "first_fired": first_fired,
            "first_survivor": first_survivor,
            "second": second,
            "second_fired": second_fired,
            "second_survivor": second_survivor,
            "resumed": resumed,
        }

        # The destination transaction has committed, but the source slot has not been
        # advanced by Debezium.  This is a real os._exit at the narrow binding window,
        # with an independently persisted runtime label for the survivor probe.
        tag = "r17_comp_commit_window"
        results["commit_before_slot_advance"] = _run_commit_window(box, tag)

        # Blackhole the live source route, then kill the child from the parent while
        # packets are still being swallowed.  Healing the relay before the next run
        # proves the survivor can recover rather than merely noticing a closed socket.
        tag = "r17_comp_blackhole"
        _add_rows(box, tag)
        _state_path(box).unlink(missing_ok=True)
        relay = TcpRelay(postgres_cluster.host, postgres_cluster.port).start()
        relay_env = {
            "PGHOST": "127.0.0.1",
            "PGPORT": str(relay.port),
            "CDC_TEST_PGPORT": str(relay.port),
        }
        process = box.spawn(
            max_seconds=120,
            idle_seconds=6,
            capture=False,
            extra_env={
                **relay_env,
                "CDC_CRASH_MATRIX_STATE": MATRIX_STATE,
            },
            matrix_arm=True,
        )
        try:
            _wait_for_state(
                box,
                lambda state: state.get("context", {}).get("ownership") == "active",
            )
            deadline = time.monotonic() + 30
            while relay.bytes_relayed < 200_000 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert relay.bytes_relayed >= 200_000, relay.bytes_relayed
            relay.blackhole()
            assert relay.blackholed
            assert process.poll() is None
            os.kill(process.pid, signal.SIGKILL)
            blackhole_returncode = process.wait(timeout=30)
            blackhole_bytes = relay.bytes_relayed
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=30)
            relay.heal()
            relay.stop()
            relay = None
        blackhole_survivor = _probe_survivor(box, tag)
        blackhole_resumed = _recover_and_probe(box, tag)
        results["blackhole_then_sigkill"] = {
            "returncode": blackhole_returncode,
            "survivor": blackhole_survivor,
            "resumed": blackhole_resumed,
            "bytes_relayed": blackhole_bytes,
        }
        return results
    finally:
        if relay is not None:
            relay.stop()
        box.cleanup()
        box.reseed()


def _motherduck_destination(case: dict[str, str]) -> Destination:
    return Destination(
        "motherduck",
        {
            "CDC_DATASET": case["dataset"],
            "CDC_MD_DATABASE": case["database"],
            "CDC_CONTROL_SCHEMA": case["control_schema"],
            "MOTHERDUCK_TOKEN": case["token"],
            "motherduck_token": case["token"],
        },
        dataset=case["dataset"],
        control_schema=case["control_schema"],
    )


def _run_contention_motherduck(
    tmp_path_factory, postgres_cluster, case: dict[str, str]
) -> dict:
    """Hold one real recovery edge while a second MotherDuck writer races it."""
    destination = _motherduck_destination(case)
    box = Sandbox(
        "real_crash_contention_md",
        tmp_path_factory.mktemp("sbx_real_crash_contention_md"),
        postgres_cluster,
    )
    first = second = None
    gate = box.dir / "recovery_gate"
    first_state = box.state_dir / "contention_first.json"
    second_state = box.state_dir / "contention_second.json"
    try:
        box.reseed()
        box.run(
            reset_state=True,
            destination=destination.kind,
            max_seconds=300,
            timeout=600,
            extra_env=destination.env,
        )

        # This is the binding-window composition against the actual MotherDuck
        # server, not the local companion scenario below.
        commit_tag = "r17_comp_md_commit_window"
        commit_window = _run_commit_window(box, commit_tag, destination)

        tag = "r17_comp_contention"
        _add_rows(box, tag)
        _advance_slot_past_new_rows(box, destination)
        first = box.spawn(
            destination=destination.kind,
            max_seconds=120,
            idle_seconds=6,
            capture=True,
            extra_env={
                **destination.env,
                "CDC_CRASH_MATRIX_CUT": CELLS[1].cut,
                "CDC_CRASH_MATRIX_STATE": first_state.name,
                "CDC_CRASH_MATRIX_GATE": str(gate),
                "CDC_CRASH_MATRIX_GATE_TIMEOUT": "60",
            },
            matrix_arm=True,
        )
        _wait_for_state(
            box,
            lambda state: state.get("context", {}).get("recovery_phase")
            == "offsets_file_deleted",
            filename=first_state.name,
        )
        second = box.spawn(
            destination=destination.kind,
            max_seconds=20,
            idle_seconds=6,
            capture=True,
            extra_env={
                **destination.env,
                "CDC_CRASH_MATRIX_CUT": CELLS[2].cut,
                "CDC_CRASH_MATRIX_STATE": second_state.name,
            },
            matrix_arm=True,
        )
        second_output, _ = second.communicate(timeout=40)
        gate.touch()
        first_output, _ = first.communicate(timeout=40)
        survivor = _probe_survivor(box, tag, destination)
        resumed = _recover_and_probe(box, tag, destination)
        return {
            "commit_window": commit_window,
            "first_returncode": first.returncode,
            "first_output": first_output,
            "second_returncode": second.returncode,
            "second_output": second_output,
            "survivor": survivor,
            "resumed": resumed,
        }
    finally:
        if first is not None and first.poll() is None:
            gate.touch()
            first.kill()
            first.wait(timeout=30)
        if second is not None and second.poll() is None:
            second.kill()
            second.wait(timeout=30)
        box.cleanup()
        box.reseed()


def _run_cells(
    tmp_path_factory, postgres_cluster, destination: Destination | None = None
) -> dict[str, dict]:
    box = Sandbox(
        "real_crash_matrix",
        tmp_path_factory.mktemp("sbx_real_crash_matrix"),
        postgres_cluster,
    )
    results: dict[str, dict] = {}
    source_connection = None
    try:
        box.reseed()
        baseline = box.run(
            reset_state=True,
            destination=destination.kind if destination is not None else "duckdb",
            max_seconds=300 if destination is not None else 150,
            timeout=600 if destination is not None else 240,
            extra_env=destination.env if destination is not None else None,
        )
        results["baseline"] = baseline
        if destination is not None:
            source_connection = psycopg.connect(box.source.dsn, autocommit=True)
        for cell in CELLS:
            tag = f"r17_{cell.name}"
            box.clear_fired_fault()
            _state_path(box).unlink(missing_ok=True)
            _add_rows(box, tag)
            if cell.recovery:
                recovery_connection = connect_motherduck(
                    destination.env["MOTHERDUCK_TOKEN"], destination.env["CDC_MD_DATABASE"]
                )
                try:
                    _advance_slot_past_new_rows(
                        box, destination, recovery_connection, source_connection
                    )
                finally:
                    recovery_connection.close()
            try:
                killed = _run_with_cut(box, cell, destination)
                destination_connection = (
                    connect_motherduck(
                        destination.env["MOTHERDUCK_TOKEN"],
                        destination.env["CDC_MD_DATABASE"],
                    )
                    if destination is not None
                    else None
                )
                try:
                    survivor = _probe_survivor(
                        box,
                        tag,
                        destination,
                        destination_connection,
                        source_connection,
                    )
                    resumed = _recover_and_probe(
                        box,
                        tag,
                        destination,
                        destination_connection,
                        source_connection,
                    )
                finally:
                    if destination_connection is not None:
                        destination_connection.close()
                results[cell.name] = {
                    "cell": cell,
                    "tag": tag,
                    "killed": killed,
                    "survivor": survivor,
                    "resumed": resumed,
                    "fired": box.fired_fault(),
                }
            except Exception as exc:
                results[cell.name] = {
                    "cell": cell,
                    "tag": tag,
                    "error": f"{type(exc).__name__}: {exc}",
                }
        return results
    finally:
        if source_connection is not None:
            source_connection.close()
        box.cleanup()
        box.reseed()


def _seed_armed_recovery(
    box: Sandbox, tag: str, destination: Destination | None = None
) -> dict:
    """Leave a real armed recovery journal for the following cross-state child."""
    _add_rows(box, tag)
    _advance_slot_past_new_rows(box, destination)
    seed = _run_with_cut(box, CELLS[3], destination)
    fired = box.fired_fault()
    survivor = _probe_survivor(box, tag, destination)
    assert seed["returncode"] == faults.DEFAULT_EXIT_CODE, seed
    assert fired and fired["point"] == CELLS[3].cut, fired
    assert survivor["durable_recovery_phase"] == "armed", survivor
    assert survivor["control_rows"]["recovery_state"] == 1, survivor
    box.clear_fired_fault()
    return {"seed": seed, "fired": fired, "survivor": survivor}


def _run_cross_state_cells(tmp_path_factory, postgres_cluster) -> dict[str, dict]:
    """Run every declared armed-journal cross-state cell in real local children."""
    box = Sandbox(
        "real_cross_state_matrix",
        tmp_path_factory.mktemp("sbx_real_cross_state_matrix"),
        postgres_cluster,
    )
    results: dict[str, dict] = {}
    try:
        box.reseed()
        results["baseline"] = box.run(reset_state=True, max_seconds=150)
        for cell in CROSS_STATE_CELLS:
            tag = f"r17_{cell.name}"
            box.clear_fired_fault()
            _state_path(box).unlink(missing_ok=True)
            seed = _seed_armed_recovery(box, tag)
            try:
                killed = _run_with_cut(box, cell)
                survivor = _probe_survivor(box, tag)
                resumed = _recover_and_probe(box, tag)
                results[cell.name] = {
                    "cell": cell,
                    "tag": tag,
                    "seed": seed,
                    "killed": killed,
                    "survivor": survivor,
                    "resumed": resumed,
                    "fired": box.fired_fault(),
                }
            except Exception as exc:
                results[cell.name] = {
                    "cell": cell,
                    "tag": tag,
                    "seed": seed,
                    "error": f"{type(exc).__name__}: {exc}",
                }
        return results
    finally:
        box.cleanup()
        box.reseed()


def _assert_exact_identities_and_values(observation: dict, label: str) -> None:
    """Require source/destination identity sets and row values to be identical."""
    identities = observation["identities"]
    values = observation["values"]
    assert set(identities["customers"]["source"]) == set(
        identities["customers"]["destination"]
    ), f"{label} customer identity sets differ: {identities['customers']}"
    assert Counter(values["customers"]) == Counter(
        values["destination_customers"]
    ), f"{label} customer values differ: {values}"
    assert Counter(identities["sensor_readings"]["source"]) == Counter(
        identities["sensor_readings"]["destination"]
    ), f"{label} keyless source/destination identity rows differ: {identities}"
    assert Counter(values["sensor_readings"]) == Counter(
        values["destination_sensor_readings"]
    ), f"{label} keyless values differ: {values}"


def _assert_destination_is_a_source_subset(observation: dict, label: str) -> None:
    """Before restart, a real cut may have applied a prefix but never foreign rows."""
    identities = observation["identities"]
    values = observation["values"]
    assert set(identities["customers"]["destination"]) <= set(
        identities["customers"]["source"]
    ), f"{label} has a destination customer identity absent from source: {identities}"
    assert Counter(values["destination_customers"]) <= Counter(values["customers"]), (
        f"{label} has a destination customer value absent from source: {values}"
    )
    assert Counter(identities["sensor_readings"]["destination"]) <= Counter(
        identities["sensor_readings"]["source"]
    ), f"{label} has a foreign keyless row: {identities}"


def _assert_matrix_cell(result: dict, cell: Cell) -> None:
    """Apply the same survivor proof to local DuckDB and MotherDuck results."""
    assert "error" not in result, f"{cell.name} did not reach its assertion: {result}"
    killed = result["killed"]
    survivor = result["survivor"]
    resumed = result["resumed"]
    fired = result["fired"]
    assert killed["returncode"] == faults.DEFAULT_EXIT_CODE, (
        f"{cell.name} ({cell.proves}) was not an os._exit crash: {killed}"
    )
    assert fired and fired["point"] == cell.cut, (
        f"{cell.name} ({cell.proves}) fired the wrong cut: {fired!r}"
    )
    assert fired["pid"] != os.getpid(), "the cut must happen in the dying child"
    context = survivor["state"].get("context", {})
    assert context.get("recovery_phase") == cell.expected_recovery, survivor
    assert context.get("ownership") == cell.expected_ownership, survivor
    assert context.get("completion_marker_state") == cell.expected_marker, survivor
    assert context.get("watermark") == cell.expected_watermark, survivor
    assert context.get("interruption_marker") == cell.expected_interruption_marker, survivor
    assert context.get("shutdown_sequence") == cell.expected_shutdown, survivor
    if cell.expected_marker == "shutdown_idle_written":
        assert context.get("marker_lsn") is not None, survivor
        assert survivor["durable_lsn"] is not None, survivor
        assert context["marker_lsn"] > survivor["durable_lsn"], survivor
        assert survivor["slot"] is not None, survivor
        assert survivor["slot"][1] < context["marker_lsn"], (
            f"{cell.name} ({cell.proves}) acknowledged the shutdown marker before "
            f"the crash: {survivor}"
        )
    if cell.expected_marker == "shutdown_idle_acknowledged":
        assert context.get("marker_ack_target") is not None, survivor
        assert context.get("marker_ack_lsn") == context.get("marker_lsn"), survivor
        assert context.get("marker_ack_lsn") is not None, survivor
        assert survivor["slot"] is not None, survivor
        assert survivor["slot"][1] >= context["marker_ack_lsn"], (
            f"{cell.name} ({cell.proves}) lost the shutdown marker acknowledgement: "
            f"{survivor}"
        )
    assert survivor["source_customers"] == ROWS, survivor
    assert survivor["source_readings"] == ROWS, survivor
    assert 0 <= survivor["destination_customers"] <= ROWS, survivor
    assert 0 <= survivor["destination_readings"] <= ROWS, survivor
    _assert_destination_is_a_source_subset(survivor, cell.name)
    assert survivor["destination_event_ids"][0] == survivor["destination_event_ids"][1], (
        f"{cell.name} ({cell.proves}) duplicated a keyless event before restart: "
        f"{survivor['destination_event_ids']}"
    )
    assert survivor["control_rows"]["recovery_state"] == (
        1 if cell.recovery or cell.prior_recovery else 0
    ), (
        f"{cell.name} ({cell.proves}) left an unexpected recovery control row: "
        f"{survivor}"
    )
    if cell.recovery:
        expected_offset_rows = 1 if cell.expected_recovery in {
            "requested", "offsets_file_deleted"
        } else 0
        assert survivor["control_rows"]["debezium_offsets"] == expected_offset_rows, (
            f"{cell.name} ({cell.proves}) left the wrong durable resume-row count: "
            f"{survivor}"
        )
    elif cell.prior_recovery:
        assert survivor["durable_recovery_phase"] == "armed", survivor
        # The recovery obligation remains authoritative while a later engine may
        # recreate offsets.dat and/or a destination resume row for its snapshot.
        if survivor["slot"] is not None and survivor["durable_lsn"] is not None:
            assert survivor["slot"][1] <= survivor["durable_lsn"], survivor
    else:
        assert survivor["offset_file"], survivor
        assert survivor["control_rows"]["debezium_offsets"] == 1, survivor

    # The file/row/slot observations are independent of the state journal.  Invariant O
    # is checked whenever both positions exist; a recovery may intentionally remove the
    # slot or resume row while its journal is still the obligation.
    if cell.expected_recovery == "requested":
        assert survivor["offset_file"], survivor
    if (
        cell.expected_recovery in {"offsets_file_deleted", "resume_point_deleted", "armed"}
        and not cell.prior_recovery
    ):
        assert not survivor["offset_file"], survivor
    if cell.recovery:
        if cell.expected_recovery == "armed":
            assert survivor["slot"] is None, (
                f"{cell.name} ({cell.proves}) recorded armed before dropping the slot: "
                f"{survivor!r}"
            )
        else:
            assert survivor["slot"] is not None, survivor
            if cell.expected_recovery in {"requested", "offsets_file_deleted"}:
                assert survivor["durable_lsn"] is not None, survivor
                assert survivor["slot"][1] > survivor["durable_lsn"], (
                    f"{cell.name} ({cell.proves}) did not preserve the deliberately "
                    "slot-ahead recovery obligation: "
                    f"{survivor!r}"
                )
            else:
                assert survivor["durable_lsn"] is None, (
                    f"{cell.name} ({cell.proves}) retained the resume point after "
                    f"deleting it: {survivor!r}"
                )
    elif cell.prior_recovery:
        # The armed journal intentionally has no destination resume row.  A later
        # engine may already have created a slot, but Invariant O has no pair to
        # compare until the recovery obligation is discharged.
        assert survivor["control_rows"]["recovery_state"] == 1, survivor
    elif survivor["slot"] is not None and survivor["durable_lsn"] is not None:
        assert survivor["slot"][1] <= survivor["durable_lsn"], (
            f"{cell.name} ({cell.proves}) let the slot outrun the durable destination: "
            f"{survivor!r}"
        )

    recovered = resumed["run"]
    assert recovered["returncode"] == 0 and recovered.get("ok") is True, (
        f"{cell.name} did not resume cleanly after the real crash ({cell.proves}): "
        f"{recovered}"
    )
    after = resumed["after"]
    assert after["destination_customers"] == after["source_customers"] == ROWS, after
    assert after["destination_readings"] == after["source_readings"] == ROWS, after
    assert after["recovery_phase"] == "absent", (
        f"{cell.name} left a durable recovery obligation after restart: {after}"
    )
    assert after["slot"] is not None, f"{cell.name} did not leave a usable slot: {after}"
    assert after["durable_lsn"] is not None, after
    assert after["slot"][1] <= after["durable_lsn"], (
        f"{cell.name} violated Invariant O after restart: {after}"
    )
    _assert_exact_identities_and_values(after, f"{cell.name} after restart")
    assert after["destination_event_ids"][0] == after["destination_event_ids"][1], (
        f"{cell.name} ({cell.proves}) left duplicate keyless event identities: "
        f"{after['destination_event_ids']}"
    )


@pytest.fixture(scope="module")
def real_matrix(tmp_path_factory, postgres_cluster):
    return _run_cells(tmp_path_factory, postgres_cluster)


@pytest.mark.slow
@pytest.mark.parametrize("cell", CELLS, ids=lambda cell: cell.name)
def test_every_real_matrix_cell_kills_and_recovers(real_matrix, cell):
    """Every row names the invariant it proves and checks the real survivor twice."""
    _assert_matrix_cell(real_matrix[cell.name], cell)


@pytest.fixture(scope="module")
def real_matrix_motherduck(tmp_path_factory, postgres_cluster, motherduck_module_case):
    destination = _motherduck_destination(motherduck_module_case)
    return _run_cells(tmp_path_factory, postgres_cluster, destination)


@pytest.mark.motherduck
@pytest.mark.e2e
@pytest.mark.slow
def test_every_real_matrix_cell_kills_and_recovers_on_motherduck(real_matrix_motherduck):
    """The same committed-crash cells are observed in the real cloud destination."""
    for cell in CELLS:
        _assert_matrix_cell(real_matrix_motherduck[cell.name], cell)


@pytest.fixture(scope="module")
def armed_recovery_cross_state_matrix(tmp_path_factory, postgres_cluster):
    return _run_cross_state_cells(tmp_path_factory, postgres_cluster)


@pytest.mark.slow
@pytest.mark.parametrize("cell", CROSS_STATE_CELLS, ids=lambda cell: cell.name)
def test_armed_recovery_cross_state_cells_kill_and_recover(
    armed_recovery_cross_state_matrix, cell
):
    """A prior armed journal is a real survivor precondition for later cuts."""
    _assert_matrix_cell(armed_recovery_cross_state_matrix[cell.name], cell)


@pytest.fixture(scope="module")
def composed_faults(tmp_path_factory, postgres_cluster):
    return _run_compositions(tmp_path_factory, postgres_cluster)


def _assert_composed_recovery(result: dict) -> None:
    recovered = result["resumed"]
    assert recovered["run"]["returncode"] == 0
    assert recovered["run"].get("ok") is True, recovered["run"]
    after = recovered["after"]
    assert after["destination_customers"] == after["source_customers"] == ROWS, after
    assert after["destination_readings"] == after["source_readings"] == ROWS, after
    _assert_exact_identities_and_values(after, "composed recovery")
    assert after["destination_event_ids"][0] == after["destination_event_ids"][1], after
    assert after["recovery_phase"] == "absent", after
    assert after["control_rows"]["recovery_state"] == 0, after
    assert after["control_rows"]["debezium_offsets"] == 1, after
    assert after["slot"] is not None and after["durable_lsn"] is not None, after
    assert after["slot"][1] <= after["durable_lsn"], after


@pytest.mark.slow
def test_real_composed_faults_are_lossless_and_single_writer(composed_faults):
    """Overlapping crashes exercise the edges that no single-anchor test can reach."""
    recovery = composed_faults["crash_during_recovery"]
    assert recovery["first"]["returncode"] == faults.DEFAULT_EXIT_CODE, recovery
    assert recovery["second"]["returncode"] == faults.DEFAULT_EXIT_CODE, recovery
    assert recovery["first_fired"]["point"] == CELLS[1].cut, recovery
    assert recovery["second_fired"]["point"] == CELLS[2].cut, recovery
    assert recovery["first_survivor"]["recovery_phase"] == "offsets_file_deleted"
    assert recovery["second_survivor"]["recovery_phase"] == "resume_point_deleted"
    assert recovery["first_survivor"]["control_rows"]["recovery_state"] == 1
    assert recovery["second_survivor"]["control_rows"]["recovery_state"] == 1
    _assert_composed_recovery(recovery)

    window = composed_faults["commit_before_slot_advance"]
    assert window["crashed"]["returncode"] == faults.DEFAULT_EXIT_CODE, window
    assert window["fired"]["point"] == "post_commit_pre_ack", window
    assert window["survivor"]["destination_customers"] == ROWS, window
    assert window["survivor"]["destination_readings"] == ROWS, window
    assert window["survivor"]["durable_lsn"] is not None, window
    assert window["survivor"]["slot"][1] <= window["survivor"]["durable_lsn"], window
    _assert_destination_is_a_source_subset(window["survivor"], "commit-window survivor")
    _assert_composed_recovery(window)

    blackhole = composed_faults["blackhole_then_sigkill"]
    assert blackhole["returncode"] == -signal.SIGKILL, blackhole
    assert blackhole["bytes_relayed"] >= 200_000, blackhole
    assert blackhole["survivor"]["state"]["pid"] != os.getpid(), blackhole
    assert blackhole["survivor"]["state"]["context"]["ownership"] == "active", blackhole
    _assert_composed_recovery(blackhole)


@pytest.fixture(scope="module")
def motherduck_contention(tmp_path_factory, postgres_cluster, motherduck_module_case):
    return _run_contention_motherduck(
        tmp_path_factory, postgres_cluster, motherduck_module_case
    )


@pytest.mark.motherduck
@pytest.mark.e2e
@pytest.mark.slow
def test_real_recovery_paths_contend_for_one_motherduck_control_row(motherduck_contention):
    """A live recovery writer excludes a second writer before it can mutate control."""
    result = motherduck_contention
    window = result["commit_window"]
    assert window["crashed"]["returncode"] == faults.DEFAULT_EXIT_CODE, window
    assert window["fired"]["point"] == "post_commit_pre_ack", window
    assert window["survivor"]["destination_customers"] == ROWS, window
    assert window["survivor"]["destination_readings"] == ROWS, window
    assert window["survivor"]["durable_lsn"] is not None, window
    assert window["survivor"]["slot"][1] <= window["survivor"]["durable_lsn"], window
    _assert_destination_is_a_source_subset(window["survivor"], "MotherDuck commit-window survivor")
    _assert_composed_recovery(window)

    assert result["first_returncode"] == faults.DEFAULT_EXIT_CODE, result
    assert result["second_returncode"] != 0, result
    assert "already leased" in result["second_output"].lower(), result
    assert result["survivor"]["recovery_phase"] == "offsets_file_deleted", result
    assert result["survivor"]["control_rows"]["recovery_state"] == 1, result
    _assert_composed_recovery(result)


@pytest.mark.slow
def test_real_matrix_has_a_real_sigkill_cell(tmp_path_factory, postgres_cluster):
    """A parent-issued SIGKILL is a separate proof from the injector's os._exit."""
    box = Sandbox(
        "real_sigkill_matrix",
        tmp_path_factory.mktemp("sbx_real_sigkill_matrix"),
        postgres_cluster,
    )
    process = None
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _add_rows(box, "r17_sigkill")
        _state_path(box).unlink(missing_ok=True)
        process = box.spawn(
            max_seconds=120,
            idle_seconds=6,
            capture=False,
            extra_env={
                "CDC_CRASH_MATRIX_STATE": MATRIX_STATE,
            },
            matrix_arm=True,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and process.poll() is None:
            if _state_path(box).exists():
                state = json.loads(_state_path(box).read_text())
                if state.get("context", {}).get("watermark") == "armed":
                    break
            time.sleep(0.05)
        assert process.poll() is None, "the child finished before the SIGKILL cut"
        os.kill(process.pid, signal.SIGKILL)
        returncode = process.wait(timeout=30)
        assert returncode == -signal.SIGKILL, returncode
        state = json.loads(_state_path(box).read_text())
        assert state["context"]["watermark"] == "armed", state
        recovered = box.run(max_seconds=180, timeout=260)
        assert recovered["ok"] is True, recovered
        after = _probe_survivor(box, "r17_sigkill")
        assert after["destination_customers"] == after["source_customers"] == ROWS, after
        assert after["destination_readings"] == after["source_readings"] == ROWS, after
        _assert_exact_identities_and_values(after, "SIGKILL after restart")
        assert after["destination_event_ids"][0] == after["destination_event_ids"][1], after
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        box.cleanup()
        box.reseed()
