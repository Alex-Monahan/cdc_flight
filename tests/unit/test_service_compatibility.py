"""Compatibility and liveness proofs for the service lease schema."""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import duckdb
import pytest

from cdc_flight import destination as destination_module
from cdc_flight import offsets
from cdc_flight.destination import ResumePoint
from cdc_flight.destination_lease import Lease
from cdc_flight.errors import EngineFailure, LeaseLost, OffsetUnusable


def _legacy_lease_table(con) -> None:
    con.execute("CREATE SCHEMA _cdc_flight")
    con.execute(
        """CREATE TABLE _cdc_flight.lease (
            pipeline VARCHAR PRIMARY KEY,
            owner_id VARCHAR NOT NULL,
            host VARCHAR,
            pid BIGINT,
            acquired_at TIMESTAMPTZ NOT NULL,
            renewed_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        )"""
    )
    con.execute(
        """INSERT INTO _cdc_flight.lease VALUES
        ('legacy-destination', 'old-batch-owner', 'legacy-host', NULL,
         current_timestamp, current_timestamp,
         current_timestamp + INTERVAL '0.35 seconds')"""
    )


def test_batch_lease_can_use_a_destination_created_before_service_schema(tmp_path):
    """The exact seven-column parent-branch table migrates without data loss."""
    con = duckdb.connect(str(tmp_path / "pre_service.duckdb"))
    try:
        _legacy_lease_table(con)
        destination_module.ensure_control_schema(con, "_cdc_flight")

        current = Lease(
            "legacy-destination",
            owner_id="new-batch-owner",
            control_schema="_cdc_flight",
            ttl_seconds=5,
        )
        with pytest.raises(LeaseLost):
            current.acquire(con)
        time.sleep(0.45)
        current.acquire(con)
        assert current.epoch == 2
        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='_cdc_flight' AND table_name='lease'"
            ).fetchall()
        }
        # The row assertion above is the compatibility proof; this check protects
        # against an accidental test that only happened to use a fresh table.
        assert current._row(con)[0] == "legacy-destination"
        assert current._row(con)[6] == "new-batch-owner"
        assert {"lease_id", "fencing_epoch", "service_id", "state"} <= columns
    finally:
        con.close()


def test_expired_control_renewal_cannot_report_success(tmp_path):
    con = duckdb.connect(str(tmp_path / "renewal.duckdb"))
    try:
        destination_module.ensure_control_schema(con, "_cdc_flight")
        lease = Lease(
            "renewal-destination",
            owner_id="service-owner",
            control_schema="_cdc_flight",
            ttl_seconds=0.20,
        )
        lease.acquire(con)
        time.sleep(0.35)
        with pytest.raises(LeaseLost):
            lease.renew_control(con)
    finally:
        con.close()


def test_service_offset_recheck_fails_when_a_live_offset_file_disappears(tmp_path):
    con = duckdb.connect(str(tmp_path / "offset-check.duckdb"))
    try:
        destination_module.ensure_control_schema(con, "_cdc_flight")
        point = ResumePoint(
            partition={"server": "cdcflight"},
            offset={"lsn": 100, "lsn_proc": 100},
            last_lsn=100,
            commit_id=1,
        )
        destination_module.write_resume_point(
            con,
            pipeline="offset-check",
            namespace="cdc-flight-engine",
            point=point,
            commit_id=1,
            offset_blob=None,
            offset_key_blob=None,
            control_schema="_cdc_flight",
        )
        with pytest.raises(OffsetUnusable, match="offset file"):
            offsets.verify_service_offset(
                con,
                pipeline="offset-check",
                namespace="cdc-flight-engine",
                offset_path=tmp_path / "missing-offsets.dat",
                control_schema="_cdc_flight",
            )
    finally:
        con.close()


def test_service_recheck_invariant_o_guard_is_mutation_sensitive(tmp_path, monkeypatch):
    """A confirmed source LSN ahead of durable state must stop a service run."""
    from cdc_flight import reconcile
    from cdc_flight.discovery_coordinator import LiveDiscoveryCoordinator

    con = duckdb.connect(str(tmp_path / "service-invariant-o.duckdb"))
    context = SimpleNamespace(assert_writable=lambda: None)
    coordinator = object.__new__(LiveDiscoveryCoordinator)
    coordinator.service_context = context
    coordinator.source = SimpleNamespace(dsn="postgresql://source")
    coordinator.replication = SimpleNamespace(
        slot_name="service-slot",
        offset_file=tmp_path / "offsets.dat",
    )
    coordinator.run_cfg = SimpleNamespace(jdbc_connect_timeout_seconds=1)
    coordinator.con = con
    coordinator.destination = SimpleNamespace(
        pipeline_name="service-invariant-o",
        control_schema="_cdc_flight",
    )
    coordinator.namespace = "cdc-flight-engine"
    coordinator.summary_extra = {}

    class Handler:
        _destination_operation_lock = threading.RLock()
        _quiescence = threading.Condition(_destination_operation_lock)
        _callback_sealed = False

    destination_module.ensure_control_schema(con, "_cdc_flight")
    point = ResumePoint(
        partition={"server": "cdcflight"},
        offset={"lsn": 100},
        last_lsn=100,
        commit_id=1,
    )
    destination_module.write_resume_point(
        con,
        pipeline="service-invariant-o",
        namespace="cdc-flight-engine",
        point=point,
        commit_id=1,
        offset_blob=None,
        offset_key_blob=None,
        control_schema="_cdc_flight",
    )
    monkeypatch.setattr(
        reconcile,
        "observe_slot",
        lambda *_args, **_kwargs: reconcile.SlotObservation(
            slot_exists=True,
            active=True,
            confirmed_flush_lsn=101,
            restart_lsn=90,
            system_identifier="system",
            timeline_id=1,
        ),
    )
    monkeypatch.setattr(
        destination_module,
        "read_slot_state",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "cdc_flight.discovery_coordinator.offsets.verify_service_offset",
        lambda *_args, **_kwargs: {"verified": True},
    )
    try:
        with pytest.raises(EngineFailure, match="Invariant O violation"):
            coordinator._service_recheck(Handler())
    finally:
        con.close()
