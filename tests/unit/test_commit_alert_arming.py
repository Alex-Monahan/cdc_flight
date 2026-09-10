from __future__ import annotations

import logging
import threading
import time

import duckdb
import pytest

from cdc_flight import commit_protocol, destination
from cdc_flight.applier import Applier
from cdc_flight.config import ServiceConfig
from cdc_flight.destination_fence import EpochFencedConnection
from cdc_flight.destination_lease import Lease
from cdc_flight.run_state import COMMIT_ACK
from cdc_flight.service_runtime import ServiceContext


def test_commit_alert_arm_waits_for_a_fenced_observability_writer(tmp_path, caplog):
    """The pre-arm must not lose to a concurrent run-state lease fence."""
    pipeline = "physical:commit-alert-arm"
    owner = "commit-alert-arm-owner"
    raw = duckdb.connect(str(tmp_path / "commit-alert-arm.duckdb"))
    destination.ensure_control_schema(raw, "_cdc_flight")
    lease = Lease(
        pipeline,
        owner_id=owner,
        service_id=owner,
        worker_generation=f"{owner}:generation",
        control_schema="_cdc_flight",
        ttl_seconds=30,
    )
    lease.acquire(raw)
    context = ServiceContext(
        service_id=owner,
        lease_id=lease.lease_id,
        worker_generation=lease.worker_generation,
        policy=ServiceConfig(),
    )
    context.bind(lease, raw)
    fenced = EpochFencedConnection(raw, lease, context)
    sink = destination.AlertSink(
        fenced, pipeline=pipeline, control_schema="_cdc_flight"
    )
    subject = object.__new__(Applier)
    subject.alerts = sink
    subject.pipeline = pipeline
    subject.control_schema = "_cdc_flight"
    subject.runner_id = owner
    subject.cfg = type("Config", (), {"commit_timeout": 30})()

    holder_ready = threading.Event()
    release_holder = threading.Event()
    holder_errors: list[BaseException] = []

    def hold_run_state_write() -> None:
        cursor = raw.cursor()
        try:
            with COMMIT_ACK.excluded():
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute(
                    "UPDATE _cdc_flight.lease SET renewed_at=current_timestamp "
                    "WHERE pipeline = ?",
                    [pipeline],
                )
                holder_ready.set()
                if not release_holder.wait(5):
                    holder_errors.append(TimeoutError("test holder was not released"))
                cursor.execute("ROLLBACK")
        except BaseException as exc:  # pragma: no cover - assertion below reports it
            holder_errors.append(exc)
        finally:
            cursor.close()

    holder = threading.Thread(target=hold_run_state_write)
    COMMIT_ACK.reset()
    holder.start()
    try:
        assert holder_ready.wait(2), holder_errors

        arm_errors: list[BaseException] = []
        arm_done = threading.Event()

        def arm() -> None:
            try:
                subject._arm_commit_timeout_alert(1)
            except BaseException as exc:  # pragma: no cover - assertion below reports it
                arm_errors.append(exc)
            finally:
                arm_done.set()

        arming = threading.Thread(target=arm)
        arming.start()
        # Without the fix, the alert cursor races the held lease update, returns
        # False, and the helper finishes without a durable alert. With the fix it
        # waits on the same exclusion as the run-state writer.
        time.sleep(0.1)
        release_holder.set()
        assert arm_done.wait(5), "the arming call did not finish"
        arming.join(1)
        holder.join(1)

        assert not holder_errors
        assert not arm_errors
        assert raw.execute(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE pipeline = ? AND code = 'commit_timeout'",
            [pipeline],
        ).fetchone()[0] == 1

        # The second (inner) arm observes the first durable row. It is a successful
        # idempotent ensure, not a missing alert.
        caplog.clear()
        with caplog.at_level(logging.CRITICAL, logger="cdc_flight.applier"):
            subject._arm_commit_timeout_alert(1)
        assert "could not durably arm" not in caplog.text

        # The inner arm runs while the applier's parent transaction is open. Once
        # the outer arm has succeeded, this is an idempotent read and must not try
        # to fence/write a second time against that transaction.
        raw.execute("BEGIN TRANSACTION")
        try:
            caplog.clear()
            with caplog.at_level(logging.CRITICAL, logger="cdc_flight.applier"):
                subject._arm_commit_timeout_alert(1)
            assert "could not durably arm" not in caplog.text
        finally:
            raw.execute("ROLLBACK")

        # A throwaway/non-service applier takes the inner commit watchdog too, so
        # the decorator must not skip the pre-arm merely because it has no service
        # watchdog.  This is the ordering that protects its shared parent handle.
        order: list[str] = []

        class WrapperSubject:
            service_context = None
            group = type("Group", (), {"units": [object()], "spill_commit_id": None})()
            _next_commit_id = 9

            def _arm_commit_timeout_alert(self, commit_id):
                order.append(f"arm:{commit_id}")

        def commit_body(self, trigger):
            order.append(f"body:{trigger}")
            return "committed"

        wrapped = commit_protocol._bounded_service_destination_operation(commit_body)
        assert wrapped(WrapperSubject(), "resnapshot") == "committed"
        assert order == ["arm:9", "body:resnapshot"]
    finally:
        release_holder.set()
        holder.join(2)
        sink.close()
        context.close()
        raw.close()
        COMMIT_ACK.reset()


def test_commit_alert_arm_fails_closed_without_an_independent_durable_row():
    class NoIndependentAlertSink:
        independent = False
        _sink = None

        def raise_alert_once(self, **_kwargs):
            return False

    subject = object.__new__(Applier)
    subject.alerts = NoIndependentAlertSink()
    subject.pipeline = "physical:commit-alert-arm-failure"
    subject.control_schema = "_cdc_flight"
    subject.runner_id = "commit-alert-arm-failure-owner"
    subject.cfg = type("Config", (), {"commit_timeout": 30})()

    COMMIT_ACK.reset()
    with pytest.raises(RuntimeError, match="refusing to enter a watchdog"):
        subject._arm_commit_timeout_alert(1)
    COMMIT_ACK.reset()
