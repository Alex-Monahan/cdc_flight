from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import Lab, end, keyed

from cdc_flight import commit_protocol, destination
from cdc_flight.applier import Applier
from cdc_flight.commit_group import CommitResult
from cdc_flight.config import ServiceConfig
from cdc_flight.destination_fence import EpochFencedConnection
from cdc_flight.destination_lease import Lease
from cdc_flight.run_state import COMMIT_ACK
from cdc_flight.service_runtime import ServiceContext


def _commit_timeout_rows(con, pipeline: str):
    return con.execute(
        "SELECT code, context FROM _cdc_flight.alerts "
        "WHERE pipeline = ? AND code = 'commit_timeout' ORDER BY raised_at",
        [pipeline],
    ).fetchall()


def _outer_probe_subject(raw, pipeline: str, mode: str):
    sink = destination.AlertSink(raw, pipeline=pipeline, control_schema="_cdc_flight")
    subject = object.__new__(Applier)
    subject.alerts = sink
    subject.con = raw
    subject.pipeline = pipeline
    subject.control_schema = "_cdc_flight"
    subject.runner_id = f"{pipeline}-owner"
    subject.cfg = SimpleNamespace(commit_timeout=0)
    subject.group = SimpleNamespace(units=[object()], spill_commit_id=None)
    subject._next_commit_id = 1
    subject.service_context = object() if mode == "live-service" else None
    subject.prearm_commit_watchdog = mode == "opted-resnapshot"
    return subject, sink


def _test_subprocess_env():
    env = os.environ.copy()
    test_root = Path(__file__).resolve().parents[1]
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(test_root), env.get("PYTHONPATH")) if part
    )
    return env


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
            prearm_commit_watchdog = True
            group = type("Group", (), {"units": [object()], "spill_commit_id": None})()
            _next_commit_id = 9

            def _arm_commit_timeout_alert(self, commit_id):
                order.append(f"arm:{commit_id}")

        def commit_body(self, trigger):
            order.append(f"body:{trigger}")
            return CommitResult.COMMITTED

        wrapped = commit_protocol._bounded_service_destination_operation(commit_body)
        assert wrapped(WrapperSubject(), "resnapshot") is CommitResult.COMMITTED
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


@pytest.mark.parametrize(
    "mode",
    ["live-service", "opted-resnapshot"],
    ids=["outer-live-service", "outer-opted-resnapshot"],
)
def test_outer_prearm_is_retired_after_a_known_rollback(tmp_path, mode):
    """Both outer call-site paths retire their independent pre-arm on rollback."""
    pipeline = f"physical:commit-alert-rollback-{mode}"
    raw = duckdb.connect(
        str(tmp_path / f"outer-{mode}.duckdb"),
        config=destination.DUCKDB_CONNECT_CONFIG,
    )
    destination.ensure_control_schema(raw, "_cdc_flight")
    subject, sink = _outer_probe_subject(raw, pipeline, mode)

    def body(owner, _trigger):
        owner.con.execute("BEGIN TRANSACTION")
        owner.con.execute("ROLLBACK")
        raise RuntimeError("known rollback")

    try:
        wrapped = commit_protocol._bounded_service_destination_operation(body)
        with pytest.raises(RuntimeError, match="known rollback"):
            wrapped(subject, "probe")
        assert _commit_timeout_rows(raw, pipeline) == []
    finally:
        sink.close()
        raw.close()


@pytest.mark.parametrize(
    "failure_mode",
    ["inner_arm", "post_inner_arm"],
    ids=["inner-commit-group", "post-inner-rollback"],
)
def test_inner_prearm_is_retired_after_a_known_rollback(
    tmp_path, monkeypatch, failure_mode
):
    """The real inner owner clears after both early and post-apply rollbacks."""
    pipeline = f"physical:commit-alert-inner-rollback-{failure_mode}"
    box = Lab(
        tmp_path / f"inner-{failure_mode}.duckdb",
        pipeline=pipeline,
        commit_timeout=30,
    )
    records = [
        keyed("1", 1, 10, 1, "one"),
        end("1", 1, 11, {"app.customers": 1}),
    ]
    try:
        if failure_mode == "inner_arm":

            original_arm = box.applier._arm_commit_timeout_alert

            def arm_then_fail(commit_id):
                original_arm(commit_id)
                raise RuntimeError("known rollback after inner arm")

            monkeypatch.setattr(
                box.applier, "_arm_commit_timeout_alert", arm_then_fail
            )
        else:

            def fail_watchdog(_timeout, _commit_id):
                raise RuntimeError("known rollback after inner watchdog arm")

            monkeypatch.setattr(
                commit_protocol.self_heal, "commit_watchdog", fail_watchdog
            )

        with pytest.raises(RuntimeError):
            box.run(records)
        assert _commit_timeout_rows(box.con, pipeline) == []
    finally:
        box.close()


def test_post_commit_exception_retains_the_ambiguous_prearm(tmp_path):
    """An exception after COMMIT is not a known rollback and keeps the alert."""
    pipeline = "physical:commit-alert-ambiguous-post-commit"
    box = Lab(
        tmp_path / "ambiguous-post-commit.duckdb",
        pipeline=pipeline,
        commit_timeout=30,
    )

    def fail_ack(_record):
        raise RuntimeError("acknowledgement outcome is ambiguous")

    box.committer.markProcessed = fail_ack
    try:
        with pytest.raises(RuntimeError, match="ambiguous"):
            box.run(
                [
                    keyed("1", 1, 10, 1, "one"),
                    end("1", 1, 11, {"app.customers": 1}),
                ]
            )
        rows = _commit_timeout_rows(box.con, pipeline)
        assert len(rows) == 1
        assert json.loads(rows[0][1])["alert_identity"] == (
            "commit_timeout:occurrence:commit:1"
        )
    finally:
        box.close()


_OUTER_HARD_EXIT_SCRIPT = r'''
import os
import sys
from types import SimpleNamespace

import duckdb

from cdc_flight import commit_protocol, destination
from cdc_flight.applier import Applier

path, mode = sys.argv[1:]
pipeline = f"physical:commit-alert-hard-exit-{mode}"
raw = duckdb.connect(path, config=destination.DUCKDB_CONNECT_CONFIG)
destination.ensure_control_schema(raw, "_cdc_flight")
sink = destination.AlertSink(raw, pipeline=pipeline, control_schema="_cdc_flight")
subject = object.__new__(Applier)
subject.alerts = sink
subject.con = raw
subject.pipeline = pipeline
subject.control_schema = "_cdc_flight"
subject.runner_id = f"{pipeline}-owner"
subject.cfg = SimpleNamespace(commit_timeout=0)
subject.group = SimpleNamespace(units=[object()], spill_commit_id=None)
subject._next_commit_id = 1
subject.service_context = object() if mode == "live-service" else None
subject.prearm_commit_watchdog = mode == "opted-resnapshot"

def body(_owner, _trigger):
    os._exit(75)

wrapped = commit_protocol._bounded_service_destination_operation(body)
wrapped(subject, "hard-exit-probe")
'''


@pytest.mark.parametrize(
    "mode",
    ["live-service", "opted-resnapshot"],
    ids=["outer-live-service", "outer-opted-resnapshot"],
)
def test_outer_prearm_survives_a_simulated_hard_exit(tmp_path, mode):
    """The outer call-site record remains after the process exits with 75."""
    db_path = tmp_path / f"outer-hard-exit-{mode}.duckdb"
    result = subprocess.run(
        [sys.executable, "-c", _OUTER_HARD_EXIT_SCRIPT, str(db_path), mode],
        cwd=Path(__file__).resolve().parents[2],
        env=_test_subprocess_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 75, result.stderr
    raw = duckdb.connect(str(db_path), config=destination.DUCKDB_CONNECT_CONFIG)
    pipeline = f"physical:commit-alert-hard-exit-{mode}"
    try:
        rows = _commit_timeout_rows(raw, pipeline)
        assert len(rows) == 1
        assert json.loads(rows[0][1])["alert_identity"] == (
            "commit_timeout:occurrence:commit:1"
        )
    finally:
        raw.close()


_INNER_HARD_EXIT_SCRIPT = r'''
import contextlib
import os
import sys
from pathlib import Path

from cdc_flight import commit_protocol
from support.applier_lab import Lab, end, keyed

path = Path(sys.argv[1])
pipeline = "physical:commit-alert-hard-exit-inner"
box = Lab(path, pipeline=pipeline, commit_timeout=30)

@contextlib.contextmanager
def hard_exit_after_inner_arm(_timeout, _commit_id):
    os._exit(75)

commit_protocol.self_heal.commit_watchdog = hard_exit_after_inner_arm
box.run([
    keyed("1", 1, 10, 1, "one"),
    end("1", 1, 11, {"app.customers": 1}),
])
os._exit(99)
'''


def test_inner_commit_group_prearm_survives_a_simulated_hard_exit(tmp_path):
    """The real inner call site leaves its record for a hard process exit."""
    db_path = tmp_path / "inner-hard-exit.duckdb"
    result = subprocess.run(
        [sys.executable, "-c", _INNER_HARD_EXIT_SCRIPT, str(db_path)],
        cwd=Path(__file__).resolve().parents[2],
        env=_test_subprocess_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 75, result.stderr
    raw = duckdb.connect(str(db_path), config=destination.DUCKDB_CONNECT_CONFIG)
    try:
        rows = _commit_timeout_rows(raw, "physical:commit-alert-hard-exit-inner")
        assert len(rows) == 1
        assert json.loads(rows[0][1])["alert_identity"] == (
            "commit_timeout:occurrence:commit:1"
        )
    finally:
        raw.close()
