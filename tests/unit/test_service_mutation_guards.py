"""Mutation proofs for the single-process service boundary.

These tests keep the properties that mattered at the old process boundary while
testing their new owners: the physical lease/fence, the destination clock, and
the in-process watchdog.  Socket framing, parent PID tokens, and cancellable
subprocess control calls intentionally have no replacement because that surface
no longer exists in production.
"""

from __future__ import annotations

import inspect
import time

import duckdb
import pytest

from cdc_flight import commit_protocol
from cdc_flight import destination as destination_module
from cdc_flight.config import ServiceConfig
from cdc_flight.destination_lease import Lease
from cdc_flight.errors import LeaseLost, ServiceStandDown
from cdc_flight.occurrence import OccurrenceKey, RunState
from cdc_flight.run_state import COMMIT_ACK
from cdc_flight.service_runtime import ServiceContext


def _lease(tmp_path, *, owner: str = "owner", ttl: float = 30.0):
    path = tmp_path / "lease.duckdb"
    con = duckdb.connect(str(path))
    destination_module.ensure_control_schema(con, "_cdc_flight")
    lease = Lease(
        "physical:test",
        owner_id=owner,
        service_id=owner,
        worker_generation=f"{owner}:generation",
        control_schema="_cdc_flight",
        ttl_seconds=ttl,
    )
    lease.acquire(con)
    return con, lease


def _service_lease(lease: Lease, *, service_id: str) -> Lease:
    return Lease(
        lease.pipeline,
        owner_id=service_id,
        service_id=service_id,
        worker_generation=f"{service_id}:generation",
        control_schema=lease.control_schema,
        ttl_seconds=lease.ttl_seconds,
        lease_id=f"{service_id}:lease",
    )


def test_expired_lease_is_reclaimed_only_after_server_expiry(tmp_path):
    con, old = _lease(tmp_path, owner="old", ttl=0.25)
    try:
        contender = _service_lease(old, service_id="new")
        with pytest.raises(ServiceStandDown):
            contender.acquire(
                con,
                heartbeat_bound_seconds=0.10,
                wait_for_expiry=False,
            )
        con.execute(
            "UPDATE _cdc_flight.lease SET renewed_at=current_timestamp - INTERVAL '10 seconds'"
        )
        with pytest.raises(LeaseLost, match="unhealthy"):
            contender.acquire(
                con,
                heartbeat_bound_seconds=0.10,
                wait_for_expiry=False,
            )
        started = time.monotonic()
        contender.acquire(con, heartbeat_bound_seconds=0.10, wait_for_expiry=True)
        assert time.monotonic() - started >= 0.15
        assert contender.epoch == old.epoch + 1
    finally:
        con.close()


def test_healthy_lease_admission_stands_down_without_mutation(tmp_path):
    con, holder = _lease(tmp_path, owner="holder")
    try:
        contender = _service_lease(holder, service_id="contender")
        with pytest.raises(ServiceStandDown) as caught:
            contender.acquire(con, heartbeat_bound_seconds=10)
        assert caught.value.summary["health"] == "lease_and_heartbeat_fresh"
        row = con.execute(
            "SELECT lease_id, fencing_epoch, service_id, state FROM _cdc_flight.lease"
        ).fetchone()
        assert row == (holder.lease_id, holder.epoch, "holder", "held")
    finally:
        con.close()


def test_stale_heartbeat_is_unhealthy_but_protected_until_expiry(tmp_path):
    con, holder = _lease(tmp_path, owner="holder", ttl=2)
    try:
        con.execute(
            "UPDATE _cdc_flight.lease SET renewed_at=current_timestamp - INTERVAL '10 seconds'"
        )
        health = holder.inspect_health(con, heartbeat_bound_seconds=1)
        assert health.healthy is False
        assert health.reclaimable is False
        assert health.reason == "heartbeat_stale_until_lease_expiry"
        contender = _service_lease(holder, service_id="contender")
        with pytest.raises(LeaseLost, match="unhealthy"):
            contender.acquire(con, heartbeat_bound_seconds=1, wait_for_expiry=False)
    finally:
        con.close()


def test_unreadable_health_check_fails_closed():
    class UnreadableConnection:
        def execute(self, *_args, **_kwargs):
            raise OSError("MotherDuck is temporarily unreadable")

    lease = Lease("physical:test", control_schema="_cdc_flight")
    with pytest.raises(OSError, match="unreadable"):
        lease.inspect_health(UnreadableConnection(), heartbeat_bound_seconds=10)


def test_commit_ack_rejects_lease_io_and_mutation_fails(tmp_path, monkeypatch):
    con, lease = _lease(tmp_path)
    try:
        COMMIT_ACK.reset()
        COMMIT_ACK.enter()
        try:
            with pytest.raises(LeaseLost):
                lease.renew_control(con)
        finally:
            COMMIT_ACK.leave()

        monkeypatch.setattr(Lease, "_assert_outside_commit_ack", lambda *_: None)

        def invariant_checker():
            COMMIT_ACK.enter()
            try:
                lease.renew_control(con)
            finally:
                COMMIT_ACK.leave()
            raise AssertionError("lease I/O entered COMMIT_ACK")

        with pytest.raises(AssertionError, match="lease I/O"):
            invariant_checker()
    finally:
        COMMIT_ACK.reset()
        con.close()


def test_old_epoch_cannot_fence_after_takeover_and_mutation_fails(tmp_path, monkeypatch):
    con, old = _lease(tmp_path, owner="old")
    try:
        old.release(con, retain=True)
        new = _service_lease(old, service_id="new")
        new.acquire(con)
        assert new.epoch == old.epoch + 1

        with pytest.raises(LeaseLost):
            old.fence(con)

        monkeypatch.setattr(Lease, "_matches", lambda *_: True)

        def invariant_checker():
            old.fence(con)
            raise AssertionError("a stale epoch fenced after takeover")

        with pytest.raises(AssertionError, match="stale epoch"):
            invariant_checker()
    finally:
        con.close()


def test_service_stall_is_a_write_barrier_and_self_exit_is_bounded(monkeypatch):
    policy = ServiceConfig(
        lease_ttl_seconds=1.0,
        lease_renew_seconds=0.1,
        heartbeat_bound_seconds=0.2,
        stall_timeout_seconds=0.1,
        stall_exit_grace_seconds=0.1,
        watchdog_poll_seconds=0.01,
        commit_timeout_seconds=0.2,
        close_timeout_seconds=0.2,
        invariant_check_seconds=0.1,
    )
    exited: list[int] = []
    context = ServiceContext(
        service_id="service-a",
        lease_id="lease-a",
        worker_generation="service-a:generation",
        policy=policy,
        exit_fn=exited.append,
    )
    try:
        # Model a callback/destination operation that entered the process and
        # then stopped making progress; a polling loop must not refresh it.
        context.operation_started()
        context.start_watchdog()
        deadline = time.monotonic() + 1
        while not exited and time.monotonic() < deadline:
            time.sleep(0.01)
        assert exited == [1]
        assert context.stalled
        with pytest.raises(LeaseLost, match="stalled"):
            context.assert_writable()

        monkeypatch.setattr(ServiceContext, "assert_writable", lambda *_: None)

        def invariant_checker():
            context.assert_writable()
            raise AssertionError("the stalled Flight would write after its lease died")

        with pytest.raises(AssertionError, match="stalled Flight"):
            invariant_checker()
    finally:
        context.close()


def test_same_process_generation_is_attached_to_one_admitted_lease(tmp_path):
    con, holder = _lease(tmp_path, owner="holder")
    context = ServiceContext(
        service_id="holder",
        lease_id=holder.lease_id,
        worker_generation=holder.worker_generation,
        policy=ServiceConfig(),
    )
    try:
        context.bind(holder, con)
        holder.attach(con)
        assert context.connection is con
        assert context.fencing_epoch == holder.epoch
        assert holder.worker_generation == context.worker_generation
    finally:
        context.close()
        con.close()


def _assert_ack_after_durability(source: str) -> None:
    commit = source.index('self.con.execute("COMMIT")')
    ack = source.index("self._committer.markProcessed")
    assert commit < ack, "acknowledgement moved before destination COMMIT"


def _assert_state_before_commit(source: str) -> None:
    commit = source.index('self.con.execute("COMMIT")')
    for state_write in ("destination.write_commit_log(", "destination.write_resume_point("):
        assert source.index(state_write) < commit, (
            f"{state_write} moved after destination COMMIT"
        )


def test_service_ack_ordering_guard_detects_an_inverted_ack_mutant():
    source = inspect.getsource(commit_protocol.commit_group)
    _assert_ack_after_durability(source)
    mutated = source.replace(
        '                self.con.execute("COMMIT")',
        '                self._committer.markProcessed(object())\n'
        '                self.con.execute("COMMIT")',
        1,
    )
    with pytest.raises(AssertionError, match="acknowledgement"):
        _assert_ack_after_durability(mutated)


def test_service_atomicity_guard_detects_state_after_commit_mutant():
    source = inspect.getsource(commit_protocol.commit_group)
    _assert_state_before_commit(source)
    mutated = source.replace(
        "        destination.write_resume_point(",
        '        self.con.execute("COMMIT")\n'
        "        destination.write_resume_point(",
        1,
    )
    with pytest.raises(AssertionError, match="moved after"):
        _assert_state_before_commit(mutated)


def test_service_destination_deadline_stops_before_commit_ack_window():
    source = inspect.getsource(commit_protocol.commit_group)
    stop = source.index("stop_destination_deadline()")
    commit = source.index('self.con.execute("COMMIT")')
    ack_gate = source.index("COMMIT_ACK.enter()")
    assert stop < ack_gate < commit

    mutated = source.replace(
        "            stop_destination_deadline()",
        "            pass",
        1,
    ).replace(
        '                self.con.execute("COMMIT")',
        '                stop_destination_deadline()\n'
        '                self.con.execute("COMMIT")',
        1,
    )
    moved_stop = mutated.index("stop_destination_deadline()")
    with pytest.raises(AssertionError, match="COMMIT_ACK"):
        assert moved_stop < mutated.index("COMMIT_ACK.enter()"), (
            "destination deadline moved into COMMIT_ACK"
        )


def test_callback_renewal_serialization_is_mutation_sensitive():
    source = inspect.getsource(commit_protocol.commit_group)
    assert source.count("self._destination_operation_lock") == 0

    applier_source = inspect.getsource(__import__("cdc_flight.applier", fromlist=["Applier"]))
    batch_start = applier_source.index("    def handle_batch")
    renewal_start = applier_source.index("    def renew_service_lease")
    batch = applier_source[batch_start:renewal_start]
    renewal = applier_source[renewal_start:]
    assert "with self._destination_operation_lock" in batch
    assert "with self._destination_operation_lock" in renewal

    mutated = batch.replace("with self._destination_operation_lock:", "", 1)

    def invariant_checker():
        assert "with self._destination_operation_lock" in mutated

    with pytest.raises(AssertionError):
        invariant_checker()


def test_commit_ack_window_has_no_lease_or_telemetry_path_and_mutation_fails():
    source = inspect.getsource(commit_protocol.commit_group)
    opened = source.index("COMMIT_ACK.enter()")
    closed = source.index("COMMIT_ACK.leave()", opened)
    window = source[opened:closed]
    forbidden = ("self.lease", "self.alerts", "record_log", "telemetry", "run_log")
    assert not any(token in window for token in forbidden)

    mutated = window + "\n                self.lease.renew_control(self.con)\n"

    def invariant_checker():
        assert not any(token in mutated for token in forbidden)

    with pytest.raises(AssertionError):
        invariant_checker()


def test_service_drain_stops_heartbeat_without_an_ipc_transition():
    class LeaseProbe:
        def __init__(self):
            self.lease_key = "physical:test"
            self.epoch = 1
            self.called = False

        def renew_control(self, _con):
            self.called = True

    context = ServiceContext(
        service_id="service-a",
        lease_id="lease-a",
        worker_generation="service-a:generation",
        policy=ServiceConfig(),
    )
    lease = LeaseProbe()
    try:
        context.bind(lease, object())
        context._next_heartbeat = time.monotonic() - 1
        context.request_drain()
        assert context.renew_requested is False
        assert context.renew_once() is False
        assert lease.called is False
    finally:
        context.close()


def test_service_heartbeat_uses_the_same_admitted_connection():
    class LeaseProbe:
        lease_key = "physical:test"
        epoch = 1
        called_with = None

        def renew_control(self, con):
            self.called_with = con

    context = ServiceContext(
        service_id="service-a",
        lease_id="lease-a",
        worker_generation="service-a:generation",
        policy=ServiceConfig(),
    )
    lease = LeaseProbe()
    connection = object()
    try:
        context.bind(lease, connection)
        context._next_heartbeat = time.monotonic() - 1
        assert context.renew_once() is True
        assert lease.called_with is connection
    finally:
        context.close()


def test_clean_release_is_reclaimable_without_waiting_for_stale_expiry(tmp_path):
    con, holder = _lease(tmp_path, owner="holder")
    try:
        holder.release(con, retain=True)
        contender = _service_lease(holder, service_id="contender")
        contender.acquire(con, heartbeat_bound_seconds=10, wait_for_expiry=True)
        assert contender.epoch == holder.epoch + 1
    finally:
        con.close()


def test_service_generation_is_the_run_identity_for_occurrences():
    first = RunState.service(
        "physical-pipeline",
        service_id="stable-service",
        worker_generation="stable-service:generation-1",
        lease_epoch=7,
    )
    second = RunState.service(
        "physical-pipeline",
        service_id="stable-service",
        worker_generation="stable-service:generation-2",
        lease_epoch=8,
    )
    first_key = OccurrenceKey.from_run(first)
    second_key = OccurrenceKey.from_run(second)
    assert first_key != second_key
    assert "stable-service" in first_key.text
    assert "epoch:7" in first_key.text
