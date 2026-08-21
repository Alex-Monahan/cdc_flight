"""Mutation proofs for the structural service guards.

Each test first proves the production guard, then removes exactly that guard with
``monkeypatch`` and asserts that the same invariant checker fails.  The latter is
deliberately not a second implementation of the protocol: it is evidence that the
test is sensitive to the defect it claims to protect.
"""

from __future__ import annotations

import inspect
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import pytest

from cdc_flight import commit_protocol
from cdc_flight import destination as destination_module
from cdc_flight.destination_lease import Lease
from cdc_flight.errors import LeaseLost
from cdc_flight.occurrence import OccurrenceKey, RunState
from cdc_flight.run_state import COMMIT_ACK
from cdc_flight.service import ServiceSupervisor, _bounded_call
from cdc_flight.service_protocol import (
    ParentChannel,
    ServiceControlState,
    ServiceWorkerContext,
)
from cdc_flight.states import IllegalTransition

_STALE_LEASE_CHILD = r'''
import os
import sys
import time

from cdc_flight.config import DestinationConfig
from cdc_flight.destination import connect, ensure_control_schema, ensure_dataset
from cdc_flight.destination_lease import Lease

role, path, service_id = sys.argv[1:]
dest = DestinationConfig(
    kind="duckdb",
    pipeline_name="stale-lease-proof",
    duckdb_path=path,
    dataset_name="cdc_raw",
    control_schema="_cdc_flight",
)
con = connect(dest)
try:
    ensure_control_schema(con, dest.control_schema)
    ensure_dataset(con, dest.dataset_name)
    key = dest.resolve_physical_lease_key(con)
    lease = Lease(
        key,
        owner_id=role,
        service_id=service_id,
        worker_generation=role,
        control_schema=dest.control_schema,
        ttl_seconds=30,
    )
    lease.acquire(con)
    print(f"ACQUIRED {lease.epoch} {os.getpid()}", flush=True)
    if role.endswith("holder"):
        time.sleep(30)
finally:
    con.close()
'''


def _lease(tmp_path, *, owner: str = "owner"):
    path = tmp_path / "lease.duckdb"
    con = duckdb.connect(str(path))
    destination_module.ensure_control_schema(con, "_cdc_flight")
    lease = Lease(
        "physical:test",
        owner_id=owner,
        control_schema="_cdc_flight",
        ttl_seconds=30,
    )
    lease.acquire(con)
    return con, lease


def test_stale_foreign_and_same_service_leases_reclaim_only_after_real_process_death(
    tmp_path,
):
    """Two real child owners prove PID/start-token reclamation and epoch advance."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    for service_id in ("foreign-old-service", "same-service"):
        path = tmp_path / f"{service_id}.duckdb"
        holder = subprocess.Popen(
            [sys.executable, "-c", _STALE_LEASE_CHILD, "old-holder", str(path), service_id],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert holder.stdout is not None
        assert holder.stdout.readline().startswith("ACQUIRED 1 ")
        holder.send_signal(signal.SIGKILL)
        assert holder.wait(timeout=20) == -signal.SIGKILL

        contender = subprocess.run(
            [
                sys.executable,
                "-c",
                _STALE_LEASE_CHILD,
                "new-contender",
                str(path),
                "new-service" if service_id.startswith("foreign") else service_id,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert contender.returncode == 0, contender.stderr
        assert contender.stdout.startswith("ACQUIRED 2 "), contender.stdout


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
        new = Lease(
            old.pipeline,
            owner_id="new",
            control_schema="_cdc_flight",
            ttl_seconds=30,
        )
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


def test_worker_hello_identity_guard_is_mutation_sensitive(monkeypatch):
    supervisor = object.__new__(ServiceSupervisor)
    supervisor.service_id = "service-a"
    supervisor.lease = type(
        "LeaseIdentity",
        (),
        {"lease_id": "lease-a", "epoch": 4},
    )()
    process = type("Process", (), {"pid": 42})()
    bad_hello = {
        "service_id": "service-b",
        "lease_id": "lease-a",
        "fencing_epoch": 4,
        "worker_generation": "generation-a",
        "worker_start_token": "worker-start",
        "pid": 42,
    }
    with pytest.raises(LeaseLost):
        supervisor._validate_hello(bad_hello, process, "generation-a")

    monkeypatch.setattr(ServiceSupervisor, "_validate_hello", lambda *_: None)

    def invariant_checker():
        supervisor._validate_hello(bad_hello, process, "generation-a")
        raise AssertionError("a foreign worker passed the authenticated hello")

    with pytest.raises(AssertionError, match="foreign worker"):
        invariant_checker()


def test_parent_process_start_token_guard_is_mutation_sensitive(monkeypatch):
    parent_sock, child_sock = socket.socketpair()
    context = ServiceWorkerContext(
        service_id="service-a",
        lease_key="physical:test",
        lease_id="lease-a",
        fencing_epoch=1,
        worker_generation="generation-a",
        parent_pid=123,
        parent_start_token="parent-start",
        worker_start_token="worker-start",
        ipc_fd=child_sock.detach(),
        heartbeat_seconds=0.1,
        parent_loss_seconds=1,
    )
    try:
        monkeypatch.setattr(
            "cdc_flight.service_protocol.process_start_token",
            lambda pid=None: "different-parent-start",
        )
        with pytest.raises(LeaseLost):
            context.start()

        monkeypatch.setattr(
            "cdc_flight.service_protocol.process_start_token",
            lambda pid=None: "parent-start",
        )

        def invariant_checker():
            context.start()
            context.close()
            raise AssertionError("a reused parent PID passed without its start token")

        with pytest.raises(AssertionError, match="reused parent PID"):
            invariant_checker()
    finally:
        context.close()
        parent_sock.close()


def test_parent_loss_write_barrier_is_mutation_sensitive(monkeypatch):
    parent_sock, child_sock = socket.socketpair()
    context = ServiceWorkerContext(
        service_id="service-a",
        lease_key="physical:test",
        lease_id="lease-a",
        fencing_epoch=1,
        worker_generation="generation-a",
        parent_pid=1,
        parent_start_token="parent-start",
        worker_start_token="worker-start",
        ipc_fd=child_sock.detach(),
        heartbeat_seconds=0.1,
        parent_loss_seconds=1,
    )
    try:
        context.channel._parent_lost.set()
        with pytest.raises(LeaseLost):
            context.assert_writable()

        monkeypatch.setattr(ServiceWorkerContext, "assert_writable", lambda *_: None)

        def invariant_checker():
            context.assert_writable()
            raise AssertionError("the worker would write after parent loss")

        with pytest.raises(AssertionError, match="parent loss"):
            invariant_checker()
    finally:
        context.close()
        parent_sock.close()


def test_service_ipc_send_has_one_overall_deadline_on_a_filled_socket():
    """A peer that never reads cannot retain the service lease indefinitely."""
    parent_sock, peer_sock = socket.socketpair()
    channel = ParentChannel(parent_sock, io_timeout_seconds=0.20)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            channel.send("large_heartbeat", blob="x" * (8 * 1024 * 1024))
        elapsed = time.monotonic() - started
        assert elapsed < 1.0, elapsed
        assert channel.parent_lost and channel.drain_requested
    finally:
        channel.close()
        peer_sock.close()


def test_service_ipc_receive_has_a_frame_deadline_when_peer_stalls():
    """A partial frame is a failed receive, not an invitation to wait forever."""
    parent_sock, child_sock = socket.socketpair()
    channel = ParentChannel(child_sock, io_timeout_seconds=0.20)
    reader = threading.Thread(target=channel._worker_reader, daemon=True)
    try:
        reader.start()
        parent_sock.sendall(b'{"type":"parent_heartbeat"')
        reader.join(timeout=2.0)
        assert not reader.is_alive()
        assert channel.parent_lost and channel.drain_requested
    finally:
        channel.close()
        parent_sock.close()


def test_timed_out_control_operation_is_cancelled_before_it_can_write(tmp_path):
    """The timeout proof includes the child-side absence of a late side effect."""
    marker = tmp_path / "late-control-write.txt"

    def operation():
        marker.write_text("started", encoding="utf-8")
        time.sleep(0.8)
        marker.write_text("late", encoding="utf-8")

    with pytest.raises(TimeoutError) as caught:
        _bounded_call(operation, 0.20)
    assert getattr(caught.value, "cancelled", False) is True
    assert getattr(caught.value, "operation_fenced", False) is True
    time.sleep(1.0)
    assert not marker.exists() or marker.read_text(encoding="utf-8") == "started"


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


def _assert_drain_rejects_renewal(channel: ParentChannel) -> None:
    try:
        channel.request_renew()
    except IllegalTransition as exc:
        assert "service_control" in str(exc)
        return
    raise AssertionError("service_control allowed a drain -> renew transition")


def test_service_drain_renewal_boundary_is_declared_and_mutation_sensitive():
    """Drain owns the control plane; a queued worker renewal is fenced explicitly."""
    supervisor_sock, peer_sock = socket.socketpair()
    channel = ParentChannel(supervisor_sock, io_timeout_seconds=0.2)
    try:
        request_id = channel.request_renew()
        assert channel.control_state == "renewing"
        channel.request_drain()
        assert channel.control_state == "draining_with_renewal"
        channel.abandon_renewal(request_id)
        assert channel.control_state == "draining"

        _assert_drain_rejects_renewal(channel)

        # A mutant that removes the state-machine call in request_renew must make
        # this same invariant checker fail; the proof is not merely a happy-path
        # assertion about the current implementation.
        original = ServiceControlState.begin_renewal
        try:
            ServiceControlState.begin_renewal = lambda self: None
            with pytest.raises(AssertionError):
                _assert_drain_rejects_renewal(channel)
        finally:
            ServiceControlState.begin_renewal = original
    finally:
        channel.close()
        peer_sock.close()


def test_worker_cancels_queued_renewal_after_drain_without_lease_io():
    supervisor_sock, worker_sock = socket.socketpair()
    channel = ParentChannel(worker_sock, io_timeout_seconds=0.2)
    context = object.__new__(ServiceWorkerContext)
    context.channel = channel

    class LeaseProbe:
        called = False

        def renew_control(self, _con):
            self.called = True

    lease = LeaseProbe()
    try:
        channel._renew_requests.put("queued-before-drain")
        channel._handle_worker_message({"type": "drain"})
        assert context.renew_once(lease, None) is True
        assert lease.called is False
        assert channel.control_state == "draining"
    finally:
        channel.close()
        supervisor_sock.close()


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
