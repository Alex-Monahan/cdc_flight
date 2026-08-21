"""Authenticated local supervisor/worker protocol for service mode.

The protocol is deliberately tiny and synchronous at the commit boundary.  The
worker owns the data connection and the parent owns the physical lease.  A worker
must receive the parent's acknowledgement of ``commit_prepare`` before it opens
``COMMIT_ACK``; the parent then defers lease renewal until ``commit_complete``.
No database operation is performed by this module.
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import select
import socket
import threading
import time
import uuid

from . import faults
from .errors import LeaseLost
from .machines import (
    SERVICE_CONTROL,
    SERVICE_CONTROL_ACTIVE,
    SERVICE_CONTROL_CLOSED,
    SERVICE_CONTROL_DRAINING,
    SERVICE_CONTROL_DRAINING_WITH_RENEWAL,
    SERVICE_CONTROL_RENEWING,
)
from .states import IllegalTransition

__all__ = [
    "ParentChannel",
    "ServiceControlState",
    "ServiceWorkerContext",
    "process_start_token",
]


def process_start_token(pid: int | None = None) -> str:
    """Return a PID-reuse-resistant local process identity.

    macOS and Linux both provide a kernel-backed start value through ``ps``.  The
    fallback is a process-local nonce and is therefore only suitable for proving
    identity to the process that issued it; it is still safer than PID alone and
    is persisted in the lease row so a PID reuse cannot silently match a token.
    """

    pid = int(os.getpid() if pid is None else pid)
    try:
        import subprocess

        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True,
            text=True,
            check=True,
            timeout=2,
        )
        start = result.stdout.strip()
        if start:
            return f"{socket.gethostname()}:{pid}:{start}"
    except (OSError, subprocess.SubprocessError):
        pass
    return f"{socket.gethostname()}:{pid}:nonce:{uuid.uuid4().hex}"


DEFAULT_IPC_TIMEOUT_SECONDS = 5.0


class ServiceControlState:
    """Serialize the renewal/drain boundary for one worker generation.

    The socket reader and the worker/supervisor loop are separate threads.  A
    boolean drain flag cannot say whether a renewal was already authorized when
    drain arrived, so both sides use this one guarded state owner.  The machine
    intentionally has no ``draining -> renewing`` edge; callers that attempt it
    receive :class:`IllegalTransition`.
    """

    def __init__(self) -> None:
        self._state = SERVICE_CONTROL.initial
        self._lock = threading.RLock()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def drain_started(self) -> bool:
        return self.state in {
            SERVICE_CONTROL_DRAINING_WITH_RENEWAL,
            SERVICE_CONTROL_DRAINING,
            SERVICE_CONTROL_CLOSED,
        }

    @property
    def renewal_in_flight(self) -> bool:
        return self.state in {
            SERVICE_CONTROL_RENEWING,
            SERVICE_CONTROL_DRAINING_WITH_RENEWAL,
        }

    @property
    def renewal_allowed(self) -> bool:
        return self.state == SERVICE_CONTROL_ACTIVE

    def _to(self, state: str) -> None:
        SERVICE_CONTROL.check(self._state, state)
        self._state = state

    def begin_renewal(self) -> None:
        """Authorize one renewal dispatch, rejecting every drain state."""
        with self._lock:
            self._to(SERVICE_CONTROL_RENEWING)

    def begin_drain(self) -> None:
        """Enter drain, preserving whether a renewal was already in flight."""
        with self._lock:
            if self._state == SERVICE_CONTROL_ACTIVE:
                self._to(SERVICE_CONTROL_DRAINING)
            elif self._state == SERVICE_CONTROL_RENEWING:
                self._to(SERVICE_CONTROL_DRAINING_WITH_RENEWAL)
            elif self._state in {
                SERVICE_CONTROL_DRAINING_WITH_RENEWAL,
                SERVICE_CONTROL_DRAINING,
            }:
                self._to(self._state)
            elif self._state == SERVICE_CONTROL_CLOSED:
                raise IllegalTransition(
                    "service_control: closed cannot begin drain"
                )

    def resolve_renewal(self) -> None:
        """Resolve a renewal response or its bounded cancellation/fence."""
        with self._lock:
            if self._state == SERVICE_CONTROL_RENEWING:
                self._to(SERVICE_CONTROL_ACTIVE)
            elif self._state == SERVICE_CONTROL_DRAINING_WITH_RENEWAL:
                self._to(SERVICE_CONTROL_DRAINING)
            else:
                raise IllegalTransition(
                    "service_control: renewal resolved without an in-flight renewal"
                )

    def fence_unanswered_renewal(self) -> None:
        """Record a worker fence when an in-flight response cannot arrive."""
        with self._lock:
            if self._state == SERVICE_CONTROL_RENEWING:
                self._to(SERVICE_CONTROL_DRAINING_WITH_RENEWAL)
            if self._state == SERVICE_CONTROL_DRAINING_WITH_RENEWAL:
                self._to(SERVICE_CONTROL_DRAINING)
            elif self._state not in {
                SERVICE_CONTROL_ACTIVE,
                SERVICE_CONTROL_DRAINING,
                SERVICE_CONTROL_CLOSED,
            }:
                raise IllegalTransition(
                    f"service_control: cannot fence from {self._state!r}"
                )

    def close(self) -> None:
        """Close only after drain or an explicit unanswered-renewal fence."""
        with self._lock:
            if self._state in {
                SERVICE_CONTROL_CLOSED,
                SERVICE_CONTROL_ACTIVE,
                SERVICE_CONTROL_DRAINING,
            }:
                self._to(SERVICE_CONTROL_CLOSED)
            else:
                raise IllegalTransition(
                    f"service_control: cannot close with {self._state!r} renewal"
                )


def _send(
    sock: socket.socket,
    lock: threading.Lock,
    message: dict,
    *,
    timeout: float = DEFAULT_IPC_TIMEOUT_SECONDS,
) -> None:
    """Send one complete frame under one operation deadline.

    A non-blocking socket only makes each individual ``send`` bounded.  The old
    loop reset its 250 ms select slice forever when the peer stopped reading.  The
    deadline belongs to the frame, so a full local socket fails closed instead of
    retaining a lease or a drain request indefinitely.
    """
    payload = (json.dumps(message, separators=(",", ":"), sort_keys=True) + "\n").encode()
    deadline = time.monotonic() + max(0.001, float(timeout))
    with lock:
        view = memoryview(payload)
        while view:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("service IPC send exceeded its overall deadline")
            try:
                sent = sock.send(view)
            except BlockingIOError:
                _, writable, _ = select.select([], [sock], [], min(0.25, remaining))
                if not writable:
                    continue
                continue
            except InterruptedError:
                continue
            if sent <= 0:
                raise BrokenPipeError("service IPC channel made no progress")
            view = view[sent:]


class ParentChannel:
    """One side of the authenticated local socketpair.

    The same implementation is used in the supervisor and worker.  The worker
    reader owns command handling; the supervisor reader owns lease-gate state.
    """

    def __init__(
        self,
        sock: socket.socket,
        *,
        heartbeat_seconds: float = 1.0,
        io_timeout_seconds: float = DEFAULT_IPC_TIMEOUT_SECONDS,
    ) -> None:
        self.sock = sock
        self.sock.setblocking(False)
        self.heartbeat_seconds = heartbeat_seconds
        self.io_timeout_seconds = max(0.001, float(io_timeout_seconds))
        self._send_lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = threading.Event()
        self._parent_lost = threading.Event()
        self._drain = threading.Event()
        self._transport_error: str | None = None
        self._gate_active = threading.Event()
        self._last_parent_heartbeat = time.monotonic()
        self._last_worker_heartbeat = time.monotonic()
        self._pending: dict[str, tuple[threading.Event, dict | None]] = {}
        self._pending_lock = threading.Lock()
        self._renew_requests: queue.SimpleQueue[str] = queue.SimpleQueue()
        self._control = ServiceControlState()
        self._hello_event = threading.Event()
        self._start_event = threading.Event()
        self._hello: dict | None = None
        self._reader: threading.Thread | None = None
        self._heartbeater: threading.Thread | None = None

    # -- common wire surface -------------------------------------------------
    def send(self, message_type: str, **payload) -> None:
        if self._closed.is_set():
            raise BrokenPipeError("service IPC channel is closed")
        try:
            _send(
                self.sock,
                self._send_lock,
                {"type": message_type, **payload},
                timeout=self.io_timeout_seconds,
            )
        except (BrokenPipeError, OSError, TimeoutError) as exc:
            # A failed frame is a write barrier.  The caller may decide whether
            # to restart or release the lease, but this channel must never be
            # treated as writable after its overall deadline expired.
            self._mark_transport_lost(exc)
            raise

    def _mark_drain(self) -> None:
        """Publish transport/drain loss through the control state owner."""
        self._drain.set()
        if self._control.state != SERVICE_CONTROL_CLOSED:
            try:
                self._control.begin_drain()
            except IllegalTransition:
                # A reader/heartbeat can observe the peer close concurrently
                # with the supervisor's final channel.close().  The closed
                # state is already the terminal fence; do not turn that cleanup
                # race into a daemon-thread exception.
                if self._control.state != SERVICE_CONTROL_CLOSED:
                    raise

    def _mark_transport_lost(self, exc: BaseException | None = None) -> None:
        if exc is not None:
            self._transport_error = f"{type(exc).__name__}: {exc}"
        self._parent_lost.set()
        self._mark_drain()

    @property
    def transport_error(self) -> str | None:
        return self._transport_error

    @property
    def parent_lost(self) -> bool:
        return self._parent_lost.is_set()

    @property
    def drain_requested(self) -> bool:
        return self._drain.is_set()

    @property
    def gate_active(self) -> bool:
        return self._gate_active.is_set()

    @property
    def control_state(self) -> str:
        return self._control.state

    @property
    def renewal_allowed(self) -> bool:
        return self._control.renewal_allowed

    @property
    def renewal_in_flight(self) -> bool:
        return self._control.renewal_in_flight

    @property
    def last_parent_heartbeat(self) -> float:
        return self._last_parent_heartbeat

    @property
    def last_worker_heartbeat(self) -> float:
        return self._last_worker_heartbeat

    def close(self) -> None:
        if self._control.renewal_in_flight:
            self._control.fence_unanswered_renewal()
        self._control.close()
        self._stop.set()
        self._closed.set()
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.sock.close()

    # -- worker side ---------------------------------------------------------
    def start_worker(
        self,
        *,
        service_id: str,
        lease_id: str,
        fencing_epoch: int,
        worker_generation: str,
        worker_start_token: str,
        parent_loss_seconds: float,
    ) -> None:
        self._reader = threading.Thread(
            target=self._worker_reader,
            name="cdc-service-parent-reader",
            daemon=True,
        )
        self._reader.start()
        self._heartbeater = threading.Thread(
            target=self._worker_heartbeat,
            args=(service_id, lease_id, fencing_epoch, worker_generation, worker_start_token,
                  parent_loss_seconds),
            name="cdc-service-worker-heartbeat",
            daemon=True,
        )
        self._heartbeater.start()

    def _worker_heartbeat(self, service_id, lease_id, epoch, generation, token, loss_seconds):
        try:
            faults.matrix_crash("service_heartbeat_write")
            self.send(
                "hello",
                service_id=service_id,
                lease_id=lease_id,
                fencing_epoch=epoch,
                worker_generation=generation,
                worker_start_token=token,
                pid=os.getpid(),
            )
            while not self._stop.wait(self.heartbeat_seconds):
                if time.monotonic() - self._last_parent_heartbeat > loss_seconds:
                    self._mark_transport_lost()
                    return
                faults.matrix_crash("service_heartbeat_write")
                self.send("worker_heartbeat", pid=os.getpid(), at=time.time())
        except (BrokenPipeError, OSError, TimeoutError):
            self._mark_transport_lost()

    def _worker_reader(self) -> None:
        buffer = bytearray()
        frame_started_at: float | None = None
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self.sock], [], [], 0.25)
            except (OSError, ValueError):
                self._mark_transport_lost()
                return
            if not ready:
                if (
                    buffer
                    and frame_started_at is not None
                    and time.monotonic() - frame_started_at >= self.io_timeout_seconds
                ):
                    self._mark_transport_lost()
                    return
                continue
            try:
                chunk = self.sock.recv(65536)
            except BlockingIOError:
                continue
            except OSError:
                self._mark_transport_lost()
                return
            if not chunk:
                self._mark_transport_lost()
                return
            if not buffer:
                frame_started_at = time.monotonic()
            buffer.extend(chunk)
            while b"\n" in buffer:
                raw, _, remainder = buffer.partition(b"\n")
                buffer = bytearray(remainder)
                try:
                    message = json.loads(raw.decode())
                except (UnicodeDecodeError, json.JSONDecodeError):
                    self._mark_transport_lost()
                    return
                self._handle_worker_message(message)
                frame_started_at = time.monotonic() if buffer else None

    def _handle_worker_message(self, message: dict) -> None:
        message_type = message.get("type")
        if message_type == "parent_heartbeat":
            self._last_parent_heartbeat = time.monotonic()
            return
        if message_type == "drain":
            self._mark_drain()
            return
        if message_type == "start":
            self._start_event.set()
            return
        if message_type == "renew":
            self._renew_requests.put(str(message.get("request_id", "")))
            return
        if message_type == "commit_prepare_ack":
            request_id = str(message.get("request_id", ""))
            with self._pending_lock:
                pending = self._pending.get(request_id)
                if pending is not None:
                    pending[1] = message
                    pending[0].set()
            return

    def before_commit_ack(self, timeout: float) -> None:
        """Freeze parent renewal before the worker opens ``COMMIT_ACK``."""
        if self.parent_lost:
            raise LeaseLost("the supervisor IPC heartbeat was lost before commit")
        request_id = uuid.uuid4().hex
        done = threading.Event()
        pending = [done, None]
        with self._pending_lock:
            self._pending[request_id] = pending
        try:
            self.send("commit_prepare", request_id=request_id)
            if not done.wait(timeout):
                self._parent_lost.set()
                raise LeaseLost("the supervisor did not fence lease renewal before commit")
            if self.parent_lost or not pending[1] or pending[1].get("ok") is not True:
                raise LeaseLost("the supervisor refused the commit fence")
            self._gate_active.set()
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def pop_renew_request(self) -> str | None:
        try:
            return self._renew_requests.get_nowait()
        except queue.Empty:
            return None

    @property
    def has_renew_request(self) -> bool:
        return self._renew_requests.qsize() > 0

    def request_renew(self) -> str:
        # This is the parent-side dispatch gate.  It is intentionally a machine
        # transition rather than ``if not drain_requested``: a drain->renew call
        # is an illegal protocol edge and must fail loudly.
        self._control.begin_renewal()
        request_id = uuid.uuid4().hex
        done = threading.Event()
        with self._pending_lock:
            self._pending[request_id] = [done, None]
        try:
            self.send("renew", request_id=request_id)
        except BaseException:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            if self._control.renewal_in_flight:
                self._control.fence_unanswered_renewal()
            raise
        return request_id

    def renew_result(self, request_id: str) -> dict | None:
        with self._pending_lock:
            pending = self._pending.get(request_id)
            if pending is None or not pending[0].is_set():
                return None
            self._pending.pop(request_id, None)
            result = pending[1]
        self._control.resolve_renewal()
        return result

    def abandon_renewal(self, request_id: str | None = None) -> None:
        """Resolve an unanswered renewal after its operation bound expires."""
        if request_id is not None:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        self._control.fence_unanswered_renewal()

    def after_commit_ack(self) -> None:
        self._gate_active.clear()
        with contextlib.suppress(BrokenPipeError, OSError):
            self.send("commit_complete")

    # -- supervisor side ----------------------------------------------------
    def start_supervisor(self, *, parent_heartbeat_seconds: float) -> None:
        self._reader = threading.Thread(
            target=self._supervisor_reader,
            name="cdc-service-worker-reader",
            daemon=True,
        )
        self._reader.start()
        self._heartbeater = threading.Thread(
            target=self._supervisor_heartbeat,
            args=(parent_heartbeat_seconds,),
            name="cdc-service-supervisor-heartbeat",
            daemon=True,
        )
        self._heartbeater.start()

    def _supervisor_heartbeat(self, interval: float) -> None:
        while not self._stop.wait(interval):
            try:
                faults.matrix_crash("service_heartbeat_write")
                self.send("parent_heartbeat", pid=os.getpid(), at=time.time())
            except (BrokenPipeError, OSError, TimeoutError) as exc:
                self._mark_transport_lost(exc)
                return

    def _supervisor_reader(self) -> None:
        buffer = bytearray()
        frame_started_at: float | None = None
        while not self._stop.is_set():
            try:
                ready, _, _ = select.select([self.sock], [], [], 0.25)
            except (OSError, ValueError) as exc:
                self._mark_transport_lost(exc)
                return
            if not ready:
                if (
                    buffer
                    and frame_started_at is not None
                    and time.monotonic() - frame_started_at >= self.io_timeout_seconds
                ):
                    self._parent_lost.set()
                    return
                continue
            try:
                chunk = self.sock.recv(65536)
            except BlockingIOError:
                continue
            except OSError as exc:
                self._mark_transport_lost(exc)
                return
            if not chunk:
                self._mark_transport_lost()
                return
            if not buffer:
                frame_started_at = time.monotonic()
            buffer.extend(chunk)
            while b"\n" in buffer:
                raw, _, remainder = buffer.partition(b"\n")
                buffer = bytearray(remainder)
                try:
                    message = json.loads(raw.decode())
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    self._mark_transport_lost(exc)
                    return
                self._handle_supervisor_message(message)
                frame_started_at = time.monotonic() if buffer else None

    def _handle_supervisor_message(self, message: dict) -> None:
        message_type = message.get("type")
        if message_type == "hello":
            self._hello = message
            self._last_worker_heartbeat = time.monotonic()
            self._hello_event.set()
            return
        if message_type == "worker_heartbeat":
            self._last_worker_heartbeat = time.monotonic()
            return
        if message_type == "commit_prepare":
            self._gate_active.set()
            with contextlib.suppress(BrokenPipeError, OSError):
                self.send(
                    "commit_prepare_ack",
                    request_id=message.get("request_id"),
                    ok=not self._parent_lost.is_set(),
                )
            return
        if message_type == "commit_complete":
            self._gate_active.clear()
            return
        if message_type in {"renew_complete", "renew_failed", "renew_cancelled"}:
            request_id = str(message.get("request_id", ""))
            with self._pending_lock:
                pending = self._pending.get(request_id)
                if pending is not None:
                    pending[1] = message
                    pending[0].set()

    @property
    def hello(self) -> dict | None:
        return getattr(self, "_hello", None)

    def wait_for_hello(self, timeout: float) -> dict:
        if not self._hello_event.wait(timeout):
            raise TimeoutError("service worker did not complete the IPC handshake")
        return self.hello or {}

    def request_drain(self) -> None:
        # Enter the declared drain state before the frame is dispatched.  A
        # concurrent/scheduled renewal therefore sees the rejected edge even if
        # the worker has not processed this frame yet.
        self._control.begin_drain()
        self._drain.set()
        self.send("drain")

    def allow_start(self) -> None:
        self.send("start")

    def wait_for_start(self, timeout: float) -> bool:
        return self._start_event.wait(timeout)


class ServiceWorkerContext:
    """Worker-owned service state passed into the shared Flight worker core."""

    def __init__(
        self,
        *,
        service_id: str,
        lease_key: str,
        lease_id: str,
        fencing_epoch: int,
        worker_generation: str,
        parent_pid: int,
        parent_start_token: str,
        worker_start_token: str,
        ipc_fd: int,
        heartbeat_seconds: float,
        parent_loss_seconds: float,
        io_timeout_seconds: float = DEFAULT_IPC_TIMEOUT_SECONDS,
        invariant_check_seconds: float = 30.0,
    ) -> None:
        self.service_id = service_id
        self.lease_key = lease_key
        self.lease_id = lease_id
        self.fencing_epoch = int(fencing_epoch)
        self.worker_generation = worker_generation
        self.parent_pid = int(parent_pid)
        self.parent_start_token = parent_start_token
        self.worker_start_token = worker_start_token
        self.channel = ParentChannel(
            socket.socket(fileno=int(ipc_fd)),
            heartbeat_seconds=heartbeat_seconds,
            io_timeout_seconds=io_timeout_seconds,
        )
        self.parent_loss_seconds = parent_loss_seconds
        self.invariant_check_seconds = max(0.1, float(invariant_check_seconds))
        self.stop_event = threading.Event()
        self._started = False

    @classmethod
    def from_environment(cls) -> ServiceWorkerContext:
        required = {
            "service_id": "CDC_SERVICE_ID",
            "lease_key": "CDC_SERVICE_LEASE_KEY",
            "lease_id": "CDC_SERVICE_LEASE_ID",
            "fencing_epoch": "CDC_SERVICE_FENCING_EPOCH",
            "worker_generation": "CDC_SERVICE_WORKER_GENERATION",
            "parent_pid": "CDC_SERVICE_PARENT_PID",
            "parent_start_token": "CDC_SERVICE_PARENT_START_TOKEN",
            "ipc_fd": "CDC_SERVICE_IPC_FD",
        }
        values = {}
        for name, env_name in required.items():
            value = os.environ.get(env_name)
            if value is None or not value.strip():
                raise RuntimeError(f"service worker is missing {env_name}")
            values[name] = value
        return cls(
            **values,
            worker_start_token=process_start_token(),
            heartbeat_seconds=float(os.environ.get("CDC_SERVICE_PARENT_HEARTBEAT_SECONDS", "1")),
            parent_loss_seconds=float(os.environ.get("CDC_SERVICE_PARENT_LOSS_SECONDS", "5")),
            io_timeout_seconds=float(
                os.environ.get(
                    "CDC_SERVICE_IPC_TIMEOUT_SECONDS",
                    str(DEFAULT_IPC_TIMEOUT_SECONDS),
                )
            ),
            invariant_check_seconds=float(
                os.environ.get("CDC_SERVICE_INVARIANT_CHECK_SECONDS", "30")
            ),
        )

    def start(self) -> None:
        if not self._started:
            if process_start_token(self.parent_pid) != self.parent_start_token:
                self.channel.close()
                raise LeaseLost("the parent process-start token no longer matches")
            self.channel.start_worker(
                service_id=self.service_id,
                lease_id=self.lease_id,
                fencing_epoch=self.fencing_epoch,
                worker_generation=self.worker_generation,
                worker_start_token=self.worker_start_token,
                parent_loss_seconds=self.parent_loss_seconds,
            )
            self._started = True

    def wait_for_start(self, timeout: float) -> None:
        if not self.channel.wait_for_start(timeout):
            raise LeaseLost("the supervisor did not release the worker start gate")
        self.assert_writable()

    @property
    def parent_lost(self) -> bool:
        return self.channel.parent_lost

    @property
    def drain_requested(self) -> bool:
        return self.channel.drain_requested

    def assert_writable(self) -> None:
        if self.parent_lost:
            raise LeaseLost("the parent supervisor is no longer alive")

    def before_commit_ack(self, timeout: float) -> None:
        self.assert_writable()
        self.channel.before_commit_ack(timeout)

    def after_commit_ack(self) -> None:
        self.channel.after_commit_ack()

    @property
    def renew_requested(self) -> bool:
        # The request remains pending in the worker until the data connection
        # successfully renews it.  This property only answers whether there is
        # work waiting; ``renew_once`` consumes exactly one request.
        return self.channel.has_renew_request

    def renew_once(self, lease, con) -> bool:
        request_id = self.channel.pop_renew_request()
        if request_id is None:
            return False
        try:
            # The worker loop checks drain first, but the reader thread can queue
            # a renewal just before it publishes the drain frame.  The same state
            # machine therefore guards the worker-side dispatch too.  A queued
            # stale request is answered as a cancellation and never reaches lease
            # I/O.
            self.channel._control.begin_renewal()
        except IllegalTransition:
            if not self.channel.drain_requested:
                raise
            try:
                self.channel.send(
                    "renew_cancelled",
                    request_id=request_id,
                    ok=False,
                    reason="drain_started_before_renewal_dispatch",
                )
            except (BrokenPipeError, OSError, TimeoutError) as transport:
                # ``send`` has already published channel loss and drain.  Keep
                # the transport error as an explicit outcome even when its peer
                # can no longer receive the cancellation frame.
                self.channel._mark_transport_lost(transport)
            return True
        try:
            # A renewal request can be queued just before the socket observes
            # parent EOF.  The worker must not turn that stale request into a
            # lease write after the parent-loss barrier is already set.
            self.assert_writable()
            lease.renew_control(con)
        except BaseException as exc:
            if self.channel.drain_requested and isinstance(
                exc, (BrokenPipeError, OSError, TimeoutError)
            ):
                try:
                    self.channel.send(
                        "renew_cancelled",
                        request_id=request_id,
                        ok=False,
                        reason=f"drain_in_flight_transport:{type(exc).__name__}",
                    )
                except (BrokenPipeError, OSError, TimeoutError) as transport:
                    self.channel._mark_transport_lost(transport)
                self.channel._control.resolve_renewal()
                return True
            try:
                self.channel.send(
                    "renew_failed", request_id=request_id, error=f"{type(exc).__name__}: {exc}"
                )
            except (BrokenPipeError, OSError, TimeoutError) as transport:
                self.channel._mark_transport_lost(transport)
            self.channel._control.resolve_renewal()
            raise
        try:
            self.channel.send("renew_complete", request_id=request_id, ok=True)
        except (BrokenPipeError, OSError, TimeoutError) as transport:
            # The peer has already closed its side while this authorized renewal
            # was finishing.  The channel's transport-loss state is the real
            # information; drain makes the resulting worker exit intentional.
            self.channel._mark_transport_lost(transport)
            self.channel._control.resolve_renewal()
            if not self.channel.drain_requested:
                raise
            return True
        self.channel._control.resolve_renewal()
        return True

    def close(self) -> None:
        self.channel.close()
