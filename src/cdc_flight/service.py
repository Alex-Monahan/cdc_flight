"""Long-running supervisor/worker service adapter.

The supervisor owns the physical destination lease and the worker owns the one
Debezium/data connection.  The supervisor never imports the engine module; that
keeps the parent a small control process and makes SIGKILL/restart ownership
observable in the durable lease row.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

from . import destination as dest_mod
from . import faults
from .config import DestinationConfig, ServiceConfig
from .destination import Lease
from .errors import LeaseLost
from .service_protocol import ParentChannel, process_start_token

log = logging.getLogger("cdc_flight.service")


def _bounded_call(operation, timeout: float):
    """Run one control operation with a finite wait and one owning thread."""
    done = threading.Event()
    result: list[object] = []
    failure: list[BaseException] = []

    def invoke() -> None:
        try:
            result.append(operation())
        except BaseException as exc:  # pass the real failure to the owner
            failure.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=invoke, name="cdc-service-control-operation", daemon=True)
    thread.start()
    if not done.wait(timeout):
        raise TimeoutError(f"service control operation exceeded {timeout:.1f}s")
    if failure:
        raise failure[0]
    return result[0] if result else None


class ServiceSupervisor:
    """Own one physical lease and supervise one worker generation at a time."""

    def __init__(self, *, destination: str | None = None, worker_command=None) -> None:
        self.destination_kind = destination
        self.dest = DestinationConfig(
            **({"kind": destination} if destination is not None else {})
        )
        self.policy = ServiceConfig()
        self.service_id = self.policy.service_id
        self.parent_pid = os.getpid()
        self.parent_start_token = process_start_token(self.parent_pid)
        if worker_command is not None:
            self.worker_command = list(worker_command)
        else:
            test_child = os.environ.get("CDC_SERVICE_WORKER_SCRIPT")
            self.worker_command = (
                [sys.executable, test_child, "--service-worker"]
                if test_child
                else None
            )
        self.stop_requested = threading.Event()
        self.hard_stop_requested = threading.Event()
        self._old_handlers: dict[int, object] = {}
        self.con = None
        self.lease: Lease | None = None
        self.lease_held = False
        self.destination_lease_key: str | None = None
        self.worker: subprocess.Popen | None = None
        self.channel: ParentChannel | None = None
        self.generation: str | None = None
        self.restart_count = 0
        self._drain_sent = False
        self._hard_stop_failed = False

    # -- signal boundary ----------------------------------------------------
    def _install_signal_handlers(self) -> None:
        def request_stop(signum, _frame) -> None:
            # Signal handlers only set intent.  Lease/telemetry/engine work stays
            # on the supervisor loop where it has an operation deadline.
            if self.stop_requested.is_set():
                self.hard_stop_requested.set()
            else:
                self.stop_requested.set()
            log.warning("service received signal %s; stop=%s", signum,
                        self.hard_stop_requested.is_set())

        for signum in (signal.SIGTERM, signal.SIGINT):
            self._old_handlers[signum] = signal.signal(signum, request_stop)

    def _restore_signal_handlers(self) -> None:
        for signum, previous in self._old_handlers.items():
            signal.signal(signum, previous)
        self._old_handlers.clear()

    # -- control/lease ------------------------------------------------------
    def _control(self, operation):
        return _bounded_call(operation, self.policy.operation_timeout_seconds)

    def _open_control(self):
        """Open a lease connection only while the worker has no data handle."""
        return dest_mod.connect(self.dest)

    def _close_control(self) -> None:
        if self.con is not None:
            with contextlib.suppress(Exception):
                self.con.close()
            self.con = None

    def _open_lease(self) -> None:
        self.con = dest_mod.connect(self.dest)
        dest_mod.ensure_control_schema(self.con, self.dest.control_schema)
        dest_mod.ensure_dataset(self.con, self.dest.dataset_name)
        self.destination_lease_key = self.dest.resolve_physical_lease_key(self.con)
        self.lease = Lease(
            self.destination_lease_key,
            owner_id=self.service_id,
            ttl_seconds=self.policy.lease_ttl_seconds,
            control_schema=self.dest.control_schema,
            label=self.dest.pipeline_name,
            service_id=self.service_id,
            worker_generation="supervisor",
            process_start_token=self.parent_start_token,
        )
        self._control(lambda: self.lease.acquire(self.con))
        self.lease_held = True
        faults.matrix_crash("service_lease_acquire")
        log.info(
            "service %s acquired physical destination %s at lease epoch %s",
            self.service_id,
            self.destination_lease_key,
            self.lease.epoch,
        )

    # -- child lifecycle ----------------------------------------------------
    def _command_for_worker(self) -> list[str]:
        if self.worker_command is not None:
            return list(self.worker_command)
        return [sys.executable, "-m", "cdc_flight.service", "--worker"]

    def _launch_worker(self) -> None:
        if self.lease is None:
            raise LeaseLost("cannot launch a worker without a physical lease")
        if self.con is None:
            self.con = self._open_control()
        generation = f"{self.service_id}:{uuid.uuid4().hex}"
        parent_sock, child_sock = socket.socketpair()
        child_fd = child_sock.fileno()
        env = os.environ.copy()
        env.update(
            {
                "CDC_SERVICE_WORKER": "1",
                "CDC_SERVICE_MODE": "1",
                "CDC_SERVICE_ID": self.service_id,
                "CDC_SERVICE_LEASE_KEY": self.destination_lease_key or "",
                "CDC_SERVICE_LEASE_ID": self.lease.lease_id,
                "CDC_SERVICE_FENCING_EPOCH": str(self.lease.epoch),
                "CDC_SERVICE_WORKER_GENERATION": generation,
                "CDC_SERVICE_PARENT_PID": str(self.parent_pid),
                "CDC_SERVICE_PARENT_START_TOKEN": self.parent_start_token,
                "CDC_SERVICE_PARENT_HEARTBEAT_SECONDS": str(
                    self.policy.parent_heartbeat_seconds
                ),
                "CDC_SERVICE_PARENT_LOSS_SECONDS": str(self.policy.parent_loss_seconds),
                "CDC_SERVICE_WORKER_START_TIMEOUT": str(self.policy.worker_start_timeout),
                "CDC_SERVICE_IPC_FD": str(child_fd),
            }
        )
        try:
            command = self._command_for_worker()
            process = subprocess.Popen(
                command,
                env=env,
                close_fds=True,
                pass_fds=(child_fd,),
            )
        except BaseException:
            parent_sock.close()
            child_sock.close()
            raise
        finally:
            with contextlib.suppress(OSError):
                child_sock.close()

        channel = ParentChannel(
            parent_sock, heartbeat_seconds=self.policy.parent_heartbeat_seconds
        )
        channel.start_supervisor(
            parent_heartbeat_seconds=self.policy.parent_heartbeat_seconds
        )
        self.worker = process
        self.channel = channel
        self.generation = generation
        self._drain_sent = False
        pending_token = f"pending:{process.pid}:{generation}"
        try:
            # The child is held at its start gate while this row becomes the one
            # durable worker assignment for the current fencing epoch.
            self._control(
                lambda: self.lease.assign_worker(
                    self.con,
                    pid=process.pid,
                    start_token=pending_token,
                    generation=generation,
                )
            )
            hello = channel.wait_for_hello(self.policy.worker_start_timeout)
            self._validate_hello(hello, process, generation)
            self._control(
                lambda: self.lease.confirm_worker(
                    self.con,
                    generation=generation,
                    start_token=str(hello["worker_start_token"]),
                )
            )
            # A local DuckDB destination has a process-level file lock.  The
            # supervisor relinquishes its control handle before the worker opens
            # the data connection; renewals are then authorized over IPC and
            # executed by the worker's serialized destination core.
            self._close_control()
            channel.allow_start()
            log.info("worker generation %s passed the authenticated start gate", generation)
        except BaseException:
            self._kill_worker(process)
            channel.close()
            with contextlib.suppress(Exception):
                if self.con is None:
                    self.con = self._open_control()
                self._control(
                    lambda: self.lease.mark_worker_finished(
                        self.con, generation=generation
                    )
                )
            self._close_control()
            self.worker = None
            self.channel = None
            self.generation = None
            raise

    def _validate_hello(self, hello: dict, process: subprocess.Popen, generation: str) -> None:
        expected = {
            "service_id": self.service_id,
            "lease_id": self.lease.lease_id if self.lease is not None else None,
            "fencing_epoch": self.lease.epoch if self.lease is not None else None,
            "worker_generation": generation,
            "pid": process.pid,
        }
        for key, value in expected.items():
            observed = hello.get(key)
            if key == "fencing_epoch":
                observed = int(observed) if observed is not None else None
            if observed != value:
                raise LeaseLost(
                    f"worker hello {key}={observed!r} does not match {value!r}"
                )
        token = hello.get("worker_start_token")
        if not isinstance(token, str) or not token.strip():
            raise LeaseLost("worker hello omitted its process-start token")

    def _kill_worker(self, process: subprocess.Popen | None = None) -> bool:
        process = process or self.worker
        if process is None:
            return True
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        try:
            process.wait(timeout=self.policy.operation_timeout_seconds)
        except subprocess.TimeoutExpired:
            self._hard_stop_failed = True
            log.critical(
                "worker pid %s did not die within the hard-stop deadline; retaining "
                "the physical lease",
                process.pid,
            )
            return False
        return True

    def _finish_generation(self) -> None:
        process, channel, generation = self.worker, self.channel, self.generation
        if process is None or generation is None:
            return
        if process.poll() is None:
            return
        if channel is not None:
            channel.close()
        if self.lease is not None and self.lease_held:
            control = None
            try:
                control = self.con or self._open_control()
                self._control(
                    lambda: self.lease.mark_worker_finished(
                        control, generation=generation
                    )
                )
            except Exception:
                log.warning("could not mark worker generation finished", exc_info=True)
            finally:
                if control is not None:
                    with contextlib.suppress(Exception):
                        control.close()
                self.con = None
        self.worker = None
        self.channel = None
        self.generation = None

    def _request_drain(self) -> None:
        if self._drain_sent or self.channel is None:
            return
        try:
            self.channel.request_drain()
            self._drain_sent = True
        except (BrokenPipeError, OSError):
            self.hard_stop_requested.set()

    # -- service loop -------------------------------------------------------
    def run(self) -> int:
        self._install_signal_handlers()
        exit_code = 1
        try:
            self._open_lease()
            self._launch_worker()
            next_renewal = time.monotonic() + self.policy.lease_renew_seconds
            renew_pending: str | None = None
            renew_deadline: float | None = None
            drain_deadline = None
            while True:
                now = time.monotonic()
                process = self.worker
                channel = self.channel
                if process is None or channel is None:
                    if self.stop_requested.is_set():
                        exit_code = 0
                        break
                    if self.restart_count >= self.policy.max_worker_restarts:
                        log.error("worker restart budget exhausted")
                        break
                    self.restart_count += 1
                    self._launch_worker()
                    next_renewal = time.monotonic() + self.policy.lease_renew_seconds
                    renew_pending = None
                    renew_deadline = None
                    continue

                return_code = process.poll()
                if return_code is not None:
                    log.error(
                        "worker generation %s exited with code %s",
                        self.generation,
                        return_code,
                    )
                    self._finish_generation()
                    if self.stop_requested.is_set():
                        exit_code = 0 if return_code == 0 else 1
                        break
                    if self.restart_count >= self.policy.max_worker_restarts:
                        break
                    self.restart_count += 1
                    self._launch_worker()
                    next_renewal = time.monotonic() + self.policy.lease_renew_seconds
                    renew_pending = None
                    renew_deadline = None
                    continue

                if self.hard_stop_requested.is_set():
                    self._kill_worker(process)
                    self._finish_generation()
                    break

                if self.stop_requested.is_set():
                    self._request_drain()
                    if drain_deadline is None:
                        drain_deadline = now + self.policy.drain_deadline_seconds
                    if now >= drain_deadline:
                        log.error("worker did not drain before the bounded deadline")
                        self.hard_stop_requested.set()
                        self._kill_worker(process)
                        self._finish_generation()
                        break

                if renew_pending is not None:
                    result = channel.renew_result(renew_pending)
                    if result is not None:
                        if result.get("ok") is not True:
                            log.critical("worker could not renew the physical lease: %s", result)
                            self.hard_stop_requested.set()
                            self._kill_worker(process)
                            self._finish_generation()
                            break
                        renew_pending = None
                        renew_deadline = None
                        next_renewal = now + self.policy.lease_renew_seconds
                    elif renew_deadline is not None and now >= renew_deadline:
                        log.critical("worker lease renewal exceeded its operation deadline")
                        self.hard_stop_requested.set()
                        self._kill_worker(process)
                        self._finish_generation()
                        break
                elif now >= next_renewal and not channel.gate_active:
                    try:
                        faults.matrix_crash("service_lease_renewal")
                        renew_pending = channel.request_renew()
                        renew_deadline = now + self.policy.operation_timeout_seconds
                    except BaseException as exc:
                        log.critical("physical lease renewal request failed closed: %s", exc)
                        self.hard_stop_requested.set()
                        self._kill_worker(process)
                        self._finish_generation()
                        break

                if (
                    now - channel.last_worker_heartbeat
                    > self.policy.worker_heartbeat_timeout
                ):
                    log.error("worker heartbeat stalled; hard-stopping that generation")
                    self._kill_worker(process)
                    self._finish_generation()
                    if self.restart_count >= self.policy.max_worker_restarts:
                        break
                    self.restart_count += 1
                    self._launch_worker()
                    next_renewal = time.monotonic() + self.policy.lease_renew_seconds
                    renew_pending = None
                    renew_deadline = None
                    continue
                time.sleep(min(0.25, self.policy.parent_heartbeat_seconds))
            return exit_code
        except (LeaseLost, OSError, RuntimeError) as exc:
            log.critical("service supervisor failed closed: %s", exc, exc_info=True)
            return 1
        finally:
            if self.worker is not None:
                self._kill_worker(self.worker)
                self._finish_generation()
            if self.channel is not None:
                self.channel.close()
            if (
                self.lease_held
                and self.lease is not None
                and not self._hard_stop_failed
            ):
                with contextlib.suppress(Exception):
                    faults.matrix_crash("service_lease_release")
                    control = self._open_control()
                    try:
                        self._control(lambda: self.lease.release(control, retain=True))
                    finally:
                        with contextlib.suppress(Exception):
                            control.close()
            elif self._hard_stop_failed:
                log.critical(
                    "worker hard-stop was not proven; leaving the physical lease "
                    "for its durable expiry/recovery proof"
                )
            self._close_control()
            self._restore_signal_handlers()


def worker_main() -> int:
    """Enter the worker adapter without importing it in the supervisor parent."""
    from .pipeline import main as pipeline_main

    return pipeline_main([])


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "--worker":
        return worker_main()
    parser = argparse.ArgumentParser(prog="cdc-flight-service")
    parser.add_argument("--destination", choices=["duckdb", "motherduck"], default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )
    supervisor = ServiceSupervisor(destination=args.destination)
    return supervisor.run()


if __name__ == "__main__":
    raise SystemExit(main())
