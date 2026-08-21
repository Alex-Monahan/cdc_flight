"""Long-running supervisor/worker service adapter.

The supervisor owns the physical destination lease and the worker owns the one
Debezium/data connection.  The supervisor never imports the engine module; that
keeps the parent a small control process and makes SIGKILL/restart ownership
observable in the durable lease row.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import logging
import multiprocessing
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


class _RemoteOperationError(RuntimeError):
    """An exception reported by a killable control-operation child."""

    def __init__(self, type_name: str, message: str):
        self.type_name = type_name
        super().__init__(message)


def _callable_child(operation, sender) -> None:
    """Execute one picklable operation and return only a serialisable outcome."""
    try:
        sender.send(("ok", operation()))
    except BaseException as exc:
        with contextlib.suppress(Exception):
            sender.send(("error", type(exc).__name__, str(exc)))
    finally:
        with contextlib.suppress(Exception):
            sender.close()


def _bounded_call(operation, timeout: float, *, process_context: str = "fork"):
    """Run one operation in a process that can actually be cancelled.

    A daemon thread can bound the caller's wait but cannot stop a blocked database
    call.  The old implementation therefore left a live operation behind.  The
    service supervisor uses the ``spawn`` context and a serialisable request; the
    default ``fork`` context keeps this small helper useful for direct local probes
    whose callable is not importable.  On expiry the child is terminated and joined
    before the timeout is reported, so no operation thread or process can continue
    writing after the caller has failed closed.
    """
    if timeout <= 0:
        raise ValueError("service control operation timeout must be positive")
    try:
        context = multiprocessing.get_context(process_context)
    except ValueError:
        context = multiprocessing.get_context()
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_callable_child, args=(operation, sender))
    process.daemon = True
    process.start()
    sender.close()
    deadline = time.monotonic() + float(timeout)
    outcome = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if receiver.poll(min(0.05, remaining)):
            outcome = receiver.recv()
            break
        if not process.is_alive():
            break
    if outcome is None and process.is_alive():
        process.terminate()
        process.join(timeout=min(1.0, max(0.05, timeout)))
        if process.is_alive():
            with contextlib.suppress(Exception):
                process.kill()
            process.join(timeout=1.0)
        receiver.close()
        error = TimeoutError(f"service control operation exceeded {timeout:.1f}s")
        # These attributes are deliberately part of the proof surface: callers
        # can distinguish a cancelled/fenced operation from a mere wait timeout.
        error.cancelled = True
        error.operation_fenced = not process.is_alive()
        raise error
    if outcome is None:
        with contextlib.suppress(Exception):
            if receiver.poll(0.1):
                outcome = receiver.recv()
    process.join(timeout=1.0)
    receiver.close()
    if outcome is None:
        raise _RemoteOperationError(
            "ChildProcessError",
            f"service control operation exited without a result (code={process.exitcode})",
        )
    if outcome[0] == "error":
        raise _RemoteOperationError(outcome[1], outcome[2])
    return outcome[1]


def _control_operation(payload: dict):
    """Open, perform, and close one bounded supervisor control operation.

    The process boundary is intentional.  A DuckDB/MotherDuck connection is never
    inherited by a worker or retained by the supervisor between operations, and a
    timed-out connect/query/DDL/lease write is killed with its owning process.
    """
    con = None
    try:
        dest = payload["destination"]
        con = dest_mod.connect(dest)
        if payload.get("ensure_schema"):
            dest_mod.ensure_control_schema(con, dest.control_schema)
            dest_mod.ensure_dataset(con, dest.dataset_name)
        operation = payload["operation"]
        if operation == "acquire":
            lease_key = dest.resolve_physical_lease_key(con)
            lease = Lease(
                lease_key,
                owner_id=payload["service_id"],
                ttl_seconds=payload["ttl_seconds"],
                control_schema=dest.control_schema,
                label=dest.pipeline_name,
                lease_id=payload["lease_id"],
                service_id=payload["service_id"],
                worker_generation="supervisor",
                process_start_token=payload["parent_start_token"],
            )
            lease.acquire(con)
            return {"lease_key": lease_key, "epoch": lease.epoch}

        lease_key = payload.get("lease_key")
        if operation == "release_identity":
            # Re-resolve even when the supervisor has a provisional DuckDB key:
            # a timed-out acquire may have resolved a physical MotherDuck key
            # before its response was fenced.
            lease_key = dest.resolve_physical_lease_key(con)
        lease = Lease(
            lease_key,
            owner_id=payload["service_id"],
            ttl_seconds=payload["ttl_seconds"],
            control_schema=dest.control_schema,
            label=dest.pipeline_name,
            lease_id=payload["lease_id"],
            fencing_epoch=payload.get("epoch"),
            service_id=payload["service_id"],
            worker_generation=payload.get("generation") or "supervisor",
            process_start_token=payload["parent_start_token"],
        )
        if operation == "assign_worker":
            lease.assign_worker(
                con,
                pid=payload["pid"],
                start_token=payload["start_token"],
                generation=payload["generation"],
            )
        elif operation == "confirm_worker":
            lease.confirm_worker(
                con,
                generation=payload["generation"],
                start_token=payload["start_token"],
            )
        elif operation == "mark_worker_finished":
            lease.mark_worker_finished(con, generation=payload["generation"])
        elif operation == "release":
            lease.release(con, retain=True)
        elif operation == "release_identity":
            # Used only after a control child timed out before it could return its
            # acquired epoch.  The exact lease_id/owner predicate prevents this
            # cleanup from touching a successor epoch.
            con.execute(
                f"UPDATE {dest_mod._control_table(dest.control_schema, 'lease')} "
                "SET state='released', worker_pid=NULL, worker_start_token=NULL "
                "WHERE pipeline=? AND owner_id=? AND lease_id=?",
                [lease_key, payload["service_id"], payload["lease_id"]],
            )
        else:
            raise ValueError(f"unknown service control operation {operation!r}")
        return None
    finally:
        if con is not None:
            with contextlib.suppress(Exception):
                con.close()


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
        self._control_fenced = False
        self._lease_id = uuid.uuid4().hex

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
    def _control(self, payload: dict):
        """Run one serialised, killable control-plane operation."""
        try:
            return _bounded_call(
                functools.partial(_control_operation, payload),
                self.policy.operation_timeout_seconds,
                process_context="spawn",
            )
        except _RemoteOperationError as exc:
            if exc.type_name == "LeaseLost":
                raise LeaseLost(str(exc)) from exc
            raise
        except TimeoutError:
            # _bounded_call has terminated and joined the child.  Its database
            # work cannot continue, but an acquire may have committed before the
            # response was lost; the exact lease id is retained for cleanup.
            self._control_fenced = True
            raise

    def _close_control(self) -> None:
        # Control connections are opened and retired inside _control's child.
        self.con = None

    def _open_lease(self) -> None:
        # A local key is available before the control child resolves it.  For
        # MotherDuck the child fills it from the live catalog; a timed-out acquire
        # is still cleanable by the exact lease id below once that resolution is
        # retried in the cleanup child.
        if self.dest.kind == "duckdb":
            self.destination_lease_key = self.dest.lease_key
        result = self._control(
            {
                "operation": "acquire",
                "destination": self.dest,
                "ensure_schema": True,
                "service_id": self.service_id,
                "lease_id": self._lease_id,
                "ttl_seconds": self.policy.lease_ttl_seconds,
                "parent_start_token": self.parent_start_token,
            }
        )
        self.destination_lease_key = result["lease_key"]
        self.lease = Lease(
            self.destination_lease_key,
            owner_id=self.service_id,
            ttl_seconds=self.policy.lease_ttl_seconds,
            control_schema=self.dest.control_schema,
            label=self.dest.pipeline_name,
            service_id=self.service_id,
            worker_generation="supervisor",
            lease_id=self._lease_id,
            process_start_token=self.parent_start_token,
        )
        self.lease.fencing_epoch = int(result["epoch"])
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
                "CDC_SERVICE_IPC_TIMEOUT_SECONDS": str(
                    self.policy.operation_timeout_seconds
                ),
                "CDC_SERVICE_IPC_FD": str(child_fd),
                # The CLI destination is part of the authenticated worker
                # configuration.  Without this, a MotherDuck supervisor handed
                # a MotherDuck lease to a worker that defaulted to DuckDB.
                "CDC_DESTINATION": self.dest.kind,
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
            parent_sock,
            heartbeat_seconds=self.policy.parent_heartbeat_seconds,
            io_timeout_seconds=self.policy.operation_timeout_seconds,
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
                {
                    "operation": "assign_worker",
                    "destination": self.dest,
                    "service_id": self.service_id,
                    "lease_id": self.lease.lease_id,
                    "lease_key": self.destination_lease_key,
                    "epoch": self.lease.epoch,
                    "ttl_seconds": self.policy.lease_ttl_seconds,
                    "parent_start_token": self.parent_start_token,
                    "pid": process.pid,
                    "start_token": pending_token,
                    "generation": generation,
                }
            )
            hello = channel.wait_for_hello(self.policy.worker_start_timeout)
            self._validate_hello(hello, process, generation)
            self._control(
                {
                    "operation": "confirm_worker",
                    "destination": self.dest,
                    "service_id": self.service_id,
                    "lease_id": self.lease.lease_id,
                    "lease_key": self.destination_lease_key,
                    "epoch": self.lease.epoch,
                    "ttl_seconds": self.policy.lease_ttl_seconds,
                    "parent_start_token": self.parent_start_token,
                    "start_token": str(hello["worker_start_token"]),
                    "generation": generation,
                }
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
                self._control(
                    {
                        "operation": "mark_worker_finished",
                        "destination": self.dest,
                        "service_id": self.service_id,
                        "lease_id": self.lease.lease_id,
                        "lease_key": self.destination_lease_key,
                        "epoch": self.lease.epoch,
                        "ttl_seconds": self.policy.lease_ttl_seconds,
                        "parent_start_token": self.parent_start_token,
                        "generation": generation,
                    }
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
            try:
                self._control(
                    {
                        "operation": "mark_worker_finished",
                        "destination": self.dest,
                        "service_id": self.service_id,
                        "lease_id": self.lease.lease_id,
                        "lease_key": self.destination_lease_key,
                        "epoch": self.lease.epoch,
                        "ttl_seconds": self.policy.lease_ttl_seconds,
                        "parent_start_token": self.parent_start_token,
                        "generation": generation,
                    }
                )
            except Exception:
                log.warning("could not mark worker generation finished", exc_info=True)
            finally:
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
        except (LeaseLost, OSError, RuntimeError, TimeoutError) as exc:
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
                and not self._control_fenced
            ):
                with contextlib.suppress(Exception):
                    faults.matrix_crash("service_lease_release")
                    self._control(
                        {
                            "operation": "release",
                            "destination": self.dest,
                            "service_id": self.service_id,
                            "lease_id": self.lease.lease_id,
                            "lease_key": self.destination_lease_key,
                            "epoch": self.lease.epoch,
                            "ttl_seconds": self.policy.lease_ttl_seconds,
                            "parent_start_token": self.parent_start_token,
                        }
                    )
            elif self._control_fenced:
                # A timed-out child may have committed an acquire before its
                # response was lost.  Release only that exact identity; if the
                # cleanup itself cannot complete, durable expiry remains the
                # fail-closed fence rather than an active orphan writer.
                with contextlib.suppress(Exception):
                    self._control(
                        {
                            "operation": "release_identity",
                            "destination": self.dest,
                            "service_id": self.service_id,
                            "lease_id": self._lease_id,
                            "lease_key": self.destination_lease_key,
                            "ttl_seconds": self.policy.lease_ttl_seconds,
                            "parent_start_token": self.parent_start_token,
                        }
                    )
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
