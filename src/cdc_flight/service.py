"""Single-process long-running Flight entry point.

The external scheduler starts this program every minute.  Admission is the
watchdog: a fresh lease heartbeat means this invocation is a successful
stand-down; an expired or released lease is atomically claimed at the next
fencing epoch.  The admitted process then runs the normal pipeline and keeps
renewing its lease through the pipeline's serialized destination owner.

There is intentionally no supervisor process, worker process, socket channel,
or in-process restart loop here.  A crashed Flight ends; the next scheduled
Flight performs admission again.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import uuid

from . import destination as dest_mod
from .config import DestinationConfig, ServiceConfig
from .control_schema import ensure_service_lease_bootstrap
from .destination_fence import EpochFencedConnection
from .destination_lease import Lease
from .errors import AdmissionError, ServiceStandDown
from .service_runtime import ServiceContext

log = logging.getLogger("cdc_flight.service")


class SingleProcessFlight:
    """Admit, run, drain, and release exactly one Flight process."""

    def __init__(self, *, destination: str | None = None) -> None:
        self.destination_kind = destination
        self.dest = DestinationConfig(
            **({"kind": destination} if destination is not None else {})
        )
        self.policy = ServiceConfig()
        self.service_id = self.policy.service_id
        self.lease_id = uuid.uuid4().hex
        self.generation = f"{self.service_id}:{uuid.uuid4().hex}"
        self.context = ServiceContext(
            service_id=self.service_id,
            lease_id=self.lease_id,
            worker_generation=self.generation,
            policy=self.policy,
        )
        self.lease: Lease | None = None
        self.admitted = False
        self._old_handlers: dict[int, object] = {}
        self._signal_callback = None
        self.context.set_signal_reinstaller(self._install_signal_handlers)

    def _install_signal_handlers(self) -> None:
        if self._signal_callback is None:
            def request_stop(signum, _frame) -> None:
                if self.context.drain_requested:
                    self.context.request_hard_stop()
                else:
                    self.context.request_drain()
                log.warning(
                    "service received signal %s; drain=%s hard_stop=%s",
                    signum,
                    self.context.drain_requested,
                    self.context.hard_stop_event.is_set(),
                )

            self._signal_callback = request_stop

        for signum in (signal.SIGTERM, signal.SIGINT):
            if signum not in self._old_handlers:
                self._old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, self._signal_callback)

    def _restore_signal_handlers(self) -> None:
        for signum, previous in self._old_handlers.items():
            signal.signal(signum, previous)
        self._old_handlers.clear()

    def _make_lease(self, lease_key: str) -> Lease:
        return Lease(
            lease_key,
            owner_id=self.service_id,
            ttl_seconds=self.policy.lease_ttl_seconds,
            control_schema=self.dest.control_schema,
            label=self.dest.pipeline_name,
            lease_id=self.lease_id,
            service_id=self.service_id,
            worker_generation=self.generation,
        )

    def _admit(self) -> None:
        """Perform the health decision before constructing the CDC engine."""
        con = None
        try:
            try:
                con = dest_mod.connect(self.dest, create_database=False)
            except Exception as connect_error:
                # A local DuckDB holder owns an OS file lock, so a successor
                # cannot even open a normal connection to read the lease.  A
                # read-only admission probe still answers the critical healthy
                # stand-down decision without weakening the writer lock.  If the
                # probe cannot prove a fresh heartbeat, preserve the original
                # failure and fail closed; no local takeover is attempted through
                # an unreadable or stale read-only handle.
                if self.dest.kind != "duckdb" or "conflicting lock" not in str(
                    connect_error
                ).lower():
                    raise
                con = dest_mod.connect(
                    self.dest, read_only=True, create_database=False
                )
                lease_key = self.dest.resolve_physical_lease_key(con)
                lease = self._make_lease(lease_key)
                health = lease.inspect_health(
                    con,
                    heartbeat_bound_seconds=self.policy.heartbeat_bound_seconds,
                )
                if health.healthy:
                    raise ServiceStandDown(
                        "another Flight holds a fresh lease heartbeat",
                        {
                            "health": health.reason,
                            "fencing_epoch": health.fencing_epoch,
                            "read_only_admission": True,
                        },
                    ) from connect_error
                raise connect_error
            # Before an epoch exists, only the admission lease table may be
            # bootstrapped.  All current control/catalog/recovery DDL is routed
            # through the epoch-fenced pipeline handle after this CAS.
            ensure_service_lease_bootstrap(con, self.dest.control_schema)
            lease_key = self.dest.resolve_physical_lease_key(con)
            lease = self._make_lease(lease_key)
            lease.acquire(
                con,
                heartbeat_bound_seconds=self.policy.heartbeat_bound_seconds,
                wait_for_expiry=True,
            )
            self.lease = lease
            self.admitted = True
            # This is a test-only crash point after the durable admission.  A
            # real hard death leaves exactly the lease expiry as recovery proof.
            from . import faults

            faults.matrix_crash("service_lease_acquire")
            self.context.bind(lease, None)
            log.info(
                "admitted as the live Flight for physical destination %s at epoch %s",
                lease_key,
                lease.epoch,
            )
        finally:
            if con is not None:
                # The pipeline opens the one data connection after admission.
                # Retiring this bootstrap handle is what keeps local DuckDB and
                # MotherDuck on the same single-owner path.
                dest_mod.release_connection(con)

    def _release_if_needed(self) -> None:
        if not self.admitted or self.lease is None:
            return
        if self.context.lease_release_attempted:
            # The pipeline's terminal teardown already used the epoch-fenced
            # destination handle.  Reopening a second MotherDuck writer here can
            # hang after a source-dark engine close and adds no safety: a failed
            # release remains protected by the durable lease expiry.
            self.admitted = False
            return
        con = None
        fenced_con = None
        try:
            con = dest_mod.connect(self.dest, create_database=False)
            fenced_con = EpochFencedConnection(con, self.lease, self.context)
            from . import faults

            faults.matrix_crash("service_lease_release")
            # Lease.release is still an exact identity/epoch operation, and the
            # wrapper adds the same immediate fence that protects every other
            # service-owned mutation.
            self.lease.release(fenced_con, retain=True)
            log.info("released service lease epoch %s", self.lease.epoch)
        except Exception:
            # A clean release is preferred, but a failure here must not turn
            # into an unbounded shutdown.  The durable expiry is the remaining
            # fail-closed recovery mechanism.
            log.critical("could not release the service lease; expiry remains the fence", exc_info=True)
        finally:
            if con is not None:
                dest_mod.release_connection(con)
            self.admitted = False

    @staticmethod
    def _write_summary(summary: dict) -> None:
        from .pipeline import _write_summary

        _write_summary(summary)

    def run(self) -> int:
        self._install_signal_handlers()
        # Admission is itself a bounded service operation. Starting the local
        # watchdog before the first destination connection means a blocked
        # MotherDuck health read cannot leave a scheduled Flight hanging
        # forever; the watchdog exits the unreadable instance fail-closed.
        self.context.start_watchdog()
        # Admission is the first bounded operation, before ``pipeline.run`` has
        # a chance to install its own startup marker.  Without this boundary a
        # slow but legitimate MotherDuck lease/readiness handshake was judged by
        # the idle-progress clock and the holder could die before its walsender
        # ever attached.
        self.context.operation_started()
        try:
            try:
                self._admit()
            except AdmissionError as admission:
                if isinstance(admission, ServiceStandDown):
                    summary = {
                        "ok": True,
                        "status": "SUCCEEDED",
                        "run_status": "SUCCEEDED",
                        "run_outcome": "stand_down",
                        "stop_reason": "stand_down",
                        "stand_down": True,
                        "service_mode": True,
                        **admission.summary,
                    }
                    log.info("stand-down: another Flight is healthy; no data work needed")
                    self._write_summary(summary)
                    return 0
                summary = {
                    "ok": False,
                    "status": "FAILED",
                    "run_status": "FAILED",
                    "run_outcome": "admission_failed",
                    "stop_reason": "admission_failed",
                    "service_mode": True,
                    "error": str(admission),
                    "error_type": type(admission).__name__,
                }
                self._write_summary(summary)
                log.exception("single-process Flight admission failed closed")
                return 1
            except BaseException as exc:
                # Admission is deliberately fail-closed.  A destination read
                # failure is not permission to take over, but it is still a
                # failed scheduled run and must be visible as such.
                summary = {
                    "ok": False,
                    "status": "FAILED",
                    "run_status": "FAILED",
                    "run_outcome": "admission_failed",
                    "stop_reason": "admission_failed",
                    "service_mode": True,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
                self._write_summary(summary)
                log.exception("single-process Flight admission failed closed")
                return 1

            from . import faults

            faults.matrix_crash("service_startup")
            from . import pipeline

            try:
                result = pipeline.run(
                    destination=self.destination_kind,
                    service_context=self.context,
                )
            except BaseException as exc:
                summary = dict(getattr(exc, "summary", {}) or {})
                summary.setdefault("stop_reason", "error")
                summary.setdefault("ok", False)
                summary["status"] = "FAILED"
                summary["run_status"] = "FAILED"
                summary["service_mode"] = True
                summary["error"] = str(exc)
                summary["error_type"] = type(exc).__name__
                summary["service_heartbeat"] = self.context.heartbeat_summary()
                self._write_summary(summary)
                log.exception("single-process Flight failed")
                return 1

            summary = dict(result or {})
            summary["service_mode"] = True
            summary["service_heartbeat"] = self.context.heartbeat_summary()
            if self.context.hard_stop_event.is_set():
                summary["status"] = "CANCELLED"
                summary["run_status"] = "CANCELLED"
                summary["stop_reason"] = "operator_hard_stop"
                summary["ok"] = False
                self._write_summary(summary)
                return 1
            if self.context.stop_event.is_set():
                summary["status"] = "CANCELLED"
                summary["run_status"] = "CANCELLED"
                summary["stop_reason"] = "operator_stop"
                summary["ok"] = True
                self._write_summary(summary)
                return 0
            summary.setdefault("status", "SUCCEEDED" if summary.get("ok") else "FAILED")
            summary.setdefault("run_status", summary["status"])
            self._write_summary(summary)
            return 0 if summary.get("ok") is True else 1
        finally:
            self.context.close()
            self._release_if_needed()
            self._restore_signal_handlers()


def main(argv: list[str] | None = None) -> int:
    """Run one scheduled MotherDuck-style Flight instance."""
    parser = argparse.ArgumentParser(prog="cdc-flight-service")
    parser.add_argument("--destination", choices=["duckdb", "motherduck"], default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )
    code = SingleProcessFlight(destination=args.destination).run()
    # Debezium/JPype leaves native threads and Python extension objects behind.
    # Reuse the pipeline's tested shutdown boundary: it attempts the JVM shutdown
    # while Python objects are still alive, then performs the hard process exit
    # that prevents interpreter finalizers from running on a JVM thread.
    from .pipeline import shutdown_and_exit

    shutdown_and_exit(code)


if __name__ == "__main__":
    raise SystemExit(main())
