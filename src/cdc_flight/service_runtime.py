"""In-process liveness context for the long-running Flight.

There is deliberately no transport in this module.  The service loop, the
Debezium callback, the lease, and the watchdog all belong to one Python
process.  The callback serializes destination operations; the service loop
uses that same lock for a lease heartbeat when the callback is quiescent.

The watchdog never touches MotherDuck.  If the process stops servicing its
own loop long enough, it requests a drain and then hard-exits.  That keeps a
wedged process from renewing forever while preserving the rule that only the
data owner performs destination I/O.
"""

from __future__ import annotations

import logging
import os
import threading
import time

from . import faults
from .config import ServiceConfig
from .errors import LeaseLost

log = logging.getLogger("cdc_flight.service_runtime")


class ServiceContext:
    """One admitted service run's local lease and liveness state."""

    def __init__(
        self,
        *,
        service_id: str,
        lease_id: str,
        worker_generation: str,
        policy: ServiceConfig,
        exit_fn=None,
    ) -> None:
        self.service_id = service_id
        self.lease_id = lease_id
        # The durable column retains ``worker_generation`` for schema and
        # occurrence-key compatibility.  It names this one process generation;
        # it is not a child process identity.
        self.worker_generation = worker_generation
        self.policy = policy
        self.lease_key: str | None = None
        self.fencing_epoch: int | None = None
        self.lease = None
        self.connection = None
        # The pipeline owns the admitted connection and normally performs the
        # retained release in its terminal teardown.  The service wrapper must not
        # open a second MotherDuck writer after that teardown: besides being
        # unnecessary, that secondary connect can outlive the bounded engine close.
        self._lease_release_attempted = False
        self.stop_event = threading.Event()
        self.hard_stop_event = threading.Event()
        self._drain_event = threading.Event()
        self._stall_event = threading.Event()
        self._lease_lost_event = threading.Event()
        self._closed = threading.Event()
        self._lock = threading.Lock()
        self._last_progress = time.monotonic()
        self._last_heartbeat = time.monotonic()
        self._next_heartbeat = time.monotonic() + policy.lease_renew_seconds
        self._last_source_observation: float | None = None
        self._source_health_status: str | None = None
        self._stall_message: str | None = None
        self._lease_failure: BaseException | None = None
        self._watchdog: threading.Thread | None = None
        self._exit_fn = exit_fn or os._exit
        self._started = False
        self._signal_reinstaller = None

    def set_signal_reinstaller(self, callback) -> None:
        """Set the process signal hook that must be reapplied after JVM startup."""
        self._signal_reinstaller = callback

    @property
    def invariant_check_seconds(self) -> float:
        """Expose the policy interval to the in-process engine loop."""
        return self.policy.invariant_check_seconds

    def rearm_process_signals(self) -> None:
        """Reapply SIGTERM/SIGINT after JPype has installed native handlers."""
        callback = self._signal_reinstaller
        if callback is not None:
            callback()

    def bind(self, lease, connection) -> None:
        """Attach the pipeline's one destination connection to the admission."""
        self.lease = lease
        self.connection = connection
        self.lease_key = lease.lease_key
        self.fencing_epoch = lease.epoch

    @property
    def lease_release_attempted(self) -> bool:
        with self._lock:
            return self._lease_release_attempted

    def note_lease_release_attempted(self) -> None:
        """Publish that pipeline teardown consumed the admitted lease handle."""
        with self._lock:
            self._lease_release_attempted = True

    def observe_source_health(self, status: str, observed_at: float | None) -> None:
        """Publish a sampler observation without manufacturing application progress.

        ``status`` is a source witness, not a heartbeat.  The watchdog may use a
        fresh ``connected_quiet`` observation to preserve a legitimately idle
        source, while ``disconnected``/``unknown`` deliberately stop renewals and
        are left to the supervisor's bounded source-dark decision.
        """
        if observed_at is None:
            return
        with self._lock:
            if (
                self._last_source_observation is not None
                and observed_at <= self._last_source_observation
            ):
                return
            self._last_source_observation = float(observed_at)
            self._source_health_status = str(status)

    @property
    def source_health_status(self) -> str | None:
        with self._lock:
            return self._source_health_status

    def source_health_allows_renewal(self) -> bool:
        """Only a fresh connected-and-quiet source may renew without source work."""
        with self._lock:
            observed = self._last_source_observation
            status = self._source_health_status
        if observed is None or status != "connected_quiet":
            return False
        return (
            time.monotonic() - observed
            <= self.policy.source_health_stale_seconds
        )

    def note_source_dark(self) -> None:
        """Record a diagnosed source outage for the fail-closed teardown path."""
        with self._lock:
            self._source_health_status = "dark"

    def start_watchdog(self) -> None:
        if self._started:
            return
        self._started = True
        self._watchdog = threading.Thread(
            target=self._watchdog_loop,
            name="cdc-flight-service-watchdog",
            daemon=True,
        )
        self._watchdog.start()

    def _watchdog_loop(self) -> None:
        while not self._closed.wait(self.policy.watchdog_poll_seconds):
            with self._lock:
                now = time.monotonic()
                stalled_for = time.monotonic() - self._last_progress
                already_stalled = self._stall_event.is_set()
                source_observation = self._last_source_observation
                source_status = self._source_health_status
            source_witness_fresh = (
                source_observation is not None
                and now - source_observation
                <= self.policy.source_health_stale_seconds
            )
            if source_witness_fresh and source_status in {
                "connected_quiet",
                "disconnected",
                "unknown",
                "unobserved",
                "retrying",
                "dark",
            }:
                # A fresh source observation is real external evidence.  It may
                # preserve quiet or carry a diagnosed outage to the supervisor, but
                # it never advances ``_last_progress`` and never permits renewal on
                # a disconnected source.
                continue
            if stalled_for < self.policy.stall_timeout_seconds:
                continue
            if not already_stalled:
                message = (
                    "service progress stalled for "
                    f"{stalled_for:.1f}s; stopping before the lease can be renewed"
                )
                with self._lock:
                    self._stall_message = message
                    self._stall_event.set()
                log.critical(message)
                self.request_drain()
            if stalled_for >= (
                self.policy.stall_timeout_seconds
                + self.policy.stall_exit_grace_seconds
            ):
                # This path is intentionally I/O-free.  A stalled destination
                # cannot be made safe by asking the same stalled process to
                # write an alert or release its lease.  The next Flight sees
                # the expired heartbeat and records the recovery/failure.
                log.critical(
                    "service watchdog hard-exiting after %.1fs without progress",
                    stalled_for,
                )
                self._exit_fn(1)
                return

    def mark_progress(self) -> None:
        """Publish local progress; this is memory-only and safe in any phase."""
        with self._lock:
            self._last_progress = time.monotonic()

    def operation_started(self) -> None:
        """Mark entry into a callback or destination operation.

        The service loop may continue to run while a Debezium callback is
        blocked. Its polling alone is not evidence that the callback is making
        progress, so the callback owns this timestamp until it completes.
        """
        # Admission to a callback is not forward motion.  The callback marks each
        # received source record and the durable COMMIT marks destination progress.
        return None

    def operation_finished(self, *, progressed: bool = False) -> None:
        """Mark completion only when the callback actually moved data forward."""
        if progressed:
            self.mark_progress()

    @property
    def last_progress(self) -> float:
        with self._lock:
            return self._last_progress

    @property
    def stalled(self) -> bool:
        return self._stall_event.is_set()

    @property
    def stall_message(self) -> str | None:
        with self._lock:
            return self._stall_message

    @property
    def lease_lost(self) -> bool:
        return self._lease_lost_event.is_set()

    @property
    def lease_failure(self) -> BaseException | None:
        with self._lock:
            return self._lease_failure

    @property
    def drain_requested(self) -> bool:
        return self._drain_event.is_set()

    @property
    def renew_requested(self) -> bool:
        return (
            not self.drain_requested
            and not self.lease_lost
            and time.monotonic() >= self._next_heartbeat
        )

    def request_drain(self) -> None:
        self.stop_event.set()
        self._drain_event.set()

    def request_hard_stop(self) -> None:
        self.hard_stop_event.set()
        self.request_drain()

    def assert_writable(self) -> None:
        if self.lease_lost:
            failure = self.lease_failure
            if failure is not None:
                raise LeaseLost(f"service lease was lost: {failure}") from failure
            raise LeaseLost("service lease was lost")
        if self.stalled:
            raise LeaseLost(self.stall_message or "service progress stalled")

    def renew_once(self, lease=None, con=None) -> bool:
        """Write one bounded heartbeat while the applier is quiescent."""
        if not self.renew_requested:
            return False
        lease = lease or self.lease
        con = con or self.connection
        if lease is None or con is None:
            raise LeaseLost("service heartbeat has no admitted lease connection")
        if not self.source_health_allows_renewal():
            return False
        self.assert_writable()
        try:
            faults.matrix_crash("service_lease_renewal")
            faults.matrix_crash("service_heartbeat_write")
            lease.renew_control(con)
        except BaseException as exc:
            with self._lock:
                self._lease_failure = exc
                self._lease_lost_event.set()
            self.request_drain()
            raise
        with self._lock:
            self._last_heartbeat = time.monotonic()
            self._next_heartbeat = (
                self._last_heartbeat + self.policy.lease_renew_seconds
            )
        return True

    def heartbeat_summary(self) -> dict[str, object]:
        with self._lock:
            return {
                "service_id": self.service_id,
                "lease_id": self.lease_id,
                "lease_key": self.lease_key,
                "fencing_epoch": self.fencing_epoch,
                "last_progress_age_sec": round(time.monotonic() - self._last_progress, 3),
                "last_heartbeat_age_sec": round(time.monotonic() - self._last_heartbeat, 3),
                "source_health_status": self._source_health_status,
                "source_health_observation_age_sec": (
                    round(time.monotonic() - self._last_source_observation, 3)
                    if self._last_source_observation is not None
                    else None
                ),
                "stalled": self.stalled,
                "lease_lost": self.lease_lost,
            }

    def close(self) -> None:
        self._closed.set()


__all__ = ["ServiceContext"]
