"""Source-corroborated liveness: is the connector actually streaming?

Why this exists (ADR 0001 §9.1; review finding Opus B5)
-------------------------------------------------------
`run_engine_bounded` used to declare a run `idle` - and therefore **successful** -
purely because no batch had arrived for `--idle-seconds`. Measured failure:

    $ psql ... -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                    WHERE backend_type = 'walsender'"
    ... ErrorHandler: Producer failure / Retry 1 of 3 retries will be attempted
    ... BaseSourceTask: Going to restart connector after 10 sec. after a retriable exception
    $ tail last_run.json
      "ok": true, "records": 118785, "stop_reason": "idle"        # of 250 000 rows
    EXIT=0

The connector wraps a dead walsender in a `RetriableException`
(`BaseSourceTask.java:518-556`, `startIfNeededAndPossible()` at `:462-495`) and
sleeps for `retriable.restart.connector.wait.ms` (10 s by default). During that
sleep no batches arrive, so an 8 s idle timer fires and the supervisor reports
success on a *partial* delivery. The default idle window is **shorter than
Debezium's restart backoff**, so this is the expected outcome, not a rare race.

A timer cannot distinguish "nothing left to do" from "not currently connected".
The source can: while the connector holds the replication stream, its slot is
`active`; during a restart backoff it is not, and the un-consumed WAL behind
`confirmed_flush_lsn` is exactly the work still owed.

This module samples `pg_replication_slots` on its own short-timeout connection
(which is also the 4.6 silently-dead-node detector) and answers one question:
**is it safe to call this stream idle?**  In service mode, a slot sample is only
corroboration.  The service witness also requires the admitted engine thread to
be alive and a recent callback/commit/ack from this Flight.  A different client
can keep a slot active, and an attached walsender can hold WAL without delivering
anything; neither can satisfy that local proof.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from . import faults
from .machines import SOURCE_HEALTH_STATES
from .source_marker import IDLE_HEARTBEAT, SourceMarker

log = logging.getLogger("cdc_flight.source_health")

#: How far `confirmed_flush_lsn` may trail `pg_current_wal_lsn()` and still count
#: as "caught up".
#:
#: MEASURED (2026-07-30, 60 000-row stream into local DuckDB, per-batch offset
#: flush). A healthy run settles at **328-384 bytes** of lag within a second of
#: the last batch and stays there. The B5 failure - walsender killed mid-stream,
#: connector in retriable-restart backoff - sits at **2.1-3.4 MB** for the whole
#: dangerous window. 64 KiB is ~170x the observed healthy value and ~35x below
#: the observed failure, which is as much separation as this signal offers.
#:
#: Note that `pg_stat_replication.sent_lsn` is NOT a usable progress signal here:
#: it read 0-48 bytes behind current WAL even while `confirmed_flush_lsn` was
#: 19 MB behind, because the walsender had already sent everything into
#: Debezium's in-memory queue. Only the *confirmed* position reflects what the
#: consumer has durably taken.
DEFAULT_MAX_IDLE_LAG_BYTES = 64 * 1024

_SLOT_SQL = """
SELECT s.active,
       s.active_pid,
       s.confirmed_flush_lsn IS NOT NULL AS has_confirmed,
       CASE WHEN s.confirmed_flush_lsn IS NULL THEN NULL
            ELSE (s.confirmed_flush_lsn - '0/0')::BIGINT END AS confirmed_pos,
       CASE WHEN s.restart_lsn IS NULL THEN NULL
            ELSE (s.restart_lsn - '0/0')::BIGINT END AS restart_pos,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn), 0)::BIGINT
FROM pg_replication_slots s
WHERE s.slot_name = %s
"""


@dataclass
class SlotSample:
    """One observation of the replication slot, or the reason there is none."""

    at: float
    exists: bool = False
    active: bool = False
    #: PostgreSQL's current walsender PID.  It is operator corroboration only:
    #: the PID changes on every legitimate reconnect/takeover and is never used
    #: as the service's identity proof.
    active_pid: int | None = None
    lag_bytes: int | None = None
    confirmed_pos: int | None = None
    restart_pos: int | None = None
    error: str | None = None
    #: Wall-clock time near the SQL result. ``at`` remains monotonic for duration
    #: clocks; this value makes the persisted operator sample attributable to a time.
    observed_at: datetime | None = None

    @property
    def streaming(self) -> bool:
        """True when a walsender is attached to our slot right now."""
        return self.exists and self.active

    @property
    def unknown(self) -> bool:
        return self.error is not None


@dataclass
class SourceHealth:
    """Background sampler for the replication slot backing this run.

    Deliberately fail-soft: if the slot cannot be queried at all (no
    credentials, a firewall, `psycopg` missing) the sampler reports `unknown` and
    the supervisor falls back to timer-only idle detection with a warning, rather
    than turning every run into a `--max-seconds` wait.
    """

    dsn: str
    slot_name: str
    #: A separate write route for the one-shot transactional marker used to make
    #: the post-commit hand-off observable on a quiet source.  It is deliberately
    #: not the Debezium replication connection; in a hot-standby topology this is
    #: the primary DSN.
    primary_dsn: str | None = None
    source_marker: SourceMarker | None = None
    max_lag_bytes: int = DEFAULT_MAX_IDLE_LAG_BYTES
    interval: float = 0.5
    connect_timeout: int = 5
    #: Bounds a query on an ALREADY-CONNECTED socket. `connect_timeout` does not:
    #: it covers the handshake, and a source that goes dark mid-connection leaves the
    #: sampler blocked for ever, which is how "the source is dark" stopped being
    #: observable at all (Codex r2 MAJOR-4).
    query_timeout_ms: int = 4000
    #: Optional in-process liveness projection. It is called by the sampler thread
    #: itself so a slow destination run-log write cannot make a fresh source witness
    #: look stale to the service watchdog.
    observation_callback: Callable[[SourceHealth], None] | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _last: SlotSample | None = field(default=None, repr=False)
    _not_streaming_since: float | None = field(default=None, repr=False)
    _streaming_since: float | None = field(default=None, repr=False)
    _stream_interruptions: int = field(default=0, repr=False)
    _interruption_confirmed_pos: int | None = field(default=None, repr=False)
    _recovered_after_interruption: bool = field(default=False, repr=False)
    #: A walsender that repeatedly reattaches without advancing confirmed WAL is
    #: still in retry/backoff, even though point samples occasionally say active.
    #: The clock clears after stable streaming or confirmed-LSN movement; a single
    #: brief network blip therefore does not turn a quiet connected source dark.
    _retrying_since: float | None = field(default=None, repr=False)
    _lag_stable_since: float | None = field(default=None, repr=False)
    _prev_lag: int | None = field(default=None, repr=False)
    #: when the sampler last started failing outright, and whether it ever worked
    _unknown_since: float | None = field(default=None, repr=False)
    _ever_sampled: bool = field(default=False, repr=False)
    _ever_streamed: bool = field(default=False, repr=False)
    _unknown_samples: int = field(default=0, repr=False)
    _last_idle_marker_lsn: int | None = field(default=None, repr=False)
    #: Service-mode evidence is kept separately from the generic slot fold.  A
    #: sampler can say "active" while our engine is dead or while no callback has
    #: completed; those observations must age into source_dark instead of renewing.
    _service_status: str | None = field(default=None, repr=False)
    _service_stalled_since: float | None = field(default=None, repr=False)
    _service_engine_thread_dead: bool = field(default=False, repr=False)
    _service_lag_bytes: int | None = field(default=None, repr=False)

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> SourceHealth:
        self._thread = threading.Thread(target=self._loop, name="source-health", daemon=True)
        self._thread.start()
        return self

    def stop(self, *, timeout: float | None = None) -> bool:
        """Stop the sampler under an explicit bound and report quiescence.

        A join of interval*3 was shorter than the configured JDBC query budget, so a
        sampler blocked in a source read could outlive the run with no durable verdict.
        The default covers one bounded query plus the connection handshake; callers
        with a run-level budget may provide a tighter/longer bound.
        """
        self._stop.set()
        if self._thread is not None:
            wait = (
                timeout
                if timeout is not None
                else self.connect_timeout + self.query_timeout_ms / 1000.0 + 1.0
            )
            self._thread.join(timeout=max(0.01, wait))
            return not self._thread.is_alive()
        return True

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._ingest(self.sample_once())
            self._stop.wait(self.interval)

    def _ingest(self, sample: SlotSample) -> None:
        """Fold one observation into the derived clocks.

        A separate method because the *interesting* transitions (a sampler that
        worked and then went dark) are otherwise only reachable by breaking a real
        network, and they are the ones TODO 4.6(b) is about.
        """
        with self._lock:
            was_streaming = self._last is not None and self._last.streaming
            if (
                was_streaming
                and not sample.streaming
                # Slot creation/snapshot startup can briefly attach and detach
                # before PostgreSQL has published any confirmed position. That is
                # not a delivery interruption; counting it would fail closed on
                # every fresh snapshot run.
                and self._last.confirmed_pos is not None
            ):
                self._stream_interruptions += 1
                self._interruption_confirmed_pos = self._last.confirmed_pos
                self._recovered_after_interruption = False
                if self._retrying_since is None:
                    self._retrying_since = sample.at
            elif (
                self._interruption_confirmed_pos is not None
                and sample.streaming
                and sample.confirmed_pos is not None
                and sample.confirmed_pos > self._interruption_confirmed_pos
            ):
                self._recovered_after_interruption = True
                self._retrying_since = None
            self._last = sample
            if sample.streaming:
                self._ever_streamed = True
            # `unknown` used to be treated as "streaming" here, which RESET the
            # not-streaming clock: a blackholed Postgres therefore reported
            # `not_streaming_for == 0` and `run_engine_bounded`'s --max-seconds
            # guard could never fire (TODO 4.6(b), measured). An observation we
            # could not make is not evidence that anything is healthy.
            if sample.streaming:
                self._not_streaming_since = None
            elif self._not_streaming_since is None:
                self._not_streaming_since = sample.at

            if sample.unknown:
                self._unknown_samples += 1
                if self._unknown_since is None:
                    self._unknown_since = sample.at
            else:
                self._unknown_since = None
                self._ever_sampled = True

            if sample.streaming:
                if self._streaming_since is None:
                    self._streaming_since = sample.at
                elif (
                    self._retrying_since is not None
                    and sample.at - self._streaming_since >= max(self.interval * 10, 5.0)
                ):
                    self._retrying_since = None
            else:
                self._streaming_since = None

            # A backlog is stable only while its value is unchanged. The old
            # ``lag_decreased_at`` clock was not reset when lag INCREASED, so a
            # continuously growing source could eventually be misclassified as a
            # flat, finished backlog. That was the remaining false-green shape in
            # the walsender probe.
            lag = sample.lag_bytes
            if lag is not None:
                if self._prev_lag is None or lag != self._prev_lag:
                    self._lag_stable_since = sample.at
                self._prev_lag = lag
        callback = self.observation_callback
        if callback is not None:
            callback(self)

    # -- sampling ----------------------------------------------------------- #
    def sample_once(self) -> SlotSample:
        now = time.monotonic()
        try:
            import psycopg

            # `connect_timeout` bounds the HANDSHAKE and nothing else. A relay that
            # blackholes packets *after* the socket is established leaves the query
            # blocked on a recv that will never return, so this sampler stops
            # publishing entirely: `unknown` is never recorded, `unknown_for` never
            # reaches `CDC_SOURCE_DARK_SECONDS`, and the run dies of the shutdown
            # symptom (`hung`) with the diagnosis (`source_dark`) never formed. That
            # made the network-blackhole proof itself timing-dependent, which is
            # exactly the kind of evidence rubric 1.7 is not allowed to rest on
            # (Codex r2 MAJOR-4).
            #
            # Two bounds, because they cover different halves: `statement_timeout` is
            # the server's, and a server we cannot reach cannot enforce it;
            # `tcp_user_timeout` plus keepalives are the client's, and they are what
            # actually fires against a blackhole. Both are well under
            # `CDC_SOURCE_DARK_SECONDS`.
            with psycopg.connect(
                self.dsn,
                autocommit=True,
                connect_timeout=self.connect_timeout,
                options=f"-c statement_timeout={self.query_timeout_ms}",
                keepalives=1,
                keepalives_idle=1,
                keepalives_interval=1,
                keepalives_count=2,
                tcp_user_timeout=self.query_timeout_ms,
            ) as conn:
                row = conn.execute(_SLOT_SQL, (self.slot_name,)).fetchone()
        except Exception as exc:
            return SlotSample(
                at=now,
                error=f"{type(exc).__name__}: {exc}",
                observed_at=datetime.now(UTC),
            )
        if row is None:
            return SlotSample(at=now, exists=False, observed_at=datetime.now(UTC))
        active, active_pid, has_confirmed, confirmed_pos, restart_pos, lag = row
        return SlotSample(
            at=now,
            exists=True,
            active=bool(active),
            active_pid=(int(active_pid) if active_pid is not None else None),
            confirmed_pos=(int(confirmed_pos) if has_confirmed else None),
            restart_pos=(int(restart_pos) if restart_pos is not None else None),
            lag_bytes=int(lag) if has_confirmed else None,
            observed_at=datetime.now(UTC),
        )

    # -- what the supervisor asks ------------------------------------------- #
    @property
    def last(self) -> SlotSample | None:
        with self._lock:
            return self._last

    @property
    def not_streaming_for(self) -> float:
        """Seconds the slot has continuously had no walsender attached."""
        with self._lock:
            if self._not_streaming_since is None:
                return 0.0
            return time.monotonic() - self._not_streaming_since

    @property
    def streaming_for(self) -> float:
        """Seconds the slot has *continuously* had a walsender attached."""
        with self._lock:
            if self._streaming_since is None:
                return 0.0
            return time.monotonic() - self._streaming_since

    @property
    def stream_interruptions(self) -> int:
        """Number of observed streaming -> non-streaming transitions this run."""
        with self._lock:
            return self._stream_interruptions

    @property
    def recovered_after_interruption(self) -> bool:
        """Whether confirmed WAL advanced after the last observed interruption."""
        with self._lock:
            return self._recovered_after_interruption

    @property
    def unknown_for(self) -> float:
        """Seconds the source has continuously been *unaskable*.

        0.0 when the last sample succeeded. This is the signal the supervisor uses
        to refuse a successful run on a source it cannot see (TODO 4.6(b)).
        """
        with self._lock:
            if self._unknown_since is None:
                return 0.0
            return time.monotonic() - self._unknown_since

    @property
    def retrying_for(self) -> float:
        """Seconds since an interrupted stream last proved stable recovery."""
        with self._lock:
            if self._retrying_since is None:
                return 0.0
            return time.monotonic() - self._retrying_since

    @property
    def dark_for(self) -> float:
        """Seconds the last successful source has been dark or unaskable."""
        with self._lock:
            service_since = self._service_stalled_since
        service_for = (
            max(0.0, time.monotonic() - service_since)
            if service_since is not None
            else 0.0
        )
        return max(self.not_streaming_for, self.unknown_for, self.retrying_for, service_for)

    @property
    def ever_sampled(self) -> bool:
        """True once the slot has been read successfully at least once.

        The whole of the fail-soft distinction: a sampler that never worked cannot
        tell us anything and must not block a run; one that worked and stopped is
        reporting an outage.
        """
        with self._lock:
            return self._ever_sampled

    @property
    def ever_streamed(self) -> bool:
        """True once a walsender has actually been observed on this slot."""
        with self._lock:
            return self._ever_streamed

    @property
    def lag_steady_for(self) -> float:
        """Seconds since the slot's observed backlog last changed."""
        with self._lock:
            if self._lag_stable_since is None:
                return 0.0
            return time.monotonic() - self._lag_stable_since

    def wait_for_confirmed(
        self, target: int, *, timeout: float, marker_lsn: int | None = None
    ) -> bool:
        """Wait while the live connector publishes a durable LSN to PostgreSQL.

        ``markBatchFinished()`` acknowledges records only after the destination
        transaction commits.  Debezium sends that acknowledgement to the logical
        slot on its next poll, so stopping immediately after a quiet callback can
        leave ``confirmed_flush_lsn`` behind a perfectly durable destination.  Keep
        the engine alive for this short, bounded hand-off; a timeout is a failed
        proof, never a reason to claim the slot advanced.
        """
        target = int(target)
        marker_lsn = marker_lsn if marker_lsn is not None else self._last_idle_marker_lsn
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            sample = self.last
            if (
                sample is not None
                and sample.confirmed_pos is not None
                and sample.confirmed_pos >= target
            ):
                self._record_idle_ack(target, marker_lsn=marker_lsn)
                return True
            time.sleep(min(self.interval, max(0.01, deadline - time.monotonic())))
        sample = self.last
        confirmed = bool(
            sample is not None
            and sample.confirmed_pos is not None
            and sample.confirmed_pos >= target
        )
        if confirmed:
            self._record_idle_ack(target, marker_lsn=marker_lsn)
        return confirmed

    @staticmethod
    def _record_idle_ack(target: int, *, marker_lsn: int | None = None) -> None:
        faults.runtime_state(
            completion_marker_state="shutdown_idle_acknowledged",
            marker_ack_target=target,
            marker_ack_lsn=marker_lsn,
        )
        faults.matrix_crash("shutdown_idle_marker_acknowledged")

    def confirmed_at_least(self, target: int) -> bool:
        """Return the last sampled slot proof without waiting."""
        sample = self.last
        return bool(
            sample is not None
            and sample.confirmed_pos is not None
            and sample.confirmed_pos >= int(target)
        )

    def emit_idle_marker(self, target: int) -> int | None:
        """Emit one transactional source marker on the separate primary route.

        Debezium only sends slot feedback while its replication loop receives a
        message.  A quiet source can therefore leave a destination commit durable
        while ``confirmed_flush_lsn`` remains frozen until the next business
        transaction.  The marker is an offset-only, whole PostgreSQL transaction;
        its own destination control-unit commit is what makes acknowledging the
        marker (and everything before it) safe under Invariant O.
        """
        marker_lsn = self.emit_marker(
            self.source_marker,
            IDLE_HEARTBEAT,
            {"slot": self.slot_name, "durable_lsn": int(target)},
        )
        if marker_lsn is not None:
            self._last_idle_marker_lsn = marker_lsn
            faults.runtime_state(
                completion_marker_state="shutdown_idle_written", marker_lsn=marker_lsn
            )
            faults.matrix_crash("shutdown_idle_marker_written")
        return marker_lsn

    def emit_marker(self, marker, reason: str, payload: dict) -> int | None:
        """Write one whole, transactional marker on the primary route.

        Returns **the LSN PostgreSQL assigned it**, or ``None`` when the source
        could not be written to at all (a read-only replica, a missing privilege,
        an exhausted budget, an operator who disabled markers).  This is the one
        place in the Flight that writes to the source, and both callers need the
        same three things from it: the separate primary connection, the same
        bounded timeouts the sampler uses, and an error that is an operational
        condition rather than a crash.
        """
        dsn = self.primary_dsn
        if marker is None or not dsn:
            return None
        try:
            import psycopg

            with psycopg.connect(
                dsn,
                autocommit=True,
                connect_timeout=self.connect_timeout,
                options=f"-c statement_timeout={self.query_timeout_ms}",
                keepalives=1,
                keepalives_idle=1,
                keepalives_interval=1,
                keepalives_count=2,
                tcp_user_timeout=self.query_timeout_ms,
            ) as conn:
                if not marker.emit(conn, reason, payload):
                    return None
                return marker.last_lsn
        except Exception as exc:
            marker.last_error = f"{type(exc).__name__}: {exc}"
            log.error(
                "could not emit the transactional %s marker on the primary: %s",
                reason, marker.last_error,
            )
            return None

    def state(self, *, dark_after: float = 0.0) -> str:
        """The fold's classification, as ONE declared value (rubric 1.9).

        `SourceHealth` is a **fold over observations**, not a state machine, and it
        should stay one: there is no durable state and no transition anybody can cut. But
        the *classification* of the fold was written out three separate times — in
        `may_declare_idle()`, in `summary()`, and again in the supervisor's
        `ever_sampled and unknown_for >= ...` test — and the value that mattered most had
        no name at all. `unknown_never_sampled` is A51 row 50: the documented fail-open
        where a source that was dark before we ever looked degrades the run to the
        timer-only path and can report success on a delivery that never started.

        The domain is `machines.SOURCE_HEALTH_STATES`. `dark_after` (the run's
        `CDC_SOURCE_DARK_SECONDS`) separates `unknown` from `dark`; passing 0 means "do
        not make that distinction", which is what a caller with no threshold wants.
        """
        sample = self.last
        if sample is None:
            return SOURCE_HEALTH_STATES.parse("unsampled")
        if sample.unknown:
            if not self.ever_sampled:
                return SOURCE_HEALTH_STATES.parse("unknown_never_sampled")
            if dark_after > 0 and self.unknown_for >= dark_after:
                return SOURCE_HEALTH_STATES.parse("dark")
            return SOURCE_HEALTH_STATES.parse("unknown")
        if (
            dark_after > 0
            and self.ever_sampled
            and not sample.streaming
            and self.dark_for >= dark_after
        ):
            return SOURCE_HEALTH_STATES.parse("dark")
        if (
            dark_after > 0
            and self.ever_sampled
            and sample.streaming
            and self.retrying_for >= dark_after
        ):
            return SOURCE_HEALTH_STATES.parse("dark")
        return SOURCE_HEALTH_STATES.parse(
            "streaming" if sample.streaming else "not_streaming"
        )

    def _publish_service_status(
        self,
        status: str,
        *,
        observed_at: float,
        engine_thread_alive: bool,
        lag_bytes: int | None,
    ) -> str:
        """Record the service verdict and its fail-closed aging clock."""
        with self._lock:
            self._service_status = status
            self._service_engine_thread_dead = not engine_thread_alive
            self._service_lag_bytes = lag_bytes
            if status in {"connected_quiet", "connected_busy"}:
                self._service_stalled_since = None
            elif (
                status in {"stalled", "unproven", "engine_thread_dead"}
                and self._service_stalled_since is None
            ):
                self._service_stalled_since = observed_at
        return status

    def service_status(
        self,
        received_high_water: int | None = None,
        *,
        engine_thread_alive: bool,
        own_progress_at: float | None,
        own_ack_at: float | None,
        own_ack_lsn: int | None,
        durable_lsn: int | None,
        progress_stale_after: float,
    ) -> str:
        """Classify source evidence for the single-process lease watchdog.

        The service witness is deliberately conjunctive:

        * the sampler observation is fresh and the slot is active;
        * the *admitted* Debezium engine thread is still alive;
        * this process has recently completed a callback/commit and a
          ``markBatchFinished`` acknowledgement tied to a durable resume point.

        ``connected_quiet`` and ``connected_busy`` are therefore classifications
        of a proven Flight-owned stream.  If that proof goes stale, the source's
        retained-WAL lag is used as the discriminator: pending source WAL with no
        Flight progress is ``stalled``.  The cluster-side lag is not used as a
        stand-alone heartbeat and no slot activity can bypass these checks.

        ``max_lag_bytes`` is intentionally not part of this service decision.  The
        exact source-position relation (zero versus pending WAL) and our own
        progress are the signal; a size cutoff would turn an outage into a tuning
        exercise and would recreate the gate's false-green shape.
        """
        now = time.monotonic()
        sample = self.last
        if sample is None:
            return self._publish_service_status(
                "unobserved",
                observed_at=now,
                engine_thread_alive=engine_thread_alive,
                lag_bytes=None,
            )
        if now - sample.at > max(self.interval * 3.0, 1.0):
            return self._publish_service_status(
                "stale",
                observed_at=now,
                engine_thread_alive=engine_thread_alive,
                lag_bytes=sample.lag_bytes,
            )
        if sample.unknown or not sample.streaming:
            return self._publish_service_status(
                "unknown" if sample.unknown else "disconnected",
                observed_at=now,
                engine_thread_alive=engine_thread_alive,
                lag_bytes=sample.lag_bytes,
            )
        if not engine_thread_alive:
            return self._publish_service_status(
                "engine_thread_dead",
                observed_at=now,
                engine_thread_alive=False,
                lag_bytes=sample.lag_bytes,
            )

        # A reconnect after a walsender interruption is not recovery until this
        # Flight's confirmed position advances.  If PostgreSQL is retaining WAL
        # while that proof is absent, start the fail-closed stall clock at the
        # interruption rather than granting a fresh active sample a new lease.
        if (
            self.stream_interruptions
            and not self.recovered_after_interruption
            and (sample.lag_bytes or 0) > 0
        ):
            return self._publish_service_status(
                "stalled",
                observed_at=now,
                engine_thread_alive=True,
                lag_bytes=sample.lag_bytes,
            )

        # The local proof is required even when a different client has already
        # moved confirmed_flush_lsn to the current WAL.  That client cannot create
        # any of these timestamps or the durable LSN owned by this process.
        missing_own_proof = (
            own_progress_at is None
            or own_ack_at is None
            or own_ack_lsn is None
            or durable_lsn is None
        )
        if missing_own_proof:
            status = "stalled" if (sample.lag_bytes or 0) > 0 else "unproven"
            return self._publish_service_status(
                status,
                observed_at=now,
                engine_thread_alive=True,
                lag_bytes=sample.lag_bytes,
            )

        if sample.confirmed_pos is not None and sample.confirmed_pos > durable_lsn:
            # This is the source-side form of Invariant O: another client may have
            # acknowledged the slot beyond this Flight's durable destination point.
            return self._publish_service_status(
                "stalled",
                observed_at=now,
                engine_thread_alive=True,
                lag_bytes=sample.lag_bytes,
            )

        progress_age = now - max(float(own_progress_at), float(own_ack_at))
        if progress_age > max(float(progress_stale_after), 0.0):
            status = "stalled" if (sample.lag_bytes or 0) > 0 else "unproven"
            return self._publish_service_status(
                status,
                observed_at=now,
                engine_thread_alive=True,
                lag_bytes=sample.lag_bytes,
            )

        # A Flight-owned acknowledgement is the only quiet-position proof.  The
        # per-slot value is exact here: zero means our delivered high-water is at
        # or below the slot confirmation; positive means the live Flight is busy
        # catching up.  The cluster-retained lag above remains the fail-closed
        # discriminator when this local progress proof disappears.
        outstanding = self.per_slot_outstanding_bytes(received_high_water)
        if outstanding is None:
            return self._publish_service_status(
                "unproven",
                observed_at=now,
                engine_thread_alive=True,
                lag_bytes=sample.lag_bytes,
            )
        return self._publish_service_status(
            "connected_quiet" if outstanding == 0 else "connected_busy",
            observed_at=now,
            engine_thread_alive=True,
            lag_bytes=sample.lag_bytes,
        )

    def outstanding_bytes(self, received_high_water: int | None) -> int | None:
        """OUR undelivered backlog, in bytes, or ``None`` when it cannot be read.

        ``slot_lag_bytes`` is ``pg_wal_lsn_diff(pg_current_wal_lsn(),
        confirmed_flush_lsn)`` — the WAL PostgreSQL must RETAIN, which is a
        CLUSTER-wide quantity.  Another database in the same cluster moves
        ``pg_current_wal_lsn()`` on every 0.5 s sample without a single byte of
        it being ours, which is exactly how review r12 (R12-3) measured every
        bounded run burning its whole ``--max-seconds`` under an ordinary
        neighbour: 60.44 s with a co-tenant against 10.55 s without.

        The per-slot reference is the highest source LSN the connector has
        actually DELIVERED to this run (seeded with the durable resume point, so
        a run that receives nothing still has one).  What that reference minus
        ``confirmed_flush_lsn`` measures is precisely "delivered to us and not
        yet durable", which is what "are we behind?" means and what rubric B5 is
        about.  It is unaffected by a co-tenant and it does not need a second
        clock to tell growth from catch-up.
        """
        sample = self.last
        if sample is None or sample.lag_bytes is None:
            return None
        per_slot = self.per_slot_outstanding_bytes(received_high_water)
        if per_slot is None:
            # No per-slot reference available: fall back to the retained-WAL
            # figure rather than inventing a smaller one.
            return sample.lag_bytes
        return per_slot

    def per_slot_outstanding_bytes(self, received_high_water: int | None) -> int | None:
        """Return ``received_high_water_lsn - confirmed_flush_lsn`` in bytes.

        Both LSNs are sampled/maintained as PostgreSQL's numeric WAL byte positions:
        the high-water mark is the greatest source LSN delivered to this handler, and
        ``confirmed_flush_lsn`` is the slot acknowledgement observed in the same
        source-health query. This is the lag written to ``run_logs``; it is not the
        cluster-wide retained-WAL figure in ``SlotSample.lag_bytes``.
        """
        sample = self.last
        if (
            sample is None
            or sample.confirmed_pos is None
            or received_high_water is None
        ):
            return None
        return max(0, int(received_high_water) - int(sample.confirmed_pos))

    def operator_lag_context(self, received_high_water: int | None) -> dict:
        """Describe the exact pair and timestamp behind an operator lag value."""
        sample = self.last
        return {
            "lag_definition": (
                "max(0, received_high_water_lsn - confirmed_flush_lsn)"
            ),
            "received_high_water_lsn": received_high_water,
            "confirmed_flush_lsn": (
                sample.confirmed_pos if sample is not None else None
            ),
            "cluster_retained_lag_bytes": (
                sample.lag_bytes if sample is not None else None
            ),
            "lag_sampled_at": (
                sample.observed_at.isoformat()
                if sample is not None and sample.observed_at is not None
                else None
            ),
        }

    def fall_behind_reason(
        self,
        *,
        current_wal_lsn: int | None,
        oldest_pending_source_ts_ms: int | None,
        now_ms: int,
        size_threshold_bytes: int | None,
        time_threshold_ms: int | None,
    ) -> str | None:
        """Use this fold's slot observation plus admitted-unit age for refresh.

        The scheduler does not open another slot sampler.  An absent oldest pending
        timestamp remains unknown/false; the timestamp of the last applied event is
        never substituted as queue age.
        """
        from .backfill import fall_behind_reason

        sample = self.last
        return fall_behind_reason(
            current_wal_lsn=current_wal_lsn,
            confirmed_flush_lsn=(sample.confirmed_pos if sample else None),
            oldest_pending_source_ts_ms=oldest_pending_source_ts_ms,
            now_ms=now_ms,
            size_threshold_bytes=size_threshold_bytes,
            time_threshold_ms=time_threshold_ms,
        )

    def may_declare_idle(
        self, *, min_seconds: float, received_high_water: int | None = None
    ) -> bool:
        """Corroborate a quiet timer against the source.

        Requires the source to have agreed *continuously* for `min_seconds`, not
        merely at the instant we asked. A single sample is not enough: measured
        on the walsender-kill scenario, the connector reconnects for about one
        second between restart attempts, and a point-in-time check that happened
        to land in that second declared a 2.3 MB backlog "idle".

        Returns `True` when the source could **never** be consulted, so a missing
        `psycopg`, bad credentials or a firewall that was always there degrade to
        the old timer-only behaviour instead of turning every run into a
        `--max-seconds` wait. It does **not** return True for a source that was
        answering and has gone dark: that is the silently-dead-node shape (rubric
        4.6), and the measured consequence of the old unconditional fail-soft was a
        blackholed Postgres exiting `ok: true` on a partial delivery (TODO 4.6(b)).
        """
        sample = self.last
        if sample is None:
            return False  # not sampled yet - the run has barely started
        if sample.unknown:
            return not self._ever_sampled
        # ``streaming_for`` is derived from the last observed transition.  A stale
        # active sample cannot prove that the walsender is still attached: the
        # walsender-kill path can release the slot immediately after that sample and
        # Debezium then sits in its restart backoff while the supervisor's idle timer
        # expires.  Require a recent successful observation before accepting the
        # continuously-streaming proof; the sampler will publish the inactive state
        # on its next bounded poll instead of allowing stale state to become success.
        if time.monotonic() - sample.at > max(self.interval * 3.0, 1.0):
            return False
        # (1) A walsender must have been attached to our slot for the whole quiet
        #     window. This is the signal that catches the B5 failure: during a
        #     retriable restart the slot is released, and the connector briefly
        #     re-attaches between attempts - so a point-in-time check is not
        #     enough, but a sustained one is.
        if self.streaming_for < min_seconds:
            return False
        # (2) And OUR backlog must be gone.  ROUND 13 (review r12, R12-3 and
        #     R12-6).  Round 12 measured this against `pg_current_wal_lsn()`,
        #     which is cluster-wide: the reviewer proved that one ordinary
        #     co-tenant database writing WAL made this branch unsatisfiable and
        #     cost every bounded run its entire `--max-seconds` (60.44 s against
        #     10.55 s, one file swapped).  `outstanding_bytes` measures the
        #     per-slot quantity instead — delivered to us, not yet confirmed —
        #     so an unrelated writer cannot make us look behind, and a backlog
        #     that grows *because we are behind* still does.
        outstanding = self.outstanding_bytes(received_high_water)
        if outstanding is None or outstanding <= self.max_lag_bytes:
            return True
        # (3) ROUND 12's withdrawn obligation, restored CONDITIONALLY, which is
        #     what makes it satisfiable (review r12, R12-6, and its own prescribed
        #     fix).  Round 12 required unconditionally that `confirmed_pos` have
        #     advanced past the position held at the last streaming ->
        #     not-streaming transition; that is unsatisfiable for a run with
        #     nothing left to deliver, because `confirmed_flush_lsn` only moves
        #     when the connector flushes a NEW offset — so a connector that
        #     blipped once at start-up and then streamed cleanly could never
        #     satisfy it, ordinary runs burned their whole `--max-seconds`, and
        #     armed fault anchors were pre-empted.  Reaching here means there IS
        #     something outstanding, which is exactly the B5 shape the obligation
        #     exists for: streaming, a walsender interruption with real bytes
        #     undelivered, and a reattachment whose `confirmed_pos` never advances
        #     past the interruption.  A run with nothing outstanding returned True
        #     above and never sees this gate.
        if self._stream_interruptions and not self._recovered_after_interruption:
            return False
        #     Otherwise: a backlog still *changing* is unambiguous catch-up; one
        #     that has been exactly flat for the whole quiet window means nothing
        #     more is coming.
        return self.lag_steady_for >= min_seconds

    def summary(self) -> dict:
        sample = self.last
        if sample is None:
            return {"slot_health": self.state()}
        if sample.unknown:
            return {
                # The declared classification, so `unknown_never_sampled` - the
                # fail-open A51 row 50 is about - finally appears in the run summary
                # instead of being inferred from `slot_ever_sampled` next to it.
                "slot_health": self.state(),
                "slot_error": sample.error,
                "slot_unknown_for_sec": round(self.unknown_for, 1),
                "slot_ever_sampled": self.ever_sampled,
                "slot_ever_streamed": self.ever_streamed,
                "slot_not_streaming_for_sec": round(self.not_streaming_for, 1),
                "slot_retrying_for_sec": round(self.retrying_for, 1),
                "slot_stream_interruptions": self.stream_interruptions,
                "slot_recovered_after_interruption": self.recovered_after_interruption,
            }
        return {
            "slot_health": self.state(),
            "slot_exists": sample.exists,
            "slot_active": sample.active,
            "slot_active_pid": sample.active_pid,
            "slot_confirmed_pos": sample.confirmed_pos,
            "slot_restart_pos": sample.restart_pos,
            "slot_lag_bytes": sample.lag_bytes,
            "slot_lag_sampled_at": (
                sample.observed_at.isoformat()
                if sample.observed_at is not None
                else None
            ),
            "slot_streaming_for_sec": round(self.streaming_for, 1),
            "slot_retrying_for_sec": round(self.retrying_for, 1),
            "slot_ever_streamed": self.ever_streamed,
            "slot_lag_steady_for_sec": round(self.lag_steady_for, 1),
            "slot_stream_interruptions": self.stream_interruptions,
            "slot_recovered_after_interruption": self.recovered_after_interruption,
            "service_liveness_status": self._service_status,
            "service_engine_thread_dead": self._service_engine_thread_dead,
            "service_lag_bytes": self._service_lag_bytes,
            "service_stalled_for_sec": round(
                max(0.0, time.monotonic() - self._service_stalled_since)
                if self._service_stalled_since is not None
                else 0.0,
                1,
            ),
            **(
                {
                    "source_marker": self.source_marker.summary()
                }
                if self.source_marker is not None
                else {}
            ),
        }
