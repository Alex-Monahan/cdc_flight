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
**is it safe to call this stream idle?**
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

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
       s.confirmed_flush_lsn IS NOT NULL AS has_confirmed,
       CASE WHEN s.confirmed_flush_lsn IS NULL THEN NULL
            ELSE (s.confirmed_flush_lsn - '0/0')::BIGINT END AS confirmed_pos,
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
    lag_bytes: int | None = None
    confirmed_pos: int | None = None
    error: str | None = None

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
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _last: SlotSample | None = field(default=None, repr=False)
    _not_streaming_since: float | None = field(default=None, repr=False)
    _streaming_since: float | None = field(default=None, repr=False)
    _stream_interruptions: int = field(default=0, repr=False)
    _interruption_confirmed_pos: int | None = field(default=None, repr=False)
    _recovered_after_interruption: bool = field(default=False, repr=False)
    _lag_stable_since: float | None = field(default=None, repr=False)
    _prev_lag: int | None = field(default=None, repr=False)
    #: when the sampler last started failing outright, and whether it ever worked
    _unknown_since: float | None = field(default=None, repr=False)
    _ever_sampled: bool = field(default=False, repr=False)
    _ever_streamed: bool = field(default=False, repr=False)
    _unknown_samples: int = field(default=0, repr=False)

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> SourceHealth:
        self._thread = threading.Thread(target=self._loop, name="source-health", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval * 3)

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
            elif (
                self._interruption_confirmed_pos is not None
                and sample.streaming
                and sample.confirmed_pos is not None
                and sample.confirmed_pos > self._interruption_confirmed_pos
            ):
                self._recovered_after_interruption = True
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
            return SlotSample(at=now, error=f"{type(exc).__name__}: {exc}")
        if row is None:
            return SlotSample(at=now, exists=False)
        active, has_confirmed, confirmed_pos, lag = row
        return SlotSample(
            at=now,
            exists=True,
            active=bool(active),
            confirmed_pos=(int(confirmed_pos) if has_confirmed else None),
            lag_bytes=int(lag) if has_confirmed else None,
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

    def wait_for_confirmed(self, target: int, *, timeout: float) -> bool:
        """Wait while the live connector publishes a durable LSN to PostgreSQL.

        ``markBatchFinished()`` acknowledges records only after the destination
        transaction commits.  Debezium sends that acknowledgement to the logical
        slot on its next poll, so stopping immediately after a quiet callback can
        leave ``confirmed_flush_lsn`` behind a perfectly durable destination.  Keep
        the engine alive for this short, bounded hand-off; a timeout is a failed
        proof, never a reason to claim the slot advanced.
        """
        target = int(target)
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            sample = self.last
            if (
                sample is not None
                and sample.confirmed_pos is not None
                and sample.confirmed_pos >= target
            ):
                return True
            time.sleep(min(self.interval, max(0.01, deadline - time.monotonic())))
        sample = self.last
        return bool(
            sample is not None
            and sample.confirmed_pos is not None
            and sample.confirmed_pos >= target
        )

    def confirmed_at_least(self, target: int) -> bool:
        """Return the last sampled slot proof without waiting."""
        sample = self.last
        return bool(
            sample is not None
            and sample.confirmed_pos is not None
            and sample.confirmed_pos >= int(target)
        )

    def emit_idle_marker(self, target: int) -> bool:
        """Emit one transactional source marker on the separate primary route.

        Debezium only sends slot feedback while its replication loop receives a
        message.  A quiet source can therefore leave a destination commit durable
        while ``confirmed_flush_lsn`` remains frozen until the next business
        transaction.  The marker is an offset-only, whole PostgreSQL transaction;
        its own destination control-unit commit is what makes acknowledging the
        marker (and everything before it) safe under Invariant O.
        """
        marker = self.source_marker
        dsn = self.primary_dsn
        if marker is None or not dsn:
            return False
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
                return marker.emit(
                    conn,
                    IDLE_HEARTBEAT,
                    {"slot": self.slot_name, "durable_lsn": int(target)},
                )
        except Exception as exc:
            marker.last_error = f"{type(exc).__name__}: {exc}"
            log.error(
                "could not emit the transactional idle marker on the primary: %s",
                marker.last_error,
            )
            return False

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
        return SOURCE_HEALTH_STATES.parse(
            "streaming" if sample.streaming else "not_streaming"
        )

    def may_declare_idle(self, *, min_seconds: float) -> bool:
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
        # ROUND 12, WITHDRAWN AFTER MEASUREMENT. This is where round 12 additionally
        # required `confirmed_pos` to have advanced *past* the position held at the
        # last streaming -> not-streaming transition. That obligation is unsatisfiable
        # for a run with nothing left to deliver: `confirmed_flush_lsn` only moves
        # when the connector flushes a NEW offset, so a connector that blipped once
        # during start-up and then streamed cleanly for the whole run could never
        # satisfy it. Measured consequences on this tree: ordinary runs became unable
        # to declare idle and burned their whole `--max-seconds`; armed fault anchors
        # in `tests/rubric/1.1_exactly_once_pk` and `tests/rubric/1.7_fault_injection`
        # were pre-empted by the resulting failure; and the slow lane's own
        # walsender/sigkill scenarios timed out waiting for a slot the interruption
        # gate was keeping detached. The counters below are kept as DIAGNOSTICS in
        # `summary()`, but they are not a verdict. What actually closes B5 is the pair
        # above and below: a walsender attached CONTINUOUSLY for the whole quiet
        # window, and a backlog that is either gone or exactly unchanged for it (the
        # `lag_stable_since` clock, which any change resets — a source writer still
        # ahead of us therefore cannot look flat).
        #
        # (2) And the backlog must either be gone, or have stopped shrinking.
        #     MEASURED: after a reconnect, `confirmed_flush_lsn` stops tracking
        #     `pg_current_wal_lsn()` and freezes ~1.8 MB behind even once every
        #     row has been delivered, so "lag is small" alone is not a usable
        #     completion test - it would burn the run to `--max-seconds`. A lag
        #     that is still *decreasing* is unambiguous catch-up; one that has
        #     been flat for the whole quiet window means nothing more is coming.
        if sample.lag_bytes is None or sample.lag_bytes <= self.max_lag_bytes:
            return True
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
                "slot_stream_interruptions": self.stream_interruptions,
                "slot_recovered_after_interruption": self.recovered_after_interruption,
            }
        return {
            "slot_health": self.state(),
            "slot_exists": sample.exists,
            "slot_active": sample.active,
            "slot_confirmed_pos": sample.confirmed_pos,
            "slot_lag_bytes": sample.lag_bytes,
            "slot_streaming_for_sec": round(self.streaming_for, 1),
            "slot_ever_streamed": self.ever_streamed,
            "slot_lag_steady_for_sec": round(self.lag_steady_for, 1),
            "slot_stream_interruptions": self.stream_interruptions,
            "slot_recovered_after_interruption": self.recovered_after_interruption,
            **(
                {
                    "source_marker": self.source_marker.summary()
                }
                if self.source_marker is not None
                else {}
            ),
        }
