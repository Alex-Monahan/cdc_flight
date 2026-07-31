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
    max_lag_bytes: int = DEFAULT_MAX_IDLE_LAG_BYTES
    interval: float = 0.5
    connect_timeout: int = 5
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _last: SlotSample | None = field(default=None, repr=False)
    _not_streaming_since: float | None = field(default=None, repr=False)
    _streaming_since: float | None = field(default=None, repr=False)
    _lag_decreased_at: float | None = field(default=None, repr=False)
    _prev_lag: int | None = field(default=None, repr=False)
    #: when the sampler last started failing outright, and whether it ever worked
    _unknown_since: float | None = field(default=None, repr=False)
    _ever_sampled: bool = field(default=False, repr=False)
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
            self._last = sample
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

            # "Still catching up" == the backlog is shrinking. A backlog that
            # has stopped shrinking means nothing more is coming, even when it
            # is large (see `may_declare_idle`).
            lag = sample.lag_bytes
            if lag is not None and self._prev_lag is not None and lag < self._prev_lag:
                self._lag_decreased_at = sample.at
            if lag is not None:
                self._prev_lag = lag
            if self._lag_decreased_at is None:
                self._lag_decreased_at = sample.at

    # -- sampling ----------------------------------------------------------- #
    def sample_once(self) -> SlotSample:
        now = time.monotonic()
        try:
            import psycopg

            with psycopg.connect(
                self.dsn, autocommit=True, connect_timeout=self.connect_timeout
            ) as conn:
                row = conn.execute(_SLOT_SQL, (self.slot_name,)).fetchone()
        except Exception as exc:
            return SlotSample(at=now, error=f"{type(exc).__name__}: {exc}")
        if row is None:
            return SlotSample(at=now, exists=False)
        active, has_confirmed, lag = row
        return SlotSample(
            at=now,
            exists=True,
            active=bool(active),
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
    def lag_steady_for(self) -> float:
        """Seconds since the slot's backlog last got smaller."""
        with self._lock:
            if self._lag_decreased_at is None:
                return 0.0
            return time.monotonic() - self._lag_decreased_at

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
        # (1) A walsender must have been attached to our slot for the whole quiet
        #     window. This is the signal that catches the B5 failure: during a
        #     retriable restart the slot is released, and the connector briefly
        #     re-attaches between attempts - so a point-in-time check is not
        #     enough, but a sustained one is.
        if self.streaming_for < min_seconds:
            return False
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
            return {"slot_health": "unsampled"}
        if sample.unknown:
            return {
                "slot_health": "unknown",
                "slot_error": sample.error,
                "slot_unknown_for_sec": round(self.unknown_for, 1),
                "slot_ever_sampled": self.ever_sampled,
                "slot_not_streaming_for_sec": round(self.not_streaming_for, 1),
            }
        return {
            "slot_health": "streaming" if sample.streaming else "not_streaming",
            "slot_exists": sample.exists,
            "slot_active": sample.active,
            "slot_lag_bytes": sample.lag_bytes,
            "slot_streaming_for_sec": round(self.streaming_for, 1),
            "slot_lag_steady_for_sec": round(self.lag_steady_for, 1),
        }
