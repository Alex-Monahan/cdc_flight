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
from .witness_contract import (
    STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
    ServiceWitnessEvidence,
    evaluate_service_witness,
)

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

# The finite-run sampler only needs the slot liveness and confirmed position. The
# service watchdog opts into the identity join below; keeping that expensive,
# cluster-wide statistics lookup off the ordinary watermark path matters because
# xdist workers create/drop databases while their bounded runs are sampling.
_SLOT_SQL_FAST = """
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

_SLOT_SQL = """
SELECT (s.slot_name IS NOT NULL) AS slot_exists,
       -- Keep activity independent from existence.  A missing row must not
       -- accidentally satisfy a future ``active``-only witness mutation.
       COALESCE(s.active, TRUE) AS active,
       s.active_pid,
       a.pid AS activity_pid,
       a.application_name AS activity_application_name,
       a.backend_type AS activity_backend_type,
       a.backend_start AS activity_backend_start,
       r.pid AS replication_pid,
       r.application_name AS replication_application_name,
       s.confirmed_flush_lsn IS NOT NULL AS has_confirmed,
       CASE WHEN s.confirmed_flush_lsn IS NULL THEN NULL
            ELSE (s.confirmed_flush_lsn - '0/0')::BIGINT END AS confirmed_pos,
       CASE WHEN s.restart_lsn IS NULL THEN NULL
            ELSE (s.restart_lsn - '0/0')::BIGINT END AS restart_pos,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn), 0)::BIGINT
FROM (SELECT %s::name AS slot_name) requested
LEFT JOIN pg_replication_slots s ON s.slot_name = requested.slot_name
LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
"""

_PUBLICATION_HAS_TABLES_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_publication_tables
    WHERE pubname = %s
)
"""

# ``pg_publication_tables`` membership is only a coarse publication check. A
# publication can contain a relation that this connector excludes, a relation the
# source user cannot read, or a partitioned root with no leaf relation to publish.
# Resolve the configured relation OIDs in PostgreSQL and require the published
# relation to be the configured relation or a relation on either side of the same
# partition tree, a usable SELECT route, and at least one readable leaf when the
# published relation is partitioned. This is configuration/readiness evidence for
# a quiet source; it is never used as a substitute for data-delivery progress.
_PUBLICATION_CONFIGURED_ROUTE_SQL = """
WITH requested(qualified) AS (
    SELECT unnest(%s::text[])
), requested_relations AS (
    SELECT c.oid AS relid
    FROM requested r
    JOIN pg_namespace n
      ON n.nspname = split_part(r.qualified, '.', 1)
    JOIN pg_class c
      ON c.relnamespace = n.oid
     AND c.relname = split_part(r.qualified, '.', 2)
)
SELECT EXISTS (
    SELECT 1
    FROM pg_publication_tables published
    JOIN pg_namespace n
      ON n.nspname = published.schemaname
    JOIN pg_class c
      ON c.relnamespace = n.oid
     AND c.relname = published.tablename
    WHERE published.pubname = %s
      AND EXISTS (
          SELECT 1
          FROM requested_relations requested
          WHERE requested.relid = c.oid
             OR EXISTS (
                 SELECT 1
                 FROM pg_partition_tree(c.oid) tree
                 WHERE tree.relid = requested.relid
             )
             OR EXISTS (
                 SELECT 1
                 FROM pg_partition_tree(requested.relid) tree
                 WHERE tree.relid = c.oid
             )
      )
      AND has_table_privilege(c.oid, 'SELECT')
      AND (
          c.relkind <> 'p'
          OR EXISTS (
              SELECT 1
              FROM pg_partition_tree(c.oid) leaf
              WHERE leaf.isleaf
                AND has_table_privilege(leaf.relid, 'SELECT')
          )
      )
)
"""


@dataclass
class SlotSample:
    """One observation of the replication slot, or the reason there is none."""

    at: float
    exists: bool = False
    active: bool = False
    #: PostgreSQL's current walsender PID.  The PID is joined to both server-side
    #: activity views below; it is not persisted across reconnects.
    active_pid: int | None = None
    activity_pid: int | None = None
    activity_application_name: str | None = None
    activity_backend_type: str | None = None
    activity_backend_start: datetime | None = None
    replication_pid: int | None = None
    replication_application_name: str | None = None
    lag_bytes: int | None = None
    confirmed_pos: int | None = None
    restart_pos: int | None = None
    error: str | None = None
    #: Wall-clock time near the SQL result. ``at`` remains monotonic for duration
    #: clocks; this value makes the persisted operator sample attributable to a time.
    observed_at: datetime | None = None
    #: ``None`` means the optional publication contract was not requested (finite
    #: runs); ``False`` is a real empty-publication observation.
    publication_has_tables: bool | None = None
    #: ``True`` only when the publication overlaps the configured, readable source
    #: route. ``False`` distinguishes an excluded/unreadable/no-leaf route from a
    #: correctly configured but empty source.
    publication_has_configured_tables: bool | None = None

    @property
    def streaming(self) -> bool:
        """True when a walsender is attached to our slot right now."""
        return self.exists and self.active

    @property
    def identity_context(self) -> str:
        """Classify the slot PID's server-side identity evidence."""
        if not self.streaming:
            return "not_streaming"
        if (
            self.active_pid is None
            or self.activity_pid != self.active_pid
            or self.replication_pid != self.active_pid
        ):
            return "unproven"
        if (
            self.activity_backend_type != "walsender"
            or self.activity_application_name
            != STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME
            or self.replication_application_name
            != STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME
        ):
            return "foreign_walsender"
        return "stock_debezium"

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
    #: Stock Debezium 3.6 hard-codes this value on its replication JDBC
    #: connection.  ``None`` is retained for pure unit fakes; service-mode
    #: SourceHealth instances require the joined server-side identity proof.
    expected_application_name: str | None = None
    #: The identity join is a service watchdog witness. Finite runs still use
    #: source-health corroboration and the completion watermark, but must not make
    #: every ordinary slot sample scan the cluster-wide activity views while the
    #: 12-worker harness is creating and dropping databases.
    identity_required: bool = False
    #: Service mode must prove that Debezium's configured publication contains at
    #: least one source table. Heartbeats and logical messages can advance an empty
    #: publication while delivering no data; that is not a healthy service.
    publication_name: str | None = None
    #: The final table.include.list used by the stock connector. ``None`` retains
    #: the unit/fake compatibility path where publication membership is the only
    #: configured-route fact available; production service callers always pass it.
    capture_tables: tuple[str, ...] | None = None
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
    #: The immediately preceding observation is needed to bracket an own ack.
    #: A post-ack sample alone cannot prove that a newly attached, generic-stock-
    #: labelled backend was the connector that produced the ack.
    _previous: SlotSample | None = field(default=None, repr=False)
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
    #: Whether the current service clock was started by a known publication-route
    #: mismatch.  That observation is actionable even before the slot attaches;
    #: it must not inherit an earlier mismatch clock after the route is repaired.
    _service_route_mismatch: bool = field(default=False, repr=False)
    _service_engine_thread_dead: bool = field(default=False, repr=False)
    _service_lag_bytes: int | None = field(default=None, repr=False)
    _service_quiet_ready: bool = field(default=False, repr=False)
    #: A source callback/ack observed after this exact backend identity was
    #: sampled certifies the PID for this process generation.  The PID alone is
    #: not durable across reconnects; backend_start closes the PID-reuse gap.
    _bound_walsender_pid: int | None = field(default=None, repr=False)
    _bound_walsender_backend_start: datetime | None = field(default=None, repr=False)
    _bound_walsender_ack_at: float | None = field(default=None, repr=False)

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
            self._previous = self._last
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
                sql = _SLOT_SQL if self.identity_required else _SLOT_SQL_FAST
                row = conn.execute(sql, (self.slot_name,)).fetchone()
                publication_has_tables = None
                publication_has_configured_tables = None
                if self.publication_name is not None:
                    publication_row = conn.execute(
                        _PUBLICATION_HAS_TABLES_SQL, (self.publication_name,)
                    ).fetchone()
                    publication_has_tables = bool(
                        publication_row is not None and publication_row[0]
                    )
                    if self.capture_tables is None:
                        # Compatibility for pure fakes that predate the route
                        # query. Production callers pass the final connector
                        # include list and take the stricter SQL path below.
                        publication_has_configured_tables = publication_has_tables
                    else:
                        route_row = conn.execute(
                            _PUBLICATION_CONFIGURED_ROUTE_SQL,
                            (list(self.capture_tables), self.publication_name),
                        ).fetchone()
                        publication_has_configured_tables = bool(
                            route_row is not None and route_row[0]
                        )
        except Exception as exc:
            return SlotSample(
                at=now,
                error=f"{type(exc).__name__}: {exc}",
                observed_at=datetime.now(UTC),
            )
        if row is None:  # pragma: no cover - the requested-row query always returns one
            return SlotSample(
                at=now,
                exists=False,
                publication_has_tables=publication_has_tables,
                publication_has_configured_tables=publication_has_configured_tables,
                observed_at=datetime.now(UTC),
            )
        if not self.identity_required:
            active, active_pid, has_confirmed, confirmed_pos, restart_pos, lag = row
            return SlotSample(
                at=time.monotonic(),
                exists=True,
                active=bool(active),
                active_pid=(int(active_pid) if active_pid is not None else None),
                confirmed_pos=(int(confirmed_pos) if has_confirmed else None),
                restart_pos=(int(restart_pos) if restart_pos is not None else None),
                lag_bytes=int(lag) if has_confirmed else None,
                publication_has_tables=publication_has_tables,
                publication_has_configured_tables=publication_has_configured_tables,
                observed_at=datetime.now(UTC),
            )
        (
            exists,
            active,
            active_pid,
            activity_pid,
            activity_application_name,
            activity_backend_type,
            activity_backend_start,
            replication_pid,
            replication_application_name,
            has_confirmed,
            confirmed_pos,
            restart_pos,
            lag,
        ) = row
        return SlotSample(
            at=time.monotonic(),
            exists=bool(exists),
            active=bool(active),
            active_pid=(int(active_pid) if active_pid is not None else None),
            activity_pid=(int(activity_pid) if activity_pid is not None else None),
            activity_application_name=(
                str(activity_application_name)
                if activity_application_name is not None
                else None
            ),
            activity_backend_type=(
                str(activity_backend_type) if activity_backend_type is not None else None
            ),
            activity_backend_start=activity_backend_start,
            replication_pid=(int(replication_pid) if replication_pid is not None else None),
            replication_application_name=(
                str(replication_application_name)
                if replication_application_name is not None
                else None
            ),
            confirmed_pos=(int(confirmed_pos) if has_confirmed else None),
            restart_pos=(int(restart_pos) if restart_pos is not None else None),
            lag_bytes=int(lag) if has_confirmed else None,
            publication_has_tables=publication_has_tables,
            publication_has_configured_tables=publication_has_configured_tables,
            observed_at=datetime.now(UTC),
        )

    # -- what the supervisor asks ------------------------------------------- #
    @property
    def last(self) -> SlotSample | None:
        with self._lock:
            return self._last

    @property
    def service_quiet_ready(self) -> bool:
        """Whether the latest service fold admitted the explicit quiet route."""
        with self._lock:
            return self._service_quiet_ready

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
        quiet_source_ready: bool = False,
        route_mismatch: bool = False,
        route_mismatch_observed_at: float | None = None,
    ) -> str:
        """Record the service verdict and its fail-closed aging clock."""
        with self._lock:
            self._service_status = status
            self._service_engine_thread_dead = not engine_thread_alive
            self._service_lag_bytes = lag_bytes
            self._service_quiet_ready = bool(
                status == "connected_quiet" and quiet_source_ready
            )
            if status in {"connected_quiet", "connected_busy"}:
                self._service_stalled_since = None
            elif route_mismatch:
                # A successful publication/catalog sample already proves that the
                # configured route cannot deliver.  Start the bounded source-dark
                # clock at that observation, even if slot attachment is still in
                # progress and the witness fold currently says ``disconnected``.
                if self._service_stalled_since is None or not self._service_route_mismatch:
                    self._service_stalled_since = (
                        route_mismatch_observed_at
                        if route_mismatch_observed_at is not None
                        else observed_at
                    )
            elif self._service_route_mismatch:
                # The known route failure ended, but the service is not healthy yet;
                # begin a fresh clock for whatever current witness is missing.
                self._service_stalled_since = observed_at
            elif (
                status
                in {
                    "stalled",
                    "unproven",
                    "foreign_walsender",
                    "engine_thread_dead",
                }
                and self._service_stalled_since is None
            ):
                self._service_stalled_since = observed_at
            self._service_route_mismatch = route_mismatch
        return status

    def _walsender_is_ours(
        self,
        sample: SlotSample | None,
        *,
        own_certification_at: float | None,
    ) -> bool:
        """Require a current PID to be certified by this Flight's own ack.

        Stock Debezium 3.6 hard-codes ``Debezium Streaming`` for its logical
        replication connection, so that application name is a class marker, not
        a process-unique name.  The sampler therefore binds the slot PID and
        PostgreSQL ``backend_start`` only when two adjacent stock-labelled samples
        bracket this Flight's acknowledgement.  A post-ack sample alone is not
        enough: a foreign client could take the slot immediately after our ack and
        present the same generic application name.  A legitimate reconnect is
        admitted only after the restarted stock engine is observed before and after
        its new acknowledgement.
        """
        if self.expected_application_name is None or sample is None:
            return False
        if sample.identity_context != "stock_debezium":
            return False
        if sample.active_pid is None or sample.activity_backend_start is None:
            return False
        current = (sample.active_pid, sample.activity_backend_start)
        with self._lock:
            bound = (
                self._bound_walsender_pid,
                self._bound_walsender_backend_start,
            )
            previous = self._previous
            newer_ack = (
                own_certification_at is not None
                and (
                    self._bound_walsender_ack_at is None
                    or own_certification_at > self._bound_walsender_ack_at
                )
            )
            if current != bound:
                if (
                    not newer_ack
                    or own_certification_at is None
                    or sample.at < own_certification_at
                    or previous is None
                    or not previous.streaming
                    or previous.identity_context != "stock_debezium"
                    or previous.active_pid != sample.active_pid
                    or previous.activity_backend_start != sample.activity_backend_start
                    or previous.at > own_certification_at
                ):
                    return False
                self._bound_walsender_pid = sample.active_pid
                self._bound_walsender_backend_start = sample.activity_backend_start
                self._bound_walsender_ack_at = own_certification_at
            return (
                self._bound_walsender_pid == sample.active_pid
                and self._bound_walsender_backend_start
                == sample.activity_backend_start
            )

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
        quiet_source_ready: bool = False,
        own_identity_at: float | None = None,
    ) -> str:
        """Classify source evidence for the single-process lease watchdog.

        The service witness is deliberately conjunctive:

        * the sampler observation is fresh and the slot is active;
        * the *admitted* Debezium engine thread is still alive;
        * this process has recently delivered source data and completed its
          durable acknowledgement path, or it has completed a snapshot/streaming
          hand-off for a configured, readable, caught-up route that is genuinely
          quiet. The latter is an explicit quiet-source admission, never a
          heartbeat-derived progress timestamp.

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
        sample_age = float("inf") if sample is None else max(0.0, now - sample.at)
        publication_has_tables = (
            self.publication_name is None
            or bool(sample is not None and sample.publication_has_tables)
        )
        publication_has_configured_tables = (
            self.publication_name is None
            or bool(
                sample is not None
                and (
                    sample.publication_has_configured_tables
                    if self.capture_tables is not None
                    else sample.publication_has_tables
                )
            )
        )
        route_mismatch = bool(
            self.publication_name is not None
            and sample is not None
            and sample.publication_has_tables is True
            and sample.publication_has_configured_tables is False
        )
        quiet_ready = bool(
            quiet_source_ready
            and publication_has_tables
            and publication_has_configured_tables
            and received_high_water is not None
            and sample is not None
            and sample.confirmed_pos is not None
            and received_high_water <= sample.confirmed_pos
        )
        evidence = ServiceWitnessEvidence(
            now=now,
            sample_present=sample is not None,
            sample_error=bool(sample is not None and sample.unknown),
            sample_age=sample_age,
            sample_stale_after=max(self.interval * 3.0, 1.0),
            slot_exists=bool(sample is not None and sample.exists),
            slot_active=bool(sample is not None and sample.active),
            publication_has_tables=publication_has_tables,
            publication_has_configured_tables=publication_has_configured_tables,
            walsender_identity=(
                (
                    sample is not None
                    and sample.streaming
                    and self.expected_application_name is None
                )
                or (
                    self.expected_application_name
                    == STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME
                    and self._walsender_is_ours(
                        sample,
                        own_certification_at=(
                            own_identity_at
                            if own_identity_at is not None
                            else own_ack_at
                        ),
                    )
                )
            ),
            engine_thread_alive=bool(engine_thread_alive),
            stream_recovery_pending=bool(
                self.stream_interruptions
                and not self.recovered_after_interruption
            ),
            retained_lag_bytes=sample.lag_bytes if sample is not None else None,
            own_progress_at=own_progress_at,
            own_ack_at=own_ack_at,
            own_ack_lsn=own_ack_lsn,
            durable_lsn=durable_lsn,
            confirmed_pos=(sample.confirmed_pos if sample is not None else None),
            received_high_water=received_high_water,
            progress_stale_after=max(float(progress_stale_after), 0.0),
            own_identity_at=own_identity_at,
            source_quiet_ready=quiet_ready,
        )
        status = evaluate_service_witness(evidence)
        return self._publish_service_status(
            status,
            observed_at=now,
            engine_thread_alive=engine_thread_alive,
            lag_bytes=sample.lag_bytes if sample is not None else None,
            quiet_source_ready=quiet_ready,
            route_mismatch=route_mismatch,
            route_mismatch_observed_at=sample.at if route_mismatch else None,
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
        # A foreign client can keep the slot active while this Flight is idle.
        # In production the stock connector identity is mandatory; pure unit
        # fakes leave ``expected_application_name`` unset and retain the older
        # generic source-health semantics.
        if (
            self.identity_required
            and self.expected_application_name is not None
            and sample.identity_context != "stock_debezium"
        ):
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
            return {
                "slot_health": self.state(),
                "source_publication": self.publication_name,
                "source_publication_has_configured_tables": None,
                "service_source_quiet_ready": False,
            }
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
                "source_publication": self.publication_name,
                "source_publication_has_tables": sample.publication_has_tables,
                "source_publication_has_configured_tables": (
                    sample.publication_has_configured_tables
                ),
                "service_source_quiet_ready": self.service_quiet_ready,
            }
        return {
            "slot_health": self.state(),
            "slot_exists": sample.exists,
            "slot_active": sample.active,
            "slot_attached": sample.streaming,
            "slot_active_pid": sample.active_pid,
            "slot_active_activity_pid": sample.activity_pid,
            "slot_active_application_name": sample.activity_application_name,
            "slot_active_backend_type": sample.activity_backend_type,
            "slot_active_backend_start": (
                sample.activity_backend_start.isoformat()
                if sample.activity_backend_start is not None
                else None
            ),
            "slot_replication_pid": sample.replication_pid,
            "slot_replication_application_name": sample.replication_application_name,
            "slot_walsender_identity": sample.identity_context,
            "slot_expected_application_name": self.expected_application_name,
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
            "source_publication": self.publication_name,
            "source_publication_has_tables": sample.publication_has_tables,
            "source_publication_has_configured_tables": (
                sample.publication_has_configured_tables
            ),
            "service_liveness_status": self._service_status,
            "service_source_quiet_ready": self.service_quiet_ready,
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
