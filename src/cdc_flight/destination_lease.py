"""Single-writer physical destination leases and bounded handle retirement.

The lease row is a fencing record, not a run bracket.  ``pipeline`` is retained as
the compatibility column but contains the resolved physical destination key.  A
successful takeover always increments ``fencing_epoch``; one Flight can only fence
and commit under the exact ``lease_id``/epoch/generation it acquired for itself.
"""

from __future__ import annotations

import contextlib
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from . import destination as _d
from .destination_fence import unwrap_destination_handle
from .errors import LeaseLost, ServiceStandDown
from .occurrence import LeaseState, _lease_receipt_from_durable
from .retirement import RetirementResult, retire_handle
from .run_state import COMMIT_ACK

log = _d.log
_control_table = _d._control_table
now = _d.now
LEASE_CONFLICT_BUDGET_SEC = _d.LEASE_CONFLICT_BUDGET_SEC
LEASE_CONFLICT_RETRY_SEC = _d.LEASE_CONFLICT_RETRY_SEC
resolve_control_schema = _d.resolve_control_schema
quote = _d.quote


def _batch_owner_dead(host: str | None, pid: int | None) -> bool:
    """Allow finite crash-recovery to retain its pre-service behavior.

    This path is intentionally unreachable for service admission: service calls
    ``acquire`` with a heartbeat bound and may reclaim only on the destination
    clock's expiry. Batch runs have always reclaimed a locally dead finite-run
    owner immediately so the exactly-once crash matrix can restart without
    waiting a lease TTL. PID liveness is only a local hint; a remote or unreadable
    owner remains protected until expiry.
    """
    if not host or host != socket.gethostname() or not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OverflowError, ValueError):
        return False
    return False


@dataclass(frozen=True)
class LeaseHealth:
    """A server-clock health decision for one physical lease row."""

    exists: bool
    healthy: bool
    reclaimable: bool
    reason: str
    renewed_at: object | None = None
    expires_at: object | None = None
    fencing_epoch: int | None = None
    service_id: str | None = None


def _held_state(value: object) -> bool:
    """Accept old schema state spellings while publishing only ``held``."""
    return str(value or "held") not in {"released"}


@dataclass
class Lease:
    """A physical destination lease, compatible with the batch ``Lease`` API."""

    pipeline: str
    owner_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ttl_seconds: float = 60.0
    control_schema: str | None = None
    label: str | None = None
    lease_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    fencing_epoch: int | None = None
    service_id: str | None = None
    worker_generation: str | None = None
    # A successor uses this durable receipt to emit one recovery alert after it
    # reclaims an expired service holder. It is not part of lease identity and is
    # never populated for a clean release or a finite batch lease.
    reclaimed_receipt: object | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.service_id is None:
            self.service_id = self.owner_id
        if self.worker_generation is None:
            self.worker_generation = self.owner_id

    @property
    def name(self) -> str:
        return self.label or self.pipeline

    @property
    def lease_key(self) -> str:
        return self.pipeline

    @property
    def epoch(self) -> int:
        if self.fencing_epoch is None:
            raise LeaseLost("the lease has not been acquired and has no fencing epoch")
        return int(self.fencing_epoch)

    def _assert_outside_commit_ack(self, operation: str) -> None:
        if COMMIT_ACK.active:
            raise LeaseLost(f"lease {operation} is forbidden inside the COMMIT_ACK window")

    def _server_now(self, con):
        row = unwrap_destination_handle(con).execute("SELECT current_timestamp").fetchone()
        if not row or row[0] is None:
            raise RuntimeError("destination did not return its server clock")
        return row[0]

    def _row(self, con):
        return unwrap_destination_handle(con).execute(
            f"SELECT pipeline, lease_key, lease_id, fencing_epoch, service_id, "
            f"worker_generation, owner_id, host, pid, process_start_token, "
            f"worker_pid, worker_start_token, acquired_at, renewed_at, expires_at, state "
            f"FROM {_control_table(self.control_schema, 'lease')} WHERE pipeline = ?",
            [self.pipeline],
        ).fetchone()

    def _receipt(self, row, operation: str):
        if row is None:
            return None
        return _lease_receipt_from_durable(
            LeaseState(
                pipeline=str(row[1] or row[0]),
                owner_id=str(row[6]),
                operation=operation,
                lease_id=str(row[2]) if row[2] is not None else None,
                fencing_epoch=int(row[3]) if row[3] is not None else None,
                service_id=str(row[4]) if row[4] is not None else None,
                worker_generation=str(row[5]) if row[5] is not None else None,
            ),
            details={
                "alert_pipeline": self.label or self.pipeline,
                "acquired_at": row[12],
                "renewed_at": row[13],
                "expires_at": row[14],
                "state": row[15],
            },
        )

    def _durable_receipt(self, con, row, operation: str):
        if row is None:
            return None
        query = (
            f"SELECT pipeline, lease_key, lease_id, fencing_epoch, service_id, "
            f"worker_generation, owner_id, host, pid, process_start_token, "
            f"worker_pid, worker_start_token, acquired_at, renewed_at, expires_at, state "
            f"FROM {_control_table(self.control_schema, 'lease')} WHERE pipeline = ?"
        )
        if not _d._committed_row_matches(
            unwrap_destination_handle(con), query, [self.pipeline], row
        ):
            return None
        return self._receipt(row, operation)

    def _raise_conflict(self, row, operation: str = "acquire", con=None) -> None:
        expires = row[14]
        expires_text = expires.isoformat() if hasattr(expires, "isoformat") else expires
        raise LeaseLost(
            f"physical destination {self.name!r} is already leased by runner {row[6]} "
            f"(pid {row[8]} on {row[7]}) until {expires_text}; a second Flight "
            "is forbidden",
            lease_state=(
                self._durable_receipt(con, row, operation)
                if con is not None
                else self._receipt(row, operation)
            ),
        )

    def _retry_lease_write(self, con, operation: str, write):
        """Retry only an idempotent control-plane write after a cloud conflict.

        MotherDuck can retain an abandoned writer's server-side transaction for a
        short interval after a hard child death.  The old lease implementation
        already had this measured recovery path.  It is safe for control-plane
        writes because the callback's data transaction is not involved; callers
        that are fencing a data transaction deliberately do not use this helper.
        """
        deadline = time.monotonic() + LEASE_CONFLICT_BUDGET_SEC
        attempt = 0
        while True:
            attempt += 1
            try:
                return write()
            except Exception as exc:
                if "conflict" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                log.warning(
                    "lease control write %r is locked by an abandoned transaction "
                    "(attempt %s): %s; retrying",
                    operation,
                    attempt,
                    exc,
                )
                with contextlib.suppress(Exception):
                    con.execute("ROLLBACK")
                time.sleep(LEASE_CONFLICT_RETRY_SEC)

    def _health_from_row(self, row, current, heartbeat_bound_seconds: float) -> LeaseHealth:
        if row is None:
            return LeaseHealth(
                exists=False,
                healthy=False,
                reclaimable=True,
                reason="no_lease_row",
            )
        renewed_at = row[13]
        expires_at = row[14]
        state = str(row[15] or "held")
        if state == "released":
            return LeaseHealth(
                exists=True,
                healthy=False,
                reclaimable=True,
                reason="released",
                renewed_at=renewed_at,
                expires_at=expires_at,
                fencing_epoch=int(row[3] or 0),
                service_id=str(row[4]) if row[4] is not None else None,
            )
        if expires_at is None or expires_at <= current:
            return LeaseHealth(
                exists=True,
                healthy=False,
                reclaimable=True,
                reason="lease_expired",
                renewed_at=renewed_at,
                expires_at=expires_at,
                fencing_epoch=int(row[3] or 0),
                service_id=str(row[4]) if row[4] is not None else None,
            )
        heartbeat_cutoff = current - timedelta(seconds=heartbeat_bound_seconds)
        if renewed_at is None or renewed_at < heartbeat_cutoff:
            return LeaseHealth(
                exists=True,
                healthy=False,
                reclaimable=False,
                reason="heartbeat_stale_until_lease_expiry",
                renewed_at=renewed_at,
                expires_at=expires_at,
                fencing_epoch=int(row[3] or 0),
                service_id=str(row[4]) if row[4] is not None else None,
            )
        return LeaseHealth(
            exists=True,
            healthy=True,
            reclaimable=False,
            reason="lease_and_heartbeat_fresh",
            renewed_at=renewed_at,
            expires_at=expires_at,
            fencing_epoch=int(row[3] or 0),
            service_id=str(row[4]) if row[4] is not None else None,
        )

    def inspect_health(self, con, *, heartbeat_bound_seconds: float) -> LeaseHealth:
        """Read health using only the destination's authoritative clock.

        Any connection/query exception deliberately escapes.  A caller must
        fail closed when MotherDuck cannot be read; it must never convert an
        unreadable lease into a free destination.
        """
        if heartbeat_bound_seconds <= 0:
            raise ValueError("heartbeat_bound_seconds must be positive")
        current = self._server_now(con)
        return self._health_from_row(self._row(con), current, heartbeat_bound_seconds)

    def acquire(
        self,
        con,
        *,
        heartbeat_bound_seconds: float | None = None,
        wait_for_expiry: bool = False,
    ) -> None:
        """Acquire or conditionally take over one physical destination key.

        Service admission adds the health decision: a fresh lease heartbeat
        raises :class:`ServiceStandDown`; a stale heartbeat is unhealthy but
        remains protected until the server-side expiry.  The final takeover is
        an epoch-conditional update, so simultaneous starters leave exactly one
        winner and make the loser re-read the winner's fresh row.
        """
        self._assert_outside_commit_ack("acquire")
        # The extra second covers the final 250 ms server-clock poll without
        # turning a lease-expiry wait into an unbounded operation.
        admission_started = time.monotonic()
        admission_budget = self.ttl_seconds + 1.0
        current = self._server_now(con)
        row = self._row(con)
        if row is None:
            self.fencing_epoch = 1
            try:
                con.execute(
                    f"INSERT INTO {_control_table(self.control_schema, 'lease')} "
                    "(pipeline, lease_key, lease_id, fencing_epoch, service_id, "
                    "worker_generation, owner_id, host, pid, acquired_at, renewed_at, "
                    "expires_at, state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        self.pipeline, self.pipeline, self.lease_id, 1, self.service_id,
                        self.worker_generation, self.owner_id, socket.gethostname(),
                        os.getpid(), current, current,
                        current + timedelta(seconds=self.ttl_seconds), "held",
                    ],
                )
            except Exception as insert_error:
                observed = self._row(con)
                if observed is not None:
                    if heartbeat_bound_seconds is not None:
                        health = self._health_from_row(
                            observed,
                            self._server_now(con),
                            heartbeat_bound_seconds,
                        )
                        if health.healthy:
                            raise ServiceStandDown(
                                "another Flight acquired the physical destination "
                                "during simultaneous startup",
                                {"health": health.reason, "fencing_epoch": health.fencing_epoch},
                            ) from insert_error
                    self._raise_conflict(observed, con=con)
                raise
            return

        # A batch caller can pass the same acquired Lease object through more
        # than one orchestration layer.  Re-admission of that exact identity is
        # idempotent; a second Flight has a different lease_id and still
        # conflicts even when it uses the same service_id.
        if row[2] == self.lease_id and row[6] == self.owner_id:
            self.fencing_epoch = int(row[3] or 1)
            return

        if heartbeat_bound_seconds is not None:
            health = self._health_from_row(row, current, heartbeat_bound_seconds)
            if health.healthy:
                raise ServiceStandDown(
                    "another Flight holds a fresh lease heartbeat",
                    {"health": health.reason, "fencing_epoch": health.fencing_epoch},
                )
            if not health.reclaimable:
                if not wait_for_expiry:
                    raise LeaseLost(
                        "the existing Flight is unhealthy but its lease has not expired; "
                        "refusing an unsafe takeover",
                        lease_state=self._receipt(row, "health_check"),
                    )
                # A stale-but-unexpired holder is not healthy, but it is still
                # protected.  Waiting is bounded by the row's expiry; the next
                # server-clock read remains the authority before the CAS.
                while True:
                    if time.monotonic() - admission_started >= admission_budget:
                        raise LeaseLost(
                            "the bounded wait for the unhealthy lease to expire was "
                            "exhausted; refusing an unproven takeover",
                            lease_state=self._receipt(row, "health_check_timeout"),
                        )
                    current = self._server_now(con)
                    row = self._row(con)
                    if row is None:
                        break
                    health = self._health_from_row(row, current, heartbeat_bound_seconds)
                    if health.healthy:
                        raise ServiceStandDown(
                            "the incumbent became healthy while takeover waited",
                            {"health": health.reason, "fencing_epoch": health.fencing_epoch},
                        )
                    if health.reclaimable:
                        break
                    remaining = max(
                        0.0,
                        (health.expires_at - current).total_seconds(),
                    )
                    if remaining <= 0:
                        break
                    time.sleep(min(0.25, remaining))
                current = self._server_now(con)
                row = self._row(con)
                if row is None:
                    self.fencing_epoch = 1
                    return self.acquire(
                        con,
                        heartbeat_bound_seconds=heartbeat_bound_seconds,
                        wait_for_expiry=wait_for_expiry,
                    )
        else:
            expires_at = row[14]
            live = _held_state(row[15]) and expires_at is not None and expires_at > current
            owner_dead = (
                heartbeat_bound_seconds is None
                and _batch_owner_dead(row[7], row[8])
            )
            if live and not owner_dead:
                self._raise_conflict(row, con=con)
            if live and owner_dead:
                # This is finite batch crash recovery only. A service holder is
                # never reclaimed from a local PID observation.
                log.warning(
                    "reclaiming finite batch lease %r after local owner death "
                    "owner=%s pid=%s",
                    self.name,
                    row[6],
                    row[8],
                )

        old_epoch = int(row[3] or 0)
        reclaimed_receipt = None
        if heartbeat_bound_seconds is not None and health.reason == "lease_expired":
            # This is durable evidence that the prior service generation died or
            # stopped renewing. A clean ``released`` row remains operator-silent.
            reclaimed_receipt = self._receipt(row, "service_holder_reclaimed")
        next_epoch = max(1, old_epoch + 1)
        self.fencing_epoch = next_epoch

        batch_dead_predicate = ""
        batch_dead_params: list[object] = []
        if heartbeat_bound_seconds is None and _batch_owner_dead(row[7], row[8]):
            batch_dead_predicate = " OR (host=? AND pid=?)"
            batch_dead_params = [row[7], row[8]]

        def takeover():
            return con.execute(
                f"UPDATE {_control_table(self.control_schema, 'lease')} SET "
                "lease_key=?, lease_id=?, fencing_epoch=?, service_id=?, worker_generation=?, "
                "owner_id=?, host=?, pid=?, process_start_token=NULL, worker_pid=NULL, "
                "worker_start_token=NULL, acquired_at=?, renewed_at=?, expires_at=?, state=? "
                "WHERE pipeline=? AND coalesce(fencing_epoch, 0)=? "
                f"AND (state='released' OR expires_at <= ?{batch_dead_predicate})",
                [
                    self.pipeline, self.lease_id, next_epoch, self.service_id,
                    self.worker_generation, self.owner_id, socket.gethostname(), os.getpid(),
                    current, current, current + timedelta(seconds=self.ttl_seconds), "held",
                    self.pipeline, old_epoch, current,
                    *batch_dead_params,
                ],
            )

        self._retry_lease_write(con, "takeover", takeover)
        observed = self._row(con)
        if observed is None or observed[2] != self.lease_id or int(observed[3] or 0) != next_epoch:
            if observed is not None:
                if heartbeat_bound_seconds is not None:
                    health = self._health_from_row(
                        observed,
                        self._server_now(con),
                        heartbeat_bound_seconds,
                    )
                    if health.healthy:
                        raise ServiceStandDown(
                            "another Flight won the conditional takeover race",
                            {"health": health.reason, "fencing_epoch": health.fencing_epoch},
                        )
                self._raise_conflict(observed, con=con)
            raise LeaseLost("the physical lease takeover lost its conditional epoch race")
        self.reclaimed_receipt = reclaimed_receipt

    def _matches(self, row) -> bool:
        return bool(
            row is not None
            and row[2] == self.lease_id
            and int(row[3] or 0) == self.epoch
            and row[6] == self.owner_id
            and (row[4] is None or row[4] == self.service_id)
            and (row[5] is None or row[5] == self.worker_generation)
            and _held_state(row[15])
        )

    def _conditional_refresh(
        self,
        con,
        *,
        operation: str,
        require_live: bool = False,
        retry_conflicts: bool = False,
    ) -> None:
        if self.fencing_epoch is None:
            raise LeaseLost(f"cannot {operation} a lease without an epoch")
        raw_con = unwrap_destination_handle(con)
        current = self._server_now(raw_con)
        params = [
            current,
            current + timedelta(seconds=self.ttl_seconds),
            self.pipeline,
            self.owner_id,
            self.lease_id,
            self.epoch,
        ]
        identity_predicate = " AND coalesce(service_id, ?) = ? "
        params.extend([self.service_id, self.service_id])
        identity_predicate += "AND coalesce(worker_generation, ?) = ? "
        params.extend([self.worker_generation, self.worker_generation])
        live_predicate = " AND expires_at > ?" if require_live else ""
        if require_live:
            params.append(current)
        def refresh():
            return raw_con.execute(
                f"UPDATE {_control_table(self.control_schema, 'lease')} SET renewed_at=?, "
                "expires_at=? WHERE pipeline=? AND owner_id=? AND lease_id=? "
                f"AND fencing_epoch=?{identity_predicate}AND state <> 'released'"
                f"{live_predicate}",
                params,
            )

        if retry_conflicts:
            self._retry_lease_write(con, operation, refresh)
        else:
            refresh()
        row = self._row(con)
        # ``UPDATE ... WHERE expires_at > current`` may affect zero rows after
        # expiry.  Identity alone is not an acknowledgement of renewal: the row
        # must also be live in a fresh destination-clock observation.
        live_after = True
        if require_live:
            observed_now = self._server_now(raw_con)
            live_after = bool(row is not None and row[14] is not None and row[14] > observed_now)
        if not self._matches(row) or not live_after:
            raise LeaseLost(
                f"lease for {self.name!r} was lost during {operation}; refusing data write",
                lease_state=self._durable_receipt(con, row, operation),
            )

    def renew(self, con) -> None:
        """Compatibility name for the applier's same-transaction fence."""
        self.fence(con)

    def fence(self, con) -> None:
        """Fence the exact epoch inside the destination data transaction."""
        self._assert_outside_commit_ack("fence")
        self._conditional_refresh(con, operation="fence", require_live=True)

    def renew_control(self, con) -> None:
        """Renew on the service control connection, never in COMMIT_ACK."""
        self._assert_outside_commit_ack("renew")
        self._conditional_refresh(
            con, operation="renew", require_live=True, retry_conflicts=True
        )

    def assert_current(self, con) -> None:
        self._assert_outside_commit_ack("verify")
        row = self._row(con)
        current = self._server_now(con)
        if not self._matches(row) or row[14] is None or row[14] <= current:
            raise LeaseLost(
                f"physical lease {self.name!r} is no longer owned by epoch "
                f"{self.fencing_epoch}",
                lease_state=self._receipt(row, "verify"),
            )

    def attach(self, con) -> None:
        """Attach this process to its already-admitted fencing epoch."""
        self._assert_outside_commit_ack("attach")
        self.assert_current(con)
        row = self._row(con)
        if row[4] != self.service_id or row[5] != self.worker_generation:
            raise LeaseLost("service generation does not match the physical lease")

    def release(self, con, *, retain: bool = False) -> None:
        self._assert_outside_commit_ack("release")
        try:
            if retain:
                con.execute(
                    f"UPDATE {_control_table(self.control_schema, 'lease')} SET state=? "
                    "WHERE pipeline=? AND owner_id=? AND lease_id=? AND fencing_epoch=?",
                    ["released", self.pipeline, self.owner_id, self.lease_id, self.epoch],
                )
            else:
                con.execute(
                    f"DELETE FROM {_control_table(self.control_schema, 'lease')} "
                    "WHERE pipeline=? AND owner_id=? AND lease_id=? AND fencing_epoch=?",
                    [self.pipeline, self.owner_id, self.lease_id, self.epoch],
                )
        except Exception:  # pragma: no cover - release is best effort at process exit
            log.debug("could not release lease", exc_info=True)


def release_connection(con, *, timeout: float = 5.0) -> RetirementResult:
    """Close a destination connection under the canonical bounded protocol."""
    return retire_handle(
        con,
        timeout=timeout,
        thread_name="cdc-destination-close",
        description="the destination connection",
    )


def probe_transactional_ddl(con, *, control_schema: str | None = None) -> bool:
    """Empirically determine whether the destination rolls back DDL."""
    probe_a = _control_table(control_schema, "__ddl_probe_a")
    probe_b = _control_table(control_schema, "__ddl_probe_b")
    try:
        con.execute(f"DROP TABLE IF EXISTS {probe_a}")
        con.execute(f"DROP TABLE IF EXISTS {probe_b}")
        con.execute(f"CREATE TABLE {probe_a} (x INTEGER)")
        con.execute(f"CREATE TABLE {probe_b} (x INTEGER)")
        con.execute("BEGIN TRANSACTION")
        con.execute(f"DROP TABLE {probe_a}")
        con.execute(f"ALTER TABLE {probe_b} RENAME TO {quote('__ddl_probe_a')}")
        con.execute("ROLLBACK")
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name LIKE '__ddl_probe%'",
            [resolve_control_schema(control_schema)],
        ).fetchall()
        return {"__ddl_probe_a", "__ddl_probe_b"} <= {r[0] for r in rows}
    except Exception as exc:
        log.info("transactional DDL probe failed (%s); using the CTAS swap", exc)
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        return False
    finally:
        for name in (probe_a, probe_b):
            with contextlib.suppress(Exception):
                con.execute(f"DROP TABLE IF EXISTS {name}")
