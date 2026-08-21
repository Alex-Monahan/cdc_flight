"""Single-writer physical destination leases and bounded handle retirement.

The lease row is a fencing record, not a run bracket.  ``pipeline`` is retained as
the compatibility column but contains the resolved physical destination key.  A
successful takeover always increments ``fencing_epoch``; a worker can only fence
and commit under the exact ``lease_id``/epoch/generation it was handed by its
supervisor.
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
from .errors import LeaseLost
from .occurrence import LeaseState, _lease_receipt_from_durable
from .retirement import RetirementResult, retire_handle
from .run_state import COMMIT_ACK
from .service_protocol import process_start_token as _process_start_token

log = _d.log
_control_table = _d._control_table
now = _d.now
LEASE_CONFLICT_BUDGET_SEC = _d.LEASE_CONFLICT_BUDGET_SEC
LEASE_CONFLICT_RETRY_SEC = _d.LEASE_CONFLICT_RETRY_SEC
resolve_control_schema = _d.resolve_control_schema
quote = _d.quote


def _is_dead(host: str | None, pid: int | None) -> bool:
    """Legacy PID-only probe retained for callers that only need a local hint."""
    if not host or not pid or host != socket.gethostname():
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OverflowError, ValueError):
        return False
    return False


def _owner_proof(host: str | None, pid: int | None, start_token: str | None) -> bool:
    """Prove a local owner is dead or that its PID has been reused.

    A remote hostname/PID is never treated as proof.  A remote stale row is
    reclaimable only after the destination server reports expiry.
    """
    if not host or host != socket.gethostname() or not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OverflowError, ValueError):
        return False
    if not start_token:
        return False
    try:
        return _process_start_token(int(pid)) != str(start_token)
    except Exception:
        return False


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
    process_start_token: str | None = None
    worker_pid: int | None = None
    worker_start_token: str | None = None

    def __post_init__(self) -> None:
        if self.service_id is None:
            self.service_id = self.owner_id
        if self.worker_generation is None:
            self.worker_generation = self.owner_id
        if self.process_start_token is None:
            self.process_start_token = _process_start_token()

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
        try:
            row = con.execute("SELECT current_timestamp").fetchone()
            if row and row[0] is not None:
                return row[0]
        except Exception:
            pass
        return now()

    def _row(self, con):
        return con.execute(
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
        if not _d._committed_row_matches(con, query, [self.pipeline], row):
            return None
        return self._receipt(row, operation)

    def _raise_conflict(self, row, operation: str = "acquire", con=None) -> None:
        expires = row[14]
        expires_text = expires.isoformat() if hasattr(expires, "isoformat") else expires
        raise LeaseLost(
            f"physical destination {self.name!r} is already leased by runner {row[6]} "
            f"(pid {row[8]} on {row[7]}) until {expires_text}; a second data worker "
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

    def acquire(self, con) -> None:
        """Acquire or conditionally take over one physical key.

        The read is advisory; the update/insert carries the epoch predicate.  A
        concurrent winner is re-read and refused, so two supervisors cannot both
        pass admission even when they start at the same instant.
        """
        self._assert_outside_commit_ack("acquire")
        current = self._server_now(con)
        row = self._row(con)
        if row is None:
            self.fencing_epoch = 1
            try:
                con.execute(
                    f"INSERT INTO {_control_table(self.control_schema, 'lease')} "
                    "(pipeline, lease_key, lease_id, fencing_epoch, service_id, "
                    "worker_generation, owner_id, host, pid, process_start_token, "
                    "acquired_at, renewed_at, expires_at, state) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        self.pipeline, self.pipeline, self.lease_id, 1, self.service_id,
                        self.worker_generation, self.owner_id, socket.gethostname(),
                        os.getpid(), self.process_start_token, current, current,
                        current + timedelta(seconds=self.ttl_seconds), "supervisor_held",
                    ],
                )
            except Exception:
                observed = self._row(con)
                if observed is not None:
                    self._raise_conflict(observed, con=con)
                raise
            return

        # A batch caller can pass the same acquired Lease object through more
        # than one orchestration layer.  Re-admission of that exact identity is
        # idempotent; a second supervisor has a different lease_id and still
        # conflicts even when it uses the same service_id.
        if row[2] == self.lease_id and row[6] == self.owner_id:
            self.fencing_epoch = int(row[3] or 1)
            return

        expires_at = row[14]
        live = (
            str(row[15] or "supervisor_held") != "released"
            and expires_at is not None
            and expires_at > current
        )
        # A service lease names two live processes.  Proving only that the
        # supervisor PID disappeared is insufficient after a parent SIGKILL:
        # the worker may still be draining or may still own an open destination
        # transaction.  A takeover is therefore admissible only when the parent
        # is proven gone and the assigned worker (if any) is also proven gone.
        supervisor_dead = _owner_proof(row[7], row[8], row[9])
        worker_dead = row[10] is None or _owner_proof(row[7], row[10], row[11])
        proof_dead = supervisor_dead and worker_dead
        if live and not proof_dead:
            self._raise_conflict(row, con=con)
        if live and proof_dead:
            log.warning(
                "reclaiming physical lease %r after process-start proof owner=%s pid=%s",
                self.name,
                row[6],
                row[8],
            )
        old_epoch = int(row[3] or 0)
        next_epoch = max(1, old_epoch + 1)
        self.fencing_epoch = next_epoch
        def takeover():
            return con.execute(
                f"UPDATE {_control_table(self.control_schema, 'lease')} SET "
                "lease_key=?, lease_id=?, fencing_epoch=?, service_id=?, worker_generation=?, "
                "owner_id=?, host=?, pid=?, process_start_token=?, worker_pid=NULL, "
                "worker_start_token=NULL, acquired_at=?, renewed_at=?, expires_at=?, state=? "
                "WHERE pipeline=? AND coalesce(fencing_epoch, 0)=?",
                [
                    self.pipeline, self.lease_id, next_epoch, self.service_id,
                    self.worker_generation, self.owner_id, socket.gethostname(), os.getpid(),
                    self.process_start_token, current, current,
                    current + timedelta(seconds=self.ttl_seconds), "supervisor_held",
                    self.pipeline, old_epoch,
                ],
            )

        self._retry_lease_write(con, "takeover", takeover)
        observed = self._row(con)
        if observed is None or observed[2] != self.lease_id or int(observed[3] or 0) != next_epoch:
            if observed is not None:
                self._raise_conflict(observed, con=con)
            raise LeaseLost("the physical lease takeover lost its conditional epoch race")

    def _matches(self, row) -> bool:
        return bool(
            row is not None
            and row[2] == self.lease_id
            and int(row[3] or 0) == self.epoch
            and row[6] == self.owner_id
            and (row[4] is None or row[4] == self.service_id)
            and (row[5] is None or row[5] == self.worker_generation)
            and (
                self.worker_start_token is None
                or row[11] is None
                or row[11] == self.worker_start_token
            )
            and str(row[15] or "supervisor_held") != "released"
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
        current = self._server_now(con)
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
        if self.worker_start_token is not None:
            identity_predicate += "AND coalesce(worker_start_token, ?) = ? "
            params.extend([self.worker_start_token, self.worker_start_token])
        live_predicate = " AND expires_at > ?" if require_live else ""
        if require_live:
            params.append(current)
        def refresh():
            return con.execute(
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
            observed_now = self._server_now(con)
            live_after = bool(row is not None and row[14] is not None and row[14] > observed_now)
        if not self._matches(row) or not live_after:
            raise LeaseLost(
                f"lease for {self.name!r} was lost during {operation}; refusing data write",
                lease_state=self._durable_receipt(con, row, operation),
            )

    def renew(self, con) -> None:
        """Compatibility name for the worker's same-transaction fence."""
        self.fence(con)

    def fence(self, con) -> None:
        """Fence the exact epoch inside the destination data transaction."""
        self._assert_outside_commit_ack("fence")
        self._conditional_refresh(con, operation="fence", require_live=True)

    def renew_control(self, con) -> None:
        """Renew on the supervisor control connection, never in COMMIT_ACK."""
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
        """Attach a worker to a supervisor-acquired epoch without reacquiring it."""
        self._assert_outside_commit_ack("attach")
        self.assert_current(con)
        row = self._row(con)
        if row[4] != self.service_id or row[5] != self.worker_generation:
            raise LeaseLost("worker generation does not match the physical lease")
        if row[9] != self.process_start_token:
            raise LeaseLost("worker parent process-start token does not match the lease")
        if self.worker_start_token is not None and row[11] != self.worker_start_token:
            raise LeaseLost("worker process-start token does not match the physical lease")

    def assign_worker(self, con, *, pid: int, start_token: str, generation: str) -> None:
        self._assert_outside_commit_ack("assign_worker")
        if self.fencing_epoch is None:
            raise LeaseLost("cannot assign a worker before acquiring the lease")
        con.execute(
            f"UPDATE {_control_table(self.control_schema, 'lease')} SET "
            "worker_generation=?, worker_pid=?, worker_start_token=?, state=? "
            "WHERE pipeline=? AND owner_id=? AND lease_id=? AND fencing_epoch=?",
            [generation, int(pid), start_token, "worker_starting", self.pipeline,
             self.owner_id, self.lease_id, self.epoch],
        )
        self.worker_generation = generation
        self.worker_pid = int(pid)
        self.worker_start_token = start_token
        row = self._row(con)
        if not self._matches(row) or row[10] != int(pid) or row[5] != generation:
            raise LeaseLost("worker assignment lost the physical lease epoch race")

    def mark_worker_active(self, con, *, generation: str, start_token: str) -> None:
        self._assert_outside_commit_ack("mark_worker_active")
        con.execute(
            f"UPDATE {_control_table(self.control_schema, 'lease')} SET state=? "
            "WHERE pipeline=? AND owner_id=? AND lease_id=? AND fencing_epoch=? "
            "AND worker_generation=? AND worker_start_token=?",
            ["worker_active", self.pipeline, self.owner_id, self.lease_id, self.epoch,
             generation, start_token],
        )
        row = self._row(con)
        if not self._matches(row) or row[15] != "worker_active":
            raise LeaseLost("worker activation lost the physical lease")

    def confirm_worker(self, con, *, generation: str, start_token: str) -> None:
        """Bind the child-reported process-start token before it writes data."""
        self._assert_outside_commit_ack("confirm_worker")
        con.execute(
            f"UPDATE {_control_table(self.control_schema, 'lease')} SET "
            "worker_start_token=?, state=? WHERE pipeline=? AND owner_id=? "
            "AND lease_id=? AND fencing_epoch=? AND worker_generation=? "
            "AND state='worker_starting'",
            [start_token, "worker_active", self.pipeline, self.owner_id,
             self.lease_id, self.epoch, generation],
        )
        self.worker_start_token = start_token
        row = self._row(con)
        if (
            not self._matches(row)
            or row[5] != generation
            or row[11] != start_token
            or row[15] != "worker_active"
        ):
            raise LeaseLost("worker process-start confirmation lost the physical lease")

    def mark_worker_finished(self, con, *, generation: str) -> None:
        self._assert_outside_commit_ack("mark_worker_finished")
        con.execute(
            f"UPDATE {_control_table(self.control_schema, 'lease')} SET "
            "worker_pid=NULL, worker_start_token=NULL, state=? "
            "WHERE pipeline=? AND owner_id=? AND lease_id=? AND fencing_epoch=? "
            "AND worker_generation=?",
            ["supervisor_held", self.pipeline, self.owner_id, self.lease_id, self.epoch,
             generation],
        )

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
