"""The destination connection and the `_cdc_flight` control schema (ADR 0001 §4.8).

One DuckDB/MotherDuck connection, owned by the applier, carrying both the data
and the state. Everything in this module is written **inside** the commit
group's transaction unless the docstring says otherwise, because principle (4)
- data commits and state commits are atomic with one another - is what the whole
design rests on.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import socket
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .errors import LeaseLost
from .naming import quote

log = logging.getLogger("cdc_flight.destination")

CONTROL_SCHEMA = "_cdc_flight"


# --------------------------------------------------------------------------- #
# resume point
# --------------------------------------------------------------------------- #
@dataclass
class ResumePoint:
    """Where the destination says we are. The only source of truth (ADR §4.5)."""

    partition: dict[str, Any] = field(default_factory=dict)
    offset: dict[str, Any] = field(default_factory=dict)
    last_lsn: int = 0
    last_txn_id: str | None = None
    last_total_order: int | None = None
    commit_id: int = 0
    snapshot_epoch: int = 0

    def to_json(self) -> str:
        return json.dumps(
            {
                "partition": self.partition,
                "offset": self.offset,
                "last_lsn": self.last_lsn,
                "last_txn_id": self.last_txn_id,
                "last_total_order": self.last_total_order,
            },
            separators=(",", ":"),
            sort_keys=False,
        )

    @classmethod
    def from_json(cls, text: str, **extra) -> ResumePoint:
        payload = json.loads(text)
        return cls(
            partition=payload.get("partition") or {},
            offset=payload.get("offset") or {},
            last_lsn=int(payload.get("last_lsn") or 0),
            last_txn_id=payload.get("last_txn_id"),
            last_total_order=payload.get("last_total_order"),
            **extra,
        )


# --------------------------------------------------------------------------- #
# connection
# --------------------------------------------------------------------------- #
def connect(dest) -> Any:
    """Open the one destination connection the applier writes through."""
    import duckdb

    if dest.kind == "duckdb":
        dest.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(dest.duckdb_path))

    if dest.kind == "motherduck":
        from .config import motherduck_token

        token = motherduck_token()
        if not token:
            raise RuntimeError(
                "CDC_DESTINATION=motherduck but neither `motherduck_token` nor "
                "`MOTHERDUCK_TOKEN` is set in the environment."
            )
        bootstrap = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            bootstrap.execute(f'CREATE DATABASE IF NOT EXISTS "{dest.motherduck_database}"')
        finally:
            bootstrap.close()
        return duckdb.connect(
            f"md:{dest.motherduck_database}?motherduck_token={token}"
        )

    raise ValueError(f"unknown destination {dest.kind!r} (expected duckdb|motherduck)")


CONTROL_DDL = [
    f"CREATE SCHEMA IF NOT EXISTS {CONTROL_SCHEMA}",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.debezium_offsets (
            pipeline          VARCHAR     NOT NULL,
            namespace         VARCHAR     NOT NULL,
            resume_json       VARCHAR     NOT NULL,
            offset_blob       BLOB,
            offset_key_blob   BLOB,
            commit_id         BIGINT      NOT NULL,
            last_lsn          BIGINT      NOT NULL,
            last_txn_id       VARCHAR,
            last_total_order  BIGINT,
            snapshot_epoch    BIGINT      NOT NULL DEFAULT 0,
            updated_at        TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pipeline, namespace)
        )""",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.commit_log (
            commit_id       BIGINT      PRIMARY KEY,
            pipeline        VARCHAR     NOT NULL,
            runner_id       VARCHAR     NOT NULL,
            opened_at       TIMESTAMPTZ NOT NULL,
            committed_at    TIMESTAMPTZ NOT NULL,
            trigger         VARCHAR     NOT NULL,
            unit_count      BIGINT      NOT NULL,
            event_count     BIGINT      NOT NULL,
            fenced_units    BIGINT      NOT NULL DEFAULT 0,
            spilled         BOOLEAN     NOT NULL DEFAULT false,
            first_txn_id    VARCHAR,
            last_txn_id     VARCHAR,
            first_lsn       BIGINT,
            last_lsn        BIGINT,
            max_source_ts   TIMESTAMPTZ,
            tables_touched  VARCHAR[]
        )""",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.lease (
            pipeline        VARCHAR     PRIMARY KEY,
            owner_id        VARCHAR     NOT NULL,
            host            VARCHAR,
            pid             BIGINT,
            acquired_at     TIMESTAMPTZ NOT NULL,
            renewed_at      TIMESTAMPTZ NOT NULL,
            expires_at      TIMESTAMPTZ NOT NULL
        )""",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.table_state (
            pipeline        VARCHAR     NOT NULL,
            source_schema   VARCHAR     NOT NULL,
            source_table    VARCHAR     NOT NULL,
            target_table    VARCHAR     NOT NULL,
            refresh_mode    VARCHAR     NOT NULL DEFAULT 'cdc',
            delete_mode     VARCHAR     NOT NULL DEFAULT 'hard',
            history_mode    VARCHAR     NOT NULL DEFAULT 'none',
            key_strategy    VARCHAR     NOT NULL DEFAULT 'pk',
            key_columns     VARCHAR[],
            snapshot_state  VARCHAR     NOT NULL DEFAULT 'none',
            snapshot_epoch  BIGINT      NOT NULL DEFAULT 0,
            snapshot_lsn    BIGINT,
            last_commit_id  BIGINT,
            PRIMARY KEY (pipeline, source_schema, source_table)
        )""",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.spill_events (
            commit_id      BIGINT   NOT NULL,
            unit_seq       BIGINT   NOT NULL,
            event_seq      BIGINT   NOT NULL,
            target_table   VARCHAR  NOT NULL,
            source_schema  VARCHAR,
            source_table   VARCHAR,
            lsn            BIGINT,
            txn_id         VARCHAR,
            total_order    BIGINT,
            cdcf_event_id  VARCHAR  NOT NULL,
            op             VARCHAR  NOT NULL,
            source_ts_ms   BIGINT,
            before_json    VARCHAR,
            after_json     VARCHAR,
            key_json       VARCHAR
        )""",
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.alerts (
            pipeline        VARCHAR     NOT NULL,
            raised_at       TIMESTAMPTZ NOT NULL,
            severity        VARCHAR     NOT NULL,
            code            VARCHAR     NOT NULL,
            message         VARCHAR     NOT NULL,
            context         VARCHAR
        )""",
]


def ensure_control_schema(con) -> None:
    for statement in CONTROL_DDL:
        con.execute(statement)


def ensure_dataset(con, dataset: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(dataset)}")


def now() -> datetime:
    return datetime.now(UTC)


# --------------------------------------------------------------------------- #
# resume point I/O
# --------------------------------------------------------------------------- #
def read_resume_point(con, pipeline: str, namespace: str) -> ResumePoint | None:
    rows = con.execute(
        f"SELECT resume_json, commit_id, last_lsn, last_txn_id, last_total_order, "
        f"       snapshot_epoch, offset_blob, offset_key_blob "
        f"FROM {CONTROL_SCHEMA}.debezium_offsets WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    ).fetchall()
    if not rows:
        return None
    (
        resume_json,
        commit_id,
        last_lsn,
        last_txn_id,
        last_total_order,
        snapshot_epoch,
        _blob,
        _key_blob,
    ) = rows[0]
    point = ResumePoint.from_json(resume_json)
    point.commit_id = int(commit_id or 0)
    point.last_lsn = int(last_lsn or point.last_lsn or 0)
    point.last_txn_id = last_txn_id
    point.last_total_order = last_total_order
    point.snapshot_epoch = int(snapshot_epoch or 0)
    return point


def read_offset_blobs(con, pipeline: str, namespace: str) -> tuple[bytes | None, bytes | None]:
    rows = con.execute(
        f"SELECT offset_blob, offset_key_blob FROM {CONTROL_SCHEMA}.debezium_offsets "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    ).fetchall()
    if not rows:
        return None, None
    blob, key_blob = rows[0]
    return (bytes(blob) if blob is not None else None,
            bytes(key_blob) if key_blob is not None else None)


def write_resume_point(
    con,
    *,
    pipeline: str,
    namespace: str,
    point: ResumePoint,
    commit_id: int,
    offset_blob: bytes | None,
    offset_key_blob: bytes | None,
) -> None:
    """The statement that makes principle (4) literal. Runs inside the group txn."""
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.debezium_offsets "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    )
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.debezium_offsets "
        "(pipeline, namespace, resume_json, offset_blob, offset_key_blob, commit_id, "
        " last_lsn, last_txn_id, last_total_order, snapshot_epoch, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            pipeline,
            namespace,
            point.to_json(),
            offset_blob,
            offset_key_blob,
            commit_id,
            point.last_lsn,
            point.last_txn_id,
            point.last_total_order,
            point.snapshot_epoch,
            now(),
        ],
    )


def next_commit_id(con) -> int:
    row = con.execute(
        f"SELECT coalesce(max(commit_id), 0) FROM {CONTROL_SCHEMA}.commit_log"
    ).fetchone()
    return int(row[0] or 0) + 1


def write_commit_log(con, **kwargs) -> None:
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.commit_log "
        "(commit_id, pipeline, runner_id, opened_at, committed_at, trigger, "
        " unit_count, event_count, fenced_units, spilled, first_txn_id, last_txn_id, "
        " first_lsn, last_lsn, max_source_ts, tables_touched) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            kwargs["commit_id"],
            kwargs["pipeline"],
            kwargs["runner_id"],
            kwargs["opened_at"],
            kwargs["committed_at"],
            kwargs["trigger"],
            kwargs["unit_count"],
            kwargs["event_count"],
            kwargs["fenced_units"],
            kwargs["spilled"],
            kwargs["first_txn_id"],
            kwargs["last_txn_id"],
            kwargs["first_lsn"],
            kwargs["last_lsn"],
            kwargs["max_source_ts"],
            kwargs["tables_touched"],
        ],
    )


def raise_alert(con, *, pipeline: str, severity: str, code: str, message: str, context=None):
    """Alerts are written on whatever connection is handy - deliberately not
    transactional with the data (ADR §9.1: a signal that disappears when the
    apply rolls back is the signal you need most)."""
    try:
        con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.alerts "
            "(pipeline, raised_at, severity, code, message, context) VALUES (?,?,?,?,?,?)",
            [pipeline, now(), severity, code, message,
             json.dumps(context, default=str) if context else None],
        )
    except Exception:  # pragma: no cover - alerting must never mask the cause
        log.warning("could not write alert %s", code, exc_info=True)


# --------------------------------------------------------------------------- #
# single-writer lease (rubric 4.2)
# --------------------------------------------------------------------------- #
def _is_dead(host: str | None, pid: int | None) -> bool:
    """True only when we can *prove* the recorded owner is gone.

    Provable means: it claimed this host, and the pid does not exist. A lease from
    another host is never assumed dead - there the TTL is the only safe answer.
    """
    if not host or not pid or host != socket.gethostname():
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OverflowError, ValueError):
        return False
    return False


@dataclass
class Lease:
    pipeline: str
    owner_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ttl_seconds: float = 60.0

    def acquire(self, con) -> None:
        rows = con.execute(
            f"SELECT owner_id, expires_at, host, pid FROM {CONTROL_SCHEMA}.lease "
            "WHERE pipeline = ?",
            [self.pipeline],
        ).fetchall()
        current = now()
        if rows:
            owner, expires_at, host, pid = rows[0]
            live = owner != self.owner_id and expires_at is not None and expires_at > current
            if live and _is_dead(host, pid):
                # A process that was SIGKILLed (or that fault injection `os._exit`ed)
                # never released its lease. Waiting out the TTL would make crash
                # RECOVERY - the normal path this whole design exists to make safe -
                # depend on a timer. A lease whose owning pid is demonstrably gone on
                # this host is not a concurrent writer, so it is reclaimed and said so.
                log.warning(
                    "reclaiming the lease for %r from dead runner %s (pid %s on %s)",
                    self.pipeline, owner, pid, host,
                )
                live = False
            if live:
                raise LeaseLost(
                    f"pipeline {self.pipeline!r} is already leased by runner {owner} "
                    f"(pid {pid} on {host}) until {expires_at.isoformat()}; a second "
                    "concurrent Flight would double-write (rubric 4.2)"
                )
        self._upsert(con, current)

    def renew(self, con) -> None:
        """Renewed *inside* every commit group, so the loser of a race fails
        before it writes rather than after."""
        rows = con.execute(
            f"SELECT owner_id FROM {CONTROL_SCHEMA}.lease WHERE pipeline = ?",
            [self.pipeline],
        ).fetchall()
        if rows and rows[0][0] != self.owner_id:
            raise LeaseLost(
                f"lease for {self.pipeline!r} was taken by runner {rows[0][0]}; "
                "this commit group must not be applied (rubric 4.2)"
            )
        self._upsert(con, now())

    def _upsert(self, con, current: datetime) -> None:
        from datetime import timedelta

        expires = current + timedelta(seconds=self.ttl_seconds)
        con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.lease WHERE pipeline = ?", [self.pipeline]
        )
        con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.lease "
            "(pipeline, owner_id, host, pid, acquired_at, renewed_at, expires_at) "
            "VALUES (?,?,?,?,?,?,?)",
            [self.pipeline, self.owner_id, socket.gethostname(), os.getpid(),
             current, current, expires],
        )

    def release(self, con) -> None:
        try:
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.lease WHERE pipeline = ? AND owner_id = ?",
                [self.pipeline, self.owner_id],
            )
        except Exception:  # pragma: no cover
            log.debug("could not release lease", exc_info=True)


# --------------------------------------------------------------------------- #
# ADR §14.1 — is DROP/RENAME transactional at this destination?
# --------------------------------------------------------------------------- #
def probe_transactional_ddl(con) -> bool:
    """Answer ADR 0001's biggest open question empirically, once per run.

    The shadow-table swap (D7) is `DROP` + `ALTER … RENAME` inside the commit
    group's transaction. If the destination does not honour that transactionally
    the swap has to fall back to `CREATE OR REPLACE TABLE … AS SELECT`, which the
    rubric explicitly allows ("BEGIN / COMMIT transactionality fine too"). The
    probe is a few milliseconds and removes a guess from the design.
    """
    probe_a = f"{CONTROL_SCHEMA}.__ddl_probe_a"
    probe_b = f"{CONTROL_SCHEMA}.__ddl_probe_b"
    try:
        con.execute(f"DROP TABLE IF EXISTS {probe_a}")
        con.execute(f"DROP TABLE IF EXISTS {probe_b}")
        con.execute(f"CREATE TABLE {probe_a} (x INTEGER)")
        con.execute(f"CREATE TABLE {probe_b} (x INTEGER)")
        con.execute("BEGIN TRANSACTION")
        con.execute(f"DROP TABLE {probe_a}")
        con.execute(f"ALTER TABLE {probe_b} RENAME TO __ddl_probe_a")
        con.execute("ROLLBACK")
        # Transactional iff the rollback put both tables back.
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{CONTROL_SCHEMA}' AND table_name LIKE '__ddl_probe%'"
        ).fetchall()
        names = {r[0] for r in rows}
        return {"__ddl_probe_a", "__ddl_probe_b"} <= names
    except Exception as exc:
        log.info("transactional DDL probe failed (%s); using the CTAS swap", exc)
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        return False
    finally:
        for name in (probe_a, probe_b):
            with contextlib.suppress(Exception):  # pragma: no cover
                con.execute(f"DROP TABLE IF EXISTS {name}")
