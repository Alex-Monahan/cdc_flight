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
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .errors import LeaseLost
from .naming import quote

log = logging.getLogger("cdc_flight.destination")

CONTROL_SCHEMA = "_cdc_flight"

#: `table_state.snapshot_state` for a table whose destination data cannot be trusted
#: and which CDC alone cannot rebuild: a source relation that was dropped and
#: recreated (rubric 1.5), or a table caught by rubric 1.8's slot-mismatch recovery.
#: It is the queue `cdc_flight.resnapshot` works from. Defined here rather than in
#: `catalog_apply` because three modules now write it and this is the one they all
#: already depend on.
AWAITING_SNAPSHOT = "awaiting_snapshot"

#: How long a lease write may keep retrying a write-write conflict, and how long it
#: waits between attempts. See `Lease._write` - this exists because a hard crash
#: leaves an abandoned MotherDuck transaction holding the lease row.
LEASE_CONFLICT_BUDGET_SEC = 30.0
LEASE_CONFLICT_RETRY_SEC = 1.0


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
    # `PRIMARY KEY (pipeline, commit_id)`, not `PRIMARY KEY (commit_id)`. The id is
    # allocated as `max(commit_id) + 1` and that cannot be atomic on this
    # destination, so a globally unique key made two *different, valid* pipelines
    # race into a primary-key failure: the loser rolled back safely, but a
    # destination with more than one pipeline could not operate, and "global
    # commit_id" was acting as a coordination mechanism with no global lease
    # (Codex 9). Scoped per pipeline it matches the lease's scope, and the
    # allocation below is monotone within a pipeline.
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.commit_log (
            commit_id       BIGINT      NOT NULL,
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
            tables_touched  VARCHAR[],
            PRIMARY KEY (pipeline, commit_id)
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
    # rubric 1.5. The audit trail for everything that happens to a table rather than
    # to a row: TRUNCATE, DROP, a drop-and-recreate, leaving or joining the
    # publication, and (for 2.3) a table appearing. Written INSIDE the commit group's
    # transaction, so "the destination table was emptied" and "here is why" are one
    # atomic fact. It is also the answer to what a truncate means for history: the
    # current-state table is emptied because Postgres emptied it, and the marker is
    # what a changelog table (8.2) will carry as its truncate row.
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.table_events (
            pipeline        VARCHAR     NOT NULL,
            commit_id       BIGINT      NOT NULL,
            seq             BIGINT      NOT NULL DEFAULT 0,
            occurred_at     TIMESTAMPTZ NOT NULL,
            event           VARCHAR     NOT NULL,
            source_schema   VARCHAR     NOT NULL,
            source_table    VARCHAR     NOT NULL,
            target_table    VARCHAR,
            applied         BOOLEAN     NOT NULL,
            lsn             BIGINT,
            txn_id          VARCHAR,
            rows_removed    BIGINT,
            detail          VARCHAR
        )""",
    # rubric 1.5 / 2.3. What the source catalog looked like the last time we saw it.
    # The `relation_oid` is the load-bearing column: it is the only thing that tells a
    # dropped-and-recreated table from the one we were replicating, and persisting it
    # is what makes that detection survive a restart.
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.source_relations (
            pipeline          VARCHAR     NOT NULL,
            source_schema     VARCHAR     NOT NULL,
            source_table      VARCHAR     NOT NULL,
            relation_oid      BIGINT      NOT NULL,
            published         BOOLEAN     NOT NULL,
            replica_identity  VARCHAR,
            first_seen_at     TIMESTAMPTZ NOT NULL,
            last_seen_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pipeline, source_schema, source_table)
        )""",
    # rubric 1.8. What the *slot and the source cluster* looked like the last time we
    # acquired them. Three of the four cases 1.8 has to detect are invisible from a
    # single observation: a slot that was dropped and recreated at the same name has a
    # perfectly ordinary `confirmed_flush_lsn`, a source restored from a base backup
    # has a perfectly ordinary slot, and a rewound timeline looks like a quiet source.
    # What gives them away is a comparison against the *previous* observation, so the
    # previous observation has to be durable.
    #
    # Written outside the commit group's transaction on purpose: it is an observation
    # about the source, not a fact about the data, and recording it must not be able to
    # fail a commit. Correctness never depends on it - every check degrades to
    # "cannot compare, so assume nothing changed" when the row is missing - it only
    # makes the *detectable* set larger.
    f"""CREATE TABLE IF NOT EXISTS {CONTROL_SCHEMA}.slot_state (
            pipeline           VARCHAR     NOT NULL,
            slot_name          VARCHAR     NOT NULL,
            system_identifier  VARCHAR,
            timeline_id        BIGINT,
            restart_lsn        BIGINT,
            confirmed_flush_lsn BIGINT,
            current_wal_lsn    BIGINT,
            durable_lsn        BIGINT,
            observed_at        TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pipeline, slot_name)
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


#: Columns of `commit_log`, in DDL order, used by the key migration below.
_COMMIT_LOG_COLUMNS = (
    "commit_id", "pipeline", "runner_id", "opened_at", "committed_at", "trigger",
    "unit_count", "event_count", "fenced_units", "spilled", "first_txn_id",
    "last_txn_id", "first_lsn", "last_lsn", "max_source_ts", "tables_touched",
)


def ensure_control_schema(con) -> None:
    _migrate_commit_log_key(con)
    for statement in CONTROL_DDL:
        con.execute(statement)


def _commit_log_primary_key(con) -> tuple[str, ...] | None:
    """The column list of `commit_log`'s PRIMARY KEY, or None if unknowable."""
    try:
        rows = con.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = ? AND table_name = 'commit_log' "
            "AND constraint_type = 'PRIMARY KEY'",
            [CONTROL_SCHEMA],
        ).fetchall()
    except Exception:  # pragma: no cover - a destination without duckdb_constraints()
        log.debug("could not read commit_log constraints", exc_info=True)
        return None
    if not rows:
        return ()
    return tuple(str(c) for c in rows[0][0])


def _migrate_commit_log_key(con) -> None:
    """Move `commit_log` from `PRIMARY KEY (commit_id)` to `(pipeline, commit_id)`.

    Needed because `CREATE TABLE IF NOT EXISTS` cannot change a key, and a
    destination that already hosts a pipeline would otherwise reject the *first*
    commit of a second pipeline: ids are now allocated per pipeline, so a new
    pipeline starts again at 1 (Codex 9). MEASURED against the shared MotherDuck
    development database, which already had the global key.

    Runs before any commit group opens a transaction, and is a no-op once done.
    """
    existing = _commit_log_primary_key(con)
    if existing is None or existing == () or set(existing) == {"pipeline", "commit_id"}:
        return
    log.warning(
        "migrating %s.commit_log from PRIMARY KEY %s to (pipeline, commit_id)",
        CONTROL_SCHEMA, existing,
    )
    columns = ", ".join(_COMMIT_LOG_COLUMNS)
    old = f"{CONTROL_SCHEMA}.commit_log__cdcf_oldkey"
    con.execute(f"DROP TABLE IF EXISTS {old}")
    con.execute(f"ALTER TABLE {CONTROL_SCHEMA}.commit_log RENAME TO commit_log__cdcf_oldkey")
    for statement in CONTROL_DDL:
        if ".commit_log (" in statement:
            con.execute(statement)
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.commit_log ({columns}) SELECT {columns} FROM {old}"
    )
    con.execute(f"DROP TABLE {old}")


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


def next_commit_id(con, pipeline: str) -> int:
    """The next commit id **for this pipeline** (Codex 9).

    Scoped, because `max(...) + 1` cannot be made atomic here and the lease is
    per-pipeline: two different pipelines writing to one destination used to
    contend for the same global id. Within a pipeline the lease guarantees a single
    writer, so monotone-per-pipeline is exactly as strong as the lease is.
    """
    row = con.execute(
        f"SELECT coalesce(max(commit_id), 0) FROM {CONTROL_SCHEMA}.commit_log "
        "WHERE pipeline = ?",
        [pipeline],
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


def write_table_event(
    con,
    *,
    pipeline: str,
    commit_id: int,
    seq: int,
    event: str,
    source_schema: str,
    source_table: str,
    target_table: str | None,
    applied: bool,
    lsn: int | None = None,
    txn_id: str | None = None,
    rows_removed: int | None = None,
    detail: str | None = None,
) -> None:
    """One `table_events` row, inside the commit group's transaction (rubric 1.5)."""
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.table_events "
        "(pipeline, commit_id, seq, occurred_at, event, source_schema, source_table, "
        " target_table, applied, lsn, txn_id, rows_removed, detail) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            pipeline, commit_id, seq, now(), event, source_schema, source_table,
            target_table, applied, lsn, txn_id, rows_removed, detail,
        ],
    )


def upsert_source_relation(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    relation_oid: int,
    published: bool,
    replica_identity: str | None,
) -> None:
    """Record what the source catalog says, inside the commit group's transaction.

    DELETE + INSERT rather than an upsert: the destination is DuckDB/MotherDuck and
    this is the same pattern `write_resume_point` uses, so there is one idiom for
    "replace this row" in the whole control schema.
    """
    first_seen = con.execute(
        f"SELECT first_seen_at FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchall()
    current = now()
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.source_relations "
        "(pipeline, source_schema, source_table, relation_oid, published, "
        " replica_identity, first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
        [
            pipeline, source_schema, source_table, relation_oid, published,
            replica_identity, (first_seen[0][0] if first_seen else current), current,
        ],
    )


def forget_source_relation(con, *, pipeline: str, source_schema: str, source_table: str) -> None:
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.source_relations "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )


def forget_table_state(con, *, pipeline: str, source_schema: str, source_table: str) -> None:
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )


class AlertSink:
    """`_cdc_flight.alerts` on its **own** connection (ADR §9.1, Codex 7 / Opus M-2).

    The comment at the call site used to say "deliberately NOT in this transaction"
    while handing `raise_alert` the applier's own connection, with `BEGIN TRANSACTION`
    open. It was therefore fully transactional and a rolled-back apply discarded it —
    measured: inject `pre_commit:raise` after a detected drop and the DDL correctly
    rolls back while `alerts` is empty. That is precisely the case §9.1 introduces the
    alert for: a destructive change that keeps *failing* to apply (lease loss,
    destination error, repeated crash) produced no signal at all.

    `con.cursor()` is a separate connection to the same database, with its own
    transaction context. VERIFIED on DuckDB 1.5.4: an INSERT on the cursor while the
    parent connection holds an open write transaction succeeds, and survives the
    parent's `ROLLBACK`. `alerts` is only ever written through this sink, so there is
    no writer to conflict with.

    If a destination cannot give us an independent connection, the sink degrades to
    the caller's connection and says so in the row itself (`context.transactional`),
    rather than silently labelling a same-connection insert non-transactional.
    """

    def __init__(self, con, *, pipeline: str):
        self.pipeline = pipeline
        self._main = con
        self._sink = None
        self.independent = False
        try:
            self._sink = con.cursor()
            self.independent = True
        except Exception:  # pragma: no cover - a destination without cursors
            log.warning(
                "could not open an independent connection for alerts; they will be "
                "written inside the commit group's transaction and a rolled-back "
                "apply will discard them",
                exc_info=True,
            )

    def raise_alert(
        self, *, severity: str, code: str, message: str, context=None
    ) -> bool:
        """Write one alert. Returns True if it went to the independent connection."""
        payload = dict(context or {})
        if not self.independent:
            payload["transactional"] = True
        con = self._sink if self.independent else self._main
        try:
            con.execute(
                f"INSERT INTO {CONTROL_SCHEMA}.alerts "
                "(pipeline, raised_at, severity, code, message, context) VALUES (?,?,?,?,?,?)",
                [self.pipeline, now(), severity, code, message,
                 json.dumps(payload, default=str) if payload else None],
            )
        except Exception:  # pragma: no cover - alerting must never mask the cause
            log.warning("could not write alert %s", code, exc_info=True)
            return False
        log.warning("ALERT %s/%s: %s", severity, code, message)
        return self.independent

    def close(self) -> None:
        if self._sink is not None:
            with contextlib.suppress(Exception):
                self._sink.close()
            self._sink = None


def raise_alert(con, *, pipeline: str, severity: str, code: str, message: str, context=None):
    """One-shot alert on a connection the caller owns.

    Kept for callers outside a commit group (start-up, shutdown), where the
    connection has no open transaction and a separate one buys nothing. Anything
    inside a commit group must use `AlertSink`.
    """
    try:
        con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.alerts "
            "(pipeline, raised_at, severity, code, message, context) VALUES (?,?,?,?,?,?)",
            [pipeline, now(), severity, code, message,
             json.dumps(context, default=str) if context else None],
        )
    except Exception:  # pragma: no cover - alerting must never mask the cause
        log.warning("could not write alert %s", code, exc_info=True)


def read_slot_state(con, pipeline: str, slot_name: str) -> dict | None:
    """The last recorded observation of this pipeline's slot, or None (rubric 1.8)."""
    rows = con.execute(
        f"SELECT system_identifier, timeline_id, restart_lsn, confirmed_flush_lsn, "
        f"       current_wal_lsn, durable_lsn, observed_at "
        f"FROM {CONTROL_SCHEMA}.slot_state WHERE pipeline = ? AND slot_name = ?",
        [pipeline, slot_name],
    ).fetchall()
    if not rows:
        return None
    keys = (
        "system_identifier", "timeline_id", "restart_lsn", "confirmed_flush_lsn",
        "current_wal_lsn", "durable_lsn", "observed_at",
    )
    return dict(zip(keys, rows[0], strict=True))


def write_slot_state(con, *, pipeline: str, slot_name: str, observation: dict) -> None:
    """Record what the slot and the source cluster look like now (rubric 1.8).

    DELETE + INSERT, the control schema's one idiom for "replace this row". Called on
    its own, never inside a commit group: see the DDL comment.
    """
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.slot_state WHERE pipeline = ? AND slot_name = ?",
        [pipeline, slot_name],
    )
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.slot_state "
        "(pipeline, slot_name, system_identifier, timeline_id, restart_lsn, "
        " confirmed_flush_lsn, current_wal_lsn, durable_lsn, observed_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        [
            pipeline,
            slot_name,
            observation.get("system_identifier"),
            observation.get("timeline_id"),
            observation.get("restart_lsn"),
            observation.get("confirmed_flush_lsn"),
            observation.get("current_wal_lsn"),
            observation.get("durable_lsn"),
            now(),
        ],
    )


def tables_awaiting_snapshot(con, pipeline: str) -> list[tuple[str, str, str]]:
    """`(source_schema, source_table, target_table)` for every table owed a snapshot.

    The queue rubric 1.6's re-snapshot works from and rubric 1.5's `recreated` action
    and rubric 1.8's recovery both write into. Ordered so a re-snapshot is
    deterministic and its logs are diffable.
    """
    rows = con.execute(
        f"SELECT source_schema, source_table, target_table FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND snapshot_state = ? ORDER BY source_schema, source_table",
        [pipeline, AWAITING_SNAPSHOT],
    ).fetchall()
    return [(str(a), str(b), str(c)) for a, b, c in rows]


def request_snapshot(
    con, *, pipeline: str, tables: list[tuple[str, str, str]], detail: str
) -> int:
    """Mark tables as owing a snapshot. Returns how many rows were marked.

    Idempotent: a table already `awaiting_snapshot` stays so. It deliberately does
    NOT touch the destination table - the data stays queryable, stale and flagged,
    until the re-snapshot swaps a complete image over it in one transaction.
    """
    marked = 0
    for schema, table, target in tables:
        con.execute(
            f"UPDATE {CONTROL_SCHEMA}.table_state SET snapshot_state = ? "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [AWAITING_SNAPSHOT, pipeline, schema, table],
        )
        existing = con.execute(
            f"SELECT 1 FROM {CONTROL_SCHEMA}.table_state "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [pipeline, schema, table],
        ).fetchall()
        if not existing:
            mark_awaiting_snapshot(
                con, pipeline=pipeline, source_schema=schema, source_table=table,
                target_table=target, state=AWAITING_SNAPSHOT,
            )
        marked += 1
    log.warning("marked %s table(s) as awaiting a snapshot: %s", marked, detail)
    return marked


def mark_awaiting_snapshot(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    state: str,
) -> None:
    """Record that a table's destination data is gone and CDC cannot rebuild it.

    Rubric 1.5 / Opus Q1. A `recreated` source relation means the destination table
    held a *different* relation's rows: keeping them presents pre-drop data as
    current, and dropping them and letting ordinary CDC re-create a partial table is
    worse still, because the destination then looks healthy while being silently
    incomplete. So the row survives the drop carrying `snapshot_state` — the run
    summary and `inspect` surface it, and rubric 2.3/3.4's re-snapshot clears it.
    """
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.table_state "
        "(pipeline, source_schema, source_table, target_table, snapshot_state) "
        "VALUES (?,?,?,?,?)",
        [pipeline, source_schema, source_table, target_table, state],
    )


def register_table(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
) -> None:
    """Persist source-to-destination ownership, inside the transaction that creates it.

    Codex 5: `table_state` is the canonical registry the catalog watcher seeds itself
    from, and it used to be written only by the snapshot coordinator. A table first
    materialised by streaming DML therefore had no durable row, so a `DROP TABLE`
    while the pipeline was down left an orphan destination table that no later poll
    could ever report — `_compare` skips a name it has no oid for and does not believe
    is ours. Written by whoever creates the table, whatever the origin.
    """
    existing = con.execute(
        f"SELECT 1 FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchall()
    if existing:
        return
    con.execute(
        f"INSERT INTO {CONTROL_SCHEMA}.table_state "
        "(pipeline, source_schema, source_table, target_table) VALUES (?,?,?,?)",
        [pipeline, source_schema, source_table, target_table],
    )


# --------------------------------------------------------------------------- #
# single-writer lease (rubric 4.2)
# --------------------------------------------------------------------------- #
def _is_dead(host: str | None, pid: int | None) -> bool:
    """True when the recorded owner is gone *as far as this process can tell*.

    "As far as this process can tell" means: it recorded this hostname, and no such
    pid exists in **our** PID namespace. That is a proof only when the recorded
    owner shared that namespace - inside containers that share a hostname across
    PID namespaces it can reclaim a live lease (Opus MINOR-10), which is why the
    guarantee is stated this way rather than as "provable". A lease from another
    host is never assumed dead: there the TTL is the only safe answer.
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
        self._write(con, current, expires)

    def _write(self, con, current: datetime, expires: datetime) -> None:
        """DELETE + INSERT the lease row, retrying a write-write conflict.

        MEASURED against MotherDuck, 2026-07-31, while adding the MotherDuck fault
        tests: after a hard crash (`os._exit`, the fault injector's SIGKILL
        equivalent) the dead process leaves an **uncommitted server-side
        transaction** that had already touched this row, so the next runner's
        `DELETE` fails with `TransactionContext Error: Conflict on tuple deletion!`.
        The lease logic is right - the dead pid is reclaimable - but the write has
        to outlive the moment MotherDuck spends aborting the abandoned transaction.

        Retrying is safe: the row is the lease's own bookkeeping, the statements are
        idempotent, and this runs before any data is written. Failing after the
        budget is also safe - the run exits non-zero and nothing was applied - but it
        would make crash recovery depend on a timer, which is exactly what `_is_dead`
        exists to avoid.
        """
        deadline = time.monotonic() + LEASE_CONFLICT_BUDGET_SEC
        attempt = 0
        while True:
            attempt += 1
            try:
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
                return
            except Exception as exc:
                if "conflict" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                log.warning(
                    "lease row for %r is locked by an abandoned transaction (attempt %s): "
                    "%s; retrying",
                    self.pipeline, attempt, exc,
                )
                with contextlib.suppress(Exception):
                    con.execute("ROLLBACK")
                time.sleep(LEASE_CONFLICT_RETRY_SEC)

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
