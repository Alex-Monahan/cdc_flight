"""The destination connection and the `_cdc_flight` control schema (ADR 0001 §4.8).

One DuckDB/MotherDuck connection, owned by the applier, carrying both the data
and the state. Everything in this module is written **inside** the commit
group's transaction unless the docstring says otherwise, because principle (4)
- data commits and state commits are atomic with one another - is what the whole
design rests on.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from . import faults, table_lifecycle
from .config import resolve_control_schema
from .control_schema import CONTROL_DDL, ensure_control_schema
from .errors import CANONICAL_REFUSAL_CLASS, LeaseLost
from .machines import (
    LIFECYCLE_DURABLE_VALUES,
    REFUSAL_ABSENT,
    REFUSAL_PENDING,
    REFUSAL_QUARANTINED,
    REFUSAL_RESOLVED,
    SCHEMA_REFUSAL,
    SLOT_VERDICTS,
)
from .naming import control_table, quote
from .retirement import RetirementResult, retire_handle

# Re-exported: `source_relations.py` is a split of this module, not a new dependency
# for its callers (Codex r3 MINOR / the 1,000-line threshold).
from .source_relations import (  # noqa: F401
    flush_learned_relations,
    forget_source_relation,
    upsert_source_relation,
)

__all__ = ["CONTROL_DDL", "ensure_control_schema"]

log = logging.getLogger("cdc_flight.destination")

# DuckDB 1.5.4 supports VARIANT in persistent databases only when the file is
# created with the v1.5 storage compatibility level.  Its default VARIANT
# shredding path also refuses to append after a large object-heavy row group;
# disabling that optimization keeps the native VARIANT contract intact.  The
# same settings are accepted by MotherDuck, keeping destination setup on one
# runtime-neutral contract.
DUCKDB_CONNECT_CONFIG = {
    "storage_compatibility_version": "v1.5.0",
    "variant_minimum_shredding_size": "-1",
}


def assert_runtime_capabilities(con) -> None:
    """Verify the effective DuckDB/MotherDuck settings, not only the request."""
    names = tuple(DUCKDB_CONNECT_CONFIG)
    rows = con.execute(
        "SELECT name, value FROM duckdb_settings() WHERE name IN (?, ?)", names
    ).fetchall()
    effective = {str(name): str(value) for name, value in rows}
    expected = {name: str(value) for name, value in DUCKDB_CONNECT_CONFIG.items()}
    if effective != expected:
        raise RuntimeError(
            "destination runtime did not apply the required VARIANT settings: "
            f"expected {expected!r}, got {effective!r}"
        )

def _control_table(control_schema: str | None, table: str) -> str:
    return control_table(resolve_control_schema(control_schema), table)

#: `table_state.snapshot_state` for a table whose destination data cannot be trusted
#: and which CDC alone cannot rebuild: a source relation that was dropped and
#: recreated (rubric 1.5), or a table caught by rubric 1.8's slot-mismatch recovery.
#: It is the queue `cdc_flight.resnapshot` works from. Defined here rather than in
#: `catalog_apply` because three modules now write it and this is the one they all
#: already depend on.
AWAITING_SNAPSHOT = "awaiting_snapshot"

#: `table_state.snapshot_state`, frozen. ADR §4.8 declared `none|in_progress|complete|
#: failed` and the value the whole re-snapshot machinery runs on - `awaiting_snapshot` -
#: was not in the declared domain, while `failed` was declared and never written by
#: anything. Nothing validated a read, so a typo anywhere would have produced a state
#: that silently belongs to no queue (architecture review, finding 2).
#:
#: The domain, the legal edges and the writers now live in `cdc_flight.table_lifecycle`
#: (rubric 1.9, ADR §20/A55); these names are re-exported so the existing call sites and
#: test literals keep reading as they did.
SNAPSHOT_NONE = table_lifecycle.NONE
SNAPSHOT_IN_PROGRESS = table_lifecycle.IN_PROGRESS
SNAPSHOT_COMPLETE = table_lifecycle.COMPLETE
SNAPSHOT_GONE = table_lifecycle.GONE
SNAPSHOT_STATES = LIFECYCLE_DURABLE_VALUES

#: States that mean "this table does not hold a trustworthy image of the source".
#: `in_progress` is in here and that is the whole point: it is durable, it is NOT
#: terminal, and until now no durable query selected it, so a table whose snapshot was
#: interrupted by anything the `except BaseException` handler cannot catch - `os._exit`,
#: `SIGKILL`, the commit watchdog - was invisible to every queue and to the recovery
#: journal's "is the rebuild finished?" test (architecture review, finding 1). It is now
#: DERIVED from the machine's terminal set rather than restated as a second literal.
SNAPSHOT_STATES_OWING_WORK = table_lifecycle.OWING_WORK

#: How long a lease write may keep retrying a write-write conflict, and how long it
#: waits between attempts. See `Lease._write` - this exists because a hard crash
#: leaves an abandoned MotherDuck transaction holding the lease row.
LEASE_CONFLICT_BUDGET_SEC = 30.0
LEASE_CONFLICT_RETRY_SEC = 1.0

#: How many times this process has queued a table rebuild - rubric 1.7's `<nth>` for
#: the `table_rebuild_queued` anchor.
def _queueing() -> int:
    return faults.arrival("table_rebuild_queued")


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
        con = duckdb.connect(str(dest.duckdb_path), config=DUCKDB_CONNECT_CONFIG)
        assert_runtime_capabilities(con)
        return con

    if dest.kind == "motherduck":
        from .config import motherduck_token

        token = motherduck_token()
        if not token:
            raise RuntimeError(
                "CDC_DESTINATION=motherduck but neither `motherduck_token` nor "
                "`MOTHERDUCK_TOKEN` is set in the environment."
            )
        bootstrap = duckdb.connect(
            f"md:?motherduck_token={token}", config=DUCKDB_CONNECT_CONFIG
        )
        try:
            assert_runtime_capabilities(bootstrap)
            bootstrap.execute(
                f"CREATE DATABASE IF NOT EXISTS {quote(dest.motherduck_database)}"
            )
        finally:
            bootstrap.close()
        con = duckdb.connect(
            f"md:{dest.motherduck_database}?motherduck_token={token}",
            config=DUCKDB_CONNECT_CONFIG,
        )
        assert_runtime_capabilities(con)
        return con

    raise ValueError(f"unknown destination {dest.kind!r} (expected duckdb|motherduck)")


def ensure_dataset(con, dataset: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(dataset)}")


def now() -> datetime:
    return datetime.now(UTC)


# --------------------------------------------------------------------------- #
# resume point I/O
# --------------------------------------------------------------------------- #
def read_resume_point(
    con, pipeline: str, namespace: str, *, control_schema: str | None = None
) -> ResumePoint | None:
    rows = con.execute(
        f"SELECT resume_json, commit_id, last_lsn, last_txn_id, last_total_order, "
        f"       snapshot_epoch, offset_blob, offset_key_blob "
        f"FROM {_control_table(control_schema, 'debezium_offsets')} "
        "WHERE pipeline = ? AND namespace = ?",
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


def read_offset_blobs(
    con, pipeline: str, namespace: str, *, control_schema: str | None = None
) -> tuple[bytes | None, bytes | None]:
    rows = con.execute(
        f"SELECT offset_blob, offset_key_blob FROM "
        f"{_control_table(control_schema, 'debezium_offsets')} "
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
    control_schema: str | None = None,
) -> None:
    """The statement that makes principle (4) literal. Runs inside the group txn."""
    con.execute(
        f"DELETE FROM {_control_table(control_schema, 'debezium_offsets')} "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    )
    con.execute(
        f"INSERT INTO {_control_table(control_schema, 'debezium_offsets')} "
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


def next_commit_id(con, pipeline: str, *, control_schema: str | None = None) -> int:
    """The next commit id **for this pipeline** (Codex 9).

    Scoped, because `max(...) + 1` cannot be made atomic here and the lease is
    per-pipeline: two different pipelines writing to one destination used to
    contend for the same global id. Within a pipeline the lease guarantees a single
    writer, so monotone-per-pipeline is exactly as strong as the lease is.
    """
    row = con.execute(
        f"SELECT coalesce(max(commit_id), 0) FROM "
        f"{_control_table(control_schema, 'commit_log')} "
        "WHERE pipeline = ?",
        [pipeline],
    ).fetchone()
    return int(row[0] or 0) + 1


def write_commit_log(con, **kwargs) -> None:
    control_schema = kwargs.pop("control_schema", None)
    con.execute(
        f"INSERT INTO {_control_table(control_schema, 'commit_log')} "
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
    control_schema: str | None = None,
) -> None:
    """One `table_events` row, inside the commit group's transaction (rubric 1.5)."""
    con.execute(
        f"INSERT INTO {_control_table(control_schema, 'table_events')} "
        "(pipeline, commit_id, seq, occurred_at, event, source_schema, source_table, "
        " target_table, applied, lsn, txn_id, rows_removed, detail) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            pipeline, commit_id, seq, now(), event, source_schema, source_table,
            target_table, applied, lsn, txn_id, rows_removed, detail,
        ],
    )


def write_column_presence(
    con,
    *,
    target_dataset: str,
    target_table: str,
    event_id: str,
    column_name: str,
    present: bool = True,
    patch_digest: str | None = None,
    control_schema: str | None = None,
) -> None:
    """Record row-image field presence atomically with the row write.

    ``NULL`` cannot carry presence information.  This tiny journal is consumed only
    by a fenced late-rename merge and is deleted when that merge completes.
    """
    con.execute(
        f"INSERT OR REPLACE INTO {_control_table(control_schema, 'column_presence')} "
        "(target_dataset, target_table, event_id, column_name, present, patch_digest) "
        "VALUES (?,?,?,?,?,?)",
        [target_dataset, target_table, event_id, column_name, present, patch_digest],
    )


def write_column_presence_batch(
    con, rows: list[tuple], *, control_schema: str | None = None
) -> None:
    """Write immutable row-image presence facts in bounded SQL batches.

    The facts are part of the caller's open commit-group transaction and are immutable
    for an event.  Use the same Arrow-backed bulk path as destination row writes: a
    row-at-a-time indexed insert turns a large whole-transaction group into minutes of
    index maintenance and an avoidable multi-gigabyte memory spike.
    """
    if not rows:
        return
    from .apply_sql import BOOLEAN, VARCHAR, bulk_insert

    normalized = [
        [*row, None] if len(row) == 4 else list(row)
        for row in rows
    ]

    bulk_insert(
        con,
        _control_table(control_schema, "column_presence"),
        [
            "target_dataset", "target_table", "event_id", "column_name", "present",
            "patch_digest",
        ],
        normalized,
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, BOOLEAN, VARCHAR],
        replace=True,
    )


def _stable_refusal_fingerprint(
    *,
    source_schema: str,
    source_table: str,
    input_fingerprint: str | None,
) -> str:
    """Return the durable identity shared by every refusal origin.

    Row-value refusals supply the descriptor/table fingerprint from the planner.  All
    other origins get a stable table identity; origin labels and human exception text
    are deliberately excluded because different seams may observe the same condition.
    """
    if input_fingerprint:
        return str(input_fingerprint)
    payload = f"{source_schema}.{source_table}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _ensure_awaiting_snapshot(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str | None,
    reason: str,
    control_schema: str | None,
) -> None:
    """Keep the physical table visibly stale while any refusal is unresolved."""
    current = table_lifecycle.read(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        control_schema=control_schema,
    )
    if current == AWAITING_SNAPSHOT:
        return
    table_lifecycle.transition(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        to=AWAITING_SNAPSHOT,
        reason=reason,
        target_table=target_table,
        control_schema=control_schema,
    )


def _next_table_event_seq(con, *, pipeline: str, control_schema: str | None) -> int:
    return int(
        con.execute(
            f"SELECT coalesce(max(seq), -1) + 1 FROM "
            f"{_control_table(control_schema, 'table_events')} "
            "WHERE pipeline = ? AND commit_id = 0",
            [pipeline],
        ).fetchone()[0]
    )


def record_schema_refusal(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str | None,
    detected_lsn: int | None,
    reason: str,
    input_fingerprint: str | None = None,
    source_fingerprint: str | None = None,
    control_schema: str | None = None,
) -> str:
    """Persist one scoped refusal and quarantine only its source table on repeat.

    The refusal row is an explicit durability boundary.  A first value refusal is
    pending and requests the existing automatic rebuild.  If that rebuild reads the
    same row image and fails again, the row becomes ``quarantined`` and the table is
    moved out of ordinary row admission.  Quarantine remains ``awaiting_snapshot``:
    it is visibly stale, is selected by the recovery queue, and can be reactivated when
    the source/schema condition clears.  The source slot may advance only after this
    obligation is durable, because the future full snapshot reads current source state.
    """
    fingerprint = _stable_refusal_fingerprint(
        source_schema=source_schema,
        source_table=source_table,
        input_fingerprint=input_fingerprint,
    )
    canonical_class = CANONICAL_REFUSAL_CLASS
    source_fingerprint = str(source_fingerprint) if source_fingerprint else None
    quarantine_alert = None
    result = REFUSAL_PENDING
    con.execute("BEGIN TRANSACTION")
    try:
        previous = con.execute(
            f"SELECT state, refusal_fingerprint, source_fingerprint "
            f"FROM {_control_table(control_schema, 'schema_refusals')} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [pipeline, source_schema, source_table],
        ).fetchone()
        before = previous[0] if previous else REFUSAL_ABSENT
        if before == REFUSAL_QUARANTINED:
            SCHEMA_REFUSAL.check(before, REFUSAL_QUARANTINED)
            _ensure_awaiting_snapshot(
                con,
                pipeline=pipeline,
                source_schema=source_schema,
                source_table=source_table,
                target_table=target_table,
                reason="a quarantined refusal remains stale until a full resnapshot",
                control_schema=control_schema,
            )
            con.execute("COMMIT")
            return REFUSAL_QUARANTINED

        stored_fingerprint = previous[1] if previous else None
        repeated_input = (
            before == REFUSAL_PENDING and fingerprint == stored_fingerprint
        )
        if repeated_input:
            # A retry may reach this writer through a source-catalog path that has
            # no new fingerprint. Preserve the first positive fingerprint instead
            # of replacing it with NULL: otherwise the next run mistakes the same
            # unchanged relation for repaired evidence and reactivates quarantine.
            recorded_source_fingerprint = (
                source_fingerprint if source_fingerprint is not None else previous[2]
            )
            SCHEMA_REFUSAL.check(before, REFUSAL_QUARANTINED)
            con.execute(
                f"UPDATE {_control_table(control_schema, 'schema_refusals')} SET "
                "target_table = ?, detected_lsn = ?, reason = ?, "
                "refusal_fingerprint = ?, source_fingerprint = ?, refusal_class = ?, "
                "state = ?, refused_at = ? "
                "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                [
                    target_table, detected_lsn, reason, fingerprint,
                    recorded_source_fingerprint,
                    canonical_class, REFUSAL_QUARANTINED, now(), pipeline,
                    source_schema, source_table,
                ],
            )
            _ensure_awaiting_snapshot(
                con,
                pipeline=pipeline,
                source_schema=source_schema,
                source_table=source_table,
                target_table=target_table,
                reason=(
                    "identical durable input refused twice; table is quarantined and "
                    "marked stale pending a full resnapshot"
                ),
                control_schema=control_schema,
            )
            existing_event = con.execute(
                f"SELECT 1 FROM {_control_table(control_schema, 'table_events')} "
                "WHERE pipeline = ? AND commit_id = 0 AND event = 'schema_quarantine' "
                "AND source_schema = ? AND source_table = ?",
                [pipeline, source_schema, source_table],
            ).fetchone()
            if existing_event is None:
                write_table_event(
                    con,
                    pipeline=pipeline,
                    commit_id=0,
                    seq=_next_table_event_seq(
                        con, pipeline=pipeline, control_schema=control_schema
                    ),
                    event="schema_quarantine",
                    source_schema=source_schema,
                    source_table=source_table,
                    target_table=target_table,
                    applied=False,
                    lsn=detected_lsn,
                    detail=reason,
                    control_schema=control_schema,
                )
            alert_marker = f'%"refusal_fingerprint": "{fingerprint}"%'
            alert_exists = con.execute(
                f"SELECT 1 FROM {_control_table(control_schema, 'alerts')} "
                "WHERE pipeline = ? AND code = 'schema_table_quarantined' "
                "AND context LIKE ? LIMIT 1",
                [pipeline, alert_marker],
            ).fetchone()
            if alert_exists is None:
                quarantine_alert = {
                    "source_schema": source_schema,
                    "source_table": source_table,
                    "target_table": target_table,
                    "refusal_class": canonical_class,
                    "refusal_fingerprint": fingerprint,
                    "resnapshot_required": True,
                }
            result = REFUSAL_QUARANTINED
            con.execute("COMMIT")
        else:
            SCHEMA_REFUSAL.check(before, REFUSAL_PENDING)
            con.execute(
                f"INSERT OR REPLACE INTO {_control_table(control_schema, 'schema_refusals')} "
                "(pipeline, source_schema, source_table, target_table, detected_lsn, "
                "reason, refusal_fingerprint, source_fingerprint, refusal_class, "
                "state, refused_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    pipeline, source_schema, source_table, target_table, detected_lsn,
                    reason, fingerprint, source_fingerprint, canonical_class,
                    REFUSAL_PENDING, now(),
                ],
            )
            _ensure_awaiting_snapshot(
                con,
                pipeline=pipeline,
                source_schema=source_schema,
                source_table=source_table,
                target_table=target_table,
                reason="a schema/value refusal made the destination image stale",
                control_schema=control_schema,
            )
            existing_event = con.execute(
                f"SELECT 1 FROM {_control_table(control_schema, 'table_events')} "
                "WHERE pipeline = ? AND commit_id = 0 AND event = 'schema_refusal' "
                "AND source_schema = ? AND source_table = ?",
                [pipeline, source_schema, source_table],
            ).fetchone()
            if existing_event is None:
                write_table_event(
                    con,
                    pipeline=pipeline,
                    commit_id=0,
                    seq=_next_table_event_seq(
                        con, pipeline=pipeline, control_schema=control_schema
                    ),
                    event="schema_refusal",
                    source_schema=source_schema,
                    source_table=source_table,
                    target_table=target_table,
                    applied=False,
                    lsn=detected_lsn,
                    detail=reason,
                    control_schema=control_schema,
                )
            con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise

    # This is deliberately outside the transaction/rollback handler.  Alerting is
    # diagnostic; a sink failure must never turn a committed quarantine into a second
    # refusal or mask the durable state transition.
    if quarantine_alert is not None:
        raise_alert(
            con,
            pipeline=pipeline,
            severity="critical",
            code="schema_table_quarantined",
            message=(
                f"{source_schema}.{source_table} is quarantined after a repeated "
                "schema/value refusal; its destination image is stale/unavailable "
                "until a full resnapshot completes"
            ),
            context=quarantine_alert,
            control_schema=control_schema,
        )
        log.error(
            "quarantined %s.%s after an identical durable input refused twice",
            source_schema,
            source_table,
        )
    return result


def pending_schema_refusals(
    con, pipeline: str, *, control_schema: str | None = None
) -> list[tuple]:
    return con.execute(
        f"SELECT source_schema, source_table, reason FROM "
        f"{_control_table(control_schema, 'schema_refusals')} "
        "WHERE pipeline = ? AND state = ? ORDER BY source_schema, source_table",
        [pipeline, REFUSAL_PENDING],
    ).fetchall()


def quarantined_tables(
    con, pipeline: str, *, control_schema: str | None = None
) -> set[str]:
    """Return durable table identities whose refusal will not be retried."""
    return {
        f"{schema}.{table}"
        for schema, table in con.execute(
            f"SELECT source_schema, source_table FROM "
            f"{_control_table(control_schema, 'schema_refusals')} "
            "WHERE pipeline = ? AND state = ?",
            [pipeline, REFUSAL_QUARANTINED],
        ).fetchall()
    }


def blocked_schema_tables(
    con, pipeline: str, *, control_schema: str | None = None
) -> set[str]:
    """Return quarantined tables whose ordinary CDC admission is permanently held.

    ``pending`` is intentionally absent.  A pending refusal must be retried so the
    same durable origin can either succeed after repair or take the declared
    ``pending -> quarantined`` edge.  Once quarantined, the full current-source
    resnapshot is the only re-entry path and streaming rows are skipped safely.
    """
    blocked = {
        f"{schema}.{table}"
        for schema, table in con.execute(
            f"SELECT source_schema, source_table FROM "
            f"{_control_table(control_schema, 'schema_refusals')} "
            "WHERE pipeline = ? AND state = ?",
            [pipeline, REFUSAL_QUARANTINED],
        ).fetchall()
    }
    blocked.update(
        f"{schema}.{table}"
        for schema, table in con.execute(
            f"SELECT source_schema, source_table FROM "
            f"{_control_table(control_schema, 'table_state')} "
            "WHERE pipeline = ? AND snapshot_state = ?",
            [pipeline, table_lifecycle.GONE],
        ).fetchall()
    )
    return blocked


def quarantine_retry_allowed(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    source_exists: bool,
    source_fingerprint: str | None,
    control_schema: str | None = None,
) -> bool:
    """Return whether an observable source change may re-enter quarantine.

    A quarantined table is not retried merely because another run started.  The only
    automatic triggers are positive source absence (the drop policy must discharge it)
    or a changed relation/descriptor fingerprint.  An unavailable catalog read is
    deliberately not a trigger.
    """
    row = con.execute(
        f"SELECT state, source_fingerprint FROM "
        f"{_control_table(control_schema, 'schema_refusals')} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchone()
    if row is None or str(row[0]) != REFUSAL_QUARANTINED:
        return False
    if not source_exists:
        return True
    if not source_fingerprint:
        return False
    if row[1] is None:
        # A source read that successfully produced a fingerprint is positive repair
        # evidence even when the original refusal recorded no source fingerprint.
        return True
    return str(source_fingerprint) != str(row[1])


def reactivate_schema_refusal(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str | None = None,
    control_schema: str | None = None,
) -> bool:
    """Move an already-authorized quarantine back to pending for a full resnapshot.

    This is the declared ``quarantined -> pending`` trigger.  It durably preserves
    ``table_state=awaiting_snapshot`` before any source read, so a slot advance can
    never make the table appear current without the later snapshot swap/empty proof.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        row = con.execute(
            f"SELECT state FROM {_control_table(control_schema, 'schema_refusals')} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [pipeline, source_schema, source_table],
        ).fetchone()
        if row is None or str(row[0]) != REFUSAL_QUARANTINED:
            con.execute("COMMIT")
            return False
        SCHEMA_REFUSAL.check(REFUSAL_QUARANTINED, REFUSAL_PENDING)
        con.execute(
            f"UPDATE {_control_table(control_schema, 'schema_refusals')} "
            "SET state = ?, refused_at = ? "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [REFUSAL_PENDING, now(), pipeline, source_schema, source_table],
        )
        _ensure_awaiting_snapshot(
            con,
            pipeline=pipeline,
            source_schema=source_schema,
            source_table=source_table,
            target_table=target_table,
            reason="a quarantined refusal was automatically reactivated for a full resnapshot",
            control_schema=control_schema,
        )
        existing_event = con.execute(
            f"SELECT 1 FROM {_control_table(control_schema, 'table_events')} "
            "WHERE pipeline = ? AND commit_id = 0 AND event = 'schema_reactivation' "
            "AND source_schema = ? AND source_table = ?",
            [pipeline, source_schema, source_table],
        ).fetchone()
        if existing_event is None:
            write_table_event(
                con,
                pipeline=pipeline,
                commit_id=0,
                seq=_next_table_event_seq(
                    con, pipeline=pipeline, control_schema=control_schema
                ),
                event="schema_reactivation",
                source_schema=source_schema,
                source_table=source_table,
                target_table=target_table,
                applied=False,
                lsn=None,
                detail=(
                    "blocking condition may have cleared; a complete current-source "
                    "resnapshot is now required before resolution"
                ),
                control_schema=control_schema,
            )
        con.execute("COMMIT")
        log.warning(
            "reactivated quarantined table %s.%s for an automatic full resnapshot",
            source_schema,
            source_table,
        )
        return True
    except BaseException:
        con.execute("ROLLBACK")
        raise


def resolve_schema_refusal(
    con, *, pipeline: str, source_schema: str, source_table: str,
    control_schema: str | None = None,
) -> bool:
    """Discharge a refusal only after a complete replacement image is durable.

    The caller owns the surrounding transaction.  Keeping this transition beside the
    refusal writer makes the error obligation explicit: a successful snapshot swaps or
    verifies-empty the destination and resolves the refusal in the same MotherDuck
    transaction, so a crash cannot publish one half of that pair.
    """
    row = con.execute(
        f"SELECT state FROM {_control_table(control_schema, 'schema_refusals')} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchone()
    if row is None:
        return False
    before = str(row[0])
    SCHEMA_REFUSAL.check(before, REFUSAL_RESOLVED)
    if before == REFUSAL_RESOLVED:
        return False
    lifecycle = table_lifecycle.read(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        control_schema=control_schema,
    )
    if lifecycle not in {table_lifecycle.COMPLETE, table_lifecycle.GONE}:
        raise RuntimeError(
            f"cannot resolve schema refusal for {source_schema}.{source_table} "
            f"while table lifecycle is {lifecycle!r}; a completed full resnapshot "
            "must publish current data first, or the source must be positively "
            "discharged as gone"
        )
    con.execute(
        f"UPDATE {_control_table(control_schema, 'schema_refusals')} SET state = ? "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [REFUSAL_RESOLVED, pipeline, source_schema, source_table],
    )
    return True


def forget_table_state(
    con, *, pipeline: str, source_schema: str, source_table: str, alerts=None,
    control_schema: str | None = None,
) -> None:
    """The source relation is gone: `TableLifecycle -> absent` (rubric 1.9)."""
    table_lifecycle.forget(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        reason="the source relation was dropped (rubric 1.5)",
        alerts=alerts,
        control_schema=control_schema,
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

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.pipeline = pipeline
        self.control_schema = control_schema
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
                f"INSERT INTO {_control_table(self.control_schema, 'alerts')} "
                "(pipeline, raised_at, severity, code, message, context) VALUES (?,?,?,?,?,?)",
                [self.pipeline, now(), severity, code, message,
                 json.dumps(payload, default=str) if payload else None],
            )
        except Exception:  # pragma: no cover - alerting must never mask the cause
            log.warning("could not write alert %s", code, exc_info=True)
            return False
        log.warning("ALERT %s/%s: %s", severity, code, message)
        return self.independent

    def request_snapshot(
        self, *, pipeline: str, schema: str, table: str, target: str
    ) -> bool:
        """Mark a table `awaiting_snapshot` so the request OUTLIVES a rolled-back group.

        Rubric 4.7. The one caller is `AmbiguousDelete`: the group that could not be
        folded must roll back (never commit a guess), and the request to rebuild the
        table must survive that rollback or the next run replays into the same ambiguity
        for ever. Same connection as the alerts, for the same reason and with the same
        verified property: an INSERT on `con.cursor()` survives the parent connection's
        ROLLBACK.

        Returns False when there is no independent connection, in which case the request
        would be discarded with the group and saying so is the honest outcome.
        """
        if not self.independent or self._sink is None:
            log.error(
                "cannot record a re-snapshot request for %s.%s outside the transaction; "
                "it would be discarded with the rolled-back group",
                schema, table,
            )
            return False
        try:
            request_snapshot(
                self._sink,
                pipeline=pipeline,
                tables=[(schema, table, target)],
                detail=f"AmbiguousDelete on {schema}.{table} (rubric 4.7 self-heal)",
                control_schema=self.control_schema,
            )
        except Exception:  # pragma: no cover - never mask the original failure
            log.warning("could not record the re-snapshot request", exc_info=True)
            return False
        return True

    def close(self) -> None:
        if self._sink is not None:
            with contextlib.suppress(Exception):
                self._sink.close()
            self._sink = None


def raise_alert(
    con, *, pipeline: str, severity: str, code: str, message: str, context=None,
    control_schema: str | None = None,
):
    """One-shot alert on a connection the caller owns.

    Kept for callers outside a commit group (start-up, shutdown), where the
    connection has no open transaction and a separate one buys nothing. Anything
    inside a commit group must use `AlertSink`.
    """
    try:
        con.execute(
            f"INSERT INTO {_control_table(control_schema, 'alerts')} "
            "(pipeline, raised_at, severity, code, message, context) VALUES (?,?,?,?,?,?)",
            [pipeline, now(), severity, code, message,
             json.dumps(context, default=str) if context else None],
        )
    except Exception:  # pragma: no cover - alerting must never mask the cause
        log.warning("could not write alert %s", code, exc_info=True)


def read_slot_state(
    con, pipeline: str, slot_name: str, *, control_schema: str | None = None
) -> dict | None:
    """The last recorded observation of this pipeline's slot, or None (rubric 1.8)."""
    rows = con.execute(
        f"SELECT system_identifier, timeline_id, restart_lsn, confirmed_flush_lsn, "
        f"       current_wal_lsn, durable_lsn, observed_at, verdict, verdict_message, "
        f"       verdict_at "
        f"FROM {_control_table(control_schema, 'slot_state')} "
        "WHERE pipeline = ? AND slot_name = ?",
        [pipeline, slot_name],
    ).fetchall()
    if not rows:
        return None
    keys = (
        "system_identifier", "timeline_id", "restart_lsn", "confirmed_flush_lsn",
        "current_wal_lsn", "durable_lsn", "observed_at", "verdict", "verdict_message",
        "verdict_at",
    )
    return dict(zip(keys, rows[0], strict=True))


def write_slot_state(
    con,
    *,
    pipeline: str,
    slot_name: str,
    observation: dict,
    verdict: str | None = None,
    verdict_message: str | None = None,
    control_schema: str | None = None,
) -> None:
    """Record what the slot and the source cluster look like now (rubric 1.8).

    DELETE + INSERT **in one transaction**. It used to be two autocommitted statements,
    so a crash between them destroyed the only previous observation and silently shrank
    the next acquisition's detectable set - `slot_recreated` and `source_identity_changed`
    both need memory to fire at all (Codex M6 / Opus MINOR-7). Called on its own, never
    inside a commit group: see the DDL comment.

    The **verdict** goes in the same transaction as the observation it was computed from
    (Codex r1 MAJOR-5): "why did this state machine begin" was previously answerable only
    from `last_run.json` on whichever host happened to run, so the destination could not
    explain its own rebuild. Validated through `machines.SLOT_VERDICTS`.
    """
    if verdict is not None:
        verdict = SLOT_VERDICTS.parse(verdict)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"DELETE FROM {_control_table(control_schema, 'slot_state')} "
            "WHERE pipeline = ? AND slot_name = ?",
            [pipeline, slot_name],
        )
        con.execute(
            f"INSERT INTO {_control_table(control_schema, 'slot_state')} "
            "(pipeline, slot_name, system_identifier, timeline_id, restart_lsn, "
            " confirmed_flush_lsn, current_wal_lsn, durable_lsn, observed_at, "
            " verdict, verdict_message, verdict_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
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
                verdict,
                verdict_message,
                now() if verdict is not None else None,
            ],
        )
        con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise


def tables_awaiting_snapshot(
    con, pipeline: str, *, control_schema: str | None = None
) -> list[tuple[str, str, str]]:
    """`(source_schema, source_table, target_table)` for every table owed a snapshot.

    The queue rubric 1.6's re-snapshot works from and rubric 1.5's `recreated` action
    and rubric 1.8's recovery both write into. Ordered so a re-snapshot is
    deterministic and its logs are diffable.

    **The queue selects every NON-TERMINAL lifecycle state**, not only
    `awaiting_snapshot` (rubric 1.9). `in_progress` is durable and non-terminal, and
    selecting only the one value meant a table a hard crash left half-snapshotted was in
    no queue at all. `promote_interrupted_snapshots()` still runs at start-up and is
    still the right thing to do — it makes the state honest rather than merely
    selected — but the queue no longer *depends* on somebody having called it.
    """
    placeholders = ", ".join("?" for _ in SNAPSHOT_STATES_OWING_WORK)
    rows = con.execute(
        f"SELECT source_schema, source_table, target_table FROM "
        f"{_control_table(control_schema, 'table_state')} "
        f"WHERE pipeline = ? AND snapshot_state IN ({placeholders}) "
        "ORDER BY source_schema, source_table",
        [pipeline, *sorted(SNAPSHOT_STATES_OWING_WORK)],
    ).fetchall()
    return [(str(a), str(b), str(c)) for a, b, c in rows]


def read_snapshot_states(
    con, pipeline: str, *, control_schema: str | None = None
) -> dict[str, str]:
    """`"<schema>.<table>" -> snapshot_state`, VALIDATED against the frozen domain.

    A state outside the domain is a bug in whatever wrote it, and the honest response is
    a loud failure rather than a table that quietly belongs to no queue.
    """
    return table_lifecycle.read_all(con, pipeline, control_schema=control_schema)


def replacement_snapshot_is_current(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    relation,
    control_schema: str | None = None,
) -> bool:
    """Return whether a completed image is for this exact replacement relation.

    ``snapshot_state=complete`` alone is not enough: a baseline/recreate check can
    mark an old image complete while a new catalog fact is still due. The durable
    snapshot LSN and source relation generation together identify the completed
    replacement, so a merely marked ``complete`` state cannot suppress work.
    """
    row = con.execute(
        f"SELECT ts.snapshot_lsn, sr.relation_oid, sr.relation_filenode, "
        f"sr.relation_type_oid FROM {_control_table(control_schema, 'table_state')} ts "
        f"LEFT JOIN {_control_table(control_schema, 'source_relations')} sr "
        "ON sr.pipeline = ts.pipeline AND sr.source_schema = ts.source_schema "
        "AND sr.source_table = ts.source_table "
        "WHERE ts.pipeline = ? AND ts.source_schema = ? AND ts.source_table = ? "
        "AND ts.snapshot_state = ?",
        [pipeline, source_schema, source_table, table_lifecycle.COMPLETE],
    ).fetchone()
    if row is None or row[0] is None or row[1] != getattr(relation, "oid", None):
        return False
    for index, attribute in ((2, "relfilenode"), (3, "relation_type_oid")):
        observed = getattr(relation, attribute, None)
        if observed is not None and row[index] is not None and row[index] != observed:
            return False
    return True


def promote_interrupted_snapshots(
    con, pipeline: str, *, control_schema: str | None = None
) -> list[str]:
    """Turn every durable `in_progress` row into owed work. Call once, at start-up.

    `in_progress` is written the instant a table's first snapshot record arrives and is
    cleared only by the swap. It is durable and it is **not** terminal, and until this
    existed the only thing that recovered from it was the applier's
    `except BaseException` - a handler that `os._exit` (the fault injector, the commit
    watchdog) and `SIGKILL` both step straight over. The consequence was concrete: the
    recovery journal's "no table owes a snapshot any more" test could pass, and the run
    could log "recovery COMPLETE: every captured table has a fresh image", while a table
    sat half-snapshotted (architecture review, finding 1).

    At start-up nothing is mid-snapshot by definition, so `in_progress` can only mean a
    previous process died inside one. Promoting it to `awaiting_snapshot` is what makes
    that discoverable from durable state alone, after ANY crash.
    """
    names = table_lifecycle.transition_all(
        con,
        pipeline=pipeline,
        frm=SNAPSHOT_IN_PROGRESS,
        to=AWAITING_SNAPSHOT,
        reason="a previous process died inside this table's snapshot",
        control_schema=control_schema,
    )
    if names:
        log.warning(
            "%s table(s) were left mid-snapshot by an earlier process and are now marked "
            "awaiting_snapshot: %s", len(names), ", ".join(names),
        )
    return names


def request_snapshot(
    con, *, pipeline: str, tables: list[tuple[str, str, str]], detail: str,
    control_schema: str | None = None,
) -> int:
    """Mark tables as owing a snapshot. Returns how many `table_state` rows now say so.

    Idempotent: a table already `awaiting_snapshot` stays so. It deliberately does
    NOT touch the destination table - the data stays queryable, stale and flagged,
    until the re-snapshot swaps a complete image over it in one transaction.

    The return value counts rows **verified** to carry `awaiting_snapshot` afterwards.
    It used to increment once per input tuple whatever happened, so it returned
    `len(tables)` unconditionally and the test asserting on it restated its own
    configuration (Opus MINOR-1).
    """
    for index, (schema, table, target) in enumerate(tables):
        # One call: `absent -> awaiting_snapshot` (INSERT) and `x -> awaiting_snapshot`
        # (UPDATE) are the same declared edge set, and the machine picks the statement.
        table_lifecycle.transition(
            con,
            pipeline=pipeline,
            source_schema=schema,
            source_table=table,
            to=AWAITING_SNAPSHOT,
            reason=detail,
            target_table=target,
            control_schema=control_schema,
        )
        if index == 0:
            # rubric 1.7: the durable to-do list is **mid-write** — one table has taken
            # its lifecycle edge and the rest have not. The anchor used to fire before
            # the loop, which proves that a pre-write rollback is clean and nothing
            # about a partially-written queue (Codex r1 MAJOR-6). A crash here must
            # leave either "nothing is owed" or "these tables are owed" and never a
            # half-written queue that a journal claims to explain — which is why
            # `recovery.begin` wraps this and the journal INSERT in one transaction.
            faults.maybe_crash("table_rebuild_queued", _queueing())
    marked = 0
    for schema, table, _target in tables:
        rows = con.execute(
            f"SELECT snapshot_state FROM {_control_table(control_schema, 'table_state')} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [pipeline, schema, table],
        ).fetchall()
        if rows and str(rows[0][0]) == AWAITING_SNAPSHOT:
            marked += 1
    log.warning("marked %s table(s) as awaiting a snapshot: %s", marked, detail)
    return marked


def destination_holds_rows(
    con, *, dataset: str, tables: list[tuple[str, str, str]],
    control_schema: str | None = None,
) -> dict[str, int]:
    """`"<schema>.<table>" -> row count` for every captured table that EXISTS and is
    non-empty in the destination.

    The fact rubric 1.8's `no_durable_destination_row` cell was deciding without
    (Opus BLOCKER-2). Both the ADR and `RUBRIC_STATUS` describe that cell as
    "destination **empty**, slot positioned", and the code checked only that
    `_cdc_flight.debezium_offsets` had no row - so a healthy, fully populated
    destination whose control row had been lost was rebuilt from whatever source the
    DSN happened to name. Tables that do not exist are simply absent from the result.
    """
    held: dict[str, int] = {}
    for schema, table, target in tables:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [dataset, target],
        ).fetchone()[0]
        if not exists:
            continue
        try:
            count = con.execute(
                f"SELECT count(*) FROM {quote(dataset)}.{quote(target)}"
            ).fetchone()[0]
        except Exception:  # pragma: no cover - an unreadable table is not proof of empty
            log.warning("could not count %s.%s", dataset, target, exc_info=True)
            count = -1
        if count != 0:
            held[f"{schema}.{table}"] = int(count)
    return held


def mark_awaiting_snapshot(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    state: str,
    control_schema: str | None = None,
) -> None:
    """Record that a table's destination image cannot be trusted by CDC alone.

    Rubric 1.5 / Opus Q1. A `recreated` source relation means the destination table
    held a *different* relation's rows: keeping them presents pre-drop data as
    current, and dropping them and letting ordinary CDC re-create a partial table is
    worse still, because the destination then looks healthy while being silently
    incomplete. In either drop policy the row carries `snapshot_state` — the run
    summary and `inspect` surface it, and rubric 2.3/3.4's re-snapshot clears it. The
    catalog apply phase preserves the physical retained image in the same
    transaction as this transition. The replacement snapshot owns the later atomic
    swap, or the final source-missing policy owns any eventual destruction.
    """
    table_lifecycle.transition(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        to=state,
        reason="the source relation was replaced; the destination rows are a different relation's",
        target_table=target_table,
        # The row's identity is being re-established against a relation that is not the
        # one it described, so the snapshot bookkeeping goes with it.
        replace=True,
        control_schema=control_schema,
    )


def register_table(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    control_schema: str | None = None,
) -> None:
    """Persist source-to-destination ownership, inside the transaction that creates it.

    Codex 5: `table_state` is the canonical registry the catalog watcher seeds itself
    from, and it used to be written only by the snapshot coordinator. A table first
    materialised by streaming DML therefore had no durable row, so a `DROP TABLE`
    while the pipeline was down left an orphan destination table that no later poll
    could ever report — `_compare` skips a name it has no oid for and does not believe
    is ours. Written by whoever creates the table, whatever the origin.

    `absent -> none` and nothing else: a table that already has a row is already
    registered, and re-registering it would overwrite whatever lifecycle state it is
    genuinely in (a re-snapshot in flight, a rebuild owed) with "never snapshotted".
    """
    lifecycle = table_lifecycle.read(
        con, pipeline=pipeline, source_schema=source_schema, source_table=source_table,
        control_schema=control_schema,
    )
    if lifecycle == table_lifecycle.GONE:
        # A same-name replacement must first be discovered and routed through the
        # catalog/re-snapshot owner. Never let a stream row reopen a gone table.
        return
    if lifecycle != table_lifecycle.ABSENT:
        return
    table_lifecycle.transition(
        con,
        pipeline=pipeline,
        source_schema=source_schema,
        source_table=source_table,
        to=table_lifecycle.NONE,
        reason="a destination table was materialised for this relation",
        target_table=target_table,
        control_schema=control_schema,
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
    control_schema: str | None = None

    def acquire(self, con) -> None:
        rows = con.execute(
            f"SELECT owner_id, expires_at, host, pid FROM "
            f"{_control_table(self.control_schema, 'lease')} "
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
            f"SELECT owner_id FROM {_control_table(self.control_schema, 'lease')} "
            "WHERE pipeline = ?",
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
                    f"DELETE FROM {_control_table(self.control_schema, 'lease')} "
                    "WHERE pipeline = ?", [self.pipeline]
                )
                con.execute(
                    f"INSERT INTO {_control_table(self.control_schema, 'lease')} "
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
                f"DELETE FROM {_control_table(self.control_schema, 'lease')} "
                "WHERE pipeline = ? AND owner_id = ?",
                [self.pipeline, self.owner_id],
            )
        except Exception:  # pragma: no cover
            log.debug("could not release lease", exc_info=True)


# --------------------------------------------------------------------------- #
# ADR §14.1 — is DROP/RENAME transactional at this destination?
# --------------------------------------------------------------------------- #
def release_connection(con, *, timeout: float = 5.0) -> RetirementResult:
    """Close the destination connection under the canonical bounded protocol.

    The same protocol `RunPhaseWriter.close()` uses, one level out, and it is here for
    the same measured reason (Codex r6 MAJOR-1). Round 5 found the heartbeat *cursor*
    being closed under a live statement; round 6 found that bounding the cursor and then
    closing its **parent** one statement later is the identical unbounded wait — the
    reviewer drove the production ordering against a real serialized DuckDB sink and
    watched `RunPhaseWriter` retire correctly at 7.005 s while the process was still
    alive with no exit code at 12 s, stuck in this call. A bound on a child resource is
    not a bound on the process that closes its parent.

    So the close runs on a daemon thread and the run stops waiting for it. `abandoned`
    is a real outcome, not a failure: the handle dies with the process, `main()` gets to
    write `last_run.json`, and `shutdown_and_exit()` gets to deliver the exit code the
    run actually earned. A destination connection nobody can close is a wedged
    destination; refusing to *exit* over it turns an observability problem into an
    availability one.
    """
    return retire_handle(
        con,
        timeout=timeout,
        thread_name="cdc-destination-close",
        description="the destination connection",
    )


def probe_transactional_ddl(con, *, control_schema: str | None = None) -> bool:
    """Answer ADR 0001's biggest open question empirically, once per run.

    The shadow-table swap (D7) is `DROP` + `ALTER … RENAME` inside the commit
    group's transaction. If the destination does not honour that transactionally
    the swap has to fall back to `CREATE OR REPLACE TABLE … AS SELECT`, which the
    rubric explicitly allows ("BEGIN / COMMIT transactionality fine too"). The
    probe is a few milliseconds and removes a guess from the design.
    """
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
        # Transactional iff the rollback put both tables back.
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name LIKE '__ddl_probe%'",
            [resolve_control_schema(control_schema)],
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
