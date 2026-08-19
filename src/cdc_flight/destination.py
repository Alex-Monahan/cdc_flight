"""The destination connection and the `_cdc_flight` control schema (ADR 0001 §4.8).

One DuckDB/MotherDuck connection, owned by the applier, carrying both the data
and the state. Everything in this module is written **inside** the commit
group's transaction unless the docstring says otherwise, because principle (4)
- data commits and state commits are atomic with one another - is what the whole
design rests on.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from . import faults, table_lifecycle
from .config import resolve_control_schema, resolve_motherduck_database
from .control_schema import CONTROL_DDL, ensure_control_schema
from .errors import CANONICAL_REFUSAL_CLASS, OffsetUnusable  # noqa: F401
from .machines import (
    KEYLESS_EVENT,
    KEYLESS_EVENT_APPLIED,
    KEYLESS_EVENT_UNSEEN,
    LIFECYCLE_DURABLE_VALUES,
    REFUSAL_ABSENT,  # noqa: F401
    REFUSAL_PENDING,  # noqa: F401
    REFUSAL_QUARANTINED,  # noqa: F401
    REFUSAL_RESOLVED,  # noqa: F401
    SCHEMA_REFUSAL,  # noqa: F401
    SLOT_VERDICTS,  # noqa: F401
)
from .naming import control_table, quote

# Re-exported: `source_relations.py` is a split of this module, not a new dependency
# for its callers (Codex r3 MINOR / the destination ownership split).
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
        try:
            payload = json.loads(text)
            if not isinstance(payload, dict):
                raise ValueError("resume JSON must be an object")
            partition = payload.get("partition") or {}
            offset = payload.get("offset") or {}
            if not isinstance(partition, dict) or not isinstance(offset, dict):
                raise ValueError("resume partition and offset must be objects")
            last_lsn = int(payload.get("last_lsn") or 0)
            if last_lsn < 0:
                raise ValueError("resume last_lsn must be non-negative")
            total_order = payload.get("last_total_order")
            if total_order is not None:
                total_order = int(total_order)
            return cls(
                partition=partition,
                offset=offset,
                last_lsn=last_lsn,
                last_txn_id=payload.get("last_txn_id"),
                last_total_order=total_order,
                **extra,
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise OffsetUnusable(f"resume point is not usable JSON: {exc}") from exc


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
        bootstrap = duckdb.connect(f"md:?motherduck_token={token}", config=DUCKDB_CONNECT_CONFIG)
        try:
            assert_runtime_capabilities(bootstrap)
            # Resolve the account-level spelling before opening the database-specific
            # connection. This makes case/whitespace/quoting aliases share the same
            # MotherDuck local cache and the same physical catalog identity; the lease
            # resolver repeats the server query after schemas are ready.
            database = resolve_motherduck_database(bootstrap, dest.motherduck_database)
            if database is None:
                bootstrap.execute(
                    f"CREATE DATABASE IF NOT EXISTS {quote(dest.motherduck_database)}"
                )
                database = resolve_motherduck_database(
                    bootstrap, dest.motherduck_database
                )
            if database is None:
                raise RuntimeError(
                    f"MotherDuck did not resolve database {dest.motherduck_database!r}"
                )
        finally:
            bootstrap.close()
        con = duckdb.connect(
            f"md:{database}?motherduck_token={token}",
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
    try:
        point = ResumePoint.from_json(resume_json)
    except OffsetUnusable as exc:
        raise OffsetUnusable(
            f"durable resume point for pipeline={pipeline!r}, namespace={namespace!r} "
            f"is unusable: {exc}"
        ) from exc
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
    return (
        bytes(blob) if blob is not None else None,
        bytes(key_blob) if key_blob is not None else None,
    )


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
            pipeline,
            commit_id,
            seq,
            now(),
            event,
            source_schema,
            source_table,
            target_table,
            applied,
            lsn,
            txn_id,
            rows_removed,
            detail,
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

    normalized = [[*row, None] if len(row) == 4 else list(row) for row in rows]

    bulk_insert(
        con,
        _control_table(control_schema, "column_presence"),
        [
            "target_dataset",
            "target_table",
            "event_id",
            "column_name",
            "present",
            "patch_digest",
        ],
        normalized,
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, BOOLEAN, VARCHAR],
        replace=True,
    )


def read_keyless_event_state(
    con,
    *,
    pipeline: str,
    target_table: str,
    event_id: str,
    control_schema: str | None = None,
) -> str | None:
    """Read one keyless event's durable mutation state inside the group transaction."""
    row = con.execute(
        f"SELECT state FROM {_control_table(control_schema, 'keyless_events')} "
        "WHERE pipeline = ? AND target_table = ? AND event_id = ?",
        [pipeline, target_table, event_id],
    ).fetchone()
    if row is None:
        return None
    return KEYLESS_EVENT.parse(row[0])


def write_keyless_events(
    con,
    rows: list[tuple],
    *,
    pipeline: str,
    control_schema: str | None = None,
) -> None:
    """Persist keyless event transitions atomically with their physical rows."""
    if not rows:
        return
    from .apply_sql import VARCHAR, bulk_insert

    normalized: list[list[Any]] = []
    for target_table, event_id, operation, image_digest in rows:
        KEYLESS_EVENT.check(KEYLESS_EVENT_UNSEEN, KEYLESS_EVENT_APPLIED)
        normalized.append(
            [
                pipeline,
                target_table,
                event_id,
                operation,
                KEYLESS_EVENT_APPLIED,
                image_digest,
                now(),
            ]
        )
    bulk_insert(
        con,
        _control_table(control_schema, "keyless_events"),
        [
            "pipeline",
            "target_table",
            "event_id",
            "operation",
            "state",
            "image_digest",
            "applied_at",
        ],
        normalized,
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, "TIMESTAMPTZ"],
    )


# The destination coordinator keeps the stable public module surface while the
# state owners remain independently measurable.
from .destination_alerts import (  # noqa: E402, F401
    AlertSink,
    alert_marker_exists,
    destination_holds_rows,
    fallback_alert_path,
    mark_awaiting_snapshot,
    observe_source_health,
    persist_fallback_alert,
    promote_interrupted_snapshots,
    raise_alert,
    raise_alert_once,
    read_slot_state,
    read_snapshot_states,
    register_table,
    replacement_snapshot_is_current,
    replay_fallback_alerts,
    request_snapshot,
    tables_awaiting_snapshot,
    write_slot_state,
)
from .destination_lease import (  # noqa: E402, F401
    Lease,
    _is_dead,
    probe_transactional_ddl,
    release_connection,
)
from .destination_refusals import (  # noqa: E402, F401
    _ensure_awaiting_snapshot,
    _next_table_event_seq,
    _stable_refusal_fingerprint,
    blocked_schema_tables,
    forget_table_state,
    pending_schema_refusals,
    quarantine_retry_allowed,
    quarantined_tables,
    reactivate_schema_refusal,
    record_schema_refusal,
    resolve_schema_refusal,
    schema_refusal_state,
)
