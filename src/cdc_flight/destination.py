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
from .errors import (  # noqa: F401
    CANONICAL_REFUSAL_CLASS,
    DestinationIdentityCollision,
    OffsetUnusable,
)
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
from .occurrence import OffsetRowState, _offset_row_receipt_from_durable

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
def connect(
    dest, *, read_only: bool = False, create_database: bool = True
) -> Any:
    """Open the one destination connection the applier writes through."""
    import duckdb

    if dest.kind == "duckdb":
        dest.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(
            str(dest.duckdb_path), config=DUCKDB_CONNECT_CONFIG, read_only=read_only
        )
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
                if not create_database:
                    raise RuntimeError(
                        f"MotherDuck destination database {dest.motherduck_database!r} "
                        "does not exist; a service Flight will not create a database "
                        "before it owns a fencing epoch"
                    )
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
            read_only=read_only,
        )
        assert_runtime_capabilities(con)
        return con

    raise ValueError(f"unknown destination {dest.kind!r} (expected duckdb|motherduck)")


def ensure_dataset(con, dataset: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(dataset)}")


def now() -> datetime:
    return datetime.now(UTC)


def _committed_row_matches(con, query: str, params, observed_row) -> bool:
    """Prove that an observed row is in the committed database snapshot.

    DuckDB's public ``cursor()`` API creates an independent connection/transaction.
    It therefore cannot see writes that are still uncommitted on ``con``.  Read-side
    receipt issuers use this helper with the complete row they observed: a receipt is
    issued only when the independent snapshot returns that exact row.  Merely asking
    the caller's connection to read again would validate its own uncommitted view and
    would not establish durability.
    """
    independent = None
    try:
        independent = con.cursor()
        committed_rows = independent.execute(query, params).fetchall()
        return len(committed_rows) == 1 and tuple(committed_rows[0]) == tuple(observed_row)
    except Exception:
        log.warning("could not verify a read-side row through an independent snapshot", exc_info=True)
        return False
    finally:
        if independent is not None:
            try:
                independent.close()
            except Exception:  # pragma: no cover - cursor cleanup is best effort
                log.debug("could not close independent durability cursor", exc_info=True)


# --------------------------------------------------------------------------- #
# resume point I/O
# --------------------------------------------------------------------------- #
def read_resume_point(
    con, pipeline: str, namespace: str, *, control_schema: str | None = None
) -> ResumePoint | None:
    rows = con.execute(
        f"SELECT resume_json, commit_id, last_lsn, last_txn_id, last_total_order, "
        f"       snapshot_epoch, updated_at, offset_blob, offset_key_blob "
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
        updated_at,
        _blob,
        _key_blob,
    ) = rows[0]
    committed_query = (
        f"SELECT resume_json, commit_id, last_lsn, last_txn_id, last_total_order, "
        f"       snapshot_epoch, updated_at, offset_blob, offset_key_blob "
        f"FROM {_control_table(control_schema, 'debezium_offsets')} "
        "WHERE pipeline = ? AND namespace = ?"
    )
    committed = _committed_row_matches(
        con, committed_query, [pipeline, namespace], rows[0]
    )
    offset_row = OffsetRowState(
        pipeline=pipeline,
        namespace=namespace,
        resume_json=str(resume_json),
        commit_id=int(commit_id or 0),
        snapshot_epoch=int(snapshot_epoch or 0),
        last_lsn=int(last_lsn or 0),
        updated_at=updated_at,
    )
    try:
        point = ResumePoint.from_json(resume_json)
    except OffsetUnusable as exc:
        offset_receipt = (
            _offset_row_receipt_from_durable(offset_row) if committed else None
        )
        raise OffsetUnusable(
            f"durable resume point for pipeline={pipeline!r}, namespace={namespace!r} "
            f"is unusable: {exc}",
            offset_row=offset_receipt,
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


def write_delete_ledger(
    con,
    item,
    *,
    pipeline: str,
    commit_id: int,
    control_schema: str | None = None,
) -> None:
    """Persist hard/soft delete effects beside the row mutation transaction."""
    effects = dict(getattr(item, "delete_effects", {}) or {})
    if not effects:
        return

    from .apply_sql import BIGINT, VARCHAR, bulk_insert

    rows = []
    for event_id, operation in effects.items():
        identity_digest = delete_identity_digest(operation)
        effect_digest = delete_effect_digest(operation)
        rows.append(
            [
                pipeline,
                item.target,
                event_id,
                item.source_schema,
                item.source_table,
                operation.source_lsn,
                operation.txn_id,
                operation.total_order,
                operation.delete_mode or getattr(item, "delete_mode", "hard"),
                int(getattr(item, "delete_policy_epoch", 1) or 1),
                getattr(item, "delete_policy_digest", None),
                identity_digest,
                "applied",
                effect_digest,
                commit_id,
                now(),
            ]
        )
    # A replay normally returns before this point through claim_delete_ledger.  The
    # second read is intentional: it closes the race between an older caller and
    # this writer and turns a same-ID/different-effect collision into a refusal
    # instead of an opaque primary-key error.
    for event_id, operation in effects.items():
        if claim_delete_ledger(
            con,
            pipeline=pipeline,
            target_table=item.target,
            event_id=event_id,
            source_schema=item.source_schema,
            source_table=item.source_table,
            source_lsn=operation.source_lsn,
            txn_id=operation.txn_id,
            total_order=operation.total_order,
            delete_mode=operation.delete_mode or getattr(item, "delete_mode", "hard"),
            policy_epoch=int(getattr(item, "delete_policy_epoch", 1) or 1),
            policy_digest=getattr(item, "delete_policy_digest", None),
            identity_digest=delete_identity_digest(operation),
            effect_digest=delete_effect_digest(operation),
            control_schema=control_schema,
        ):
            rows = [row for row in rows if row[2] != event_id]
    if not rows:
        return
    bulk_insert(
        con,
        _control_table(control_schema, "delete_ledger"),
        [
            "pipeline", "target_table", "event_id", "source_schema", "source_table",
            "source_lsn", "txn_id", "total_order", "delete_mode", "policy_epoch",
            "policy_digest", "identity_digest", "effect_state", "effect_digest",
            "applied_commit_id", "applied_at",
        ],
        rows,
        [
            VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT,
            VARCHAR, BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, "TIMESTAMPTZ",
        ],
    )


def delete_identity_digest(operation) -> str:
    """Return a value-free digest of the sanitized source identity image."""
    import hashlib

    source_digest = getattr(operation, "image_digest", None) or ""
    return hashlib.sha256(
        f"delete-identity\x00{getattr(operation, 'operation', 'd')}\x00{source_digest}".encode()
    ).hexdigest()


def delete_effect_digest(operation) -> str:
    """Return a value-free digest of the mode-specific delete effect."""
    import hashlib

    material = "\x00".join(
        (
            getattr(operation, "operation", "d"),
            getattr(operation, "delete_mode", "hard") or "hard",
            getattr(operation, "image_digest", None) or "",
        )
    )
    return hashlib.sha256(f"delete-effect\x00{material}".encode()).hexdigest()


def claim_delete_ledger(
    con,
    *,
    pipeline: str,
    target_table: str,
    event_id: str,
    source_schema: str | None,
    source_table: str | None,
    source_lsn: int | None,
    txn_id: str | None,
    total_order: int | None,
    delete_mode: str,
    policy_epoch: int,
    policy_digest: str | None,
    identity_digest: str,
    effect_digest: str,
    control_schema: str | None = None,
) -> bool:
    """Fence a delete before physical DML when its durable effect already exists."""
    columns = (
        "source_schema, source_table, source_lsn, txn_id, total_order, delete_mode, "
        "policy_epoch, policy_digest, identity_digest, effect_state, effect_digest"
    )
    row = con.execute(
        f"SELECT {columns} FROM {_control_table(control_schema, 'delete_ledger')} "
        "WHERE pipeline = ? AND target_table = ? AND event_id = ?",
        [pipeline, target_table, event_id],
    ).fetchone()
    if row is None:
        return False
    expected = (
        source_schema,
        source_table,
        source_lsn,
        txn_id,
        total_order,
        delete_mode,
        int(policy_epoch),
        policy_digest,
        identity_digest,
        "applied",
        effect_digest,
    )
    names = (
        "source_schema", "source_table", "source_lsn", "txn_id", "total_order",
        "delete_mode", "policy_epoch", "policy_digest", "identity_digest",
        "effect_state", "effect_digest",
    )
    for name, observed, wanted in zip(names, row, expected, strict=True):
        if observed != wanted:
            raise DestinationIdentityCollision(
                f"delete ledger identity collision for event {event_id!r}: {name} differs",
                source_schema=source_schema,
                source_table=source_table,
                target=target_table,
            )
    return True


def write_table_policy_state(
    con,
    item,
    *,
    pipeline: str,
    target_table: str | None = None,
    control_schema: str | None = None,
) -> None:
    """Persist the effective delete/PII policy epoch for a materialized relation."""
    if not item.source_schema or not item.source_table:
        return
    table = _control_table(control_schema, "table_state")
    values = [
        target_table or item.target,
        getattr(item, "delete_mode", "hard"),
        int(getattr(item, "delete_policy_epoch", 1) or 1),
        getattr(item, "delete_policy_digest", None),
        int(getattr(item, "policy_epoch", 0) or 0),
        getattr(item, "policy_digest", None),
        getattr(item, "pii_salt_id", None),
    ]
    # Do not use INSERT OR REPLACE here: DuckDB implements REPLACE as a delete plus
    # insert, which would reset refresh/history/snapshot state that belongs to the
    # catalog and lifecycle owners.  Policy state is an additive update to the
    # durable relation row, in the same transaction as its data effect.
    con.execute(
        f"UPDATE {table} SET target_table = ?, delete_mode = ?, "
        "delete_policy_epoch = ?, delete_policy_digest = ?, pii_policy_epoch = ?, "
        "pii_policy_digest = ?, pii_salt_id = ? "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [*values, pipeline, item.source_schema, item.source_table],
    )
    present = con.execute(
        f"SELECT 1 FROM {table} WHERE pipeline = ? AND source_schema = ? "
        "AND source_table = ?",
        [pipeline, item.source_schema, item.source_table],
    ).fetchone()
    if present is None:
        con.execute(
            f"INSERT INTO {table} "
            "(pipeline, source_schema, source_table, target_table, delete_mode, "
            "delete_policy_epoch, delete_policy_digest, pii_policy_epoch, "
            "pii_policy_digest, pii_salt_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                pipeline,
                item.source_schema,
                item.source_table,
                *values,
            ],
        )


def read_table_policy_state(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    control_schema: str | None = None,
) -> dict[str, Any] | None:
    """Read the last committed policy state for one source relation."""
    row = con.execute(
        f"SELECT target_table, delete_mode, delete_policy_epoch, "
        f"delete_policy_digest, pii_policy_epoch, pii_policy_digest, pii_salt_id "
        f"FROM {_control_table(control_schema, 'table_state')} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchone()
    if row is None:
        return None
    names = (
        "target_table", "delete_mode", "delete_policy_epoch", "delete_policy_digest",
        "pii_policy_epoch", "pii_policy_digest", "pii_salt_id",
    )
    return dict(zip(names, row, strict=True))


def write_policy_alerts(
    con,
    alerts: list[dict],
    *,
    pipeline: str,
    control_schema: str | None = None,
) -> None:
    """Write value-free policy governance alerts inside the data transaction."""
    if not alerts:
        return
    from .apply_sql import BIGINT, VARCHAR, bulk_insert

    rows = []
    for alert in alerts:
        rows.append(
            [
                pipeline,
                alert.get("source_schema") or "",
                alert.get("source_table") or "",
                alert.get("target_table"),
                alert.get("column") or "",
                alert.get("action") or "exclude",
                alert.get("rule_id"),
                int(alert.get("policy_epoch", 0) or 0),
                alert.get("policy_digest") or "",
                alert.get("event_id"),
                alert.get("source_lsn"),
                alert.get("code") or "unmatched_column",
                now(),
            ]
        )
    bulk_insert(
        con,
        _control_table(control_schema, "policy_alerts"),
        [
            "pipeline", "source_schema", "source_table", "target_table", "column_name",
            "action", "rule_id", "policy_epoch", "policy_digest", "event_id",
            "source_lsn", "code", "raised_at",
        ],
        rows,
        [
            VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT,
            VARCHAR, VARCHAR, BIGINT, VARCHAR, "TIMESTAMPTZ",
        ],
        replace=True,
    )


def read_event_ledger(
    con,
    *,
    pipeline: str,
    target_table: str,
    event_id: str,
    control_schema: str | None = None,
) -> dict[str, Any] | None:
    """Read one shared event-ledger row from the caller's open transaction."""
    columns = (
        "operation, payload_digest, state, source_schema, source_table, "
        "source_cluster_id, source_timeline, relation_generation, source_lsn, "
        "commit_lsn, txn_id, total_order, key_guard_digest, policy_epoch, "
        "policy_digest, delete_mode, snapshot_epoch, applied_at"
    )
    row = con.execute(
        f"SELECT {columns} FROM {_control_table(control_schema, 'event_ledger')} "
        "WHERE pipeline = ? AND target_table = ? AND event_id = ?",
        [pipeline, target_table, event_id],
    ).fetchone()
    if row is None:
        return None
    names = (
        "operation", "payload_digest", "state", "source_schema", "source_table",
        "source_cluster_id", "source_timeline", "relation_generation", "source_lsn",
        "commit_lsn", "txn_id", "total_order", "key_guard_digest", "policy_epoch",
        "policy_digest", "delete_mode", "snapshot_epoch", "applied_at",
    )
    return {
        "pipeline": pipeline,
        "target_table": target_table,
        "event_id": event_id,
        **dict(zip(names, row, strict=True)),
    }


class EventLedgerBatch:
    """Claim event identities with bounded reads and one bulk insert per plan.

    Snapshot callbacks can contain tens of thousands of rows.  The old claim path
    issued a destination SELECT and INSERT for every row, which made the callback
    itself the long-lived owner of the JPype bridge and prevented supervisor
    shutdown from reaching its quiescence proof.  This cache keeps the exact
    collision checks in Python for the duration of the open destination
    transaction, then inserts all newly claimed rows before any table materializer
    runs.  A concurrent primary-key conflict still fails closed.
    """

    _COLUMNS = (
        "pipeline", "target_table", "event_id", "operation", "payload_digest",
        "state", "source_schema", "source_table", "source_cluster_id",
        "source_timeline", "relation_generation", "source_lsn", "commit_lsn",
        "txn_id", "total_order", "key_guard_digest", "policy_epoch",
        "policy_digest", "delete_mode", "snapshot_epoch", "applied_at",
    )
    _READ_COLUMNS = (
        "operation, payload_digest, state, source_schema, source_table, "
        "source_cluster_id, source_timeline, relation_generation, source_lsn, "
        "commit_lsn, txn_id, total_order, key_guard_digest, policy_epoch, "
        "policy_digest, delete_mode, snapshot_epoch, applied_at"
    )
    _READ_NAMES = (
        "operation", "payload_digest", "state", "source_schema", "source_table",
        "source_cluster_id", "source_timeline", "relation_generation", "source_lsn",
        "commit_lsn", "txn_id", "total_order", "key_guard_digest", "policy_epoch",
        "policy_digest", "delete_mode", "snapshot_epoch", "applied_at",
    )

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = control_schema
        self._known: dict[tuple[str, str], object] = {}
        self._loaded_targets: set[str] = set()
        #: Streaming identities carry the source transaction id.  Loading only that
        #: transaction keeps replay checks exact without turning a long-lived
        #: pipeline into an unbounded event-ledger cache.
        self._loaded_transactions: set[tuple[str, str]] = set()
        self._pending: list[list[Any]] = []

    def _row_from_values(self, target_table: str, event_id: str, row) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "target_table": target_table,
            "event_id": event_id,
            **dict(zip(self._READ_NAMES, row, strict=True)),
        }

    def _load_target(self, target_table: str) -> None:
        if target_table in self._loaded_targets:
            return
        rows = self.con.execute(
            f"SELECT event_id, {self._READ_COLUMNS} FROM "
            f"{_control_table(self.control_schema, 'event_ledger')} "
            "WHERE pipeline = ? AND target_table = ?",
            [self.pipeline, target_table],
        ).fetchall()
        for row in rows:
            event_id = str(row[0])
            self._known[(target_table, event_id)] = self._row_from_values(
                target_table, event_id, row[1:]
            )
        self._loaded_targets.add(target_table)

    def _load_transaction(self, target_table: str, txn_id: str) -> None:
        """Read committed claims for one source transaction into the plan cache."""
        cache_key = (target_table, str(txn_id))
        if cache_key in self._loaded_transactions:
            return
        rows = self.con.execute(
            f"SELECT event_id, {self._READ_COLUMNS} FROM "
            f"{_control_table(self.control_schema, 'event_ledger')} "
            "WHERE pipeline = ? AND target_table = ? AND txn_id = ?",
            [self.pipeline, target_table, txn_id],
        ).fetchall()
        for row in rows:
            event_id = str(row[0])
            self._known[(target_table, event_id)] = self._row_from_values(
                target_table, event_id, row[1:]
            )
        self._loaded_transactions.add(cache_key)

    def prefetch_transactions(self, pairs: list[tuple[str, str]]) -> None:
        """Load a bounded set of streaming transactions with batched reads.

        A high-TPS source commonly emits many one-row PostgreSQL transactions.
        The identity check still happens before folding and the ledger insert still
        happens in this destination transaction, but a plan can discover existing
        claims with a bounded number of indexed reads instead of one round trip per
        source transaction.  The caller supplies only the current whole-unit group,
        so this cache cannot become a process-lifetime event-ledger index.
        """
        grouped: dict[str, set[str]] = {}
        for target_table, txn_id in pairs:
            if txn_id is None:
                continue
            grouped.setdefault(str(target_table), set()).add(str(txn_id))
        batch_size = 1024
        for target_table, txn_ids in grouped.items():
            pending = sorted(
                txn_id
                for txn_id in txn_ids
                if (target_table, txn_id) not in self._loaded_transactions
            )
            for start in range(0, len(pending), batch_size):
                batch = pending[start : start + batch_size]
                placeholders = ", ".join("?" for _ in batch)
                rows = self.con.execute(
                    f"SELECT event_id, {self._READ_COLUMNS} FROM "
                    f"{_control_table(self.control_schema, 'event_ledger')} "
                    "WHERE pipeline = ? AND target_table = ? "
                    f"AND txn_id IN ({placeholders})",
                    [self.pipeline, target_table, *batch],
                ).fetchall()
                for row in rows:
                    event_id = str(row[0])
                    self._known[(target_table, event_id)] = self._row_from_values(
                        target_table, event_id, row[1:]
                    )
                self._loaded_transactions.update(
                    (target_table, txn_id) for txn_id in batch
                )

    @staticmethod
    def _pending_row(identity, *, pipeline: str, target_table: str, source_lsn: int | None):
        return [
            pipeline,
            target_table,
            identity.event_id,
            identity.operation,
            identity.payload_digest,
            "applied",
            identity.source_schema,
            identity.source_table,
            identity.source_cluster_id,
            identity.source_timeline,
            identity.relation_generation,
            source_lsn,
            identity.commit_lsn,
            identity.txn_id,
            identity.total_order,
            identity.key_guard_digest,
            identity.policy_epoch,
            identity.policy_digest,
            identity.delete_mode,
            identity.snapshot_epoch,
            now(),
        ]

    def claim(self, identity, *, target_table: str, source_lsn: int | None = None,
              snapshot: bool = False) -> bool:
        """Return whether an exact identity was already applied."""
        from .event_ledger import assert_same_identity

        key = (target_table, identity.event_id)
        if snapshot:
            self._load_target(target_table)
        elif identity.txn_id is not None:
            self._load_transaction(target_table, str(identity.txn_id))
        observed = self._known.get(key)
        if observed is not None:
            if isinstance(observed, dict):
                try:
                    assert_same_identity(observed, identity)
                except DestinationIdentityCollision as collision:
                    collision.target = target_table
                    raise
                return str(observed["state"]) == "applied"
            # An identity already pending in this transaction has the same
            # semantics as an applied row once the transaction commits.  Check it
            # before suppressing the duplicate operation.
            try:
                assert_same_identity(observed, identity)
            except DestinationIdentityCollision as collision:
                collision.target = target_table
                raise
            return True

        if not snapshot and identity.txn_id is None:
            existing = read_event_ledger(
                self.con,
                pipeline=self.pipeline,
                target_table=target_table,
                event_id=identity.event_id,
                control_schema=self.control_schema,
            )
            if existing is not None:
                self._known[key] = existing
                try:
                    assert_same_identity(existing, identity)
                except DestinationIdentityCollision as collision:
                    collision.target = target_table
                    raise
                return str(existing["state"]) == "applied"

        # Store the identity-shaped mapping as the pending value so a repeated
        # event in one group receives the same collision validation as a durable
        # replay.  `as_dict()` contains every field checked by the oracle.
        pending = identity.as_dict()
        pending["state"] = "applied"
        self._known[key] = pending
        self._pending.append(
            self._pending_row(
                identity,
                pipeline=self.pipeline,
                target_table=target_table,
                source_lsn=source_lsn,
            )
        )
        return False

    def flush(self) -> None:
        """Insert pending claims before the plan's physical materializers run."""
        if not self._pending:
            return
        from .apply_sql import BIGINT, VARCHAR, bulk_insert

        types = [
            VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
            VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, VARCHAR, BIGINT, VARCHAR,
            BIGINT, VARCHAR, VARCHAR, BIGINT, "TIMESTAMPTZ",
        ]
        bulk_insert(
            self.con,
            _control_table(self.control_schema, "event_ledger"),
            list(self._COLUMNS),
            self._pending,
            types,
        )
        self._pending.clear()


def claim_event_ledger(
    con,
    identity,
    *,
    pipeline: str,
    target_table: str,
    source_lsn: int | None = None,
    control_schema: str | None = None,
    ledger: EventLedgerBatch | None = None,
    snapshot: bool = False,
) -> bool:
    """Claim an event in the current data transaction.

    Returns ``True`` when the exact event was already applied.  It is important
    that the existing-row read and a new-row insert happen on ``con``: this is
    deliberately not a receipt connection and deliberately not a second commit.
    """
    if ledger is not None:
        return ledger.claim(
            identity,
            target_table=target_table,
            source_lsn=source_lsn,
            snapshot=snapshot,
        )

    from .event_ledger import assert_same_identity

    existing = read_event_ledger(
        con,
        pipeline=pipeline,
        target_table=target_table,
        event_id=identity.event_id,
        control_schema=control_schema,
    )
    if existing is not None:
        try:
            assert_same_identity(existing, identity)
        except DestinationIdentityCollision as collision:
            collision.target = target_table
            raise
        return str(existing["state"]) == "applied"

    con.execute(
        f"INSERT INTO {_control_table(control_schema, 'event_ledger')} "
        "(pipeline, target_table, event_id, operation, payload_digest, state, "
        " source_schema, source_table, source_cluster_id, source_timeline, "
        " relation_generation, source_lsn, commit_lsn, txn_id, total_order, "
        " key_guard_digest, policy_epoch, policy_digest, delete_mode, "
        " snapshot_epoch, applied_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            pipeline,
            target_table,
            identity.event_id,
            identity.operation,
            identity.payload_digest,
            "applied",
            identity.source_schema,
            identity.source_table,
            identity.source_cluster_id,
            identity.source_timeline,
            identity.relation_generation,
            source_lsn,
            identity.commit_lsn,
            identity.txn_id,
            identity.total_order,
            identity.key_guard_digest,
            identity.policy_epoch,
            identity.policy_digest,
            identity.delete_mode,
            identity.snapshot_epoch,
            now(),
        ],
    )
    return False


# The destination coordinator keeps the stable public module surface while the
# state owners remain independently measurable.
from .destination_alerts import (  # noqa: E402, F401
    AlertSink,
    alert_identity_exists,
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
from .occurrence import (  # noqa: E402, F401
    CommitReservation,
    EpisodeReceipt,
    EpisodeState,
    LeaseReceipt,
    LeaseState,
    OccurrenceKey,
    OffsetRowReceipt,
    RecoveryGeneration,
    RecoveryJournalReceipt,
    RunState,
    SlotState,
    SlotStateReceipt,
)
