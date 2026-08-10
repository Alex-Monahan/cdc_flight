"""Final source evidence and policy settlement for re-snapshot obligations."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from . import destination as dest_mod
from . import naming, table_lifecycle
from . import resnapshot_projection as projection
from .config import DROP_IGNORE, DROP_LOG, DROP_REPLICATE
from .naming import quote
from .resnapshot_projection import ProjectionEvent

log = logging.getLogger("cdc_flight.resnapshot")


@dataclass
class EmptinessEvidence:
    """The completion, row, WAL, and source-identity facts for one final check."""

    snapshot_phase_ended: bool
    tables_seen: set[str]
    source_empty_at: dict[str, int]
    wal_lsn: int | None
    source_missing: set[str] = field(default_factory=set)

    def verdict(self, qualified: str) -> tuple[bool, str]:
        if not self.snapshot_phase_ended:
            return False, (
                "the snapshot completion callbacks never proved the end of the whole "
                "capture set, so this table may simply not have been reached"
            )
        if qualified in self.tables_seen:
            return False, (
                "the engine produced snapshot records for this table but no shadow was "
                "swapped in, so the image is partial, not empty"
            )
        if qualified in self.source_missing:
            return False, "the source relation is absent, not an empty relation"
        if self.wal_lsn is None:
            return False, "no source WAL position was sampled for the emptiness check"
        count = self.source_empty_at.get(qualified)
        if count is None:
            return False, "the source row count for this table could not be read"
        if count != 0:
            return False, f"the source relation holds {count} row(s)"
        return True, ""


def gather_emptiness_evidence(
    dsn: str,
    *,
    pending: list[tuple[str, str, str]],
    snapshot_phase_ended: bool,
    tables_seen: set[str],
) -> EmptinessEvidence:
    """Sample source WAL, then read existence/counts in repeatable read."""
    if not pending:
        return EmptinessEvidence(snapshot_phase_ended, tables_seen, {}, None)
    counts: dict[str, int] = {}
    source_missing: set[str] = set()
    wal_lsn: int | None = None
    try:
        import psycopg

        with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
            row = conn.execute(
                "SELECT (pg_current_wal_lsn() - '0/0')::bigint"
            ).fetchone()
            wal_lsn = int(row[0]) if row and row[0] is not None else None
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            for schema, table, _target in pending:
                exists = conn.execute(
                    "SELECT 1 FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = %s "
                    "AND c.relkind IN ('r', 'p', 'f', 'm')",
                    [schema, table],
                ).fetchone()
                qualified = f"{schema}.{table}"
                if exists is None:
                    source_missing.add(qualified)
                    continue
                found = conn.execute(
                    f"SELECT count(*) FROM {quote(schema)}.{quote(table)}"
                ).fetchone()
                counts[qualified] = int(found[0]) if found else -1
            conn.commit()
    except Exception as exc:  # pragma: no cover - the source may be unreachable
        log.error(
            "could not verify at the source whether %s is empty: %s",
            ", ".join(f"{s}.{t}" for s, t, _ in pending),
            exc,
        )
        return EmptinessEvidence(
            snapshot_phase_ended, tables_seen, counts, None, source_missing
        )
    return EmptinessEvidence(
        snapshot_phase_ended, tables_seen, counts, wal_lsn, source_missing
    )


def finish_source_missing_tables(
    con,
    *,
    pipeline: str,
    dataset: str,
    tables: list[tuple[str, str, str]],
    done: set[str],
    evidence: EmptinessEvidence,
    drop_mode: str = DROP_LOG,
    namespace: str | None = None,
    snapshot_epoch: int | None = None,
    control_schema: str | None = None,
) -> tuple[list[str], list[str]]:
    """Apply the configured drop policy only after final source absence evidence."""
    if drop_mode == DROP_IGNORE:
        # Ignore disables new catalog polling, but it must not turn an already
        # durable rebuild obligation into permission to destroy its retained image.
        drop_mode = DROP_LOG
    if drop_mode not in {DROP_LOG, DROP_REPLICATE}:
        raise ValueError(
            f"source-missing policy requires log or replicate, got {drop_mode!r}"
        )
    candidates: list[tuple[str, str, str]] = []
    for schema, table, target in tables:
        qualified = f"{schema}.{table}"
        if (
            qualified in done
            or qualified not in evidence.source_missing
            or not evidence.snapshot_phase_ended
            or qualified in evidence.tables_seen
            or evidence.wal_lsn is None
        ):
            if (
                qualified in evidence.source_missing
                and qualified not in done
                and (
                    not evidence.snapshot_phase_ended
                    or qualified in evidence.tables_seen
                    or evidence.wal_lsn is None
                )
            ):
                log.error(
                    "source %s is absent without a final complete source check; "
                    "leaving its retained image owed",
                    qualified,
                )
            continue
        candidates.append((schema, table, target))
    if not candidates:
        return [], []

    logged: list[str] = []
    dropped: list[str] = []
    con.execute("BEGIN TRANSACTION")
    try:
        for schema, table, target in candidates:
            qualified = f"{schema}.{table}"
            shadow = naming.shadow_table(target)
            con.execute(f"DROP TABLE IF EXISTS {quote(dataset)}.{quote(shadow)}")
            applied = drop_mode == DROP_REPLICATE
            detail = (
                "the final source catalog observation found the replacement relation "
                "absent after re-snapshot; "
                + (
                    "the retained destination image was kept as a logged drop"
                    if not applied
                    else "the configured DROP_REPLICATE policy removed the retained image"
                )
                + f" at source WAL position {evidence.wal_lsn}"
            )
            state = table_lifecycle.read(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                control_schema=control_schema,
            )
            if state != table_lifecycle.AWAITING:
                table_lifecycle.transition(
                    con,
                    pipeline=pipeline,
                    source_schema=schema,
                    source_table=table,
                    to=table_lifecycle.AWAITING,
                    reason="normalizing a source-missing re-snapshot obligation",
                    control_schema=control_schema,
                )
            table_lifecycle.transition(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                to=table_lifecycle.COMPLETE,
                reason=(
                    "the final source observation proved the relation absent; "
                    "the retained image is a logged drop"
                    if not applied
                    else "the final source observation proved the relation absent before DROP_REPLICATE"
                ),
                snapshot_lsn=evidence.wal_lsn,
                control_schema=control_schema,
            )
            # A dropped source has a complete, verified absence result.  It is the
            # one quarantine trigger that can discharge without publishing rows.
            dest_mod.resolve_schema_refusal(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                control_schema=control_schema,
            )
            projection.project_snapshot_completion(
                con,
                pipeline=pipeline,
                namespace=namespace,
                source_schema=schema,
                source_table=table,
                target_table=target,
                snapshot_lsn=evidence.wal_lsn,
                commit_id=0,
                events=(
                    ProjectionEvent(
                        "source_missing",
                        detail,
                        table_event="dropped",
                        table_event_detail=detail,
                        seq=0,
                        applied=applied,
                    ),
                ),
                snapshot_epoch=snapshot_epoch,
                control_schema=control_schema,
            )
            if applied:
                con.execute(f"DROP TABLE IF EXISTS {quote(dataset)}.{quote(target)}")
                dest_mod.forget_table_state(
                    con,
                    pipeline=pipeline,
                    source_schema=schema,
                    source_table=table,
                    control_schema=control_schema,
                )
                dest_mod.forget_source_relation(
                    con,
                    pipeline=pipeline,
                    source_schema=schema,
                    source_table=table,
                    control_schema=control_schema,
                )
                dropped.append(qualified)
            else:
                logged.append(qualified)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return logged, dropped
