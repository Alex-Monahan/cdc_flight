"""Durable schema-refusal and quarantine state ownership."""

from __future__ import annotations

import contextlib
import hashlib

from . import destination as _d

log = _d.log
now = _d.now
table_lifecycle = _d.table_lifecycle
_control_table = _d._control_table
AWAITING_SNAPSHOT = _d.AWAITING_SNAPSHOT
CANONICAL_REFUSAL_CLASS = _d.CANONICAL_REFUSAL_CLASS
REFUSAL_ABSENT = _d.REFUSAL_ABSENT
REFUSAL_PENDING = _d.REFUSAL_PENDING
REFUSAL_QUARANTINED = _d.REFUSAL_QUARANTINED
REFUSAL_RESOLVED = _d.REFUSAL_RESOLVED
SCHEMA_REFUSAL = _d.SCHEMA_REFUSAL
write_table_event = _d.write_table_event


def alert_marker_exists(*args, **kwargs):
    return _d.alert_marker_exists(*args, **kwargs)


def raise_alert(*args, **kwargs):
    return _d.raise_alert(*args, **kwargs)

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
    transaction_open: bool = False,
    deferred_alerts: list[dict] | None = None,
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
    owns_transaction = not transaction_open
    if owns_transaction:
        con.execute("BEGIN TRANSACTION")
    try:
        previous = con.execute(
            f"SELECT state, refusal_fingerprint, source_fingerprint, reason "
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
            if owns_transaction:
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
            # Keep the first concrete exception as the durable diagnosis.  A later
            # blocked-table observation is a lifecycle fact, not new evidence, and
            # must not replace `decimal.InvalidOperation`/`ValueError`/etc. with a
            # generic retry explanation (R14-13).
            recorded_reason = previous[3] or reason
            incident_at = now()
            incident_marker = f"{fingerprint}:{incident_at.isoformat()}"
            SCHEMA_REFUSAL.check(before, REFUSAL_QUARANTINED)
            con.execute(
                f"UPDATE {_control_table(control_schema, 'schema_refusals')} SET "
                "target_table = ?, detected_lsn = ?, reason = ?, "
                "refusal_fingerprint = ?, source_fingerprint = ?, refusal_class = ?, "
                "state = ?, refused_at = ? "
                "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                [
                    target_table, detected_lsn, recorded_reason, fingerprint,
                    recorded_source_fingerprint,
                    canonical_class, REFUSAL_QUARANTINED, incident_at, pipeline,
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
            if not alert_marker_exists(
                con,
                pipeline=pipeline,
                code="schema_table_quarantined",
                marker_key="incident_marker",
                marker_value=incident_marker,
                control_schema=control_schema,
            ):
                quarantine_alert = {
                    "source_schema": source_schema,
                    "source_table": source_table,
                    "target_table": target_table,
                    "refusal_class": canonical_class,
                    "refusal_fingerprint": fingerprint,
                    "incident_marker": incident_marker,
                    "resnapshot_required": True,
                }
            result = REFUSAL_QUARANTINED
            if owns_transaction:
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
            if owns_transaction:
                con.execute("COMMIT")
    except BaseException:
        if owns_transaction:
            with contextlib.suppress(Exception):
                con.execute("ROLLBACK")
        raise

    # This is deliberately outside the transaction/rollback handler.  Alerting is
    # diagnostic; a sink failure must never turn a committed quarantine into a second
    # refusal or mask the durable state transition.
    if quarantine_alert is not None:
        alert = {
            "severity": "critical",
            "code": "schema_table_quarantined",
            "message": (
                f"{source_schema}.{source_table} is quarantined after a repeated "
                "schema/value refusal; its destination image is stale/unavailable "
                "until a full resnapshot completes"
            ),
            "context": quarantine_alert,
        }
        if deferred_alerts is not None:
            deferred_alerts.append(alert)
        else:
            raise_alert(
                con,
                pipeline=pipeline,
                severity=alert["severity"],
                code=alert["code"],
                message=alert["message"],
                context=alert["context"],
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


def schema_refusal_state(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    control_schema: str | None = None,
) -> str | None:
    """Read one refusal state for an explicit operator acknowledgement decision."""
    row = con.execute(
        f"SELECT state FROM {_control_table(control_schema, 'schema_refusals')} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchone()
    return None if row is None else str(row[0])


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

    A stored ``source_fingerprint`` of NULL (an older refusal, or one recorded from a
    seam that had no source read) is ADOPTED, not treated as evidence.  The r11 fix
    made a NULL return True so a NULL quarantine could not be a permanent dead end;
    because nothing ever wrote the observed fingerprint back, the same NULL was read
    on every subsequent run and the table was reactivated and re-refused once per
    run for ever, contradicting the paragraph above.  The first successful source
    read now becomes the comparison baseline and by itself authorizes nothing; a
    LATER change to that baseline is still detected, so the dead end stays closed
    and the retry is bounded rather than per-run.
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
        # Adopt the baseline on the caller's own control connection.  The guard on
        # `source_fingerprint IS NULL` keeps this idempotent and stops it from
        # clobbering a fingerprint another writer recorded meanwhile; the row must
        # still be quarantined, so an interleaved reactivation is not undone either.
        con.execute(
            f"UPDATE {_control_table(control_schema, 'schema_refusals')} "
            "SET source_fingerprint = ? WHERE pipeline = ? AND source_schema = ? "
            "AND source_table = ? AND state = ? AND source_fingerprint IS NULL",
            [
                str(source_fingerprint), pipeline, source_schema, source_table,
                REFUSAL_QUARANTINED,
            ],
        )
        log.info(
            "adopted source fingerprint %s as the quarantine baseline for %s.%s; "
            "a later change to it is an automatic retry trigger",
            source_fingerprint, source_schema, source_table,
        )
        return False
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
