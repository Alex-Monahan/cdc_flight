"""Alert, slot, and snapshot-obligation state ownership."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path

from . import destination as _d

log = _d.log
now = _d.now
_control_table = _d._control_table
_queueing = _d._queueing
AWAITING_SNAPSHOT = _d.AWAITING_SNAPSHOT
SNAPSHOT_IN_PROGRESS = _d.SNAPSHOT_IN_PROGRESS
SNAPSHOT_STATES_OWING_WORK = _d.SNAPSHOT_STATES_OWING_WORK
SLOT_VERDICTS = _d.SLOT_VERDICTS
faults = _d.faults
table_lifecycle = _d.table_lifecycle
quote = _d.quote

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

    def raise_alert_once(
        self, *, severity: str, code: str, message: str, marker_value: str,
        context=None,
    ) -> bool:
        """Write one durable alert for one condition occurrence.

        Use the sink's independent cursor for both the probe and the insert.  A
        pre-engine failure can leave the applier connection in an aborted transaction;
        routing the alert through that same handle would make the operator signal
        disappear when the failed transaction is retired.
        """
        con = self._sink if self.independent else self._main
        if alert_marker_exists(
            con,
            pipeline=self.pipeline,
            code=code,
            marker_key="condition_marker",
            marker_value=marker_value,
            control_schema=self.control_schema,
        ):
            return False
        payload = dict(context or {})
        payload["condition_marker"] = marker_value
        return self.raise_alert(
            severity=severity,
            code=code,
            message=message,
            context=payload,
        )

    def clear_alert_once(self, *, code: str, marker_value: str) -> bool:
        """Remove a pre-armed alert after its bounded operation succeeds.

        This is intentionally called only after the commit/ack exclusion has closed.
        A failed or hard-exited operation leaves the conservative alert in place for
        the next run to reconcile.
        """
        con = self._sink if self.independent else self._main
        marker = f'%"condition_marker": "{marker_value}"%'
        try:
            con.execute(
                f"DELETE FROM {_control_table(self.control_schema, 'alerts')} "
                "WHERE pipeline = ? AND code = ? AND context LIKE ?",
                [self.pipeline, code, marker],
            )
        except Exception:  # pragma: no cover - conservative alert remains durable
            log.warning("could not clear completed %s alert", code, exc_info=True)
            return False
        return True

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


def alert_marker_exists(
    con,
    *,
    pipeline: str,
    code: str,
    marker_key: str,
    marker_value: str,
    control_schema: str | None = None,
) -> bool:
    """Whether an alert of `code` already carries this context marker.

    The shared probe behind "at most one alert per (relation, condition)".  An alert
    that describes a *standing* condition — a quarantine, a permanently deferred
    change — must be raised once, not once per poll: the same condition re-observed
    every few seconds otherwise grows `alerts` without bound and buries the signal
    it exists to give.  Callers put a stable fingerprint in the alert's JSON context
    and probe for it here before emitting.

    A failed probe returns False (emit anyway): losing an alert is worse than
    duplicating one, and this must never raise into a caller's decision path.
    """
    marker = f'%"{marker_key}": "{marker_value}"%'
    try:
        row = con.execute(
            f"SELECT 1 FROM {_control_table(control_schema, 'alerts')} "
            "WHERE pipeline = ? AND code = ? AND context LIKE ? LIMIT 1",
            [pipeline, code, marker],
        ).fetchone()
    except Exception:  # pragma: no cover - dedup must never mask the caller
        log.warning("could not probe existing %s alerts", code, exc_info=True)
        return False
    return row is not None


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


def raise_alert_once(
    con,
    *,
    pipeline: str,
    severity: str,
    code: str,
    message: str,
    marker_value: str,
    context=None,
    control_schema: str | None = None,
) -> bool:
    """Persist one alert for one durable occurrence.

    Failure paths are often revisited on every bounded runner invocation.  The
    condition marker makes that repetition a single operator incident while a
    changed marker (for example a new slot LSN or a new failure fingerprint) is a
    new occurrence.  A failed probe intentionally falls through to an insert:
    losing the alert is worse than a duplicate.
    """
    if alert_marker_exists(
        con,
        pipeline=pipeline,
        code=code,
        marker_key="condition_marker",
        marker_value=marker_value,
        control_schema=control_schema,
    ):
        return False
    payload = dict(context or {})
    payload["condition_marker"] = marker_value
    raise_alert(
        con,
        pipeline=pipeline,
        severity=severity,
        code=code,
        message=message,
        context=payload,
        control_schema=control_schema,
    )
    return True


def fallback_alert_path(dest) -> Path:
    """The local durable alert surface used when a destination cannot be opened.

    A DuckDB file lock prevents a second connection from reaching
    ``_cdc_flight.alerts``.  The sidecar is deliberately next to that exact file, so
    it remains visible to the operator and is shared by every pipeline spelling that
    points at the same destination. MotherDuck connection failures use the local
    pipeline state directory until a destination connection can be restored.
    """
    if dest.kind == "duckdb":
        resolved = Path(dest.duckdb_path).expanduser().resolve(strict=False)
        return Path(f"{resolved}.cdc_alerts.jsonl")
    return Path(dest.pipelines_dir) / f"{dest.pipeline_name}.cdc_alerts.jsonl"


def persist_fallback_alert(
    dest,
    *,
    runner_id: str,
    severity: str,
    code: str,
    message: str,
    marker_value: str,
    context=None,
) -> Path:
    """Synchronously append and fsync an alert when no destination handle exists.

    This is a bounded, append-only operator surface rather than a best-effort log.
    A failure to create or fsync it is raised to the caller, which retains the original
    non-zero run failure and emits a critical stderr diagnostic.
    """
    path = fallback_alert_path(dest)
    payload = {
        "pipeline": dest.pipeline_name,
        "runner_id": runner_id,
        "raised_at": now().isoformat(),
        "severity": severity,
        "code": code,
        "message": message,
        "marker_value": marker_value,
        "context": dict(context or {}),
        "destination_kind": dest.kind,
        "destination_path": str(getattr(dest, "duckdb_path", "")),
    }
    line = (json.dumps(payload, default=str, sort_keys=True) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        str(path), os.O_WRONLY | os.O_APPEND | os.O_CREAT,
        0o600,
    )
    try:
        written = 0
        while written < len(line):
            written += os.write(descriptor, line[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


def replay_fallback_alerts(con, dest) -> int:
    """Project valid sidecar rows onto the normal durable alert table.

    The sidecar is retained as an audit trail; the condition marker makes replay
    idempotent. A partially written final line is left for the next run and does not
    erase the complete lines before it.
    """
    path = fallback_alert_path(dest)
    if not path.exists():
        return 0
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:  # pragma: no cover - the sidecar itself remains operator-visible
        log.critical("could not read fallback alert sidecar %s", path, exc_info=True)
        return 0
    recorded = 0
    for line in lines:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            log.warning("ignoring an incomplete fallback alert line in %s", path)
            continue
        pipeline = str(item.get("pipeline") or dest.pipeline_name)
        sink = AlertSink(con, pipeline=pipeline, control_schema=dest.control_schema)
        try:
            if sink.raise_alert_once(
                severity=str(item.get("severity") or "critical"),
                code=str(item.get("code") or "destination_unavailable"),
                message=str(item.get("message") or "destination was unavailable"),
                marker_value=str(item.get("marker_value") or "fallback:unknown"),
                context=dict(item.get("context") or {}) | {
                    "fallback_alert_path": str(path),
                    "replayed": True,
                },
            ):
                recorded += 1
        finally:
            sink.close()
    return recorded


def observe_source_health(
    con,
    *,
    pipeline: str,
    state: str,
    confirmed_flush_lsn: int | None = None,
    control_schema: str | None = None,
) -> dict | None:
    """Persist the source reachability episode used by ``source_dark`` alerts.

    ``reachable`` closes a prior dark episode. ``dark`` opens exactly one new episode
    until a reachable observation is recorded. ``unknown`` is intentionally not a
    transition: a source that cannot be sampled must not fabricate a recovery or a
    second incident.
    """
    if state not in {"reachable", "dark"}:
        return None
    table = _control_table(control_schema, "source_health_episodes")
    current = con.execute(
        f"SELECT episode_id, state FROM {table} WHERE pipeline = ?",
        [pipeline],
    ).fetchone()
    timestamp = now()
    con.execute("BEGIN TRANSACTION")
    try:
        if current is None:
            episode_id = 0 if state == "reachable" else 1
            con.execute(
                f"INSERT INTO {table} "
                "(pipeline, episode_id, state, opened_at, recovered_at, "
                "last_confirmed_flush_lsn, observed_at) VALUES (?,?,?,?,?,?,?)",
                [
                    pipeline, episode_id, state,
                    timestamp if state == "dark" else None,
                    None, confirmed_flush_lsn, timestamp,
                ],
            )
        else:
            episode_id, previous_state = int(current[0]), str(current[1])
            if state == "dark" and previous_state != "dark":
                episode_id += 1
                con.execute(
                    f"UPDATE {table} SET episode_id = ?, state = ?, opened_at = ?, "
                    "recovered_at = NULL, last_confirmed_flush_lsn = ?, observed_at = ? "
                    "WHERE pipeline = ?",
                    [episode_id, state, timestamp, confirmed_flush_lsn, timestamp, pipeline],
                )
            else:
                con.execute(
                    f"UPDATE {table} SET state = ?, recovered_at = ?, "
                    "last_confirmed_flush_lsn = ?, observed_at = ? WHERE pipeline = ?",
                    [state, timestamp if state == "reachable" else None,
                     confirmed_flush_lsn, timestamp, pipeline],
                )
        con.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            con.execute("ROLLBACK")
        raise
    row = con.execute(
        f"SELECT episode_id, state, observed_at FROM {table} WHERE pipeline = ?",
        [pipeline],
    ).fetchone()
    return {
        "episode_id": int(row[0]),
        "state": str(row[1]),
        "observed_at": row[2],
    } if row else None


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
