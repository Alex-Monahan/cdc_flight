"""The ONE writer of `table_state.snapshot_state` (rubric 1.9, ADR §20/A55).

Per-table lifecycle was the biggest implicit state machine in the tree: one durable
column written from five modules, a domain that had drifted from the ADR in both
directions (`failed` declared and never written, `awaiting_snapshot` written by three
modules and never declared), no validation on read, and one durable non-terminal value
— `in_progress` — that no durable queue selected. That last one had a measured
consequence: a process killed inside a snapshot left a table half-built and invisible,
and the recovery journal's "does anything still owe work?" test could pass over it and
log *"recovery COMPLETE: every captured table has a fresh image"*.

The 1.6—1.8 fix round closed the hole with `promote_interrupted_snapshots()`. This
module closes the *class*: there is now exactly one `UPDATE ... SET snapshot_state` and
one `INSERT ... snapshot_state` in `src/`, both here, both preceded by
`TABLE_LIFECYCLE.check(from, to)`. `tests/1.9_state_machines/test_1_9_table_lifecycle.py`
greps the tree and fails if a second writer appears — because a machine with two
writers is a machine with one writer and one bug waiting to be written.

An undeclared edge is a **loud failure plus an alert**, never a silent write. The
argument for that severity: every state in this domain decides whether a destination
table is trusted, so a transition nobody designed is, by construction, a table whose
trustworthiness nobody has reasoned about. Refusing leaves the previous state — which
is always the *more* conservative of the two, because every edge into a terminal state
is declared and only the undeclared ones can lose owed work.
"""

from __future__ import annotations

import logging

from .control_schema import CONTROL_SCHEMA
from .machines import (
    LIFECYCLE_ABSENT,
    LIFECYCLE_AWAITING,
    LIFECYCLE_COMPLETE,
    LIFECYCLE_DURABLE_VALUES,
    LIFECYCLE_IN_PROGRESS,
    LIFECYCLE_NONE,
    LIFECYCLE_OWING_WORK,
    TABLE_LIFECYCLE,
)
from .states import IllegalTransition, UnknownState

log = logging.getLogger("cdc_flight.table_lifecycle")

__all__ = [
    "ABSENT",
    "AWAITING",
    "COMPLETE",
    "IN_PROGRESS",
    "NONE",
    "OWING_WORK",
    "forget",
    "read",
    "read_all",
    "transition",
    "transition_all",
]

ABSENT = LIFECYCLE_ABSENT
NONE = LIFECYCLE_NONE
IN_PROGRESS = LIFECYCLE_IN_PROGRESS
COMPLETE = LIFECYCLE_COMPLETE
AWAITING = LIFECYCLE_AWAITING
OWING_WORK = LIFECYCLE_OWING_WORK

#: Distinguishes "leave this column alone" from "write NULL".
_KEEP = object()


# --------------------------------------------------------------------------- #
# reading
# --------------------------------------------------------------------------- #
def read(con, *, pipeline: str, source_schema: str, source_table: str) -> str:
    """This table's lifecycle state, or `absent` when it has no row.

    Validated: a value outside the declared domain raises `UnknownState` rather than
    being skipped, because a state that belongs to no queue and no recovery path is
    exactly the shape that makes a rebuild silently never happen.
    """
    rows = con.execute(
        f"SELECT snapshot_state FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchall()
    if not rows:
        return ABSENT
    return _parse(rows[0][0], f"{source_schema}.{source_table}")


def read_all(con, pipeline: str) -> dict[str, str]:
    """`"<schema>.<table>" -> state`, every row, validated."""
    return {
        f"{schema}.{table}": _parse(state, f"{schema}.{table}")
        for schema, table, state in con.execute(
            f"SELECT source_schema, source_table, snapshot_state FROM "
            f"{CONTROL_SCHEMA}.table_state WHERE pipeline = ?",
            [pipeline],
        ).fetchall()
    }


def owing_work(con, pipeline: str) -> list[str]:
    """Every table in a state that means "this image cannot be trusted".

    Derived from the machine's `terminal` set, not from a second literal list — with
    the one exception `machines.LIFECYCLE_OWING_WORK` states: `none` is non-terminal and
    not owed, because the run's ordinary `snapshot.mode` covers a table that has never
    been snapshotted.
    """
    return sorted(
        name for name, state in read_all(con, pipeline).items() if state in OWING_WORK
    )


def _parse(value, qualified: str) -> str:
    text = str(value)
    if text not in LIFECYCLE_DURABLE_VALUES:
        raise UnknownState(
            f"{CONTROL_SCHEMA}.table_state for {qualified} carries "
            f"snapshot_state={text!r}, which is not one of "
            f"{sorted(LIFECYCLE_DURABLE_VALUES)}. A state outside the frozen domain "
            "belongs to no queue and no recovery path, so it is refused rather than "
            "skipped (ADR 0001 §4.8, §20/A55)."
        )
    return text


# --------------------------------------------------------------------------- #
# writing — the only two statements in the tree that touch the column
# --------------------------------------------------------------------------- #
def transition(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    to: str,
    reason: str,
    target_table: str | None = None,
    epoch=_KEEP,
    snapshot_lsn=_KEEP,
    last_commit_id=_KEEP,
    replace: bool = False,
    alerts=None,
) -> str:
    """Move one table to `to`, asserting the edge first. Returns the previous state.

    `replace=True` is DELETE+INSERT, which is what a caller wants when the row's
    *identity* is being re-established (a new shadow, a relation that came back under a
    different target name); everything else is an UPDATE that leaves the per-table
    configuration columns alone.
    """
    frm = read(
        con, pipeline=pipeline, source_schema=source_schema, source_table=source_table
    )
    _check(
        frm, to,
        pipeline=pipeline, qualified=f"{source_schema}.{source_table}",
        reason=reason, alerts=alerts,
    )
    if frm == ABSENT or replace:
        if frm != ABSENT:
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.table_state "
                "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                [pipeline, source_schema, source_table],
            )
        con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.table_state "
            "(pipeline, source_schema, source_table, target_table, snapshot_state, "
            " snapshot_epoch, snapshot_lsn, last_commit_id) VALUES (?,?,?,?,?,?,?,?)",
            [
                pipeline, source_schema, source_table, target_table or source_table, to,
                0 if epoch is _KEEP else epoch,
                None if snapshot_lsn is _KEEP else snapshot_lsn,
                None if last_commit_id is _KEEP else last_commit_id,
            ],
        )
        _log(frm, to, f"{source_schema}.{source_table}", reason)
        return frm

    sets = ["snapshot_state = ?"]
    params: list = [to]
    for column, value in (
        ("snapshot_epoch", epoch),
        ("snapshot_lsn", snapshot_lsn),
        ("last_commit_id", last_commit_id),
    ):
        if value is not _KEEP:
            sets.append(f"{column} = ?")
            params.append(value)
    if target_table is not None:
        sets.append("target_table = ?")
        params.append(target_table)
    con.execute(
        f"UPDATE {CONTROL_SCHEMA}.table_state SET {', '.join(sets)} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [*params, pipeline, source_schema, source_table],
    )
    _log(frm, to, f"{source_schema}.{source_table}", reason)
    return frm


def transition_all(
    con,
    *,
    pipeline: str,
    frm: str,
    to: str,
    reason: str,
    reset_snapshot_columns: bool = False,
) -> list[str]:
    """Move every table currently in `frm` to `to`. Returns the qualified names.

    One edge check for the whole set, because it is one edge; the per-row read the
    single-table path does would be N queries for a statement that already knows both
    ends. Used by `--reset-state` and by the start-up promotion of tables a crash left
    `in_progress`.
    """
    if frm != to:
        _check(frm, to, pipeline=pipeline, qualified="*", reason=reason, alerts=None)
    rows = con.execute(
        f"SELECT source_schema, source_table FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND snapshot_state = ? ORDER BY source_schema, source_table",
        [pipeline, frm],
    ).fetchall()
    if not rows:
        return []
    extra = (
        ", snapshot_epoch = 0, snapshot_lsn = NULL, last_commit_id = NULL"
        if reset_snapshot_columns else ""
    )
    con.execute(
        f"UPDATE {CONTROL_SCHEMA}.table_state SET snapshot_state = ?{extra} "
        "WHERE pipeline = ? AND snapshot_state = ?",
        [to, pipeline, frm],
    )
    names = [f"{a}.{b}" for a, b in rows]
    log.info(
        "table_lifecycle: %s table(s) %s -> %s (%s): %s",
        len(names), frm, to, reason, ", ".join(names),
    )
    return names


def reset_all(con, *, pipeline: str, reason: str) -> list[str]:
    """`--reset-state`: every table's snapshot bookkeeping goes back to `none`.

    Deliberately NOT a DELETE. `table_state` is the canonical source-to-destination
    ownership registry, and deleting it made `--reset-state` produce a permanent
    zombie destination table (Opus MAJOR-4, measured).
    """
    moved: list[str] = []
    for frm in (IN_PROGRESS, COMPLETE, AWAITING):
        moved += transition_all(
            con, pipeline=pipeline, frm=frm, to=NONE, reason=reason,
            reset_snapshot_columns=True,
        )
    # Rows already at `none` still need their epoch/lsn cleared, and `none -> none` is
    # a declared no-op edge rather than a special case.
    transition_all(
        con, pipeline=pipeline, frm=NONE, to=NONE, reason=reason,
        reset_snapshot_columns=True,
    )
    return sorted(moved)


def forget(
    con, *, pipeline: str, source_schema: str, source_table: str, reason: str, alerts=None
) -> str:
    """The source relation is gone: the registry row goes with it (`-> absent`)."""
    frm = read(
        con, pipeline=pipeline, source_schema=source_schema, source_table=source_table
    )
    if frm == ABSENT:
        return frm
    _check(
        frm, ABSENT,
        pipeline=pipeline, qualified=f"{source_schema}.{source_table}",
        reason=reason, alerts=alerts,
    )
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    )
    _log(frm, ABSENT, f"{source_schema}.{source_table}", reason)
    return frm


# --------------------------------------------------------------------------- #
# the check
# --------------------------------------------------------------------------- #
def _check(frm: str, to: str, *, pipeline: str, qualified: str, reason: str, alerts) -> None:
    try:
        TABLE_LIFECYCLE.check(frm, to)
    except IllegalTransition as illegal:
        message = (
            f"{qualified}: refusing an undeclared table-lifecycle transition "
            f"{frm!r} -> {to!r} (asked for by: {reason}). {illegal}"
        )
        log.error("%s", message)
        if alerts is not None:
            try:
                alerts.raise_alert(
                    severity="critical",
                    code="illegal_table_lifecycle_transition",
                    message=message,
                    context={
                        "pipeline": pipeline, "table": qualified,
                        "from": frm, "to": to, "reason": reason,
                    },
                )
            except Exception:  # pragma: no cover - alerting must never mask the cause
                log.debug("could not alert on an illegal transition", exc_info=True)
        raise


def _log(frm: str, to: str, qualified: str, reason: str) -> None:
    if frm == to:
        log.debug("table_lifecycle: %s stays %s (%s)", qualified, to, reason)
    else:
        log.info("table_lifecycle: %s %s -> %s (%s)", qualified, frm, to, reason)
