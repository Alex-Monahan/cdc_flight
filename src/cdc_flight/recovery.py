"""Rubric 1.8's acquisition recovery, as a crash-recoverable state machine (§19/A53).

Split out of `reconcile.py`, which had grown a pure decision function, offset-file
forensics, source-identity comparison and a sequence of destructive side effects into
one 713-line "reconciliation feature" with no durable owner (Codex B6). This module owns
exactly one thing: **what has to happen after the slot check says the destination must
be rebuilt, and how that survives a crash at any point.**

## Why a journal

The recovery mutates four independent durable things:

1. the durable to-do list (`table_state.snapshot_state = 'awaiting_snapshot'`);
2. `offsets.dat` on disk;
3. the durable resume point (`_cdc_flight.debezium_offsets`);
4. the replication slot on the source.

Nothing outside a single destination transaction can make two of those atomic, so the
question is not "how do we avoid an intermediate state" but "**can the Flight recognise
its own intermediate state**". It could not. Two cuts were reproduced:

* crash between (3) and (2) in the old order left `row absent / file present`, which the
  Flight diagnoses as `orphan_offset_file` — a refusal that exists to protect an
  operator who pointed the DSN at the wrong database — and it then refused to start for
  ever, three restarts running, until a human passed `--accept-orphan-offsets`
  (Opus MAJOR-1);
* crash after (4) lost the in-memory `snapshot.mode='initial'` override, so the next run
  saw no row, no file and no slot and called it an ordinary fresh start rather than the
  recovery it was in the middle of (Codex B3).

So: **the intent is written first, durably, atomically with the to-do list**, and every
later step is idempotent and re-entrant from the phase the journal records. The file is
now deleted *before* the resume row, which makes the one remaining unjournalled cut
benign in its own right (`file absent / row present` is `file_missing_rebuilt`), but the
journal is what makes the claim structural rather than lucky.

## The phases

    requested ──▶ offsets_file_deleted ──▶ resume_point_deleted ──▶ armed
        │                 │                        │                  │
        │                 │                        │                  └─ the snapshot
        │                 │                        │                     is owed; the
        │                 │                        │                     row survives
        │                 │                        │                     until it is
        │                 │                        │                     done
        └─ every phase is resumable from the row alone, and every step is a no-op when
           it has already happened

`requested` is written **inside** a destination transaction with the table marking and
(where the decision calls for it) the catalog invalidation, so "a recovery is owed" and
"here is what it owes" become true together or not at all.

## The one step that may not be stepped over

Dropping the slot. A45 measured that Debezium only pairs the snapshot with an exact WAL
position when it creates the slot **itself**; a re-snapshot that runs against a surviving
slot resumes the stream from a `confirmed_flush_lsn` we cannot account for, past the
snapshot's consistent point, which is the loss window rubric 1.8 exists to close. A drop
that neither succeeds nor proves the slot absent therefore raises `RecoveryFailed` with
the journal intact, and the next run retries from the same phase. It used to be caught,
recorded as the string `drop_failed: ...`, and stepped straight over — while the caller
*also* erased the recorded LSN baseline, destroying the evidence that would have caught
it next time (Codex B4).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from pathlib import Path

from .destination import CONTROL_SCHEMA, now, raise_alert, request_snapshot
from .errors import RecoveryFailed

log = logging.getLogger("cdc_flight.recovery")

#: In the order they happen. `armed` is terminal for the *mutation* sequence and is
#: cleared only once the snapshot the recovery asked for has actually been taken.
PHASE_REQUESTED = "requested"
PHASE_FILE_DELETED = "offsets_file_deleted"
PHASE_ROW_DELETED = "resume_point_deleted"
PHASE_ARMED = "armed"

PHASES = (PHASE_REQUESTED, PHASE_FILE_DELETED, PHASE_ROW_DELETED, PHASE_ARMED)

#: The `snapshot.mode` a recovery forces. Persisted, because the whole point of the
#: journal is that this intent outlives the process that formed it.
FORCED_SNAPSHOT_MODE = "initial"


@dataclass
class RecoveryRecord:
    """One journalled recovery, exactly as the destination holds it."""

    recovery_id: str
    decision: str
    phase: str
    slot_name: str | None = None
    offset_path: str | None = None
    snapshot_mode: str | None = None
    forget_catalog: bool = False
    tables_marked: int = 0
    message: str = ""

    def as_dict(self) -> dict:
        return {
            "recovery_id": self.recovery_id,
            "decision": self.decision,
            "phase": self.phase,
            "slot": self.slot_name,
            "snapshot_mode": self.snapshot_mode,
            "forget_catalog": self.forget_catalog,
            "tables_marked": self.tables_marked,
            "message": self.message,
        }


def read(con, *, pipeline: str, namespace: str) -> RecoveryRecord | None:
    """The recovery this pipeline is in the middle of, or None."""
    rows = con.execute(
        f"SELECT recovery_id, decision, phase, slot_name, offset_path, snapshot_mode, "
        f"       forget_catalog, tables_marked, message "
        f"FROM {CONTROL_SCHEMA}.recovery_state WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    ).fetchall()
    if not rows:
        return None
    (rid, decision, phase, slot, path, mode, forget, marked, message) = rows[0]
    return RecoveryRecord(
        recovery_id=str(rid),
        decision=str(decision),
        phase=str(phase),
        slot_name=str(slot) if slot is not None else None,
        offset_path=str(path) if path is not None else None,
        snapshot_mode=str(mode) if mode is not None else None,
        forget_catalog=bool(forget),
        tables_marked=int(marked or 0),
        message=str(message or ""),
    )


def _write_phase(con, *, pipeline: str, namespace: str, phase: str) -> None:
    con.execute(
        f"UPDATE {CONTROL_SCHEMA}.recovery_state SET phase = ?, updated_at = ? "
        "WHERE pipeline = ? AND namespace = ?",
        [phase, now(), pipeline, namespace],
    )


def clear(con, *, pipeline: str, namespace: str) -> None:
    """The recovery is done: the tables it owed have been snapshotted."""
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.recovery_state WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    )


def begin(
    con,
    *,
    pipeline: str,
    namespace: str,
    decision: str,
    message: str,
    slot_name: str,
    offset_path: Path,
    captured_tables: list[tuple[str, str, str]],
    forget_catalog: bool,
    context: dict | None = None,
) -> RecoveryRecord:
    """Write the intent, atomically with the to-do list. NOTHING is destroyed here.

    The destination tables stay exactly as they are, still queryable, and each one is
    replaced only when its shadow is complete, in one transaction (D7). What this makes
    durable is *that a rebuild is owed and why*, before the first byte of anything else
    changes — so a crash one statement later is recognisable rather than mysterious.

    Idempotent: an existing journal row for this pipeline is replaced, because a second
    detection of the same unsafe state is the same recovery, not a new one.
    """
    record = RecoveryRecord(
        recovery_id=uuid.uuid4().hex,
        decision=decision,
        phase=PHASE_REQUESTED,
        slot_name=slot_name,
        offset_path=str(offset_path),
        snapshot_mode=FORCED_SNAPSHOT_MODE,
        forget_catalog=forget_catalog,
        message=message,
    )
    raise_alert(
        con, pipeline=pipeline, severity="critical", code=decision,
        message=(
            f"{message}. Rebuilding every captured table from the source "
            f"({len(captured_tables)} tables); the destination stays queryable until "
            "each table's snapshot is complete and swapped in one transaction."
        ),
        context=(context or {}) | {"recovery_id": record.recovery_id},
    )
    con.execute("BEGIN TRANSACTION")
    try:
        marked = request_snapshot(
            con,
            pipeline=pipeline,
            tables=captured_tables,
            detail=f"{decision}: {message}",
        )
        record.tables_marked = marked
        if forget_catalog:
            # A different cluster's oids are not our relations' oids, and comparing them
            # would make the catalog watcher conclude that every table was dropped and
            # recreated - which the mass-drop circuit breaker then refuses, correctly and
            # unhelpfully. Forget what we knew about a catalog that no longer exists.
            #
            # Widened from `source_identity_changed` alone: a base-backup restore can
            # keep the same `system_identifier`, rewind WAL, change the timeline and
            # still hand back different relation oids (Codex M1).
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?",
                [pipeline],
            )
        con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.recovery_state WHERE pipeline = ? "
            "AND namespace = ?",
            [pipeline, namespace],
        )
        con.execute(
            f"INSERT INTO {CONTROL_SCHEMA}.recovery_state "
            "(pipeline, namespace, recovery_id, decision, phase, slot_name, "
            " offset_path, snapshot_mode, forget_catalog, tables_marked, message, "
            " requested_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                pipeline, namespace, record.recovery_id, decision, PHASE_REQUESTED,
                slot_name, str(offset_path), FORCED_SNAPSHOT_MODE, forget_catalog,
                marked, message, now(), now(),
            ],
        )
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except Exception:  # pragma: no cover - never mask the original error
            log.debug("rollback of the recovery journal failed", exc_info=True)
        raise
    log.warning(
        "rubric 1.8 recovery %s REQUESTED (%s): %s table(s) owe a snapshot",
        record.recovery_id, decision, marked,
    )
    return record


def resume(
    con,
    *,
    pipeline: str,
    namespace: str,
    record: RecoveryRecord,
    dsn: str,
    drop_slot=None,
    on_phase=None,
) -> dict:
    """Run the recovery forward from whatever phase the journal records. Idempotent.

    Every step is written to survive being run twice and being run after a crash, so
    this is safe to call on every acquisition for as long as a journal row exists.

    `on_phase` is a test seam: it is called with the phase name **after** that phase's
    durable effect and before the phase is recorded, which is where the crash-at-every-
    step tests cut. It is not reachable from configuration.
    """
    from . import reconcile as reconcile_mod

    drop_slot = drop_slot or reconcile_mod.drop_slot
    result = {
        "recovery_id": record.recovery_id,
        "decision": record.decision,
        "resumed_from": record.phase,
        "tables_marked": record.tables_marked,
        "message": record.message,
    }
    offset_path = Path(record.offset_path) if record.offset_path else None

    if record.phase == PHASE_REQUESTED:
        # The offsets FILE first, and the durable row second (Opus MAJOR-1). The
        # reverse order leaves `row absent / file present`, which is the
        # `orphan_offset_file` refusal - the right answer for an operator who pointed
        # the DSN at the wrong database and the wrong one for our own half-finished
        # recovery. This order leaves `file absent / row present`, which reconciliation
        # simply rebuilds.
        removed = False
        if offset_path is not None:
            removed = offset_path.exists()
            offset_path.unlink(missing_ok=True)
        result["offset_file"] = "removed" if removed else "absent"
        if on_phase is not None:
            on_phase(PHASE_FILE_DELETED)
        _write_phase(con, pipeline=pipeline, namespace=namespace, phase=PHASE_FILE_DELETED)
        record.phase = PHASE_FILE_DELETED

    if record.phase == PHASE_FILE_DELETED:
        con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.debezium_offsets WHERE pipeline = ? "
            "AND namespace = ?",
            [pipeline, namespace],
        )
        if on_phase is not None:
            on_phase(PHASE_ROW_DELETED)
        _write_phase(con, pipeline=pipeline, namespace=namespace, phase=PHASE_ROW_DELETED)
        record.phase = PHASE_ROW_DELETED

    if record.phase == PHASE_ROW_DELETED:
        slot_action = _drop_the_slot_or_fail(
            drop_slot, dsn=dsn, slot_name=record.slot_name, record=record
        )
        result["slot"] = slot_action
        if on_phase is not None:
            on_phase(PHASE_ARMED)
        _write_phase(con, pipeline=pipeline, namespace=namespace, phase=PHASE_ARMED)
        record.phase = PHASE_ARMED

    result["phase"] = record.phase
    result.setdefault("slot", "already dropped by an earlier attempt")
    result.setdefault("offset_file", "already removed by an earlier attempt")
    log.warning(
        "rubric 1.8 recovery %s is ARMED (%s): %s table(s) awaiting a snapshot, resume "
        "point deleted, offsets file %s, slot %s",
        record.recovery_id, record.decision, record.tables_marked,
        result["offset_file"], result["slot"],
    )
    return result


def _drop_the_slot_or_fail(drop_slot, *, dsn: str, slot_name: str | None, record) -> str:
    """`dropped` or `absent`, or `RecoveryFailed`. There is no third outcome.

    See the module docstring: a re-snapshot started against a surviving slot has an
    uncoordinated image/stream boundary, which is the exact loss window rubric 1.8
    exists to close, so "the drop failed, carry on" is not available. Raising leaves
    the journal at `resume_point_deleted`, which the next acquisition resumes from.
    """
    if not slot_name:  # pragma: no cover - the journal always records one
        return "not attempted"
    try:
        action = drop_slot(dsn, slot_name)
    except Exception as exc:
        raise RecoveryFailed(
            f"the replication slot {slot_name!r} could not be dropped ({exc}), so "
            f"recovery {record.recovery_id} ({record.decision}) cannot continue. "
            "Debezium only pairs a snapshot with an exact WAL position when it creates "
            "the slot itself (ADR 0001 §19/A45), so re-snapshotting against the "
            "surviving slot would resume the stream past the snapshot's consistent "
            "point - the loss window rubric 1.8 exists to close. The recovery journal "
            "is intact and the next run retries this step; the usual cause is another "
            "backend still holding the slot."
        ) from exc
    if action not in ("dropped", "absent"):  # pragma: no cover - defensive
        raise RecoveryFailed(
            f"dropping {slot_name!r} returned {action!r}, which is neither 'dropped' "
            "nor 'absent', so the slot cannot be shown to be gone"
        )
    return action
