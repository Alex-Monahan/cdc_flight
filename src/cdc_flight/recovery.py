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

import json
import logging
import shutil
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from . import catalog_baseline, table_lifecycle
from .config import resolve_control_schema
from .destination import now, raise_alert, request_snapshot
from .errors import RecoveryFailed
from .faults import arrival, matrix_crash, maybe_crash, runtime_state
from .machines import (
    ACQUISITION_RECOVERY,
    RECOVERY_ABSENT,
    RECOVERY_ARMED,
    RECOVERY_FILE_DELETED,
    RECOVERY_REQUESTED,
    RECOVERY_ROW_DELETED,
)
from .naming import control_table

log = logging.getLogger("cdc_flight.recovery")

#: The decision an operator's `--accept-orphan-offsets` writes. It is a recovery like
#: any other now (Codex r1 BLOCKER-1): `offsets` used to drop the slot and
#: unlink the file and only *then* journal what it had done, so a hard exit in that gap
#: lost the durable obligation to rebuild and the next run called the leftovers an
#: ordinary fresh start - the exact B3/A53 state the journal exists to prevent.
ORPHAN_DECISION = "orphan_offsets_accepted"

#: The decision `--reset-state` writes. Same reasoning (Codex r1 MAJOR-4): reset used to
#: be five independent durable mutations plus a process-local `snapshot.mode='initial'`,
#: argued convergent. It is not: with a positioned slot and a populated destination the
#: next run's slot check hits the deliberate `no_durable_destination_row` refusal before
#: `will_snapshot_everything` is even computed, and repeating the flag does not drop
#: that slot. Journalled, it is one idempotent sequence that finishes without the flag.
RESET_DECISION = "operator_reset"

#: Decisions that ALSO clear the per-table snapshot bookkeeping (epoch, `snapshot_lsn`,
#: `last_commit_id`) before recording the obligation. "Start over" means those columns
#: go back to nothing; it does **not** mean the obligation is weaker, and the first cut
#: made exactly that mistake (Codex r2 BLOCKER-1). Every captured table is marked
#: `awaiting_snapshot` here as it is for every other recovery.
RESET_TABLE_DECISIONS = (RESET_DECISION,)

#: In the order they happen. `armed` is terminal for the *mutation* sequence and is
#: cleared only once the snapshot the recovery asked for has actually been taken.
#: The names and the legal edges are `machines.ACQUISITION_RECOVERY`; re-exported here
#: because this module is where they are read and written (rubric 1.9).
PHASE_REQUESTED = RECOVERY_REQUESTED
PHASE_FILE_DELETED = RECOVERY_FILE_DELETED
PHASE_ROW_DELETED = RECOVERY_ROW_DELETED
PHASE_ARMED = RECOVERY_ARMED
#: `absent` is the pseudo-state for "no journal row"; it is not a column value.
PHASE_ABSENT = RECOVERY_ABSENT

PHASES = (PHASE_REQUESTED, PHASE_FILE_DELETED, PHASE_ROW_DELETED, PHASE_ARMED)

#: The `snapshot.mode` a recovery forces. Persisted, because the whole point of the
#: journal is that this intent outlives the process that formed it.
FORCED_SNAPSHOT_MODE = "initial"


#: Rubric 1.7's `<nth>` for the recovery anchors: a recovery normally happens once per
#: run, so `<nth>` is 1, but a run that arms a *second* recovery after resuming a first
#: one reaches the same boundary twice and a test must be able to name which.
def _reached(phase: str) -> int:
    return arrival(f"recovery:{phase}")


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
    #: The captured set this recovery took responsibility for, `["schema.table", ...]`.
    #: Persisted (Codex r1 MAJOR-5): completion used to be re-derived from *all* current
    #: lifecycle rows, so "the rebuild finished" was a statement about whatever the
    #: destination happens to hold now rather than about the obligation the recovery
    #: recorded. A table added to the include list mid-rebuild changed the answer.
    captured: list[str] = field(default_factory=list)
    #: For `--reset-state`: the state directory the reset must clear. `offset_path`
    #: alone is not enough, because "start over" means the whole Debezium scratch area.
    state_dir: str | None = None

    def as_dict(self) -> dict:
        return {
            "recovery_id": self.recovery_id,
            "decision": self.decision,
            "phase": self.phase,
            "slot": self.slot_name,
            "snapshot_mode": self.snapshot_mode,
            "forget_catalog": self.forget_catalog,
            "tables_marked": self.tables_marked,
            "captured": list(self.captured),
            "message": self.message,
        }


@dataclass
class Completion:
    """Whether a journalled recovery has actually finished, and why not if it has not.

    Returned by `complete_if_ready`, which is the ONE owner of that predicate
    (Codex r1 MAJOR-5 / open question 2). It used to be six lines inside
    `pipeline.run()` that read every lifecycle row and called `clear()` directly, so the
    recovery machine did not own its own terminal edge and a run could report `ok: true`
    with the journal still armed.
    """

    cleared: bool
    recovery_id: str
    still_owed: tuple[str, ...] = ()
    has_resume_point: bool = True
    reason: str = ""


def read(
    con,
    *,
    pipeline: str,
    namespace: str,
    control_schema: str | None = None,
) -> RecoveryRecord | None:
    """The recovery this pipeline is in the middle of, or None."""
    rows = con.execute(
        f"SELECT recovery_id, decision, phase, slot_name, offset_path, snapshot_mode, "
        f"       forget_catalog, tables_marked, message, captured_json, state_dir "
        f"FROM {control_table(resolve_control_schema(control_schema), 'recovery_state')} "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    ).fetchall()
    if not rows:
        return None
    (rid, decision, phase, slot, path, mode, forget, marked, message,
     captured_json, state_dir) = rows[0]
    if str(phase) not in PHASES:
        # `PHASES` was declared and never enforced: `read()` accepted any string and
        # `resume()` then matched none of its branches, fell through every `if`, and
        # logged "recovery is ARMED" while having done nothing at all - a silent no-op
        # wearing a success message (architecture review, finding 3). A phase we cannot
        # resume from is a loud failure; the row is still there for a human to read.
        raise RecoveryFailed(
            f"the recovery journal for pipeline={pipeline!r} namespace={namespace!r} "
            f"records phase {phase!r}, which is not one of {list(PHASES)}. Refusing to "
            "guess which durable mutations have already happened."
        )
    try:
        captured = list(json.loads(captured_json)) if captured_json else []
    except ValueError:  # pragma: no cover - a corrupted journal column
        log.error("recovery journal captured_json did not decode; treating as empty")
        captured = []
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
        captured=[str(c) for c in captured],
        state_dir=str(state_dir) if state_dir is not None else None,
    )


def _write_phase(
    con,
    *,
    pipeline: str,
    namespace: str,
    phase: str,
    frm: str,
    control_schema: str | None = None,
) -> None:
    """The ONE writer of `recovery_state.phase`, and it asserts the edge first.

    `PHASES` used to be declared and never consumed: `read()` accepted any string and
    `resume()` then matched none of its branches, fell through every `if`, and logged
    "recovery is ARMED" having done nothing at all. The domain check landed in the
    1.6-1.8 fix round; rubric 1.9 adds the *edges*, so a future caller cannot jump from
    `requested` straight to `armed` and claim a slot was dropped that never was.
    """
    ACQUISITION_RECOVERY.check(frm, phase)
    con.execute(
        f"UPDATE {control_table(resolve_control_schema(control_schema), 'recovery_state')} "
        "SET phase = ?, updated_at = ? "
        "WHERE pipeline = ? AND namespace = ?",
        [phase, now(), pipeline, namespace],
    )


def clear(
    con,
    *,
    pipeline: str,
    namespace: str,
    control_schema: str | None = None,
) -> None:
    """The recovery is done: the tables it owed have been snapshotted.

    `armed -> absent` is the ONLY declared edge into `absent`. Clearing from an earlier
    phase would discard the record of a destructive sequence that is still half-done -
    exactly the state the journal exists to name - so a caller that tries is refused.
    """
    record = read(
        con,
        pipeline=pipeline,
        namespace=namespace,
        control_schema=control_schema,
    )
    if record is None:
        return
    ACQUISITION_RECOVERY.check(record.phase, RECOVERY_ABSENT)
    con.execute(
        f"DELETE FROM {control_table(resolve_control_schema(control_schema), 'recovery_state')} "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    )
    runtime_state(recovery_phase=PHASE_ABSENT)


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
    state_dir: Path | None = None,
    severity: str = "critical",
    control_schema: str | None = None,
) -> RecoveryRecord:
    """Write the intent, atomically with the to-do list. NOTHING is destroyed here.

    The destination tables stay exactly as they are, still queryable, and each one is
    replaced only when its shadow is complete, in one transaction (D7). What this makes
    durable is *that a rebuild is owed and why*, before the first byte of anything else
    changes — so a crash one statement later is recognisable rather than mysterious.

    Idempotent: an existing journal row for this pipeline is replaced, because a second
    detection of the same unsafe state is the same recovery, not a new one.
    """
    existing = read(
        con,
        pipeline=pipeline,
        namespace=namespace,
        control_schema=control_schema,
    )
    ACQUISITION_RECOVERY.check(
        existing.phase if existing is not None else RECOVERY_ABSENT, RECOVERY_REQUESTED
    )
    captured = [f"{schema}.{table}" for schema, table, _target in captured_tables]
    record = RecoveryRecord(
        recovery_id=uuid.uuid4().hex,
        decision=decision,
        phase=PHASE_REQUESTED,
        slot_name=slot_name,
        offset_path=str(offset_path),
        snapshot_mode=FORCED_SNAPSHOT_MODE,
        forget_catalog=forget_catalog,
        message=message,
        captured=captured,
        state_dir=str(state_dir) if state_dir is not None else None,
    )
    raise_alert(
        con, pipeline=pipeline, severity=severity, code=decision,
        message=(
            f"{message}. Rebuilding every captured table from the source "
            f"({len(captured_tables)} tables); the destination stays queryable until "
            "each table's snapshot is complete and swapped in one transaction."
        ),
        context=(context or {}) | {"recovery_id": record.recovery_id},
        control_schema=control_schema,
    )
    con.execute("BEGIN TRANSACTION")
    try:
        if decision in RESET_TABLE_DECISIONS:
            # `--reset-state` first puts the per-table snapshot bookkeeping (epoch,
            # `snapshot_lsn`, `last_commit_id`) back to nothing, which is what "start
            # over" means for those columns.
            table_lifecycle.reset_all(
                con,
                pipeline=pipeline,
                reason=f"{decision}: {message}",
                control_schema=control_schema,
            )
        # ...and then EVERY captured table is owed a fresh image, reset included.
        #
        # The first cut of the journalled reset stopped at `reset_all()` and recorded
        # `tables_marked=0`, which made its obligation vacuous: `none` is not an owing
        # state, a captured table with no row at all was accepted too, and completion
        # skipped the resume-point requirement entirely. A source table that is EMPTY
        # emits no snapshot records, so nothing rebuilt it — and the reset cleared its
        # own journal, reported `ok: true`, and left the destination holding stale rows
        # the source no longer had (Codex r2 BLOCKER-1, reproduced end to end). An
        # obligation that any outcome satisfies is not an obligation.
        marked = request_snapshot(
            con,
            pipeline=pipeline,
            tables=captured_tables,
            detail=f"{decision}: {message}",
            control_schema=control_schema,
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
                f"DELETE FROM {control_table(resolve_control_schema(control_schema), 'source_relations')} "
                "WHERE pipeline = ?",
                [pipeline],
            )
            # The CLAIM about that registry goes with it, in the same transaction
            # (rubric 1.9/SM-E). Keeping a `stale`/`invalidated` mark about a catalog
            # that no longer exists would be worse than no mark: it would make the next
            # run reconcile the *replacement* registry against relations this recovery
            # has already marked for rebuild.
            catalog_baseline.forget(
                con, pipeline, control_schema=control_schema
            )
        con.execute(
            f"DELETE FROM {control_table(resolve_control_schema(control_schema), 'recovery_state')} "
            "WHERE pipeline = ? "
            "AND namespace = ?",
            [pipeline, namespace],
        )
        con.execute(
            f"INSERT INTO {control_table(resolve_control_schema(control_schema), 'recovery_state')} "
            "(pipeline, namespace, recovery_id, decision, phase, slot_name, "
            " offset_path, snapshot_mode, forget_catalog, tables_marked, message, "
            " captured_json, state_dir, requested_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                pipeline, namespace, record.recovery_id, decision, PHASE_REQUESTED,
                slot_name, str(offset_path), FORCED_SNAPSHOT_MODE, forget_catalog,
                marked, message, json.dumps(captured), record.state_dir, now(), now(),
            ],
        )
        con.execute("COMMIT")
        runtime_state(recovery_phase=PHASE_REQUESTED)
        matrix_crash("recovery_requested_recorded")
        # rubric 1.7: the journal row and the to-do list are now durable and NOTHING
        # has been destroyed. A crash here must leave a resumable `requested` journal.
        maybe_crash("recovery_requested", _reached(PHASE_REQUESTED))
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
    control_schema: str | None = None,
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
    runtime_state(recovery_phase=record.phase)
    offset_path = Path(record.offset_path) if record.offset_path else None

    if record.phase == PHASE_REQUESTED:
        # The offsets FILE first, and the durable row second (Opus MAJOR-1). The
        # reverse order leaves `row absent / file present`, which is the
        # `orphan_offset_file` refusal - the right answer for an operator who pointed
        # the DSN at the wrong database and the wrong one for our own half-finished
        # recovery. This order leaves `file absent / row present`, which reconciliation
        # simply rebuilds.
        removed = False
        if record.state_dir:
            # `--reset-state` means start over at the Debezium end too, and `offsets.dat`
            # is not the only scratch file in there. Removing the tree and recreating it
            # is idempotent: a crash between the two leaves no directory, which the next
            # run's `state_dir.mkdir(parents=True, exist_ok=True)` restores.
            directory = Path(record.state_dir)
            removed = offset_path is not None and offset_path.exists()
            shutil.rmtree(directory, ignore_errors=True)
            directory.mkdir(parents=True, exist_ok=True)
            result["state_dir"] = "cleared"
        elif offset_path is not None:
            removed = offset_path.exists()
            offset_path.unlink(missing_ok=True)
        result["offset_file"] = "removed" if removed else "absent"
        if on_phase is not None:
            on_phase(PHASE_FILE_DELETED)
        # rubric 1.7: the file is gone and the journal still says `requested`. This is
        # the cut A53's crash table calls benign (`file absent / row present` ->
        # `file_missing_rebuilt`), and it is now an INJECTED fault rather than a claim.
        maybe_crash("recovery_offsets_file_deleted", _reached(PHASE_FILE_DELETED))
        _write_phase(
            con, pipeline=pipeline, namespace=namespace,
            phase=PHASE_FILE_DELETED, frm=PHASE_REQUESTED,
            control_schema=control_schema,
        )
        record.phase = PHASE_FILE_DELETED
        runtime_state(recovery_phase=record.phase)
        matrix_crash("recovery_offsets_file_deleted_recorded")

    if record.phase == PHASE_FILE_DELETED:
        con.execute(
            f"DELETE FROM {control_table(resolve_control_schema(control_schema), 'debezium_offsets')} "
            "WHERE pipeline = ? "
            "AND namespace = ?",
            [pipeline, namespace],
        )
        if on_phase is not None:
            on_phase(PHASE_ROW_DELETED)
        # rubric 1.7: the durable resume point is gone and the slot is not. The next
        # acquisition must resume from `offsets_file_deleted` and re-run this step.
        maybe_crash("recovery_resume_point_deleted", _reached(PHASE_ROW_DELETED))
        _write_phase(
            con, pipeline=pipeline, namespace=namespace,
            phase=PHASE_ROW_DELETED, frm=PHASE_FILE_DELETED,
            control_schema=control_schema,
        )
        record.phase = PHASE_ROW_DELETED
        runtime_state(recovery_phase=record.phase)
        matrix_crash("recovery_resume_point_deleted_recorded")

    if record.phase == PHASE_ROW_DELETED:
        slot_action = _drop_the_slot_or_fail(
            drop_slot, dsn=dsn, slot_name=record.slot_name, record=record
        )
        result["slot"] = slot_action
        if on_phase is not None:
            on_phase(PHASE_ARMED)
        # rubric 1.7: the slot is dropped and the journal has not recorded it. The
        # dangerous one: a next run that could not tell would re-snapshot against a
        # surviving slot, or lose the forced snapshot mode entirely (Codex B3).
        maybe_crash("recovery_armed", _reached(PHASE_ARMED))
        _write_phase(
            con, pipeline=pipeline, namespace=namespace,
            phase=PHASE_ARMED, frm=PHASE_ROW_DELETED,
            control_schema=control_schema,
        )
        record.phase = PHASE_ARMED
        runtime_state(recovery_phase=record.phase)
        matrix_crash("recovery_armed_recorded")

    if record.phase != PHASE_ARMED:  # pragma: no cover - the ladder above is total
        raise RecoveryFailed(
            f"recovery {record.recovery_id} did not reach {PHASE_ARMED!r} (stopped at "
            f"{record.phase!r}); refusing to report an armed recovery that is not armed"
        )
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


def complete_if_ready(
    con,
    *,
    pipeline: str,
    namespace: str,
    record: RecoveryRecord,
    verified_empty: list[str] | tuple[str, ...] = (),
    control_schema: str | None = None,
) -> Completion:
    """Has the rebuild this journal demanded actually happened? Clear it if so.

    **The recovery machine owns its own terminal edge** (Codex r1 MAJOR-5, and the
    review's answer to open question 2). This predicate used to live inline in
    `pipeline.run()`, reading every current `table_state` row and calling `clear()`
    directly, which meant three things the machine could not defend:

    * the obligation was re-derived from whatever the destination holds *now* rather
      than from the captured set the journal recorded — so a table that joined or left
      the include list mid-rebuild changed the answer;
    * `clear()` was reachable from outside the module that declares
      `armed -> absent`;
    * a false predicate only added a summary key, so the run still reported success
      with a destructive sequence half-finished.

    The caller gets a typed result; `pipeline.run()` turns "not cleared" into a
    non-successful run.
    """
    states = table_lifecycle.read_all(
        con, pipeline, control_schema=control_schema
    )
    #: The captured set the JOURNAL recorded, falling back to everything the pipeline
    #: currently knows about for journals written before the column existed.
    obligation = list(record.captured) or sorted(states)
    # POSITIVE terminal evidence, per relation. `not in LIFECYCLE_OWING_WORK` was the
    # wrong test in both directions (Codex r2 BLOCKER-1): `none` means "registered,
    # never snapshotted", and a captured relation with no row at all means "nothing was
    # ever written for it" - and both passed. `complete` is the ONE state that says the
    # destination table holds a trustworthy image, and it is what a rebuild has to
    # produce; `finish_verified_empty_tables` is how a table that is genuinely empty at
    # the source reaches it.
    still_owed = tuple(
        name for name in obligation if states.get(name) != table_lifecycle.COMPLETE
    )
    has_resume = bool(
        con.execute(
            f"SELECT 1 FROM {control_table(resolve_control_schema(control_schema), 'debezium_offsets')} "
            "WHERE pipeline = ? AND namespace = ?",
            [pipeline, namespace],
        ).fetchall()
    )
    # A resume point proves the rebuilt image was handed over to a stream, and it is
    # required of EVERY recovery. The first cut exempted `--reset-state` on the grounds
    # that its obligation was only bookkeeping; that exemption is exactly what let a
    # reset clear itself having rebuilt nothing.
    #
    # There is ONE case where no such handoff can exist and demanding it is not
    # conservatism but a permanent stall: a capture set in which every relation was
    # PROVEN empty at the source on this run. Nothing streamed, so the applier committed
    # no group and wrote no resume point, and no future run can produce one either
    # (Codex r3 MAJOR-1). What stands in for it is stronger and per relation: each of
    # those tables carries the `snapshot_lsn` fence `finish_verified_empty_tables`
    # sampled BEFORE the counts that proved it empty.
    #
    # And it is deliberately NOT patched with a synthetic resume row. The first attempt
    # wrote one with an empty offset map, which is not an offset the connector can resume
    # from: the next run started with no offset at all, took an `initial` snapshot
    # against the SURVIVING slot, and delivered concurrently-written rows twice — once as
    # `r` and once as `c` (Codex r4 BLOCKER-1, reproduced). With no resume row the next
    # run is an honest `fresh_start`: the slot check sees an empty destination, arms a
    # recovery, and that recovery DROPS the slot, so Debezium creates its own and the
    # snapshot/stream boundary is exact (A45). Noisier, and correct.
    empty_discharged = bool(obligation) and set(obligation) <= set(verified_empty)
    if still_owed or not (has_resume or empty_discharged):
        reason = (
            f"{len(still_owed)} captured table(s) are not `complete` "
            f"({', '.join(still_owed)})" if still_owed
            else "the destination has no resume point, so the rebuilt image was never "
                 "handed over to a stream"
        )
        log.warning(
            "rubric 1.8 recovery %s is STILL ARMED: %s", record.recovery_id, reason
        )
        return Completion(
            cleared=False,
            recovery_id=record.recovery_id,
            still_owed=still_owed,
            has_resume_point=has_resume,
            reason=reason,
        )
    clear(
        con,
        pipeline=pipeline,
        namespace=namespace,
        control_schema=control_schema,
    )
    handoff = (
        "the destination has a resume point again" if has_resume
        else "every captured relation was PROVEN empty at the source, so there is no "
             "resume point and none can exist: the per-relation snapshot_lsn fence is "
             "the handoff evidence, and the next run is an honest fresh start"
    )
    log.warning(
        "rubric 1.8 recovery %s is COMPLETE: every captured table has a fresh image and "
        "%s", record.recovery_id, handoff,
    )
    return Completion(
        cleared=True, recovery_id=record.recovery_id, has_resume_point=has_resume,
        reason=(
            "every captured table reached a terminal lifecycle state" if has_resume
            else "every captured table was verified empty at the source"
        ),
    )


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
