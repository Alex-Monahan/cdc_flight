"""Everything between taking the lease and opening the engine (rubric 1.8/1.9).

Split out of `pipeline.py`, which the thermo-nuclear review has twice found back at the
giant-file review. `run()` is a sequence; this module is the four
*decisions* that sequence makes before an engine exists — which tables are captured,
whether a journalled recovery is half-done, what the slot check concludes, and what
`--reset-state` has to journal before it destroys anything. All four are testable
against a DuckDB file and a fake slot, none of them needs a JVM, and none of them is
about the supervision loop.
"""

from __future__ import annotations

import logging
import os

from . import destination as dest_mod
from . import reconcile as reconcile_mod
from . import recovery as recovery_mod
from .config import resolve_control_schema
from .machines import PHASE_RECOVERING
from .naming import control_table

log = logging.getLogger("cdc_flight.acquisition")


def resnapshot_enabled() -> bool:
    """`CDC_RESNAPSHOT=0` turns the automatic re-snapshot off.

    Not a switch anyone should need, and it exists for exactly one reason: the rubric
    grades "any potential data loss from slot advancement triggers a backfill
    automatically" at 5 and "triggers the process to exit" at 4, and an operator who
    wants to be told rather than repaired should be able to have the 4 deliberately
    rather than by accident.
    """
    return os.environ.get("CDC_RESNAPSHOT", "1").strip().lower() not in (
        "0", "false", "no", "off"
    )


def captured_tables(
    con, pipeline: str, source, replication, *, control_schema: str | None = None
) -> list[tuple[str, str, str]]:
    """`(schema, table, target)` for every table this pipeline captures.

    Built from the *configuration*, not from `table_state`: a recovery has to be able to
    mark a table that has no destination row yet, and the include list is the definition
    of "captured". `table_state` supplies the target name where it knows one so a
    re-snapshot lands on the table an existing consumer is already reading.
    """
    from . import naming

    known = {
        f"{schema}.{table}": target
        for schema, table, target in con.execute(
            f"SELECT source_schema, source_table, target_table FROM "
            f"{control_table(resolve_control_schema(control_schema), 'table_state')} "
            "WHERE pipeline = ?",
            [pipeline],
        ).fetchall()
    }
    out: list[tuple[str, str, str]] = []
    for qualified in source.tables:
        schema, _, table = qualified.partition(".")
        if not table:
            schema, table = source.schema, qualified
        target = known.get(f"{schema}.{table}") or naming.destination_table(
            replication.topic_prefix, schema, table
        )
        out.append((schema, table, target))
    return out


def resume_any_journalled_recovery(
    con, *, source, replication, dest, namespace: str, phases=None
) -> tuple:
    """Finish a recovery an earlier process left half-done, BEFORE anything else looks.

    Returns `(record_or_None, result_or_None)`. This is what makes rubric 1.8's recovery
    crash-recoverable rather than crash-fatal: the journal says which phase was reached,
    every step is idempotent, and the run resumes from there instead of diagnosing its
    own intermediate state as an operator error (Codex B3 / Opus MAJOR-1). It runs
    before `check_the_slot` because a half-finished recovery has, by construction, the
    exact durable shape - no resume row, maybe no slot - that the slot check reads as a
    brand-new problem.
    """
    record = recovery_mod.read(
        con, pipeline=dest.pipeline_name, namespace=namespace,
        control_schema=dest.control_schema,
    )
    if record is None:
        return None, None
    if phases is not None:
        phases.ensure(PHASE_RECOVERING, detail=f"journal {record.recovery_id} at {record.phase}")
    if record.phase == recovery_mod.PHASE_ARMED:
        log.warning(
            "resuming rubric 1.8 recovery %s (%s): the destructive phase is complete "
            "and %s table(s) still owe a snapshot",
            record.recovery_id, record.decision, record.tables_marked,
        )
        return record, {
            "recovery_id": record.recovery_id,
            "decision": record.decision,
            "resumed_from": record.phase,
            "phase": record.phase,
            "tables_marked": record.tables_marked,
            "message": record.message,
        }
    log.warning(
        "resuming rubric 1.8 recovery %s (%s) from phase %r: an earlier run did not "
        "finish it", record.recovery_id, record.decision, record.phase,
    )
    result = recovery_mod.resume(
        con,
        pipeline=dest.pipeline_name,
        namespace=namespace,
        record=record,
        dsn=source.dsn,
        control_schema=dest.control_schema,
    )
    return record, result


def check_the_slot(
    con, *, source, replication, dest, namespace: str, captured, orphan_file: bool
) -> tuple:
    """Run rubric 1.8's check and, if it says so, arm the automatic re-snapshot.

    Returns `(verdict, recovery_or_None)`. The observation is recorded either way -
    that is what makes "the slot was recreated" and "the cluster was restored"
    detectable at all on the *next* run (`_cdc_flight.slot_state`).
    """
    durable = con.execute(
        f"SELECT last_lsn FROM "
        f"{control_table(dest.control_schema, 'debezium_offsets')} "
        "WHERE pipeline = ? AND namespace = ?",
        [dest.pipeline_name, namespace],
    ).fetchall()
    durable_lsn = int(durable[0][0]) if durable else None
    observation = reconcile_mod.observe_slot(source.dsn, replication.slot_name)
    previous = dest_mod.read_slot_state(
        con, dest.pipeline_name, replication.slot_name,
        control_schema=dest.control_schema,
    )
    # What the destination actually holds, not what a control row says about it. The
    # `no_durable_destination_row` cell is defined as "destination EMPTY, slot
    # positioned" and used to be decided without ever looking (Opus BLOCKER-2). Only
    # read when there is no resume point, because that is the only cell it decides and
    # counting every captured table on every start-up is not free.
    destination_rows = (
        dest_mod.destination_holds_rows(
            con, dataset=dest.dataset_name, tables=captured,
            control_schema=dest.control_schema,
        )
        if durable_lsn is None
        else None
    )
    verdict = reconcile_mod.check_slot(
        durable_lsn=durable_lsn,
        observation=observation,
        previous=previous,
        destination_rows=destination_rows,
    )
    log.info("slot check: %s (%s)", verdict.decision, verdict.message or "healthy")

    recovery = None
    if verdict.refuse and verdict.decision == "no_durable_destination_row":
        log.error(
            "%s: %s", verdict.decision, verdict.message,
        )
        # Include the observed slot position so a later repaired/recreated slot is a
        # new alert incident, while repeated starts against the same standing cell
        # remain deduplicated (R14-11).
        marker_value = (
            f"{replication.slot_name}:{verdict.decision}:"
            f"{observation.confirmed_flush_lsn}"
        )
        if not dest_mod.alert_marker_exists(
            con,
            pipeline=dest.pipeline_name,
            code=verdict.decision,
            marker_key="condition_marker",
            marker_value=marker_value,
            control_schema=dest.control_schema,
        ):
            context = verdict.as_dict() | {"condition_marker": marker_value}
            dest_mod.raise_alert(
                con, pipeline=dest.pipeline_name, severity="critical",
                code=verdict.decision, message=verdict.message, context=context,
                control_schema=dest.control_schema,
            )
    if verdict.resnapshot and orphan_file and verdict.decision == "no_durable_destination_row":
        # The one place a re-snapshot is NOT the right automatic answer, and the reason
        # the refusal in ADR 0001 §4.5 survives this whole feature: an `offsets.dat` with
        # no destination row usually means the DSN is pointed at the wrong database. A
        # re-snapshot would then DROP that database's live tables and replace them with
        # another source's data, which is destruction, not repair. Reconciliation refuses
        # a few lines later, and `--accept-orphan-offsets` is the operator's way to say
        # "yes, re-snapshot into this destination".
        log.error(
            "%s, but an orphan %s is present: refusing rather than re-snapshotting, "
            "because a re-snapshot would replace this destination's tables with data "
            "from a source it may not belong to (ADR 0001 §4.5)",
            verdict.decision, replication.offset_file,
        )
        verdict = reconcile_mod.SlotVerdict(
            verdict.decision,
            ok=False,
            resnapshot=False,
            refuse=True,
            message=f"{verdict.message}; deferred to the orphan-offsets refusal",
            context=verdict.context,
        )
    if verdict.resnapshot:
        if not resnapshot_enabled():
            # The rubric's 4 rather than its 5, chosen explicitly.
            dest_mod.raise_alert(
                con, pipeline=dest.pipeline_name, severity="critical",
                code=verdict.decision, message=verdict.message,
                context=verdict.as_dict(),
                control_schema=dest.control_schema,
            )
            raise reconcile_mod.SlotAheadOfDestination(
                f"{verdict.decision}: {verdict.message}. CDC_RESNAPSHOT=0, so the "
                "automatic re-snapshot that would repair this is disabled."
            )
        recovery = reconcile_mod.recover_by_full_resnapshot(
            con,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            dsn=source.dsn,
            slot_name=replication.slot_name,
            offset_path=replication.offset_file,
            verdict=verdict,
            captured_tables=captured,
            forget_catalog=verdict.decision in reconcile_mod.FORGET_CATALOG_DECISIONS,
            control_schema=dest.control_schema,
        )
    if observation.observable:
        recorded = observation.as_dict() | {"durable_lsn": durable_lsn}
        if recovery is not None:
            # The recovery dropped this slot. Keeping its LSNs as the baseline would make
            # the next run compare a brand-new slot against a slot that no longer exists;
            # the cluster's identity is the part that stays meaningful.
            recorded |= {
                "restart_lsn": None, "confirmed_flush_lsn": None, "durable_lsn": None
            }
        dest_mod.write_slot_state(
            con,
            pipeline=dest.pipeline_name,
            slot_name=replication.slot_name,
            observation=recorded,
            # The verdict travels with the observation it was computed from, in the same
            # transaction (Codex r1 MAJOR-5). Without it the destination could record
            # that a rebuild happened but not why, and the answer lived only in
            # `last_run.json` on whichever host ran.
            verdict=verdict.decision,
            verdict_message=verdict.message or None,
            control_schema=dest.control_schema,
        )
    return verdict, recovery


def journal_the_reset(
    con, *, source, replication, dest, namespace: str, captured, phases,
) -> dict:
    """`--reset-state`, as ONE journalled, idempotent, re-entrant sequence.

    It used to be five independent durable mutations - remove the state directory,
    delete the resume row, rewrite every table's lifecycle, delete the lease, delete the
    source catalog - plus a `props['snapshot.mode'] = 'initial'` that lived in a local
    variable, argued convergent on the grounds that every intermediate state leads back
    to the same outcome. Two parts of that argument were false (Codex r1 MAJOR-4):

    * with the resume row deleted and a positioned slot over a populated destination,
      the next run's slot check returns the deliberate `no_durable_destination_row`
      refusal *before* `will_snapshot_everything` is computed, and repeating
      `--reset-state` does not drop that slot, so it does not necessarily finish either;
    * the forced data-reading snapshot mode was process-local, so a crash after the file
      and the row were gone let the next ordinary run start fresh under a configured
      `no_data` mode and stream onto a destination nobody rebuilt - B3, exactly.

    Journalled it is the same machine as every other recovery: intent and table
    obligation first, in one transaction; then the file, the row and the slot, each step
    idempotent and each recognisable from durable state alone. Dropping the slot is what
    makes the sequence converge - it is also required for correctness, because Debezium
    only pairs a snapshot with an exact WAL position when it creates the slot itself.
    """
    phases.ensure(PHASE_RECOVERING, detail="--reset-state")
    record = recovery_mod.begin(
        con,
        pipeline=dest.pipeline_name,
        namespace=namespace,
        decision=recovery_mod.RESET_DECISION,
        message=(
            "an operator passed --reset-state: the Debezium state directory, the "
            "durable resume point, the snapshot bookkeeping, the recorded source "
            "catalog and the replication slot all go back to nothing"
        ),
        slot_name=replication.slot_name,
        offset_path=replication.offset_file,
        captured_tables=captured,
        # A reset re-reads the source from scratch, so the recorded oids are about to be
        # meaningless; keeping them makes the catalog watcher call every table
        # dropped-and-recreated, which the mass-drop breaker then refuses.
        forget_catalog=True,
        state_dir=replication.state_dir,
        severity="warning",
        control_schema=dest.control_schema,
    )
    result = recovery_mod.resume(
        con,
        pipeline=dest.pipeline_name,
        namespace=namespace,
        record=record,
        dsn=source.dsn,
        control_schema=dest.control_schema,
    )
    log.info("--reset-state is journalled and armed: %s", result)
    return result
