"""The bounded engine runner: how a run decides it is finished, or that it failed.

Extracted from `pipeline.py` (Codex B6). `pipeline.run()` had grown to own acquisition
observation, destructive recovery, snapshot-mode override, re-snapshot orchestration,
epoch repair, catalog start-up, engine lifecycle **and** the supervision loop in one
nested routine. The supervision loop is the part with the most independent reasons to
change and the only part `cdc_flight.resnapshot` also uses, so it is the natural seam.

Four independent things can go wrong and all four must reach the caller; the docstring
below is the contract.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from . import table_lifecycle
from .applier import Applier
from .config import RunConfig
from .errors import EngineFailure
from .machines import PHASE_DRAINING
from .run_state import RunOutcome
from .snapshot_completion import SnapshotCompletion
from .source_health import SourceHealth

if TYPE_CHECKING:  # `engine` imports pydbzengine, which boots a JVM on import.
    from .engine import SupervisedDebeziumEngine

log = logging.getLogger("cdc_flight.supervisor")


@dataclass(frozen=True)
class QuiescenceProof:
    """The supervisor's completed callback-boundary verdict.

    It is published from the shutdown ``finally`` itself so a pending
    ``BaseException`` cannot skip the ownership transition by bypassing summary
    construction below that block.
    """

    applier_quiesced: bool


def run_engine_bounded(
    engine: SupervisedDebeziumEngine,
    handler: Applier,
    run: RunConfig,
    health: SourceHealth | None = None,
    *,
    engine_terminates_normally: bool = False,
    catalog=None,
    catalog_drain_seconds: float = 30.0,
    stop_when=None,
    completion: SnapshotCompletion | None = None,
    phases=None,
    outcome: RunOutcome | None = None,
    quiescence_observer=None,
    keep_catalog: bool = False,
) -> dict:
    """Run the Debezium engine until the *source* agrees it is idle, or the deadline hits.

    Four independent things can go wrong, and all four must reach the caller:

    * `engine.run()` raises on this process's engine thread (rare);
    * the applier raises (captured by `handler.error`);
    * the *engine itself* fails - a connector that cannot start, or a streaming
      error. Debezium reports that through its `CompletionCallback` and returns
      normally, so it is only visible via `SupervisedDebeziumEngine.failure`;
    * the connector stops streaming *without failing* - a retriable exception
      puts it into a restart backoff that is longer than our idle window, so an
      idle timer alone reports success on a partial delivery (Opus B5). `health`
      corroborates "quiet" against `pg_replication_slots`.

    Before a quiet run is allowed to shut down there is one more barrier (Codex 6):
    a **synchronous final catalog poll**, and then a bounded wait for any destructive
    change it queued to be fenced and applied. Without it a `DROP TABLE` on a quiet
    source normally could not be seen until the *next* scheduled run - the watcher
    polls every 10 s while the idle window is 8 s - which makes "detected in 10
    seconds" misleading. A change that is still unresolved when the barrier expires
    makes the run **non-successful**: the destination is knowingly out of step with the
    source, and reporting `ok: true` on that is not honest.

    **The outcome is a `RunOutcome`, not a string** (rubric 1.9). It used to be assigned
    by plain `=` in eight places including inside the `finally` below, with the
    cause-before-symptom rule written out as `if stop_reason not in ("source_dark",
    "engine_error")` — twice. A49 measured what that costs: a dark source makes
    `engine.close()` hang almost by definition, so the `finally` replaced the diagnosis
    with the consequence and a blackholed Postgres was reported as `hung`. The
    precedence is now declared once, in `machines.RUN_OUTCOME`, and a downgrade is an
    edge that does not exist rather than a tuple somebody has to remember to extend.

    `phases` is an optional `run_state.RunPhaseWriter`: the supervisor owns exactly one
    phase transition, `streaming -> draining`, because this is the only place that knows
    when the engine stopped producing and started shutting down.

    `keep_catalog` is used only by the in-process discovery hand-off. It leaves the
    catalog watcher alive after this engine instance is quiescent so the caller can
    stop it, run the existing blocking re-snapshot, and attach a fresh engine to the
    same main slot. No final catalog verdict is taken for an intermediate hand-off.

    `outcome` is the run's **one** `RunOutcome`. It used to be constructed here while
    `RunPhaseWriter` constructed a second, unrelated one, so `last_run.json` shipped
    `stop_reason="idle"` next to `run_outcome="max_seconds"` on ordinary successful runs
    and the two owners could disagree about how badly a run had gone (Codex r1 MAJOR-2).
    The caller passes `phases.outcome`; the default keeps this function usable alone.
    """
    # Production callers pass the policy selected during acquisition. The default keeps
    # this low-level helper useful for streaming-only fakes without inventing a second
    # completion definition.
    completion = completion or SnapshotCompletion.streaming_only()
    started = time.monotonic()
    error_box: list[BaseException] = []
    final_poll_done = False
    quiesced = True
    applier_quiesced = True
    drain_until = 0.0
    catalog_unresolved: list[str] = []
    intermediate_handoff = False
    initial_durable_lsn = int(
        getattr(getattr(handler, "resume_point", None), "last_lsn", 0) or 0
    )
    acknowledgement_timeout: dict[str, int | float] | None = None

    def pending_fenced():
        if catalog is None:
            return []
        method = getattr(catalog, "pending_fenced", None)
        return method() if method is not None else catalog.pending_destructive()

    def pending_admission():
        if catalog is None:
            return []
        method = getattr(catalog, "pending_admission", None)
        return list(method()) if method is not None else []

    def _run():
        try:
            engine.run()
        except BaseException as exc:
            error_box.append(exc)

    thread = threading.Thread(target=_run, name="debezium-engine", daemon=True)
    thread.start()

    # rubric 1.9: a precedence, not a string. `record()` keeps the most severe value it
    # has been given, so no later assignment can overwrite an earlier diagnosis. ONE
    # per run, shared with the phase writer that publishes it (Codex r1 MAJOR-2).
    outcome = outcome if outcome is not None else RunOutcome("max_seconds")
    idle_blocked_by_source = 0
    idle_candidate_since: float | None = None
    source_dark_after: float | None = None
    close_hung = False
    try:
        while thread.is_alive():
            elapsed = time.monotonic() - started
            if elapsed >= run.max_seconds:
                outcome.record("max_seconds")
                break
            if error_box or engine.failure is not None:
                outcome.record("engine_error")
                break
            # TODO 4.6(b): a source that was answering and has gone completely dark
            # is a failure with a bounded detection time, not something to discover
            # when --max-seconds happens to expire. `ever_sampled` keeps an
            # environment where the slot could never be read (no psycopg, no
            # privilege) on the old timer-only path.
            if (
                health is not None
                and run.source_dark_seconds > 0
                and health.ever_sampled
                and health.unknown_for >= run.source_dark_seconds
            ):
                outcome.record("source_dark")
                source_dark_after = round(elapsed, 2)
                break
            # An explicit "the work this engine was started for is done" signal. Only
            # `cdc_flight.resnapshot` supplies one: a re-snapshot is finished the moment
            # its last shadow is swapped in, and waiting out an idle window instead
            # would add `--idle-seconds` to every recovery for nothing. Checked with the
            # applier NOT busy so a group in flight is never abandoned.
            if stop_when is not None and not handler.busy and stop_when():
                outcome.record("work_done")
                intermediate_handoff = bool(keep_catalog)
                break
            enough = handler.record_count >= run.min_records
            quiet = handler.seconds_since_last_batch >= run.idle_seconds
            # Never stop before the connector has had a chance to start, and
            # never stop while a commit group is being applied.
            warmed_up = elapsed >= min(run.idle_seconds, 5.0)
            if enough and quiet and warmed_up and not handler.busy:
                source_idle = (
                    health is None
                    or health.may_declare_idle(min_seconds=run.idle_seconds)
                )
                if source_idle and completion.phase_ended:
                    # The asynchronous slot sampler's last active observation can
                    # race a walsender termination at the exact idle boundary. Keep
                    # an idle candidate open for at least two sampler intervals and
                    # require a fresh observation taken after the candidate began.
                    # This is a state proof, not another arbitrary workload sleep.
                    health_interval = getattr(health, "interval", None)
                    if health is not None and health_interval is not None:
                        if idle_candidate_since is None:
                            idle_candidate_since = time.monotonic()
                            time.sleep(0.05)
                            continue
                        candidate_age = time.monotonic() - idle_candidate_since
                        latest = getattr(health, "last", None)
                        latest_at = getattr(latest, "at", None)
                        required_confirmation = max(float(health_interval) * 2.0, 1.0)
                        if (
                            candidate_age < required_confirmation
                            or latest_at is None
                            or latest_at < idle_candidate_since
                        ):
                            time.sleep(0.05)
                            continue
                        # The source may have changed state after the first
                        # ``source_idle`` calculation. Re-run the actual proof at
                        # the end of the candidate window; freshness alone cannot
                        # turn a walsender kill that happened during this window
                        # into an idle verdict.
                        if not health.may_declare_idle(min_seconds=run.idle_seconds):
                            idle_candidate_since = None
                            time.sleep(0.05)
                            continue
                    idle_candidate_since = None
                    if catalog is not None and not intermediate_handoff and not final_poll_done:
                        # The synchronous final poll. A DROP that happened after the
                        # last scheduled poll is seen by THIS run, and it is also the
                        # poll that completes `CDC_DROP_CONFIRM_POLLS` on a short run.
                        final_poll_done = True
                        catalog.poll_quietly()
                        drain_until = time.monotonic() + catalog_drain_seconds
                    unresolved = (
                        [c.qualified for c in pending_fenced()]
                        if catalog is not None
                        else []
                    )
                    if unresolved and time.monotonic() < drain_until:
                        # The drain barrier: the fence marker has been emitted, so a
                        # WAL record past the detection point is on its way and the
                        # applier will apply the change on the group that carries it.
                        if idle_blocked_by_source % 40 == 0:
                            log.info(
                                "holding the engine open for %s unresolved destructive "
                                "catalog change(s): %s",
                                len(unresolved), ", ".join(sorted(unresolved)),
                            )
                        idle_blocked_by_source += 1
                        time.sleep(0.25)
                        continue
                    catalog_unresolved = unresolved
                    outcome.record("idle")
                    break
                idle_candidate_since = None
                # A quiet stream that the source refuses to call idle is NOT a
                # verdict yet.  A walsender that has detached — including one that
                # detached and has not re-confirmed a position — is exactly the
                # state Debezium's own retriable-restart backoff exists to repair,
                # and that backoff is measured in tens of seconds.  Ending the run
                # here would abandon a connector that is about to reattach, deliver
                # nothing, advance neither LSN, and leave the WAL to grow: a
                # transient source blip would become a permanently failing run.
                # The safety property r11 asked for does not need an early exit —
                # `may_declare_idle` already refuses to call this state idle, so a
                # run in it CANNOT report success. It burns its own `--max-seconds`
                # budget instead, and the end-of-run verdict below turns the
                # unrepaired case into a loud failure with the delivery named.
                idle_blocked_by_source += 1
                if not source_idle and idle_blocked_by_source % 20 == 1:
                    log.warning(
                        "stream quiet for %.1fs but the source disagrees it is idle: %s",
                        handler.seconds_since_last_batch,
                        health.summary(),
                    )
            else:
                idle_candidate_since = None
            time.sleep(0.25)
        else:
            outcome.record("engine_finished")
    finally:
        # The destination commit and Debezium acknowledgement are deliberately
        # separate.  PostgreSQL receives the slot feedback on the connector's next
        # poll, so closing the engine at the first quiet instant can leave
        # confirmed_flush_lsn behind a durable MotherDuck commit.  Keep the live
        # connector open for one bounded feedback hand-off before sealing callback
        # admission.  A timeout is a failed proof, not a successful run with a
        # merely lagging slot.
        durable_lsn = int(
            getattr(getattr(handler, "resume_point", None), "last_lsn", 0) or 0
        )
        if (
            health is not None
            and durable_lsn > initial_durable_lsn
            and health.ever_sampled
            and not getattr(getattr(handler, "cfg", None), "resnapshot", False)
            and outcome.value not in {"source_dark", "hung"}
        ):
            wait_seconds = min(run.close_timeout, 10.0)
            marker_emitted = False
            if not health.confirmed_at_least(durable_lsn):
                # A quiet source does not necessarily deliver another poll after
                # markBatchFinished().  Give the live connector one whole,
                # offset-only PostgreSQL transaction to carry the already durable
                # destination position to the slot.  The write is on the explicit
                # primary route, never Debezium's replication connection.
                marker_emitted = health.emit_idle_marker(durable_lsn)
            if not health.wait_for_confirmed(durable_lsn, timeout=wait_seconds):
                sample = health.last
                acknowledgement_timeout = {
                    "durable_lsn": durable_lsn,
                    "confirmed_pos": (
                        sample.confirmed_pos
                        if sample is not None
                        else None
                    ),
                    "wait_seconds": wait_seconds,
                    "marker_emitted": marker_emitted,
                }
                outcome.record("engine_error")
                log.error(
                    "the source slot did not confirm durable LSN %s within %.1fs "
                    "(observed=%s)",
                    durable_lsn,
                    wait_seconds,
                    acknowledgement_timeout["confirmed_pos"],
                )
        # Seal admission BEFORE shutdown does anything that can touch the destination.
        # New callbacks are now recorded no-ops. An already-admitted callback still owns
        # the connection until `wait_for_quiescence()` proves it has left.
        handler.shutdown(reason="supervisor_shutdown")
        intentional = (
            outcome.value != "engine_error"
            and getattr(handler, "error", None) is None
            and not error_box
        )
        log.info(
            "closing debezium engine (reason=%s, intentional=%s)", outcome.value, intentional
        )
        closer = threading.Thread(
            target=engine.close,
            kwargs={"intentional": intentional},
            name="debezium-close",
            daemon=True,
        )
        closer.start()
        closer.join(timeout=run.close_timeout)
        if closer.is_alive():
            log.error("engine.close() did not return within %ss", run.close_timeout)
            close_hung = True
            # The CAUSE, not the symptom. A source that has gone dark makes
            # `engine.close()` hang almost by definition - the connector is waiting on a
            # socket that will never answer - and overwriting `source_dark` with `hung`
            # reported the consequence and lost the diagnosis. The same principle the
            # error path already applies to `EngineFailure`'s cause ordering.
            outcome.record("hung")
        thread.join(timeout=60)
        if thread.is_alive():
            log.error("debezium engine thread did not stop within 60s")
            close_hung = True
            outcome.record("hung")
        applier_quiesced = handler.wait_for_quiescence(timeout=run.close_timeout)
        proof = QuiescenceProof(applier_quiesced=applier_quiesced)
        if quiescence_observer is not None:
            # Publish immediately after the bounded proof. In particular, keep this
            # inside the `finally`: KeyboardInterrupt/SystemExit pending from the main
            # body resume unwinding as soon as this block ends and skip the summary.
            quiescence_observer(proof)
        if not applier_quiesced:
            log.error(
                "an admitted Debezium callback did not leave within %ss; retaining the "
                "destination runtime for that callback and refusing teardown",
                run.close_timeout,
            )
            close_hung = True
            outcome.record("hung")
        if catalog is not None and not intermediate_handoff:
            # QUIESCED BEFORE ANY VERDICT IS TAKEN (Codex r2 MAJOR-3), and quiescence is
            # now something we PROVE rather than something `stop()` attempts (Codex r3
            # MAJOR-3). The poller runs on its own thread; a poll that outlives a timed
            # join can take an undeclared transition, or learn a relation, after the
            # checks below have already concluded the run was a success. `stop()` returns
            # whether the thread is actually dead, and a false is a failed run — see the
            # `catalog_quiesced` check after the summary is built.
            quiesced = catalog.stop()
        if applier_quiesced and phases is not None and not intermediate_handoff:
            # This cursor is a child of the applier's parent connection. It is safe to
            # write `draining` only after the callback boundary is quiescent.
            try:
                phases.to(PHASE_DRAINING, detail=f"stop_reason={outcome.value}")
            except Exception:  # pragma: no cover - the edge is declared
                log.error("could not record the draining phase", exc_info=True)
        # ADR 0001 §3.2: the un-ENDed tail is DISCARDED, never guessed at. It is
        # safe to discard precisely because Invariant O means the offset store
        # still points before it, so it replays on the next run.
        if applier_quiesced:
            discarded = handler.drain_on_shutdown()
            if discarded:
                log.info("discarded %s un-committed tail events at shutdown", discarded)

    summary = {
        "stop_reason": outcome.value,
        "elapsed_sec": round(time.monotonic() - started, 2),
        "records": handler.record_count,
        "batches": handler.batch_count,
        "data_batches": handler.data_batch_count,
        "skipped": handler.skipped_count,
        "tables": handler.snapshot_counts(),
        "offset_flushes_verified": engine.offset_flushes_verified,
        **handler.stats(),
    }
    summary.update(completion.as_dict())
    if close_hung:
        # Recorded even when it is not the reported reason, because "we could not shut
        # the engine down" is operationally interesting whatever caused it.
        summary["close_hung"] = True
    summary["applier_quiesced"] = applier_quiesced
    if not applier_quiesced:
        summary["destination_owner"] = "live_applier_callback"
    if source_dark_after is not None:
        # The number RUBRIC_STATUS's `CDC_SOURCE_DARK_SECONDS` claim rests on, measured
        # at the moment of detection rather than inferred from when the process happened
        # to exit - the process still has to tear down a JVM whose connector is blocked
        # on a dead socket, which is another minute (Opus MINOR-5).
        summary["source_dark_detected_after_sec"] = source_dark_after
    if health is not None:
        summary.update(health.summary())
    if acknowledgement_timeout is not None:
        summary["slot_acknowledgement_timeout"] = acknowledgement_timeout
    if engine.suppressed_message:
        summary["suppressed_engine_message"] = engine.suppressed_message

    failure = engine.failure
    if acknowledgement_timeout is not None:
        raise EngineFailure(
            "the destination committed through durable LSN "
            f"{acknowledgement_timeout['durable_lsn']}, but the live source slot "
            "did not confirm that LSN before shutdown; refusing to report a "
            "successful or contained run",
            summary,
        )
    if not applier_quiesced:
        raise EngineFailure(
            "an admitted Debezium callback did not quiesce after callback admission "
            "was sealed; the live callback retains exclusive ownership of the destination "
            "runtime and teardown is refused",
            summary,
        )
    if error_box or handler.error is not None or failure is not None:
        cause = error_box[0] if error_box else handler.error
        # OUR exception is the root cause; Debezium's is the consequence. It used to
        # be the other way round, and the consequence is always the more generic
        # message: a destination write that failed mid-transaction was reported as
        # `java.lang.InterruptedException` (pydbzengine interrupts the engine thread
        # when the handler raises), which makes `last_run.json` accurate about *that
        # something failed* and wrong about what. Rubric 1.7 asks for an accurate
        # summary, so both are reported, cause first.
        parts = []
        if cause is not None:
            parts.append(f"{type(cause).__name__}: {cause}")
            summary["error_cause_type"] = type(cause).__name__
        if failure is not None:
            parts.append(f"debezium engine: {failure}")
        message = " | ".join(parts) or "the engine failed without a message"
        outcome.record("engine_error")
        summary["stop_reason"] = outcome.value
        raise EngineFailure(message, summary) from cause

    if outcome.value == "hung":
        raise EngineFailure("debezium engine thread did not stop within 60s", summary)

    if outcome.value == "source_dark":
        raise EngineFailure(
            f"the source has been unreachable for {health.unknown_for:.1f}s "
            f"({health.summary()}); the delivery cannot be shown to be complete, so "
            "this run is not a success (TODO 4.6(b))",
            summary,
        )

    if outcome.value == "engine_error":
        # A fail-closed backstop. Every engine_error origin above raises with its own
        # named cause; reaching here means one was recorded with no diagnosis, which
        # must never become a successful run.
        raise EngineFailure(
            "the run was classified as an engine error with no more specific cause "
            f"({health.summary() if health is not None else 'no source health'})",
            summary,
        )

    if completion.required and not completion.completed:
        raise EngineFailure(
            "the required snapshot did not complete before the engine stopped; "
            "source-idle timing is not positive evidence that Debezium delivered the "
            "snapshot terminal signal",
            summary,
        )

    if outcome.value == "engine_finished" and not engine_terminates_normally:
        raise EngineFailure(
            "the Debezium engine terminated before the supervisor requested a stop "
            f"(completion success={engine.completed_success}); in streaming mode "
            "that is engine death, not a clean finish",
            summary,
        )

    if health is not None and outcome.value == "max_seconds":
        not_streaming_for = health.not_streaming_for
        if not_streaming_for >= run.idle_seconds:
            raise EngineFailure(
                "reached --max-seconds while the connector was not streaming for "
                f"{not_streaming_for:.1f}s ({health.summary()}); the delivery is "
                "incomplete, so this run is not a success",
                summary,
            )
        # A source that has gone dark reports `not_streaming_for` now (it used to be
        # reset by every `unknown` sample), but say so explicitly: "I could not ask"
        # and "I asked and it was idle" must never share an exit code.
        if health.ever_sampled and health.unknown_for >= run.idle_seconds:
            raise EngineFailure(
                "reached --max-seconds while the source could not be consulted for "
                f"{health.unknown_for:.1f}s ({health.summary()})",
                summary,
            )

    if catalog is not None and not quiesced:
        # A verdict taken over a thread that is still running is not a verdict
        # (Codex r3 MAJOR-3). Everything below — the machine-error check, the unresolved
        # destructive changes, and the caller's flush of learned relations — reads state
        # the live poller can still mutate. Fail closed.
        outcome.record("engine_error")
        summary["stop_reason"] = outcome.value
        summary["catalog_quiesced"] = False
        raise EngineFailure(
            "the source-catalog poller did not stop, so its state can still change "
            "after this run is judged: neither the undeclared-transition check nor the "
            "pending-change check can be trusted, and the relations it learned must not "
            "be persisted over a live writer. Refusing to report success",
            summary,
        )

    if (
        catalog is not None
        and getattr(catalog, "poll_seconds", 0) > 0
        and not getattr(catalog, "successful_polls", 1)
    ):
        # A run that never read the source catalog ONCE has no baseline: it cannot have
        # noticed a `DROP TABLE`, and it has nothing to persist, so reporting success
        # says "I checked and everything is fine" when nothing was checked. The
        # consequence was measured (Codex r4 BLOCKER-2): with every poll timing out, a
        # quiet run returned `ok=true` and learned zero relations; an offline
        # drop-and-recreate then left the old relation's rows beside the new one's for
        # ever, because the following runs adopted the replacement generation as the baseline
        # they had never had. Proving the poller is DEAD is not the same as proving it
        # ever SPOKE.
        outcome.record("engine_error")
        summary["stop_reason"] = outcome.value
        summary["catalog_successful_polls"] = 0
        raise EngineFailure(
            "the source catalog could not be read even once during this run "
            f"(last error: {getattr(catalog, 'last_error', None)!r}), so a dropped or "
            "recreated relation could not have been detected and no relation baseline "
            "can be persisted. Refusing to report success over an unchecked catalog",
            summary,
        )

    if catalog is not None and getattr(catalog, "machine_error", None):
        # A51 row 51, as a policy rather than a promise. A catalog change that moved
        # along an edge `machines.CATALOG_CHANGE` does not declare is a destructive DDL
        # nobody reasoned about; `poll_quietly` used to write it to `last_error` and let
        # the run report success (Codex r1 MAJOR-1).
        outcome.record("engine_error")
        summary["stop_reason"] = outcome.value
        summary["catalog_machine_error"] = catalog.machine_error
        raise EngineFailure(
            "the source-catalog state machine took an undeclared transition during "
            f"this run ({catalog.machine_error}); a DDL fact moved through the "
            "observe -> confirm -> fence -> apply pipeline along a path nobody "
            "declared, so this run is not a success (ADR 0001 §19/A51 row 51)",
            summary,
        )

    if catalog is not None and not intermediate_handoff:
        still_pending = [c.qualified for c in pending_fenced()]
        admission_pending = pending_admission()
        if still_pending or catalog_unresolved or admission_pending:
            names = sorted(
                set(still_pending) | set(catalog_unresolved) | set(admission_pending)
            )
            outcome.record("catalog_unresolved")
            summary["stop_reason"] = outcome.value
            summary["catalog_unresolved_tables"] = names
            if admission_pending:
                summary["catalog_publication_admission_pending"] = admission_pending
            # Codex 6: deferring is the correct *safety* choice - a destructive action
            # whose fence has not opened must not be guessed past - but it is not
            # faithful propagation and it is not honest to call the run successful.
            # The most common cause is a source that cannot be written to (a read-only
            # replica, a missing privilege), which `catalog_marker_error` names.
            raise EngineFailure(
                f"{len(names)} source-catalog obligation(s) are still "
                f"unresolved at shutdown ({', '.join(names)}): the destination is "
                "knowingly out of step with the source. Most often the WAL fence "
                "marker could not be written to the source (see "
                f"catalog_marker_error={summary.get('catalog_marker_error')!r}), so no "
                "LSN past the detection point can be proven to have flowed",
                summary,
            )

    is_resnapshot = bool(
        getattr(getattr(handler, "cfg", None), "resnapshot", False)
    )
    snapshot_required = bool(
        getattr(handler, "snapshot_completion_required", False)
    )
    if not intermediate_handoff and not is_resnapshot and not snapshot_required:
        # Lifecycle trust is independent of row presence and of whether a catalog
        # watcher was attached to this bounded engine. Do not let a target that is
        # empty, absent, or merely marked for rebuild pass the engine-level success
        # gate.
        con = getattr(handler, "con", None)
        pipeline = getattr(handler, "pipeline", None)
        owing = (
            table_lifecycle.owing_work(
                con,
                pipeline,
                control_schema=getattr(handler, "control_schema", None),
            )
            if con is not None and pipeline is not None
            else []
        )
        if owing:
            outcome.record("catalog_unresolved")
            summary["stop_reason"] = outcome.value
            summary["tables_awaiting_snapshot_unhandled"] = owing
            raise EngineFailure(
                "the destination still owes a table lifecycle rebuild at engine "
                "shutdown: " + ", ".join(owing),
                summary,
            )

    summary["ok"] = True
    return summary
