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
from typing import TYPE_CHECKING

from .applier import Applier
from .config import RunConfig
from .errors import EngineFailure
from .source_health import SourceHealth

if TYPE_CHECKING:  # `engine` imports pydbzengine, which boots a JVM on import.
    from .engine import SupervisedDebeziumEngine

log = logging.getLogger("cdc_flight.supervisor")


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
    """
    started = time.monotonic()
    error_box: list[BaseException] = []
    final_poll_done = False
    drain_until = 0.0
    catalog_unresolved: list[str] = []

    def _run():
        try:
            engine.run()
        except BaseException as exc:
            error_box.append(exc)

    thread = threading.Thread(target=_run, name="debezium-engine", daemon=True)
    thread.start()

    stop_reason = "max_seconds"
    idle_blocked_by_source = 0
    source_dark_after: float | None = None
    close_hung = False
    try:
        while thread.is_alive():
            elapsed = time.monotonic() - started
            if elapsed >= run.max_seconds:
                stop_reason = "max_seconds"
                break
            if error_box or engine.failure is not None:
                stop_reason = "engine_error"
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
                stop_reason = "source_dark"
                source_dark_after = round(elapsed, 2)
                break
            # An explicit "the work this engine was started for is done" signal. Only
            # `cdc_flight.resnapshot` supplies one: a re-snapshot is finished the moment
            # its last shadow is swapped in, and waiting out an idle window instead
            # would add `--idle-seconds` to every recovery for nothing. Checked with the
            # applier NOT busy so a group in flight is never abandoned.
            if stop_when is not None and not handler.busy and stop_when():
                stop_reason = "work_done"
                break
            enough = handler.record_count >= run.min_records
            quiet = handler.seconds_since_last_batch >= run.idle_seconds
            # Never stop before the connector has had a chance to start, and
            # never stop while a commit group is being applied.
            warmed_up = elapsed >= min(run.idle_seconds, 5.0)
            if enough and quiet and warmed_up and not handler.busy:
                if health is None or health.may_declare_idle(min_seconds=run.idle_seconds):
                    if catalog is not None and not final_poll_done:
                        # The synchronous final poll. A DROP that happened after the
                        # last scheduled poll is seen by THIS run, and it is also the
                        # poll that completes `CDC_DROP_CONFIRM_POLLS` on a short run.
                        final_poll_done = True
                        catalog.poll_quietly()
                        drain_until = time.monotonic() + catalog_drain_seconds
                    unresolved = (
                        [c.qualified for c in catalog.pending_destructive()]
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
                    stop_reason = "idle"
                    break
                idle_blocked_by_source += 1
                if idle_blocked_by_source % 20 == 1:
                    log.warning(
                        "stream quiet for %.1fs but the source disagrees it is idle: %s",
                        handler.seconds_since_last_batch,
                        health.summary(),
                    )
            time.sleep(0.25)
        else:
            stop_reason = "engine_finished"
    finally:
        intentional = stop_reason != "engine_error" and handler.error is None and not error_box
        log.info("closing debezium engine (reason=%s, intentional=%s)", stop_reason, intentional)

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
            if stop_reason not in ("source_dark", "engine_error"):
                stop_reason = "hung"
        thread.join(timeout=60)
        if thread.is_alive():
            log.error("debezium engine thread did not stop within 60s")
            close_hung = True
            if stop_reason not in ("source_dark", "engine_error"):
                stop_reason = "hung"
        # ADR 0001 §3.2: the un-ENDed tail is DISCARDED, never guessed at. It is
        # safe to discard precisely because Invariant O means the offset store
        # still points before it, so it replays on the next run.
        discarded = handler.drain_on_shutdown()
        if discarded:
            log.info("discarded %s un-committed tail events at shutdown", discarded)

    summary = {
        "stop_reason": stop_reason,
        "elapsed_sec": round(time.monotonic() - started, 2),
        "records": handler.record_count,
        "batches": handler.batch_count,
        "data_batches": handler.data_batch_count,
        "skipped": handler.skipped_count,
        "tables": handler.snapshot_counts(),
        "offset_flushes_verified": engine.offset_flushes_verified,
        **handler.stats(),
    }
    if close_hung:
        # Recorded even when it is not the reported reason, because "we could not shut
        # the engine down" is operationally interesting whatever caused it.
        summary["close_hung"] = True
    if source_dark_after is not None:
        # The number RUBRIC_STATUS's `CDC_SOURCE_DARK_SECONDS` claim rests on, measured
        # at the moment of detection rather than inferred from when the process happened
        # to exit - the process still has to tear down a JVM whose connector is blocked
        # on a dead socket, which is another minute (Opus MINOR-5).
        summary["source_dark_detected_after_sec"] = source_dark_after
    if health is not None:
        summary.update(health.summary())
    if engine.suppressed_message:
        summary["suppressed_engine_message"] = engine.suppressed_message

    failure = engine.failure
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
        summary["stop_reason"] = "engine_error"
        raise EngineFailure(message, summary) from cause

    if stop_reason == "hung":
        raise EngineFailure("debezium engine thread did not stop within 60s", summary)

    if stop_reason == "source_dark":
        raise EngineFailure(
            f"the source has been unreachable for {health.unknown_for:.1f}s "
            f"({health.summary()}); the delivery cannot be shown to be complete, so "
            "this run is not a success (TODO 4.6(b))",
            summary,
        )

    if stop_reason == "engine_finished" and not engine_terminates_normally:
        raise EngineFailure(
            "the Debezium engine terminated before the supervisor requested a stop "
            f"(completion success={engine.completed_success}); in streaming mode "
            "that is engine death, not a clean finish",
            summary,
        )

    if health is not None and stop_reason == "max_seconds":
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

    if catalog is not None:
        still_pending = [c.qualified for c in catalog.pending_destructive()]
        if still_pending or catalog_unresolved:
            names = sorted(set(still_pending) | set(catalog_unresolved))
            summary["stop_reason"] = "catalog_unresolved"
            summary["catalog_unresolved_tables"] = names
            # Codex 6: deferring is the correct *safety* choice - a destructive action
            # whose fence has not opened must not be guessed past - but it is not
            # faithful propagation and it is not honest to call the run successful.
            # The most common cause is a source that cannot be written to (a read-only
            # replica, a missing privilege), which `catalog_marker_error` names.
            raise EngineFailure(
                f"{len(names)} destructive source-catalog change(s) are still "
                f"unresolved at shutdown ({', '.join(names)}): the destination is "
                "knowingly out of step with the source. Most often the WAL fence "
                "marker could not be written to the source (see "
                f"catalog_marker_error={summary.get('catalog_marker_error')!r}), so no "
                "LSN past the detection point can be proven to have flowed",
                summary,
            )

    summary["ok"] = True
    return summary
