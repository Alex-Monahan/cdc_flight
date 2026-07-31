"""cdc_flight baseline pipeline: Postgres -> Debezium embedded engine -> dlt -> DuckDB / MotherDuck.

Run it as a bounded job:

    cdc-flight --destination duckdb --max-seconds 60

The engine runs on a background thread; the main thread supervises it and closes
it once the change stream has been quiet for `--idle-seconds` (and at least
`--min-records` records have landed), or `--max-seconds` elapses. That bounded
shape is what a scheduled MotherDuck Flight needs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

# dlt phones home by default; keep the dev loop quiet and offline.
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")

import dlt

from .config import (
    DestinationConfig,
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    motherduck_token,
)
from .debezium_props import build_properties, internal_topic_prefixes
from .errors import EngineFailure
from .faults import validate_env as validate_fault_env
from .handler import DltChangeHandler
from .source_health import SourceHealth

if TYPE_CHECKING:  # `engine` imports pydbzengine, which boots a JVM on import.
    from .engine import SupervisedDebeziumEngine

log = logging.getLogger("cdc_flight.pipeline")


# --------------------------------------------------------------------------- #
# destination
# --------------------------------------------------------------------------- #
def ensure_motherduck_database(database: str, token: str) -> None:
    """MotherDuck refuses to ATTACH a database that does not exist yet."""
    import duckdb

    con = duckdb.connect(f"md:?motherduck_token={token}")
    try:
        con.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
        log.info("MotherDuck database %r ready", database)
    finally:
        con.close()


def build_dlt_pipeline(dest: DestinationConfig) -> dlt.Pipeline:
    dest.pipelines_dir.mkdir(parents=True, exist_ok=True)

    if dest.kind == "duckdb":
        dest.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        destination = dlt.destinations.duckdb(str(dest.duckdb_path))
    elif dest.kind == "motherduck":
        token = motherduck_token()
        if not token:
            raise RuntimeError(
                "CDC_DESTINATION=motherduck but neither `motherduck_token` nor "
                "`MOTHERDUCK_TOKEN` is set in the environment."
            )
        ensure_motherduck_database(dest.motherduck_database, token)
        destination = dlt.destinations.motherduck(
            credentials=f"md:{dest.motherduck_database}?motherduck_token={token}"
        )
    else:
        raise ValueError(f"unknown destination {dest.kind!r} (expected duckdb|motherduck)")

    return dlt.pipeline(
        pipeline_name=dest.pipeline_name,
        destination=destination,
        dataset_name=dest.dataset_name,
        pipelines_dir=str(dest.pipelines_dir),
        progress=None,
    )


# --------------------------------------------------------------------------- #
# bounded engine runner
# --------------------------------------------------------------------------- #
def run_engine_bounded(
    engine: SupervisedDebeziumEngine,
    handler: DltChangeHandler,
    run: RunConfig,
    health: SourceHealth | None = None,
    *,
    engine_terminates_normally: bool = False,
) -> dict:
    """Run the Debezium engine until the *source* agrees it is idle, or the deadline hits.

    Four independent things can go wrong, and all four must reach the caller:

    * `engine.run()` raises on this process's engine thread (rare);
    * the handler's dlt load raises (captured by `handler.error`);
    * the *engine itself* fails - a connector that cannot start, or a streaming
      error. Debezium reports that through its `CompletionCallback` and returns
      normally, so it is only visible via `SupervisedDebeziumEngine.failure`.
      Missing this case is what made every rubric-§4 failure mode exit 0;
    * the connector stops streaming *without failing* - a retriable exception
      puts it into a restart backoff that is longer than our idle window, so an
      idle timer alone reports success on a partial delivery (Opus B5). `health`
      corroborates "quiet" against `pg_replication_slots`.
    """
    started = time.monotonic()
    error_box: list[BaseException] = []

    def _run():
        try:
            engine.run()
        except BaseException as exc:
            error_box.append(exc)

    thread = threading.Thread(target=_run, name="debezium-engine", daemon=True)
    thread.start()

    stop_reason = "max_seconds"
    idle_blocked_by_source = 0
    try:
        while thread.is_alive():
            elapsed = time.monotonic() - started
            if elapsed >= run.max_seconds:
                stop_reason = "max_seconds"
                break
            if error_box or engine.failure is not None:
                stop_reason = "engine_error"
                break
            enough = handler.record_count >= run.min_records
            quiet = handler.seconds_since_last_batch >= run.idle_seconds
            # Never stop before we have given the connector a chance to start up,
            # and never stop while a batch is still being loaded (closing the
            # engine mid-batch interrupts the record committer).
            warmed_up = elapsed >= min(run.idle_seconds, 5.0)
            if enough and quiet and warmed_up and not handler.busy:
                if health is None or health.may_declare_idle(min_seconds=run.idle_seconds):
                    stop_reason = "idle"
                    break
                # The stream is quiet but the source says the connector is not
                # streaming (or is far behind): keep waiting rather than
                # reporting a partial delivery as a success.
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
        # Only an *intentional* stop may arm the shutdown-noise filter, or a real
        # failure racing the close gets discarded as noise (Opus M1).
        intentional = stop_reason != "engine_error" and handler.error is None and not error_box
        log.info("closing debezium engine (reason=%s, intentional=%s)", stop_reason, intentional)

        # `close()` gets the same bounded supervision as the engine thread.
        # Previously it was called synchronously *before* the join, so a hang
        # inside close() never reached the join-based "hung" verdict and the
        # advertised 60 s hang detector did not actually guard this path
        # (Codex 11). The process-level hard stop is `shutdown_and_exit()`'s
        # watchdog timer, which fires even if the interpreter is wedged.
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
            stop_reason = "hung"
        thread.join(timeout=60)
        if thread.is_alive():
            log.error("debezium engine thread did not stop within 60s")
            stop_reason = "hung"

    summary = {
        "stop_reason": stop_reason,
        "elapsed_sec": round(time.monotonic() - started, 2),
        "records": handler.record_count,
        "batches": handler.batch_count,
        "data_batches": handler.data_batch_count,
        "skipped": handler.skipped_count,
        "tables": handler.snapshot_counts(),
        "offset_flushes_verified": engine.offset_flushes_verified,
    }
    if health is not None:
        summary.update(health.summary())
    if engine.suppressed_message:
        # Never let a suppressed failure be invisible to an operator (Opus M1).
        summary["suppressed_engine_message"] = engine.suppressed_message

    failure = engine.failure
    if error_box or handler.error is not None or failure is not None:
        cause = error_box[0] if error_box else handler.error
        message = failure if failure is not None else f"{type(cause).__name__}: {cause}"
        summary["stop_reason"] = "engine_error"
        raise EngineFailure(message, summary) from cause

    # A hang is a failure too: the watchdog, not a clean shutdown, ended the run
    # (rubric 4.5 - a hang that exits 0 is invisible to any scheduler).
    if stop_reason == "hung":
        raise EngineFailure("debezium engine thread did not stop within 60s", summary)

    # The engine thread returned on its own, before the supervisor asked for a
    # stop. For a *streaming* mode that is engine death, even when Debezium
    # invoked its completion callback with success=true (a swallowed
    # StopEngineException looks exactly like this). Only a snapshot mode that is
    # designed to terminate may end this way (Codex 10).
    if stop_reason == "engine_finished" and not engine_terminates_normally:
        raise EngineFailure(
            "the Debezium engine terminated before the supervisor requested a stop "
            f"(completion success={engine.completed_success}); in streaming mode "
            "that is engine death, not a clean finish",
            summary,
        )

    # Stopping on the deadline while the connector is not attached to its slot is
    # engine death wearing a bounded-run costume: report it, do not claim ok.
    if health is not None and stop_reason == "max_seconds":
        not_streaming_for = health.not_streaming_for
        if not_streaming_for >= run.idle_seconds:
            raise EngineFailure(
                "reached --max-seconds while the connector was not streaming for "
                f"{not_streaming_for:.1f}s ({health.summary()}); the delivery is "
                "incomplete, so this run is not a success",
                summary,
            )

    summary["ok"] = True
    return summary


# --------------------------------------------------------------------------- #
# entrypoint
# --------------------------------------------------------------------------- #
def run(
    *,
    destination: str | None = None,
    max_seconds: float | None = None,
    idle_seconds: float | None = None,
    min_records: int | None = None,
    snapshot_mode: str | None = None,
    reset_state: bool = False,
) -> dict:
    # Parse CDC_FAULT_INJECT once, here, so a typo fails the run instead of
    # leaving a fault test vacuously green (Codex 9).
    fault_spec = validate_fault_env()
    if fault_spec:
        log.warning("fault injection armed: point=%s data_batch=%s action=%s", *fault_spec)

    source = SourceConfig()
    replication = ReplicationConfig()
    dest = DestinationConfig(**({"kind": destination} if destination else {}))
    run_cfg = RunConfig(
        **{
            k: v
            for k, v in {
                "max_seconds": max_seconds,
                "idle_seconds": idle_seconds,
                "min_records": min_records,
            }.items()
            if v is not None
        }
    )

    if reset_state and replication.state_dir.exists():
        log.info("resetting CDC state at %s", replication.state_dir)
        shutil.rmtree(replication.state_dir)
    replication.state_dir.mkdir(parents=True, exist_ok=True)

    props = build_properties(source, replication, snapshot_mode=snapshot_mode)
    log.info(
        "source=%s:%s/%s tables=%s slot=%s snapshot=%s destination=%s",
        source.host,
        source.port,
        source.dbname,
        source.tables,
        replication.slot_name,
        props["snapshot.mode"],
        dest.kind,
    )

    # Imported late: importing pydbzengine boots a JVM.
    from .engine import SupervisedDebeziumEngine

    dlt_pipeline = build_dlt_pipeline(dest)
    handler = DltChangeHandler(
        dlt_pipeline,
        internal_topic_prefixes=internal_topic_prefixes(replication.topic_prefix),
    )
    engine = SupervisedDebeziumEngine(
        properties=props,
        handler=handler,
        offset_file=replication.offset_file,
        always_commit_offsets=props.get("offset.flush.interval.ms") == "0",
    )
    health = SourceHealth(
        dsn=source.dsn,
        slot_name=replication.slot_name,
        max_lag_bytes=run_cfg.idle_max_lag_bytes,
    ).start()

    def _decorate(result: dict) -> dict:
        result["destination"] = dest.kind
        result["dataset"] = dest.dataset_name
        if dest.kind == "duckdb":
            result["duckdb_path"] = str(dest.duckdb_path)
        else:
            result["motherduck_database"] = dest.motherduck_database
        return result

    # The only snapshot modes that are *supposed* to end the engine on their own.
    terminating_modes = {"initial_only", "recovery_only"}

    try:
        return _decorate(
            run_engine_bounded(
                engine,
                handler,
                run_cfg,
                health,
                engine_terminates_normally=props["snapshot.mode"] in terminating_modes,
            )
        )
    except EngineFailure as failure:
        _decorate(failure.summary)
        raise
    finally:
        health.stop()


def shutdown_and_exit(code: int = 0, timeout: float = 15.0) -> None:
    """Tear the JVM down and guarantee the process actually exits.

    Debezium leaves non-daemon JVM threads behind, so a plain `return` from
    `main()` leaves the interpreter hanging forever after the work is done. We
    ask JPype to shut the JVM down (must happen on the main thread) behind a
    watchdog that hard-exits if that itself wedges.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    watchdog = threading.Timer(timeout, lambda: os._exit(code))
    watchdog.daemon = True
    watchdog.start()
    try:
        import jpype

        if jpype.isJVMStarted():
            jpype.shutdownJVM()
    except Exception:
        log.debug("JVM shutdown raised; exiting anyway", exc_info=True)
    os._exit(code)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-flight", description=__doc__)
    parser.add_argument("--destination", choices=["duckdb", "motherduck"], default=None)
    parser.add_argument("--max-seconds", type=float, default=None)
    parser.add_argument("--idle-seconds", type=float, default=None)
    parser.add_argument("--min-records", type=int, default=None)
    parser.add_argument(
        "--snapshot-mode",
        default=None,
        help="Debezium snapshot.mode (initial, no_data, initial_only, always, when_needed, ...)",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="delete Debezium offsets and dlt pipeline state before running",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )

    try:
        result = run(
            destination=args.destination,
            max_seconds=args.max_seconds,
            idle_seconds=args.idle_seconds,
            min_records=args.min_records,
            snapshot_mode=args.snapshot_mode,
            reset_state=args.reset_state,
        )
    # `BaseException`, not `Exception`: Ctrl-C must still reach
    # `shutdown_and_exit()`, because Debezium leaves non-daemon JVM threads
    # behind and a bare `raise` would hang the interpreter forever - a rubric-4.5
    # regression hiding in the error path (Opus m3).
    except BaseException as exc:
        log.exception("pipeline run failed")
        # A failed run still owes the operator (and rubric 6.1/6.2) a
        # machine-readable summary saying *why* it failed.
        summary = dict(getattr(exc, "summary", {}) or {})
        summary.setdefault("stop_reason", "error")
        summary["ok"] = False
        summary["error"] = str(exc)
        summary["error_type"] = type(exc).__name__
        _write_summary(summary)
        shutdown_and_exit(1)
        return 1  # unreachable; keeps type checkers happy

    _write_summary(result)
    shutdown_and_exit(0)
    return 0


def _write_summary(summary: dict) -> None:
    payload = json.dumps(summary, indent=2, sort_keys=True, default=str)
    print(payload)
    try:
        state_dir = ReplicationConfig().state_dir
        state_dir.mkdir(parents=True, exist_ok=True)
        Path(state_dir / "last_run.json").write_text(payload)
    except Exception:  # pragma: no cover - never let reporting mask the outcome
        log.warning("could not write last_run.json", exc_info=True)


if __name__ == "__main__":
    sys.exit(main())
