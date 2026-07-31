"""cdc_flight: Postgres -> Debezium embedded engine -> the transactional applier
-> DuckDB / MotherDuck.

    cdc-flight --destination duckdb --max-seconds 60

The engine runs on a background thread; the main thread supervises it and closes
it once the change stream has been quiet for `--idle-seconds` (and the *source*
agrees the connector is idle), or `--max-seconds` elapses.

**The dlt load path is gone** (ADR 0001 D1/D10). `dlt.pipeline.run()` cannot host
the resume point in the destination transaction, cannot span tables in one
transaction, and opens one transaction per table inside every load package
(`repos/dlt/dlt/destinations/insert_job_client.py:24`,
`repos/dlt/dlt/load/load.py:637-647`), so rubric 1.1/1.2/1.3 are unreachable
through it. dlt survives as a *library*: `cdc_flight.naming` calls its
`snake_case` normaliser so destination identifiers stay byte-identical across
this migration.
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
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from . import catalog as catalog_mod
from . import destination as dest_mod
from . import reconcile as reconcile_mod
from .applier import Applier, ApplierConfig
from .config import (
    CatalogConfig,
    DestinationConfig,
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    applier_settings,
    lease_ttl_seconds,
)
from .debezium_props import assert_no_internal_topic_collision, build_properties
from .destination import CONTROL_SCHEMA, Lease
from .errors import EngineFailure
from .faults import validate_env as validate_fault_env
from .source_health import SourceHealth

if TYPE_CHECKING:  # `engine` imports pydbzengine, which boots a JVM on import.
    from .engine import SupervisedDebeziumEngine

log = logging.getLogger("cdc_flight.pipeline")


# --------------------------------------------------------------------------- #
# bounded engine runner
# --------------------------------------------------------------------------- #
def run_engine_bounded(
    engine: SupervisedDebeziumEngine,
    handler: Applier,
    run: RunConfig,
    health: SourceHealth | None = None,
    *,
    engine_terminates_normally: bool = False,
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
            # Never stop before the connector has had a chance to start, and
            # never stop while a commit group is being applied.
            warmed_up = elapsed >= min(run.idle_seconds, 5.0)
            if enough and quiet and warmed_up and not handler.busy:
                if health is None or health.may_declare_idle(min_seconds=run.idle_seconds):
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
            stop_reason = "hung"
        thread.join(timeout=60)
        if thread.is_alive():
            log.error("debezium engine thread did not stop within 60s")
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
    if health is not None:
        summary.update(health.summary())
    if engine.suppressed_message:
        summary["suppressed_engine_message"] = engine.suppressed_message

    failure = engine.failure
    if error_box or handler.error is not None or failure is not None:
        cause = error_box[0] if error_box else handler.error
        message = failure if failure is not None else f"{type(cause).__name__}: {cause}"
        summary["stop_reason"] = "engine_error"
        raise EngineFailure(message, summary) from cause

    if stop_reason == "hung":
        raise EngineFailure("debezium engine thread did not stop within 60s", summary)

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
    accept_orphan_offsets: bool = False,
) -> dict:
    # Parse CDC_FAULT_INJECT once, here, so a typo fails the run instead of
    # leaving a fault test vacuously green (Codex 9).
    fault_spec = validate_fault_env()
    if fault_spec:
        log.warning("fault injection armed: point=%s group=%s action=%s", *fault_spec)

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

    replication.state_dir.mkdir(parents=True, exist_ok=True)
    settings = applier_settings()
    # `skipped.operations` is what decides whether a TRUNCATE is decoded at all, so
    # the truncate policy has to be known before the engine properties are built
    # (rubric 1.5).
    props = build_properties(
        source,
        replication,
        snapshot_mode=snapshot_mode,
        truncate_mode=settings["truncate_mode"],
    )
    # A captured table whose topic collides with `<prefix>.transaction` would be
    # decoded as transaction metadata and never applied. Not reachable with the
    # pinned topic-naming strategy, and asserted rather than reasoned about
    # (Opus MINOR-6).
    assert_no_internal_topic_collision(replication.topic_prefix, source.tables)
    namespace = props["name"]
    runner_id = uuid.uuid4().hex

    log.info(
        "source=%s:%s/%s tables=%s slot=%s snapshot=%s destination=%s",
        source.host, source.port, source.dbname, source.tables,
        replication.slot_name, props["snapshot.mode"], dest.kind,
    )

    con = dest_mod.connect(dest)
    summary_extra: dict = {}
    try:
        dest_mod.ensure_control_schema(con)
        dest_mod.ensure_dataset(con, dest.dataset_name)

        if reset_state:
            # "Start over" has to mean start over at *both* ends, or the file is
            # deleted while the destination still claims a resume point and
            # reconciliation correctly refuses to re-snapshot.
            log.info("resetting CDC state at %s and in %s", replication.state_dir, CONTROL_SCHEMA)
            shutil.rmtree(replication.state_dir, ignore_errors=True)
            replication.state_dir.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.debezium_offsets WHERE pipeline = ?",
                [dest.pipeline_name],
            )
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.table_state WHERE pipeline = ?",
                [dest.pipeline_name],
            )
            con.execute(
                f"DELETE FROM {CONTROL_SCHEMA}.lease WHERE pipeline = ?",
                [dest.pipeline_name],
            )

        applier_cfg = ApplierConfig(
            max_batch_size=int(props["max.batch.size"]),
            **settings,
        )

        outcome = reconcile_mod.reconcile(
            con,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            offset_path=replication.offset_file,
            accept_orphan=accept_orphan_offsets,
            repair=applier_cfg.repair_offset_file,
        )
        summary_extra["reconciliation"] = outcome.decision
        summary_extra["reconciliation_detail"] = outcome.message
        log.info("start-up reconciliation: %s (%s)", outcome.decision, outcome.message)

        # ADR §4.7 - the Invariant-O guard, at start-up. `snapshot_mode` is what
        # decides the "slot exists / no durable destination row" cell (Codex 3).
        summary_extra["invariant_o_start"] = reconcile_mod.check_invariant_o(
            con, pipeline=dest.pipeline_name, namespace=namespace,
            dsn=source.dsn, slot_name=replication.slot_name,
            snapshot_mode=props["snapshot.mode"],
        )

        lease = Lease(dest.pipeline_name, owner_id=runner_id, ttl_seconds=lease_ttl_seconds())
        lease.acquire(con)

        # ADR §14.1's open question, answered by measurement rather than by guess.
        # AFTER the lease: the probe DROPs and CREATEs shared
        # `_cdc_flight.__ddl_probe_*` tables, so a runner that is about to be
        # rejected by the lease could otherwise drop the incumbent's probe tables
        # mid-probe and make the incumbent conclude `transactional_ddl=False`
        # (Opus MINOR-7).
        transactional_ddl = dest_mod.probe_transactional_ddl(con)
        summary_extra["transactional_ddl"] = transactional_ddl

        # rubric 1.5: `DROP TABLE` is not in the replication stream, so the source
        # catalog is polled on its own connection. Started BEFORE the engine, so a
        # table dropped while this pipeline was down is detected on this run rather
        # than one poll interval into it.
        catalog_cfg = CatalogConfig()
        watcher = None
        if applier_cfg.drop_mode != "ignore" and catalog_cfg.poll_seconds > 0:
            watcher = catalog_mod.CatalogWatcher(
                dsn=source.dsn,
                publication=replication.publication_name,
                schema=source.schema,
                include={t if "." in t else f"{source.schema}.{t}" for t in source.tables},
                known=catalog_mod.read_known_relations(con, dest.pipeline_name),
                replicated=catalog_mod.seed_from_table_state(con, dest.pipeline_name),
                poll_seconds=catalog_cfg.poll_seconds,
                emit_marker=catalog_cfg.emit_marker,
                marker_prefix=catalog_cfg.marker_prefix,
                grace_seconds=catalog_cfg.grace_seconds,
            ).start()

        # Imported late: importing pydbzengine boots a JVM.
        from .engine import SupervisedDebeziumEngine

        applier = Applier(
            con,
            pipeline=dest.pipeline_name,
            namespace=namespace,
            dataset=dest.dataset_name,
            topic_prefix=replication.topic_prefix,
            offset_path=replication.offset_file,
            resume_point=outcome.resume_point,
            config=applier_cfg,
            lease=lease,
            runner_id=runner_id,
            transactional_ddl=transactional_ddl,
            catalog=watcher,
        )
        engine = SupervisedDebeziumEngine(
            properties=props,
            handler=applier,
            offset_file=replication.offset_file,
            always_commit_offsets=props.get("offset.flush.interval.ms") == "0",
        )
        # Wired EXPLICITLY. It used to be attached as a side effect of
        # `engine.consumer`'s `cached_property` being evaluated before `engine`'s,
        # which is a third-party property-evaluation order (Opus B2 note): correct
        # today, invisible if it ever changes. Touching `engine.consumer` here makes
        # the dependency a statement, and the assertion makes it a checked one.
        applier.verifier = None
        engine.consumer  # noqa: B018 - builds the consumer and attaches the verifier
        if applier.cfg.verify_offset_file:
            assert applier.verifier is not None, (
                "the offset-flush verifier was not attached to the applier; a silently "
                "failed markBatchFinished() would be invisible (ADR 0001 §4.2)"
            )
        health = SourceHealth(
            dsn=source.dsn,
            slot_name=replication.slot_name,
            max_lag_bytes=run_cfg.idle_max_lag_bytes,
        ).start()

        def _decorate(result: dict) -> dict:
            result.update(summary_extra)
            result["destination"] = dest.kind
            result["dataset"] = dest.dataset_name
            result["runner_id"] = runner_id
            if dest.kind == "duckdb":
                result["duckdb_path"] = str(dest.duckdb_path)
            else:
                result["motherduck_database"] = dest.motherduck_database
            return result

        terminating_modes = {"initial_only", "recovery_only"}
        try:
            result = run_engine_bounded(
                engine, applier, run_cfg, health,
                engine_terminates_normally=props["snapshot.mode"] in terminating_modes,
            )
            summary_extra["invariant_o_end"] = reconcile_mod.check_invariant_o(
                con, pipeline=dest.pipeline_name, namespace=namespace,
                dsn=source.dsn, slot_name=replication.slot_name,
                snapshot_mode=props["snapshot.mode"],
            )
            return _decorate(result)
        except EngineFailure as failure:
            _decorate(failure.summary)
            raise
        finally:
            health.stop()
            if watcher is not None:
                watcher.stop()
            applier.shutdown()
            lease.release(con)
    finally:
        try:
            con.close()
        except Exception:  # pragma: no cover
            log.debug("closing the destination connection failed", exc_info=True)


def shutdown_and_exit(code: int = 0, timeout: float = 15.0) -> None:
    """Tear the JVM down and guarantee the process actually exits.

    Debezium leaves non-daemon JVM threads behind, so a plain `return` from
    `main()` leaves the interpreter hanging forever after the work is done.
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
        help="delete the Debezium offsets file AND the destination's resume point",
    )
    parser.add_argument(
        "--accept-orphan-offsets",
        action="store_true",
        help=(
            "delete an offsets.dat that has no matching destination row and force a "
            "re-snapshot (ADR 0001 §4.5). Without this the run REFUSES to start."
        ),
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
            accept_orphan_offsets=args.accept_orphan_offsets,
        )
    # `BaseException`, not `Exception`: Ctrl-C must still reach
    # `shutdown_and_exit()`, because Debezium leaves non-daemon JVM threads
    # behind and a bare `raise` would hang the interpreter forever.
    except BaseException as exc:
        log.exception("pipeline run failed")
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
