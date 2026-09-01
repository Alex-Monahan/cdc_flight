"""Re-snapshotting a table that CDC cannot rebuild (rubric 1.6, 1.8, and 1.5's last mark).

Three separate items need the same thing and none of them had it: rubric 1.5's
`recreated` relation retains its old image while it is left `awaiting_snapshot`; rubric
1.8's answer to an externally advanced slot is "trigger a backfill automatically";
rubric 1.6 cannot claim consistency for a re-snapshot it does not perform. This module
is that one thing.

## The mechanism, and why this one

**A blocking re-snapshot through a short-lived Debezium engine with its own fresh
replication slot, before the main stream starts.** The alternatives and why not:

* *Debezium incremental snapshots (signal-based).* Refused once already (review finding
  M-7) and still refused: they need a signalling data collection on the source (a write
  path we do not have on a replica, rubric 7.2) and they interleave snapshot chunks with
  the live stream under watermark-based **deduplication** — a mechanism whose correctness
  is "we drop the duplicates we know about", which is the opposite of the structural
  argument the rest of this design rests on.
* *Reading the table ourselves over psycopg.* Fully in our control and wrong for a
  boring reason: the destination's column shapes come from Debezium's converters, and
  today those converters are lossy in ways rubric 2.4 has not fixed yet (numerics as
  base64, dates as bigints). A hand-read image would disagree with the CDC events that
  land on top of it — a *different* encoding, not a better one, until 2.4 lands.
* *An in-process second engine running concurrently with the main one.* The correctness
  argument needs events for the re-snapshotted table to be held somewhere durable
  between the image and the swap; that is real machinery (a per-table buffer, a second
  fence) and it belongs to rubric 3.3/3.4, which own "other tables keep streaming while
  this one backfills". Here the run is a bounded batch job, so blocking is free: WAL is
  retained by the main slot, which is not consumed while this runs, and nothing is lost.

## Why the slot has to be *fresh*

MEASURED, in the engine log, not assumed. Debezium pairs the snapshot with an exact WAL
position **only when it creates the slot itself**: `CREATE_REPLICATION_SLOT` returns a
`consistent_point` and a `snapshot_name`, and the snapshot transaction then runs
`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '…'`
(`PostgresSnapshotChangeEventSource.snapshotTransactionIsolationLevelStatement`), with
`getTransactionStartLsn()` returning `slotCreatedInfo.startLsn()` — commented upstream
as "crucial so that if any SQL operations occur mid-snapshot they'll be properly
captured when streaming begins; otherwise they'll be lost".

With a **pre-existing** slot neither happens: an ordinary isolation level, and the start
LSN comes from `pg_current_wal_lsn()` read after the snapshot transaction has already
begun. Verified against Debezium 3.6 / Postgres 18: `snapshot.mode=initial_only` creates
no slot at all and takes that second path.

So: `snapshot.mode=initial` (which streams, so the slot is created) on a slot name that
does not exist, and the engine is stopped the moment the snapshot is complete.

## The consistent point, and what it fences

`C` is the exported snapshot's consistent point, which Debezium puts in every snapshot
record's `source.lsn`. Postgres guarantees the pairing: a transaction is visible in the
exported snapshot **iff** it committed before `C`. That makes the hand-over exact:

* a transaction with commit LSN `< C` is in the image, so the main stream must **not**
  apply it again — it is fenced by `table_state.snapshot_lsn`;
* a transaction with commit LSN `>= C` is not in the image, so it must be applied on top,
  even if some of its individual events have LSNs below `C` (they were uncommitted when
  the snapshot was taken). This is why the watermark is compared against the *commit*
  LSN of the whole unit and never against an event's own LSN — an event-level comparison
  would drop exactly the straddling transaction.

**CDC during the re-snapshot** therefore has one-line semantics: there is none, because
the main stream is not running; and everything the main stream later delivers for the
table is either fenced (before `C`) or applied on top (at or after `C`).

## Completion, and the two things it is not (Codex B1 / Opus BLOCKER-1)

"The re-snapshot completed" means **every requested table reached a terminal state**,
and there are exactly two terminal states:

* `swapped` — a shadow was built and atomically renamed over the live table. `C` is the
  snapshot records' own `source.lsn`, which is `slotCreatedInfo.startLsn()`.
* `verified_empty` — three independent facts agree: Debezium emitted the ordered
  per-table and global Initial Snapshot notifications (so the engine reached the end of
  the *whole* capture set), this table produced **zero** snapshot records, and a source
  count taken afterwards says the relation holds no rows. Only then is the destination
  table emptied.

Anything else leaves the table `awaiting_snapshot` and fails the run. The previous
implementation inferred "empty" from "not swapped", which is a statement about *our*
engine and not about the source: a table the engine stopped before reaching had its
live destination rows deleted and an audit row written claiming the source was empty.

### `C` for a verified-empty table is not `C` for a swapped one

An empty table produces no snapshot records, so it has no `source.lsn` to read `C` out
of. Polling the throwaway slot for one is a race: for an all-empty capture set the
engine can finish the image, enter streaming and advance `confirmed_flush_lsn` before
the first poll lands, and fencing at a value *ahead* of the image is silent loss
(Codex B2).

So a verified-empty table is fenced at `pg_current_wal_lsn()` **sampled before** the
emptiness is verified, in that order and on that connection. The argument is exact:
every transaction with commit LSN below that sample committed before the sample, so it
is visible to the `REPEATABLE READ` snapshot the count is taken in; a count of zero
therefore means no transaction below the sample left a row behind, and emptying the
destination is correct for all of them. Every transaction at or above the sample is
**not** fenced and is applied by the main stream on top. Neither direction can lose.

The two readings of `C` for a *swapped* table (the snapshot records, and the throwaway
slot's first observed `confirmed_flush_lsn`) are cross-checked and a disagreement is
**fatal**: it falsifies the assumption that either reading identifies the exported
snapshot, and the old `min()` resolution knowingly duplicated keyless rows, which
trades a rubric-1.6 violation for a rubric-1.2 one.
"""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import threading
from dataclasses import dataclass, field

from . import destination as dest_mod
from . import reconcile as reconcile_mod
from . import resnapshot_projection as projection
from . import resnapshot_refusal, table_lifecycle
from .applier import Applier
from .config import (
    DROP_LOG,
    ApplierConfig,
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    resolve_control_schema,
    source_connection_kwargs,
)
from .debezium_props import build_properties
from .destination import ResumePoint
from .errors import EngineFailure
from .naming import control_table, quote
from .ownership import DestinationOwnership
from .resnapshot_projection import ProjectionEvent
from .resnapshot_recovery import InterruptionRecovery
from .resnapshot_source_policy import (
    EmptinessEvidence,
    finish_source_missing_tables,
)
from .resnapshot_source_policy import gather_emptiness_evidence as _gather_emptiness_evidence
from .snapshot_completion import SnapshotCompletion
from .source_health import SourceHealth
from .source_routes import SourceRoutePolicy

log = logging.getLogger("cdc_flight.resnapshot")
OWNER = "resnapshot-protocol"

#: Suffix for the throwaway slot. Kept short: Postgres slot names are limited to 63
#: characters and the base name is already operator-chosen.
SLOT_SUFFIX = "_rs"


@dataclass
class ResnapshotOutcome:
    """What one re-snapshot pass did. Everything here lands in the run summary."""

    requested: list[str] = field(default_factory=list)
    swapped: list[str] = field(default_factory=list)
    emptied: list[str] = field(default_factory=list)
    logged_drops: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    quarantined: list[str] = field(default_factory=list)
    consistent_lsn: int | None = None
    slot_consistent_lsn: int | None = None
    snapshot_record_lsn: int | None = None
    empty_check_lsn: int | None = None
    snapshot_phase_ended: bool = False
    tables_scanned: list[str] = field(default_factory=list)
    events: int = 0
    reason: str = ""
    engine_stop_reason: str = ""
    snapshot_epoch: int = 0

    def as_dict(self) -> dict:
        return {
            "resnapshot_requested": self.requested,
            "resnapshot_swapped": self.swapped,
            "resnapshot_emptied": self.emptied,
            "resnapshot_logged_drops": self.logged_drops,
            "resnapshot_dropped": self.dropped,
            "resnapshot_quarantined": self.quarantined,
            "resnapshot_consistent_lsn": self.consistent_lsn,
            "resnapshot_slot_consistent_lsn": self.slot_consistent_lsn,
            "resnapshot_snapshot_record_lsn": self.snapshot_record_lsn,
            "resnapshot_empty_check_lsn": self.empty_check_lsn,
            "resnapshot_snapshot_phase_ended": self.snapshot_phase_ended,
            "resnapshot_tables_scanned": self.tables_scanned,
            "resnapshot_events": self.events,
            "resnapshot_reason": self.reason,
            "resnapshot_engine_stop_reason": self.engine_stop_reason,
            "resnapshot_snapshot_epoch": self.snapshot_epoch,
        }

    @property
    def still_owed(self) -> list[str]:
        """Requested tables that reached neither terminal state."""
        return sorted(
            set(self.requested)
            - set(self.swapped)
            - set(self.emptied)
            - set(self.logged_drops)
            - set(self.dropped)
            - set(self.quarantined)
        )

def _record_snapshot_swap_audit(
    con,
    *,
    pipeline: str,
    state,
    snapshot_lsn: int | None,
    commit_id: int,
    reason: str,
    new_relations: set[str],
    namespace: str | None = None,
    snapshot_epoch: int | None = None,
    control_schema: str | None = None,
) -> None:
    """Project a swapped image's audit and refusal discharge inside its COMMIT.

    ``SnapshotCoordinator.swap`` invokes this callback while the image transaction is
    still open, so the image and its lifecycle/audit projection commit together.
    """
    qualified = f"{state.schema}.{state.table}"
    detail = (
        f"re-snapshotted at consistent point {snapshot_lsn} ({reason}). "
        "The table holds exact current state; change events of transactions "
        "that committed before this LSN are fenced rather than applied, so "
        "per-event history for that span is the snapshot image and not the "
        "individual events (rubric 8.2's changelog is discontinuous here)."
    )
    events = [ProjectionEvent("resnapshot", detail, table_event="resnapshot", seq=0)]
    if qualified in new_relations:
        events.append(
            ProjectionEvent(
                "new",
                "new source relation discovered by the catalog watcher and "
                "snapshotted before streaming",
                table_event="new",
                seq=1,
            )
        )
    projection.project_snapshot_completion(
        con,
        pipeline=pipeline,
        source_schema=state.schema,
        source_table=state.table,
        target_table=state.target,
        snapshot_lsn=snapshot_lsn,
        commit_id=commit_id,
        events=tuple(events),
        namespace=namespace,
        snapshot_epoch=snapshot_epoch,
        control_schema=control_schema,
    )


def _completed_tables(
    con, pipeline: str, tables: list[tuple[str, str, str]]
) -> list[str]:
    """Return requested tables whose current lifecycle is complete.

    The bounded snapshot engine uses this read-only view after its coordinator has
    committed the canonical swap projection.  It deliberately performs no second
    projection or control-plane write: current-run recovery reads the durable lifecycle
    and audit state directly.
    """
    return [
        f"{schema}.{table}"
        for schema, table, _target in tables
        if table_lifecycle.read(
            con, pipeline=pipeline, source_schema=schema, source_table=table
        )
        == table_lifecycle.COMPLETE
    ]


class _SlotWatcher:
    """Records the FIRST `confirmed_flush_lsn` the throwaway slot ever shows.

    That first value should be the slot's consistent point: while the snapshot is
    running the only offset the connector has is the snapshot's own LSN, so any flush
    confirms exactly `C`.

    It is a **corroboration** and never a source of `C` on its own. Snapshot completion
    comes from Debezium's ordered per-table/global notifications, including for empty
    tables; source streaming and a slot poll are never completion edges. An empty table
    is fenced at a WAL position we sample ourselves, immediately before verifying the
    emptiness, which cannot be ahead of the image.
    """

    #: Tight, because the value is only trustworthy for as long as the slot has not
    #: advanced, and the slot may exist for only a few milliseconds before the first
    #: snapshot record is emitted.
    INTERVAL = 0.02

    def __init__(self, dsn: str, slot_name: str, interval: float = INTERVAL):
        self.dsn = dsn
        self.slot_name = slot_name
        self.interval = interval
        self.first_confirmed: int | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> _SlotWatcher:
        self._thread = threading.Thread(target=self._loop, name="resnap-slot", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self.first_confirmed is None:
                observation = reconcile_mod.observe_slot(
                    self.dsn, self.slot_name, connect_timeout=3
                )
                if observation.confirmed_flush_lsn is not None:
                    self.first_confirmed = observation.confirmed_flush_lsn
                    log.info(
                        "re-snapshot slot %s reached its consistent point at %s",
                        self.slot_name, self.first_confirmed,
                    )
                    return
            self._stop.wait(self.interval)


def slot_name_for(base: str) -> str:
    """A throwaway slot name derived from the pipeline's own, within 63 characters."""
    return f"{base[: 63 - len(SLOT_SUFFIX)]}{SLOT_SUFFIX}"


def sweep_stale_slot(dsn: str, base_slot_name: str) -> str:
    """Drop OUR throwaway slot if a previous run left one behind (Opus MAJOR-2).

    A leaked `_rs` slot holds WAL on the source for ever and counts against
    `max_replication_slots`; two independent review sessions leaked one on the shared
    development cluster in a single day. Called unconditionally at start-up, so the
    slot is reclaimed on the next run of the pipeline that created it whether or not
    another re-snapshot is due.

    Only ever the name this pipeline derives from its own slot: sweeping by suffix
    would delete another pipeline's in-flight re-snapshot slot.
    """
    slot = slot_name_for(base_slot_name)
    try:
        action = reconcile_mod.drop_slot(dsn, slot)
    except Exception as exc:  # pragma: no cover - the slot may be held right now
        log.warning("could not sweep the stale re-snapshot slot %r: %s", slot, exc)
        return f"sweep_failed: {exc}"
    if action == "dropped":
        log.warning(
            "dropped a leaked re-snapshot slot %r left by an earlier run; it had been "
            "holding WAL on the source", slot,
        )
    return action


def build_resnapshot_properties(
    source: SourceConfig,
    replication: ReplicationConfig,
    *,
    tables: list[tuple[str, str, str]],
    truncate_mode: str,
    routes: SourceRoutePolicy | None = None,
) -> dict[str, str]:
    """Build the throwaway connector properties for one exact source image.

    The shared connector property already pins every source acquisition to one reader.
    This builder remains the owner of the throwaway connector's table scope and name;
    it does not maintain a second snapshot-threading rule.
    """
    include = sorted({f"{schema}.{table}" for schema, table, _ in tables})
    props = build_properties(
        source,
        replication,
        routes=routes or source.route_policy,
        snapshot_mode="initial",
        truncate_mode=truncate_mode,
    )
    props["name"] = f"{props['name']}-resnapshot"
    props["table.include.list"] = ",".join(include)
    # Debezium only snapshots what it captures, and capturing more would stream more.
    props["snapshot.include.collection.list"] = ",".join(include)
    return props


def run(
    con,
    *,
    source: SourceConfig,
    replication: ReplicationConfig,
    routes: SourceRoutePolicy | None = None,
    pipeline: str,
    dataset: str,
    tables: list[tuple[str, str, str]],
    settings: dict,
    run_cfg: RunConfig,
    lease,
    runner_id: str,
    transactional_ddl: bool,
    epoch_base: int,
    reason: str,
    namespace: str,
    ownership: DestinationOwnership,
    new_relations: set[str] | None = None,
    drop_mode: str = DROP_LOG,
    control_schema: str | None = None,
    on_swap=None,
) -> ResnapshotOutcome:
    """Re-snapshot `tables` into shadow tables and swap them in, then return.

    Blocking and synchronous: the caller has not started the main engine yet. Raises
    `EngineFailure` if the re-snapshot does not complete, because the alternative is a
    run that streams CDC onto a destination table it knows to be incomplete.
    """
    outcome = ResnapshotOutcome(
        requested=[f"{s}.{t}" for s, t, _ in tables], reason=reason
    )
    if not tables:
        return outcome

    routes = routes or source.route_policy

    slot = slot_name_for(replication.slot_name)
    state_dir = replication.state_dir / "resnapshot"
    recovery = InterruptionRecovery.prepare(
        state_dir, pipeline=pipeline, tables=tables
    )
    # A leftover slot from an interrupted re-snapshot would make Debezium take the
    # pre-existing-slot path, which is exactly the path that does not export a snapshot.
    # Slot removal is local logical-slot administration.  In standby mode the
    # slot physically belongs to the decoder endpoint, so it must never be sent
    # through the primary source-write route.
    reconcile_mod.drop_slot(routes.slot_owner_dsn, slot)

    resnap_replication = dataclasses.replace(
        replication, slot_name=slot, state_dir=state_dir
    )
    props = build_resnapshot_properties(
        source,
        resnap_replication,
        tables=tables,
        truncate_mode=settings["truncate_mode"],
        routes=routes,
    )
    include = props["table.include.list"].split(",")

    resnap_settings = dict(settings)
    cfg = ApplierConfig(
        max_batch_size=int(props["max.batch.size"]),
        commit_timeout=run_cfg.commit_timeout,
        resnapshot=True,
        # Nothing about a re-snapshot should be able to destroy a table: the catalog
        # watcher is not running, and a truncate arriving on this short stream is a
        # streaming event, which this applier discards anyway.
        **{**resnap_settings, "drop_mode": "ignore"},
    )

    log.warning(
        "RE-SNAPSHOT starting for %s (%s) via throwaway slot %r",
        ", ".join(include), reason, slot,
    )
    watcher = _SlotWatcher(routes.read_dsn, slot).start()
    health = None
    applier = None
    source_stopped = False
    completion = SnapshotCompletion.full_snapshot(include)
    def snapshot_audit(state, snapshot_lsn, commit_id):
        _record_snapshot_swap_audit(
            con,
            pipeline=pipeline,
            state=state,
            snapshot_lsn=snapshot_lsn,
            commit_id=commit_id,
            reason=reason,
            new_relations=new_relations or set(),
            namespace=namespace,
            snapshot_epoch=epoch_base + len(tables) + 1,
            control_schema=control_schema,
        )
        if on_swap is not None:
            on_swap(state, snapshot_lsn, commit_id)
    descriptor_connection = None
    descriptor_provider = None
    try:
        # The throwaway applier deliberately has no live CatalogWatcher, but its
        # snapshot shadow must use the same catalog-authoritative descriptors as the
        # main stream.  Read only the requested relation/type facts once; no source
        # row values are fetched and no TOAST behavior is involved here.
        import psycopg

        from .catalog_descriptors import RelationDescriptorProvider

        descriptor_connection = psycopg.connect(
            routes.read_dsn,
            autocommit=True,
            **source_connection_kwargs(
                connect_timeout=run_cfg.jdbc_connect_timeout_seconds,
                socket_timeout_seconds=run_cfg.jdbc_socket_timeout_seconds,
            ),
        )
        descriptor_provider = RelationDescriptorProvider.from_tables(
            descriptor_connection, tables, source_dsn=routes.read_dsn
        )
        applier = Applier(
            con,
            pipeline=pipeline,
            namespace=f"{namespace}::resnapshot",
            dataset=dataset,
            topic_prefix=replication.topic_prefix,
            marker_prefixes=("cdcf", "cdc_flight_heartbeat"),
            signal_data_collection=props.get("signal.data.collection"),
            offset_path=resnap_replication.offset_file,
            resume_point=ResumePoint(snapshot_epoch=epoch_base),
            config=cfg,
            lease=lease,
            runner_id=runner_id,
            transactional_ddl=transactional_ddl,
            catalog=None,
            completion=completion,
            snapshot_audit=snapshot_audit,
            descriptor_provider=descriptor_provider,
            binary_handling_mode=props.get("binary.handling.mode", "base64"),
            hstore_handling_mode=props.get("hstore.handling.mode", "map"),
            control_schema=control_schema,
            message_prefix_allowlist=replication.message_prefix_allowlist,
        )
        ownership.attach(applier)
        # Keep the historical pipeline seam: tests and embedding callers replace
        # the bounded runner there, while production still resolves to the
        # supervisor implementation re-exported by pipeline.py.
        from . import pipeline as pipeline_mod
        from .engine import SupervisedDebeziumEngine

        engine = SupervisedDebeziumEngine(
            properties=props,
            handler=applier,
            offset_file=resnap_replication.offset_file,
            always_commit_offsets=props.get("offset.flush.interval.ms") == "0",
        )
        applier.verifier = None
        engine.consumer  # noqa: B018 - builds the consumer and attaches the verifier
        health = SourceHealth(
            dsn=routes.read_dsn,
            slot_name=slot,
            max_lag_bytes=run_cfg.idle_max_lag_bytes,
            primary_dsn=routes.source_write_dsn,
            source_write_dsn=routes.source_write_dsn,
            standby_heartbeat=routes.role == "standby",
        ).start()
        ownership.activate(applier)
        try:
            summary = pipeline_mod.run_engine_bounded(
                engine,
                applier,
                dataclasses.replace(
                    run_cfg,
                    # The snapshot is over only after the ordered per-table and global
                    # callbacks prove it. Empty tables emit no row records, but their
                    # direct notifications still supply the completion evidence; idle
                    # is never a snapshot-completion fallback.
                    idle_seconds=min(run_cfg.idle_seconds, 6.0),
                    min_records=0,
                ),
                health,
                stop_when=lambda: completion.completed,
                completion=completion,
                quiescence_observer=ownership.quiescence_observer(applier),
            )
        finally:
            health.stop()
            watcher.stop()
            source_stopped = True
            # This is the same semantic fact the supervisor used to authorize the
            # stop and the emptiness decision below; sample it once for the outcome.
            outcome.snapshot_phase_ended = completion.phase_ended
            outcome.tables_scanned = sorted(completion.tables_seen)
        outcome.engine_stop_reason = str(summary.get("stop_reason"))
        outcome.events = int(summary.get("applied_events") or 0)

        outcome.slot_consistent_lsn = watcher.first_confirmed
        outcome.snapshot_record_lsn = applier.last_snapshot_lsn
        outcome.consistent_lsn = agree_on_consistent_point(
            applier.last_snapshot_lsn, watcher.first_confirmed
        )

        if outcome.consistent_lsn is not None:
            outcome.swapped = _completed_tables(
                con,
                pipeline,
                tables,
            )
        pending = [t for t in tables if f"{t[0]}.{t[1]}" not in set(outcome.swapped)]
        evidence = _gather_emptiness_evidence(
            routes.read_dsn,
            pending=pending,
            snapshot_phase_ended=outcome.snapshot_phase_ended,
            tables_seen=completion.tables_seen,
        )
        outcome.empty_check_lsn = evidence.wal_lsn
        outcome.logged_drops, outcome.dropped = finish_source_missing_tables(
            con,
            pipeline=pipeline,
            dataset=dataset,
            tables=tables,
            done=set(outcome.swapped),
            evidence=evidence,
            drop_mode=drop_mode,
            namespace=namespace,
            snapshot_epoch=epoch_base + len(tables) + 1,
        )
        resolved = set(outcome.swapped) | set(outcome.logged_drops) | set(outcome.dropped)
        outcome.emptied = finish_verified_empty_tables(
            con,
            pipeline=pipeline,
            dataset=dataset,
            tables=tables,
            done=resolved,
            evidence=evidence,
            new_relations=new_relations or set(),
            namespace=namespace,
            snapshot_epoch=epoch_base + len(tables) + 1,
        )
        terminal = (
            set(outcome.swapped)
            | set(outcome.emptied)
            | set(outcome.logged_drops)
            | set(outcome.dropped)
        )
        if outcome.consistent_lsn is None and terminal != set(outcome.requested):
            raise EngineFailure(
                "the re-snapshot produced no consistent point: neither a snapshot "
                f"record nor the throwaway slot {slot!r} yielded an LSN, so the "
                "hand-over between the image and the stream cannot be fenced (rubric "
                "1.6). Nothing was swapped and the tables stay marked for a "
                "re-snapshot.",
                outcome.as_dict(),
            )
        assert_every_requested_table_completed(outcome)
        outcome.snapshot_epoch = epoch_base + len(tables) + 1
        recovery.consume()
        log.warning(
            "RE-SNAPSHOT complete at consistent point %s: swapped %s, emptied %s, "
            "logged drops %s, dropped %s",
            outcome.consistent_lsn,
            outcome.swapped or "-",
            outcome.emptied or "-",
            outcome.logged_drops or "-",
            outcome.dropped or "-",
        )
        return outcome
    except BaseException as exc:
        # Anything that escapes leaves the requested tables in whatever state the
        # engine got them to. `SnapshotCoordinator` writes `in_progress` the moment a
        # table's first record arrives, and `in_progress` is NOT in the
        # `tables_awaiting_snapshot` queue - so a partial re-snapshot that simply
        # raised would drop the table off the durable to-do list and the next run
        # would stream CDC onto a half-built image. Re-assert the to-do list only when
        # this thread still owns the connection. A live callback gets the pre-armed
        # filesystem recovery marker instead; the next process consumes it before
        # reading the owed queue.
        summary = dict(getattr(exc, "summary", {}) or {})
        destination_safe = True
        if applier is not None and ownership.owns(applier):
            destination_safe = ownership.retire_if_quiescent(
                reason="resnapshot_setup_failed"
            )
        failed_quiescence = not destination_safe
        if failed_quiescence:
            recovery.retain_in(summary)
            with contextlib.suppress(Exception):
                exc.summary = summary
            log.critical(
                "re-snapshot callback did not quiesce; retaining its destination "
                "runtime, throwaway slot and offset state. Recovery is armed at %s",
                recovery.marker,
            )
        else:
            refused = resnapshot_refusal.cause(exc)
            if refused is not None:
                resnapshot_refusal.persist(
                    con,
                    refused=refused,
                    pipeline=pipeline,
                    tables=tables,
                    source_dsn=routes.read_dsn,
                    control_schema=control_schema,
                )
            reassert_owed(
                con,
                pipeline=pipeline,
                tables=tables,
                terminal=outcome,
                control_schema=control_schema,
            )
            durable_quarantine = dest_mod.quarantined_tables(
                con, pipeline, control_schema=control_schema
            )
            quarantined_requested = sorted(
                qualified
                for qualified in outcome.requested
                if qualified in durable_quarantine
            )
            if refused is not None and quarantined_requested:
                outcome.quarantined = quarantined_requested
                outcome.engine_stop_reason = "schema_refusal_quarantined"
                outcome.reason = (
                    "identical durable input refused again; the table is durably "
                    "quarantined and healthy co-published tables may proceed"
                )
                assert_every_requested_table_completed(outcome)
                recovery.consume()
                log.error(
                    "RE-SNAPSHOT quarantined %s after deterministic schema refusal",
                    ", ".join(quarantined_requested),
                )
                return outcome
            recovery.consume()
        raise
    finally:
        provider_close = getattr(descriptor_provider, "close", None)
        if provider_close is None:
            provider_owner = getattr(descriptor_provider, "__self__", None)
            provider_close = getattr(provider_owner, "close", None)
        if provider_close is not None:
            provider_close()
        if descriptor_connection is not None:
            descriptor_connection.close()
        if not source_stopped:
            if health is not None:
                health.stop()
            watcher.stop()
        if applier is not None and ownership.owns(applier):
            ownership.retire_if_quiescent(reason="resnapshot_teardown")
        if (
            (applier is None or not ownership.owns(applier))
            and recovery.consumed
        ):
            # Named by us, created by Debezium, ours to reclaim only after the callback
            # ownership token proves every destination user has left AND a safe owner
            # has discharged the durable recovery obligation.
            # Terminal throwaway-slot retirement is also source administration.
            recovery.retire_terminal_resources(dsn=routes.slot_owner_dsn, slot=slot)


def reassert_owed(
    con,
    *,
    pipeline: str,
    tables: list[tuple[str, str, str]],
    terminal: ResnapshotOutcome,
    control_schema: str | None = None,
) -> list[str]:
    """Reassert non-terminal requested tables, excluding terminal/quarantined ones."""
    finished = (
        set(terminal.swapped)
        | set(terminal.emptied)
        | set(terminal.logged_drops)
        | set(terminal.dropped)
        | set(terminal.quarantined)
    )
    durable_quarantine = dest_mod.quarantined_tables(
        con, pipeline, control_schema=control_schema
    )
    owed = [
        t for t in tables
        if f"{t[0]}.{t[1]}" not in finished
        and f"{t[0]}.{t[1]}" not in durable_quarantine
    ]
    if not owed:
        return []
    dest_mod.request_snapshot(
        con,
        pipeline=pipeline,
        tables=owed,
        detail="the re-snapshot did not complete for these tables (rubric 1.6)",
        control_schema=control_schema,
    )
    return [f"{s}.{t}" for s, t, _ in owed]


def assert_every_requested_table_completed(outcome: ResnapshotOutcome) -> None:
    """Completion means every REQUESTED table reached a terminal state, or nothing.

    This guard used to be unreachable: the function that produced `emptied` appended
    every not-yet-swapped table to it unconditionally, so `still_owed` was provably
    `[]` for every possible input and the `EngineFailure` below was dead code (Opus
    BLOCKER-1). Now that a table can end a re-snapshot neither swapped nor
    verified-empty, it has a live branch — and the run has to fail, because the
    alternative is streaming CDC onto a destination table we know is incomplete.
    """
    owed = outcome.still_owed
    if not owed:
        return
    raise EngineFailure(
        f"the re-snapshot did not complete for {', '.join(owed)}: those tables are "
        "still marked `awaiting_snapshot` and the destination is knowingly incomplete "
        "for them (rubric 1.6). Nothing about them was changed, so the next run "
        "finishes the job.",
        outcome.as_dict(),
    )


def agree_on_consistent_point(record_lsn: int | None, slot_lsn: int | None) -> int | None:
    """Cross-check the two independent readings of `C`. A disagreement is FATAL.

    They are the same number by construction and they are read two completely
    different ways, so a disagreement means one of the assumptions in this module's
    docstring is wrong — and once that is true, *neither* reading can be shown to
    identify the exported snapshot, so neither is a boundary anything may rest on.

    This used to take the `min()`, on the argument that fencing too low can only
    re-apply. That argument is true and insufficient: re-applying **duplicates** on a
    keyless table, which is a violation of rubric 1.2's exactly-once claim, so `min`
    does not avoid a correctness violation, it chooses a different one. Both reviewers
    independently reached "hard-fail" (Codex B2, Opus Q1). The cost is one extra run:
    the tables stay `awaiting_snapshot` and the next attempt takes a fresh `C`.
    """
    if record_lsn is not None and slot_lsn is not None and record_lsn != slot_lsn:
        raise EngineFailure(
            "the re-snapshot's two readings of the consistent point DISAGREE: the "
            f"snapshot records say {record_lsn}, the throwaway slot said {slot_lsn}. "
            "One of them does not identify the exported snapshot, so neither can fence "
            "the hand-over between the image and the stream. Refusing to publish a "
            "watermark; the tables stay marked for a re-snapshot and the next run takes "
            "a fresh consistent point (rubric 1.6, ADR 0001 §19/A52).",
            {"snapshot_record_lsn": record_lsn, "slot_consistent_lsn": slot_lsn},
        )
    return record_lsn if record_lsn is not None else slot_lsn


def finish_verified_empty_tables(
    con,
    *,
    pipeline: str,
    dataset: str,
    tables: list[tuple[str, str, str]],
    done: set[str],
    evidence: EmptinessEvidence,
    new_relations: set[str] | None = None,
    namespace: str | None = None,
    snapshot_epoch: int | None = None,
    control_schema: str | None = None,
) -> list[str]:
    """Empty the destination for the tables PROVEN empty at the source. Nothing else.

    A table with no rows emits no snapshot records, so no shadow is created and no swap
    happens — and the destination table would keep the rows it had, which after a
    re-snapshot is stale data presented as current. So the destination table is
    emptied, in one transaction with its `table_state` row and an audit marker.

    The classification is the dangerous half, and it is what this function exists to
    make explicit. "Produced no snapshot records" is a fact about *our engine*, not
    about the source: a table the engine stopped before reaching also produces no
    records, and deleting its live destination rows is silent destruction (Opus
    BLOCKER-1, reproduced against a populated table). Every pending table is therefore
    checked against `EmptinessEvidence`, which requires the direct per-table/global
    completion callbacks, zero records for *this* table, and a source count of zero. A
    table that fails any of the
    three is left completely untouched and stays `awaiting_snapshot`; the caller then
    fails the run through `assert_every_requested_table_completed`.
    """
    emptied: list[str] = []
    discovered = new_relations or set()
    pending: list[tuple[str, str, str]] = []
    for schema, table, target in tables:
        qualified = f"{schema}.{table}"
        if qualified in done:
            continue
        ok, why = evidence.verdict(qualified)
        if ok:
            pending.append((schema, table, target))
        else:
            log.error(
                "NOT classifying %s as empty and NOT touching its destination table: "
                "%s. It stays marked awaiting_snapshot (rubric 1.6).", qualified, why,
            )
    if not pending:
        return emptied
    consistent_lsn = evidence.wal_lsn
    con.execute("BEGIN TRANSACTION")
    try:
        for schema, table, target in pending:
            exists = con.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [dataset, target],
            ).fetchone()[0]
            removed = None
            if exists:
                removed = con.execute(
                    f"SELECT count(*) FROM {quote(dataset)}.{quote(target)}"
                ).fetchone()[0]
                con.execute(f"DELETE FROM {quote(dataset)}.{quote(target)}")
            # rubric 1.9: `awaiting_snapshot -> complete` with no shadow to swap. It is
            # a declared edge precisely because a table can be proven empty at the
            # source; `none -> complete` is NOT declared, which is what keeps this path
            # reachable only from the owed queue.
            table_lifecycle.transition(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                to=table_lifecycle.COMPLETE,
                reason="verified empty at the source; the destination table was emptied",
                snapshot_lsn=consistent_lsn,
                control_schema=control_schema,
            )
            audit_detail = (
                f"re-snapshot verified the source relation empty at consistent point "
                f"{consistent_lsn}; the destination image was emptied atomically "
                "and fenced at that point."
            )
            empty_detail = (
                "the source relation was VERIFIED to hold no rows: the ordered "
                "per-table and global snapshot callbacks completed, and no record "
                "was produced for this table, and a REPEATABLE READ count taken after "
                f"pg_current_wal_lsn()={consistent_lsn} returned zero. The "
                "destination table was emptied rather than swapped, and is fenced "
                "at that LSN so every later transaction is applied on top."
            )
            audit_events = [
                ProjectionEvent(
                    "resnapshot",
                    audit_detail,
                    table_event="resnapshot_empty",
                    table_event_detail=empty_detail,
                    seq=0,
                    rows_removed=removed,
                )
            ]
            if qualified in discovered:
                audit_events.append(
                    ProjectionEvent(
                        "new",
                        "new source relation discovered by the catalog watcher and "
                        "verified empty before streaming",
                        table_event="new",
                        seq=1,
                    )
                )
            projection.project_snapshot_completion(
                con,
                pipeline=pipeline,
                namespace=namespace,
                source_schema=schema,
                source_table=table,
                target_table=target,
                snapshot_lsn=consistent_lsn,
                commit_id=0,
                events=tuple(audit_events),
                snapshot_epoch=snapshot_epoch,
                control_schema=control_schema,
            )
            emptied.append(f"{schema}.{table}")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return emptied


def read_watermarks(
    con, pipeline: str, *, control_schema: str | None = None
) -> dict[str, int]:
    """`"<schema>.<table>" -> snapshot_lsn` for every table with a complete image.

    The main applier's per-table fence (see `planner.GroupPlan`). Reading it for *every*
    complete table rather than only for re-snapshotted ones is deliberate: after an
    ordinary initial snapshot the same rule holds and is a no-op, and a fence that is
    only armed on some paths is a fence nobody can reason about.
    """
    rows = con.execute(
        f"SELECT source_schema, source_table, snapshot_lsn FROM "
        f"{control_table(resolve_control_schema(control_schema), 'table_state')} "
        "WHERE pipeline = ? AND snapshot_state = 'complete' AND snapshot_lsn IS NOT NULL",
        [pipeline],
    ).fetchall()
    return {f"{schema}.{table}": int(lsn) for schema, table, lsn in rows}


def finish_empty_tables_after_main_snapshot(
    con,
    *,
    pipeline: str,
    dataset: str,
    dsn: str,
    owed: list[tuple[str, str, str]],
    completion: SnapshotCompletion,
    drop_mode: str = DROP_LOG,
    control_schema: str | None = None,
) -> tuple[list[str], int | None]:
    """Close out the tables a MAIN-engine snapshot left owed because they are empty.

    A source relation with zero rows emits no snapshot records at all, so
    `SnapshotCoordinator` never opens a shadow for it and never swaps one in — and the
    destination table keeps whatever it held before. For an ordinary run that is fine:
    nothing claimed the table had been rebuilt. For a run carrying a **journalled
    obligation** it is not, and the first cut of the journalled `--reset-state` proved
    how badly: it recorded the rebuild as complete and left the destination holding rows
    the source had truncated away (Codex r2 BLOCKER-1).

    The blocking re-snapshot path has always handled this correctly, through
    `EmptinessEvidence` and `finish_verified_empty_tables`. This is the same machinery,
    applied to the same question after the main engine's own snapshot — the three
    independent facts are unchanged, and so is the rule that a table failing any of them
    is left completely untouched and stays owed:

    1. Debezium's ordered per-table and global completion notifications prove that the
       engine saw the whole capture set;
    2. this table produced **zero** snapshot records;
    3. a source count taken after the engine stopped says the relation holds no rows,
       fenced at a WAL position sampled before that count.

    Returns `(emptied_tables, fence_lsn)`. The fence is the WAL position the emptiness
    was proven at, and the caller needs it: an entirely empty capture set produces no
    Debezium records, so the applier commits no group and writes no resume point, and a
    recovery that (correctly) demands one would never clear (Codex r3 MAJOR-1).
    """
    if not owed:
        return [], None
    evidence = _gather_emptiness_evidence(
        dsn,
        pending=owed,
        snapshot_phase_ended=completion.phase_ended,
        tables_seen=completion.tables_seen,
    )
    logged, dropped = finish_source_missing_tables(
        con,
        pipeline=pipeline,
        dataset=dataset,
        tables=owed,
        done=set(),
        evidence=evidence,
        drop_mode=drop_mode,
        namespace=None,
        control_schema=control_schema,
    )
    emptied = finish_verified_empty_tables(
        con,
        pipeline=pipeline,
        dataset=dataset,
        tables=owed,
        done=set(logged) | set(dropped),
        evidence=evidence,
        control_schema=control_schema,
    )
    return emptied + logged + dropped, evidence.wal_lsn
