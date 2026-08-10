"""Source DDL that logical decoding never tells us about (rubric 1.5; seeds 2.3).

`DROP TABLE` is not in the replication stream. pgoutput carries `INSERT`,
`UPDATE`, `DELETE`, `TRUNCATE` and logical-decoding messages, and nothing else:
Debezium's Postgres connector has no DDL event source at all (`include.schema.
changes` is a MySQL/SQL Server feature; for Postgres it is a no-op). A dropped
table therefore looks *exactly* like a table that simply stopped changing, and the
baseline evidence for 1.5 is that `cdcflight_app_documents` kept its two rows
forever after `DROP TABLE app.documents`.

So it has to be **detected out of band**, and the only place the truth lives is the
source catalog. This module polls it on its own short-lived connection - the same
shape as `source_health.py` - and answers four questions per replicated table:

| observation | change | what 1.5 does with it |
|---|---|---|
| the name is gone from `pg_class` | `dropped` | apply the configured source-missing policy (`DROP_LOG` retains/audits; `DROP_REPLICATE` removes) |
| the name is back with a different `oid` | `recreated` | retain the old destination image, mark it `awaiting_snapshot`, and let the replacement snapshot or final source-missing policy own destruction |
| the name is there but no longer in the publication | `unpublished` | nothing but a marker + alert: Postgres still holds the rows, so dropping the destination would destroy data the source has |
| a table in the watched schemas we have never seen | `new` | add it to a table-scoped publication, audit it, and hand it to the existing single-table re-snapshot path (rubric 2.3) |

The watcher owns safe discovery admission (`ALTER PUBLICATION ... ADD TABLE` for a
table-scoped publication); `cdc_flight.catalog_apply` still owns destructive policy,
schema DDL, the circuit breaker and all destination actions.  The observation and any
destructive action remain separated in time by the WAL fence, which is where a stale
fact could become a wrong drop.

**The fence.** A detected drop must not be applied before the destination has
consumed every event that happened *before* it. The drop is discovered after the
fact, so the poll records a standby-safe source WAL position as `detected_lsn` and
the applier holds the action until its durable resume point reaches that LSN. On a quiet source
nothing would ever advance it, so a **transactional** logical-decoding message is
written on the source (`cdc_flight.source_marker` explains why transactional is
load-bearing and why that component is shared with D9's heartbeat).

If the marker cannot be written - a read-only replica, no permission - the action is
**not** applied, `marker_error` is preserved for the run summary, and the run does
not report success while a destructive change is unresolved: forcing the action on a
timer would drop a table whose in-flight events then re-create it as a zombie.
`CDC_CATALOG_GRACE` exists for operators who prefer that trade explicitly, and a
non-zero grace is **excluded from the structural correctness claim** - it applies a
destructive action before the fence that makes it safe.

**Confirmation.** A catalog change is queued only after the relation has been absent
(or its oid changed) on `CDC_DROP_CONFIRM_POLLS` consecutive polls (default 2). A
newer observed generation supersedes the stale observation while preserving the
`awaiting_snapshot` obligation; it never turns a retained recreate image into an
unquarantined drop. A poll that observes *zero* relations in the schema is discarded
outright: that is the wrong-database / mid-restore signature and can never legitimately
mean "drop everything" (Opus Q2/Q5).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field, replace

from . import (
    catalog_admission as admission_mod,
)
from . import (
    catalog_change_queue,
    catalog_generation,
    catalog_poll,
    catalog_state,
    catalog_support,
    state_interactions,
)
from . import source_marker as marker_mod
from .machines import (
    ADMISSION_ADMITTED,
    ADMISSION_EXTERNAL,
    CHANGE_APPLIED,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_OBSERVED,
    CHANGE_PENDING,
    CHANGE_SUPERSEDED,
    CHANGE_UNCONFIRMED,
    LIVE_CHANGE_STATES,
    SCHEMA_UNAVAILABLE,
    SCHEMA_VISIBLE,
)
from .schema_evolution import diff_columns

SourceRelation = catalog_state.SourceRelation
CatalogChange = catalog_state.CatalogChange
CHANGE_DROPPED = catalog_state.CHANGE_DROPPED
CHANGE_RECREATED = catalog_state.CHANGE_RECREATED
CHANGE_UNPUBLISHED = catalog_state.CHANGE_UNPUBLISHED
CHANGE_REPUBLISHED = catalog_state.CHANGE_REPUBLISHED
CHANGE_NEW = catalog_state.CHANGE_NEW
CHANGE_SCHEMA = catalog_state.CHANGE_SCHEMA
DESTRUCTIVE = catalog_state.DESTRUCTIVE
FENCED = catalog_state.FENCED

log = logging.getLogger("cdc_flight.catalog")

DROP_REPLICATE = "replicate"
DROP_LOG = "log"
DROP_IGNORE = "ignore"

#: Base prefix; the reason is appended, so the fence marker's `pg_logical_emit_message`
#: prefix is `cdcf_catalog_fence` and D9's heartbeat will be `cdcf_idle_heartbeat`
#: (`cdc_flight.source_marker`).
MARKER_PREFIX = "cdcf"


@dataclass
class _ToastAdmissionContext:
    """One source transaction held across the complete unit admission.

    The relation lock is deliberately held until the applier has admitted every
    event in the source unit.  Reusing only the boolean decision would reopen the
    r6 race: a concurrent ``REPLICA IDENTITY DEFAULT`` could happen between two
    events.  Keeping the source transaction open makes the lock cover every
    event-admission decision while still doing one catalog read per residual table.
    """

    txn_id: str
    conn: object
    decisions: dict[str, tuple[tuple, bool]] = field(default_factory=dict)

def _queued(change: CatalogChange) -> CatalogChange:
    return catalog_state.queued(change)


def _missing_value(raw: str | None, type_name: str) -> object | None:
    return catalog_state._missing_value(raw, type_name)


def read_known_relations(
    con, pipeline: str, *, control_schema: str | None = None
) -> dict[str, SourceRelation]:
    try:
        return catalog_state.read_known_relations(
            con, pipeline, control_schema=control_schema
        )
    except Exception as exc:
        from .errors import SchemaEvolutionRefused

        if not isinstance(exc, SchemaEvolutionRefused):
            raise
        # A durable descriptor miss is not a startup deadlock and is never repaired
        # by guessing. Record the same refusal/awaiting-snapshot obligation used by
        # the live applier, then let the next catalog observation drive a fresh
        # relation read and single-table resnapshot.
        if exc.source_schema and exc.source_table:
            from . import destination

            destination.record_schema_refusal(
                con,
                pipeline=pipeline,
                control_schema=control_schema,
                source_schema=exc.source_schema,
                source_table=exc.source_table,
                target_table=exc.target,
                detected_lsn=exc.detected_lsn,
                reason=str(exc),
            )
            log.error(
                "durable catalog descriptor refusal for %s.%s; automatic "
                "catalog reread/resnapshot is now owed: %s",
                exc.source_schema,
                exc.source_table,
                exc,
            )
            return {}
        raise


def seed_from_table_state(
    con, pipeline: str, *, control_schema: str | None = None
) -> set[str]:
    return catalog_state.seed_from_table_state(
        con, pipeline, control_schema=control_schema
    )


class CatalogWatcher:
    """Polls the source catalog on its own connection. Owns no destination state."""

    def __init__(
        self,
        *,
        dsn: str,
        primary_dsn: str | None = None,
        publication: str,
        schema: str,
        include: set[str],
        schemas: set[str] | None = None,
        all_schemas: bool = False,
        auto_discover: bool = False,
        publication_ownership: str = "flight",
        known: dict[str, SourceRelation] | None = None,
        replicated: set[str] | None = None,
        unrelatable: set[str] | None = None,
        poll_seconds: float = 10.0,
        connect_timeout: int = 5,
        emit_marker: bool = True,
        marker_prefix: str = MARKER_PREFIX,
        grace_seconds: float = 0.0,
        confirm_polls: int = 2,
        marker_max_writes: int | None = 60,
        binary_handling_mode: str = "base64",
        hstore_handling_mode: str = "map",
    ):
        self.dsn = dsn
        # Catalog queries may use a hot standby, but publication admission and
        # transactional logical-decoding markers are writes and must use the
        # configured primary.  The default preserves the primary-only topology.
        self.primary_dsn = primary_dsn or dsn
        self.publication = publication
        self.schema = schema
        #: qualified names the configuration says we replicate (`table.include.list`)
        self.include = set(include)
        #: Schemas to poll. `all_schemas` is the default pipeline mode; the explicit
        #: set keeps deployments that intentionally limit catalog discovery bounded.
        self.schemas = set(schemas or {schema})
        self.all_schemas = bool(all_schemas)
        #: When enabled, every non-partition relation in the watched schemas is a
        #: candidate. The publication remains the source of truth for streaming, and
        #: a table-scoped publication is amended by the discovery policy.
        self.auto_discover = bool(auto_discover)
        if publication_ownership not in {"flight", "external"}:
            raise ValueError(
                "publication_ownership must be 'flight' or 'external', got "
                f"{publication_ownership!r}"
            )
        #: The owner is a policy input, not an inference from publication membership.
        #: `flight` may issue ALTER PUBLICATION; `external` may only observe a table
        #: that is already streamable and refuses otherwise.
        self.publication_ownership = publication_ownership
        #: qualified names we have a destination table for
        self.replicated = set(replicated or ())
        #: Relations the destination holds rows for whose observed identity may NOT be
        #: adopted as history (rubric 1.9, `machines.CATALOG_BASELINE`). Computed once
        #: per run from durable state by `catalog_baseline.unrelatable_relations` and
        #: handed in, so the *decision* is a pure function of what the destination
        #: durably says and the *observation* stays this class's only job.
        #:
        #: Empty on every ordinary run. Non-empty only when a previous run left the
        #: baseline unconfirmed — and then adopting the currently observed oid would be
        #: the r5 BLOCKER: the old relation's rows beside the new relation's, for ever,
        #: with every run reporting success.
        self.unrelatable = set(unrelatable or ())
        self.known: dict[str, SourceRelation] = dict(known or {})
        self.poll_seconds = poll_seconds
        self.connect_timeout = connect_timeout
        #: Bounds a query on an ALREADY-CONNECTED source socket (Codex r3 MAJOR-3).
        self.query_timeout_ms = 4000
        #: How long `stop()` waits for the poll thread to actually die.
        self.quiesce_timeout = 15.0
        #: Set by `stop()`. The supervisor refuses to report success while it is False:
        #: a verdict taken over a thread that is still running is not a verdict.
        self.quiesced = False
        self.grace_seconds = grace_seconds
        #: How many consecutive polls must agree before a DESTRUCTIVE change is
        #: queued at all (Opus Q5). 1 restores the old behaviour.
        self.confirm_polls = max(1, int(confirm_polls))
        self.marker = marker_mod.SourceMarker(
            prefix=marker_prefix, enabled=emit_marker, max_writes=marker_max_writes
        )
        self.binary_handling_mode = str(binary_handling_mode)
        self.hstore_handling_mode = str(hstore_handling_mode)

        self._lock = threading.Lock()
        #: A dedicated primary connection is reused for locked admission scopes.
        #: Each scope is one complete decoded PostgreSQL transaction, so its source
        #: relation locks cover every event-admission decision in that unit.
        self._toast_admission_lock = threading.Lock()
        self._toast_admission_conn = None
        self._toast_admission_contexts: dict[str, _ToastAdmissionContext] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        #: Every change this run is still carrying, whatever state it is in. The
        #: *state* decides what it is, not which container it sits in (rubric 1.9):
        #: `pending()` filters on `_LIVE_CHANGE_STATES | {due}`, and a change that
        #: reaches a terminal state is removed by `resolve()` / `_supersede()`.
        self._changes: list[CatalogChange] = []
        #: relations whose `source_relations` row needs (re)writing
        self._dirty: dict[str, SourceRelation] = {}
        #: data units that already triggered a synchronous late-schema probe
        self._schema_probe_names: set[str] = set()
        #: The latest per-schema visibility proof. `empty` and `unavailable` are
        #: safety states, not evidence that every known table in that schema vanished.
        self._schema_liveness: dict[str, str] = {
            name: SCHEMA_VISIBLE for name in self.schemas
        }
        self._admission_errors: dict[str, str] = {}
        self._schema_refusals: dict[str, object] = {}
        #: TOAST classification is stable for one observed catalog generation. Keep
        #: the event path from rebuilding the same table policy for every record;
        #: source revalidation remains a separate admission operation below.
        self._toast_policy_cache: dict[str, tuple[tuple, object]] = {}
        self.toast_policy_builds = 0
        self.toast_policy_cache_hits = 0
        self.toast_admission_checks = 0
        self.toast_source_revalidations = 0
        self.toast_admission_rejections = 0
        #: `name -> the CatalogChange object that is in state `unconfirmed``.
        #: It used to be `name -> ((kind, new_oid), count)` while the observation's own
        #: object was thrown away and a *new* one constructed for the confirming poll -
        #: so the declared `unconfirmed -> pending` edge described no object production
        #: ever advanced (Codex r1 MAJOR-1). The same object now carries the streak.
        self._unconfirmed: dict[str, CatalogChange] = {}
        self.polls = 0
        #: Polls that actually READ the source catalog and compared it. `polls` counts
        #: attempts, including the discarded empty-schema observation, and a run with
        #: zero successful ones has no baseline at all: it cannot have noticed a drop,
        #: and it has nothing to persist. The supervisor refuses to call such a run
        #: successful (Codex r4 BLOCKER-2).
        self.successful_polls = 0
        self.empty_polls = 0
        self.superseded = 0
        self.last_error: str | None = None
        #: An undeclared state-machine transition or a state outside the domain. Kept
        #: separate from `last_error` because the policy is different: a poll that could
        #: not reach the source is transient and fails soft, while a catalog change that
        #: moved along an edge nobody declared is a destructive DDL nobody reasoned
        #: about. `supervisor.run_engine_bounded` fails the run on this one (A51 row 51).
        self.machine_error: str | None = None
        self.last_lsn: int = 0
        self._snapshot_partitions: set[str] = set()
        #: Monotone watcher epoch used by a destination plan to identify the
        #: observation set it was built from.  Settlement may still absorb an older
        #: committed plan, but it must not clear dirty state learned in a later epoch.
        self._epoch = 0

    @property
    def emit_marker(self) -> bool:
        return self.marker.enabled

    @property
    def markers_emitted(self) -> int:
        return self.marker.writes

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> CatalogWatcher:
        if self.poll_seconds <= 0:
            log.info("catalog polling disabled (poll_seconds=%s)", self.poll_seconds)
            return self
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError(
                "cannot restart the catalog watcher while its previous polling "
                "thread is alive; quiescence must be proved before a hand-off"
            )
        # A live discovery hand-off pauses the watcher while the main engine is
        # quiesced and the throwaway snapshot runs. Reusing the same watcher preserves
        # its catalog-change machine and baselines, so restart the thread rather than
        # constructing a second observer. The stop event belongs to the old thread.
        self._stop.clear()
        self.quiesced = False
        # Make discovery/re-snapshot decisions before the main engine is built. The
        # background loop still polls at the configured interval, but a table created
        # while the process was down is available to the existing blocking snapshot
        # machinery immediately rather than one interval later.
        self.poll_quietly()
        self._thread = threading.Thread(target=self._loop, name="cdc-catalog", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> bool:
        """Stop polling and report whether the thread is **actually dead**.

        It used to set the event, join for `max(1, poll_seconds)`, and return whatever
        happened. That is not quiescence, and the difference is load-bearing: the
        supervisor takes its terminal catalog verdict on the strength of this call, so a
        poll that outlives the join can take an undeclared transition, or learn a
        relation, *after* the run has been judged a success (Codex r3 MAJOR-3,
        reproduced with a real watcher thread held at a barrier).

        The join is generous now — a poll's own queries are bounded (see `_connect`), so
        a live thread past this point means something is genuinely wrong — and the
        boolean is what the supervisor refuses to report success without.
        """
        self._stop.set()
        if self._thread is None:
            self._close_toast_admission_connection()
            return True
        self._thread.join(timeout=self.quiesce_timeout)
        alive = self._thread.is_alive()
        if alive:
            log.error(
                "the catalog poller did not stop within %.1fs; it may still be holding "
                "a source connection and may still mutate its own state",
                self.quiesce_timeout,
            )
        self.quiesced = not alive
        if self.quiesced:
            self._close_toast_admission_connection()
        return self.quiesced

    def _close_toast_admission_connection(self) -> None:
        """Close reusable admission connections after all callbacks quiesce."""
        with self._toast_admission_lock:
            contexts = tuple(self._toast_admission_contexts.values())
            self._toast_admission_contexts.clear()
            for context in contexts:
                try:
                    context.conn.execute("ROLLBACK")
                except Exception:
                    log.debug("could not roll back toast admission scope", exc_info=True)
                context.conn.close()
            conn = self._toast_admission_conn
            self._toast_admission_conn = None
            if conn is not None:
                conn.close()

    def _connect(self):
        """A source connection whose queries are BOUNDED, not just its handshake.

        `connect_timeout` covers the handshake and nothing else, so a source that goes
        dark on an established socket leaves a poll blocked for ever — which is how
        `stop()` came to return with its thread alive (Codex r3 MAJOR-3). The same two
        bounds `SourceHealth` uses: the server's `statement_timeout`, and the client's
        keepalives plus `tcp_user_timeout`, which are what actually fire against a
        blackhole.
        """
        return catalog_poll.connect(self)

    def _loop(self) -> None:
        # Poll once immediately: a table dropped while this pipeline was down must be
        # noticed on the run that follows, not `poll_seconds` into it.
        while True:
            self.poll_quietly()
            if self._stop.wait(self.poll_seconds):
                return

    def poll_quietly(self) -> list[CatalogChange]:
        return catalog_poll.poll_quietly(self)

    def remember_schema_refusal(self, refused) -> None:
        name = refused.target or (
            f"{refused.source_schema}.{refused.source_table}"
            if refused.source_schema and refused.source_table
            else "catalog"
        )
        with self._lock:
            self._schema_refusals[str(name)] = refused

    def schema_refusals(self) -> tuple[object, ...]:
        with self._lock:
            return tuple(self._schema_refusals.values())

    # -- polling ------------------------------------------------------------ #
    def poll(self) -> list[CatalogChange]:
        return catalog_poll.poll(self)

    def _ensure_published(self, conn, observed, changes: list[CatalogChange]) -> None:
        admission_mod.ensure_published(self, conn, observed, changes)

    def captured_relations(self) -> tuple[SourceRelation, ...]:
        """Published relations from the latest successful catalog observation."""
        with self._lock:
            return tuple(
                sorted(
                    (
                        relation for relation in self.known.values()
                        if relation.published
                        and relation.admission_state in {
                            ADMISSION_ADMITTED, ADMISSION_EXTERNAL
                        }
                    ),
                    key=lambda relation: relation.qualified,
                )
            )

    def descriptors_for(self, qualified: str) -> dict[str, object]:
        """Return the latest catalog descriptor tree for one source relation.

        Row envelopes are allowed to omit Connect logical names (notably when
        Debezium is configured to emit decimal/interval strings).  The catalog
        observation already carries the source type identity, so DML can use it as
        the authoritative descriptor without issuing a catalog query per event.
        """
        from .errors import SchemaEvolutionRefused
        from .typed_types import UnsupportedType, native_type

        with self._lock:
            relation = self.known.get(str(qualified))
            if relation is None:
                return {}
            descriptors = {
                column.destination_name: column.descriptor
                for column in relation.columns
                if column.descriptor is not None
            }
        for name, descriptor in descriptors.items():
            try:
                native_type(descriptor)
            except (UnsupportedType, ValueError) as exc:
                raise SchemaEvolutionRefused(
                    f"source catalog descriptor for {qualified}.{name} is not "
                    f"deliverable through the strict native authority: {exc}",
                    source_schema=str(qualified).partition(".")[0],
                    source_table=str(qualified).partition(".")[2],
                    target=str(qualified),
                ) from exc
        return descriptors

    def _toast_policy_key(self, relation: SourceRelation) -> tuple:
        return (
            int(relation.oid),
            relation.relfilenode,
            relation.relation_type_oid,
            relation.replica_identity,
            relation.full_activation_lsn,
            relation.full_invalidation_lsn,
            tuple(
                (
                    column.destination_name,
                    column.type_identity,
                    column.attstorage,
                )
                for column in relation.columns
            ),
            self._epoch,
        )

    def toast_policy_for(self, qualified: str, *, event_lsn: int | None = None):
        """Return the route, closing an observed FULL interval at an event LSN."""
        from .toast import classify_relation

        with self._lock:
            relation = self.known.get(str(qualified))
            if relation is None:
                return None
            if (
                event_lsn is not None
                and str(relation.replica_identity).lower() != "f"
                and relation.full_activation_lsn is not None
                and relation.full_invalidation_lsn is None
            ):
                try:
                    candidate = int(event_lsn)
                except (TypeError, ValueError):
                    candidate = 0
                if candidate > int(relation.full_activation_lsn):
                    relation = replace(relation, full_invalidation_lsn=candidate)
                    self.known[str(qualified)] = relation
                    self._dirty[str(qualified)] = relation
            cache_key = self._toast_policy_key(relation)
            cached = self._toast_policy_cache.get(str(qualified))
            if cached is not None and cached[0] == cache_key:
                self.toast_policy_cache_hits += 1
                return cached[1]
            policy = classify_relation(
                relation.qualified,
                relation.columns,
                replica_identity=relation.replica_identity,
                binary_mode=self.binary_handling_mode,
                hstore_mode=self.hstore_handling_mode,
                full_activation_lsn=relation.full_activation_lsn,
                full_invalidation_lsn=relation.full_invalidation_lsn,
            )
            self._toast_policy_cache[str(qualified)] = (cache_key, policy)
            self.toast_policy_builds += 1
            return policy

    def _close_toast_interval(self, qualified: str, event_lsn: int | None) -> None:
        """Record a conservative upper bound after locked source revalidation fails."""
        with self._lock:
            relation = self.known.get(str(qualified))
            if relation is None or relation.full_activation_lsn is None:
                return
            activation = int(relation.full_activation_lsn)
            try:
                candidate = int(event_lsn) if event_lsn is not None else activation + 1
            except (TypeError, ValueError):
                candidate = activation + 1
            if candidate <= activation:
                candidate = activation + 1
            if relation.full_invalidation_lsn is None or candidate < int(
                relation.full_invalidation_lsn
            ):
                relation = replace(relation, full_invalidation_lsn=candidate)
                self.known[str(qualified)] = relation
                self._dirty[str(qualified)] = relation
                self._toast_policy_cache.pop(str(qualified), None)

    def admit_toast_event(
        self,
        qualified: str,
        event_lsn: int | None = None,
        txn_id: str | None = None,
    ) -> bool:
        """Validate one residual event inside its source-unit admission scope.

        The policy cache answers the cheap generation question.  For a residual
        table, the first event of a complete source unit opens a source transaction,
        locks the relation, and re-reads ``relreplident``.  The transaction stays
        open until :meth:`end_toast_admission` after the unit's last event has been
        admitted.  A concurrent ``REPLICA IDENTITY DEFAULT`` therefore cannot slip
        between two event decisions.  Direct callers that omit ``txn_id`` retain a
        short, one-event scope for probes and tests.
        """
        from .naming import quote
        from .toast import ToastRoute

        self.toast_admission_checks += 1
        policy = self.toast_policy_for(qualified, event_lsn=event_lsn)
        if policy is None:
            return True
        if not policy.accepts_event(event_lsn):
            self.toast_admission_rejections += 1
            return False
        if policy.route is not ToastRoute.REPLICA_IDENTITY_FULL:
            return True
        with self._lock:
            relation = self.known.get(str(qualified))
        if relation is None:
            self.toast_admission_rejections += 1
            return False
        admission_key = (
            self._toast_policy_key(relation),
            str(txn_id),
        ) if txn_id is not None else None
        with self._toast_admission_lock:
            context = (
                self._toast_admission_contexts.get(str(txn_id))
                if txn_id is not None
                else None
            )
            try:
                if context is not None:
                    cached = context.decisions.get(str(qualified))
                    if cached is not None and cached[0] == admission_key:
                        if not cached[1] or not policy.accepts_event(event_lsn):
                            self.toast_admission_rejections += 1
                            return False
                        return True
                    conn = context.conn
                else:
                    conn = self._toast_admission_conn
                    self._toast_admission_conn = None
                if conn is None or conn.closed:
                    conn = catalog_poll.connect(
                        self, autocommit=False, dsn=self.primary_dsn
                    )
                if context is None:
                    conn.execute("BEGIN TRANSACTION")
                if txn_id is not None and context is None:
                    context = _ToastAdmissionContext(str(txn_id), conn)
                    self._toast_admission_contexts[str(txn_id)] = context
                conn.execute(
                    f"LOCK TABLE {quote(relation.schema)}.{quote(relation.table)} "
                    "IN ACCESS SHARE MODE NOWAIT"
                )
                row = conn.execute(
                    "SELECT relreplident FROM pg_class WHERE oid = %s", [relation.oid]
                ).fetchone()
                self.toast_source_revalidations += 1
                current_full = bool(row and str(row[0]).lower() == "f")
                accepted = current_full and policy.accepts_event(event_lsn)
                if context is not None:
                    context.decisions[str(qualified)] = (admission_key, accepted)
                else:
                    conn.execute("COMMIT")
                    self._toast_admission_conn = conn
                if not accepted:
                    self._close_toast_interval(qualified, event_lsn)
                    self.toast_admission_rejections += 1
                return accepted
            except Exception:
                if context is not None:
                    self._toast_admission_contexts.pop(str(context.txn_id), None)
                if conn is not None:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception:
                        log.debug("could not roll back toast admission probe", exc_info=True)
                    conn.close()
                self._close_toast_interval(qualified, event_lsn)
                self.toast_admission_rejections += 1
                return False

    def end_toast_admission(self, txn_id: str, *, commit: bool) -> None:
        """Release the source locks after a complete unit is admitted.

        ``commit=False`` is used when folding or destination planning rejects the
        unit.  The source-side transaction is observational, so either outcome only
        releases its locks; the destination transaction remains the durability
        authority.
        """
        key = str(txn_id)
        with self._toast_admission_lock:
            context = self._toast_admission_contexts.pop(key, None)
            if context is None:
                return
            try:
                context.conn.execute("COMMIT" if commit else "ROLLBACK")
                if self._toast_admission_conn is None or self._toast_admission_conn.closed:
                    self._toast_admission_conn = context.conn
                else:
                    context.conn.close()
            except Exception:
                try:
                    context.conn.execute("ROLLBACK")
                except Exception:
                    log.debug("could not roll back toast admission scope", exc_info=True)
                context.conn.close()

    def event_shape_missing(self, record, catalog_names: set[str]) -> tuple[str, ...]:
        return catalog_support.event_shape_missing(self, record, catalog_names)

    def snapshot_names(self) -> tuple[str, ...]:
        """Logical relations whose snapshot callbacks are expected at startup.

        With ``publish_via_partition_root`` Debezium emits the child rows under the
        published parent relation and reports completion for that parent. The catalog
        query still records child partitions for 7.3 diagnostics, but they must not be
        added to the exact callback set or a healthy snapshot would look incomplete.
        """
        with self._lock:
            return tuple(
                sorted(
                    {
                        relation.qualified
                        for relation in self.known.values()
                        if relation.published
                        and relation.admission_state in {
                            ADMISSION_ADMITTED, ADMISSION_EXTERNAL
                        }
                        and not relation.is_partition
                    }
                )
            )

    def new_relations(self, *, exclude: set[str] | None = None) -> tuple[SourceRelation, ...]:
        """Relations whose first-sight ``new`` marker is still live."""
        excluded = exclude or set()
        with self._lock:
            return tuple(
                sorted(
                    (
                        self.known.get(change.qualified)
                        for change in self._live()
                        if (
                        change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
                            and change.qualified not in excluded
                            and self.known.get(change.qualified)
                            and self.known[change.qualified].published
                            and self.known[change.qualified].admission_state in {
                                ADMISSION_ADMITTED, ADMISSION_EXTERNAL
                            }
                            and state_interactions.discovery_admission_allowed(
                                change, self.known.get(change.qualified)
                            )
                        )
                    ),
                    key=lambda relation: relation.qualified,
                )
            )

    def complete_discoveries(self, names: set[str]) -> list[str]:
        """Close ``new`` changes discharged by a completed relation re-snapshot.

        Live discovery applies its audit marker from the re-snapshot commit, before a
        resumed main-stream group exists.  Leaving the watcher's ``new`` change pending
        would make that first later CDC group write a second, ``applied=False`` marker.
        Move the existing object through the declared catalog machine and retain its
        dirty relation for the normal durable registry flush at run teardown.
        """
        completed: list[str] = []
        wanted = set(names)
        with self._lock:
            for change in self._live():
                if change.kind != CHANGE_NEW or change.qualified not in wanted:
                    continue
                if change.state != CHANGE_DUE:
                    change.to(CHANGE_DUE)
                change.to(CHANGE_APPLIED)
                completed.append(change.qualified)
            if completed:
                done = set(completed)
                self._changes = [
                    change for change in self._changes if change.qualified not in done
                ]
            self.replicated |= wanted
        return completed

    def pending_admission(self) -> tuple[str, ...]:
        """Discovery relations that cannot yet enter the snapshot hand-off.

        This is intentionally independent of ``new_relations``: an unpublished or
        refused relation is not snapshot-ready, but it must remain visible to the
        supervisor and to the next poll/restart.
        """
        with self._lock:
            return tuple(
                sorted(
                    change.qualified
                    for change in self._live()
                    if change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
                    and (
                        self.known.get(change.qualified) is None
                        or self.known[change.qualified].admission_state
                        not in {ADMISSION_ADMITTED, ADMISSION_EXTERNAL}
                        or not self.known[change.qualified].published
                    )
                )
            )

    def observe_unit(self, unit) -> None:
        from . import catalog_support

        catalog_support.observe_unit(self, unit)

    def allowed_event_fields(self, qualified: str) -> set[str]:
        """Return the union of the current and every fenced schema epoch."""
        with self._lock:
            relation = self.known.get(qualified)
            allowed = (
                {column.destination_name for column in relation.columns}
                if relation is not None
                else set()
            )
            for change in self._live():
                if change.qualified != qualified or change.kind != CHANGE_SCHEMA:
                    continue
                for column in change.column_changes:
                    if column.destination_old_name:
                        allowed.add(column.destination_old_name)
                    if column.destination_new_name:
                        allowed.add(column.destination_new_name)
            return allowed

    def read_columns(
        self,
        relation: SourceRelation,
        key_columns: tuple[str, ...],
        value_columns: tuple[str, ...],
    ) -> list[tuple]:
        from . import catalog_support

        return catalog_support.read_columns(
            self, relation, key_columns, value_columns
        )

    def _compare(self, observed: dict[str, SourceRelation], lsn: int) -> list[CatalogChange]:
        added: list[CatalogChange] = []
        superseded: list[str] = []
        with self._lock:
            self.polls += 1
            self.last_lsn = lsn
            self._epoch += 1
            # Pending destructive changes are `interesting` even after their relation
            # was forgotten: the *cancellation* in guard 2 depends on this poll
            # visiting the name at all (Codex 4).
            interesting = (
                self.include
                | self.replicated
                | set(self.known)
                | {c.qualified for c in self._live() if c.kind in DESTRUCTIVE}
            )
            if self.auto_discover:
                # `observed` is the catalog's complete relation set in this mode. This
                # is what makes a table omitted from CDC_TABLES discoverable; the
                # publication membership still decides whether it can stream.
                interesting |= set(observed)
            for name in sorted(interesting):
                source_schema, _, source_table = name.partition(".")
                if not source_table or (
                    not self.all_schemas and source_schema not in self.schemas
                ):
                    # A relation outside the configured catalog scope is unobserved,
                    # not dropped. In all-schemas mode this branch is only defensive.
                    continue
                if self._schema_liveness.get(
                    source_schema,
                    SCHEMA_UNAVAILABLE if self.all_schemas else SCHEMA_VISIBLE,
                ) != SCHEMA_VISIBLE:
                    # Empty/unavailable is an ERROR/LIVENESS observation, never a
                    # destructive absence proof. A later positive poll re-enters the
                    # ordinary comparison path.
                    continue
                current = observed.get(name)
                previous = self.known.get(name)
                if (
                    current is not None
                    and current.is_partition
                    and name not in self.include
                    and name not in self.replicated
                    and name not in self.known
                ):
                    # Publication-root snapshots may report child partitions, but a
                    # child is not an independent discovery target.
                    continue
                pending_recreates, retained_relation = catalog_generation.pending_for(
                    self._changes, self.known, name, previous
                )
                if current is not None:
                    if previous is not None:
                        # Preserve the durable admission state while projecting this
                        # poll. A persisted ERROR/REFUSED row must be retried after a
                        # restart instead of being reset by SourceRelation defaults.
                        current = replace(
                            current, admission_state=previous.admission_state
                        )
                        observed[name] = current
                    # A present relation cancels a drop.  A physical rewrite within the
                    # same lifecycle refreshes an existing recreate obligation; it does
                    # not supersede it merely because its relfilenode changed.
                    matching_recreate = catalog_generation.matching_recreate(
                        pending_recreates, current
                    )
                    if matching_recreate is not None:
                        catalog_generation.refresh_recreate(
                            matching_recreate, current, lsn
                        )
                        self.known[name] = current
                        self._dirty[name] = current
                    elif catalog_generation.has_newer_recreate(
                        pending_recreates, current
                    ):
                        superseded.extend(self._supersede(name, CHANGE_RECREATED))
                    superseded.extend(self._supersede(name, CHANGE_DROPPED))
                    if previous is not None and catalog_generation.lifecycle_identities_equal(
                        current, previous
                    ):
                        column_diff = diff_columns(previous.columns, current.columns)
                        stale = self._unconfirmed.get(name)
                        if stale is not None and not column_diff:
                            self._unconfirmed.pop(name, None)
                            # The relation is unchanged, so whatever streak was building
                            # describes a world that no longer exists. Cancelled through
                            # the machine rather than dropped on the floor, so nothing is
                            # left in a state nothing will ever advance.
                            stale.to(CHANGE_SUPERSEDED)
                            self.superseded += 1
                elif not pending_recreates:
                    # A replacement that disappears before its quarantine plan is
                    # durable is not yet a final DROP decision. Keep the recreate
                    # obligation so the retained image survives until the next
                    # re-snapshot can read the source and apply DROP_LOG or
                    # DROP_REPLICATE from that final fact.
                    superseded.extend(self._supersede(name, CHANGE_RECREATED))
                # AFTER supersession: a change this poll has just cancelled must not
                # then suppress the change this poll should queue instead.
                queued = any(
                    c.qualified == name and c.kind in FENCED for c in self._live()
                )
                if previous is None:
                    if current is None:
                        if name not in self.replicated or queued:
                            continue
                        # We hold a destination table for it and the source does not
                        # have the table. That IS a drop, and it is the case that
                        # matters most: a table dropped while this pipeline was down,
                        # or one replicated before `source_relations` existed, has no
                        # persisted oid to compare against. Reporting it only when an
                        # oid happens to be on file would make restart-time detection
                        # depend on bookkeeping luck (MEASURED: the first cut did, and
                        # a drop between two runs went unnoticed).
                        schema, _, table = name.partition(".")
                        change = self._confirm(
                            name,
                            CatalogChange(
                                kind=CHANGE_DROPPED, schema=schema, table=table,
                                detected_lsn=lsn,
                            ),
                        )
                        if change is not None:
                            added.append(change)
                        continue
                    if name in self.unrelatable:
                        # RECONCILE, DO NOT ADOPT (rubric 1.9, Codex r5 BLOCKER-1).
                        #
                        # The destination holds rows for this relation, the durable
                        # baseline says a run failed to confirm it, and there is no
                        # recorded oid to compare against. "First sight" would write
                        # the currently observed oid down as history — and from then on
                        # the registry agrees with the source, so a drop-and-recreate
                        # that happened in the unchecked window is undetectable for
                        # ever. Measured: old rows beside new, every run successful.
                        #
                        # It is queued as `recreated` because that is exactly what it
                        # may be, and because `recreated` is the existing machinery for
                        # "the destination table holds a different relation's rows":
                        # confirmed over `confirm_polls`, fenced on the WAL, and it
                        # leaves the table
                        # `awaiting_snapshot` so the rebuild is owed durably by
                        # `TABLE_LIFECYCLE` rather than by this run's memory.
                        change = self._confirm(
                            name,
                            self._change(
                                CHANGE_RECREATED, current, lsn,
                                old_oid=None, new_oid=current.oid,
                            ),
                        )
                        if change is not None:
                            added.append(change)
                            # Recorded only now, and `dirty()` excludes it while the
                            # replacement obligation is still pending, so the oid
                            # becomes history in the SAME transaction that records the
                            # lifecycle quarantine and marks it owed - never before.
                            self.known[name] = current
                            self._dirty[name] = current
                        continue
                    if name in self.replicated or name in self.include or self.auto_discover:
                        # First sight. Record the oid; report `new` only for something
                        # we have never replicated (rubric 2.3's hook).
                        self.known[name] = current
                        self._dirty[name] = current
                        if name not in self.replicated:
                            added.append(
                                self._change(CHANGE_NEW, current, lsn, new_oid=current.oid)
                            )
                    continue
                if current is None:
                    if queued:
                        # One pending destructive action per relation, or the next poll
                        # reports the same drop again while the first is still waiting
                        # for its fence (MEASURED: two markers for one DROP). The oid
                        # and the membership are deliberately KEPT: the action may yet
                        # be refused or superseded, and forgetting them made a
                        # cancelled drop indistinguishable from a table we never had.
                        continue
                    drop_relation = retained_relation or previous
                    if drop_relation is not None and drop_relation is not previous:
                        self.known[name] = retained_relation
                        self._dirty[name] = retained_relation
                    change = self._confirm(
                        name, catalog_generation.dropped_change(drop_relation, lsn)
                    )
                    if change is not None:
                        added.append(change)
                    continue
                if not catalog_generation.lifecycle_identities_equal(current, previous):
                    if queued:
                        continue
                    change = self._confirm(
                        name,
                        catalog_generation.recreated_change(
                            current, retained_relation or previous, lsn
                        ),
                    )
                    if change is not None:
                        added.append(change)
                        self.known[name] = current
                        self._dirty[name] = current
                    continue
                admission_ready = {ADMISSION_ADMITTED, ADMISSION_EXTERNAL}
                if (
                    self.auto_discover
                    and name not in self.replicated
                    and current.admission_state not in admission_ready
                    and not any(
                        change.qualified == name and change.kind == CHANGE_NEW
                        for change in self._live()
                    )
                ):
                    # Recreate live NEW work from the durable source-relations row.
                    # Admission ERROR/REFUSED is an obligation, not a one-run log.
                    added.append(
                        self._change(CHANGE_NEW, current, lsn, new_oid=current.oid)
                    )
                    continue
                column_changes = diff_columns(previous.columns, current.columns)
                if column_changes:
                    schema_queued = any(
                        c.qualified == name and c.kind == CHANGE_SCHEMA
                        for c in self._live()
                    )
                    if not schema_queued:
                        change = self._confirm(
                            name,
                            self._change(
                                CHANGE_SCHEMA,
                                current,
                                lsn,
                                old_oid=previous.oid,
                                new_oid=current.oid,
                                column_changes=column_changes,
                            ),
                        )
                        if change is not None:
                            added.append(change)
                            self.known[name] = current
                            self._dirty[name] = current
                    # Do not collapse a schema transition into a plain source-relation
                    # update: the destination action must happen before this baseline
                    # is persisted.
                    continue
                if current.published != previous.published:
                    added.append(
                        self._change(
                            CHANGE_UNPUBLISHED if not current.published else CHANGE_REPUBLISHED,
                            current, lsn, old_oid=previous.oid, new_oid=current.oid,
                        )
                    )
                if current != previous:
                    self.known[name] = current
                    self._dirty[name] = current
            for change in added:
                change.to(CHANGE_PENDING)
            self._changes.extend(added)
        for change in added:
            log.warning(
                "source catalog change: %s %s (oid %s -> %s) detected at lsn %s after "
                "%s confirming poll(s)",
                change.kind, change.qualified, change.old_oid, change.new_oid,
                change.detected_lsn, change.confirmations,
            )
        for name in superseded:
            log.warning(
                "superseding a stale catalog action for %s: the relation is present "
                "at the source again", name,
            )
        return added

    def _confirm(self, name: str, change: CatalogChange) -> CatalogChange | None:
        return catalog_change_queue.confirm(self, name, change)

    def _supersede(self, name: str, *kinds: str) -> list[str]:
        """Cancel live changes of `kinds` for `name`. Caller holds the lock."""
        cancelled = [
            c for c in self._live() if c.qualified == name and c.kind in kinds
        ]
        unconfirmed = self._unconfirmed.get(name)
        if unconfirmed is not None and unconfirmed.kind in kinds:
            unconfirmed.to(CHANGE_SUPERSEDED)
            self._unconfirmed.pop(name, None)
            self.superseded += 1
        if not cancelled:
            return []
        for change in cancelled:
            change.to(CHANGE_SUPERSEDED)
        self.superseded += len(cancelled)
        self._changes = [c for c in self._changes if c.state in LIVE_CHANGE_STATES]
        return [name]

    def _change(self, kind, relation: SourceRelation, lsn: int, **oids) -> CatalogChange:
        return catalog_change_queue.make_change(self, kind, relation, lsn, **oids)

    def supersede_recreated(self, change: CatalogChange, current) -> CatalogChange | None:
        return catalog_change_queue.supersede_recreated(self, change, current)

    def _emit_marker(self, conn, changes: list[CatalogChange]) -> None:
        """Write a WAL record past the detected change, so the fence can open.

        **Only changes that are still waiting for their fence are moved to `marked`**
        (Codex r1 MAJOR-1). A change the applier has already taken through `due()` is
        still in the live list - `resolve()` removes it only after the COMMIT - and this
        loop used to walk it back to `marked`, which `machines.CATALOG_CHANGE` does not
        declare and which is meaningless anyway: its fence is already open. The real
        polling thread reached that edge whenever a poll overlapped an applier that had
        just asked what was due, and `poll_quietly` wrote the `IllegalTransition` to
        `last_error` and carried on.
        """
        payload = {"changes": [c.kind + ":" + c.qualified for c in changes]}
        if not self.marker.emit(conn, marker_mod.CATALOG_FENCE, payload):
            self.last_error = self.marker.last_error or (
                "the catalog fence marker could not be written to the source"
            )
            return
        with self._lock:
            for change in self._live():
                queued = _queued(change)
                if queued.can(CHANGE_MARKED):
                    queued.to(CHANGE_MARKED)

    # -- what the applier asks ---------------------------------------------- #
    def due(self, durable_lsn: int) -> list[CatalogChange]:
        """Pending changes whose fence has opened, in detection order.

        The fence is `durable_lsn >= detected_lsn`: everything that happened before
        the DDL is already committed at the destination, so applying the DDL now
        cannot delete rows that a later event would have re-created.
        """
        out: list[CatalogChange] = []
        with self._lock:
            for change in self._live():
                _queued(change)
                if change.kind not in FENCED:
                    change.to(CHANGE_DUE)
                    # Nothing is removed for a `new`, `unpublished` or `republished`
                    # change - it is a marker row and an operator decision - so there is
                    # nothing for the fence to protect.
                    out.append(change)
                    continue
                if durable_lsn >= change.detected_lsn:
                    change.to(CHANGE_DUE)
                    out.append(change)
                    continue
                change.deferrals += 1
                change.to(CHANGE_DEFERRED)
                if self.grace_seconds and (
                    time.monotonic() - change.detected_at >= self.grace_seconds
                ):
                    log.warning(
                        "applying %s for %s after %.0fs of grace even though the "
                        "destination is only at lsn %s (< %s): in-flight events for "
                        "that table could re-create it. CDC_CATALOG_GRACE is EXCLUDED "
                        "from the structural correctness guarantee (ADR 0001 §18/A38).",
                        change.kind, change.qualified, self.grace_seconds,
                        durable_lsn, change.detected_lsn,
                    )
                    change.to(CHANGE_DUE)
                    out.append(change)
        return out

    def resolve(self, changes: list[CatalogChange]) -> None:
        """Drop changes that have reached a terminal state. Caller has COMMITted."""
        with self._lock:
            done = set(map(id, changes))
            self._changes = [c for c in self._changes if id(c) not in done]

    def settle(self, changes: list[CatalogChange], planned_epoch: int | None = None) -> None:
        catalog_change_queue.settle(self, changes, planned_epoch)

    def queue(self, change: CatalogChange) -> CatalogChange:
        """Put a change into the queue by taking the `observed -> pending` EDGE.

        The one way in, for `_compare` and for the 1.5 suite alike. Appending to the
        list without moving the state was how "it is in the pending list but its state
        says `observed`" became representable, and a distinction with no meaning is
        exactly what rubric 1.9 is about (Codex r1 MAJOR-1).
        """
        with self._lock:
            if change.state in (CHANGE_OBSERVED, CHANGE_UNCONFIRMED):
                change.to(CHANGE_PENDING)
            self._changes.append(change)
            self._epoch += 1
        return change

    def _live(self) -> list[CatalogChange]:
        """The changes whose STATE says they are still this watcher's business.

        The list is an ordering; the state is the meaning (rubric 1.9). Filtering here
        rather than trusting membership is what stops a change that was superseded or
        applied from being re-queued by a poll that happens to still see it.
        """
        return [c for c in self._changes if c.state in LIVE_CHANGE_STATES]

    def pending(self) -> list[CatalogChange]:
        with self._lock:
            return self._live()

    def pending_destructive(self) -> list[CatalogChange]:
        with self._lock:
            return [c for c in self._live() if c.kind in DESTRUCTIVE]

    def pending_fenced(self) -> list[CatalogChange]:
        """Fenced catalog work still waiting for a destination commit.

        Schema changes share the destructive-change WAL fence even though their
        destination action is non-destructive. The supervisor must therefore hold a
        quiet run open for both classes; checking only ``pending_destructive`` could
        leave an ADD/DROP/RENAME discovered by the final poll until the next run.
        """
        with self._lock:
            return [c for c in self._live() if c.kind in FENCED]

    def dirty(self, *, exclude: set[str] | None = None) -> list[SourceRelation]:
        """Relations whose persisted row is stale, minus `exclude`. Non-destructive.

        `exclude` is how the applier keeps persisted state from running ahead of the
        actions it implies: while a `recreated` change is still waiting for its fence,
        writing the new oid would make the next run see agreement with the source and
        never notice the drop. Nothing is forgotten until `clear_dirty()`, which the
        applier calls only after its transaction has **committed**.
        """
        blocked = exclude or set()
        with self._lock:
            return [rel for name, rel in self._dirty.items() if name not in blocked]

    def clear_dirty(self, names: list[str]) -> None:
        with self._lock:
            for name in names:
                self._dirty.pop(name, None)

    def clear_dirty_if_current(
        self, relations, planned_epoch: int | None = None
    ) -> None:
        catalog_change_queue.clear_dirty_if_current(self, relations, planned_epoch)

    def forget(self, name: str) -> None:
        with self._lock:
            self.known.pop(name, None)
            self._dirty.pop(name, None)
            self._toast_policy_cache.pop(name, None)
            stale = self._unconfirmed.pop(name, None)
            if stale is not None:
                stale.to(CHANGE_SUPERSEDED)
                self.superseded += 1
            self.replicated.discard(name)

    def observe_replicated(self, names: set[str]) -> None:
        """Tell the watcher which tables now have destination tables."""
        with self._lock:
            self.replicated |= names

    def summary(self) -> dict:
        return catalog_support.summary(self)
