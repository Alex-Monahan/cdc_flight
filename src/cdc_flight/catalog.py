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
| the name is gone from `pg_class` | `dropped` | drop the destination table |
| the name is back with a different `oid` | `recreated` | drop the destination table and mark it `awaiting_snapshot` (rubric 2.3/3.4 owns rebuilding it) |
| the name is there but no longer in the publication | `unpublished` | nothing but a marker + alert: Postgres still holds the rows, so dropping the destination would destroy data the source has |
| a table in the include list we have never seen | `new` | nothing but a marker; rubric 2.3 owns discovery, and this is the mechanism it will use |

**This module observes. It never decides**: `cdc_flight.catalog_apply` owns the
policy, the circuit breaker and the DDL, because the observation and the action are
separated in time and the gap is where a stale fact becomes a wrong drop.

**The fence.** A detected drop must not be applied before the destination has
consumed every event that happened *before* it. The drop is discovered after the
fact, so the poll records `pg_current_wal_lsn()` as `detected_lsn` and the applier
holds the action until its durable resume point reaches that LSN. On a quiet source
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

**Confirmation.** A destructive change is queued only after the relation has been
absent (or the oid changed) on `CDC_DROP_CONFIRM_POLLS` consecutive polls (default 2),
and a relation that reappears **cancels** any pending destructive action for it. A
poll that observes *zero* relations in the schema is discarded outright: that is the
wrong-database / mid-restore signature and can never legitimately mean "drop
everything" (Opus Q2/Q5).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

from . import source_marker as marker_mod
from .destination import CONTROL_SCHEMA
from .machines import (
    CATALOG_CHANGE,
    CHANGE_DEFERRED,
    CHANGE_DUE,
    CHANGE_MARKED,
    CHANGE_OBSERVED,
    CHANGE_PENDING,
    CHANGE_SUPERSEDED,
    CHANGE_UNCONFIRMED,
)

log = logging.getLogger("cdc_flight.catalog")

CHANGE_DROPPED = "dropped"
CHANGE_RECREATED = "recreated"
CHANGE_UNPUBLISHED = "unpublished"
CHANGE_REPUBLISHED = "republished"
CHANGE_NEW = "new"

#: Actions that remove the destination table when `drop_mode='replicate'`.
DESTRUCTIVE = (CHANGE_DROPPED, CHANGE_RECREATED)

DROP_REPLICATE = "replicate"
DROP_LOG = "log"
DROP_IGNORE = "ignore"

#: Base prefix; the reason is appended, so the fence marker's `pg_logical_emit_message`
#: prefix is `cdcf_catalog_fence` and D9's heartbeat will be `cdcf_idle_heartbeat`
#: (`cdc_flight.source_marker`).
MARKER_PREFIX = "cdcf"

#: One row per table in the captured schema, with the two facts logical decoding
#: cannot give us: the relation `oid` (identity across a drop + recreate) and
#: publication membership. `relkind IN ('r','p')` covers ordinary and partitioned
#: tables; a partition is an ordinary table whose parent is published, which is why
#: the include list - not this query - decides what counts as ours.
_CATALOG_SQL = """
SELECT n.nspname                                  AS source_schema,
       c.relname                                  AS source_table,
       c.oid::bigint                              AS relation_oid,
       c.relreplident                             AS replica_identity,
       (pr.prrelid IS NOT NULL)                   AS published
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_publication p ON p.pubname = %s
LEFT JOIN pg_publication_rel pr ON pr.prrelid = c.oid AND pr.prpubid = p.oid
WHERE c.relkind IN ('r', 'p') AND n.nspname = %s
"""

#: Guard 3's query (`catalog_apply`): the current oid of specific relations, read
#: immediately before anything is destroyed.
_OID_SQL = """
SELECT n.nspname, c.relname, c.oid::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND (n.nspname, c.relname) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
"""

_LSN_SQL = "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"


@dataclass(frozen=True)
class SourceRelation:
    schema: str
    table: str
    oid: int
    published: bool
    replica_identity: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class CatalogChange:
    """One DDL fact about one table, plus the LSN that fences it."""

    kind: str
    schema: str
    table: str
    detected_lsn: int
    detected_at: float = field(default_factory=time.monotonic)
    old_oid: int | None = None
    new_oid: int | None = None
    #: True once a WAL marker has been emitted past `detected_lsn`, so the fence is
    #: guaranteed to open. False means the source could not be written to.
    #: Informational: the *behavioural* fence is `durable_lsn >= detected_lsn` in
    #: `due()`, and the marker only guarantees that comparison will eventually be
    #: satisfiable (Opus MINOR-2 - the old docstring claimed `fenced` gated the
    #: action, which it never did).
    fenced: bool = False
    #: how many times the applier has looked at this change and declined
    deferrals: int = 0
    #: consecutive polls that agreed with this observation before it was queued
    confirmations: int = 1
    #: rubric 1.9 (SM-D). Where this change is in the observe -> confirm -> fence ->
    #: apply pipeline, as ONE named value. It used to be spread over four containers and
    #: three counters (`_unconfirmed`, `_pending`, `refused`, `awaiting_snapshot`,
    #: `fenced`, `deferrals`, `confirmations`), which is how `fenced` came to be
    #: documented as gating an action it never gated (Opus MINOR-2). Memory only: a
    #: lost pending change is re-detected on the next poll, which is correct, so
    #: persisting it would buy nothing and would need a new durable domain.
    state: str = CHANGE_OBSERVED

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"

    def to(self, state: str) -> None:
        """Move through `machines.CATALOG_CHANGE`, asserting the edge. Idempotent.

        Loud on an undeclared edge, like every other machine here: this one carries no
        durable consequence, but a change that reached `applied` from somewhere nobody
        declared is a destructive DDL nobody reasoned about.
        """
        if state == self.state:
            return
        CATALOG_CHANGE.check(self.state, state)
        self.state = state

    def context(self) -> dict:
        return {
            "kind": self.kind,
            "table": self.qualified,
            "detected_lsn": self.detected_lsn,
            "old_oid": self.old_oid,
            "new_oid": self.new_oid,
            "fenced": self.fenced,
            "state": self.state,
            "confirmations": self.confirmations,
        }


def _queued(change: CatalogChange) -> CatalogChange:
    """Membership of `_pending` IS the `pending` state (rubric 1.9, SM-D).

    `_compare()` sets it when it extends the list, but a change can also be put there
    directly - the 1.5 suite constructs one and queues it so a destructive action can be
    tested without a live source DDL - and "it is in the pending list but its state says
    `observed`" would be a distinction with no meaning. Normalising here keeps the
    declared edges honest instead of adding `observed -> everything`.

    Only from `observed`: a change that is already `due` or `deferred` is still in the
    list and must not be walked backwards to `pending`, which would make "how far did
    this change get" a function of how many commit groups have looked at it.
    """
    if change.state == CHANGE_OBSERVED:
        change.to(CHANGE_PENDING)
    return change


def read_known_relations(con, pipeline: str) -> dict[str, SourceRelation]:
    """What this pipeline last observed about the source, from the destination.

    This is what makes drop detection survive a restart: without the persisted
    `oid` a table that was dropped (or dropped and recreated with a different
    shape) while the pipeline was down is indistinguishable from one that never
    changed.
    """
    rows = con.execute(
        f"SELECT source_schema, source_table, relation_oid, published, replica_identity "
        f"FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?",
        [pipeline],
    ).fetchall()
    known: dict[str, SourceRelation] = {}
    for schema, table, oid, published, identity in rows:
        known[f"{schema}.{table}"] = SourceRelation(
            schema=schema,
            table=table,
            oid=int(oid or 0),
            published=bool(published),
            replica_identity=identity or "d",
        )
    return known


def seed_from_table_state(con, pipeline: str) -> set[str]:
    """Tables this pipeline owns a destination table for (`table_state` rows).

    `table_state` is the canonical source-to-destination registry, written inside
    the same transaction that first materialises a table whether that happens through
    a snapshot or through streaming DML alone (Codex 5). Before that it was written
    only by the snapshot coordinator, so a table that only ever existed through
    streaming had no durable row - and a `DROP TABLE` while the pipeline was down was
    then invisible for ever, because `_compare` has nothing to compare against and no
    reason to believe the name is ours.
    """
    rows = con.execute(
        f"SELECT source_schema, source_table FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ?",
        [pipeline],
    ).fetchall()
    return {f"{schema}.{table}" for schema, table in rows}


class CatalogWatcher:
    """Polls the source catalog on its own connection. Owns no destination state."""

    def __init__(
        self,
        *,
        dsn: str,
        publication: str,
        schema: str,
        include: set[str],
        known: dict[str, SourceRelation] | None = None,
        replicated: set[str] | None = None,
        poll_seconds: float = 10.0,
        connect_timeout: int = 5,
        emit_marker: bool = True,
        marker_prefix: str = MARKER_PREFIX,
        grace_seconds: float = 0.0,
        confirm_polls: int = 2,
        marker_max_writes: int | None = 60,
    ):
        self.dsn = dsn
        self.publication = publication
        self.schema = schema
        #: qualified names the configuration says we replicate (`table.include.list`)
        self.include = set(include)
        #: qualified names we have a destination table for
        self.replicated = set(replicated or ())
        self.known: dict[str, SourceRelation] = dict(known or {})
        self.poll_seconds = poll_seconds
        self.connect_timeout = connect_timeout
        self.grace_seconds = grace_seconds
        #: How many consecutive polls must agree before a DESTRUCTIVE change is
        #: queued at all (Opus Q5). 1 restores the old behaviour.
        self.confirm_polls = max(1, int(confirm_polls))
        self.marker = marker_mod.SourceMarker(
            prefix=marker_prefix, enabled=emit_marker, max_writes=marker_max_writes
        )

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pending: list[CatalogChange] = []
        #: relations whose `source_relations` row needs (re)writing
        self._dirty: dict[str, SourceRelation] = {}
        #: `name -> ((kind, new_oid), consecutive polls that agreed)`
        self._unconfirmed: dict[str, tuple[tuple[str, int | None], int]] = {}
        self.polls = 0
        self.empty_polls = 0
        self.superseded = 0
        self.last_error: str | None = None
        self.last_lsn: int = 0

    @property
    def emit_marker(self) -> bool:
        return self.marker.enabled

    @property
    def markers_emitted(self) -> int:
        return self.marker.writes

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> CatalogWatcher:
        if self.poll_seconds <= 0:
            log.info("catalog polling disabled (poll_seconds=%s)", self.poll_seconds)
            return self
        self._thread = threading.Thread(target=self._loop, name="cdc-catalog", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.poll_seconds))

    def _loop(self) -> None:
        # Poll once immediately: a table dropped while this pipeline was down must be
        # noticed on the run that follows, not `poll_seconds` into it.
        while True:
            self.poll_quietly()
            if self._stop.wait(self.poll_seconds):
                return

    def poll_quietly(self) -> list[CatalogChange]:
        """One poll that never raises. Fail soft, like `SourceHealth`."""
        try:
            return self.poll()
        except Exception as exc:  # pragma: no cover - exercised through the thread
            self.last_error = f"{type(exc).__name__}: {exc}"
            log.warning("catalog poll failed: %s", self.last_error)
            return []

    # -- polling ------------------------------------------------------------ #
    def poll(self) -> list[CatalogChange]:
        """One observation. Returns the changes it added to the pending list."""
        import psycopg

        with psycopg.connect(
            self.dsn, autocommit=True, connect_timeout=self.connect_timeout
        ) as conn:
            rows = conn.execute(_CATALOG_SQL, (self.publication, self.schema)).fetchall()
            lsn = int(conn.execute(_LSN_SQL).fetchone()[0])
            observed = {
                f"{r[0]}.{r[1]}": SourceRelation(
                    schema=r[0], table=r[1], oid=int(r[2]),
                    replica_identity=str(r[3]), published=bool(r[4]),
                )
                for r in rows
            }
            if not observed:
                # Opus Q2's absolute guard. An empty schema is the signature of a DSN
                # pointed at the wrong database, a failover target that has not been
                # migrated, or a source mid-`pg_restore`. It can never legitimately
                # mean "every table was dropped", so the observation is discarded
                # rather than acted on.
                with self._lock:
                    self.polls += 1
                    self.empty_polls += 1
                    self.last_lsn = lsn
                self.last_error = (
                    f"the polled schema {self.schema!r} contains no tables at all; "
                    "this observation was DISCARDED rather than read as a mass drop"
                )
                log.error("catalog poll: %s", self.last_error)
                return []
            added = self._compare(observed, lsn)
            # Emitted while a **destructive** change is pending, not only when one is
            # new: one tiny WAL record per poll interval, which makes the fence
            # self-healing (a marker that was written but not delivered is simply
            # followed by another one), bounded by the marker's own write budget.
            # Nothing is written to the source when there is nothing to fence.
            unfenced = [c for c in self.pending() if c.kind in DESTRUCTIVE]
            if unfenced:
                self._emit_marker(conn, [c for c in added if c.kind in DESTRUCTIVE] or unfenced)
        if self.marker.last_error is None:
            self.last_error = None
        return added

    def relation_oids(self, names: set[tuple[str, str]]) -> dict[str, int | None]:
        """Current oid of each `(schema, table)`, `None` when it does not exist.

        Guard 3 of `catalog_apply`: read on this watcher's own connection immediately
        before anything is destroyed. Raises on a source error, because "I could not
        ask" must never be read as "it is gone".
        """
        import psycopg

        if not self.dsn:
            # An empty DSN makes libpq connect to ITS defaults, which is a different
            # cluster on a different port. Refusing is what makes "fail closed" true.
            raise ValueError("this watcher has no DSN, so the source cannot be re-read")
        schemas = [s for s, _ in names]
        tables = [t for _, t in names]
        with psycopg.connect(
            self.dsn, autocommit=True, connect_timeout=self.connect_timeout
        ) as conn:
            rows = conn.execute(_OID_SQL, (schemas, tables)).fetchall()
        found = {f"{schema}.{table}": int(oid) for schema, table, oid in rows}
        return {f"{s}.{t}": found.get(f"{s}.{t}") for s, t in names}

    def _compare(self, observed: dict[str, SourceRelation], lsn: int) -> list[CatalogChange]:
        added: list[CatalogChange] = []
        superseded: list[str] = []
        with self._lock:
            self.polls += 1
            self.last_lsn = lsn
            # Pending destructive changes are `interesting` even after their relation
            # was forgotten: the *cancellation* in guard 2 depends on this poll
            # visiting the name at all (Codex 4).
            interesting = (
                self.include
                | self.replicated
                | set(self.known)
                | {c.qualified for c in self._pending if c.kind in DESTRUCTIVE}
            )
            for name in sorted(interesting):
                if not name.startswith(f"{self.schema}."):
                    # Opus MINOR-4: `_CATALOG_SQL` polls ONE schema, while
                    # `observe_replicated` accepts any `schema.table` the stream
                    # carries. A name outside the polled schema is simply unobserved,
                    # and reading that as `dropped` would destroy its destination table
                    # the moment multi-schema capture lands (2.3 / 3.x).
                    continue
                current = observed.get(name)
                previous = self.known.get(name)
                if current is not None:
                    # Guard 2: the relation is there, so a pending `dropped` for it
                    # describes a world that no longer exists. A pending `recreated`
                    # is NOT superseded by the relation being present - that is what a
                    # recreate looks like.
                    superseded.extend(self._supersede(name, CHANGE_DROPPED))
                    if previous is not None and current.oid == previous.oid:
                        self._unconfirmed.pop(name, None)
                else:
                    # And symmetrically: a relation that has since gone away makes a
                    # pending `recreated` stale. Its own drop is confirmed below.
                    superseded.extend(self._supersede(name, CHANGE_RECREATED))
                # AFTER supersession: a change this poll has just cancelled must not
                # then suppress the change this poll should queue instead.
                queued = any(
                    c.qualified == name and c.kind in DESTRUCTIVE for c in self._pending
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
                    if name in self.replicated or name in self.include:
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
                    change = self._confirm(
                        name,
                        self._change(CHANGE_DROPPED, previous, lsn, old_oid=previous.oid),
                    )
                    if change is not None:
                        added.append(change)
                    continue
                if current.oid != previous.oid:
                    if queued:
                        continue
                    change = self._confirm(
                        name,
                        self._change(
                            CHANGE_RECREATED, current, lsn,
                            old_oid=previous.oid, new_oid=current.oid,
                        ),
                    )
                    if change is not None:
                        added.append(change)
                        self.known[name] = current
                        self._dirty[name] = current
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
            self._pending.extend(added)
        for change in added:
            log.warning(
                "source catalog change: %s %s (oid %s -> %s) detected at lsn %s after "
                "%s confirming poll(s)",
                change.kind, change.qualified, change.old_oid, change.new_oid,
                change.detected_lsn, change.confirmations,
            )
        for name in superseded:
            log.warning(
                "cancelling a pending destructive action for %s: the relation is "
                "present at the source again", name,
            )
        return added

    def _confirm(self, name: str, change: CatalogChange) -> CatalogChange | None:
        """Queue a destructive observation only once enough polls have agreed.

        `CDC_DROP_CONFIRM_POLLS` (default 2). This costs at most one poll interval of
        extra latency on a real drop and removes a whole class of transient-catalog and
        mid-DDL false positive at essentially no cost (Opus Q5). Returns the change once
        the streak is complete, `None` while it is still building; the streak resets
        whenever the *shape* of the observation changes. Held under `self._lock`.
        """
        shape = (change.kind, change.new_oid)
        seen = self._unconfirmed.get(name)
        if seen is not None and seen[0] != shape:
            seen = None
        count = (seen[1] if seen else 0) + 1
        if count < self.confirm_polls:
            change.to(CHANGE_UNCONFIRMED)
            self._unconfirmed[name] = (shape, count)
            log.info(
                "%s observed for %s (%s/%s confirming polls); not queued yet",
                change.kind, name, count, self.confirm_polls,
            )
            return None
        self._unconfirmed.pop(name, None)
        change.confirmations = count
        return change

    def _supersede(self, name: str, *kinds: str) -> list[str]:
        """Cancel pending changes of `kinds` for `name`. Caller holds the lock."""
        keep = [
            c for c in self._pending if not (c.qualified == name and c.kind in kinds)
        ]
        if len(keep) == len(self._pending):
            return []
        kept = set(map(id, keep))
        for change in self._pending:
            if id(change) not in kept:
                change.to(CHANGE_SUPERSEDED)
        self.superseded += len(self._pending) - len(keep)
        self._pending = keep
        return [name]

    def _change(self, kind, relation: SourceRelation, lsn: int, **oids) -> CatalogChange:
        return CatalogChange(
            kind=kind, schema=relation.schema, table=relation.table, detected_lsn=lsn, **oids
        )

    def _emit_marker(self, conn, changes: list[CatalogChange]) -> None:
        """Write a WAL record past the detected change, so the fence can open."""
        payload = {"changes": [c.kind + ":" + c.qualified for c in changes]}
        if not self.marker.emit(conn, marker_mod.CATALOG_FENCE, payload):
            self.last_error = self.marker.last_error or (
                "the catalog fence marker could not be written to the source"
            )
            return
        with self._lock:
            for change in self._pending:
                change.fenced = True
                _queued(change).to(CHANGE_MARKED)

    # -- what the applier asks ---------------------------------------------- #
    def due(self, durable_lsn: int) -> list[CatalogChange]:
        """Pending changes whose fence has opened, in detection order.

        The fence is `durable_lsn >= detected_lsn`: everything that happened before
        the DDL is already committed at the destination, so applying the DDL now
        cannot delete rows that a later event would have re-created.
        """
        out: list[CatalogChange] = []
        with self._lock:
            for change in self._pending:
                _queued(change)
                if change.kind not in DESTRUCTIVE:
                    change.to(CHANGE_DUE)
                    # Nothing is removed for a `new`, `unpublished` or `republished`
                    # change - it is a marker row and an operator decision - so there is
                    # nothing for the fence to protect. Fencing them anyway kept them
                    # pending on an idle stream, which in turn kept the watcher writing
                    # marker records to the source for no reason.
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
        with self._lock:
            done = set(map(id, changes))
            self._pending = [c for c in self._pending if id(c) not in done]

    def pending(self) -> list[CatalogChange]:
        with self._lock:
            return list(self._pending)

    def pending_destructive(self) -> list[CatalogChange]:
        with self._lock:
            return [c for c in self._pending if c.kind in DESTRUCTIVE]

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

    def forget(self, name: str) -> None:
        with self._lock:
            self.known.pop(name, None)
            self._dirty.pop(name, None)
            self._unconfirmed.pop(name, None)
            self.replicated.discard(name)

    def observe_replicated(self, names: set[str]) -> None:
        """Tell the watcher which tables now have destination tables."""
        with self._lock:
            self.replicated |= names

    def summary(self) -> dict:
        with self._lock:
            pending = list(self._pending)
        return {
            "catalog_polls": self.polls,
            "catalog_empty_polls": self.empty_polls,
            "catalog_markers": self.marker.writes,
            "catalog_pending": len(pending),
            "catalog_pending_destructive": sum(
                1 for c in pending if c.kind in DESTRUCTIVE
            ),
            "catalog_superseded": self.superseded,
            "catalog_error": self.last_error,
            # Preserved rather than cleared by the next successful poll: a marker
            # failure is exactly the state in which a destructive change cannot be
            # applied, and the run must not look healthy while it persists (Codex 6).
            "catalog_marker_error": self.marker.last_error,
            "catalog_marker_capable": self.marker.capable,
        }
