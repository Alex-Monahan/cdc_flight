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
| the name is back with a different `oid` | `recreated` | drop the destination table; the new table is a new table (rubric 2.3 owns re-snapshotting it) |
| the name is there but no longer in the publication | `unpublished` | nothing but a marker + alert: Postgres still holds the rows, so dropping the destination would destroy data the source has |
| a table in the include list we have never seen | `new` | nothing but a marker; rubric 2.3 owns discovery, and this is the mechanism it will use |

**The fence.** A detected drop must not be applied before the destination has
consumed every event that happened *before* it. The drop is discovered after the
fact, so the poll records `pg_current_wal_lsn()` as `detected_lsn` and the applier
holds the action until its durable resume point reaches that LSN. On a quiet source
nothing would ever advance it, so the watcher writes a **logical-decoding message**
(`pg_logical_emit_message(false, …)`) on the source: a WAL record past the drop
whose delivery proves the drop point has been passed. That is ADR 0001 D9's source
heartbeat mechanism, one poll early, and it is why `messages` matters
(`PgOutputMessageDecoder.defaultOptions` sets the slot option on PG14+, so they
arrive without any configuration of ours).

If the marker cannot be written - a read-only replica, no permission - the action
is **not** applied and an alert says so, because forcing it on a timer would drop a
table whose in-flight events then re-create it as a zombie. `CDC_CATALOG_GRACE`
exists for operators who prefer that trade explicitly.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field

from .destination import CONTROL_SCHEMA

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

MARKER_PREFIX = "cdcf_catalog"

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
    fenced: bool = False
    #: how many times the applier has looked at this change and declined
    deferrals: int = 0

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"

    def context(self) -> dict:
        return {
            "kind": self.kind,
            "table": self.qualified,
            "detected_lsn": self.detected_lsn,
            "old_oid": self.old_oid,
            "new_oid": self.new_oid,
            "fenced": self.fenced,
        }


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
    """Tables this pipeline has actually replicated (`table_state` rows).

    A table with a destination table but no `source_relations` row predates this
    mechanism; its `oid` is recorded on the first poll and only *later* changes are
    reported, which is the honest behaviour - we cannot know an oid we never saw.
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
        self.emit_marker = emit_marker
        self.marker_prefix = marker_prefix
        self.grace_seconds = grace_seconds

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pending: list[CatalogChange] = []
        #: relations whose `source_relations` row needs (re)writing
        self._dirty: dict[str, SourceRelation] = {}
        self.polls = 0
        self.markers_emitted = 0
        self.last_error: str | None = None
        self.last_lsn: int = 0

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
            try:
                self.poll()
            except Exception as exc:  # pragma: no cover - fail soft, like SourceHealth
                self.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("catalog poll failed: %s", self.last_error)
            if self._stop.wait(self.poll_seconds):
                return

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
            added = self._compare(observed, lsn)
            # Emitted while a **destructive** change is pending, not only when one is
            # new: one tiny WAL record per poll interval, which makes the fence
            # self-healing (a marker that was written but not delivered - see
            # `_emit_marker` - is simply followed by another one). Nothing is written to
            # the source when there is nothing to fence.
            unfenced = [c for c in self.pending() if c.kind in DESTRUCTIVE]
            if self.emit_marker and unfenced:
                self._emit_marker(conn, [c for c in added if c.kind in DESTRUCTIVE] or unfenced)
        self.last_error = None
        return added

    def _compare(self, observed: dict[str, SourceRelation], lsn: int) -> list[CatalogChange]:
        added: list[CatalogChange] = []
        with self._lock:
            self.polls += 1
            self.last_lsn = lsn
            interesting = self.include | self.replicated | set(self.known)
            for name in sorted(interesting):
                current = observed.get(name)
                previous = self.known.get(name)
                if previous is None:
                    if current is None:
                        if name not in self.replicated:
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
                        added.append(
                            CatalogChange(
                                kind=CHANGE_DROPPED, schema=schema, table=table,
                                detected_lsn=lsn,
                            )
                        )
                        self.replicated.discard(name)
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
                    added.append(
                        self._change(CHANGE_DROPPED, previous, lsn, old_oid=previous.oid)
                    )
                    self.known.pop(name, None)
                    # BOTH, or the next poll reports the same drop again through the
                    # no-persisted-oid path above while the first one is still waiting
                    # for its fence (MEASURED: two `dropped` markers for one DROP).
                    self.replicated.discard(name)
                    continue
                if current.oid != previous.oid:
                    added.append(
                        self._change(
                            CHANGE_RECREATED, current, lsn,
                            old_oid=previous.oid, new_oid=current.oid,
                        )
                    )
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
            self._pending.extend(added)
        for change in added:
            log.warning(
                "source catalog change: %s %s (oid %s -> %s) detected at lsn %s",
                change.kind, change.qualified, change.old_oid, change.new_oid,
                change.detected_lsn,
            )
        return added

    def _change(self, kind, relation: SourceRelation, lsn: int, **oids) -> CatalogChange:
        return CatalogChange(
            kind=kind, schema=relation.schema, table=relation.table, detected_lsn=lsn, **oids
        )

    def _emit_marker(self, conn, changes: list[CatalogChange]) -> None:
        """Write a WAL record past the detected change, so the fence can open.

        **Transactional** (`pg_logical_emit_message(true, …)`), and that is a measured
        decision rather than a preference. A non-transactional message is the obvious
        choice - it carries no transaction id, so `TransactionMonitor.dataEvent`
        returns early and it stays out of every `END.event_count` - but it does not
        end Debezium's WAL-position search after a restart:
        `WalPositionLocator.resumeFromLsn` only stops searching on a **COMMIT** whose
        LSN is past the stored one (`case MESSAGE:` logs and falls through), and while
        it is searching `skipMessage()` drops every record. MEASURED, 2026-07-31: a
        quiet run whose only new WAL was a non-transactional marker delivered
        `records=0` with the slot 770 KB behind and never applied the drop, while the
        same code with a committed transaction after the marker applied it in ~1 s.

        A transactional message arrives as BEGIN + `op="m"` + END, which is exactly the
        shape ADR D9's source heartbeat specifies and the assembler already proves
        whole (its `data_collections` pseudo-entry is covered by `message_count`,
        Opus M-5).
        """
        payload = json.dumps(
            {"changes": [c.kind + ":" + c.qualified for c in changes]}, separators=(",", ":")
        )
        try:
            conn.execute(
                "SELECT pg_logical_emit_message(true, %s, %s)", (self.marker_prefix, payload)
            )
        except Exception as exc:
            log.error(
                "could not emit the catalog fence marker (%s). The detected change "
                "cannot be proven to be behind the stream, so it will NOT be applied "
                "until an event past lsn %s arrives; see rubric 1.5 / ADR D9.",
                exc, changes[0].detected_lsn if changes else 0,
            )
            return
        self.markers_emitted += 1
        with self._lock:
            for change in self._pending:
                change.fenced = True

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
                if change.kind not in DESTRUCTIVE:
                    # Nothing is removed for a `new`, `unpublished` or `republished`
                    # change - it is a marker row and an operator decision - so there is
                    # nothing for the fence to protect. Fencing them anyway kept them
                    # pending on an idle stream, which in turn kept the watcher writing
                    # marker records to the source for no reason.
                    out.append(change)
                    continue
                if durable_lsn >= change.detected_lsn:
                    out.append(change)
                    continue
                change.deferrals += 1
                if self.grace_seconds and (
                    time.monotonic() - change.detected_at >= self.grace_seconds
                ):
                    log.warning(
                        "applying %s for %s after %.0fs of grace even though the "
                        "destination is only at lsn %s (< %s): in-flight events for "
                        "that table could re-create it (CDC_CATALOG_GRACE)",
                        change.kind, change.qualified, self.grace_seconds,
                        durable_lsn, change.detected_lsn,
                    )
                    out.append(change)
        return out

    def resolve(self, changes: list[CatalogChange]) -> None:
        with self._lock:
            done = set(map(id, changes))
            self._pending = [c for c in self._pending if id(c) not in done]

    def pending(self) -> list[CatalogChange]:
        with self._lock:
            return list(self._pending)

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
            self.replicated.discard(name)

    def observe_replicated(self, names: set[str]) -> None:
        """Tell the watcher which tables now have destination tables."""
        with self._lock:
            self.replicated |= names

    def summary(self) -> dict:
        with self._lock:
            return {
                "catalog_polls": self.polls,
                "catalog_markers": self.markers_emitted,
                "catalog_pending": len(self._pending),
                "catalog_error": self.last_error,
            }
