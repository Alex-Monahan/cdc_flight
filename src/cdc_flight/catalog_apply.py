"""Turning "the source table is gone" into destination DDL, safely (rubric 1.5).

`cdc_flight.catalog` *observes*; this module *decides*, and the separation is the
answer to the sharpest finding of the 1.5 review: the watcher's observation and the
destructive action are separated in time by the LSN fence, and the code used to
apply the action without ever asking whether the fact still held.

Four guards, in the order they run:

1. **the fence** — `durable_lsn >= detected_lsn`, so the destination has already
   committed every event that happened before the DDL and no in-flight event can
   re-create the table as a zombie;
2. **supersession** — a newer observation cancels an older pending action for the
   same relation, so a table that came back before its drop was applied is never
   dropped (`CatalogWatcher._supersede`, Codex 4);
3. **revalidation** — the relation is re-queried on the watcher's own connection
   immediately before the DDL, and a relation that exists is not dropped no matter
   how old the queued observation is. Fails **closed**: if the source cannot be
   asked, nothing is destroyed;
4. **the circuit breaker** — one poll may destroy at most `CDC_DROP_MAX_PER_POLL`
   relations (default 1). Every plural case is a schema migration or a
   misconfiguration, and both want a human (Opus MAJOR-3 / Q2). None of the set is
   applied when the limit is exceeded: applying the first N and stopping would be the
   worst of both.

A `recreated` relation is the one case where the source table *exists* and the
destination table is still wrong — it holds the rows of a different relation. It is
dropped, alerted on, and the table is marked `awaiting_snapshot` in `table_state`
so the incompleteness is loud rather than silent (Opus Q1); rubric 2.3/3.4 own the
automatic re-snapshot that turns that flag off.

Alerts are returned, never written here: they must survive a rollback, which means a
different connection and a moment after the transaction has settled (Codex 7).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from . import destination, naming
from .catalog import (
    CHANGE_DROPPED,
    CHANGE_NEW,
    CHANGE_RECREATED,
    CHANGE_SCHEMA,
    DESTRUCTIVE,
    FENCED,
    CatalogChange,
)
from .config import DROP_IGNORE, DROP_REPLICATE
from .errors import SchemaEvolutionRefused
from .machines import CHANGE_APPLIED, CHANGE_REFUSED
from .schema_evolution import apply_column_changes

log = logging.getLogger("cdc_flight.catalog_apply")

#: `table_state.snapshot_state` for a relation whose destination table was removed
#: because the source relation was replaced. The rows are gone and CDC alone cannot
#: rebuild them, so anything that reads this table must know it is incomplete.
#: Canonical definition now lives in `destination`, which rubric 1.6's re-snapshot and
#: rubric 1.8's recovery also write; re-exported so the name still reads locally.
AWAITING_SNAPSHOT = destination.AWAITING_SNAPSHOT


class _Sentinel:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return self.name


#: the source could not be re-read, which is NOT evidence that anything is gone
UNKNOWN = _Sentinel("UNKNOWN")
#: revalidation is switched off (`CDC_DROP_REVALIDATE=0`)
SKIPPED = _Sentinel("SKIPPED")


def _stale(change: CatalogChange, oid) -> str | None:
    """Why this destructive change must not be applied, or None if it still holds."""
    if oid is SKIPPED:
        return None
    if oid is UNKNOWN:
        return "the source could not be re-read to confirm it"
    if change.kind == CHANGE_DROPPED:
        if oid is not None:
            return f"the relation exists at the source again (oid {oid})"
        return None
    # CHANGE_RECREATED: the source relation exists, and the destination table is only
    # wrong because it holds a DIFFERENT relation's rows. That is still true only if
    # the oid we observed is the oid that is there now.
    if oid is None:
        return "the relation has since been dropped; the drop will be detected on its own"
    if change.new_oid is not None and oid != change.new_oid:
        return (
            f"the relation was replaced again (oid {oid}, not the observed "
            f"{change.new_oid}); a newer observation supersedes this one"
        )
    return None


@dataclass(frozen=True)
class CatalogAction:
    """One decided change: what happens to which destination table, and why."""

    change: CatalogChange
    target: str
    destructive: bool
    detail: str | None = None


@dataclass(frozen=True)
class CatalogPlan:
    """An immutable transaction plan. Nothing here has touched the destination yet."""

    actions: tuple[CatalogAction, ...] = ()
    relations: tuple = ()
    #: destructive changes deliberately held back, with the reason
    refused: tuple[tuple[CatalogChange, str], ...] = ()
    alerts: list = field(default_factory=list)

    @property
    def destructive(self) -> tuple[CatalogAction, ...]:
        return tuple(a for a in self.actions if a.destructive)


class CatalogCoordinator:
    """Plans, applies and settles the source-catalog changes of one commit group."""

    def __init__(
        self,
        *,
        catalog,
        pipeline: str,
        topic_prefix: str,
        drop_mode: str,
        registry_of,
        max_destructive_per_group: int = 1,
        allow_mass_drop: bool = False,
        revalidate: bool = True,
    ):
        self.catalog = catalog
        self.pipeline = pipeline
        self.topic_prefix = topic_prefix
        self.drop_mode = drop_mode
        self._registry_of = registry_of
        self.max_destructive_per_group = max_destructive_per_group
        self.allow_mass_drop = allow_mass_drop
        self.revalidate = revalidate
        self.tables_dropped = 0
        self.changes_applied = 0
        self.destructive_refused = 0
        self.awaiting_snapshot: set[str] = set()

    @property
    def registry(self):
        return self._registry_of()

    @property
    def enabled(self) -> bool:
        return self.catalog is not None and self.drop_mode != DROP_IGNORE

    # ------------------------------------------------------------------ #
    # planning
    # ------------------------------------------------------------------ #
    def plan(self, durable_lsn: int) -> CatalogPlan:
        if not self.enabled:
            return CatalogPlan()
        due = self.catalog.due(durable_lsn)
        # NO EARLY RETURN WHEN NOTHING IS DUE. `source_relations` is the only thing that
        # makes a DROP or a drop-and-recreate detectable across a restart — without the
        # persisted `relation_oid` the next run has nothing to compare against — and it
        # was written only as a side effect of a plan that had at least one due change.
        # A pipeline whose catalog is simply quiet therefore never persisted what it had
        # learned, and the first run after `--reset-state` (which discards
        # `source_relations` deliberately, because the oids are about to be re-read) left
        # the destination permanently unable to notice the next recreate. MEASURED: the
        # rubric-1.5 recreated-relation E2E stopped detecting anything the moment
        # `--reset-state` began registering every captured table, because a registered
        # table produces no `new` change and nothing else was due.
        actions: list[CatalogAction] = []
        refused: list[tuple[CatalogChange, str]] = []
        alerts: list[dict] = []

        new_changes = [change for change in due if change.kind == CHANGE_NEW]
        if len(new_changes) > 1:
            names = sorted(change.qualified for change in new_changes)
            alerts.append(
                {
                    "severity": "warning",
                    "code": "mass_add_observed",
                    "on_rollback": False,
                    "message": (
                        f"automatically accepting {len(names)} newly discovered source "
                        "relations; adds are safe by default, but this may indicate a "
                        "schema migration or an overly broad discovery scope"
                    ),
                    "context": {"tables": names, "safe_default": True},
                }
            )
            log.warning(
                "mass catalog add: automatically accepting %s discovered relations "
                "(%s)",
                len(names),
                ", ".join(names),
            )

        destructive_changes = [
            c
            for c in due
            if c.kind in DESTRUCTIVE and self.drop_mode == DROP_REPLICATE
        ]
        limit = self.max_destructive_per_group
        blocked: set[int] = set()
        if (
            not self.allow_mass_drop
            and limit >= 0
            and len(destructive_changes) > limit
        ):
            # Guard 4. `DROP SCHEMA app CASCADE`, a DSN repointed at an empty
            # database, a failover target whose schema has not been created yet, or a
            # source mid-`pg_restore` all look like this, and none of them should be
            # able to amplify into whole-warehouse destruction with no human in the
            # loop (Opus MAJOR-3).
            names = sorted(c.qualified for c in destructive_changes)
            reason = (
                f"{len(names)} relations would be destroyed at once, above "
                f"CDC_DROP_MAX_PER_POLL={limit}"
            )
            for change in destructive_changes:
                blocked.add(id(change))
                change.to(CHANGE_REFUSED)  # rubric 1.9 (SM-D)
                refused.append((change, reason))
            alerts.append(
                {
                    "severity": "critical",
                    "code": "mass_drop_refused",
                    # Survives a rollback: "I refused to destroy your warehouse" is
                    # the one signal that must never depend on the apply succeeding.
                    "on_rollback": True,
                    "message": (
                        f"refusing to destroy {len(names)} destination tables in one "
                        f"commit group ({', '.join(names)}). {reason}. They stay pending; "
                        "set CDC_DROP_ALLOW_MASS=1 to authorise, or raise "
                        "CDC_DROP_MAX_PER_POLL."
                    ),
                    "context": {"tables": names, "limit": limit},
                }
            )
            log.error(
                "CIRCUIT BREAKER: %s destructive catalog actions in one group (%s); "
                "applying none of them",
                len(names), ", ".join(names),
            )

        oids = self._source_oids([c for c in destructive_changes if id(c) not in blocked])
        for change in due:
            destructive = (
                change.kind in DESTRUCTIVE and self.drop_mode == DROP_REPLICATE
            )
            if destructive and id(change) in blocked:
                continue
            target = naming.destination_table(
                self.topic_prefix, change.schema, change.table
            )
            if destructive:
                reason = _stale(change, oids.get(change.qualified, UNKNOWN))
                if reason is not None:
                    # Guard 3, fail-closed. The queued observation and the DDL are
                    # separated by the fence, and the fence can be arbitrarily wide on
                    # a quiet source; a fact that is no longer true must not destroy a
                    # live relation's destination table (Codex 4).
                    change.to(CHANGE_REFUSED)  # rubric 1.9 (SM-D)
                    refused.append((change, reason))
                    log.warning(
                        "not dropping %s: %s (the change stays pending)",
                        target, reason,
                    )
                    continue
            detail = None
            if change.kind in DESTRUCTIVE and not destructive:
                detail = f"drop_mode={self.drop_mode}"
            elif destructive and change.kind == CHANGE_RECREATED:
                detail = (
                    f"recreated with oid {change.new_oid} (was {change.old_oid}); the "
                    "destination table held the OLD relation's rows and was dropped, "
                    f"and `table_state.snapshot_state` is now {AWAITING_SNAPSHOT!r}: it "
                    "is INCOMPLETE until a re-snapshot runs (rubric 2.3/3.4)"
                )
            elif destructive and not change.fenced:
                detail = "applied without a WAL fence marker (CDC_CATALOG_GRACE)"
            elif change.kind == CHANGE_SCHEMA:
                detail = "; ".join(
                    (
                        f"{item.old_name} -> {item.new_name}"
                        if item.kind == "renamed"
                        else item.new_name or item.old_name or f"attnum {item.attnum}"
                    )
                    for item in change.column_changes
                )
            actions.append(
                CatalogAction(
                    change=change, target=target, destructive=destructive, detail=detail
                )
            )
            if change.kind in DESTRUCTIVE:
                alerts.append(
                    {
                        "severity": "warning" if destructive else "info",
                        "code": f"table_{change.kind}",
                        # Describes an action this transaction performed, so it must
                        # not outlive the rollback that undid it.
                        "on_rollback": False,
                        "message": (
                            f"{change.qualified} {change.kind} at the source; the "
                            + (
                                "destination table was dropped"
                                if destructive
                                else f"destination table was KEPT (drop_mode={self.drop_mode})"
                            )
                        ),
                        "context": change.context(),
                    }
                )
        for change, reason in refused:
            alerts.append(
                {
                    "severity": "warning",
                    "code": "destructive_change_deferred",
                    "on_rollback": True,
                    "message": (
                        f"{change.kind} for {change.qualified} was NOT applied: {reason}"
                    ),
                    "context": {**change.context(), "reason": reason},
                }
            )
        # Guard: persisted state must never run ahead of the action it implies. A
        # `source_relations` row carrying the new oid would make the *next* run agree
        # with the source and never notice the drop at all. Only a change that would
        # REMOVE the destination table has to block persistence - letting `new` or
        # `unpublished` block it was measured to leave a table with no row at all,
        # which is how a drop between two runs went undetected.
        applied = {id(a.change) for a in actions}
        remaining = {
            change.qualified
            for change in self.catalog.pending()
            if change.kind in FENCED and id(change) not in applied
        }
        relations = list(self.catalog.dirty(exclude=remaining))
        known_relations = {relation.qualified for relation in relations}
        # A schema action carries the post-DDL relation explicitly. Persisting it in
        # this same transaction is what makes a restart see the new column baseline
        # only after the destination RENAME/ADD/DROP has committed.
        for action in actions:
            relation = action.change.new_relation
            if relation is not None and relation.qualified not in known_relations:
                relations.append(relation)
                known_relations.add(relation.qualified)
        return CatalogPlan(
            actions=tuple(actions),
            relations=tuple(relations),
            refused=tuple(refused),
            alerts=alerts,
        )

    def _source_oids(self, changes: list[CatalogChange]) -> dict[str, object]:
        """The relations' oids *right now*, read on the watcher's own connection.

        `UNKNOWN` for every relation when the source cannot be asked, which
        `_stale()` turns into a refusal: "I could not ask" must never be read as
        "it is gone".
        """
        if not changes:
            return {}
        names = {(c.schema, c.table) for c in changes}
        if not self.revalidate:
            return dict.fromkeys((f"{s}.{t}" for s, t in names), SKIPPED)
        try:
            return dict(self.catalog.relation_oids(names))
        except Exception as exc:  # pragma: no cover - fail closed on any source error
            log.warning("could not revalidate %s before dropping: %s", sorted(names), exc)
            return dict.fromkeys((f"{s}.{t}" for s, t in names), UNKNOWN)

    # ------------------------------------------------------------------ #
    # applying, inside the commit group's transaction
    # ------------------------------------------------------------------ #
    def apply(self, con, plan: CatalogPlan, stats: dict) -> list[dict]:
        """Execute the plan's DDL and state writes. Returns `table_events` rows.

        Runs inside the commit group's transaction, *after* the group's events, so a
        `DROP` cannot remove rows that an event of this same group had still to add,
        and a crash between the drop and the resume-point write replays both.
        """
        markers: list[dict] = []
        for action in plan.actions:
            change = action.change
            if change.kind == CHANGE_SCHEMA:
                try:
                    apply_column_changes(
                        self.registry, action.target, change.column_changes
                    )
                except SchemaEvolutionRefused as refused:
                    refused.source_schema = refused.source_schema or change.schema
                    refused.source_table = refused.source_table or change.table
                    refused.target = refused.target or action.target
                    refused.detected_lsn = (
                        refused.detected_lsn or change.detected_lsn
                    )
                    raise
                stats["tables"].add(action.target)
            if action.destructive:
                # The shadow goes too: a table dropped mid-backfill would otherwise
                # leave `<target>__cdcf_tmp` behind forever.
                self.registry.drop(naming.shadow_table(action.target))
                self.registry.drop(action.target)
                if change.kind == CHANGE_RECREATED:
                    destination.mark_awaiting_snapshot(
                        con,
                        pipeline=self.pipeline,
                        source_schema=change.schema,
                        source_table=change.table,
                        target_table=action.target,
                        state=AWAITING_SNAPSHOT,
                    )
                    self.awaiting_snapshot.add(change.qualified)
                else:
                    destination.forget_table_state(
                        con,
                        pipeline=self.pipeline,
                        source_schema=change.schema,
                        source_table=change.table,
                    )
                stats["tables"].add(action.target)
                self.tables_dropped += 1
            if change.kind == CHANGE_DROPPED:
                destination.forget_source_relation(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                )
            if change.kind == CHANGE_SCHEMA and change.column_changes:
                event_details = [
                    (
                        f"column_{column_change.kind}",
                        (
                            f"{column_change.old_name} -> {column_change.new_name}"
                            if column_change.kind == "renamed"
                            else column_change.new_name
                            or column_change.old_name
                            or f"attnum {column_change.attnum}"
                        ),
                    )
                    for column_change in change.column_changes
                ]
            else:
                event_details = [(change.kind, action.detail)]
            for event, detail in event_details:
                markers.append(
                    {
                        "event": event,
                        "source_schema": change.schema,
                        "source_table": change.table,
                        "target_table": action.target,
                        "applied": action.destructive or change.kind == CHANGE_SCHEMA,
                        "lsn": change.detected_lsn,
                        "txn_id": None,
                        "detail": detail,
                        "rows_removed": None,
                    }
                )
            self.changes_applied += 1
        for relation in plan.relations:
            destination.upsert_source_relation(
                con,
                pipeline=self.pipeline,
                source_schema=relation.schema,
                source_table=relation.table,
                relation_oid=relation.oid,
                published=relation.published,
                admission_state=relation.admission_state or "external",
                replica_identity=relation.replica_identity,
                columns=relation.columns,
            )
        self.destructive_refused += len(plan.refused)
        return markers

    def backfill_schema(self, con, plan: CatalogPlan) -> None:
        """Backfill values for source columns added by this schema plan.

        PostgreSQL's ADD COLUMN default is visible immediately on existing source
        rows, but those rows have no pgoutput UPDATE.  The schema DDL is applied in the
        first phase of the commit; this second, still-transactional phase reads the
        fenced source rows and updates only the newly added destination columns.  A
        source read failure aborts the destination transaction so the schema action
        remains pending and cannot publish a shape/data mismatch.
        """
        if self.catalog is None or not hasattr(self.catalog, "read_columns"):
            return
        for action in plan.actions:
            change = action.change
            if change.kind != CHANGE_SCHEMA or change.new_relation is None:
                continue
            value_columns = tuple(
                item.destination_new_name
                for item in change.column_changes
                if item.kind == "added" and item.destination_new_name
            )
            if not value_columns:
                continue
            table = self.registry.get(action.target)
            if not table.exists:
                continue
            source_names = {
                naming.normalize(column.name) for column in change.new_relation.columns
            }
            stable_keys = tuple(
                key for key in table.key_columns if key in source_names
            )
            try:
                if len(stable_keys) == len(table.key_columns) and stable_keys:
                    rows = self.catalog.read_columns(
                        change.new_relation,
                        stable_keys,
                        value_columns,
                    )
                    if not rows:
                        defaults = self._missing_defaults(
                            change.new_relation, value_columns
                        )
                        if defaults is not None:
                            self.registry.backfill_constant_columns(
                                action.target,
                                value_columns=value_columns,
                                rows=[defaults],
                            )
                            continue
                    self.registry.backfill_columns(
                        action.target,
                        key_columns=stable_keys,
                        value_columns=value_columns,
                        rows=rows,
                    )
                    continue

                # Keyless CDC rows are intentionally keyed by the connector event id,
                # not by source row content.  There is no honest per-row join for an
                # ADD, but a uniform source value can be applied to the complete
                # changelog without fabricating identity.
                rows = self.catalog.read_columns(change.new_relation, (), value_columns)
                if not rows:
                    defaults = self._missing_defaults(change.new_relation, value_columns)
                    if defaults is not None:
                        self.registry.backfill_constant_columns(
                            action.target,
                            value_columns=value_columns,
                            rows=[defaults],
                        )
                        continue
                self.registry.backfill_constant_columns(
                    action.target,
                    value_columns=value_columns,
                    rows=rows,
                )
            except SchemaEvolutionRefused as refused:
                refused.source_schema = refused.source_schema or change.schema
                refused.source_table = refused.source_table or change.table
                refused.target = refused.target or action.target
                refused.detected_lsn = refused.detected_lsn or change.detected_lsn
                raise

    @staticmethod
    def _missing_defaults(relation, value_columns: tuple[str, ...]) -> tuple | None:
        by_name = {
            naming.normalize(column.name): column for column in relation.columns
        }
        values = []
        for name in value_columns:
            column = by_name.get(name)
            if column is None or not column.has_missing_default:
                return None
            values.append(column.missing_value)
        return tuple(values)

    # ------------------------------------------------------------------ #
    # after COMMIT
    # ------------------------------------------------------------------ #
    def settle(self, plan: CatalogPlan, source_tables: set[str]) -> None:
        """Forget the catalog work this group made durable. Runs after COMMIT.

        Resolved only *after* the commit: forgetting a change whose DDL then rolled
        back would leave the destination table in place with nothing left in this
        process to re-detect it.
        """
        if self.catalog is None:
            return
        changes = [action.change for action in plan.actions]
        if changes:
            # rubric 1.9 (SM-D): `due -> applied` is terminal, and it is recorded only
            # AFTER the COMMIT for the same reason `resolve()` is - a change marked
            # applied over a rolled-back DDL is a destructive action nothing will
            # re-detect.
            for change in changes:
                change.to(CHANGE_APPLIED)
            self.catalog.resolve(changes)
            for action in plan.actions:
                if action.change.kind == CHANGE_DROPPED and action.destructive:
                    # No destination table any more, so it is not a replicated table
                    # any more: if the name comes back it is a NEW table (2.3), not a
                    # continuation of this one.
                    self.catalog.forget(action.change.qualified)
        if plan.relations:
            self.catalog.clear_dirty([rel.qualified for rel in plan.relations])
        if source_tables:
            self.catalog.observe_replicated(source_tables)

    def summary(self) -> dict:
        return {
            "tables_dropped": self.tables_dropped,
            "catalog_changes_applied": self.changes_applied,
            "catalog_destructive_refused": self.destructive_refused,
            "tables_awaiting_snapshot": sorted(self.awaiting_snapshot),
        }
