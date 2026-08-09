"""Apply durable asynchronous catalog observations inside one MD transaction.

`cdc_flight.catalog` observes source state and queues a WAL-fenced change.  This
module plans that durable observation; it never re-reads the source and never holds
a source lock across a destination commit.  A recreate is intentionally a table
trust boundary: the target is quarantined and marked ``awaiting_snapshot`` in the
same transaction as the group's DML, resume point, and commit log.  The existing
table-scoped resnapshot later swaps a complete image atomically.

The watcher may be late, so the destination can be stale before the observation is
durable.  Baseline/completion refuse an untrusted or owing table; there is no
commit-time proof which could turn a source error into an acknowledged, lost unit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from . import destination, naming, table_lifecycle
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
from .machines import CHANGE_DEFERRED, CHANGE_REFUSED, require_admission_state
from .schema_evolution import apply_column_changes

log = logging.getLogger("cdc_flight.catalog_apply")

# Canonical definition lives in destination; re-export the name for local callers.
AWAITING_SNAPSHOT = destination.AWAITING_SNAPSHOT


@dataclass(frozen=True)
class CatalogAction:
    """One decided change: what happens to which destination table, and why."""

    change: CatalogChange
    target: str
    destructive: bool
    detail: str | None = None


@dataclass(frozen=True)
class CatalogPlan:
    """An immutable destination transaction plan."""

    actions: tuple[CatalogAction, ...] = ()
    relations: tuple = ()
    #: Watcher observation epoch at plan time; settlement uses it for diagnostics and
    #: identity-aware dirty clearing when a newer poll overlaps the commit.
    catalog_epoch: int = 0
    #: Destructive changes deliberately held back by the circuit breaker.
    refused: tuple[tuple[CatalogChange, str], ...] = ()
    alerts: list = field(default_factory=list)

    @property
    def destructive(self) -> tuple[CatalogAction, ...]:
        return tuple(action for action in self.actions if action.destructive)


class CatalogCoordinator:
    """Plan, apply, and settle durable watcher state."""

    def __init__(
        self,
        *,
        catalog,
        pipeline: str,
        topic_prefix: str,
        drop_mode: str,
        registry_of,
        lifecycle_con=None,
        control_schema: str | None = None,
        max_destructive_per_group: int = 1,
        allow_mass_drop: bool = False,
    ):
        self.catalog = catalog
        self.pipeline = pipeline
        self.topic_prefix = topic_prefix
        self.drop_mode = drop_mode
        self._registry_of = registry_of
        self._lifecycle_con = lifecycle_con
        self.control_schema = control_schema
        self.max_destructive_per_group = max_destructive_per_group
        self.allow_mass_drop = allow_mass_drop
        self.tables_dropped = 0
        self.changes_applied = 0
        self.destructive_refused = 0
        self.awaiting_snapshot: set[str] = set()
        self.tables_quarantined = 0

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
        """Turn due watcher state into a destination-only transaction plan.

        The catalog observation and the WAL fence are durable inputs.  A later poll
        supersedes stale work before this method is called; once due, the observation
        is applied atomically and the following poll can repair any newly learned
        generation.  There is deliberately no source proof, lock, or final read here.
        """
        if not self.enabled:
            return CatalogPlan()

        due = self.catalog.due(durable_lsn)
        if self._lifecycle_con is not None:
            owing = set(
                table_lifecycle.owing_work(
                    self._lifecycle_con,
                    self.pipeline,
                    control_schema=self.control_schema,
                )
            )
            eligible: list[CatalogChange] = []
            for change in due:
                if change.kind == CHANGE_DROPPED and change.qualified in owing:
                    # A source-side drop observed while the retained image is still
                    # awaiting its replacement snapshot is not a final drop policy
                    # decision. Keep the catalog fact live, but do not let a stale
                    # CHANGE_DROPPED plan destroy or log the retained image before
                    # the re-snapshot's final source evidence decides.
                    change.to(CHANGE_DEFERRED)
                    log.warning(
                        "deferring %s for %s while the replacement image is owed; "
                        "the re-snapshot owns the final source-missing policy",
                        change.kind,
                        change.qualified,
                    )
                else:
                    eligible.append(change)
            due = eligible
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
                "mass catalog add: automatically accepting %s discovered relations (%s)",
                len(names),
                ", ".join(names),
            )

        # The breaker protects physical DROP_REPLICATE destruction.  A recreate is a
        # table-scoped quarantine boundary, but its retained image is deliberately kept
        # until the replacement snapshot's atomic swap (or the final source-missing
        # policy decision) proves that destruction is safe.
        destructive_changes = [
            change
            for change in due
            if change.kind == CHANGE_DROPPED and self.drop_mode == DROP_REPLICATE
        ]
        blocked: set[int] = set()
        limit = self.max_destructive_per_group
        if (
            not self.allow_mass_drop
            and limit >= 0
            and len(destructive_changes) > limit
        ):
            names = sorted(change.qualified for change in destructive_changes)
            reason = (
                f"{len(names)} relations would be destroyed at once, above "
                f"CDC_DROP_MAX_PER_POLL={limit}"
            )
            for change in destructive_changes:
                blocked.add(id(change))
                change.to(CHANGE_REFUSED)
                refused.append((change, reason))
            alerts.append(
                {
                    "severity": "critical",
                    "code": "mass_drop_refused",
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
                len(names),
                ", ".join(names),
            )

        for change in due:
            if id(change) in blocked:
                continue
            destructive = (
                change.kind == CHANGE_DROPPED and self.drop_mode == DROP_REPLICATE
            )
            target = naming.destination_table(
                self.topic_prefix, change.schema, change.table
            )
            if change.kind == CHANGE_RECREATED:
                detail = (
                    f"recreated with oid {change.new_oid} (was {change.old_oid}); the "
                    "destination table held the old relation's rows and was "
                    "quarantined while its retained image was preserved; "
                    "`table_state.snapshot_state` is now "
                    f"{AWAITING_SNAPSHOT!r} until a complete re-snapshot runs"
                )
            elif change.kind in DESTRUCTIVE and not destructive:
                detail = f"drop_mode={self.drop_mode}; retained image is historical log state"
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
            else:
                detail = None
            actions.append(
                CatalogAction(
                    change=change,
                    target=target,
                    destructive=destructive,
                    detail=detail,
                )
            )
            if change.kind in DESTRUCTIVE:
                alerts.append(
                    {
                        "severity": "warning" if destructive else "info",
                        "code": f"table_{change.kind}",
                        "on_rollback": False,
                        "message": (
                            f"{change.qualified} {change.kind} at the source; the "
                            + (
                                "destination table was dropped"
                                if destructive
                                else "retained image was quarantined for re-snapshot"
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

        # State must never run ahead of the action it implies.  A log-only drop keeps
        # its old relation identity so baseline can explain the retained image; a
        # replicate drop forgets it in apply().
        applied = {id(action.change) for action in actions}
        remaining = {
            change.qualified
            for change in self.catalog.pending()
            if change.kind in FENCED and id(change) not in applied
        }
        dropped = {
            action.change.qualified
            for action in actions
            if action.change.kind == CHANGE_DROPPED and action.destructive
        }
        relations = list(self.catalog.dirty(exclude=remaining | dropped))
        known_relations = {relation.qualified for relation in relations}
        for action in actions:
            relation = action.change.new_relation
            if (
                relation is not None
                and not (
                    action.change.kind == CHANGE_DROPPED and action.destructive
                )
                and relation.qualified not in known_relations
            ):
                relations.append(relation)
                known_relations.add(relation.qualified)

        return CatalogPlan(
            actions=tuple(actions),
            relations=tuple(relations),
            catalog_epoch=self.catalog.epoch,
            refused=tuple(refused),
            alerts=alerts,
        )

    # ------------------------------------------------------------------ #
    # applying, inside the commit group's transaction
    # ------------------------------------------------------------------ #
    def apply(self, con, plan: CatalogPlan, stats: dict) -> list[dict]:
        """Execute DDL and state writes after group DML, before MD COMMIT."""
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
                    refused.detected_lsn = refused.detected_lsn or change.detected_lsn
                    raise
                stats["tables"].add(action.target)
            if action.destructive and change.kind != CHANGE_RECREATED:
                self.registry.drop(naming.shadow_table(action.target))
                self.registry.drop(action.target)
                destination.forget_table_state(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                    control_schema=self.control_schema,
                )
                stats["tables"].add(action.target)
                self.tables_dropped += 1
            if change.kind == CHANGE_RECREATED:
                # This is the convergence boundary.  Rows admitted before the watcher
                # learned the new generation remain as a retained recovery image until
                # the complete replacement is durable.  SnapshotCoordinator.swap()
                # owns the only normal destruction point, inside its atomic swap.
                stats["tables"].add(action.target)
                self.tables_quarantined += 1
                destination.mark_awaiting_snapshot(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                    target_table=action.target,
                    state=AWAITING_SNAPSHOT,
                    control_schema=self.control_schema,
                )
                self.awaiting_snapshot.add(change.qualified)
            if change.kind == CHANGE_DROPPED and action.destructive:
                destination.forget_source_relation(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                    control_schema=self.control_schema,
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
                        "applied": (
                            action.destructive
                            or change.kind in {CHANGE_RECREATED, CHANGE_SCHEMA}
                        ),
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
                relation_filenode=relation.relfilenode,
                relation_type_oid=relation.relation_type_oid,
                published=relation.published,
                admission_state=require_admission_state(relation.admission_state),
                replica_identity=relation.replica_identity,
                columns=relation.columns,
                control_schema=self.control_schema,
            )
        self.destructive_refused += len(plan.refused)
        return markers

    def backfill_schema(self, con, plan: CatalogPlan) -> None:
        """Backfill source values for columns added by a schema action."""
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
                        change.new_relation, stable_keys, value_columns
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
        by_name = {naming.normalize(column.name): column for column in relation.columns}
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
        """Forget catalog work only after the MD transaction commits."""
        if self.catalog is None:
            return
        changes = [action.change for action in plan.actions]
        if changes:
            # A live watcher can supersede a due plan after the destination COMMIT.
            # The committed fact is still settled as applied; the newer change remains
            # in the watcher queue.  The watcher owns this idempotent transition.
            self.catalog.settle(changes, plan.catalog_epoch)
            for action in plan.actions:
                if (
                    action.change.kind == CHANGE_DROPPED
                    and action.destructive
                    and not any(
                        change.qualified == action.change.qualified
                        for change in self.catalog.pending()
                    )
                ):
                    # A newer source generation may already be live in the watcher.
                    # The committed drop fact is settled, but it must not erase the
                    # newer generation's known/dirty identity or pending obligation.
                    self.catalog.forget(action.change.qualified)
        if plan.relations:
            self.catalog.clear_dirty_if_current(plan.relations, plan.catalog_epoch)
        if source_tables:
            self.catalog.observe_replicated(source_tables)

    def summary(self) -> dict:
        return {
            "tables_dropped": self.tables_dropped,
            "tables_quarantined": self.tables_quarantined,
            "catalog_changes_applied": self.changes_applied,
            "catalog_destructive_refused": self.destructive_refused,
            "tables_awaiting_snapshot": sorted(self.awaiting_snapshot),
        }
