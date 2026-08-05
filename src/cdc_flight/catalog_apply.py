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
3. **revalidation** — the source generation proof is acquired on a separate source
   transaction at the last moment, with an `ACCESS SHARE` lease held through the
   destination commit. A relation that exists is not dropped when the proof says the
   queued fact is stale. Fails **closed**: if the source cannot be proved, nothing is
   destroyed;
4. **the circuit breaker** — one poll may destroy at most `CDC_DROP_MAX_PER_POLL`
   relations (default 1). Every plural case is a schema migration or a
   misconfiguration, and both want a human (Opus MAJOR-3 / Q2). None of the set is
   applied when the limit is exceeded: applying the first N and stopping would be the
   worst of both.

A `recreated` relation is the one case where the source table *exists* and the
destination table is still wrong — it holds the rows of a different relation. It is
quarantined in both drop modes: the physical target and any snapshot shadow are
removed, and the table is marked `awaiting_snapshot` in `table_state` in the same
transaction. That makes the incomplete image loud without leaving a mixed table
queryable; rubric 2.3/3.4 own the automatic re-snapshot that turns the flag off.

Alerts are returned, never written here: they must survive a rollback, which means a
different connection and a moment after the transaction has settled (Codex 7).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from . import catalog_generation, destination, naming
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
from .machines import CHANGE_APPLIED, CHANGE_REFUSED, require_admission_state
from .schema_evolution import apply_column_changes

log = logging.getLogger("cdc_flight.catalog_apply")

#: `table_state.snapshot_state` for a relation whose destination table was removed
#: because the source relation was replaced. The rows are gone and CDC alone cannot
#: rebuild them, so anything that reads this table must know it is incomplete.
#: Canonical definition now lives in `destination`, which rubric 1.6's re-snapshot and
#: rubric 1.8's recovery also write; re-exported so the name still reads locally.
AWAITING_SNAPSHOT = destination.AWAITING_SNAPSHOT


#: the source could not be re-read, which is NOT evidence that anything is gone
UNKNOWN = catalog_generation.UNKNOWN
#: revalidation is switched off (`CDC_DROP_REVALIDATE=0`)
class _Skipped:
    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return "SKIPPED"


SKIPPED = _Skipped()


def _stale(change: CatalogChange, generation) -> str | None:
    """Why this destructive change must not be applied, or None if it still holds."""
    if generation is SKIPPED:
        return None
    if generation is None:
        return "the source generation proof is absent"
    if generation.state in {
        catalog_generation.GENERATION_UNKNOWN,
        catalog_generation.GENERATION_AMBIGUOUS,
        catalog_generation.GENERATION_BOUNDARY_UNPROVEN,
    }:
        return f"the source generation proof is {generation.state}"
    if change.kind == CHANGE_DROPPED:
        if generation.state == catalog_generation.GENERATION_ABSENT:
            return None
        return (
            "the relation exists at the source again "
            f"(identity {generation.current_identity})"
        )
    # A recreated action is safe only when the same complete token is still present.
    if generation.state == catalog_generation.GENERATION_ABSENT:
        return "the relation has since been dropped; the drop will be detected on its own"
    if generation.state == catalog_generation.GENERATION_NEWER:
        return (
            "the relation was replaced again "
            f"(identity {generation.current_identity}); a newer observation supersedes this one"
        )
    return None if generation.state == catalog_generation.GENERATION_CURRENT else (
        f"the source generation proof is {generation.state}"
    )


def _same_oid_rewrite(change: CatalogChange) -> bool:
    """Whether the observed change differs only in PostgreSQL's physical token."""
    old = catalog_generation.coerce_identity(
        change.old_identity or change.old_oid or change.old_relation
    )
    new = catalog_generation.coerce_identity(
        change.new_identity or change.new_oid or change.new_relation
    )
    return bool(
        old
        and new
        and old.oid == new.oid
        and old.relfilenode != new.relfilenode
        and old.reltype_oid == new.reltype_oid
        and old.complete
        and new.complete
    )


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
    #: The exact proof consumed to make the revalidation decisions in this plan.
    generation_proof: dict = field(default_factory=dict)
    #: Same-OID physical rewrites proven by a streamed TRUNCATE. They refresh the
    #: durable token but do not quarantine the destination image.
    generation_refreshes: tuple[CatalogChange, ...] = ()

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
    def plan(
        self,
        durable_lsn: int,
        *,
        source_proof: dict[str, object] | None = None,
        strict_boundary: bool = False,
        streamed_truncates: set[str] | None = None,
        streamed_rows: set[str] | None = None,
    ) -> CatalogPlan:
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
        generation_refreshes: list[CatalogChange] = []
        streamed_truncates = streamed_truncates or set()
        streamed_rows = streamed_rows or set()

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
            if c.kind in DESTRUCTIVE
            and self.drop_mode == DROP_REPLICATE
            and not (
                _same_oid_rewrite(c)
                and c.qualified in streamed_truncates
            )
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

        revalidation_changes = [
            c
            for c in due
            if c.kind == CHANGE_RECREATED
            or (c.kind == CHANGE_DROPPED and self.drop_mode == DROP_REPLICATE)
        ]
        proofs = self._source_proofs(
            [c for c in revalidation_changes if id(c) not in blocked],
            source_proof=source_proof,
        )
        for change in due:
            if change.kind == CHANGE_RECREATED:
                raw_proof = proofs.get(change.qualified, UNKNOWN)
                same_oid_rewrite = _same_oid_rewrite(change)
                expected_identity = (
                    (change.old_identity or change.old_oid)
                    if same_oid_rewrite
                    else (change.new_identity or change.new_oid)
                )
                generation = (
                    raw_proof
                    if raw_proof is SKIPPED
                    else catalog_generation.check(
                        expected_identity,
                        raw_proof,
                        minimum_lsn=durable_lsn if strict_boundary else None,
                    )
                )
                if generation.state == catalog_generation.GENERATION_NEWER:
                    if same_oid_rewrite:
                        observed_generation = catalog_generation.check(
                            change.new_identity or change.new_oid,
                            raw_proof,
                            minimum_lsn=durable_lsn if strict_boundary else None,
                        )
                        if (
                            observed_generation.state
                            == catalog_generation.GENERATION_CURRENT
                            and change.qualified in streamed_truncates
                        ):
                            generation_refreshes.append(change)
                            continue
                        if (
                            observed_generation.state
                            == catalog_generation.GENERATION_CURRENT
                            and change.qualified not in streamed_rows
                        ):
                            reason = (
                                "same-OID relfilenode change is ambiguous without a "
                                "streamed TRUNCATE or replacement row at this boundary"
                            )
                            change.to(CHANGE_REFUSED)
                            refused.append((change, reason))
                            log.warning(
                                "not applying %s: %s (the change stays pending)",
                                change.qualified,
                                reason,
                            )
                            continue
                    # The source advanced again while the queued action was waiting.
                    # Replace the obligation and apply only the newest generation.
                    replacement = self.catalog.supersede_recreated(
                        change, generation.current_identity or generation.current_oid
                    )
                    if replacement is None:
                        # No full relation observation exists to construct a safe
                        # newest-generation action. Keep the stale obligation live and
                        # fail closed until the watcher supplies that observation.
                        # (The normal watcher path never reaches this legacy-shape cell.)
                        reason = (
                            "the newer source generation was observed without a full "
                            "relation shape; waiting for catalog discovery"
                        )
                        change.to(CHANGE_REFUSED)  # rubric 1.9 (SM-D)
                        refused.append((change, reason))
                        log.warning("not applying %s: %s", change.qualified, reason)
                        continue
                    change = replacement
                elif generation.state == catalog_generation.GENERATION_ABSENT:
                    if strict_boundary:
                        reason = (
                            "the final source proof is absent; the replacement may have "
                            "come and gone, so its retained image is fenced"
                        )
                        change.to(CHANGE_REFUSED)
                        refused.append((change, reason))
                        log.warning("not applying %s: %s", change.qualified, reason)
                        continue
                    # A->B was never applied and B is now gone. It is a genuine final
                    # drop, not a replacement quarantine; the configured drop mode now
                    # owns the outcome (DROP_LOG retains A, DROP_REPLICATE removes it).
                    change = self.catalog.reclassify_recreated_as_drop(change)
                elif generation.state != catalog_generation.GENERATION_CURRENT:
                    reason = _stale(change, generation)
                    change.to(CHANGE_REFUSED)  # rubric 1.9 (SM-D)
                    refused.append((change, reason or "source generation is unknown"))
                    log.warning(
                        "not applying %s: %s (the change stays pending)",
                        change.qualified,
                        reason,
                    )
                    continue
            destructive = (
                change.kind in DESTRUCTIVE and self.drop_mode == DROP_REPLICATE
            )
            if destructive and id(change) in blocked:
                continue
            target = naming.destination_table(
                self.topic_prefix, change.schema, change.table
            )
            if destructive:
                expected = (
                    (change.new_identity or change.new_oid)
                    if change.kind == CHANGE_RECREATED
                    else (change.old_identity or change.old_oid)
                )
                raw_proof = proofs.get(change.qualified, UNKNOWN)
                generation = (
                    raw_proof
                    if raw_proof is SKIPPED
                    else catalog_generation.check(expected, raw_proof)
                )
                reason = _stale(change, generation)
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
            if change.kind == CHANGE_RECREATED:
                detail = (
                    f"recreated with oid {change.new_oid} (was {change.old_oid}); the "
                    "destination table held the OLD relation's rows and was "
                    + "quarantined and dropped"
                    + ", and `table_state.snapshot_state` is now "
                    f"{AWAITING_SNAPSHOT!r}: it is INCOMPLETE until a re-snapshot "
                    "runs (rubric 2.3/3.4)"
                )
            elif change.kind in DESTRUCTIVE and not destructive:
                detail = f"drop_mode={self.drop_mode}"
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
        # Guard: persisted state must never run ahead of the action it implies. A
        # `source_relations` row carrying the new oid would make the *next* run agree
        # with the source and never notice the drop at all. Only a change that would
        # REMOVE the destination table has to block persistence - letting `new` or
        # `unpublished` block it was measured to leave a table with no row at all,
        # which is how a drop between two runs went undetected. A log-only drop is the
        # opposite case: the destination table remains, so its durable pre-drop
        # identity must remain with it for catalog-baseline confirmation.
        applied = {id(a.change) for a in actions}
        applied.update(id(change) for change in generation_refreshes)
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
        # A schema action carries the post-DDL relation explicitly. Persisting it in
        # this same transaction is what makes a restart see the new column baseline
        # only after the destination RENAME/ADD/DROP has committed.
        for action in actions:
            relation = action.change.new_relation
            # A destructive drop has no post-action source baseline. Re-inserting the
            # old `new_relation` after `forget_source_relation()` would make a later
            # recreate look like CHANGE_RECREATED instead of a new discovery, and it
            # would bypass the discovery re-snapshot hand-off entirely. A log-only drop
            # deliberately keeps the destination table, so retaining that same old
            # identity is what makes the non-destructive marker durable and relatable.
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
            refused=tuple(refused),
            alerts=alerts,
            generation_proof=proofs,
            generation_refreshes=tuple(generation_refreshes),
        )

    def read_generation_proof(self, names) -> dict[str, catalog_generation.GenerationProof]:
        """Read a non-locking planning proof; the commit boundary uses a lease."""
        names = set(names)
        if not names:
            return {}
        if self.catalog is None or not hasattr(self.catalog, "relation_oids"):
            return {
                f"{schema}.{table}": catalog_generation.GenerationProof.unknown(
                    "catalog watcher has no generation reader"
                )
                for schema, table in names
            }
        try:
            values = self.catalog.relation_oids(names)
        except Exception as exc:  # fail closed
            log.warning("could not read source generations: %s", exc)
            return {
                f"{schema}.{table}": catalog_generation.GenerationProof.unknown(str(exc))
                for schema, table in names
            }
        return {
            f"{schema}.{table}": catalog_generation.coerce_proof(
                values.get(f"{schema}.{table}")
            )
            for schema, table in names
        }

    def acquire_generation_proof(self, names) -> catalog_generation.GenerationProofLease:
        """Acquire the last-moment proof and source DDL lease."""
        names = set(names)
        if not names:
            return catalog_generation.GenerationProofLease({})
        if self.catalog is None:
            return catalog_generation.GenerationProofLease(
                {
                    f"{schema}.{table}": catalog_generation.GenerationProof.unknown(
                        "catalog watcher is unavailable"
                    )
                    for schema, table in names
                }
            )
        method = getattr(self.catalog, "generation_proof_lease", None)
        if method is not None:
            return method(names)
        return catalog_generation.GenerationProofLease(
            self.read_generation_proof(names)
        )

    def _source_proofs(
        self,
        changes: list[CatalogChange],
        *,
        source_proof: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Return the proof used by every revalidation decision in this plan.

        Plain drops retain the documented ``CDC_DROP_REVALIDATE=0`` escape hatch.
        Recreates never do: a replacement action always needs a generation proof.
        """
        if not changes:
            return {}
        names = {(c.schema, c.table) for c in changes}
        if source_proof is not None:
            return {
                f"{schema}.{table}": (
                    SKIPPED
                    if (
                        not self.revalidate
                        and c.kind == CHANGE_DROPPED
                    )
                    else source_proof.get(f"{schema}.{table}", UNKNOWN)
                )
                for schema, table in names
                for c in changes
                if c.qualified == f"{schema}.{table}"
            }
        if not self.revalidate:
            # Recreates are never allowed to opt out of the generation check.  The
            # switch remains a drop-only compatibility escape hatch.
            recreate_names = {c.qualified for c in changes if c.kind == CHANGE_RECREATED}
            return {
                f"{schema}.{table}": (
                    UNKNOWN if f"{schema}.{table}" in recreate_names else SKIPPED
                )
                for schema, table in names
            }
        return self.read_generation_proof(names)

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
            if action.destructive and change.kind != CHANGE_RECREATED:
                # The shadow goes too: a table dropped mid-backfill would otherwise
                # leave `<target>__cdcf_tmp` behind forever.
                self.registry.drop(naming.shadow_table(action.target))
                self.registry.drop(action.target)
                destination.forget_table_state(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                )
                stats["tables"].add(action.target)
                self.tables_dropped += 1
            if change.kind == CHANGE_RECREATED:
                # A replacement relation is a new source lifecycle. The retained image
                # is not a valid baseline for the new oid, and identity admission has
                # already fenced any replacement tail that could have arrived before
                # this observation. Quarantine the physical image in the same
                # destination transaction as the durable owed state: after COMMIT there
                # is no queryable mixed table for log mode, and the re-snapshot shadow
                # can be renamed into the now-absent target atomically.
                self.registry.drop(naming.shadow_table(action.target))
                self.registry.drop(action.target)
                stats["tables"].add(action.target)
                self.tables_quarantined += 1
                if action.destructive:
                    self.tables_dropped += 1
                destination.mark_awaiting_snapshot(
                    con,
                    pipeline=self.pipeline,
                    source_schema=change.schema,
                    source_table=change.table,
                    target_table=action.target,
                    state=AWAITING_SNAPSHOT,
                )
                self.awaiting_snapshot.add(change.qualified)
            if change.kind == CHANGE_DROPPED and action.destructive:
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
        changes.extend(plan.generation_refreshes)
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
            "tables_quarantined": self.tables_quarantined,
            "catalog_changes_applied": self.changes_applied,
            "catalog_destructive_refused": self.destructive_refused,
            "tables_awaiting_snapshot": sorted(self.awaiting_snapshot),
        }
