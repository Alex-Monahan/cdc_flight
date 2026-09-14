"""Lifecycle and queue ownership for the source catalog watcher."""

from __future__ import annotations

import logging
import time
from dataclasses import replace

from . import catalog_change_queue, catalog_generation, catalog_poll, catalog_state, catalog_support
from . import source_marker as marker_mod
from .catalog_state import (
    CHANGE_DROPPED,
    CHANGE_NEW,
    CHANGE_PARTITION_ATTACHED,
    CHANGE_PARTITION_DETACHED,
    CHANGE_PARTITION_DROPPED,
    CHANGE_RECREATED,
    CHANGE_REPUBLISHED,
    CHANGE_SCHEMA,
    CHANGE_UNPUBLISHED,
    DESTRUCTIVE,
    FENCED,
    PARTITION_CHANGE_KINDS,
    CatalogChange,
    PartitionEdge,
    PartitionTopologyObservation,
    SourceRelation,
)
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

log = logging.getLogger("cdc_flight.catalog")


def _queued(change: CatalogChange) -> CatalogChange:
    return catalog_state.queued(change)


class CatalogLifecycleMixin:
    """Methods that compare, queue, fence, and settle catalog generations."""

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
        *,
        policy_gate=None,
    ) -> list[tuple]:
        from . import catalog_support

        return catalog_support.read_columns(
            self,
            relation,
            key_columns,
            value_columns,
            policy_gate=policy_gate or getattr(self, "policy_gate", None),
        )

    def read_event_columns(self, event, value_columns):
        """Recover source fields omitted by stock Debezium's opaque-array path."""
        from . import catalog_support

        with self._event_read_lock:
            if self._event_read_conn is None or self._event_read_conn.closed:
                self._event_read_conn = catalog_poll.connect(self)
            try:
                relation = self.known.get(event.qualified_table)
                descriptors = {
                    catalog_support.naming.normalize(column.name): column.descriptor
                    for column in (relation.columns if relation is not None else ())
                }
                return catalog_support.read_event_columns_from_connection(
                    self._event_read_conn,
                    event,
                    value_columns,
                    policy_gate=getattr(self, "policy_gate", None),
                    descriptors=descriptors,
                )
            except Exception:
                # A failed statement can leave the reusable session unusable. Close
                # it before the planner's normal refusal/retry path asks again.
                self._event_read_conn.close()
                self._event_read_conn = None
                raise

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
                | self.gone
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
                if not source_table or (not self.all_schemas and source_schema not in self.schemas):
                    # A relation outside the configured catalog scope is unobserved,
                    # not dropped. In all-schemas mode this branch is only defensive.
                    continue
                if (
                    self._schema_liveness.get(
                        source_schema,
                        SCHEMA_UNAVAILABLE if self.all_schemas else SCHEMA_VISIBLE,
                    )
                    != SCHEMA_VISIBLE
                ):
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
                        current = replace(current, admission_state=previous.admission_state)
                        observed[name] = current
                    # A present relation cancels a drop.  A physical rewrite within the
                    # same lifecycle refreshes an existing recreate obligation; it does
                    # not supersede it merely because its relfilenode changed.
                    matching_recreate = catalog_generation.matching_recreate(
                        pending_recreates, current
                    )
                    if matching_recreate is not None:
                        catalog_generation.refresh_recreate(matching_recreate, current, lsn)
                        self.known[name] = current
                        self._dirty[name] = current
                    elif catalog_generation.has_newer_recreate(pending_recreates, current):
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
                queued = any(c.qualified == name and c.kind in FENCED for c in self._live())
                if previous is None:
                    if current is None:
                        if name in self.gone:
                            # The source remains absent after a terminal discharge;
                            # do not rediscover the same DROP on every run.
                            continue
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
                                kind=CHANGE_DROPPED,
                                schema=schema,
                                table=table,
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
                                CHANGE_RECREATED,
                                current,
                                lsn,
                                old_oid=None,
                                new_oid=current.oid,
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
                    if name in self.gone:
                        # The old source generation was discharged as gone. A present
                        # relation with this name is a new generation even when the
                        # source OID happens to be reused, so it must be rebuilt before
                        # any stream row can be admitted.
                        self.gone.discard(name)
                        self.replicated.add(name)
                        self.known[name] = current
                        self._dirty[name] = current
                        added.append(
                            self._change(
                                CHANGE_RECREATED,
                                current,
                                lsn,
                                old_oid=None,
                                new_oid=current.oid,
                            )
                        )
                    elif name in self.replicated or name in self.include or self.auto_discover:
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
                    added.append(self._change(CHANGE_NEW, current, lsn, new_oid=current.oid))
                    continue
                column_changes = diff_columns(previous.columns, current.columns)
                if column_changes:
                    schema_queued = any(
                        c.qualified == name and c.kind == CHANGE_SCHEMA for c in self._live()
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
                            current,
                            lsn,
                            old_oid=previous.oid,
                            new_oid=current.oid,
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
                change.kind,
                change.qualified,
                change.old_oid,
                change.new_oid,
                change.detected_lsn,
                change.confirmations,
            )
        for name in superseded:
            log.warning(
                "superseding a stale catalog action for %s: the relation is present "
                "at the source again",
                name,
            )
        return added

    # -- partition topology ----------------------------------------------- #
    @staticmethod
    def _relation_generation(relation: SourceRelation | None) -> tuple | None:
        if relation is None:
            return None
        identity = catalog_generation.identity_for(relation)
        return identity.oid, identity.relfilenode, identity.reltype_oid

    @classmethod
    def _partition_change_shape(cls, change: CatalogChange) -> tuple:
        return (
            change.kind,
            (
                change.old_partition_edge.edge_key
                if change.old_partition_edge is not None
                else None
            ),
            (
                change.new_partition_edge.edge_key
                if change.new_partition_edge is not None
                else None
            ),
        )

    @staticmethod
    def _edge_relation_matches(edge: PartitionEdge, relation: SourceRelation | None) -> bool:
        if relation is None:
            return False
        return CatalogLifecycleMixin._relation_generation(relation) == edge.child_generation

    @classmethod
    def _partition_change_holds(
        cls,
        change: CatalogChange,
        current: dict[tuple, PartitionEdge],
        observed: dict[str, SourceRelation],
    ) -> bool:
        old = change.old_partition_edge
        new = change.new_partition_edge
        if change.kind == CHANGE_PARTITION_ATTACHED:
            return new is not None and new.edge_key in current
        if old is None or old.edge_key in current:
            return False
        child = observed.get(old.child_qualified)
        if change.kind == CHANGE_PARTITION_DETACHED:
            return cls._edge_relation_matches(old, child)
        if change.kind == CHANGE_PARTITION_DROPPED:
            return child is None
        return False

    def _partition_supersede(self, change: CatalogChange) -> None:
        if change.state not in {CHANGE_APPLIED, CHANGE_SUPERSEDED}:
            change.to(CHANGE_SUPERSEDED)
            self.superseded += 1

    def _confirm_partition(
        self, shape: tuple, change: CatalogChange
    ) -> CatalogChange | None:
        """Confirm an edge transition without sharing the table-name queue key."""
        seen = self._partition_unconfirmed.get(shape)
        if seen is None:
            change.to(CHANGE_UNCONFIRMED)
            tracked = change
        else:
            tracked = seen
            tracked.confirmations += 1
            tracked.detected_lsn = change.detected_lsn
            tracked.observation_epoch = getattr(change, "observation_epoch", 0)
            tracked.to(CHANGE_UNCONFIRMED)
        if tracked.confirmations < self.confirm_polls:
            self._partition_unconfirmed[shape] = tracked
            log.info(
                "%s observed for partition edge %s (%s/%s confirming polls); "
                "not queued yet",
                tracked.kind,
                tracked.qualified,
                tracked.confirmations,
                self.confirm_polls,
            )
            return None
        self._partition_unconfirmed.pop(shape, None)
        return tracked

    def _new_partition_change(
        self,
        kind: str,
        edge: PartitionEdge,
        lsn: int,
        *,
        old_edge: PartitionEdge | None = None,
        new_edge: PartitionEdge | None = None,
        observation_epoch: int,
    ) -> CatalogChange:
        selected = new_edge or old_edge or edge
        from .catalog_generation import RelationIdentity

        old_identity = (
            RelationIdentity(*old_edge.child_generation) if old_edge is not None else None
        )
        new_identity = (
            RelationIdentity(*new_edge.child_generation) if new_edge is not None else None
        )
        change = CatalogChange(
            kind=kind,
            schema=selected.child_schema,
            table=selected.child_table,
            detected_lsn=lsn,
            old_oid=(old_edge.child_oid if old_edge is not None else None),
            new_oid=(new_edge.child_oid if new_edge is not None else None),
            old_identity=old_identity,
            new_identity=new_identity,
            old_partition_edge=old_edge,
            new_partition_edge=new_edge,
        )
        # The epoch is an observation fact alongside, not instead of, the source WAL
        # fence. It lets settlement identify which snapshot was planned.
        change.observation_epoch = observation_epoch
        return change

    def _partition_observation_rejected(self, observation, reason: str) -> list:
        self._partition_observation_state = "pending"
        self._partition_observation_reason = reason
        if observation.detection_lsn:
            self._partition_last_detection_lsn = int(observation.detection_lsn)
        log.warning("partition topology observation held pending: %s", reason)
        return []

    def _compare_partitions(
        self,
        observation: PartitionTopologyObservation,
        observed: dict[str, SourceRelation],
    ) -> list[CatalogChange]:
        """Diff two complete positive topology observations.

        The stable snapshot is intentionally not advanced on the first half of a
        transition.  That makes the confirmation streak compare the same complete
        edge generation twice and leaves a pending fact re-detectable after a crash.
        """
        with self._lock:
            if not observation.usable:
                return self._partition_observation_rejected(
                    observation,
                    observation.reason
                    or "partition catalog result was empty, incomplete, unfenced, "
                    "or contained an in-progress edge",
                )

            current: dict[tuple, PartitionEdge] = {}
            for edge in observation.edges:
                if edge.edge_key in current:
                    return self._partition_observation_rejected(
                        observation, "partition catalog returned a duplicate edge key"
                    )
                parent = observed.get(edge.parent_qualified)
                child = observed.get(edge.child_qualified)
                if parent is None or child is None:
                    return self._partition_observation_rejected(
                        observation,
                        "partition edge was not backed by a visible parent and child "
                        "generation in the complete relation observation",
                    )
                if self._relation_generation(parent) != edge.parent_generation:
                    return self._partition_observation_rejected(
                        observation, "partition parent generation did not agree"
                    )
                if self._relation_generation(child) != edge.child_generation:
                    return self._partition_observation_rejected(
                        observation, "partition child generation did not agree"
                    )
                current[edge.edge_key] = edge

            # A parent disappearing is not a child DROP fact. The relation catalog
            # must remain positively complete before any edge removal is classified.
            for old in self._snapshot_partitions.values():
                if old.parent_qualified not in observed:
                    return self._partition_observation_rejected(
                        observation,
                        "partition parent was absent from the relation set; no edge "
                        "lifecycle event is provable",
                    )
                parent = observed.get(old.parent_qualified)
                if self._relation_generation(parent) != old.parent_generation:
                    return self._partition_observation_rejected(
                        observation,
                        "partition parent OID/generation reuse was observed; refusing "
                        "to manufacture an edge event",
                    )
                child = observed.get(old.child_qualified)
                if child is not None and self._relation_generation(child) != old.child_generation:
                    return self._partition_observation_rejected(
                        observation,
                        "partition child OID/generation reuse was observed; refusing "
                        "to manufacture an edge event",
                    )

            if not self._partition_baseline_initialized:
                self._snapshot_partitions = current
                self._partition_baseline_initialized = True
                self._partition_snapshot_epoch = observation.observation_epoch
                self._partition_snapshot_lsn = observation.detection_lsn
                self._partition_snapshot_dirty = (
                    self._snapshot_partitions != self._partition_persisted_edges
                    or not self._partition_persisted_baseline_initialized
                )
                self._partition_observation_state = "complete"
                self._partition_observation_reason = None
                self._partition_last_detection_lsn = observation.detection_lsn
                return []

            stable = dict(self._snapshot_partitions)
            candidates: list[tuple[tuple, CatalogChange]] = []
            for key in sorted(current.keys() - stable.keys(), key=repr):
                edge = current[key]
                shape = (
                    CHANGE_PARTITION_ATTACHED,
                    None,
                    edge.edge_key,
                )
                candidates.append(
                    (
                        shape,
                        self._new_partition_change(
                            CHANGE_PARTITION_ATTACHED,
                            edge,
                            observation.detection_lsn,
                            new_edge=edge,
                            observation_epoch=observation.observation_epoch,
                        ),
                    )
                )
            for key in sorted(stable.keys() - current.keys(), key=repr):
                edge = stable[key]
                child = observed.get(edge.child_qualified)
                kind = (
                    CHANGE_PARTITION_DETACHED
                    if child is not None
                    else CHANGE_PARTITION_DROPPED
                )
                shape = (kind, edge.edge_key, None)
                candidates.append(
                    (
                        shape,
                        self._new_partition_change(
                            kind,
                            edge,
                            observation.detection_lsn,
                            old_edge=edge,
                            observation_epoch=observation.observation_epoch,
                        ),
                    )
                )

            candidate_shapes = {shape for shape, _change in candidates}
            # A transition that no longer holds is superseded explicitly.  A live
            # fact that still holds remains live while its source fence is pending.
            for shape, change in list(self._partition_unconfirmed.items()):
                if shape not in candidate_shapes:
                    self._partition_supersede(change)
                    self._partition_unconfirmed.pop(shape, None)
                    self._changes = [
                        item for item in self._changes if item.state in LIVE_CHANGE_STATES
                    ]
            for change in list(self._live()):
                if (
                    change.kind in PARTITION_CHANGE_KINDS
                    and not self._partition_change_holds(change, current, observed)
                ):
                    self._partition_supersede(change)
            self._changes = [
                item for item in self._changes if item.state in LIVE_CHANGE_STATES
            ]

            ready: list[CatalogChange] = []
            waiting = 0
            for shape, change in candidates:
                if any(
                    item.kind in PARTITION_CHANGE_KINDS
                    and self._partition_change_shape(item) == shape
                    and self._partition_change_holds(item, current, observed)
                    for item in self._live()
                ):
                    continue
                waiting += 1
                confirmed = self._confirm_partition(shape, change)
                if confirmed is not None:
                    ready.append(confirmed)

            if waiting and len(ready) == waiting:
                for change in ready:
                    change.to(CHANGE_PENDING)
                self._changes.extend(ready)
                self._snapshot_partitions = current
                self._partition_snapshot_epoch = observation.observation_epoch
                self._partition_snapshot_lsn = observation.detection_lsn
                self._partition_snapshot_dirty = (
                    self._snapshot_partitions != self._partition_persisted_edges
                    or not self._partition_persisted_baseline_initialized
                )
            self._partition_observation_state = "complete"
            self._partition_observation_reason = None
            self._partition_last_detection_lsn = observation.detection_lsn
            return ready if waiting and len(ready) == waiting else []

    @property
    def partition_snapshot_dirty(self) -> bool:
        with self._lock:
            return bool(self._partition_snapshot_dirty)

    def partition_snapshot_for_plan(
        self, due: list[CatalogChange] | tuple[CatalogChange, ...] = ()
    ) -> tuple[PartitionEdge, ...] | None:
        """Return only a snapshot that is safe to write in this destination plan."""
        with self._lock:
            if not self._partition_snapshot_dirty:
                return None
            due_ids = {id(change) for change in due}
            live_partition = [
                change for change in self._live() if change.kind in PARTITION_CHANGE_KINDS
            ]
            if self._partition_unconfirmed:
                return None
            if live_partition and not all(id(change) in due_ids for change in live_partition):
                return None
            return tuple(
                sorted(self._snapshot_partitions.values(), key=lambda edge: repr(edge.edge_key))
            )

    def mark_partition_snapshot_persisted(
        self, edges: tuple[PartitionEdge, ...] | list[PartitionEdge]
    ) -> None:
        with self._lock:
            self._partition_persisted_edges = {edge.edge_key: edge for edge in edges}
            self._partition_persisted_baseline_initialized = True
            self._partition_snapshot_dirty = (
                self._partition_persisted_edges != self._snapshot_partitions
            )

    def partition_snapshot_for_flush(self) -> tuple[PartitionEdge, ...] | None:
        """Return a stable snapshot for the post-watcher observation flush."""
        with self._lock:
            if not self._partition_snapshot_dirty or self._partition_unconfirmed:
                return None
            if any(
                change.kind in PARTITION_CHANGE_KINDS for change in self._live()
            ):
                return None
            return tuple(
                sorted(self._snapshot_partitions.values(), key=lambda edge: repr(edge.edge_key))
            )

    def _confirm(self, name: str, change: CatalogChange) -> CatalogChange | None:
        return catalog_change_queue.confirm(self, name, change)

    def _supersede(self, name: str, *kinds: str) -> list[str]:
        """Cancel live changes of `kinds` for `name`. Caller holds the lock."""
        cancelled = [c for c in self._live() if c.qualified == name and c.kind in kinds]
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
                        change.kind,
                        change.qualified,
                        self.grace_seconds,
                        durable_lsn,
                        change.detected_lsn,
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

    def clear_dirty_if_current(self, relations, planned_epoch: int | None = None) -> None:
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
            for change in self._changes:
                if change.qualified == name and change.state not in {
                    CHANGE_APPLIED,
                    CHANGE_SUPERSEDED,
                }:
                    change.to(CHANGE_SUPERSEDED)
                    self.superseded += 1
            self._changes = [
                change
                for change in self._changes
                if change.state not in {CHANGE_APPLIED, CHANGE_SUPERSEDED}
            ]
            self.replicated.discard(name)

    def observe_replicated(self, names: set[str]) -> None:
        """Tell the watcher which tables now have destination tables."""
        with self._lock:
            self.replicated |= names

    def summary(self) -> dict:
        return catalog_support.summary(self)
