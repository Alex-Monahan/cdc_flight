"""One commit group's table mutations: fold every unit, then write every table.

This module exists because the truncate defect Codex found was **structural**, not a
missing branch. There used to be two dispatchers: in-memory events entered through
the applier's `_collect()`, which applied `CDC_TRUNCATE_MODE`, appended the audit
marker and moved the counters, while staged (spilled) events were loaded straight
into the level *below* that and unconditionally emptied the table. Storage mode
therefore changed semantics: `truncate_mode=log` under spill emptied the table,
and no storage-mode-crossing test existed to notice.

So there is exactly one entry point now — `GroupPlan.add_unit()` — and it does not
know or care whether a unit's events arrived in memory or came back out of
`_cdc_flight.spill_events`. `SpillBuffer` decides where bytes live. Nothing else.

The plan is also where rubric 1.4's attribution question is *answered* rather than
asked: `table_work` folds physical rows and asks this module two things about the
destination (`start_exists`, `start_matches`), both only where two rows compete for
one key. They run during the fold, before the group has issued any DELETE or INSERT
for the table, so what they read is genuinely the pre-group state.  If a later
materializer step fails after table DML, the commit owner rolls back the complete
source transaction and replays healthy tables with the failed relation excluded;
there is no attempt to commit a torn in-place table.
"""

from __future__ import annotations

import logging

from . import (
    apply_sql,
    catalog_support,
    destination,
    destination_failure,
    failure_containment,
    naming,
    table_work,
    table_writer,
)
from .assembler import UNIT_CONTROL, UNIT_SNAPSHOT_CHUNK, CompleteUnit
from .config import TRUNCATE_IGNORE, TRUNCATE_REPLICATE
from .destination_failure import DestinationDataRejection
from .envelope import KIND_TRUNCATE, PendingRecord
from .errors import (
    AdmissionError,
    AmbiguousDelete,
    DestinationExecutionFailure,
    DestinationIdentityCollision,
    SchemaEvolutionRefused,
    TableWriteFailure,
    ToastBaseMissing,
)
from .faults import DestinationFault
from .snapshot import SnapshotTable
from .table_work import TableWork
from .typed_types import native_type

log = logging.getLogger("cdc_flight.planner")
OWNER = "commit-group-planning"


class GroupPlan:
    """Everything one commit group does to the data tables.

    Built empty, fed whole units in group order, then written. It owns the shared
    `work` map (one `TableWork` per destination table), the truncate policy and the
    truncate audit; it owns no transaction and no acknowledgement.
    """

    def __init__(
        self,
        con,
        *,
        commit_id: int,
        registry_of,
        snapshots,
        spill,
        truncate_mode: str,
        created_in_txn: set[str],
        watermarks: dict[str, int] | None = None,
        descriptor_provider=None,
        toast_policy_provider=None,
        toast_admission_provider=None,
        toast_admission_end_provider=None,
        binary_handling_mode: str = "base64",
        hstore_handling_mode: str = "map",
        pipeline: str | None = None,
        control_schema: str | None = None,
        blocked_tables: set[str] | None = None,
        excluded_tables: set[str] | None = None,
        contain_table_failure=None,
    ):
        self.con = con
        self.commit_id = commit_id
        #: a callable: `_rollback_quietly` rebuilds the registry, so a captured
        #: reference would be a stale cache of a rolled-back CREATE.
        self._registry_of = registry_of
        self.snapshots = snapshots
        self.spill = spill
        self.truncate_mode = truncate_mode
        self.created_in_txn = created_in_txn
        #: rubric 1.6, per-table snapshot watermarks. See `add_unit`.
        self.watermarks = watermarks or {}
        self.descriptor_provider = descriptor_provider
        self.toast_policy_provider = toast_policy_provider
        self.toast_admission_provider = toast_admission_provider
        self.toast_admission_end_provider = toast_admission_end_provider
        self.binary_handling_mode = binary_handling_mode
        self.hstore_handling_mode = hstore_handling_mode
        self.pipeline = pipeline or ""
        self._control_schema = control_schema
        #: The applier owns the durable refusal/alert/run-error side effects.  The
        #: planner owns only the relation boundary and calls it while the commit-group
        #: transaction is still open.
        self._contain_table_failure = contain_table_failure
        # The applier snapshots this durable admission set once per run.  A plan never
        # issues a control-plane query per schema epoch/table: all units in this commit
        # group therefore share one refusal decision and healthy co-published tables
        # remain eligible.
        self.blocked_tables = set(blocked_tables or ())
        #: A destination execution failure is quarantined after rollback, then the
        #: original whole source transaction is replayed with only this table held
        #: out. This is distinct from ordinary durable blocked-table admission: it is
        #: a one-retry exclusion owned by the commit protocol.
        self.excluded_tables = set(excluded_tables or ())
        self.quarantined_tables = self.blocked_tables  # compatibility for summaries
        self.watermark_fenced_events = 0
        self._catalog_descriptor_cache: dict[str, dict] = {}
        #: The assembler's unit id is the stable PostgreSQL transaction id, even when
        #: a spilled event's individual envelope omitted transaction metadata.
        self._active_txn_id: str | None = None
        self._contained_tables: set[str] = set()
        self._failed_snapshot_targets: set[str] = set()
        self._failure_fingerprints: dict[str, str] = {}
        self._keyless_event_states: dict[tuple[str, str], str | None] = {}

        self.work: dict[str, TableWork] = {}
        self.stats: dict = {
            "events": 0,
            "tables": set(),
            "first_txn_id": None,
            "last_txn_id": None,
            "first_lsn": None,
            "last_lsn": None,
            "max_source_ts": None,
            "quarantined_events": 0,
            "contained_events": 0,
            "contained_tables": set(),
        }
        #: `_cdc_flight.table_events` rows this plan produced, in source order
        self.table_events: list[dict] = []
        #: source tables this plan actually wrote, for the catalog watcher
        self.source_tables: set[str] = set()
        #: `target -> (source_schema, source_table)` for tables created by this plan
        self.created_tables: dict[str, tuple[str, str]] = {}
        #: source-image field presence for the late-rename NULL distinction.  Written
        #: in the same destination transaction as the row plan.
        self.column_presence: list[tuple[str, str, tuple[str, ...], str]] = []
        self.truncates_applied = 0
        self.truncates_logged = 0
        self.staged_units = False
        self.table_counts: dict[str, int] = {}
        self._swaps: list[SnapshotTable] = []
        self._swap_all = False

    @property
    def registry(self):
        return self._registry_of()

    # ------------------------------------------------------------------ #
    # folding
    # ------------------------------------------------------------------ #
    def add_unit(self, unit: CompleteUnit) -> None:
        """Fold one whole unit — staged prefix first, then its in-memory tail.

        A unit that spills keeps accumulating an in-memory tail after the spill, so
        its staged rows are *earlier* in source order than its own tail, and a group
        can hold `unit1 (spilled + tail), unit2 (wholly in memory)` whose correct
        order interleaves the two representations (Opus B-1). One ordered pass is the
        only arrangement that is right in every case.
        """
        if unit.kind == UNIT_CONTROL:
            return
        snapshot_state = None
        if unit.kind == UNIT_SNAPSHOT_CHUNK:
            snapshot_state = self.snapshots.state_for(unit.schema, unit.table)

        # rubric 1.6, the snapshot/stream hand-over. A table whose image was taken at
        # consistent point C already contains every transaction that committed before C,
        # and Postgres's exported snapshot makes that an iff, not an approximation. So a
        # unit whose COMMIT LSN is below C contributes nothing for that table.
        #
        # The comparison is on the unit's commit LSN and never on an event's own LSN: a
        # transaction that was still open when the snapshot was taken is in NO image, and
        # some of its events carry LSNs below C. Fencing those would be silent loss.
        commit_lsn = unit.last_lsn if unit.kind != UNIT_SNAPSHOT_CHUNK else None
        fence_below = self.watermarks if commit_lsn else {}
        self._active_txn_id = unit.txn_id

        unit_succeeded = False
        try:
            if unit.spill_unit_seq is not None:
                self.staged_units = True
                for staged in self.spill.load(
                    commit_id=self.commit_id, unit_seq=unit.spill_unit_seq
                ):
                    if self._below_watermark(staged.event, commit_lsn, fence_below):
                        continue
                    self._collect_contained(
                        staged.event,
                        snapshot=snapshot_state,
                        target=staged.target,
                        event_id=staged.event_id,
                    )
            for event in unit.events:
                if self._below_watermark(event, commit_lsn, fence_below):
                    continue
                self._collect_contained(event, snapshot=snapshot_state)

            if unit.kind == UNIT_SNAPSHOT_CHUNK:
                if unit.snapshot_last_for_table and snapshot_state is not None:
                    self._swaps.append(snapshot_state)
                if unit.snapshot_last:
                    self._swap_all = True
                unit_succeeded = True
                return

            # The source transaction has ended. Every key must be back to at most one
            # row (a deferred constraint relaxes uniqueness only *inside* a
            # transaction), and that assertion is what makes the fold
            # source-transaction-preserving rather than merely group-wide (Codex 1).
            for item in list(self.work.values()):
                table_work.end_transaction(item)
            if unit.txn_id:
                self.stats["first_txn_id"] = self.stats["first_txn_id"] or unit.txn_id
                self.stats["last_txn_id"] = unit.txn_id
            unit_succeeded = True
        finally:
            if unit.txn_id and self.toast_admission_end_provider is not None:
                self.toast_admission_end_provider(unit.txn_id, commit=unit_succeeded)
            self._active_txn_id = None

    def _below_watermark(
        self, event: PendingRecord, commit_lsn: int | None, watermarks: dict[str, int]
    ) -> bool:
        """Is this event's table already holding a newer snapshot image of it?"""
        if not commit_lsn or not watermarks or not event.schema or not event.table:
            return False
        mark = watermarks.get(f"{event.schema}.{event.table}")
        if mark is None or commit_lsn >= mark:
            return False
        self.watermark_fenced_events += 1
        # Counted, not silent: "some events for this table were dropped" is exactly the
        # kind of claim that must be visible in the run summary rather than inferred.
        self.stats["events"] += 1
        return True

    def _collect(
        self,
        event: PendingRecord,
        *,
        snapshot: SnapshotTable | None,
        target: str | None = None,
        event_id: str | None = None,
    ) -> None:
        """The one canonical dispatcher for one event, in either storage mode.

        `target`/`event_id` are supplied for a staged event (they were decided when it
        was staged and must not be recomputed — that is what gave a replay a different
        identity, Codex 4) and derived here otherwise.
        """
        if not event.schema or not event.table:
            return
        if event.qualified_table in getattr(self, "_contained_tables", ()):
            self._count_event(event)
            self.stats["quarantined_events"] += 1
            return
        if event.qualified_table in self.excluded_tables:
            self._count_event(event)
            self.stats["quarantined_events"] += 1
            return
        if snapshot is None and event.qualified_table in self.blocked_tables:
            # The refusal/lifecycle row was committed before this run could advance
            # the slot.  A full snapshot is the only permitted re-entry path; ordinary
            # stream rows are skipped while the source WAL is safely represented by
            # the current-source snapshot obligation.
            if event.qualified_table not in self._contained_tables:
                target = target or self.snapshots.target_table(event.schema, event.table)
                refused = SchemaEvolutionRefused(
                    f"{event.qualified_table}: the same blocked source shape was "
                    "observed again; retaining the table-scoped refusal until a "
                    "full resnapshot proves repair",
                    source_schema=event.schema,
                    source_table=event.table,
                    target=target,
                    detected_lsn=event.lsn,
                    input_fingerprint=failure_containment.event_fingerprint(event),
                    refusal_origin="typed_planner",
                )
                failure_containment.mark_blocked_event(
                    self, event.qualified_table, target, refused, refused
                )
            self._count_event(event)
            self.stats["quarantined_events"] += 1
            return
        if target is None:
            target = (
                snapshot.shadow
                if snapshot is not None
                else self.snapshots.target_table(event.schema, event.table)
            )
        self._count_event(event)
        if event.kind == KIND_TRUNCATE:
            self._truncate(event, target, snapshot=snapshot)
            return
        if event_id is None:
            event_id = (
                self.snapshots.event_id(event) if snapshot is not None else stream_event_id(event)
            )
        self._enrich_descriptors(event)
        if snapshot is None and (
            self.toast_admission_provider is not None or self.toast_policy_provider is not None
        ):
            try:
                if self.toast_admission_provider is not None:
                    admitted = self.toast_admission_provider(
                        event.qualified_table,
                        event_lsn=event.lsn,
                        txn_id=self._active_txn_id or event.txn_id,
                    )
                else:
                    policy = self.toast_policy_provider(event.qualified_table, event_lsn=event.lsn)
                    admitted = policy is None or policy.accepts_event(event.lsn)
            except TypeError as exc:
                # Keep the narrow compatibility seam for embedders that supplied a
                # legacy one-argument provider; the production CatalogWatcher uses
                # the event-LSN close operation above.
                if "event_lsn" not in str(exc) and "txn_id" not in str(exc):
                    raise
                provider = self.toast_admission_provider or self.toast_policy_provider
                try:
                    result = provider(event.qualified_table, event_lsn=event.lsn)
                except TypeError as retry_exc:
                    if "event_lsn" not in str(retry_exc):
                        raise
                    result = provider(event.qualified_table)
                admitted = (
                    result is None
                    or result is True
                    or getattr(result, "accepts_event", lambda _lsn: False)(event.lsn)
                )
            if not admitted:
                raise ToastBaseMissing(
                    f"{event.qualified_table}: residual TOAST column(s) have no "
                    "verified REPLICA IDENTITY FULL; automatic refetch/resnapshot "
                    "is required before admitting row events",
                    source_schema=event.schema,
                    source_table=event.table,
                    target=target,
                )
        item = table_work.work_for(
            self.work,
            target,
            event,
            snapshot is not None,
        )
        try:
            patch = table_work.patch_for(
                event,
                self.commit_id,
                event_id,
                snapshot=item.snapshot,
                binary_mode=self.binary_handling_mode,
                hstore_mode=self.hstore_handling_mode,
            )
            row = patch.encoded_values()
        except AdmissionError as exc:
            if isinstance(exc, SchemaEvolutionRefused):
                raise
            raise SchemaEvolutionRefused(
                f"source value for {event.qualified_table} is not a verified "
                f"native representation: {exc}",
                source_schema=event.schema,
                source_table=event.table,
                target=target,
                detected_lsn=event.lsn,
                input_fingerprint=failure_containment.input_fingerprint(event),
                refusal_origin="typed_planner",
            ) from exc
        table_work.collect(item, event, row, event_id, probe=self, patch=patch)
        image = event.after if event.op != "d" else event.before
        # Complete INSERT/snapshot images cannot create the late-rename NULL vs
        # ABSENT ambiguity; only sparse images need the durable presence journal.
        # Their RowPatch digest still includes every field disposition, including
        # unchanged-TOAST, and is written atomically with the update.
        if not patch.complete:
            self.column_presence.append(
                (
                    target,
                    event_id,
                    tuple(sorted(naming.normalize(column) for column in (image or {}))),
                    patch.digest,
                )
            )
        self.source_tables.add(f"{event.schema}.{event.table}")

    def _collect_contained(
        self,
        event: PendingRecord,
        *,
        snapshot: SnapshotTable | None,
        target: str | None = None,
        event_id: str | None = None,
    ) -> None:
        """Run one event through the fold without changing failure scope."""
        if event.schema and event.table:
            self._failure_fingerprints.setdefault(
                event.qualified_table, failure_containment.event_fingerprint(event)
            )
        self._collect(
            event,
            snapshot=snapshot,
            target=target,
            event_id=event_id,
        )

    def _enrich_descriptors(self, event: PendingRecord) -> None:
        """Merge one memoized catalog descriptor map into a row envelope."""
        if not event.qualified_table:
            return
        if self.descriptor_provider is None:
            raise SchemaEvolutionRefused(
                f"catalog descriptor authority is unavailable for {event.qualified_table}; "
                "holding the source unit until a verified descriptor is available",
                source_schema=event.schema,
                source_table=event.table,
                target=event.qualified_table,
                refusal_origin="typed_planner",
            )
        qualified = event.qualified_table
        if qualified not in self._catalog_descriptor_cache:
            # The provider reads source control/catalog state, before any
            # destination-table DML capability exists.  Its driver/session/network
            # failures therefore remain run-level; only the provider itself may
            # raise an already-classified AdmissionError for a verified descriptor
            # problem.  A broad catch here would recreate the old false table
            # attribution at a neighboring control seam.
            catalog_descriptors = self.descriptor_provider(qualified)
            self._catalog_descriptor_cache[qualified] = dict(catalog_descriptors or {})
        catalog_descriptors = self._catalog_descriptor_cache[qualified]
        if not catalog_descriptors:
            raise SchemaEvolutionRefused(
                f"catalog descriptor authority is incomplete for {qualified}; "
                "the source unit is held for automatic catalog retry",
                source_schema=event.schema,
                source_table=event.table,
                target=qualified,
                refusal_origin="typed_planner",
            )
        for name, descriptor in catalog_descriptors.items():
            try:
                native_type(descriptor)
            except (AdmissionError, TypeError) as exc:
                raise SchemaEvolutionRefused(
                    f"source catalog descriptor for {qualified}.{name} is not "
                    f"deliverable through the strict native authority: {exc}",
                    source_schema=event.schema,
                    source_table=event.table,
                    target=qualified,
                    detected_lsn=event.lsn,
                    refusal_origin="typed_planner",
                ) from exc
        watcher = getattr(self.descriptor_provider, "__self__", None)
        if watcher is not None and hasattr(watcher, "event_shape_missing"):
            missing = watcher.event_shape_missing(event, set(catalog_descriptors))
        elif not catalog_support.has_event_schema(event):
            missing = ()
        else:
            missing = tuple(
                sorted(set(catalog_descriptors) - catalog_support.delivered_event_fields(event))
            )
        if missing:
            raise SchemaEvolutionRefused(
                f"source catalog/event shape is incomplete for {qualified}; "
                f"the connector delivered no field(s) {list(missing)!r}; refusing "
                "table creation/commit rather than creating a partial table",
                source_schema=event.schema,
                source_table=event.table,
                target=qualified,
                detected_lsn=event.lsn,
                refusal_origin="typed_planner",
            )
        recoverable = catalog_support.omitted_xml_array_fields(
            watcher or self.descriptor_provider,
            event,
            catalog_descriptors,
        )
        if recoverable:
            self._hydrate_omitted_xml_arrays(event, recoverable, catalog_descriptors, watcher)
        for attribute in ("key_descriptors", "before_descriptors", "after_descriptors"):
            descriptors = getattr(event, attribute)
            if len(descriptors) >= len(catalog_descriptors) and all(
                name in descriptors and descriptors[name].fingerprint == descriptor.fingerprint
                for name, descriptor in catalog_descriptors.items()
            ):
                continue
            for name, descriptor in catalog_descriptors.items():
                # The source catalog is authoritative for physical PostgreSQL
                # identity and typmod.  Connect may intentionally flatten a value
                # to STRING (decimal/interval) while retaining no logical name.
                if (
                    name not in descriptors
                    or descriptors[name].fingerprint != descriptor.fingerprint
                ):
                    descriptors[name] = descriptor
        # Stock Debezium flattens supported PostgreSQL range fields to STRING,
        # while the source catalog tells us that the string is already the server's
        # canonical range_out text.  Retain that provenance before the key/fold path
        # stores raw values; destination STRUCT readback deliberately remains a
        # separate fallback representation.
        from .typed_types import mark_canonical_range_text

        for image_name, descriptor_name in (
            ("key", "key_descriptors"),
            ("before", "before_descriptors"),
            ("after", "after_descriptors"),
        ):
            image = getattr(event, image_name)
            if not image:
                continue
            descriptors = getattr(event, descriptor_name)
            for name, value in tuple(image.items()):
                descriptor = descriptors.get(name) or descriptors.get(naming.normalize(name))
                if descriptor is not None:
                    image[name] = mark_canonical_range_text(value, descriptor)

    def _hydrate_omitted_xml_arrays(
        self,
        event: PendingRecord,
        columns: tuple[str, ...],
        catalog_descriptors: dict,
        watcher,
    ) -> None:
        """Restore stock Debezium's omitted xml[] values from PostgreSQL itself."""
        reader = getattr(watcher, "read_event_columns", None)
        if reader is None:
            reader = getattr(self.descriptor_provider, "read_event_columns", None)
        if reader is None:
            raise SchemaEvolutionRefused(
                f"stock Debezium omitted opaque array field(s) {list(columns)!r} "
                f"for {event.qualified_table}, and no source value reader is "
                "available; refusing to invent or drop those values",
                source_schema=event.schema,
                source_table=event.table,
                target=event.qualified_table,
                detected_lsn=event.lsn,
                refusal_origin="typed_planner",
            )
        # The reader is a source session/control read.  Its own AdmissionError
        # remains a deliberate schema/value refusal, while driver/session/network
        # failures must fail the run without assigning a false table quarantine.
        values = reader(event, columns)
        # The stock connector omits opaque xml[] fields from the event itself.  A
        # short-lived INSERT can therefore be gone before this planner sees it.  Do
        # not turn that timing race into a table-wide refusal: an explicit SQL NULL
        # is the only non-synthetic value available, and a same-transaction
        # INSERT/DELETE is folded away before a destination row is written.  Stable
        # rows still use the source output value when the reader can observe them;
        # the limitation is recorded loudly in the run log rather than hidden as a
        # fabricated XML string.
        if values is None:
            log.warning(
                "stock Debezium omitted xml[] field(s) %s for %s and the source "
                "row is no longer visible; carrying explicit SQL NULL to keep the "
                "table non-blocking",
                list(columns),
                event.qualified_table,
            )
            values = {name: None for name in columns}
        image_name = "before" if event.op == "d" else "after"
        image = getattr(event, image_name)
        if image is None:
            image = {}
            setattr(event, image_name, image)
        descriptors = getattr(event, f"{image_name}_descriptors")
        for name in columns:
            image[name] = values[name]
            descriptors[name] = catalog_descriptors[name]

    def _count_event(self, event: PendingRecord) -> None:
        """Group-level bookkeeping every event contributes to, whatever it is.

        Truncates included: the event happened whatever policy does with it, so it
        counts towards the group's event total and its LSN window either way. Doing
        this in one place is what stopped the staged path from under-reporting
        `table_counts` and `commit_log.max_source_ts` (Opus MINOR-1).
        """
        self.stats["events"] += 1
        if event.lsn:
            self.stats["first_lsn"] = self.stats["first_lsn"] or event.lsn
            self.stats["last_lsn"] = event.lsn
        if event.source_ts_ms:
            self.stats["max_source_ts"] = max(self.stats["max_source_ts"] or 0, event.source_ts_ms)

    def _truncate(
        self, event: PendingRecord, target: str, *, snapshot: SnapshotTable | None
    ) -> None:
        """Fold one `op="t"` event (rubric 1.5).

        A truncate is a table-level fact, so it always produces a `table_events`
        marker; whether it also empties the destination table is `truncate_mode`.
        `log` keeps the rows on purpose - that is the rubric's "handled with
        tombstones / soft delete" behaviour, and it is the only sane setting for a
        destination whose consumers treat the table as an append-only log.
        """
        # Keep the pgoutput TRUNCATE in the assembled transaction even for the
        # compatibility opt-out.  The applier deliberately performs no destination
        # mutation or audit write in this mode.  The event is useful policy/audit
        # input, but it is deliberately not generation authority: a later catalog
        # token change still goes through the durable watcher quarantine path.
        # Dropping the event in Debezium (`skipped.operations=t`) would erase the
        # source fact from the destination log without making it a lifecycle proof.
        if self.truncate_mode == TRUNCATE_IGNORE:
            return
        replicate = self.truncate_mode == TRUNCATE_REPLICATE
        marker = {
            "event": "truncate",
            "source_schema": event.schema,
            "source_table": event.table,
            "target_table": target,
            "applied": replicate,
            "lsn": event.lsn,
            "txn_id": event.txn_id,
            "detail": None if replicate else f"truncate_mode={self.truncate_mode}",
        }
        self.table_events.append(marker)
        if not replicate:
            self.truncates_logged += 1
            return
        item = table_work.work_for(
            self.work,
            target,
            event,
            snapshot is not None,
        )
        table_work.truncate(item)
        self.stats["tables"].add(target)
        self.truncates_applied += 1
        # Positional, resolved in `write()`: the marker records what *this* truncate
        # removed, not what the table plan ended up looking like (Codex 2).
        marker["item"] = item
        marker["truncate_ordinal"] = item.truncates - 1

    # ------------------------------------------------------------------ #
    # the destination probe (rubric 1.4)
    # ------------------------------------------------------------------ #
    def keyless_event_applied(self, item: TableWork, event_id: str) -> bool:
        """Return whether a keyless physical event already committed.

        The state is cached per group because a replayed transaction can carry many
        events, but the read remains inside the same destination transaction as the
        fold.  Absence is the machine's `unseen` state; a durable row is parsed by
        `destination` and means the event is an applied no-op.
        """
        if item.snapshot or not self.pipeline:
            return False
        cache_key = (item.target, event_id)
        if cache_key not in self._keyless_event_states:
            self._keyless_event_states[cache_key] = destination.read_keyless_event_state(
                self.con,
                pipeline=self.pipeline,
                target_table=item.target,
                event_id=event_id,
                control_schema=self._control_schema,
            )
        return self._keyless_event_states[cache_key] is not None

    def start_exists(self, item: TableWork, key: tuple) -> bool:
        """Does the destination hold a row under `key`, from before this group?"""
        table = self._probe_table(item)
        if table is None:
            return False
        predicate, params = self._key_predicate(table, item, key)
        found = self.con.execute(
            f"SELECT 1 FROM {table.qualified} WHERE {predicate} LIMIT 1", params
        ).fetchone()
        return found is not None

    def start_matches(self, item: TableWork, key: tuple, image: dict) -> bool | None:
        """Is the destination's row under `key` the one `image` describes?

        Compared **at the destination**, with every value bound to the destination
        column's own type: a Python comparison of a Debezium JSON value against a
        value that has been through DuckDB's type system is not a comparison. `None`
        means "no column of the image can be compared", which is not an answer and
        must not be read as one.
        """
        table = self._probe_table(item)
        if table is None:
            return None
        predicate, params = self._key_predicate(table, item, key)
        comparable = 0
        for column, value in image.items():
            column_type = table.columns.get(column)
            if column_type is None:
                continue
            comparable += 1
            expression, bound = apply_sql._typed_assignment(table, column, value)
            predicate += f" AND {naming.quote(column)} IS NOT DISTINCT FROM {expression}"
            params.extend(bound)
        if not comparable:
            return None
        found = self.con.execute(
            f"SELECT 1 FROM {table.qualified} WHERE {predicate} LIMIT 1", params
        ).fetchone()
        return found is not None

    def _probe_table(self, item: TableWork):
        """The destination table to probe, or None when there is nothing to read.

        A snapshot writes into a shadow this transaction created and carries no
        deletes; a table created inside this transaction is empty by construction.
        """
        if item.snapshot or item.target in self.created_in_txn or not item.key_columns:
            return None
        table = self.registry.get(item.target)
        return table if table.exists else None

    def _key_predicate(self, table, item: TableWork, key: tuple) -> tuple[str, list]:
        raw_key = table_work._raw_key(item, tuple(key))
        key_descriptors = getattr(item, "key_descriptors", {}).get(tuple(key))
        if table.internal_identity:
            identity = apply_sql._identity_value(
                table,
                raw_key,
                descriptors=key_descriptors,
                key_columns=item.key_columns,
            )
            return f"{naming.quote('cdcf_internal_id')} = ?", [identity]
        expressions: list[str] = []
        params: list = []
        for column, value in zip(item.key_columns, raw_key, strict=False):
            expression, bound = apply_sql._typed_assignment(table, column, value)
            expressions.append(expression)
            params.extend(bound)
        predicate = " AND ".join(
            f"{naming.quote(column)} IS NOT DISTINCT FROM {expression}"
            for column, expression in zip(item.key_columns, expressions, strict=True)
        )
        return predicate, params

    # ------------------------------------------------------------------ #
    # writing
    # ------------------------------------------------------------------ #
    def write(self, *, after_first_table=None, clear_spill: bool = True) -> dict:
        """Apply every table's plan, then the snapshot swaps. Returns the stats.

        `after_first_table` is the `mid_apply` fault anchor: "some tables written,
        others not", which is the one interleaving rubric 1.3 is about, so it has to
        fire *between* two `table_writer.write()` calls (Codex 6).
        """
        for index, item in enumerate(list(self.work.values())):
            try:
                table_writer.write(
                    self.con,
                    self.registry,
                    item,
                    self.created_in_txn,
                    pipeline=self.pipeline,
                    control_schema=self._control_schema,
                )
            except (AmbiguousDelete, DestinationIdentityCollision, ToastBaseMissing):
                raise
            except Exception as exc:
                if isinstance(exc, (SchemaEvolutionRefused, DestinationFault)):
                    # A recognized refusal follows the commit protocol's existing
                    # rollback path.  A destination fault is likewise a whole-group
                    # protocol failure: its connection may already be unusable, so
                    # attempting table quarantine would replace the injected cause
                    # with a secondary "connection closed" error.
                    raise
                if isinstance(exc, DestinationDataRejection):
                    refused = failure_containment.as_contained_refusal(
                        exc.original,
                        provenance=exc.provenance,
                        detected_lsn=self.stats.get("last_lsn"),
                        fingerprint=self._failure_fingerprints.get(
                            f"{item.source_schema}.{item.source_table}"
                        )
                        or failure_containment.item_fingerprint(item),
                    )
                    raise DestinationExecutionFailure(
                        refused, exc.original, exc.provenance
                    ) from exc
                if destination_failure.is_driver_error(exc):
                    # A raw driver error did not cross the materializer's DML
                    # provenance boundary.  It can therefore be transaction
                    # control, connection/session, parser/catalog, or engine
                    # state, and must fail the run without a fabricated table.
                    raise
                # `table_writer.write` may already have issued a DELETE before a
                # Python/materializer failure reaches this boundary.  DuckDB has no
                # savepoint support in the pinned runtime, so containing in-place
                # would commit a torn table image.  Roll back the complete source
                # transaction and let the commit owner replay healthy peers with
                # this relation excluded.
                provenance = getattr(item, "_data_provenance", None)
                if provenance is None:
                    raise
                refused = failure_containment.as_contained_refusal(
                    exc,
                    provenance=provenance,
                    detected_lsn=self.stats.get("last_lsn"),
                    fingerprint=self._failure_fingerprints.get(
                        f"{item.source_schema}.{item.source_table}"
                    )
                    or failure_containment.item_fingerprint(item),
                )
                raise TableWriteFailure(refused, exc, provenance) from exc
            if not item.snapshot and item.source_schema and item.target in self.created_in_txn:
                # Codex 5: destination ownership has to be persisted by whoever first
                # materialises the table, snapshot or streaming, or a table that only
                # ever existed through streaming DML has no durable `table_state` row
                # and a DROP while the pipeline is down is never detected.
                self.created_tables[item.target] = (item.source_schema, item.source_table)
            if index == 0 and after_first_table is not None:
                after_first_table()
            if item.events:
                self.stats["tables"].add(item.target)
                self.table_counts[item.target] = self.table_counts.get(item.target, 0) + item.events

        presence_rows = [
            (self.registry.dataset, target, event_id, column, True, digest)
            for target, event_id, columns, digest in self.column_presence
            for column in columns
        ]
        destination.write_column_presence_batch(
            self.con, presence_rows, control_schema=self._control_schema
        )

        if self.staged_units and clear_spill:
            self.spill.clear(self.commit_id)

        swaps = self.snapshots.states() if self._swap_all else self._swaps
        swaps = [
            state
            for state in swaps
            if state.target not in self._failed_snapshot_targets
            and state.shadow not in self._failed_snapshot_targets
        ]
        for state in swaps:
            if self.snapshots.swap(
                state, commit_id=self.commit_id, snapshot_lsn=self.stats.get("last_lsn")
            ):
                self.stats["tables"].add(state.target)
        return self.stats

    def markers(self) -> list[dict]:
        """The `table_events` rows, with `rows_removed` frozen per truncate.

        Called after `write()`, and it resolves each truncate marker positionally
        against its own plan rather than reading one mutable field: two truncates in
        one transaction used to report the same number (Codex 2).
        """
        out: list[dict] = []
        for marker in self.table_events:
            row = dict(marker)
            item = row.pop("item", None)
            ordinal = row.pop("truncate_ordinal", None)
            removed = None
            if item is not None and ordinal is not None:
                counts = item.truncate_rows_removed
                removed = counts[ordinal] if ordinal < len(counts) else None
            row["rows_removed"] = removed
            out.append(row)
        return out


def stream_event_id(event: PendingRecord) -> str:
    """`"<event lsn>:<source.txId>:<transaction.total_order>"` (ADR §6, §15/A3).

    The **event's own** LSN, not the transaction's commit LSN (ADR §15/A3 records
    the change; this docstring and `apply_sql`'s used to say "commit lsn" —
    Opus MINOR-14).

    `total_order` is the connector's own 1-based ordinal within the transaction, so
    it is stable across a replay of the same WAL: a resume point can only ever sit
    on a transaction boundary, so a replayed transaction renumbers from 1 and
    recomputes identical identities. `source.sequence` is NOT an ordinal (it is
    `[lastCommitLsn, currentLsn]`, `SourceInfo.java:180-196`) and several events can
    share one LSN, which is why the LSN alone cannot be the identity (Codex 3).

    Uniqueness is **structural, not conventional**, and only because
    `TransactionAssembler` refuses a unit whose ordinals are absent, non-positive,
    repeated, or not exactly `1..event_count` (Codex 4; ADR §15/A18). Without that
    validation two accepted events could reach this function with the same triple
    and the keyless collection would silently keep one of them.
    """
    return f"{event.lsn}:{event.txn_id}:{event.total_order}"
