"""Destination schema registry and DDL ownership.

This module owns source-descriptor authority, physical table shape, and typed
shadow swaps. Identity serialization and row materialization live in sibling
modules; the compatibility facade keeps historical imports stable.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .errors import (
    DestinationIdentityCollision,
    SchemaBackfillRefused,
    SchemaEvolutionRefused,
)
from .naming import CDCF_EVENT_ID, quote

BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR = "BOOLEAN", "BIGINT", "DOUBLE", "JSON", "VARCHAR"


def widen(current: str | None, incoming: str | None) -> str | None:
    """Least type that holds both; ambiguous changes remain conservative text."""
    if current is None:
        return incoming
    if incoming is None or current == incoming:
        return current
    if {current, incoming} == {BIGINT, DOUBLE}:
        return DOUBLE
    return VARCHAR


class TableSchema:
    """The destination shape of one replicated table, cached for the process."""

    def __init__(self, name: str, dataset: str):
        self.name = name
        self.dataset = dataset
        self.columns: dict[str, str] = {}
        #: the destination's own type string, before `_normalise_type` collapses it.
        #: Kept so we never ALTER a column whose real type we did not recognise
        #: (Opus MINOR-15): TIMESTAMP normalises to VARCHAR, and "widening" a
        #: TIMESTAMP column to VARCHAR is destructive-by-accident.
        self.raw_types: dict[str, str] = {}
        self.key_columns: tuple[str, ...] = ()
        #: Source identity, separate from a generated destination key used when a
        #: UNION/LIST/STRUCT/MAP cannot be indexed.
        self.source_key_columns: tuple[str, ...] = ()
        self.exists = False
        #: True when the table carries a destination-side PRIMARY KEY on its
        #: identity columns (Opus M-2).
        self.constrained = False
        #: Exact source descriptors used to construct the physical columns.  They are
        #: process-local cache only; the durable source_relations descriptor and the
        #: destination's UNION declaration remain the two durable truths.
        self.source_descriptors: dict[str, Any] = {}
        self.native_types: dict[str, Any] = {}
        self.primary_key_columns: tuple[str, ...] = ()
        self.internal_identity = False
        #: Whether the durable sidecar has established the source key tuple.  A
        #: legacy table with only ``cdcf_internal_id`` has no source-key metadata;
        #: the next catalog-authoritative ensure is allowed to repair that shape.
        self.key_metadata_loaded = False
        #: Every source descriptor that has occupied an internal-identity key
        #: column.  Retaining the history lets a post-DDL delete resolve the old
        #: UNION member instead of binding only the current descriptor.
        self.identity_descriptors: dict[str, tuple[Any, ...]] = {}

    @property
    def qualified(self) -> str:
        return f"{quote(self.dataset)}.{quote(self.name)}"


class SchemaRegistry:
    """Creates and evolves destination tables. All DDL runs in the caller's txn.

    Every table it creates carries a `PRIMARY KEY` on its identity columns - the
    source key columns for a keyed table, `cdcf_event_id` for a keyless one - so
    that "duplication is impossible" is *enforced by the destination* rather than
    asserted by the applier (Opus M-2). That is what turns the whole B-1 class of
    apply-path defect from silent corruption into a failed transaction, and a failed
    transaction is safe: it rolls back and replays. MEASURED on DuckDB 1.5.4:
    200 000 rows x 2 columns through Arrow into a table with a PRIMARY KEY takes
    0.03 s, and DELETE-then-INSERT of the same key inside one transaction is
    accepted, so the merge path is unaffected.
    """

    def __init__(self, con, dataset: str, *, constraints: bool = True):
        self.con = con
        self.dataset = dataset
        self.constraints = constraints
        self._tables: dict[str, TableSchema] = {}
        self._identity_history: dict[str, dict[str, tuple[Any, ...]]] = {}
        self._typed_swap_count = 0

    def get(self, name: str) -> TableSchema:
        table = self._tables.get(name)
        if table is None:
            table = TableSchema(name, self.dataset)
            table.identity_descriptors = dict(self._identity_history.get(name, {}))
            self._load(table)
            self._tables[name] = table
        return table

    def forget(self, name: str) -> None:
        table = self._tables.pop(name, None)
        if table is not None and table.identity_descriptors:
            self._identity_history[name] = dict(table.identity_descriptors)

    def _load(self, table: TableSchema) -> None:
        self._refresh(table)
        self._load_key_metadata(table)

    @property
    def _key_metadata_qualified(self) -> str:
        return f"{quote(self.dataset)}.{quote('__cdcf_key_metadata')}"

    def _load_key_metadata(self, table: TableSchema) -> None:
        if not table.exists:
            return
        from .typed_types import SourceTypeDescriptor

        try:
            rows = self.con.execute(
                f"SELECT source_key_columns, source_descriptors, "
                f"identity_descriptors FROM {self._key_metadata_qualified} "
                "WHERE target_table = ?",
                [table.name],
            ).fetchall()
        except Exception:
            # The sidecar is introduced lazily, so a pre-existing destination has
            # no row until the first typed ensure.  It is not source-catalog
            # authority and must never turn a catalog failure into an empty map.
            return
        if not rows:
            return
        key_json, descriptor_json, history_json = rows[0]
        try:
            table.source_key_columns = tuple(json.loads(key_json or "[]"))
            table.key_columns = table.source_key_columns or table.key_columns
            descriptors = json.loads(descriptor_json or "{}")
            table.source_descriptors = {
                str(column): SourceTypeDescriptor.from_dict(value)
                for column, value in descriptors.items()
            }
            history = json.loads(history_json or "{}")
            loaded_history = {
                str(column): tuple(SourceTypeDescriptor.from_dict(item) for item in values)
                for column, values in history.items()
            }
            table.identity_descriptors = {
                column: tuple(
                    dict.fromkeys(
                        (*table.identity_descriptors.get(column, ()),
                         *loaded_history.get(column, ()))
                    )
                )
                for column in set(table.identity_descriptors) | set(loaded_history)
            }
            table.key_metadata_loaded = True
        except (TypeError, ValueError, KeyError) as exc:
            raise SchemaEvolutionRefused(
                f"destination key metadata for {table.name} is corrupt; refusing "
                "to guess a source identity",
                target=table.name,
            ) from exc

    def _persist_key_metadata(self, table: TableSchema) -> None:
        """Persist source-key and old-descriptor identity facts atomically."""
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {self._key_metadata_qualified} ("
            "target_table VARCHAR PRIMARY KEY, source_key_columns VARCHAR, "
            "source_descriptors VARCHAR, identity_descriptors VARCHAR)"
        )
        descriptors = {
            column: descriptor.to_dict()
            for column, descriptor in table.source_descriptors.items()
            if hasattr(descriptor, "to_dict")
        }
        history = {
            column: [descriptor.to_dict() for descriptor in descriptors_for_column]
            for column, descriptors_for_column in table.identity_descriptors.items()
        }
        self.con.execute(
            f"DELETE FROM {self._key_metadata_qualified} WHERE target_table = ?",
            [table.name],
        )
        self.con.execute(
            f"INSERT INTO {self._key_metadata_qualified} VALUES (?, ?, ?, ?)",
            [
                table.name,
                json.dumps(list(table.source_key_columns)),
                json.dumps(descriptors, sort_keys=True),
                json.dumps(history, sort_keys=True),
            ],
        )
        table.key_metadata_loaded = True

    def _delete_key_metadata(self, name: str) -> None:
        try:
            self.con.execute(
                f"DELETE FROM {self._key_metadata_qualified} WHERE target_table = ?",
                [name],
            )
        except Exception:
            # No sidecar exists for a legacy table; dropping that table remains the
            # caller's only required operation.
            return

    def _refresh(self, table: TableSchema) -> None:
        rows = self.con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [self.dataset, table.name],
        ).fetchall()
        if rows:
            table.exists = True
            table.columns = {name: _normalise_type(dtype) for name, dtype in rows}
            # Preserve DuckDB's physical declaration (especially UNION member
            # names); normalisation is only for comparison, never for rebuilding a
            # shadow table.
            table.raw_types = {name: str(dtype) for name, dtype in rows}
            key_rows = self.con.execute(
                "SELECT k.column_name "
                "FROM information_schema.key_column_usage k "
                "JOIN information_schema.table_constraints t "
                "  ON t.constraint_schema = k.constraint_schema "
                " AND t.constraint_name = k.constraint_name "
                " AND t.table_name = k.table_name "
                "WHERE k.table_schema = ? AND k.table_name = ? "
                "  AND t.constraint_type = 'PRIMARY KEY' "
                "ORDER BY k.ordinal_position",
                [self.dataset, table.name],
            ).fetchall()
            if key_rows:
                table.primary_key_columns = tuple(row[0] for row in key_rows)
                table.internal_identity = "cdcf_internal_id" in table.primary_key_columns
                table.key_columns = table.source_key_columns or table.primary_key_columns

    # -- DDL ---------------------------------------------------------------- #
    def ensure(
        self,
        name: str,
        *,
        columns: dict[str, str],
        key_columns: tuple[str, ...],
        strict: bool = False,
    ) -> tuple[TableSchema, bool]:
        """`(schema, created_now)`. `created_now` lets the caller skip the DELETE
        half of a merge against a table it just created."""
        table = self.get(name)
        if key_columns:
            # NOT unconditionally: a group whose only event for this table is a
            # TRUNCATE carries no message key, so it would erase the cached identity
            # and silently disarm `assert_identity_is_unique` for the rest of the run
            # (Opus MINOR-3).
            table.key_columns = key_columns
        if not table.exists:
            self._create(table, columns, key_columns)
            return table, True

        for col, ctype in columns.items():
            existing = table.columns.get(col)
            if existing is None:
                # rubric 2.1 - an added source column must simply appear.
                try:
                    self.con.execute(
                        f"ALTER TABLE {table.qualified} ADD COLUMN {quote(col)} {ctype}"
                    )
                except Exception as exc:
                    if strict:
                        raise SchemaEvolutionRefused(
                            f"cannot add source column {name}.{col}: destination DDL "
                            "failed, so the catalog baseline cannot be persisted",
                            target=name,
                        ) from exc
                    raise
                table.columns[col] = ctype
                table.raw_types[col] = ctype
                continue
            widened = widen(existing, ctype)
            if widened == existing:
                if strict and existing != ctype:
                    raise SchemaEvolutionRefused(
                        f"cannot apply schema change to {name}.{col}: destination "
                        f"type {existing} cannot adopt source type {ctype} through "
                        "the safe widening lattice; refusing to persist an unadopted "
                        "catalog baseline",
                        target=name,
                    )
                continue
            raw = table.raw_types.get(col, existing)
            if raw.upper() not in _RECOGNISED_TYPES:
                # `_normalise_type` collapses TIMESTAMP / DECIMAL / DATE / BLOB / LIST
                # to VARCHAR, so an "upgrade" computed from that lattice can narrow a
                # real TIMESTAMP column to text. Refuse explicitly instead of doing it
                # by accident; rubric 2.4/2.5 own the real answer (Opus MINOR-15).
                message = (
                    f"cannot apply schema change to {name}.{col}: destination type "
                    f"{raw} is outside the safe widening lattice; refusing to persist "
                    f"a catalog baseline as {widened}"
                )
                if strict:
                    raise SchemaEvolutionRefused(message, target=name)
                log.warning(message)
                continue
            try:
                self.con.execute(
                    f"ALTER TABLE {table.qualified} ALTER COLUMN {quote(col)} "
                    f"SET DATA TYPE {widened}"
                )
                table.columns[col] = widened
                table.raw_types[col] = widened
            except Exception as exc:  # rubric 2.5 owns the real answer
                message = (
                    f"could not apply schema change to {name}.{col}: cannot widen "
                    f"{existing} to {widened}: {exc}"
                )
                if strict:
                    raise SchemaEvolutionRefused(message, target=name) from exc
                log.warning(message)
        return table, False

    def ensure_typed(
        self,
        name: str,
        *,
        columns: dict[str, Any],
        key_columns: tuple[str, ...],
    ) -> tuple[TableSchema, bool]:
        """Create a table from source descriptors, never from observed Python values.

        This is the 2.4 creation path.  ``columns`` may contain
        ``SourceTypeDescriptor`` or ``NativeType`` objects.  A UNION/list/struct/map
        key cannot be indexed by DuckDB, so the physical table receives a generated
        internal identity primary key; the source key columns remain ordinary typed
        columns and are retained for source attribution.
        """
        from .typed_types import NativeType, SourceTypeDescriptor, native_type

        table = self.get(name)
        requested_key_columns = tuple(key_columns)
        if table.key_metadata_loaded:
            previous_key_columns = tuple(table.source_key_columns)
        elif table.internal_identity:
            # An old internal-identity table predates the sidecar.  The incoming
            # catalog key is the only safe source-key fact available; the physical
            # cdcf id itself is not a source column.
            previous_key_columns = requested_key_columns
        else:
            previous_key_columns = tuple(table.source_key_columns or table.key_columns)
        resolved: dict[str, NativeType] = {}
        descriptors: dict[str, SourceTypeDescriptor] = {}
        physical_columns: dict[str, str] = {}
        for column, descriptor in columns.items():
            if isinstance(descriptor, str):
                physical_columns[column] = descriptor
                continue
            target = native_type(descriptor, for_key=column in key_columns)
            resolved[column] = target
            physical_columns[column] = target.sql
            source_descriptor = target.source if isinstance(descriptor, NativeType) else descriptor
            if source_descriptor is not None:
                descriptors[column] = source_descriptor
        if table.exists:
            for column, physical_type in physical_columns.items():
                target = resolved.get(column)
                if column in table.columns:
                    existing_type = table.raw_types.get(column, table.columns[column])
                    if (
                        target is not None
                        and _is_numeric_inner_union(existing_type)
                        and target.kind != "NUMERIC_UNION"
                    ):
                        raise SchemaEvolutionRefused(
                            f"cannot write {name}.{column}: the source descriptor "
                            f"resolves to {physical_type} (kind={target.kind}), but the destination is "
                            f"the bounded numeric inner UNION {existing_type}; a "
                            "typed shadow conversion must run before post-change "
                            "events are admitted",
                            target=name,
                        )
                    key_status_changed = (column in previous_key_columns) != (
                        column in requested_key_columns
                    )
                    json_key_transition = _json_key_transition(existing_type, physical_type)
                    allowed_json_transition = json_key_transition and (
                        key_status_changed
                        or (
                            not table.key_metadata_loaded
                            and table.internal_identity
                            and column in requested_key_columns
                        )
                    )
                    if (
                        not _type_sql_equal(existing_type, physical_type)
                        and not str(existing_type).upper().startswith("UNION(")
                        and not allowed_json_transition
                        and not _lossless_numeric_supertype(existing_type, physical_type)
                    ):
                        raise SchemaEvolutionRefused(
                            f"cannot reinterpret existing destination {name}.{column} "
                            f"from {existing_type} as {physical_type}; a typed shadow "
                            "repair is required",
                            target=name,
                        )
                    continue
                try:
                    self.con.execute(
                        f"ALTER TABLE {table.qualified} ADD COLUMN {quote(column)} "
                        f"{physical_type}"
                    )
                except Exception as exc:
                    raise SchemaEvolutionRefused(
                        f"cannot add typed source column {name}.{column}: destination "
                        "DDL failed, so the catalog baseline cannot be persisted",
                        target=name,
                    ) from exc
                table.columns[column] = _normalise_type(physical_type)
                table.raw_types[column] = physical_type
            table.source_descriptors.update(descriptors)
            representation_change = any(
                column in table.columns
                and _json_key_transition(
                    table.raw_types.get(column, table.columns[column]),
                    physical_columns[column],
                )
                and (
                    column in previous_key_columns
                    or column in requested_key_columns
                    or not table.key_metadata_loaded
                )
                for column in resolved
                if column in physical_columns
            )
            if previous_key_columns != requested_key_columns or representation_change:
                return self._rebind_key_identity(
                    table,
                    key_columns=requested_key_columns,
                    resolved=resolved,
                    descriptors=descriptors,
                )
            for column, target in resolved.items():
                physical = table.raw_types.get(column, "")
                if _is_top_level_union(physical):
                    # A bounded PostgreSQL NUMERIC is itself represented by an
                    # inner UNION(finite DECIMAL, special DOUBLE).  That is the
                    # numeric value representation, not the 2.5 source-type
                    # history UNION.  Rehydrate it as the recursive numeric type
                    # so the next value uses ``finite``/``special`` directly
                    # instead of inventing an outer fingerprint member.
                    if target.kind == "NUMERIC_UNION" and _is_numeric_inner_union(physical):
                        if not _type_sql_equal(physical, target.sql):
                            raise SchemaEvolutionRefused(
                                f"cannot reinterpret existing numeric destination "
                                f"{name}.{column} from {physical} as {target.sql}; "
                                "a typed shadow conversion is required",
                                target=name,
                            )
                        table.native_types[column] = target
                    else:
                        table.native_types[column] = _physical_union_native(
                            physical, source=descriptors.get(column)
                        )
                else:
                    table.native_types[column] = target
            if requested_key_columns:
                table.key_columns = requested_key_columns
                table.source_key_columns = requested_key_columns
            self._persist_key_metadata(table)
            return table, False

        indexable = all(
            (
                column == CDCF_EVENT_ID
                and physical_columns.get(column, "").upper() == VARCHAR
            )
            or (column in resolved and resolved[column].indexable)
            for column in key_columns
        )
        primary = tuple(key_columns) if indexable else ("cdcf_internal_id",)
        if not indexable:
            physical_columns = {
                **physical_columns,
                "cdcf_internal_id": 'VARCHAR DEFAULT uuid()',
            }
        self._create_strict(table, physical_columns, primary)
        table.key_columns = tuple(key_columns)
        table.source_key_columns = tuple(key_columns)
        table.primary_key_columns = primary
        table.internal_identity = not indexable
        table.source_descriptors = descriptors
        table.native_types = resolved
        if table.internal_identity:
            table.identity_descriptors = {
                column: (descriptors[column],)
                for column in key_columns
                if column in descriptors
            }
        self._persist_key_metadata(table)
        return table, True

    def _rebind_key_identity(
        self,
        table: TableSchema,
        *,
        key_columns: tuple[str, ...],
        resolved: dict[str, Any],
        descriptors: dict[str, Any],
    ) -> tuple[TableSchema, bool]:
        """Rebuild a destination whose source key tuple changed.

        DuckDB/MotherDuck do not support changing a primary-key column list in place.
        More importantly, JSONB has two deliberate physical representations: JSON for
        an indexed source key and VARIANT for an ordinary value.  Rebinding the key
        therefore has to be one shadow copy, with the exact current catalog mapping
        on the shadow, rather than an ALTER followed by a best-effort insert.
        """
        from .typed_types import NativeType

        old_descriptors = dict(table.source_descriptors)
        old_native_types = dict(table.native_types)
        old_raw_types = dict(table.raw_types)
        target_types: dict[str, str] = {}
        target_native: dict[str, NativeType] = {}
        for column, current_type in old_raw_types.items():
            if column == "cdcf_internal_id":
                continue
            target = resolved.get(column)
            if target is None:
                target_types[column] = current_type
                if column in old_native_types:
                    target_native[column] = old_native_types[column]
                continue
            # A source-type UNION is durable history.  A key rebind changes its
            # identity enforcement, not the historical value representation.
            if _is_top_level_union(current_type):
                target_types[column] = current_type
                target_native[column] = _physical_union_native(
                    current_type, source=descriptors.get(column)
                )
            else:
                target_types[column] = target.sql
                target_native[column] = target

        indexable = all(
            column in resolved
            and resolved[column].indexable
            and not _is_top_level_union(target_types.get(column, ""))
            for column in key_columns
        )
        primary = tuple(key_columns) if indexable else ("cdcf_internal_id",)
        if not self.constraints:
            constraint_sql = ""
        else:
            constraint_sql = ", PRIMARY KEY (" + ", ".join(
                quote(column) for column in primary
            ) + ")"
        if not indexable:
            target_types["cdcf_internal_id"] = 'VARCHAR'

        shadow = f"{table.name}__cdcf_key_shadow"
        definitions = ", ".join(
            f"{quote(column)} {type_name}"
            for column, type_name in target_types.items()
        )
        try:
            self.con.execute(f"DROP TABLE IF EXISTS {quote(self.dataset)}.{quote(shadow)}")
            self.con.execute(
                f"CREATE TABLE {quote(self.dataset)}.{quote(shadow)} "
                f"({definitions}{constraint_sql})"
            )
            target_columns = list(target_types)
            changed_python: set[str] = set()
            for column in target_columns:
                if column == "cdcf_internal_id":
                    continue
                current_type = old_raw_types.get(column, target_types[column])
                desired_type = target_types[column]
                if _type_sql_equal(current_type, desired_type):
                    continue
                elif _json_key_transition(current_type, desired_type) or _lossless_numeric_supertype(
                    current_type, desired_type
                ):
                    changed_python.add(column)
                else:
                    # UNION history has already been deliberately retained above;
                    # any other mismatch belongs to the normal 2.5 typed shadow
                    # conversion and must not be silently narrowed here.
                    raise SchemaEvolutionRefused(
                        f"cannot rebind {table.name}.{column}: destination type "
                        f"{current_type} does not match {desired_type}; a typed "
                        "shadow conversion is required",
                        target=table.name,
                    )
            _copy_rows_with_identity(
                self.con,
                table,
                shadow,
                target_types,
                target_native,
                key_columns=key_columns,
                identity_descriptors={**old_descriptors, **descriptors},
                changed_python=frozenset(changed_python),
            )
            self.con.execute(f"DROP TABLE {table.qualified}")
            from . import faults

            self._typed_swap_count += 1
            faults.maybe_crash("swap", self._typed_swap_count)
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(shadow)} "
                f"RENAME TO {quote(table.name)}"
            )
        except Exception as exc:
            from . import faults

            if isinstance(exc, faults.InjectedFault):
                raise
            raise SchemaEvolutionRefused(
                f"typed key shadow conversion failed for {table.name}: {exc}",
                target=table.name,
            ) from exc

        # `forget()` retains the old identity descriptors before the fresh physical
        # metadata load.  Reattach the requested source key and its complete history
        # so a later process-local post-migration event can resolve the old member.
        old_identity = dict(table.identity_descriptors)
        self.forget(table.name)
        refreshed = self.get(table.name)
        refreshed.key_columns = key_columns
        refreshed.source_key_columns = key_columns
        refreshed.primary_key_columns = primary
        refreshed.internal_identity = not indexable
        refreshed.constrained = bool(self.constraints and primary)
        refreshed.source_descriptors = {**old_descriptors, **descriptors}
        refreshed.native_types = {**old_native_types, **target_native}
        if refreshed.internal_identity:
            for column in key_columns:
                history = list(old_identity.get(column, ()))
                history.extend(
                    descriptor
                    for descriptor in (old_descriptors.get(column), descriptors.get(column))
                    if descriptor is not None
                )
                refreshed.identity_descriptors[column] = tuple(dict.fromkeys(history))
        self._persist_key_metadata(refreshed)
        return refreshed, False

    def _create_strict(
        self, table: TableSchema, columns: dict[str, str], primary_key_columns: tuple[str, ...]
    ) -> None:
        definitions = ", ".join(f"{quote(column)} {ctype}" for column, ctype in columns.items())
        constraint = (
            ", PRIMARY KEY (" + ", ".join(quote(column) for column in primary_key_columns) + ")"
            if self.constraints and primary_key_columns
            else ""
        )
        try:
            self.con.execute(
                f"CREATE TABLE {table.qualified} ({definitions}{constraint})"
            )
        except Exception as exc:
            raise SchemaEvolutionRefused(
                f"cannot create typed destination {table.name}: {exc}", target=table.name
            ) from exc
        table.columns = {column: _normalise_type(ctype) for column, ctype in columns.items()}
        table.raw_types = dict(columns)
        table.exists = True
        table.constrained = bool(constraint)
        table.primary_key_columns = primary_key_columns

    def convert_column_to_union(
        self,
        name: str,
        column: str,
        old_descriptor: Any,
        new_descriptor: Any,
    ) -> TableSchema:
        """Convert one source column through the sole typed shadow-swap path.

        The copy is performed before the live table is dropped.  DuckDB's cast from a
        UNION to an expanded UNION preserves existing member tags, while a scalar is
        wrapped explicitly with its stable fingerprinted member.  No direct ALTER and
        no text/JSON fallback is permitted here.
        """
        from .typed_types import (
            SourceTypeDescriptor,
            native_type,
            union_member_name,
        )

        table = self.get(name)
        if not table.exists:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: destination table does not exist", target=name
            )
        if table.internal_identity and not table.key_metadata_loaded:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: the destination has an internal "
                "identity but no durable source-key metadata; automatic resnapshot "
                "must establish the catalog-authoritative key before typed evolution",
                target=name,
            )
        old_source = old_descriptor if isinstance(old_descriptor, SourceTypeDescriptor) else SourceTypeDescriptor.from_dict(old_descriptor)
        new_source = new_descriptor if isinstance(new_descriptor, SourceTypeDescriptor) else SourceTypeDescriptor.from_dict(new_descriptor)
        source_key = column in table.source_key_columns or column in table.key_columns
        old_native = native_type(old_source, for_key=source_key)
        new_native = native_type(new_source, for_key=source_key)
        source_key_columns = tuple(table.source_key_columns or table.key_columns)
        cached_descriptors = dict(table.source_descriptors)
        cached_native_types = dict(table.native_types)
        if column in table.key_columns:
            for key_column in table.key_columns:
                history = list(table.identity_descriptors.get(key_column, ()))
                previous = cached_descriptors.get(key_column)
                if previous is not None and previous not in history:
                    history.append(previous)
                if key_column == column and old_source not in history:
                    history.append(old_source)
                if history:
                    table.identity_descriptors[key_column] = tuple(history)
        physical = str(table.raw_types.get(column, table.columns.get(column, old_native.sql)))

        # If a previous change already introduced the member, the catalog observation
        # is a repeated same-type observation; reusing the existing declaration is
        # idempotent and does not create a duplicate member.
        current_members = _union_member_names(physical)
        wanted_name = union_member_name(new_source)
        if wanted_name in current_members:
            declared = dict(_union_members(physical)).get(wanted_name)
            if declared is None or _type_sql_equal(declared, new_native.sql) is False:
                raise SchemaEvolutionRefused(
                    f"cannot reuse UNION member {wanted_name} on {name}.{column}: "
                    f"physical type {declared!r} disagrees with descriptor type "
                    f"{new_native.sql!r}",
                    target=name,
                )
            table.source_descriptors[column] = new_source
            table.native_types[column] = _physical_union_native(
                physical, source=new_source
            )
            self._persist_key_metadata(table)
            return table

        if _is_numeric_inner_union(physical):
            # The existing bounded NUMERIC declaration is the old source type's
            # inner value UNION.  Source-type evolution adds one stable outer
            # member for that complete representation; it must not append a
            # fingerprint member beside ``finite`` and ``special``.
            if old_native.kind != "NUMERIC_UNION":
                raise SchemaEvolutionRefused(
                    f"cannot convert {name}.{column}: physical numeric UNION does "
                    "not match the old source descriptor",
                    target=name,
                )
            old_member_name = union_member_name(old_source)
            union_sql = (
                f"UNION({old_member_name} {old_native.sql},{wanted_name} {new_native.sql})"
            )
            expression = (
                f"union_value({old_member_name} := "
                f"CAST({quote(column)} AS {old_native.sql}))"
            )
        elif _is_top_level_union(physical):
            # The physical declaration is the durable member history.  Reuse its SQL
            # and append the new member; the old descriptor is only needed to resolve
            # the new member's native SQL when a process restarted.
            member_sql = _union_members(physical)
            members = [(member_name, member_type) for member_name, member_type in member_sql]
            members.append((wanted_name, new_native.sql))
            union_sql = "UNION(" + ",".join(f"{member} {sql}" for member, sql in members) + ")"
            expression = f"CAST({quote(column)} AS {union_sql})"
        else:
            member_name = union_member_name(old_source)
            union_sql = f"UNION({member_name} {old_native.sql},{wanted_name} {new_native.sql})"
            expression = (
                f"union_value({member_name} := "
                f"CAST({quote(column)} AS {old_native.sql}))"
            )

        shadow = f"{name}__cdcf_typed_shadow"
        self.con.execute(f"DROP TABLE IF EXISTS {quote(self.dataset)}.{quote(shadow)}")
        definitions: list[str] = []
        target_types = dict(table.raw_types)
        target_types[column] = union_sql
        for current_column, current_type in table.raw_types.items():
            if current_column == column:
                definitions.append(f"{quote(current_column)} {union_sql}")
            else:
                definitions.append(f"{quote(current_column)} {current_type}")
        primary = table.primary_key_columns or table.key_columns
        # Every source-type change produces a UNION, and DuckDB forbids any UNION
        # column from being an index/primary-key expression even when both members
        # individually are scalar/indexable.
        if column in primary:
            primary = ("cdcf_internal_id",)
            if "cdcf_internal_id" not in table.raw_types:
                definitions.append('"cdcf_internal_id" VARCHAR DEFAULT uuid()')
                target_types["cdcf_internal_id"] = "VARCHAR"
        if not primary:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: no destination identity is available", target=name
            )
        ddl = (
            f"CREATE TABLE {quote(self.dataset)}.{quote(shadow)} "
            f"({', '.join(definitions)}, PRIMARY KEY ({', '.join(quote(item) for item in primary)}))"
        )
        try:
            self.con.execute(ddl)
            target_columns = list(table.raw_types)
            if "cdcf_internal_id" in primary and "cdcf_internal_id" not in target_columns:
                target_columns.append("cdcf_internal_id")
            if "cdcf_internal_id" in primary:
                target_native = dict(cached_native_types)
                target_native[column] = _physical_union_native(
                    union_sql, source=new_source
                )
                identity_descriptors = {**cached_descriptors, column: old_source}
                _copy_rows_with_identity(
                    self.con,
                    table,
                    shadow,
                    target_types,
                    target_native,
                    key_columns=source_key_columns or (column,),
                    identity_descriptors=identity_descriptors,
                    changed_sql={column: expression},
                    union_columns=frozenset({column}),
                )
            else:
                select_expressions = [
                    expression if current_column == column else quote(current_column)
                    for current_column in target_columns
                ]
                self.con.execute(
                    f"INSERT INTO {quote(self.dataset)}.{quote(shadow)} "
                    f"({', '.join(quote(item) for item in target_columns)}) "
                    f"SELECT {', '.join(select_expressions)} FROM {table.qualified}"
                )
            self.con.execute(f"DROP TABLE {table.qualified}")
            from . import faults

            self._typed_swap_count += 1
            faults.maybe_crash("swap", self._typed_swap_count)
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(shadow)} RENAME TO {quote(name)}"
            )
        except Exception as exc:
            from . import faults

            if isinstance(exc, faults.InjectedFault):
                raise
            raise SchemaEvolutionRefused(
                f"typed UNION shadow conversion failed for {name}.{column}: {exc}", target=name
            ) from exc
        self.forget(name)
        table = self.get(name)
        table.key_columns = source_key_columns
        table.source_key_columns = source_key_columns
        table.primary_key_columns = primary
        table.internal_identity = primary == ("cdcf_internal_id",)
        table.source_descriptors = {**cached_descriptors, column: new_source}
        table.native_types = {
            **cached_native_types,
            column: _physical_union_native(union_sql, source=new_source),
        }
        self._persist_key_metadata(table)
        return table

    def _create(
        self, table: TableSchema, columns: dict[str, str], key_columns: tuple[str, ...]
    ) -> None:
        defs = ", ".join(f"{quote(col)} {ctype}" for col, ctype in columns.items())
        constraint = ""
        if self.constraints and key_columns and all(c in columns for c in key_columns):
            constraint = ", PRIMARY KEY (" + ", ".join(quote(c) for c in key_columns) + ")"
        try:
            self.con.execute(
                f"CREATE TABLE IF NOT EXISTS {table.qualified} ({defs}{constraint})"
            )
            table.constrained = bool(constraint)
        except Exception as exc:
            if not constraint:
                raise
            # A destination that cannot express the constraint must not block the
            # load; `_assert_identity_is_unique` becomes the enforcement instead.
            log.warning(
                "could not create %s with a PRIMARY KEY on %s (%s); falling back to a "
                "post-apply uniqueness assertion inside the commit group",
                table.name, key_columns, exc,
            )
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {table.qualified} ({defs})")
            table.constrained = False
        table.columns = dict(columns)
        table.raw_types = dict(columns)
        table.exists = True
        table.source_key_columns = tuple(key_columns)

    def drop(self, name: str) -> None:
        table = self.get(name)
        self.con.execute(f"DROP TABLE IF EXISTS {table.qualified}")
        self._delete_key_metadata(name)
        self.forget(name)

    def drop_column(self, name: str, column: str) -> None:
        """Physically remove a source-dropped column in the open transaction."""
        table = self.get(name)
        if not table.exists or column not in table.columns:
            return
        if column in table.key_columns:
            raise SchemaEvolutionRefused(
                f"cannot drop primary-key column {name}.{column}: the source did not "
                "provide a replacement identity, so the destination refuses to "
                "continue without a lossless row key",
                target=name,
            )
        try:
            self.con.execute(
                f"ALTER TABLE {table.qualified} DROP COLUMN {quote(column)}"
            )
        except Exception as exc:
            raise SchemaEvolutionRefused(
                f"cannot drop source column {name}.{column}: destination DDL failed, "
                "so the catalog baseline cannot be persisted",
                target=name,
            ) from exc
        table.columns.pop(column, None)
        table.raw_types.pop(column, None)
        table.key_columns = tuple(c for c in table.key_columns if c != column)
        table.source_key_columns = tuple(c for c in table.source_key_columns if c != column)
        self._persist_key_metadata(table)

    def rename_column(self, name: str, old: str | None, new: str | None) -> None:
        """Apply a true rename, including a late-arriving new-name row.

        The latter shape is possible because the catalog poll and the Debezium callback
        are independent.  Merging with ``COALESCE`` preserves old rows and any new-name
        values already applied, then the old physical column is removed.  All statements
        run in the applier's transaction, so no consumer observes the intermediate pair.
        """
        if not old or not new or old == new:
            return
        table = self.get(name)
        if not table.exists:
            return
        # A late Debezium row may already have caused the normal ensure path to add the
        # new-name column since this registry entry was cached. Re-read metadata before
        # deciding whether this is a physical RENAME or a merge.
        self._refresh(table)
        old_exists = old in table.columns
        new_exists = new in table.columns
        if old_exists and not new_exists:
            try:
                self.con.execute(
                    f"ALTER TABLE {table.qualified} RENAME COLUMN {quote(old)} "
                    f"TO {quote(new)}"
                )
            except Exception as exc:
                raise SchemaEvolutionRefused(
                    f"cannot rename source column {name}.{old} -> {new}: destination "
                    "DDL failed, so the catalog baseline cannot be persisted",
                    target=name,
                ) from exc
            table.columns[new] = table.columns.pop(old)
            table.raw_types[new] = table.raw_types.pop(old)
            if old in table.source_descriptors:
                table.source_descriptors[new] = table.source_descriptors.pop(old)
            if old in table.native_types:
                table.native_types[new] = table.native_types.pop(old)
        elif old_exists and new_exists:
            # Prefer the already-arriving new image; fall back to the old value for rows
            # that predate the catalog poll. This is the only safe merge for a rename
            # whose source values are allowed to be NULL.
            # A NULL in the new-name column is a real source value, not proof that the
            # new field was absent.  The presence journal is written with each CDC row
            # inside its commit group; only rows with no explicit new-name field fall
            # back to the old physical column.  Direct unit callers without the
            # journal retain the conservative old COALESCE behavior for compatibility.
            if CDCF_EVENT_ID in table.columns:
                self.con.execute(
                    f"UPDATE {table.qualified} AS t SET {quote(new)} = {quote(old)} "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {quote('_cdc_flight')}."
                    f"{quote('column_presence')} AS p "
                    f"WHERE p.target_dataset = ? AND p.target_table = ? "
                    f"AND p.event_id = t.{quote(CDCF_EVENT_ID)} "
                    f"AND p.column_name = ? AND p.present)",
                    [self.dataset, table.name, new],
                )
            else:
                self.con.execute(
                    f"UPDATE {table.qualified} SET {quote(new)} = "
                    f"COALESCE({quote(new)}, {quote(old)})"
                )
            key_was_renamed = old in table.key_columns
            if key_was_renamed and table.constrained:
                # DuckDB/MotherDuck do not support DROP CONSTRAINT for a primary key.
                # Rebuild the table inside the caller's transaction so the new identity
                # is validated before the old table is removed.  A duplicate new key
                # therefore raises while the old table still exists and the enclosing
                # commit group can roll back the whole schema change.
                new_key_columns = tuple(new if c == old else c for c in table.key_columns)
                self._rebuild_with_primary_key(
                    table,
                    drop_column=old,
                    key_columns=new_key_columns,
                )
            else:
                try:
                    self.con.execute(
                        f"ALTER TABLE {table.qualified} DROP COLUMN {quote(old)}"
                    )
                except Exception as exc:
                    raise SchemaEvolutionRefused(
                        f"cannot finish late rename {name}.{old} -> {new}: destination "
                        "DDL failed, so the catalog baseline cannot be persisted",
                        target=name,
                    ) from exc
                table.columns.pop(old, None)
                table.raw_types.pop(old, None)
            if CDCF_EVENT_ID in table.columns:
                self.con.execute(
                    f"DELETE FROM {quote('_cdc_flight')}."
                    f"{quote('column_presence')} "
                    "WHERE target_dataset = ? AND target_table = ? AND column_name = ?",
                    [self.dataset, table.name, new],
                )
        # If only the new name remains, an earlier group already performed the
        # idempotent late-rename merge. If neither remains, the source relation may have
        # produced no row carrying either shape; the catalog action remains harmless.
        table.key_columns = tuple(new if c == old else c for c in table.key_columns)
        table.source_key_columns = tuple(
            new if c == old else c for c in table.source_key_columns
        )
        self._persist_key_metadata(table)

    def _rebuild_with_primary_key(
        self,
        table: TableSchema,
        *,
        drop_column: str,
        key_columns: tuple[str, ...],
    ) -> None:
        """Recreate a constrained table when its identity column is renamed.

        DuckDB has no ``DROP CONSTRAINT`` implementation.  The temporary table is
        created with the replacement key before the source table is dropped, so a
        uniqueness failure cannot leave a destination with neither identity nor data.
        The caller owns the transaction; the table-cache update happens only after all
        physical statements succeed.
        """
        columns = {
            column: type_name
            for column, type_name in table.columns.items()
            if column != drop_column
        }
        raw_types = {
            column: table.raw_types.get(column, type_name)
            for column, type_name in columns.items()
        }
        if not key_columns or any(column not in columns for column in key_columns):
            raise SchemaEvolutionRefused(
                f"cannot rebind primary-key identity {drop_column!r} on {table.name}: "
                "the replacement key is not present in the destination schema",
                target=table.name,
            )
        temp_name = f"{table.name}__cdcf_pk_rebind"
        definitions = ", ".join(
            f"{quote(column)} {raw_types[column]}" for column in columns
        )
        key_sql = ", ".join(quote(column) for column in key_columns)
        try:
            self.con.execute(
                f"CREATE TABLE {quote(self.dataset)}.{quote(temp_name)} "
                f"({definitions}, PRIMARY KEY ({key_sql}))"
            )
            column_sql = ", ".join(quote(column) for column in columns)
            self.con.execute(
                f"INSERT INTO {quote(self.dataset)}.{quote(temp_name)} ({column_sql}) "
                f"SELECT {column_sql} FROM {table.qualified}"
            )
            self.con.execute(f"DROP TABLE {table.qualified}")
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(temp_name)} "
                f"RENAME TO {quote(table.name)}"
            )
        except Exception as exc:
            try:
                self.con.execute(
                    f"DROP TABLE IF EXISTS {quote(self.dataset)}.{quote(temp_name)}"
                )
            except Exception:  # pragma: no cover - the caller rolls back the transaction
                log.debug("could not clean up failed PK-rebind table", exc_info=True)
            raise SchemaEvolutionRefused(
                f"cannot rebind primary-key identity {drop_column!r} -> "
                f"{key_columns!r} on {table.name}: the destination identity is not "
                "unique or the table could not be rebuilt",
                target=table.name,
            ) from exc
        table.columns = columns
        table.raw_types = raw_types
        table.key_columns = key_columns
        table.source_key_columns = key_columns
        table.constrained = True

    def backfill_columns(
        self,
        name: str,
        *,
        key_columns: tuple[str, ...],
        value_columns: tuple[str, ...],
        rows: list[tuple],
    ) -> None:
        """Copy current source values into newly added columns in this transaction.

        PostgreSQL evaluates an ADD COLUMN default for rows already in the source.
        The CDC stream only carries future row changes, so a destination-side ADD alone
        would leave those rows NULL forever.  The catalog fence supplies a stable key
        and source read; this method makes the existing destination rows agree before
        the commit becomes durable.  It deliberately uses UPDATE, not a delete/insert
        merge, so the operation remains safe after the schema DDL has run in the same
        transaction on DuckDB/MotherDuck.
        """
        if not key_columns or not value_columns or not rows:
            return
        table = self.get(name)
        if not table.exists:
            return
        key_columns = tuple(column for column in key_columns if column in table.columns)
        value_columns = tuple(column for column in value_columns if column in table.columns)
        if not key_columns or not value_columns:
            return
        value_count = len(value_columns)
        key_count = len(key_columns)
        for row in rows:
            keys = row[:key_count]
            values = row[key_count : key_count + value_count]
            set_parts: list[str] = []
            params: list[Any] = []
            for column, value in zip(value_columns, values, strict=True):
                expression, bound = _typed_assignment(table, column, value)
                set_parts.append(f"{quote(column)} = {expression}")
                params.extend(bound)
            where_parts: list[str] = []
            for column, value in zip(key_columns, keys, strict=True):
                expression, bound = _typed_assignment(table, column, value)
                where_parts.append(
                    f"{quote(column)} IS NOT DISTINCT FROM {expression}"
                )
                params.extend(bound)
            self.con.execute(
                f"UPDATE {table.qualified} SET {', '.join(set_parts)} "
                f"WHERE {' AND '.join(where_parts)}",
                params,
            )

    def backfill_constant_columns(
        self,
        name: str,
        *,
        value_columns: tuple[str, ...],
        rows: list[tuple],
    ) -> None:
        """Backfill a keyless destination when the source values are uniform.

        A source table without a primary key has no durable identity that can match a
        current source row to a historical ``cdcf_event_id`` row.  Applying a value
        that differs by source row would therefore be an invented mapping.  PostgreSQL
        ADD COLUMN normally gives every existing row one default (or NULL), so that
        common case has a safe all-rows operation; a non-uniform source is rejected by
        the caller before this update runs.
        """
        if not value_columns:
            return
        table = self.get(name)
        if not table.exists:
            return
        value_columns = tuple(column for column in value_columns if column in table.columns)
        if not value_columns:
            return
        if not rows:
            destination_rows = self.con.execute(
                f"SELECT count(*) FROM {table.qualified}"
            ).fetchone()[0]
            if destination_rows:
                raise SchemaBackfillRefused(
                    f"cannot backfill keyless table {name}: the source returned no "
                    f"rows for an added column while {destination_rows} destination "
                    "changelog rows already exist; no stable identity or source value "
                    "proves what those rows should contain"
                )
            return
        values = tuple(rows[0][: len(value_columns)])
        if any(tuple(row[: len(value_columns)]) != values for row in rows[1:]):
            raise SchemaBackfillRefused(
                f"cannot backfill keyless table {name}: added-column values are not "
                "uniform and the source has no stable row identity"
            )
        set_parts: list[str] = []
        params: list[Any] = []
        for column, value in zip(value_columns, values, strict=True):
            expression, bound = _typed_assignment(table, column, value)
            set_parts.append(f"{quote(column)} = {expression}")
            params.extend(bound)
        self.con.execute(
            f"UPDATE {table.qualified} SET {', '.join(set_parts)}", params
        )


#: Destination types the widening lattice actually understands. Anything else is
#: reported as VARCHAR by `_normalise_type` and must never be ALTERed on that basis.

_RECOGNISED_TYPES = frozenset(
    {
        BOOLEAN, BIGINT, DOUBLE, JSON_T, "VARIANT", VARCHAR,
        "TEXT", "STRING", "INT64", "HUGEINT", "INTEGER", "INT", "INT32", "SMALLINT",
        "FLOAT", "REAL", "FLOAT8",
    }
)


def assert_identity_is_unique(con, table: TableSchema) -> None:
    """Fallback for a destination that cannot express the PRIMARY KEY (Opus M-2).

    Runs **inside** the commit group's transaction, so a violation rolls the whole
    group back and the events replay. Only used when `_create` could not attach the
    constraint; with the constraint in place the destination raises on the INSERT
    itself and this is never called.
    """
    if table.constrained or not table.key_columns:
        return
    cols = ", ".join(quote(c) for c in table.key_columns)
    duplicates = con.execute(
        f"SELECT count(*) FROM (SELECT {cols} FROM {table.qualified} "
        f"GROUP BY {cols} HAVING count(*) > 1)"
    ).fetchone()[0]
    if duplicates:
        raise DestinationIdentityCollision(
            f"{table.qualified} holds {duplicates} identity value(s) more than once on "
            f"({cols}). Exactly-once delivery means one row per identity, so this commit "
            "group is rolled back (ADR 0001 §15/A21)."
        )


def _normalise_type(duckdb_type: str) -> str:
    upper = str(duckdb_type).upper()
    if upper.startswith("VARCHAR") or upper in ("TEXT", "STRING"):
        return VARCHAR
    if upper in ("SMALLINT", "INT16"):
        return "SMALLINT"
    if upper in ("INTEGER", "INT", "INT32"):
        return "INTEGER"
    if upper in ("BIGINT", "INT64", "HUGEINT"):
        return BIGINT
    if upper in ("DOUBLE", "FLOAT", "REAL", "FLOAT8"):
        return upper if upper != "REAL" else "FLOAT"
    if upper == "BOOLEAN":
        return BOOLEAN
    if upper in {"BLOB", "BYTEA"}:
        return "BLOB"
    if upper == "JSON":
        return JSON_T
    if upper == "VARIANT":
        return "VARIANT"
    if upper in {"DATE", "TIME", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "TIME WITH TIME ZONE", "TIMETZ", "INTERVAL", "UUID"}:
        return upper
    if upper.startswith(("STRUCT(", "MAP(", "UNION(", "ENUM(", "LIST(", "DECIMAL(", "BIGNUM")) or upper.endswith("[]"):
        return upper
    return VARCHAR


def _union_members(physical: str) -> list[tuple[str, str]]:
    """Parse the top-level member declaration exposed by DuckDB metadata."""
    text = physical.strip()
    if text.upper().startswith("UNION(") and text.endswith(")"):
        text = text[text.find("(") + 1 : -1]
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    if text[start:].strip():
        parts.append(text[start:])
    result = []
    for part in parts:
        name, separator, type_name = part.strip().partition(" ")
        if separator and name:
            result.append((name.strip('"'), type_name.strip()))
    return result


def _is_top_level_union(physical: str) -> bool:
    text = str(physical).strip()
    if not text.upper().startswith("UNION("):
        return False
    depth = 0
    opening = text.find("(")
    for index in range(opening, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def _is_numeric_inner_union(physical: str) -> bool:
    """Whether a physical UNION is the numeric finite/special value encoding."""
    if not _is_top_level_union(physical):
        return False
    members = _union_members(physical)
    return len(members) == 2 and {name.lower() for name, _ in members} == {
        "finite",
        "special",
    }


def _union_member_names(physical: str) -> set[str]:
    return {name.lower() for name, _ in _union_members(physical)}


def _type_sql_equal(left: str, right: str) -> bool:
    """Compare physical/member SQL without treating harmless whitespace as drift."""
    import re

    def compact(value: str) -> str:
        normalized = re.sub(r"\s+", " ", str(value).strip()).upper()
        for spelling, canonical in (
            ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"),
            ("TIME WITH TIME ZONE", "TIMETZ"),
            ("CHARACTER VARYING", "VARCHAR"),
            ("DOUBLE PRECISION", "DOUBLE"),
        ):
            normalized = normalized.replace(spelling, canonical)
        return normalized.replace(" ", "")

    return compact(left) == compact(right)


def _json_key_transition(current: str, desired: str) -> bool:
    """Whether a key-status change is a lossless JSONB representation swap.

    A composite/array/map key can carry the JSONB field several levels down.  In
    that case the physical declarations are STRUCT/LIST/MAP expressions rather
    than the scalar ``JSON``/``VARIANT`` pair, but the only representation change
    permitted here is still the recursive JSONB key representation change.
    """
    current_name = str(current).strip().upper()
    desired_name = str(desired).strip().upper()
    if current_name == desired_name:
        return False
    if {current_name, desired_name} == {"JSON", "VARIANT"}:
        return True
    return (
        ("VARIANT" in current_name and "JSON" in desired_name)
        or ("JSON" in current_name and "VARIANT" in desired_name)
    )


def _lossless_numeric_supertype(current: str, desired: str) -> bool:
    """Allow a legacy inferred integer column to adopt its narrower catalog fact."""
    ranks = {"SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "HUGEINT": 4}
    current_name = _normalise_type(current)
    desired_name = _normalise_type(desired)
    return (
        current_name in ranks
        and desired_name in ranks
        and ranks[current_name] >= ranks[desired_name]
    )

def _physical_union_native(physical: str, *, source=None):
    """Rehydrate a cached native UNION from the destination declaration."""
    from .typed_types import NativeMember, NativeType

    members = tuple(
        NativeMember(
            name,
            NativeType(_normalise_type(type_name), type_name),
        )
        for name, type_name in _union_members(physical)
    )
    return NativeType("UNION", physical, source=source, members=members, indexable=False)


# --------------------------------------------------------------------------- #
# the two statements an apply is made of
# --------------------------------------------------------------------------- #


def _copy_rows_with_identity(*args, **kwargs):
    from .typed_materialization import _copy_rows_with_identity as copy_rows
    return copy_rows(*args, **kwargs)


def _typed_assignment(*args, **kwargs):
    from .typed_materialization import _typed_assignment as assignment
    return assignment(*args, **kwargs)


__all__ = [
    "SchemaRegistry",
    "TableSchema",
    "assert_identity_is_unique",
    "_is_numeric_inner_union",
    "_is_top_level_union",
    "_normalise_type",
    "_physical_union_native",
    "_type_sql_equal",
    "_union_member_names",
    "_union_members",
    "widen",
]

