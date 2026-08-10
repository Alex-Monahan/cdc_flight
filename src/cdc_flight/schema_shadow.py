"""Typed shadow-swap ownership for source-type and key-shape changes."""

from __future__ import annotations

from typing import Any

from . import faults
from .errors import SchemaEvolutionRefused
from .naming import quote
from .schema_ddl import (
    _is_numeric_inner_union,
    _is_top_level_union,
    _json_key_transition,
    _lossless_numeric_supertype,
    _physical_union_native,
    _type_sql_equal,
    _union_member_names,
    _union_members,
)

OWNER = "typed-shadow"


class ShadowOwner:
    """Mixin containing the only two operations allowed to replace a live table."""

    def _rebind_key_identity(
        self,
        table: Any,
        *,
        key_columns: tuple[str, ...],
        resolved: dict[str, Any],
        descriptors: dict[str, Any],
    ) -> tuple[Any, bool]:
        """Rebuild a destination whose source key tuple or JSON representation changed."""
        from .typed_materialization import _copy_rows_with_identity
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
            # A source-type UNION is durable value representation.  A key rebind
            # changes identity enforcement, not the values represented by members.
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
        constraint_sql = (
            ""
            if not self.constraints
            else ", PRIMARY KEY ("
            + ", ".join(quote(column) for column in primary)
            + ")"
        )
        if not indexable:
            target_types["cdcf_internal_id"] = "VARCHAR"

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
            changed_python: set[str] = set()
            for column in target_types:
                if column == "cdcf_internal_id":
                    continue
                current_type = old_raw_types.get(column, target_types[column])
                desired_type = target_types[column]
                if _type_sql_equal(current_type, desired_type):
                    continue
                if _json_key_transition(current_type, desired_type) or _lossless_numeric_supertype(
                    current_type, desired_type
                ):
                    changed_python.add(column)
                else:
                    raise SchemaEvolutionRefused(
                        f"cannot rebind {table.name}.{column}: destination type "
                        f"{current_type} does not match {desired_type}; a typed "
                        "shadow conversion is required",
                        target=table.name,
                        refusal_origin="schema_shadow",
                    )
            _copy_rows_with_identity(
                self.con,
                table,
                shadow,
                target_types,
                target_native,
                key_columns=key_columns,
                descriptors={**old_descriptors, **descriptors},
                changed_python=frozenset(changed_python),
            )
            self.con.execute(f"DROP TABLE {table.qualified}")
            self._typed_swap_count += 1
            faults.maybe_crash("swap", self._typed_swap_count)
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(shadow)} "
                f"RENAME TO {quote(table.name)}"
            )
        except Exception as exc:
            if isinstance(exc, faults.InjectedFault):
                raise
            raise SchemaEvolutionRefused(
                f"typed key shadow conversion failed for {table.name}: {exc}",
                target=table.name,
                refusal_origin="schema_shadow",
            ) from exc

        self.forget(table.name)
        refreshed = self.get(table.name)
        refreshed.key_columns = key_columns
        refreshed.source_key_columns = key_columns
        refreshed.primary_key_columns = primary
        refreshed.internal_identity = not indexable
        refreshed.constrained = bool(self.constraints and primary)
        refreshed.source_descriptors = {**old_descriptors, **descriptors}
        refreshed.native_types = {**old_native_types, **target_native}
        self._persist_key_metadata(refreshed)
        return refreshed, False

    def convert_column_to_union(
        self,
        name: str,
        column: str,
        old_descriptor: Any,
        new_descriptor: Any,
    ) -> Any:
        """Convert one source column through the sole typed shadow-swap path."""
        from .typed_materialization import _copy_rows_with_identity
        from .typed_types import SourceTypeDescriptor, native_type, union_member_name

        table = self.get(name)
        if not table.exists:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: destination table does not exist",
                target=name,
                refusal_origin="schema_shadow",
            )
        if table.internal_identity and not table.key_metadata_loaded:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: the destination has an internal "
                "identity but no durable source-key metadata; automatic resnapshot "
                "must establish the catalog-authoritative key before typed evolution",
                target=name,
                refusal_origin="schema_shadow",
            )
        old_source = (
            old_descriptor
            if isinstance(old_descriptor, SourceTypeDescriptor)
            else SourceTypeDescriptor.from_dict(old_descriptor)
        )
        new_source = (
            new_descriptor
            if isinstance(new_descriptor, SourceTypeDescriptor)
            else SourceTypeDescriptor.from_dict(new_descriptor)
        )
        source_key = column in table.source_key_columns or column in table.key_columns
        old_native = native_type(old_source, for_key=source_key)
        new_native = native_type(new_source, for_key=source_key)
        source_key_columns = tuple(table.source_key_columns or table.key_columns)
        cached_descriptors = dict(table.source_descriptors)
        cached_native_types = dict(table.native_types)
        physical = str(
            table.raw_types.get(column, table.columns.get(column, old_native.sql))
        )

        current_members = _union_member_names(physical)
        wanted_name = union_member_name(new_source)
        if wanted_name in current_members:
            declared = dict(_union_members(physical)).get(wanted_name)
            if declared is None or not _type_sql_equal(declared, new_native.sql):
                raise SchemaEvolutionRefused(
                    f"cannot reuse UNION member {wanted_name} on {name}.{column}: "
                    f"physical type {declared!r} disagrees with descriptor type "
                    f"{new_native.sql!r}",
                    target=name,
                    refusal_origin="schema_shadow",
                )
            table.source_descriptors[column] = new_source
            table.native_types[column] = _physical_union_native(
                physical, source=new_source
            )
            self._persist_key_metadata(table)
            return table

        if _is_numeric_inner_union(physical):
            if old_native.kind != "NUMERIC_UNION":
                raise SchemaEvolutionRefused(
                    f"cannot convert {name}.{column}: physical numeric UNION does "
                    "not match the old source descriptor",
                    target=name,
                    refusal_origin="schema_shadow",
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
            member_sql = _union_members(physical)
            members = [
                (member_name, member_type) for member_name, member_type in member_sql
            ]
            members.append((wanted_name, new_native.sql))
            union_sql = "UNION(" + ",".join(
                f"{member} {sql}" for member, sql in members
            ) + ")"
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
        if column in primary:
            primary = ("cdcf_internal_id",)
            if "cdcf_internal_id" not in table.raw_types:
                definitions.append('"cdcf_internal_id" VARCHAR DEFAULT uuid()')
                target_types["cdcf_internal_id"] = "VARCHAR"
        if not primary:
            raise SchemaEvolutionRefused(
                f"cannot convert {name}.{column}: no destination identity is available",
                target=name,
                refusal_origin="schema_shadow",
            )
        ddl = (
            f"CREATE TABLE {quote(self.dataset)}.{quote(shadow)} "
            f"({', '.join(definitions)}, PRIMARY KEY "
            f"({', '.join(quote(item) for item in primary)}))"
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
                # Every row is encoded in the current canonical form; carrying an
                # existing ID would recreate the type-changed-key orphan.
                current_descriptors = {**cached_descriptors, column: new_source}
                _copy_rows_with_identity(
                    self.con,
                    table,
                    shadow,
                    target_types,
                    target_native,
                    key_columns=source_key_columns or (column,),
                    descriptors=current_descriptors,
                    changed_sql={column: expression},
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
            self._typed_swap_count += 1
            faults.maybe_crash("swap", self._typed_swap_count)
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(shadow)} "
                f"RENAME TO {quote(name)}"
            )
        except Exception as exc:
            if isinstance(exc, faults.InjectedFault):
                raise
            raise SchemaEvolutionRefused(
                f"typed UNION shadow conversion failed for {name}.{column}: {exc}",
                target=name,
                refusal_origin="schema_shadow",
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

__all__ = ["OWNER", "ShadowOwner"]
