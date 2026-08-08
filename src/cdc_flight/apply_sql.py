"""Turning change events into SQL, inside the caller's transaction (ADR 0001 D1).

Every function here executes on the applier's single connection and **never**
opens or closes a transaction: the commit group owns that (principle 4). The
apply model is one mechanism for both table shapes:

| table | identity key | effect of an event |
|---|---|---|
| has a primary key (Debezium emits a message key) | the PK columns | delete every touched key, then insert the group's final row per key |
| no primary key (`key() is null`) | `cdcf_event_id` | delete that event id if present, then insert - so a replayed event cannot duplicate and two byte-identical source rows stay two rows |

"Delete every touched key, then insert the final row" is what makes a **primary
key update** correct without a special case (rubric 1.4): the old key is in the
touched set because it is the `before` image, the new key is in it because it is
the `after` image, so the row is deleted under the old key and inserted under the
new one inside the same commit group. No consumer can ever see it under both.

The keyless rule is what makes rubric 1.2 reachable. `cdcf_event_id` is
`"<event lsn>:<source.txId>:<transaction.total_order>"` - the connector's own
bookkeeping, not ours, so a replayed event recomputes the *same* id, while two
genuinely identical source rows are two different events and keep two different
ids. Nothing that deduplicates by row *content* can do both. (It is the **event's
own** LSN, not the transaction's commit LSN - ADR §15/A3; this docstring said
`commit_lsn` long after the code changed, Opus MINOR-14.)

Every table created here also carries a destination-side `PRIMARY KEY` on those
identity columns, so a duplicate identity is a failed transaction rather than a
silently corrupted table (Opus M-2).
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from .errors import (
    DestinationIdentityCollision,
    SchemaBackfillRefused,
    SchemaEvolutionRefused,
)
from .naming import (
    CDCF_COMMIT_ID,
    CDCF_EVENT_ID,
    CDCF_TOTAL_ORDER,
    quote,
)

log = logging.getLogger("cdc_flight.apply_sql")

#: How many key tuples go into one `DELETE … EXISTS (VALUES …)` statement.
DELETE_CHUNK = 2000
#: Upper bound on bound parameters per statement. Rows per statement is derived
#: from this and the column count, so a 40-column table does not build a
#: statement 40x larger than a 1-column one.
MAX_PARAMS_PER_STATEMENT = 40_000
#: Never build a statement with more rows than this, whatever the column count.
MAX_ROWS_PER_STATEMENT = 5_000
#: Rows per registered Arrow batch. Bounds the peak Python->Arrow copy, not the
#: transaction: the whole group is still one COMMIT.
ARROW_CHUNK = 100_000

BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR = "BOOLEAN", "BIGINT", "DOUBLE", "JSON", "VARCHAR"


def sql_type(value: Any) -> str | None:
    """DuckDB type for one JSON-decoded value; None when it tells us nothing."""
    if value is None:
        return None
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return BIGINT
    if isinstance(value, float):
        return DOUBLE
    if isinstance(value, (dict, list)):
        return JSON_T
    return VARCHAR


def widen(current: str | None, incoming: str | None) -> str | None:
    """Least type that holds both. Deliberately conservative: anything ambiguous
    becomes VARCHAR, because losing a value is worse than losing a type.
    Rubric 2.5 (MotherDuck UNION types) replaces this with something better."""
    if current is None:
        return incoming
    if incoming is None or current == incoming:
        return current
    pair = {current, incoming}
    if pair == {BIGINT, DOUBLE}:
        return DOUBLE
    return VARCHAR


def bind(value: Any, column_type: str) -> Any:
    """Coerce a JSON-decoded value for a bound parameter of `column_type`."""
    if value is None:
        return None
    # Typed UNION values are lowered by `insert_typed_rows`, which needs the member
    # name to construct `union_value(member := ?)`; a plain bind remains useful for
    # destination probes and keeps the underlying value available there.
    try:
        from .typed_types import UnionValue

        if isinstance(value, UnionValue):
            value = value.value
    except ImportError:  # pragma: no cover - import cycle during interpreter startup
        pass
    if column_type == JSON_T:
        return value if isinstance(value, str) else json.dumps(value, default=str)
    if column_type == VARCHAR:
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return str(value)
    if column_type == DOUBLE:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("nan", "+nan", "-nan"):
                return math.nan
            if lowered in ("infinity", "inf", "+infinity", "+inf"):
                return math.inf
            if lowered in ("-infinity", "-inf"):
                return -math.inf
            try:
                return float(value)
            except ValueError:
                return None
        return float(value)
    if column_type in {"SMALLINT", "INTEGER", "INT", "INT32", BIGINT, "INT64", "HUGEINT"}:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return int(value)
    if column_type == BOOLEAN:
        if isinstance(value, str):
            return value.strip().lower() in ("true", "t", "1")
        return bool(value)
    if column_type in {"BLOB", "BYTEA"}:
        return value if isinstance(value, (bytes, bytearray)) else str(value).encode()
    return value  # pragma: no cover


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

    def get(self, name: str) -> TableSchema:
        table = self._tables.get(name)
        if table is None:
            table = TableSchema(name, self.dataset)
            self._load(table)
            self._tables[name] = table
        return table

    def forget(self, name: str) -> None:
        self._tables.pop(name, None)

    def _load(self, table: TableSchema) -> None:
        self._refresh(table)

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
        resolved: dict[str, NativeType] = {}
        descriptors: dict[str, SourceTypeDescriptor] = {}
        physical_columns: dict[str, str] = {}
        for column, descriptor in columns.items():
            if isinstance(descriptor, str):
                physical_columns[column] = descriptor
                continue
            target = native_type(descriptor)
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
                    if not _type_sql_equal(existing_type, physical_type) and not str(
                        existing_type
                    ).upper().startswith("UNION("):
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
            if key_columns:
                table.key_columns = key_columns
                table.source_key_columns = key_columns
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
        return table, True

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
        old_source = old_descriptor if isinstance(old_descriptor, SourceTypeDescriptor) else SourceTypeDescriptor.from_dict(old_descriptor)
        new_source = new_descriptor if isinstance(new_descriptor, SourceTypeDescriptor) else SourceTypeDescriptor.from_dict(new_descriptor)
        old_native = native_type(old_source)
        new_native = native_type(new_source)
        source_key_columns = tuple(table.source_key_columns or table.key_columns)
        cached_descriptors = dict(table.source_descriptors)
        cached_native_types = dict(table.native_types)
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
            select_expressions: list[str] = []
            for current_column in target_columns:
                if current_column == column:
                    select_expressions.append(expression)
                elif current_column == "cdcf_internal_id" and current_column not in table.raw_types:
                    select_expressions.append(
                        _identity_expression(
                            table,
                            table.key_columns or (column,),
                        )
                    )
                else:
                    select_expressions.append(quote(current_column))
            self.con.execute(
                f"INSERT INTO {quote(self.dataset)}.{quote(shadow)} "
                f"({', '.join(quote(item) for item in target_columns)}) "
                f"SELECT {', '.join(select_expressions)} FROM {table.qualified}"
            )
            self.con.execute(f"DROP TABLE {table.qualified}")
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(shadow)} RENAME TO {quote(name)}"
            )
        except Exception as exc:
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
        BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR,
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


def _identity_expression(table: TableSchema, columns: tuple[str, ...]) -> str:
    """Build a deterministic, length-prefixed typed identity serialization.

    The internal identity is a uniqueness key, not a digest.  Each component carries
    its source fingerprint, NULL marker, UNION tag where applicable, and the byte
    length of its rendered native value.  Length prefixes make concatenation
    unambiguous and avoid the old delimiter/hash shortcut while retaining a SQL-only
    shadow copy.
    """
    expressions: list[str] = []
    for column in columns:
        descriptor = table.source_descriptors.get(column)
        fingerprint = descriptor.fingerprint if descriptor is not None else "legacy"
        fingerprint_sql = "'" + fingerprint.replace("'", "''") + "'"
        value_sql = f"CAST({quote(column)} AS VARCHAR)"
        native = table.native_types.get(column)
        if native is not None and native.kind in {"UNION", "NUMERIC_UNION"}:
            tag_sql = f"CAST(union_tag({quote(column)}) AS VARCHAR)"
        else:
            tag_sql = "'value'"
        payload = (
            f"CASE WHEN {quote(column)} IS NULL THEN {fingerprint_sql} || ':NULL' "
            f"ELSE {fingerprint_sql} || ':' || {tag_sql} || ':' || "
            f"CAST(length({value_sql}) AS VARCHAR) || ':' || {value_sql} END"
        )
        expressions.append(
            f"CAST(length({payload}) AS VARCHAR) || ':' || {payload}"
        )
    return " || ".join(expressions)


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
def delete_keys(con, table: TableSchema, key_columns: tuple[str, ...], keys: list[tuple]) -> None:
    if not keys:
        return
    cols = ", ".join(quote(c) for c in key_columns)
    predicate = " AND ".join(
        f"t.{quote(c)} IS NOT DISTINCT FROM v.{quote(c)}" for c in key_columns
    )
    if len(keys) > DELETE_CHUNK:
        # One anti-join instead of hundreds of table scans. The staging table is
        # temporary and therefore invisible to any other connection, so it cannot
        # weaken the atomicity guarantee the commit group exists to provide.
        staging = "_cdcf_delete_keys"
        types = [table.raw_types.get(c, table.columns.get(c, VARCHAR)) for c in key_columns]
        defs = ", ".join(f"{quote(c)} {t}" for c, t in zip(key_columns, types, strict=True))
        con.execute(f"CREATE OR REPLACE TEMP TABLE {staging} ({defs})")
        if table.native_types and any(column in table.native_types for column in key_columns):
            insert_typed_rows(
                con,
                table,
                list(key_columns),
                [list(k) for k in keys],
                [table.native_types.get(column) for column in key_columns],
                target=staging,
            )
        else:
            bulk_insert(con, staging, list(key_columns), [list(k) for k in keys], types)
        con.execute(
            f"DELETE FROM {table.qualified} AS t WHERE EXISTS "
            f"(SELECT 1 FROM {staging} AS v WHERE {predicate})"
        )
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        return
    for start in range(0, len(keys), DELETE_CHUNK):
        chunk = keys[start : start + DELETE_CHUNK]
        value_sql: list[str] = []
        params: list[Any] = []
        for key in chunk:
            expressions: list[str] = []
            for column, value in zip(key_columns, key, strict=True):
                expression, bound = _key_parameter(value, table, column)
                expressions.append(expression)
                params.extend(bound)
            value_sql.append("(" + ", ".join(expressions) + ")")
        placeholders = ", ".join(value_sql)
        con.execute(
            f"DELETE FROM {table.qualified} AS t WHERE EXISTS "
            f"(SELECT 1 FROM (VALUES {placeholders}) AS v({cols}) WHERE {predicate})",
            params,
        )


def _key_parameter(value: Any, table: TableSchema, column: str) -> tuple[str, list[Any]]:
    native = table.native_types.get(column)
    if native is not None:
        return _typed_parameter(value, native)
    return "?", [value]


def rows_per_statement(n_columns: int) -> int:
    if n_columns <= 0:  # pragma: no cover - defensive
        return MAX_ROWS_PER_STATEMENT
    return max(1, min(MAX_ROWS_PER_STATEMENT, MAX_PARAMS_PER_STATEMENT // n_columns))


def _arrow_type(sql_type: str):
    import pyarrow as pa

    return {
        BIGINT: pa.int64(),
        DOUBLE: pa.float64(),
        BOOLEAN: pa.bool_(),
    }.get(sql_type, pa.string())


def bulk_insert(
    con,
    target: str,
    columns: list[str],
    rows: list[list],
    types: list[str] | None = None,
    *,
    replace: bool = False,
) -> None:
    """Insert many rows through a registered Arrow table.

    MEASURED, 2026-07-30, 200 000 rows x 19 columns into local DuckDB inside one
    transaction:

    | strategy | time |
    |---|---|
    | `con.executemany(INSERT … VALUES (?,…))` | **410 s** |
    | chunked multi-row `INSERT … VALUES (…),(…),…` | **> 7 min**, abandoned |
    | register an Arrow table + `INSERT … SELECT` | **1.37 s** |

    and against MotherDuck, 1 500 rows: `executemany` 27.9 s (a network round trip
    *per row*), multi-row `VALUES` 0.65 s, Arrow 1.87 s.

    So Arrow is the only strategy that is fast at both ends, and `executemany` -
    the obvious way to write this - is 300x slower than it looks even locally.
    That is what turned one 200 000-row Postgres transaction into a commit group
    that could not finish inside the slow test's 300 s deadline.

    `pyarrow` is a hard dependency for this reason; if it is somehow missing the
    code falls back to `executemany` and logs, because a slow apply is better
    than a failed one.
    """
    if not rows:
        return
    collist = ", ".join(quote(c) for c in columns)
    column_types = types or [VARCHAR] * len(columns)
    verb = "INSERT OR REPLACE" if replace else "INSERT"
    try:
        import pyarrow as pa
    except ImportError:  # pragma: no cover - pyarrow is a declared dependency
        log.warning("pyarrow is unavailable; falling back to a slow row-at-a-time insert")
        placeholders = ", ".join("?" for _ in columns)
        con.executemany(
            f"{verb} INTO {target} ({collist}) VALUES ({placeholders})", rows
        )
        return

    view = "cdcf_bulk_rows"
    for start in range(0, len(rows), ARROW_CHUNK):
        batch = rows[start : start + ARROW_CHUNK]
        arrays = {}
        for index, column in enumerate(columns):
            values = [row[index] for row in batch]
            try:
                arrays[column] = pa.array(values, type=_arrow_type(column_types[index]))
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError) as exc:
                # A typed column must never silently become text.  The caller can
                # still explicitly request VARCHAR for an obscure source type, but a
                # native matrix row failing Arrow is a hard error with its column
                # context.  The old string retry was the 2.4 data-loss path.
                raise ValueError(
                    f"cannot build Arrow values for destination column {column} "
                    f"declared {column_types[index]!r}: {exc}"
                ) from exc
        table = pa.table(arrays)
        con.register(view, table)
        try:
            con.execute(f"{verb} INTO {target} ({collist}) SELECT * FROM {view}")
        finally:
            con.unregister(view)


def insert_rows(
    con, table: TableSchema, columns: list[str], rows: list[list]
) -> None:
    if table.native_types:
        insert_typed_rows(
            con,
            table,
            columns,
            rows,
            [table.native_types.get(column) for column in columns],
        )
        return
    bulk_insert(
        con,
        table.qualified,
        columns,
        rows,
        [table.columns.get(c, VARCHAR) for c in columns],
    )


def insert_typed_rows(
    con,
    table: TableSchema,
    columns: list[str],
    rows: list[list],
    native_types: list[Any],
    *,
    target: str | None = None,
) -> None:
    """Insert rows whose values carry explicit native/UNION semantics.

    Arrow has no portable representation for DuckDB's tagged UNION.  Such rows use
    generated parameterized SQL with ``union_value`` expressions; scalar and nested
    non-UNION rows use the same statement shape, and bounded multi-row statements
    keep the encoder and physical declaration in lockstep without a network round
    trip per row.  This path is used for typed source rows only.  The legacy untyped
    path above remains for compatibility with old callers and does not participate in
    2.4/2.5 schema creation.
    """
    if not rows:
        return
    from .typed_types import NativeType, UnionValue

    target = table.qualified if target is None else target
    collist = ", ".join(quote(column) for column in columns)
    value_rows: list[str] = []
    batch_params: list[Any] = []

    def flush() -> None:
        if not value_rows:
            return
        con.execute(
            f"INSERT INTO {target} ({collist}) VALUES {', '.join(value_rows)}",
            batch_params,
        )
        value_rows.clear()
        batch_params.clear()

    for row in rows:
        expressions: list[str] = []
        row_params: list[Any] = []
        for value, native in zip(row, native_types, strict=True):
            native = native if isinstance(native, NativeType) else None
            value = _prepare_typed_value(value, native)
            expression, bound = _typed_parameter(value, native)
            expressions.append(expression)
            row_params.extend(bound)
        if any(_contains_union(parameter, UnionValue) for parameter in row_params):
            raise ValueError(
                f"typed parameter escaped UNION lowering for columns {columns!r}: "
                f"{row_params!r}"
            )
        if value_rows and (
            len(value_rows) >= MAX_ROWS_PER_STATEMENT
            or len(batch_params) + len(row_params) > MAX_PARAMS_PER_STATEMENT
        ):
            flush()
        value_rows.append("(" + ", ".join(expressions) + ")")
        batch_params.extend(row_params)
    flush()


def _typed_parameter(value: Any, native: Any) -> tuple[str, list[Any]]:
    from .typed_types import NativeType, UnionValue

    if value is None:
        return "NULL", []
    if native is None:
        return "?", [value]
    if isinstance(value, UnionValue):
        member_native = _union_member_native(native, value.member)
        if value.native is not None and (
            member_native is None
            or _type_sql_equal(member_native.sql, value.native.sql)
        ):
            member_native = value.native
        if value.value is None and member_native is not None:
            return (
                f"union_value({quote(value.member)} := "
                f"CAST(NULL AS {member_native.sql}))",
                [],
            )
        if isinstance(value.value, UnionValue) or member_native is not None:
            inner_expression, inner_params = _typed_parameter(
                value.value, NativeType("UNION", "UNION")
                if member_native is None
                else member_native,
            )
            return (
                f"union_value({quote(value.member)} := {inner_expression})",
                inner_params,
            )
        return f"union_value({quote(value.member)} := ?)", [value.value]
    if native.kind in {"UNION", "NUMERIC_UNION"}:
        raise ValueError(
            f"value for {native.sql} lacks an explicit UNION member; refusing an implicit cast"
        )
    if native.kind == "LIST" and native.children:
        values = value if isinstance(value, (list, tuple)) else []
        expressions: list[str] = []
        params: list[Any] = []
        for item in values:
            expression, bound = _typed_parameter(item, native.children[0])
            expressions.append(expression)
            params.extend(bound)
        return f"[{', '.join(expressions)}]::{native.sql}", params
    if native.kind in {"STRUCT", "NUMERIC_VARIABLE"} and native.fields:
        if not isinstance(value, dict):
            raise ValueError(f"value for {native.sql} is not a mapping")
        expressions: list[str] = []
        params: list[Any] = []
        for field_name, field_native in native.fields:
            expression, bound = _typed_parameter(value.get(field_name), field_native)
            expressions.append(f"{quote(field_name)} := {expression}")
            params.extend(bound)
        return (
            f"CAST(struct_pack({', '.join(expressions)}) AS {native.sql})",
            params,
        )
    if native.kind == "MAP" and native.key is not None and native.value is not None:
        if not isinstance(value, dict):
            raise ValueError(f"value for {native.sql} is not a mapping")
        key_expressions: list[str] = []
        value_expressions: list[str] = []
        key_params: list[Any] = []
        value_params: list[Any] = []
        for key, item in value.items():
            key_expression, item_key_params = _typed_parameter(key, native.key)
            item_expression, item_value_params = _typed_parameter(item, native.value)
            key_expressions.append(key_expression)
            value_expressions.append(item_expression)
            key_params.extend(item_key_params)
            value_params.extend(item_value_params)
        return (
            f"CAST(MAP([{', '.join(key_expressions)}], "
            f"[{', '.join(value_expressions)}]) AS {native.sql})",
            [*key_params, *value_params],
        )
    if native.kind in {"LIST", "STRUCT", "MAP", "NUMERIC_VARIABLE"}:
        return f"CAST(? AS {native.sql})", [value]
    return "?", [value]


def _union_member_native(native: Any, member_name: str) -> Any:
    if getattr(native, "kind", None) not in {"UNION", "NUMERIC_UNION"}:
        return None
    lowered = str(member_name).lower()
    for member in getattr(native, "members", ()):
        if str(member.name).lower() == lowered:
            return member.type
    return None


def _contains_union(value: Any, union_class: type) -> bool:
    if isinstance(value, union_class):
        return True
    if isinstance(value, dict):
        return any(_contains_union(item, union_class) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_union(item, union_class) for item in value)
    return False


def _prepare_typed_value(value: Any, native: Any) -> Any:
    """Materialize an implicit source value into its declared UNION member."""
    from .typed_types import UnionValue, encode_value, native_type, union_member_name

    if native is None or isinstance(value, UnionValue):
        return value
    if native.source is not None:
        encoded = encode_value(value, native.source)
        if native.kind == "UNION":
            return UnionValue(
                union_member_name(native.source),
                encoded,
                native=native_type(native.source),
            )
        if native.kind == "NUMERIC_UNION":
            # ``encode_value`` already returns the inner finite/special
            # UnionValue.  Wrapping it again would ask DuckDB to cast a tagged
            # UNION into DECIMAL and lose the numeric member boundary.
            if encoded is None:
                return UnionValue("finite", None, native=native_type(native.source))
            return encoded
        return encoded
    if value is None and native.kind in {"UNION", "NUMERIC_UNION"}:
        member = "finite" if native.kind == "NUMERIC_UNION" else (
            native.members[0].name if native.members else "m_null"
        )
        return UnionValue(member, None)
    return value


def _typed_assignment(table: TableSchema, column: str, value: Any) -> tuple[str, list[Any]]:
    """Encode a backfill assignment against the table's exact native type."""
    from .typed_types import UnionValue, encode_value, native_type, union_member_name

    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if source is not None:
        value = encode_value(value, source)
        if native is not None and native.kind in {"UNION", "NUMERIC_UNION"}:
            member = union_member_name(source) if native.kind == "UNION" else "finite"
            if not isinstance(value, UnionValue) or value.member != member:
                value = UnionValue(member, value, native=native_type(source))
    return _typed_parameter(value, native)


__all__ = [
    "CDCF_COMMIT_ID",
    "CDCF_EVENT_ID",
    "CDCF_TOTAL_ORDER",
    "SchemaRegistry",
    "TableSchema",
    "bind",
    "delete_keys",
    "insert_rows",
    "insert_typed_rows",
    "sql_type",
    "widen",
]
