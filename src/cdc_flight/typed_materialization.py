"""Typed row materialization and shadow-copy ownership."""

from __future__ import annotations

import logging
from typing import Any

from .identity_codec import (
    _identity_value,
    _stored_identity_source_value,
    canonical_jsonb_text,
)
from .naming import CDCF_EVENT_ID, quote
from .schema_registry import TableSchema, _normalise_type, _type_sql_equal

BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR = "BOOLEAN", "BIGINT", "DOUBLE", "JSON", "VARCHAR"
DELETE_CHUNK = 2000
MAX_PARAMS_PER_STATEMENT = 40_000
MAX_ROWS_PER_STATEMENT = 5_000
ARROW_CHUNK = 100_000

log = logging.getLogger("cdc_flight.typed_materialization")

def _copy_rows_with_identity(
    con,
    table: TableSchema,
    shadow: str,
    target_types: dict[str, str],
    target_native: dict[str, Any],
    *,
    key_columns: tuple[str, ...],
    descriptors: dict[str, Any],
    changed_sql: dict[str, str] | None = None,
    changed_python: frozenset[str] = frozenset(),
) -> None:
    """Copy existing rows while using the canonical Python identity encoder.

    DuckDB's SQL display casts are deliberately not used for identity materialization.
    The old implementation generated an ID with ``CAST(value AS VARCHAR)`` and later
    looked it up with Python ``str(value)``; BLOB and nested STRUCT values proved that
    those are different languages.  This routine reads each old source key, calls the
    same recursive encoder used by inserts/lookups, and binds that ID into the shadow
    row.  Ordinary columns continue to be copied by SQL so UNION values retain their
    physical tags without a Python round trip.
    """
    old_columns = list(table.raw_types)
    if not old_columns:
        return
    rows = con.execute(
        f"SELECT rowid, {', '.join(quote(column) for column in old_columns)} "
        f"FROM {table.qualified}"
    ).fetchall()
    changed_sql = changed_sql or {}
    column_indexes = {column: index + 1 for index, column in enumerate(old_columns)}
    target_columns = list(target_types)
    from .typed_types import adapt_value, encode_value

    for row in rows:
        rowid = row[0]
        raw = {column: row[index] for column, index in column_indexes.items()}
        key_values = tuple(
            _stored_identity_source_value(raw.get("cdcf_internal_id"), index)
            or raw[column]
            for index, column in enumerate(key_columns)
        )
        identity = _identity_value(
            table,
            key_values,
            descriptors=descriptors,
            key_columns=key_columns,
        )
        expressions: list[str] = []
        params: list[Any] = []
        for column in target_columns:
            if column == CDCF_EVENT_ID and column not in raw:
                # This branch is defensive; the applier event identity is never a
                # source-key shadow column, but it keeps a malformed legacy table from
                # silently receiving a NULL in a rebuild.
                expressions.append("NULL")
            elif column == "cdcf_internal_id":
                expressions.append("?")
                params.append(identity)
            elif column in changed_sql:
                expressions.append(changed_sql[column])
            elif column in changed_python:
                descriptor = descriptors.get(column) or table.source_descriptors.get(column)
                target = target_native.get(column)
                if descriptor is not None and target is not None:
                    # Shadow copies use the same source->native adapter as the
                    # streaming path.  In particular, a multirange source is
                    # deliberately VARCHAR at both destinations and must retain
                    # PostgreSQL's canonical text rather than the old structural
                    # list representation.
                    encoded = adapt_value(raw.get(column), target)
                elif descriptor is not None:
                    encoded = encode_value(raw.get(column), descriptor)
                else:
                    encoded = raw.get(column)
                expression, bound = _typed_parameter(encoded, target_native.get(column))
                expressions.append(expression)
                params.extend(bound)
            else:
                expressions.append(quote(column))
        params.append(rowid)
        con.execute(
            f"INSERT INTO {quote(table.dataset)}.{quote(shadow)} "
            f"({', '.join(quote(column) for column in target_columns)}) "
            f"SELECT {', '.join(expressions)} FROM {table.qualified} WHERE rowid = ?",
            params,
        )

def delete_keys(con, table: TableSchema, key_columns: tuple[str, ...], keys: list[tuple]) -> None:
    if not keys:
        return
    if table.internal_identity:
        identities = [
            _identity_value(table, tuple(key), key_columns=key_columns)
            for key in keys
        ]
        identities = list(dict.fromkeys(identities))
        placeholders = ", ".join("?" for _ in identities)
        con.execute(
            f"DELETE FROM {table.qualified} WHERE {quote('cdcf_internal_id')} "
            f"IN ({placeholders})",
            identities,
        )
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
            key_rows = [list(k) for k in keys]
            native_types = [table.native_types.get(column) for column in key_columns]
            from .typed_types import UnionValue

            arrow_safe = all(
                _arrow_native_supported(native, native.sql if native is not None else types[index])
                and all(not _contains_union(row[index], UnionValue) for row in key_rows)
                for index, native in enumerate(native_types)
            )
            if arrow_safe:
                bulk_insert(con, staging, list(key_columns), key_rows, types)
            else:
                insert_typed_rows(
                    con,
                    table,
                    list(key_columns),
                    key_rows,
                    native_types,
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
        from .typed_types import adapt_value

        return _typed_parameter(adapt_value(value, native), native)
    return "?", [value]


def update_rows(
    con,
    table: TableSchema,
    key_columns: tuple[str, ...],
    updates: list[tuple[tuple, dict[str, Any]]],
) -> int | None:
    """Apply sparse row patches without materialising unchanged columns.

    Each item is ``(source_key, assignments)``.  Rows with the same assignment
    shape share one parameterised ``UPDATE .. FROM (VALUES ...)`` statement; a
    physical key move simply includes the new key in ``assignments`` while the
    old key remains ``source_key``.  ``RETURNING`` makes a missing destination
    base observable, which is required before accepting a sparse update.
    """
    if not updates:
        return 0
    if not key_columns:
        raise ValueError("sparse updates require at least one identity column")
    if table.internal_identity:
        return _update_rows_by_internal_identity(con, table, key_columns, updates)

    groups: dict[tuple[str, ...], list[tuple[tuple, dict[str, Any]]]] = {}
    for source_key, assignments in updates:
        shape = tuple(sorted(str(column) for column in assignments))
        if not shape:
            continue
        groups.setdefault(shape, []).append((tuple(source_key), assignments))

    affected = 0
    for assignment_columns, group in groups.items():
        source_aliases = tuple(f"__cdcf_src_{index}" for index in range(len(key_columns)))
        assignment_aliases = tuple(
            f"__cdcf_set_{index}" for index in range(len(assignment_columns))
        )
        aliases = source_aliases + assignment_aliases
        alias_sql = ", ".join(quote(alias) for alias in aliases)
        predicate = " AND ".join(
            f"t.{quote(column)} IS NOT DISTINCT FROM v.{quote(alias)}"
            for column, alias in zip(key_columns, source_aliases, strict=True)
        )
        set_sql = ", ".join(
            f"{quote(column)} = v.{quote(alias)}"
            for column, alias in zip(assignment_columns, assignment_aliases, strict=True)
        )

        value_rows: list[str] = []
        params: list[Any] = []

        def flush(
            *,
            rows=value_rows,
            bound=params,
            update_sql=set_sql,
            values_alias_sql=alias_sql,
            where_sql=predicate,
        ) -> None:
            nonlocal affected
            if not rows:
                return
            result = con.execute(
                f"UPDATE {table.qualified} AS t SET {update_sql} FROM "
                f"(VALUES {', '.join(rows)}) AS v({values_alias_sql}) "
                f"WHERE {where_sql} RETURNING 1",
                bound,
            )
            returned = result.fetchall()
            affected += len(returned)
            rows.clear()
            bound.clear()

        for source_key, assignments in group:
            expressions: list[str] = []
            row_params: list[Any] = []
            for column, value in zip(key_columns, source_key, strict=True):
                expression, bound = _key_parameter(value, table, column)
                expressions.append(expression)
                row_params.extend(bound)
            for column in assignment_columns:
                expression, bound = _typed_assignment(table, column, assignments[column])
                expressions.append(expression)
                row_params.extend(bound)
            if value_rows and (
                len(value_rows) >= MAX_ROWS_PER_STATEMENT
                or len(params) + len(row_params) > MAX_PARAMS_PER_STATEMENT
            ):
                flush()
            value_rows.append("(" + ", ".join(expressions) + ")")
            params.extend(row_params)
        flush()
    return affected


def _update_rows_by_internal_identity(
    con,
    table: TableSchema,
    key_columns: tuple[str, ...],
    updates: list[tuple[tuple, dict[str, Any]]],
) -> int:
    """Update by stable internal identity, including a source key move."""
    affected = 0
    for source_key, assignments in updates:
        set_parts: list[str] = []
        params: list[Any] = []
        for column, value in assignments.items():
            expression, bound = _typed_assignment(table, column, value)
            set_parts.append(f"{quote(column)} = {expression}")
            params.extend(bound)
        if all(column in assignments for column in key_columns):
            target_values = tuple(assignments[column] for column in key_columns)
            set_parts.append(f"{quote('cdcf_internal_id')} = ?")
            params.append(
                _identity_value(table, target_values, key_columns=key_columns)
            )
        if not set_parts:
            continue
        identity = _identity_value(table, source_key, key_columns=key_columns)
        result = con.execute(
            f"UPDATE {table.qualified} SET {', '.join(set_parts)} WHERE "
            f"{quote('cdcf_internal_id')} = ? RETURNING 1",
            [*params, identity],
        )
        affected += len(result.fetchall())
    return affected


def rows_per_statement(n_columns: int) -> int:
    if n_columns <= 0:  # pragma: no cover - defensive
        return MAX_ROWS_PER_STATEMENT
    return max(1, min(MAX_ROWS_PER_STATEMENT, MAX_PARAMS_PER_STATEMENT // n_columns))


def _arrow_type(sql_type: str):
    import pyarrow as pa

    upper = str(sql_type).upper()
    if upper.startswith(("STRUCT(", "MAP(", "LIST(", "UNION(")) or upper.endswith("[]"):
        return None
    if upper.startswith("VARCHAR") or upper in {"TEXT", "STRING", JSON_T, "UUID"}:
        return pa.string()
    if upper in {"BLOB", "BYTEA"}:
        return pa.binary()
    if upper in {"SMALLINT", "INT16"}:
        return pa.int16()
    if upper in {"INTEGER", "INT", "INT32"}:
        return pa.int32()
    if upper in {BIGINT, "INT64", "HUGEINT"}:
        return pa.int64()
    if upper in {"FLOAT", "REAL", "FLOAT4"}:
        return pa.float32()
    if upper in {DOUBLE, "FLOAT8"}:
        return pa.float64()
    if upper == BOOLEAN:
        return pa.bool_()
    if upper in {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"}:
        return pa.timestamp("us", tz="UTC")
    if upper == "TIMESTAMP":
        return pa.timestamp("us")
    if upper == "DATE":
        return pa.date32()
    if upper == "TIME":
        return pa.time64("us")
    return pa.string()


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
    columns = list(columns)
    rows = [list(row) for row in rows]
    source_key_columns = tuple(table.source_key_columns or table.key_columns)
    if table.internal_identity and "cdcf_internal_id" not in columns and rows:
        key_indexes = [
            columns.index(column)
            for column in source_key_columns
            if column in columns
        ]
        if len(key_indexes) == len(source_key_columns):
            rows = [
                [
                    *row,
                    _identity_value(
                        table,
                        tuple(row[index] for index in key_indexes),
                        key_columns=source_key_columns,
                    )
                ]
                for row in rows
            ]
            columns.append("cdcf_internal_id")
    if table.native_types:
        # Scalar native columns already have their exact physical declaration and
        # their values have been source-encoded by ``table_work``.  UNION, map,
        # and arbitrary struct paths stay on the typed SQL encoder.  The bounded
        # numeric UNION used for ordinary PostgreSQL NUMERIC is represented by two
        # Arrow staging fields and reconstructed in one INSERT ... SELECT.
        # PyArrow cannot represent PostgreSQL's explicit temporal infinity marker
        # as a DATE/TIMESTAMP scalar.  Keep the value lossless and use the same
        # parameterized CAST path as shadow copies and typed backfills; this is a
        # narrow fallback for the special value, not a VARCHAR downgrade.
        if _contains_postgres_infinity(rows):
            insert_typed_rows(
                con,
                table,
                columns,
                rows,
                [table.native_types.get(column) for column in columns],
            )
            return
        if _native_arrow_safe(table, columns, rows):
            _bulk_insert_typed_rows(con, table, columns, rows)
            return
        if _native_numeric_union_arrow_safe(table, columns, rows):
            _bulk_insert_typed_rows(con, table, columns, rows)
            return
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


def _arrow_native_supported(native: Any, physical: str) -> bool:
    kind = getattr(native, "kind", None)
    source = getattr(native, "source", None)
    seen: set[int] = set()
    while source is not None and getattr(source, "domain_base", None) is not None and id(source) not in seen:
        seen.add(id(source))
        source = source.domain_base
    # JSONB's PostgreSQL equality is semantic.  Arrow would bind the already
    # encoded text directly and bypass the canonical JSONB text path below.
    if source is not None and str(getattr(source, "kind", "")).lower() == "jsonb":
        return False
    if kind in {"UNION", "MAP", "STRUCT", "NUMERIC_VARIABLE", "INTERVAL", "TIMETZ"}:
        return False
    if kind == "LIST":
        return bool(native.children) and _arrow_native_supported(
            native.children[0], native.children[0].sql
        )
    if kind == "NUMERIC_UNION":
        return False
    normalized = _normalise_type(physical)
    return normalized in {
        "SMALLINT", "INTEGER", BIGINT, "FLOAT", DOUBLE, BOOLEAN, VARCHAR,
        JSON_T, "UUID", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "BLOB",
    }


def _native_arrow_safe(table: TableSchema, columns: list[str], rows: list[list]) -> bool:
    from .typed_types import UnionValue

    for index, column in enumerate(columns):
        native = table.native_types.get(column)
        physical = table.raw_types.get(column, table.columns.get(column, VARCHAR))
        if native is not None and native.kind == "NUMERIC_UNION":
            return False
        if not _arrow_native_supported(native, physical):
            return False
        for row in rows:
            if _contains_union(row[index], UnionValue):
                return False
    return True


def _native_numeric_union_arrow_safe(
    table: TableSchema, columns: list[str], rows: list[list]
) -> bool:
    from .typed_types import UnionValue

    found_numeric_union = False
    for index, column in enumerate(columns):
        native = table.native_types.get(column)
        physical = table.raw_types.get(column, table.columns.get(column, VARCHAR))
        if native is not None and native.kind == "NUMERIC_UNION":
            found_numeric_union = True
            for row in rows:
                value = row[index]
                if not isinstance(value, UnionValue) or value.member not in {"finite", "special"}:
                    return False
                if value.value is None:
                    return False
            continue
        if not _arrow_native_supported(native, physical):
            return False
        for row in rows:
            if _contains_union(row[index], UnionValue):
                return False
    return found_numeric_union


def _contains_postgres_infinity(value: Any) -> bool:
    """Return whether a typed row contains the explicit temporal sentinel."""
    from .typed_types import PostgresInfinity

    if isinstance(value, PostgresInfinity):
        return True
    if isinstance(value, dict):
        return any(
            _contains_postgres_infinity(key) or _contains_postgres_infinity(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_postgres_infinity(item) for item in value)
    return False


def _bulk_insert_typed_rows(con, table: TableSchema, columns: list[str], rows: list[list]) -> None:
    """Bulk-load native rows, rebuilding bounded numeric UNIONs in SQL.

    A normal source table can combine JSON, UUID, timestamps, lists, and a bounded
    NUMERIC UNION.  Passing every field through a parameterized struct/UNION
    expression made a 200k-row replay spend minutes in the DuckDB binder.  Arrow
    carries the ordinary fields in bulk; only the typed UNION disposition remains
    an explicit SQL expression.  Unsupported nested UNIONs keep the strict path.
    """
    import pyarrow as pa

    from .typed_types import UnionValue

    prepared = [
        [
            _prepare_typed_value(value, table.native_types.get(column))
            for column, value in zip(columns, row, strict=True)
        ]
        for row in rows
    ]
    arrays: dict[str, Any] = {}
    select_expressions: list[str] = []
    for index, column in enumerate(columns):
        native = table.native_types.get(column)
        physical = native.sql if native is not None else table.raw_types.get(
            column, table.columns.get(column, VARCHAR)
        )
        if native is not None and native.kind == "NUMERIC_UNION":
            member_name = f"__cdcf_union_{index}_member"
            value_name = f"__cdcf_union_{index}_value"
            members = {member.name.lower(): member.type for member in native.members}
            member_values: list[str] = []
            value_values: list[str | None] = []
            for row in prepared:
                value = row[index]
                if not isinstance(value, UnionValue) or value.member.lower() not in members:
                    raise ValueError(f"invalid numeric UNION value for {column}: {value!r}")
                member = value.member.lower()
                member_type = members[member]
                member_values.append(member)
                value_values.append(None if value.value is None else str(value.value))
                if member_type is None:  # pragma: no cover - NativeType invariant
                    raise ValueError(f"numeric UNION member {member!r} has no type")
            arrays[member_name] = pa.array(member_values, type=pa.string())
            arrays[value_name] = pa.array(value_values, type=pa.string())
            arms = []
            for member, member_type in members.items():
                arms.append(
                    f"WHEN v.{quote(member_name)} = '{member}' THEN "
                    f"CAST(union_value({quote(member)} := "
                    f"CAST(v.{quote(value_name)} AS {member_type.sql})) AS {native.sql})"
                )
            select_expressions.append("CASE " + " ".join(arms) + " END")
            continue
        values = [row[index] for row in prepared]
        arrow_type = _arrow_type(physical)
        arrays[column] = pa.array(values, type=arrow_type)
        select_expressions.append(f"v.{quote(column)}")

    view = "cdcf_typed_bulk_rows"
    arrow_table = pa.table(arrays)
    con.register(view, arrow_table)
    try:
        collist = ", ".join(quote(column) for column in columns)
        con.execute(
            f"INSERT INTO {table.qualified} ({collist}) SELECT "
            f"{', '.join(select_expressions)} FROM {view} AS v"
        )
    finally:
        con.unregister(view)


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
    from .typed_types import JsonbNull, NativeType, PostgresInfinity, UnionValue

    if value is None:
        return "NULL", []
    if native is None:
        return "?", [value]
    if isinstance(value, PostgresInfinity):
        if native.kind in {"DATE", "TIMESTAMP", "TIMESTAMPTZ"}:
            return f"CAST(? AS {native.sql})", [str(value)]
        raise ValueError(
            f"PostgreSQL infinity cannot bind to destination type {native.sql}"
        )
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
            if member_native is not None and inner_expression == "?":
                # DuckDB infers a common parameter type across a VALUES relation.
                # Without this cast, a first value such as 120.00 fixes DECIMAL(5,2)
                # and a later valid 1999.99 is rejected before the UNION receives it.
                inner_expression = f"CAST(? AS {member_native.sql})"
            return (
                f"union_value({quote(value.member)} := {inner_expression})",
                inner_params,
            )
        return f"union_value({quote(value.member)} := ?)", [value.value]
    if native.kind == "VARIANT":
        if isinstance(value, JsonbNull):
            return "CAST(JSON 'null' AS VARIANT)", []
        if isinstance(value, str):
            # A direct Python string bind becomes a VARIANT string.  JSONB's
            # wire value is JSON text, so parse it first and then construct the
            # native VARIANT value.  This is also the recursive form used inside
            # LIST/STRUCT/MAP and UNION members.
            return "CAST(CAST(? AS JSON) AS VARIANT)", [value]
        raise ValueError(f"value for VARIANT is not validated JSON text: {value!r}")
    if native.kind == "JSON":
        source = native.source
        seen: set[int] = set()
        while source is not None and getattr(source, "domain_base", None) is not None and id(source) not in seen:
            seen.add(id(source))
            source = source.domain_base
        if source is not None and str(getattr(source, "kind", "")).lower() == "jsonb":
            value = canonical_jsonb_text(value)
        if isinstance(value, JsonbNull):
            return "JSON 'null'", []
        if isinstance(value, str):
            return "CAST(? AS JSON)", [value]
        raise ValueError(f"value for JSON is not validated JSON text: {value!r}")
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
    if native.kind in {
        "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "BOOLEAN",
        "VARCHAR", "BLOB", "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ",
        "TIMETZ", "INTERVAL", "UUID", "ENUM",
    }:
        # A Python float is bound as DOUBLE by DuckDB unless the target type is
        # explicit.  Predicates must compare against the stored native value (in
        # particular PostgreSQL binary32), and assignments benefit from the same
        # one-type boundary.
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
    from .typed_types import UnionValue, adapt_value

    if native is None or isinstance(value, UnionValue):
        return value
    if native.source is not None:
        adapted = adapt_value(value, native)
        return _tagged_union_null(native) if adapted is None else adapted
    if value is None and native.kind in {"UNION", "NUMERIC_UNION"}:
        return _tagged_union_null(native)
    return value


def _tagged_union_null(native: Any) -> Any:
    """Keep the established typed-NULL member when a UNION receives SQL NULL."""
    from .typed_types import UnionValue, native_type, union_member_name

    if native.kind == "NUMERIC_UNION":
        return UnionValue(
            "finite",
            None,
            native=native_type(native.source) if native.source is not None else None,
        )
    if native.kind == "UNION":
        source = native.source
        member = union_member_name(source) if source is not None else (
            native.members[0].name if native.members else "m_null"
        )
        return UnionValue(
            member,
            None,
            native=native_type(source) if source is not None else None,
        )
    return None


def _typed_assignment(table: TableSchema, column: str, value: Any) -> tuple[str, list[Any]]:
    """Encode a backfill assignment against the table's exact native type."""
    from .typed_types import adapt_value, encode_value

    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if source is not None:
        if native is not None:
            value = adapt_value(value, native)
            if value is None and native.kind in {"UNION", "NUMERIC_UNION"}:
                value = _tagged_union_null(native)
        else:
            value = encode_value(value, source)
    return _typed_parameter(value, native)


__all__ = [
    "_copy_rows_with_identity",
    "_typed_assignment",
    "bulk_insert",
    "delete_keys",
    "insert_rows",
    "insert_typed_rows",
    "update_rows",
]
