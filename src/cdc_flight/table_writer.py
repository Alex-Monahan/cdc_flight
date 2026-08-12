"""Destination materialization for a prepared table fold.

``table_work`` owns the in-memory source fold.  This module owns the separate
destination concern: schema admission, keyed merge SQL, keyless physical SQL,
and truncate auditing.  The caller owns the surrounding transaction.
"""

from __future__ import annotations

from typing import Any

from . import apply_sql, destination, naming
from .destination_failure import MaterializationConnection
from .errors import (
    AdmissionError,
    AmbiguousDelete,
    DestinationIdentityCollision,
    SchemaEvolutionRefused,
    as_schema_refusal,
)
from .row_patch import RowPatch
from .table_work import (
    APPLIER_COLUMN_TYPES,
    START,
    RowMove,
    TableWork,
    _missing_toast_base,
    _raw_key,
)

OWNER = "table-destination-materialization"


def write(
    con,
    registry,
    item: TableWork,
    created_in_txn: set[str],
    *,
    pipeline: str | None = None,
    control_schema: str | None = None,
) -> None:
    """Apply one prepared table plan without opening or closing a transaction."""
    if item.truncated and not item.live and not registry.get(item.target).exists:
        item.rows_removed = 0
        _finish_truncate_audit(item)
        return

    columns = {col: ctype or apply_sql.VARCHAR for col, ctype in item.columns.items()}
    columns.update(APPLIER_COLUMN_TYPES)
    for column in item.key_columns:
        columns.setdefault(column, apply_sql.VARCHAR)

    if item.native_columns:
        typed_columns = {**columns, **item.native_columns}
        try:
            table, created = registry.ensure_typed(
                item.target, columns=typed_columns, key_columns=item.key_columns
            )
        except AdmissionError as error:
            refused = as_schema_refusal(
                error,
                refusal_origin="table_writer",
                source_schema=item.source_schema,
                source_table=item.source_table,
                target=item.target,
            )
            refused.source_schema = refused.source_schema or item.source_schema
            refused.source_table = refused.source_table or item.source_table
            refused.target = refused.target or item.target
            raise
    elif item.truncated and registry.get(item.target).exists:
        table, created = registry.get(item.target), False
    else:
        raise SchemaEvolutionRefused(
            f"{item.target}: catalog-authoritative descriptors are incomplete; "
            "the production typed path cannot create or write an untyped table",
            source_schema=item.source_schema,
            source_table=item.source_table,
            target=item.target,
            refusal_origin="table_writer",
        )
    if created:
        created_in_txn.add(item.target)
    fresh = item.target in created_in_txn

    column_order = [c for c in table.columns if c in columns] + [
        c for c in columns if c not in table.columns
    ]
    column_order = list(dict.fromkeys(column_order))
    data_con = MaterializationConnection(con)

    if item.keyless:
        _write_keyless_operations(
            data_con,
            table,
            item,
            column_order,
            fresh=fresh,
            pipeline=pipeline,
            control_schema=control_schema,
        )
        _finish_truncate_audit(item)
        try:
            apply_sql.assert_identity_is_unique(con, table)
        except DestinationIdentityCollision as collision:
            collision.source_schema = item.source_schema
            collision.source_table = item.source_table
            collision.target = item.target
            raise
        return

    delete_keys, rows, partial_updates, moves = _plan(item)
    if item.truncated:
        item.rows_removed = 0 if fresh else _delete_all(data_con, table)
    elif not item.snapshot and not fresh and delete_keys:
        keys = [
            tuple(
                _key_value(table, col, value)
                for col, value in zip(item.key_columns, _raw_key(item, key), strict=False)
            )
            for key in delete_keys
        ]
        apply_sql.delete_keys(data_con, table, item.key_columns, keys)
    _finish_truncate_audit(item)

    updates: list[tuple[tuple, dict[str, Any]]] = []
    for key, patch in partial_updates:
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, _raw_key(item, key), strict=False)
        )
        updates.append((source_key, patch.bindable_values()))
    for move in moves:
        assignments = move.patch.bindable_values()
        target_key = _raw_key(item, move.target_key)
        source_key_values = _raw_key(item, move.source_key)
        for column, value in zip(item.key_columns, target_key, strict=False):
            assignments[column] = value
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, source_key_values, strict=False)
        )
        updates.append((source_key, assignments))
    if updates:
        affected = 0 if fresh else apply_sql.update_rows(
            data_con, table, item.key_columns, updates
        )
        if fresh or affected != len(updates):
            _missing_toast_base(
                item,
                None,
                reason=(
                    f"sparse update matched {affected} of {len(updates)} "
                    "destination base row(s)"
                ),
            )

    apply_sql.insert_rows(
        data_con,
        table,
        column_order,
        [
            [
                (
                    _typed_value(table, col, row.get(col))
                    if table.native_types and col in table.native_types
                    else apply_sql.bind(row.get(col), table.columns.get(col, apply_sql.VARCHAR))
                )
                for col in column_order
            ]
            for row in rows
        ],
    )
    try:
        apply_sql.assert_identity_is_unique(con, table)
    except DestinationIdentityCollision as collision:
        collision.source_schema = item.source_schema
        collision.source_table = item.source_table
        collision.target = item.target
        raise


def _write_keyless_operations(
    con,
    table,
    item: TableWork,
    column_order: list[str],
    *,
    fresh: bool,
    pipeline: str | None,
    control_schema: str | None,
) -> None:
    """Execute keyless physical operations in source order."""
    pending: list[dict[str, Any]] = []
    table_fresh = fresh

    def flush_inserts() -> None:
        nonlocal table_fresh
        if not pending:
            return
        apply_sql.insert_rows(
            con,
            table,
            column_order,
            [
                [
                    (
                        _typed_value(table, column, row.get(column))
                        if table.native_types and column in table.native_types
                        else apply_sql.bind(
                            row.get(column), table.columns.get(column, apply_sql.VARCHAR)
                        )
                    )
                    for column in column_order
                ]
                for row in pending
            ],
        )
        pending.clear()
        table_fresh = False

    for operation in item.keyless_operations:
        if operation.operation in {"c", "r"}:
            if operation.after is not None:
                pending.append(operation.after)
            continue
        flush_inserts()
        if operation.operation == "t":
            item.rows_removed = 0 if table_fresh else _delete_all(con, table)
            table_fresh = False
            continue
        if operation.operation not in {"d", "u"}:
            raise SchemaEvolutionRefused(
                f"{item.target}: unsupported keyless operation {operation.operation!r}",
                source_schema=item.source_schema,
                source_table=item.source_table,
                target=item.target,
                refusal_origin="table_writer",
            )
        if operation.before is None:
            _missing_toast_base(
                item,
                None,
                reason=f"keyless {operation.operation} has no complete before-image",
            )
        affected = apply_sql.delete_matching_row(
            con,
            table,
            tuple(operation.before),
            operation.before,
        )
        if affected != 1:
            _missing_toast_base(
                item,
                None,
                reason=(
                    f"keyless {operation.operation} before-image matched "
                    f"{affected} destination row(s), expected exactly one"
                ),
            )
        if operation.operation == "u":
            if operation.after is None:
                _missing_toast_base(
                    item, None, reason="keyless UPDATE has no complete after-image"
                )
            pending.append(operation.after)
            flush_inserts()

    flush_inserts()
    if item.keyless_ledger and not item.snapshot and pipeline:
        destination_rows = [
            (item.target, event_id, operation, digest)
            for event_id, operation, digest in item.keyless_ledger
        ]
        destination.write_keyless_events(
            con,
            destination_rows,
            pipeline=pipeline,
            control_schema=control_schema,
        )


def _typed_value(table, column: str, value):
    """Bind a source value to the current physical native declaration."""
    from .typed_types import adapt_value

    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if native is None or source is None:
        return value
    return adapt_value(value, native)


def _key_value(table, column: str, value):
    """Encode a key using the same source descriptor as the row path."""
    from .typed_types import adapt_value

    if table.internal_identity:
        return value
    native = table.native_types.get(column)
    source = table.source_descriptors.get(column)
    if native is None or source is None:
        return apply_sql.bind(value, table.columns.get(column, apply_sql.VARCHAR))
    return adapt_value(value, native)


def _plan(item: TableWork) -> tuple[
    list[tuple],
    list[dict],
    list[tuple[tuple, RowPatch]],
    list[RowMove],
]:
    """Read the keyed physical fold into destination operations."""
    delete_keys: list[tuple] = []
    rows: list[dict] = []
    partial_updates: list[tuple[tuple, RowPatch]] = []
    moves: list[RowMove] = []
    for key, entries in item.live.items():
        if len(entries) == 1 and entries[0] is START:
            continue
        if not entries:
            delete_keys.append(key)
            continue
        if len(entries) == 1:
            entry = entries[0]
            if isinstance(entry, RowMove):
                moves.append(entry)
            elif isinstance(entry, RowPatch) and not entry.complete:
                partial_updates.append((key, entry))
            else:
                delete_keys.append(key)
                rows.append(entry.encoded_values() if isinstance(entry, RowPatch) else entry)
        else:
            concrete = [entry for entry in entries if entry is not START]
            if len(concrete) > 1:
                raise AmbiguousDelete(
                    f"{item.target}: identity {key!r} would be written twice by one "
                    "commit group (ADR 0001 §18/A35)",
                    source_schema=item.source_schema,
                    source_table=item.source_table,
                    target=item.target,
                )
            for entry in concrete:
                if isinstance(entry, RowPatch) and not entry.complete:
                    partial_updates.append((key, entry))
                elif isinstance(entry, RowMove):
                    moves.append(entry)
                else:
                    delete_keys.append(key)
                    rows.append(
                        entry.encoded_values() if isinstance(entry, RowPatch) else entry
                    )
    move_sources = {move.source_key for move in moves}
    delete_keys = [key for key in delete_keys if key not in move_sources]
    return delete_keys, rows, partial_updates, moves


def _finish_truncate_audit(item: TableWork) -> None:
    counts: list[int | None] = list(item.truncate_marks)
    if counts:
        counts[0] = None if item.rows_removed is None else counts[0] + item.rows_removed
    item.truncate_rows_removed = tuple(counts)


def _delete_all(con, table) -> int | None:
    result = con.execute(f"DELETE FROM {table.qualified}")
    try:
        row = result.fetchone()
    except Exception:  # pragma: no cover - a destination that returns no result set
        return None
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):  # pragma: no cover
        return None


def live_names(tables: set[str]) -> set[str]:
    """Report the live table rather than a snapshot shadow."""
    return {
        t[: -len(naming.SHADOW_SUFFIX)] if t.endswith(naming.SHADOW_SUFFIX) else t
        for t in tables
    }
