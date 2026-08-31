"""Destination materialization for a prepared table fold.

``table_work`` owns the in-memory source fold.  This module owns the separate
destination concern: schema admission, keyed merge SQL, keyless physical SQL,
and truncate auditing.  The caller owns the surrounding transaction.
"""

from __future__ import annotations

from typing import Any

from . import apply_sql, destination_failure, naming
from .destination_failure import (
    DestinationDataRejection,
    MaterializationConnection,
    execute_table_dml,
)
from .errors import (
    AdmissionError,
    AmbiguousDelete,
    DestinationIdentityCollision,
    SchemaEvolutionRefused,
    TableWriteFailure,
    as_schema_refusal,
)
from .faults import DestinationFault
from .row_patch import RowPatch
from .table_work import (
    APPLIER_COLUMN_TYPES,
    START,
    RowMove,
    TableWork,
    _missing_toast_base,
    _raw_key,
)
from .typed_types import FieldValue

OWNER = "table-destination-materialization"


def _table_dml_connection(con, target: str):
    """Create the one DML facade owned by this table writer."""
    return MaterializationConnection(con, target)


def write(
    con,
    registry,
    item: TableWork,
    created_in_txn: set[str],
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

    # Add the fixed metadata columns to a legacy table before the typed source
    # ensure. DuckDB's ALTER TABLE transaction bookkeeping can reject a later
    # Arrow/DML write when metadata ALTERs are interleaved with source-column ALTERs.
    # New tables receive all of these columns in _create_strict below.
    existing_table = registry.get(item.target)
    if existing_table.exists:
        # The first four fields are the fixed row-state prefix. Adding that prefix
        # before SchemaRegistry's source-column loop avoids a DuckDB catalog bug
        # when all fixed metadata fields are introduced by the typed ensure in one
        # pass; the remaining fixed fields can then be added by that same loop.
        _ensure_applier_columns(
            con,
            existing_table,
            names=(
                naming.CDCF_COMMIT_ID,
                naming.CDCF_EVENT_ID,
                naming.CDCF_TOTAL_ORDER,
                naming.CDCF_DELETED,
            ),
        )

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
    _ensure_applier_columns(con, table)

    column_order = [c for c in table.columns if c in columns] + [
        c for c in columns if c not in table.columns
    ]
    column_order = list(dict.fromkeys(column_order))
    ensure_live_view(con, table, target=item.target)
    data_con = _table_dml_connection(con, item.target)
    try:
        if (
            not item.snapshot
            and not fresh
            and item.previous_delete_mode == "soft"
            and item.delete_mode == "hard"
        ):
            # A soft->hard policy transition is itself a fenced data operation:
            # remove committed tombstones before admitting the next hard-mode
            # source unit.  No row is resurrected, and the existing event/delete
            # ledgers remain durable in this same transaction.
            execute_table_dml(
                data_con,
                f"DELETE FROM {table.qualified} "
                f"WHERE {naming.quote('cdcf_deleted')} = true",
            )
        _write_table(con, table, item, column_order, fresh=fresh, data_con=data_con)
    except (
        AdmissionError,
        AmbiguousDelete,
        DestinationDataRejection,
        DestinationFault,
        DestinationIdentityCollision,
    ):
        raise
    except Exception as error:
        if destination_failure.is_driver_error(error):
            raise
        raise TableWriteFailure(None, error, data_con.target) from error


def _write_table(con, table, item: TableWork, column_order: list[str], *, fresh: bool, data_con):
    """Apply one table's data-plane operations under one validated DML scope."""
    if item.keyless:
        _write_keyless_operations(
            data_con,
            table,
            item,
            column_order,
            fresh=fresh,
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
    if not item.snapshot and not fresh and item.soft_deletes:
        _mark_keyed_soft_deletes(data_con, table, item)
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
    for key, patch in item.soft_replacements.items():
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, _raw_key(item, key), strict=False)
        )
        assignments = patch.bindable_values()
        assignments["cdcf_deleted"] = False
        assignments["cdcf_delete_event_id"] = None
        assignments["cdcf_delete_lsn"] = None
        updates.append((source_key, assignments))
    if updates:
        affected = (
            0 if fresh else apply_sql.update_rows(data_con, table, item.key_columns, updates)
        )
        if fresh or affected != len(updates):
            _missing_toast_base(
                item,
                None,
                reason=(
                    f"sparse update matched {affected} of {len(updates)} destination base row(s)"
                ),
            )

    if rows:
        apply_sql.insert_rows(
            data_con,
            table,
            column_order,
            [
                [
                    (
                        row.get(col)
                        if table.native_types and col in table.native_types
                        else apply_sql.bind(row.get(col), table.columns.get(col, apply_sql.VARCHAR))
                    )
                    for col in column_order
                ]
                for row in rows
            ],
            values_are_encoded=True,
        )
    try:
        apply_sql.assert_identity_is_unique(con, table)
    except DestinationIdentityCollision as collision:
        collision.source_schema = item.source_schema
        collision.source_table = item.source_table
        collision.target = item.target
        raise


def _write_keyless_operations(
    data_con,
    table,
    item: TableWork,
    column_order: list[str],
    *,
    fresh: bool,
) -> None:
    """Execute keyless physical operations in source order."""
    pending: list[dict[str, Any]] = []
    table_fresh = fresh

    def flush_inserts() -> None:
        nonlocal table_fresh
        if not pending:
            return
        apply_sql.insert_rows(
            data_con,
            table,
            column_order,
            [
                [
                    (
                        row.get(column)
                        if table.native_types and column in table.native_types
                        else apply_sql.bind(
                            row.get(column), table.columns.get(column, apply_sql.VARCHAR)
                        )
                    )
                    for column in column_order
                ]
                for row in pending
            ],
            values_are_encoded=True,
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
            item.rows_removed = 0 if table_fresh else _delete_all(data_con, table)
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
        if operation.operation == "d" and operation.delete_mode == "soft":
            affected = _mark_keyless_soft_delete(data_con, table, operation)
        else:
            affected = apply_sql.delete_matching_row(
                data_con,
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
                _missing_toast_base(item, None, reason="keyless UPDATE has no complete after-image")
            pending.append(operation.after)
            flush_inserts()

    flush_inserts()


def ensure_live_view(con, table, *, target: str | None = None) -> None:
    """Expose one stable current/live projection for both storage modes."""
    target = target or getattr(table, "name", None)
    if not target:
        raise ValueError("a destination target is required to create its live view")
    view_name = naming.quote(f"{target}__live")
    qualified_view = f"{naming.quote(table.dataset)}.{view_name}"
    con.execute(
        f"CREATE OR REPLACE VIEW {qualified_view} AS SELECT * FROM {table.qualified} "
        # Legacy tables receive a nullable additive metadata column because
        # DuckDB cannot add a constrained column in-place. NULL means "not yet
        # tombstoned" for those rows and must remain visible in the live view.
        f"WHERE {naming.quote('cdcf_deleted')} IS NOT TRUE"
    )


def _ensure_applier_columns(con, table, *, names=None) -> None:
    """Repair additive metadata on a legacy table before creating the live view."""
    selected = names if names is not None else APPLIER_COLUMN_TYPES
    for column in selected:
        type_name = APPLIER_COLUMN_TYPES[column]
        if column in table.columns:
            continue
        ddl_type = (
            # DuckDB rejects constraints in ALTER TABLE ADD COLUMN, and a DEFAULT
            # on this column conflicts with later same-transaction source ALTERs.
            # The create path declares cdcf_deleted NOT NULL; legacy tables get a
            # plain boolean; the live-view predicate treats legacy NULL as not
            # deleted and every newly written row binds the field explicitly.
            "BOOLEAN"
            if column == "cdcf_deleted"
            else type_name
        )
        con.execute(
            f"ALTER TABLE {table.qualified} ADD COLUMN {naming.quote(column)} {ddl_type}"
        )
        table.columns[column] = type_name
        table.raw_types[column] = type_name


def _mark_keyed_soft_deletes(data_con, table, item: TableWork) -> None:
    updates = []
    for key, operation in item.soft_deletes.items():
        source_key = tuple(
            _key_value(table, column, value)
            for column, value in zip(item.key_columns, _raw_key(item, key), strict=False)
        )
        updates.append(
            (
                source_key,
                {
                    "cdcf_deleted": True,
                    "cdcf_delete_event_id": operation.event_id,
                    "cdcf_delete_lsn": operation.source_lsn,
                },
            )
        )
    if updates:
        # A replay is fenced by event_ledger before this point. A zero-row mark is
        # therefore a legitimate reconciliation no-op (the source row may already
        # have been physically removed by an earlier hard policy epoch).
        apply_sql.update_rows(data_con, table, item.key_columns, updates)


def _mark_keyless_soft_delete(data_con, table, operation) -> int:
    """Mark exactly one keyless physical row without removing its before-image."""
    predicates = []
    params = []
    for column, value in operation.before.items():
        if column not in table.columns or column in APPLIER_COLUMN_TYPES:
            continue
        expression, bound = apply_sql._typed_assignment(table, column, value)
        predicates.append(f"{naming.quote(column)} IS NOT DISTINCT FROM {expression}")
        params.extend(bound)
    if not predicates:
        return 0
    assignments = (
        f"{naming.quote('cdcf_deleted')} = true, "
        f"{naming.quote('cdcf_delete_event_id')} = ?, "
        f"{naming.quote('cdcf_delete_lsn')} = ?"
    )
    result = execute_table_dml(
        data_con,
        f"UPDATE {table.qualified} SET {assignments} WHERE "
        + " AND ".join(predicates)
        + " RETURNING 1",
        [operation.event_id, operation.source_lsn, *params],
    )
    return len(result.fetchall())


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


def _plan(
    item: TableWork,
) -> tuple[
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
            if key not in item.soft_deletes:
                delete_keys.append(key)
            continue
        if len(entries) == 1:
            entry = entries[0]
            if key in item.soft_deletes:
                if isinstance(entry, RowPatch):
                    item.soft_replacements[key] = entry
                elif isinstance(entry, RowMove):
                    item.soft_replacements[key] = entry.patch
                else:
                    item.soft_replacements[key] = RowPatch(
                        {name: FieldValue.of(value) for name, value in entry.items()},
                        complete=True,
                    )
                continue
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
                    f"{item.target}: opaque identity {key} would be written twice by one "
                    "commit group (ADR 0001 §18/A35)",
                    source_schema=item.source_schema,
                    source_table=item.source_table,
                    target=item.target,
                )
            for entry in concrete:
                if key in item.soft_deletes:
                    if isinstance(entry, RowPatch):
                        item.soft_replacements[key] = entry
                    elif isinstance(entry, RowMove):
                        item.soft_replacements[key] = entry.patch
                    continue
                if isinstance(entry, RowPatch) and not entry.complete:
                    partial_updates.append((key, entry))
                elif isinstance(entry, RowMove):
                    moves.append(entry)
                else:
                    delete_keys.append(key)
                    rows.append(entry.encoded_values() if isinstance(entry, RowPatch) else entry)
    move_sources = {move.source_key for move in moves}
    delete_keys = [key for key in delete_keys if key not in move_sources]
    return delete_keys, rows, partial_updates, moves


def _finish_truncate_audit(item: TableWork) -> None:
    counts: list[int | None] = list(item.truncate_marks)
    if counts:
        counts[0] = None if item.rows_removed is None else counts[0] + item.rows_removed
    item.truncate_rows_removed = tuple(counts)


def _delete_all(con, table) -> int | None:
    result = execute_table_dml(con, f"DELETE FROM {table.qualified}")
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
        t[: -len(naming.SHADOW_SUFFIX)] if t.endswith(naming.SHADOW_SUFFIX) else t for t in tables
    }
