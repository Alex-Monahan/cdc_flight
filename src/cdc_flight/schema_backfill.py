"""Destination backfill ownership for catalog-added columns."""

from __future__ import annotations

from typing import Any

from .errors import SchemaBackfillRefused
from .naming import quote

OWNER = "destination-backfill"


def _assignment(table, column: str, value: Any):
    """Route every typed ADD-column value failure through schema refusal."""
    from .typed_materialization import _typed_assignment

    try:
        return _typed_assignment(table, column, value)
    except Exception as exc:
        raise SchemaBackfillRefused(
            f"cannot backfill {table.name}.{column}: the source value is not "
            "deliverable through the current destination type",
            target=table.name,
            refusal_origin="schema_backfill",
        ) from exc


class BackfillOwner:
    """Mixin containing only source-read backfill mutations."""

    def backfill_columns(
        self,
        name: str,
        *,
        key_columns: tuple[str, ...],
        value_columns: tuple[str, ...],
        rows: list[tuple],
    ) -> None:
        """Copy current source values into newly added columns in this transaction."""
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
                expression, bound = _assignment(table, column, value)
                set_parts.append(f"{quote(column)} = {expression}")
                params.extend(bound)
            where_parts: list[str] = []
            for column, value in zip(key_columns, keys, strict=True):
                expression, bound = _assignment(table, column, value)
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
        """Backfill a keyless destination only when source values are uniform."""
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
                    "proves what those rows should contain",
                    refusal_origin="schema_backfill",
                )
            return
        values = tuple(rows[0][: len(value_columns)])
        if any(tuple(row[: len(value_columns)]) != values for row in rows[1:]):
            raise SchemaBackfillRefused(
                f"cannot backfill keyless table {name}: added-column values are not "
                "uniform and the source has no stable row identity",
                refusal_origin="schema_backfill",
            )
        set_parts: list[str] = []
        params: list[Any] = []
        for column, value in zip(value_columns, values, strict=True):
            expression, bound = _assignment(table, column, value)
            set_parts.append(f"{quote(column)} = {expression}")
            params.extend(bound)
        self.con.execute(
            f"UPDATE {table.qualified} SET {', '.join(set_parts)}", params
        )


__all__ = ["OWNER", "BackfillOwner"]
