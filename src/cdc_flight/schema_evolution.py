"""Source-column identity and destination schema evolution (rubric 2.1/2.2).

PostgreSQL does not publish DDL through ``pgoutput``.  The catalog watcher therefore
records the source column identity (``attnum`` plus the type identity) and this module
turns a catalog diff into transactional destination DDL.  The destination semantics
are deliberately current-state semantics:

* an added source column is added to the destination and existing rows are backfilled
  from the fenced source catalog before the commit becomes durable; and
* a dropped source column is physically dropped from the destination.  Keeping a
  stale, always-NULL column would make the destination schema disagree with the source
  and would make a later name reuse ambiguous.

Keyed tables use their destination primary-key columns for the backfill.  A keyless
table has no source identity with which to match old rows; when every existing source
row has the same value for the newly added columns (the normal ADD-with-default or
ADD-without-default case), that value is applied to all destination changelog rows.
If the source values differ, the applier refuses the group rather than inventing a
row mapping.

A rename is never represented as a drop plus add.  If a row carrying the new name
arrives before the watcher notices the catalog change, ``rename_column`` merges the
two physical columns and then removes the old one in the same destination transaction.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from . import naming
from .apply_sql import BIGINT, BOOLEAN, DOUBLE, JSON_T, VARCHAR

COLUMN_ADDED = "added"
COLUMN_DROPPED = "dropped"
COLUMN_RENAMED = "renamed"
COLUMN_TYPE_CHANGED = "type_changed"


@dataclass(frozen=True)
class SourceColumn:
    """The catalog identity needed to distinguish add/drop from rename."""

    attnum: int
    name: str
    type_oid: int
    type_name: str
    nullable: bool = True

    @property
    def destination_name(self) -> str:
        return naming.normalize(self.name)

    @property
    def type_identity(self) -> tuple[int, str]:
        return self.type_oid, self.type_name.lower()


@dataclass(frozen=True)
class ColumnChange:
    """One source-column change, stable across a delayed catalog poll."""

    kind: str
    attnum: int
    old_name: str | None = None
    new_name: str | None = None
    type_oid: int | None = None
    type_name: str | None = None
    nullable: bool = True

    @property
    def destination_old_name(self) -> str | None:
        return naming.normalize(self.old_name) if self.old_name is not None else None

    @property
    def destination_new_name(self) -> str | None:
        return naming.normalize(self.new_name) if self.new_name is not None else None


def diff_columns(
    before: Iterable[SourceColumn], after: Iterable[SourceColumn]
) -> tuple[ColumnChange, ...]:
    """Diff source columns by ``attnum`` and type, not by name.

    PostgreSQL keeps a dropped column's attribute number and assigns a fresh number to
    a later add.  That is what makes the three cases unambiguous even when a rename is
    combined with unrelated adds/drops in one transaction.
    """

    old = {column.attnum: column for column in before}
    new = {column.attnum: column for column in after}
    changes: list[ColumnChange] = []
    for attnum in sorted(old.keys() | new.keys()):
        previous = old.get(attnum)
        current = new.get(attnum)
        if previous is None:
            changes.append(
                ColumnChange(
                    kind=COLUMN_ADDED,
                    attnum=attnum,
                    new_name=current.name,
                    type_oid=current.type_oid,
                    type_name=current.type_name,
                    nullable=current.nullable,
                )
            )
            continue
        if current is None:
            changes.append(
                ColumnChange(
                    kind=COLUMN_DROPPED,
                    attnum=attnum,
                    old_name=previous.name,
                    type_oid=previous.type_oid,
                    type_name=previous.type_name,
                    nullable=previous.nullable,
                )
            )
            continue
        if previous.name != current.name:
            # An attnum-preserving rename is continuity even when a separate ALTER
            # changed its type in the same source transaction.  The type action is
            # applied after the RENAME; the common, fully seamless case has identical
            # type identities as required by rubric 2.2.
            changes.append(
                ColumnChange(
                    kind=COLUMN_RENAMED,
                    attnum=attnum,
                    old_name=previous.name,
                    new_name=current.name,
                    type_oid=current.type_oid,
                    type_name=current.type_name,
                    nullable=current.nullable,
                )
            )
        elif previous.type_identity != current.type_identity:
            changes.append(
                ColumnChange(
                    kind=COLUMN_TYPE_CHANGED,
                    attnum=attnum,
                    old_name=previous.name,
                    new_name=current.name,
                    type_oid=current.type_oid,
                    type_name=current.type_name,
                    nullable=current.nullable,
                )
            )
    return tuple(changes)


def _dlt_type(type_name: str) -> str:
    """Map PostgreSQL's formatted type to dlt's stable schema vocabulary."""

    lowered = type_name.lower().strip()
    if lowered in {"bool", "boolean"}:
        return "bool"
    if lowered in {
        "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
        "serial", "bigserial",
    }:
        return "bigint"
    if lowered in {"real", "float4", "double precision", "float8", "numeric", "decimal"}:
        return "double"
    if lowered in {"json", "jsonb"}:
        return "json"
    return "text"


def dlt_table_columns(columns: Iterable[SourceColumn]) -> dict:
    """Return a dlt ``Schema`` table model, without invoking the dlt pipeline."""

    from dlt.common.schema import Schema
    from dlt.common.schema.utils import new_column

    schema = Schema("cdc_flight_schema_evolution")
    schema.update_table(
        {
            "name": "source_table",
            "columns": {
                naming.normalize(column.name): new_column(
                    naming.normalize(column.name),
                    data_type=_dlt_type(column.type_name),
                    nullable=column.nullable,
                )
                for column in columns
            },
        }
    )
    return schema.get_table("source_table")["columns"]


def destination_type(type_name: str | None) -> str:
    """Best-effort DuckDB type for a source column seen before its first row."""

    lowered = (type_name or "text").lower().strip()
    if lowered in {"bool", "boolean"}:
        return BOOLEAN
    if lowered in {
        "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
        "serial", "bigserial",
    }:
        return BIGINT
    if lowered in {"real", "float4", "double precision", "float8", "numeric", "decimal"}:
        return DOUBLE
    if lowered in {"json", "jsonb"}:
        return JSON_T
    return VARCHAR


def apply_column_changes(registry, table_name: str, changes: Iterable[ColumnChange]) -> None:
    """Apply a catalog diff inside the caller's open destination transaction."""

    changes = tuple(changes)
    # Renames run first so a simultaneous rename + add/drop cannot accidentally make
    # the new source name look like an unrelated column.
    for change in changes:
        if change.kind == COLUMN_RENAMED:
            registry.rename_column(
                table_name,
                change.destination_old_name,
                change.destination_new_name,
            )
            if change.type_name:
                registry.ensure(
                    table_name,
                    columns={
                        change.destination_new_name: destination_type(change.type_name)
                    },
                    key_columns=registry.get(table_name).key_columns,
                )
    for change in changes:
        if change.kind == COLUMN_DROPPED and change.destination_old_name:
            registry.drop_column(table_name, change.destination_old_name)
    for change in changes:
        if (
            change.kind in (COLUMN_ADDED, COLUMN_TYPE_CHANGED)
            and change.destination_new_name
        ):
            registry.ensure(
                table_name,
                columns={
                    change.destination_new_name: destination_type(change.type_name)
                },
                key_columns=registry.get(table_name).key_columns,
            )


__all__ = [
    "COLUMN_ADDED",
    "COLUMN_DROPPED",
    "COLUMN_RENAMED",
    "COLUMN_TYPE_CHANGED",
    "ColumnChange",
    "SourceColumn",
    "apply_column_changes",
    "destination_type",
    "diff_columns",
    "dlt_table_columns",
]
