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
from .errors import SchemaEvolutionRefused
from .typed_types import SourceTypeDescriptor, TypedValueError, native_type

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
    #: PostgreSQL's `atthasmissing`/`attmissingval` evidence for an ADD DEFAULT.
    #: It lets a keyless empty source prove the value without inventing a row join.
    has_missing_default: bool = False
    missing_value: object | None = None
    #: Full recursive source descriptor.  It is persisted in source_relations and
    #: supersedes the legacy OID/name pair for type identity.
    descriptor: SourceTypeDescriptor | None = None
    typmod: int | None = None
    #: PostgreSQL ``pg_attribute.attstorage``.  ``p`` is the only column-level
    #: exclusion from the TOAST classification; table ``reltoastrelid`` is not.
    attstorage: str | None = None

    def __post_init__(self) -> None:
        if self.descriptor is None:
            object.__setattr__(
                self,
                "descriptor",
                descriptor_from_type_name(
                    self.type_name,
                    oid=self.type_oid,
                    typmod=self.typmod,
                    nullable=self.nullable,
                ),
            )

    @property
    def destination_name(self) -> str:
        return naming.normalize(self.name)

    @property
    def type_identity(self) -> str:
        return self.descriptor.fingerprint if self.descriptor is not None else f"{self.type_oid}:{self.type_name.lower()}"


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
    #: True when an attnum-preserving rename also changed its source type.  It is
    #: retained on the rename event so the destination can require a strict, explicit
    #: ALTER rather than silently treating the new baseline as adopted.
    type_changed: bool = False
    old_type_oid: int | None = None
    old_type_name: str | None = None
    old_descriptor: SourceTypeDescriptor | None = None
    new_descriptor: SourceTypeDescriptor | None = None

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
                    new_descriptor=current.descriptor,
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
                    old_descriptor=previous.descriptor,
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
                    type_changed=previous.type_identity != current.type_identity,
                    old_type_oid=previous.type_oid,
                    old_type_name=previous.type_name,
                    old_descriptor=previous.descriptor,
                    new_descriptor=current.descriptor,
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
                    old_type_oid=previous.type_oid,
                    old_type_name=previous.type_name,
                    old_descriptor=previous.descriptor,
                    new_descriptor=current.descriptor,
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
    """Strict destination SQL for a source catalog type."""
    return native_type(descriptor_from_type_name(type_name or "text")).sql


def apply_column_changes(registry, table_name: str, changes: Iterable[ColumnChange]) -> None:
    """Apply a catalog diff inside the caller's open destination transaction."""

    changes = tuple(changes)
    # If a rename targets the physical name of a different attnum that is being
    # dropped in this same catalog observation, drop that *old identity* first.  The
    # alternative (all renames first) turns `a -> b; drop b` into `drop b` after the
    # rename, deleting the newly renamed identity.
    renames = [change for change in changes if change.kind == COLUMN_RENAMED]
    early_drops = [
        change for change in changes
        if change.kind == COLUMN_DROPPED
        and change.destination_old_name in {item.destination_new_name for item in renames}
    ]
    late_drops = [change for change in changes if change not in early_drops]
    for change in (*early_drops, *renames, *late_drops):
        if change.kind == COLUMN_RENAMED:
            registry.rename_column(
                table_name,
                change.destination_old_name,
                change.destination_new_name,
            )
            if change.type_name and change.type_changed:
                if change.old_descriptor is None or change.new_descriptor is None:
                    raise SchemaEvolutionRefused(
                        f"cannot apply rename/type change to {table_name}: source "
                        "descriptor is missing, so the safe widening lattice cannot "
                        "prove a native UNION member",
                        target=table_name,
                        refusal_origin="schema_evolution",
                    )
                old_descriptor = change.old_descriptor or descriptor_from_type_name(
                    change.old_type_name or "text", oid=change.old_type_oid
                )
                new_descriptor = change.new_descriptor or descriptor_from_type_name(
                    change.type_name or "text", oid=change.type_oid
                )
                try:
                    registry.convert_column_to_union(
                        table_name,
                        change.destination_new_name,
                        old_descriptor,
                        new_descriptor,
                    )
                except (TypedValueError, ValueError) as exc:
                    raise SchemaEvolutionRefused(
                        f"cannot change source column {table_name}.{change.destination_new_name}: "
                        f"the source descriptor is not deliverable: {exc}",
                        target=table_name,
                        refusal_origin="schema_evolution",
                    ) from exc
        elif change.kind == COLUMN_DROPPED and change.destination_old_name:
            registry.drop_column(table_name, change.destination_old_name)
    for change in changes:
        if (
            change.kind == COLUMN_ADDED
            and change.destination_new_name
        ):
            descriptor = change.new_descriptor or descriptor_from_type_name(
                change.type_name or "text", oid=change.type_oid
            )
            try:
                registry.ensure_typed(
                    table_name,
                    columns={change.destination_new_name: descriptor},
                    key_columns=registry.get(table_name).key_columns,
                )
            except (TypedValueError, ValueError) as exc:
                raise SchemaEvolutionRefused(
                    f"cannot add source column {table_name}.{change.destination_new_name}: "
                    f"the source descriptor is not deliverable: {exc}",
                    target=table_name,
                    refusal_origin="schema_evolution",
                ) from exc
        elif change.kind == COLUMN_TYPE_CHANGED and change.destination_new_name:
            old_descriptor = change.old_descriptor or descriptor_from_type_name(
                change.old_type_name or "text", oid=change.old_type_oid
            )
            new_descriptor = change.new_descriptor or descriptor_from_type_name(
                change.type_name or "text", oid=change.type_oid
            )
            try:
                registry.convert_column_to_union(
                    table_name,
                    change.destination_new_name,
                    old_descriptor,
                    new_descriptor,
                )
            except (TypedValueError, ValueError) as exc:
                raise SchemaEvolutionRefused(
                    f"cannot change source column {table_name}.{change.destination_new_name}: "
                    f"the source descriptor is not deliverable: {exc}",
                    target=table_name,
                    refusal_origin="schema_evolution",
                ) from exc


def descriptor_from_type_name(
    type_name: str,
    *,
    oid: int | None = None,
    typmod: int | None = None,
    nullable: bool = True,
) -> SourceTypeDescriptor:
    """Construct a catalog descriptor when a targeted OID read is unavailable.

    The catalog watcher normally supplies the richer recursive descriptor.  This
    conservative parser covers the stable formatted names used by unit callers and
    never turns an unknown name into a native type: ``native_type`` will refuse it.
    """
    text = str(type_name or "text").strip()
    if text.endswith("[]"):
        element = descriptor_from_type_name(text[:-2], oid=None, nullable=nullable)
        return SourceTypeDescriptor(oid, text, "array", typmod=typmod, array_element=element, nullable=nullable)
    lowered = text.lower()
    kind = lowered
    precision = scale = None
    if lowered.startswith(("numeric(", "decimal(")):
        base, _, args = lowered.partition("(")
        values = args.rstrip(")").split(",")
        kind = base
        precision = int(values[0]) if values and values[0].strip().isdigit() else None
        scale = int(values[1]) if len(values) > 1 and values[1].strip().lstrip("-").isdigit() else None
    aliases = {
        "smallint": "int2", "integer": "int4", "int": "int4", "bigint": "int8",
        "real": "float4", "double precision": "float8", "boolean": "bool",
        "character varying": "varchar", "character": "char",
    }
    return SourceTypeDescriptor(
        oid=oid,
        qualified_name=text,
        kind=aliases.get(kind, kind),
        typmod=typmod,
        precision=precision,
        scale=scale,
        nullable=nullable,
    )


__all__ = [
    "COLUMN_ADDED",
    "COLUMN_DROPPED",
    "COLUMN_RENAMED",
    "COLUMN_TYPE_CHANGED",
    "ColumnChange",
    "SourceColumn",
    "apply_column_changes",
    "descriptor_from_type_name",
    "destination_type",
    "diff_columns",
    "dlt_table_columns",
]
