"""Catalog relation/change state and durable baseline reconstruction.

This module owns the data model of an observation.  ``catalog.py`` coordinates polling
and lifecycle, while this file keeps the state value, its history and restart reader in
one small seam.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

from .destination import CONTROL_SCHEMA
from .errors import SchemaEvolutionRefused
from .machines import (
    ADMISSION_EXTERNAL,
    ADMISSION_PENDING,
    CATALOG_CHANGE,
    CHANGE_MARKED,
    CHANGE_OBSERVED,
    CHANGE_PENDING,
    require_admission_state,
)
from .schema_evolution import ColumnChange, SourceColumn
from .typed_types import SourceTypeDescriptor

CHANGE_DROPPED = "dropped"
CHANGE_RECREATED = "recreated"
CHANGE_UNPUBLISHED = "unpublished"
CHANGE_REPUBLISHED = "republished"
CHANGE_NEW = "new"
CHANGE_SCHEMA = "schema_changed"
DESTRUCTIVE = (CHANGE_DROPPED, CHANGE_RECREATED)
FENCED = (*DESTRUCTIVE, CHANGE_SCHEMA)


class _AdmissionStateUnset:
    """Sentinel for observations that have not come from durable state yet."""


_ADMISSION_STATE_UNSET = _AdmissionStateUnset()


@dataclass(frozen=True)
class SourceRelation:
    schema: str
    table: str
    oid: int
    published: bool
    replica_identity: str
    columns: tuple[SourceColumn, ...] = ()
    # OIDs can be reused after DROP/CREATE.  The source catalog's relfilenode is
    # the physical half of the durable lifecycle token; relation_type_oid completes
    # it for partitioned parents, whose relfilenode is always 0.  Both are nullable
    # only for legacy destination rows and lightweight test doubles.
    relfilenode: int | None = None
    relation_type_oid: int | None = None
    publication_all_tables: bool = False
    is_partition: bool = False
    admission_state: str | _AdmissionStateUnset | None = _ADMISSION_STATE_UNSET
    #: Source LSN boundary after which residual TOAST events were generated under
    #: verified REPLICA IDENTITY FULL.
    full_activation_lsn: int | None = None

    def __post_init__(self) -> None:
        state = self.admission_state
        if state is _ADMISSION_STATE_UNSET:
            state = ADMISSION_EXTERNAL if self.published else ADMISSION_PENDING
        state = require_admission_state(state)
        object.__setattr__(self, "admission_state", state)
        try:
            boundary = int(self.full_activation_lsn) if self.full_activation_lsn is not None else None
        except (TypeError, ValueError):
            boundary = None
        object.__setattr__(
            self,
            "full_activation_lsn",
            boundary if boundary is not None and boundary > 0 else None,
        )

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def toast_policy(self):
        """Current-runtime TOAST route for this catalog epoch."""
        from .toast import classify_relation

        return classify_relation(
            self.qualified,
            self.columns,
            replica_identity=self.replica_identity,
            full_activation_lsn=self.full_activation_lsn,
        )


@dataclass
class CatalogChange:
    kind: str
    schema: str
    table: str
    detected_lsn: int
    detected_at: float = field(default_factory=time.monotonic)
    old_oid: int | None = None
    new_oid: int | None = None
    # Strong generation tokens carried alongside the old integer fields for
    # compatibility with existing audit consumers.
    old_identity: object | None = None
    new_identity: object | None = None
    #: The relation whose destination image this action still represents.  A queued
    #: A->B recreate may be superseded by B->C before its destination action commits;
    #: carrying A here lets a later final drop retain the correct log image rather than
    #: accidentally persisting B as history.
    old_relation: SourceRelation | None = None
    new_relation: SourceRelation | None = None
    column_changes: tuple[ColumnChange, ...] = ()
    deferrals: int = 0
    confirmations: int = 1
    state: str = CHANGE_OBSERVED

    def __post_init__(self) -> None:
        CATALOG_CHANGE.parse(self.state)
        self.history: list[str] = [self.state]

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"

    @property
    def fenced(self) -> bool:
        return CHANGE_MARKED in self.history

    def to(self, state: str) -> None:
        if state == self.state:
            return
        CATALOG_CHANGE.check(self.state, state)
        self.state = state
        self.history.append(state)

    def can(self, state: str) -> bool:
        return self.state == state or CATALOG_CHANGE.allows(self.state, state)

    def context(self) -> dict:
        return {
            "kind": self.kind,
            "table": self.qualified,
            "detected_lsn": self.detected_lsn,
            "old_oid": self.old_oid,
            "new_oid": self.new_oid,
            "old_identity": repr(self.old_identity),
            "new_identity": repr(self.new_identity),
            "columns": [
                {
                    "kind": change.kind,
                    "attnum": change.attnum,
                    "old_name": change.old_name,
                    "new_name": change.new_name,
                }
                for change in self.column_changes
            ],
            "fenced": self.fenced,
            "state": self.state,
            "confirmations": self.confirmations,
        }


def queued(change: CatalogChange) -> CatalogChange:
    if change.state == CHANGE_OBSERVED:
        change.to(CHANGE_PENDING)
    return change


def _missing_value(raw: str | None, type_name: str) -> object | None:
    if raw is None:
        return None
    text = str(raw)
    if text.startswith("{") and text.endswith("}"):
        text = text[1:-1]
    if text == "" or text.upper() == "NULL":
        return None
    lowered = type_name.lower()
    try:
        if lowered in {"smallint", "integer", "bigint", "int2", "int4", "int8"}:
            return int(text)
        if lowered in {
            "real", "double precision", "float4", "float8", "numeric", "decimal"
        }:
            return float(text)
        if lowered in {"boolean", "bool"}:
            return text.lower() in {"t", "true", "1"}
    except ValueError:
        return None
    return text.replace('\\"', '"').replace('\\\\', '\\')


def read_known_relations(con, pipeline: str) -> dict[str, SourceRelation]:
    rows = con.execute(
        f"SELECT source_schema, source_table, relation_oid, relation_filenode, "
        "relation_type_oid, "
        "published, replica_identity, full_activation_lsn, columns_json, admission_state "
        f"FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?",
        [pipeline],
    ).fetchall()
    known: dict[str, SourceRelation] = {}
    for (
        schema,
        table,
        oid,
        relfilenode,
        relation_type_oid,
        published,
        identity,
        full_activation_lsn,
        columns_json,
        admission_state,
    ) in rows:
        try:
            raw_columns = json.loads(columns_json or "[]")
        except (TypeError, ValueError):
            raw_columns = []
        columns = tuple(
            SourceColumn(
                attnum=int(raw["attnum"]),
                name=str(raw["name"]),
                type_oid=int(raw["type_oid"]),
                type_name=str(raw["type_name"]),
                typmod=(int(raw["typmod"]) if raw.get("typmod") is not None else None),
                nullable=bool(raw.get("nullable", True)),
                has_missing_default=bool(raw.get("has_missing_default", False)),
                missing_value=_missing_value(
                    raw.get("missing_value_text"), str(raw["type_name"])
                ),
                attstorage=(str(raw["attstorage"]) if raw.get("attstorage") else None),
                descriptor=_durable_descriptor(raw, schema=schema, table=table),
            )
            for raw in raw_columns
        )
        known[f"{schema}.{table}"] = SourceRelation(
            schema=schema,
            table=table,
            oid=int(oid or 0),
            relfilenode=(int(relfilenode) if relfilenode is not None else None),
            relation_type_oid=(
                int(relation_type_oid) if relation_type_oid is not None else None
            ),
            published=bool(published),
            replica_identity=identity or "d",
            columns=columns,
            # This is a durable read, so NULL must reach the machine boundary and be
            # refused; it is not an observation that may derive a default from `published`.
            admission_state=admission_state,
            full_activation_lsn=(
                int(full_activation_lsn)
                if full_activation_lsn is not None and int(full_activation_lsn) > 0
                else None
            ),
        )
    return known


def _durable_descriptor(raw: dict, *, schema: str, table: str) -> SourceTypeDescriptor:
    """Load only catalog-authoritative descriptors from durable state."""
    serialized = raw.get("descriptor")
    if not serialized:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is incomplete for {schema}.{table}."
            f"{raw.get('name', '<unknown>')}: refusing to infer type "
            f"OID {raw.get('type_oid')} from {raw.get('type_name')!r}",
            source_schema=str(schema),
            source_table=str(table),
            target=f"{schema}.{table}",
        )
    try:
        return SourceTypeDescriptor.from_dict(serialized)
    except (TypeError, ValueError, KeyError) as exc:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is corrupt for {schema}.{table}."
            f"{raw.get('name', '<unknown>')}: refusing to infer type",
            source_schema=str(schema),
            source_table=str(table),
            target=f"{schema}.{table}",
        ) from exc


def seed_from_table_state(con, pipeline: str) -> set[str]:
    rows = con.execute(
        f"SELECT source_schema, source_table FROM {CONTROL_SCHEMA}.table_state "
        "WHERE pipeline = ?",
        [pipeline],
    ).fetchall()
    return {f"{schema}.{table}" for schema, table in rows}
