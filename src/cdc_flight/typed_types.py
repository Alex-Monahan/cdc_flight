"""The source-descriptor and native-value contract for rubric 2.4/2.5.

Destination types are never inferred from whichever Python value happens to be
observed first.  That cannot represent an empty array, an all-NULL column, a logical
Connect value, or a source type boundary.  This module is intentionally a small value
boundary: catalog facts and Connect schemas become immutable source descriptors, one
recursive resolver returns the destination type, and one encoder turns values into
values that the destination can bind without a lossy fallback.

There is no per-type builder registry here.  The recursive dispatch is the same
contract used by DML, snapshot/backfill, UNION conversion, and spill codecs.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import ipaddress
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any
from uuid import UUID


class UnsupportedType(ValueError):
    """A source type without an allowlisted, native destination representation."""


class InvalidTypedValue(ValueError):
    """A value that cannot be represented by its declared source descriptor."""


class FieldState(StrEnum):
    VALUE = "value"
    EXPLICIT_NULL = "explicit_null"
    UNCHANGED_TOAST = "unchanged_toast"
    ABSENT = "absent"


class CanonicalRangeText(str):
    """PostgreSQL ``range_out`` text retained from a source change event.

    A plain string is still accepted by the compatibility/value boundary because
    destination readback and older callers do not carry provenance.  This narrow
    subtype is used only after catalog descriptors have identified a source range;
    the identity codec can therefore use PostgreSQL's already-canonical text without
    parsing and re-serializing it in Python.
    """


class OpaqueText(str):
    """Text already accepted by the opaque transport boundary.

    This provenance marker prevents a decoded value from being interpreted as a
    second base64 payload when one commit group crosses the fold/spill/bind seams.
    It carries no PostgreSQL grammar or normalization semantics.
    """


@dataclass(frozen=True, slots=True)
class PostgresInfinity:
    """A PostgreSQL timestamp/date infinity endpoint for native binding."""

    positive: bool

    def __str__(self) -> str:
        return "infinity" if self.positive else "-infinity"


@dataclass(frozen=True)
class JsonbNull:
    """A JSONB document whose root value is JSON ``null``.

    PostgreSQL JSONB's JSON-null document is distinct from a SQL NULL before it
    reaches DuckDB.  Keeping that distinction in the typed value boundary lets
    nested encoders and UNION member binders choose the native VARIANT form
    explicitly.  DuckDB 1.5.4 currently exposes both forms as VARIANT_NULL when
    read back; the distinction is therefore intentionally not collapsed in spill
    serialization before the destination bind.
    """


JSONB_NULL = JsonbNull()


def _freeze_pairs(value: Mapping[str, Any] | None) -> tuple[tuple[str, str], ...]:
    if not value:
        return ()
    return tuple(sorted((str(key), str(item)) for key, item in value.items()))


@dataclass(frozen=True)
class SourceTypeDescriptor:
    """An immutable, recursively serializable source type description.

    ``kind`` is the PostgreSQL/Connect semantic kind, not a Python runtime type.
    The catalog supplies OID/name/typmod facts while the Connect converter supplies
    logical schema facts.  Both are retained in the fingerprint so a domain, enum
    label change, typmod change, or nested field change cannot reuse an old UNION
    member accidentally.
    """

    oid: int | None
    qualified_name: str
    kind: str
    typmod: int | None = None
    precision: int | None = None
    scale: int | None = None
    domain_base: SourceTypeDescriptor | None = None
    array_element: SourceTypeDescriptor | None = None
    map_key: SourceTypeDescriptor | None = None
    map_value: SourceTypeDescriptor | None = None
    enum_labels: tuple[str, ...] = ()
    composite_fields: tuple[tuple[str, SourceTypeDescriptor], ...] = ()
    range_subtype: SourceTypeDescriptor | None = None
    extension: str | None = None
    connect_name: str | None = None
    connect_parameters: tuple[tuple[str, str], ...] = ()
    nullable: bool = True
    metadata: tuple[tuple[str, str], ...] = ()
    _fingerprint_cache: str | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "qualified_name", str(self.qualified_name or "unknown"))
        object.__setattr__(self, "kind", _kind_name(self.kind, self.qualified_name))
        object.__setattr__(self, "enum_labels", tuple(str(item) for item in self.enum_labels))
        object.__setattr__(
            self,
            "composite_fields",
            tuple((str(name), descriptor) for name, descriptor in self.composite_fields),
        )
        object.__setattr__(self, "connect_parameters", _pairs(self.connect_parameters))
        object.__setattr__(self, "metadata", _pairs(self.metadata))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> SourceTypeDescriptor:
        """Build a descriptor from catalog JSON or its own serialized form."""

        raw_fields = value.get("composite_fields", value.get("fields", ())) or ()
        fields: list[tuple[str, SourceTypeDescriptor]] = []
        for item in raw_fields:
            if isinstance(item, Mapping):
                name = item.get("name", item.get("field"))
                raw_descriptor = item.get("descriptor", item.get("schema", item))
                if name is not None:
                    fields.append((str(name), _descriptor_from_any(raw_descriptor)))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                fields.append((str(item[0]), _descriptor_from_any(item[1])))

        def child(*names: str) -> SourceTypeDescriptor | None:
            for name in names:
                raw = value.get(name)
                if raw is not None:
                    return _descriptor_from_any(raw)
            return None

        return cls(
            oid=_as_int(value.get("oid", value.get("type_oid"))),
            qualified_name=str(
                value.get("qualified_name", value.get("type_name", value.get("name", "unknown")))
            ),
            kind=str(value.get("kind", value.get("type_kind", value.get("type", "unknown")))),
            typmod=_as_int(value.get("typmod", value.get("type_mod"))),
            precision=_as_int(value.get("precision")),
            scale=_as_int(value.get("scale")),
            domain_base=child("domain_base", "base", "typbasetype"),
            array_element=child("array_element", "element", "typelem"),
            map_key=child("map_key", "key_schema"),
            map_value=child("map_value", "map_value", "value_schema"),
            enum_labels=tuple(value.get("enum_labels", value.get("labels", ())) or ()),
            composite_fields=tuple(fields),
            range_subtype=child("range_subtype", "subtype", "rngsubtype"),
            extension=(str(value["extension"]) if value.get("extension") is not None else None),
            connect_name=(
                str(value["connect_name"])
                if value.get("connect_name") is not None
                else (str(value["name"]) if str(value.get("name", "")).startswith("io.") else None)
            ),
            connect_parameters=_pairs(value.get("connect_parameters", value.get("parameters"))),
            nullable=bool(value.get("nullable", True)),
            metadata=_pairs(value.get("metadata")),
        )

    @classmethod
    def from_catalog(cls, value: Mapping[str, Any]) -> SourceTypeDescriptor:
        return cls.from_dict(value)

    @classmethod
    def from_connect_schema(
        cls, schema: Mapping[str, Any]
    ) -> SourceTypeDescriptor:
        """Translate a schema-enabled JSON converter schema into source facts."""

        if not schema:
            raise ValueError("a Connect schema is required for a source descriptor")
        if "schema" in schema and isinstance(schema.get("schema"), Mapping):
            schema = schema["schema"]
        raw_type = str(schema.get("type", "unknown")).lower()
        logical = str(schema.get("name", ""))
        logical_lower = logical.lower()
        parameters = schema.get("parameters") or {}

        if raw_type == "array":
            return cls(
                oid=None,
                qualified_name=logical or "connect.array",
                kind="array",
                array_element=cls.from_connect_schema(schema.get("items", schema.get("value_schema"))),
                connect_name=logical or None,
                connect_parameters=_pairs(parameters),
                nullable=bool(schema.get("optional", True)),
            )
        if raw_type == "map":
            return cls(
                oid=None,
                qualified_name=logical or "connect.map",
                kind="map",
                map_key=cls.from_connect_schema(schema.get("keys", schema.get("key_schema"))),
                map_value=cls.from_connect_schema(schema.get("values", schema.get("value_schema"))),
                connect_name=logical or None,
                connect_parameters=_pairs(parameters),
                nullable=bool(schema.get("optional", True)),
            )
        if raw_type == "struct":
            fields: list[tuple[str, SourceTypeDescriptor]] = []
            for field_schema in schema.get("fields", ()) or ():
                if not isinstance(field_schema, Mapping):
                    continue
                field_name = field_schema.get("field", field_schema.get("name"))
                if field_name is None:
                    continue
                nested = field_schema.get("schema")
                if nested is None and "type" in field_schema:
                    nested = field_schema
                fields.append((str(field_name), cls.from_connect_schema(nested)))
            kind = "struct"
            if "variable" in logical_lower and "decimal" in logical_lower:
                kind = "numeric_variable"
            elif "point" in logical_lower:
                kind = "point"
            return cls(
                oid=None,
                qualified_name=logical or "connect.struct",
                kind=kind,
                composite_fields=tuple(fields),
                connect_name=logical or None,
                connect_parameters=_pairs(parameters),
                nullable=bool(schema.get("optional", True)),
            )

        kind = _connect_kind(raw_type, logical_lower)
        precision = _as_int(parameters.get("connect.decimal.precision", parameters.get("precision")))
        scale = _as_int(parameters.get("scale"))
        enum_labels = _connect_enum_labels(schema, parameters)
        return cls(
            oid=None,
            qualified_name=logical or f"connect.{raw_type}",
            kind=kind,
            precision=precision,
            scale=scale,
            enum_labels=enum_labels,
            connect_name=logical or None,
            connect_parameters=_pairs(parameters),
            nullable=bool(schema.get("optional", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "oid": self.oid,
            "qualified_name": self.qualified_name,
            "kind": self.kind,
            "typmod": self.typmod,
            "precision": self.precision,
            "scale": self.scale,
            "domain_base": self.domain_base.to_dict() if self.domain_base else None,
            "array_element": self.array_element.to_dict() if self.array_element else None,
            "map_key": self.map_key.to_dict() if self.map_key else None,
            "map_value": self.map_value.to_dict() if self.map_value else None,
            "enum_labels": list(self.enum_labels),
            "composite_fields": [
                {"name": name, "descriptor": descriptor.to_dict()}
                for name, descriptor in self.composite_fields
            ],
            "range_subtype": self.range_subtype.to_dict() if self.range_subtype else None,
            "extension": self.extension,
            "connect_name": self.connect_name,
            "connect_parameters": dict(self.connect_parameters),
            "nullable": self.nullable,
            "metadata": dict(self.metadata),
        }

    @property
    def fingerprint(self) -> str:
        if self._fingerprint_cache is not None:
            return self._fingerprint_cache
        payload = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        object.__setattr__(self, "_fingerprint_cache", fingerprint)
        return fingerprint

    @property
    def type_identity(self) -> str:
        return self.fingerprint


def mark_canonical_range_text(
    value: Any, descriptor: SourceTypeDescriptor | None
) -> Any:
    """Mark source range text after catalog descriptors are authoritative.

    Debezium's supported PostgreSQL range fields are STRING values containing the
    server's ``range_out`` result.  The catalog is the only place that tells us that
    a flattened string is a range rather than ordinary text, so marking belongs at
    that enrichment seam.  Composite/array/map recursion keeps nested keys on the
    same source-text path.  Structured mappings and list-valued compatibility input
    are intentionally left alone: those are destination/value-boundary forms.
    """

    if descriptor is None or value is None:
        return value
    source = descriptor
    seen: set[int] = set()
    while source.domain_base is not None and id(source) not in seen:
        seen.add(id(source))
        source = source.domain_base
    kind = str(source.kind or source.qualified_name).lower()
    range_kinds = {
        "range", "daterange", "int4range", "int8range", "numrange",
        "tsrange", "tstzrange",
    }
    if kind in range_kinds and isinstance(value, str):
        return value if isinstance(value, CanonicalRangeText) else CanonicalRangeText(value)
    if kind == "multirange" and isinstance(value, str):
        return canonical_multirange_text(value, source)
    if kind in {"struct", "composite"} and isinstance(value, Mapping):
        return {
            name: mark_canonical_range_text(
                item,
                dict(source.composite_fields).get(str(name)),
            )
            for name, item in value.items()
        }
    if kind == "array" and isinstance(value, (list, tuple)):
        return [mark_canonical_range_text(item, source.array_element) for item in value]
    if kind == "map" and isinstance(value, Mapping):
        return {
            mark_canonical_range_text(key, source.map_key): mark_canonical_range_text(
                item, source.map_value
            )
            for key, item in value.items()
        }
    return value


@dataclass(frozen=True)
class NativeMember:
    name: str
    type: NativeType
    descriptor: SourceTypeDescriptor | None = None

    @property
    def fingerprint(self) -> str:
        return self.descriptor.fingerprint if self.descriptor else self.type.fingerprint


@dataclass(frozen=True)
class NativeType:
    """A recursive destination type used for SQL, encoding, and comparisons."""

    kind: str
    sql: str
    source: SourceTypeDescriptor | None = None
    children: tuple[NativeType, ...] = ()
    fields: tuple[tuple[str, NativeType], ...] = ()
    members: tuple[NativeMember, ...] = ()
    key: NativeType | None = None
    value: NativeType | None = None
    indexable: bool = True

    @property
    def fingerprint(self) -> str:
        payload = {
            "kind": self.kind,
            "sql": self.sql,
            "source": self.source.fingerprint if self.source else None,
            "children": [child.fingerprint for child in self.children],
            "fields": [(name, child.fingerprint) for name, child in self.fields],
            "members": [(member.name, member.fingerprint, member.type.sql) for member in self.members],
            "key": self.key.fingerprint if self.key else None,
            "value": self.value.fingerprint if self.value else None,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class UnionValue:
    """A value plus the exact UNION member it belongs to."""

    member: str
    value: Any
    native: NativeType | None = None


@dataclass(frozen=True)
class FieldValue:
    state: FieldState
    value: Any = None
    descriptor: SourceTypeDescriptor | None = None

    @classmethod
    def of(cls, value: Any, descriptor: SourceTypeDescriptor | None = None) -> FieldValue:
        if value is None:
            return cls(FieldState.EXPLICIT_NULL, descriptor=descriptor)
        return cls(FieldState.VALUE, value=value, descriptor=descriptor)

    @classmethod
    def explicit_null(cls, descriptor: SourceTypeDescriptor | None = None) -> FieldValue:
        return cls(FieldState.EXPLICIT_NULL, descriptor=descriptor)

    @classmethod
    def unchanged_toast(cls, descriptor: SourceTypeDescriptor | None = None) -> FieldValue:
        # The row-patch boundary owns this disposition; it is never a bindable value.
        return cls(FieldState.UNCHANGED_TOAST, descriptor=descriptor)

    @classmethod
    def absent(cls, descriptor: SourceTypeDescriptor | None = None) -> FieldValue:
        return cls(FieldState.ABSENT, descriptor=descriptor)

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "value": _jsonable(self.value),
            "descriptor": self.descriptor.to_dict() if self.descriptor else None,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FieldValue:
        state = FieldState(str(value.get("state", FieldState.ABSENT.value)))
        descriptor = (
            SourceTypeDescriptor.from_dict(value["descriptor"])
            if value.get("descriptor")
            else None
        )
        return cls(state, _from_jsonable(value.get("value")), descriptor)


@dataclass(frozen=True)
class TypedImage:
    """A field-presence map for a before, after, or key image."""

    fields: tuple[tuple[str, FieldValue], ...] = ()

    @classmethod
    def from_mapping(
        cls,
        image: Mapping[str, Any] | None,
        descriptors: Mapping[str, SourceTypeDescriptor] | None = None,
    ) -> TypedImage:
        descriptors = descriptors or {}
        values = image or {}
        return cls(
            tuple(
                (str(name), FieldValue.of(value, descriptors.get(str(name))))
                for name, value in values.items()
            )
        )

    def field(self, name: str) -> FieldValue:
        for field_name, value in self.fields:
            if field_name == name:
                return value
        return FieldValue.absent()

    def to_dict(self) -> dict[str, Any]:
        return {"fields": {name: value.to_dict() for name, value in self.fields}}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> TypedImage:
        raw = value.get("fields", value)
        return cls(tuple((str(name), FieldValue.from_dict(item)) for name, item in raw.items()))


def native_type(
    source: SourceTypeDescriptor | NativeType, *, for_key: bool = False
) -> NativeType:
    """Resolve one source descriptor recursively.

    ``for_key`` is the one destination-independent identity decision.  PostgreSQL
    JSONB values use VARIANT for ordinary columns, while a JSONB source key uses
    JSON because DuckDB does not permit VARIANT in an index key.  The source
    descriptor remains JSONB in both cases, so this is a representation choice,
    not a type inference or a lossy fallback.
    """

    if isinstance(source, NativeType):
        return source
    descriptor = source
    kind = _kind_name(descriptor.kind, descriptor.qualified_name)
    if kind == "domain":
        if descriptor.domain_base is None:
            raise UnsupportedType(f"domain {descriptor.qualified_name} has no base descriptor")
        base = native_type(descriptor.domain_base, for_key=for_key)
        return NativeType(base.kind, base.sql, descriptor, base.children, base.fields, base.members, base.key, base.value, base.indexable)
    if kind in {"smallint", "int2", "smallserial"}:
        return NativeType("SMALLINT", "SMALLINT", descriptor)
    if kind in {"integer", "int", "int4", "serial"}:
        return NativeType("INTEGER", "INTEGER", descriptor)
    if kind in {"bigint", "int8", "bigserial", "oid", "xid"}:
        return NativeType("BIGINT", "BIGINT", descriptor)
    if kind in {"real", "float4"}:
        return NativeType("FLOAT", "FLOAT", descriptor)
    if kind in {"double", "float8", "double precision"}:
        return NativeType("DOUBLE", "DOUBLE", descriptor)
    if kind in {"boolean", "bool", "bit1"}:
        return NativeType("BOOLEAN", "BOOLEAN", descriptor)
    if kind in {"char", "bpchar", "varchar", "text", "citext", "name", "string"}:
        return NativeType("VARCHAR", "VARCHAR", descriptor)
    if kind in {"bytea", "bytes", "blob"}:
        return NativeType("BLOB", "BLOB", descriptor)
    if kind in {"numeric", "decimal"}:
        if descriptor.precision is not None and descriptor.precision <= 38:
            precision = descriptor.precision
            scale = descriptor.scale or 0
            finite = NativeType("DECIMAL", f"DECIMAL({precision},{scale})", descriptor)
            special = NativeType("DOUBLE", "DOUBLE", descriptor)
            return NativeType(
                "NUMERIC_UNION",
                f"UNION(finite {finite.sql},special {special.sql})",
                descriptor,
                members=(
                    NativeMember("finite", finite, descriptor),
                    NativeMember("special", special, descriptor),
                ),
                indexable=False,
            )
        return _variable_numeric(descriptor)
    if kind in {"numeric_variable", "variable_scale_numeric"}:
        return _variable_numeric(descriptor)
    if kind in {"date"}:
        return NativeType("DATE", "DATE", descriptor)
    if kind in {"time", "time_microseconds", "microtime"}:
        return NativeType("TIME", "TIME", descriptor)
    if kind in {"timestamp", "timestamp_microseconds", "microtimestamp"}:
        return NativeType("TIMESTAMP", "TIMESTAMP", descriptor)
    if kind in {"timestamptz", "zonedtimestamp"}:
        return NativeType("TIMESTAMPTZ", "TIMESTAMPTZ", descriptor)
    if kind in {"timetz", "zonedtime"}:
        return NativeType("TIMETZ", "TIMETZ", descriptor)
    if kind in {"interval"}:
        # DuckDB accepts INTERVAL values but cannot use INTERVAL in an index/PRIMARY
        # KEY. Keep the native value column and route source-key identity through the
        # canonical codec, just like LIST/STRUCT/MAP and variable NUMERIC.
        return NativeType("INTERVAL", "INTERVAL", descriptor, indexable=False)
    if kind in {"uuid"}:
        return NativeType("UUID", "UUID", descriptor)
    if kind == "json":
        return NativeType("JSON", "JSON", descriptor)
    if kind == "jsonb":
        if for_key:
            return NativeType("JSON", "JSON", descriptor)
        return NativeType("VARIANT", "VARIANT", descriptor)
    if kind == "array":
        if descriptor.array_element is None:
            raise UnsupportedType(f"array {descriptor.qualified_name} has no element descriptor")
        child = native_type(descriptor.array_element, for_key=for_key)
        return NativeType("LIST", f"{child.sql}[]", descriptor, children=(child,), indexable=False)
    if kind in {"struct", "composite"}:
        fields = tuple(
            (name, native_type(child, for_key=for_key))
            for name, child in descriptor.composite_fields
        )
        if not fields:
            raise UnsupportedType(f"composite {descriptor.qualified_name} has no fields")
        sql = "STRUCT(" + ",".join(f"{_quote_identifier(name)} {child.sql}" for name, child in fields) + ")"
        return NativeType("STRUCT", sql, descriptor, fields=fields, indexable=False)
    if kind == "map":
        if descriptor.map_key is None or descriptor.map_value is None:
            raise UnsupportedType(f"map {descriptor.qualified_name} has incomplete key/value descriptors")
        key = native_type(descriptor.map_key, for_key=for_key)
        value = native_type(descriptor.map_value, for_key=for_key)
        return NativeType("MAP", f"MAP({key.sql},{value.sql})", descriptor, key=key, value=value, indexable=False)
    if kind == "enum":
        if not descriptor.enum_labels:
            raise UnsupportedType(f"enum {descriptor.qualified_name} has no labels")
        labels = ",".join("'" + label.replace("'", "''") + "'" for label in descriptor.enum_labels)
        return NativeType("ENUM", f"ENUM({labels})", descriptor)
    if kind == "point":
        fields = (("x", NativeType("DOUBLE", "DOUBLE", descriptor)), ("y", NativeType("DOUBLE", "DOUBLE", descriptor)))
        return NativeType("STRUCT", "STRUCT(x DOUBLE,y DOUBLE)", descriptor, fields=fields, indexable=False)
    if kind in {"geometry", "geography", "postgis"}:
        fields = (("srid", NativeType("INTEGER", "INTEGER", descriptor)), ("wkb", NativeType("BLOB", "BLOB", descriptor)))
        return NativeType("STRUCT", "STRUCT(srid INTEGER,wkb BLOB)", descriptor, fields=fields, indexable=False)
    if kind in {"range", "daterange", "int4range", "int8range", "numrange", "tsrange", "tstzrange"}:
        if descriptor.range_subtype is None:
            raise UnsupportedType(f"range {descriptor.qualified_name} has no subtype descriptor")
        subtype = native_type(descriptor.range_subtype, for_key=for_key)
        fields = (
            ("is_empty", NativeType("BOOLEAN", "BOOLEAN", descriptor)),
            ("lower", subtype),
            ("upper", subtype),
            ("lower_inclusive", NativeType("BOOLEAN", "BOOLEAN", descriptor)),
            ("upper_inclusive", NativeType("BOOLEAN", "BOOLEAN", descriptor)),
        )
        sql = "STRUCT(" + ",".join(f"{name} {child.sql}" for name, child in fields) + ")"
        return NativeType("STRUCT", sql, descriptor, fields=fields, indexable=False)
    if kind == "multirange":
        if descriptor.range_subtype is None:
            raise UnsupportedType(f"multirange {descriptor.qualified_name} has no range subtype")
        # Stock Debezium emits this unknown JDBC type as opaque PostgreSQL text;
        # local DuckDB and MotherDuck share one indexable VARCHAR representation.
        return NativeType("VARCHAR", "VARCHAR", descriptor)
    if kind in {"bit", "varbit"}:
        fields = (("bits", NativeType("BLOB", "BLOB", descriptor)), ("bit_length", NativeType("INTEGER", "INTEGER", descriptor)))
        return NativeType("STRUCT", "STRUCT(bits BLOB,bit_length INTEGER)", descriptor, fields=fields, indexable=False)
    if kind in {"vector", "halfvec"}:
        child = NativeType("DOUBLE", "DOUBLE", descriptor)
        return NativeType("LIST", "DOUBLE[]", descriptor, children=(child,), indexable=False)
    if kind == "sparsevec":
        fields = (
            ("dimensions", NativeType("SMALLINT", "SMALLINT", descriptor)),
            ("vector", NativeType("MAP", "MAP(SMALLINT,DOUBLE)", descriptor, indexable=False)),
        )
        return NativeType("STRUCT", "STRUCT(dimensions SMALLINT,vector MAP(SMALLINT,DOUBLE))", descriptor, fields=fields, indexable=False)
    if kind in _OPAQUE_TEXT_KINDS and _opaque_descriptor_allowed(descriptor, kind):
        return NativeType("VARCHAR", "VARCHAR", descriptor)
    raise UnsupportedType(
        f"source type {descriptor.qualified_name!r} (kind={descriptor.kind!r}, oid={descriptor.oid}) "
        "has no verified native destination representation"
    )


def union_member_name(descriptor: SourceTypeDescriptor | NativeType) -> str:
    fingerprint = descriptor.fingerprint
    return "m_" + fingerprint[:16]


def union_type(descriptors: tuple[SourceTypeDescriptor, ...] | list[SourceTypeDescriptor]) -> NativeType:
    """Build one physical UNION, deduplicating members by source fingerprint."""

    members: list[NativeMember] = []
    seen: set[str] = set()
    for descriptor in descriptors:
        if descriptor.fingerprint in seen:
            continue
        seen.add(descriptor.fingerprint)
        members.append(NativeMember(union_member_name(descriptor), native_type(descriptor), descriptor))
    if len(members) < 2:
        raise ValueError("a UNION requires at least two distinct source descriptors")
    sql = "UNION(" + ",".join(f"{member.name} {member.type.sql}" for member in members) + ")"
    return NativeType("UNION", sql, members=tuple(members), indexable=False)


def numeric_value(value: Any, descriptor: SourceTypeDescriptor | None = None) -> UnionValue:
    """Encode finite numeric text as DECIMAL and specials as DOUBLE."""

    if isinstance(value, UnionValue):
        return value
    if isinstance(value, float) and math.isnan(value):
        return UnionValue("special", value)
    if isinstance(value, float) and math.isinf(value):
        return UnionValue("special", value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"nan", "+nan", "-nan"}:
            return UnionValue("special", math.nan)
        if lowered in {"infinity", "+infinity", "inf", "+inf"}:
            return UnionValue("special", math.inf)
        if lowered in {"-infinity", "-inf"}:
            return UnionValue("special", -math.inf)
    try:
        return UnionValue("finite", value if isinstance(value, Decimal) else Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidTypedValue(f"{value!r} is not a PostgreSQL numeric value") from exc


def encode_value(value: Any, descriptor: SourceTypeDescriptor | NativeType) -> Any:
    """Encode a value strictly according to its declared descriptor."""

    if value is None:
        return None
    target = native_type(descriptor)
    source = descriptor.source if isinstance(descriptor, NativeType) else descriptor
    kind = _kind_name(source.kind, source.qualified_name) if source else target.kind.lower()

    if kind == "domain" and source.domain_base is not None:
        return encode_value(value, source.domain_base)
    if kind in {"numeric", "decimal"} and target.kind == "NUMERIC_UNION":
        return numeric_value(value, source)
    if kind in {"numeric_variable", "variable_scale_numeric"} or target.kind == "NUMERIC_VARIABLE":
        return _encode_variable_numeric(value)
    if kind in {"smallint", "int2", "smallserial", "integer", "int", "int4", "serial", "bigint", "int8", "bigserial", "oid", "xid"}:
        try:
            integer = int(value)
        except (TypeError, ValueError) as exc:
            raise InvalidTypedValue(f"{value!r} is not an integer") from exc
        limits = (-(2**15), 2**15 - 1) if kind in {"smallint", "int2", "smallserial"} else ((-(2**31), 2**31 - 1) if kind in {"integer", "int", "int4", "serial"} else (-(2**63), 2**63 - 1))
        if not limits[0] <= integer <= limits[1]:
            raise InvalidTypedValue(f"{integer} is outside {kind} range")
        return integer
    if kind in {"real", "float4", "double", "float8", "double precision"}:
        if isinstance(value, str):
            return _float_text(value)
        return float(value)
    if kind in {"boolean", "bool", "bit1"}:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered not in {"true", "false", "t", "f", "1", "0"}:
                raise InvalidTypedValue(f"{value!r} is not a boolean")
            return lowered in {"true", "t", "1"}
        return bool(value)
    if kind in {"char", "bpchar", "varchar", "text", "citext", "name", "string"}:
        return str(value)
    if kind in {"bytea", "bytes", "blob"}:
        return _decode_bytes(value)
    if kind == "array":
        if not isinstance(value, (list, tuple)):
            raise InvalidTypedValue(f"{value!r} is not an array")
        if source.array_element is None:
            raise UnsupportedType(f"array {source.qualified_name} has no element descriptor")
        return [encode_value(item, source.array_element) for item in value]
    if kind in {"struct", "composite", "point", "geometry", "geography", "postgis"}:
        return _encode_struct(value, source)
    if kind == "map":
        if not isinstance(value, Mapping):
            raise InvalidTypedValue(f"{value!r} is not a map")
        if source.map_key is None or source.map_value is None:
            raise UnsupportedType(f"map {source.qualified_name} has no key/value descriptor")
        return {
            encode_value(key, source.map_key): encode_value(item, source.map_value)
            for key, item in value.items()
        }
    if kind == "enum":
        text = str(value)
        if source.enum_labels and text not in source.enum_labels:
            raise InvalidTypedValue(f"enum value {text!r} is not in {source.enum_labels!r}")
        return text
    if kind in {"date"}:
        return _date_value(value)
    if kind in {"time", "time_microseconds", "microtime"}:
        return _time_value(value)
    if kind in {"timestamp", "timestamp_microseconds", "microtimestamp", "timestamptz", "zonedtimestamp"}:
        return _datetime_value(value, zoned=kind in {"timestamptz", "zonedtimestamp"})
    if kind in {"timetz", "zonedtime"}:
        return _time_value(value, preserve_zone=True)
    if kind == "interval":
        return _interval_value(value)
    if kind == "uuid":
        try:
            return str(value if isinstance(value, UUID) else UUID(str(value)))
        except (ValueError, AttributeError) as exc:
            raise InvalidTypedValue(f"{value!r} is not a UUID") from exc
    if kind == "json":
        return _encode_json(value, jsonb=False)
    if kind == "jsonb":
        return _encode_json(value, jsonb=True)
    if kind in {"bit", "varbit"}:
        return _encode_bits(value, source)
    if kind in {"range", "daterange", "int4range", "int8range", "numrange", "tsrange", "tstzrange"}:
        return _encode_range(value, source)
    if kind == "multirange":
        if isinstance(value, CanonicalRangeText):
            return str(value)
        if isinstance(value, str):
            value = _multirange_parts(value)
        if not isinstance(value, (list, tuple)):
            raise InvalidTypedValue(f"{value!r} is not a multirange value")
        if source.range_subtype is None:
            raise UnsupportedType(f"multirange {source.qualified_name} has no range subtype")
        return [encode_value(item, source.range_subtype) for item in value]
    if kind == "xml":
        # PostgreSQL's xml_out is the source boundary: its default version=1.0
        # declaration is already absent from SELECT/COPY/format('%s', value), and
        # stock Debezium delivers that same text.  Admit the opaque output bytes;
        # the output-function corpus proves the normalization on both runtimes.
        return _decode_opaque_text(value, source)
    if kind == "money":
        return _money_output_text(value, source)
    if kind in {"inet", "cidr"}:
        # Debezium's wire value is text, but the catalog ADD-column backfill uses
        # psycopg's native ipaddress objects.  Their ``str`` spelling is PostgreSQL's
        # output-function spelling: an IPv4Address has no synthetic /32, while an
        # IPv4Interface retains an explicit prefix.  Do not route these through the
        # old ``::text`` oracle.
        if isinstance(
            value,
            (
                ipaddress.IPv4Address,
                ipaddress.IPv6Address,
                ipaddress.IPv4Interface,
                ipaddress.IPv6Interface,
                ipaddress.IPv4Network,
                ipaddress.IPv6Network,
            ),
        ):
            return OpaqueText(str(value))
        return _decode_opaque_text(value, source)
    if kind in _OPAQUE_TEXT_KINDS:
        if not _opaque_descriptor_allowed(source, kind):
            raise UnsupportedType(
                f"source type {source.qualified_name!r} (kind={source.kind!r}, oid={source.oid}) "
                "is not an allowlisted opaque PostgreSQL type"
            )
        return _decode_opaque_text(value, source)
    if kind in _OBSCURE_TEXT_KINDS:
        raise UnsupportedType(
            f"source type {source.qualified_name!r} (kind={source.kind!r}, oid={source.oid}) "
            "has no verified value codec"
        )
    if target.kind == "VARCHAR" and source is None:
        return str(value)
    return value


def adapt_value(value: Any, target: NativeType) -> Any:
    """Adapt a value to a native target exactly once.

    UNION values are already wire/tagged values. Keeping them unchanged here
    is important because this adapter is shared by inserts, updates, replay,
    spill and the assignment seam; re-wrapping one changes a numeric special
    into ``finite(special)`` and makes DuckDB reject the assignment.
    """

    if value is None or isinstance(value, UnionValue):
        return value
    source = target.source
    if source is None:
        return value
    if (
        _kind_name(source.kind, source.qualified_name) == "multirange"
        and target.kind == "VARCHAR"
    ):
        return canonical_multirange_text(value, source)
    encoded = encode_value(value, source)
    if target.kind == "NUMERIC_UNION":
        if isinstance(encoded, UnionValue):
            return encoded
        return UnionValue("finite", encoded, native=native_type(source))
    if target.kind == "UNION":
        return UnionValue(union_member_name(source), encoded, native=native_type(source))
    return encoded


def _variable_numeric(value: SourceTypeDescriptor) -> NativeType:
    coefficient = NativeType("BIGNUM", "BIGNUM", value)
    scale = NativeType("INTEGER", "INTEGER", value)
    special = NativeType("DOUBLE", "DOUBLE", value)
    fields = (("coefficient", coefficient), ("scale", scale), ("special", special))
    return NativeType("NUMERIC_VARIABLE", "STRUCT(coefficient BIGNUM,scale INTEGER,special DOUBLE)", value, fields=fields, indexable=False)


def _encode_variable_numeric(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        # The VariableScaleDecimal converter may already have materialized the
        # destination-shaped struct, especially for NaN/Infinity.  Validate and
        # retain it rather than trying to parse the mapping as numeric text.
        if "coefficient" in value or "special" in value:
            special = value.get("special")
            if special is not None:
                special_value = numeric_value(special).value
                if not isinstance(special_value, float) or not (
                    math.isnan(special_value) or math.isinf(special_value)
                ):
                    raise InvalidTypedValue(
                        f"{special!r} is not a numeric special value"
                    )
                return {"coefficient": None, "scale": None, "special": special_value}
            coefficient = value.get("coefficient")
            scale = value.get("scale", 0)
            if coefficient is None:
                return {"coefficient": None, "scale": None, "special": None}
            try:
                return {
                    "coefficient": int(coefficient),
                    "scale": int(scale or 0),
                    "special": None,
                }
            except (TypeError, ValueError) as exc:
                raise InvalidTypedValue(f"{value!r} is not a variable numeric value") from exc
        if "value" in value and "scale" in value:
            raw = value["value"]
            if isinstance(raw, (bytes, bytearray)):
                coefficient = int.from_bytes(raw, byteorder="big", signed=True)
                return {
                    "coefficient": coefficient,
                    "scale": int(value["scale"]),
                    "special": None,
                }
            if isinstance(raw, str):
                numeric = numeric_value(raw)
                if numeric.member == "special":
                    return {
                        "coefficient": None,
                        "scale": None,
                        "special": numeric.value,
                    }
                decimal = numeric.value
                assert isinstance(decimal, Decimal)
                scale = int(value["scale"])
                return {
                    "coefficient": int(decimal.scaleb(scale)),
                    "scale": scale,
                    "special": None,
                }
    special = numeric_value(value)
    if special.member == "special":
        return {"coefficient": None, "scale": None, "special": special.value}
    decimal = special.value
    assert isinstance(decimal, Decimal)
    exponent = decimal.as_tuple().exponent
    scale = -exponent if isinstance(exponent, int) else 0
    coefficient = int(decimal.scaleb(scale))
    return {"coefficient": coefficient, "scale": scale, "special": None}


def _encode_struct(value: Any, source: SourceTypeDescriptor) -> dict[str, Any]:
    if source.kind == "point":
        if isinstance(value, (list, tuple)):
            value = {"x": value[0], "y": value[1]}
        elif isinstance(value, str):
            match = re.fullmatch(r"\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)", value)
            if match:
                value = {"x": match.group(1), "y": match.group(2)}
        if isinstance(value, Mapping):
            return {"x": float(value.get("x")), "y": float(value.get("y"))}
    if not isinstance(value, Mapping):
        raise InvalidTypedValue(f"{value!r} is not a STRUCT value")
    result: dict[str, Any] = {}
    for name, descriptor in source.composite_fields:
        result[name] = encode_value(value.get(name), descriptor)
    if source.kind in {"geometry", "geography", "postgis"}:
        result.setdefault("srid", 0)
        result["wkb"] = _decode_bytes(value.get("wkb", b""))
    return result


def _encode_json(value: Any, *, jsonb: bool) -> str | JsonbNull:
    """Validate JSON and canonicalize JSONB without changing JSON object order."""

    try:
        if jsonb and isinstance(value, JsonbNull):
            return value
        if isinstance(value, str):
            parsed = json.loads(value, parse_constant=_reject_json_constant)
            if not jsonb:
                return value
        else:
            parsed = value
        if jsonb and parsed is None:
            return JSONB_NULL
        return json.dumps(
            parsed,
            sort_keys=jsonb,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        name = "jsonb" if jsonb else "json"
        raise InvalidTypedValue(f"{value!r} is not valid PostgreSQL {name} JSON") from exc


def _reject_json_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant {value!r} is not valid JSON")


def _encode_bits(value: Any, source: SourceTypeDescriptor) -> dict[str, Any]:
    if isinstance(value, Mapping) and "bits" in value:
        bits = value.get("bits")
        packed = _decode_bytes(bits)
        try:
            bit_length = int(value.get("bit_length", len(packed or b"") * 8))
        except (TypeError, ValueError) as exc:
            raise InvalidTypedValue(f"{value!r} is not a bit value") from exc
        return {"bits": packed, "bit_length": bit_length}
    if isinstance(value, str) and value and set(value) <= {"0", "1"}:
        bit_text = value
        bit_length = len(bit_text)
        packed = int(bit_text, 2).to_bytes((bit_length + 7) // 8, "big")
    else:
        packed = _decode_bytes(value)
        bit_length = source.typmod if source.kind == "bit" and source.typmod else len(packed or b"") * 8
    return {"bits": packed, "bit_length": bit_length}


def _encode_range(value: Any, source: SourceTypeDescriptor) -> dict[str, Any]:
    if source.range_subtype is None:
        raise UnsupportedType(f"range {source.qualified_name} has no subtype descriptor")
    if isinstance(value, Mapping):
        return {
            "is_empty": bool(value.get("is_empty", False)),
            "lower": encode_value(value.get("lower"), source.range_subtype),
            "upper": encode_value(value.get("upper"), source.range_subtype),
            "lower_inclusive": bool(value.get("lower_inclusive", False)),
            "upper_inclusive": bool(value.get("upper_inclusive", False)),
        }
    text = str(value).strip()
    if text.lower() in {"empty", "(empty)"}:
        return {
            "is_empty": True,
            "lower": None,
            "upper": None,
            "lower_inclusive": False,
            "upper_inclusive": False,
        }
    if len(text) < 2 or text[0] not in "([" or text[-1] not in ")]":
        raise InvalidTypedValue(f"{value!r} is not a PostgreSQL range value")
    inner = text[1:-1]
    comma = _range_separator(inner)
    if comma is None:
        raise InvalidTypedValue(f"{value!r} is not a PostgreSQL range value")
    lower_text, upper_text = inner[:comma], inner[comma + 1 :]
    lower_text = _unquote_range_bound(lower_text.strip())
    upper_text = _unquote_range_bound(upper_text.strip())
    return {
        "is_empty": False,
        "lower": (
            encode_value(lower_text, source.range_subtype) if lower_text != "" else None
        ),
        "upper": (
            encode_value(upper_text, source.range_subtype) if upper_text != "" else None
        ),
        "lower_inclusive": text[0] == "[",
        "upper_inclusive": text[-1] == "]",
    }


def _multirange_parts(value: str) -> list[str]:
    """Split PostgreSQL multirange text without interpreting its bounds."""

    text = value.strip()
    if text.lower() in {"{}", "{empty}"}:
        return []
    if len(text) < 2 or text[0] != "{" or text[-1] != "}":
        raise InvalidTypedValue(f"{value!r} is not a PostgreSQL multirange value")
    parts: list[str] = []
    start = 1
    depth = 0
    quoted = False
    escaped = False
    for index, char in enumerate(text[1:-1], 1):
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif not quoted and char in "([":
            depth += 1
        elif not quoted and char in ")]":
            depth -= 1
        elif not quoted and char == "," and depth == 0:
            part = text[start:index].strip()
            if part:
                parts.append(part)
            start = index + 1
    part = text[start:-1].strip()
    if part:
        parts.append(part)
    return parts


def canonical_multirange_text(
    value: Any, source: SourceTypeDescriptor
) -> CanonicalRangeText:
    """Return the one PostgreSQL-text representation used by both destinations.

    ``include.unknown.datatypes=true`` reaches the JSON engine through the pinned
    ``binary.handling.mode=base64`` converter, so an opaque multirange byte value is
    observed here as base64 text.  Decode only that opaque transport form; a source
    event already marked ``CanonicalRangeText`` is retained byte-for-byte.  Lists
    remain a compatibility/value-boundary form for existing snapshot and identity
    tests and are rendered into the same canonical range text before binding.
    """
    if source.range_subtype is None:
        raise UnsupportedType(f"multirange {source.qualified_name} has no range subtype")
    if isinstance(value, CanonicalRangeText):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidTypedValue(
                f"multirange {source.qualified_name} is not UTF-8 text"
            ) from exc
        return _canonical_multirange_text_candidate(text, source)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            return _canonical_multirange_text_candidate(text, source)
        try:
            decoded = base64.b64decode(text, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            raise InvalidTypedValue(
                f"{value!r} is neither PostgreSQL multirange text nor opaque UTF-8 bytes"
            ) from None
        return _canonical_multirange_text_candidate(decoded, source)
    if isinstance(value, (list, tuple)):
        return _render_multirange_parts(value, source)
    raise InvalidTypedValue(f"{value!r} is not a multirange value")


def _canonical_multirange_text_candidate(
    text: str, source: SourceTypeDescriptor
) -> CanonicalRangeText:
    stripped = str(text).strip()
    try:
        _multirange_parts(stripped)
    except InvalidTypedValue:
        raise InvalidTypedValue(
            f"{text!r} is not PostgreSQL multirange text"
        ) from None
    return CanonicalRangeText(stripped)


def _render_multirange_parts(
    values: list | tuple, source: SourceTypeDescriptor
) -> CanonicalRangeText:
    """Render compatibility range values after PostgreSQL equality merging."""
    from . import identity_codec

    encoded = [
        _encode_range(item, source.range_subtype)
        for item in values
    ]
    normalized = [
        identity_codec._normalise_range(item, source.range_subtype)
        for item in encoded
    ]
    merged = identity_codec._merge_ranges(
        [item for item in normalized if not item["empty"]],
        source.range_subtype,
    )
    return CanonicalRangeText(
        "{" + ",".join(_render_range_text(item, source.range_subtype) for item in merged) + "}"
    )


def _render_range_text(value: dict[str, Any], source: SourceTypeDescriptor) -> str:
    if value["empty"]:
        return "empty"
    lower = _render_range_bound(value["lower"], source.range_subtype)
    upper = _render_range_bound(value["upper"], source.range_subtype)
    return (
        ("[" if value["lower_inclusive"] else "(")
        + lower
        + ","
        + upper
        + ("]" if value["upper_inclusive"] else ")")
    )


def _render_range_bound(value: Any, source: SourceTypeDescriptor | None) -> str:
    if value is None:
        return ""
    if isinstance(value, UnionValue):
        value = value.value
    if isinstance(value, Decimal):
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        text = text or "0"
    elif isinstance(value, (date, datetime, time)):
        text = value.isoformat()
    else:
        text = str(value)
    if any(char in text for char in ',()[]{}"\\') or text == "":
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _range_separator(value: str) -> int | None:
    quoted = False
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            quoted = not quoted
        elif char == "," and not quoted:
            return index
    return None


def _unquote_range_bound(value: str) -> str:
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value.replace(r"\\", "\\").replace(r'\"', '"')


def _decode_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            raise InvalidTypedValue("bytea value is not valid base64") from None
    raise InvalidTypedValue(f"{value!r} is not bytes or base64 text")


def _float_text(value: str) -> float:
    lowered = value.strip().lower()
    if lowered in {"nan", "+nan", "-nan"}:
        return math.nan
    if lowered in {"infinity", "+infinity", "inf", "+inf"}:
        return math.inf
    if lowered in {"-infinity", "-inf"}:
        return -math.inf
    try:
        return float(value)
    except ValueError as exc:
        raise InvalidTypedValue(f"{value!r} is not a floating-point value") from exc


def _date_value(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"infinity", "+infinity", "-infinity"}:
        return PostgresInfinity(not lowered.startswith("-"))  # type: ignore[return-value]
    if isinstance(value, int):
        return date(1970, 1, 1) + timedelta(days=value)
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise InvalidTypedValue(f"{value!r} is not an ISO date") from exc


def _time_value(value: Any, *, preserve_zone: bool = False) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, int):
        micros = value
        if not 0 <= micros < 86_400_000_000:
            raise InvalidTypedValue(f"microtime {micros} is outside one day")
        hours, remainder = divmod(micros, 3_600_000_000)
        minutes, remainder = divmod(remainder, 60_000_000)
        seconds, micros = divmod(remainder, 1_000_000)
        return time(hours, minutes, seconds, micros)
    try:
        return time.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidTypedValue(f"{value!r} is not an ISO time") from exc


def _datetime_value(value: Any, *, zoned: bool) -> datetime:
    if isinstance(value, datetime):
        return value
    lowered = str(value).strip().lower()
    if lowered in {"infinity", "+infinity", "-infinity"}:
        return PostgresInfinity(not lowered.startswith("-"))  # type: ignore[return-value]
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1_000_000, tz=UTC if zoned else None)
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if zoned and result.tzinfo is None:
            result = result.replace(tzinfo=UTC)
        return result
    except ValueError as exc:
        raise InvalidTypedValue(f"{value!r} is not an ISO timestamp") from exc


def _interval_value(value: Any) -> Any:
    """Translate Debezium's ISO-8601 duration into DuckDB interval text.

    ``interval.handling.mode=string`` preserves PostgreSQL's interval instead of
    exposing an opaque integer, but its ``P...T...`` spelling is not accepted by
    DuckDB's interval parser.  Keep years/months as calendar components (rather
    than pretending they are a fixed number of days) and let the destination
    perform the final native INTERVAL conversion.
    """
    if isinstance(value, timedelta):
        return value
    if not isinstance(value, str):
        raise InvalidTypedValue(f"{value!r} is not an interval value")
    text = value.strip()
    match = re.fullmatch(
        r"(?P<sign>[+-])?P(?:(?P<years>[0-9]+(?:\.[0-9]+)?)Y)?"
        r"(?:(?P<months>[0-9]+(?:\.[0-9]+)?)M)?"
        r"(?:(?P<weeks>[0-9]+(?:\.[0-9]+)?)W)?"
        r"(?:(?P<days>[0-9]+(?:\.[0-9]+)?)D)?"
        r"(?:T(?:(?P<hours>[0-9]+(?:\.[0-9]+)?)H)?"
        r"(?:(?P<minutes>[0-9]+(?:\.[0-9]+)?)M)?"
        r"(?:(?P<seconds>[0-9]+(?:\.[0-9]+)?)S)?)?",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        # PostgreSQL's textual interval form is already understood by DuckDB;
        # retaining it is still a native INTERVAL bind, not a VARCHAR fallback.
        return text
    sign = "-" if match.group("sign") == "-" else ""
    parts: list[str] = []
    for field_name, unit in (
        ("years", "years"),
        ("months", "months"),
        ("weeks", "weeks"),
        ("days", "days"),
        ("hours", "hours"),
        ("minutes", "minutes"),
        ("seconds", "seconds"),
    ):
        raw = match.group(field_name)
        if raw is not None:
            parts.append(f"{sign}{raw} {unit}")
    return " ".join(parts) or "0 seconds"


def _descriptor_from_any(value: Any) -> SourceTypeDescriptor:
    if isinstance(value, SourceTypeDescriptor):
        return value
    if isinstance(value, Mapping):
        if "type" in value and ("qualified_name" not in value and "type_name" not in value):
            return SourceTypeDescriptor.from_connect_schema(value)
        return SourceTypeDescriptor.from_dict(value)
    raise TypeError(f"cannot make a source descriptor from {value!r}")


def _connect_kind(raw_type: str, logical: str) -> str:
    if "variabledecimal" in logical or "variablescaledecimal" in logical:
        return "numeric_variable"
    if "decimal" in logical:
        return "numeric"
    if logical.endswith(".date") or logical.endswith("date"):
        return "date"
    if "zonedtimestamp" in logical:
        return "timestamptz"
    if "microtimestamp" in logical or logical.endswith("timestamp"):
        return "timestamp"
    if "zonedtime" in logical:
        return "timetz"
    if "microtime" in logical or logical.endswith("time"):
        return "time"
    if logical.endswith("uuid"):
        return "uuid"
    if logical.endswith("json"):
        return "json"
    if logical.endswith("enum"):
        return "enum"
    if logical.endswith("interval"):
        return "interval"
    if raw_type in {"int8", "long", "int64"}:
        return "int8"
    if raw_type in {"int16", "short"}:
        return "int2"
    if raw_type in {"int32", "int", "integer"}:
        return "int4"
    if raw_type in {"float32", "float"}:
        return "float4"
    if raw_type in {"float64", "double"}:
        return "float8"
    if raw_type == "bytes":
        return "bytea"
    if raw_type == "string":
        return "text"
    return raw_type


def _connect_enum_labels(schema: Mapping[str, Any], parameters: Mapping[str, Any]) -> tuple[str, ...]:
    labels = schema.get("values", schema.get("enum", parameters.get("allowed")))
    if isinstance(labels, str):
        return tuple(item for item in labels.split(",") if item)
    return tuple(str(item) for item in (labels or ()))


def _decode_opaque_text(value: Any, source: SourceTypeDescriptor) -> str:
    """Carry an allowlisted opaque value as PostgreSQL's text, without interpreting it.

    The stock connector has two wire shapes.  ``_BASE64_OPAQUE_KINDS`` arrives as
    base64 text; the remaining allowlisted values arrive as text.  Both paths have
    exactly one semantic check: bytes must decode as strict UTF-8.  The decoded text
    is never stripped, parsed, validated, or normalised.
    """

    if isinstance(value, OpaqueText):
        return value

    kind = _kind_name(source.kind, source.qualified_name)
    if kind in _BASE64_OPAQUE_KINDS:
        if isinstance(value, (bytes, bytearray, memoryview)):
            try:
                payload = bytes(value).decode("ascii")
            except UnicodeDecodeError as exc:
                raise InvalidTypedValue(
                    f"{source.qualified_name} base64 payload is not ASCII"
                ) from exc
        elif isinstance(value, str):
            payload = value
        else:
            raise InvalidTypedValue(
                f"{source.qualified_name} opaque payload must be base64 text or bytes"
            )
        try:
            decoded = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise InvalidTypedValue(
                f"{source.qualified_name} opaque payload is not valid base64"
            ) from exc
        try:
            return OpaqueText(decoded.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise InvalidTypedValue(
                f"{source.qualified_name} opaque payload is not strict UTF-8"
            ) from exc

    if isinstance(value, str):
        return OpaqueText(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        # A few connector versions expose the same OTHER value as bytes containing
        # ASCII base64.  Prefer that explicit transport when it is unambiguous;
        # otherwise the bytes themselves are the text payload.
        try:
            payload = raw.decode("ascii")
            decoded = base64.b64decode(payload, validate=True)
        except (UnicodeDecodeError, binascii.Error, ValueError):
            decoded = None
        if decoded is not None:
            try:
                return OpaqueText(decoded.decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise InvalidTypedValue(
                    f"{source.qualified_name} opaque payload is not strict UTF-8"
                ) from exc
        try:
            return OpaqueText(raw.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise InvalidTypedValue(
                f"{source.qualified_name} opaque payload is not strict UTF-8"
            ) from exc
    raise InvalidTypedValue(
        f"{source.qualified_name} opaque payload must be text or bytes"
    )


def _money_output_text(value: Any, source: SourceTypeDescriptor) -> OpaqueText:
    """Render the stock wire number using the source ``money_out`` locale.

    Debezium's PostgreSQL money converter supplies the numeric amount, while the
    PostgreSQL output function supplies the locale decoration.  The locale is read
    once by the catalog descriptor authority and carried in descriptor metadata;
    silently using C/en_US here would admit a value the source never had.
    """

    locale_name = dict(source.metadata).get("lc_monetary", "C")
    normalized_locale = str(locale_name).lower().replace("-", "_")
    if (
        normalized_locale in {"c", "posix"}
        or normalized_locale.startswith(("c.", "posix."))
        or normalized_locale.startswith("en_us")
    ):
        symbol = "$"
    elif normalized_locale.startswith("en_gb"):
        symbol = "£"
    else:
        raise UnsupportedType(
            f"{source.qualified_name} has unsupported lc_monetary={locale_name!r}; "
            "the money output function cannot be reconstructed exactly"
        )

    if isinstance(value, OpaqueText):
        text = str(value).strip()
        if text.startswith(symbol) or text.startswith(f"-{symbol}"):
            return OpaqueText(text)
        raise InvalidTypedValue(
            f"{source.qualified_name} money text {text!r} does not match "
            f"the source lc_monetary={locale_name!r} output"
        )
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = _decode_opaque_text(value, source)
    if isinstance(value, str):
        text = value.strip()
        if text[:1] in {"$", "£", "€", "₹"} or text.startswith(("-$", "-£", "-€", "-₹")):
            if text.startswith(symbol) or text.startswith(f"-{symbol}"):
                return OpaqueText(text)
            raise InvalidTypedValue(
                f"{source.qualified_name} money text {text!r} does not match "
                f"the source lc_monetary={locale_name!r} output"
            )
        value = text
    try:
        raw_amount = Decimal(str(value))
        amount = raw_amount.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise InvalidTypedValue(
            f"{source.qualified_name} stock Debezium money value {value!r} "
            "is not a finite decimal amount"
        ) from exc
    if not amount.is_finite():
        raise InvalidTypedValue(
            f"{source.qualified_name} stock Debezium money value {value!r} "
            "is not a finite decimal amount"
        )
    if raw_amount != amount:
        raise InvalidTypedValue(
            f"{source.qualified_name} stock Debezium money value {value!r} "
            "has more precision than PostgreSQL money_out can represent"
        )
    negative = amount < 0
    rendered = f"{abs(amount):,.2f}"
    return OpaqueText(f"-{symbol}{rendered}" if negative else f"{symbol}{rendered}")


def _kind_name(kind: Any, qualified_name: str) -> str:
    value = str(kind or "unknown").lower().strip()
    name = qualified_name.rsplit(".", 1)[-1].lower()
    aliases = {
        "int2": "int2", "smallint": "int2", "int4": "int4", "integer": "int4",
        "int8": "int8", "bigint": "int8", "float4": "float4", "real": "float4",
        "float8": "float8", "double": "float8", "double precision": "float8",
        "bool": "bool", "boolean": "bool", "character varying": "varchar",
        "bpchar": "bpchar", "character": "char", "json": "json", "jsonb": "jsonb",
    }
    # PostgreSQL exposes int2vector/oidvector with array-like catalog metadata,
    # but they are opaque system types, not PostgreSQL array values.  Keep the
    # exact built-in type name so the OID allowlist can choose VARCHAR or refusal
    # before any recursive array codec sees the value.
    if value == "array" and name in {"int2vector", "oidvector"}:
        return name
    if value in {"unknown", "user", "base", "scalar"} and name in aliases:
        return aliases[name]
    if value.startswith("_") and value[1:] in aliases:
        return "array"
    return aliases.get(value, value)


def _pairs(value: Any) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return _freeze_pairs(value)
    return tuple(sorted((str(item[0]), str(item[1])) for item in value))


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _jsonable(value: Any) -> Any:
    if isinstance(value, JsonbNull):
        return {"__cdc_jsonb_null__": True}
    if isinstance(value, OpaqueText):
        return {"__opaque_text__": str(value)}
    if isinstance(value, UnionValue):
        return {"__union_member__": value.member, "value": _jsonable(value.value)}
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, (date, time, datetime)):
        return {"__temporal__": value.isoformat()}
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return {"__float__": repr(value)}
    return value


def _from_jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        if value.get("__cdc_jsonb_null__") is True and len(value) == 1:
            return JSONB_NULL
        if "__opaque_text__" in value and len(value) == 1:
            return OpaqueText(str(value["__opaque_text__"]))
        if "__union_member__" in value:
            return UnionValue(str(value["__union_member__"]), _from_jsonable(value.get("value")))
        if "__decimal__" in value:
            return Decimal(str(value["__decimal__"]))
        if "__temporal__" in value:
            return value["__temporal__"]
        if "__bytes__" in value:
            return base64.b64decode(value["__bytes__"])
        if "__float__" in value:
            return _float_text(str(value["__float__"]))
        return {key: _from_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_jsonable(item) for item in value]
    return value


_OPAQUE_TEXT_KINDS = frozenset({
    "tsquery", "jsonpath", "pg_lsn", "tsvector", "xml", "money", "inet", "cidr",
    "macaddr", "macaddr8", "int2vector",
})
_BASE64_OPAQUE_KINDS = frozenset({"tsquery", "jsonpath", "pg_lsn"})
_OPAQUE_TEXT_OIDS = {
    "tsquery": frozenset({3615}),
    "jsonpath": frozenset({4072}),
    "pg_lsn": frozenset({3220}),
    "tsvector": frozenset({3614}),
    "xml": frozenset({142}),
    "money": frozenset({790}),
    "inet": frozenset({869}),
    "cidr": frozenset({650}),
    "macaddr": frozenset({829}),
    "macaddr8": frozenset({774}),
    "int2vector": frozenset({22}),
}
_OBSCURE_TEXT_KINDS = frozenset({
    "ltree", "oidvector", "xid8", "regproc", "regprocedure", "regoper", "regoperator",
    "regclass", "regcollation", "regconfig", "regdictionary", "regnamespace", "regrole",
    "regtype", "aclitem", "pg_node_tree", "tinterval", "snapshot", "opaque",
})


def _opaque_descriptor_allowed(
    descriptor: SourceTypeDescriptor, kind: str
) -> bool:
    """Allow only catalog-identified built-in opaque types.

    Names alone are not an authority: a user type can have the same spelling in a
    different schema.  The catalog OID is retained in every source descriptor and
    is the allowlist key here.
    """

    return descriptor.oid in _OPAQUE_TEXT_OIDS.get(kind, ())


__all__ = [
    "JSONB_NULL",
    "CanonicalRangeText",
    "FieldState",
    "FieldValue",
    "InvalidTypedValue",
    "JsonbNull",
    "NativeMember",
    "NativeType",
    "OpaqueText",
    "PostgresInfinity",
    "SourceTypeDescriptor",
    "TypedImage",
    "UnionValue",
    "UnsupportedType",
    "adapt_value",
    "canonical_multirange_text",
    "encode_value",
    "mark_canonical_range_text",
    "native_type",
    "numeric_value",
    "union_member_name",
    "union_type",
]
