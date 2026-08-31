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

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from .errors import AdmissionError


class UnsupportedType(AdmissionError):
    """A source type without an allowlisted, native destination representation."""


class InvalidTypedValue(AdmissionError):
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
    #: PostgreSQL's catalog-resolved ``typoutput`` identity. Policy transforms
    #: may use a streamed string only when this authority (or an explicit
    #: ``PostgreSQLOutputText`` proof) is present.
    output_function_oid: int | None = None
    output_function_schema: str | None = None
    output_function_name: str | None = None
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
            output_function_oid=_as_int(
                value.get("output_function_oid", value.get("typoutput"))
            ),
            output_function_schema=(
                str(value["output_function_schema"])
                if value.get("output_function_schema") is not None
                else None
            ),
            output_function_name=(
                str(value["output_function_name"])
                if value.get("output_function_name") is not None
                else None
            ),
        )

    @classmethod
    def from_catalog(cls, value: Mapping[str, Any]) -> SourceTypeDescriptor:
        return cls.from_dict(value)

    @classmethod
    def from_connect_schema(cls, schema: Mapping[str, Any]) -> SourceTypeDescriptor:
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
                array_element=cls.from_connect_schema(
                    schema.get("items", schema.get("value_schema"))
                ),
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
        precision = _as_int(
            parameters.get("connect.decimal.precision", parameters.get("precision"))
        )
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
            "output_function_oid": self.output_function_oid,
            "output_function_schema": self.output_function_schema,
            "output_function_name": self.output_function_name,
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


def mark_canonical_range_text(value: Any, descriptor: SourceTypeDescriptor | None) -> Any:
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
        "range",
        "daterange",
        "int4range",
        "int8range",
        "numrange",
        "tsrange",
        "tstzrange",
    }
    if kind in range_kinds and isinstance(value, str):
        return value if isinstance(value, CanonicalRangeText) else CanonicalRangeText(value)
    if kind == "multirange" and isinstance(value, str):
        # Stock Debezium already carries PostgreSQL's multirange output text.  The
        # marker records that fact; the helper only unwraps the connector's
        # base64 transport form and never parses, canonicalizes, or re-renders the
        # server's value before the destination sees it.
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
            "members": [
                (member.name, member.fingerprint, member.type.sql) for member in self.members
            ],
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
            SourceTypeDescriptor.from_dict(value["descriptor"]) if value.get("descriptor") else None
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


def native_type(source: SourceTypeDescriptor | NativeType, *, for_key: bool = False) -> NativeType:
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
        return NativeType(
            base.kind,
            base.sql,
            descriptor,
            base.children,
            base.fields,
            base.members,
            base.key,
            base.value,
            base.indexable,
        )
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
        sql = (
            "STRUCT("
            + ",".join(f"{_quote_identifier(name)} {child.sql}" for name, child in fields)
            + ")"
        )
        return NativeType("STRUCT", sql, descriptor, fields=fields, indexable=False)
    if kind == "map":
        if descriptor.map_key is None or descriptor.map_value is None:
            raise UnsupportedType(
                f"map {descriptor.qualified_name} has incomplete key/value descriptors"
            )
        key = native_type(descriptor.map_key, for_key=for_key)
        value = native_type(descriptor.map_value, for_key=for_key)
        return NativeType(
            "MAP", f"MAP({key.sql},{value.sql})", descriptor, key=key, value=value, indexable=False
        )
    if kind == "enum":
        if not descriptor.enum_labels:
            raise UnsupportedType(f"enum {descriptor.qualified_name} has no labels")
        labels = ",".join("'" + label.replace("'", "''") + "'" for label in descriptor.enum_labels)
        return NativeType("ENUM", f"ENUM({labels})", descriptor)
    if kind == "point":
        fields = (
            ("x", NativeType("DOUBLE", "DOUBLE", descriptor)),
            ("y", NativeType("DOUBLE", "DOUBLE", descriptor)),
        )
        return NativeType(
            "STRUCT", "STRUCT(x DOUBLE,y DOUBLE)", descriptor, fields=fields, indexable=False
        )
    if kind in {"geometry", "geography", "postgis"}:
        fields = (
            ("srid", NativeType("INTEGER", "INTEGER", descriptor)),
            ("wkb", NativeType("BLOB", "BLOB", descriptor)),
        )
        return NativeType(
            "STRUCT", "STRUCT(srid INTEGER,wkb BLOB)", descriptor, fields=fields, indexable=False
        )
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
        fields = (
            ("bits", NativeType("BLOB", "BLOB", descriptor)),
            ("bit_length", NativeType("INTEGER", "INTEGER", descriptor)),
        )
        return NativeType(
            "STRUCT",
            "STRUCT(bits BLOB,bit_length INTEGER)",
            descriptor,
            fields=fields,
            indexable=False,
        )
    if kind in {"vector", "halfvec"}:
        child = NativeType("DOUBLE", "DOUBLE", descriptor)
        return NativeType("LIST", "DOUBLE[]", descriptor, children=(child,), indexable=False)
    if kind == "sparsevec":
        fields = (
            ("dimensions", NativeType("SMALLINT", "SMALLINT", descriptor)),
            ("vector", NativeType("MAP", "MAP(SMALLINT,DOUBLE)", descriptor, indexable=False)),
        )
        return NativeType(
            "STRUCT",
            "STRUCT(dimensions SMALLINT,vector MAP(SMALLINT,DOUBLE))",
            descriptor,
            fields=fields,
            indexable=False,
        )
    if kind == "money":
        # The ONE deliberate carve-out from the opaque-type OID allowlist below.
        # Standing directive: `money` must never refuse, block or quarantine a
        # table; it flows into VARCHAR verbatim under any `lc_monetary`.  The
        # allowlist (`_OPAQUE_TEXT_OIDS["money"] == {790}`) made that false for a
        # descriptor spelling money with any other OID -- a non-default catalog,
        # an extension or a re-created type -- because `encode_value` resolves the
        # native type BEFORE it reaches its own unconditional money branch, so the
        # refusal happened here instead.  Resolving money by kind alone keeps the
        # promise where it is actually made.  Every OTHER opaque kind keeps the
        # OID allowlist exactly as it is: names alone remain no authority there.
        return NativeType("VARCHAR", "VARCHAR", descriptor)
    if kind in _OPAQUE_TEXT_KINDS and _opaque_descriptor_allowed(descriptor, kind):
        return NativeType("VARCHAR", "VARCHAR", descriptor)
    raise UnsupportedType(
        f"source type {descriptor.qualified_name!r} (kind={descriptor.kind!r}, oid={descriptor.oid}) "
        "has no verified native destination representation"
    )


def union_member_name(descriptor: SourceTypeDescriptor | NativeType) -> str:
    fingerprint = descriptor.fingerprint
    return "m_" + fingerprint[:16]


def union_type(
    descriptors: tuple[SourceTypeDescriptor, ...] | list[SourceTypeDescriptor],
) -> NativeType:
    """Build one physical UNION, deduplicating members by source fingerprint."""

    members: list[NativeMember] = []
    seen: set[str] = set()
    for descriptor in descriptors:
        if descriptor.fingerprint in seen:
            continue
        seen.add(descriptor.fingerprint)
        members.append(
            NativeMember(union_member_name(descriptor), native_type(descriptor), descriptor)
        )
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
        if lowered in {
            "infinity",
            "+infinity",
            "inf",
            "+inf",
            "positive_infinity",
            "positive infinity",
        }:
            return UnionValue("special", math.inf)
        if lowered in {"-infinity", "-inf", "negative_infinity", "negative infinity"}:
            return UnionValue("special", -math.inf)
    try:
        return UnionValue("finite", value if isinstance(value, Decimal) else Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError) as exc:
        value_type = type(value)
        raise InvalidTypedValue(
            "value of type "
            f"{value_type.__module__}.{value_type.__qualname__} "
            "is not a PostgreSQL numeric value"
        ) from exc


# Value encoding and transport helpers live in their own owner module.  The
# imports below intentionally restore the historical public surface while keeping
# descriptor/type ownership separate from value-codec ownership.
from .typed_value_codec import (  # noqa: E402
    _OPAQUE_TEXT_KINDS,
    _connect_enum_labels,
    _connect_kind,
    _descriptor_from_any,
    _kind_name,
    _multirange_parts,  # noqa: F401
    _opaque_descriptor_allowed,
    _variable_numeric,
    adapt_value,
    canonical_multirange_text,
    encode_value,
)
from .typed_value_transport import (  # noqa: E402
    _as_int,
    _from_jsonable,
    _jsonable,
    _pairs,
    _quote_identifier,
)

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
