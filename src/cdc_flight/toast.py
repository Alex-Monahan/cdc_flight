"""Current-runtime TOAST marker policy.

Debezium 3.6 exposes an unchanged TOAST value as an ordinary converted value,
so the Python boundary cannot use object identity.  The configured ``hex:00``
marker is nevertheless structurally outside the PostgreSQL domains covered by
this module.  Recognition is consequently deliberately narrow: a NUL is a
marker only when the source descriptor is one of the probed structural shapes.

This module is the single source for the connector property and its decoded
marker.  It must not grow a generic ``value == placeholder`` shortcut.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .typed_types import FieldValue, SourceTypeDescriptor

# Debezium's ``hex:`` parser turns this into one Java/Python U+0000 code point.
UNAVAILABLE_VALUE_PLACEHOLDER = "hex:00"
STRUCTURAL_MARKER = "\x00"
STRUCTURAL_MARKER_BYTES = b"\x00"

TEXT_STRUCTURAL_KINDS = frozenset(
    {"text", "varchar", "bpchar", "char", "character", "character varying", "json", "jsonb", "xml"}
)
SUPPORTED_BINARY_MODES = frozenset({"bytes", "base64", "base64-url-safe", "hex"})
NON_TOAST_SCALAR_KINDS = frozenset(
    {
        "bool", "boolean", "bit1", "smallint", "int2", "smallserial", "integer",
        "int", "int4", "serial", "bigint", "int8", "bigserial", "oid", "xid",
        "xid8", "real", "float4", "double", "float8", "double precision", "date",
        "time", "timestamp", "timestamptz", "timetz", "interval", "uuid", "enum",
    }
)


class ToastRoute(StrEnum):
    NONE = "none"
    STRUCTURAL = "structural"
    REPLICA_IDENTITY_FULL = "replica_identity_full"
    FALLBACK = "fallback"


@dataclass(frozen=True)
class ToastColumnPolicy:
    name: str
    route: ToastRoute
    reason: str
    structural: bool = False
    residual: bool = False


@dataclass(frozen=True)
class ToastTablePolicy:
    table: str
    route: ToastRoute
    structural_columns: tuple[str, ...] = ()
    residual_columns: tuple[str, ...] = ()
    fixed_columns: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    @property
    def efficient(self) -> bool:
        return self.route in {ToastRoute.NONE, ToastRoute.STRUCTURAL}

    @property
    def fallback_required(self) -> bool:
        return bool(self.residual_columns)


def _kind(descriptor: SourceTypeDescriptor | None) -> str:
    if descriptor is None:
        return "unknown"
    value = descriptor
    seen: set[int] = set()
    while value is not None and value.domain_base is not None and id(value) not in seen:
        seen.add(id(value))
        value = value.domain_base
    return str(value.kind or value.qualified_name).lower().strip()


def _qualified(descriptor: SourceTypeDescriptor | None) -> str:
    return str(descriptor.qualified_name if descriptor is not None else "").lower()


def is_hstore(descriptor: SourceTypeDescriptor | None) -> bool:
    return _kind(descriptor) == "map" and "hstore" in _qualified(descriptor)


def is_structural_scalar(descriptor: SourceTypeDescriptor | None) -> bool:
    return _kind(descriptor) in TEXT_STRUCTURAL_KINDS


def is_structural_array(descriptor: SourceTypeDescriptor | None) -> bool:
    return _kind(descriptor) == "array" and is_structural_scalar(
        descriptor.array_element if descriptor is not None else None
    )


def is_structural_type(descriptor: SourceTypeDescriptor | None) -> bool:
    return is_structural_scalar(descriptor) or is_structural_array(descriptor) or is_hstore(
        descriptor
    )


def _has_nul(value: Any) -> bool:
    if isinstance(value, str):
        return STRUCTURAL_MARKER in value
    if isinstance(value, (list, tuple)):
        return any(_has_nul(item) for item in value)
    if isinstance(value, Mapping):
        return any(_has_nul(key) or _has_nul(item) for key, item in value.items())
    return False


def _canonical_hstore_marker(value: Any) -> bool:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
    if not isinstance(value, Mapping) or len(value) != 1:
        return False
    key, item = next(iter(value.items()))
    return key == STRUCTURAL_MARKER and item == STRUCTURAL_MARKER


def is_structural_marker(
    value: Any,
    descriptor: SourceTypeDescriptor | None,
    *,
    binary_mode: str = "base64",
    hstore_mode: str = "map",
) -> bool:
    """Return true only for a probed marker representation.

    Equality with the configured property is intentionally absent.  A normal
    source string such as the old printable Debezium token remains ``VALUE``.
    """

    if descriptor is None:
        return False
    kind = _kind(descriptor)
    if kind in TEXT_STRUCTURAL_KINDS:
        return isinstance(value, str) and _has_nul(value)
    if kind == "array":
        element = descriptor.array_element
        return (
            element is not None
            and _kind(element) in TEXT_STRUCTURAL_KINDS
            and isinstance(value, (list, tuple))
            and _has_nul(value)
        )
    if kind == "map" and is_hstore(descriptor):
        return hstore_mode in {"map", "json", "json-string", "default"} and _canonical_hstore_marker(value)
    if kind in {"bytea", "bytes", "blob"} and binary_mode == "hex":
        return isinstance(value, str) and _has_nul(value)
    return False


def field_value(
    value: Any,
    descriptor: SourceTypeDescriptor | None,
    *,
    binary_mode: str = "base64",
    hstore_mode: str = "map",
) -> FieldValue:
    """Decode one field into the closed RowPatch disposition domain."""

    if value is None:
        return FieldValue.explicit_null(descriptor)
    if is_structural_marker(
        value, descriptor, binary_mode=binary_mode, hstore_mode=hstore_mode
    ):
        return FieldValue.unchanged_toast(descriptor)
    return FieldValue.of(value, descriptor)


def classify_column(
    name: str,
    descriptor: SourceTypeDescriptor | None,
    *,
    attstorage: str | None = None,
    binary_mode: str = "base64",
    hstore_mode: str = "map",
) -> ToastColumnPolicy:
    """Classify one catalog column without inspecting row values."""

    storage = (attstorage or "x").lower()
    if storage == "p":
        return ToastColumnPolicy(name, ToastRoute.NONE, "attstorage=p excludes the column from TOAST risk")

    kind = _kind(descriptor)
    qualified = _qualified(descriptor)
    if is_structural_scalar(descriptor):
        return ToastColumnPolicy(
            name, ToastRoute.STRUCTURAL,
            "NUL is not representable in the tested PostgreSQL text-like domain",
            structural=True,
        )
    if is_structural_array(descriptor):
        return ToastColumnPolicy(
            name, ToastRoute.STRUCTURAL,
            "NUL-bearing elements are not representable in the tested text-like array",
            structural=True,
        )
    if is_hstore(descriptor) and hstore_mode in {"map", "json", "json-string", "default"}:
        return ToastColumnPolicy(
            name, ToastRoute.STRUCTURAL,
            "hstore keys and values are PostgreSQL text and cannot contain NUL",
            structural=True,
        )
    if kind in {"bytea", "bytes", "blob"} and binary_mode == "hex":
        return ToastColumnPolicy(
            name, ToastRoute.STRUCTURAL,
            "the tested scalar hex representation preserves the NUL marker outside bytea text",
            structural=True,
        )

    if kind in {"bytea", "bytes", "blob"}:
        reason = f"bytea marker is representable or lossy under binary.handling.mode={binary_mode!r}"
    elif kind == "array":
        reason = "derived array marker is representable for this element type"
    elif "hstore" in qualified:
        reason = "hstore array/opaque form is not the tested scalar MAP marker"
    elif kind in {"composite", "struct", "point", "geometry", "geography", "postgis"}:
        reason = "arbitrary composite/structured TOAST form is not marker-safe at SourceRecord"
    elif kind in NON_TOAST_SCALAR_KINDS:
        return ToastColumnPolicy(name, ToastRoute.NONE, "fixed-width or non-TOAST scalar")
    else:
        reason = "TOAST-capable type is outside the structural marker allowlist"
    return ToastColumnPolicy(name, ToastRoute.FALLBACK, reason, residual=True)


def classify_relation(
    table: str,
    columns,
    *,
    replica_identity: str = "d",
    binary_mode: str = "base64",
    hstore_mode: str = "map",
) -> ToastTablePolicy:
    """Classify all fields and select one table-scoped route."""

    structural: list[str] = []
    residual: list[str] = []
    fixed: list[str] = []
    reasons: list[str] = []
    for column in columns:
        if isinstance(column, tuple):
            name, descriptor, storage = (*column, None, None, None)[:3]
        else:
            name = getattr(column, "destination_name", getattr(column, "name", ""))
            descriptor = getattr(column, "descriptor", None)
            storage = getattr(column, "attstorage", None)
        policy = classify_column(
            str(name), descriptor, attstorage=storage,
            binary_mode=binary_mode, hstore_mode=hstore_mode,
        )
        if policy.route is ToastRoute.NONE:
            fixed.append(str(name))
        elif policy.structural:
            structural.append(str(name))
        else:
            residual.append(str(name))
            reasons.append(f"{name}: {policy.reason}")
    if residual:
        route = (
            ToastRoute.REPLICA_IDENTITY_FULL
            if str(replica_identity).lower() == "f"
            else ToastRoute.FALLBACK
        )
    elif structural:
        route = ToastRoute.STRUCTURAL
    else:
        route = ToastRoute.NONE
    return ToastTablePolicy(
        table=str(table), route=route,
        structural_columns=tuple(structural), residual_columns=tuple(residual),
        fixed_columns=tuple(fixed), reasons=tuple(reasons),
    )


__all__ = [
    "STRUCTURAL_MARKER",
    "STRUCTURAL_MARKER_BYTES",
    "SUPPORTED_BINARY_MODES",
    "TEXT_STRUCTURAL_KINDS",
    "UNAVAILABLE_VALUE_PLACEHOLDER",
    "ToastColumnPolicy",
    "ToastRoute",
    "ToastTablePolicy",
    "classify_column",
    "classify_relation",
    "field_value",
    "is_hstore",
    "is_structural_marker",
    "is_structural_type",
]
