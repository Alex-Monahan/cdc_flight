"""Canonical, source-typed identity codec.

The codec is deliberately independent of destination readback. It receives
source values plus catalog descriptors and emits one typed, JSON-safe identity
tree for every runtime.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any

def _identity_runtime(value: Any) -> Any:
    """Return the JSON-safe leaf representation owned by the identity encoder.

    Identity is a typed serialization, not a display string.  In particular, bytes are
    represented as hexadecimal data and non-finite floats have explicit tokens.  This
    prevents a destination's pretty-printer (``CAST(BLOB AS VARCHAR)`` was the old
    example) from becoming part of the source identity contract.
    """
    from datetime import date, datetime, time, timedelta
    from decimal import Decimal

    from .typed_types import JsonbNull, UnionValue

    if isinstance(value, JsonbNull):
        return {"jsonb_null": True}
    if isinstance(value, UnionValue):
        return {
            "union_member": value.member,
            "value": _identity_runtime(value.value),
        }
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_hex": bytes(value).hex()}
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, time):
        return {"time": value.isoformat()}
    if isinstance(value, timedelta):
        return {"timedelta_microseconds": value.total_seconds() * 1_000_000}
    if isinstance(value, float):
        if math.isnan(value):
            return {"float": "NaN"}
        if math.isinf(value):
            return {"float": "Infinity" if value > 0 else "-Infinity"}
        return {"float": repr(value)}
    if isinstance(value, Mapping):
        items = [
            [_identity_runtime(key), _identity_runtime(item)]
            for key, item in value.items()
        ]
        items.sort(key=lambda item: json.dumps(item[0], sort_keys=True, separators=(",", ":")))
        return {"map": items}
    if isinstance(value, (list, tuple)):
        return {"list": [_identity_runtime(item) for item in value]}
    return value


def _identity_tree(value: Any, descriptor: Any) -> Any:
    """Encode one source value recursively according to its catalog descriptor."""
    from .typed_types import JsonbNull, UnionValue, encode_value

    if descriptor is None:
        return _identity_runtime(value)
    source = descriptor
    seen: set[int] = set()
    while getattr(source, "domain_base", None) is not None and id(source) not in seen:
        seen.add(id(source))
        source = source.domain_base
    kind = str(source.kind or source.qualified_name).lower()
    if isinstance(value, UnionValue):
        return _identity_runtime(value)
    if kind == "jsonb" and value is None:
        # SQL NULL and a JSONB root-null document can look identical after a
        # destination round trip.  The source identity must retain the distinction
        # before DuckDB's VARIANT display layer collapses it.
        return {"sql_null": True}
    encoded = encode_value(value, descriptor)
    if encoded is None and kind != "jsonb":
        return None
    if kind == "jsonb":
        if isinstance(encoded, JsonbNull):
            return {"jsonb": None}
        try:
            parsed = json.loads(str(encoded))
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = str(encoded)
        return {"jsonb": _identity_runtime(parsed)}
    if kind == "json":
        # PostgreSQL JSON is textual and its destination JSON representation preserves
        # that text.  JSONB, above, deliberately takes the structural path instead.
        return {"json_text": str(encoded)}
    if kind in {"bytea", "bytes", "blob"}:
        return _identity_runtime(encoded)
    if kind in {"struct", "composite", "point", "geometry", "geography", "postgis"}:
        mapping = encoded if isinstance(encoded, Mapping) else {}
        return {
            "struct": [
                [name, _identity_tree(mapping.get(name), child)]
                for name, child in source.composite_fields
            ]
        }
    if kind == "array" and source.array_element is not None:
        values = encoded if isinstance(encoded, (list, tuple)) else []
        return {
            "list": [_identity_tree(item, source.array_element) for item in values]
        }
    if kind == "map" and source.map_key is not None and source.map_value is not None:
        mapping = encoded if isinstance(encoded, Mapping) else {}
        items = [
            [
                _identity_tree(key, source.map_key),
                _identity_tree(item, source.map_value),
            ]
            for key, item in mapping.items()
        ]
        items.sort(key=lambda item: json.dumps(item[0], sort_keys=True, separators=(",", ":")))
        return {"map": items}
    return _identity_runtime(encoded)


def _identity_value(
    table: TableSchema,
    values: tuple[Any, ...] | list[Any],
    *,
    descriptors: dict[str, Any] | None = None,
    key_columns: tuple[str, ...] | None = None,
    union_columns: frozenset[str] = frozenset(),
) -> str:
    """Build one canonical recursive identity for writes, lookups and rebuilds."""
    from .typed_types import UnionValue, union_member_name

    descriptors = descriptors or {}
    key_columns = tuple(key_columns or table.source_key_columns or table.key_columns)
    components: list[dict[str, Any]] = []
    for column, value in zip(key_columns, values, strict=True):
        descriptor = descriptors.get(column) or table.source_descriptors.get(column)
        native = table.native_types.get(column)
        if isinstance(value, UnionValue):
            tag = value.member
            tree = _identity_runtime(value.value)
        else:
            encoded = _identity_tree(value, descriptor)
            if column in union_columns or (
                native is not None and native.kind == "UNION" and descriptor is not None
            ):
                tag = union_member_name(descriptor)
            elif native is not None and native.kind == "NUMERIC_UNION":
                tag = encoded.get("union_member") if isinstance(encoded, dict) else "value"
            else:
                tag = "value"
            tree = encoded
        fingerprint = descriptor.fingerprint if descriptor is not None else "legacy"
        source_kind = str(getattr(descriptor, "kind", "")).lower() if descriptor else ""
        if value is None and source_kind != "jsonb":
            components.append({"descriptor": fingerprint, "state": "sql_null"})
        else:
            components.append({
                "descriptor": fingerprint,
                "member": tag,
                "value": tree,
            })
    return json.dumps(components, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _identity_candidates(
    table: TableSchema,
    values: tuple[Any, ...] | list[Any],
    *,
    descriptors: dict[str, Any] | None = None,
    key_columns: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    """Resolve current and historical source descriptors to stored identities."""
    from itertools import product

    descriptors = descriptors or {}
    key_columns = tuple(key_columns or table.source_key_columns or table.key_columns)
    choices = []
    for column in key_columns:
        explicit = descriptors.get(column)
        if explicit is not None:
            history = table.identity_descriptors.get(column, ())
            current = table.source_descriptors.get(column)
            choices.append(tuple(dict.fromkeys((explicit, *history, current))))
            continue
        history = table.identity_descriptors.get(column, ())
        current = table.source_descriptors.get(column)
        choices.append(tuple(dict.fromkeys((*history, current))))
    if not all(choices):
        return (
            _identity_value(
                table,
                values,
                descriptors=descriptors,
                key_columns=key_columns,
            ),
        )
    result = []
    for selected in product(*choices):
        result.append(
            _identity_value(
                table,
                values,
                descriptors=dict(zip(key_columns, selected, strict=True)),
                key_columns=key_columns,
            )
        )
    return tuple(dict.fromkeys(result))


def identity_value(table: Any, values: tuple[Any, ...], *, key_columns: tuple[str, ...]) -> str:
    """Public spelling for the canonical source identity encoder."""
    return _identity_value(table, values, key_columns=key_columns)


__all__ = [
    "identity_value",
    "_identity_candidates",
    "_identity_runtime",
    "_identity_tree",
    "_identity_value",
]

