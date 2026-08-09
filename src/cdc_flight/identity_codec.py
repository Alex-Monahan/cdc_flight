"""One source-semantic, destination-round-trip-stable identity codec.

The encoder receives the current source descriptor and a value from either the
source event or a destination readback.  It deliberately ignores physical UNION
member names and destination display formatting; those are storage details, not
source identity.  The same function is used by writes, predicates, and typed
shadow copies.
"""

from __future__ import annotations

import json
import math
import re
import struct
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .schema_registry import TableSchema


def _canonical_decimal_text(value: Any) -> str:
    """Render a finite PostgreSQL number without scale or exponent noise."""
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{value!r} is not a number") from exc
    if not decimal.is_finite():
        raise ValueError(f"{value!r} is not a finite number")
    if decimal == 0:
        return "0"
    text = format(decimal, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _jsonb_parse(value: Any) -> Any:
    """Parse one JSONB wire value with exact decimals and last-key-wins."""
    from .typed_types import JsonbNull

    if isinstance(value, JsonbNull):
        return None
    if isinstance(value, str):
        return json.loads(
            value,
            parse_int=Decimal,
            parse_float=Decimal,
            object_pairs_hook=lambda pairs: {
                str(key): item for key, item in pairs
            },
        )
    return value


def _jsonb_identity(value: Any) -> Any:
    """Encode PostgreSQL JSONB equality classes recursively."""
    from .typed_types import JsonbNull

    if isinstance(value, JsonbNull) or value is None:
        return {"null": True}
    if isinstance(value, bool):
        return {"boolean": value}
    if isinstance(value, (Decimal, int, float)):
        return {"number": _canonical_decimal_text(value)}
    if isinstance(value, str):
        return {"string": value}
    if isinstance(value, Mapping):
        items = [[str(key), _jsonb_identity(item)] for key, item in value.items()]
        items.sort(key=lambda item: item[0])
        return {"object": items}
    if isinstance(value, (list, tuple)):
        return {"array": [_jsonb_identity(item) for item in value]}
    return {"string": str(value)}


def _jsonb_text(value: Any) -> str:
    """Serialize the parsed JSONB tree as canonical compact JSON text."""
    from .typed_types import JsonbNull

    if isinstance(value, JsonbNull) or value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (Decimal, int, float)):
        return _canonical_decimal_text(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, Mapping):
        items = sorted(
            ((str(key), item) for key, item in value.items()),
            key=lambda pair: pair[0],
        )
        return "{" + ",".join(
            json.dumps(key, ensure_ascii=False, separators=(",", ":"))
            + ":"
            + _jsonb_text(item)
            for key, item in items
        ) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_jsonb_text(item) for item in value) + "]"
    return json.dumps(str(value), ensure_ascii=False, separators=(",", ":"))


def canonical_jsonb_identity(value: Any) -> Any:
    """Public codec entry point for PostgreSQL-semantic JSONB identity."""
    return _jsonb_identity(_jsonb_parse(value))


def canonical_jsonb_text(value: Any) -> str:
    """Return the physical JSON text used for indexed JSONB source keys."""
    return _jsonb_text(_jsonb_parse(value))


def _canonical_float(value: Any, *, bits: int | None = None) -> str:
    """Encode one PostgreSQL floating equality class without signed-zero drift."""
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "Infinity" if number > 0 else "-Infinity"
    if number == 0:
        return "0"
    if bits == 32:
        number = struct.unpack("!f", struct.pack("!f", number))[0]
    return repr(number)


def _decimal_microseconds(value: str) -> int:
    """Convert an interval seconds token to integer microseconds exactly."""
    scaled = Decimal(value) * Decimal(1_000_000)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"interval precision exceeds microseconds: {value!r}")
    return int(scaled)


_INTERVAL_ISO = re.compile(
    r"(?P<sign>[+-])?P(?:(?P<years>[0-9]+(?:\.[0-9]+)?)Y)?"
    r"(?:(?P<months>[0-9]+(?:\.[0-9]+)?)M)?"
    r"(?:(?P<weeks>[0-9]+(?:\.[0-9]+)?)W)?"
    r"(?:(?P<days>[0-9]+(?:\.[0-9]+)?)D)?"
    r"(?:T(?:(?P<hours>[0-9]+(?:\.[0-9]+)?)H)?"
    r"(?:(?P<minutes>[0-9]+(?:\.[0-9]+)?)M)?"
    r"(?:(?P<seconds>[0-9]+(?:\.[0-9]+)?)S)?)?",
    re.IGNORECASE,
)
_INTERVAL_TOKEN = re.compile(
    r"(?P<value>[+-]?[0-9]+(?:\.[0-9]+)?)\s*"
    r"(?P<unit>years?|mons?|months?|weeks?|days?|hours?|hrs?|minutes?|mins?|seconds?|secs?)",
    re.IGNORECASE,
)
_INTERVAL_CLOCK = re.compile(
    r"(?P<hours>[+-]?[0-9]+):(?P<minutes>[0-9]{2}):"
    r"(?P<seconds>[0-9]{2}(?:\.[0-9]+)?)"
)


def _interval_units(value: Any) -> dict[str, int]:
    """Return exact integer ``(months, days, microseconds)`` equality units.

    PostgreSQL compares a calendar month as 30 days.  DuckDB's Python adapter
    exposes the same native interval as ``timedelta`` and therefore has no month
    field to read back.  Folding months into days preserves PostgreSQL equality
    while making source text and destination readback use one shape.  No float
    arithmetic is used, so large-day intervals retain every microsecond.
    """
    if isinstance(value, timedelta):
        return {
            "months": 0,
            "days": int(value.days),
            "microseconds": int(value.seconds * 1_000_000 + value.microseconds),
        }
    if isinstance(value, Mapping) and {"months", "days", "microseconds"} <= set(value):
        months = int(value["months"])
        return {
            "months": 0,
            "days": int(value["days"]) + months * 30,
            "microseconds": int(value["microseconds"]),
        }

    text = str(value).strip()
    months = Decimal(0)
    days = Decimal(0)
    microseconds = 0
    iso = _INTERVAL_ISO.fullmatch(text)
    if iso:
        sign = -1 if iso.group("sign") == "-" else 1
        months = sign * (
            Decimal(iso.group("years") or 0) * 12
            + Decimal(iso.group("months") or 0)
        )
        days = sign * (
            Decimal(iso.group("weeks") or 0) * 7
            + Decimal(iso.group("days") or 0)
        )
        microseconds = sign * (
            int(Decimal(iso.group("hours") or 0) * 3_600_000_000)
            + int(Decimal(iso.group("minutes") or 0) * 60_000_000)
            + _decimal_microseconds(iso.group("seconds") or "0")
        )
    else:
        tokens = list(_INTERVAL_TOKEN.finditer(text))
        clock = _INTERVAL_CLOCK.search(text)
        if not tokens and not clock:
            raise ValueError(f"{value!r} is not an interval value")
        for token in tokens:
            number = Decimal(token.group("value"))
            unit = token.group("unit").lower()
            if unit.startswith("year"):
                months += number * 12
            elif unit.startswith(("mon", "month")):
                months += number
            elif unit.startswith("week"):
                days += number * 7
            elif unit.startswith("day"):
                days += number
            elif unit.startswith(("hour", "hr")):
                microseconds += int(number * 3_600_000_000)
            elif unit.startswith(("minute", "min")):
                microseconds += int(number * 60_000_000)
            else:
                microseconds += _decimal_microseconds(str(number))
        if clock:
            microseconds += (
                int(Decimal(clock.group("hours")) * 3_600_000_000)
                + int(Decimal(clock.group("minutes")) * 60_000_000)
                + _decimal_microseconds(clock.group("seconds"))
            )
    if months != months.to_integral_value() or days != days.to_integral_value():
        raise ValueError(f"interval calendar units are fractional: {value!r}")
    days += months * 30
    return {"months": 0, "days": int(days), "microseconds": int(microseconds)}


def _numeric_identity(value: Any) -> dict[str, Any]:
    """Collapse raw, bounded-UNION, and outer-UNION numerics to one tree."""
    from .typed_types import UnionValue

    member: str | None = None
    while isinstance(value, UnionValue):
        member = str(value.member).lower()
        value = value.value
        if member in {"finite", "special"}:
            break
    if isinstance(value, Mapping) and {"coefficient", "scale"} <= set(value):
        special = value.get("special")
        if special is not None:
            return {"numeric": {"special": _canonical_float(special)}}
        coefficient = int(value["coefficient"])
        scale = int(value["scale"] or 0)
        value = Decimal(coefficient).scaleb(-scale)
    if member == "special" or (
        isinstance(value, float) and (math.isnan(value) or math.isinf(value))
    ):
        return {"numeric": {"special": _canonical_float(value)}}
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{value!r} is not a numeric value") from exc
    if not decimal.is_finite():
        return {"numeric": {"special": _canonical_float(float(decimal))}}
    return {"numeric": {"finite": _canonical_decimal_text(decimal)}}


def _unwrap_destination_union(value: Any, source_kind: str) -> Any:
    """Remove physical source-type UNION tags before source-semantic encoding."""
    from .typed_types import UnionValue

    if source_kind in {"numeric", "decimal", "numeric_variable", "variable_scale_numeric"}:
        return value
    while isinstance(value, UnionValue):
        value = value.value
    return value


def _union_contains_null(value: Any) -> bool:
    from .typed_types import UnionValue

    return isinstance(value, UnionValue) and (
        value.value is None or _union_contains_null(value.value)
    )


def _identity_runtime(value: Any) -> Any:
    """Return the JSON-safe representation for an already-normalized runtime leaf."""
    from .typed_types import JsonbNull, UnionValue

    if isinstance(value, JsonbNull):
        return {"jsonb_null": True}
    if isinstance(value, UnionValue):
        return {"union_member": value.member, "value": _identity_runtime(value.value)}
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_hex": bytes(value).hex()}
    if isinstance(value, Decimal):
        return {"decimal": _canonical_decimal_text(value)}
    if isinstance(value, datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, date) and not isinstance(value, datetime):
        return {"date": value.isoformat()}
    if isinstance(value, time):
        return {"time": value.isoformat()}
    if isinstance(value, timedelta):
        return {"interval_units": _interval_units(value)}
    if isinstance(value, float):
        return {"float": _canonical_float(value)}
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
    """Encode one value recursively according to source semantics."""
    from .typed_types import encode_value

    if descriptor is None:
        return _identity_runtime(value)
    source = descriptor
    seen: set[int] = set()
    while getattr(source, "domain_base", None) is not None and id(source) not in seen:
        seen.add(id(source))
        source = source.domain_base
    kind = str(source.kind or source.qualified_name).lower()
    # Physical source-type UNION tags may occur at any recursive node (for
    # example a UNION-valued field inside a composite key), not only at the
    # root key column.  Strip those storage tags before the descriptor encoder
    # sees the value; numeric's finite/special UNION remains semantic and is
    # normalized by _numeric_identity below.
    value = _unwrap_destination_union(value, kind)
    if _union_contains_null(value):
        return None
    if kind == "jsonb" and value is None:
        # SQL NULL and a JSONB root-null document collapse in some destination
        # readback APIs; retain SQL NULL before it reaches that display layer.
        return {"sql_null": True}
    encoded = encode_value(value, descriptor)
    if encoded is None and kind != "jsonb":
        return None
    if kind in {"real", "float4"}:
        return {"float": _canonical_float(encoded, bits=32)}
    if kind in {"double", "float8", "double precision"}:
        return {"float": _canonical_float(encoded)}
    if kind in {"numeric", "decimal"}:
        return _numeric_identity(encoded)
    if kind in {"numeric_variable", "variable_scale_numeric"}:
        if isinstance(encoded, Mapping) and encoded.get("special") is not None:
            return {"numeric": {"special": _canonical_float(encoded["special"])}}
        if isinstance(encoded, Mapping):
            coefficient = encoded.get("coefficient")
            scale = int(encoded.get("scale") or 0)
            if coefficient is None:
                return {"numeric": {"finite": "0"}}
            return _numeric_identity(Decimal(int(coefficient)).scaleb(-scale))
        return _numeric_identity(encoded)
    if kind == "jsonb":
        return {"jsonb": canonical_jsonb_identity(encoded)}
    if kind == "json":
        return {"json_text": str(encoded)}
    if kind in {"bytea", "bytes", "blob"}:
        return _identity_runtime(encoded)
    if kind in {"timestamptz", "zonedtimestamp"} and isinstance(encoded, datetime):
        return {"timestamptz": encoded.astimezone(UTC).isoformat()}
    if kind == "interval":
        return {"interval": _interval_units(encoded)}
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
        return {"list": [_identity_tree(item, source.array_element) for item in values]}
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
) -> str:
    """Build the one current canonical identity for writes, lookups, and swaps."""
    descriptors = descriptors or {}
    key_columns = tuple(key_columns or table.source_key_columns or table.key_columns)
    components: list[dict[str, Any]] = []
    for column, value in zip(key_columns, values, strict=True):
        descriptor = descriptors.get(column) or table.source_descriptors.get(column)
        source_kind = str(getattr(descriptor, "kind", "")).lower() if descriptor else ""
        semantic_value = _unwrap_destination_union(value, source_kind)
        fingerprint = descriptor.fingerprint if descriptor is not None else "legacy"
        if (semantic_value is None or _union_contains_null(semantic_value)) and source_kind != "jsonb":
            components.append({"descriptor": fingerprint, "state": "sql_null"})
            continue
        components.append({
            "descriptor": fingerprint,
            "member": "value",
            "value": _identity_tree(semantic_value, descriptor),
        })
    return json.dumps(components, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def identity_value(table: Any, values: tuple[Any, ...], *, key_columns: tuple[str, ...]) -> str:
    """Public spelling for the canonical source identity encoder."""
    return _identity_value(table, values, key_columns=key_columns)


__all__ = [
    "_identity_runtime",
    "_identity_tree",
    "_identity_value",
    "canonical_jsonb_identity",
    "canonical_jsonb_text",
    "identity_value",
]
