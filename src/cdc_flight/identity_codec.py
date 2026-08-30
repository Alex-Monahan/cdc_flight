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
from functools import cmp_to_key
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .schema_registry import TableSchema


def _value_type(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _canonical_decimal_text(value: Any) -> str:
    """Render a finite PostgreSQL number without scale or exponent noise."""
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"value of type {_value_type(value)} is not a number") from exc
    if not decimal.is_finite():
        raise ValueError(f"value of type {_value_type(value)} is not a finite number")
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
    if bits in {16, 32}:
        format_code = "e" if bits == 16 else "f"
        number = struct.unpack(f"!{format_code}", struct.pack(f"!{format_code}", number))[0]
    return repr(number)


def _decimal_microseconds(value: str) -> int:
    """Convert an interval seconds token to integer microseconds exactly."""
    scaled = Decimal(value) * Decimal(1_000_000)
    if scaled != scaled.to_integral_value():
        raise ValueError("interval precision exceeds microseconds")
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
            raise ValueError(f"value of type {_value_type(value)} is not an interval value")
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
        raise ValueError("interval calendar units are fractional")
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
        raise ValueError(f"value of type {_value_type(value)} is not a numeric value") from exc
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
    from .typed_types import JsonbNull, PostgresInfinity, UnionValue

    if isinstance(value, JsonbNull):
        return {"jsonb_null": True}
    if isinstance(value, UnionValue):
        return {"union_member": value.member, "value": _identity_runtime(value.value)}
    if isinstance(value, PostgresInfinity):
        return {"infinity": "positive" if value.positive else "negative"}
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


class _RangeInfinity:
    """An internal bound used when discrete canonicalization crosses a type edge."""

    __slots__ = ("positive",)

    def __init__(self, positive: bool):
        self.positive = bool(positive)


def _range_name(source: Any) -> str:
    return str(getattr(source, "qualified_name", "")).rsplit(".", 1)[-1].lower()


def _range_subtype(source: Any) -> Any:
    value = source
    seen: set[int] = set()
    while getattr(value, "domain_base", None) is not None and id(value) not in seen:
        seen.add(id(value))
        value = value.domain_base
    return value


def _range_step(value: Any, subtype: Any) -> Any:
    """Return the next discrete subtype value without float arithmetic."""
    from .typed_types import PostgresInfinity

    if isinstance(value, PostgresInfinity):
        return value
    subtype = _range_subtype(subtype)
    kind = str(getattr(subtype, "kind", "")).lower()
    if kind in {"int2", "smallint", "int4", "integer", "int8", "bigint"}:
        return int(value) + 1
    if kind == "date" and isinstance(value, date):
        try:
            return value + timedelta(days=1)
        except OverflowError:
            return _RangeInfinity(True)
    return value


def _range_discrete(source: Any) -> bool:
    """Whether PostgreSQL has a built-in canonical [) form for this range."""
    return _range_name(source) in {"int4range", "int8range", "daterange"}


def _range_order_value(value: Any) -> Any:
    """Unwrap destination wrappers before comparing two same-subtype bounds."""
    from .typed_types import PostgresInfinity, UnionValue

    while isinstance(value, UnionValue):
        value = value.value
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    if isinstance(value, (_RangeInfinity, PostgresInfinity)):
        return value
    if isinstance(value, Mapping):
        # DuckDB's variable-scale NUMERIC child is a STRUCT on destination
        # readback.  This is a value-boundary fallback only; canonical source text
        # never enters this comparison path.
        if "coefficient" in value and "scale" in value:
            try:
                return Decimal(int(value["coefficient"])).scaleb(-int(value["scale"] or 0))
            except (TypeError, ValueError, InvalidOperation):
                return value
        if value.get("special") is not None:
            return value["special"]
    return value


def _range_compare(left: Any, right: Any) -> int:
    """Compare bounds after their source subtype has been encoded."""
    from .typed_types import PostgresInfinity

    if isinstance(left, (_RangeInfinity, PostgresInfinity)):
        if isinstance(right, (_RangeInfinity, PostgresInfinity)):
            return (left.positive > right.positive) - (left.positive < right.positive)
        return 1 if left.positive else -1
    if isinstance(right, (_RangeInfinity, PostgresInfinity)):
        return -1 if right.positive else 1
    left = _range_order_value(left)
    right = _range_order_value(right)
    try:
        return (left > right) - (left < right)
    except TypeError:
        left_text = repr(left)
        right_text = repr(right)
        return (left_text > right_text) - (left_text < right_text)


def _normalise_range(value: Any, source: Any) -> dict[str, Any]:
    """Normalize one PostgreSQL range before its bounds enter the identity tree."""
    if not isinstance(value, Mapping):
        raise ValueError(f"value of type {_value_type(value)} is not an encoded range")
    subtype = source.range_subtype
    empty = bool(value.get("is_empty", False))
    lower = value.get("lower")
    upper = value.get("upper")
    lower_inclusive = bool(value.get("lower_inclusive", False)) and lower is not None
    upper_inclusive = bool(value.get("upper_inclusive", False)) and upper is not None
    if empty:
        return {
            "empty": True,
            "lower": None,
            "upper": None,
            "lower_inclusive": False,
            "upper_inclusive": False,
        }
    if _range_discrete(source):
        if lower is not None and not lower_inclusive:
            lower = _range_step(lower, subtype)
        if lower is not None:
            lower_inclusive = True
        if upper is not None and upper_inclusive:
            upper = _range_step(upper, subtype)
        if upper is not None:
            upper_inclusive = False
    if lower is not None and upper is not None:
        order = _range_compare(lower, upper)
        if order > 0 or (order == 0 and not (lower_inclusive and upper_inclusive)):
            return {
                "empty": True,
                "lower": None,
                "upper": None,
                "lower_inclusive": False,
                "upper_inclusive": False,
            }
    return {
        "empty": False,
        "lower": lower,
        "upper": upper,
        "lower_inclusive": lower_inclusive,
        "upper_inclusive": upper_inclusive,
    }


def _range_bound_identity(value: Any, subtype: Any) -> Any:
    from .typed_types import PostgresInfinity

    if value is None:
        return {"unbounded": True}
    if isinstance(value, (_RangeInfinity, PostgresInfinity)):
        return {"infinity": "positive" if value.positive else "negative"}
    return _identity_tree(value, subtype)


def _range_identity(value: Any, source: Any) -> dict[str, Any]:
    normalized = _normalise_range(value, source)
    if normalized["empty"]:
        return {"range": {"empty": True}}
    subtype = source.range_subtype
    return {
        "range": {
            "empty": False,
            "lower": _range_bound_identity(normalized["lower"], subtype),
            "lower_inclusive": normalized["lower_inclusive"],
            "upper": _range_bound_identity(normalized["upper"], subtype),
            "upper_inclusive": normalized["upper_inclusive"],
        }
    }


def _ranges_mergeable(left: dict[str, Any], right: dict[str, Any], source: Any) -> bool:
    if left["upper"] is None or right["lower"] is None:
        return True
    order = _range_compare(left["upper"], right["lower"])
    if order > 0:
        return True
    if order < 0:
        return False
    # Discrete canonical ranges are half-open, so equality is adjacency. For a
    # continuous subtype, the shared point must belong to at least one range.
    return _range_discrete(source) or left["upper_inclusive"] or right["lower_inclusive"]


def _merge_ranges(ranges: list[dict[str, Any]], source: Any) -> list[dict[str, Any]]:
    def compare(left: dict[str, Any], right: dict[str, Any]) -> int:
        left_lower = left["lower"]
        right_lower = right["lower"]
        if left_lower is None or right_lower is None:
            if left_lower is None and right_lower is None:
                order = 0
            else:
                order = -1 if left_lower is None else 1
        else:
            order = _range_compare(left_lower, right_lower)
        if order:
            return order
        # PostgreSQL's canonical multirange has one range per equality class;
        # this tie-break only makes malformed/overlapping input deterministic.
        if left["lower_inclusive"] != right["lower_inclusive"]:
            return -1 if left["lower_inclusive"] else 1
        return 0

    ranges.sort(key=cmp_to_key(compare))
    merged: list[dict[str, Any]] = []
    for current in ranges:
        if not merged or not _ranges_mergeable(merged[-1], current, source):
            merged.append(dict(current))
            continue
        previous = merged[-1]
        if previous["upper"] is None or current["upper"] is None:
            previous["upper"] = None
            previous["upper_inclusive"] = False
            continue
        order = _range_compare(previous["upper"], current["upper"])
        if order < 0:
            previous["upper"] = current["upper"]
            previous["upper_inclusive"] = current["upper_inclusive"]
        elif order == 0:
            previous["upper_inclusive"] = (
                previous["upper_inclusive"] or current["upper_inclusive"]
            )
    return merged


def _multirange_identity(value: Any, source: Any) -> dict[str, Any]:
    range_source = source.range_subtype
    normalized = [
        _normalise_range(item, range_source)
        for item in (value if isinstance(value, (list, tuple)) else ())
    ]
    normalized = [item for item in normalized if not item["empty"]]
    return {
        "multirange": [
            _range_identity(item, range_source)["range"]
            for item in _merge_ranges(normalized, range_source)
        ]
    }


def _time_identity(value: Any, *, zoned: bool) -> dict[str, Any]:
    if not isinstance(value, time):
        return {"time": str(value)}
    if not zoned or value.tzinfo is None:
        return {"time": value.isoformat()}
    offset = value.utcoffset() or timedelta()
    day_microseconds = 86_400_000_000
    local_microseconds = (
        (value.hour * 3_600 + value.minute * 60 + value.second) * 1_000_000
        + value.microsecond
    )
    offset_microseconds = (
        (offset.days * 86_400 + offset.seconds) * 1_000_000 + offset.microseconds
    )
    return {"timetz": (local_microseconds - offset_microseconds) % day_microseconds}


_IDENTITY_TEXT_KINDS = frozenset(
    {
        "char", "bpchar", "varchar", "text", "citext", "name", "string",
        # These are the only opaque kinds whose descriptor/value seam can reach
        # identity_codec: encode_value rejects every non-allowlisted opaque kind.
        "tsquery", "jsonpath", "pg_lsn", "tsvector", "xml", "cidr", "macaddr",
        "macaddr8", "int2vector",
    }
)


def _identity_tree(value: Any, descriptor: Any) -> Any:
    """Encode one value recursively according to source semantics."""
    from .typed_types import (
        CanonicalRangeText,
        canonical_multirange_text,
        encode_value,
    )

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
    if (
        isinstance(value, str)
        and "infinity" in value.lower()
        and kind in {
            "range", "daterange", "int4range", "int8range", "numrange",
            "tsrange", "tstzrange",
        }
    ):
        # PostgreSQL's special infinity endpoints are values, not the empty text
        # between unbounded delimiters.  Preserve this source spelling directly;
        # asking the ISO parser to interpret it is precisely the r5 defect.
        return {"range_text": str(value).strip()}
    if isinstance(value, CanonicalRangeText) and kind in {
        "range", "daterange", "int4range", "int8range", "numrange",
        "tsrange", "tstzrange",
    }:
        return {"range_text": str(value)}
    if kind == "multirange" and isinstance(value, (str, bytes, bytearray, memoryview)):
        # Multirange destinations are deliberately VARCHAR because stock
        # Debezium delivers PostgreSQL's output text.  Preserve the established
        # semantic identity for parseable PostgreSQL output, but retain arbitrary
        # connector/source text verbatim when its grammar is not ours to prove.
        # The latter is the important safety property: identity formation must not
        # reject or invent a source value merely because a future PostgreSQL type
        # spelling is unknown to this Python parser.
        from .typed_types import _multirange_parts, encode_value

        text = str(canonical_multirange_text(value, source))
        try:
            encoded_parts = [
                encode_value(part, source.range_subtype)
                for part in _multirange_parts(text)
            ]
            return _multirange_identity(encoded_parts, source)
        except (TypeError, ValueError):
            return {"multirange_text": text}
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
    if kind in {"smallint", "int2", "smallserial", "integer", "int", "int4", "serial", "bigint", "int8", "bigserial", "oid", "xid"}:
        return {"integer": str(int(encoded))}
    if kind in {"boolean", "bool", "bit1"}:
        return {"boolean": bool(encoded)}
    if kind in {"char", "bpchar"}:
        return {"text": str(encoded).rstrip(" ")}
    if kind in _IDENTITY_TEXT_KINDS:
        return {"text": str(encoded)}
    if kind == "enum":
        return {"enum": str(encoded)}
    if kind == "uuid":
        return {"uuid": str(encoded).lower()}
    if kind == "date" and isinstance(encoded, date):
        return {"date": encoded.isoformat()}
    if kind in {"time", "time_microseconds", "microtime"}:
        return _time_identity(encoded, zoned=False)
    if kind in {"timetz", "zonedtime"}:
        return _time_identity(encoded, zoned=True)
    if kind in {"timestamp", "timestamp_microseconds", "microtimestamp"}:
        return {"timestamp": encoded.isoformat() if isinstance(encoded, datetime) else str(encoded)}
    if kind in {"range", "daterange", "int4range", "int8range", "numrange", "tsrange", "tstzrange"}:
        return _range_identity(encoded, source)
    if kind == "multirange":
        return _multirange_identity(encoded, source)
    if kind == "bit" or kind == "varbit":
        mapping = encoded if isinstance(encoded, Mapping) else {}
        bits = mapping.get("bits")
        return {
            "bits": bytes(bits or b"").hex() if isinstance(bits, (bytes, bytearray)) else str(bits),
            "bit_length": int(mapping.get("bit_length", 0)),
        }
    if kind in {"vector", "halfvec"}:
        bits = 16 if kind == "halfvec" else 32
        values = encoded if isinstance(encoded, (list, tuple)) else []
        return {"vector": [_canonical_float(item, bits=bits) for item in values]}
    if kind == "sparsevec":
        mapping = encoded if isinstance(encoded, Mapping) else {}
        vector = mapping.get("vector", {})
        items = [
            [str(key), _canonical_float(item, bits=32)]
            for key, item in (vector.items() if isinstance(vector, Mapping) else ())
        ]
        items.sort(key=lambda item: int(item[0]) if item[0].lstrip("-").isdigit() else item[0])
        return {"sparsevec": [int(mapping.get("dimensions", 0)), items]}
    if kind == "point":
        mapping = encoded if isinstance(encoded, Mapping) else {}
        return {
            "point": [
                _canonical_float(mapping.get("x")),
                _canonical_float(mapping.get("y")),
            ]
        }
    if kind in {"geometry", "geography", "postgis"}:
        mapping = encoded if isinstance(encoded, Mapping) else {}
        wkb = mapping.get("wkb")
        return {
            "geometry": {
                "srid": int(mapping.get("srid", 0)),
                "wkb": bytes(wkb or b"").hex()
                if isinstance(wkb, (bytes, bytearray))
                else str(wkb),
            }
        }
    if kind in {"struct", "composite"}:
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
        native = getattr(table, "native_types", {}).get(column)
        if source_kind == "multirange" and getattr(native, "kind", None) == "VARCHAR":
            from .typed_types import canonical_multirange_text

            semantic_value = canonical_multirange_text(semantic_value, descriptor)
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


def _stored_identity_source_value(identity: Any, component: int) -> Any | None:
    """Recover retained PostgreSQL source text for a typed shadow copy.

    A source range event carries PostgreSQL's canonical text into the internal
    identity.  If a later typed UNION swap changes that key's current descriptor,
    the native destination STRUCT/LIST no longer contains that text; using its
    Python display value would create a different current identity.  This helper
    only reuses the value already in the current row identity.  It does not search
    historical candidates or create a second identity format.
    """
    from .typed_types import CanonicalRangeText

    if not isinstance(identity, str):
        return None
    try:
        components = json.loads(identity)
        value = components[component].get("value")
    except (IndexError, AttributeError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(value, Mapping):
        return None
    for name in ("range_text", "multirange_text"):
        text = value.get(name)
        if isinstance(text, str):
            return CanonicalRangeText(text)
    return None


def identity_value(table: Any, values: tuple[Any, ...], *, key_columns: tuple[str, ...]) -> str:
    """Public spelling for the canonical source identity encoder."""
    return _identity_value(table, values, key_columns=key_columns)


__all__ = [
    "_identity_runtime",
    "_identity_tree",
    "_identity_value",
    "_stored_identity_source_value",
    "canonical_jsonb_identity",
    "canonical_jsonb_text",
    "identity_value",
]
