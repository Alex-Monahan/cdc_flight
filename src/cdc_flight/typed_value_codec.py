"""Value encoding and transport ownership for declared PostgreSQL types."""

from __future__ import annotations

import base64
import binascii
import ipaddress
import json
import math
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from . import identity_codec
from .typed_types import (
    JSONB_NULL,
    CanonicalRangeText,
    InvalidTypedValue,
    JsonbNull,
    NativeType,
    OpaqueText,
    PostgresInfinity,
    SourceTypeDescriptor,
    UnionValue,
    UnsupportedType,
    native_type,
    numeric_value,
    union_member_name,
)


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
    if kind in {
        "smallint",
        "int2",
        "smallserial",
        "integer",
        "int",
        "int4",
        "serial",
        "bigint",
        "int8",
        "bigserial",
        "oid",
        "xid",
    }:
        try:
            integer = int(value)
        except (TypeError, ValueError) as exc:
            raise InvalidTypedValue(f"{value!r} is not an integer") from exc
        limits = (
            (-(2**15), 2**15 - 1)
            if kind in {"smallint", "int2", "smallserial"}
            else (
                (-(2**31), 2**31 - 1)
                if kind in {"integer", "int", "int4", "serial"}
                else (-(2**63), 2**63 - 1)
            )
        )
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
    if kind in {
        "timestamp",
        "timestamp_microseconds",
        "microtimestamp",
        "timestamptz",
        "zonedtimestamp",
    }:
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
            # A source event is already PostgreSQL's multirange output text, and it
            # is carried verbatim.  The connector's base64 transport marker is
            # unwrapped one level up, in `adapt_value`/`mark_canonical_range_text`,
            # which every live value path goes through; no range grammar or
            # equality logic belongs on this value path either way.
            return value
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
        # PostgreSQL/stock Debezium already supplied the money text.  Money is an
        # unconditional VARCHAR transport boundary: no locale lookup, formatting,
        # parsing, validation or reconstruction, and — deliberately — no path out
        # of this branch that can raise.  It is placed above the generic opaque
        # branch precisely so that neither the descriptor allowlist nor
        # `_decode_opaque_text`'s transport heuristics can ever refuse a `money`
        # column: money must never block a table under any `lc_monetary`.  The
        # claim used to be false one line earlier: `native_type` above still ran
        # the money descriptor through the opaque OID allowlist and raised
        # `UnsupportedType` for any OID other than 790 before this branch could
        # be reached.  `native_type` now resolves money by kind alone, so the
        # whole call really cannot raise for a money descriptor.
        return value
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
    if _kind_name(source.kind, source.qualified_name) == "multirange" and target.kind == "VARCHAR":
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
    return NativeType(
        "NUMERIC_VARIABLE",
        "STRUCT(coefficient BIGNUM,scale INTEGER,special DOUBLE)",
        value,
        fields=fields,
        indexable=False,
    )


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
                    raise InvalidTypedValue(f"{special!r} is not a numeric special value")
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
        bit_length = (
            source.typmod if source.kind == "bit" and source.typmod else len(packed or b"") * 8
        )
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
        "lower": (encode_value(lower_text, source.range_subtype) if lower_text != "" else None),
        "upper": (encode_value(upper_text, source.range_subtype) if upper_text != "" else None),
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


def canonical_multirange_text(value: Any, source: SourceTypeDescriptor) -> CanonicalRangeText:
    """Unwrap the connector transport without rewriting PostgreSQL's text.

    ``include.unknown.datatypes=true`` reaches the JSON engine through the pinned
    ``binary.handling.mode=base64`` converter, so an opaque multirange byte value is
    observed here as base64 text.  Decode only that opaque transport form; a source
    event already marked ``CanonicalRangeText`` is retained byte-for-byte.  Lists
    remain a compatibility/value-boundary form for existing snapshot and identity
    tests and are rendered only because they are not delivered source text.
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
        return CanonicalRangeText(text)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            return CanonicalRangeText(value)
        try:
            decoded = base64.b64decode(text, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            # Plain connector text is already the value.  Do not reject it merely
            # because Python cannot prove its grammar.
            return CanonicalRangeText(value)
        return CanonicalRangeText(decoded)
    if isinstance(value, (list, tuple)):
        return _render_multirange_parts(value, source)
    raise InvalidTypedValue(f"{value!r} is not a multirange value")


def _render_multirange_parts(
    values: list | tuple, source: SourceTypeDescriptor
) -> CanonicalRangeText:
    """Render compatibility range values after PostgreSQL equality merging."""

    encoded = [_encode_range(item, source.range_subtype) for item in values]
    normalized = [identity_codec._normalise_range(item, source.range_subtype) for item in encoded]
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
    return value.replace(r"\\", "\\").replace(r"\"", '"')


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
        # Stock Debezium's PostgreSQL converter can carry a date infinity as the
        # out-of-range java.sql.Date millisecond sentinel instead of the textual
        # spelling.  It is a value marker, not an epoch-day date; treating it as
        # days either overflows immediately or fabricates a finite date.
        date_infinity = _postgres_date_infinity_from_epoch_days(value)
        if date_infinity is not None:
            return date_infinity  # type: ignore[return-value]
        infinity = _postgres_infinity_from_numeric(value)
        if infinity is not None:
            return infinity  # type: ignore[return-value]
        try:
            return date(1970, 1, 1) + timedelta(days=value)
        except OverflowError as exc:
            raise InvalidTypedValue(
                f"date epoch-day value {value!r} is outside Python's supported range"
            ) from exc
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
        # Debezium's adaptive temporal converters preserve PostgreSQL's two
        # infinity sentinels as a numeric value.  Check the marker before asking
        # datetime to materialize a year outside Python's supported range.
        infinity = _postgres_infinity_from_numeric(value)
        if infinity is not None:
            return infinity  # type: ignore[return-value]
        return datetime.fromtimestamp(value / 1_000_000, tz=UTC if zoned else None)
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if zoned and result.tzinfo is None:
            result = result.replace(tzinfo=UTC)
        return result
    except ValueError as exc:
        raise InvalidTypedValue(f"{value!r} is not an ISO timestamp") from exc


# PostgreSQL's JDBC/stock-Debezium infinity representations.  The first pair is
# the exact timestamp/date sentinel emitted by PostgresValueConverter; the second
# pair covers the Long extrema used by its epoch-nanos path.  Keeping this marker
# recognition exact is important: ordinary, very-large finite epoch values must
# still fail loudly instead of being silently relabelled as infinity.
_POSITIVE_POSTGRES_INFINITY_NUMERICS = frozenset({9223372036825200000, 9223372036854775807})
_NEGATIVE_POSTGRES_INFINITY_NUMERICS = frozenset({-9223372036832400000, -9223372036854775808})
# With Debezium's ``Date`` logical representation, PostgreSQL's two sentinels
# can arrive through either temporal precision path.  The LocalDate path has
# the epoch-day values below.  The java.sql.Date path first creates a Date from
# the millisecond sentinel, then Debezium narrows its epoch day to int32; the
# two wrapped values are included explicitly as well.  These are markers, not
# broad ranges, so ordinary out-of-range finite dates still fail loudly.
_POSTGRES_DATE_INFINITY_EPOCH_DAYS = {
    -2147472692: True,
    -2147472691: False,
    -622191234: True,
    -625821272: False,
}


def _postgres_infinity_from_numeric(value: int) -> PostgresInfinity | None:
    if value in _POSITIVE_POSTGRES_INFINITY_NUMERICS:
        return PostgresInfinity(True)
    if value in _NEGATIVE_POSTGRES_INFINITY_NUMERICS:
        return PostgresInfinity(False)
    return None


def _postgres_date_infinity_from_epoch_days(value: int) -> PostgresInfinity | None:
    positive = _POSTGRES_DATE_INFINITY_EPOCH_DAYS.get(value)
    return None if positive is None else PostgresInfinity(positive)


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


def _connect_enum_labels(
    schema: Mapping[str, Any], parameters: Mapping[str, Any]
) -> tuple[str, ...]:
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
    raise InvalidTypedValue(f"{source.qualified_name} opaque payload must be text or bytes")


def _kind_name(kind: Any, qualified_name: str) -> str:
    value = str(kind or "unknown").lower().strip()
    name = qualified_name.rsplit(".", 1)[-1].lower()
    aliases = {
        "int2": "int2",
        "smallint": "int2",
        "int4": "int4",
        "integer": "int4",
        "int8": "int8",
        "bigint": "int8",
        "float4": "float4",
        "real": "float4",
        "float8": "float8",
        "double": "float8",
        "double precision": "float8",
        "bool": "bool",
        "boolean": "bool",
        "character varying": "varchar",
        "bpchar": "bpchar",
        "character": "char",
        "json": "json",
        "jsonb": "jsonb",
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


_OPAQUE_TEXT_KINDS = frozenset(
    {
        "tsquery",
        "jsonpath",
        "pg_lsn",
        "tsvector",
        "xml",
        "money",
        "inet",
        "cidr",
        "macaddr",
        "macaddr8",
        "int2vector",
    }
)
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
_OBSCURE_TEXT_KINDS = frozenset(
    {
        "ltree",
        "oidvector",
        "xid8",
        "regproc",
        "regprocedure",
        "regoper",
        "regoperator",
        "regclass",
        "regcollation",
        "regconfig",
        "regdictionary",
        "regnamespace",
        "regrole",
        "regtype",
        "aclitem",
        "pg_node_tree",
        "tinterval",
        "snapshot",
        "opaque",
    }
)


def _opaque_descriptor_allowed(descriptor: SourceTypeDescriptor, kind: str) -> bool:
    """Allow only catalog-identified built-in opaque types.

    Names alone are not an authority: a user type can have the same spelling in a
    different schema.  The catalog OID is retained in every source descriptor and
    is the allowlist key here.
    """

    return descriptor.oid in _OPAQUE_TEXT_OIDS.get(kind, ())
