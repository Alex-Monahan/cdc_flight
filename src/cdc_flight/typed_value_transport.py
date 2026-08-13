"""Small serialization and descriptor-parameter helpers for typed values."""

from __future__ import annotations

import base64
import math
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from .typed_types import (
    JSONB_NULL,
    JsonbNull,
    OpaqueText,
    UnionValue,
    _freeze_pairs,
)


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


def _float_marker(value: str) -> float:
    lowered = value.strip().lower()
    if lowered in {"nan", "+nan", "-nan"}:
        return math.nan
    if lowered in {"infinity", "+infinity", "inf", "+inf"}:
        return math.inf
    if lowered in {"-infinity", "-inf"}:
        return -math.inf
    return float(value)


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
            return _float_marker(str(value["__float__"]))
        return {key: _from_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_from_jsonable(item) for item in value]
    return value
