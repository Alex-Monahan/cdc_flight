"""Compatibility facade for the split apply ownership modules.

The public module name remains stable for callers, while ownership is explicit:
schema_registry owns DDL, identity_codec owns source identity, and
typed_materialization owns typed row writes and shadow copying.
"""

from __future__ import annotations

import json
import math
from typing import Any

from .identity_codec import (  # noqa: F401
    _identity_runtime,
    _identity_tree,
    _identity_value,
    identity_value,
)
from .naming import CDCF_COMMIT_ID, CDCF_EVENT_ID, CDCF_TOTAL_ORDER
from .schema_registry import (  # noqa: F401
    SchemaRegistry,
    TableSchema,
    _is_numeric_inner_union,
    _is_top_level_union,
    _normalise_type,
    _physical_union_native,
    _type_sql_equal,
    _union_member_names,
    _union_members,
    assert_identity_is_unique,
    widen,
)
from .typed_materialization import (  # noqa: F401
    _copy_rows_with_identity,
    _typed_assignment,
    bulk_insert,
    delete_keys,
    insert_rows,
    insert_typed_rows,
    update_rows,
)

BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR = "BOOLEAN", "BIGINT", "DOUBLE", "JSON", "VARCHAR"


def sql_type(value: Any) -> str | None:
    """DuckDB type for one JSON-decoded value; None when it tells us nothing."""
    if value is None:
        return None
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return BIGINT
    if isinstance(value, float):
        return DOUBLE
    if isinstance(value, (dict, list)):
        return JSON_T
    return VARCHAR


def bind(value: Any, column_type: str) -> Any:
    """Coerce a JSON-decoded value for one destination parameter type."""
    if value is None:
        return None
    try:
        from .typed_types import UnionValue
        if isinstance(value, UnionValue):
            value = value.value
    except ImportError:  # pragma: no cover
        pass
    if column_type == JSON_T:
        return value if isinstance(value, str) else json.dumps(value, default=str)
    if column_type == VARCHAR:
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return str(value)
    if column_type == DOUBLE:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("nan", "+nan", "-nan"):
                return math.nan
            if lowered in ("infinity", "inf", "+infinity", "+inf"):
                return math.inf
            if lowered in ("-infinity", "-inf"):
                return -math.inf
            try:
                return float(value)
            except ValueError:
                return None
        return float(value)
    if column_type in {"SMALLINT", "INTEGER", "INT", "INT32", BIGINT, "INT64", "HUGEINT"}:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return int(value)
    if column_type == BOOLEAN:
        if isinstance(value, str):
            return value.strip().lower() in ("true", "t", "1")
        return bool(value)
    if column_type in {"BLOB", "BYTEA"}:
        return value if isinstance(value, (bytes, bytearray)) else str(value).encode()
    return value


__all__ = [
    "BIGINT",
    "BOOLEAN",
    "CDCF_COMMIT_ID",
    "CDCF_EVENT_ID",
    "CDCF_TOTAL_ORDER",
    "DOUBLE",
    "JSON_T",
    "VARCHAR",
    "SchemaRegistry",
    "TableSchema",
    "assert_identity_is_unique",
    "bind",
    "bulk_insert",
    "delete_keys",
    "identity_value",
    "insert_rows",
    "insert_typed_rows",
    "sql_type",
    "update_rows",
    "widen",
]
