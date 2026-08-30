"""Destination DDL ownership for the schema registry.

This module owns physical type normalization, safe widening decisions, and the
three table-rebuild operations.  It deliberately knows nothing about source
catalog state or row identity; callers provide the registry object whose open
transaction it mutates.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .errors import DestinationIdentityCollision, SchemaEvolutionRefused
from .naming import quote

BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR = "BOOLEAN", "BIGINT", "DOUBLE", "JSON", "VARCHAR"
OWNER = "destination-ddl"

log = logging.getLogger("cdc_flight.schema_ddl")


def _definition_for_column(column: str, type_name: str) -> str:
    """Give the soft-delete marker its durable non-null live-row default."""
    if column == "cdcf_deleted" and str(type_name).upper() == "BOOLEAN":
        return "BOOLEAN NOT NULL DEFAULT false"
    return type_name


def widen(current: str | None, incoming: str | None) -> str | None:
    """Return the least destination type that holds both inputs."""
    if current is None:
        return incoming
    if incoming is None or current == incoming:
        return current
    if {current, incoming} == {BIGINT, DOUBLE}:
        return DOUBLE
    return VARCHAR


_RECOGNISED_TYPES = frozenset(
    {
        BOOLEAN,
        BIGINT,
        DOUBLE,
        JSON_T,
        "VARIANT",
        VARCHAR,
        "TEXT",
        "STRING",
        "INT64",
        "HUGEINT",
        "INTEGER",
        "INT",
        "INT32",
        "SMALLINT",
        "FLOAT",
        "REAL",
        "FLOAT8",
    }
)


def _normalise_type(duckdb_type: str) -> str:
    upper = str(duckdb_type).upper()
    if upper.startswith("VARCHAR") or upper in ("TEXT", "STRING"):
        return VARCHAR
    if upper in ("SMALLINT", "INT16"):
        return "SMALLINT"
    if upper in ("INTEGER", "INT", "INT32"):
        return "INTEGER"
    if upper in ("BIGINT", "INT64", "HUGEINT"):
        return BIGINT
    if upper in ("DOUBLE", "FLOAT", "REAL", "FLOAT8"):
        return upper if upper != "REAL" else "FLOAT"
    if upper == "BOOLEAN":
        return BOOLEAN
    if upper in {"BLOB", "BYTEA"}:
        return "BLOB"
    if upper == "JSON":
        return JSON_T
    if upper == "VARIANT":
        return "VARIANT"
    if upper in {
        "DATE",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMPTZ",
        "TIME WITH TIME ZONE",
        "TIMETZ",
        "INTERVAL",
        "UUID",
    }:
        return upper
    if upper.startswith(
        ("STRUCT(", "MAP(", "UNION(", "ENUM(", "LIST(", "DECIMAL(", "BIGNUM")
    ) or upper.endswith("[]"):
        return upper
    return VARCHAR


def _union_members(physical: str) -> list[tuple[str, str]]:
    """Parse the top-level member declaration exposed by DuckDB metadata."""
    text = physical.strip()
    if text.upper().startswith("UNION(") and text.endswith(")"):
        text = text[text.find("(") + 1 : -1]
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    if text[start:].strip():
        parts.append(text[start:])
    result = []
    for part in parts:
        name, separator, type_name = part.strip().partition(" ")
        if separator and name:
            result.append((name.strip('"'), type_name.strip()))
    return result


def _is_top_level_union(physical: str) -> bool:
    text = str(physical).strip()
    if not text.upper().startswith("UNION("):
        return False
    depth = 0
    opening = text.find("(")
    for index in range(opening, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index == len(text) - 1
    return False


def _is_numeric_inner_union(physical: str) -> bool:
    """Whether a physical UNION is the numeric finite/special value encoding."""
    if not _is_top_level_union(physical):
        return False
    members = _union_members(physical)
    return len(members) == 2 and {name.lower() for name, _ in members} == {
        "finite",
        "special",
    }


def _union_member_names(physical: str) -> set[str]:
    return {name.lower() for name, _ in _union_members(physical)}


def _type_sql_equal(left: str, right: str) -> bool:
    """Compare physical/member SQL without treating harmless whitespace as drift."""

    def compact(value: str) -> str:
        normalized = re.sub(r"\s+", " ", str(value).strip()).upper()
        for spelling, canonical in (
            ("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"),
            ("TIME WITH TIME ZONE", "TIMETZ"),
            ("CHARACTER VARYING", "VARCHAR"),
            ("DOUBLE PRECISION", "DOUBLE"),
        ):
            normalized = normalized.replace(spelling, canonical)
        return normalized.replace(" ", "")

    return compact(left) == compact(right)


def _json_key_transition(current: str, desired: str) -> bool:
    """Whether a key-status change is a lossless JSON representation swap."""
    current_name = str(current).strip().upper()
    desired_name = str(desired).strip().upper()
    if current_name == desired_name:
        return False
    if {current_name, desired_name} == {"JSON", "VARIANT"}:
        return True
    return (
        ("VARIANT" in current_name and "JSON" in desired_name)
        or ("JSON" in current_name and "VARIANT" in desired_name)
    )


def _lossless_numeric_supertype(current: str, desired: str) -> bool:
    """Allow a legacy inferred integer column to adopt a narrower catalog fact."""
    ranks = {"SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "HUGEINT": 4}
    current_name = _normalise_type(current)
    desired_name = _normalise_type(desired)
    return (
        current_name in ranks
        and desired_name in ranks
        and ranks[current_name] >= ranks[desired_name]
    )


def _physical_union_native(physical: str, *, source=None):
    """Rehydrate a cached native UNION from the destination declaration."""
    from .typed_types import NativeMember, NativeType

    members = tuple(
        NativeMember(
            name,
            NativeType(_normalise_type(type_name), type_name),
        )
        for name, type_name in _union_members(physical)
    )
    return NativeType("UNION", physical, source=source, members=members, indexable=False)


class DDLOwner:
    """Mixin containing the registry's destination-DDL operations."""

    def _create_strict(
        self, table: Any, columns: dict[str, str], primary_key_columns: tuple[str, ...]
    ) -> None:
        definitions = ", ".join(
            f"{quote(column)} {_definition_for_column(column, ctype)}"
            for column, ctype in columns.items()
        )
        constraint = (
            ", PRIMARY KEY ("
            + ", ".join(quote(column) for column in primary_key_columns)
            + ")"
            if self.constraints and primary_key_columns
            else ""
        )
        try:
            self.con.execute(f"CREATE TABLE {table.qualified} ({definitions}{constraint})")
        except Exception as exc:
            raise SchemaEvolutionRefused(
                f"cannot create typed destination {table.name}: {exc}",
                target=table.name,
                refusal_origin="schema_ddl",
            ) from exc
        table.columns = {
            column: _normalise_type(ctype) for column, ctype in columns.items()
        }
        table.raw_types = dict(columns)
        table.exists = True
        table.constrained = bool(constraint)
        table.primary_key_columns = primary_key_columns

    def _create(
        self, table: Any, columns: dict[str, str], key_columns: tuple[str, ...]
    ) -> None:
        definitions = ", ".join(f"{quote(col)} {ctype}" for col, ctype in columns.items())
        constraint = ""
        if self.constraints and key_columns and all(c in columns for c in key_columns):
            constraint = ", PRIMARY KEY (" + ", ".join(
                quote(c) for c in key_columns
            ) + ")"
        try:
            self.con.execute(
                f"CREATE TABLE IF NOT EXISTS {table.qualified} "
                f"({definitions}{constraint})"
            )
            table.constrained = bool(constraint)
        except Exception as exc:
            if not constraint:
                raise
            log.warning(
                "could not create %s with a PRIMARY KEY on %s (%s); falling back to a "
                "post-apply uniqueness assertion inside the commit group",
                table.name,
                key_columns,
                exc,
            )
            self.con.execute(
                f"CREATE TABLE IF NOT EXISTS {table.qualified} ({definitions})"
            )
            table.constrained = False
        table.columns = dict(columns)
        table.raw_types = dict(columns)
        table.exists = True
        table.source_key_columns = tuple(key_columns)

    def _rebuild_with_primary_key(
        self,
        table: Any,
        *,
        drop_column: str,
        key_columns: tuple[str, ...],
    ) -> None:
        """Recreate a constrained table when its identity column is renamed."""
        columns = {
            column: type_name
            for column, type_name in table.columns.items()
            if column != drop_column
        }
        raw_types = {
            column: table.raw_types.get(column, type_name)
            for column, type_name in columns.items()
        }
        if not key_columns or any(column not in columns for column in key_columns):
            raise SchemaEvolutionRefused(
                f"cannot rebind primary-key identity {drop_column!r} on {table.name}: "
                "the replacement key is not present in the destination schema",
                target=table.name,
                refusal_origin="schema_ddl",
            )
        temp_name = f"{table.name}__cdcf_pk_rebind"
        definitions = ", ".join(
            f"{quote(column)} {raw_types[column]}" for column in columns
        )
        key_sql = ", ".join(quote(column) for column in key_columns)
        try:
            self.con.execute(
                f"CREATE TABLE {quote(self.dataset)}.{quote(temp_name)} "
                f"({definitions}, PRIMARY KEY ({key_sql}))"
            )
            column_sql = ", ".join(quote(column) for column in columns)
            self.con.execute(
                f"INSERT INTO {quote(self.dataset)}.{quote(temp_name)} ({column_sql}) "
                f"SELECT {column_sql} FROM {table.qualified}"
            )
            self.con.execute(f"DROP TABLE {table.qualified}")
            self.con.execute(
                f"ALTER TABLE {quote(self.dataset)}.{quote(temp_name)} "
                f"RENAME TO {quote(table.name)}"
            )
        except Exception as exc:
            try:
                self.con.execute(
                    f"DROP TABLE IF EXISTS {quote(self.dataset)}.{quote(temp_name)}"
                )
            except Exception:  # pragma: no cover - caller rolls back the transaction
                log.debug("could not clean up failed PK-rebind table", exc_info=True)
            raise SchemaEvolutionRefused(
                f"cannot rebind primary-key identity {drop_column!r} -> "
                f"{key_columns!r} on {table.name}: the destination identity is not "
                "unique or the table could not be rebuilt",
                target=table.name,
                refusal_origin="schema_ddl",
            ) from exc
        table.columns = columns
        table.raw_types = raw_types
        table.key_columns = key_columns
        table.source_key_columns = key_columns
        table.constrained = True


def assert_identity_is_unique(con, table: Any) -> None:
    """Enforce one row per identity when the destination lacks a PK."""
    if table.constrained or not table.key_columns:
        return
    cols = ", ".join(quote(column) for column in table.key_columns)
    duplicates = con.execute(
        f"SELECT count(*) FROM (SELECT {cols} FROM {table.qualified} "
        f"GROUP BY {cols} HAVING count(*) > 1)"
    ).fetchone()[0]
    if duplicates:
        raise DestinationIdentityCollision(
            f"{table.qualified} holds {duplicates} identity value(s) more than once on "
            f"({cols}). Exactly-once delivery means one row per identity, so this commit "
            "group is rolled back (ADR 0001 §15/A21)."
        )


__all__ = [
    "BIGINT",
    "BOOLEAN",
    "DOUBLE",
    "JSON_T",
    "OWNER",
    "VARCHAR",
    "_RECOGNISED_TYPES",
    "DDLOwner",
    "_is_numeric_inner_union",
    "_is_top_level_union",
    "_json_key_transition",
    "_lossless_numeric_supertype",
    "_normalise_type",
    "_physical_union_native",
    "_type_sql_equal",
    "_union_member_names",
    "_union_members",
    "assert_identity_is_unique",
    "widen",
]
