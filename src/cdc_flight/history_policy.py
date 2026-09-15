"""Durable admission of the per-table history policy.

Round 1 deliberately owns only the policy boundary. The durable authority is the
existing table_state.history_mode column; this module validates the public input,
checks the source identity supplied by CatalogWatcher, and changes that one column
or creates the lifecycle row before any normal pipeline event work begins.

Enabling history is intentionally a pre-materialization operation. A current-only
destination image cannot be promoted to historical without inventing versions that
were never observed, so an existing image is a hard refusal. Event dispatch and
history-preserving refresh remain later rounds.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass

from . import table_lifecycle
from .config import resolve_control_schema
from .errors import AdmissionError
from .naming import control_table

HISTORY_MODES = frozenset({"none", "scd2"})
HISTORY_POLICY_AUTHORITY = "table_state.history_mode"
_UNQUOTED_IDENTIFIER = re.compile(r"^[^\W\d]\w*$", re.UNICODE)


class HistoryPolicyRefused(AdmissionError):
    """A history policy input or transition is not safe to admit."""


@dataclass(frozen=True)
class HistoryModeAdmission:
    """The committed result of one table-scoped policy operation."""

    qualified_table: str
    mode: str
    previous_mode: str
    lifecycle_state: str
    target_table: str
    changed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "qualified_table": self.qualified_table,
            "history_mode": self.mode,
            "previous_history_mode": self.previous_mode,
            "snapshot_state": self.lifecycle_state,
            "target_table": self.target_table,
            "changed": self.changed,
            "policy_authority": HISTORY_POLICY_AUTHORITY,
            "transaction_scope": "one_destination_transaction",
        }


def _refuse(message: str) -> None:
    raise HistoryPolicyRefused(message)


def _skip_space(text: str, index: int) -> int:
    while index < len(text) and text[index].isspace():
        index += 1
    return index


def _identifier(text: str, index: int, label: str) -> tuple[str, int]:
    index = _skip_space(text, index)
    if index >= len(text):
        _refuse(f"history policy requires a qualified table; missing {label} identifier")
    if text[index] == '"':
        index += 1
        value: list[str] = []
        while index < len(text):
            character = text[index]
            if character == '"':
                if index + 1 < len(text) and text[index + 1] == '"':
                    value.append('"')
                    index += 2
                    continue
                index += 1
                if not value or "\x00" in value:
                    _refuse(f"history policy {label} identifier may not be empty")
                return "".join(value), _skip_space(text, index)
            value.append(character)
            index += 1
        _refuse(f"history policy {label} identifier has an unterminated quote")

    start = index
    while index < len(text) and not text[index].isspace() and text[index] != ".":
        if text[index] == '"':
            _refuse(f"history policy {label} identifier has invalid quoting")
        index += 1
    raw = text[start:index]
    if not raw or not _UNQUOTED_IDENTIFIER.fullmatch(raw):
        _refuse(
            f"history policy {label} identifier {raw!r} is not a PostgreSQL identifier"
        )
    return raw.lower(), _skip_space(text, index)


def parse_qualified_table(value: object) -> tuple[str, str]:
    """Parse exactly schema.table using PostgreSQL identifier spelling rules."""
    text = str(value).strip()
    if not text:
        _refuse("history policy table must be qualified as schema.table")
    schema, index = _identifier(text, 0, "schema")
    if index >= len(text) or text[index] != ".":
        _refuse(
            "history policy requires exactly one qualified table identity "
            "(schema.table); an unqualified or ambiguous identity was refused"
        )
    table, index = _identifier(text, index + 1, "table")
    if index != len(text):
        _refuse(
            "history policy requires exactly one qualified table identity "
            "(schema.table); extra qualification was refused"
        )
    return schema, table


def parse_history_mode(value: object) -> str:
    """Validate and canonicalize the only two durable history modes."""
    if not isinstance(value, str):
        _refuse(
            f"unsupported history mode {value!r}; expected one of "
            f"{sorted(HISTORY_MODES)}"
        )
    mode = str(value).strip().lower()
    if mode not in HISTORY_MODES:
        _refuse(
            f"unsupported history mode {value!r}; expected one of "
            f"{sorted(HISTORY_MODES)}"
        )
    return mode


def _table(control_schema: str | None) -> str:
    return control_table(resolve_control_schema(control_schema), "table_state")


def _missing_table_state(error: BaseException) -> bool:
    message = str(error).lower()
    return "table_state" in message and (
        "does not exist" in message
        or "not found" in message
        or "catalog error" in message
    )


def read_history_mode(
    con,
    *,
    pipeline: str,
    source_schema: str,
    source_table: str,
    control_schema: str | None = None,
) -> str:
    """Read one committed policy; absence means the durable default none."""
    if con is None:
        return "none"
    try:
        row = con.execute(
            f"SELECT history_mode FROM {_table(control_schema)} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [pipeline, source_schema, source_table],
        ).fetchone()
    except Exception as error:
        # Compatibility-only planner probes may run before control DDL. Once the
        # current schema exists, all other read failures remain visible and cannot
        # silently turn a malformed policy into current-only work.
        if _missing_table_state(error):
            return "none"
        raise
    if row is None or row[0] is None:
        return "none"
    return parse_history_mode(row[0])


def _target_exists(con, *, dataset: str, target_table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ? LIMIT 1",
        [dataset, target_table],
    ).fetchone()
    return row is not None


def _row(con, *, pipeline: str, source_schema: str, source_table: str, control_schema):
    return con.execute(
        f"SELECT target_table, snapshot_state, history_mode FROM {_table(control_schema)} "
        "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
        [pipeline, source_schema, source_table],
    ).fetchone()


def _validate_source_identity(source_relation, *, schema: str, table: str) -> None:
    if source_relation is None:
        _refuse(f"source relation {schema}.{table} was not found in the catalog")
    if (
        str(getattr(source_relation, "schema", "")) != schema
        or str(getattr(source_relation, "table", "")) != table
    ):
        _refuse(
            f"catalog identity refusal: requested {schema}.{table}, but the observed "
            "relation identity did not match"
        )
    if bool(getattr(source_relation, "is_partition", False)):
        _refuse(
            f"history policy refuses partition child {schema}.{table}; its parent is "
            "the captured relation"
        )
    key_columns = tuple(getattr(source_relation, "primary_key_columns", ()) or ())
    if not key_columns:
        _refuse(
            f"history policy refuses keyless relation {schema}.{table}; stock Debezium "
            "does not provide a stable lineage for arbitrary duplicate rows"
        )


def admit_history_mode(
    con,
    *,
    pipeline: str,
    source_relation,
    qualified_table: object | None = None,
    mode: object,
    target_table: str,
    dataset: str,
    control_schema: str | None = None,
) -> HistoryModeAdmission:
    """Atomically admit one validated source relation's durable history mode.

    The caller supplies a fenced destination connection and has already serialized
    the service operation. This function owns exactly one destination transaction;
    it never creates a history bundle or publishes a source signal.
    """
    if qualified_table is None:
        schema = str(getattr(source_relation, "schema", ""))
        table = str(getattr(source_relation, "table", ""))
    else:
        schema, table = parse_qualified_table(qualified_table)
    _validate_source_identity(source_relation, schema=schema, table=table)
    selected = parse_history_mode(mode)
    if not target_table or not dataset:
        _refuse("history policy requires a destination dataset and target table")
    qualified = f"{schema}.{table}"
    state_table = _table(control_schema)
    con.execute("BEGIN TRANSACTION")
    try:
        row = _row(
            con,
            pipeline=pipeline,
            source_schema=schema,
            source_table=table,
            control_schema=control_schema,
        )
        if row is None:
            if selected == "scd2" and _target_exists(
                con, dataset=dataset, target_table=target_table
            ):
                _refuse(
                    f"history policy refuses {qualified}: current-only destination "
                    f"{dataset}.{target_table} already exists; pre-enable history "
                    "cannot be fabricated"
                )
            table_lifecycle.transition(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                to=table_lifecycle.NONE,
                reason=f"history mode {selected} admitted for {qualified}",
                target_table=target_table,
                history_mode=selected,
                control_schema=control_schema,
            )
            lifecycle = table_lifecycle.NONE
            previous = "none"
            changed = selected != "none"
        else:
            existing_target, _raw_lifecycle, raw_mode = row
            lifecycle = table_lifecycle.read(
                con,
                pipeline=pipeline,
                source_schema=schema,
                source_table=table,
                control_schema=control_schema,
            )
            current = parse_history_mode(raw_mode)
            existing_target = str(existing_target)
            if current == selected:
                previous = current
                changed = False
            elif current == "scd2" and selected == "none":
                _refuse(
                    f"history policy refuses downgrade of {qualified} from scd2 to none; "
                    "historical rows must not be hidden"
                )
            else:
                if lifecycle != table_lifecycle.NONE:
                    _refuse(
                        f"history policy refuses enabling scd2 for {qualified} while its "
                        f"lifecycle is {lifecycle!r}; admission is allowed only before "
                        "materialization"
                    )
                if _target_exists(
                    con, dataset=dataset, target_table=existing_target
                ):
                    _refuse(
                        f"history policy refuses {qualified}: current-only destination "
                        f"{dataset}.{existing_target} already exists; pre-enable history "
                        "cannot be fabricated"
                    )
                con.execute(
                    f"UPDATE {state_table} SET history_mode = ? "
                    "WHERE pipeline = ? AND source_schema = ? AND source_table = ? "
                    "AND history_mode = ?",
                    [selected, pipeline, schema, table, current],
                )
                previous = current
                changed = True
            target_table = existing_target
        con.execute("COMMIT")
    except BaseException:
        with suppress(Exception):
            con.execute("ROLLBACK")
        raise
    return HistoryModeAdmission(
        qualified_table=qualified,
        mode=selected,
        previous_mode=previous,
        lifecycle_state=str(lifecycle),
        target_table=str(target_table),
        changed=changed,
    )


__all__ = [
    "HISTORY_MODES",
    "HISTORY_POLICY_AUTHORITY",
    "HistoryModeAdmission",
    "HistoryPolicyRefused",
    "admit_history_mode",
    "parse_history_mode",
    "parse_qualified_table",
    "read_history_mode",
]
