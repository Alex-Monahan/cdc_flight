"""Turning change events into SQL, inside the caller's transaction (ADR 0001 D1).

Every function here executes on the applier's single connection and **never**
opens or closes a transaction: the commit group owns that (principle 4). The
apply model is one mechanism for both table shapes:

| table | identity key | effect of an event |
|---|---|---|
| has a primary key (Debezium emits a message key) | the PK columns | delete every touched key, then insert the group's final row per key |
| no primary key (`key() is null`) | `cdcf_event_id` | delete that event id if present, then insert - so a replayed event cannot duplicate and two byte-identical source rows stay two rows |

"Delete every touched key, then insert the final row" is what makes a **primary
key update** correct without a special case (rubric 1.4): the old key is in the
touched set because it is the `before` image, the new key is in it because it is
the `after` image, so the row is deleted under the old key and inserted under the
new one inside the same commit group. No consumer can ever see it under both.

The keyless rule is what makes rubric 1.2 reachable. `cdcf_event_id` is
`"<event lsn>:<source.txId>:<transaction.total_order>"` - the connector's own
bookkeeping, not ours, so a replayed event recomputes the *same* id, while two
genuinely identical source rows are two different events and keep two different
ids. Nothing that deduplicates by row *content* can do both. (It is the **event's
own** LSN, not the transaction's commit LSN - ADR §15/A3; this docstring said
`commit_lsn` long after the code changed, Opus MINOR-14.)

Every table created here also carries a destination-side `PRIMARY KEY` on those
identity columns, so a duplicate identity is a failed transaction rather than a
silently corrupted table (Opus M-2).
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from .errors import (
    DestinationIdentityCollision,
    SchemaBackfillRefused,
    SchemaEvolutionRefused,
)
from .naming import (
    CDCF_COMMIT_ID,
    CDCF_EVENT_ID,
    CDCF_TOTAL_ORDER,
    quote,
)

log = logging.getLogger("cdc_flight.apply_sql")

#: How many key tuples go into one `DELETE … EXISTS (VALUES …)` statement.
DELETE_CHUNK = 2000
#: Upper bound on bound parameters per statement. Rows per statement is derived
#: from this and the column count, so a 40-column table does not build a
#: statement 40x larger than a 1-column one.
MAX_PARAMS_PER_STATEMENT = 40_000
#: Never build a statement with more rows than this, whatever the column count.
MAX_ROWS_PER_STATEMENT = 5_000
#: Rows per registered Arrow batch. Bounds the peak Python->Arrow copy, not the
#: transaction: the whole group is still one COMMIT.
ARROW_CHUNK = 100_000

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


def widen(current: str | None, incoming: str | None) -> str | None:
    """Least type that holds both. Deliberately conservative: anything ambiguous
    becomes VARCHAR, because losing a value is worse than losing a type.
    Rubric 2.5 (MotherDuck UNION types) replaces this with something better."""
    if current is None:
        return incoming
    if incoming is None or current == incoming:
        return current
    pair = {current, incoming}
    if pair == {BIGINT, DOUBLE}:
        return DOUBLE
    return VARCHAR


def bind(value: Any, column_type: str) -> Any:
    """Coerce a JSON-decoded value for a bound parameter of `column_type`."""
    if value is None:
        return None
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
    if column_type == BIGINT:
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
    return value  # pragma: no cover


class TableSchema:
    """The destination shape of one replicated table, cached for the process."""

    def __init__(self, name: str, dataset: str):
        self.name = name
        self.dataset = dataset
        self.columns: dict[str, str] = {}
        #: the destination's own type string, before `_normalise_type` collapses it.
        #: Kept so we never ALTER a column whose real type we did not recognise
        #: (Opus MINOR-15): TIMESTAMP normalises to VARCHAR, and "widening" a
        #: TIMESTAMP column to VARCHAR is destructive-by-accident.
        self.raw_types: dict[str, str] = {}
        self.key_columns: tuple[str, ...] = ()
        self.exists = False
        #: True when the table carries a destination-side PRIMARY KEY on its
        #: identity columns (Opus M-2).
        self.constrained = False

    @property
    def qualified(self) -> str:
        return f"{quote(self.dataset)}.{quote(self.name)}"


class SchemaRegistry:
    """Creates and evolves destination tables. All DDL runs in the caller's txn.

    Every table it creates carries a `PRIMARY KEY` on its identity columns - the
    source key columns for a keyed table, `cdcf_event_id` for a keyless one - so
    that "duplication is impossible" is *enforced by the destination* rather than
    asserted by the applier (Opus M-2). That is what turns the whole B-1 class of
    apply-path defect from silent corruption into a failed transaction, and a failed
    transaction is safe: it rolls back and replays. MEASURED on DuckDB 1.5.4:
    200 000 rows x 2 columns through Arrow into a table with a PRIMARY KEY takes
    0.03 s, and DELETE-then-INSERT of the same key inside one transaction is
    accepted, so the merge path is unaffected.
    """

    def __init__(self, con, dataset: str, *, constraints: bool = True):
        self.con = con
        self.dataset = dataset
        self.constraints = constraints
        self._tables: dict[str, TableSchema] = {}

    def get(self, name: str) -> TableSchema:
        table = self._tables.get(name)
        if table is None:
            table = TableSchema(name, self.dataset)
            self._load(table)
            self._tables[name] = table
        return table

    def forget(self, name: str) -> None:
        self._tables.pop(name, None)

    def _load(self, table: TableSchema) -> None:
        self._refresh(table)

    def _refresh(self, table: TableSchema) -> None:
        rows = self.con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [self.dataset, table.name],
        ).fetchall()
        if rows:
            table.exists = True
            table.columns = {name: _normalise_type(dtype) for name, dtype in rows}
            table.raw_types = {name: str(dtype).upper() for name, dtype in rows}
            key_rows = self.con.execute(
                "SELECT k.column_name "
                "FROM information_schema.key_column_usage k "
                "JOIN information_schema.table_constraints t "
                "  ON t.constraint_schema = k.constraint_schema "
                " AND t.constraint_name = k.constraint_name "
                " AND t.table_name = k.table_name "
                "WHERE k.table_schema = ? AND k.table_name = ? "
                "  AND t.constraint_type = 'PRIMARY KEY' "
                "ORDER BY k.ordinal_position",
                [self.dataset, table.name],
            ).fetchall()
            if key_rows:
                table.key_columns = tuple(row[0] for row in key_rows)

    # -- DDL ---------------------------------------------------------------- #
    def ensure(
        self,
        name: str,
        *,
        columns: dict[str, str],
        key_columns: tuple[str, ...],
        strict: bool = False,
    ) -> tuple[TableSchema, bool]:
        """`(schema, created_now)`. `created_now` lets the caller skip the DELETE
        half of a merge against a table it just created."""
        table = self.get(name)
        if key_columns:
            # NOT unconditionally: a group whose only event for this table is a
            # TRUNCATE carries no message key, so it would erase the cached identity
            # and silently disarm `assert_identity_is_unique` for the rest of the run
            # (Opus MINOR-3).
            table.key_columns = key_columns
        if not table.exists:
            self._create(table, columns, key_columns)
            return table, True

        for col, ctype in columns.items():
            existing = table.columns.get(col)
            if existing is None:
                # rubric 2.1 - an added source column must simply appear.
                try:
                    self.con.execute(
                        f"ALTER TABLE {table.qualified} ADD COLUMN {quote(col)} {ctype}"
                    )
                except Exception as exc:
                    if strict:
                        raise SchemaEvolutionRefused(
                            f"cannot add source column {name}.{col}: destination DDL "
                            "failed, so the catalog baseline cannot be persisted",
                            target=name,
                        ) from exc
                    raise
                table.columns[col] = ctype
                table.raw_types[col] = ctype
                continue
            widened = widen(existing, ctype)
            if widened == existing:
                if strict and existing != ctype:
                    raise SchemaEvolutionRefused(
                        f"cannot apply schema change to {name}.{col}: destination "
                        f"type {existing} cannot adopt source type {ctype} through "
                        "the safe widening lattice; refusing to persist an unadopted "
                        "catalog baseline",
                        target=name,
                    )
                continue
            raw = table.raw_types.get(col, existing)
            if raw not in _RECOGNISED_TYPES:
                # `_normalise_type` collapses TIMESTAMP / DECIMAL / DATE / BLOB / LIST
                # to VARCHAR, so an "upgrade" computed from that lattice can narrow a
                # real TIMESTAMP column to text. Refuse explicitly instead of doing it
                # by accident; rubric 2.4/2.5 own the real answer (Opus MINOR-15).
                message = (
                    f"cannot apply schema change to {name}.{col}: destination type "
                    f"{raw} is outside the safe widening lattice; refusing to persist "
                    f"a catalog baseline as {widened}"
                )
                if strict:
                    raise SchemaEvolutionRefused(message, target=name)
                log.warning(message)
                continue
            try:
                self.con.execute(
                    f"ALTER TABLE {table.qualified} ALTER COLUMN {quote(col)} "
                    f"SET DATA TYPE {widened}"
                )
                table.columns[col] = widened
                table.raw_types[col] = widened
            except Exception as exc:  # rubric 2.5 owns the real answer
                message = (
                    f"could not apply schema change to {name}.{col}: cannot widen "
                    f"{existing} to {widened}: {exc}"
                )
                if strict:
                    raise SchemaEvolutionRefused(message, target=name) from exc
                log.warning(message)
        return table, False

    def _create(
        self, table: TableSchema, columns: dict[str, str], key_columns: tuple[str, ...]
    ) -> None:
        defs = ", ".join(f"{quote(col)} {ctype}" for col, ctype in columns.items())
        constraint = ""
        if self.constraints and key_columns and all(c in columns for c in key_columns):
            constraint = ", PRIMARY KEY (" + ", ".join(quote(c) for c in key_columns) + ")"
        try:
            self.con.execute(
                f"CREATE TABLE IF NOT EXISTS {table.qualified} ({defs}{constraint})"
            )
            table.constrained = bool(constraint)
        except Exception as exc:
            if not constraint:
                raise
            # A destination that cannot express the constraint must not block the
            # load; `_assert_identity_is_unique` becomes the enforcement instead.
            log.warning(
                "could not create %s with a PRIMARY KEY on %s (%s); falling back to a "
                "post-apply uniqueness assertion inside the commit group",
                table.name, key_columns, exc,
            )
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {table.qualified} ({defs})")
            table.constrained = False
        table.columns = dict(columns)
        table.raw_types = dict(columns)
        table.exists = True

    def drop(self, name: str) -> None:
        table = self.get(name)
        self.con.execute(f"DROP TABLE IF EXISTS {table.qualified}")
        self.forget(name)

    def drop_column(self, name: str, column: str) -> None:
        """Physically remove a source-dropped column in the open transaction."""
        table = self.get(name)
        if not table.exists or column not in table.columns:
            return
        if column in table.key_columns:
            raise SchemaEvolutionRefused(
                f"cannot drop primary-key column {name}.{column}: the source did not "
                "provide a replacement identity, so the destination refuses to "
                "continue without a lossless row key",
                target=name,
            )
        try:
            self.con.execute(
                f"ALTER TABLE {table.qualified} DROP COLUMN {quote(column)}"
            )
        except Exception as exc:
            raise SchemaEvolutionRefused(
                f"cannot drop source column {name}.{column}: destination DDL failed, "
                "so the catalog baseline cannot be persisted",
                target=name,
            ) from exc
        table.columns.pop(column, None)
        table.raw_types.pop(column, None)
        table.key_columns = tuple(c for c in table.key_columns if c != column)

    def rename_column(self, name: str, old: str | None, new: str | None) -> None:
        """Apply a true rename, including a late-arriving new-name row.

        The latter shape is possible because the catalog poll and the Debezium callback
        are independent.  Merging with ``COALESCE`` preserves old rows and any new-name
        values already applied, then the old physical column is removed.  All statements
        run in the applier's transaction, so no consumer observes the intermediate pair.
        """
        if not old or not new or old == new:
            return
        table = self.get(name)
        if not table.exists:
            return
        # A late Debezium row may already have caused the normal ensure path to add the
        # new-name column since this registry entry was cached. Re-read metadata before
        # deciding whether this is a physical RENAME or a merge.
        self._refresh(table)
        old_exists = old in table.columns
        new_exists = new in table.columns
        if old_exists and not new_exists:
            try:
                self.con.execute(
                    f"ALTER TABLE {table.qualified} RENAME COLUMN {quote(old)} "
                    f"TO {quote(new)}"
                )
            except Exception as exc:
                raise SchemaEvolutionRefused(
                    f"cannot rename source column {name}.{old} -> {new}: destination "
                    "DDL failed, so the catalog baseline cannot be persisted",
                    target=name,
                ) from exc
            table.columns[new] = table.columns.pop(old)
            table.raw_types[new] = table.raw_types.pop(old)
        elif old_exists and new_exists:
            # Prefer the already-arriving new image; fall back to the old value for rows
            # that predate the catalog poll. This is the only safe merge for a rename
            # whose source values are allowed to be NULL.
            # A NULL in the new-name column is a real source value, not proof that the
            # new field was absent.  The presence journal is written with each CDC row
            # inside its commit group; only rows with no explicit new-name field fall
            # back to the old physical column.  Direct unit callers without the
            # journal retain the conservative old COALESCE behavior for compatibility.
            if CDCF_EVENT_ID in table.columns:
                self.con.execute(
                    f"UPDATE {table.qualified} AS t SET {quote(new)} = {quote(old)} "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {quote('_cdc_flight')}."
                    f"{quote('column_presence')} AS p "
                    f"WHERE p.target_dataset = ? AND p.target_table = ? "
                    f"AND p.event_id = t.{quote(CDCF_EVENT_ID)} "
                    f"AND p.column_name = ? AND p.present)",
                    [self.dataset, table.name, new],
                )
            else:
                self.con.execute(
                    f"UPDATE {table.qualified} SET {quote(new)} = "
                    f"COALESCE({quote(new)}, {quote(old)})"
                )
            key_was_renamed = old in table.key_columns
            if key_was_renamed and table.constrained:
                # DuckDB/MotherDuck do not support DROP CONSTRAINT for a primary key.
                # Rebuild the table inside the caller's transaction so the new identity
                # is validated before the old table is removed.  A duplicate new key
                # therefore raises while the old table still exists and the enclosing
                # commit group can roll back the whole schema change.
                new_key_columns = tuple(new if c == old else c for c in table.key_columns)
                self._rebuild_with_primary_key(
                    table,
                    drop_column=old,
                    key_columns=new_key_columns,
                )
            else:
                try:
                    self.con.execute(
                        f"ALTER TABLE {table.qualified} DROP COLUMN {quote(old)}"
                    )
                except Exception as exc:
                    raise SchemaEvolutionRefused(
                        f"cannot finish late rename {name}.{old} -> {new}: destination "
                        "DDL failed, so the catalog baseline cannot be persisted",
                        target=name,
                    ) from exc
                table.columns.pop(old, None)
                table.raw_types.pop(old, None)
            if CDCF_EVENT_ID in table.columns:
                self.con.execute(
                    f"DELETE FROM {quote('_cdc_flight')}."
                    f"{quote('column_presence')} "
                    "WHERE target_dataset = ? AND target_table = ? AND column_name = ?",
                    [self.dataset, table.name, new],
                )
        # If only the new name remains, an earlier group already performed the
        # idempotent late-rename merge. If neither remains, the source relation may have
        # produced no row carrying either shape; the catalog action remains harmless.
        table.key_columns = tuple(new if c == old else c for c in table.key_columns)

    def _rebuild_with_primary_key(
        self,
        table: TableSchema,
        *,
        drop_column: str,
        key_columns: tuple[str, ...],
    ) -> None:
        """Recreate a constrained table when its identity column is renamed.

        DuckDB has no ``DROP CONSTRAINT`` implementation.  The temporary table is
        created with the replacement key before the source table is dropped, so a
        uniqueness failure cannot leave a destination with neither identity nor data.
        The caller owns the transaction; the table-cache update happens only after all
        physical statements succeed.
        """
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
            except Exception:  # pragma: no cover - the caller rolls back the transaction
                log.debug("could not clean up failed PK-rebind table", exc_info=True)
            raise SchemaEvolutionRefused(
                f"cannot rebind primary-key identity {drop_column!r} -> "
                f"{key_columns!r} on {table.name}: the destination identity is not "
                "unique or the table could not be rebuilt",
                target=table.name,
            ) from exc
        table.columns = columns
        table.raw_types = raw_types
        table.key_columns = key_columns
        table.constrained = True

    def backfill_columns(
        self,
        name: str,
        *,
        key_columns: tuple[str, ...],
        value_columns: tuple[str, ...],
        rows: list[tuple],
    ) -> None:
        """Copy current source values into newly added columns in this transaction.

        PostgreSQL evaluates an ADD COLUMN default for rows already in the source.
        The CDC stream only carries future row changes, so a destination-side ADD alone
        would leave those rows NULL forever.  The catalog fence supplies a stable key
        and source read; this method makes the existing destination rows agree before
        the commit becomes durable.  It deliberately uses UPDATE, not a delete/insert
        merge, so the operation remains safe after the schema DDL has run in the same
        transaction on DuckDB/MotherDuck.
        """
        if not key_columns or not value_columns or not rows:
            return
        table = self.get(name)
        if not table.exists:
            return
        key_columns = tuple(column for column in key_columns if column in table.columns)
        value_columns = tuple(column for column in value_columns if column in table.columns)
        if not key_columns or not value_columns:
            return
        set_clause = ", ".join(f"{quote(column)} = ?" for column in value_columns)
        where_clause = " AND ".join(
            f"{quote(column)} IS NOT DISTINCT FROM ?" for column in key_columns
        )
        value_count = len(value_columns)
        key_count = len(key_columns)
        for row in rows:
            keys = row[:key_count]
            values = row[key_count : key_count + value_count]
            params = [
                bind(value, table.columns[column])
                for column, value in zip(value_columns, values, strict=True)
            ] + [
                bind(value, table.columns[column])
                for column, value in zip(key_columns, keys, strict=True)
            ]
            self.con.execute(
                f"UPDATE {table.qualified} SET {set_clause} WHERE {where_clause}",
                params,
            )

    def backfill_constant_columns(
        self,
        name: str,
        *,
        value_columns: tuple[str, ...],
        rows: list[tuple],
    ) -> None:
        """Backfill a keyless destination when the source values are uniform.

        A source table without a primary key has no durable identity that can match a
        current source row to a historical ``cdcf_event_id`` row.  Applying a value
        that differs by source row would therefore be an invented mapping.  PostgreSQL
        ADD COLUMN normally gives every existing row one default (or NULL), so that
        common case has a safe all-rows operation; a non-uniform source is rejected by
        the caller before this update runs.
        """
        if not value_columns:
            return
        table = self.get(name)
        if not table.exists:
            return
        value_columns = tuple(column for column in value_columns if column in table.columns)
        if not value_columns:
            return
        if not rows:
            destination_rows = self.con.execute(
                f"SELECT count(*) FROM {table.qualified}"
            ).fetchone()[0]
            if destination_rows:
                raise SchemaBackfillRefused(
                    f"cannot backfill keyless table {name}: the source returned no "
                    f"rows for an added column while {destination_rows} destination "
                    "changelog rows already exist; no stable identity or source value "
                    "proves what those rows should contain"
                )
            return
        values = tuple(rows[0][: len(value_columns)])
        if any(tuple(row[: len(value_columns)]) != values for row in rows[1:]):
            raise SchemaBackfillRefused(
                f"cannot backfill keyless table {name}: added-column values are not "
                "uniform and the source has no stable row identity"
            )
        set_clause = ", ".join(f"{quote(column)} = ?" for column in value_columns)
        params = [
            bind(value, table.columns[column])
            for column, value in zip(value_columns, values, strict=True)
        ]
        self.con.execute(f"UPDATE {table.qualified} SET {set_clause}", params)


#: Destination types the widening lattice actually understands. Anything else is
#: reported as VARCHAR by `_normalise_type` and must never be ALTERed on that basis.
_RECOGNISED_TYPES = frozenset(
    {
        BOOLEAN, BIGINT, DOUBLE, JSON_T, VARCHAR,
        "TEXT", "STRING", "INT64", "HUGEINT", "INTEGER", "INT", "INT32", "SMALLINT",
        "FLOAT", "REAL", "FLOAT8",
    }
)


def assert_identity_is_unique(con, table: TableSchema) -> None:
    """Fallback for a destination that cannot express the PRIMARY KEY (Opus M-2).

    Runs **inside** the commit group's transaction, so a violation rolls the whole
    group back and the events replay. Only used when `_create` could not attach the
    constraint; with the constraint in place the destination raises on the INSERT
    itself and this is never called.
    """
    if table.constrained or not table.key_columns:
        return
    cols = ", ".join(quote(c) for c in table.key_columns)
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


def _normalise_type(duckdb_type: str) -> str:
    upper = duckdb_type.upper()
    if upper.startswith("VARCHAR") or upper in ("TEXT", "STRING"):
        return VARCHAR
    if upper in ("BIGINT", "INT64", "HUGEINT", "INTEGER", "INT", "INT32", "SMALLINT"):
        return BIGINT
    if upper in ("DOUBLE", "FLOAT", "REAL", "FLOAT8"):
        return DOUBLE
    if upper == "BOOLEAN":
        return BOOLEAN
    if upper == "JSON":
        return JSON_T
    return VARCHAR


# --------------------------------------------------------------------------- #
# the two statements an apply is made of
# --------------------------------------------------------------------------- #
def delete_keys(con, table: TableSchema, key_columns: tuple[str, ...], keys: list[tuple]) -> None:
    if not keys:
        return
    cols = ", ".join(quote(c) for c in key_columns)
    predicate = " AND ".join(
        f"t.{quote(c)} IS NOT DISTINCT FROM v.{quote(c)}" for c in key_columns
    )
    if len(keys) > DELETE_CHUNK:
        # One anti-join instead of hundreds of table scans. The staging table is
        # temporary and therefore invisible to any other connection, so it cannot
        # weaken the atomicity guarantee the commit group exists to provide.
        staging = "_cdcf_delete_keys"
        types = [table.columns.get(c, VARCHAR) for c in key_columns]
        defs = ", ".join(f"{quote(c)} {t}" for c, t in zip(key_columns, types, strict=True))
        con.execute(f"CREATE OR REPLACE TEMP TABLE {staging} ({defs})")
        bulk_insert(con, staging, list(key_columns), [list(k) for k in keys], types)
        con.execute(
            f"DELETE FROM {table.qualified} AS t WHERE EXISTS "
            f"(SELECT 1 FROM {staging} AS v WHERE {predicate})"
        )
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        return
    for start in range(0, len(keys), DELETE_CHUNK):
        chunk = keys[start : start + DELETE_CHUNK]
        placeholders = ", ".join(
            "(" + ", ".join("?" for _ in key_columns) + ")" for _ in chunk
        )
        params: list[Any] = []
        for key in chunk:
            params.extend(key)
        con.execute(
            f"DELETE FROM {table.qualified} AS t WHERE EXISTS "
            f"(SELECT 1 FROM (VALUES {placeholders}) AS v({cols}) WHERE {predicate})",
            params,
        )


def rows_per_statement(n_columns: int) -> int:
    if n_columns <= 0:  # pragma: no cover - defensive
        return MAX_ROWS_PER_STATEMENT
    return max(1, min(MAX_ROWS_PER_STATEMENT, MAX_PARAMS_PER_STATEMENT // n_columns))


def _arrow_type(sql_type: str):
    import pyarrow as pa

    return {
        BIGINT: pa.int64(),
        DOUBLE: pa.float64(),
        BOOLEAN: pa.bool_(),
    }.get(sql_type, pa.string())


def bulk_insert(
    con, target: str, columns: list[str], rows: list[list], types: list[str] | None = None
) -> None:
    """Insert many rows through a registered Arrow table.

    MEASURED, 2026-07-30, 200 000 rows x 19 columns into local DuckDB inside one
    transaction:

    | strategy | time |
    |---|---|
    | `con.executemany(INSERT … VALUES (?,…))` | **410 s** |
    | chunked multi-row `INSERT … VALUES (…),(…),…` | **> 7 min**, abandoned |
    | register an Arrow table + `INSERT … SELECT` | **1.37 s** |

    and against MotherDuck, 1 500 rows: `executemany` 27.9 s (a network round trip
    *per row*), multi-row `VALUES` 0.65 s, Arrow 1.87 s.

    So Arrow is the only strategy that is fast at both ends, and `executemany` -
    the obvious way to write this - is 300x slower than it looks even locally.
    That is what turned one 200 000-row Postgres transaction into a commit group
    that could not finish inside the slow test's 300 s deadline.

    `pyarrow` is a hard dependency for this reason; if it is somehow missing the
    code falls back to `executemany` and logs, because a slow apply is better
    than a failed one.
    """
    if not rows:
        return
    collist = ", ".join(quote(c) for c in columns)
    column_types = types or [VARCHAR] * len(columns)
    try:
        import pyarrow as pa
    except ImportError:  # pragma: no cover - pyarrow is a declared dependency
        log.warning("pyarrow is unavailable; falling back to a slow row-at-a-time insert")
        placeholders = ", ".join("?" for _ in columns)
        con.executemany(f"INSERT INTO {target} ({collist}) VALUES ({placeholders})", rows)
        return

    view = "cdcf_bulk_rows"
    for start in range(0, len(rows), ARROW_CHUNK):
        batch = rows[start : start + ARROW_CHUNK]
        arrays = {}
        for index, column in enumerate(columns):
            values = [row[index] for row in batch]
            try:
                arrays[column] = pa.array(values, type=_arrow_type(column_types[index]))
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
                # A value the declared type cannot hold (a huge integer, a mixed
                # column mid-evolution). Keeping the value as text is always
                # possible and DuckDB casts it on the way in; losing it is not.
                arrays[column] = pa.array(
                    [None if v is None else str(v) for v in values], type=pa.string()
                )
        table = pa.table(arrays)
        con.register(view, table)
        try:
            con.execute(f"INSERT INTO {target} ({collist}) SELECT * FROM {view}")
        finally:
            con.unregister(view)


def insert_rows(
    con, table: TableSchema, columns: list[str], rows: list[list]
) -> None:
    bulk_insert(
        con,
        table.qualified,
        columns,
        rows,
        [table.columns.get(c, VARCHAR) for c in columns],
    )


__all__ = [
    "CDCF_COMMIT_ID",
    "CDCF_EVENT_ID",
    "CDCF_TOTAL_ORDER",
    "SchemaRegistry",
    "TableSchema",
    "bind",
    "delete_keys",
    "insert_rows",
    "sql_type",
    "widen",
]
