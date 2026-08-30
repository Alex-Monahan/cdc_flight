"""The capability boundary for table-data destination failures.

DuckDB uses one exception hierarchy for row/value rejection and for control,
catalog, connection, and transaction failures.  The materializer therefore gets
only a writer-owned destination-table scope.  It validates every DML statement
against that scope, while source attribution stays with the current ``TableWork``;
there is no source-claim object for a caller to forge or reuse.
"""

from __future__ import annotations

import re
from typing import Any

OWNER = "destination-failure-classification"

# These are the driver errors reachable when a source row/value is bound to a
# destination DML statement.  InvalidInputException is the important binding
# case: DuckDB raises it before it can classify a Python integer larger than its
# 128-bit binding range.  NotImplementedException covers a driver refusal to
# transform an unsupported Python value.  Neither class is a transaction,
# connection, parser, catalog, or session failure.
DATA_REJECTION_EXCEPTION_NAMES = (
    "ConversionException",
    "ConstraintException",
    "InvalidInputException",
    "NotImplementedException",
    "OutOfRangeException",
    "TypeMismatchException",
)

# DuckDB 1.5.4's remaining exported Error subclasses are operation, parser,
# catalog, connection, or transaction failures. Keeping this sibling list next to
# the data list makes a driver upgrade fail the structural test if it introduces a
# new Error class that has not been deliberately classified.
NON_DATA_EXCEPTION_NAMES = (
    "BinderException",
    "CatalogException",
    "ConnectionException",
    "DataError",
    "DatabaseError",
    "DependencyException",
    "FatalException",
    "HTTPException",
    "IOException",
    "IntegrityError",
    "InternalError",
    "InternalException",
    "InterruptException",
    "InvalidTypeException",
    "NotSupportedError",
    "OperationalError",
    "OutOfMemoryException",
    "ParserException",
    "PermissionException",
    "ProgrammingError",
    "SequenceException",
    "SerializationException",
    "SyntaxException",
    "TransactionException",
)


_IDENTIFIER = r"(?:\"(?:[^\"]|\"\")*\"|[A-Za-z_][A-Za-z0-9_$]*)"
_DML_TARGET = re.compile(
    rf"\b(?:INSERT(?:\s+OR\s+[A-Z]+)?\s+INTO|UPDATE|DELETE\s+FROM)\s+"
    rf"({_IDENTIFIER}(?:\s*\.\s*{_IDENTIFIER})*)",
    re.IGNORECASE,
)
_INTERNAL_TARGETS = frozenset(
    {"_cdcf_delete_keys", "cdcf_bulk_rows", "cdcf_typed_bulk_rows"}
)


def _relation_name(identifier: str) -> str:
    part = identifier.rsplit(".", 1)[-1].strip()
    if part.startswith('"') and part.endswith('"'):
        part = part[1:-1].replace('""', '"')
    return part.lower()


def _check_statement_binding(target: str, statement: str) -> None:
    match = _DML_TARGET.search(statement)
    if match is None:
        return
    actual = _relation_name(match.group(1))
    expected = _relation_name(target)
    if actual != expected and actual not in _INTERNAL_TARGETS:
        raise ValueError(
            f"table-DML scope for {expected!r} cannot execute a statement targeting "
            f"{actual!r}"
        )


class DestinationDataRejection(RuntimeError):
    """A classified value/row rejection at a destination-table DML boundary."""

    def __init__(
        self,
        original: Exception,
        *,
        operation: str,
        statement: str,
        target: str,
    ):
        super().__init__(
            f"destination data rejection: "
            f"{type(original).__module__}.{type(original).__qualname__}"
        )
        self.original = original
        self.operation = operation
        self.statement = statement
        self.target = target


def _data_rejection_types() -> tuple[type[Exception], ...]:
    """Resolve only the explicit value/row rejection classes."""
    try:
        import duckdb
    except ImportError:  # pragma: no cover - DuckDB is a production dependency
        return ()
    return tuple(
        getattr(duckdb, name) for name in DATA_REJECTION_EXCEPTION_NAMES if hasattr(duckdb, name)
    )


def is_driver_error(error: Exception) -> bool:
    """Return whether an unwrapped exception belongs to the destination driver."""
    try:
        import duckdb
    except ImportError:  # pragma: no cover - DuckDB is a production dependency
        return False
    return isinstance(error, duckdb.Error)


def _wrap_data_rejection(
    error: Exception,
    *,
    operation: str,
    statement: str,
    target: str,
) -> None:
    if isinstance(error, _data_rejection_types()):
        raise DestinationDataRejection(
            error,
            operation=operation,
            statement=statement,
            target=target,
        ) from error
    raise error


class MaterializationConnection:
    """DML-only facade bound to one destination table.

    It deliberately has no generic ``execute``/``executemany`` forwarding API.
    Table materialization calls the explicit DML methods; control-state helpers
    receive the raw connection and can therefore never be wrapped by this
    boundary, regardless of the SQL statement they execute.
    """

    __slots__ = ("_connection", "_target")

    def __init__(self, connection: Any, target: str):
        if not isinstance(target, str) or not target:
            raise ValueError("a concrete destination table is required")
        self._connection = connection
        self._target = target

    @property
    def target(self) -> str:
        return self._target

    def _execute_table_dml(self, statement: str, parameters=None):
        _check_statement_binding(self._target, statement)
        try:
            if parameters is None:
                return self._connection.execute(statement)
            return self._connection.execute(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="table_dml",
                statement=statement,
                target=self._target,
            )
            raise  # pragma: no cover - _wrap_data_rejection always raises

    def _executemany_table_dml(self, statement: str, parameters):
        _check_statement_binding(self._target, statement)
        try:
            return self._connection.executemany(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="table_dml_many",
                statement=statement,
                target=self._target,
            )
            raise  # pragma: no cover - _wrap_data_rejection always raises

    def register(self, *args, **kwargs):
        return self._connection.register(*args, **kwargs)

    def unregister(self, *args, **kwargs):
        return self._connection.unregister(*args, **kwargs)


def execute_table_dml(connection, statement: str, parameters=None):
    """Route a table DML operation through its capability when present."""
    if isinstance(connection, MaterializationConnection):
        return connection._execute_table_dml(statement, parameters)
    if parameters is None:
        return connection.execute(statement)
    return connection.execute(statement, parameters)


def executemany_table_dml(connection, statement: str, parameters):
    """Route bulk table DML through its capability when present."""
    if isinstance(connection, MaterializationConnection):
        return connection._executemany_table_dml(statement, parameters)
    return connection.executemany(statement, parameters)
