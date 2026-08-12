"""Provenance for destination failures raised during table materialization.

An exception class is not a table boundary.  DuckDB's broad error hierarchy is
also used for transaction control, parser/catalog, connection, and session
failures.  A failure is table-containable only when the materializer caught one
of the driver's *value/row rejection* classes while executing a data operation.
Everything else keeps its run-level meaning.
"""

from __future__ import annotations

from typing import Any

OWNER = "destination-failure-classification"

# This is deliberately a closed taxonomy of driver errors that describe a
# rejected value, row, or column.  The operation provenance below is equally
# load-bearing: these names alone never make an exception containable.
DATA_REJECTION_EXCEPTION_NAMES = (
    "ConversionException",
    "ConstraintException",
    "OutOfRangeException",
)

_CONTROL_STATEMENT_PREFIXES = (
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "SET",
    "RESET",
    "PRAGMA",
    "ATTACH",
    "DETACH",
    "LOAD",
    "INSTALL",
    "USE",
)


class DestinationDataRejection(RuntimeError):
    """A driver value/row rejection observed at the table-DML boundary."""

    def __init__(self, original: Exception, *, operation: str, statement: str):
        super().__init__(str(original))
        self.original = original
        self.operation = operation
        self.statement = statement


def _data_rejection_types() -> tuple[type[Exception], ...]:
    """Resolve only the explicitly data-shaped driver classes."""
    try:
        import duckdb
    except ImportError:  # pragma: no cover - DuckDB is a production dependency
        return ()
    return tuple(
        getattr(duckdb, name)
        for name in DATA_REJECTION_EXCEPTION_NAMES
        if hasattr(duckdb, name)
    )


def is_driver_error(error: Exception) -> bool:
    """Return whether an unwrapped exception belongs to the destination driver.

    This is a loudness check, not a containment classifier.  A driver error that
    did not cross ``MaterializationConnection``'s data-operation boundary must
    never be converted into a source-table refusal.
    """
    try:
        import duckdb
    except ImportError:  # pragma: no cover - DuckDB is a production dependency
        return False
    return isinstance(error, duckdb.Error)


def _wrap_data_rejection(error: Exception, *, operation: str, statement: str):
    if (
        not statement.lstrip().upper().startswith(_CONTROL_STATEMENT_PREFIXES)
        and isinstance(error, _data_rejection_types())
    ):
        raise DestinationDataRejection(
            error,
            operation=operation,
            statement=statement,
        ) from error
    raise error


class MaterializationConnection:
    """Connection facade that records DML provenance without hiding control errors.

    The facade is passed only to table materialization.  Its ``execute`` and
    ``executemany`` methods wrap the narrow value/row rejection taxonomy; a
    transaction-control or other driver exception is delegated unchanged.  The
    rest of DuckDB's connection API remains available for Arrow registration and
    cleanup through ``__getattr__``.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def execute(self, statement: str, parameters=None):
        try:
            if parameters is None:
                return self._connection.execute(statement)
            return self._connection.execute(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="execute",
                statement=statement,
            )
            raise  # pragma: no cover - _wrap_data_rejection always raises

    def executemany(self, statement: str, parameters):
        try:
            return self._connection.executemany(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="executemany",
                statement=statement,
            )
            raise  # pragma: no cover - _wrap_data_rejection always raises

    def __getattr__(self, name: str):
        return getattr(self._connection, name)
