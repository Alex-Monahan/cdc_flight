"""The capability boundary for table-data destination failures.

DuckDB uses one exception hierarchy for row/value rejection and for control,
catalog, connection, and transaction failures.  The only safe containment
capability is therefore an opaque token minted by the table writer at the first
destination-table DML operation.  Control-state code receives the raw
connection and cannot acquire that capability from it.
"""

from __future__ import annotations

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


def _make_provenance_capability():
    """Create the token type and its closed-over mint without exposing the mint."""
    capability_token = object()

    class TableDataProvenance:
        """An opaque, relation-specific table-DML capability."""

        __slots__ = ("__source_schema", "__source_table", "__target")

        def __init__(self, capability, source_schema: str, source_table: str, target: str):
            if capability is not capability_token:
                raise TypeError("table-data provenance can only be minted by the table writer")
            if not all(
                isinstance(value, str) and value for value in (source_schema, source_table, target)
            ):
                raise ValueError(
                    "table-data provenance requires a concrete source relation and target"
                )
            self.__source_schema = source_schema
            self.__source_table = source_table
            self.__target = target

        @property
        def source_schema(self) -> str:
            return self.__source_schema

        @property
        def source_table(self) -> str:
            return self.__source_table

        @property
        def target(self) -> str:
            return self.__target

        @property
        def qualified_source(self) -> str:
            return f"{self.__source_schema}.{self.__source_table}"

    def mint(
        source_schema: str | None, source_table: str | None, target: str | None
    ) -> TableDataProvenance:
        return TableDataProvenance(capability_token, source_schema, source_table, target)

    return TableDataProvenance, mint


TableDataProvenance, _mint_table_data_provenance = _make_provenance_capability()


class DestinationDataRejection(RuntimeError):
    """A classified value/row rejection at a destination-table DML boundary."""

    def __init__(
        self,
        original: Exception,
        *,
        operation: str,
        statement: str,
        provenance: TableDataProvenance,
    ):
        super().__init__(str(original))
        self.original = original
        self.operation = operation
        self.statement = statement
        self.provenance = provenance


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
    provenance: TableDataProvenance,
) -> None:
    if isinstance(error, _data_rejection_types()):
        raise DestinationDataRejection(
            error,
            operation=operation,
            statement=statement,
            provenance=provenance,
        ) from error
    raise error


class MaterializationConnection:
    """DML-only facade carrying an opaque table-data provenance capability.

    It deliberately has no generic ``execute``/``executemany`` forwarding API.
    Table materialization calls the explicit DML methods; control-state helpers
    receive the raw connection and can therefore never be wrapped by this
    boundary, regardless of the SQL statement they execute.
    """

    __slots__ = ("_connection", "_provenance")

    def __init__(self, connection: Any, provenance: TableDataProvenance):
        if not isinstance(provenance, TableDataProvenance):
            raise TypeError("a table-data provenance capability is required")
        self._connection = connection
        self._provenance = provenance

    @property
    def provenance(self) -> TableDataProvenance:
        return self._provenance

    def _execute_table_dml(self, statement: str, parameters=None):
        try:
            if parameters is None:
                return self._connection.execute(statement)
            return self._connection.execute(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="table_dml",
                statement=statement,
                provenance=self._provenance,
            )
            raise  # pragma: no cover - _wrap_data_rejection always raises

    def _executemany_table_dml(self, statement: str, parameters):
        try:
            return self._connection.executemany(statement, parameters)
        except Exception as error:
            _wrap_data_rejection(
                error,
                operation="table_dml_many",
                statement=statement,
                provenance=self._provenance,
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
