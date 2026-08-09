"""Small, honest helpers for real MotherDuck capability evidence."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

from cdc_flight.naming import quote


def connect(token: str, database: str) -> duckdb.DuckDBPyConnection:
    """Open a fresh cloud connection and refresh its catalog snapshot."""

    con = duckdb.connect(f"md:{database}?motherduck_token={token}")
    con.execute("FORCE CHECKPOINT")
    return con


def assert_runtime(con: duckdb.DuckDBPyConnection) -> None:
    """Assert the cloud connection exposes the supported DuckDB runtime."""

    version = str(con.execute("SELECT version()").fetchone()[0])
    assert "v1.5." in version, version


@contextmanager
def scratch_database(token: str, prefix: str) -> Iterator[str]:
    """Create and always remove an isolated MotherDuck database."""

    database = f"{prefix}_{uuid.uuid4().hex[:10]}"
    bootstrap = duckdb.connect(f"md:?motherduck_token={token}")
    try:
        bootstrap.execute(f"CREATE DATABASE {quote(database)}")
    finally:
        bootstrap.close()
    try:
        yield database
    finally:
        _drop_database(token, database)


def create_database(token: str, database: str) -> None:
    """Create one named MotherDuck database for a worker-owned test session."""

    bootstrap = duckdb.connect(f"md:?motherduck_token={token}")
    try:
        bootstrap.execute(f"CREATE DATABASE {quote(database)}")
    finally:
        bootstrap.close()


def _database_names(connect_factory=duckdb.connect, *, token: str) -> set[str]:
    """Read the account catalog through a newly opened connection."""
    con = connect_factory(f"md:?motherduck_token={token}")
    try:
        return {str(row[0]) for row in con.execute("SHOW DATABASES").fetchall()}
    finally:
        con.close()


def _schema_names(
    connect_factory=duckdb.connect, *, token: str, database: str
) -> set[str]:
    """Read one database's schemas through a newly opened connection."""

    con = connect_factory(f"md:{database}?motherduck_token={token}")
    try:
        return {
            str(row[0])
            for row in con.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        }
    finally:
        con.close()


def _drop_database(
    token: str,
    database: str,
    *,
    connect_factory=duckdb.connect,
    attempts: int = 5,
    delay: float = 1.0,
) -> None:
    """Drop a scratch database, prove absence, and surface every failure.

    MotherDuck may briefly retain a catalog entry after a successful drop. Each
    attempt therefore uses fresh connections for both the DROP and the verification.
    A failed DROP is never treated as a successful context exit.
    """
    if attempts < 1:
        raise ValueError("attempts must be positive")
    last_error: BaseException | None = None
    for attempt in range(attempts):
        last_error = None
        cleanup = connect_factory(f"md:?motherduck_token={token}")
        try:
            cleanup.execute(f"DROP DATABASE {quote(database)}")
        except BaseException as exc:
            last_error = exc
        finally:
            cleanup.close()
        if last_error is None:
            try:
                names = _database_names(connect_factory, token=token)
                if database not in names:
                    return
                last_error = RuntimeError(
                    f"MotherDuck still lists scratch database {database!r} after DROP"
                )
            except BaseException as exc:
                last_error = exc
        if attempt + 1 < attempts:
            time.sleep(delay)
    raise RuntimeError(
        f"could not prove MotherDuck scratch database {database!r} was dropped"
    ) from last_error


def _drop_schema(
    token: str,
    database: str,
    schema: str,
    *,
    connect_factory=duckdb.connect,
    attempts: int = 5,
    delay: float = 1.0,
) -> None:
    """Drop a per-test schema, prove absence, and surface every failure."""

    if attempts < 1:
        raise ValueError("attempts must be positive")
    last_error: BaseException | None = None
    for attempt in range(attempts):
        last_error = None
        cleanup = connect_factory(f"md:{database}?motherduck_token={token}")
        try:
            cleanup.execute(f"DROP SCHEMA IF EXISTS {quote(schema)} CASCADE")
        except BaseException as exc:
            last_error = exc
        finally:
            cleanup.close()
        if last_error is None:
            try:
                names = _schema_names(
                    connect_factory, token=token, database=database
                )
                if schema not in names:
                    return
                last_error = RuntimeError(
                    f"MotherDuck still lists test schema {schema!r} after DROP"
                )
            except BaseException as exc:
                last_error = exc
        if attempt + 1 < attempts:
            time.sleep(delay)
    raise RuntimeError(
        f"could not prove MotherDuck test schema {schema!r} was dropped"
    ) from last_error
