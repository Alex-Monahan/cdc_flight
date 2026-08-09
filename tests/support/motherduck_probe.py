"""Small, honest helpers for real MotherDuck capability evidence."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb

from cdc_flight.destination import DUCKDB_CONNECT_CONFIG


def connect(token: str, database: str) -> duckdb.DuckDBPyConnection:
    """Open a fresh cloud connection and refresh its catalog snapshot."""

    con = duckdb.connect(
        f"md:{database}?motherduck_token={token}", config=DUCKDB_CONNECT_CONFIG
    )
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
    bootstrap = duckdb.connect(
        f"md:?motherduck_token={token}", config=DUCKDB_CONNECT_CONFIG
    )
    try:
        bootstrap.execute(f'CREATE DATABASE "{database}"')
    finally:
        bootstrap.close()
    try:
        yield database
    finally:
        cleanup = duckdb.connect(
            f"md:?motherduck_token={token}", config=DUCKDB_CONNECT_CONFIG
        )
        try:
            with contextlib.suppress(duckdb.Error):
                cleanup.execute(f'DROP DATABASE "{database}"')
        finally:
            cleanup.close()
