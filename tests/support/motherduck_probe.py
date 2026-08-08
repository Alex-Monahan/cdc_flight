"""Small, honest helpers for real MotherDuck capability evidence."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import duckdb


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
        bootstrap.execute(f'CREATE DATABASE "{database}"')
    finally:
        bootstrap.close()
    try:
        yield database
    finally:
        cleanup = duckdb.connect(f"md:?motherduck_token={token}")
        try:
            with contextlib.suppress(duckdb.Error):
                cleanup.execute(f'DROP DATABASE "{database}"')
        finally:
            cleanup.close()
