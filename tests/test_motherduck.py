"""MotherDuck smoke test.

Deselected by `make test`; run with `make test-md` or `pytest -m motherduck`.
Kept deliberately light - one snapshot load into `cdc_flight_dev`.
"""

from __future__ import annotations

import contextlib
import os
import time
import uuid

import duckdb
import pytest

from cdc_flight.config import motherduck_token

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

MD_DATABASE = "cdc_flight_dev"


def md_connect(token: str, database: str = MD_DATABASE):
    """Connect to MotherDuck with a **fresh** view of the catalog.

    MEASURED, 2026-07-30: `duckdb.connect()` caches the database instance per DSN
    *within a process*, and MotherDuck's catalog snapshot rides on that instance.
    So a test process that has already opened `md:<db>` — even on a connection it
    has since closed — gets a stale catalog on the next `connect()` and cannot see
    what a pipeline **subprocess** committed in between. Locally this never
    happens, because DuckDB-on-a-file has no catalog to be stale about.

    That is a test-harness hazard, not an applier one (each pipeline run is its
    own process), but it is exactly the kind of thing that makes a MotherDuck
    assertion pass vacuously, so it is fixed rather than worked around.
    `FORCE CHECKPOINT` is what re-syncs the cached instance.
    """
    con = duckdb.connect(f"md:{database}?motherduck_token={token}")
    con.execute("FORCE CHECKPOINT")
    return con


def wait_for_tables(con, dataset: str, timeout: float = 90.0) -> set[str]:
    """Poll until the pipeline subprocess's tables become visible to `con`.

    See `md_connect`. A cached instance's catalog catches up on its own schedule,
    so the honest thing for a test that reads across a process boundary is to
    wait for it rather than to sample once and conclude the write never happened.
    Returns whatever is visible when the deadline passes, so the caller's
    assertion message stays informative.
    """
    deadline = time.monotonic() + timeout
    tables: set[str] = set()
    while True:
        tables = {
            t
            for (t,) in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [dataset],
            ).fetchall()
        }
        if tables or time.monotonic() >= deadline:
            return tables
        time.sleep(2.0)
        with contextlib.suppress(duckdb.Error):
            con.execute("FORCE CHECKPOINT")


@pytest.fixture(scope="module")
def md_token() -> str:
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    return token


@pytest.fixture
def md_dataset(md_token: str):
    """A throwaway dataset per run, dropped afterwards."""
    name = f"cdc_smoke_{uuid.uuid4().hex[:8]}"
    con = duckdb.connect(f"md:?motherduck_token={md_token}")
    con.execute(f'CREATE DATABASE IF NOT EXISTS "{MD_DATABASE}"')
    con.close()
    yield name
    con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={md_token}")
    try:
        con.execute(f'DROP SCHEMA IF EXISTS "{MD_DATABASE}"."{name}" CASCADE')
    finally:
        con.close()


def test_snapshot_loads_into_motherduck(fresh_seed, run_pipeline, md_token, md_dataset):
    result = run_pipeline(
        destination="motherduck",
        reset_state=True,
        max_seconds=240,
        idle_seconds=10,
        timeout=420,
        extra_env={
            "CDC_DATASET": md_dataset,
            "CDC_MD_DATABASE": MD_DATABASE,
            "MOTHERDUCK_TOKEN": md_token,
            "motherduck_token": md_token,
        },
    )
    assert result["destination"] == "motherduck"
    assert result["motherduck_database"] == MD_DATABASE
    assert result["records"] == 20, result

    con = md_connect(md_token)
    try:
        tables = wait_for_tables(con, md_dataset)
        assert "cdcflight_app_customers" in tables, (
            f"dataset {md_dataset!r} holds {sorted(tables)}; run summary="
            f"{ {k: v for k, v in result.items() if k != 'output'} }"
        )

        n, ops = con.execute(
            f'SELECT count(*), count(DISTINCT dbz_op) FROM "{MD_DATABASE}"."{md_dataset}".'
            '"cdcflight_app_customers"'
        ).fetchone()
        assert n == 5
        assert ops == 1  # snapshot only -> all rows are op='r'

        names = {
            r[0]
            for r in con.execute(
                f'SELECT name FROM "{MD_DATABASE}"."{md_dataset}"."cdcflight_app_customers"'
            ).fetchall()
        }
        assert "Ada Lovelace" in names
    finally:
        con.close()


def test_motherduck_token_is_available_to_the_suite(md_token):
    assert len(md_token) > 20
    assert os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
