"""MotherDuck smoke test.

Deselected by `make test`; run with `make test-md` or `pytest -m motherduck`.
Kept deliberately light - one snapshot load into `cdc_flight_dev`.
"""

from __future__ import annotations

import os
import uuid

import duckdb
import pytest

from cdc_flight.config import motherduck_token

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

MD_DATABASE = "cdc_flight_dev"


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

    con = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={md_token}")
    try:
        tables = {
            t
            for (t,) in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [md_dataset],
            ).fetchall()
        }
        assert "cdcflight_app_customers" in tables, sorted(tables)

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
