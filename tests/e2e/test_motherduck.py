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
from support.motherduck_probe import scratch_database

from cdc_flight.catalog_state import read_known_relations
from cdc_flight.config import motherduck_token
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.states import UnknownState

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

def md_connect(token: str, database: str):
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
def md_case(md_token: str):
    """A complete per-test MotherDuck database and control schema."""
    with scratch_database(md_token, "cdc_smoke") as database:
        yield {
            "database": database,
            "dataset": f"cdc_smoke_{uuid.uuid4().hex[:8]}",
            "control_schema": f"_cdc_flight_{uuid.uuid4().hex[:8]}",
        }


def test_snapshot_loads_into_motherduck(fresh_seed, run_pipeline, md_token, md_case):
    database = md_case["database"]
    dataset = md_case["dataset"]
    result = run_pipeline(
        destination="motherduck",
        reset_state=True,
        max_seconds=240,
        idle_seconds=10,
        timeout=420,
        extra_env={
            "CDC_DATASET": dataset,
            "CDC_MD_DATABASE": database,
            "CDC_CONTROL_SCHEMA": md_case["control_schema"],
            "MOTHERDUCK_TOKEN": md_token,
            "motherduck_token": md_token,
        },
    )
    assert result["destination"] == "motherduck"
    assert result["motherduck_database"] == database
    assert result["records"] == 20, result

    con = md_connect(md_token, database)
    try:
        tables = wait_for_tables(con, dataset)
        assert "cdcflight_app_customers" in tables, (
            f"dataset {dataset!r} holds {sorted(tables)}; run summary="
            f"{ {k: v for k, v in result.items() if k != 'output'} }"
        )

        n, ops = con.execute(
            f'SELECT count(*), count(DISTINCT dbz_op) FROM "{database}"."{dataset}".'
            '"cdcflight_app_customers"'
        ).fetchone()
        assert n == 5
        assert ops == 1  # snapshot only -> all rows are op='r'

        names = {
            r[0]
            for r in con.execute(
                f'SELECT name FROM "{database}"."{dataset}"."cdcflight_app_customers"'
            ).fetchall()
        }
        assert "Ada Lovelace" in names
    finally:
        con.close()


def test_motherduck_token_is_available_to_the_suite(md_token):
    assert len(md_token) > 20
    assert os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")


def test_legacy_control_schema_migration_is_motherduck_compatible(md_token):
    """The real cloud destination accepts the additive migration and its backfill."""
    with scratch_database(md_token, "cdc_control_schema") as database:
        con = duckdb.connect(f"md:{database}?motherduck_token={md_token}")
        try:
            con.execute("CREATE SCHEMA _cdc_flight")
            con.execute(
            """
            CREATE TABLE _cdc_flight.source_relations (
                pipeline        VARCHAR NOT NULL,
                source_schema   VARCHAR NOT NULL,
                source_table    VARCHAR NOT NULL,
                relation_oid    BIGINT NOT NULL,
                published       BOOLEAN NOT NULL,
                replica_identity VARCHAR,
                first_seen_at   TIMESTAMPTZ NOT NULL,
                last_seen_at    TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (pipeline, source_schema, source_table)
            )
            """
            )
            con.execute(
            """
            INSERT INTO _cdc_flight.source_relations
            VALUES ('p', 'app', 'customers', 42, true, 'd', now(), now())
            """
            )

            ensure_control_schema(con)
            assert con.execute(
                "SELECT admission_state FROM _cdc_flight.source_relations"
            ).fetchall() == [("external",)]

            # Idempotence and the state-machine refusal both run against the cloud table.
            ensure_control_schema(con)
            con.execute("UPDATE _cdc_flight.source_relations SET admission_state = NULL")
            with pytest.raises(UnknownState, match="NULL"):
                read_known_relations(con, "p")
        finally:
            con.close()
