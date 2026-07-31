"""Rubric 1.5 against **MotherDuck**, because that is where the rubric lives.

The rest of 1.5 runs against DuckDB-on-a-file. Two of its statements are
destination-specific and cannot be assumed to behave identically on a server-side
transaction implementation:

* the `DELETE FROM <table>` a truncate issues inside the commit group (and whether
  the destination reports how many rows it removed, which is what the marker records);
* the `DROP TABLE` a detected source drop issues inside that same transaction — DDL
  transactionality at MotherDuck is probed per run (`transactional_ddl`), and this is
  the first thing in the repo that issues a `DROP` for a reason other than a snapshot
  swap.

One small scenario, one demo table, two runs. Deselected by `make test`; run with
`make test-md`.
"""

from __future__ import annotations

import uuid

import duckdb
import pytest

from cdc_flight.config import motherduck_token

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

#: MotherDuck's catalog snapshot is cached per DSN in this process, so a reader here
#: can go stale against writes made by the pipeline subprocess (ADR §15/A15).
REFRESH = "FORCE CHECKPOINT"

MD_DATABASE = "cdc_flight_dev"
TR = "cdcflight_app_md_trunc"
DR = "cdcflight_app_md_drop"
TABLES = "customers,orders,sensor_readings,documents,wide_types,audit_log,md_trunc,md_drop"


@pytest.fixture(scope="module")
def md_token() -> str:
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    return token


@pytest.fixture(scope="module")
def md_truncate_drop(sandbox, md_token) -> dict:
    dataset = f"cdc_15_{uuid.uuid4().hex[:8]}"
    dsn = f"md:{MD_DATABASE}?motherduck_token={md_token}"
    env = {
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": MD_DATABASE,
        "MOTHERDUCK_TOKEN": md_token,
        "motherduck_token": md_token,
        "CDC_TABLES": TABLES,
        "CDC_CATALOG_POLL_SECONDS": "1",
    }

    bootstrap = duckdb.connect(f"md:?motherduck_token={md_token}")
    bootstrap.execute(f'CREATE DATABASE IF NOT EXISTS "{MD_DATABASE}"')
    bootstrap.close()

    box = sandbox
    box.reseed()
    box.sql(
        [
            "CREATE TABLE app.md_trunc (id bigint PRIMARY KEY, label text NOT NULL)",
            "CREATE TABLE app.md_drop (id bigint PRIMARY KEY, label text NOT NULL)",
            "INSERT INTO app.md_trunc VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO app.md_drop VALUES (1, 'here'), (2, 'for now')",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.md_trunc, app.md_drop",
        ]
    )
    box.run(
        reset_state=True, destination="motherduck", max_seconds=300,
        timeout=600, extra_env=env,
    )

    box.sql(
        ["TRUNCATE TABLE app.md_trunc", "INSERT INTO app.md_trunc VALUES (9, 'after')"],
        one_transaction=True,
    )
    box.sql("DROP TABLE app.md_drop")
    box.sql("INSERT INTO app.customers (id, name, email) VALUES (8500, 'md', 'md@example.com')")

    streamed = box.run(
        destination="motherduck", max_seconds=400, idle_seconds=10,
        timeout=700, extra_env=env,
    )
    settled = box.run(
        destination="motherduck", max_seconds=300, idle_seconds=8,
        timeout=600, extra_env=env,
    )

    con = duckdb.connect(dsn)
    con.execute(REFRESH)
    try:
        yield {"box": box, "con": con, "dataset": dataset,
               "streamed": streamed, "settled": settled}
    finally:
        con.execute(f'DROP SCHEMA IF EXISTS "{MD_DATABASE}"."{dataset}" CASCADE')
        con.close()
        box.reseed()


def _rows(state, sql: str, params: list | None = None) -> list[tuple]:
    return state["con"].execute(sql, params or []).fetchall()


def test_the_runs_reached_motherduck_cleanly(md_truncate_drop):
    assert md_truncate_drop["streamed"]["ok"] is True
    assert md_truncate_drop["settled"]["ok"] is True
    assert md_truncate_drop["streamed"]["destination"] == "motherduck"


def test_the_truncate_emptied_the_motherduck_table(md_truncate_drop):
    dataset = md_truncate_drop["dataset"]
    assert _rows(md_truncate_drop, f'SELECT id FROM "{dataset}"."{TR}" ORDER BY id') == [(9,)]


def test_the_truncate_marker_counted_the_rows_motherduck_removed(md_truncate_drop):
    """`DELETE FROM` must report its row count here too, or the audit trail says
    "unknown" where the whole point is to say what was lost."""
    rows = _rows(
        md_truncate_drop,
        "SELECT rows_removed FROM _cdc_flight.table_events WHERE event = 'truncate' "
        "AND source_table = 'md_trunc'",
    )
    assert rows == [(3,)]


def test_the_dropped_table_is_gone_from_motherduck(md_truncate_drop):
    dataset = md_truncate_drop["dataset"]
    present = _rows(
        md_truncate_drop,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? "
        "AND table_name = ?",
        [dataset, DR],
    )
    assert present == [(0,)]
    assert md_truncate_drop["streamed"]["tables_dropped"] + md_truncate_drop["settled"][
        "tables_dropped"
    ] >= 1


def test_the_rest_of_the_stream_still_landed(md_truncate_drop):
    dataset = md_truncate_drop["dataset"]
    assert _rows(
        md_truncate_drop,
        f'SELECT name FROM "{dataset}"."cdcflight_app_customers" WHERE id = 8500',
    ) == [("md",)]
