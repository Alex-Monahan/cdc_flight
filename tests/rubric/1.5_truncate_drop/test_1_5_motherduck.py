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

pytestmark = [
    pytest.mark.motherduck,
    pytest.mark.e2e,
    pytest.mark.xdist_group("md_1_5"),
]

#: MotherDuck's catalog snapshot is cached per DSN in this process, so a reader here
#: can go stale against writes made by the pipeline subprocess (ADR §15/A15).
REFRESH = "FORCE CHECKPOINT"

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
def md_database(motherduck_module_case):
    return motherduck_module_case


@pytest.fixture(scope="module")
def md_truncate_drop(sandbox, md_token, md_database) -> dict:
    suffix = uuid.uuid4().hex[:8]
    dataset = f"cdc_15_{suffix}"
    pipeline = f"cdc_15_{suffix}"
    database = md_database["database"]
    control_schema = md_database["control_schema"]
    dsn = f"md:{database}?motherduck_token={md_token}"
    env = {
        "CDC_PIPELINE_NAME": pipeline,
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": md_token,
        "motherduck_token": md_token,
        "CDC_TABLES": TABLES,
        "CDC_CATALOG_POLL_SECONDS": "1",
    }

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
        yield {"box": box, "con": con, "database": database,
               "control_schema": control_schema, "dataset": dataset, "pipeline": pipeline,
               "streamed": streamed, "settled": settled}
    finally:
        con.close()
        box.reseed()


def _rows(state, sql: str, params: list | None = None) -> list[tuple]:
    return state["con"].execute(sql, params or []).fetchall()


def _state(state, table: str) -> str:
    return f'"{state["control_schema"]}"."{table}"'


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
        f"SELECT rows_removed FROM {_state(md_truncate_drop, 'table_events')} WHERE pipeline = ? "
        "AND event = 'truncate' AND source_table = 'md_trunc'",
        [md_truncate_drop["pipeline"]],
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


def test_the_alert_sink_is_an_independent_connection_at_motherduck(md_truncate_drop):
    """Codex 7 / Opus M-2. `AlertSink` uses `con.cursor()` so a destructive-DDL alert
    survives the rollback of the apply it describes. That is VERIFIED on DuckDB; here
    it is verified against a server-side transaction implementation, because a
    same-connection insert dressed up as non-transactional is the defect this replaces.
    """
    assert md_truncate_drop["streamed"]["alerts_out_of_transaction"] is True
    assert md_truncate_drop["settled"]["alerts_out_of_transaction"] is True
    codes = _rows(
        md_truncate_drop,
        f"SELECT DISTINCT code FROM {_state(md_truncate_drop, 'alerts')} WHERE pipeline = ?",
        [md_truncate_drop["pipeline"]],
    )
    assert ("table_dropped",) in codes


def test_the_source_relation_ownership_survived(md_truncate_drop):
    """ADR §18/A39: `table_state` is the registry the watcher seeds itself from, and it
    is written by whoever first materialises the table."""
    owned = _rows(
        md_truncate_drop,
        f"SELECT source_table FROM {_state(md_truncate_drop, 'table_state')} WHERE pipeline = ? "
        "ORDER BY source_table",
        [md_truncate_drop["pipeline"]],
    )
    assert ("md_trunc",) in owned
    assert ("md_drop",) not in owned, "the dropped table's ownership row went with it"


# --------------------------------------------------------------------------- #
# a fault around a truncate, at MotherDuck (Codex 9-point item 9)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def md_truncate_rollback(sandbox, md_token, md_database) -> dict:
    """A truncate whose commit group fails at `pre_commit`, then the replay.

    The `DELETE FROM` and the `table_events` marker are inside one server-side
    transaction at MotherDuck; the rubric's claim is that a rolled-back group leaves
    **every row** in place and records nothing. Verified against DuckDB in the default
    suite; this is the same assertion where the transaction is not local.
    """
    suffix = uuid.uuid4().hex[:8]
    dataset = f"cdc_15rb_{suffix}"
    pipeline = f"cdc_15rb_{suffix}"
    database = md_database["database"]
    control_schema = md_database["control_schema"]
    dsn = f"md:{database}?motherduck_token={md_token}"
    env = {
        "CDC_PIPELINE_NAME": pipeline,
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": md_token,
        "motherduck_token": md_token,
        "CDC_TABLES": TABLES,
        "CDC_CATALOG_POLL_SECONDS": "1",
    }
    box = sandbox
    box.reseed()
    box.sql(
        [
            "CREATE TABLE app.md_trunc (id bigint PRIMARY KEY, label text NOT NULL)",
            "CREATE TABLE app.md_drop (id bigint PRIMARY KEY, label text NOT NULL)",
            "INSERT INTO app.md_trunc VALUES (1, 'a'), (2, 'b'), (3, 'c')",
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
    failed = box.run(
        destination="motherduck", max_seconds=300, idle_seconds=8, timeout=600,
        expect_success=False,
        extra_env={**env, "CDC_FAULT_INJECT": "pre_commit:1:raise"},
    )
    con = duckdb.connect(dsn)
    con.execute(REFRESH)
    after_failure = con.execute(
        f'SELECT id FROM "{dataset}"."{TR}" ORDER BY id'
    ).fetchall()
    # Scoped to truncate markers: the first run records one `new` row per discovered
    # table (rubric 2.3's hook), which is not what this asserts.
    markers_after_failure = con.execute(
        f"SELECT count(*) FROM {_state({'control_schema': control_schema}, 'table_events')} "
        "WHERE pipeline = ? "
        "AND event = 'truncate'",
        [pipeline],
    ).fetchall()
    con.close()

    replayed = box.run(
        destination="motherduck", max_seconds=300, idle_seconds=8, timeout=600,
        extra_env=env,
    )
    con = duckdb.connect(dsn)
    con.execute(REFRESH)
    try:
        yield {
            "con": con, "database": database, "control_schema": control_schema,
            "dataset": dataset, "pipeline": pipeline,
            "failed": failed, "replayed": replayed,
            "after_failure": after_failure,
            "markers_after_failure": markers_after_failure,
        }
    finally:
        con.close()
        box.reseed()


def test_the_injected_fault_actually_failed_the_run(md_truncate_rollback):
    assert md_truncate_rollback["failed"]["returncode"] != 0
    assert md_truncate_rollback["failed"].get("ok") is not True


def test_a_rolled_back_truncate_leaves_every_motherduck_row(md_truncate_rollback):
    assert md_truncate_rollback["after_failure"] == [(1,), (2,), (3,)]
    assert md_truncate_rollback["markers_after_failure"] == [(0,)], (
        "no truncate marker may outlive the apply it describes"
    )


def test_the_replay_lands_the_truncate_exactly_once(md_truncate_rollback):
    dataset = md_truncate_rollback["dataset"]
    assert md_truncate_rollback["replayed"]["ok"] is True
    assert _rows(md_truncate_rollback, f'SELECT id FROM "{dataset}"."{TR}" ORDER BY id') == [
        (9,)
    ]
    assert _rows(
        md_truncate_rollback,
        f"SELECT count(*), max(rows_removed) FROM "
        f"{_state(md_truncate_rollback, 'table_events')} "
        "WHERE pipeline = ? AND event = 'truncate'",
        [md_truncate_rollback["pipeline"]],
    ) == [(1, 3)]
