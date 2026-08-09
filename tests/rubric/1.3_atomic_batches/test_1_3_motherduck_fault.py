"""Rubric 1.1 / 1.2 / 1.3 — the protocol fault matrix against **MotherDuck**.

The observer test next door is healthy-path evidence: it proves no independent
connection ever sees a partial Postgres transaction. It does not put a fault
barrier anywhere, so the whole crash-and-recover argument was only ever executed
against DuckDB-on-a-file — a different transaction implementation from the one
rubric 1.3 is actually about (Codex 6, Opus M-4).

This module crashes the pipeline at the two anchors that matter most, against real
MotherDuck, and asserts exactly-once on recovery:

* `mid_apply` — table A written, table B not, transaction still open. MotherDuck
  must roll the whole group back.
* `post_commit_pre_ack` — committed at MotherDuck, Debezium not acknowledged. The
  at-least-once window; under Invariant O the recovery run must re-apply nothing.

It also checks the thing that cannot be checked locally: that MotherDuck accepts
the destination-side `PRIMARY KEY` the identity columns carry (Opus M-2). If it
did not, `SchemaRegistry._create` would silently fall back to the post-apply
uniqueness assertion, and the constraint would exist only on DuckDB.

Deselected by `make test`; run with `make test-md`.
"""

from __future__ import annotations

import uuid

import duckdb
import pytest
from support.motherduck_probe import scratch_database

from cdc_flight.config import motherduck_token

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

REFRESH = "FORCE CHECKPOINT"
N = 20
#: (anchor, tag). Two anchors, not seven: each is a crash plus a recovery run
#: against the network, and `make test-md` has to stay usable.
ANCHORS = [("mid_apply", "mdfm"), ("post_commit_pre_ack", "mdfa")]


@pytest.fixture(scope="module")
def md_token() -> str:
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    return token


@pytest.fixture
def md_crashed(sandbox, md_token) -> dict:
    with scratch_database(md_token, "cdc_fault") as database:
        dataset = f"cdc_fault_{uuid.uuid4().hex[:8]}"
        control_schema = f"_cdc_flight_{uuid.uuid4().hex[:8]}"
        dsn = f"md:{database}?motherduck_token={md_token}"
        env = {
            "CDC_DATASET": dataset,
            "CDC_MD_DATABASE": database,
            "CDC_CONTROL_SCHEMA": control_schema,
            "MOTHERDUCK_TOKEN": md_token,
            "motherduck_token": md_token,
        }

        sandbox.reseed()
        sandbox.run(
            reset_state=True, destination="motherduck", max_seconds=300,
            timeout=600, extra_env=env,
        )

        results: dict[str, dict] = {}
        for anchor, tag in ANCHORS:
            sandbox.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT "
                f"'{tag}-c-' || i, '{tag}-c-' || i || '@example.com' "
                f"FROM generate_series(1, {N}) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                f"'{tag.upper()}', i * 1.25, 'C' FROM generate_series(1, {N}) i",
            ],
            one_transaction=True,
        )
            crashed = sandbox.run(
                destination="motherduck", max_seconds=300, timeout=600,
                expect_success=False,
                extra_env={**env, "CDC_FAULT_INJECT": f"{anchor}:1"},
            )
            recovered = sandbox.run(
                destination="motherduck", max_seconds=300, timeout=600, extra_env=env
            )
            results[anchor] = {"crashed": crashed, "recovered": recovered, "tag": tag}

        con = duckdb.connect(dsn)
        con.execute(REFRESH)
        try:
            yield {
                "con": con,
                "database": database,
                "control_schema": control_schema,
                "dataset": dataset,
                "results": results,
                "n": N,
            }
        finally:
            con.close()


def _q(md, sql: str):
    return md["con"].execute(sql).fetchall()


@pytest.mark.parametrize("anchor", [a for a, _ in ANCHORS])
def test_the_motherduck_fault_actually_fired(md_crashed, anchor):
    """Guard: a fault that did not fire makes every assertion below vacuous."""
    outcome = md_crashed["results"][anchor]
    assert outcome["crashed"]["returncode"] == 137, outcome["crashed"]
    assert outcome["recovered"]["returncode"] == 0, outcome["recovered"]


@pytest.mark.parametrize("anchor", [a for a, _ in ANCHORS])
def test_motherduck_delivery_is_exactly_once_after_a_crash(md_crashed, anchor):
    """Measured on the keyless changelog, where a PK merge cannot hide a dup."""
    dataset, n = md_crashed["dataset"], md_crashed["n"]
    tag = md_crashed["results"][anchor]["tag"]

    missing = _q(
        md_crashed,
        f"SELECT count(*) FROM generate_series(1, {n}) g(i) WHERE NOT EXISTS ("
        f'  SELECT 1 FROM "{dataset}"."cdcflight_app_customers" c '
        f"  WHERE c.name = '{tag}-c-' || g.i)",
    )[0][0]
    assert missing == 0, f"crash at {anchor} lost {missing} of {n} keyed rows in MotherDuck"

    events, unique = _q(
        md_crashed,
        f'SELECT count(*), count(DISTINCT cdcf_event_id) FROM "{dataset}".'
        f"\"cdcflight_app_sensor_readings\" WHERE sensor_id = '{tag.upper()}'",
    )[0]
    assert events == n, (
        f"crash at {anchor}: MotherDuck holds {events} change events, the source "
        f"produced {n}"
    )
    assert unique == n, f"crash at {anchor}: {n - unique} change events applied twice"


def test_a_torn_group_was_never_visible_in_motherduck(md_crashed):
    """`mid_apply` fires between two table writes, so the crashed run had written
    one table and not the other. MotherDuck must have rolled both back — the
    recovery run is what put them there, in ONE commit group."""
    dataset = md_crashed["dataset"]
    tag = md_crashed["results"]["mid_apply"]["tag"]
    commits = _q(
        md_crashed,
        f'SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_customers" '
        f"WHERE name LIKE '{tag}-c-%' "
        f'UNION SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_sensor_readings" '
        f"WHERE sensor_id = '{tag.upper()}'",
    )
    assert len(commits) == 1, (
        f"the transaction torn by the mid_apply crash is spread over {len(commits)} "
        f"commit groups in MotherDuck: {commits}"
    )


def test_the_committed_anchor_did_not_replay_in_motherduck(md_crashed):
    """`post_commit_pre_ack` is the Invariant-O payoff, verified at the real
    destination: the resume point went to MotherDuck with the rows, so the recovery
    run has nothing to re-apply."""
    recovered = md_crashed["results"]["post_commit_pre_ack"]["recovered"]
    assert recovered["applied_events"] == 0, recovered


def test_motherduck_accepts_the_destination_side_primary_key(md_crashed):
    """Opus M-2 has to be real at the destination that matters.

    If MotherDuck rejected `PRIMARY KEY` on `CREATE TABLE`, the applier would fall
    back to the post-apply uniqueness assertion and the constraint would exist only
    locally — so this asserts the constraint is actually enforced by MotherDuck.
    """
    dataset = md_crashed["dataset"]
    con = md_crashed["con"]
    row = con.execute(
        f'SELECT cdcf_event_id FROM "{dataset}"."cdcflight_app_sensor_readings" LIMIT 1'
    ).fetchone()
    assert row, "no keyless rows in MotherDuck to test the constraint with"
    with pytest.raises(duckdb.Error) as excinfo:
        con.execute(
            f'INSERT INTO "{dataset}"."cdcflight_app_sensor_readings" (cdcf_event_id) '
            "VALUES (?)",
            [row[0]],
        )
    assert "constraint" in str(excinfo.value).lower(), (
        "MotherDuck accepted a duplicate cdcf_event_id, so the identity is not "
        f"enforced by the destination: {excinfo.value}"
    )
