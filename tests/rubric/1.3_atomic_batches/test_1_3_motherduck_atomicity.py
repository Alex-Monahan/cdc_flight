"""Rubric 1.3 - atomicity as an *observer of MotherDuck* would experience it.

Why this file exists (Codex 7)
------------------------------
`test_1_3_atomic_batches.py` runs against local DuckDB and reconstructs the
sequence of destination commits after the fact. That is good gap evidence, but
it is the wrong *target* evidence for two reasons:

1. rubric 1.3 asks for atomicity **in MotherDuck**, and DuckDB-on-a-file is a
   different transaction implementation;
2. metadata equality (`all rows share one cdcf_commit_id`) is satisfiable by an
   implementation that stamps the same id in two separate commits. Only a
   *visibility* assertion - a second connection that never sees a prefix - is
   proof.

DuckDB's single-writer file lock makes a concurrent observer impossible locally;
MotherDuck's server-side storage makes it trivial. So the visibility proof lives
here, marked `motherduck`, and is deliberately kept **small**: one Postgres
transaction of `2 * N` events across two tables, one observer thread sampling
both tables, no large loads.

Deselected by `make test`; run with `make test-md`.
"""

from __future__ import annotations

import threading
import time
import uuid

import duckdb
import pytest
from support.applier_lab import (
    FakeCommitter,
    begin,
    end,
    fixture_descriptors,
    keyed,
    snap,
)

from cdc_flight import destination as dest_mod
from cdc_flight.applier import Applier
from cdc_flight.config import ApplierConfig, motherduck_token
from cdc_flight.destination import Lease, ResumePoint
from cdc_flight.envelope import KIND_SNAPSHOT_BOUNDARY, PendingRecord
from cdc_flight.snapshot_completion import SnapshotCompletion

pytestmark = [pytest.mark.motherduck, pytest.mark.e2e]

#: MEASURED, 2026-07-30: `duckdb.connect()` caches the database instance per DSN
#: within a process and MotherDuck's catalog snapshot rides on it, so a reader in
#: THIS process can go stale against writes made by the pipeline SUBPROCESS.
#: `FORCE CHECKPOINT` re-syncs it. Without this the observer below would sample a
#: frozen `(0, 0)` forever and `test_target_no_observer_ever_sees_a_partial_transaction`
#: would pass vacuously - which is why that test now also requires the observer to
#: have seen the transition.
REFRESH = "FORCE CHECKPOINT"

N = 1500  # per table; 2 * N = 3000 events > max.batch.size (2048)



@pytest.fixture(scope="module")
def md_token() -> str:
    token = motherduck_token()
    if not token:
        pytest.skip("`motherduck_token` not set")
    return token


@pytest.fixture
def md_observed_txn(sandbox, md_token, motherduck_case) -> dict:
    """Stream one multi-table PG transaction into MotherDuck, watching from outside.

    The observer polls both tables from a *separate* MotherDuck connection while
    the pipeline writes, and records every `(customers, orders)` pair it sees.
    An atomic implementation can only ever be observed at `(0, 0)` or `(N, N)`.
    """
    database = motherduck_case["database"]
    dataset = f"cdc_atomic_{uuid.uuid4().hex[:8]}"
    control_schema = motherduck_case["control_schema"]
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

    sandbox.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            "'mdatomic-c-' || i, 'mdatomic-c-' || i || '@example.com' "
            f"FROM generate_series(1, {N}) i",
            "INSERT INTO app.orders (customer_id, total_amount) "
            "SELECT id, 10.00 FROM app.customers WHERE name LIKE 'mdatomic-c-%'",
        ],
        one_transaction=True,
    )

    observations: list[tuple[int, int]] = []
    stop = threading.Event()

    def _observe():
        con = duckdb.connect(dsn)
        try:
            while not stop.is_set():
                try:
                    con.execute(REFRESH)
                    row = con.execute(
                        f'SELECT (SELECT count(*) FROM "{dataset}"."cdcflight_app_customers" '
                        "        WHERE name LIKE 'mdatomic-c-%'), "
                        f'       (SELECT count(*) FROM "{dataset}"."cdcflight_app_orders" o '
                        f'        WHERE EXISTS (SELECT 1 FROM "{dataset}"."cdcflight_app_customers" c '
                        "                      WHERE c.id = o.customer_id "
                        "                        AND c.name LIKE 'mdatomic-c-%'))"
                    ).fetchone()
                    observations.append((int(row[0]), int(row[1])))
                except duckdb.Error:
                    observations.append((0, 0))  # tables not created yet
                stop.wait(0.25)
        finally:
            con.close()

    watcher = threading.Thread(target=_observe, name="md-observer", daemon=True)
    watcher.start()
    try:
        streamed = sandbox.run(
            destination="motherduck", max_seconds=400, idle_seconds=10,
            timeout=700, extra_env=env,
        )
    finally:
        time.sleep(1.0)
        stop.set()
        watcher.join(timeout=15)

    con = duckdb.connect(dsn)
    con.execute(REFRESH)
    try:
        yield {
            "box": sandbox,
            "con": con,
            "database": database,
            "control_schema": control_schema,
            "dataset": dataset,
            "streamed": streamed,
            "observations": observations,
            "n": N,
        }
    finally:
        con.close()


@pytest.fixture
def md_case(motherduck_case):
    return motherduck_case


def test_scenario_reached_motherduck(md_observed_txn):
    """Guard: without this, every assertion below is vacuous.

    "The observer never saw a partial transaction" is trivially true of an
    observer that never saw anything, so the guard has to establish that it was
    genuinely watching **across** the commit: it must have sampled the state
    before the transaction landed AND the state after it landed.
    """
    con, dataset, n = md_observed_txn["con"], md_observed_txn["dataset"], md_observed_txn["n"]
    landed = con.execute(
        f'SELECT count(*) FROM "{dataset}"."cdcflight_app_customers" '
        "WHERE name LIKE 'mdatomic-c-%'"
    ).fetchone()[0]
    assert landed == n, md_observed_txn["streamed"]
    observations = md_observed_txn["observations"]
    assert observations, "the observer never sampled MotherDuck"
    assert (0, 0) in observations, (
        "the observer never saw the pre-transaction state, so it started too late "
        f"to prove anything: {observations[:20]}"
    )
    assert (n, n) in observations, (
        "the observer never saw the committed transaction, so it was reading a "
        f"stale catalog and every atomicity assertion would pass vacuously: "
        f"{observations[:20]}"
    )



def test_target_no_observer_ever_sees_a_partial_transaction(md_observed_txn):
    """TARGET BEHAVIOUR (now met) - the actual proof of rubric 1.3.

    Every observation from an independent MotherDuck connection must be either
    "the transaction is not there yet" or "the whole transaction is there".
    Nothing in between may ever be visible, in either table.
    """
    n = md_observed_txn["n"]
    torn = [pair for pair in md_observed_txn["observations"] if pair not in {(0, 0), (n, n)}]
    assert not torn, (
        f"{len(torn)} observations saw a partial Postgres transaction in MotherDuck, "
        f"e.g. {torn[:10]}"
    )


def test_target_one_commit_group_per_pg_transaction_in_motherduck(md_observed_txn):
    """TARGET BEHAVIOUR (now met) - and the metadata agrees with what the observer saw."""
    con, dataset = md_observed_txn["con"], md_observed_txn["dataset"]
    commits = con.execute(
        f'SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_customers" '
        "WHERE name LIKE 'mdatomic-c-%' "
        f'UNION SELECT DISTINCT cdcf_commit_id FROM "{dataset}"."cdcflight_app_orders" o '
        f'WHERE EXISTS (SELECT 1 FROM "{dataset}"."cdcflight_app_customers" c '
        "              WHERE c.id = o.customer_id AND c.name LIKE 'mdatomic-c-%')"
    ).fetchall()
    assert len(commits) == 1, f"PG transaction split across {len(commits)} commit groups"


def test_motherduck_fenced_spilled_overlap_is_dropped_without_owner(md_case, md_token, tmp_path):
    """MotherDuck sees no destination owner for a discarded overlap."""
    database = md_case["database"]
    control_schema = md_case["control_schema"]
    dataset = f"cdc_overlap_{uuid.uuid4().hex[:8]}"
    pipeline = f"md_overlap_{uuid.uuid4().hex[:8]}"
    dsn = f"md:{database}?motherduck_token={md_token}"

    con = duckdb.connect(dsn)
    dest_mod.ensure_control_schema(con, control_schema)
    dest_mod.ensure_dataset(con, dataset)
    lease = Lease(pipeline, ttl_seconds=600, control_schema=control_schema)
    lease.acquire(con)
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "2",
        },
    )
    completion.observe_notification("COMPLETED", {})
    applier = Applier(
        con,
        pipeline=pipeline,
        namespace="md-overlap",
        dataset=dataset,
        topic_prefix="cdcflight",
        offset_path=tmp_path / "offsets.dat",
        resume_point=ResumePoint(),
        config=ApplierConfig(
            verify_offset_file=False,
            resnapshot=True,
            snapshot_chunk_events=1,
            unit_spill_events=2,
        ),
        lease=lease,
        runner_id="md-overlap-runner",
        completion=completion,
        # Direct lab construction has no CatalogWatcher; supply the same explicit
        # fixture descriptor authority used by the other Applier matrix tests.
        descriptor_provider=fixture_descriptors,
        control_schema=control_schema,
    )
    committer = FakeCommitter()
    applier._committer = committer

    boundary = PendingRecord(
        raw=None,
        kind=KIND_SNAPSHOT_BOUNDARY,
        topic="cdcflight.cdc_flight_snapshot_notifications",
        nbytes=0,
        lsn=201,
        source_partition={"server": "cdcflight"},
        source_offset={"lsn": 201, "lsn_proc": 201, "ts_usec": 201000},
    )

    def feed(records):
        for record in records:
            for unit in applier.assembler.feed(record):
                applier._add_unit(unit)

    try:
        feed([snap("customers", 100, ident=1, marker="true")])
        for unit in applier.assembler.feed_snapshot_boundary(boundary):
            applier._add_unit(unit)
        feed(
            [
                begin("md-overlap-txn", 300),
                keyed("md-overlap-txn", 1, 301, 2, "overlap-a"),
                keyed("md-overlap-txn", 2, 302, 3, "overlap-b"),
                end("md-overlap-txn", 2, 303, {"app.customers": 2}),
            ]
        )
        feed([snap("customers", 200, ident=2, marker="last")])
        applier.commit_group("motherduck_matrix")

        assert applier.fenced_spilled_events == 0
        assert applier.snapshot_completed is True
        assert committer.marked > 0
        verify = duckdb.connect(dsn)
        try:
            verify.execute("FORCE CHECKPOINT")
            table = f'"{dataset}"."cdcflight_app_customers"'
            assert verify.execute(f"SELECT id, name FROM {table} ORDER BY id").fetchall() == [
                (1, "s"),
                (2, "s"),
            ]
            assert verify.execute(
                f'SELECT count(*) FROM "{control_schema}"."spill_events"'
            ).fetchone()[0] == 0
            assert verify.execute(
                f'SELECT count(*), sum(fenced_units) FROM "{control_schema}"."commit_log" '
                "WHERE pipeline = ?",
                [pipeline],
            ).fetchone() == (1, 0)
        finally:
            verify.close()
    finally:
        applier.drain_on_shutdown()
        lease.release(con)
        applier.alerts.close()
        con.close()
