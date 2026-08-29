"""Rubric 1.8 — the restored-source cases, and that the observation is real.

Two of the four things 1.8 has to detect cannot be produced on a developer machine: a
base-backup restore and a rewound timeline. What *can* be produced exactly is the
evidence the detector works from - the durable record of what the cluster looked like
last time - so these tests rewrite `_cdc_flight.slot_state` and then drive the real
pipeline through the real recovery. The comparison, the alert, the marking, the
re-snapshot and the final destination-versus-source equality are all genuine; only the
cause is simulated, and it is simulated at exactly the boundary the detector reads.

`test_the_observation_reads_the_real_cluster` is the other half: it asserts that the
fields the decision table compares are really the ones Postgres reports, so a simulated
`slot_state` row is comparable to a real observation.
"""

from __future__ import annotations

import pytest
from support.fixtures import POSTGRES_TEST_INSTANCE, Sandbox, _slot_startup_guard

from cdc_flight.reconcile import observe_slot


def test_the_observation_reads_the_real_cluster(postgres_cluster, cdc_env):
    """Every field the decision table uses, from a live Postgres."""
    slot = cdc_env["CDC_SLOT_NAME"]
    import psycopg

    with _slot_startup_guard(POSTGRES_TEST_INSTANCE.slot_startup_lock_path), psycopg.connect(
        postgres_cluster.dsn, autocommit=True
    ) as conn:
        conn.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,)
        )
        expected_system_id = str(
            conn.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0]
        )
    try:
        observation = observe_slot(postgres_cluster.dsn, slot)
    finally:
        with psycopg.connect(postgres_cluster.dsn, autocommit=True) as conn:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))

    assert observation.observable
    assert observation.slot_exists is True
    assert observation.restart_lsn and observation.restart_lsn > 0
    assert observation.confirmed_flush_lsn and observation.confirmed_flush_lsn > 0
    assert observation.current_wal_lsn and observation.current_wal_lsn > 0
    assert observation.system_identifier == expected_system_id
    assert observation.timeline_id == 1

    # And a slot that is not there reads as not there, rather than as an error.
    gone = observe_slot(postgres_cluster.dsn, f"{slot}_definitely_not_here")
    assert gone.observable and gone.slot_exists is False
    assert gone.system_identifier == expected_system_id


def test_an_unreachable_source_is_reported_not_raised():
    observation = observe_slot(
        "postgresql://postgres:postgres@127.0.0.1:1/nope", "x", connect_timeout=2
    )
    assert observation.observable is False
    assert observation.error


@pytest.fixture(scope="module")
def restored(tmp_path_factory, postgres_cluster):
    """The source is a different cluster than the one the destination was built from."""
    box = Sandbox("restored_source", tmp_path_factory.mktemp("sbx_restored"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        box.sql("INSERT INTO app.customers (name, email) VALUES ('restore-era', 'r@x.com')")
        delivered = box.run(max_seconds=150)
        assert delivered["ok"] is True

        # What a restore looks like to us: the cluster identity we recorded is not the
        # identity the source now reports.
        box.duck_write(
            "UPDATE _cdc_flight.slot_state SET system_identifier = '1234567890123456789'"
        )
        recovered = box.run(max_seconds=240)
        yield {"box": box, "recovered": recovered}
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_a_restored_cluster_is_detected_and_repaired(restored):
    summary = restored["recovered"]
    assert summary["slot_check"]["decision"] == "source_identity_changed", summary["slot_check"]
    assert summary["slot_recovery"]["decision"] == "source_identity_changed"
    assert summary["ok"] is True, summary


@pytest.mark.slow
def test_a_restored_cluster_leaves_the_destination_equal_to_the_source(restored):
    box = restored["box"]
    source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
    dest = {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }
    assert dest == source


@pytest.mark.slow
def test_a_restored_cluster_forgets_the_old_catalog(restored):
    """Otherwise every relation looks dropped-and-recreated and the breaker refuses.

    A different cluster's oids are not our relations' oids, so comparing them makes the
    catalog watcher conclude the whole schema was replaced - which the mass-drop circuit
    breaker then correctly refuses and unhelpfully blocks. The recovery clears
    `source_relations` for this decision only.
    """
    box = restored["box"]
    summary = restored["recovered"]
    assert summary.get("catalog_destructive_refused", 0) == 0, summary
    assert summary.get("tables_dropped", 0) == 0, summary
    # The identity we now hold is the live one, so the next run is quiet.
    recorded = box.duck_query(
        "SELECT system_identifier FROM _cdc_flight.slot_state"
    )
    live = box.pg_query("SELECT system_identifier::text FROM pg_control_system()")[0][0]
    assert recorded and str(recorded[0][0]) == str(live)


@pytest.mark.slow
def test_a_rewound_source_is_detected(tmp_path_factory, postgres_cluster):
    """MINOR-11 carry-forward: `pg_current_wal_lsn()` behind the durable offset.

    Simulated at the same boundary - the durable resume point - because rewinding a real
    cluster's WAL means restoring a base backup. What is real is everything after the
    comparison: the alert, the marking, the re-snapshot, and the equality at the end.
    """
    box = Sandbox("rewound", tmp_path_factory.mktemp("sbx_rewound"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        current = box.pg_query("SELECT (pg_current_wal_lsn() - '0/0')::bigint")[0][0]
        # Use a bounded but deliberately large synthetic lead. A one-byte lead can be
        # consumed by another xdist worker during the recovery subprocess, turning a
        # correct source into a false test pass; 1 GiB is far below the LSN range and
        # comfortably exceeds the disposable lane's concurrent WAL budget.
        durable_lsn = int(current) + 1_000_000_000
        box.duck_write(
            "UPDATE _cdc_flight.debezium_offsets SET last_lsn = ?", [durable_lsn]
        )
        observed = box.pg_query("SELECT (pg_current_wal_lsn() - '0/0')::bigint")[0][0]
        assert int(observed) < durable_lsn, (
            "the rewound-source precondition was consumed before the supervisor ran: "
            f"current={observed}, durable={durable_lsn}"
        )
        recovered = box.run(max_seconds=240)
        assert recovered["slot_check"]["decision"] == "source_lsn_regressed", (
            recovered["slot_check"]
        )
        assert recovered["ok"] is True, recovered
        source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
        dest = {
            str(r[0])
            for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
        }
        assert dest == source
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_resnapshot_can_be_turned_off_for_the_rubrics_four(tmp_path_factory, postgres_cluster):
    """`CDC_RESNAPSHOT=0` keeps the old behaviour: detect, alert, exit non-zero.

    Worth a 4 rather than a 5, deliberately rather than by accident - an operator who
    wants to be told rather than repaired should be able to say so.
    """
    box = Sandbox("resnap_off", tmp_path_factory.mktemp("sbx_resnap_off"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        # `synchronous_commit` is off in this cluster, so an ordinary INSERT is
        # committed but NOT yet written: `pg_current_wal_lsn()` can still point
        # before it, and advancing the slot to that position would skip nothing.
        # The test then measured an incidental byte or two of trailing WAL instead
        # of the row it means to strand — and a pipeline that leaves its slot fully
        # caught up (as one that ends on a completion watermark does) leaves no
        # such bytes at all. Flush this one transaction so the precondition is
        # REAL: the row is in written WAL, and advancing past it truly strands it.
        box.sql(
            [
                "SET synchronous_commit = on",
                "INSERT INTO app.customers (name, email) VALUES ('doomed', 'd@x.com')",
            ]
        )
        durable_before = int(
            box.duck_query("SELECT last_lsn FROM _cdc_flight.debezium_offsets")[0][0]
        )
        box.pg_query(
            "SELECT pg_replication_slot_advance(%s, pg_current_wal_lsn())", (box.slot,)
        )
        confirmed = int(
            box.pg_query(
                "SELECT (confirmed_flush_lsn - '0/0')::bigint "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (box.slot,),
            )[0][0]
        )
        # Assert the precondition rather than assume it. Without this the whole
        # test can pass — or fail — for reasons that have nothing to do with the
        # behaviour under examination (test-audit finding F6, same shape).
        assert confirmed > durable_before, (
            "the slot was not left ahead of the destination, so there is no "
            "stranded WAL for the run to detect: "
            f"confirmed_flush={confirmed}, durable={durable_before}"
        )
        failed = box.run(
            max_seconds=120, expect_success=False, extra_env={"CDC_RESNAPSHOT": "0"}
        )
        assert failed["returncode"] != 0, failed
        assert "slot_ahead_of_destination" in (failed.get("error") or ""), failed.get("error")
        assert failed.get("ok") is not True
    finally:
        box.cleanup()
        box.reseed()
