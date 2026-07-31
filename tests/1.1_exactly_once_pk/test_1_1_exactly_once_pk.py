"""Rubric 1.1 - exactly-once delivery for tables WITH a primary key.

See README.md for the failure mode and the test conventions.

Review note (Opus M6 / Codex 8), and the reason the TARGET tests below look the
way they do: for a primary-keyed table, `count(*) == 50 AND count(DISTINCT id)
== 50` is satisfied by any implementation that merges on `id`, **even if the
same change event was delivered and applied twice**. Row-shape assertions
therefore cannot distinguish exactly-once *delivery* from idempotent
*application*. The assertions here are consequently made against an **event
ledger**: how many change events the source produced versus how many the
destination holds. The baseline destination is an append-only changelog, so the
row count *is* the event count today; once ADR 0001 D8 splits current-state from
changelog, these tests point at the changelog and keep meaning the same thing.
"""

from __future__ import annotations

import signal
import time

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
REPLAY_FILTER = "name LIKE 'replay-c-%'"



def test_scenario_crashed_after_commit_and_recovered(crash_replay):
    """Guard: without this every assertion below is vacuous.

    The fault fires at `post_commit_pre_ack` - the destination transaction has
    COMMITTED and Debezium has NOT been acknowledged - and the process dies with
    137. That is the exact window a `kill -9` hits.

    Note what this test does NOT assert any more. Before the applier it asserted
    `replayed["records"] > 0`, i.e. that the crash caused a replay. Under
    Invariant O there is nothing to replay: the resume point went into the same
    MotherDuck transaction as the rows, so start-up reconciliation rebuilds
    `offsets.dat` from it and the connector resumes *after* the committed batch.
    "No replay happened" is the improvement, so it cannot also be the guard. The
    guard is now: the fault really fired, the restart really succeeded, and the
    committed rows really are there.
    """
    crashed, replayed = crash_replay["crashed"], crash_replay["replayed"]
    assert crashed["returncode"] == 137, crashed
    assert replayed["returncode"] == 0, replayed
    assert replayed["reconciliation"] in {"file_behind_rebuilt", "resume"}, replayed
    box = crash_replay["box"]
    n = crash_replay["customers"]
    landed = box.scalar(f"SELECT count(*) FROM {CUSTOMERS} WHERE {REPLAY_FILTER}")
    assert landed == n, (
        f"the crashed run committed {n} customers before dying; the destination "
        f"holds {landed}"
    )





def test_no_rows_are_lost(crash_replay):
    """Regression guard: at-least-once must never decay into at-most-once."""
    box = crash_replay["box"]
    n = crash_replay["customers"]
    missing = box.duck_query(
        f"SELECT count(*) FROM generate_series(1, {n}) g(i) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {CUSTOMERS} c "
        "WHERE c.name = 'replay-c-' || g.i)"
    )[0][0]
    assert missing == 0, f"{missing} source rows never reached the destination"


def test_target_change_event_ledger_balances(crash_replay):
    """TARGET BEHAVIOUR (now met) - the destination holds exactly the events the source produced.

    THE assertion for 1.1, and the one a PK merge cannot fake: the source
    produced `customers` INSERT events in the replayed transaction, so the
    destination must hold exactly that many change events for them. A merge on
    `id` collapses a duplicate delivery into one row and would silently pass a
    row-count test; it cannot pass an event-count test against an append-only
    changelog.
    """
    box = crash_replay["box"]
    expected = crash_replay["customers"]
    events = box.scalar(
        f"SELECT count(*) FROM {CUSTOMERS} WHERE {REPLAY_FILTER} AND dbz_op = 'c'"
    )
    assert events == expected, (
        f"source produced {expected} INSERT events, destination holds {events} "
        "change events for them"
    )


def test_target_exactly_once_pk(crash_replay):
    """TARGET BEHAVIOUR (now met) - each source change event lands exactly once."""
    box = crash_replay["box"]
    n = crash_replay["customers"]
    rows, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE {REPLAY_FILTER}"
    )[0]
    assert rows == n, f"expected exactly {n} rows, got {rows}"
    assert distinct == n


def test_target_no_duplicate_change_events(crash_replay):
    """TARGET BEHAVIOUR (now met) - (lsn, table, key) identifies a change event uniquely."""
    box = crash_replay["box"]
    dupes = box.duck_query(
        f"SELECT count(*) FROM (SELECT dbz_lsn, id FROM {CUSTOMERS} "
        f"WHERE {REPLAY_FILTER} GROUP BY 1, 2 HAVING count(*) > 1)"
    )[0][0]
    assert dupes == 0, f"{dupes} change events delivered more than once"


def test_target_slot_never_outruns_the_destination(crash_replay):
    """TARGET BEHAVIOUR (now met) - ADR 0001 §4.7's Invariant-O guard.

    `slot.confirmed_flush_lsn <= debezium_offsets.last_lsn` is the ONLY detector
    for the class of bug that produced ADR revision 2 (a lifecycle path
    confirming an LSN the destination never committed). It must be sampled at
    every observed moment, and it is asserted here - after a crash and a restart,
    which is when it would first be violated.

    Under Invariant O this should be unfalsifiable. It is asserted anyway, and
    after a crash and a restart specifically, because that is the moment a
    lifecycle path that confirmed an LSN the destination never committed would
    show itself.
    """
    box = crash_replay["box"]
    rows = box.duck_query(
        "SELECT last_lsn FROM _cdc_flight.debezium_offsets LIMIT 1"
    )
    assert rows, "_cdc_flight.debezium_offsets has no row for this pipeline"
    durable_lsn = rows[0][0]
    confirmed = box.pg_query(
        "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    assert confirmed, f"slot {box.slot} is gone"
    assert confirmed[0][0] <= durable_lsn, (
        f"slot confirmed_flush_lsn={confirmed[0][0]} is AHEAD of the durable "
        f"destination offset {durable_lsn}: data in between is unrecoverable"
    )


@pytest.mark.slow
def test_slow_real_sigkill_is_exactly_once(sandbox):
    """The un-simulated fault: a real `kill -9` mid-load, then a restart.

    REWRITTEN when the applier landed, in two ways that both make it stronger.

    **1. It asserts exactly-once, not just no-loss.** The old version asserted only
    "nothing was lost", because whether a SIGKILL duplicated depended on where it
    landed relative to the offset flush, and that race is not winnable reliably
    (`probes/p07` lost it at 60 k rows, `probes/p13` won it at 400 k). Under
    Invariant O the outcome no longer depends on where the kill lands: whatever the
    destination committed is durable *together with* the resume point, and whatever
    it did not is replayed. So `total == distinct == rows` is a legitimate
    assertion for an un-simulated kill, and it is the strongest statement this test
    can make.

    **2. The workload is many transactions, not one.** 200 000 rows in a single
    Postgres transaction is a single commit group, so *any* kill before the end
    replays everything and the interesting case - a kill after some groups have
    committed and before others - never occurs. 40 transactions of 5 000 rows
    produce many commit groups, so the kill genuinely lands between them.

    The kill is timed off the replication slot becoming active rather than a fixed
    sleep: the applier now streams 200 000 rows in ~30 s, and the old 35 s sleep
    let the run finish before the signal arrived (which is how this rewrite
    started).
    """
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=200, idle_seconds=8, timeout=400)

    txns, per_txn = 40, 5_000
    rows = txns * per_txn
    for t in range(txns):
        sandbox.sql(
            "INSERT INTO app.customers (name, email) SELECT "
            "'kill9-' || i, 'kill9-' || i || '@example.com' "
            f"FROM generate_series({t * per_txn + 1}, {(t + 1) * per_txn}) i",
            one_transaction=True,
        )

    proc = sandbox.spawn(max_seconds=400, idle_seconds=10)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        time.sleep(1)
        if sandbox.pg_query(
            "SELECT active FROM pg_replication_slots WHERE slot_name = %s AND active",
            (sandbox.slot,),
        ):
            break
    time.sleep(8)  # long enough that several commit groups have gone through
    assert proc.poll() is None, (
        "the run finished before the SIGKILL could land, so the test would be "
        "vacuous; raise the workload"
    )
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=60)
    assert proc.returncode != 0, "process survived SIGKILL"

    recovered = sandbox.run(max_seconds=400, idle_seconds=10, timeout=600)
    total, distinct = sandbox.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE name LIKE 'kill9-%'"
    )[0]
    assert distinct == rows, f"rows LOST across a SIGKILL: {rows - distinct} of {rows}"
    assert total == rows, (
        f"{total - rows} DUPLICATE rows survived a SIGKILL; under Invariant O a crash "
        "can only replay what the destination never committed"
    )
    print(
        f"\nSIGKILL mid-stream over {txns} transactions: {total} rows / {distinct} "
        f"distinct => 0 duplicates, 0 lost (recovery re-applied "
        f"{recovered['applied_events']} events)"
    )

    durable = sandbox.scalar("SELECT last_lsn FROM _cdc_flight.debezium_offsets LIMIT 1")
    confirmed = sandbox.pg_query(
        "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots WHERE slot_name = %s",
        (sandbox.slot,),
    )
    assert confirmed[0][0] <= durable, (
        f"after a real SIGKILL the slot confirmed {confirmed[0][0]}, ahead of the "
        f"durable destination offset {durable}"
    )
