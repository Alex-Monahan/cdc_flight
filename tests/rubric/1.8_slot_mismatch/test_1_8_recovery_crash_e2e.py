"""Rubric 1.7/1.8 — a REAL process killed inside the acquisition recovery.

The pairing for `tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py`. That file
proves each anchor is reachable and that the journal it leaves is resumable, in
milliseconds, with an injectable slot drop; this one kills an actual `cdc-flight`
process with `os._exit` at the anchor, against a real Postgres slot, and then checks the
whole destination against the whole source.

**Why the pair, and not just the fast one.** The honest hold that kept 1.7 at 4 was that
the acquisition-recovery crash cuts were proven through a *test seam* — `resume(
on_phase=...)` raising a Python exception — and a raised exception is not a crash: it
unwinds `finally` blocks, closes the destination connection, flushes the JVM. `os._exit`
does none of that. The claim under test is that durable state alone is enough, and only
a process that dies without running any of its own cleanup tests that claim.

`recovery_armed` is the anchor chosen for the slow lane because it is the dangerous one:
the replication slot has been dropped and the journal does not yet record it. Before the
journal existed, a crash exactly there lost the forced `snapshot.mode='initial'` — it
lived only in a local variable — and the next run saw no row, no file and no slot and
called it an ordinary fresh start, streaming onto tables that were never rebuilt
(Codex B3).

Slow lane: it needs a baseline snapshot, a real slot advance, a killed run and two more
runs, and it costs ~2 minutes.
"""

from __future__ import annotations

import pytest
from support.fixtures import Sandbox

ROWS = 20


@pytest.fixture(scope="module")
def crashed_recovery(tmp_path_factory, postgres_cluster):
    """Advance the slot, then die at `recovery_armed`, then let the next run finish it."""
    box = Sandbox(
        "recovery_crash", tmp_path_factory.mktemp("sbx_recovery_crash"), postgres_cluster
    )
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)

        # The data an advance discards: this is what a rubric-1 tool loses silently.
        # `synchronous_commit` is off in this cluster, so the COMMIT alone does not
        # put these rows inside `pg_current_wal_lsn()` and the advance below would
        # strand nothing — leaving the whole scenario (the detection, the recovery,
        # the anchor) unreachable for a reason that has nothing to do with it.
        box.sql(
            [
                "SET synchronous_commit = on",
                "INSERT INTO app.customers (name, email) SELECT 'rc-' || i, "
                f"'rc-' || i || '@example.com' FROM generate_series(1, {ROWS}) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                f"'RC', i * 1.25, 'C' FROM generate_series(1, {ROWS}) i",
            ],
            one_transaction=True,
        )
        durable_before = int(
            box.duck_query("SELECT last_lsn FROM _cdc_flight.debezium_offsets")[0][0]
        )
        box.pg_query(
            "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, pg_current_wal_lsn())",
            (box.slot,),
        )
        confirmed = int(
            box.pg_query(
                "SELECT (confirmed_flush_lsn - '0/0')::bigint "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (box.slot,),
            )[0][0]
        )
        # The precondition, asserted rather than assumed (test-audit finding F6).
        assert confirmed > durable_before, (
            "the advance did not strand anything, so no acquisition recovery can "
            f"be armed: confirmed_flush={confirmed}, durable={durable_before}"
        )

        box.clear_fired_fault()
        killed = box.run(
            max_seconds=240,
            timeout=320,
            expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "recovery_armed:1"},
        )
        fired = box.fired_fault()
        # The next acquisition resumes from durable state alone.
        finished = box.run(max_seconds=300, timeout=400)
        yield {"box": box, "killed": killed, "fired": fired, "finished": finished}
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_the_anchor_that_fired_is_the_anchor_that_was_armed(crashed_recovery):
    """A54: an exit code cannot carry which fault produced it.

    `-9`, `137` and `1` are all "the process died", and one of them is what the harness
    does when it gives up. The fsynced record is written before `os._exit` runs.
    """
    fired = crashed_recovery["fired"]
    assert fired is not None, crashed_recovery["killed"].get("output", "")[-3000:]
    assert fired["point"] == "recovery_armed", fired
    assert crashed_recovery["killed"]["returncode"] == 137


@pytest.mark.slow
def test_the_next_run_finishes_the_recovery_from_durable_state_alone(crashed_recovery):
    summary = crashed_recovery["finished"]
    assert summary["ok"] is True, summary
    # It resumed the journal rather than diagnosing its own leftovers as a fresh problem
    # (the permanent `orphan_offset_file` refusal, Opus MAJOR-1) or as an ordinary fresh
    # start (Codex B3).
    resumed = summary.get("recovery_resumed") or {}
    assert resumed.get("resumed_from") == "resume_point_deleted", summary
    assert summary.get("recovery_cleared"), summary


@pytest.mark.slow
def test_the_destination_equals_the_source_exactly_afterwards(crashed_recovery):
    """The only honest test of an automatic repair: exact counts on both sides."""
    box: Sandbox = crashed_recovery["box"]
    src_customers = box.pg_query("SELECT count(*) FROM app.customers")[0][0]
    dst_customers = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')}"
    )
    assert dst_customers == src_customers

    # A keyless table too: a changelog cannot absorb a duplicate, and an upsert cannot
    # hide a short delivery there.
    src_readings = box.pg_query("SELECT count(*) FROM app.sensor_readings")[0][0]
    dst_readings = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_sensor_readings')}"
    )
    assert dst_readings == src_readings

    lost = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} "
        "WHERE name LIKE 'rc-%'"
    )
    assert lost == ROWS, "the rows the advance discarded were rebuilt by the snapshot"


@pytest.mark.slow
def test_no_table_is_left_owing_work(crashed_recovery):
    box: Sandbox = crashed_recovery["box"]
    owed = box.duck_query(
        "SELECT source_schema, source_table, snapshot_state FROM _cdc_flight.table_state "
        "WHERE snapshot_state <> 'complete'"
    )
    assert owed == [], owed
    assert box.duck_query("SELECT count(*) FROM _cdc_flight.recovery_state")[0][0] == 0


@pytest.mark.slow
def test_cdc_works_again_after_the_crashed_recovery(crashed_recovery):
    box: Sandbox = crashed_recovery["box"]
    box.sql("INSERT INTO app.customers (name, email) VALUES ('after-crash', 'a@x.com')")
    assert box.run(max_seconds=150)["ok"] is True
    assert box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} "
        "WHERE name = 'after-crash'"
    ) == 1
