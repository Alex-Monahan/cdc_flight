"""Rubric 1.1 — start-up reconciliation, and the fence that makes it optional.

ADR 0001 §4.5 says `offsets.dat` is never a source of truth. Two properties have
to hold and they pull in opposite directions, so both are tested:

1. **The repair works.** A file that lags, leads, is corrupt or is missing is
   rebuilt from `_cdc_flight.debezium_offsets`, byte-compatibly with Kafka's
   `FileOffsetBackingStore`, so Debezium resumes exactly where the destination
   says.
2. **Correctness does not depend on the repair.** With
   `CDC_OFFSET_FILE_REPAIR=0` the file is left alone, Debezium replays whatever
   it covers, and the applier's unit-level fence drops the replay. If this test
   ever failed, the exactly-once claim would have quietly become an ordering
   argument again - which is the exact thing ADR revision 2 exists to remove.

And the case that must **refuse to start**: a file with no matching destination
row. That file may be arbitrarily *ahead* of anything durable, so trusting it is
silent loss - and it is what happens the first time the applier meets a machine
that already has a baseline `.cdc_state/offsets.dat`.
"""

from __future__ import annotations

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
READINGS = '"cdc_raw"."cdcflight_app_sensor_readings"'
N = 15


@pytest.fixture(scope="module")
def seeded(sandbox) -> dict:
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)
    return {"box": sandbox}


def _write_batch(box, tag: str, n: int = N) -> None:
    box.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            f"'{tag}-c-' || i, '{tag}-c-' || i || '@example.com' "
            f"FROM generate_series(1, {n}) i",
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
            f"'{tag.upper()}', i * 2.25, 'C' FROM generate_series(1, {n}) i",
        ],
        one_transaction=True,
    )


def test_orphan_offsets_file_refuses_to_start(sandbox, tmp_path_factory):
    """The load-bearing row of ADR 0001 §4.5's decision table.

    An `offsets.dat` with no destination row is pointed at a *different*
    destination database here - the same shape as pointing a fresh MotherDuck
    database at a machine that already has offsets. It must refuse, loudly,
    rather than resume from a position nothing has ever committed.
    """
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)
    assert sandbox.offset_file.exists()

    other_db = tmp_path_factory.mktemp("orphan") / "other.duckdb"
    refused = sandbox.run(
        max_seconds=90,
        expect_success=False,
        extra_env={"CDC_DUCKDB_PATH": str(other_db)},
    )
    assert refused["returncode"] != 0, refused
    assert "REFUSING TO START" in refused["output"], refused["output"][-2000:]
    assert "orphan" in refused["output"].lower()

    # ... and the documented escape hatch deletes the file instead of trusting it.
    accepted = sandbox.run(
        max_seconds=150,
        extra_env={"CDC_DUCKDB_PATH": str(other_db)},
        accept_orphan_offsets=True,
    )
    assert accepted["reconciliation"] == "orphan_accepted_resnapshot", accepted


def test_offsets_file_is_rebuilt_when_it_goes_missing(seeded):
    """`absent file / present row` - resume from the destination, do NOT re-snapshot."""
    box = seeded["box"]
    _write_batch(box, "recon-missing")
    box.run(max_seconds=150)
    before = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = 'RECON-MISSING'")
    assert before == N

    box.offset_file.unlink()
    recovered = box.run(max_seconds=150)
    assert recovered["reconciliation"] == "file_missing_rebuilt", recovered
    assert box.offset_file.exists(), "the offsets file was not rebuilt"
    after = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = 'RECON-MISSING'")
    assert after == before, (
        "rebuilding the offsets file from the destination re-delivered "
        f"{after - before} events"
    )
    assert recovered["applied_events"] == 0


def test_a_corrupt_offsets_file_is_rebuilt_not_trusted(seeded):
    """A crash during `FileOffsetBackingStore.save()` leaves a truncated file."""
    box = seeded["box"]
    _write_batch(box, "recon-corrupt")
    box.run(max_seconds=150)
    before = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = 'RECON-CORRUPT'")
    assert before == N

    raw = box.offset_file.read_bytes()
    box.offset_file.write_bytes(raw[: len(raw) // 2])
    recovered = box.run(max_seconds=150)
    assert recovered["reconciliation"] == "file_corrupt_rebuilt", recovered
    after = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = 'RECON-CORRUPT'")
    assert after == before


def test_the_fence_alone_prevents_duplication_with_repair_disabled(seeded):
    """Correctness must not depend on the offsets-file repair.

    This is the crash matrix's F6 row with the safety net removed: crash at
    `post_commit_pre_ack` (destination committed, Debezium not acknowledged) and
    then restart with `CDC_OFFSET_FILE_REPAIR=0`, so `offsets.dat` keeps pointing
    *before* the committed transaction and Debezium genuinely re-delivers it.
    The applier's unit-level fence (ADR §4.4) must drop every re-delivered unit.

    Why this shape and not "roll the offsets file back by hand": Postgres will not
    stream from before the slot's `restart_lsn`, so a hand-rolled file usually
    replays nothing and the test would be vacuous (measured while writing the 1.1
    gap tests). After a `post_commit_pre_ack` crash the slot was never confirmed
    past the batch either, so the replay is real.
    """
    box = seeded["box"]
    no_repair = {"CDC_OFFSET_FILE_REPAIR": "0"}

    _write_batch(box, "fence")
    crashed = box.run(
        max_seconds=150,
        expect_success=False,
        extra_env={**no_repair, "CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
    )
    assert crashed["returncode"] == 137, crashed

    replayed = box.run(max_seconds=150, extra_env=no_repair)
    assert replayed["reconciliation"] == "file_behind_rebuilt", replayed
    assert replayed["records"] > 0, (
        "Postgres re-delivered nothing, so the fence was never exercised: "
        f"{replayed}"
    )
    assert replayed["fenced_units"] > 0, (
        "events were re-delivered but no unit was fenced; without the fence the "
        f"applier would have applied them twice: {replayed}"
    )
    assert replayed["applied_events"] == 0, replayed

    assert box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = 'FENCE'") == N, (
        "a re-delivered transaction was applied a second time to the keyless table"
    )
    keyed, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE name LIKE 'fence-c-%'"
    )[0]
    assert (keyed, distinct) == (N, N)
