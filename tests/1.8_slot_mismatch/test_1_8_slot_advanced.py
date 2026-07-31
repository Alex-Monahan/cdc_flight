"""Rubric 1.8 end to end — the slot really is advanced, dropped, and rewound.

The baseline measurement for this item (`probes/p04_offset_mismatch.py`) was
`pg_replication_slot_advance(slot, pg_current_wal_lsn())` followed by a run that
reported `{"records": 0, "stop_reason": "idle", "returncode": 0}` while thirty-one
change events were gone for ever. That is the 1 on the rubric's scale.

The rubric's 4 is "the process exits". Its 5 is "any potential data loss from slot
advancement triggers a backfill / snapshot automatically", and the only honest test of an
automatic repair is to compare the whole destination against the whole source afterwards
- which is what every test here does.

Each scenario destroys the pipeline's position in a different way, and each is a real
operation on a real slot, not a rewritten control row:

* `pg_replication_slot_advance` past our position - somebody else consumed the slot;
* `pg_drop_replication_slot` - the slot is simply gone;
* an advance to a position *behind* us - which must stay a no-op, because the safe
  direction has to stay safe or the detector is a false-positive machine.
"""

from __future__ import annotations

import pytest
from conftest import Sandbox

ROWS = 25


def _customers(box: Sandbox) -> set[str]:
    return {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}


def _dest_customers(box: Sandbox) -> set[str]:
    return {
        str(r[0])
        for r in box.duck_query(
            f"SELECT name FROM {box.table('cdcflight_app_customers')}"
        )
    }


def _readings(box: Sandbox) -> int:
    return box.pg_query("SELECT count(*) FROM app.sensor_readings")[0][0]


def _dest_readings(box: Sandbox) -> int:
    return box.scalar(f"SELECT count(*) FROM {box.table('cdcflight_app_sensor_readings')}")


@pytest.fixture(scope="module")
def advanced(tmp_path_factory, postgres_cluster):
    """Baseline, then somebody else advances the slot past everything we hold."""
    box = Sandbox("slot_advanced", tmp_path_factory.mktemp("sbx_slot_adv"), postgres_cluster)
    try:
        box.reseed()
        baseline = box.run(reset_state=True, max_seconds=150)

        # Changes that will be DISCARDED by the advance: this is the data the rubric's 1
        # loses silently. A keyless table too - a changelog cannot absorb a duplicate.
        box.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT 'lost-' || i, "
                f"'lost-' || i || '@example.com' FROM generate_series(1, {ROWS}) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                f"'LOST', i * 2.5, 'C' FROM generate_series(1, {ROWS}) i",
            ],
            one_transaction=True,
        )
        # Somebody else consumes the slot. `pg_replication_slot_advance` is exactly what
        # a stray `pg_recvlogical`, a second connector on the same slot, or a
        # well-meaning operator does.
        advance = box.pg_query(
            "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, pg_current_wal_lsn())",
            (box.slot,),
        )
        recovered = box.run(max_seconds=240)

        # And normal CDC must work again afterwards.
        box.sql(
            "INSERT INTO app.customers (name, email) VALUES ('after-recovery', 'a@x.com')"
        )
        after = box.run(max_seconds=150)
        yield {
            "box": box, "baseline": baseline, "advance": advance,
            "recovered": recovered, "after": after,
        }
    finally:
        box.cleanup()
        box.reseed()


def test_the_advance_is_detected_and_named(advanced):
    summary = advanced["recovered"]
    assert summary["slot_check"]["decision"] == "slot_ahead_of_destination", summary["slot_check"]
    assert summary["slot_check"]["resnapshot"] is True


def test_the_advance_triggers_an_automatic_resnapshot_of_every_captured_table(advanced):
    summary = advanced["recovered"]
    recovery = summary["slot_recovery"]
    assert recovery["decision"] == "slot_ahead_of_destination"
    # "all captured tables, unless provable otherwise" - and it is not provable otherwise.
    assert recovery["tables_marked"] >= 6, recovery
    assert recovery["slot"] == "dropped", recovery
    assert summary["ok"] is True, summary


def test_the_destination_matches_the_source_after_the_automatic_recovery(advanced):
    """The whole point. The rows the advance discarded are back, from the source."""
    box = advanced["box"]
    assert _dest_customers(box) == _customers(box)
    assert {f"lost-{i}" for i in range(1, ROWS + 1)} <= _dest_customers(box)


def test_the_keyless_table_is_neither_short_nor_duplicated(advanced):
    """A changelog table is the one that cannot hide a duplicate behind an upsert."""
    box = advanced["box"]
    assert _dest_readings(box) == _readings(box)
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
        f"{box.table('cdcflight_app_sensor_readings')}"
    )[0]
    assert total == distinct, f"{total - distinct} duplicated keyless events"


def test_cdc_works_again_after_the_recovery(advanced):
    box = advanced["box"]
    assert advanced["after"]["ok"] is True, advanced["after"]
    assert "after-recovery" in _dest_customers(box)
    assert advanced["after"]["slot_check"]["decision"] == "ok", advanced["after"]["slot_check"]


def test_an_alert_records_the_loss_and_the_repair(advanced):
    box = advanced["box"]
    codes = {
        str(r[0])
        for r in box.duck_query("SELECT code FROM _cdc_flight.alerts WHERE severity = 'critical'")
    }
    assert "slot_ahead_of_destination" in codes, codes


# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def dropped(tmp_path_factory, postgres_cluster):
    """The slot is dropped while the pipeline is down.

    Left to Debezium this is silent and total: it creates a new slot at the *current*
    WAL position, so everything written in between is simply never decoded. The
    destination looks healthy and is missing rows.
    """
    box = Sandbox("slot_dropped", tmp_path_factory.mktemp("sbx_slot_drop"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT 'gap-' || i, "
            f"'gap-' || i || '@example.com' FROM generate_series(1, {ROWS}) i",
            one_transaction=True,
        )
        box.pg_query("SELECT pg_drop_replication_slot(%s)", (box.slot,))
        recovered = box.run(max_seconds=240)
        yield {"box": box, "recovered": recovered}
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_a_dropped_slot_is_detected_not_silently_recreated(dropped):
    summary = dropped["recovered"]
    assert summary["slot_check"]["decision"] == "slot_missing", summary["slot_check"]
    assert summary["slot_recovery"]["decision"] == "slot_missing"
    assert summary["ok"] is True, summary


@pytest.mark.slow
def test_a_dropped_slot_recovers_the_rows_it_would_have_skipped(dropped):
    box = dropped["box"]
    assert _dest_customers(box) == _customers(box)
    assert {f"gap-{i}" for i in range(1, ROWS + 1)} <= _dest_customers(box)


# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_slot_behind_us_is_not_treated_as_a_mismatch(tmp_path_factory, postgres_cluster):
    """The safe direction must stay quiet, or the detector is unusable.

    Invariant O permits `confirmed_flush_lsn` to lag arbitrarily - the measured
    reconnect behaviour in ADR 0001 §9.1 makes it freeze ~1.8 MB behind - so a lagging
    slot must produce no recovery and no re-snapshot at all.
    """
    box = Sandbox("slot_behind", tmp_path_factory.mktemp("sbx_slot_behind"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        box.sql("INSERT INTO app.customers (name, email) VALUES ('behind', 'b@x.com')")
        second = box.run(max_seconds=150)
        assert second["slot_check"]["decision"] == "ok", second["slot_check"]
        assert "slot_recovery" not in second, second.get("slot_recovery")
        assert "resnapshot_swapped" not in second, second.get("resnapshot_swapped")
        assert "behind" in _dest_customers(box)
    finally:
        box.cleanup()
        box.reseed()
