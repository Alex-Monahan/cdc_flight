"""Rubric 1.8 — every cell of the slot-mismatch decision table, as a unit test.

`reconcile.check_slot` is a pure function of (durable offset, one observation, the
previous observation) precisely so that this file can exist. Two of the four cases
1.8 has to detect cannot otherwise be produced on a developer machine at all - a
base-backup restore and a rewound timeline - and a rubric item whose interesting
cases are untestable is a rubric item nobody will keep working.

The pairing with reality is in `test_1_8_slot_advanced.py` (a genuinely advanced
slot, a genuinely dropped one) and in `test_1_8_observation.py` (the observation
really does read those fields out of Postgres).
"""

from __future__ import annotations

import pytest

from cdc_flight.reconcile import RESNAPSHOT_DECISIONS, SlotObservation, check_slot


def obs(**kwargs) -> SlotObservation:
    base = {
        "slot_exists": True,
        "active": True,
        "restart_lsn": 1_000,
        "confirmed_flush_lsn": 1_000,
        "current_wal_lsn": 2_000,
        "system_identifier": "7000000000000000001",
        "timeline_id": 1,
    }
    return SlotObservation(**{**base, **kwargs})


def test_a_healthy_slot_is_ok():
    verdict = check_slot(durable_lsn=1_000, observation=obs(), previous=None)
    assert verdict.decision == "ok"
    assert verdict.ok is True
    assert verdict.resnapshot is False


def test_a_slot_behind_the_destination_is_ok():
    """The safe direction. Invariant O allows the slot to lag arbitrarily."""
    verdict = check_slot(
        durable_lsn=5_000,
        observation=obs(confirmed_flush_lsn=1_000, current_wal_lsn=6_000),
        previous=None,
    )
    assert verdict.decision == "ok"


def test_an_externally_advanced_slot_triggers_a_resnapshot():
    verdict = check_slot(
        durable_lsn=1_000, observation=obs(confirmed_flush_lsn=9_000), previous=None
    )
    assert verdict.decision == "slot_ahead_of_destination"
    assert verdict.resnapshot is True
    assert verdict.ok is False
    assert "9000" in verdict.message and "1000" in verdict.message


def test_a_missing_slot_triggers_a_resnapshot():
    """A new slot starts at the CURRENT WAL position, so the gap is silent."""
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(slot_exists=False, active=False, restart_lsn=None,
                        confirmed_flush_lsn=None),
        previous=None,
    )
    assert verdict.decision == "slot_missing"
    assert verdict.resnapshot is True


def test_an_invalidated_slot_requires_local_repair_before_a_full_resnapshot():
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(wal_status="lost", invalidation_reason="rows_removed"),
        previous=None,
    )
    assert verdict.decision == "slot_invalidated"
    assert verdict.resnapshot is True
    assert "local standby slot" in verdict.message
    assert "primary logical slot is never a fallback" in verdict.message


def test_a_recreated_slot_is_caught_by_the_previous_observation():
    """Same name, ordinary position, but BOTH positions went backwards."""
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(restart_lsn=500, confirmed_flush_lsn=500),
        previous={
            "restart_lsn": 900,
            "confirmed_flush_lsn": 950,
            "system_identifier": "7000000000000000001",
        },
    )
    assert verdict.decision == "slot_recreated"
    assert verdict.resnapshot is True


def test_a_checkpoint_artefact_is_not_a_recreated_slot():
    """Opus MINOR-4: `restart_lsn` alone has a false-positive mode.

    A logical slot's `restart_lsn` is only persisted at checkpoint, so after an unclean
    Postgres restart the recovered value can be behind the one we recorded — on a slot
    that is perfectly healthy. Firing there rebuilds every captured table, which
    replaces a keyless changelog with an image: a worse outcome than the case being
    detected. A genuinely recreated slot cannot have confirmed a position we never saw
    it reach, so the confirmed position is the discriminator.
    """
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(restart_lsn=500, confirmed_flush_lsn=1_000),
        previous={
            "restart_lsn": 900,
            "confirmed_flush_lsn": 900,
            "system_identifier": "7000000000000000001",
        },
    )
    assert verdict.decision == "ok", verdict.message


def test_a_recreated_slot_is_invisible_without_a_previous_observation():
    """Stated so the limit of the detector is on the record, not assumed away.

    With no `slot_state` row (a first run, or a destination restored without it) a slot
    recreated at a position *behind* the durable offset looks like an ordinary lagging
    slot, which is the safe direction and genuinely indistinguishable. What makes that
    acceptable is that the WAL such a slot holds still starts before our position, so
    the stream replays rather than skips - the events are re-delivered and the fence
    drops them.
    """
    verdict = check_slot(
        durable_lsn=1_000, observation=obs(restart_lsn=500, confirmed_flush_lsn=500),
        previous=None,
    )
    assert verdict.decision == "ok"


def test_a_restored_cluster_is_named_as_such():
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(system_identifier="7999999999999999999"),
        previous={"system_identifier": "7000000000000000001", "restart_lsn": 900},
    )
    assert verdict.decision == "source_identity_changed"
    assert verdict.resnapshot is True


def test_a_rewound_source_is_detected_without_a_previous_observation():
    """MINOR-11 carry-forward: a backward LSN jump used to be fenced, not detected."""
    verdict = check_slot(
        durable_lsn=9_000,
        observation=obs(current_wal_lsn=4_000, restart_lsn=3_900, confirmed_flush_lsn=3_900),
        previous=None,
    )
    assert verdict.decision == "source_lsn_regressed"
    assert verdict.resnapshot is True


def test_the_cause_is_reported_not_the_symptom():
    """A restored cluster shows a regressed LSN *and* a recreated slot."""
    verdict = check_slot(
        durable_lsn=9_000,
        observation=obs(
            system_identifier="7999999999999999999", current_wal_lsn=4_000, restart_lsn=100
        ),
        previous={"system_identifier": "7000000000000000001", "restart_lsn": 8_000},
    )
    assert verdict.decision == "source_identity_changed"


def test_no_durable_row_with_a_positioned_slot_and_an_EMPTY_destination_rebuilds():
    """Rebuilding an empty destination destroys nothing, so it is automatic."""
    verdict = check_slot(
        durable_lsn=None, observation=obs(), previous=None, destination_rows={}
    )
    assert verdict.decision == "no_durable_destination_row"
    assert verdict.resnapshot is True
    assert verdict.refuse is False


def test_no_durable_row_with_a_POPULATED_destination_refuses(caplog):
    """Opus BLOCKER-2, reproduced: this cell rebuilt a healthy warehouse.

    Both ADR §19/A50 and RUBRIC_STATUS describe the cell as "destination **empty**,
    slot positioned". The code tested only that `_cdc_flight.debezium_offsets` had no
    row and never looked at the destination, so a state directory pointed at an
    existing warehouse - `--reset-state`, a restore without `_cdc_flight`, a wrong DSN -
    silently dropped and rebuilt every captured table. The `offsets.dat` refusal that
    was supposed to be the guard is a fact about a FILE, not about the destination.

    This is a safety REGRESSION against `main`, where the same cell raised.
    """
    verdict = check_slot(
        durable_lsn=None,
        observation=obs(),
        previous=None,
        destination_rows={"app.customers": 5, "app.orders": 12},
    )
    assert verdict.decision == "no_durable_destination_row"
    assert verdict.refuse is True, "a populated destination must not be auto-rebuilt"
    assert verdict.resnapshot is False
    assert verdict.ok is False
    assert "app.customers (5 rows)" in verdict.message


def test_a_destination_that_was_not_inspected_is_treated_as_populated():
    """`None` is "I did not look", which is never a licence to destroy."""
    verdict = check_slot(
        durable_lsn=None, observation=obs(), previous=None, destination_rows=None
    )
    assert verdict.refuse is True
    assert verdict.resnapshot is False


def test_a_forked_timeline_is_detected(caplog):
    """Codex B5, reproduced: the same system id + a new timeline returned `ok`.

    `timeline_id` was persisted in `slot_state` from the first cut and never read.
    Postgres REUSES WAL positions across a fork, so every scalar comparison in the
    table - current position, restart position, confirmed position - can look perfectly
    healthy while the destination's offset names a point on a history that no longer
    exists.
    """
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(timeline_id=2),
        previous={"system_identifier": "7000000000000000001", "timeline_id": 1,
                  "restart_lsn": 900},
    )
    assert verdict.decision == "source_timeline_changed"
    assert verdict.resnapshot is True
    assert verdict.ok is False
    assert "1" in verdict.message and "2" in verdict.message


def test_an_unchanged_timeline_is_not_a_fork():
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(timeline_id=1),
        previous={"system_identifier": "7000000000000000001", "timeline_id": 1,
                  "restart_lsn": 900},
    )
    assert verdict.decision == "ok"


def test_a_changed_identity_outranks_a_changed_timeline():
    """A restore changes both; "this is a different cluster" is the useful sentence."""
    verdict = check_slot(
        durable_lsn=1_000,
        observation=obs(system_identifier="7999999999999999999", timeline_id=3),
        previous={"system_identifier": "7000000000000000001", "timeline_id": 1,
                  "restart_lsn": 900},
    )
    assert verdict.decision == "source_identity_changed"


def test_a_forked_timeline_invalidates_the_recorded_catalog():
    """Codex M1: a fork can keep the system id and change every relation oid."""
    from cdc_flight.reconcile import FORGET_CATALOG_DECISIONS

    assert "source_timeline_changed" in FORGET_CATALOG_DECISIONS
    assert "source_lsn_regressed" in FORGET_CATALOG_DECISIONS
    assert "source_identity_changed" in FORGET_CATALOG_DECISIONS


def test_nothing_anywhere_is_a_fresh_start():
    verdict = check_slot(
        durable_lsn=None,
        observation=obs(slot_exists=False, active=False, restart_lsn=None,
                        confirmed_flush_lsn=None),
        previous=None,
    )
    assert verdict.decision == "fresh_start"
    assert verdict.ok is True
    assert verdict.resnapshot is False


def test_an_unobservable_source_is_not_ok_and_not_a_resnapshot():
    """We cannot conclude loss, and we certainly cannot snapshot from it."""
    verdict = check_slot(
        durable_lsn=1_000,
        observation=SlotObservation(error="OperationalError: timeout"),
        previous=None,
    )
    assert verdict.decision == "source_unobservable"
    assert verdict.ok is False
    assert verdict.resnapshot is False


@pytest.mark.parametrize("decision", RESNAPSHOT_DECISIONS)
def test_every_resnapshot_decision_is_reachable(decision):
    """Guard against a decision name that only exists in the tuple."""
    cases = {
        "slot_ahead_of_destination": (1_000, obs(confirmed_flush_lsn=9_000), None),
        "slot_missing": (
            1_000,
            obs(slot_exists=False, active=False, restart_lsn=None, confirmed_flush_lsn=None),
            None,
        ),
        "slot_invalidated": (
            1_000,
            obs(wal_status="lost", invalidation_reason="wal_removed"),
            None,
        ),
        "slot_recreated": (
            1_000,
            obs(restart_lsn=500, confirmed_flush_lsn=500),
            {"restart_lsn": 900, "confirmed_flush_lsn": 950},
        ),
        "source_identity_changed": (
            1_000, obs(system_identifier="9"), {"system_identifier": "1"}
        ),
        "source_lsn_regressed": (9_000, obs(current_wal_lsn=4_000), None),
        "source_timeline_changed": (
            1_000, obs(timeline_id=7), {"timeline_id": 1}
        ),
        "no_durable_destination_row": (None, obs(), None),
    }
    durable, observation, previous = cases[decision]
    verdict = check_slot(
        durable_lsn=durable,
        observation=observation,
        previous=previous,
        # Every one of these decisions is safe to automate; the destination-emptiness
        # input only gates `no_durable_destination_row`.
        destination_rows={},
    )
    assert verdict.decision == decision
    assert verdict.resnapshot is True
