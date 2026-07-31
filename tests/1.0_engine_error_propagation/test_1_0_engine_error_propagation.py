"""TODO 1.0(b): Debezium engine failures must not be reported as success.

See README.md in this directory for the defect and the test conventions.
"""

from __future__ import annotations

import pytest

# The Debezium message emitted when the slot no longer covers the stored offset.
SLOT_GONE = "no longer available on the server"


@pytest.fixture(scope="module")
def dropped_slot_scenario(sandbox) -> dict:
    """Snapshot, make one change, drop the slot underneath us, run again."""
    sandbox.reseed()
    healthy = sandbox.run(reset_state=True, max_seconds=150)
    seeded_customers = sandbox.pg_query("SELECT count(*) FROM app.customers")[0][0]

    sandbox.sql(
        "INSERT INTO app.customers (name, email) VALUES "
        "('after slot drop', 'after-slot-drop@example.com')"
    )
    sandbox.drop_slot()

    broken = sandbox.run(max_seconds=90, expect_success=False)
    return {"healthy": healthy, "broken": broken, "seeded_customers": seeded_customers}


def test_healthy_run_reports_success(dropped_slot_scenario, sandbox):
    """Guard rail: the failure detector must not fire on a clean shutdown.

    Asserts a *scenario-specific* fact rather than the seed's global total: the
    previous `records == 20` coupled an engine-supervision test to the seed file
    and to every other writer on the shared cluster (Opus m2 / Codex 12).
    """
    healthy = dropped_slot_scenario["healthy"]
    assert healthy["returncode"] == 0, healthy
    assert healthy["stop_reason"] in {"idle", "engine_finished"}, healthy
    assert "error" not in healthy, healthy
    landed = sandbox.scalar(
        'SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers"'
    )
    assert landed == dropped_slot_scenario["seeded_customers"], (
        "the healthy snapshot must contain exactly the customers that were in the "
        f"source when it ran: {healthy}"
    )
    assert healthy["records"] >= landed, healthy


def test_dropped_slot_surfaces_as_a_failure(dropped_slot_scenario):
    """The whole point of 1.0(b): engine death must be a non-zero exit."""
    broken = dropped_slot_scenario["broken"]
    assert broken["returncode"] != 0, (
        "engine failed to start (slot dropped) but the process reported success; "
        f"summary={broken}"
    )


def test_failure_carries_the_debezium_error_message(dropped_slot_scenario):
    """An operator (and rubric 6.2 alerting) needs the *reason*, not just a code."""
    broken = dropped_slot_scenario["broken"]
    assert SLOT_GONE in broken["output"], broken["output"][-2000:]
    assert broken.get("stop_reason") == "engine_error", broken
    assert SLOT_GONE in (broken.get("error") or ""), broken


def test_failed_run_does_not_claim_records(dropped_slot_scenario):
    """`records: 0` alongside exit 0 is the shape that made p04/p11 invisible."""
    broken = dropped_slot_scenario["broken"]
    assert broken.get("records", 0) == 0, broken
    assert broken.get("ok") is False, broken


@pytest.mark.slow
def test_corrupt_offset_is_repaired_rather_than_fatal(sandbox):
    """UPDATED when the transactional applier landed.

    This used to assert that a structurally invalid `offsets.dat` fails loudly,
    which was right when the file was the only record of where we were: reading
    garbage and carrying on would have been silent loss.

    It no longer is. Under ADR 0001 §4.5 the file is a scratch buffer and
    `_cdc_flight.debezium_offsets` is the truth, so a corrupt file has a correct
    answer - rebuild it from the destination and carry on - and failing instead
    would be a needless outage. The property that must still hold is that the
    corrupt file is never *trusted*: the run must report the repair, and it must
    not re-deliver anything.
    """
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0
    landed = sandbox.scalar('SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers"')

    sandbox.offset_file.write_bytes(b"this is not a serialized offset map\n")

    repaired = sandbox.run(max_seconds=90)
    assert repaired["reconciliation"] == "file_corrupt_rebuilt", repaired
    assert repaired["applied_events"] == 0, repaired
    assert (
        sandbox.scalar('SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers"') == landed
    )
