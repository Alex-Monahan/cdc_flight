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

    sandbox.sql(
        "INSERT INTO app.customers (name, email) VALUES "
        "('after slot drop', 'after-slot-drop@example.com')"
    )
    sandbox.drop_slot()

    broken = sandbox.run(max_seconds=90, expect_success=False)
    return {"healthy": healthy, "broken": broken}


def test_healthy_run_reports_success(dropped_slot_scenario):
    """Guard rail: the failure detector must not fire on a clean shutdown."""
    healthy = dropped_slot_scenario["healthy"]
    assert healthy["returncode"] == 0, healthy
    assert healthy["records"] == 20, healthy
    assert healthy["stop_reason"] in {"idle", "engine_finished"}, healthy
    assert "error" not in healthy, healthy


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
def test_corrupt_offset_surfaces_as_a_failure(sandbox):
    """A structurally invalid offset file must also fail loudly (rubric 4.3)."""
    sandbox.reseed()
    first = sandbox.run(reset_state=True, max_seconds=150)
    assert first["returncode"] == 0

    sandbox.offset_file.write_bytes(b"this is not a serialized offset map\n")

    broken = sandbox.run(max_seconds=90, expect_success=False)
    assert broken["returncode"] != 0, broken
    assert broken.get("stop_reason") == "engine_error", broken
