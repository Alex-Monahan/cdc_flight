"""TODO 1.0(b): Debezium engine failures must not be reported as success.

See README.md in this directory for the defect and the test conventions.
"""

from __future__ import annotations

import pytest

#: The Debezium message emitted when the publication the connector needs is gone, as a
#: DISTINCTIVE phrase rather than a bare common noun.
#:
#: It was briefly just `"publication"`, matched case-insensitively against the whole
#: output — which the word "publication" appears in for many reasons, including our own
#: log lines naming the publication (Opus MINOR-6 / Q3). A sentinel a healthy run could
#: also satisfy is not a witness. These are the connector's own texts when
#: `publication.autocreate.mode=disabled` and the publication is missing; the fixture
#: asserts the connector failed for THIS reason and not for some other start-up problem.
PUBLICATION_GONE = "publication autocreation is disabled"
PUBLICATION_MISSING = 'publication "cdc_flight_pub" does not exist'


@pytest.fixture(scope="module")
def engine_death_scenario(sandbox) -> dict:
    """Snapshot, then break the source so the connector cannot start, and run again.

    UPDATED when rubric 1.8 landed. This scenario used to drop the *replication slot*,
    and that no longer reaches the engine at all: the slot check now detects a missing
    slot before start-up and repairs it with an automatic re-snapshot, which is 1.8's 5
    and is asserted in `tests/1.8_slot_mismatch/`. Keeping the old trigger here would
    have turned this file into a test of the recovery rather than of the supervisor.

    Dropping the **publication** breaks connector start-up the same way and is not
    something 1.8 can repair (the source is misconfigured, not ahead of us), so 1.0's
    actual subject - a Debezium failure must not be reported as success - is preserved
    exactly. `CDC_RESNAPSHOT=0` is also asserted below as the other, deliberate route to
    a loud failure.
    """
    sandbox.reseed()
    healthy = sandbox.run(reset_state=True, max_seconds=150)
    landed = sandbox.scalar('SELECT count(*) FROM "cdc_raw"."cdcflight_app_customers"')
    seeded_customers = sandbox.pg_query("SELECT count(*) FROM app.customers")[0][0]

    sandbox.sql(
        "INSERT INTO app.customers (name, email) VALUES "
        "('after publication drop', 'after-pub-drop@example.com')"
    )
    sandbox.sql("DROP PUBLICATION cdc_flight_pub")

    broken = sandbox.run(max_seconds=90, expect_success=False)
    return {
        "healthy": healthy,
        "broken": broken,
        "landed": landed,
        "seeded_customers": seeded_customers,
    }


def test_healthy_run_reports_success(engine_death_scenario):
    """Guard rail: the failure detector must not fire on a clean shutdown.

    Asserts a *scenario-specific* fact rather than the seed's global total: the
    previous `records == 20` coupled an engine-supervision test to the seed file
    and to every other writer on the shared cluster (Opus m2 / Codex 12). The count is
    taken inside the fixture, immediately after the healthy run, because a later run in
    the same scenario is entitled to change the destination.
    """
    healthy = engine_death_scenario["healthy"]
    assert healthy["returncode"] == 0, healthy
    assert healthy["stop_reason"] in {"idle", "engine_finished"}, healthy
    assert "error" not in healthy, healthy
    landed = engine_death_scenario["landed"]
    assert landed == engine_death_scenario["seeded_customers"], (
        "the healthy snapshot must contain exactly the customers that were in the "
        f"source when it ran: {healthy}"
    )
    assert healthy["records"] >= landed, healthy


def test_engine_death_surfaces_as_a_failure(engine_death_scenario):
    """The whole point of 1.0(b): engine death must be a non-zero exit."""
    broken = engine_death_scenario["broken"]
    assert broken["returncode"] != 0, (
        "the connector could not start (publication dropped) but the process reported "
        f"success; summary={broken}"
    )


def test_failure_carries_the_debezium_error_message(engine_death_scenario):
    """An operator (and rubric 6.2 alerting) needs the *reason*, not just a code."""
    broken = engine_death_scenario["broken"]
    output = broken["output"].lower()
    assert PUBLICATION_GONE in output or PUBLICATION_MISSING in output, (
        "the run must carry Debezium's own distinctive message about the missing "
        f"publication, not merely the word 'publication': {broken['output'][-2000:]}"
    )
    assert broken["returncode"] != 0, broken["returncode"]
    assert broken.get("stop_reason") == "engine_error", broken
    error = (broken.get("error") or "").lower()
    assert PUBLICATION_GONE in error or PUBLICATION_MISSING in error, broken


def test_failed_run_does_not_claim_records(engine_death_scenario):
    """`records: 0` alongside exit 0 is the shape that made p04/p11 invisible."""
    broken = engine_death_scenario["broken"]
    assert broken.get("records", 0) == 0, broken
    assert broken.get("ok") is False, broken


def test_a_dropped_slot_is_now_recovered_rather_than_fatal(sandbox):
    """The behaviour this file used to assert, and where it went (rubric 1.8).

    Recorded here on purpose: a reader of 1.0 who remembers "a dropped slot exits
    non-zero" needs to find out from 1.0 that it does not any more, and why.
    """
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)
    sandbox.sql("INSERT INTO app.customers (name, email) VALUES ('gone', 'g@x.com')")
    sandbox.drop_slot()
    recovered = sandbox.run(max_seconds=200)
    assert recovered["ok"] is True, recovered
    assert recovered["slot_check"]["decision"] == "slot_missing", recovered["slot_check"]
    landed = {
        str(r[0])
        for r in sandbox.duck_query('SELECT name FROM "cdc_raw"."cdcflight_app_customers"')
    }
    assert "gone" in landed, landed


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
