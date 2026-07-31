"""Rubric 1.7 / 1.1 — the interleavings the default matrix does not have room for.

`test_1_1_fault_matrix.py` walks six anchors with a hard exit inside the 10-minute
default budget. The reviews asked for more than that (Codex 6, Opus M-4): the
`decode` anchor, the `raise` action that drives Debezium's **error teardown** (L3)
rather than process death, a crash during the **snapshot** phase and its swap, a
replay of a **spilled** transaction, and a genuinely failed **offset flush**. Each
of those is a crash/recovery cycle of ~40 s, so they live here under `slow`.

Every scenario asserts the same two things the matrix does — the destination holds
exactly the change events the source produced, and all of them — measured on the
keyless changelog, where a merge on a primary key cannot absorb a second delivery.
"""

from __future__ import annotations

import os
import stat

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
READINGS = '"cdc_raw"."cdcflight_app_sensor_readings"'
N = 12

pytestmark = pytest.mark.slow


def _write_batch(box, tag: str, n: int = N) -> None:
    box.sql(
        [
            "INSERT INTO app.customers (name, email) SELECT "
            f"'{tag}-c-' || i, '{tag}-c-' || i || '@example.com' "
            f"FROM generate_series(1, {n}) i",
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
            f"'{tag.upper()}', i * 3.5, 'C' FROM generate_series(1, {n}) i",
        ],
        one_transaction=True,
    )


def _assert_exactly_once(box, tag: str, n: int = N) -> None:
    missing = box.scalar(
        f"SELECT count(*) FROM generate_series(1, {n}) g(i) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {CUSTOMERS} c WHERE c.name = '{tag}-c-' || g.i)"
    )
    assert missing == 0, f"{missing} of {n} keyed rows were lost"
    events, unique = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {READINGS} "
        f"WHERE sensor_id = '{tag.upper()}'"
    )[0]
    assert events == n, f"the changelog holds {events} change events, the source produced {n}"
    assert unique == n, f"{n - unique} change events were applied more than once"


@pytest.fixture(scope="module")
def seeded(sandbox):
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=200, timeout=400)
    return sandbox


# --------------------------------------------------------------------------- #
# the two anchors and the action the default matrix leaves out
# --------------------------------------------------------------------------- #
def test_a_crash_at_decode_loses_nothing(seeded):
    """`decode` - records assembled, no destination transaction open at all."""
    box = seeded
    _write_batch(box, "il-decode")
    crashed = box.run(
        max_seconds=200, expect_success=False, extra_env={"CDC_FAULT_INJECT": "decode:1"}
    )
    assert crashed["returncode"] == 137, crashed
    box.run(max_seconds=200)
    _assert_exactly_once(box, "il-decode")


def test_an_error_teardown_before_the_commit_loses_nothing(seeded):
    """The `raise` action: Debezium's L3 path, not process death.

    `stopSourceTasks()` calls `commitOffsets()` itself during teardown, so this is
    the lifecycle path that could flush an offset the destination never committed.
    Invariant O is what closes it — the offset store holds nothing for this group.
    """
    box = seeded
    _write_batch(box, "il-raise-pre")
    failed = box.run(
        max_seconds=200,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": "pre_commit:1:raise"},
    )
    assert failed["returncode"] != 0, failed
    box.run(max_seconds=200)
    _assert_exactly_once(box, "il-raise-pre")


def test_an_error_teardown_after_the_commit_does_not_duplicate(seeded):
    """The same path with the destination already committed: the group must not
    be applied twice when the engine restarts."""
    box = seeded
    _write_batch(box, "il-raise-post")
    failed = box.run(
        max_seconds=200,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": "post_commit_pre_ack:1:raise"},
    )
    assert failed["returncode"] != 0, failed
    box.run(max_seconds=200)
    _assert_exactly_once(box, "il-raise-post")


def test_a_between_table_crash_and_a_spilled_replay_together(seeded):
    """`mid_apply` with spill forced: table A written, table B not, and the unit's
    prefix already staged. The recovery run replays a *spilled* transaction, so the
    fence has to suppress its staged prefix as well as its in-memory tail."""
    box = seeded
    spill = {"CDC_UNIT_SPILL_EVENTS": "4"}
    _write_batch(box, "il-mid-spill")
    crashed = box.run(
        max_seconds=200,
        expect_success=False,
        extra_env={**spill, "CDC_FAULT_INJECT": "mid_apply:1"},
    )
    assert crashed["returncode"] == 137, crashed
    recovered = box.run(max_seconds=200, extra_env=spill)
    assert recovered["spilled_events"] > 0, (
        f"the spill never fired on the recovery run; the test is vacuous: {recovered}"
    )
    _assert_exactly_once(box, "il-mid-spill")


# --------------------------------------------------------------------------- #
# the snapshot phase (Opus M-4: no anchor test ever entered it)
# --------------------------------------------------------------------------- #
def test_a_crash_during_the_snapshot_phase_leaves_no_partial_table(sandbox):
    """A snapshot commit group crashes before `COMMIT`; the shadow table and its
    `table_state` row roll back with it, and the re-snapshot is clean.

    ADR §3.5's idempotency claim is that a crash mid-snapshot is safe *because the
    shadow is dropped and rebuilt*, not because of event identity. This is that
    claim, executed.
    """
    box = sandbox
    box.reseed()
    crashed = box.run(
        reset_state=True,
        max_seconds=200,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": "pre_commit:1"},
    )
    assert crashed["returncode"] == 137, crashed

    recovered = box.run(max_seconds=200, timeout=400)
    assert recovered["returncode"] == 0, recovered
    # No shadow table survived, and the live tables carry the whole snapshot.
    shadows = box.duck_query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'cdc_raw' AND table_name LIKE '%__cdcf_tmp'"
    )
    assert shadows == [], f"a shadow table outlived the crash: {shadows}"
    source_rows = box.pg_query("SELECT count(*) FROM app.customers")[0][0]
    landed = box.scalar(f"SELECT count(*) FROM {CUSTOMERS}")
    assert landed == source_rows, (
        f"the re-snapshot landed {landed} rows, the source has {source_rows}"
    )
    dupes = box.scalar(
        f"SELECT count(*) FROM (SELECT cdcf_event_id FROM {CUSTOMERS} "
        "GROUP BY 1 HAVING count(*) > 1)"
    )
    assert dupes == 0, f"{dupes} snapshot identities collided across the re-snapshot"


# --------------------------------------------------------------------------- #
# a genuinely failed offset flush (Opus B2's canary, end to end)
# --------------------------------------------------------------------------- #
def test_an_offset_flush_that_cannot_happen_fails_the_run_without_losing_data(seeded):
    """Debezium swallows every non-timeout flush failure and returns normally.

    Making `offsets.dat` read-only is the cheapest real version of that. The run
    must fail (it is the canary for a broken offset store) and must lose nothing:
    under Invariant O the acknowledgement is after the commit, so a failed flush
    can only cause a replay, which the fence then drops.
    """
    box = seeded
    _write_batch(box, "il-flush")
    original = box.offset_file.stat().st_mode
    box.offset_file.chmod(stat.S_IRUSR)
    try:
        failed = box.run(max_seconds=200, expect_success=False)
    finally:
        os.chmod(box.offset_file, original)

    assert failed["returncode"] != 0, (
        f"a flush that could not happen was reported as success: {failed}"
    )
    assert "offset" in (failed.get("error", "") + failed.get("output", "")).lower()

    recovered = box.run(max_seconds=200)
    assert recovered["returncode"] == 0, recovered
    _assert_exactly_once(box, "il-flush")
