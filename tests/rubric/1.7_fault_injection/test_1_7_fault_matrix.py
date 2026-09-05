"""Rubric 1.7 — the fault matrix: every anchor, enumerated, with its outcome class.

The rubric's 5 is "robust injection of failures in testing". Individual fault tests
prove individual faults; what makes the injection *robust* is that the set is
**enumerated from the code** rather than from a list somebody remembered to update. So
this module parametrises over `faults.ALL_POINTS` itself, and a new anchor added to
`faults.py` without a matrix entry fails `test_every_anchor_has_a_declared_outcome`
rather than being quietly untested.

Every fault must land in exactly one of two outcome classes, and the class is
**derived from what the run did**, not read back out of the table:

* `RECOVERS` — the run exits zero and the ledger is intact.
* `LOUD` — the run exits non-zero, `last_run.json` is accurate, the anchor that fired is
  the anchor that was armed, and a following run makes the destination equal the source
  exactly. (Every `LOUD` case is also asserted to recover, because a loud failure that
  cannot be recovered from is not much better than a silent one.)
* `SILENT` — anything else. The table declares no `SILENT` anchor, and
  `_observed_outcome()` is what makes that a *finding* rather than a spelling
  convention: the old `test_no_anchor_is_allowed_to_be_silent` asserted that a
  hand-written dict contained no `SILENT` string, which could only fail if somebody
  edited the dict (Opus MINOR-1, "it is the test the SILENT-bucket claim points at").

Two things were vacuous here and are not any more (Codex M2):

* the run was accepted as loud on `returncode != 0` alone, without establishing that the
  **selected** fault had fired — a start-up failure for an unrelated reason passed;
* recovery was asserted with counts against a tag, but nothing named the anchor in the
  summary, so a scenario that quietly stopped firing looked identical to one that fired
  and recovered.

Both are fixed by `faults.record_fired()`: every anchor writes `fault_fired.json` into
the run's state directory before it does anything else, fsynced, so even `os._exit`
leaves the evidence.

The default suite runs a representative of each *mechanism and each outcome shape*;
`-m slow` runs the whole matrix.
"""

from __future__ import annotations

import pytest
from support.fixtures import Sandbox

from cdc_flight import faults

RECOVERS = "recovers"
LOUD = "loud"
SILENT = "silent"

#: anchor -> (outcome class, what the fault means, `<nth>` to use)
#:
MATRIX: dict[str, tuple[str, str]] = {
    "decode": (LOUD, "the process dies after decoding a batch, before any transaction"),
    "begin": (LOUD, "the process dies with BEGIN issued and nothing applied"),
    "spill": (LOUD, "the process dies with a unit's events staged in spill_events"),
    "mid_apply": (LOUD, "the process dies between two tables of one transaction"),
    "pre_commit": (LOUD, "the process dies with everything written and not committed"),
    "post_commit_pre_ack": (
        LOUD, "the process dies committed but unacknowledged - the at-least-once window"
    ),
    "post_ack": (LOUD, "the process dies acknowledged but with the slot unconfirmed"),
    "swap": (LOUD, "the process dies between the DROP and the RENAME of a swap"),
    "destination_write": (LOUD, "the destination rejects a data write mid-transaction"),
    "destination_commit": (LOUD, "COMMIT raises before the statement runs"),
    "destination_commit_late": (
        LOUD, "COMMIT EXECUTES and then raises - the genuinely ambiguous case"
    ),
    "destination_hang": (LOUD, "COMMIT never returns - bounded by CDC_COMMIT_TIMEOUT"),
    "destination_close": (LOUD, "the destination connection is severed mid-transaction"),
    # The acquisition-recovery anchors (rubric 1.7's closure, landed with 1.9's state
    # machines). Like `swap` they need a *scenario* the generic one cannot produce - a
    # slot the check declares unusable - so they are declared here and asserted
    # elsewhere: `tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py` guards all
    # five in the DEFAULT lane in milliseconds, and
    # `tests/rubric/1.8_slot_mismatch/test_1_8_recovery_crash_e2e.py` kills a real process at
    # `recovery_armed` against a real Postgres slot under `-m slow`.
    "recovery_requested": (
        LOUD, "the process dies with the journal and the to-do list durable and nothing destroyed"
    ),
    "recovery_offsets_file_deleted": (
        LOUD, "the process dies with offsets.dat gone and the journal still at `requested`"
    ),
    "recovery_resume_point_deleted": (
        LOUD, "the process dies with the durable resume point gone and the slot still there"
    ),
    "recovery_armed": (
        LOUD, "the process dies with the main slot retained and the journal not yet recording it"
    ),
    "table_rebuild_queued": (
        LOUD, "the process dies while the durable to-do list is being written"
    ),
    # rev 14, rubric 1.9's catalog-baseline machine. The first two are crash cuts
    # across its new edges; `catalog_poll` is not a crash at all but a *degraded
    # dependency*, and it is here because round 5 had to monkeypatch it to reproduce
    # a consistency defect the suite could not express (Codex r5 BLOCKER-1).
    "catalog_baseline_marked": (
        LOUD, "the process dies with the baseline durably unconfirmed and nothing else done"
    ),
    "catalog_baseline_pre_valid": (
        LOUD, "the process dies with the learned relations flushed and the promotion unwritten"
    ),
    "catalog_poll": (
        LOUD,
        "EVERY source-catalog poll fails, so the run reads the catalog zero times: it "
        "must not report success, and it must leave a durable record that no baseline "
        "was established",
    ),
}

#: Anchors whose scenario is owned by another module, with the module named. Absent
#: from the generic parametrisation below and NOT absent from `MATRIX`: an anchor with
#: no declared outcome is what `test_every_anchor_has_a_declared_outcome` exists to
#: catch, and quietly dropping one from the table would be the same hole through a
#: different door.
ELSEWHERE = {
    "swap": "tests/rubric/1.6_snapshot_consistency/ (needs a shadow table to swap)",
    "recovery_requested": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    "recovery_offsets_file_deleted": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    "recovery_resume_point_deleted": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    "recovery_armed": (
        "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py (default) + "
        "tests/rubric/1.8_slot_mismatch/test_1_8_recovery_crash_e2e.py (slow, real process)"
    ),
    "table_rebuild_queued": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    "catalog_baseline_marked": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    "catalog_baseline_pre_valid": "tests/rubric/1.7_fault_injection/test_1_7_recovery_anchors.py",
    # A repeating fault, not a crash: the generic matrix asserts a process death and
    # this one asserts a whole run's worth of unreadable catalog, plus what the NEXT
    # run does with the record it left. Both halves live where the composition does.
    "catalog_poll": (
        "tests/rubric/1.9_state_machines/test_1_9_catalog_baseline.py (default, in process) + "
        "tests/rubric/1.9_state_machines/test_1_9_catalog_baseline_e2e.py (slow, real cluster)"
    ),
}

#: What the default suite guards. One per *behaviour class*, not one per mechanism
#: (Opus Q5): a hard process death at the commit boundary (`post_commit_pre_ack`, the
#: at-least-once window and the most dangerous anchor in the protocol), a destination
#: that rejects a write (`destination_write`), and a destination that committed and
#: could not say so (`destination_commit_late`). 10 of 12 anchors used to be slow-only,
#: which put the guard for the machinery this branch added outside the gate that runs.
DEFAULT_SUITE = {"post_commit_pre_ack", "destination_write", "destination_commit_late"}

#: Extra environment some anchors need before they can fire at all.
#:
#: MEASURED while writing this file: `spill` is unreachable at shipped defaults for any
#: workload a test suite would use (500 000 events / 64 MB per unit), so it fired never
#: and the case passed as "the run succeeded" — which is exactly the vacuously-green
#: shape the `<nth>` validation exists to prevent, arriving through a different door. An
#: anchor whose arming depends on the workload has to say what workload arms it.
ARMING: dict[str, dict[str, str]] = {
    "spill": {"CDC_UNIT_SPILL_EVENTS": "5", "CDC_UNIT_SPILL_BYTES": "512"},
    # The hang duration is now its own variable rather than `<action>` reinterpreted as
    # seconds, so this says what it means: hang for longer than the watchdog, and wind
    # the watchdog down so the test does not take five minutes.
    "destination_hang": {"CDC_COMMIT_TIMEOUT": "5", "CDC_FAULT_HANG_SECONDS": "600"},
}

#: The exit code an anchor is required to produce. `EX_TEMPFAIL` for the commit
#: watchdog, because that is the whole point of it; the injector's own code for a hard
#: process death; 1 for anything that unwinds through `main()`'s handler.
EX_TEMPFAIL = 75
EXPECTED_EXIT: dict[str, int] = {
    "destination_hang": EX_TEMPFAIL,
}

ROWS = 30


def test_every_anchor_has_a_declared_outcome():
    """The guard that makes this file a matrix rather than a list."""
    missing = sorted(set(faults.ALL_POINTS) - set(MATRIX))
    assert not missing, (
        f"fault anchor(s) {missing} exist in cdc_flight.faults with no declared outcome "
        "class. Add them to MATRIX and give them a scenario, or rubric 1.7's "
        "'robust injection' is a claim about the anchors somebody remembered."
    )
    extra = sorted(set(MATRIX) - set(faults.ALL_POINTS))
    assert not extra, f"MATRIX names anchor(s) that do not exist: {extra}"


def test_every_anchor_this_file_does_not_run_names_where_it_is_run():
    """An anchor excluded from the generic scenario must say who proves it.

    `swap` was excluded with a comment; the recovery anchors would have been excluded
    the same way. A comment is not a guard: if the module it names is deleted or
    renamed, nothing fails. The path is asserted to exist.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    for point, where in ELSEWHERE.items():
        assert point in MATRIX, point
        first = where.split(" ")[0]
        assert (root / first).exists(), f"{point} points at {first}, which does not exist"


def test_the_declared_outcome_classes_are_the_ones_this_file_can_observe():
    """A declared class nothing can observe is a class nothing is proving.

    This replaces `test_no_anchor_is_allowed_to_be_silent`, which asserted that a
    hand-written dict contained no `SILENT` value and could therefore only fail if
    somebody edited the dict (Opus MINOR-1). The emptiness of the SILENT bucket is now
    established by `_observed_outcome()` in every scenario below, which *derives* the
    class from the run and fails if it is `SILENT`.
    """
    declared = {outcome for outcome, _ in MATRIX.values()}
    assert declared <= {RECOVERS, LOUD}
    assert SILENT not in declared


@pytest.fixture(scope="module")
def matrix_box(tmp_path_factory, postgres_cluster):
    box = Sandbox("fault_matrix", tmp_path_factory.mktemp("sbx_matrix"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        yield box
    finally:
        box.cleanup()
        box.reseed()


def _params():
    out = []
    for point, (outcome, description) in sorted(MATRIX.items()):
        if point in ELSEWHERE:
            continue  # asserted by the module named in ELSEWHERE
        marks = [] if point in DEFAULT_SUITE else [pytest.mark.slow]
        out.append(pytest.param(point, outcome, description, marks=marks, id=point))
    return out


def _observed_outcome(box: Sandbox, point: str, failed: dict) -> tuple[str, str]:
    """DERIVE the outcome class from the run. Returns `(class, why)`.

    The classification is:

    * the armed anchor did not fire at all -> `SILENT`, whatever the exit code says. A
      run that dies of an unrelated start-up error is not evidence about this anchor;
    * it fired and the run exited zero -> `RECOVERS`;
    * it fired and the run exited non-zero -> `LOUD`.
    """
    fired = box.fired_fault()
    if fired is None or fired.get("point") != point:
        return SILENT, (
            f"the armed anchor {point!r} left no fired record (saw {fired!r}); the run "
            f"ended rc={failed['returncode']} for some other reason, so this scenario "
            "proves nothing about the anchor it names"
        )
    if failed["returncode"] == 0 and failed.get("ok") is True:
        return RECOVERS, "the anchor fired and the run still finished cleanly"
    return LOUD, f"the anchor fired and the run exited {failed['returncode']}"


@pytest.mark.parametrize(("point", "outcome", "description"), _params())
def test_the_fault_lands_in_its_declared_outcome_class(matrix_box, point, outcome, description):
    box = matrix_box
    tag = f"mx{point.replace('_', '')}"
    box.clear_fired_fault()
    box.sql(
        "INSERT INTO app.customers (name, email) SELECT "
        f"'{tag}-' || i, '{tag}-' || i || '@example.com' "
        f"FROM generate_series(1, {ROWS}) i",
        one_transaction=True,
    )
    # A keyless table in the same transaction, so "no duplication" is checked where an
    # upsert cannot hide it.
    box.sql(
        "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
        f"'{tag}', i * 1.5, 'C' FROM generate_series(1, {ROWS}) i",
        one_transaction=True,
    )
    env = {"CDC_FAULT_INJECT": f"{point}:1", **ARMING.get(point, {})}
    failed = box.run(max_seconds=120, timeout=200, expect_success=False, extra_env=env)

    observed, why = _observed_outcome(box, point, failed)
    assert observed != SILENT, f"{point} ({description}): {why}"
    assert observed == outcome, (
        f"{point} ({description}) was declared {outcome} and behaved {observed}: {why}"
    )
    assert failed.get("ok") is not True, failed
    expected_exit = EXPECTED_EXIT.get(point)
    if expected_exit is not None:
        assert failed["returncode"] == expected_exit, (
            f"{point} must exit exactly {expected_exit}, not {failed['returncode']}"
        )

    recovered = box.run(max_seconds=200)
    assert recovered["ok"] is True, recovered
    _assert_ledger_intact(box, tag)


def _assert_ledger_intact(box: Sandbox, tag: str) -> None:
    """The source's own counts are the ledger; the destination must match them.

    EXACT counts on both sides, never "at least": `>=` cannot see a duplicate and `!= 0`
    cannot see a short delivery, and both are what a recovery test exists to catch.
    """
    src_customers = box.pg_query(
        "SELECT count(*) FROM app.customers WHERE name LIKE %s", (f"{tag}-%",)
    )[0][0]
    dst_customers = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} WHERE name LIKE ?",
        [f"{tag}-%"],
    )
    assert dst_customers == src_customers == ROWS, (dst_customers, src_customers)

    src_readings = box.pg_query(
        "SELECT count(*) FROM app.sensor_readings WHERE sensor_id = %s", (tag,)
    )[0][0]
    dst_readings = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_sensor_readings')} "
        "WHERE sensor_id = ?",
        [tag],
    )
    assert dst_readings == src_readings == ROWS, (dst_readings, src_readings)

    for table in ("cdcflight_app_customers", "cdcflight_app_sensor_readings"):
        total, distinct = box.duck_query(
            f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {box.table(table)}"
        )[0]
        assert total == distinct, f"{table}: {total - distinct} duplicated change events"
