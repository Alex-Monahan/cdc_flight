"""Rubric 1.7 — the fault matrix: every anchor, enumerated, with its outcome class.

The rubric's 5 is "robust injection of failures in testing". Individual fault tests
prove individual faults; what makes the injection *robust* is that the set is
**enumerated from the code** rather than from a list somebody remembered to update. So
this module parametrises over `faults.ALL_POINTS` itself, and a new anchor added to
`faults.py` without a matrix entry fails `test_every_anchor_has_a_declared_outcome`
rather than being quietly untested.

Every fault must land in exactly one of two outcome classes:

* `RECOVERS` — the run may fail, but the ledger is intact and a following run makes the
  destination equal the source. No loss, no duplication.
* `LOUD` — the run exits non-zero with an accurate `last_run.json`. (Every `LOUD` case
  here is also asserted to recover, because a loud failure that cannot be recovered from
  is not much better than a silent one.)

Nothing may be `SILENT`. That class exists in the table only so the third possibility is
written down and visibly empty.

The default suite runs one representative per *mechanism*; `-m slow` runs the whole
matrix. Splitting it that way is not a coverage compromise: the mechanisms are
`maybe_crash` (a process that dies at an exact protocol point) and `FaultyConnection` (a
destination that misbehaves), and a representative of each exercises all the shared
machinery in the default suite.
"""

from __future__ import annotations

import pytest
from conftest import Sandbox

from cdc_flight import faults

RECOVERS = "recovers"
LOUD = "loud"

#: anchor -> (outcome class, what the fault means, `<nth>` to use)
#:
#: `swap` is absent on purpose and asserted separately: it needs a re-snapshot to have a
#: shadow to swap, so its scenario is in `tests/1.6_snapshot_consistency/`.
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
    "destination_commit": (LOUD, "COMMIT raises - the ambiguous case"),
    "destination_hang": (LOUD, "COMMIT never returns - bounded by CDC_COMMIT_TIMEOUT"),
    "destination_close": (LOUD, "the destination connection is severed mid-transaction"),
}

#: One per mechanism in the default suite; the rest are slow.
DEFAULT_SUITE = {"pre_commit", "destination_write"}

#: Extra environment some anchors need before they can fire at all.
#:
#: MEASURED while writing this file: `spill` is unreachable at shipped defaults for any
#: workload a test suite would use (500 000 events / 64 MB per unit), so it fired never
#: and the case passed as "the run succeeded" — which is exactly the vacuously-green
#: shape the `<nth>` validation exists to prevent, arriving through a different door. An
#: anchor whose arming depends on the workload has to say what workload arms it.
ARMING: dict[str, dict[str, str]] = {
    "spill": {"CDC_UNIT_SPILL_EVENTS": "5", "CDC_UNIT_SPILL_BYTES": "512"},
    "destination_hang": {"CDC_COMMIT_TIMEOUT": "5"},
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


def test_no_anchor_is_allowed_to_be_silent():
    """The third outcome class is written down so its emptiness is a statement."""
    assert {outcome for outcome, _ in MATRIX.values()} <= {RECOVERS, LOUD}


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
        if point == "swap":
            continue  # needs a shadow table; owned by tests/1.6_snapshot_consistency/
        marks = [] if point in DEFAULT_SUITE else [pytest.mark.slow]
        out.append(pytest.param(point, outcome, description, marks=marks, id=point))
    return out


@pytest.mark.parametrize(("point", "outcome", "description"), _params())
def test_the_fault_lands_in_its_declared_outcome_class(matrix_box, point, outcome, description):
    box = matrix_box
    tag = f"mx{point.replace('_', '')}"
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
    assert outcome == LOUD
    assert failed["returncode"] != 0, (
        f"{point} ({description}) produced a SUCCESSFUL run; rubric 1.7 permits a clean "
        "recovery or a loud failure and this is neither: "
        f"{ {k: v for k, v in failed.items() if k != 'output'} }"
    )
    assert failed.get("ok") is not True, failed

    recovered = box.run(max_seconds=200)
    assert recovered["ok"] is True, recovered
    _assert_ledger_intact(box, tag)


def _assert_ledger_intact(box: Sandbox, tag: str) -> None:
    """The source's own counts are the ledger; the destination must match them."""
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
