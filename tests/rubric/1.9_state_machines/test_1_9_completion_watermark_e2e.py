"""The completion watermark against a real cluster and a real Debezium engine.

`test_1_9_completion_watermark.py` pins the decision in microseconds over fakes.
This file proves the composition, which is the part that was actually broken: that
a `cdc-flight` run **reaches a position** instead of **waiting out a timer**, that
the position it reaches is one PostgreSQL assigned, and that the replication slot
is left confirmed past it with bounded retained WAL.

Both runs below use the SAME `--idle-seconds`. The only difference is whether the
source can be marked. If the watermark were cosmetic the two would take the same
time; the assertion is that they do not.
"""

from __future__ import annotations

import time

import pytest

from cdc_flight.machines import WATERMARK_REACHED, WATERMARK_UNAVAILABLE

pytestmark = [pytest.mark.e2e]

#: Long enough that no run below could plausibly have waited it out.
IDLE_SECONDS = 15.0


@pytest.fixture(scope="module")
def watermark_runs(sandbox) -> dict:
    """One seeded snapshot, then two quiet incremental runs at the same idle window."""
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=120, idle_seconds=IDLE_SECONDS)

    sandbox.sql(
        "INSERT INTO app.customers (name, email) "
        "SELECT 'wm-' || i, 'wm-' || i || '@example.com' "
        "FROM generate_series(1, 25) i"
    )
    before = _wal_lsn(sandbox)
    started = time.monotonic()
    watermarked = sandbox.run(max_seconds=120, idle_seconds=IDLE_SECONDS)
    watermarked["wall"] = time.monotonic() - started
    watermarked["slot"] = _slot(sandbox)
    watermarked["source_lsn_before"] = before

    # The same run with the one thing the watermark needs taken away.
    sandbox.sql(
        "INSERT INTO app.customers (name, email) "
        "SELECT 'wm2-' || i, 'wm2-' || i || '@example.com' "
        "FROM generate_series(1, 25) i"
    )
    started = time.monotonic()
    unmarkable = sandbox.run(
        max_seconds=120,
        idle_seconds=IDLE_SECONDS,
        extra_env={"CDC_COMPLETION_WATERMARK": "0"},
    )
    unmarkable["wall"] = time.monotonic() - started
    unmarkable["slot"] = _slot(sandbox)
    return {"watermarked": watermarked, "unmarkable": unmarkable, "box": sandbox}


def _wal_lsn(box) -> int:
    return int(box.pg_query("SELECT (pg_current_wal_lsn() - '0/0')::BIGINT")[0][0])


def _slot(box) -> dict:
    rows = box.pg_query(
        "SELECT (restart_lsn - '0/0')::BIGINT, (confirmed_flush_lsn - '0/0')::BIGINT, "
        "       pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn)::BIGINT, "
        "       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::BIGINT, active "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    if not rows:
        return {}
    restart, confirmed, unconsumed, cluster_retained, active = rows[0]
    return {
        "restart_lsn": int(restart),
        "confirmed_flush_lsn": int(confirmed),
        # What THIS slot pins behind its own confirmed position. The
        # `pg_current_wal_lsn()` figure beside it is cluster-wide and a co-tenant
        # database moves it, which is the exact mistake review r12 (R12-3)
        # measured costing every bounded run its whole `--max-seconds`.
        "unconsumed_wal_bytes": int(unconsumed),
        "cluster_retained_wal_bytes": int(cluster_retained),
        "active": bool(active),
    }


def test_a_quiet_run_ends_on_the_watermark_not_on_the_idle_timer(watermark_runs):
    run = watermark_runs["watermarked"]
    assert run["ok"] is True
    assert run["stop_reason"] == "idle"
    assert run["completion_watermark"] == WATERMARK_REACHED
    assert run["elapsed_sec"] < IDLE_SECONDS, (
        "the supervised run outlived its own quiet window, so it cannot have "
        f"stopped on a position: {run['elapsed_sec']}s of {IDLE_SECONDS}s"
    )
    # The whole point of the change, as a number: ~4 s of pipeline instead of
    # ~4 s + idle_seconds. Wall includes JVM boot and teardown, so it is compared
    # against the window itself rather than against a hand-picked constant.
    assert run["wall"] < IDLE_SECONDS


def test_the_watermark_is_a_real_postgresql_position_the_destination_reached(
    watermark_runs,
):
    run = watermark_runs["watermarked"]
    target = run["completion_watermark_lsn"]
    assert isinstance(target, int) and target > 0
    # It was taken DURING this run, after the connector attached, not before it.
    assert target > run["source_lsn_before"]
    # And the destination is durably at or past it. `durable_lsn` is the resume
    # point, which under Invariant O is written inside the same MotherDuck/DuckDB
    # transaction as the data and only becomes visible when that COMMIT returns.
    assert run["durable_lsn"] >= target


def test_the_slot_is_left_confirmed_past_the_watermark(watermark_runs):
    """The durability half: the run stopped early, and the SOURCE agrees.

    Stopping early would be a defect if it left the slot behind the destination:
    WAL the destination already holds would be re-read, and WAL it does not hold
    would look consumed. Neither happens - `confirmed_flush_lsn` is at or past
    both the watermark and the durable resume point.
    """
    run = watermark_runs["watermarked"]
    slot = run["slot"]
    assert slot, "the run must leave its slot in place for the next one"
    assert slot["active"] is False
    assert slot["confirmed_flush_lsn"] >= run["completion_watermark_lsn"], slot
    # Invariant O from the source side: nothing the destination made durable is
    # unconfirmed at the source when the run ends.
    assert slot["confirmed_flush_lsn"] >= run["durable_lsn"], (slot, run["durable_lsn"])
    assert slot["restart_lsn"] <= slot["confirmed_flush_lsn"], slot


def test_the_retention_horizon_keeps_moving_across_runs(watermark_runs):
    """rubric 4.4's clause: the slot must not freeze, or WAL grows without bound.

    The absolute byte figures beside these positions are CLUSTER-wide - a
    co-tenant database moves `pg_current_wal_lsn()` without a byte of it being
    ours (review r12 R12-3) - so what is asserted is the per-slot property that a
    frozen slot would violate: both positions strictly advance from one run to the
    next, so no run leaves WAL pinned behind it.
    """
    first = watermark_runs["watermarked"]["slot"]
    second = watermark_runs["unmarkable"]["slot"]
    assert second["confirmed_flush_lsn"] > first["confirmed_flush_lsn"], (first, second)
    assert second["restart_lsn"] >= first["restart_lsn"], (first, second)


def test_the_run_delivered_everything_it_claimed_to(watermark_runs):
    box = watermark_runs["box"]
    source = box.pg_query("SELECT count(*) FROM app.customers")[0][0]
    landed = box.scalar(f'SELECT count(*) FROM {box.table("cdcflight_app_customers")}')
    assert landed == source


def test_a_source_that_cannot_be_marked_still_falls_back_to_the_quiet_window(
    watermark_runs,
):
    """The declared fallback, measured. With no marker there is no position to
    reach, so the run keeps the source-corroborated quiet window it always had —
    and pays for it, which is exactly what the watermark exists to stop."""
    run = watermark_runs["unmarkable"]
    assert run["ok"] is True
    assert run["stop_reason"] == "idle"
    assert run["completion_watermark"] == WATERMARK_UNAVAILABLE
    assert run["elapsed_sec"] >= IDLE_SECONDS, (
        "the fallback is a quiet window; a run that ends before it has not "
        "waited for one"
    )
    assert run["elapsed_sec"] > watermark_runs["watermarked"]["elapsed_sec"] * 2
