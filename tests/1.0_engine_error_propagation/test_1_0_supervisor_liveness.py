"""TODO 1.0(feedback): a connector in retriable-restart backoff is NOT "idle".

The defect (review finding Opus B5, measured before this test existed)
----------------------------------------------------------------------
`run_engine_bounded` declared a run `idle` - and therefore **successful** -
purely because no batch had arrived for `--idle-seconds`. Killing the walsender
mid-stream produced:

    ... ErrorHandler: Producer failure / Retry 1 of 3 retries will be attempted
    ... BaseSourceTask: Going to restart connector after 10 sec. after a retriable exception
    { "ok": true, "records": 118785, "stop_reason": "idle" }      # of 250 000 rows
    EXIT=0

The default idle window (8 s) is *shorter* than Debezium's restart backoff
(10 s), so a quiet stream during a reconnect is indistinguishable from a
finished one - unless the supervisor asks the source. This test pins the
property that matters and is deliberately written so that it cannot be satisfied
by shrinking the workload or lengthening a timer:

    **a run may report `ok` only if it delivered everything.**

Both acceptable outcomes are allowed - recover and deliver all of it, or fail
loudly - because which one happens depends on whether Debezium's three retries
suffice. What is never acceptable is `ok: true` on a partial delivery.

Marked `slow`: it needs a workload big enough that a mid-stream kill lands
mid-stream. Run with `make test-slow`.
"""

from __future__ import annotations

import json
import time

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
ROWS = 60_000
SENTINEL = "wskill-"


@pytest.mark.slow
def test_walsender_kill_never_reports_ok_on_partial_delivery(sandbox):
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)

    sandbox.sql(
        f"INSERT INTO app.customers (name, email) SELECT "
        f"'{SENTINEL}' || i, '{SENTINEL}' || i || '@example.com' "
        f"FROM generate_series(1, {ROWS}) i"
    )

    proc = sandbox.spawn(max_seconds=420, idle_seconds=8)
    try:
        # Wait until the connector is genuinely streaming (the slot is held), then
        # give it long enough to be part-way through, and cut the connection.
        deadline = time.monotonic() + 120
        killed = 0
        while time.monotonic() < deadline:
            time.sleep(2)
            if sandbox.pg_query(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s AND active",
                (sandbox.slot,),
            ):
                time.sleep(12)  # stream for a while so the kill lands mid-delivery
                killed = sandbox.kill_walsender()
                break
        assert killed == 1, (
            "could not terminate the walsender for this run's slot, so the test "
            "would be vacuous"
        )
        returncode = proc.wait(timeout=600)
    finally:
        if proc.poll() is None:  # pragma: no cover - only on an unexpected hang
            proc.kill()
            proc.wait(timeout=30)

    summary = sandbox.last_summary()
    delivered = sandbox.scalar(
        f"SELECT count(DISTINCT id) FROM {CUSTOMERS} WHERE name LIKE '{SENTINEL}%'"
    )
    detail = json.dumps({k: v for k, v in summary.items() if k != "output"}, indent=2)

    if returncode == 0 and summary.get("ok") is True:
        assert delivered == ROWS, (
            "THE B5 REGRESSION: the run reported success while only "
            f"{delivered} of {ROWS} rows had been delivered. A quiet stream during "
            "Debezium's retriable-restart backoff was mistaken for an idle one.\n"
            f"{detail}"
        )
        # Having recovered, the supervisor must have been able to say why it was
        # safe to stop.
        assert summary.get("slot_health") in {"streaming", "unknown"}, detail
    else:
        assert delivered < ROWS or returncode != 0, detail
        assert summary.get("ok") is not True, detail
