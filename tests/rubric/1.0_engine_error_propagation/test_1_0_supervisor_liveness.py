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
import threading
import time

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
#: The kill predicate below uses the durable slot position, not a guessed sleep.
#: The workload is produced behind a durable prefix barrier, so its size is a
#: bounded source-writing budget rather than a timing surrogate.
ROWS = 80_000
SENTINEL = "wskill-"
# Build a sizeable, source-committed WAL tail before the kill and hold the producer
# there. A single future transaction is too easy for Debezium to consume between two
# sampler observations; the kill proof needs a measured backlog, not a lucky instant.
PREKILL_TAIL_ROWS = 60_000


@pytest.mark.slow
def test_walsender_kill_never_reports_ok_on_partial_delivery(sandbox):
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=200, idle_seconds=8, timeout=400)

    chunk = 1_000
    proc = sandbox.spawn(max_seconds=500, idle_seconds=8)
    sandbox.wait_for_slot_active(process=proc, timeout=120, poll_seconds=0.1)
    before_lsn = int(
        sandbox.pg_query(
            "SELECT COALESCE(confirmed_flush_lsn - '0/0', 0) "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (sandbox.slot,),
        )[0][0]
    )
    prefix_rows = 4 * chunk
    for start in range(1, prefix_rows + 1, chunk):
        sandbox.sql(
            f"INSERT INTO app.customers (name, email) SELECT "
            f"'{SENTINEL}' || i, '{SENTINEL}' || i || '@example.com' "
            f"FROM generate_series({start}, {start + chunk - 1}) i"
        )

    tail_committed = 0
    writer_errors: list[BaseException] = []
    writer_done = threading.Event()
    tail_ready = threading.Event()
    release_all = threading.Event()

    def write_tail() -> None:
        nonlocal tail_committed
        try:
            for start in range(prefix_rows + 1, ROWS + 1, chunk):
                sandbox.sql(
                    f"INSERT INTO app.customers (name, email) SELECT "
                    f"'{SENTINEL}' || i, '{SENTINEL}' || i || '@example.com' "
                    f"FROM generate_series({start}, {start + chunk - 1}) i"
                )
                tail_committed += chunk
                if tail_committed >= PREKILL_TAIL_ROWS and not release_all.is_set():
                    tail_ready.set()
                    release_all.wait()
        except BaseException as exc:  # report the source-writer cause below
            writer_errors.append(exc)
        finally:
            writer_done.set()

    writer = threading.Thread(target=write_tail, name="fix12-walsender-writer")
    writer.start()
    try:
        progress_deadline = time.monotonic() + 180
        prefix_durable = None
        while time.monotonic() < progress_deadline:
            if proc.poll() is not None:
                raise AssertionError(
                    "the pipeline exited before the durable prefix barrier "
                    f"(returncode={proc.returncode})"
                )
            if writer_errors:
                raise AssertionError("source workload writer failed") from writer_errors[0]
            position = sandbox.pg_query(
                "SELECT COALESCE(confirmed_flush_lsn - '0/0', 0) "
                "FROM pg_replication_slots WHERE slot_name = %s AND active",
                (sandbox.slot,),
            )
            if position and before_lsn < int(position[0][0]):
                prefix_durable = int(position[0][0])
                break
            time.sleep(0.1)
        assert prefix_durable is not None, (
            "the durable slot position never crossed the source-produced prefix; "
            f"before={before_lsn}"
        )

        # Wait until the destination has durably acknowledged the prefix while the
        # concurrently written tail has already created future WAL.  This is a
        # source/destination state predicate, not a guessed sleep.
        deadline = time.monotonic() + 180
        killed = 0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            if writer_errors:
                raise AssertionError("source workload writer failed") from writer_errors[0]
            position = sandbox.pg_query(
                "SELECT COALESCE(confirmed_flush_lsn - '0/0', 0) "
                "FROM pg_replication_slots WHERE slot_name = %s AND active",
                (sandbox.slot,),
            )
            current = int(
                sandbox.pg_query("SELECT pg_current_wal_lsn() - '0/0'")[0][0]
            )
            if (
                position
                and int(position[0][0]) > before_lsn
                and tail_ready.is_set()
                and current - int(position[0][0]) >= 256 * 1024
                and not writer_done.is_set()
            ):
                killed = sandbox.kill_walsender()
                break
            if writer_done.is_set():
                break
            time.sleep(0.05)
        assert killed == 1, (
            "could not terminate the walsender after the durable prefix and before "
            "the concurrently written tail finished; the test would be vacuous"
        )
        # Keep producing the remaining source workload while Debezium handles the
        # walsender failure. The pre-kill gate already proved a substantial future
        # WAL tail; releasing the remainder ensures a successful recovery has to
        # deliver the complete declared workload.
        release_all.set()
        returncode = proc.wait(timeout=600)
    finally:
        release_all.set()
        if proc.poll() is None:  # pragma: no cover - only on an unexpected hang
            proc.kill()
            proc.wait(timeout=30)
        writer.join(timeout=180)
        if writer.is_alive():
            raise AssertionError("the source workload writer did not finish")
        if writer_errors:
            raise AssertionError("source workload writer failed") from writer_errors[0]

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
        # Having recovered, the run must say *why* it was safe to stop, and the
        # Invariant-O guard must still hold across the reconnect.
        #
        # This used to assert `slot_health in {streaming, unknown}`. That is not a
        # property of a successful run: the health summary is taken after
        # `engine.close()`, by which time the slot has legitimately been released,
        # so it reads `not_streaming` whenever the shutdown wins the race. It
        # passed by luck, not by construction.
        assert summary.get("stop_reason") == "idle", detail
        assert summary.get("invariant_o_end", {}).get("ok") is True, detail
    else:
        assert delivered < ROWS or returncode != 0, detail
        assert summary.get("ok") is not True, detail
