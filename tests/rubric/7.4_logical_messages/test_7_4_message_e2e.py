"""Real PostgreSQL/stock-Debezium proof for rubric §7.4 messages.

The startup handshake is deliberately part of this test rather than an implicit
sleep.  ``NEVER_ARMED`` means the child never reached the slot-active plus
ordinary-DML sentinel precondition; ``FIRED`` means that precondition was durable
and the source messages were emitted, so a later failure is a delivery failure.
"""

from __future__ import annotations

import subprocess
import time
import uuid

import duckdb
import psycopg
import pytest

from cdc_flight.logical_messages import read_logical_messages

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

_SLOT_SQL = (
    "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint, "
    "       (restart_lsn - '0/0'::pg_lsn)::bigint "
    "FROM pg_replication_slots WHERE slot_name = %s"
)


def _child_tail(process) -> tuple[str, str]:
    """Return exited-child evidence without hiding a failed precondition."""
    if process.poll() is None:
        return "", ""
    stdout, stderr = process.communicate()
    return stdout[-3000:], stderr[-6000:]


def _stop_child_for_precondition_evidence(process) -> tuple[str, str]:
    """Bound a stuck child before reporting a NEVER_ARMED precondition failure."""
    if process.poll() is None:
        process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=30)
    return stdout[-3000:], stderr[-6000:]


def _wait_for_sentinel_commit(sandbox, process, sentinel: str, *, timeout: float) -> None:
    """Wait for a clean child exit, then prove the sentinel is in the destination.

    DuckDB's local file lock intentionally rejects a second process while the
    writer connection is open.  The child therefore has to release that lock
    before this parent can read the durable destination row.  This is still a
    destination predicate, not a timer: a clean child with no sentinel is an
    explicit NEVER_ARMED failure.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = _child_tail(process)
            if process.returncode != 0:
                raise AssertionError(
                    "NEVER_ARMED: child exited before the ordinary DML sentinel "
                    f"could be checked in the destination (returncode={process.returncode}); "
                    f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
                )
            query = (
                'SELECT name FROM "cdc_raw"."cdcflight_app_customers" '
                "WHERE name = ?"
            )
            if sandbox.duck_query(query, [sentinel]):
                return
            raise AssertionError(
                "NEVER_ARMED: the child exited cleanly, but the ordinary DML "
                f"sentinel {sentinel!r} is absent from the durable destination; "
                f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
            )
        time.sleep(0.2)
    stdout, stderr = _stop_child_for_precondition_evidence(process)
    raise AssertionError(
        "NEVER_ARMED: the active child did not reach a clean destination-check "
        f"boundary for ordinary DML sentinel {sentinel!r} "
        f"(returncode={process.returncode}); summary={sandbox.last_summary()}"
        f"\nstdout={stdout}\nstderr={stderr}"
    )


def _emit_message(source, *, transactional: bool, prefix: str, content: str) -> int:
    """Emit one source message and return PostgreSQL's assigned WAL position."""
    sql = (
        "SELECT (pg_logical_emit_message(%s, %s, %s) - "
        "'0/0'::pg_lsn)::bigint"
    )
    with psycopg.connect(source.dsn, autocommit=True) as conn:
        if transactional:
            with conn.transaction():
                return int(conn.execute(sql, (True, prefix, content)).fetchone()[0])
        return int(conn.execute(sql, (False, prefix, content)).fetchone()[0])


def _wait_for_confirmed(sandbox, process, target: int, *, timeout: float) -> int:
    """Wait for the running slot to confirm the emitted source position."""
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                "FIRED: child exited before the source message offset was "
                f"confirmed (returncode={process.returncode}, target={target})"
            )
        row = sandbox.pg_query(_SLOT_SQL, (sandbox.slot,))
        if row and row[0][0] is not None:
            latest = int(row[0][0])
            if latest >= target:
                return latest
        time.sleep(0.2)
    raise AssertionError(
        "FIRED: the destination rows were durable, but confirmed_flush_lsn did "
        f"not reach the message WAL position {target}; last={latest}"
    )


def test_stock_messages_are_exact_consumer_rows_and_do_not_become_liveness_data(sandbox):
    """Capture both source message shapes through the stock connector."""
    sandbox.reseed()
    control_env = {"CDC_COMPLETION_WATERMARK": "0"}
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=6,
        extra_env=control_env,
    )
    assert baseline["returncode"] == 0, baseline

    token = uuid.uuid4().hex
    sentinel = f"p74-sentinel-{token}"
    sentinel_email = f"{token}@example.com"
    transactional_prefix = f"app_p74_txn_{token}"
    non_transactional_prefix = f"app_p74_non_txn_{token}"
    # ``Sandbox.spawn`` is intentionally a raw child launcher, so remove the
    # completed baseline's summary before the live precondition starts.  A stale
    # summary must never be mistaken for evidence that this child reached its
    # sentinel; the child will publish its own summary on exit.
    (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
    precondition_process = sandbox.spawn(
        max_seconds=180,
        idle_seconds=45,
        # The baseline has already established the destination image and the
        # durable source offset.  Starting this armed child in no-data mode
        # makes the slot-active predicate lead into the streaming phase rather
        # than into a second snapshot/catch-up race; the ordinary DML sentinel
        # remains the proof that the child is actually delivering source data.
        snapshot_mode="no_data",
        extra_env=control_env,
        capture=True,
    )
    fired = False
    process = None
    try:
        try:
            # This is the existing predicate: it checks both child liveness and
            # that this sandbox's named replication slot is ACTIVE.
            sandbox.wait_for_slot_active(process=precondition_process, timeout=74)
        except AssertionError as exc:
            raise AssertionError(f"NEVER_ARMED: slot-active precondition failed: {exc}") from exc

        try:
            sandbox.sql(
                "INSERT INTO app.customers (name, email) VALUES "
                f"('{sentinel}', '{sentinel_email}')"
            )
            _wait_for_sentinel_commit(
                sandbox, precondition_process, sentinel, timeout=135
            )
            precondition_summary = sandbox.last_summary()
            assert precondition_summary.get("data_commit_groups") == 1, (
                "NEVER_ARMED: the sentinel child exited without one ordinary-DML "
                f"commit group: {precondition_summary}"
            )
        except AssertionError:
            raise
        except Exception as exc:
            raise AssertionError(
                f"NEVER_ARMED: could not establish the ordinary DML sentinel: {exc}"
            ) from exc
        # The local DuckDB writer lock is released by the precondition child before
        # the destination predicate above is read.  Start the message child from
        # that proven offset; no message is emitted until its own named slot is
        # active and the durable sentinel proof has completed.
        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        process = sandbox.spawn(
            max_seconds=180,
            idle_seconds=45,
            snapshot_mode="no_data",
            extra_env=control_env,
            capture=True,
        )
        try:
            sandbox.wait_for_slot_active(process=process, timeout=74)
        except AssertionError as exc:
            raise AssertionError(
                f"NEVER_ARMED: message child slot-active precondition failed: {exc}"
            ) from exc

        # From this point on the test is FIRED.  No message is emitted before the
        # active-slot and durable-sentinel handshake has completed.
        fired = True
        txn_lsn = _emit_message(
            sandbox.source,
            transactional=True,
            prefix=transactional_prefix,
            content="p74-transactional",
        )
        non_txn_lsn = _emit_message(
            sandbox.source,
            transactional=False,
            prefix=non_transactional_prefix,
            content="",
        )

        # Both message positions must be acknowledged by the running stock slot.
        # The consumer rows are read after this child exits, because a local
        # DuckDB read-only connection cannot open while the writer process owns
        # the database file lock.
        _wait_for_confirmed(sandbox, process, max(txn_lsn, non_txn_lsn), timeout=15)
        quiet_before = int(sandbox.pg_query(_SLOT_SQL, (sandbox.slot,))[0][0])

        # Wait beyond the 5-second stock heartbeat cadence while emitting no new
        # source DML.  A message callback cannot refresh the data-quiet clock, so
        # a premature child exit is a FIRED failure, not a vacuous success.
        quiet_deadline = time.monotonic() + 12
        quiet_after = quiet_before
        while time.monotonic() < quiet_deadline:
            if process.poll() is not None:
                raise AssertionError(
                    "FIRED: child stopped before quiet-source heartbeat proof "
                    f"(returncode={process.returncode})"
                )
            quiet_after = int(sandbox.pg_query(_SLOT_SQL, (sandbox.slot,))[0][0])
            if quiet_after > quiet_before:
                break
            time.sleep(0.25)
        assert quiet_after > quiet_before, (
            "FIRED: quiet source heartbeat did not advance confirmed_flush_lsn; "
            f"before={quiet_before}, after={quiet_after}"
        )
        try:
            process.wait(timeout=90)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                "FIRED: quiet source never reached the liveness idle boundary after "
                "the heartbeat advanced"
            ) from exc
    except Exception as exc:
        if fired and not str(exc).startswith("FIRED:"):
            raise AssertionError(f"FIRED: message delivery/heartbeat assertion failed: {exc}") from exc
        raise
    finally:
        if precondition_process.poll() is None:
            precondition_process.terminate()
        precondition_process.communicate(timeout=60)
        if process is not None and process.poll() is None:
            process.terminate()
        stdout, stderr = (
            process.communicate(timeout=60) if process is not None else ("", "")
        )

    assert process is not None and process.returncode == 0, (
        "FIRED: live message scenario child did not shut down cleanly; "
        f"returncode={process.returncode}\nstdout={stdout[-3000:]}\nstderr={stderr[-6000:]}"
    )
    summary = sandbox.last_summary()
    assert summary.get("returncode", 0) == 0 or summary.get("ok") is True, summary
    counts = summary.get("logical_messages", {})
    assert counts.get("logical_messages_delivered", 0) >= 2, summary
    assert counts.get("logical_messages_internal", 0) >= 1, summary
    assert summary.get("data_commit_groups") == 0, (
        "logical messages must not refresh the delivered-source liveness high-water "
        f"or become data commit groups: {summary}"
    )

    pipeline = sandbox.env["CDC_PIPELINE_NAME"]
    with duckdb.connect(str(sandbox.duckdb_path), read_only=True) as con:
        consumed = read_logical_messages(
            con,
            dataset="cdc_raw",
            pipeline=pipeline,
        )
    selected = {
        row["prefix"]: row
        for row in consumed
        if row["prefix"] in {transactional_prefix, non_transactional_prefix}
    }
    assert set(selected) == {transactional_prefix, non_transactional_prefix}
    assert selected[transactional_prefix]["content"] == b"p74-transactional"
    assert selected[non_transactional_prefix]["content"] == b""
    assert selected[transactional_prefix]["is_transactional"] is True
    assert selected[non_transactional_prefix]["is_transactional"] is False
    assert selected[transactional_prefix]["txn_id"]
    assert selected[transactional_prefix]["total_order"] == 1
    assert selected[non_transactional_prefix]["txn_id"] is None
    assert selected[non_transactional_prefix]["total_order"] is None
    for row in selected.values():
        assert row["source_lsn"] is not None or row["source_sequence"] is not None

    audit = sandbox.duck_query(
        "SELECT prefix, status, byte_length FROM _cdc_flight.logical_message_audit "
        "WHERE pipeline = ? AND prefix IN (?, ?) ORDER BY prefix",
        [pipeline, transactional_prefix, non_transactional_prefix],
    )
    assert audit == [
        (non_transactional_prefix, "delivered", 0),
        (transactional_prefix, "delivered", len(b"p74-transactional")),
    ]
    assert all(
        "content" not in observation and "payload" not in observation
        for observation in summary.get("logical_message_observations", [])
    )
