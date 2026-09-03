"""Real PostgreSQL/stock-Debezium proof for rubric §7.4 messages.

The startup handshake is deliberately part of this test rather than an implicit
sleep.  ``NEVER_ARMED`` means the child never reached the slot-active plus
ordinary-DML sentinel precondition; ``FIRED`` means that precondition was durable
and the source messages were emitted, so a later failure is a delivery failure.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid

import duckdb
import psycopg
import pytest
from support.fixtures import Sandbox

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


def _prime_replay_marker(box: Sandbox) -> dict[str, object]:
    """Create the real post-COMMIT/pre-ack survivor used by each replay cut."""
    baseline = box.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=6,
        extra_env={"CDC_COMPLETION_WATERMARK": "0"},
    )
    assert baseline["returncode"] == 0, baseline
    token = uuid.uuid4().hex
    tx_prefix = f"app_p72_txn_{token}"
    non_prefix = f"app_p72_non_txn_{token}"
    tx_content = "p72-transactional"
    emit_sql = (
        "SELECT (pg_logical_emit_message(%s, %s, %s) - "
        "'0/0'::pg_lsn)::bigint"
    )
    with psycopg.connect(box.source.dsn, autocommit=True) as conn, conn.transaction():
        conn.execute(
            "INSERT INTO app.customers (name, email) VALUES (%s, %s)",
            (f"p72-data-{token}", f"{token}@example.com"),
        )
        tx_lsn = int(
            conn.execute(emit_sql, (True, tx_prefix, tx_content)).fetchone()[0]
        )
    non_lsn = _emit_message(
        box.source,
        transactional=False,
        prefix=non_prefix,
        content="",
    )
    crashed = box.run(
        max_seconds=180,
        idle_seconds=8,
        timeout=300,
        expect_success=False,
        extra_env={
            "CDC_COMPLETION_WATERMARK": "0",
            "CDC_FAULT_INJECT": "post_commit_pre_ack:1",
        },
    )
    assert crashed["returncode"] == 137, crashed
    return {
        "tx_prefix": tx_prefix,
        "non_prefix": non_prefix,
        "tx_lsn": tx_lsn,
        "non_lsn": non_lsn,
        "data_name": f"p72-data-{token}",
    }


def _assert_replay_survivor(box: Sandbox, case: dict[str, object]) -> None:
    pipeline = box.env["CDC_PIPELINE_NAME"]
    prefixes = [case["non_prefix"], case["tx_prefix"]]
    rows = box.duck_query(
        "SELECT prefix, content, is_transactional FROM cdc_raw.cdcflight_logical_messages "
        "WHERE pipeline = ? AND prefix IN (?, ?) ORDER BY prefix",
        [pipeline, *prefixes],
    )
    assert len(rows) == 2, rows
    assert [row[0] for row in rows] == prefixes, rows
    assert rows[0][1] == b"" and rows[0][2] is False, rows
    assert rows[1][1] == b"p72-transactional" and rows[1][2] is True, rows
    assert box.duck_query(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name = ?",
        [case["data_name"]],
    ) == [(1,)]
    ledger = box.duck_query(
        "SELECT count(*), count(DISTINCT event_id) FROM _cdc_flight.event_ledger "
        "WHERE pipeline = ? AND target_table = 'cdc_raw.cdcflight_logical_messages' "
        "AND event_id IN (SELECT message_id FROM cdc_raw.cdcflight_logical_messages "
        "WHERE pipeline = ? AND prefix IN (?, ?))",
        [pipeline, pipeline, *prefixes],
    )
    assert ledger == [(2, 2)], ledger
    audit = box.duck_query(
        "SELECT prefix, status, byte_length FROM _cdc_flight.logical_message_audit "
        "WHERE pipeline = ? AND prefix IN (?, ?) ORDER BY prefix",
        [pipeline, *prefixes],
    )
    assert audit == [
        (prefixes[0], "delivered", 0),
        (prefixes[1], "delivered", len(b"p72-transactional")),
    ]

    durable = int(
        box.duck_query(
            "SELECT last_lsn FROM _cdc_flight.debezium_offsets "
            "WHERE pipeline = ? AND namespace = 'cdc-flight-engine'",
            [pipeline],
        )[0][0]
    )
    slot = box.pg_query(_SLOT_SQL, (box.slot,))[0][0]
    assert slot is not None and int(slot) <= durable, {"slot": slot, "durable": durable}
    assert not (box.state_dir / ".offsets.replay.intent").exists()
    assert not (box.state_dir / ".offsets.replay.dat").exists()


@pytest.mark.parametrize(
    "cut",
    [
        "after_prepare",
        "file_exists_before_first_md_commit",
        "mid_replay_before_first_md_commit",
        "after_md_commit_before_install",
        "during_copy_before_fsync",
        "at_os_replace",
        "after_install_before_clear",
        "after_intent_clear_before_cleanup",
    ],
)
def test_replay_intent_survives_real_process_death_at_every_interleaving(
    tmp_path, postgres_cluster, cut
):
    """Kill real children at each replay edge and prove the next run self-heals."""
    box = Sandbox(f"p72_replay_{cut}", tmp_path / cut, postgres_cluster)
    try:
        box.reseed()
        case = _prime_replay_marker(box)
        box.clear_fired_fault()
        matrix_state = "p72_replay_matrix.json"
        matrix_cut = {
            "after_prepare": "source_replay_after_prepare",
            "file_exists_before_first_md_commit": (
                "source_replay_file_exists_before_first_md_commit"
            ),
            "mid_replay_before_first_md_commit": (
                "source_replay_mid_replay_before_first_md_commit"
            ),
            "after_md_commit_before_install": (
                "source_replay_after_md_commit_before_install"
            ),
            "during_copy_before_fsync": "source_replay_during_copy_before_fsync",
            # The cut fires immediately before the atomic syscall. The old-or-new
            # proof is therefore exercised at the replace boundary itself; this
            # instance leaves the old complete canonical file for the retry.
            "at_os_replace": "source_replay_at_os_replace",
            "after_install_before_clear": "source_replay_after_install_before_clear",
            "after_intent_clear_before_cleanup": (
                "source_replay_after_intent_clear_before_cleanup"
            ),
        }.get(cut)
        fault_env = {
            "CDC_COMPLETION_WATERMARK": "0",
            "CDC_CRASH_MATRIX_STATE": matrix_state,
        }
        fault_env["CDC_CRASH_MATRIX_CUT"] = matrix_cut

        if cut == "file_exists_before_first_md_commit":
            # First make a complete replay file, then die before its successor
            # prepares the next disposable path. No commit is made by that successor.
            first = box.run(
                max_seconds=180,
                idle_seconds=8,
                timeout=300,
                expect_success=False,
                matrix_arm=True,
                extra_env={
                    "CDC_COMPLETION_WATERMARK": "0",
                    "CDC_CRASH_MATRIX_CUT": (
                        "source_replay_after_md_commit_before_install"
                    ),
                    "CDC_CRASH_MATRIX_STATE": matrix_state,
                },
            )
            assert first["returncode"] == 137, first
            assert box.fired_fault()["point"] == "source_replay_after_md_commit_before_install"
            replay_path = box.state_dir / ".offsets.replay.dat"
            assert replay_path.exists() and replay_path.stat().st_size > 0
            box.clear_fired_fault()

        killed = box.run(
            max_seconds=180,
            idle_seconds=8,
            timeout=300,
            expect_success=False,
            matrix_arm=True,
            extra_env=fault_env,
        )
        assert killed["returncode"] == 137, killed
        fired = box.fired_fault()
        expected_point = matrix_cut or "begin"
        assert fired and fired["point"] == expected_point, fired
        state_path = box.state_dir / matrix_state
        assert state_path.exists(), f"no durable kill witness for {cut}"
        kill_state = json.loads(state_path.read_text())
        resume_lsn = kill_state.get("context", {}).get("source_replay_resume_lsn")
        assert resume_lsn is not None, kill_state

        recovered = box.run(
            max_seconds=180,
            idle_seconds=8,
            timeout=300,
            extra_env={"CDC_COMPLETION_WATERMARK": "0"},
        )
        assert recovered["returncode"] == 0, recovered
        _assert_replay_survivor(box, case)
        if cut in {"after_install_before_clear", "after_intent_clear_before_cleanup"}:
            if cut == "after_intent_clear_before_cleanup":
                assert recovered.get("source_replay_orphan_reclaimed") is True, recovered
                assert recovered.get("source_replay_intent_cleared_on_start") is None, recovered
            else:
                assert recovered.get("source_replay_intent_cleared_on_start") is True, recovered
            assert recovered.get("source_replay_from_slot") is None, recovered
            assert recovered.get("reconciliation") == "resume", recovered
            recovered_resume_lsn = recovered.get("invariant_o_start", {}).get(
                "durable_lsn"
            )
        else:
            assert recovered.get("source_replay_from_slot") is True, recovered
            recovered_resume_lsn = recovered.get("source_replay_resume_lsn")
        assert (
            isinstance(recovered_resume_lsn, int)
            and recovered_resume_lsn >= resume_lsn
        ), recovered
        (box.state_dir / "p72_replay_recovered.json").write_text(
            json.dumps(
                {
                    "cut": cut,
                    "fired": fired,
                    "kill_resume_lsn": resume_lsn,
                    "recovered_resume_lsn": recovered_resume_lsn,
                    "source_replay_from_slot": recovered.get("source_replay_from_slot"),
                    "source_replay_intent_cleared_on_start": recovered.get(
                        "source_replay_intent_cleared_on_start"
                    ),
                    "logical_message_rows": 2,
                    "ledger_rows": 2,
                    "customer_rows": 1,
                    "invariant_o_end": recovered.get("invariant_o_end"),
                },
                sort_keys=True,
            )
        )
    finally:
        box.cleanup()
        box.reseed()


@pytest.mark.parametrize("route", ["slot_missing", "operator_reset"])
def test_full_recovery_preserves_the_pre_ack_message_certificate(
    tmp_path, postgres_cluster, route
):
    """B1 sequence: durable commit, pre-ack death, pending replay, full recovery."""
    box = Sandbox(f"p72_full_recovery_{route}", tmp_path / route, postgres_cluster)
    try:
        box.reseed()
        case = _prime_replay_marker(box)

        # The first successor durably owns source replay, but dies before it can
        # consume the slot. This is the pending/installing state that the full
        # recovery route must derive from MotherDuck rather than forget.
        pending = box.run(
            max_seconds=180,
            idle_seconds=8,
            timeout=300,
            expect_success=False,
            matrix_arm=True,
            extra_env={
                "CDC_COMPLETION_WATERMARK": "0",
                "CDC_CRASH_MATRIX_CUT": "source_replay_after_prepare",
                "CDC_CRASH_MATRIX_STATE": "p72_full_recovery_matrix.json",
            },
        )
        assert pending["returncode"] == 137, pending
        assert box.fired_fault()["point"] == "source_replay_after_prepare"
        assert (box.state_dir / ".offsets.replay.intent").exists()

        if route == "slot_missing":
            box.drop_slot()
            recovered = box.run(
                max_seconds=180,
                idle_seconds=8,
                timeout=300,
                extra_env={"CDC_COMPLETION_WATERMARK": "0"},
            )
        else:
            recovered = box.run(
                reset_state=True,
                max_seconds=180,
                idle_seconds=8,
                timeout=300,
                extra_env={"CDC_COMPLETION_WATERMARK": "0"},
            )
        assert recovered["returncode"] == 0, recovered
        _assert_replay_survivor(box, case)

        certificate = recovered.get("logical_message_recovery")
        assert isinstance(certificate, dict), recovered
        assert certificate["certified_count"] == 2, recovered
        assert len(set(certificate["certified_message_ids"])) == 2, recovered
        assert certificate["obligations"] == []
        assert certificate["obligation_count"] == 0
        if route == "slot_missing":
            assert certificate["replay_intent_cleared"] is True
            assert recovered.get("source_replay_intent_superseded") == "slot_recovery", recovered
        else:
            reset_certificate = recovered.get("reset_state", {}).get(
                "logical_message_certificate"
            )
            assert isinstance(reset_certificate, dict), recovered
            assert reset_certificate["certified_count"] == 2, recovered
            assert recovered.get("reset_state", {}).get("replay_intent_cleared") is True, recovered
        if route == "slot_missing":
            assert recovered.get("slot_check", {}).get("decision") == "slot_missing", recovered
        else:
            assert recovered.get("reset_state", {}).get("decision") == "operator_reset", recovered
    finally:
        box.cleanup()
        box.reseed()
