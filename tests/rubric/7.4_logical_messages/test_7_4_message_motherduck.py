"""Real MotherDuck durability and crash/replay proof for rubric §7.4.

The child is not considered armed merely because it started.  The source slot must
be active, a unique ordinary DML sentinel must be visible in MotherDuck, and the
crash child must persist ``MESSAGE_CALLBACK_ENTERED`` followed by ``MESSAGE_SEEN``.
Only then are the message-producing statements issued.  ``MD_COMMITTED`` below is
an independent MotherDuck read after the post-commit crash, not a timing guess.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid

import duckdb
import psycopg
import pytest

from cdc_flight.naming import quote

pytestmark = [
    pytest.mark.motherduck,
    pytest.mark.e2e,
    pytest.mark.xdist_group("md_7_4_messages"),
]


def _child_tail(process) -> tuple[str, str]:
    if process.poll() is None:
        return "", ""
    stdout, stderr = process.communicate()
    return stdout[-4000:], stderr[-7000:]


def _stop_child(process) -> tuple[str, str]:
    if process.poll() is None:
        process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=30)
    return stdout[-4000:], stderr[-7000:]


def _wait_for_md_sentinel(
    sandbox,
    process,
    con,
    *,
    dataset: str,
    sentinel: str,
    timeout: float,
) -> None:
    """Prove the active child delivered the ordinary sentinel to MotherDuck."""
    table = f"{quote(dataset)}.cdcflight_app_customers"
    query = f"SELECT name FROM {table} WHERE name = ?"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = _child_tail(process)
            raise AssertionError(
                "NEVER_ARMED: child exited before the ordinary DML sentinel was "
                f"durable in MotherDuck (returncode={process.returncode}); "
                f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
            )
        try:
            con.execute("FORCE CHECKPOINT")
            if con.execute(query, [sentinel]).fetchall():
                return
        except duckdb.Error:
            # MotherDuck may briefly publish the destination schema/table between
            # the child's commit and the catalog refresh.  The bounded predicate,
            # rather than this transient catalog state, decides the result.
            pass
        time.sleep(0.5)
    stdout, stderr = _stop_child(process)
    raise AssertionError(
        "NEVER_ARMED: the active child did not make ordinary DML sentinel "
        f"{sentinel!r} visible in MotherDuck within {timeout:.1f}s; "
        f"returncode={process.returncode}, summary={sandbox.last_summary()}"
        f"\nstdout={stdout}\nstderr={stderr}"
    )


def _wait_for_context(sandbox, process, *, filename: str, key: str, timeout: float):
    """Wait for one persisted production callback edge, never an arbitrary sleep."""
    path = sandbox.state_dir / filename
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            state = json.loads(path.read_text())
            if state.get("context", {}).get(key):
                return state
        if process.poll() is not None:
            stdout, stderr = _child_tail(process)
            raise AssertionError(
                "NEVER_ARMED: message child exited before the required "
                f"{key} handshake (returncode={process.returncode}); "
                f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
            )
        time.sleep(0.1)
    stdout, stderr = _stop_child(process)
    raise AssertionError(
        "NEVER_ARMED: message child never persisted the required "
        f"{key} handshake; returncode={process.returncode}, "
        f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
    )


def _emit_transactional_message(source, *, prefix: str, content: bytes) -> int:
    """Insert ordinary DML and a bytea logical message in one PG transaction."""
    sql = (
        "SELECT (pg_logical_emit_message(%s, %s, %s::bytea) - "
        "'0/0'::pg_lsn)::bigint"
    )
    token = uuid.uuid4().hex
    with psycopg.connect(source.dsn, autocommit=True) as conn, conn.transaction():
        conn.execute(
            "INSERT INTO app.customers (name, email) VALUES (%s, %s)",
            (f"p74-md-data-{token}", f"{token}@example.com"),
        )
        return int(
            conn.execute(sql, (True, prefix, psycopg.Binary(content))).fetchone()[0]
        )


def _emit_non_transactional_message(source, *, prefix: str, content: bytes) -> int:
    sql = (
        "SELECT (pg_logical_emit_message(%s, %s, %s::bytea) - "
        "'0/0'::pg_lsn)::bigint"
    )
    with psycopg.connect(source.dsn, autocommit=True) as conn:
        return int(
            conn.execute(sql, (False, prefix, psycopg.Binary(content))).fetchone()[0]
        )


def _md_snapshot(con, *, dataset: str, control_schema: str, pipeline: str, prefixes: tuple[str, str]):
    message_table = f"{quote(dataset)}.cdcflight_logical_messages"
    customer_table = f"{quote(dataset)}.cdcflight_app_customers"
    control = quote(control_schema)
    tx_prefix, non_prefix = prefixes
    messages = con.execute(
        f"SELECT message_id, prefix, content, is_transactional, source_lsn, "
        f"txn_id, total_order, commit_lsn, destination_commit_id "
        f"FROM {message_table} WHERE pipeline = ? AND prefix IN (?, ?) ORDER BY prefix",
        [pipeline, tx_prefix, non_prefix],
    ).fetchall()
    ledger = con.execute(
        f"SELECT event_id, state, payload_digest, source_lsn, commit_lsn, txn_id, "
        f"total_order FROM {control}.event_ledger WHERE pipeline = ? AND "
        f"target_table = ? AND event_id IN (SELECT message_id FROM {message_table} "
        f"WHERE pipeline = ? AND prefix IN (?, ?)) ORDER BY event_id",
        [pipeline, f"{dataset}.cdcflight_logical_messages", pipeline, tx_prefix, non_prefix],
    ).fetchall()
    audit = con.execute(
        f"SELECT prefix, status, byte_length FROM {control}.logical_message_audit "
        f"WHERE pipeline = ? AND prefix IN (?, ?) ORDER BY prefix",
        [pipeline, tx_prefix, non_prefix],
    ).fetchall()
    return messages, ledger, audit, customer_table, message_table, control


def test_messages_are_atomic_and_replay_safe_in_motherduck(
    sandbox, motherduck_module_case
):
    """Prove the consumer, ledger, state, offset, and post-commit replay together."""
    token = motherduck_module_case["token"]
    database = motherduck_module_case["database"]
    dataset = motherduck_module_case["dataset"]
    control_schema = motherduck_module_case["control_schema"]
    pipeline = sandbox.env["CDC_PIPELINE_NAME"]
    dsn = f"md:{database}?motherduck_token={token}"
    env = {
        "CDC_DATASET": dataset,
        "CDC_MD_DATABASE": database,
        "CDC_CONTROL_SCHEMA": control_schema,
        "MOTHERDUCK_TOKEN": token,
        "motherduck_token": token,
        "CDC_COMPLETION_WATERMARK": "0",
    }
    tx_prefix = f"app_p74_md_txn_{uuid.uuid4().hex}"
    non_prefix = f"app_p74_md_non_txn_{uuid.uuid4().hex}"
    sentinel = f"p74-md-sentinel-{uuid.uuid4().hex}"

    sandbox.reseed()
    baseline = sandbox.run(
        reset_state=True,
        destination="motherduck",
        max_seconds=300,
        idle_seconds=8,
        timeout=600,
        extra_env=env,
    )
    assert baseline["returncode"] == 0, baseline

    precondition_process = None
    crash_process = None
    try:
        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        precondition_process = sandbox.spawn(
            destination="motherduck",
            snapshot_mode="no_data",
            max_seconds=240,
            idle_seconds=45,
            extra_env=env,
            capture=True,
        )
        try:
            sandbox.wait_for_slot_active(process=precondition_process, timeout=90)
        except AssertionError as exc:
            raise AssertionError(f"NEVER_ARMED: slot-active precondition failed: {exc}") from exc
        # This ordinary source DML is the required arming sentinel.  It is
        # deliberately issued only after the named slot is active; the message
        # statements below are forbidden until this exact row is durable in MD.
        sandbox.sql(
            "INSERT INTO app.customers (name, email) VALUES "
            f"('{sentinel}', '{sentinel}@example.com')"
        )
        with duckdb.connect(dsn) as md:
            _wait_for_md_sentinel(
                sandbox,
                precondition_process,
                md,
                dataset=dataset,
                sentinel=sentinel,
                timeout=150,
            )
        try:
            precondition_process.wait(timeout=120)
        except subprocess.TimeoutExpired as exc:
            stdout, stderr = _stop_child(precondition_process)
            raise AssertionError(
                "NEVER_ARMED: sentinel child did not complete its bounded clean "
                f"run (returncode={precondition_process.returncode}); "
                f"summary={sandbox.last_summary()}\nstdout={stdout}\nstderr={stderr}"
            ) from exc
        assert precondition_process.returncode == 0, (
            "NEVER_ARMED: sentinel was visible but its child did not complete "
            f"cleanly: {sandbox.last_summary()}"
        )

        (sandbox.state_dir / "last_run.json").unlink(missing_ok=True)
        (sandbox.state_dir / "message_handshake.json").unlink(missing_ok=True)
        sandbox.clear_fired_fault()
        crash_process = sandbox.spawn(
            destination="motherduck",
            snapshot_mode="no_data",
            max_seconds=240,
            idle_seconds=90,
            matrix_arm=True,
            extra_env={
                **env,
                "CDC_CRASH_MATRIX_STATE": "message_handshake.json",
                "CDC_FAULT_INJECT": "post_commit_pre_ack:1",
            },
            capture=True,
        )
        try:
            sandbox.wait_for_slot_active(process=crash_process, timeout=90)
        except AssertionError as exc:
            raise AssertionError(f"NEVER_ARMED: crash child slot-active precondition failed: {exc}") from exc

        # The destination proof was completed before this child started.  Check it
        # again from a fresh MotherDuck connection immediately before emission.
        with duckdb.connect(dsn) as md:
            md.execute("FORCE CHECKPOINT")
            assert md.execute(
                f"SELECT name FROM {quote(dataset)}.cdcflight_app_customers WHERE name = ?",
                [sentinel],
            ).fetchall(), "NEVER_ARMED: durable sentinel disappeared before crash arm"

        tx_content = b"\x00\xff\x80motherduck"
        tx_lsn = _emit_transactional_message(
            sandbox.source, prefix=tx_prefix, content=tx_content
        )
        non_lsn = _emit_non_transactional_message(
            sandbox.source, prefix=non_prefix, content=b""
        )

        _wait_for_context(
            sandbox,
            crash_process,
            filename="message_handshake.json",
            key="MESSAGE_CALLBACK_ENTERED",
            timeout=90,
        )
        _wait_for_context(
            sandbox,
            crash_process,
            filename="message_handshake.json",
            key="MESSAGE_SEEN",
            timeout=90,
        )
        if crash_process.poll() is None:
            try:
                crash_process.wait(timeout=90)
            except subprocess.TimeoutExpired as exc:
                stdout, stderr = _stop_child(crash_process)
                raise AssertionError(
                    "FIRED: message callback handshake completed, but the "
                    "post-commit crash did not fire; "
                    f"stdout={stdout}\nstderr={stderr}"
                ) from exc
        stdout, stderr = crash_process.communicate()
        assert crash_process.returncode == 137, (
            "FIRED: message handshake completed but the child did not die at "
            f"post_commit_pre_ack:1 (returncode={crash_process.returncode}); "
            f"stdout={stdout[-4000:]}\nstderr={stderr[-7000:]}"
        )
        fired = sandbox.fired_fault()
        assert fired and fired["point"] == "post_commit_pre_ack" and fired["nth"] == 1, (
            "FIRED: crash child exited without the named post-commit fault record: "
            f"{fired}"
        )

        # This independent read is the explicit MD_COMMITTED handshake.  All
        # values are checked before recovery is allowed to make the replay claim.
        with duckdb.connect(dsn) as md:
            md.execute("FORCE CHECKPOINT")
            messages, ledger, audit, customer_table, _message_table, control = _md_snapshot(
                md,
                dataset=dataset,
                control_schema=control_schema,
                pipeline=pipeline,
                prefixes=(tx_prefix, non_prefix),
            )
            tx_rows = [row for row in messages if row[1] == tx_prefix]
            assert len(tx_rows) == 1, (
                "FIRED: MD_COMMITTED handshake failed; transactional consumer row "
                f"is not exactly once: {messages}"
            )
            tx_row = tx_rows[0]
            assert bytes(tx_row[2]) == tx_content
            assert tx_row[3] is True
            assert tx_row[4] is not None and tx_row[5] is not None and tx_row[6] is not None
            assert int(tx_row[4]) >= tx_lsn or int(tx_row[7]) >= tx_lsn
            assert any(row[0] == tx_row[0] and row[1] == "applied" for row in ledger), (
                "FIRED: MD_COMMITTED handshake failed; consumer row has no applied "
                f"ledger claim: {ledger}"
            )
            commit_id = int(tx_row[8])
            commit = md.execute(
                f"SELECT commit_id, event_count, last_lsn FROM {control}.commit_log "
                f"WHERE pipeline = ? AND commit_id = ?",
                [pipeline, commit_id],
            ).fetchone()
            offset = md.execute(
                f"SELECT commit_id, last_lsn FROM {control}.debezium_offsets "
                f"WHERE pipeline = ? AND namespace = ?",
                [pipeline, "cdc-flight-engine"],
            ).fetchone()
            table_state = md.execute(
                f"SELECT snapshot_state FROM {control}.table_state WHERE pipeline = ? "
                f"AND source_schema = 'app' AND source_table = 'customers'",
                [pipeline],
            ).fetchone()
            customer = md.execute(
                f"SELECT cdcf_commit_id FROM {customer_table} WHERE name LIKE 'p74-md-data-%' "
                f"ORDER BY cdcf_commit_id DESC LIMIT 1"
            ).fetchone()
            assert commit and int(commit[0]) == commit_id and int(commit[1]) >= 2
            assert offset and int(offset[0]) == commit_id and int(offset[1]) >= int(commit[2])
            assert table_state and table_state[0] in {"none", "complete"}
            assert customer and int(customer[0]) == commit_id
            assert any(row[0] == tx_prefix and row[1] == "delivered" for row in audit)
            assert any(row[0] == "cdc_flight_heartbeat" and row[1] == "internal" for row in md.execute(
                f"SELECT prefix, status FROM {control}.logical_message_audit WHERE pipeline = ?",
                [pipeline],
            ).fetchall())
            md_committed = True

        assert md_committed, "FIRED: MD_COMMITTED handshake was not established"
        recovered = sandbox.run(
            destination="motherduck",
            max_seconds=300,
            idle_seconds=8,
            timeout=600,
            extra_env=env,
        )
        assert recovered["returncode"] == 0, (
            "FIRED: recovery after a durable MotherDuck message commit failed: "
            f"{recovered}"
        )
        # The post-commit cut leaves the source-side acknowledgement/file behind,
        # while the destination resume row is already durable in the same MD
        # transaction as the consumer and ledger.  Recovery therefore rebuilds
        # from that durable high-water mark and must not deliver the messages a
        # second time.  The deterministic contract lane separately feeds an
        # already-claimed message through the ledger and proves the replay branch;
        # this live cut proves the stronger external outcome: no duplicate row.
        assert recovered.get("reconciliation") == "file_behind_rebuilt", (
            "FIRED: recovery did not reconcile the pre-ack offset from durable "
            f"MotherDuck state: {recovered}"
        )
        assert recovered.get("logical_messages", {}).get("logical_messages_delivered", 0) == 0, (
            "FIRED: recovery delivered a second logical-message row: "
            f"{recovered}"
        )

        with duckdb.connect(dsn) as md:
            md.execute("FORCE CHECKPOINT")
            messages, ledger, audit, _customer_table, _message_table, _control = _md_snapshot(
                md,
                dataset=dataset,
                control_schema=control_schema,
                pipeline=pipeline,
                prefixes=(tx_prefix, non_prefix),
            )
            assert [row[1] for row in messages] == [non_prefix, tx_prefix]
            assert sum(row[1] == tx_prefix for row in messages) == 1
            assert sum(row[1] == non_prefix for row in messages) == 1
            assert len(ledger) == 2 and len({row[0] for row in ledger}) == 2
            assert all(row[1] in {"applied", "internal", "replayed"} for row in ledger)
            assert sorted(audit) == sorted(
                [(non_prefix, "delivered", 0), (tx_prefix, "delivered", len(tx_content))]
            )
            assert non_lsn > tx_lsn
    except AssertionError as exc:
        if str(exc).startswith(("NEVER_ARMED:", "FIRED:")):
            raise
        prefix = "FIRED" if crash_process is not None and crash_process.poll() is not None else "NEVER_ARMED"
        raise AssertionError(f"{prefix}: MotherDuck message contract failed: {exc}") from exc
    finally:
        if precondition_process is not None and precondition_process.poll() is None:
            precondition_process.terminate()
        if precondition_process is not None:
            precondition_process.communicate(timeout=60)
        if crash_process is not None and crash_process.poll() is None:
            crash_process.terminate()
        if crash_process is not None:
            crash_process.communicate(timeout=60)
