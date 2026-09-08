"""§3.3 live stock proof: real DML lands inside a real incremental scan."""

from __future__ import annotations

import contextlib
import json
import os
import re
from dataclasses import asdict

import psycopg
import pytest
from support.live_stock import (
    BoundedSourceCommitLedger,
    LiveStockHarness,
    SourceTransactionWriter,
    assert_exact_rows,
)

from cdc_flight import naming

pytestmark = pytest.mark.slow

SELECTED_TABLE = "p3a_scan_boundary"
UNRELATED_TABLE = "orders"
CAPTURED_TABLES = f"{SELECTED_TABLE},{UNRELATED_TABLE}"
SCAN_ROWS = 5_000
REQUEST_ID = "p3a-scan-boundary-request"
SIGNAL_ID = "p3a-scan-boundary-signal"
UPDATE_LABEL = "p3a-update"
DELETE_LABEL = "p3a-delete"
INSERT_LABEL = "p3a-insert"
UNRELATED_LABEL = "p3a-unrelated"


def _worker_namespace() -> tuple[str, str]:
    raw_worker = os.environ.get("PYTEST_XDIST_WORKER", "serial")
    worker = re.sub(r"[^a-z0-9_]", "_", raw_worker.lower()).strip("_") or "serial"
    return f"_cdc_flight_{worker}", f"cdc_raw_{worker}"


def test_stock_dml_commits_inside_scan_boundary_and_publishes_one_image(sandbox):
    """A stock scan and ordinary CDC converge after three separate source commits."""
    control_schema, dataset = _worker_namespace()
    pipeline = f"{sandbox.env['CDC_PIPELINE_NAME']}_{control_schema.rsplit('_', 1)[-1]}"
    sandbox.env.update(
        {
            "CDC_PIPELINE_NAME": pipeline,
            "CDC_CONTROL_SCHEMA": control_schema,
            "CDC_DATASET": dataset,
        }
    )
    sandbox.reseed()
    sandbox.sql(
        [
            f"CREATE TABLE app.{SELECTED_TABLE} ("
            "id bigint PRIMARY KEY, marker text NOT NULL, value integer NOT NULL)",
            f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{SELECTED_TABLE}",
            f"INSERT INTO app.{SELECTED_TABLE} (id, marker, value) "
            "VALUES (1, 'seed-1', 1)",
        ],
        one_transaction=True,
    )

    baseline = sandbox.run(
        reset_state=True,
        max_seconds=180,
        idle_seconds=6,
        extra_env={
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_TABLES": CAPTURED_TABLES,
        },
    )
    assert baseline["stop_reason"] in {"idle", "engine_finished"}, baseline

    # Keep the setup run short, then seed the large real source image while the
    # pipeline is stopped. The subsequent stock run therefore opens its actual
    # incremental table scan over all 5,000 rows.
    sandbox.sql(
        [
            f"INSERT INTO app.{SELECTED_TABLE} (id, marker, value) "
            "SELECT i, CASE WHEN i = 2 THEN 'p3a-delete' "
            "ELSE 'seed-' || i::text END, i::integer "
            f"FROM generate_series(2, {SCAN_ROWS}) AS rows(i)",
        ],
        one_transaction=True,
    )

    harness = LiveStockHarness(
        sandbox,
        control_schema=control_schema,
        dataset=dataset,
        pipeline=pipeline,
        source_table=SELECTED_TABLE,
        signal_tables=(f"app.{SELECTED_TABLE}",),
        request_id=REQUEST_ID,
        signal_id=SIGNAL_ID,
    )
    process = None
    ledger: BoundedSourceCommitLedger | None = None
    stdout = stderr = ""
    try:
        admitted_signal, run_ids = harness.admit_and_publish()
        assert admitted_signal == SIGNAL_ID
        assert len(run_ids) == 1

        # This is the ordinary executable and stock connector path. The source signal
        # is already durable; the process must discover it through its real slot.
        harness.start_normal_stock(captured_tables=CAPTURED_TABLES)
        process = harness.process
        boundary = harness.wait_for_scan_boundary()
        assert boundary.state == "loading"
        assert boundary.notification_status in {"STARTED", "IN_PROGRESS"}
        assert boundary.table_state == "in_progress"
        assert boundary.shadow_table

        # The diagnostic observer starts only after the durable STARTED/IN_PROGRESS
        # boundary. It is bounded to the writer interval below and never replaces the
        # production source owner.
        ledger = BoundedSourceCommitLedger.open(sandbox)
        writer = SourceTransactionWriter(sandbox.source.dsn)
        writer.commit(
            UPDATE_LABEL,
            "update",
            f"UPDATE app.{SELECTED_TABLE} SET marker = %s, value = %s WHERE id = 1",
            (UPDATE_LABEL, -1),
        )
        writer.commit(
            DELETE_LABEL,
            "delete",
            f"DELETE FROM app.{SELECTED_TABLE} WHERE id = 2",
        )
        writer.commit(
            INSERT_LABEL,
            "insert",
            f"INSERT INTO app.{SELECTED_TABLE} (id, marker, value) VALUES (%s, %s, %s)",
            (SCAN_ROWS + 1, INSERT_LABEL, SCAN_ROWS + 1),
        )
        # `orders` is not in the stock signal. Its ordinary CDC update must continue
        # while the selected table is routed to the retained shadow.
        writer.commit(
            UNRELATED_LABEL,
            "ordinary_cdc",
            "UPDATE app.orders SET note = %s WHERE id = 1",
            (UNRELATED_LABEL,),
        )

        assert writer.commits
        assert all(
            commit.committed_at_monotonic_ns > boundary.observed_at_monotonic_ns
            for commit in writer.commits
        ), {
            "boundary": asdict(boundary),
            "writer_commits": [asdict(commit) for commit in writer.commits],
        }

        expected_labels = {UPDATE_LABEL, DELETE_LABEL, INSERT_LABEL, UNRELATED_LABEL}
        source_commits = ledger.read(
            expected_labels,
            evidence={
                UPDATE_LABEL: "marker[text]:'p3a-update'",
                DELETE_LABEL: "DELETE: id[bigint]:2",
                INSERT_LABEL: "marker[text]:'p3a-insert'",
                UNRELATED_LABEL: "note[text]:'p3a-unrelated'",
            },
        )
        assert ledger.consuming_reads == 1
        assert len({fact.xid for fact in source_commits.values()}) == len(expected_labels)
        assert len({fact.commit_lsn for fact in source_commits.values()}) == len(expected_labels)

        terminal = harness.wait_for_terminal()
        assert terminal.state == "complete"
        assert terminal.notification_status == "COMPLETED"
        assert terminal.table_state == "complete"

        stdout, stderr = process.communicate(timeout=240)
        assert process.returncode == 0, (stdout[-3000:], stderr[-6000:])
        summary = sandbox.last_summary()
        assert summary["stop_reason"] in {"idle", "engine_finished"}, summary

        trace = summary.get("backfill_notification_trace", [])
        selected_trace = [
            entry
            for entry in trace
            if entry.get("table") == f"app.{SELECTED_TABLE}"
            or entry.get("observation") in {"STARTED", "COMPLETED"}
        ]
        assert selected_trace, json.dumps(trace, sort_keys=True, default=str)
        observations = [entry["observation"] for entry in selected_trace]
        assert "STARTED" in observations or "IN_PROGRESS" in observations, trace
        terminal_notifications = [
            entry for entry in selected_trace if entry["observation"] == "TABLE_SCAN_COMPLETED"
        ]
        assert len(terminal_notifications) == 1, trace
        terminal_notification = terminal_notifications[0]
        writer_commit_ns = max(
            commit.committed_at_monotonic_ns for commit in writer.commits
        )
        assert terminal_notification["observed_at_monotonic_ns"] > writer_commit_ns, {
            "trace": selected_trace,
            "writer_commits": [asdict(commit) for commit in writer.commits],
        }
        # The production callback is received first; the durable sidecar observation
        # follows the transaction that applies that terminal notification.
        assert terminal.observed_at_monotonic_ns > terminal_notification[
            "observed_at_monotonic_ns"
        ]

        target = naming.destination_table("cdcflight", "app", SELECTED_TABLE)
        shadow = naming.shadow_table(target)
        with psycopg.connect(sandbox.source.dsn) as source:
            source_selected = source.execute(
                f"SELECT id, marker, value FROM app.{SELECTED_TABLE} ORDER BY id"
            ).fetchall()
            source_unrelated = source.execute(
                "SELECT id, customer_id, status, total_amount, currency, note "
                "FROM app.orders ORDER BY id"
            ).fetchall()
        destination_selected = sandbox.duck_query(
            f'SELECT id, marker, value FROM "{dataset}"."{target}" ORDER BY id'
        )
        destination_unrelated = sandbox.duck_query(
            f'SELECT id, customer_id, status, total_amount, currency, note '
            f'FROM "{dataset}"."cdcflight_app_orders" ORDER BY id'
        )
        assert_exact_rows(source_selected, destination_selected, label=SELECTED_TABLE)
        assert_exact_rows(source_unrelated, destination_unrelated, label=UNRELATED_TABLE)

        assert all(row[0] != 2 for row in destination_selected)
        assert (1, UPDATE_LABEL, -1) in destination_selected
        assert (SCAN_ROWS + 1, INSERT_LABEL, SCAN_ROWS + 1) in destination_selected
        assert any(row[-1] == UNRELATED_LABEL for row in destination_unrelated)

        run_rows = sandbox.duck_query(
            f"SELECT state, notification_status, shadow_table FROM \"{control_schema}\".\"backfill_runs\" "
            "WHERE pipeline = ? AND request_id = ? AND source_table = ?",
            [pipeline, REQUEST_ID, SELECTED_TABLE],
        )
        assert run_rows == [("complete", "COMPLETED", shadow)]
        lifecycle_rows = sandbox.duck_query(
            f"SELECT snapshot_state FROM \"{control_schema}\".\"table_state\" "
            "WHERE pipeline = ? AND source_schema = 'app' AND source_table = ?",
            [pipeline, SELECTED_TABLE],
        )
        assert lifecycle_rows == [("complete",)]
        assert summary["snapshot_swaps"] == 1, summary
        physical = sandbox.duck_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name IN (?, ?) ORDER BY table_name",
            [dataset, target, shadow],
        )
        assert physical == [(target,)], physical

        print(
            "ROUND_A_STOCK_TRACE "
            + json.dumps(
                {
                    "boundary": asdict(boundary),
                    "terminal": asdict(terminal),
                    "writer_commits": [asdict(commit) for commit in writer.commits],
                    "source_commits": {
                        label: asdict(fact) for label, fact in source_commits.items()
                    },
                    "ledger": {
                        "slot": ledger.slot,
                        "lower_watermark": ledger.lower_watermark,
                        "consistent_point": ledger.consistent_point,
                        "upper_watermark": ledger.upper_watermark,
                        "peek_polls": ledger.peek_polls,
                        "consuming_reads": ledger.consuming_reads,
                    },
                    "notification_trace": trace,
                },
                sort_keys=True,
                default=str,
            )
        )
    finally:
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                process.communicate(timeout=120)
        if ledger is not None:
            ledger.close()
