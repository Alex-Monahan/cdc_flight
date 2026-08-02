"""The single owner of snapshot-phase completion evidence."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdc_flight.assembler import UNIT_SNAPSHOT_CHUNK
from cdc_flight.snapshot_completion import (
    SNAPSHOT_COMPLETION,
    SnapshotCompletion,
    SnapshotObservationError,
)


def _snapshot_unit(
    *, table: str = "customers", last: bool = False, event_count: int = 1
):
    return SimpleNamespace(
        kind=UNIT_SNAPSHOT_CHUNK,
        schema="app",
        table=table,
        snapshot_last=last,
        fenced=False,
        event_count=event_count,
    )


def test_declared_completion_machine_has_one_terminal_definition():
    assert SNAPSHOT_COMPLETION.states == (
        "awaiting_callbacks",
        "callbacks_started",
        "completion_notified",
        "callbacks_complete",
        "not_required",
    )
    assert SNAPSHOT_COMPLETION.terminal == {"callbacks_complete", "not_required"}
    assert SNAPSHOT_COMPLETION.edges == {
        ("awaiting_callbacks", "callbacks_started"),
        ("awaiting_callbacks", "awaiting_callbacks"),
        ("callbacks_started", "callbacks_started"),
        ("callbacks_started", "completion_notified"),
        ("callbacks_started", "callbacks_complete"),
        ("completion_notified", "completion_notified"),
        ("completion_notified", "callbacks_complete"),
        ("not_required", "not_required"),
    }


def test_committed_debezium_last_marker_is_diagnostic_not_completion():
    completion = SnapshotCompletion.full_snapshot({"app.customers"})

    completion.observe_committed_group([_snapshot_unit(last=True)], snapshot_active=False)

    assert completion.phase_ended is False
    assert completion.state == "awaiting_callbacks"
    assert completion.tables_seen == {"app.customers"}
    assert completion.marker_seen is True


def test_terminal_marker_waits_for_the_shadow_swap_to_finish():
    completion = SnapshotCompletion.full_snapshot({"app.customers"})

    completion.observe_committed_group([_snapshot_unit(last=True)], snapshot_active=True)

    assert completion.phase_ended is False
    assert completion.marker_seen is True
    completion.observe_committed_group([], snapshot_active=False)
    assert completion.phase_ended is False


def test_direct_snapshot_callbacks_complete_an_all_empty_expected_set():
    completion = SnapshotCompletion.full_snapshot({"app.customers", "app.orders"})

    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.orders",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )
    completion.observe_notification("COMPLETED", {})

    assert completion.phase_ended is True
    assert completion.state == "callbacks_complete"
    assert completion.tables_seen == set()
    assert completion.callback_tables == {"app.customers", "app.orders"}


def test_source_streaming_is_not_a_completion_observation():
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_committed_group([_snapshot_unit()], snapshot_active=True)

    with pytest.raises(SnapshotObservationError, match="source streaming"):
        completion.observe_source_streaming()

    assert completion.phase_ended is False
    assert completion.state == "awaiting_callbacks"


def test_terminal_callback_waits_for_load_delayed_row_callbacks():
    """The global callback is positive evidence, but queued rows must be durable."""
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "20",
        },
    )

    completion.observe_notification("COMPLETED", {})
    assert completion.state == "completion_notified"
    assert completion.completed is False

    completion.observe_committed_group(
        [_snapshot_unit(event_count=4)], snapshot_active=False
    )
    assert completion.state == "completion_notified"
    completion.observe_committed_group(
        [_snapshot_unit(event_count=16)], snapshot_active=False
    )
    assert completion.state == "callbacks_complete"
    assert completion.completed is True


def test_completed_callback_refuses_a_missing_per_table_terminal_mark():
    completion = SnapshotCompletion.full_snapshot({"app.customers", "app.orders"})
    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )

    with pytest.raises(SnapshotObservationError, match=r"app\.orders"):
        completion.observe_notification("COMPLETED", {})


@pytest.mark.parametrize("unexpected", ["SKIPPED", "ABORTED", "BANANA"])
def test_required_snapshot_refuses_every_unexpected_terminal_observation(unexpected):
    completion = SnapshotCompletion.full_snapshot({"app.customers"})

    with pytest.raises(SnapshotObservationError, match=unexpected):
        completion.observe_notification(unexpected, {})


def test_streaming_only_run_has_no_snapshot_obligation():
    completion = SnapshotCompletion.streaming_only()

    assert completion.phase_ended is True
    assert completion.state == "not_required"
