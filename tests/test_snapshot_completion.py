"""The single owner of snapshot-phase completion evidence."""

from __future__ import annotations

from types import SimpleNamespace

from cdc_flight.assembler import UNIT_SNAPSHOT_CHUNK
from cdc_flight.snapshot_completion import (
    SNAPSHOT_COMPLETION,
    SnapshotCompletion,
)


def _snapshot_unit(*, table: str = "customers", last: bool = False):
    return SimpleNamespace(
        kind=UNIT_SNAPSHOT_CHUNK,
        schema="app",
        table=table,
        snapshot_last=last,
        fenced=False,
    )


def test_declared_completion_machine_has_one_terminal_definition():
    assert SNAPSHOT_COMPLETION.initial == "pending"
    assert SNAPSHOT_COMPLETION.terminal == {"not_required", "record_complete", "empty_complete"}
    assert ("pending", "record_complete") in SNAPSHOT_COMPLETION.edges
    assert ("pending", "empty_complete") in SNAPSHOT_COMPLETION.edges


def test_nonempty_snapshot_completes_only_after_committed_terminal_record():
    completion = SnapshotCompletion.full_snapshot()

    completion.observe_committed_group([_snapshot_unit(last=True)], snapshot_active=False)

    assert completion.phase_ended is True
    assert completion.state == "record_complete"
    assert completion.tables_seen == {"app.customers"}
    assert completion.marker_seen is True


def test_terminal_marker_waits_for_the_shadow_swap_to_finish():
    completion = SnapshotCompletion.full_snapshot()

    completion.observe_committed_group([_snapshot_unit(last=True)], snapshot_active=True)

    assert completion.phase_ended is False
    assert completion.marker_seen is True
    completion.observe_committed_group([], snapshot_active=False)
    assert completion.phase_ended is True


def test_legitimate_all_empty_snapshot_completes_from_streaming_evidence():
    completion = SnapshotCompletion.full_snapshot()

    completion.observe_source_streaming()

    assert completion.phase_ended is True
    assert completion.state == "empty_complete"
    assert completion.tables_seen == set()


def test_empty_evidence_cannot_complete_after_snapshot_records_arrive():
    completion = SnapshotCompletion.full_snapshot()
    completion.observe_committed_group([_snapshot_unit()], snapshot_active=True)

    completion.observe_source_streaming()

    assert completion.phase_ended is False
    assert completion.state == "pending"


def test_streaming_only_run_has_no_snapshot_obligation():
    completion = SnapshotCompletion.streaming_only()

    assert completion.phase_ended is True
    assert completion.state == "not_required"
