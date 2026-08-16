"""Rubric 1.3 / 4.2 — the commit protocol itself, driven in process.

Three review findings live here, all of them about the *protocol* rather than the
data: the commit→ack window must contain nothing but the acknowledgement
(Codex 7), `commit_id` must be safe when a destination hosts more than one
pipeline (Codex 9), and a group must be one destination transaction across every
table it touches (rubric 1.3, asserted here at the mechanism level; the
end-to-end proof is the MotherDuck observer test).

Re-snapshot streaming overlap is intentionally outside this protocol. Its transaction
is proven whole by the assembler, then dropped before ``OpenGroup`` admission; only the
snapshot image uses the destination transaction and publishes the resume point. The
overlap tests below cover empty/open/boundary, spill thresholds, soft triggers, and the
r15 owner-overtake sequence as behavior: no destination rows, control rows, spill rows,
or resume advancement are attributable to the discarded stream.
"""

from __future__ import annotations

import json

import pytest
from support.applier_lab import (
    DATASET,
    Lab,
    begin,
    data,
    end,
    fixture_descriptors,
    heartbeat,
    keyed,
    snap,
)

from cdc_flight import commit_protocol, faults
from cdc_flight.envelope import KIND_SNAPSHOT_BOUNDARY, PendingRecord
from cdc_flight.errors import OffsetFlushFailed
from cdc_flight.run_state import COMMIT_ACK
from cdc_flight.snapshot_completion import SnapshotCompletion, SnapshotObservationError


def _patch_production_handle_for_pending_records(monkeypatch):
    """Drive ``Applier._handle`` with the lab's decoded records.

    ``Lab.feed`` deliberately bypasses the Debezium envelope boundary. These tests
    need the production empty-poll branch, so only the two decoders are adapted at
    the test seam; assembly, admission, commit and acknowledgement remain real.
    """
    from cdc_flight import applier as applier_module

    real_decode = applier_module.decode
    real_decode_notification = applier_module.decode_notification

    def decode(raw, **kwargs):
        if isinstance(raw, PendingRecord):
            return raw
        return real_decode(raw, **kwargs)

    def decode_notification(raw, **kwargs):
        if isinstance(raw, PendingRecord):
            return None
        return real_decode_notification(raw, **kwargs)

    monkeypatch.setattr(applier_module, "decode", decode)
    monkeypatch.setattr(applier_module, "decode_notification", decode_notification)


@pytest.fixture
def lab(tmp_path):
    boxes: list[Lab] = []

    def _make(name: str = "lab", **cfg) -> Lab:
        box = Lab(tmp_path / f"{name}.duckdb", **cfg)
        boxes.append(box)
        return box

    yield _make
    for box in boxes:
        box.close()


def test_shutdown_seals_callback_admission_and_records_late_batches(lab, monkeypatch):
    """Round 8 MAJOR-1: shutdown is a callback boundary, not only a timer stop.

    A Debezium callback which arrives after the seal must do no decode, destination SQL,
    state mutation, or acknowledgement.  The rejection is retained in the run stats so
    a late engine is observable rather than silently mistaken for an empty batch.
    """
    box = lab()
    handled: list[object] = []
    monkeypatch.setattr(
        box.applier,
        "_handle",
        lambda records, committer: handled.append((records, committer)),
    )

    box.applier.shutdown(reason="test_retirement")
    box.applier.handle_batch([object(), object()], box.committer)

    assert handled == []
    stats = box.applier.stats()
    assert stats["callback_boundary"] == "sealed"
    assert stats["callback_seal_reason"] == "test_retirement"
    assert stats["callback_batches_rejected"] == 1
    assert stats["callback_records_rejected"] == 2


class _SnapshotNotification:
    """Minimal ordered notification with a real Connect offset shape."""

    class _Map(dict):
        class _Entry:
            def __init__(self, key, value):
                self._key = key
                self._value = value

            def getKey(self):
                return self._key

            def getValue(self):
                return self._value

        def entrySet(self):
            return [self._Entry(key, value) for key, value in self.items()]

    def __init__(self, observation: str, lsn: int, data: dict[str, str] | None = None):
        self._topic = "cdcflight.cdc_flight_snapshot_notifications"
        self._value = json.dumps(
            {
                "aggregate_type": "Initial Snapshot",
                "type": observation,
                "additional_data": data or {},
            }
        )
        self._partition = self._Map(server="cdcflight")
        self._offset = self._Map(lsn=lsn, lsn_proc=lsn, ts_usec=lsn * 1000)

    def destination(self):
        return self._topic

    def value(self):
        return self._value

    def key(self):
        return None

    def sourceRecord(self):
        return self

    def sourcePartition(self):
        return self._partition

    def sourceOffset(self):
        return self._offset


def test_invalid_terminal_refuses_before_resume_commit_or_ack(lab):
    """Invariant O: a refused COMPLETED observation crosses no durable boundary."""
    box = lab()
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot(
        {"app.customers", "app.orders"}
    )

    box.applier._handle([_SnapshotNotification("STARTED", 101)], box.committer)
    with pytest.raises(SnapshotObservationError, match=r"app\.orders"):
        box.applier._handle([_SnapshotNotification("COMPLETED", 102)], box.committer)

    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.debezium_offsets") == 0


def test_valid_terminal_offset_commits_before_pending_notifications_are_acked(lab):
    """The terminal offset is durable, but its raw callback is acked only at completion."""
    box = lab()
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})

    box.applier._handle([_SnapshotNotification("STARTED", 101)], box.committer)
    box.feed([snap("customers", 100, ident=1, value="s", marker="last")])
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                102,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "1",
                },
            ),
            _SnapshotNotification("COMPLETED", 103),
        ],
        box.committer,
    )

    assert box.applier.snapshot_completed is True
    point = box.applier.resume_point
    assert point.last_lsn == 103
    assert point.offset["lsn"] == 103
    assert box.applier.commit_groups == 1
    # One row record plus the three pending notifications; the synthetic boundary has
    # raw=None and therefore cannot be acknowledged as an ordinary control record.
    assert box.committer.marked == 4
    assert box.committer.batches == 1


def test_every_snapshot_ack_is_guarded_and_empty_snapshot_arms_flush_verifier(lab):
    """Pending notification offsets share the same verified post-COMMIT path.

    The all-empty shape is the canary: the synthetic boundary has no ordinary raw
    record, so the verifier must count the three pending notifications instead.
    """
    box = lab()
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    verifier = _RecordingVerifier()
    box.applier.verifier = verifier

    class _GuardedCommitter:
        def __init__(self):
            self.marked = 0
            self.batches = 0
            self.window_states: list[bool] = []

        def markProcessed(self, record):
            self.window_states.append(COMMIT_ACK.active)
            self.marked += 1

        def markBatchFinished(self):
            self.batches += 1

    committer = _GuardedCommitter()
    box.applier._handle([_SnapshotNotification("STARTED", 101)], committer)
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                102,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "0",
                },
            ),
            _SnapshotNotification("COMPLETED", 103),
        ],
        committer,
    )

    assert committer.marked == 3
    assert committer.batches == 1
    assert committer.window_states == [True, True, True]
    assert verifier.before_calls == 1
    assert verifier.after_calls == 0

    # The comparison remains deferred until the next poll, but it is armed by the
    # notification acknowledgements even though the group carried no row record.
    box.applier._handle([], committer)
    assert verifier.after_calls == 1


def test_not_ready_terminal_boundary_refuses_streaming_phase_transition(lab):
    """A delayed final snapshot row cannot be mixed with the first stream unit."""
    box = lab(snapshot_chunk_events=1)
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    box.applier.snapshot_completion.observe_notification("STARTED", {})

    # One row is durable in the open snapshot group, but Debezium declared two.
    box.feed([snap("customers", 100, ident=1, marker="true")])
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "2",
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )
    assert box.applier.snapshot_completion.state == "completion_notified"

    with pytest.raises(SnapshotObservationError, match="streaming"):
        box.feed(
            [
                begin("stream-1", 300),
                data("stream-1", 1, 301, key={"id": 2}, after={"id": 2, "name": "c"}),
                end("stream-1", 1, 302, {"app.customers": 1}),
            ]
        )

    assert all(unit.kind != "txn" for unit in box.applier.group.units)

    # The delayed row can finish the original snapshot group and is not lost or
    # forced through a streaming route.
    box.feed([snap("customers", 200, ident=2, marker="last")])
    box.commit()
    assert box.applier.snapshot_completed is True


def test_resnapshot_fenced_overlap_survives_not_ready_terminal_boundary(lab):
    """A fenced overlap cannot force an under-counted snapshot boundary to commit."""
    box = lab(resnapshot=True, snapshot_chunk_events=1)
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    box.applier.snapshot_completion.observe_notification("STARTED", {})

    box.feed([snap("customers", 100, ident=1, marker="true")])
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "2",
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )
    assert box.applier.snapshot_completion.state == "completion_notified"
    assert [unit.kind for unit in box.applier.group.units] == [
        "snapshot_chunk",
        "control",
    ]

    box.feed(_streaming_transaction())

    assert box.applier.snapshot_completion.state == "completion_notified"
    assert [unit.kind for unit in box.applier.group.units] == [
        "snapshot_chunk",
        "control",
    ]
    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 1

    box.feed([snap("customers", 200, ident=2, marker="last")])
    box.commit()
    assert box.applier.snapshot_completed is True
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1


def test_resnapshot_fenced_spilled_overlap_does_not_start_destination_transaction(lab):
    """A fenced overlap never stages its prefix in the destination.

    The overlap is deliberately two events so the assembler crosses its configured
    threshold before the END marker makes the unit complete. The payload is dropped by
    the re-snapshot assembler path, so no destination owner exists to nest.
    """
    box = lab(resnapshot=True, snapshot_chunk_events=1, unit_spill_events=2)
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    box.applier.snapshot_completion.observe_notification("STARTED", {})

    box.feed([snap("customers", 100, ident=1, marker="true")])
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "2",
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )
    snapshot_group = box.applier.group

    overlap = [
        begin("stream-spilled", 300),
        keyed("stream-spilled", 1, 301, 2, "c"),
        keyed("stream-spilled", 2, 302, 3, "d"),
        end("stream-spilled", 2, 303, {"app.customers": 2}),
    ]
    box.feed(overlap)

    assert box.applier.spilled_events == 0
    assert box.applier.fenced_spilled_events == 0
    assert box.applier.group is snapshot_group
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0
    assert box.applier.snapshot_completion.state == "completion_notified"

    box.feed([snap("customers", 200, ident=2, marker="last")])
    box.commit()

    assert box.applier.snapshot_completed is True
    assert box.applier.commit_groups == 1
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0


def test_resnapshot_overlap_is_dropped_before_shared_commit_publication(lab):
    """A spilled overlap cannot reserve or publish the re-snapshot resume point.

    The throwaway slot's streaming transaction is already known to be discard-only.
    It must not become a destination owner merely because its prefix crosses the
    assembler threshold while a snapshot boundary is buffered. The later snapshot
    commit must publish its own terminal tuple, not a newer LSN paired with the
    boundary's older offset (the r15 overtake sequence).
    """
    box = lab(resnapshot=True, snapshot_chunk_events=1, unit_spill_events=2)
    _prepare_not_ready_snapshot_boundary(box)

    box.feed(
        [
            begin("stream-overtake", 300),
            keyed("stream-overtake", 1, 301, 2, "c"),
            keyed("stream-overtake", 2, 302, 3, "d"),
            end("stream-overtake", 2, 303, {"app.customers": 2}),
        ]
    )

    assert box.applier.group.units, "the snapshot group must remain buffered"
    assert box.applier.commit_groups == 0
    assert box.applier.last_commit_id == 0
    assert box.applier.resume_point.last_lsn == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0

    box.feed([snap("customers", 200, ident=2, marker="last")])
    box.commit()

    assert box.applier.commit_groups == 1
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1
    assert box.applier.resume_point.last_lsn == 201
    assert box.applier.resume_point.offset["lsn"] == 200
    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 2


def test_resnapshot_overlap_does_not_create_a_second_destination_owner(lab):
    """Multiple overlap units are dropped while the snapshot owner remains open."""
    box = lab(
        resnapshot=True,
        snapshot_chunk_events=2,
        unit_spill_events=2,
    )
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    box.applier.snapshot_completion.observe_notification("STARTED", {})

    box.feed(
        [
            snap("customers", 100, ident=1, marker="true"),
            snap("customers", 101, ident=2, marker="true"),
        ]
    )
    assert box.applier.group.txn_open is True
    assert box.applier.group.spill_commit_id == 1

    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "3",
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )

    box.feed(
        [
            begin("stream-spilled", 300),
            keyed("stream-spilled", 1, 301, 3, "c"),
            keyed("stream-spilled", 2, 302, 4, "d"),
            end("stream-spilled", 2, 303, {"app.customers": 2}),
        ]
    )

    assert box.applier.group.txn_open is True
    assert box.applier.fenced_spilled_events == 0
    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 2
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 2
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0

    box.feed([snap("customers", 200, ident=3, marker="last")])
    box.commit()
    assert box.applier.snapshot_completed is True
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1


def test_resnapshot_drops_empty_overlap_without_entering_streaming(lab):
    """An empty BEGIN/END overlap has no destination boundary at all."""
    box = lab(resnapshot=True, snapshot_chunk_events=1)
    _prepare_not_ready_snapshot_boundary(box)

    box.feed([begin("empty-overlap", 300), end("empty-overlap", 0, 301, {})])

    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 0
    assert box.applier.snapshot_completion.state == "completion_notified"
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0

    box.feed([snap("customers", 200, ident=2, marker="last")])
    box.commit()

    assert box.applier.snapshot_completed is True
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1


@pytest.mark.parametrize("anchor", ["pre_commit", "post_commit_pre_ack"])
def test_fault_anchors_have_no_path_for_a_discarded_overlap(lab, monkeypatch, anchor):
    """Discard-only overlap does not arm destination commit fault anchors."""
    from cdc_flight import faults

    box = lab(resnapshot=True, snapshot_chunk_events=1)
    _prepare_not_ready_snapshot_boundary(box)
    monkeypatch.setenv("CDC_FAULT_INJECT", f"{anchor}:1:raise")
    faults.refresh()

    box.feed(_streaming_transaction())

    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert box.applier.snapshot_completion.state == "completion_notified"


def _complete_empty_snapshot(box):
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_notification("STARTED", {})
    completion.observe_notification(
        "TABLE_SCAN_COMPLETED",
        {
            "scanned_collection": "app.customers",
            "status": "SUCCEEDED",
            "total_rows_scanned": "0",
        },
    )
    completion.observe_notification("COMPLETED", {})
    box.applier.snapshot_completion = completion


def _snapshot_boundary(lsn: int = 100) -> PendingRecord:
    return PendingRecord(
        raw=None,
        kind=KIND_SNAPSHOT_BOUNDARY,
        topic="cdcflight.cdc_flight_snapshot_notifications",
        nbytes=0,
        lsn=lsn,
        source_partition={"server": "cdcflight"},
        source_offset={"lsn": lsn, "lsn_proc": lsn, "ts_usec": lsn * 1000},
    )


def _add_snapshot_boundary(box, lsn: int = 100) -> None:
    for unit in box.applier.assembler.feed_snapshot_boundary(_snapshot_boundary(lsn)):
        box.applier._add_unit(unit)


def _streaming_transaction():
    return [
        begin("stream-1", 300),
        data(
            "stream-1",
            1,
            301,
            key={"id": 2},
            after={"id": 2, "name": "c"},
        ),
        end("stream-1", 1, 302, {"app.customers": 1}),
    ]


def _prepare_not_ready_snapshot_boundary(box, *, declared: int = 2) -> None:
    box.applier.snapshot_completion = SnapshotCompletion.full_snapshot({"app.customers"})
    box.applier.snapshot_completion.observe_notification("STARTED", {})
    box.feed([snap("customers", 100, ident=1, marker="true")])
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": str(declared),
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )


def test_boundary_only_group_is_classified_as_snapshot_phase(lab):
    box = lab()
    _complete_empty_snapshot(box)

    _add_snapshot_boundary(box)

    assert len(box.applier.group.units) == 1
    assert box.applier.group.is_snapshot is True


def test_empty_group_cannot_admit_streaming_before_snapshot_completed(lab):
    box = lab(full_snapshot=True)

    with pytest.raises(SnapshotObservationError, match="streaming"):
        box.run(_streaming_transaction())

    assert box.applier.commit_groups == 0
    assert box.applier.resume_point.last_lsn == 0
    assert box.applier.snapshot_completion.state == "callbacks_started"
    assert box.applier.group.units == []


def test_boundary_and_first_streaming_unit_are_committed_separately(lab):
    box = lab()
    _complete_empty_snapshot(box)
    _add_snapshot_boundary(box)

    box.feed(_streaming_transaction())

    assert box.applier.commit_groups == 1
    assert [unit.kind for unit in box.applier.group.units] == ["txn"]
    assert box.applier.snapshot_completion.state == "streaming"
    assert box.applier.resume_point.last_lsn == 100

    box.commit()
    assert box.applier.commit_groups == 2
    assert box.applier.resume_point.last_lsn == 302


def test_streaming_refusal_precedes_commit_of_open_snapshot_group(lab):
    box = lab(full_snapshot=True, snapshot_chunk_events=1)

    box.feed([snap("customers", 100, ident=1, marker="true")])
    assert [unit.kind for unit in box.applier.group.units] == ["snapshot_chunk"]

    with pytest.raises(SnapshotObservationError, match="streaming"):
        box.feed(_streaming_transaction())

    assert box.applier.commit_groups == 0
    assert box.applier.resume_point.last_lsn == 0
    assert box.committer.marked == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert [unit.kind for unit in box.applier.group.units] == ["snapshot_chunk"]


def test_resnapshot_fences_streaming_unit_before_empty_group_admission(lab):
    """A throwaway-slot transaction is discarded even before snapshot rows exist."""
    box = lab(full_snapshot=True, resnapshot=True)

    box.run(_streaming_transaction())

    assert box.applier.commit_groups == 0
    assert box.applier.resume_point.last_lsn == 0
    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 1
    assert box.applier.snapshot_completion.state == "callbacks_started"
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert len(box.applier._pending_discarded_records) == 1


def test_resnapshot_discard_stays_pending_through_empty_poll_until_snapshot_commit(
    lab, monkeypatch
):
    """An empty production poll cannot advance the throwaway slot by itself."""
    box = lab(
        full_snapshot=True,
        resnapshot=True,
        ack_every_record=True,
    )
    _patch_production_handle_for_pending_records(monkeypatch)

    box.applier._handle(_streaming_transaction(), box.committer)
    assert len(box.applier._pending_discarded_records) == 3
    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert box.applier.commit_groups == 0

    # This is the reviewed production shape: the next callback is an empty poll.
    box.applier._handle([], box.committer)
    assert len(box.applier._pending_discarded_records) == 3
    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0

    # The first replacement snapshot group owns the acknowledgement boundary.
    box.applier._handle(
        [snap("customers", 100, ident=1, marker="last")], box.committer
    )
    assert box.applier.commit_groups == 1
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1
    assert box.applier._pending_discarded_records == []
    assert box.committer.marked == 4
    assert box.committer.batches == 1


def test_empty_resnapshot_terminal_group_durably_precedes_discard_ack(lab, monkeypatch):
    """No snapshot rows still get a durable terminal commit before acknowledgement."""
    box = lab(
        full_snapshot=True,
        resnapshot=True,
        ack_every_record=True,
    )
    _patch_production_handle_for_pending_records(monkeypatch)
    completion = SnapshotCompletion.full_snapshot({"app.customers"})
    completion.observe_notification("STARTED", {})
    box.applier.snapshot_completion = completion

    box.applier._handle(_streaming_transaction(), box.committer)
    box.applier._handle(
        [
            _SnapshotNotification(
                "TABLE_SCAN_COMPLETED",
                200,
                {
                    "scanned_collection": "app.customers",
                    "status": "SUCCEEDED",
                    "total_rows_scanned": "0",
                },
            ),
            _SnapshotNotification("COMPLETED", 201),
        ],
        box.committer,
    )

    assert box.applier.commit_groups == 1
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1
    assert box.applier.snapshot_completed is True
    assert box.applier._pending_discarded_records == []
    # Two raw terminal notifications plus the three raw records of the discarded
    # whole transaction; the synthetic boundary has no raw handle.
    assert box.committer.marked == 5
    assert box.committer.batches == 1


def test_non_snapshot_commit_cannot_discharge_pending_resnapshot_handles(
    lab, monkeypatch
):
    """A durable control group is not a replacement image or terminal policy."""
    box = lab(
        full_snapshot=True,
        resnapshot=True,
        ack_every_record=True,
    )
    _patch_production_handle_for_pending_records(monkeypatch)
    box.applier._handle(_streaming_transaction(), box.committer)
    box.applier.snapshot_completion = SnapshotCompletion.streaming_only()

    box.applier._handle([heartbeat(400)], box.committer)

    assert box.applier.commit_groups == 1
    assert box.committer.marked == 1
    assert box.committer.batches == 1
    assert len(box.applier._pending_discarded_records) == 3


def test_crash_between_snapshot_commit_and_discard_ack_keeps_handles_replayable(
    lab, monkeypatch
):
    """A crash cut before acknowledgement leaves the source records pending."""
    from cdc_flight import faults

    box = lab(
        full_snapshot=True,
        resnapshot=True,
        ack_every_record=True,
    )
    _patch_production_handle_for_pending_records(monkeypatch)
    box.applier._handle(_streaming_transaction(), box.committer)

    monkeypatch.setenv("CDC_FAULT_INJECT", "post_commit_pre_ack:1:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.applier._handle(
            [snap("customers", 100, ident=1, marker="last")], box.committer
        )

    # The replacement transaction committed, but no source handle crossed the cut.
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1
    assert box.committer.marked == 0
    assert box.committer.batches == 0
    assert len(box.applier._pending_discarded_records) == 3
    box.applier.shutdown(reason="simulated_crash")
    assert len(box.applier._pending_discarded_records) == 3


def test_pre_commit_cut_requires_resume_state_inside_the_data_transaction(
    lab, monkeypatch
):
    """A mutation moving resume state after COMMIT must fail this guard."""
    box = lab()
    _patch_production_handle_for_pending_records(monkeypatch)
    calls: list[tuple] = []
    real_write_resume_point = commit_protocol.destination.write_resume_point

    def observe_resume_write(*args, **kwargs):
        calls.append((args, kwargs))
        return real_write_resume_point(*args, **kwargs)

    monkeypatch.setattr(
        commit_protocol.destination, "write_resume_point", observe_resume_write
    )
    monkeypatch.setenv("CDC_FAULT_INJECT", "pre_commit:1:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.applier._handle(_streaming_transaction(), box.committer)

    assert calls, "resume state was not staged before the pre-COMMIT cut"
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 0
    assert box.scalar("SELECT count(*) FROM _cdc_flight.debezium_offsets") == 0


def test_resnapshot_fences_streaming_unit_after_open_snapshot_group(lab):
    """A fenced overlap is dropped and does not enter live streaming."""
    box = lab(full_snapshot=True, resnapshot=True)

    box.feed([snap("customers", 100, ident=1, marker="true")])
    box.feed(_streaming_transaction())

    assert box.applier.commit_groups == 0
    assert box.applier.resume_point.last_lsn == 0
    assert [unit.kind for unit in box.applier.group.units] == ["snapshot_chunk"]
    assert box.applier.snapshot_completion.state == "callbacks_started"

    box.commit()
    assert box.applier.commit_groups == 1
    assert box.applier.resume_point.last_lsn == 100
    assert box.applier.snapshot_completion.state == "callbacks_started"


@pytest.mark.parametrize("trigger_kind", ["size", "age"])
def test_resnapshot_overlap_during_age_and_size_trigger_stays_fenced(
    lab, trigger_kind
):
    """A pending soft close cannot turn the overlap into live streaming."""
    if trigger_kind == "size":
        box = lab(
            full_snapshot=True,
            resnapshot=True,
            commit_max_events=1,
            commit_max_age=60.0,
            snapshot_chunk_events=1,
        )
    else:
        box = lab(
            full_snapshot=True,
            resnapshot=True,
            commit_max_events=1000,
            commit_max_age=1.0,
            snapshot_chunk_events=1,
        )
    box.feed([snap("customers", 100, ident=1, marker="true")])
    if trigger_kind == "size":
        assert box.applier.group.events >= box.config.commit_max_events
    else:
        box.applier.group.opened_at -= 2.0
        assert box.applier._soft_trigger_hit() is True
    # This is the timer's exact request flag; the overlap is delivered before the
    # poll thread gets to honor it, which is the composition under test.
    box.applier.group.close_requested = True

    box.feed(_streaming_transaction())

    assert box.applier.fenced_units == 1
    assert box.applier.resnapshot_discarded_events == 1
    assert box.applier.snapshot_completion.state == "callbacks_started"
    assert box.applier.group.units[0].fenced is False
    box.commit()
    assert box.scalar("SELECT count(*) FROM _cdc_flight.commit_log") == 1


class _RecordingVerifier:
    """Stands in for `OffsetFlushVerifier` and records *when* it was consulted."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.before_calls = 0
        self.after_calls = 0
        self.marks_at_before: list[int] = []

    def before(self):
        self.before_calls += 1
        return ("fingerprint", self.before_calls)

    def after(self, before, *, marked):
        self.after_calls += 1
        if self.fail:
            raise OffsetFlushFailed("the offset file did not move")


# --------------------------------------------------------------------------- #
# the commit -> ack window (Codex 7)
# --------------------------------------------------------------------------- #
def test_the_flush_check_is_deferred_out_of_the_commit_to_ack_window(lab):
    """`verifier.before()` used to run *after* `COMMIT`, between the
    `markProcessed()` calls and `markBatchFinished()`, and it stats and sha256s
    `offsets.dat`; `verifier.after()` then hashed it again before the next poll.
    The binding principle says that window contains nothing else, and neither
    check is a loss-prevention prerequisite under Invariant O.

    So: the fingerprint is taken before the commit, and the comparison happens on
    the next batch — after Debezium has had its poll/commit opportunity.
    """
    box = lab()
    verifier = _RecordingVerifier()
    box.applier.verifier = verifier

    box.run([keyed("7", 1, 101, 1, "a"), end("7", 1, 102, {"app.customers": 1})])
    assert verifier.before_calls == 1
    assert verifier.after_calls == 0, (
        "the offset-file comparison ran inside the commit->ack window"
    )

    # The next batch is where it is consulted.
    box.applier._handle([], box.committer)
    assert verifier.after_calls == 1


def test_a_deferred_flush_failure_is_still_fatal(lab):
    """Deferring the check must not weaken it: the canary still fires."""
    box = lab()
    box.applier.verifier = _RecordingVerifier(fail=True)
    box.run([keyed("7", 1, 101, 1, "a"), end("7", 1, 102, {"app.customers": 1})])
    with pytest.raises(OffsetFlushFailed):
        box.applier._handle([], box.committer)


def test_a_flush_failure_is_reported_even_if_the_run_ends_first(lab):
    """The last group of a run has no "next batch", so shutdown checks it.

    It is recorded on `applier.error` rather than raised, because
    `drain_on_shutdown` runs in a `finally` and raising there would replace
    whatever exception is already in flight; the supervisor fails the run on
    `handler.error`.
    """
    box = lab()
    box.applier.verifier = _RecordingVerifier(fail=True)
    box.run([keyed("7", 1, 101, 1, "a"), end("7", 1, 102, {"app.customers": 1})])
    box.applier.drain_on_shutdown()
    assert isinstance(box.applier.error, OffsetFlushFailed)


class _OrderingConnection:
    """Forwards to a DuckDB connection, recording the transaction statements.

    A proxy rather than a monkeypatch: `DuckDBPyConnection.execute` is read-only.
    """

    def __init__(self, con, order: list[str]):
        self._con = con
        self._order = order

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and sql.strip().upper() in ("COMMIT", "BEGIN TRANSACTION", "ROLLBACK"):
            self._order.append(sql.strip().upper())
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


def test_the_acknowledgement_happens_after_the_commit_and_only_after_it(lab):
    """Invariant O, at the mechanism level: nothing is marked before `COMMIT`."""
    box = lab()
    order: list[str] = []
    box.applier.con = _OrderingConnection(box.con, order)
    box.applier.registry.con = box.applier.con

    class _Committer:
        def markProcessed(self, record):
            order.append("markProcessed")

        def markBatchFinished(self):
            order.append("markBatchFinished")

    box.applier._committer = _Committer()
    box.feed([keyed("7", 1, 101, 1, "a"), end("7", 1, 102, {"app.customers": 1})])
    box.applier.commit_group("test")

    assert order[0] == "BEGIN TRANSACTION"
    assert "COMMIT" in order
    commit_at = order.index("COMMIT")
    assert all(
        step not in ("markProcessed", "markBatchFinished") for step in order[:commit_at]
    ), f"Debezium was acknowledged before the destination commit: {order}"
    assert order[commit_at + 1 :] == ["markProcessed", "markBatchFinished"], (
        f"the commit->ack window contains something other than the acknowledgement: {order}"
    )


# --------------------------------------------------------------------------- #
# commit_id across pipelines (Codex 9)
# --------------------------------------------------------------------------- #
def test_two_pipelines_on_one_destination_do_not_contend_for_commit_ids(tmp_path):
    """`commit_id` was globally unique and allocated as `max(commit_id) + 1`.

    That allocation cannot be atomic here, and leases are per pipeline, so two
    *different, valid* pipelines raced into a primary-key failure: the loser rolled
    back safely, but a destination hosting more than one pipeline could not operate
    (Codex 9). The key is `(pipeline, commit_id)` now, allocated monotonically per
    pipeline — which is exactly the scope the lease already guarantees.
    """
    import duckdb

    from cdc_flight import destination as dest_mod
    from cdc_flight.applier import Applier
    from cdc_flight.config import ApplierConfig
    from cdc_flight.destination import Lease, ResumePoint

    con = duckdb.connect(str(tmp_path / "shared.duckdb"))
    dest_mod.ensure_control_schema(con)
    dest_mod.ensure_dataset(con, DATASET)

    appliers = []
    for name in ("alpha", "beta"):
        lease = Lease(name, ttl_seconds=600)
        lease.acquire(con)
        applier = Applier(
            con,
            pipeline=name,
            namespace=f"{name}-ns",
            dataset=DATASET,
            topic_prefix="cdcflight",
            offset_path=tmp_path / f"{name}.dat",
            resume_point=ResumePoint(),
            config=ApplierConfig(verify_offset_file=False),
            lease=lease,
            runner_id=f"{name}-runner",
            completion=SnapshotCompletion.streaming_only(),
            descriptor_provider=fixture_descriptors,
        )
        applier._committer = type(
            "C", (), {"markProcessed": lambda s, r: None, "markBatchFinished": lambda s: None}
        )()
        appliers.append(applier)

    try:
        # Interleaved: each pipeline writes its first group, then its second.
        for round_no in range(2):
            for index, applier in enumerate(appliers):
                txn = str(10 * (round_no + 1) + index)
                lsn = 100 * (round_no + 1) + index
                for unit in applier.assembler.feed(
                    data(txn, 1, lsn, table="customers", key={"id": index + 1},
                         after={"id": index + 1, "name": txn})
                ):
                    applier._add_unit(unit)
                for unit in applier.assembler.feed(
                    end(txn, 1, lsn + 1, {"app.customers": 1})
                ):
                    applier._add_unit(unit)
                applier.lease.acquire(con)  # each pipeline owns its own lease row
                applier.commit_group("test")

        rows = con.execute(
            "SELECT pipeline, commit_id FROM _cdc_flight.commit_log ORDER BY pipeline, commit_id"
        ).fetchall()
        assert rows == [("alpha", 1), ("alpha", 2), ("beta", 1), ("beta", 2)], rows
    finally:
        for applier in appliers:
            applier.shutdown()
        con.close()


# --------------------------------------------------------------------------- #
# one transaction across every table (rubric 1.3)
# --------------------------------------------------------------------------- #
def test_a_group_spanning_three_tables_is_one_destination_transaction(lab):
    """All three tables appear with the same `cdcf_commit_id`, or none of them do."""
    box = lab()
    box.run(
        [
            keyed("7", 1, 101, 1, "c1"),
            data("7", 2, 102, table="orders", key={"id": 1}, after={"id": 1, "total": 10}),
            data("7", 3, 103, table="sensor_readings", after={"value": 1.0}),
            end("7", 3, 104, {"app.customers": 1, "app.orders": 1, "app.sensor_readings": 1}),
        ]
    )
    commit_ids = box.q(
        f'SELECT DISTINCT cdcf_commit_id FROM "{DATASET}"."{box.target("customers")}" '
        f'UNION SELECT DISTINCT cdcf_commit_id FROM "{DATASET}"."{box.target("orders")}" '
        f'UNION SELECT DISTINCT cdcf_commit_id FROM "{DATASET}"."{box.target("sensor_readings")}"'
    )
    assert len(commit_ids) == 1, f"one PG transaction landed in {len(commit_ids)} commit groups"
    logged = box.q(
        "SELECT event_count, list_sort(tables_touched) FROM _cdc_flight.commit_log "
        "ORDER BY commit_id DESC LIMIT 1"
    )
    assert logged[0][0] == 3
    assert logged[0][1] == sorted(
        [box.target("customers"), box.target("orders"), box.target("sensor_readings")]
    )
