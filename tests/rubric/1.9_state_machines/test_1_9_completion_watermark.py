"""Rubric 1.9 — the run's completion watermark, as a declared state machine.

The defect these tests pin down (measured, `codex_logs/slowlane_rootcause.md`):
**a `cdc-flight` run decided it had finished by WAITING OUT A TIMER instead of by
REACHING A WATERMARK.** One instrumented slow lane spent **1,640.1 s — 37.8 % of
the whole lane — inside the `--idle-seconds` quiet window across 218 runs that had
nothing left to deliver.**

Silence is not a fact about delivery. A position is. `CompletionWatermark` writes
one transactional marker to the source, takes the LSN PostgreSQL assigned it, and
ends the run at the instant the destination's durable resume point reaches that
LSN — which, because logical decoding hands over whole transactions in commit
order, proves that every source transaction that committed before the marker is
durable in the destination.

Everything here runs in milliseconds: no JVM, no Postgres, no subprocess. The
end-to-end proof against a real cluster and a real Debezium engine is
`test_1_9_completion_watermark_e2e.py`.
"""

from __future__ import annotations

import threading
import time

import pytest

from cdc_flight import machines as m
from cdc_flight.completion_watermark import CompletionWatermark
from cdc_flight.config import RunConfig
from cdc_flight.errors import EngineFailure
from cdc_flight.machines import (
    WATERMARK_ARMED,
    WATERMARK_REACHED,
    WATERMARK_UNARMED,
    WATERMARK_UNAVAILABLE,
)
from cdc_flight.pipeline import run_engine_bounded
from cdc_flight.snapshot_completion import SnapshotCompletion
from cdc_flight.source_marker import COMPLETION_WATERMARK, REASONS, SourceMarker
from cdc_flight.states import IllegalTransition
from cdc_flight.supervisor import ShutdownSequence


# --------------------------------------------------------------------------- #
# fakes — the supervisor's four collaborators, and nothing else
# --------------------------------------------------------------------------- #
class FakeResumePoint:
    def __init__(self, last_lsn: int = 0):
        self.last_lsn = last_lsn


class FakeHandler:
    """The applier surface the supervisor and the watermark actually read."""

    def __init__(self, *, durable_lsn: int = 0, quiet_for: float = 99.0):
        self.record_count = 0
        self.batch_count = 0
        self.data_batch_count = 0
        self.skipped_count = 0
        self.error = None
        self.busy = False
        self.seconds_since_last_batch = quiet_for
        self.resume_point = FakeResumePoint(durable_lsn)
        self.highest_source_lsn = durable_lsn
        self.lifecycle: list[str] = []
        self.quiesced = True
        self.snapshot_completion_required = False
        self.snapshot_completed = False

    def snapshot_counts(self):
        return {}

    def drain_on_shutdown(self):
        return 0

    def shutdown(self, *, reason="supervisor_shutdown"):
        self.lifecycle.append(f"seal:{reason}")

    def wait_for_quiescence(self, timeout):
        return self.quiesced

    def stats(self):
        return {}


class FakeEngine:
    """An engine that streams until the supervisor closes it, like a real one."""

    def __init__(self, *, run_seconds: float = 30.0):
        self.failure = None
        self.completed_success = True
        self.suppressed_message = None
        self.offset_flushes_verified = 0
        self._run_seconds = run_seconds
        self._closed = threading.Event()

    def run(self):
        self._closed.wait(self._run_seconds)

    def close(self, *, intentional: bool = True):
        self._closed.set()


class FakeSample:
    def __init__(self, confirmed_pos: int = 0):
        self.at = time.monotonic()
        self.confirmed_pos = confirmed_pos


class MarkableSource:
    """A source that accepts a marker and reports the LSN it was written at.

    This is the whole of `SourceHealth` the watermark needs: "can you write one
    transactional marker for me, and where did it land?".
    """

    interval = 0.5
    primary_dsn = "fake-primary"

    def __init__(self, *, lsn: int = 5000, writable: bool = True):
        self.lsn = lsn
        self.writable = writable
        if not writable:
            self.primary_dsn = None
        self.emitted: list[tuple[str, dict]] = []
        self.ever_streamed = True
        self.ever_sampled = True
        self.unknown_for = 0.0
        self.not_streaming_for = 0.0
        self.last = FakeSample()

    def emit_marker(self, marker, reason, payload):
        self.emitted.append((reason, payload))
        if not self.writable:
            return None
        return self.lsn

    def may_declare_idle(self, *, min_seconds, received_high_water=None):
        return True

    def confirmed_at_least(self, target):
        return True

    def wait_for_confirmed(self, target, *, timeout):
        return True

    def summary(self):
        return {"slot_health": "streaming"}


def _run_cfg(**kwargs) -> RunConfig:
    return RunConfig(**{"max_seconds": 6, "idle_seconds": 30, "min_records": 0, **kwargs})


def _watermark(source, **kwargs) -> CompletionWatermark:
    run = kwargs.pop("run", None) or _run_cfg()
    return CompletionWatermark(
        source,
        run,
        completion=kwargs.pop("completion", None) or SnapshotCompletion.streaming_only(),
        marker=kwargs.pop("marker", None) or SourceMarker(prefix="cdcf"),
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# the machine (rubric 1.9)
# --------------------------------------------------------------------------- #
def test_the_completion_watermark_is_a_declared_machine():
    machine = m.COMPLETION_WATERMARK
    assert machine.initial == WATERMARK_UNARMED
    assert set(machine.states) == {
        WATERMARK_UNARMED, WATERMARK_ARMED, WATERMARK_REACHED, WATERMARK_UNAVAILABLE
    }
    assert machine.durable is None, "the completion decision is per run, never durable"
    assert "completion_watermark" in m.declared_machines()


def test_a_reached_watermark_can_never_be_un_reached():
    """`reached` is terminal: ending a run is a durability decision, not a mood."""
    machine = m.COMPLETION_WATERMARK
    assert machine.is_terminal(WATERMARK_REACHED)
    assert machine.is_terminal(WATERMARK_UNAVAILABLE)
    with pytest.raises(IllegalTransition):
        machine.check(WATERMARK_REACHED, WATERMARK_UNARMED)
    with pytest.raises(IllegalTransition):
        machine.check(WATERMARK_UNAVAILABLE, WATERMARK_ARMED)
    # An unarmed run may never jump straight to a verdict; the only route to
    # `reached` is through `armed`, which is the route that writes the marker.
    with pytest.raises(IllegalTransition):
        machine.check(WATERMARK_UNARMED, WATERMARK_REACHED)


def test_the_marker_reason_is_declared():
    assert COMPLETION_WATERMARK in REASONS
    assert SourceMarker(prefix="cdcf").prefix_for(COMPLETION_WATERMARK) == (
        "cdcf_completion_watermark"
    )


def test_shutdown_cannot_close_stock_engine_before_callbacks_quiesce():
    """The close/ack boundary is a second declared 1.9 machine."""
    sequence = ShutdownSequence()
    sequence.to(m.SHUTDOWN_ACK_NOT_REQUIRED)
    sequence.to(m.SHUTDOWN_ADMISSION_SEALED)
    with pytest.raises(IllegalTransition):
        sequence.to(m.SHUTDOWN_ENGINE_CLOSING)

    sequence.to(m.SHUTDOWN_CALLBACKS_QUIESCENT)
    sequence.to(m.SHUTDOWN_OWN_EXECUTORS_STOPPED)
    sequence.to(m.SHUTDOWN_ENGINE_CLOSING)
    sequence.to(m.SHUTDOWN_ENGINE_CLOSED)
    sequence.to(m.SHUTDOWN_ENGINE_THREAD_STOPPED)
    assert sequence.summary()["shutdown_sequence_history"] == [
        m.SHUTDOWN_OPEN,
        m.SHUTDOWN_ACK_NOT_REQUIRED,
        m.SHUTDOWN_ADMISSION_SEALED,
        m.SHUTDOWN_CALLBACKS_QUIESCENT,
        m.SHUTDOWN_OWN_EXECUTORS_STOPPED,
        m.SHUTDOWN_ENGINE_CLOSING,
        m.SHUTDOWN_ENGINE_CLOSED,
        m.SHUTDOWN_ENGINE_THREAD_STOPPED,
    ]


# --------------------------------------------------------------------------- #
# the watermark itself
# --------------------------------------------------------------------------- #
def test_a_quiet_run_arms_a_watermark_instead_of_waiting_out_the_timer():
    source = MarkableSource(lsn=5000)
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)

    assert gate.reached(handler, elapsed=1.0) is False
    assert gate.state == WATERMARK_ARMED
    assert gate.target_lsn == 5000
    assert source.emitted and source.emitted[0][0] == COMPLETION_WATERMARK


def test_the_run_is_complete_only_when_the_destination_is_DURABLY_past_the_watermark():
    source = MarkableSource(lsn=5000)
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)

    gate.reached(handler, elapsed=1.0)
    handler.resume_point.last_lsn = 4999
    assert gate.reached(handler, elapsed=2.0) is False, "one byte short is not complete"
    handler.resume_point.last_lsn = 5000
    assert gate.reached(handler, elapsed=3.0) is True
    assert gate.state == WATERMARK_REACHED


def test_a_source_that_commits_more_work_after_the_watermark_does_not_withdraw_it():
    """Whole transactions only, and a position once taken is never withdrawn.

    A transaction that commits AFTER the watermark has a commit LSN > L, so it is
    decoded after the marker and belongs to the next run; the slot never advances
    past what the destination made durable, so nothing is lost either way. The
    run's claim is bounded and exact: *complete as of L*, not "the source has
    stopped".

    This used to be an `armed -> unarmed` invalidation edge with a re-arm budget
    behind it (`CDC_WATERMARK_MAX_WRITES`). Both reviewers asked for it to go:
    Opus measured that it did not deliver the quiescence property it was credited
    with (a writer at 1.5 s intervals still armed, still stopped in 5.5 s, still
    mid-write), and it is unnecessary because a position is only ever taken from
    a quiet stream in the first place. Deleting it makes `arms` deterministically
    0 or 1 per run.
    """
    source = MarkableSource(lsn=5000)
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)

    gate.reached(handler, elapsed=1.0)
    assert gate.state == WATERMARK_ARMED
    handler.data_batch_count += 1          # a transaction past the watermark arrived
    handler.resume_point.last_lsn = 9999   # and it is already durable
    assert gate.reached(handler, elapsed=2.0) is True
    assert gate.state == WATERMARK_REACHED
    assert gate.arms == 1, "one run takes at most one position"
    assert len(source.emitted) == 1

    # And the edge itself is gone from the declared machine, so no future caller
    # can reintroduce the withdrawal by hand.
    with pytest.raises(IllegalTransition):
        m.COMPLETION_WATERMARK.check(WATERMARK_ARMED, WATERMARK_UNARMED)


def test_a_position_is_only_ever_taken_from_a_quiet_stream():
    """Why the withdrawal edge above is not needed: the marker is not written
    while the connector is still handing batches over.

    Without this, a run would take a position in the middle of a backlog, reach
    it a few milliseconds later, and stop with the rest of the backlog still
    undelivered — safe under Invariant O, but it would turn one run into two for
    no reason. `CDC_WATERMARK_QUIET_SECONDS` is therefore load-bearing, and it is
    NOT a completion timer: it is capped by `--idle-seconds` and it is the only
    wait on the watermark path.
    """
    source = MarkableSource(lsn=5000)
    gate = _watermark(source, run=_run_cfg(idle_seconds=30))
    handler = FakeHandler(durable_lsn=10, quiet_for=0.1)

    assert gate.reached(handler, elapsed=1.0) is False
    assert gate.state == WATERMARK_UNARMED, "a stream mid-delivery is not a position"
    assert source.emitted == []

    handler.seconds_since_last_batch = 0.5
    assert gate.reached(handler, elapsed=1.5) is False
    assert gate.state == WATERMARK_ARMED
    assert gate.quiet_seconds == 0.5


def test_a_source_that_does_not_corroborate_quiet_never_arms():
    """A quiet callback is not proof while the source is reconnecting."""
    source = MarkableSource()
    source.may_declare_idle = lambda **_kwargs: False
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)

    assert gate.reached(handler, elapsed=1.0) is False
    assert gate.state == WATERMARK_UNARMED
    assert source.emitted == []


def test_no_watermark_is_taken_before_the_connector_has_streamed():
    """A marker written before the slot exists is WAL the slot will never carry:
    the run could never reach it and would burn `--max-seconds`."""
    source = MarkableSource(lsn=5000)
    source.ever_streamed = False
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)

    assert gate.reached(handler, elapsed=1.0) is False
    assert gate.state == WATERMARK_UNARMED
    assert source.emitted == []


def test_a_busy_applier_is_never_a_completed_run():
    source = MarkableSource()
    gate = _watermark(source)
    handler = FakeHandler(durable_lsn=10**9, quiet_for=99.0)
    handler.busy = True
    assert gate.reached(handler, elapsed=1.0) is False
    assert source.emitted == []


def test_min_records_still_gates_completion():
    source = MarkableSource()
    gate = _watermark(source, run=_run_cfg(min_records=3))
    handler = FakeHandler(durable_lsn=10**9, quiet_for=99.0)
    assert gate.reached(handler, elapsed=1.0) is False
    handler.record_count = 3
    assert gate.reached(handler, elapsed=1.0) is False  # arms, does not complete
    assert gate.state == WATERMARK_ARMED


def test_a_source_that_cannot_be_marked_falls_back_to_the_idle_window():
    """The declared fallback. A read-only replica, a missing privilege or
    `CDC_COMPLETION_WATERMARK=0` leaves the run with no way to establish a
    position, so it keeps the source-corroborated quiet window it always had —
    and says so in the summary.

    `CDC_CATALOG_MARKER=0` is deliberately NOT on that list: it governs the DDL
    fence, not the completion decision (Opus MINOR-5 — the module used to claim
    the opposite, which would have told an operator that a knob kept the Flight
    off their primary when it did not)."""
    source = MarkableSource(writable=False)
    gate = _watermark(source, run=_run_cfg(idle_seconds=1.0))
    handler = FakeHandler(durable_lsn=10, quiet_for=0.6)

    assert gate.reached(handler, elapsed=1.0) is False
    assert gate.state == WATERMARK_UNAVAILABLE
    handler.seconds_since_last_batch = 2.0
    # the fallback still needs the source-freshness confirmation window
    assert gate.reached(handler, elapsed=2.0) is False
    time.sleep(1.1)
    source.last = FakeSample()
    assert gate.reached(handler, elapsed=3.5) is True
    gate.record_stop_decision(handler)
    details = gate.as_dict()
    assert details["completion_watermark"] == WATERMARK_UNAVAILABLE
    assert details["completion_stop_condition"] == "idle_window"
    assert details["completion_idle_window_sec"] >= 1.0


def test_the_fallback_still_refuses_a_source_that_disagrees_it_is_idle():
    """Opus B5, unchanged: a quiet stream during Debezium's retriable-restart
    backoff is not an idle stream."""
    source = MarkableSource(writable=False)
    source.may_declare_idle = lambda **kwargs: False
    gate = _watermark(source, run=_run_cfg(idle_seconds=0.5))
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)
    for _ in range(5):
        assert gate.reached(handler, elapsed=5.0) is False


def test_the_watermark_path_needs_no_quiet_window_at_all():
    """The point of the change, stated as an assertion: with a watermark the run
    ends without ever satisfying `seconds_since_last_batch >= idle_seconds`."""
    source = MarkableSource(lsn=5000)
    gate = _watermark(source, run=_run_cfg(idle_seconds=30))
    handler = FakeHandler(durable_lsn=10, quiet_for=0.6)
    gate.reached(handler, elapsed=1.0)
    handler.resume_point.last_lsn = 5000
    assert gate.reached(handler, elapsed=1.3) is True
    assert handler.seconds_since_last_batch < 30


# --------------------------------------------------------------------------- #
# the supervisor, end to end over the fakes
# --------------------------------------------------------------------------- #
def test_a_run_stops_on_the_watermark_and_not_on_the_clock():
    """The regression test for the 1,640 s. `idle_seconds=30` and `max_seconds=6`:
    the only way this run can report `idle` is by reaching a position."""
    source = MarkableSource(lsn=5000)
    handler = FakeHandler(durable_lsn=5000, quiet_for=0.6)
    started = time.monotonic()
    summary = run_engine_bounded(
        FakeEngine(run_seconds=30.0), handler, _run_cfg(), source
    )
    elapsed = time.monotonic() - started
    assert summary["ok"] is True
    assert summary["stop_reason"] == "idle"
    assert summary["completion_watermark"] == WATERMARK_REACHED
    assert summary["completion_watermark_lsn"] == 5000
    assert summary["completion_stop_condition"] == "watermark"
    assert summary["completion_watermark_reached_at_sec"] <= summary[
        "completion_stop_at_sec"
    ]
    assert summary["completion_watermark_to_stop_sec"] < _run_cfg().idle_seconds
    assert summary["completion_idle_window_sec"] is None
    assert elapsed < 5.0, "the run waited on something; idle_seconds is 30"


def test_a_run_whose_destination_never_reaches_the_watermark_does_not_report_ok():
    """Stopping early on a delivery that is not durable is the one thing this
    change must never do. The destination stays behind the watermark, so the run
    burns its safety ceiling and fails loudly rather than reporting success.

    The source here is PERFECTLY HEALTHY: `not_streaming_for = 0` and it agrees
    the stream is idle, so the pre-existing "reached --max-seconds while the
    connector was not streaming" rule cannot fire. The only thing wrong with this
    run is that it took a position and never reached it, and that alone must be
    the failure — Opus MINOR-1 measured that removing one line
    (`source.not_streaming_for = 99.0`) from the old version of this test flipped
    it from `EngineFailure` to `ok: True`, which is test-audit F6's shape.
    """
    source = MarkableSource(lsn=5000)
    handler = FakeHandler(durable_lsn=10, quiet_for=99.0)
    assert source.not_streaming_for == 0.0 and source.unknown_for == 0.0
    with pytest.raises(EngineFailure) as raised:
        run_engine_bounded(
            FakeEngine(run_seconds=30.0),
            handler,
            _run_cfg(max_seconds=2, idle_seconds=1),
            source,
        )
    assert raised.value.summary["stop_reason"] == "max_seconds"
    assert raised.value.summary["completion_watermark"] == WATERMARK_ARMED
    assert "ok" not in raised.value.summary
    assert "5000" in str(raised.value), str(raised.value)


def test_a_streaming_source_that_never_went_quiet_does_not_report_ok_either():
    """Luna BLOCKER W-01, reproduced over the fakes.

    A source committing continuously never produces a quiet tick, so no position
    is ever taken and the completion machine is still `unarmed` when the run hits
    its `--max-seconds` safety ceiling. Luna measured the consequence against a
    real cluster: `returncode=0, ok=true, stop_reason=max_seconds,
    completion_watermark=unarmed` with **28 committed source rows missing from
    the destination** (customer ids 1180..1207). A run that reports success while
    committed rows are absent is data loss reported as success.

    `--max-seconds` is a SAFETY CEILING, not an exit path: a run that ends on it
    has not shown its delivery to be complete, whatever the connector's health.
    """
    source = MarkableSource(lsn=5000)
    source.not_streaming_for = 0.0     # the connector is healthy and streaming
    # ... and the stream never goes quiet long enough to take a position.
    handler = FakeHandler(durable_lsn=10, quiet_for=0.0)
    with pytest.raises(EngineFailure) as raised:
        run_engine_bounded(
            FakeEngine(run_seconds=30.0),
            handler,
            _run_cfg(max_seconds=2, idle_seconds=1),
            source,
        )
    assert raised.value.summary["stop_reason"] == "max_seconds"
    assert raised.value.summary["completion_watermark"] == WATERMARK_UNARMED
    assert raised.value.summary["completion_watermark_arms"] == 0
    assert "ok" not in raised.value.summary
    assert source.emitted == [], "a source that never went quiet was never marked"


def test_the_safety_ceiling_is_still_a_verdict_when_the_source_cannot_be_marked():
    """The declared fallback keeps its own pre-existing rules, unchanged.

    A run with no way to take a position is judged exactly as it was before the
    watermark existed: the `--idle-seconds` quiet window decides, and reaching
    `--max-seconds` is judged by whether the connector was streaming. This is the
    one path on which the ceiling is not, by itself, a failure — because there
    was never a position to reach.
    """
    source = MarkableSource(writable=False)
    source.may_declare_idle = lambda **kwargs: False
    handler = FakeHandler(durable_lsn=10, quiet_for=1.0)
    summary = run_engine_bounded(
        FakeEngine(run_seconds=30.0),
        handler,
        _run_cfg(max_seconds=1, idle_seconds=5),
        source,
    )
    assert summary["ok"] is True
    assert summary["stop_reason"] == "max_seconds"
    assert summary["completion_watermark"] == WATERMARK_UNAVAILABLE
