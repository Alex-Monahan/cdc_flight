"""Fast, no-JVM, no-Postgres unit tests for the supervisor's failure semantics.

These cover the review findings that are cheap to pin exactly and expensive to
pin end to end:

* **Codex 10** - an engine that returns on its own in streaming mode is engine
  death and must exit non-zero, even when Debezium says `success=true`.
* **Opus M1** - the shutdown-noise filter must only be armed by an *intentional*
  close, and a suppressed message must still reach the run summary.
* **Codex 9** - `CDC_FAULT_INJECT` is parsed and validated once, so a typo fails
  the run instead of leaving a fault test vacuously green.
* **Opus B2** - `OffsetFlushVerifier` raises when `offsets.dat` did not move.

They run in milliseconds, so nothing here needs a marker.
"""

from __future__ import annotations

import time

import pytest

from cdc_flight.config import RunConfig
from cdc_flight.consumer import OffsetFlushVerifier
from cdc_flight.engine import SupervisedDebeziumEngine
from cdc_flight.errors import EngineFailure, OffsetFlushFailed
from cdc_flight.faults import FaultSpecError, parse_spec
from cdc_flight.pipeline import run_engine_bounded


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class FakeEngine:
    """An engine whose `run()` simply returns, like a swallowed StopEngineException."""

    def __init__(self, *, failure=None, completed_success=True, run_seconds=0.0):
        self.failure = failure
        self.completed_success = completed_success
        self.suppressed_message = None
        self.offset_flushes_verified = 0
        self.closed_intentional: bool | None = None
        self._run_seconds = run_seconds

    def run(self):
        time.sleep(self._run_seconds)

    def close(self, *, intentional: bool = True):
        self.closed_intentional = intentional


class HangingCloseEngine(FakeEngine):
    """An engine whose `close()` never returns."""

    def close(self, *, intentional: bool = True):
        self.closed_intentional = intentional
        time.sleep(600)


class FakeHandler:
    def __init__(self):
        self.record_count = 0
        self.batch_count = 0
        self.data_batch_count = 0
        self.skipped_count = 0
        self.error = None
        self.busy = False
        self.seconds_since_last_batch = 0.0
        self.lifecycle: list[str] = []
        self.quiesced = True

    def snapshot_counts(self):
        return {}

    # The applier's surface: the supervisor discards the un-ENDed tail at
    # shutdown (ADR 0001 §3.2) and folds the applier's counters into the summary.
    def drain_on_shutdown(self):
        self.lifecycle.append("drain")
        return 0

    def shutdown(self, *, reason="supervisor_shutdown"):
        self.lifecycle.append(f"seal:{reason}")

    def wait_for_quiescence(self, timeout):
        self.lifecycle.append("wait_for_quiescence")
        return self.quiesced

    def stats(self):
        return {}


def _run_cfg(**kwargs) -> RunConfig:
    return RunConfig(**{"max_seconds": 5, "idle_seconds": 1, "min_records": 0, **kwargs})


# --------------------------------------------------------------------------- #
# Codex 10 - unexpected normal termination
# --------------------------------------------------------------------------- #
def test_engine_that_returns_on_its_own_is_a_failure_in_streaming_mode():
    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(FakeEngine(), FakeHandler(), _run_cfg())
    assert "terminated before the supervisor requested a stop" in str(excinfo.value)
    assert excinfo.value.summary["stop_reason"] == "engine_finished"


def test_engine_that_returns_on_its_own_is_fine_when_the_mode_terminates():
    summary = run_engine_bounded(
        FakeEngine(), FakeHandler(), _run_cfg(), engine_terminates_normally=True
    )
    assert summary["ok"] is True
    assert summary["stop_reason"] == "engine_finished"


def test_engine_failure_still_wins_over_the_lifecycle_check():
    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(FakeEngine(failure="boom"), FakeHandler(), _run_cfg())
    assert "boom" in str(excinfo.value)


def test_close_is_marked_unintentional_when_the_run_already_failed():
    engine = FakeEngine(failure="boom", run_seconds=3)
    with pytest.raises(EngineFailure):
        run_engine_bounded(engine, FakeHandler(), _run_cfg())
    assert engine.closed_intentional is False


def test_a_hanging_close_is_a_hang_not_a_success():
    """Codex 11: `close()` used to run synchronously *before* the join.

    So a hang inside `close()` never reached the join-based "hung" verdict, and
    the advertised hang detector did not guard this path at all - the run would
    simply never return. It now gets its own bounded supervision.
    """
    engine = HangingCloseEngine(run_seconds=1)
    handler = FakeHandler()
    handler.seconds_since_last_batch = 99
    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(engine, handler, _run_cfg(close_timeout=1))
    assert "did not stop within" in str(excinfo.value)
    assert excinfo.value.summary["stop_reason"] == "hung"


def test_close_is_marked_intentional_on_a_clean_stop():
    engine = FakeEngine(run_seconds=3)
    handler = FakeHandler()
    handler.seconds_since_last_batch = 99
    summary = run_engine_bounded(engine, handler, _run_cfg())
    assert summary["stop_reason"] == "idle"
    assert engine.closed_intentional is True


def test_a_non_quiescent_callback_keeps_ownership_and_is_not_drained():
    """Round 8 MAJOR-1: a timeout is not permission to share the connection.

    Once callback admission is sealed, an already-admitted callback may still own the
    destination.  If it cannot be proved gone, the supervisor must leave the entire
    applier runtime alone for the hard-failure path; rollback/drain on the supervisor
    thread would race data/state work on the callback thread.
    """
    handler = FakeHandler()
    handler.quiesced = False

    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(
            FakeEngine(), handler, _run_cfg(close_timeout=0.01),
            engine_terminates_normally=True,
        )

    assert handler.lifecycle == [
        "seal:supervisor_shutdown",
        "wait_for_quiescence",
    ]
    assert excinfo.value.summary["applier_quiesced"] is False
    assert excinfo.value.summary["destination_owner"] == "live_applier_callback"


# --------------------------------------------------------------------------- #
# Opus M1 - the shutdown-noise filter
# --------------------------------------------------------------------------- #
def _engine() -> SupervisedDebeziumEngine:
    # No JVM is booted: `SupervisedDebeziumEngine` only touches JPype from the
    # `consumer` / `engine` cached properties, which these tests never reach.
    return SupervisedDebeziumEngine(properties={"name": "x"}, handler=object())


def test_noise_filter_is_disarmed_until_an_intentional_close():
    engine = _engine()
    engine._on_completion(False, "org.apache.kafka...: interrupted", None)
    assert engine.failure is not None, "a failure before any close must never be filtered"


def test_noise_filter_stays_disarmed_after_an_unintentional_close():
    engine = _engine()
    engine.close(intentional=False)
    engine._on_completion(False, "InterruptedException while committing offsets", None)
    assert engine.failure is not None, (
        "closing *because* something failed must not license discarding the cause"
    )


def test_intentional_close_filters_noise_but_records_it():
    engine = _engine()
    engine.close(intentional=True)
    engine._on_completion(False, "Connector has been stopped", None)
    assert engine.failure is None
    assert engine.suppressed_message == "Connector has been stopped", (
        "a suppressed failure must still be visible to an operator"
    )


def test_intentional_close_does_not_filter_an_unrelated_failure():
    engine = _engine()
    engine.close(intentional=True)
    engine._on_completion(False, "replication slot no longer available on the server", None)
    assert engine.failure is not None


def test_close_on_an_engine_that_was_never_built_does_not_build_one():
    engine = _engine()
    engine.close()
    assert "engine" not in engine.__dict__, (
        "close() must not construct an engine through the cached_property (Opus m4)"
    )


# --------------------------------------------------------------------------- #
# Codex 9 - fault spec validation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("pre_commit:1", ("pre_commit", 1, 137)),
        ("post_commit_pre_ack:3:9", ("post_commit_pre_ack", 3, 9)),
        ("post_ack:2:raise", ("post_ack", 2, "raise")),
        # legacy names keep working
        ("before_load:1", ("pre_commit", 1, 137)),
        ("after_load:1", ("post_commit_pre_ack", 1, 137)),
    ],
)
def test_fault_spec_parses(raw, expected):
    assert parse_spec(raw) == expected


def test_fault_spec_defaults_to_none_when_unset():
    assert parse_spec(None) is None
    assert parse_spec("") is None


@pytest.mark.parametrize(
    "raw",
    [
        "not_a_point:1",
        "pre_commit:zero",
        "pre_commit:0",
        "pre_commit:1:sometimes",
        "pre_commit:1:137:extra",
    ],
)
def test_malformed_fault_spec_is_rejected(raw):
    """A typo must fail the run, not silently make a fault test vacuous."""
    with pytest.raises(FaultSpecError):
        parse_spec(raw)


# --------------------------------------------------------------------------- #
# Opus B2 - the offset flush verifier
# --------------------------------------------------------------------------- #
def test_verifier_raises_when_the_offset_file_did_not_move(tmp_path):
    path = tmp_path / "offsets.dat"
    path.write_bytes(b"initial")
    verifier = OffsetFlushVerifier(path, always_commit=True)
    before = verifier.before()
    with pytest.raises(OffsetFlushFailed) as excinfo:
        verifier.after(before, marked=5)
    assert "did not change" in str(excinfo.value)


def test_verifier_accepts_a_file_that_moved(tmp_path):
    path = tmp_path / "offsets.dat"
    path.write_bytes(b"initial")
    verifier = OffsetFlushVerifier(path, always_commit=True)
    before = verifier.before()
    path.write_bytes(b"advanced")
    verifier.after(before, marked=5)
    assert verifier.flushes_verified == 1


def test_verifier_is_inert_for_an_empty_batch(tmp_path):
    path = tmp_path / "offsets.dat"
    path.write_bytes(b"initial")
    verifier = OffsetFlushVerifier(path, always_commit=True)
    verifier.after(verifier.before(), marked=0)  # must not raise


def test_verifier_is_inert_under_a_periodic_commit_policy(tmp_path):
    """With a timer-based policy a no-op markBatchFinished() is correct."""
    path = tmp_path / "offsets.dat"
    path.write_bytes(b"initial")
    verifier = OffsetFlushVerifier(path, always_commit=False)
    verifier.after(verifier.before(), marked=5)  # must not raise
