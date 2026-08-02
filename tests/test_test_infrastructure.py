"""Regression guards for the native test infrastructure itself."""

from __future__ import annotations

import fcntl
import json
from pathlib import Path

import conftest
import pytest


def test_run_lock_takes_over_stale_metadata_after_kernel_release(tmp_path: Path):
    lock_path = tmp_path / "instance.lock"
    lock_path.write_text('{"pid": 999999, "run_uid": "crashed"}\n')

    handle = conftest._acquire_test_run_lock(
        lock_path, run_uid="replacement", wait_seconds=0
    )
    try:
        metadata = json.loads(lock_path.read_text())
        assert metadata["run_uid"] == "replacement"
        assert metadata["instance_id"] == conftest.TEST_INSTANCE_ID
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


def test_run_lock_never_takes_over_a_live_kernel_owner(tmp_path: Path):
    lock_path = tmp_path / "instance.lock"
    owner = lock_path.open("a+")
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(TimeoutError, match="timed out waiting"):
            conftest._acquire_test_run_lock(
                lock_path, run_uid="intruder", wait_seconds=0
            )
    finally:
        fcntl.flock(owner, fcntl.LOCK_UN)
        owner.close()


def test_run_and_setup_locks_are_distinct_and_instance_scoped():
    assert conftest.TEST_LOCK_PATH != conftest.TEST_SETUP_LOCK_PATH
    assert conftest.TEST_INSTANCE_ID in conftest.TEST_LOCK_PATH.name
    assert conftest.TEST_INSTANCE_ID in conftest.TEST_SETUP_LOCK_PATH.name


@pytest.mark.parametrize("requested", [6, 10])
def test_root_runner_preserves_explicit_proof_windows(monkeypatch, requested):
    observed: list[float] = []

    def fake_invoke(_env, **kwargs):
        observed.append(kwargs["idle_seconds"])
        return {"late_failure_observed": kwargs["idle_seconds"] >= requested}

    monkeypatch.setattr(conftest, "_invoke_pipeline", fake_invoke)
    runner = conftest.run_pipeline.__wrapped__({})

    summary = runner(idle_seconds=requested)

    assert observed == [requested]
    assert summary["late_failure_observed"], (
        "the fixture stopped before the caller's historical proof window"
    )


def test_sandbox_keeps_the_historical_six_second_default(monkeypatch, tmp_path: Path):
    observed: list[float] = []

    def fake_invoke(_env, **kwargs):
        observed.append(kwargs["idle_seconds"])
        return {}

    monkeypatch.setattr(conftest, "_invoke_pipeline", fake_invoke)
    sandbox = object.__new__(conftest.Sandbox)
    sandbox.env = {"CDC_STATE_DIR": str(tmp_path)}

    sandbox.run()

    assert observed == [6]
