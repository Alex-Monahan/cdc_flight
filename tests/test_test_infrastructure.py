"""Regression guards for the native test infrastructure itself."""

from __future__ import annotations

from pathlib import Path

import pytest

import conftest


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
