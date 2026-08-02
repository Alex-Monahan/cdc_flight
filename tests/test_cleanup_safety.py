"""Regression guards for destructive runtime-state cleanup."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
RUNTIME_STATE = PROJECT_DIR / "scripts" / "runtime_state.sh"


def _runtime_state(command: str, **overrides: str) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"CDC_INSTANCE_RUNTIME_ROOT", "CDC_TEST_INSTANCE_ID"}
    }
    env.update(overrides)
    return subprocess.run(
        [str(RUNTIME_STATE), command],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        env=env,
    )


def test_clean_state_never_deletes_an_overridden_external_root(tmp_path: Path):
    victim = tmp_path / "must-survive"
    victim.mkdir()
    marker = victim / "user-data.txt"
    marker.write_text("not disposable\n")

    proc = subprocess.run(
        [
            "make",
            "clean-state",
            f"CDC_INSTANCE_RUNTIME_ROOT={victim}",
        ],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )

    assert proc.returncode != 0
    assert marker.read_text() == "not disposable\n"
    assert "refusing" in (proc.stdout + proc.stderr).lower()


@pytest.mark.parametrize(
    "hostile_instance",
    [
        str(PROJECT_DIR),
        str(PROJECT_DIR.parent),
        "/tmp/external-runtime",
        "two words",
        "runtime*",
        "--preserve-root",
    ],
)
def test_clean_state_rejects_non_child_instance_values(hostile_instance: str):
    proc = _runtime_state("clean", CDC_TEST_INSTANCE_ID=hostile_instance)

    assert proc.returncode == 2
    assert "refusing" in proc.stderr.lower()


def test_clean_state_requires_the_runtime_sentinel():
    instance = "cleanup_missing_sentinel"
    target = PROJECT_DIR / ".cdc_instances" / instance
    target.mkdir(parents=True, exist_ok=True)
    marker = target / "must-survive.txt"
    marker.write_text("unmarked\n")
    try:
        proc = _runtime_state("clean", CDC_TEST_INSTANCE_ID=instance)

        assert proc.returncode == 2
        assert marker.read_text() == "unmarked\n"
        assert "missing" in proc.stderr.lower()
    finally:
        marker.unlink(missing_ok=True)
        target.rmdir()


def test_prepare_then_clean_removes_only_the_selected_runtime_child():
    instance = "cleanup_disposable_child"
    target = PROJECT_DIR / ".cdc_instances" / instance

    prepared = _runtime_state("prepare", CDC_TEST_INSTANCE_ID=instance)
    assert prepared.returncode == 0, prepared.stderr
    (target / "state.txt").write_text("disposable\n")

    cleaned = _runtime_state("clean", CDC_TEST_INSTANCE_ID=instance)

    assert cleaned.returncode == 0, cleaned.stderr
    assert not target.exists()


def test_clean_state_rejects_a_child_symlink_that_escapes_the_project(tmp_path: Path):
    instance = "cleanup_symlink_escape"
    victim = tmp_path / "must-survive"
    victim.mkdir()
    marker = victim / "user-data.txt"
    marker.write_text("external\n")
    (victim / ".cdc_flight_disposable_runtime").write_text("forged\n")
    link = PROJECT_DIR / ".cdc_instances" / instance
    link.symlink_to(victim, target_is_directory=True)
    try:
        proc = _runtime_state("clean", CDC_TEST_INSTANCE_ID=instance)

        assert proc.returncode == 2
        assert marker.read_text() == "external\n"
        assert "outside" in proc.stderr.lower()
    finally:
        link.unlink(missing_ok=True)
