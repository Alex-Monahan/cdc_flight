"""Regression guards for destructive runtime-state cleanup."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
RUNTIME_STATE = PROJECT_DIR / "scripts" / "runtime_state.sh"


@pytest.fixture
def isolated_project(tmp_path: Path) -> Path:
    """Copy the structural helper path so no test mutates the shared checkout."""
    project = tmp_path / "project"
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    for source in RUNTIME_STATE.parent.glob("runtime_state.*"):
        shutil.copy2(source, scripts / source.name)
    return project


def _runtime_state(
    project: Path, command: str, **overrides: str
) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"CDC_INSTANCE_RUNTIME_ROOT", "CDC_TEST_INSTANCE_ID"}
    }
    env.update(overrides)
    return subprocess.run(
        [str(project / "scripts" / "runtime_state.sh"), command],
        cwd=project,
        capture_output=True,
        text=True,
        env=env,
    )


def test_clean_state_never_deletes_an_overridden_external_root(
    isolated_project: Path, tmp_path: Path
):
    victim = tmp_path / "must-survive"
    victim.mkdir()
    marker = victim / "user-data.txt"
    marker.write_text("not disposable\n")

    proc = _runtime_state(
        isolated_project,
        "clean",
        CDC_INSTANCE_RUNTIME_ROOT=str(victim),
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
def test_clean_state_rejects_non_child_instance_values(
    isolated_project: Path, hostile_instance: str
):
    proc = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=hostile_instance
    )

    assert proc.returncode == 2
    assert "refusing" in proc.stderr.lower()


def test_clean_state_requires_the_runtime_sentinel(isolated_project: Path):
    instance = "cleanup_missing_sentinel"
    target = isolated_project / ".cdc_instances" / instance
    target.mkdir(parents=True, exist_ok=True)
    marker = target / "must-survive.txt"
    marker.write_text("unmarked\n")
    proc = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert proc.returncode == 2
    assert marker.read_text() == "unmarked\n"
    assert "missing" in proc.stderr.lower()


def test_prepare_refuses_to_adopt_an_unmarked_existing_child(isolated_project: Path):
    instance = "cleanup_preexisting_child"
    target = isolated_project / ".cdc_instances" / instance
    target.mkdir(parents=True)
    marker = target / "must-survive.txt"
    marker.write_text("unmarked\n")

    proc = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )

    assert proc.returncode == 2
    assert marker.read_text() == "unmarked\n"
    assert not (target / ".cdc_flight_disposable_runtime").exists()


def test_prepare_then_clean_removes_only_the_selected_runtime_child(
    isolated_project: Path,
):
    instance = "cleanup_disposable_child"
    target = isolated_project / ".cdc_instances" / instance

    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "state.txt").write_text("disposable\n")

    cleaned = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert cleaned.returncode == 0, cleaned.stderr
    assert not target.exists()


def test_clean_state_rejects_a_child_symlink_that_escapes_the_project(
    isolated_project: Path, tmp_path: Path
):
    instance = "cleanup_symlink_escape"
    victim = tmp_path / "must-survive"
    victim.mkdir()
    marker = victim / "user-data.txt"
    marker.write_text("external\n")
    (victim / ".cdc_flight_disposable_runtime").write_text("forged\n")
    runtime_parent = isolated_project / ".cdc_instances"
    runtime_parent.mkdir()
    link = runtime_parent / instance
    link.symlink_to(victim, target_is_directory=True)
    proc = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert proc.returncode == 2
    assert marker.read_text() == "external\n"
    assert "refusing" in proc.stderr.lower()


def test_clean_state_rejects_a_parent_symlink_that_escapes_the_project(
    isolated_project: Path, tmp_path: Path
):
    instance = "cleanup_parent_symlink_escape"
    external_parent = tmp_path / "external-parent"
    victim = external_parent / instance
    victim.mkdir(parents=True)
    marker = victim / "user-data.txt"
    marker.write_text("external\n")
    (victim / ".cdc_flight_disposable_runtime").write_text(
        "cdc_flight disposable runtime state\n"
        f"instance={instance}\n"
    )
    (isolated_project / ".cdc_instances").symlink_to(
        external_parent, target_is_directory=True
    )

    proc = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert proc.returncode == 2
    assert marker.read_text() == "external\n"
    assert "refusing" in proc.stderr.lower()
