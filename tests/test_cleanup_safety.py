"""Regression guards for destructive runtime-state cleanup."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

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
    shutil.copy2(PROJECT_DIR / "Makefile", project / "Makefile")
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


def _load_runtime_state(project: Path) -> ModuleType:
    helper = project / "scripts" / "runtime_state.py"
    spec = importlib.util.spec_from_file_location(
        f"runtime_state_{project.parent.name}", helper
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def test_python_cli_rejects_a_caller_selected_project_root(
    isolated_project: Path, tmp_path: Path
):
    instance = "attack"
    victim = tmp_path / "caller-selected-root"
    target = victim / ".cdc_instances" / instance
    target.mkdir(parents=True)
    marker = target / "must-survive.txt"
    marker.write_text("external\n")
    (target / ".cdc_flight_disposable_runtime").write_text(
        "cdc_flight disposable runtime state\ninstance=attack\n"
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(isolated_project / "scripts" / "runtime_state.py"),
            "--project-dir",
            str(victim),
            "clean",
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 2
    assert marker.read_text() == "external\n"
    assert target.exists()


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


def test_prepare_refuses_a_competing_directory_creator(
    isolated_project: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_competing_creator"
    target = isolated_project / ".cdc_instances" / instance
    module = _load_runtime_state(isolated_project)

    def create_competing_target(event: str, _path: Path) -> None:
        if event == "before_target_mkdir":
            target.mkdir()
            (target / "must-survive.txt").write_text("foreign\n")

    monkeypatch.setattr(module, "_checkpoint", create_competing_target)

    with pytest.raises(module.Refusal, match="sentinel"):
        module._run("prepare", instance)

    assert (target / "must-survive.txt").read_text() == "foreign\n"
    assert not (target / ".cdc_flight_disposable_runtime").exists()


def test_prepare_rolls_back_its_directory_if_sentinel_creation_fails(
    isolated_project: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_failed_sentinel"
    target = isolated_project / ".cdc_instances" / instance
    module = _load_runtime_state(isolated_project)

    def fail_fsync(_fd: int) -> None:
        raise OSError("injected fsync failure")

    monkeypatch.setattr(module.os, "fsync", fail_fsync)

    with pytest.raises(OSError, match="injected fsync failure"):
        module._run("prepare", instance)

    assert not target.exists()


def test_prepare_then_clean_removes_only_the_selected_runtime_child(
    isolated_project: Path,
):
    instance = "cleanup_disposable_child"
    target = isolated_project / ".cdc_instances" / instance

    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    nested = target / "cdc_state" / "nested"
    nested.mkdir(parents=True)
    (nested / "state.txt").write_text("disposable\n")

    cleaned = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert cleaned.returncode == 0, cleaned.stderr
    assert not target.exists()


def test_make_clean_state_uses_the_helper_derived_project_root(
    isolated_project: Path,
):
    instance = "cleanup_make_entrypoint"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "state.txt").write_text("disposable\n")

    env = os.environ.copy()
    env.pop("CDC_INSTANCE_RUNTIME_ROOT", None)
    proc = subprocess.run(
        ["make", "-s", "clean-state", f"CDC_TEST_INSTANCE_ID={instance}"],
        cwd=isolated_project,
        capture_output=True,
        text=True,
        env=env,
    )

    assert proc.returncode == 0, proc.stderr
    assert not target.exists()


def test_clean_refuses_target_rename_after_validation_without_deleting(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_target_rename"
    target = isolated_project / ".cdc_instances" / instance
    escaped = tmp_path / "escaped-target"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    marker = target / "must-survive.txt"
    marker.write_text("preserved\n")
    module = _load_runtime_state(isolated_project)

    def rename_target(event: str, _path: Path) -> None:
        if event == "after_validation":
            target.rename(escaped)

    monkeypatch.setattr(module, "_checkpoint", rename_target)

    with pytest.raises(module.Refusal, match="before quarantine"):
        module._run("clean", instance)

    assert (escaped / marker.name).read_text() == "preserved\n"


def test_clean_refuses_child_replacement_between_stat_and_open(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_child_replacement"
    target = isolated_project / ".cdc_instances" / instance
    child = target / "child"
    escaped = tmp_path / "escaped-child"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    child.mkdir()
    (child / "original.txt").write_text("original\n")
    stable = target / "stable.txt"
    stable.write_text("stable\n")
    module = _load_runtime_state(isolated_project)
    fired = False

    def replace_child(event: str, path: Path) -> None:
        nonlocal fired
        if not fired and event == "before_child_open" and path.name == "child":
            fired = True
            child.rename(escaped)
            child.mkdir()
            (child / "replacement.txt").write_text("replacement\n")

    monkeypatch.setattr(module, "_checkpoint", replace_child)

    with pytest.raises(module.Refusal, match="changed while opening"):
        module._run("clean", instance)

    assert (escaped / "original.txt").read_text() == "original\n"
    assert (child / "replacement.txt").read_text() == "replacement\n"
    assert stable.read_text() == "stable\n"


def test_clean_refuses_child_rename_after_open_without_deleting(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_child_rename"
    target = isolated_project / ".cdc_instances" / instance
    child = target / "child"
    escaped = tmp_path / "opened-child-escaped"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    child.mkdir()
    (child / "must-survive.txt").write_text("preserved\n")
    stable = target / "stable.txt"
    stable.write_text("stable\n")
    module = _load_runtime_state(isolated_project)
    fired = False

    def rename_open_child(event: str, path: Path) -> None:
        nonlocal fired
        if not fired and event == "after_child_open" and path.name == "child":
            fired = True
            child.rename(escaped)

    monkeypatch.setattr(module, "_checkpoint", rename_open_child)

    with pytest.raises(module.Refusal, match="left its verified parent"):
        module._run("clean", instance)

    assert (escaped / "must-survive.txt").read_text() == "preserved\n"
    assert stable.read_text() == "stable\n"


def test_clean_revalidates_quarantine_before_deleting(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_post_validation_symlink"
    target = isolated_project / ".cdc_instances" / instance
    victim = tmp_path / "external-post-validation"
    victim.mkdir()
    external = victim / "external.txt"
    external.write_text("external\n")
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    stable = target / "stable.txt"
    stable.write_text("stable\n")
    module = _load_runtime_state(isolated_project)

    def insert_symlink(event: str, _path: Path) -> None:
        if event == "after_validation":
            (target / "late-link").symlink_to(victim, target_is_directory=True)

    monkeypatch.setattr(module, "_checkpoint", insert_symlink)

    with pytest.raises(module.Refusal, match="symlink"):
        module._run("clean", instance)

    assert external.read_text() == "external\n"
    assert stable.read_text() == "stable\n"
    assert (target / "late-link").is_symlink()


def test_clean_preserves_target_replacement_before_final_rmdir(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_target_replacement"
    target = isolated_project / ".cdc_instances" / instance
    escaped = tmp_path / "emptied-disposable-target"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "disposable.txt").write_text("disposable\n")
    module = _load_runtime_state(isolated_project)

    def replace_target(event: str, path: Path) -> None:
        if event == "before_target_rmdir":
            path.rename(escaped)
            path.mkdir()
            (path / "must-survive.txt").write_text("replacement\n")

    monkeypatch.setattr(module, "_checkpoint", replace_target)

    with pytest.raises(module.Refusal, match="changed before removal"):
        module._run("clean", instance)

    assert escaped.exists()
    assert (target / "must-survive.txt").read_text() == "replacement\n"


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


def test_clean_state_rejects_a_nested_symlink(
    isolated_project: Path, tmp_path: Path
):
    instance = "cleanup_nested_symlink_escape"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr

    victim = tmp_path / "must-survive-nested"
    victim.mkdir()
    marker = victim / "user-data.txt"
    marker.write_text("external\n")
    (target / "nested-link").symlink_to(victim, target_is_directory=True)

    proc = _runtime_state(
        isolated_project, "clean", CDC_TEST_INSTANCE_ID=instance
    )

    assert proc.returncode == 2
    assert marker.read_text() == "external\n"
    assert target.exists()
    assert "symlink" in proc.stderr.lower()


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
