"""Regression guards for destructive runtime-state cleanup."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[2]
RUNTIME_STATE = PROJECT_DIR / "scripts" / "runtime_state.sh"


@pytest.fixture
def isolated_project(tmp_path: Path) -> Path:
    """Copy the structural helper path so no test mutates the shared checkout."""
    project = tmp_path / "project"
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    for source in RUNTIME_STATE.parent.glob("runtime_state*"):
        shutil.copy2(source, scripts / source.name)
    package = project / "src" / "cdc_flight"
    package.mkdir(parents=True)
    for name in ("__init__.py", "states.py", "machines.py"):
        shutil.copy2(PROJECT_DIR / "src" / "cdc_flight" / name, package / name)
    shutil.copy2(PROJECT_DIR / "Makefile", project / "Makefile")
    return project


def _runtime_state(
    project: Path, command: str, child: list[str] | None = None, **overrides: str
) -> subprocess.CompletedProcess:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"CDC_INSTANCE_RUNTIME_ROOT", "CDC_TEST_INSTANCE_ID"}
    }
    env.update(overrides)
    args = [str(project / "scripts" / "runtime_state.sh"), command]
    if child is not None:
        args.extend(["--", *child])
    return subprocess.run(
        args,
        cwd=project,
        capture_output=True,
        text=True,
        env=env,
    )


def _load_runtime_state(project: Path) -> ModuleType:
    helper = project / "scripts" / "runtime_state.py"
    module_name = f"runtime_state_{project.parent.name}"
    spec = importlib.util.spec_from_file_location(module_name, helper)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
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


def test_clean_refuses_when_retained_root_leaves_parent_before_sweep(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_retained_root_escape"
    target = isolated_project / ".cdc_instances" / instance
    escaped = tmp_path / "retained-root-escaped"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    marker = target / "must-survive.txt"
    marker.write_text("preserved\n")
    module = _load_runtime_state(isolated_project)

    def move_root(event: str, _path: Path) -> None:
        if event == "before_delete_tree":
            target.rename(escaped)

    monkeypatch.setattr(module, "_checkpoint", move_root)

    with pytest.raises(module.Refusal, match="left its verified parent before deletion"):
        module._run("clean", instance)

    assert (escaped / marker.name).read_text() == "preserved\n"


def test_clean_refuses_when_delete_child_leaves_retained_root(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_delete_child_escape"
    target = isolated_project / ".cdc_instances" / instance
    child = target / "child"
    escaped = tmp_path / "delete-child-escaped"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    child.mkdir()
    (child / "must-survive.txt").write_text("preserved\n")
    module = _load_runtime_state(isolated_project)

    def move_child(event: str, path: Path) -> None:
        if event == "after_delete_child_verify" and path.name == "child":
            child.rename(escaped)

    monkeypatch.setattr(module, "_checkpoint", move_child)

    with pytest.raises(module.Refusal, match="left its verified parent"):
        module._run("clean", instance)

    assert (escaped / "must-survive.txt").read_text() == "preserved\n"


def test_prepare_refuses_foreign_replacement_after_mkdir(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_post_mkdir_replacement"
    target = isolated_project / ".cdc_instances" / instance
    escaped = tmp_path / "original-root"
    module = _load_runtime_state(isolated_project)

    def replace_root(event: str, _path: Path) -> None:
        if event == "after_target_mkdir":
            target.rename(escaped)
            target.mkdir()
            (target / "must-survive.txt").write_text("foreign\n")

    monkeypatch.setattr(module, "_checkpoint", replace_root)

    with pytest.raises(module.Refusal, match="changed after publication"):
        module._run("prepare", instance)

    assert (target / "must-survive.txt").read_text() == "foreign\n"
    assert not (target / ".cdc_flight_disposable_runtime").exists()


def test_prepare_refuses_foreign_directory_at_atomic_publish(
    isolated_project: Path, monkeypatch: pytest.MonkeyPatch
):
    """A public-name competitor can never receive the private root's sentinel."""
    instance = "cleanup_mkdir_return_replacement"
    target = isolated_project / ".cdc_instances" / instance
    module = _load_runtime_state(isolated_project)

    def install_foreign(event: str, _path: Path) -> None:
        if event == "before_target_publish":
            target.mkdir()
            (target / "must-survive.txt").write_text("foreign\n")

    monkeypatch.setattr(module, "_checkpoint", install_foreign)

    with pytest.raises(module.Refusal, match="existing root is not adopted"):
        module._run("prepare", instance)

    assert (target / "must-survive.txt").read_text() == "foreign\n"
    assert not (target / module.SENTINEL_NAME).exists()
    assert not list(target.parent.glob(f".{instance}.provision.*"))


def test_clean_binds_runtime_parent_to_physical_project(
    isolated_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_parent_binding"
    parent = isolated_project / ".cdc_instances"
    escaped = tmp_path / "escaped-runtime-parent"
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    marker = parent / instance / "must-survive.txt"
    marker.write_text("preserved\n")
    module = _load_runtime_state(isolated_project)

    def move_parent(event: str, _path: Path) -> None:
        if event == "after_runtime_parent_open":
            parent.rename(escaped)

    monkeypatch.setattr(module, "_checkpoint", move_parent)

    with pytest.raises(module.Refusal, match="runtime parent"):
        module._run("clean", instance)

    assert (escaped / instance / marker.name).read_text() == "preserved\n"


def test_parent_completion_record_recovers_after_final_marker_removal(
    isolated_project: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    instance = "cleanup_terminal_recovery_clean"
    parent = isolated_project / ".cdc_instances"
    target = parent / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "must-delete.txt").write_text("disposable\n")
    module = _load_runtime_state(isolated_project)

    def interrupt(event: str, _path: Path) -> None:
        if event == "after_terminal_markers_removed":
            raise OSError("injected terminal interruption")

    monkeypatch.setattr(module, "_checkpoint", interrupt)
    with pytest.raises(OSError, match="injected terminal interruption"):
        module._run("clean", instance)

    completion = parent / module._record_name(instance)
    assert target.exists()
    assert list(target.iterdir()) == []
    assert completion.exists()

    monkeypatch.undo()
    module._run("clean", instance)
    assert not completion.exists()
    assert not target.exists()


@pytest.mark.parametrize(
    ("checkpoint", "expected_state"),
    [
        ("after_terminal_markers_removed", "completion_recorded"),
        ("after_target_rmdir", "deleted_recorded"),
    ],
)
@pytest.mark.parametrize("command", ["prepare", "run"])
def test_persistent_commands_refuse_recorded_destructive_states(
    isolated_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    checkpoint: str,
    expected_state: str,
    command: str,
):
    instance = f"cleanup_recorded_refusal_{expected_state}_{command}"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "payload").write_text("delete me\n")
    module = _load_runtime_state(isolated_project)

    def interrupt(event: str, _path: Path) -> None:
        if event == checkpoint:
            raise OSError("injected recorded-state interruption")

    monkeypatch.setattr(module, "_checkpoint", interrupt)
    with pytest.raises(OSError, match="recorded-state interruption"):
        module._run("clean", instance)
    monkeypatch.undo()

    child = [sys.executable, "-c", "raise SystemExit('child must not run')"]
    with pytest.raises(module.Refusal, match=expected_state) as raised:
        module._run(command, instance, child if command == "run" else None)
    assert "runtime_state.sh clean" in str(raised.value)


def test_run_holds_project_lock_for_pipeline_mutation_lifetime(
    isolated_project: Path,
):
    instance = "cleanup_full_lifetime_lock"
    target = isolated_project / ".cdc_instances" / instance
    env = os.environ.copy()
    env.pop("CDC_INSTANCE_RUNTIME_ROOT", None)
    env["CDC_TEST_INSTANCE_ID"] = instance
    running = subprocess.Popen(
        [
            str(isolated_project / "scripts" / "runtime_state.sh"),
            "run",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(1.5)",
        ],
        cwd=isolated_project,
        env=env,
    )
    deadline = time.monotonic() + 5
    while not (target / ".cdc_flight_disposable_runtime").exists():
        assert running.poll() is None
        assert time.monotonic() < deadline
        time.sleep(0.02)

    cleaning = subprocess.Popen(
        [str(isolated_project / "scripts" / "runtime_state.sh"), "clean"],
        cwd=isolated_project,
        env=env,
    )
    time.sleep(0.25)
    assert cleaning.poll() is None, "clean escaped the pipeline's physical project lock"
    assert running.wait(timeout=5) == 0
    assert cleaning.wait(timeout=5) == 0
    assert not target.exists()


def test_run_reopens_the_same_owned_root_for_a_second_invocation(
    isolated_project: Path,
):
    instance = "cleanup_persistent_run"
    target = isolated_project / ".cdc_instances" / instance
    write_first = (
        "from pathlib import Path; "
        "Path('.cdc_instances/cleanup_persistent_run/first').write_text('one')"
    )
    write_second = (
        "from pathlib import Path; "
        "Path('.cdc_instances/cleanup_persistent_run/second').write_text('two')"
    )

    first = _runtime_state(
        isolated_project,
        "run",
        [sys.executable, "-c", write_first],
        CDC_TEST_INSTANCE_ID=instance,
    )
    second = _runtime_state(
        isolated_project,
        "run",
        [sys.executable, "-c", write_second],
        CDC_TEST_INSTANCE_ID=instance,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert (target / "first").read_text() == "one"
    assert (target / "second").read_text() == "two"


def test_prepare_then_run_reuses_the_prepared_root(isolated_project: Path):
    instance = "cleanup_prepare_then_run"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    ran = _runtime_state(
        isolated_project,
        "run",
        [sys.executable, "-c", "from pathlib import Path; Path('.cdc_instances/cleanup_prepare_then_run/ran').touch()"],
        CDC_TEST_INSTANCE_ID=instance,
    )

    assert prepared.returncode == 0, prepared.stderr
    assert ran.returncode == 0, ran.stderr
    assert (target / "ran").exists()


def test_failed_child_leaves_the_owned_root_for_a_retry(isolated_project: Path):
    instance = "cleanup_retry_run"
    target = isolated_project / ".cdc_instances" / instance
    failed = _runtime_state(
        isolated_project,
        "run",
        [sys.executable, "-c", "raise SystemExit(17)"],
        CDC_TEST_INSTANCE_ID=instance,
    )
    retried = _runtime_state(
        isolated_project,
        "run",
        [sys.executable, "-c", "pass"],
        CDC_TEST_INSTANCE_ID=instance,
    )

    assert failed.returncode == 17
    assert target.exists()
    assert retried.returncode == 0, retried.stderr


def test_retained_authority_survives_wrapper_death_until_child_exits(
    isolated_project: Path, tmp_path: Path
):
    instance = "cleanup_wrapper_death"
    target = isolated_project / ".cdc_instances" / instance
    started = tmp_path / "child-started"
    finished = tmp_path / "child-finished"
    code = (
        "from pathlib import Path; import time; "
        f"Path({str(started)!r}).write_text('started'); time.sleep(1.2); "
        f"Path({str(finished)!r}).write_text('finished')"
    )
    env = os.environ.copy()
    env.pop("CDC_INSTANCE_RUNTIME_ROOT", None)
    env["CDC_TEST_INSTANCE_ID"] = instance
    running = subprocess.Popen(
        [
            str(isolated_project / "scripts" / "runtime_state.sh"),
            "run",
            "--",
            sys.executable,
            "-c",
            code,
        ],
        cwd=isolated_project,
        env=env,
    )
    deadline = time.monotonic() + 5
    while not started.exists():
        assert running.poll() is None
        assert time.monotonic() < deadline
        time.sleep(0.02)

    running.kill()
    assert running.wait(timeout=5) is not None
    cleaning = subprocess.Popen(
        [str(isolated_project / "scripts" / "runtime_state.sh"), "clean"],
        cwd=isolated_project,
        env=env,
    )
    time.sleep(0.2)
    assert cleaning.poll() is None, "wrapper death released authority held by the child"
    assert finished.exists() is False
    assert cleaning.poll() is None
    assert cleaning.wait(timeout=5) == 0
    assert finished.read_text() == "finished"
    assert not target.exists()


def test_clean_recovers_an_interrupted_quarantine(
    isolated_project: Path, monkeypatch: pytest.MonkeyPatch
):
    instance = "cleanup_interrupted_quarantine"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    (target / "must-delete.txt").write_text("disposable\n")
    module = _load_runtime_state(isolated_project)

    def interrupt(event: str, _path: Path) -> None:
        if event == "after_quarantine":
            raise OSError("injected interruption")

    monkeypatch.setattr(module, "_checkpoint", interrupt)
    with pytest.raises(OSError, match="injected interruption"):
        module._run("clean", instance)
    assert (target / module.QUARANTINE_NAME).exists()

    monkeypatch.undo()
    module._run("clean", instance)
    assert not target.exists()


@pytest.mark.parametrize("command", ["prepare", "run"])
@pytest.mark.parametrize("partially_swept", [False, True])
def test_persistent_commands_refuse_a_destructive_quarantine(
    isolated_project: Path,
    command: str,
    partially_swept: bool,
):
    instance = f"cleanup_quarantine_refusal_{command}_{int(partially_swept)}"
    target = isolated_project / ".cdc_instances" / instance
    prepared = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )
    assert prepared.returncode == 0, prepared.stderr
    old_payload = target / "old-payload"
    old_payload.write_text("old\n")
    (target / ".cdc_flight_runtime_quarantining").write_bytes(b"")
    if partially_swept:
        old_payload.unlink()
    child_marker = target / "child-ran"
    child = [
        sys.executable,
        "-c",
        f"from pathlib import Path; Path({str(child_marker)!r}).touch()",
    ]

    proc = _runtime_state(
        isolated_project,
        command,
        child if command == "run" else None,
        CDC_TEST_INSTANCE_ID=instance,
    )

    assert proc.returncode == 2
    assert not child_marker.exists()
    assert "quarantining" in proc.stderr
    assert "clean" in proc.stderr


def test_hard_exit_private_root_is_reconciled_by_the_next_invocation(
    isolated_project: Path,
):
    instance = "cleanup_private_hard_exit"
    helper = isolated_project / "scripts" / "runtime_state.py"
    code = (
        "import importlib.util, os, sys; "
        f"p={str(helper)!r}; "
        "s=importlib.util.spec_from_file_location('hard_exit_runtime', p); "
        "m=importlib.util.module_from_spec(s); sys.modules[s.name]=m; s.loader.exec_module(m); "
        "m._checkpoint=lambda event, path: os._exit(99) "
        "if event == 'before_target_publish' else None; "
        f"m._run('prepare', {instance!r})"
    )
    cut = subprocess.run([sys.executable, "-c", code], cwd=isolated_project)
    assert cut.returncode == 99
    parent = isolated_project / ".cdc_instances"
    assert len(list(parent.glob(f".{instance}.provision.*"))) == 1

    recovered = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )

    assert recovered.returncode == 0, recovered.stderr
    assert not list(parent.glob(f".{instance}.provision.*"))
    assert (parent / instance / ".cdc_flight_disposable_runtime").exists()


def test_malformed_private_root_is_an_unenumerated_loud_refusal(
    isolated_project: Path,
):
    instance = "cleanup_malformed_private"
    parent = isolated_project / ".cdc_instances"
    parent.mkdir()
    malformed = parent / f".{instance}.provision.not-a-declared-shape"
    malformed.mkdir()

    refused = _runtime_state(
        isolated_project, "prepare", CDC_TEST_INSTANCE_ID=instance
    )

    assert refused.returncode == 2
    assert "unenumerated lifecycle observation" in refused.stderr
    assert malformed.exists()
