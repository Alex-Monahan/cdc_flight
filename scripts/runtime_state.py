#!/usr/bin/env python3
"""Descriptor-anchored management of disposable runtime state."""

from __future__ import annotations

import argparse
import fcntl
import os
import re
import secrets
import stat
import sys
from contextlib import suppress
from pathlib import Path

RUNTIME_PARENT_NAME = ".cdc_instances"
SENTINEL_NAME = ".cdc_flight_disposable_runtime"
DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)


class Refusal(RuntimeError):
    """A runtime path failed its deletion-authority checks."""


def _sentinel_contents(instance_id: str) -> bytes:
    return (
        "cdc_flight disposable runtime state\n"
        f"instance={instance_id}\n"
    ).encode()


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _checkpoint(name: str, path: Path) -> None:
    """Expose deterministic mutation boundaries to the safety regressions."""


def _project_dir() -> Path:
    """Derive deletion authority from this helper's physical installation."""
    helper = Path(__file__).resolve(strict=True)
    if helper.name != "runtime_state.py" or helper.parent.name != "scripts":
        raise Refusal(f"runtime-state helper has an unexpected physical path: {helper}")
    return helper.parent.parent


def _verify_open_directory(fd: int, expected_path: Path) -> None:
    """Prove an open no-follow descriptor still names the exact canonical path."""
    try:
        resolved_path = Path(os.path.realpath(expected_path, strict=True))
        path_stat = os.lstat(expected_path)
        fd_stat = os.fstat(fd)
    except OSError as exc:
        raise Refusal(f"cannot verify directory {expected_path}: {exc}") from exc
    if resolved_path != expected_path:
        raise Refusal(
            f"directory resolves outside its canonical path: "
            f"{expected_path} -> {resolved_path}"
        )
    if stat.S_ISLNK(path_stat.st_mode):
        raise Refusal(f"directory is a symlink: {expected_path}")
    if not stat.S_ISDIR(path_stat.st_mode) or not stat.S_ISDIR(fd_stat.st_mode):
        raise Refusal(f"path is not a directory: {expected_path}")
    if not _same_inode(path_stat, fd_stat):
        raise Refusal(f"directory changed while being verified: {expected_path}")


def _open_child_directory(parent_fd: int, name: str, expected_path: Path) -> int:
    try:
        child_fd = os.open(name, DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise Refusal(f"cannot open directory without following links {expected_path}: {exc}") from exc
    try:
        _verify_open_directory(child_fd, expected_path)
    except BaseException:
        os.close(child_fd)
        raise
    return child_fd


def _verify_child_directory(
    parent_fd: int,
    name: str,
    child_fd: int,
    expected_path: Path,
    expected_stat: os.stat_result | None = None,
) -> os.stat_result:
    """Prove that an open directory remains bound to its parent and name."""
    try:
        path_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        fd_stat = os.fstat(child_fd)
    except OSError as exc:
        raise Refusal(f"directory left its verified parent {expected_path}: {exc}") from exc
    if not stat.S_ISDIR(path_stat.st_mode) or not stat.S_ISDIR(fd_stat.st_mode):
        raise Refusal(f"path is not a directory: {expected_path}")
    if not _same_inode(path_stat, fd_stat):
        raise Refusal(f"directory changed while bound to its parent: {expected_path}")
    if expected_stat is not None and not _same_inode(expected_stat, fd_stat):
        raise Refusal(f"directory changed while opening: {expected_path}")
    return fd_stat


def _open_runtime_parent(project_fd: int, project_dir: Path, *, create: bool) -> int | None:
    expected_parent = project_dir / RUNTIME_PARENT_NAME
    try:
        return _open_child_directory(project_fd, RUNTIME_PARENT_NAME, expected_parent)
    except Refusal as exc:
        cause = exc.__cause__
        if not isinstance(cause, FileNotFoundError):
            raise
        if not create:
            return None

    try:
        os.mkdir(RUNTIME_PARENT_NAME, dir_fd=project_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise Refusal(f"cannot create runtime parent {expected_parent}: {exc}") from exc
    return _open_child_directory(project_fd, RUNTIME_PARENT_NAME, expected_parent)


def _open_runtime_target(
    parent_fd: int,
    expected_parent: Path,
    instance_id: str,
    *,
    create: bool,
) -> tuple[int | None, bool]:
    expected_target = expected_parent / instance_id
    try:
        return _open_child_directory(parent_fd, instance_id, expected_target), False
    except Refusal as exc:
        cause = exc.__cause__
        if not isinstance(cause, FileNotFoundError):
            raise
        if not create:
            return None, False

    try:
        _checkpoint("before_target_mkdir", expected_target)
        os.mkdir(instance_id, dir_fd=parent_fd)
    except FileExistsError:
        target_fd = _open_child_directory(parent_fd, instance_id, expected_target)
        return target_fd, False
    except OSError as exc:
        raise Refusal(f"cannot create runtime directory {expected_target}: {exc}") from exc

    target_fd = _open_child_directory(parent_fd, instance_id, expected_target)
    return target_fd, True


def _require_valid_sentinel(target_fd: int, expected_target: Path, instance_id: str) -> None:
    expected = _sentinel_contents(instance_id)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        sentinel_fd = os.open(SENTINEL_NAME, flags, dir_fd=target_fd)
    except OSError as exc:
        raise Refusal(f"missing or unsafe sentinel {expected_target / SENTINEL_NAME}: {exc}") from exc
    try:
        sentinel_stat = os.fstat(sentinel_fd)
        contents = os.read(sentinel_fd, len(expected) + 1)
    finally:
        os.close(sentinel_fd)
    if not stat.S_ISREG(sentinel_stat.st_mode) or contents != expected:
        raise Refusal(f"invalid sentinel {expected_target / SENTINEL_NAME}")


def _create_sentinel(target_fd: int, expected_target: Path, instance_id: str) -> None:
    contents = _sentinel_contents(instance_id)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        sentinel_fd = os.open(SENTINEL_NAME, flags, 0o600, dir_fd=target_fd)
    except OSError as exc:
        raise Refusal(f"cannot create sentinel {expected_target / SENTINEL_NAME}: {exc}") from exc
    try:
        try:
            written = os.write(sentinel_fd, contents)
            if written != len(contents):
                raise OSError(f"short sentinel write: {written} of {len(contents)} bytes")
            os.fsync(sentinel_fd)
        finally:
            os.close(sentinel_fd)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(SENTINEL_NAME, dir_fd=target_fd)
        raise


def _validate_tree(directory_fd: int, display_path: Path) -> None:
    """Reject symlinks before deletion and open every directory without following."""
    with os.scandir(directory_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        child_path = display_path / name
        try:
            child_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as exc:
            raise Refusal(f"cannot inspect runtime path {child_path}: {exc}") from exc
        if stat.S_ISLNK(child_stat.st_mode):
            raise Refusal(f"runtime path is a symlink: {child_path}")
        if stat.S_ISDIR(child_stat.st_mode):
            _checkpoint("before_child_open", child_path)
            try:
                child_fd = os.open(name, DIRECTORY_FLAGS, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot open runtime directory {child_path}: {exc}") from exc
            try:
                _checkpoint("after_child_open", child_path)
                _verify_child_directory(
                    directory_fd, name, child_fd, child_path, child_stat
                )
                _validate_tree(child_fd, child_path)
            finally:
                os.close(child_fd)


def _delete_tree(directory_fd: int, display_path: Path) -> None:
    """Delete relative to verified descriptors, never through path resolution."""
    with os.scandir(directory_fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        child_path = display_path / name
        try:
            child_stat = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise Refusal(f"cannot inspect runtime path {child_path}: {exc}") from exc
        if stat.S_ISLNK(child_stat.st_mode):
            raise Refusal(f"runtime path became a symlink: {child_path}")
        if stat.S_ISDIR(child_stat.st_mode):
            try:
                child_fd = os.open(name, DIRECTORY_FLAGS, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot open runtime directory {child_path}: {exc}") from exc
            try:
                _verify_child_directory(
                    directory_fd, name, child_fd, child_path, child_stat
                )
                _delete_tree(child_fd, child_path)
            finally:
                os.close(child_fd)
            try:
                current_stat = os.stat(
                    name, dir_fd=directory_fd, follow_symlinks=False
                )
            except OSError as exc:
                raise Refusal(
                    f"runtime directory changed before removal {child_path}: {exc}"
                ) from exc
            if not _same_inode(child_stat, current_stat):
                raise Refusal(f"runtime directory changed before removal: {child_path}")
            try:
                os.rmdir(name, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot remove runtime directory {child_path}: {exc}") from exc
        else:
            try:
                os.unlink(name, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot remove runtime file {child_path}: {exc}") from exc


def _remove_created_target(
    parent_fd: int,
    instance_id: str,
    target_fd: int,
    target_stat: os.stat_result,
    expected_target: Path,
) -> None:
    """Roll back only the directory inode created by this prepare invocation."""
    _verify_child_directory(
        parent_fd, instance_id, target_fd, expected_target, target_stat
    )
    try:
        os.rmdir(instance_id, dir_fd=parent_fd)
    except OSError as exc:
        raise Refusal(f"cannot roll back runtime directory {expected_target}: {exc}") from exc


def _create_quarantine(
    parent_fd: int, expected_parent: Path, instance_id: str
) -> tuple[str, int, Path]:
    """Create a private, parent-anchored directory for atomic target quarantine."""
    for _attempt in range(8):
        name = f".{instance_id}.deleting.{os.getpid()}.{secrets.token_hex(8)}"
        path = expected_parent / name
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        except FileExistsError:
            continue
        except OSError as exc:
            raise Refusal(f"cannot create runtime quarantine {path}: {exc}") from exc
        try:
            return name, _open_child_directory(parent_fd, name, path), path
        except BaseException:
            with suppress(OSError):
                os.rmdir(name, dir_fd=parent_fd)
            raise
    raise Refusal(f"cannot allocate runtime quarantine beneath {expected_parent}")


def _restore_quarantined_target(
    parent_fd: int,
    quarantine_fd: int,
    quarantine_name: str,
    instance_id: str,
) -> None:
    """Best-effort restoration before any destructive work has begun."""
    try:
        os.stat(instance_id, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    except OSError:
        return
    else:
        return
    try:
        os.rename(
            "target",
            instance_id,
            src_dir_fd=quarantine_fd,
            dst_dir_fd=parent_fd,
        )
    except OSError:
        return
    with suppress(OSError):
        os.rmdir(quarantine_name, dir_fd=parent_fd)


def _quarantine_target(
    parent_fd: int,
    expected_parent: Path,
    instance_id: str,
    opened_target_stat: os.stat_result,
) -> tuple[str, int, int, Path]:
    quarantine_name, quarantine_fd, quarantine_path = _create_quarantine(
        parent_fd, expected_parent, instance_id
    )
    try:
        try:
            os.rename(
                instance_id,
                "target",
                src_dir_fd=parent_fd,
                dst_dir_fd=quarantine_fd,
            )
        except OSError as exc:
            raise Refusal(
                f"runtime directory changed before quarantine: "
                f"{expected_parent / instance_id}: {exc}"
            ) from exc
        quarantined_path = quarantine_path / "target"
        target_fd = _open_child_directory(
            quarantine_fd, "target", quarantined_path
        )
        try:
            _verify_child_directory(
                quarantine_fd,
                "target",
                target_fd,
                quarantined_path,
                opened_target_stat,
            )
        except BaseException:
            os.close(target_fd)
            _restore_quarantined_target(
                parent_fd, quarantine_fd, quarantine_name, instance_id
            )
            raise
        return quarantine_name, quarantine_fd, target_fd, quarantined_path
    except BaseException:
        os.close(quarantine_fd)
        with suppress(OSError):
            os.rmdir(quarantine_name, dir_fd=parent_fd)
        raise


def _clean_target(
    parent_fd: int,
    expected_parent: Path,
    instance_id: str,
    target_fd: int,
    opened_target_stat: os.stat_result,
) -> None:
    expected_target = expected_parent / instance_id
    _require_valid_sentinel(target_fd, expected_target, instance_id)
    _validate_tree(target_fd, expected_target)
    _checkpoint("after_validation", expected_target)

    quarantine_name, quarantine_fd, quarantined_fd, quarantined_path = (
        _quarantine_target(
            parent_fd,
            expected_parent,
            instance_id,
            opened_target_stat,
        )
    )
    quarantined_fd_open = True
    try:
        try:
            _verify_child_directory(
                parent_fd,
                quarantine_name,
                quarantine_fd,
                expected_parent / quarantine_name,
            )
            _require_valid_sentinel(quarantined_fd, quarantined_path, instance_id)
            _validate_tree(quarantined_fd, quarantined_path)
        except BaseException:
            os.close(quarantined_fd)
            quarantined_fd_open = False
            _restore_quarantined_target(
                parent_fd, quarantine_fd, quarantine_name, instance_id
            )
            raise

        _delete_tree(quarantined_fd, quarantined_path)
        os.close(quarantined_fd)
        quarantined_fd_open = False
        _checkpoint("before_target_rmdir", quarantined_path)
        try:
            current_stat = os.stat(
                "target", dir_fd=quarantine_fd, follow_symlinks=False
            )
        except OSError as exc:
            raise Refusal(f"runtime directory changed before removal: {exc}") from exc
        if not _same_inode(current_stat, opened_target_stat):
            _restore_quarantined_target(
                parent_fd, quarantine_fd, quarantine_name, instance_id
            )
            raise Refusal(f"runtime directory changed before removal: {expected_target}")
        try:
            os.rmdir("target", dir_fd=quarantine_fd)
        except OSError as exc:
            raise Refusal(f"cannot remove runtime directory {expected_target}: {exc}") from exc
        _verify_child_directory(
            parent_fd,
            quarantine_name,
            quarantine_fd,
            expected_parent / quarantine_name,
        )
    finally:
        if quarantined_fd_open:
            with suppress(OSError):
                os.close(quarantined_fd)
        os.close(quarantine_fd)
    try:
        os.rmdir(quarantine_name, dir_fd=parent_fd)
    except OSError as exc:
        raise Refusal(
            f"cannot remove runtime quarantine {expected_parent / quarantine_name}: {exc}"
        ) from exc


def _run(command: str, instance_id: str) -> None:
    if not os.O_NOFOLLOW or not getattr(os, "O_DIRECTORY", 0):
        raise Refusal("platform does not provide required no-follow directory operations")
    project_dir = _project_dir()
    if Path(os.path.realpath(project_dir, strict=True)) != project_dir:
        raise Refusal(f"project directory is not canonical: {project_dir}")

    try:
        project_fd = os.open(project_dir, DIRECTORY_FLAGS)
    except OSError as exc:
        raise Refusal(f"cannot open project directory {project_dir}: {exc}") from exc
    try:
        _verify_open_directory(project_fd, project_dir)
        try:
            fcntl.flock(project_fd, fcntl.LOCK_EX)
        except OSError as exc:
            raise Refusal(f"cannot lock physical project {project_dir}: {exc}") from exc
        parent_fd = _open_runtime_parent(
            project_fd, project_dir, create=command == "prepare"
        )
        if parent_fd is None:
            return
        try:
            expected_parent = project_dir / RUNTIME_PARENT_NAME
            target_fd, created = _open_runtime_target(
                parent_fd,
                expected_parent,
                instance_id,
                create=command == "prepare",
            )
            if target_fd is None:
                return
            try:
                expected_target = expected_parent / instance_id
                opened_target_stat = os.fstat(target_fd)
                if command == "prepare":
                    if created:
                        try:
                            _create_sentinel(target_fd, expected_target, instance_id)
                        except BaseException:
                            _remove_created_target(
                                parent_fd,
                                instance_id,
                                target_fd,
                                opened_target_stat,
                                expected_target,
                            )
                            raise
                    else:
                        _require_valid_sentinel(target_fd, expected_target, instance_id)
                    return

                _clean_target(
                    parent_fd,
                    expected_parent,
                    instance_id,
                    target_fd,
                    opened_target_stat,
                )
            finally:
                os.close(target_fd)
        finally:
            os.close(parent_fd)
    finally:
        os.close(project_fd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "clean"))
    args = parser.parse_args()

    if "CDC_INSTANCE_RUNTIME_ROOT" in os.environ:
        raise Refusal(
            "cleanup from CDC_INSTANCE_RUNTIME_ROOT; select CDC_TEST_INSTANCE_ID instead"
        )
    instance_id = os.environ.get(
        "CDC_TEST_INSTANCE_ID", f"pg{os.environ.get('CDC_TEST_PGPORT', '15432')}"
    )
    if re.fullmatch(r"[a-z0-9][a-z0-9_]*", instance_id) is None:
        raise Refusal(f"path for invalid CDC_TEST_INSTANCE_ID {instance_id!r}")

    _run(args.command, instance_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, OSError) as exc:
        print(f"ERROR: refusing runtime-state operation: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
