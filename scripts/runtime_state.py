#!/usr/bin/env python3
"""Descriptor-anchored management of disposable runtime state."""

from __future__ import annotations

import argparse
import os
import re
import stat
import sys
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
        os.mkdir(instance_id, dir_fd=parent_fd)
    except FileExistsError:
        pass
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
        os.write(sentinel_fd, contents)
        os.fsync(sentinel_fd)
    finally:
        os.close(sentinel_fd)


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
            try:
                child_fd = os.open(name, DIRECTORY_FLAGS, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot open runtime directory {child_path}: {exc}") from exc
            try:
                if not _same_inode(child_stat, os.fstat(child_fd)):
                    raise Refusal(f"runtime directory changed while opening: {child_path}")
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
                if not _same_inode(child_stat, os.fstat(child_fd)):
                    raise Refusal(f"runtime directory changed while opening: {child_path}")
                _delete_tree(child_fd, child_path)
            finally:
                os.close(child_fd)
            try:
                os.rmdir(name, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot remove runtime directory {child_path}: {exc}") from exc
        else:
            try:
                os.unlink(name, dir_fd=directory_fd)
            except OSError as exc:
                raise Refusal(f"cannot remove runtime file {child_path}: {exc}") from exc


def _run(project_dir: Path, command: str, instance_id: str) -> None:
    if not os.O_NOFOLLOW or not getattr(os, "O_DIRECTORY", 0):
        raise Refusal("platform does not provide required no-follow directory operations")
    if Path(os.path.realpath(project_dir, strict=True)) != project_dir:
        raise Refusal(f"project directory is not canonical: {project_dir}")

    try:
        project_fd = os.open(project_dir, DIRECTORY_FLAGS)
    except OSError as exc:
        raise Refusal(f"cannot open project directory {project_dir}: {exc}") from exc
    try:
        _verify_open_directory(project_fd, project_dir)
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
                        _create_sentinel(target_fd, expected_target, instance_id)
                    else:
                        _require_valid_sentinel(target_fd, expected_target, instance_id)
                    return

                _require_valid_sentinel(target_fd, expected_target, instance_id)
                _validate_tree(target_fd, expected_target)
                _delete_tree(target_fd, expected_target)
            finally:
                os.close(target_fd)

            try:
                current_stat = os.stat(
                    instance_id, dir_fd=parent_fd, follow_symlinks=False
                )
            except OSError as exc:
                raise Refusal(f"runtime directory changed before removal: {exc}") from exc
            if not _same_inode(current_stat, opened_target_stat):
                raise Refusal(f"runtime directory changed before removal: {expected_target}")
            try:
                os.rmdir(instance_id, dir_fd=parent_fd)
            except OSError as exc:
                raise Refusal(f"cannot remove runtime directory {expected_target}: {exc}") from exc
        finally:
            os.close(parent_fd)
    finally:
        os.close(project_fd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True, type=Path)
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

    _run(args.project_dir, args.command, instance_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, OSError) as exc:
        print(f"ERROR: refusing runtime-state operation: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
