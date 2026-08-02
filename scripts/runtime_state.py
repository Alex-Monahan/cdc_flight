#!/usr/bin/env python3
"""Descriptor-relative lifecycle for one disposable runtime root."""
from __future__ import annotations

import argparse
import fcntl
import os
import re
import stat
import sys
from collections import namedtuple
from contextlib import suppress
from pathlib import Path

RUNTIME_PARENT_NAME = ".cdc_instances"
SENTINEL_NAME = ".cdc_flight_disposable_runtime"
QUARANTINE_NAME = ".cdc_flight_runtime_quarantining"
DELETED_NAME = ".cdc_flight_runtime_deleted"
DIR_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_DIRECTORY | os.O_NOFOLLOW
READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
META = {SENTINEL_NAME, QUARANTINE_NAME, DELETED_NAME}
Refusal = RuntimeError
_Root = namedtuple("_Root", "parent_fd name fd identity display")
def _checkpoint(name: str, path: Path) -> None: pass
def _check(parent_fd, name, fd, display, expected=None, phase=None):
    try:
        path_stat, fd_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False), os.fstat(fd)
    except OSError as exc:
        message = f"runtime directory changed {phase}: {display}" if phase else f"runtime directory left its verified parent {display}: {exc}"
        raise Refusal(message) from exc
    if expected is not None and (expected.st_dev, expected.st_ino) != (fd_stat.st_dev, fd_stat.st_ino):
        message = f"runtime directory changed {phase}: {display}" if phase else f"runtime directory changed while opening {display}"
        raise Refusal(message)
    if (path_stat.st_dev, path_stat.st_ino) != (fd_stat.st_dev, fd_stat.st_ino):
        message = f"runtime directory changed {phase}: {display}" if phase else f"runtime directory left its verified parent {display}"
        raise Refusal(message)
def _read(root, name, limit, missing=False):
    try:
        with os.fdopen(os.open(name, READ_FLAGS, dir_fd=root.fd), "rb") as stream:
            return stream.read(limit)
    except FileNotFoundError as exc:
        if missing: return None  # noqa: E701
        raise Refusal(f"missing runtime metadata {root.display / name}") from exc
def _write(root, name, data):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        with os.fdopen(os.open(name, flags, 0o600, dir_fd=root.fd), "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=root.fd)
        raise
def _walk(fd, display, validate, root=None):
    with os.scandir(fd) as entries:
        names = [entry.name for entry in entries]
    for name in names:
        if root is not None and fd == root.fd and name in META:
            continue
        if not validate and root is not None:
            _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "before deletion")
        path = display / name
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise Refusal(f"runtime path is a symlink: {path}")
        if not stat.S_ISDIR(info.st_mode):
            if not validate: os.unlink(name, dir_fd=fd)  # noqa: E701
            continue
        _checkpoint("before_child_open" if validate else "before_delete_child_open", path)
        child_fd = os.open(name, DIR_FLAGS, dir_fd=fd)
        try:
            _checkpoint("after_child_open" if validate else "after_delete_child_open", path)
            _check(fd, name, child_fd, path, info)
            if not validate:
                _checkpoint("after_delete_child_verify", path)
                _check(fd, name, child_fd, path, info)
            _walk(child_fd, path, validate, root)
        finally:
            os.close(child_fd)
        if not validate:
            try:
                current = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except OSError as exc:
                raise Refusal(f"runtime directory changed before removal {path}: {exc}") from exc
            if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
                raise Refusal(f"runtime directory changed before removal: {path}")
            os.rmdir(name, dir_fd=fd)
def _provision(parent_fd, name, display):
    _checkpoint("before_target_mkdir", display)
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError as exc:
        raise Refusal(f"cannot provision {display}: existing root is not adopted; verify its sentinel") from exc
    identity = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _checkpoint("after_target_mkdir", display)
    fd = os.open(name, DIR_FLAGS, dir_fd=parent_fd)
    try:
        _check(parent_fd, name, fd, display, identity)
    except BaseException: os.close(fd); raise  # noqa: E701, E702
    root = _Root(parent_fd, name, fd, identity, display)
    try:
        _checkpoint("after_target_open", display)
        _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "while opening")
        _write(root, SENTINEL_NAME, ("cdc_flight disposable runtime state\n" f"instance={name}\n").encode())
        return root
    except BaseException:
        try:
            _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "during rollback")
        except BaseException:
            os.close(root.fd)
            raise
        with suppress(OSError):
            os.rmdir(name, dir_fd=parent_fd)
        os.close(root.fd)
        raise
def _finish(root):
    _write(root, DELETED_NAME, b"") if _read(root, DELETED_NAME, 1, missing=True) is None else None
    _checkpoint("before_target_rmdir", root.display)
    _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "before removal")
    try:
        for name in (SENTINEL_NAME, QUARANTINE_NAME, DELETED_NAME):
            with suppress(FileNotFoundError):
                os.unlink(name, dir_fd=root.fd)
        os.rmdir(root.name, dir_fd=root.parent_fd)
    except OSError as exc:
        with suppress(OSError, Refusal):
            _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "during recovery")
            _write(root, DELETED_NAME, b"")
        raise Refusal(f"cannot remove runtime directory {root.display}: {exc}") from exc
def _clean(root, instance_id):
    state = "deleted" if _read(root, DELETED_NAME, 1, missing=True) is not None else "quarantining" if _read(root, QUARANTINE_NAME, 1, missing=True) is not None else "active"
    if state == "deleted":
        _finish(root)
        return
    expected = ("cdc_flight disposable runtime state\n" f"instance={instance_id}\n").encode()
    if _read(root, SENTINEL_NAME, len(expected) + 1) != expected:
        raise Refusal(f"invalid sentinel {root.display / SENTINEL_NAME}")
    if state == "active":
        _walk(root.fd, root.display, True, root)
        _checkpoint("after_validation", root.display)
        _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "before quarantine")
        _write(root, QUARANTINE_NAME, b"")
        _checkpoint("after_quarantine", root.display)
    _checkpoint("before_delete_tree", root.display)
    _check(root.parent_fd, root.name, root.fd, root.display, root.identity, "before deletion")
    _walk(root.fd, root.display, True, root)
    _walk(root.fd, root.display, False, root)
    _finish(root)
def _run(command, instance_id):
    project = Path(__file__).resolve(strict=True).parent.parent
    project_fd = os.open(project, DIR_FLAGS)
    root = None
    try:
        fcntl.flock(project_fd, fcntl.LOCK_EX)
        try:
            parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
        except FileNotFoundError:
            if command == "clean":
                return
            os.mkdir(RUNTIME_PARENT_NAME, 0o700, dir_fd=project_fd)
            parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
        try:
            display = project / RUNTIME_PARENT_NAME / instance_id
            if command == "prepare":
                root = _provision(parent_fd, instance_id, display)
            else:
                try:
                    root_fd = os.open(instance_id, DIR_FLAGS, dir_fd=parent_fd)
                except FileNotFoundError:
                    pass
                else:
                    root = _Root(parent_fd, instance_id, root_fd, os.fstat(root_fd), display)
                    _check(parent_fd, instance_id, root_fd, display, root.identity, "while opening")
            if command == "clean" and root is not None:
                _clean(root, instance_id)
        finally:
            if root is not None:
                os.close(root.fd)
            os.close(parent_fd)
    finally:
        os.close(project_fd)
def main() -> int:
    """Parse the fixed instance selector and invoke the descriptor-owned operation."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "clean"))
    args = parser.parse_args()
    if "CDC_INSTANCE_RUNTIME_ROOT" in os.environ:
        raise Refusal("cleanup from CDC_INSTANCE_RUNTIME_ROOT; select CDC_TEST_INSTANCE_ID instead")
    instance_id = os.environ.get("CDC_TEST_INSTANCE_ID", f"pg{os.environ.get('CDC_TEST_PGPORT', '15432')}")
    if re.fullmatch(r"[a-z0-9][a-z0-9_]*", instance_id) is None:
        raise Refusal(f"path for invalid CDC_TEST_INSTANCE_ID {instance_id!r}")
    _run(args.command, instance_id)
if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, OSError) as exc:
        print(f"ERROR: refusing runtime-state operation: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
