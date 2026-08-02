#!/usr/bin/env python3
"""Locked, descriptor-owned lifecycle for one disposable runtime root."""
from __future__ import annotations

import argparse
import ctypes
import fcntl
import os
import re
import secrets
import stat
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

RUNTIME_PARENT_NAME = ".cdc_instances"
SENTINEL_NAME = ".cdc_flight_disposable_runtime"
QUARANTINE_NAME = ".cdc_flight_runtime_quarantining"
COMPLETION_PREFIX = ".cdc_flight_runtime_completion."
DIR_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_DIRECTORY | os.O_NOFOLLOW
READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
META = {SENTINEL_NAME, QUARANTINE_NAME}
Refusal = RuntimeError
Identity = tuple[int, int]
RENAME_NOREPLACE = 1
RENAME_EXCL = 4


@dataclass(frozen=True)
class _Parent:
    project_fd: int
    name: str
    fd: int
    identity: Identity
    display: Path


@dataclass(frozen=True)
class _Root:
    parent: _Parent
    name: str
    fd: int
    identity: Identity
    display: Path


def _checkpoint(name: str, path: Path) -> None:
    pass


def _identity(fd: int) -> Identity:
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


def _check_binding(
    parent_fd: int,
    name: str,
    fd: int,
    expected: Identity,
    display: Path,
    phase: str,
) -> None:
    try:
        path_info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        held = _identity(fd)
    except OSError as exc:
        raise Refusal(
            f"runtime directory left its verified parent {phase}: {display}: {exc}"
        ) from exc
    path_identity = path_info.st_dev, path_info.st_ino
    if held != expected or path_identity != expected:
        raise Refusal(f"runtime directory changed {phase}: {display}")


def _check_parent(parent: _Parent, phase: str) -> None:
    _check_binding(
        parent.project_fd,
        parent.name,
        parent.fd,
        parent.identity,
        parent.display,
        phase,
    )


def _check_root(root: _Root, phase: str) -> None:
    _check_parent(root.parent, phase)
    _check_binding(
        root.parent.fd,
        root.name,
        root.fd,
        root.identity,
        root.display,
        phase,
    )


def _read_at(fd: int, name: str, limit: int, *, missing: bool = False) -> bytes | None:
    try:
        with os.fdopen(os.open(name, READ_FLAGS, dir_fd=fd), "rb") as stream:
            return stream.read(limit)
    except FileNotFoundError:
        if missing:
            return None
        raise


def _read(root: _Root, name: str, limit: int, *, missing: bool = False) -> bytes | None:
    try:
        return _read_at(root.fd, name, limit, missing=missing)
    except FileNotFoundError as exc:
        raise Refusal(f"missing runtime metadata {root.display / name}") from exc


def _write_exclusive(fd: int, name: str, data: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= os.O_NOFOLLOW
    try:
        with os.fdopen(os.open(name, flags, 0o600, dir_fd=fd), "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=fd)
        raise


def _write(root: _Root, name: str, data: bytes) -> None:
    _write_exclusive(root.fd, name, data)


def _scan(fd: int) -> list[str]:
    with os.scandir(fd) as entries:
        return [entry.name for entry in entries]


def _validate_tree(fd: int, display: Path, root: _Root) -> None:
    for name in _scan(fd):
        if fd == root.fd and name in META:
            continue
        path = display / name
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise Refusal(f"runtime path is a symlink: {path}")
        if not stat.S_ISDIR(info.st_mode):
            continue
        _checkpoint("before_child_open", path)
        child_fd = os.open(name, DIR_FLAGS, dir_fd=fd)
        try:
            _checkpoint("after_child_open", path)
            _check_binding(
                fd,
                name,
                child_fd,
                (info.st_dev, info.st_ino),
                path,
                "while opening",
            )
            _validate_tree(child_fd, path, root)
        finally:
            os.close(child_fd)


def _delete_tree(fd: int, display: Path, root: _Root) -> None:
    for name in _scan(fd):
        if fd == root.fd and name in META:
            continue
        _check_root(root, "before deletion")
        path = display / name
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise Refusal(f"runtime path is a symlink: {path}")
        if not stat.S_ISDIR(info.st_mode):
            os.unlink(name, dir_fd=fd)
            continue
        _checkpoint("before_delete_child_open", path)
        child_fd = os.open(name, DIR_FLAGS, dir_fd=fd)
        try:
            _checkpoint("after_delete_child_open", path)
            child_identity = info.st_dev, info.st_ino
            _check_binding(
                fd, name, child_fd, child_identity, path, "while opening"
            )
            _checkpoint("after_delete_child_verify", path)
            _check_binding(
                fd, name, child_fd, child_identity, path, "before deletion"
            )
            _delete_tree(child_fd, path, root)
            _check_binding(
                fd, name, child_fd, child_identity, path, "before removal"
            )
        finally:
            os.close(child_fd)
        os.rmdir(name, dir_fd=fd)


def _sentinel(instance_id: str) -> bytes:
    return (
        "cdc_flight disposable runtime state\n" f"instance={instance_id}\n"
    ).encode()


def _rename_noreplace(parent_fd: int, source: str, target: str) -> None:
    """Atomically publish ``source`` without ever replacing ``target``."""
    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    target_bytes = os.fsencode(target)
    if sys.platform == "darwin":
        rename = libc.renameatx_np
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        result = rename(
            parent_fd, source_bytes, parent_fd, target_bytes, RENAME_EXCL
        )
    elif sys.platform.startswith("linux"):
        try:
            rename = libc.renameat2
        except AttributeError as exc:  # pragma: no cover - old libc fails closed
            raise Refusal("atomic no-replace directory publication is unavailable") from exc
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        result = rename(
            parent_fd, source_bytes, parent_fd, target_bytes, RENAME_NOREPLACE
        )
    else:  # pragma: no cover - unsupported platforms fail closed
        raise Refusal("atomic no-replace directory publication is unavailable")
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), target)


def _provision(parent: _Parent, name: str, display: Path) -> _Root:
    _check_parent(parent, "before provisioning")
    _checkpoint("before_target_mkdir", display)
    private_name = f".{name}.provision.{os.getpid()}.{secrets.token_hex(8)}"
    try:
        os.mkdir(private_name, 0o700, dir_fd=parent.fd)
    except FileExistsError as exc:
        raise Refusal(f"cannot allocate private runtime directory for {display}") from exc

    # Build and sentinel-mark the retained created inode under an unpredictable private
    # name.  Only an atomic no-replace rename publishes it at the public instance name,
    # so a foreign directory can make publication fail but can never receive a marker.
    fd = os.open(private_name, DIR_FLAGS, dir_fd=parent.fd)
    root = _Root(parent, private_name, fd, _identity(fd), display)
    try:
        _check_root(root, "while opening")
        if _scan(root.fd):
            raise Refusal(f"created runtime directory was replaced before marking: {display}")
        _write(root, SENTINEL_NAME, _sentinel(name))
        os.fsync(root.fd)
        _checkpoint("before_target_publish", display)
        try:
            _rename_noreplace(parent.fd, private_name, name)
        except FileExistsError as exc:
            raise Refusal(
                f"cannot provision {display}: existing root is not adopted; "
                "verify its sentinel"
            ) from exc
        root = _Root(parent, name, fd, root.identity, display)
        os.fsync(parent.fd)
        _checkpoint("after_target_mkdir", display)
        _check_root(root, "after publication")
        return root
    except BaseException as original:
        try:
            _check_root(root, "during rollback")
        except BaseException:
            os.close(root.fd)
            raise original from None
        for metadata in META:
            with suppress(FileNotFoundError):
                os.unlink(metadata, dir_fd=root.fd)
        with suppress(OSError):
            os.rmdir(root.name, dir_fd=parent.fd)
        os.close(root.fd)
        raise


def _record_name(instance_id: str) -> str:
    return f"{COMPLETION_PREFIX}{instance_id}"


def _record_data(root: _Root) -> bytes:
    dev, ino = root.identity
    return f"version=1\ninstance={root.name}\ndev={dev}\nino={ino}\n".encode()


def _parse_record(data: bytes, instance_id: str, display: Path) -> Identity:
    try:
        fields = dict(line.split("=", 1) for line in data.decode().splitlines())
        if fields != {
            "version": "1",
            "instance": instance_id,
            "dev": fields["dev"],
            "ino": fields["ino"],
        }:
            raise ValueError("unexpected completion fields")
        return int(fields["dev"]), int(fields["ino"])
    except (KeyError, UnicodeError, ValueError) as exc:
        raise Refusal(f"invalid parent completion record for {display}") from exc


def _ensure_completion_record(root: _Root) -> str:
    name = _record_name(root.name)
    expected = _record_data(root)
    current = _read_at(root.parent.fd, name, len(expected) + 1, missing=True)
    if current is None:
        _write_exclusive(root.parent.fd, name, expected)
        os.fsync(root.parent.fd)
    elif current != expected:
        raise Refusal(f"invalid parent completion record for {root.display}")
    return name


def _finish_recorded(root: _Root, record_name: str) -> None:
    _checkpoint("before_target_rmdir", root.display)
    _check_root(root, "before removal")
    leftovers = set(_scan(root.fd)) - META
    if leftovers:
        raise Refusal(
            f"completion record for {root.display} has unexpected payload: "
            f"{', '.join(sorted(leftovers))}"
        )
    for name in META:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=root.fd)
    os.fsync(root.fd)
    _checkpoint("after_terminal_markers_removed", root.display)
    _check_root(root, "before removal")
    os.rmdir(root.name, dir_fd=root.parent.fd)
    os.fsync(root.parent.fd)
    os.unlink(record_name, dir_fd=root.parent.fd)
    os.fsync(root.parent.fd)


def _finish(root: _Root) -> None:
    record_name = _ensure_completion_record(root)
    _checkpoint("after_parent_completion", root.display)
    _finish_recorded(root, record_name)


def _clean(root: _Root, instance_id: str) -> None:
    expected = _sentinel(instance_id)
    quarantining = _read(root, QUARANTINE_NAME, 1, missing=True) is not None
    if _read(root, SENTINEL_NAME, len(expected) + 1) != expected:
        raise Refusal(f"invalid sentinel {root.display / SENTINEL_NAME}")
    if not quarantining:
        _validate_tree(root.fd, root.display, root)
        _checkpoint("after_validation", root.display)
        _check_root(root, "before quarantine")
        _write(root, QUARANTINE_NAME, b"")
        os.fsync(root.fd)
        _checkpoint("after_quarantine", root.display)
    _checkpoint("before_delete_tree", root.display)
    _check_root(root, "before deletion")
    _validate_tree(root.fd, root.display, root)
    _delete_tree(root.fd, root.display, root)
    _finish(root)


def _open_root(parent: _Parent, instance_id: str, display: Path) -> _Root | None:
    try:
        fd = os.open(instance_id, DIR_FLAGS, dir_fd=parent.fd)
    except FileNotFoundError:
        return None
    root = _Root(parent, instance_id, fd, _identity(fd), display)
    try:
        _check_root(root, "while opening")
        return root
    except BaseException:
        os.close(fd)
        raise


def _reconcile_completion(parent: _Parent, instance_id: str, display: Path) -> bool:
    name = _record_name(instance_id)
    data = _read_at(parent.fd, name, 256, missing=True)
    if data is None:
        return False
    expected_identity = _parse_record(data, instance_id, display)
    root = _open_root(parent, instance_id, display)
    if root is None:
        os.unlink(name, dir_fd=parent.fd)
        os.fsync(parent.fd)
        return True
    try:
        if root.identity != expected_identity:
            raise Refusal(f"completion record no longer owns {display}")
        _finish_recorded(root, name)
        return True
    finally:
        os.close(root.fd)


def _open_parent(project_fd: int, project: Path, command: str) -> _Parent | None:
    try:
        fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    except FileNotFoundError:
        if command == "clean":
            return None
        os.mkdir(RUNTIME_PARENT_NAME, 0o700, dir_fd=project_fd)
        fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    parent = _Parent(
        project_fd,
        RUNTIME_PARENT_NAME,
        fd,
        _identity(fd),
        project / RUNTIME_PARENT_NAME,
    )
    try:
        _checkpoint("after_runtime_parent_open", parent.display)
        _check_parent(parent, "while opening runtime parent")
        return parent
    except BaseException:
        os.close(fd)
        raise


def _run(command: str, instance_id: str, child_command: list[str] | None = None) -> int:
    project = Path(__file__).resolve(strict=True).parent.parent
    project_fd = os.open(project, DIR_FLAGS)
    parent = None
    root = None
    try:
        # This lock is on the physical project-directory inode.  Every repository
        # runtime mutator enters through this helper; ``run`` retains it while the
        # pipeline child writes the verified instance directory.
        fcntl.flock(project_fd, fcntl.LOCK_EX)
        parent = _open_parent(project_fd, project, command)
        if parent is None:
            return 0
        display = parent.display / instance_id
        completed = _reconcile_completion(parent, instance_id, display)
        if command == "clean" and completed:
            return 0
        if command in {"prepare", "run"}:
            root = _provision(parent, instance_id, display)
        else:
            root = _open_root(parent, instance_id, display)
            if root is not None:
                _clean(root, instance_id)
        if command == "run":
            if not child_command:
                raise Refusal("run requires a command after --")
            return subprocess.run(child_command, cwd=project, check=False).returncode
        return 0
    finally:
        if root is not None:
            os.close(root.fd)
        if parent is not None:
            os.close(parent.fd)
        os.close(project_fd)


def main() -> int:
    """Parse the fixed instance selector and invoke the descriptor-owned operation."""
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "clean", "run"))
    parser.add_argument("child_command", nargs=argparse.REMAINDER)
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
    child_command = args.child_command
    if child_command[:1] == ["--"]:
        child_command = child_command[1:]
    return _run(args.command, instance_id, child_command)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Refusal, OSError) as exc:
        print(f"ERROR: refusing runtime-state operation: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
