#!/usr/bin/env python3
"""Own one disposable runtime with one retained, cooperative authority.

The authority is a lock on the physical project directory plus descriptor-relative
project, runtime-parent and instance handles. Every mutator holds that lock for its full
lifetime. ``run`` passes the project descriptor to its child, so wrapper death cannot
release the authority while the child still owns the runtime.

This is intentionally a cooperative-writer guarantee. Ordinary POSIX descriptors and
pre-operation ``stat`` calls cannot prove that an out-of-band same-user rename stays
under a directory after the last check and before a syscall. The binding checks remain
fail-closed diagnostics; the enforceable boundary is the inherited lock.
"""

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


@dataclass
class _Authority:
    """The one retained authority for a project, parent and selected instance."""

    project_fd: int
    parent_fd: int
    parent_identity: Identity
    parent_display: Path
    root_name: str | None = None
    root_fd: int | None = None
    root_identity: Identity | None = None
    root_display: Path | None = None

    def check(self, phase: str) -> None:
        """Check the retained bindings while the authority lock is held."""
        _check_binding(
            self.project_fd,
            RUNTIME_PARENT_NAME,
            self.parent_fd,
            self.parent_identity,
            self.parent_display,
            phase,
        )
        if self.root_fd is None or self.root_name is None or self.root_identity is None:
            return
        if self.root_display is None:  # pragma: no cover - internal construction guard
            raise Refusal("runtime authority has no root display path")
        _check_binding(
            self.parent_fd,
            self.root_name,
            self.root_fd,
            self.root_identity,
            self.root_display,
            phase,
        )


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


def _root_fd(authority: _Authority) -> int:
    if authority.root_fd is None:
        raise Refusal("runtime authority has no selected instance root")
    return authority.root_fd


def _close_root(authority: _Authority) -> None:
    if authority.root_fd is not None:
        with suppress(OSError):
            os.close(authority.root_fd)
    authority.root_name = None
    authority.root_fd = None
    authority.root_identity = None
    authority.root_display = None


def _set_root(
    authority: _Authority,
    name: str,
    fd: int,
    identity: Identity,
    display: Path,
) -> None:
    _close_root(authority)
    authority.root_name = name
    authority.root_fd = fd
    authority.root_identity = identity
    authority.root_display = display


def _read_at(fd: int, name: str, limit: int, *, missing: bool = False) -> bytes | None:
    try:
        with os.fdopen(os.open(name, READ_FLAGS, dir_fd=fd), "rb") as stream:
            return stream.read(limit)
    except FileNotFoundError:
        if missing:
            return None
        raise


def _read(authority: _Authority, name: str, limit: int, *, missing: bool = False) -> bytes | None:
    try:
        return _read_at(_root_fd(authority), name, limit, missing=missing)
    except FileNotFoundError as exc:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"missing runtime metadata {display / name}") from exc


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


def _write(authority: _Authority, name: str, data: bytes) -> None:
    _write_exclusive(_root_fd(authority), name, data)


def _scan(fd: int) -> list[str]:
    with os.scandir(fd) as entries:
        return [entry.name for entry in entries]


def _validate_tree(fd: int, display: Path, authority: _Authority) -> None:
    for name in _scan(fd):
        if fd == authority.root_fd and name in META:
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
            _validate_tree(child_fd, path, authority)
        finally:
            os.close(child_fd)


def _delete_tree(fd: int, display: Path, authority: _Authority) -> None:
    for name in _scan(fd):
        if fd == authority.root_fd and name in META:
            continue
        authority.check("before deletion")
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
            _check_binding(fd, name, child_fd, child_identity, path, "while opening")
            _checkpoint("after_delete_child_verify", path)
            _check_binding(fd, name, child_fd, child_identity, path, "before deletion")
            _delete_tree(child_fd, path, authority)
            _check_binding(fd, name, child_fd, child_identity, path, "before removal")
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
        result = rename(parent_fd, source_bytes, parent_fd, target_bytes, RENAME_EXCL)
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


def _provision(authority: _Authority, instance_id: str, display: Path) -> None:
    authority.check("before provisioning")
    _checkpoint("before_target_mkdir", display)
    private_name = (
        f".{instance_id}.provision.{os.getpid()}.{secrets.token_hex(8)}"
    )
    try:
        os.mkdir(private_name, 0o700, dir_fd=authority.parent_fd)
    except FileExistsError as exc:
        raise Refusal(f"cannot allocate private runtime directory for {display}") from exc

    # mkdir has no portable fd-returning form. The retained project lock makes the
    # mkdir -> open -> fstat sequence one authority critical section for cooperating
    # writers; the fd, not a later public-name stat, owns all marking and publication.
    try:
        fd = os.open(private_name, DIR_FLAGS, dir_fd=authority.parent_fd)
    except BaseException:
        with suppress(OSError):
            os.rmdir(private_name, dir_fd=authority.parent_fd)
        raise
    _set_root(authority, private_name, fd, _identity(fd), display)
    try:
        authority.check("while opening")
        if _scan(_root_fd(authority)):
            raise Refusal(f"created runtime directory was replaced before marking: {display}")
        _write(authority, SENTINEL_NAME, _sentinel(instance_id))
        os.fsync(_root_fd(authority))
        _checkpoint("before_target_publish", display)
        try:
            _rename_noreplace(authority.parent_fd, private_name, instance_id)
        except FileExistsError as exc:
            raise Refusal(
                f"cannot provision {display}: existing root is not adopted; "
                "verify its sentinel"
            ) from exc
        authority.root_name = instance_id
        os.fsync(authority.parent_fd)
        _checkpoint("after_target_mkdir", display)
        authority.check("after publication")
    except BaseException as original:
        try:
            authority.check("during rollback")
        except BaseException:
            _close_root(authority)
            raise original from None
        for metadata in META:
            with suppress(FileNotFoundError):
                os.unlink(metadata, dir_fd=_root_fd(authority))
        with suppress(OSError):
            os.rmdir(authority.root_name, dir_fd=authority.parent_fd)
        _close_root(authority)
        raise


def _record_name(instance_id: str) -> str:
    return f"{COMPLETION_PREFIX}{instance_id}"


def _record_data(authority: _Authority) -> bytes:
    if authority.root_name is None or authority.root_identity is None:
        raise Refusal("cannot record an unselected runtime root")
    dev, ino = authority.root_identity
    return f"version=1\ninstance={authority.root_name}\ndev={dev}\nino={ino}\n".encode()


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


def _ensure_completion_record(authority: _Authority) -> str:
    if authority.root_name is None:
        raise Refusal("cannot complete an unselected runtime root")
    name = _record_name(authority.root_name)
    expected = _record_data(authority)
    current = _read_at(authority.parent_fd, name, len(expected) + 1, missing=True)
    if current is None:
        _write_exclusive(authority.parent_fd, name, expected)
        os.fsync(authority.parent_fd)
    elif current != expected:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"invalid parent completion record for {display}")
    return name


def _finish_recorded(authority: _Authority, record_name: str) -> None:
    if authority.root_name is None or authority.root_display is None:
        raise Refusal("cannot finish an unselected runtime root")
    _checkpoint("before_target_rmdir", authority.root_display)
    authority.check("before removal")
    leftovers = set(_scan(_root_fd(authority))) - META
    if leftovers:
        raise Refusal(
            f"completion record for {authority.root_display} has unexpected payload: "
            f"{', '.join(sorted(leftovers))}"
        )
    for name in META:
        with suppress(FileNotFoundError):
            os.unlink(name, dir_fd=_root_fd(authority))
    os.fsync(_root_fd(authority))
    _checkpoint("after_terminal_markers_removed", authority.root_display)
    authority.check("before removal")
    os.rmdir(authority.root_name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)
    os.unlink(record_name, dir_fd=authority.parent_fd)
    os.fsync(authority.parent_fd)


def _finish(authority: _Authority) -> None:
    record_name = _ensure_completion_record(authority)
    display = authority.root_display or authority.parent_display
    _checkpoint("after_parent_completion", display)
    _finish_recorded(authority, record_name)


def _clean(authority: _Authority, instance_id: str) -> None:
    expected = _sentinel(instance_id)
    quarantining = _read(authority, QUARANTINE_NAME, 1, missing=True) is not None
    if _read(authority, SENTINEL_NAME, len(expected) + 1) != expected:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"invalid sentinel {display / SENTINEL_NAME}")
    display = authority.root_display or authority.parent_display
    if not quarantining:
        _validate_tree(_root_fd(authority), display, authority)
        _checkpoint("after_validation", display)
        authority.check("before quarantine")
        _write(authority, QUARANTINE_NAME, b"")
        os.fsync(_root_fd(authority))
        _checkpoint("after_quarantine", display)
    _checkpoint("before_delete_tree", display)
    authority.check("before deletion")
    _validate_tree(_root_fd(authority), display, authority)
    _delete_tree(_root_fd(authority), display, authority)
    _finish(authority)


def _open_root(authority: _Authority, instance_id: str, display: Path) -> bool:
    try:
        fd = os.open(instance_id, DIR_FLAGS, dir_fd=authority.parent_fd)
    except FileNotFoundError:
        return False
    _set_root(authority, instance_id, fd, _identity(fd), display)
    try:
        authority.check("while opening")
        return True
    except BaseException:
        _close_root(authority)
        raise


def _open_owned_root(authority: _Authority, instance_id: str, display: Path) -> bool:
    """Reopen only the exact sentinel-owned root; never adopt a directory by name."""
    if not _open_root(authority, instance_id, display):
        return False
    expected = _sentinel(instance_id)
    try:
        if _read(authority, SENTINEL_NAME, len(expected) + 1) != expected:
            raise Refusal(f"invalid sentinel {display / SENTINEL_NAME}")
        return True
    except BaseException:
        _close_root(authority)
        raise


def _reconcile_completion(
    authority: _Authority, instance_id: str, display: Path
) -> bool:
    name = _record_name(instance_id)
    data = _read_at(authority.parent_fd, name, 256, missing=True)
    if data is None:
        return False
    expected_identity = _parse_record(data, instance_id, display)
    if not _open_root(authority, instance_id, display):
        authority.check("before completion-record removal")
        os.unlink(name, dir_fd=authority.parent_fd)
        os.fsync(authority.parent_fd)
        return True
    try:
        if authority.root_identity != expected_identity:
            raise Refusal(f"completion record no longer owns {display}")
        _finish_recorded(authority, name)
        return True
    finally:
        _close_root(authority)


def _open_authority(project_fd: int, project: Path, command: str) -> _Authority | None:
    try:
        parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    except FileNotFoundError:
        if command == "clean":
            return None
        os.mkdir(RUNTIME_PARENT_NAME, 0o700, dir_fd=project_fd)
        parent_fd = os.open(RUNTIME_PARENT_NAME, DIR_FLAGS, dir_fd=project_fd)
    authority = _Authority(
        project_fd=project_fd,
        parent_fd=parent_fd,
        parent_identity=_identity(parent_fd),
        parent_display=project / RUNTIME_PARENT_NAME,
    )
    try:
        _checkpoint("after_runtime_parent_open", authority.parent_display)
        authority.check("while opening runtime parent")
        return authority
    except BaseException:
        os.close(parent_fd)
        raise


def _run(command: str, instance_id: str, child_command: list[str] | None = None) -> int:
    project = Path(__file__).resolve(strict=True).parent.parent
    project_fd = os.open(project, DIR_FLAGS)
    authority = None
    try:
        # This lock is the authority, not an advisory hint: every cooperating runtime
        # writer holds it for its whole mutation lifetime, and run passes this fd to its
        # child so wrapper death cannot release it early.
        fcntl.flock(project_fd, fcntl.LOCK_EX)
        authority = _open_authority(project_fd, project, command)
        if authority is None:
            return 0
        display = authority.parent_display / instance_id
        if command == "run" and not child_command:
            raise Refusal("run requires a command after --")
        completed = _reconcile_completion(authority, instance_id, display)
        if command == "clean" and completed:
            return 0
        if command in {"prepare", "run"}:
            if not _open_owned_root(authority, instance_id, display):
                _provision(authority, instance_id, display)
        elif _open_owned_root(authority, instance_id, display):
            _clean(authority, instance_id)
        if command == "run":
            return subprocess.run(
                child_command,
                cwd=project,
                check=False,
                pass_fds=(project_fd,),
            ).returncode
        return 0
    finally:
        if authority is not None:
            _close_root(authority)
            with suppress(OSError):
                os.close(authority.parent_fd)
        with suppress(OSError):
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
