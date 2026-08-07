"""Descriptor-bound filesystem authority for disposable runtime state."""

from __future__ import annotations

import ctypes
import os
import stat
import sys
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path

RUNTIME_PARENT_NAME = ".cdc_instances"
DIR_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_DIRECTORY | os.O_NOFOLLOW
READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
Refusal = RuntimeError
Identity = tuple[int, int]


@dataclass(frozen=True)
class RootBinding:
    """A selected root is complete or absent; partial authority is unrepresentable."""

    name: str
    fd: int
    identity: Identity
    display: Path


@dataclass
class Authority:
    """Retained project, parent, and optional complete root authority."""

    project_fd: int
    parent_fd: int
    parent_identity: Identity
    parent_display: Path
    root: RootBinding | None = None

    @property
    def root_name(self) -> str | None:
        return self.root.name if self.root is not None else None

    @property
    def root_fd(self) -> int | None:
        return self.root.fd if self.root is not None else None

    @property
    def root_identity(self) -> Identity | None:
        return self.root.identity if self.root is not None else None

    @property
    def root_display(self) -> Path | None:
        return self.root.display if self.root is not None else None

    def check(self, phase: str) -> None:
        check_binding(
            self.project_fd,
            RUNTIME_PARENT_NAME,
            self.parent_fd,
            self.parent_identity,
            self.parent_display,
            phase,
        )
        binding = self.root
        if binding is not None:
            check_binding(
                self.parent_fd,
                binding.name,
                binding.fd,
                binding.identity,
                binding.display,
                phase,
            )


def identity(fd: int) -> Identity:
    info = os.fstat(fd)
    return info.st_dev, info.st_ino


def check_binding(
    parent_fd: int,
    name: str,
    fd: int,
    expected: Identity,
    display: Path,
    phase: str,
) -> None:
    try:
        path_info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        held = identity(fd)
    except OSError as exc:
        raise Refusal(
            f"runtime directory left its verified parent {phase}: {display}: {exc}"
        ) from exc
    path_identity = path_info.st_dev, path_info.st_ino
    if held != expected or path_identity != expected:
        raise Refusal(f"runtime directory changed {phase}: {display}")


def root_fd(authority: Authority) -> int:
    if authority.root is None:
        raise Refusal("runtime authority has no selected instance root")
    return authority.root.fd


def close_root(authority: Authority) -> None:
    if authority.root is not None:
        with suppress(OSError):
            os.close(authority.root.fd)
    authority.root = None


def set_root(
    authority: Authority, name: str, fd: int, expected: Identity, display: Path
) -> None:
    close_root(authority)
    authority.root = RootBinding(name, fd, expected, display)


def rename_root(authority: Authority, name: str, display: Path | None = None) -> None:
    if authority.root is None:
        raise Refusal("cannot rename an unselected runtime root")
    authority.root = replace(
        authority.root,
        name=name,
        display=display if display is not None else authority.root.display,
    )


def read_at(fd: int, name: str, limit: int, *, missing: bool = False) -> bytes | None:
    try:
        with os.fdopen(os.open(name, READ_FLAGS, dir_fd=fd), "rb") as stream:
            return stream.read(limit)
    except FileNotFoundError:
        if missing:
            return None
        raise


def read(
    authority: Authority, name: str, limit: int, *, missing: bool = False
) -> bytes | None:
    try:
        return read_at(root_fd(authority), name, limit, missing=missing)
    except FileNotFoundError as exc:
        display = authority.root_display or authority.parent_display
        raise Refusal(f"missing runtime metadata {display / name}") from exc


def write_exclusive(fd: int, name: str, data: bytes) -> None:
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


def write(authority: Authority, name: str, data: bytes) -> None:
    write_exclusive(root_fd(authority), name, data)


def scan(fd: int) -> list[str]:
    with os.scandir(fd) as entries:
        return [entry.name for entry in entries]


def validate_tree(
    fd: int, display: Path, authority: Authority, *, metadata: set[str], checkpoint
) -> None:
    for name in scan(fd):
        if fd == authority.root_fd and name in metadata:
            continue
        path = display / name
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise Refusal(f"runtime path is a symlink: {path}")
        if not stat.S_ISDIR(info.st_mode):
            continue
        checkpoint("before_child_open", path)
        child_fd = os.open(name, DIR_FLAGS, dir_fd=fd)
        try:
            checkpoint("after_child_open", path)
            check_binding(
                fd,
                name,
                child_fd,
                (info.st_dev, info.st_ino),
                path,
                "while opening",
            )
            validate_tree(
                child_fd,
                path,
                authority,
                metadata=metadata,
                checkpoint=checkpoint,
            )
        finally:
            os.close(child_fd)


def delete_tree(
    fd: int, display: Path, authority: Authority, *, metadata: set[str], checkpoint
) -> None:
    for name in scan(fd):
        if fd == authority.root_fd and name in metadata:
            continue
        authority.check("before deletion")
        path = display / name
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISLNK(info.st_mode):
            raise Refusal(f"runtime path is a symlink: {path}")
        if not stat.S_ISDIR(info.st_mode):
            os.unlink(name, dir_fd=fd)
            continue
        checkpoint("before_delete_child_open", path)
        child_fd = os.open(name, DIR_FLAGS, dir_fd=fd)
        try:
            checkpoint("after_delete_child_open", path)
            child_identity = info.st_dev, info.st_ino
            check_binding(fd, name, child_fd, child_identity, path, "while opening")
            checkpoint("after_delete_child_verify", path)
            check_binding(fd, name, child_fd, child_identity, path, "before deletion")
            delete_tree(
                child_fd,
                path,
                authority,
                metadata=metadata,
                checkpoint=checkpoint,
            )
            check_binding(fd, name, child_fd, child_identity, path, "before removal")
        finally:
            os.close(child_fd)
        os.rmdir(name, dir_fd=fd)


# Platform-specific atomic, no-replace publication for runtime roots.
RENAME_NOREPLACE = 1
RENAME_EXCL = 4


def rename_noreplace(parent_fd: int, source: str, target: str) -> None:
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
            raise RuntimeError("atomic no-replace directory publication is unavailable") from exc
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
        raise RuntimeError("atomic no-replace directory publication is unavailable")
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), target)
