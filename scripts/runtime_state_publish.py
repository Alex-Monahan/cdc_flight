"""Platform-specific atomic, no-replace publication for runtime roots."""

from __future__ import annotations

import ctypes
import os
import sys

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
