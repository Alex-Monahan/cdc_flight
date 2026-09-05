"""Process-local mutual exclusion for one Flight state directory."""

from __future__ import annotations

import contextlib
import errno
import fcntl
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from .errors import LeaseLost


@dataclass
class StateDirectoryLease:
    """Hold an exclusive lock for every file under one state-directory path.

    The lock file lives beside the directory rather than inside it. Recovery is
    allowed to remove and recreate the state directory, so an in-tree lock file
    could be deleted while its descriptor was still held and a second Flight could
    then acquire a new inode. The sidecar is intentionally retained after release;
    unlinking a held lock file has the same split-inode race.
    """

    state_dir: Path | str
    _handle: TextIO | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.state_dir = Path(self.state_dir).expanduser().resolve(strict=False)

    @property
    def lock_path(self) -> Path:
        return self.state_dir.parent / f".{self.state_dir.name}.cdc_flight.lock"

    @property
    def held(self) -> bool:
        return self._handle is not None

    def acquire(self) -> None:
        """Acquire without waiting; a second configuration fails before state I/O."""
        if self._handle is not None:
            raise LeaseLost(f"state directory {self.state_dir} is already held by this Flight")
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                raise LeaseLost(
                    f"state directory {self.state_dir} is already leased by another Flight"
                ) from None
            raise
        self._handle = handle

    def release(self) -> None:
        """Release an acquired descriptor; safe from every teardown path."""
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        with contextlib.suppress(OSError):
            fcntl.flock(handle, fcntl.LOCK_UN)
        with contextlib.suppress(OSError):
            handle.close()

    def __enter__(self) -> StateDirectoryLease:
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()


__all__ = ["StateDirectoryLease"]
