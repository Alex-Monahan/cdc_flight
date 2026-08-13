#!/usr/bin/env python3
"""Serialize the resource-heavy slow lane across repository clones."""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
from pathlib import Path

DEFAULT_LOCK_FILE = "/tmp/cdc_flight_slow_lane.lock"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="run one command while holding the host-wide slow-lane lock"
    )
    parser.add_argument(
        "--lock-file",
        default=os.environ.get("CDC_SLOW_LANE_LOCK", DEFAULT_LOCK_FILE),
        help="shared lock path (default: %(default)s)",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        parser.error("a command is required after --")

    lock_path = Path(args.lock_file)
    with lock_path.open("a+") as lock:
        print(
            f"slow lane waiting for {lock_path} (pid={os.getpid()}, cwd={Path.cwd()})",
            flush=True,
        )
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        print(f"slow lane acquired {lock_path} (pid={os.getpid()})", flush=True)
        try:
            return subprocess.call(command)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


if __name__ == "__main__":
    sys.exit(main())
