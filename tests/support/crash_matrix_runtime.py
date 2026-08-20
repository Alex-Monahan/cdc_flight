"""Test-only implementation of the real crash-matrix hard exit.

This module is deliberately outside the package's wheel.  The production cut
points call the registered callback, but production installation cannot import
this implementation and therefore cannot hard-exit at a matrix lifecycle edge.
"""

from __future__ import annotations

import os
import sys
import time
from typing import NoReturn

from cdc_flight import faults


def _hard_exit(point: str, nth: int) -> NoReturn:
    """Persist the selected edge and perform the real child-process death."""
    faults.record_fired(point, nth, faults.DEFAULT_EXIT_CODE)
    sys.stdout.flush()
    sys.stderr.flush()
    hold_path = os.environ.get("CDC_CRASH_MATRIX_HOLD")
    if hold_path:
        # A real-process SIGKILL harness uses the production cut as a durable
        # rendezvous.  The handler remains test-only; production never installs
        # it and therefore never waits on this path.
        with open(hold_path, "w") as handle:
            handle.write(f"{os.getpid()}\n")
            handle.flush()
            os.fsync(handle.fileno())
        while True:
            time.sleep(0.1)
    os._exit(faults.DEFAULT_EXIT_CODE)


def install_matrix_crash_handler() -> None:
    """Register the test-tree hard-exit implementation in this process."""
    faults._register_matrix_crash_handler(_hard_exit)
