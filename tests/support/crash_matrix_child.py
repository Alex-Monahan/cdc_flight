"""Run the production CLI with the test-only real crash-matrix handler installed."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
TESTS_DIR = PROJECT_DIR / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from cdc_flight import faults
from cdc_flight.pipeline import main
from support.crash_matrix_runtime import install_matrix_crash_handler

install_matrix_crash_handler()
try:
    # Refuse unsafe matrix paths before the production CLI can create its ordinary
    # last_run/offset files through a symlinked CDC_STATE_DIR.
    faults.validate_matrix_state_directory()
except faults.FaultSpecError as exc:
    print(f"invalid crash-matrix state path: {exc}", file=sys.stderr)
    raise SystemExit(2) from exc

if __name__ == "__main__":
    raise SystemExit(main())
