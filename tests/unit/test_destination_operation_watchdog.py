from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path


def _run_child(body: str) -> subprocess.CompletedProcess[str]:
    project = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(project / "src"), env.get("PYTHONPATH", "")) if part
    )
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )


def test_slow_but_progressing_destination_work_resets_the_deadline():
    result = _run_child(
        """
        import time
        from cdc_flight.self_heal import (
            DestinationOperationProgress,
            destination_operation_watchdog,
        )

        progress = DestinationOperationProgress()
        with destination_operation_watchdog(0.10, progress=progress):
            for _ in range(8):
                with progress.operation("group_write", progressed=True):
                    time.sleep(0.045)
        print("completed")
        """
    )
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    assert result.stdout.strip() == "completed"


def test_a_hung_destination_operation_still_exits_75():
    result = _run_child(
        """
        import time
        from cdc_flight.self_heal import (
            DestinationOperationProgress,
            destination_operation_watchdog,
        )

        progress = DestinationOperationProgress()
        with destination_operation_watchdog(0.10, progress=progress):
            with progress.operation("group_write", progressed=True):
                time.sleep(10)
        """
    )
    assert result.returncode == 75, (result.returncode, result.stdout, result.stderr)
