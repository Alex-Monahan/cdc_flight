"""The production entrypoint selects the callback-safe Arrow allocator before imports."""

from __future__ import annotations

import os
import subprocess
import sys


def test_production_entrypoint_defaults_to_the_system_arrow_pool():
    env = dict(os.environ)
    env.pop("ARROW_DEFAULT_MEMORY_POOL", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import cdc_flight.pipeline; import pyarrow as pa; "
            "print(pa.default_memory_pool().backend_name)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert result.stdout.strip() == "system"
