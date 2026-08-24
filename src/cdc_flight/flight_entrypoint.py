"""Packaged Flight entrypoint for the continuous single-process service."""

from __future__ import annotations

import math
import os

_MAX_RUNTIME_NAMES = (
    "max_runtime_sec",
    "MAX_RUNTIME_SEC",
    "FLIGHT_MAX_RUNTIME_SEC",
)


def _require_unbounded_flight() -> float:
    """Require the scheduler's continuous-service deployment contract."""
    for name in _MAX_RUNTIME_NAMES:
        raw = os.environ.get(name)
        if raw is not None and raw != "":
            try:
                value = float(raw)
            except ValueError as exc:
                raise RuntimeError(
                    f"Flight deployment contract {name}=0 is required; got {raw!r}"
                ) from exc
            if not math.isfinite(value) or value != 0:
                raise RuntimeError(
                    "continuous cdc_flight requires max_runtime_sec=0; "
                    f"{name}={raw!r} would let the platform terminate a healthy holder"
                )
            return value
    raise RuntimeError(
        "continuous cdc_flight requires the Flight deployment contract "
        "max_runtime_sec=0; no max_runtime_sec setting was provided"
    )


def main() -> int:
    """Validate the Flight contract, then run one scheduled service instance."""
    _require_unbounded_flight()
    from .service import main as service_main

    return int(service_main())


if __name__ == "__main__":
    raise SystemExit(main())
