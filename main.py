"""Source-tree wrapper for the packaged continuous-service Flight entrypoint."""

from __future__ import annotations

from cdc_flight.flight_entrypoint import main

if __name__ == "__main__":
    raise SystemExit(main())
