"""Exception types shared across cdc_flight.

Kept in its own (import-cheap) module: `engine.py` imports pydbzengine, which
boots a JVM as a side effect of being imported, and the CLI's error handling must
not pay that cost just to name an exception.
"""

from __future__ import annotations


class EngineFailure(RuntimeError):
    """The Debezium engine terminated abnormally.

    Carries a partial run summary so the CLI can still write a machine-readable
    `last_run.json` for the failed run (rubric 6.1/6.2 depend on it).
    """

    def __init__(self, message: str, summary: dict | None = None):
        super().__init__(message)
        self.summary: dict = summary or {}
