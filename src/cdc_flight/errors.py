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


class OffsetFlushFailed(RuntimeError):
    """`markBatchFinished()` returned normally but did not flush the offset.

    Debezium swallows every non-timeout flush failure and discards the boolean
    (`AsyncEmbeddedEngine.java:894-932`, `:1369-1382`), so "the offset is now
    durable" is not something a normal return can be taken to mean. See
    `cdc_flight.consumer` and ADR 0001 §4.2.
    """


class SourceNotStreaming(RuntimeError):
    """The connector stopped streaming without the supervisor asking it to.

    Raised when a run would otherwise report a quiet stream as `idle` while the
    replication slot says the connector is not actually connected - Debezium's
    retriable-restart backoff looks exactly like an idle stream from the Python
    side (ADR 0001 §9.1; review finding Opus B5).
    """
