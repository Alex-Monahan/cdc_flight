"""A Debezium embedded engine that cannot fail silently.

`pydbzengine.DebeziumJsonEngine` builds the engine with

    DebeziumEngine.create(Json).using(props).notifying(consumer).build()

and never registers a `CompletionCallback`. Debezium 3.6's `AsyncEmbeddedEngine`
reports startup and streaming failures by calling
`completionCallback.handle(false, message, error)` and then *returning normally*
from `run()` (see
`repos/debezium/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java`
`callCompletionHandler`, and `DefaultCompletionCallback` which merely logs).

Nothing is therefore thrown on the caller's thread, so the supervising Python
code sees a clean return, prints `{"records": 0}` and exits 0 - which is exactly
what probes p04/p10/p11 recorded for a dropped slot, an externally advanced slot
and a corrupted offset. This module closes that hole by supplying our own
`CompletionCallback` and exposing the recorded failure to the supervisor.
"""

from __future__ import annotations

import logging
import threading
from functools import cached_property

from pydbzengine import BasePythonChangeHandler, DebeziumJsonEngine

from .errors import EngineFailure

__all__ = ["EngineFailure", "SupervisedDebeziumEngine"]

log = logging.getLogger("cdc_flight.engine")

# Debezium reports a graceful stop as a *failure* in some shutdown races (the
# polling thread is interrupted while we are closing the engine). Those are not
# real failures, so they are filtered out when we asked for the stop ourselves.
_SHUTDOWN_NOISE = (
    "interrupted",
    "interruptedexception",
    "rejectedexecutionexception",
    "engine has been already shut down",
    "connector has been stopped",
)


def _describe(message: str | None, error) -> str:
    """Render the Debezium message plus the root of its cause chain."""
    parts: list[str] = []
    if message:
        parts.append(str(message))
    seen = 0
    cause = error
    while cause is not None and seen < 5:
        text = str(cause)
        if text and text not in parts:
            parts.append(text)
        try:
            nxt = cause.getCause()
        except Exception:  # pragma: no cover - defensive around the JVM bridge
            break
        if nxt is None or nxt == cause:
            break
        cause = nxt
        seen += 1
    return " | ".join(parts) if parts else "engine failed without a message"


_callback_class = None


def _completion_callback_class():
    """Build the JPype proxy class lazily - it needs a running JVM."""
    global _callback_class
    if _callback_class is None:
        import jpype

        @jpype.JImplements("io.debezium.engine.DebeziumEngine$CompletionCallback")
        class _RecordingCompletionCallback:
            def __init__(self, sink):
                self._sink = sink

            @jpype.JOverride
            def handle(self, success, message, error):
                self._sink(bool(success), message, error)

        _callback_class = _RecordingCompletionCallback
    return _callback_class


class SupervisedDebeziumEngine(DebeziumJsonEngine):
    """`DebeziumJsonEngine` whose completion status is visible to Python."""

    def __init__(self, properties, handler: BasePythonChangeHandler):
        super().__init__(properties, handler)
        self._lock = threading.Lock()
        self._completed = False
        self._failure: str | None = None
        self._stop_requested = False

    # -- completion bookkeeping --------------------------------------------- #
    def _on_completion(self, success: bool, message, error) -> None:
        detail = _describe(message, error)
        with self._lock:
            self._completed = True
            if success:
                log.info("debezium engine completed: %s", detail)
                return
            if self._stop_requested and self._is_shutdown_noise(detail):
                log.info("debezium engine stopped during shutdown: %s", detail)
                return
            self._failure = detail
        log.error("debezium engine failed: %s", detail)

    @staticmethod
    def _is_shutdown_noise(detail: str) -> bool:
        lowered = detail.lower()
        return any(token in lowered for token in _SHUTDOWN_NOISE)

    @property
    def failure(self) -> str | None:
        """The Debezium failure message, or None if the engine is healthy."""
        with self._lock:
            return self._failure

    @property
    def completed(self) -> bool:
        with self._lock:
            return self._completed

    # -- engine construction ------------------------------------------------ #
    @cached_property
    def engine(self):
        from pydbzengine._jvm import DebeziumEngine, EngineFormat, Properties

        java_props = Properties()
        if isinstance(self.properties, dict):
            for key, value in self.properties.items():
                java_props.setProperty(str(key), str(value))
        else:
            java_props = self.properties

        callback = _completion_callback_class()(self._on_completion)
        # Keep a reference: JPype proxies are garbage-collected like any Python
        # object, and the JVM only holds a weak handle to them.
        self._completion_callback = callback

        return (
            DebeziumEngine.create(EngineFormat.JSON)
            .using(java_props)
            .notifying(self.consumer)
            .using(callback)
            .build()
        )

    def close(self) -> None:
        with self._lock:
            self._stop_requested = True
        super().close()
