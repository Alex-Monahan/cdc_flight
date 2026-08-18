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
import time
from functools import cached_property
from pathlib import Path

from pydbzengine import BasePythonChangeHandler, DebeziumJsonEngine

from .consumer import OffsetFlushVerifier, verifying_consumer_class
from .errors import EngineFailure

__all__ = ["EngineFailure", "SupervisedDebeziumEngine"]

log = logging.getLogger("cdc_flight.engine")

# Debezium reports a graceful stop as a *failure* in some shutdown races (the
# polling thread is interrupted while we are closing the engine). Those are not
# real failures - but ONLY when the stop was ours *and* deliberate. See
# `close(intentional=...)` below for why the previous, always-armed version of
# this filter was unsafe (review finding Opus M1).
_SHUTDOWN_NOISE = (
    "interrupted",
    "interruptedexception",
    "rejectedexecutionexception",
    "engine has been already shut down",
    "connector has been stopped",
)

#: How long after an intentional `close()` a shutdown-noise failure is still
#: attributable to that close. Debezium's teardown is bounded by
#: `internal.task.management.timeout.ms`, so anything later is a real failure.
SHUTDOWN_NOISE_WINDOW_SEC = 60.0


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

    def __init__(
        self,
        properties,
        handler: BasePythonChangeHandler,
        *,
        offset_file: Path | str | None = None,
        always_commit_offsets: bool = False,
    ):
        super().__init__(properties, handler)
        self._lock = threading.Lock()
        self._completed = False
        self._completed_success: bool | None = None
        self._failure: str | None = None
        self._suppressed: str | None = None
        # The shutdown-noise filter is DISARMED until an intentional close asks
        # for it. Previously `_stop_requested` was set by every `close()`, and
        # `close()` runs in a `finally` on every run - so the filter was always
        # armed and degenerated into "drop any failure whose text contains
        # 'interrupted'". That is exactly how a handler error propagates
        # (`pydbzengine/_jvm.py` interrupts the engine thread), so real failures
        # could be discarded (Opus M1).
        self._noise_filter_armed_at: float | None = None
        self._offset_file = Path(offset_file) if offset_file else None
        self._always_commit_offsets = always_commit_offsets
        self._verifier: OffsetFlushVerifier | None = None
        self._effective_configuration: dict[str, object] = {}

    # -- completion bookkeeping --------------------------------------------- #
    def _on_completion(self, success: bool, message, error) -> None:
        detail = _describe(message, error)
        with self._lock:
            self._completed = True
            self._completed_success = bool(success)
            if success:
                log.info("debezium engine completed: %s", detail)
                return
            if self._noise_armed_locked() and self._is_shutdown_noise(detail):
                # Never silently: an operator with a wedged pipeline needs to see
                # what we decided to ignore, so it also lands in the run summary.
                self._suppressed = detail
                log.info("debezium engine stopped during intentional shutdown: %s", detail)
                return
            self._failure = detail
        log.error("debezium engine failed: %s", detail)

    def _noise_armed_locked(self) -> bool:
        armed_at = self._noise_filter_armed_at
        if armed_at is None:
            return False
        return (time.monotonic() - armed_at) <= SHUTDOWN_NOISE_WINDOW_SEC

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
    def suppressed_message(self) -> str | None:
        """A failure that was attributed to an intentional shutdown."""
        with self._lock:
            return self._suppressed

    @property
    def completed(self) -> bool:
        with self._lock:
            return self._completed

    @property
    def completed_success(self) -> bool | None:
        """Debezium's own verdict, or None if the engine never completed."""
        with self._lock:
            return self._completed_success

    @property
    def offset_flushes_verified(self) -> int:
        return self._verifier.flushes_verified if self._verifier else 0

    @property
    def effective_configuration(self) -> dict[str, object]:
        """The stock Debezium/pgjdbc values observed while building the engine."""
        return dict(self._effective_configuration)

    # -- engine construction ------------------------------------------------ #
    @cached_property
    def consumer(self):
        """Our own `ChangeConsumer`, so a silently-failed offset flush is fatal.

        pydbzengine's consumer calls `markProcessed`/`markBatchFinished` for us
        and drops the outcome on the floor; ADR 0001 §3.7 replaces it. Doing it
        here (rather than inside the applier task) means the check is already in
        place when the applier starts relying on it.
        """
        consumer = verifying_consumer_class()()
        if self._offset_file is not None:
            self._verifier = OffsetFlushVerifier(
                self._offset_file, always_commit=self._always_commit_offsets
            )
            consumer.verifier = self._verifier
            # Under Invariant O the handler owns the acknowledgement, so it is the
            # handler - not the consumer - that must check whether the flush
            # actually happened.
            if hasattr(self._handler, "handle_batch"):
                self._handler.verifier = self._verifier
        return consumer

    @cached_property
    def engine(self):
        from pydbzengine._jvm import DebeziumEngine, EngineFormat, Properties

        java_props = Properties()
        if isinstance(self.properties, dict):
            for key, value in self.properties.items():
                java_props.setProperty(str(key), str(value))
        else:
            java_props = self.properties

        self._effective_configuration = self._probe_effective_configuration(java_props)

        callback = _completion_callback_class()(self._on_completion)
        # Keep a reference: JPype proxies are garbage-collected like any Python
        # object, and the JVM only holds a weak handle to them.
        self._completion_callback = callback

        built = (
            DebeziumEngine.create(EngineFormat.JSON)
            .using(java_props)
            .notifying(self.consumer)
            .using(callback)
            .build()
        )
        self._effective_configuration["engine_built"] = True
        return built

    @staticmethod
    def _probe_effective_configuration(java_props) -> dict[str, object]:
        """Validate the exact stock runtime properties, including driver pass-through.

        Checking the Python dict alone would repeat the dead-configuration failure this
        project already had. Debezium's own PostgresConnectorConfig consumes the
        heartbeat values, and pgjdbc's PGProperty consumes the driver subset; both
        probes run before the engine is built and therefore fail startup if a property
        is rejected or renamed.
        """
        import jpype

        configuration_cls = jpype.JClass("io.debezium.config.Configuration")
        postgres_config_cls = jpype.JClass(
            "io.debezium.connector.postgresql.PostgresConnectorConfig"
        )
        pg_property = jpype.JClass("org.postgresql.PGProperty")
        config = configuration_cls.from_(java_props)
        connector_config = postgres_config_cls(config)
        interval_ms = int(connector_config.getHeartbeatInterval().toMillis())
        action_raw = config.getString("heartbeat.action.query")
        action_query = str(action_raw) if action_raw is not None else ""
        jdbc_props = jpype.JClass("java.util.Properties")()
        for name in ("socketTimeout", "connectTimeout", "tcpKeepAlive"):
            value = config.getString(f"driver.{name}")
            if value is not None:
                jdbc_props.setProperty(name, str(value))
        effective = {
            "validation": "io.debezium.connector.postgresql.PostgresConnectorConfig",
            "heartbeat.interval.ms": interval_ms,
            "heartbeat.action.query": action_query,
            "driver.socketTimeout": str(
                pg_property.forName("socketTimeout").get(jdbc_props)
            ),
            "driver.connectTimeout": str(
                pg_property.forName("connectTimeout").get(jdbc_props)
            ),
            "driver.tcpKeepAlive": str(
                pg_property.forName("tcpKeepAlive").get(jdbc_props)
            ),
        }
        if interval_ms <= 0 or not action_query:
            raise EngineFailure(
                "stock Debezium accepted no effective idle heartbeat interval/action query"
            )
        return effective

    def close(self, *, intentional: bool = True) -> None:
        """Stop the engine.

        `intentional=False` says "we are closing *because* something already went
        wrong". The shutdown-noise filter stays disarmed in that case, so the
        interrupt Debezium reports cannot swallow the real cause.
        """
        if intentional:
            with self._lock:
                self._noise_filter_armed_at = time.monotonic()
        # `DebeziumJsonEngine.close()` does `if self.engine:`, which *constructs*
        # an engine through the cached_property when one was never started (Opus
        # m4). Guard it: closing something that does not exist should be free.
        if "engine" not in self.__dict__:
            log.debug("close() called on an engine that was never built; nothing to do")
            return
        super().close()
