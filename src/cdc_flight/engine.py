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

# This is a stock Debezium ``ChangeEventQueue`` setting.  Keep the spelling in one
# place because the proof below compares the connector task's effective
# configuration with the queue object that actually admits records.
QUEUE_SIZE_IN_BYTES_PROPERTY = "max.queue.size.in.bytes"

_JAVA_FIELD_MISSING = object()


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
        self._live_queue_metrics: dict[str, object] | None = None

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

    @property
    def live_queue_metrics(self) -> dict[str, object] | None:
        """The last verified stock ``ChangeEventQueue`` measurement, if ready."""
        if self._live_queue_metrics is None:
            return None
        return dict(self._live_queue_metrics)

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
        byte_bound_raw = config.getString(QUEUE_SIZE_IN_BYTES_PROPERTY)
        if byte_bound_raw is None or not str(byte_bound_raw).strip():
            raise EngineFailure(
                "stock Debezium configuration did not expose the required "
                f"{QUEUE_SIZE_IN_BYTES_PROPERTY} property"
            )
        try:
            byte_bound = int(str(byte_bound_raw).strip())
        except (TypeError, ValueError) as exc:
            raise EngineFailure(
                f"stock Debezium configuration exposed a non-integer "
                f"{QUEUE_SIZE_IN_BYTES_PROPERTY}={byte_bound_raw!r}"
            ) from exc
        if byte_bound <= 0:
            raise EngineFailure(
                f"stock Debezium configuration disabled the required positive "
                f"{QUEUE_SIZE_IN_BYTES_PROPERTY}: {byte_bound}"
            )
        effective[QUEUE_SIZE_IN_BYTES_PROPERTY] = byte_bound
        if interval_ms <= 0 or not action_query:
            raise EngineFailure(
                "stock Debezium accepted no effective idle heartbeat interval/action query"
            )
        return effective

    @staticmethod
    def _java_field(value, name: str):
        """Read a private Java field, including fields declared by a superclass.

        Debezium does not publish the connector task or its queue as a public Python
        API.  This deliberately narrow reflection helper is the runtime proof: it
        reads the stock engine's own task configuration and ``ChangeEventQueue``
        rather than trusting the Python property dictionary.  ``_JAVA_FIELD_MISSING``
        distinguishes a field that is not part of the stock object graph from a field
        that exists but has not been initialized yet.
        """
        if value is None:
            return None
        java_class = value.getClass()
        while java_class is not None:
            try:
                field = java_class.getDeclaredField(name)
            except Exception:
                java_class = java_class.getSuperclass()
                continue
            field.setAccessible(True)
            return field.get(value)
        return _JAVA_FIELD_MISSING

    def probe_live_queue(self) -> dict[str, object] | None:
        """Verify the effective byte bound on every live stock source-task queue.

        ``None`` means the engine is still starting: the task list or one of its
        fields exists but has not been populated yet.  A missing field or an
        inconsistent initialized value is a hard engine failure, because accepting
        that shape would turn a configuration proof into a best-effort log line.
        """
        if "engine" not in self.__dict__:
            return None
        java_engine = self.__dict__["engine"]
        tasks = self._java_field(java_engine, "tasks")
        if tasks is _JAVA_FIELD_MISSING:
            raise EngineFailure(
                "stock Debezium engine has no inspectable source-task list; "
                "cannot prove the queue byte bound"
            )
        if tasks is None:
            return None
        task_count = int(tasks.size())
        if task_count == 0:
            return None

        queues: list[dict[str, int]] = []
        for task_index in range(task_count):
            task = tasks.get(task_index)
            if task is None:
                return None
            connect_task = self._java_field(task, "connectTask")
            if connect_task is _JAVA_FIELD_MISSING:
                raise EngineFailure(
                    "stock Debezium engine task has no inspectable connectTask; "
                    "cannot prove the queue byte bound"
                )
            if connect_task is None:
                return None
            task_config = self._java_field(connect_task, "config")
            if task_config is _JAVA_FIELD_MISSING:
                raise EngineFailure(
                    "stock Debezium connector task has no inspectable effective "
                    "configuration; cannot prove the queue byte bound"
                )
            if task_config is None:
                return None
            configured_raw = task_config.getString(QUEUE_SIZE_IN_BYTES_PROPERTY)
            if configured_raw is None or not str(configured_raw).strip():
                raise EngineFailure(
                    "stock Debezium connector task omitted the effective "
                    f"{QUEUE_SIZE_IN_BYTES_PROPERTY} property"
                )
            try:
                configured_bytes = int(str(configured_raw).strip())
            except (TypeError, ValueError) as exc:
                raise EngineFailure(
                    "stock Debezium connector task has a non-integer effective "
                    f"{QUEUE_SIZE_IN_BYTES_PROPERTY}={configured_raw!r}"
                ) from exc
            if configured_bytes <= 0:
                raise EngineFailure(
                    "stock Debezium connector task disabled the required positive "
                    f"{QUEUE_SIZE_IN_BYTES_PROPERTY}: {configured_bytes}"
                )

            queue = self._java_field(connect_task, "queue")
            if queue is _JAVA_FIELD_MISSING:
                raise EngineFailure(
                    "stock Debezium connector task has no inspectable ChangeEventQueue"
                )
            if queue is None:
                return None
            observed_bytes = int(queue.maxQueueSizeInBytes())
            current_bytes = int(queue.currentQueueSizeInBytes())
            total_capacity = int(queue.totalCapacity())
            remaining_capacity = int(queue.remainingCapacity())
            current_count = total_capacity - remaining_capacity
            if observed_bytes != configured_bytes:
                raise EngineFailure(
                    "stock Debezium queue byte bound does not match its effective "
                    f"task configuration: config={configured_bytes}, "
                    f"queue={observed_bytes}"
                )
            if current_bytes < 0 or current_bytes > observed_bytes:
                raise EngineFailure(
                    "stock Debezium queue reported an invalid byte occupancy: "
                    f"current={current_bytes}, capacity={observed_bytes}"
                )
            if current_count < 0 or current_count > total_capacity:
                raise EngineFailure(
                    "stock Debezium queue reported an invalid record occupancy: "
                    f"current={current_count}, capacity={total_capacity}"
                )
            queues.append(
                {
                    "task_index": task_index,
                    "effective_task_config_max_queue_size_in_bytes": configured_bytes,
                    "queue_max_queue_size_in_bytes": observed_bytes,
                    "queue_current_size_in_bytes": current_bytes,
                    "queue_current_size": current_count,
                    "queue_total_capacity": total_capacity,
                    "queue_remaining_capacity": remaining_capacity,
                }
            )

        if not queues:
            return None
        configured_values = {
            item["effective_task_config_max_queue_size_in_bytes"] for item in queues
        }
        if len(configured_values) != 1:
            raise EngineFailure(
                "stock Debezium source tasks disagree about the effective queue "
                f"byte bound: {sorted(configured_values)}"
            )
        metrics: dict[str, object] = {
            "task_count": task_count,
            "queues": queues,
            "effective_task_config_max_queue_size_in_bytes": queues[0][
                "effective_task_config_max_queue_size_in_bytes"
            ],
            "queue_max_queue_size_in_bytes": sum(
                item["queue_max_queue_size_in_bytes"] for item in queues
            ),
            "queue_current_size_in_bytes": sum(
                item["queue_current_size_in_bytes"] for item in queues
            ),
            "queue_current_size": sum(item["queue_current_size"] for item in queues),
            "queue_total_capacity": sum(item["queue_total_capacity"] for item in queues),
            "queue_remaining_capacity": sum(
                item["queue_remaining_capacity"] for item in queues
            ),
        }
        self._live_queue_metrics = metrics
        self._effective_configuration["live_queue"] = metrics
        return dict(metrics)

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
