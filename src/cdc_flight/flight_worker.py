"""Shared worker core with finite and long-running adapters.

The adapters differ only in how they decide to request a stop.  Callback admission,
destination ownership, shutdown sealing, and the bounded engine close remain one
implementation so service mode cannot grow a second commit/ack path.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FlightWorker:
    """Own one Debezium engine/applier pair until its bounded shutdown completes."""

    engine: object
    handler: object
    run_config: object
    health: object | None = None
    supervisor_options: dict = field(default_factory=dict)
    runner: object | None = None

    def _run(self, *, service_context=None) -> dict:
        # Import lazily: importing the engine runner must not boot the stock
        # Debezium/JVM path for callers that only inspect the service protocol.
        runner = self.runner
        if runner is None:
            from .supervisor import run_engine_bounded

            runner = run_engine_bounded

        options = dict(self.supervisor_options)
        options["service_context"] = service_context
        return runner(
            self.engine,
            self.handler,
            self.run_config,
            self.health,
            **options,
        )

    def run_batch(self) -> dict:
        """Finite adapter: preserves the existing watermark/max/idle policy."""
        return self._run()

    def run_service(self, service_context) -> dict:
        """Unbounded adapter: stops only on drain, failure, or lease loss."""
        if service_context is None:
            raise ValueError("service mode requires a ServiceContext")
        return self._run(service_context=service_context)


__all__ = ["FlightWorker"]
