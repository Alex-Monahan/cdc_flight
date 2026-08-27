"""Runtime proof for the stock Debezium queue byte bound.

The Python properties dictionary is not enough: Debezium has ignored accepted
properties before, and a count-only queue is still the unsafe configuration.  These
tests start the real stock engine, walk its private task object graph, and compare
the connector task's effective configuration with the live ``ChangeEventQueue``.
The smaller sentinel is intentional: it proves the observed capacity changes with
the property instead of merely matching a hard-coded production number.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import suppress

import pytest
from support.fixtures import TEST_SLOT_PREFIX, _drop_slot

from cdc_flight.config import ReplicationConfig
from cdc_flight.debezium_props import (
    MAX_QUEUE_SIZE_IN_BYTES,
    build_properties,
)
from cdc_flight.engine import SupervisedDebeziumEngine


class _AcknowledgingHandler:
    """Keep any incidental heartbeat callback fully acknowledged."""

    def handle_batch(self, records, committer):
        for record in records:
            committer.markProcessed(record)
        if records:
            committer.markBatchFinished()


def _wait_for_live_queue(engine, runner, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        metrics = engine.probe_live_queue()
        if metrics is not None:
            return metrics
        if engine.failure is not None:
            pytest.fail(f"stock Debezium failed before queue proof: {engine.failure}")
        if not runner.is_alive():
            pytest.fail("stock Debezium stopped before its queue could be inspected")
        time.sleep(0.1)
    pytest.fail("stock Debezium did not initialize an inspectable queue within 30s")


@pytest.mark.slow
@pytest.mark.parametrize(
    ("label", "expected_bytes", "overrides"),
    [
        ("production", int(MAX_QUEUE_SIZE_IN_BYTES), {}),
        ("sentinel", 131072, {"max.queue.size.in.bytes": "131072"}),
    ],
)
def test_stock_queue_byte_bound_is_effective_in_the_live_task(
    tmp_path, postgres_cluster, label, expected_bytes, overrides
):
    slot = f"{TEST_SLOT_PREFIX}p5_queue_{label}_{os.getpid()}"[:63]
    _drop_slot(postgres_cluster, slot)
    state_dir = tmp_path / f"state_{label}"
    replication = ReplicationConfig(slot_name=slot, state_dir=state_dir)
    properties = build_properties(
        postgres_cluster,
        replication,
        snapshot_mode="no_data",
        overrides=overrides,
    )
    engine = SupervisedDebeziumEngine(properties, _AcknowledgingHandler())
    runner = threading.Thread(target=engine.run, name=f"p5-queue-{label}", daemon=True)
    close_thread = None
    try:
        runner.start()
        metrics = _wait_for_live_queue(engine, runner)

        # This first assertion is the effective task configuration, not the Python
        # input dict.  The next assertions are the queue object that actually gates
        # source admission.
        effective = engine.effective_configuration
        assert effective["max.queue.size.in.bytes"] == expected_bytes
        assert metrics["task_count"] == 1
        assert metrics["effective_task_config_max_queue_size_in_bytes"] == expected_bytes
        assert metrics["queue_max_queue_size_in_bytes"] == expected_bytes
        assert metrics["queue_current_size_in_bytes"] <= expected_bytes
        assert metrics["queue_current_size"] <= metrics["queue_total_capacity"]
        assert effective["live_queue"] == metrics
        assert metrics["queues"][0]["effective_task_config_max_queue_size_in_bytes"] == expected_bytes
        assert metrics["queues"][0]["queue_max_queue_size_in_bytes"] == expected_bytes
    finally:
        if runner.is_alive():
            close_thread = threading.Thread(
                target=lambda: engine.close(intentional=True),
                name=f"p5-queue-close-{label}",
                daemon=True,
            )
            close_thread.start()
            close_thread.join(timeout=30)
        runner.join(timeout=30)
        if close_thread is not None and close_thread.is_alive():
            pytest.fail(f"stock Debezium close hung for {label} queue proof")
        if runner.is_alive():
            pytest.fail(f"stock Debezium runner hung for {label} queue proof")
        with suppress(Exception):
            _drop_slot(postgres_cluster, slot)
