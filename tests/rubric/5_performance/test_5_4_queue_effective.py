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

import psycopg
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


class _GatedAcknowledgingHandler:
    """Hold the first callback so the stock source queue has to fill behind it."""

    def __init__(self):
        self.first_callback = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()
        self.records = 0
        self.batches = 0
        self.error = None

    def handle_batch(self, records, committer):
        with self._lock:
            self.records += len(records)
            self.batches += 1
            first = self.batches == 1
        if first:
            self.first_callback.set()
            if not self.release.wait(timeout=90):
                raise AssertionError("queue gate was not released within 90s")
        for record in records:
            committer.markProcessed(record)
        if records:
            committer.markBatchFinished()


def _wait_for_live_queue(engine, runner, timeout: float = 90.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        metrics = engine.probe_live_queue()
        # Debezium publishes its private task/queue object while the task can
        # still be in STARTING_TASKS.  Stock close() deliberately refuses that
        # state because closing there can leak resources.  Require the same
        # live task to reach its normal polling state before the proof returns;
        # otherwise a successful reflection can leave a real runner behind.
        state = None
        try:
            # Never read the cached property here: the runner thread owns engine
            # construction, and touching ``engine.engine`` from this polling
            # thread could construct a second stock JVM engine during the race.
            java_engine = engine.__dict__.get("engine")
            if java_engine is not None:
                state_ref = engine._java_field(java_engine, "state")
                state = str(state_ref.get())
        except Exception:
            pass
        if metrics is not None and state == "POLLING_TASKS":
            return metrics
        if engine.failure is not None:
            pytest.fail(f"stock Debezium failed before queue proof: {engine.failure}")
        if not runner.is_alive():
            pytest.fail("stock Debezium stopped before its queue could be inspected")
        time.sleep(0.1)
    pytest.fail("stock Debezium did not initialize an inspectable queue within 90s")


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
    # Debezium registers connector metrics under the topic prefix.  Keep the two
    # proof cases independent inside a reused worker JVM: stock teardown can
    # unregister an earlier MBean asynchronously, and a stale registration must
    # not turn the second runtime proof into a false startup timeout.
    identity = f"p5_queue_{label}_{os.getpid()}"
    properties["name"] = f"cdc-flight-{identity}"
    properties["topic.prefix"] = f"cdcflight_{identity}"
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
        assert metrics["queue_current_size_in_bytes"] >= 0
        assert metrics["queue_over_capacity_bytes"] >= 0
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


@pytest.mark.slow
def test_toast_burst_spills_and_finishes_as_one_exact_commit_group(sandbox):
    """Exercise the production byte bound alongside the transactional spill path."""
    sandbox.reseed()
    capture = {"CDC_TABLES": "documents"}
    baseline = sandbox.run(
        reset_state=True,
        max_seconds=180,
        timeout=300,
        extra_env=capture,
    )
    assert baseline["returncode"] == 0, baseline

    tag = f"p5doc-{os.getpid()}"
    sandbox.sql(
        "INSERT INTO app.documents (title, body, body_bytes, revision) "
        "SELECT %s || i, "
        "(SELECT string_agg(md5(i::text || ':' || g::text), '' ORDER BY g) "
        "FROM generate_series(1, 2048) AS g), "
        "65536, 1 FROM generate_series(1, 2048) AS rows(i)".replace(
            "%s", f"'{tag}-'"
        ),
        one_transaction=True,
    )
    source_rows = sandbox.pg_query(
        "SELECT count(*) FROM app.documents WHERE title LIKE %s", (f"{tag}-%",)
    )[0][0]
    assert source_rows == 2048

    run = sandbox.run(
        max_seconds=600,
        idle_seconds=5,
        timeout=720,
        extra_env=capture,
    )
    assert run["returncode"] == 0, run
    assert run["ok"] is True, run
    assert run["data_commit_groups"] == 1, run
    assert run["spilled_events"] > 0, run

    destination = sandbox.table("cdcflight_app_documents")
    landed, identities = sandbox.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {destination} "
        "WHERE title LIKE ?",
        [f"{tag}-%"],
    )[0]
    assert (landed, identities) == (source_rows, source_rows), run

    live_queue = run["engine_effective_configuration"]["live_queue"]
    assert live_queue["effective_task_config_max_queue_size_in_bytes"] == int(
        MAX_QUEUE_SIZE_IN_BYTES
    )
    assert live_queue["queue_max_queue_size_in_bytes"] == int(MAX_QUEUE_SIZE_IN_BYTES)
    # Stock ChangeEventQueue tests the watermark before enqueueing a record, so
    # one admitted record may take the instantaneous counter slightly above it.
    # The proof is the effective queue capacity plus a reported, finite
    # record-level overshoot—not clipping the observed metric.
    assert live_queue["queue_peak_over_capacity_bytes"] < 131072
    assert live_queue["queue_peak_size"] <= live_queue["queue_total_capacity"]

    durable = int(run["durable_lsn"])
    confirmed = int(
        sandbox.pg_query(
            "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (sandbox.slot,),
        )[0][0]
    )
    assert confirmed <= durable, {
        "run": run,
        "confirmed_flush_lsn": confirmed,
        "durable_lsn": durable,
    }


@pytest.mark.slow
def test_stock_queue_applies_byte_backpressure_before_acknowledgement(
    tmp_path, postgres_cluster
):
    """A blocked callback fills, but cannot overrun, the stock byte-bounded queue."""
    slot = f"{TEST_SLOT_PREFIX}p5_gate_{os.getpid()}"[:63]
    _drop_slot(postgres_cluster, slot)
    replication = ReplicationConfig(slot_name=slot, state_dir=tmp_path / "queue_gate")
    properties = build_properties(
        postgres_cluster,
        replication,
        snapshot_mode="no_data",
        max_batch_size=8,
        overrides={"max.queue.size": "8192"},
    )
    properties["table.include.list"] = "app.documents"
    handler = _GatedAcknowledgingHandler()
    engine = SupervisedDebeziumEngine(properties, handler)
    runner = threading.Thread(target=engine.run, name="p5-queue-gate", daemon=True)
    source_error = []
    source_started = threading.Event()
    source_finished = threading.Event()
    tag = f"p5gate-{os.getpid()}"

    def write_source_burst():
        source_started.set()
        try:
            with psycopg.connect(postgres_cluster.dsn) as conn:
                conn.execute(
                    "INSERT INTO app.documents (title, body, body_bytes, revision) "
                    "SELECT %s || i, "
                    "(SELECT string_agg(md5(i::text || ':' || g::text), '' ORDER BY g) "
                    "FROM generate_series(1, 2048) AS g), "
                    "65536, 1 FROM generate_series(1, 2048) AS rows(i)",
                    (f"{tag}-",),
                )
                conn.commit()
        except BaseException as exc:  # surfaced by the test thread below
            source_error.append(exc)
        finally:
            source_finished.set()

    source_writer = threading.Thread(target=write_source_burst, name="p5-source-burst")
    try:
        runner.start()
        deadline = time.monotonic() + 30
        live = None
        while live is None and time.monotonic() < deadline:
            live = engine.probe_live_queue()
            if engine.failure is not None:
                pytest.fail(f"stock Debezium failed before queue gate: {engine.failure}")
            if live is None:
                time.sleep(0.1)
        assert live is not None, "stock queue did not initialize before the burst"

        source_writer.start()
        assert source_started.wait(timeout=5)
        assert handler.first_callback.wait(timeout=60), (
            "the large source transaction never reached the gated callback"
        )
        samples = []
        capacity = int(live["queue_max_queue_size_in_bytes"])
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            current = engine.probe_live_queue()
            if current is not None:
                samples.append(current)
                if current["queue_current_size_in_bytes"] >= capacity * 0.85:
                    break
            if engine.failure is not None:
                pytest.fail(f"stock Debezium failed while queue was gated: {engine.failure}")
            time.sleep(0.1)

        assert samples, "the gated source produced no live queue samples"
        peak = max(samples, key=lambda item: item["queue_current_size_in_bytes"])
        assert peak["queue_current_size_in_bytes"] >= capacity * 0.85, peak
        assert peak["queue_over_capacity_bytes"] < 131072, peak
        assert peak["queue_current_size"] <= peak["queue_total_capacity"]
        assert not source_finished.is_set() or not source_error, source_error

        handler.release.set()
        source_writer.join(timeout=90)
        assert not source_writer.is_alive(), "source burst thread did not finish"
        assert source_error == [], source_error
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            with handler._lock:
                received = handler.records
            if received >= 2048:
                break
            if engine.failure is not None:
                pytest.fail(f"stock Debezium failed while draining queue: {engine.failure}")
            time.sleep(0.1)
        with handler._lock:
            received = handler.records
        assert received >= 2048, received
    finally:
        handler.release.set()
        if runner.is_alive():
            closer = threading.Thread(
                target=lambda: engine.close(intentional=True),
                name="p5-queue-gate-close",
                daemon=True,
            )
            closer.start()
            closer.join(timeout=30)
            if closer.is_alive():
                pytest.fail("stock Debezium close hung after queue gate")
        runner.join(timeout=30)
        if runner.is_alive():
            pytest.fail("stock Debezium runner hung after queue gate")
        if source_writer.is_alive():
            source_writer.join(timeout=30)
        with suppress(Exception):
            _drop_slot(postgres_cluster, slot)
