"""Rubric 1.7 / TODO 4.6(b) — a blackholed source must never report success.

This is the residual B5-class bug the pre-commit-health investigation measured and
TODO 4.6(b) recorded: `SourceHealth` reports `unknown` when the slot cannot be
queried at all, and `may_declare_idle()` returned **True** for `unknown` (a
deliberate fail-soft for "no psycopg / bad credentials"). A source that has been
*blackholed* mid-run - packets silently dropped, sockets left open, which is the
classic silently-dead-node shape rubric 4.6 is about - looks exactly like that:
no batches arrive, the slot cannot be sampled, the idle timer fires, and the run
exits `ok: true` on a partial delivery.

The blackhole is real, not simulated: the pipeline connects to Postgres through an
in-process TCP relay, and the relay stops forwarding bytes in both directions
without closing either socket. Nothing is killed and no packet filter (or root) is
needed, so this runs anywhere the rest of the suite runs.

The distinction the fix has to preserve: a sampler that has **never** succeeded
(no `psycopg`, wrong credentials, a firewall that was always there) must stay
fail-soft, or every run in such an environment burns to `--max-seconds`. A sampler
that *was* working and then went dark is a source outage, and a run that cannot
corroborate quiet against the source must not call itself successful.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from contextlib import suppress

import duckdb
import psycopg
import pytest
from support.fixtures import (
    POSTGRES_TEST_INSTANCE,
    PROJECT_DIR,
    TEST_INSTANCE_ID,
    TEST_SLOT_PREFIX,
    _executable,
    _popen_with_slot_startup_gate,
    _start_thread_with_slot_startup_gate,
)
from support.tcp_relay import TcpRelay


@pytest.fixture
def relay(postgres_cluster):
    """A TCP relay in front of Postgres that can be blackholed on demand."""
    relay = TcpRelay(postgres_cluster.host, postgres_cluster.port).start()
    try:
        yield relay
    finally:
        relay.stop()


def _wait_for(predicate, timeout: float, interval: float = 0.25) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.mark.slow
def test_a_blackholed_source_never_reports_ok(tmp_path, postgres_cluster, relay):
    """The run must exit non-zero, and `last_run.json` must say what happened."""
    slot = f"{TEST_SLOT_PREFIX}blackhole_{os.getpid()}"[:63]
    state = tmp_path / "cdc_state"
    env = {
        **os.environ,
        # Everything the pipeline does with the source - Debezium, the health
        # sampler, the catalog poller - goes through the relay.
        "PGHOST": "127.0.0.1",
        "PGPORT": str(relay.port),
        "CDC_TEST_PGPORT": str(relay.port),
        "PGDATABASE": postgres_cluster.dbname,
        "CDC_TEST_PGDATABASE": postgres_cluster.dbname,
        "CDC_STATE_DIR": str(state),
        "CDC_DUCKDB_PATH": str(tmp_path / "cdc_flight.duckdb"),
        "CDC_SLOT_NAME": slot,
        "CDC_PIPELINE_NAME": f"cdc_flight_blackhole_{TEST_INSTANCE_ID}",
        "CDC_TEST_SLOT_STARTUP_LOCK": str(
            POSTGRES_TEST_INSTANCE.slot_startup_lock_path
        ),
        "CDC_IDLE_SECONDS": "6",
        "RUNTIME__DLTHUB_TELEMETRY": "false",
    }
    _drop(postgres_cluster.dsn, slot)
    try:
        started_at = time.monotonic()
        proc = _popen_with_slot_startup_gate(
            [
                _executable("cdc-flight"),
                "--destination", "duckdb",
                "--max-seconds", "70",
                "--idle-seconds", "6",
            ],
            env=env,
                cwd=PROJECT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=False,
            )
        # Let the snapshot get going, so the sampler has succeeded at least once
        # and the run has real work in flight when the source disappears.
        assert _wait_for(lambda: relay.bytes_relayed > 200_000, timeout=90), (
            "the pipeline never streamed anything through the relay"
        )
        went_dark_at = time.monotonic() - started_at
        relay.blackhole()
        returncode = proc.wait(timeout=200)
    finally:
        with_suppress = getattr(proc, "poll", lambda: None)()
        if with_suppress is None:
            proc.kill()
        _drop(postgres_cluster.dsn, slot)

    summary_path = state / "last_run.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    control = duckdb.connect(str(tmp_path / "cdc_flight.duckdb"), read_only=True)
    try:
        alerts = control.execute(
            "SELECT severity, code FROM _cdc_flight.alerts "
            "WHERE pipeline = ? ORDER BY raised_at",
            (env["CDC_PIPELINE_NAME"],),
        ).fetchall()
    finally:
        control.close()
    assert returncode != 0, (
        f"a blackholed Postgres produced a SUCCESSFUL run: returncode={returncode} "
        f"summary={ {k: v for k, v in summary.items() if k != 'output'} }"
    )
    assert summary.get("ok") is not True, f"summary claims ok on a dead source: {summary}"
    # And it must say *why*, by NAME. `"source" in json.dumps(summary).lower()` used to
    # stand here, which `source_schema` and half a dozen other keys satisfy (Opus
    # MINOR-5). The mechanism has a name and the summary carries it.
    assert summary.get("stop_reason") == "source_dark", summary
    assert "unreachable" in (summary.get("error") or "").lower(), summary.get("error")
    source_dark_alerts = [
        (severity, code) for severity, code in alerts
        if severity == "critical" and code == "source_dark"
    ]
    assert len(source_dark_alerts) == 1, alerts
    assert any(
        severity == "critical" and code == "source_dark"
        for severity, code in alerts
    ), alerts
    # And the BOUND is measured, not asserted from the configuration. RUBRIC_STATUS
    # claims detection "within CDC_SOURCE_DARK_SECONDS (45 s)"; with `--max-seconds 70`
    # the run could equally have died of the not-streaming guard at 70 s, so the claim
    # was unproven by the test that carried it (Opus MINOR-5).
    #
    # The measurement is the run's OWN detection instant, not the process's exit: a
    # connector blocked on a dead socket takes another minute to tear its JVM down, so
    # time-to-exit is a measurement of the shutdown path and not of the detector.
    detected_at = summary.get("source_dark_detected_after_sec")
    assert detected_at is not None, summary
    detected_in = detected_at - went_dark_at
    assert 0 < detected_in < 60, (
        f"the dark source was detected {detected_in:.1f}s after the blackhole "
        f"(run-relative: dark at {went_dark_at:.1f}s, detected at {detected_at:.1f}s). "
        "CDC_SOURCE_DARK_SECONDS is 45 s; anything near --max-seconds=70 would mean the "
        "deadline ended this run rather than the detector"
    )
    # The engine usually cannot be closed cleanly against a dead socket, and that is
    # expected — but whether the JVM happens to finish `close()` inside
    # `--close-timeout` is a race, not a correctness property, and this assertion was
    # recorded flaking while every correctness assertion passed (Codex r1 MINOR-4).
    #
    # The contract is: a non-zero exit, `source_dark` by name, a bounded detection time,
    # and NO downgrade of the diagnosis to the shutdown symptom. That last part is what
    # A49 is about, and it is the only thing `close_hung` is evidence for here.
    if summary.get("close_hung"):
        assert summary.get("stop_reason") == "source_dark", (
            "engine.close() hung AND the run reported the hang: the symptom replaced "
            f"the diagnosis, which is exactly A49 ({summary})"
        )


@pytest.mark.slow
def test_stock_jdbc_blackhole_times_out_without_the_python_sampler(
    tmp_path, postgres_cluster, relay
):
    """An independent stock Debezium/JDBC connection detects the real blackhole.

    The engine is driven directly, with no ``SourceHealth`` or pipeline supervisor.
    A separate psycopg connection talks to the real PostgreSQL port and remains
    healthy while only the relay path is blackholed. That isolates pgjdbc's bounded
    socket timeout from the Python sampler's outage detector.
    """
    from pydbzengine import BasePythonChangeHandler

    from cdc_flight.config import ReplicationConfig, SourceConfig
    from cdc_flight.debezium_props import build_properties
    from cdc_flight.engine import SupervisedDebeziumEngine

    class NoopHandler(BasePythonChangeHandler):
        def handleJsonBatch(self, records):
            return None

    slot = f"{TEST_SLOT_PREFIX}jdbc_blackhole_{os.getpid()}"[:63]
    state = tmp_path / "jdbc_state"
    relay_source = SourceConfig(
        host=postgres_cluster.host,
        port=relay.port,
        user=postgres_cluster.user,
        password=postgres_cluster.password,
        dbname=postgres_cluster.dbname,
        schema=postgres_cluster.schema,
    )
    replication = ReplicationConfig(slot_name=slot, state_dir=state)
    props = build_properties(
        relay_source,
        replication,
        snapshot_mode="no_data",
        jdbc_socket_timeout_seconds=3,
        jdbc_connect_timeout_seconds=2,
    )
    _drop(postgres_cluster.dsn, slot)
    engine = SupervisedDebeziumEngine(
        props,
        NoopHandler(),
        offset_file=replication.offset_file,
        always_commit_offsets=True,
    )
    startup_gate_env = {
        "PGHOST": postgres_cluster.host,
        "PGPORT": str(postgres_cluster.port),
        "PGUSER": postgres_cluster.user,
        "PGPASSWORD": postgres_cluster.password,
        "PGDATABASE": postgres_cluster.dbname,
        "CDC_TEST_SLOT_STARTUP_LOCK": str(
            POSTGRES_TEST_INSTANCE.slot_startup_lock_path
        ),
    }
    runner = _start_thread_with_slot_startup_gate(
        engine.run,
        env=startup_gate_env,
        slot=slot,
        name="jdbc-only-blackhole",
    )
    direct_successes = 0
    blackholed_at = None
    try:
        assert _wait_for(lambda: relay.connections > 0, timeout=30), (
            "stock Debezium never opened the relay connection"
        )
        # Let the connector finish startup and put a heartbeat/query on the wire.
        assert _wait_for(lambda: relay.bytes_relayed > 100, timeout=30), (
            "stock Debezium never exchanged bytes through the relay"
        )
        with psycopg.connect(postgres_cluster.dsn, autocommit=True) as direct:
            direct.execute("SELECT 1")
            relay.blackhole()
            blackholed_at = time.monotonic()
            deadline = blackholed_at + 15
            while time.monotonic() < deadline and engine.failure is None:
                direct.execute("SELECT 1")
                direct_successes += 1
                time.sleep(0.25)
        runner.join(timeout=20)
        assert engine.failure is not None, (
            "stock Debezium/JDBC did not report the relay blackhole within its "
            f"socket timeout; effective={engine.effective_configuration}"
        )
        assert direct_successes >= 2, direct_successes
        assert blackholed_at is not None
        assert time.monotonic() - blackholed_at < 15
        assert engine.effective_configuration["driver.socketTimeout"] == "3"
    finally:
        if runner.is_alive():
            with_suppress = getattr(engine, "close", lambda **kwargs: None)
            with suppress(Exception):
                with_suppress(intentional=False)
            runner.join(timeout=20)
        _drop(postgres_cluster.dsn, slot)


def _drop(dsn: str, slot: str) -> None:
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name = %s",
                (slot,),
            )
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# The unit-level statement of the same rule, so the default suite carries it too.
# --------------------------------------------------------------------------- #
def test_unknown_after_a_working_sampler_forbids_idle():
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    # It worked: streaming, caught up, for longer than the idle window.
    health._ingest(SlotSample(at=now - 30, exists=True, active=True, lag_bytes=0))
    health._ingest(SlotSample(at=now - 0.1, exists=True, active=True, lag_bytes=0))
    assert health.may_declare_idle(min_seconds=5) is True

    # Then the source goes dark.
    health._ingest(SlotSample(at=now, error="OperationalError: timeout"))
    assert health.may_declare_idle(min_seconds=5) is False, (
        "an unreachable source that was reachable a moment ago is a source outage, "
        "not a reason to declare the stream idle (TODO 4.6(b))"
    )
    # A source that WAS answering and stopped: still `unknown`, and distinct from the
    # never-sampled case below — which is the whole point of the declared domain.
    assert health.summary()["slot_health"] == "unknown"
    assert health.state() == "unknown"


def test_stale_active_sample_cannot_prove_idle():
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    health._ingest(
        SlotSample(
            at=time.monotonic() - 30,
            exists=True,
            active=True,
            lag_bytes=0,
        )
    )
    assert health.may_declare_idle(min_seconds=5) is False


def test_service_liveness_witness_keeps_quiet_source_healthy_but_stops_retry_renewal():
    """The single-process replacement for the removed IPC frame-deadline property."""
    from cdc_flight.config import ServiceConfig
    from cdc_flight.service_runtime import ServiceContext
    from cdc_flight.source_health import SlotSample, SourceHealth

    class LeaseProbe:
        lease_key = "physical:test"
        epoch = 1
        renewed = 0

        def renew_control(self, _connection):
            self.renewed += 1

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    health._ingest(
        SlotSample(
            at=now,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    policy = ServiceConfig(
        lease_ttl_seconds=2,
        lease_renew_seconds=0.1,
        heartbeat_bound_seconds=0.3,
        stall_timeout_seconds=0.8,
        stall_exit_grace_seconds=0.2,
        watchdog_poll_seconds=0.01,
        commit_timeout_seconds=0.3,
        close_timeout_seconds=0.3,
        invariant_check_seconds=0.1,
        source_health_stale_seconds=0.5,
    )
    lease = LeaseProbe()
    context = ServiceContext(
        service_id="service",
        lease_id="lease",
        worker_generation="service:generation",
        policy=policy,
    )
    try:
        context.bind(lease, object())
        context.set_engine_thread_alive(True)
        context.note_engine_callback()
        context.note_engine_commit(100)
        context.note_engine_ack(100)
        signal = context.engine_liveness_signal()
        context.observe_source_health(
            "connected_quiet", now, engine_thread_alive=True
        )
        context._next_heartbeat = time.monotonic() - 1
        assert health.service_status(
            100,
            engine_thread_alive=signal["engine_thread_alive"],
            own_progress_at=signal["own_progress_at"],
            own_ack_at=signal["own_ack_at"],
            own_ack_lsn=signal["own_ack_lsn"],
            durable_lsn=100,
            progress_stale_after=policy.source_health_stale_seconds,
        ) == "connected_quiet"
        assert context.renew_once() is True
        assert lease.renewed == 1

        health._ingest(
            SlotSample(
                at=time.monotonic(),
                exists=True,
                active=False,
                confirmed_pos=100,
                lag_bytes=2_000_000,
            )
        )
        sample = health.last
        signal = context.engine_liveness_signal()
        disconnected = health.service_status(
            100,
            engine_thread_alive=signal["engine_thread_alive"],
            own_progress_at=signal["own_progress_at"],
            own_ack_at=signal["own_ack_at"],
            own_ack_lsn=signal["own_ack_lsn"],
            durable_lsn=100,
            progress_stale_after=policy.source_health_stale_seconds,
        )
        context.observe_source_health(
            disconnected, sample.at, engine_thread_alive=True
        )
        context._next_heartbeat = time.monotonic() - 1
        assert disconnected == "disconnected"
        assert context.renew_once() is False
        assert lease.renewed == 1
    finally:
        context.close()


def test_reconnected_stream_must_advance_confirmed_wal_before_large_lag_is_idle():
    """A retry reattachment is not proof that the interrupted delivery recovered."""
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    health._ingest(
        SlotSample(
            at=now - 20,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    health._ingest(
        SlotSample(
            at=now - 10,
            exists=True,
            active=False,
            confirmed_pos=100,
            lag_bytes=2_000_000,
        )
    )
    health._ingest(
        SlotSample(
            at=now,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=2_000_000,
        )
    )
    assert health.stream_interruptions == 1
    assert health.recovered_after_interruption is False
    assert health.may_declare_idle(min_seconds=5) is False

    health._ingest(
        SlotSample(
            at=now + 0.1,
            exists=True,
            active=True,
            confirmed_pos=101,
            lag_bytes=2_000_000,
        )
    )
    assert health.recovered_after_interruption is True


def test_growing_backlog_is_not_a_stable_idle_backlog():
    """Lag that increases while the source is active cannot satisfy the flat-lag proof."""
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    health._ingest(
        SlotSample(
            at=now - 20,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=100_000,
        )
    )
    health._ingest(
        SlotSample(
            at=now - 1,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=2_000_000,
        )
    )
    assert health.lag_steady_for < 5
    assert health.may_declare_idle(min_seconds=5) is False


def test_a_sampler_that_never_worked_stays_fail_soft():
    """No psycopg, wrong credentials, a firewall that was always there."""
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    health._ingest(SlotSample(at=now - 30, error="ModuleNotFoundError: psycopg"))
    health._ingest(SlotSample(at=now, error="ModuleNotFoundError: psycopg"))
    assert health.may_declare_idle(min_seconds=5) is True, (
        "a source that could never be consulted must degrade to timer-only idle "
        "detection rather than turning every run into a --max-seconds wait"
    )
    # rubric 1.9: the summary now carries the DECLARED classification, and this is the
    # state that never had a name — A51 row 50's fail-open. It used to read `unknown`,
    # which is the same word for "the source stopped answering" (a failure) and "we
    # could never ask" (a degradation), and an operator had to read `slot_ever_sampled`
    # in the next key to tell them apart.
    assert health.summary()["slot_health"] == "unknown_never_sampled"
    assert health.summary()["slot_ever_sampled"] is False
    assert health.state() == "unknown_never_sampled"


def test_unknown_is_reported_as_time_not_streaming():
    """`not_streaming_for` used to be RESET by an `unknown` sample."""
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://nope", slot_name="x")
    now = time.monotonic()
    health._ingest(SlotSample(at=now - 30, exists=True, active=True, lag_bytes=0))
    health._ingest(SlotSample(at=now - 10, exists=True, active=False, lag_bytes=99999))
    health._ingest(SlotSample(at=now - 5, error="OperationalError: timeout"))
    assert health.not_streaming_for >= 9.0, (
        "an unreachable source cleared the not-streaming clock, so the "
        "--max-seconds guard in run_engine_bounded could not fire"
    )
