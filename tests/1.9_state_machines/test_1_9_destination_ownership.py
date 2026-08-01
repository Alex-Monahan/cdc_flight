"""Round 9: destination ownership must compose across blocking re-snapshot.

The supervisor's callback seal is only useful if every caller preserves its result.
These tests pin the two compositions that escaped the round-8 tests: a live
re-snapshot callback unwinding into the pipeline, and an idle main applier whose
consumer failed to construct before any callback could start.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from typing import ClassVar

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import pipeline as pipeline_mod
from cdc_flight import reconcile as reconcile_mod
from cdc_flight import resnapshot as resnapshot_mod
from cdc_flight.config import (
    ReplicationConfig,
    RunConfig,
    SourceConfig,
    applier_settings,
)
from cdc_flight.errors import EngineFailure
from cdc_flight.ownership import DestinationOwnership

PIPELINE = "destination_ownership"
TABLES = [("app", "customers", "cdcflight_app_customers")]


class _Alerts:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _Applier:
    instances: ClassVar[list[_Applier]] = []

    def __init__(self, *_args, **_kwargs) -> None:
        self.alerts = _Alerts()
        self.callback_quiesced = False
        self.shutdown_calls = 0
        self.snapshot_final_seen = False
        self.snapshot_tables_seen: set[str] = set()
        self.snapshot_completed = False
        self.last_snapshot_lsn = None
        self.record_count = 0
        self.busy = True
        type(self).instances.append(self)

    def shutdown(self, **_kwargs) -> None:
        self.shutdown_calls += 1


class _Engine:
    def __init__(self, **_kwargs) -> None:
        self.consumer = object()


class _StartedResource:
    def __init__(self, *_args, **_kwargs) -> None:
        self.stopped = False
        self.first_confirmed = None

    def start(self):
        return self

    def stop(self) -> None:
        self.stopped = True


def test_resnapshot_quiescence_failure_retains_every_owned_resource(monkeypatch, tmp_path):
    """The exact nested Round-9 schedule must transfer, not share, ownership.

    Once the supervisor says an admitted callback is still live, this stack must not
    close its alert cursor, repair lifecycle state through the parent connection, drop
    the throwaway slot, or remove the Debezium offset state.
    """
    replication = ReplicationConfig(slot_name="round9", state_dir=tmp_path / "state")
    ownership = DestinationOwnership()
    drop_calls: list[str] = []
    reassert_calls: list[object] = []
    _Applier.instances.clear()

    monkeypatch.setattr(resnapshot_mod, "Applier", _Applier)
    monkeypatch.setattr(resnapshot_mod, "_SlotWatcher", _StartedResource)
    monkeypatch.setattr(resnapshot_mod, "SourceHealth", _StartedResource)
    monkeypatch.setattr(
        reconcile_mod, "drop_slot", lambda _dsn, slot: drop_calls.append(slot)
    )
    monkeypatch.setattr(
        resnapshot_mod,
        "reassert_owed",
        lambda con, **_kwargs: reassert_calls.append(con),
    )

    import cdc_flight.engine as engine_mod

    monkeypatch.setattr(engine_mod, "SupervisedDebeziumEngine", _Engine)

    def _fail_after_seal(*_args, **_kwargs):
        offset = replication.state_dir / "resnapshot" / "offsets.dat"
        offset.write_bytes(b"callback-owned")
        raise EngineFailure(
            "callback did not quiesce",
            {"applier_quiesced": False, "destination_owner": "live_applier_callback"},
        )

    monkeypatch.setattr(pipeline_mod, "run_engine_bounded", _fail_after_seal)

    with pytest.raises(EngineFailure) as raised:
        resnapshot_mod.run(
            object(),
            source=SourceConfig(),
            replication=replication,
            pipeline=PIPELINE,
            dataset="cdc_raw",
            tables=TABLES,
            settings=applier_settings(),
            run_cfg=RunConfig(close_timeout=0.01),
            lease=object(),
            runner_id="runner",
            transactional_ddl=True,
            epoch_base=0,
            reason="round-9 regression",
            namespace="main",
            ownership=ownership,
        )

    applier = _Applier.instances[-1]
    state_dir = replication.state_dir / "resnapshot"
    assert applier.alerts.close_calls == 0
    assert reassert_calls == [], "the parent connection is still callback-owned"
    assert drop_calls == ["round9_rs"], "only the pre-start stale-slot sweep is safe"
    assert (state_dir / "offsets.dat").read_bytes() == b"callback-owned"
    assert resnapshot_mod.interruption_marker(state_dir).exists()
    assert ownership.destination_quiescent is False
    assert raised.value.summary["resnapshot_recovery"] == "armed"


def test_failed_quiescence_stays_callback_owned_if_callback_leaves_during_unwind(
    monkeypatch, tmp_path
):
    """Round 10 §3.2: a late callback exit cannot revoke marker recovery.

    The supervisor has already failed its bounded quiescence proof.  The callback then
    leaves during the exception handler's critical log, before the enclosing ``finally``
    re-enters ownership cleanup.  That later observation must not delete recovery intent
    which the handler deliberately chose instead of ``reassert_owed``.
    """
    replication = ReplicationConfig(slot_name="round10", state_dir=tmp_path / "state")
    ownership = DestinationOwnership()
    drop_calls: list[str] = []
    reassert_calls: list[object] = []
    _Applier.instances.clear()

    monkeypatch.setattr(resnapshot_mod, "Applier", _Applier)
    monkeypatch.setattr(resnapshot_mod, "_SlotWatcher", _StartedResource)
    monkeypatch.setattr(resnapshot_mod, "SourceHealth", _StartedResource)
    monkeypatch.setattr(
        reconcile_mod, "drop_slot", lambda _dsn, slot: drop_calls.append(slot)
    )
    monkeypatch.setattr(
        resnapshot_mod,
        "reassert_owed",
        lambda con, **_kwargs: reassert_calls.append(con),
    )

    import cdc_flight.engine as engine_mod

    monkeypatch.setattr(engine_mod, "SupervisedDebeziumEngine", _Engine)

    def _fail_after_seal(*_args, **_kwargs):
        offset = replication.state_dir / "resnapshot" / "offsets.dat"
        offset.write_bytes(b"callback-owned")
        raise EngineFailure(
            "callback did not quiesce",
            {"applier_quiesced": False, "destination_owner": "live_applier_callback"},
        )

    monkeypatch.setattr(pipeline_mod, "run_engine_bounded", _fail_after_seal)

    def _callback_leaves_during_log(*_args, **_kwargs) -> None:
        _Applier.instances[-1].callback_quiesced = True

    monkeypatch.setattr(resnapshot_mod.log, "critical", _callback_leaves_during_log)

    with pytest.raises(EngineFailure):
        resnapshot_mod.run(
            object(),
            source=SourceConfig(),
            replication=replication,
            pipeline=PIPELINE,
            dataset="cdc_raw",
            tables=TABLES,
            settings=applier_settings(),
            run_cfg=RunConfig(close_timeout=0.01),
            lease=object(),
            runner_id="runner",
            transactional_ddl=True,
            epoch_base=0,
            reason="round-10 unwind regression",
            namespace="main",
            ownership=ownership,
        )

    applier = _Applier.instances[-1]
    state_dir = replication.state_dir / "resnapshot"
    assert applier.callback_quiesced is True, "the race configuration did not fire"
    assert reassert_calls == [], "the handler selected marker recovery"
    assert drop_calls == ["round10_rs"], "only the pre-start stale-slot sweep is safe"
    assert applier.alerts.close_calls == 0
    assert (state_dir / "offsets.dat").read_bytes() == b"callback-owned"
    assert resnapshot_mod.interruption_marker(state_dir).exists()
    assert ownership.destination_quiescent is False


class _IdleApplier:
    def __init__(self) -> None:
        self.callback_quiesced = False
        self.alerts = _Alerts()
        self.shutdown_calls = 0

    def shutdown(self, **_kwargs) -> None:
        self.shutdown_calls += 1
        self.callback_quiesced = True


class _Lease:
    def __init__(self) -> None:
        self.release_calls = 0

    def release(self, _con) -> None:
        self.release_calls += 1


class _Phases:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def ensure(self, _phase) -> None:
        self.calls.append("ensure")

    def finish(self, **_kwargs) -> None:
        self.calls.append("finish")

    def close(self) -> None:
        self.calls.append("close")

    def summary(self) -> dict:
        return {"run_phase": "failed", "heartbeat_sink_retirement": "closed"}


def test_main_consumer_construction_failure_retires_the_idle_runtime(monkeypatch):
    """A constructed-but-never-activated applier is idle, not a live callback."""
    ownership = DestinationOwnership()
    applier = _IdleApplier()
    ownership.attach(applier)
    lease = _Lease()
    phases = _Phases()
    released: list[object] = []
    con = object()
    reported: dict = {}

    class _BrokenEngine:
        @property
        def consumer(self):
            raise RuntimeError("consumer construction failed")

    # The production boundary exactly: attach happened, consumer construction raises,
    # and activation never happened.
    with pytest.raises(RuntimeError, match="consumer construction"):
        _consumer = _BrokenEngine().consumer

    monkeypatch.setattr(
        dest_mod,
        "release_connection",
        lambda actual: released.append(actual)
        or SimpleNamespace(state="closed", error=None),
    )
    pipeline_mod._teardown_destination(
        con=con,
        ownership=ownership,
        reported=reported,
        phases=phases,
        lease=lease,
        lease_held=True,
        run_ok=False,
    )

    assert applier.shutdown_calls == 1
    assert applier.alerts.close_calls == 1
    assert lease.release_calls == 1
    assert phases.calls == ["ensure", "finish", "close"]
    assert released == [con]
    assert reported["destination_connection_release"] == "closed"
    assert ownership.destination_quiescent is True

    source = inspect.getsource(pipeline_mod.run)
    assert source.index("ownership.attach(applier)") < source.index("engine.consumer")


def test_live_resnapshot_owner_blocks_the_whole_pipeline_teardown(monkeypatch):
    ownership = DestinationOwnership()
    applier = _Applier()
    ownership.attach(applier)
    ownership.activate(applier)
    lease = _Lease()
    phases = _Phases()
    reported: dict = {}

    monkeypatch.setattr(
        dest_mod,
        "release_connection",
        lambda _con: pytest.fail("the callback-owned parent connection was retired"),
    )
    pipeline_mod._teardown_destination(
        con=object(),
        ownership=ownership,
        reported=reported,
        phases=phases,
        lease=lease,
        lease_held=True,
        run_ok=False,
    )

    assert phases.calls == []
    assert lease.release_calls == 0
    assert reported["destination_connection_release"] == "abandoned"
    assert reported["destination_connection_release_reason"] == "live_applier_callback"
    assert reported["heartbeat_sink_retirement"] == "abandoned"


def test_next_run_requeues_an_armed_resnapshot_and_can_complete_it(tmp_path):
    """Durable filesystem intent bridges the hard-exit boundary without using `con`."""
    state_dir = tmp_path / "state" / "resnapshot"
    con = duckdb.connect(str(tmp_path / "destination.duckdb"))
    dest_mod.ensure_control_schema(con)
    dest_mod.ensure_dataset(con, "cdc_raw")
    dest_mod.request_snapshot(
        con, pipeline=PIPELINE, tables=TABLES, detail="initial owed work"
    )
    # Model a callback that committed its first partial snapshot chunk.
    from cdc_flight import table_lifecycle

    table_lifecycle.transition(
        con,
        pipeline=PIPELINE,
        source_schema="app",
        source_table="customers",
        to=table_lifecycle.IN_PROGRESS,
        reason="partial re-snapshot chunk",
    )
    resnapshot_mod.arm_interruption_marker(
        state_dir, pipeline=PIPELINE, tables=TABLES
    )

    requeued = resnapshot_mod.requeue_interrupted(
        con, pipeline=PIPELINE, state_dir=state_dir
    )
    assert requeued == ["app.customers"]
    assert dest_mod.tables_awaiting_snapshot(con, PIPELINE) == TABLES
    assert not resnapshot_mod.interruption_marker(state_dir).exists()

    completed = resnapshot_mod.finish_verified_empty_tables(
        con,
        pipeline=PIPELINE,
        dataset="cdc_raw",
        tables=TABLES,
        done=set(),
        evidence=resnapshot_mod.EmptinessEvidence(
            snapshot_phase_ended=True,
            tables_seen=set(),
            source_empty_at={"app.customers": 0},
            wal_lsn=123,
        ),
    )
    assert completed == ["app.customers"]
    assert dest_mod.tables_awaiting_snapshot(con, PIPELINE) == []
    con.close()
