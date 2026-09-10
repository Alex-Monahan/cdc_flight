"""Default contract proofs for the SourceHealth -> destination admission seam."""

from __future__ import annotations

import dataclasses
import inspect
from datetime import UTC, datetime

import duckdb
import pytest

from cdc_flight import destination
from cdc_flight.backfill import (
    BackfillCoordinator,
    RefreshPolicy,
    RefreshScheduler,
)
from cdc_flight.source_health import (
    SlotSample,
    SourceHealth,
    SourceHealthObservation,
    SourceHealthObservationQueue,
)


class RecordingSignalWriter:
    """Source-writer spy; production code must reach it through reconciliation."""

    def __init__(self) -> None:
        self.signals = []

    def insert(self, signal) -> str:
        self.signals.append(signal)
        return signal.signal_id


def _observation(
    *,
    sequence: int = 1,
    confirmed: int | None = 100,
    current: int | None = 100,
    observed_at: datetime | None = None,
) -> SourceHealthObservation:
    lag = None if confirmed is None or current is None else max(0, current - confirmed)
    return SourceHealthObservation(
        slot_name="p3d_contract_slot",
        sequence=sequence,
        sampled_at=float(sequence),
        observed_at=observed_at or datetime(2026, 9, 9, tzinfo=UTC),
        slot_exists=True,
        slot_active=True,
        confirmed_flush_lsn=confirmed,
        restart_lsn=confirmed,
        current_wal_lsn=current,
        lag_bytes=lag,
    )


def _owner(
    tmp_path,
    *,
    size_threshold: int | None,
    time_threshold: int | None,
    pending_source_ts_ms: int | None = None,
    pending_source_lsn: int = 200,
):
    path = tmp_path / "admission.duckdb"
    con = duckdb.connect(str(path))
    destination.ensure_control_schema(con)
    coordinator = BackfillCoordinator(
        con,
        pipeline="p3d-contract",
        control_schema="_cdc_flight",
    )
    scheduler = RefreshScheduler(
        coordinator,
        signal_writer=RecordingSignalWriter(),
    )
    scheduler.configure(
        RefreshPolicy(
            "app",
            "customers",
            mode="incremental",
            size_threshold_bytes=size_threshold,
            time_threshold_ms=time_threshold,
        )
    )
    if pending_source_ts_ms is not None:
        con.execute(
            'INSERT INTO "_cdc_flight"."source_data_facts" '
            "(pipeline, commit_id, source_schema, source_table, source_lsn, "
            "source_ts_ms, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "p3d-contract",
                1,
                "app",
                "customers",
                pending_source_lsn,
                pending_source_ts_ms,
                datetime(2026, 9, 9, tzinfo=UTC),
            ],
        )
    return con, coordinator, scheduler


def test_health_sample_only_queues_an_immutable_observation():
    queue = SourceHealthObservationQueue()
    health = SourceHealth(
        dsn="unused",
        slot_name="p3d-contract-slot",
        observation_callback=queue.publish,
    )

    health._ingest(
        SlotSample(
            at=1.0,
            exists=True,
            active=True,
            confirmed_pos=100,
            lag_bytes=50,
            observed_at=datetime(2026, 9, 9, tzinfo=UTC),
        )
    )

    observation = queue.peek()
    assert isinstance(observation, SourceHealthObservation)
    assert dataclasses.fields(type(observation))
    assert type(observation).__dataclass_params__.frozen is True
    assert observation.current_wal_lsn == 150
    assert not hasattr(observation, "admit_fall_behind")
    assert not hasattr(observation, "observation_callback")
    with pytest.raises(dataclasses.FrozenInstanceError):
        observation.confirmed_flush_lsn = 151

    # The sampler made no destination connection and no source-side request; the
    # only observable effect is one bounded queue entry.
    assert queue.stats() == {
        "bounded_capacity": 1,
        "pending": True,
        "published": 1,
        "coalesced": 0,
        "consumed": 0,
    }


def test_queue_replaces_old_samples_without_blocking_or_losing_newer_state():
    queue = SourceHealthObservationQueue()
    older = _observation(sequence=1, confirmed=100, current=200)
    newer = _observation(sequence=2, confirmed=150, current=250)

    assert queue.publish(older)
    assert queue.publish(newer)
    assert queue.peek() == newer
    assert not queue.consume(older)
    assert queue.consume(newer)
    assert queue.peek() is None
    assert queue.stats()["coalesced"] == 1


@pytest.mark.parametrize(
    ("size_threshold", "time_threshold", "current", "pending_ts", "expected"),
    [
        (50, None, 200, None, "bytes"),
        (None, 3_000, 100, 1_000, "time"),
        (50, 3_000, 200, 1_000, "both"),
    ],
)
def test_destination_owner_persists_exact_health_reason_and_publishes_after_commit(
    tmp_path,
    size_threshold,
    time_threshold,
    current,
    pending_ts,
    expected,
):
    con, _coordinator, scheduler = _owner(
        tmp_path,
        size_threshold=size_threshold,
        time_threshold=time_threshold,
        pending_source_ts_ms=pending_ts,
    )
    try:
        result = scheduler.admit_source_health_observation(
            _observation(current=current),
            now_ms=10_000,
        )

        assert result.admitted
        assert result.selected == ("app.customers",)
        assert result.reasons == (("app.customers", expected),)
        assert result.published_signal_ids == (result.signal_id,)
        assert result.as_dict()["source_effect_route"] == "reconcile_signal_effects"
        assert result.as_dict()["source_signal_inserted_directly"] is False
        assert len(scheduler.signal_writer.signals) == 1

        row = con.execute(
            'SELECT trigger_reason, signal_id, state '
            'FROM "_cdc_flight"."backfill_runs" '
            "WHERE pipeline = ?",
            ["p3d-contract"],
        ).fetchone()
        assert row == (expected, result.signal_id, "requested")
        intent = con.execute(
            'SELECT state FROM "_cdc_flight"."backfill_signal_intents" '
            "WHERE pipeline = ? AND signal_id = ?",
            ["p3d-contract", result.signal_id],
        ).fetchone()
        assert intent == ("published",)
    finally:
        con.close()


def test_unknown_pending_age_refuses_without_last_applied_substitution(tmp_path):
    con, _coordinator, scheduler = _owner(
        tmp_path,
        size_threshold=None,
        time_threshold=1,
    )
    try:
        result = scheduler.admit_source_health_observation(
            _observation(current=100),
            now_ms=10_000,
        )

        assert not result.admitted
        assert result.selected == ()
        assert result.rejected_unknown_age == ("app.customers",)
        assert scheduler.signal_writer.signals == []
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight"."backfill_runs" '
            "WHERE pipeline = ?",
            ["p3d-contract"],
        ).fetchone() == (0,)
    finally:
        con.close()


def test_same_observation_is_idempotently_coalesced_to_one_run_and_signal(tmp_path):
    con, _coordinator, scheduler = _owner(
        tmp_path,
        size_threshold=1,
        time_threshold=None,
    )
    observation = _observation(current=101)
    try:
        first = scheduler.admit_source_health_observation(observation)
        second = scheduler.admit_source_health_observation(observation)

        assert first.admitted
        assert second.coalesced
        assert not second.admitted
        assert second.signal_id is None
        assert len(scheduler.signal_writer.signals) == 1
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight"."backfill_runs" '
            "WHERE pipeline = ?",
            ["p3d-contract"],
        ).fetchone() == (1,)
    finally:
        con.close()


def test_later_sample_at_same_confirmed_boundary_is_durably_coalesced(tmp_path):
    con, _coordinator, scheduler = _owner(
        tmp_path,
        size_threshold=1,
        time_threshold=None,
    )
    try:
        first = scheduler.admit_source_health_observation(
            _observation(sequence=1, confirmed=100, current=101)
        )

        # Remove the active run after the first admission so the second
        # sample must be suppressed by the durable source-data boundary, not
        # by the repository's active-run guard.
        con.execute(
            """
            UPDATE "_cdc_flight"."backfill_runs"
            SET state = 'complete', notification_status = 'COMPLETED'
            WHERE run_id = ?
            """,
            [first.run_ids[0]],
        )

        second = scheduler.admit_source_health_observation(
            _observation(sequence=2, confirmed=100, current=102)
        )

        assert first.admitted
        assert not second.admitted
        assert second.coalesced
        assert second.coalesced_tables == ("app.customers",)
        assert second.signal_id == first.signal_id
        assert len(scheduler.signal_writer.signals) == 1
        assert con.execute(
            'SELECT count(*) FROM "_cdc_flight"."backfill_runs" '
            "WHERE pipeline = ?",
            ["p3d-contract"],
        ).fetchone() == (1,)
        assert con.execute(
            'SELECT confirmed_flush_lsn, observation_id, signal_id, trigger_reason '
            'FROM "_cdc_flight"."source_health_admissions" '
            "WHERE pipeline = ? AND source_table = ?",
            ["p3d-contract", "customers"],
        ).fetchone() == (100, "p3d_contract_slot:1", first.signal_id, "bytes")
    finally:
        con.close()


def test_health_admission_has_no_slot_acknowledgement_reachability():
    source = inspect.getsource(RefreshScheduler.admit_source_health_observation)
    for forbidden in (
        "markProcessed",
        "markBatchFinished",
        "pg_replication_slots",
        "advance_slot",
        "writer.insert",
        "StockSignalWriter",
    ):
        assert forbidden not in source
