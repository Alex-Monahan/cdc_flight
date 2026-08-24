from __future__ import annotations

import time

from cdc_flight.source_health import SlotSample, SourceHealth

_UNSET = object()


def test_operator_lag_is_delivered_minus_confirmed_not_cluster_retained_wal():
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://unused", slot_name="slot")
    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            confirmed_pos=400,
            # Deliberately much larger: this is PostgreSQL's cluster-retained value,
            # not this connector's delivered-but-unconfirmed backlog.
            lag_bytes=90_000,
        )
    )

    assert health.per_slot_outstanding_bytes(1_000) == 600
    details = health.operator_lag_context(1_000)
    assert details["lag_definition"] == (
        "max(0, received_high_water_lsn - confirmed_flush_lsn)"
    )
    assert details["received_high_water_lsn"] == 1_000
    assert details["confirmed_flush_lsn"] == 400
    assert details["cluster_retained_lag_bytes"] == 90_000


def test_operator_lag_is_unknown_without_both_lsn_ends():
    from cdc_flight.source_health import SlotSample, SourceHealth

    health = SourceHealth(dsn="postgresql://unused", slot_name="slot")
    health._ingest(SlotSample(at=time.monotonic(), exists=True, active=True))
    assert health.per_slot_outstanding_bytes(1_000) is None


def _service_status(
    health,
    *,
    engine_thread_alive=True,
    own_progress_at=_UNSET,
    own_ack_at=_UNSET,
    own_ack_lsn=100,
    durable_lsn=100,
    received_high_water=100,
    progress_stale_after=15.0,
):
    now = time.monotonic()
    return health.service_status(
        received_high_water,
        engine_thread_alive=engine_thread_alive,
        own_progress_at=now if own_progress_at is _UNSET else own_progress_at,
        own_ack_at=now if own_ack_at is _UNSET else own_ack_at,
        own_ack_lsn=own_ack_lsn,
        durable_lsn=durable_lsn,
        progress_stale_after=progress_stale_after,
    )


def _active_health(*, lag_bytes=0, confirmed_pos=100):
    health = SourceHealth(dsn="postgresql://unused", slot_name="slot")
    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3210,
            confirmed_pos=confirmed_pos,
            lag_bytes=lag_bytes,
        )
    )
    return health


def test_service_witness_requires_our_callback_commit_and_ack():
    """An active slot with no Flight-owned progress is stalled, not quiet."""
    health = _active_health(lag_bytes=3_315_744)
    now = time.monotonic()

    assert _service_status(
        health,
        own_progress_at=None,
        own_ack_at=None,
        own_ack_lsn=None,
        durable_lsn=None,
    ) == "stalled"

    assert _service_status(
        health,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=100,
        durable_lsn=100,
    ) == "connected_quiet"


def test_service_witness_dead_engine_never_becomes_quiet():
    health = _active_health(lag_bytes=3_315_744)
    now = time.monotonic()
    assert _service_status(
        health,
        engine_thread_alive=False,
        own_progress_at=now,
        own_ack_at=now,
    ) == "engine_thread_dead"
    assert health.summary()["service_engine_thread_dead"] is True


def test_service_witness_requires_our_ack_position_and_durable_resume_point():
    health = _active_health(lag_bytes=0)
    now = time.monotonic()
    assert _service_status(
        health,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=None,
        durable_lsn=100,
    ) == "unproven"

    ahead = _active_health(lag_bytes=0, confirmed_pos=101)
    assert _service_status(
        ahead,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=100,
        durable_lsn=100,
    ) == "stalled"


def test_service_witness_uses_retained_lag_to_name_stall_when_our_progress_is_stale():
    """The gate shape has per-slot outstanding zero but multi-megabyte retained WAL."""
    health = _active_health(lag_bytes=6_600_512)
    stale = time.monotonic() - 30
    assert _service_status(
        health,
        own_progress_at=stale,
        own_ack_at=stale,
    ) == "stalled"


def test_service_witness_transient_stall_recovers_with_our_next_ack():
    health = _active_health(lag_bytes=2_000_000)
    stale = time.monotonic() - 30
    assert _service_status(health, own_progress_at=stale, own_ack_at=stale) == "stalled"
    assert health.dark_for >= 0

    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3211,
            confirmed_pos=200,
            lag_bytes=1_000,
        )
    )
    now = time.monotonic()
    assert _service_status(
        health,
        own_progress_at=now,
        own_ack_at=now,
        own_ack_lsn=200,
        durable_lsn=200,
        received_high_water=200,
    ) == "connected_quiet"
    assert health.dark_for < 1


def test_service_witness_mutation_guards_cover_each_required_input():
    """Each independent witness input has a committed negative cell."""
    now = time.monotonic()

    # Removing the own-progress requirement would make this active slot quiet.
    progress_missing = _active_health(lag_bytes=0)
    assert _service_status(
        progress_missing,
        own_progress_at=None,
        own_ack_at=now,
        own_ack_lsn=100,
        durable_lsn=100,
    ) == "unproven"

    # Removing the engine-thread requirement would let the dead engine renew.
    dead_engine = _active_health(lag_bytes=0)
    assert _service_status(
        dead_engine,
        engine_thread_alive=False,
        own_progress_at=now,
        own_ack_at=now,
    ) == "engine_thread_dead"

    # Replacing retained lag with only per-slot outstanding would turn this into
    # the gate's false green: high-water equals confirmed, but source WAL is pending.
    lag_removed = _active_health(lag_bytes=3_860_000)
    stale = now - 30
    assert _service_status(
        lag_removed,
        own_progress_at=stale,
        own_ack_at=stale,
    ) == "stalled"
