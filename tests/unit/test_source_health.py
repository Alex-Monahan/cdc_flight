from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from cdc_flight.source_health import SlotSample, SourceHealth
from cdc_flight.witness_contract import (
    STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
    WITNESS_INPUTS,
    canonical_renewal_evidence,
    canonical_service_evidence,
    evaluate_service_witness,
    renewal_witness_allows,
)

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
    own_identity_at=_UNSET,
    own_ack_lsn=100,
    durable_lsn=100,
    received_high_water=100,
    progress_stale_after=15.0,
    quiet_source_ready=False,
):
    now = time.monotonic()
    sample_at = health.last.at if health.last is not None else now
    return health.service_status(
        received_high_water,
        engine_thread_alive=engine_thread_alive,
        own_progress_at=sample_at if own_progress_at is _UNSET else own_progress_at,
        own_ack_at=sample_at if own_ack_at is _UNSET else own_ack_at,
        own_ack_lsn=own_ack_lsn,
        durable_lsn=durable_lsn,
        progress_stale_after=progress_stale_after,
        quiet_source_ready=quiet_source_ready,
        own_identity_at=(
            sample_at if own_identity_at is _UNSET else own_identity_at
        ),
    )


def _active_health(*, lag_bytes=0, confirmed_pos=100):
    backend_start = datetime(2026, 1, 1, tzinfo=UTC)
    health = SourceHealth(
        dsn="postgresql://unused",
        slot_name="slot",
        expected_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
    )
    first_at = time.monotonic()
    for sample_at in (first_at, time.monotonic()):
        health._ingest(
            SlotSample(
                at=sample_at,
                exists=True,
                active=True,
                active_pid=3210,
                activity_pid=3210,
                activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
                activity_backend_type="walsender",
                activity_backend_start=backend_start,
                replication_pid=3210,
                replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
                confirmed_pos=confirmed_pos,
                lag_bytes=lag_bytes,
            )
        )
    return health


def test_service_witness_rejects_an_empty_configured_publication():
    health = SourceHealth(
        dsn="postgresql://unused",
        slot_name="slot",
        expected_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
        publication_name="empty_publication",
    )
    backend_start = datetime(2026, 1, 1, tzinfo=UTC)
    now = time.monotonic()
    for sample_at in (now - 0.1, now):
        health._ingest(
            SlotSample(
                at=sample_at,
                exists=True,
                active=True,
                active_pid=3210,
                activity_pid=3210,
                activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
                activity_backend_type="walsender",
                activity_backend_start=backend_start,
                replication_pid=3210,
                replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
                confirmed_pos=100,
                lag_bytes=0,
                publication_has_tables=False,
            )
        )

    assert _service_status(health) == "unproven"
    assert health.summary()["source_publication_has_tables"] is False


def test_service_witness_rejects_membership_without_configured_route():
    """Some published table membership cannot prove this connector can deliver."""
    health = SourceHealth(
        dsn="postgresql://unused",
        slot_name="slot",
        expected_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
        publication_name="cdc_flight_pub",
        capture_tables=("app.orders",),
    )
    backend_start = datetime(2026, 1, 1, tzinfo=UTC)
    now = time.monotonic()
    health._ingest(
        SlotSample(
            at=now - 3.0,
            exists=True,
            active=True,
            active_pid=3210,
            activity_pid=3210,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=backend_start,
            replication_pid=3210,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=100,
            lag_bytes=0,
            publication_has_tables=True,
            publication_has_configured_tables=False,
        )
    )
    _service_status(health)
    health._ingest(
        SlotSample(
            at=now,
            exists=True,
            active=True,
            active_pid=3210,
            activity_pid=3210,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=backend_start,
            replication_pid=3210,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=100,
            lag_bytes=0,
            publication_has_tables=True,
            publication_has_configured_tables=False,
        )
    )

    assert _service_status(health) == "unproven"
    assert health.dark_for >= 2.5
    summary = health.summary()
    assert summary["source_publication_has_tables"] is True
    assert summary["source_publication_has_configured_tables"] is False


def test_service_witness_accepts_a_completed_caught_up_quiet_route_without_data_ack():
    """An empty but valid configured source is quiet, not inert or stalled."""
    health = _active_health(lag_bytes=3_315_744)
    assert _service_status(
        health,
        own_progress_at=None,
        own_ack_at=None,
        own_ack_lsn=None,
        durable_lsn=100,
        quiet_source_ready=True,
        own_identity_at=health.last.at,
    ) == "connected_quiet"


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
        own_ack_at=health.last.at,
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
        own_ack_at=health.last.at,
    ) == "engine_thread_dead"
    assert health.summary()["service_engine_thread_dead"] is True


def test_service_witness_requires_our_ack_position_and_durable_resume_point():
    health = _active_health(lag_bytes=0)
    now = time.monotonic()
    assert _service_status(
        health,
        own_progress_at=now,
        own_ack_at=health.last.at,
        own_ack_lsn=None,
        durable_lsn=100,
    ) == "unproven"

    ahead = _active_health(lag_bytes=0, confirmed_pos=101)
    assert _service_status(
        ahead,
        own_progress_at=now,
        own_ack_at=ahead.last.at,
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
            activity_pid=3211,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            replication_pid=3211,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=200,
            lag_bytes=1_000,
        )
    )
    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3211,
            activity_pid=3211,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            replication_pid=3211,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=200,
            lag_bytes=1_000,
        )
    )
    now = time.monotonic()
    assert _service_status(
        health,
        own_progress_at=now,
        own_ack_at=health.last.at,
        own_ack_lsn=200,
        durable_lsn=200,
        received_high_water=200,
    ) == "connected_quiet"
    assert health.dark_for < 1


def test_service_witness_rejects_a_new_stock_pid_until_its_ack_certifies_it():
    health = _active_health(lag_bytes=0)
    first_ack = health.last.at
    assert _service_status(
        health,
        own_progress_at=first_ack,
        own_ack_at=first_ack,
    ) == "connected_quiet"

    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3211,
            activity_pid=3211,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            replication_pid=3211,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    assert _service_status(
        health,
        own_progress_at=first_ack,
        own_ack_at=first_ack,
    ) == "foreign_walsender"
    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3211,
            activity_pid=3211,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            replication_pid=3211,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    assert _service_status(
        health,
        own_progress_at=health.last.at,
        own_ack_at=health.last.at,
    ) == "connected_quiet"


def test_service_witness_rejects_a_new_stock_backend_after_a_fresh_ack():
    """A generic stock app name cannot certify a backend seen only post-ack."""
    health = _active_health(lag_bytes=0)
    first_ack = health.last.at
    assert _service_status(
        health,
        own_progress_at=first_ack,
        own_ack_at=first_ack,
    ) == "connected_quiet"

    fresh_ack = time.monotonic()
    health._ingest(
        SlotSample(
            at=time.monotonic(),
            exists=True,
            active=True,
            active_pid=3211,
            activity_pid=3211,
            activity_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            activity_backend_type="walsender",
            activity_backend_start=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            replication_pid=3211,
            replication_application_name=STOCK_DEBEZIUM_REPLICATION_APPLICATION_NAME,
            confirmed_pos=100,
            lag_bytes=0,
        )
    )
    assert _service_status(
        health,
        own_progress_at=fresh_ack,
        own_ack_at=fresh_ack,
    ) == "foreign_walsender"


@pytest.mark.parametrize(
    "spec",
    WITNESS_INPUTS,
    ids=lambda spec: spec.key.value,
)
def test_service_witness_mutation_guards_cover_each_required_input(spec):
    """The production witness registry is also the complete mutation collection.

    There is no separately maintained test-side list.  Each registered input
    supplies its own negative cell, and the pure production folds are evaluated
    against that cell.  A new guard without a negative case fails registry import;
    a guard omitted from the production fold fails this collection's expectation.
    """
    if spec.layer == "service":
        canonical = canonical_service_evidence()
        assert spec.service_guard is not None
        # The positive half catches a guard replaced by an unconditional
        # failure; the registered negative cell catches a guard removed or a
        # derived-input calculation disabled.  Both halves come from the same
        # production registry, so neither is a second hand-maintained list.
        assert spec.service_guard(canonical) is True
        mutated = spec.negative_case(canonical)
        assert spec.service_guard(mutated) is False
        assert evaluate_service_witness(mutated) == spec.expected
    else:
        canonical = canonical_renewal_evidence()
        assert spec.renewal_guard is not None
        assert spec.renewal_guard(canonical) is True
        mutated = spec.negative_case(canonical)
        assert spec.renewal_guard(mutated) is False
        assert renewal_witness_allows(mutated) is spec.expected
