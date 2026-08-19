from __future__ import annotations

import time


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
