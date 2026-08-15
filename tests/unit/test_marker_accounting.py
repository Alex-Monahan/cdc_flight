"""Unit coverage for source-marker receipt accounting."""

from __future__ import annotations

from cdc_flight.envelope import KIND_MESSAGE, KIND_TXN_BEGIN, KIND_TXN_END, PendingRecord
from cdc_flight.marker_accounting import SourceMarkerReceiptCounter


def _counter(*prefixes: str) -> SourceMarkerReceiptCounter:
    return SourceMarkerReceiptCounter(prefixes or None)


def _record(kind: str, txn_id: str, *, prefix: str | None = None) -> PendingRecord:
    return PendingRecord(
        raw=None,
        kind=kind,
        topic="cdcflight.transaction" if kind != KIND_MESSAGE else "cdcflight.app.events",
        nbytes=1,
        txn_id=txn_id,
        message_prefix=prefix,
    )


def test_marker_receipts_count_only_delivered_marker_transaction_records():
    counter = _counter()
    received = 0

    for record in (
        _record(KIND_TXN_BEGIN, "7"),
        _record(KIND_MESSAGE, "7", prefix="cdcf_completion_watermark"),
        _record(KIND_TXN_END, "7"),
    ):
        received += counter.observe(record)

    assert received == 3


def test_marker_receipts_honor_configured_catalog_prefix():
    counter = _counter("cdcf", "catalog_marker")
    received = 0

    for record in (
        _record(KIND_TXN_BEGIN, "8"),
        _record(KIND_MESSAGE, "8", prefix="catalog_marker_catalog_fence"),
        _record(KIND_TXN_END, "8"),
    ):
        received += counter.observe(record)

    assert received == 3
