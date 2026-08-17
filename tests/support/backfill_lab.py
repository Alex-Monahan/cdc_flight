"""Small test-only adapters for the rubric 3 test-first suite.

The production module is intentionally loaded lazily.  Phase 1 must fail because
the behaviour is absent, not because pytest cannot import a module that the
implementation is expected to add.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
from dataclasses import dataclass


def require_backfill():
    """Load the production backfill surface, with an explanatory Phase-1 failure."""
    spec = importlib.util.find_spec("cdc_flight.backfill")
    assert spec is not None, (
        "BACKFILL_RUN behaviour is absent: cdc_flight.backfill has not been built; "
        "this is the feature assertion, not an import/fixture failure"
    )
    return importlib.import_module("cdc_flight.backfill")


@dataclass
class RawRecord:
    """Minimal stock-Debezium-shaped record used by decoder unit tests."""

    topic: str
    payload: dict
    key_payload: dict | None = None

    def destination(self):
        return self.topic

    def value(self):
        return json.dumps(self.payload)

    def key(self):
        return None if self.key_payload is None else json.dumps(self.key_payload)


def incremental_record(
    table: str = "customers",
    *,
    key: int = 1,
    value: str = "v-1",
    signal_id: str = "signal-1",
    snapshot: str = "incremental",
):
    return RawRecord(
        topic=f"cdcflight.app.{table}",
        key_payload={"id": key},
        payload={
            "before": None,
            "after": {"id": key, "value": value},
            "source": {
                "schema": "app",
                "table": table,
                "snapshot": snapshot,
                "lsn": None,
                "ts_ms": 1_000,
            },
            "op": "r",
            "transaction": None,
            "cdc_flight_signal_id": signal_id,
        },
    )


def notification(
    observation: str,
    *,
    table: str = "app.customers",
    status: str = "SUCCEEDED",
    rows: int = 1,
    chunk: int | None = None,
    signal_id: str = "signal-1",
):
    additional = {
        "scanned_collection": table,
        "status": status,
        "total_rows_scanned": rows,
        "signal_id": signal_id,
    }
    if chunk is not None:
        additional["current_collection_in_progress"] = table
        additional["last_processed_key"] = str(chunk)
        additional["chunk_id"] = str(chunk)
    return RawRecord(
        topic="cdcflight.cdc_flight_snapshot_notifications",
        payload={
            "aggregate_type": "Incremental Snapshot",
            "type": observation,
            "additional_data": additional,
        },
    )


def image_set(rows):
    """Identity/value oracle used by the tests; counts are deliberately insufficient."""
    rows = list(rows)
    return {(row[0], row[1]) for row in rows}
