"""Run the production CLI with the test-only real crash-matrix handler installed."""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
TESTS_DIR = PROJECT_DIR / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from cdc_flight import faults
from support.crash_matrix_runtime import install_matrix_crash_handler

install_matrix_crash_handler()
try:
    # Refuse unsafe matrix paths before the production CLI can create its ordinary
    # last_run/offset files through a symlinked CDC_STATE_DIR.
    faults.validate_matrix_state_directory()
except faults.FaultSpecError as exc:
    print(f"invalid crash-matrix state path: {exc}", file=sys.stderr)
    raise SystemExit(2) from exc

if __name__ == "__main__":
    if "--service" in sys.argv:
        sys.argv.remove("--service")
        from cdc_flight.service import main as service_main

        raise SystemExit(service_main())
    if os.environ.get("CDC_BACKFILL_LAB_CHILD") == "1":
        # The resumability proof reuses this source-tree child and its registered
        # hard-exit handler.  The branch is intentionally unavailable from the
        # installed production package because the handler lives under tests/.
        from cdc_flight.backfill import run_lab_child

        raise SystemExit(run_lab_child())
    if os.environ.get("CDC_BACKFILL_SIGNAL_CHILD") == "1":
        # This remains a test-tree-only driver.  The coordinator and signal writer
        # are production objects, but the normal pipeline child below is still the
        # process that consumes the stock Debezium signal and proves the destination
        # result.  Keeping admission in a separate real process lets the live matrix
        # cut the transaction-before-signal edges without mocking either store.
        import duckdb

        from cdc_flight import destination
        from cdc_flight.backfill import BackfillCoordinator, IncrementalSignal, StockSignalWriter

        tables = tuple(
            table for table in os.environ.get("CDC_BACKFILL_TABLES", "").split(",")
            if table
        )
        signal_id = os.environ["CDC_BACKFILL_SIGNAL_ID"]
        request_id = os.environ.get("CDC_BACKFILL_REQUEST_ID") or signal_id
        control_schema = os.environ.get("CDC_CONTROL_SCHEMA", "_cdc_flight")
        dataset = os.environ.get("CDC_DATASET", "cdc_raw")
        pipeline = os.environ["CDC_PIPELINE_NAME"]
        topic_prefix = os.environ.get("CDC_TOPIC_PREFIX", "cdcflight")
        with duckdb.connect(os.environ["CDC_DUCKDB_PATH"]) as con:
            destination.ensure_control_schema(con, control_schema)
            destination.ensure_dataset(con, dataset)
            coordinator = BackfillCoordinator(
                con,
                pipeline=pipeline,
                control_schema=control_schema,
                topic_prefix=topic_prefix,
            )
            signal = IncrementalSignal(signal_id, tables)
            coordinator.request_tables(
                tables,
                request_id=request_id,
                signal_id=signal_id,
            )
        if os.environ.get("CDC_BACKFILL_INSERT_SIGNAL") == "1":
            StockSignalWriter(
                os.environ["CDC_BACKFILL_SOURCE_DSN"],
                data_collection=os.environ.get(
                    "CDC_SIGNAL_DATA_COLLECTION", "app.cdc_flight_signal"
                ),
            ).insert(signal)
        raise SystemExit(0)
    from cdc_flight.pipeline import main

    raise SystemExit(main())
