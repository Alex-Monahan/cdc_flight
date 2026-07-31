"""A subprocess that runs one step of the acquisition recovery and is allowed to DIE.

Rubric 1.7's closure needs the recovery boundaries cut by a **hard process death**, not
by a Python exception: `os._exit` runs no `except`, no `finally`, no `atexit` hook, and
the resume ladder's whole claim is that durable state alone is enough. A `:raise` action
unwinds the caller's `try`, closes the destination connection and lets the interpreter
tidy up — none of which a `SIGKILL` does (Codex r1 MAJOR-6).

`os._exit` inside pytest would take the test runner with it, so the cut happens here, in
a child process, against the same DuckDB file and the same offsets file the parent owns.
It costs milliseconds: no JVM, no Postgres, and the slot drop is a JSON file.

    python tests/recovery_crash_driver.py <duckdb> <offsets> <slots.json> begin|resume

with `CDC_FAULT_INJECT` and `CDC_STATE_DIR` set by the caller. Exits 0 when the step
completed, or with the fault's exit code when the anchor fired.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

PIPELINE = "recovery_anchors"
NAMESPACE = "recovery_anchors_ns"
TABLES = [
    ("app", "customers", "cdcflight_app_customers"),
    ("app", "orders", "cdcflight_app_orders"),
]


def main(argv: list[str]) -> int:
    duckdb_path, offsets_path, slots_path, step = argv[1:5]
    from cdc_flight import recovery as recovery_mod

    slots_file = Path(slots_path)

    def drop_slot(dsn: str, slot_name: str) -> str:
        slots = set(json.loads(slots_file.read_text()))
        if slot_name in slots:
            slots.discard(slot_name)
            slots_file.write_text(json.dumps(sorted(slots)))
            return "dropped"
        return "absent"

    con = duckdb.connect(duckdb_path)
    try:
        if step == "begin":
            recovery_mod.begin(
                con,
                pipeline=PIPELINE,
                namespace=NAMESPACE,
                decision="slot_ahead_of_destination",
                message="the slot is ahead of the destination",
                slot_name="cdc_slot",
                offset_path=Path(offsets_path),
                captured_tables=TABLES,
                forget_catalog=False,
            )
        else:
            record = recovery_mod.read(con, pipeline=PIPELINE, namespace=NAMESPACE)
            assert record is not None, "no journal to resume"
            recovery_mod.resume(
                con,
                pipeline=PIPELINE,
                namespace=NAMESPACE,
                record=record,
                dsn="postgresql://unused",
                drop_slot=drop_slot,
            )
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
