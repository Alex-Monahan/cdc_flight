"""Probe: replication slot advanced behind the pipeline's back.

Rubric item answered: 1.8 (externally-advanced slot must be detected and trigger
a backfill).

Sequence
  1. snapshot run (slot + offsets established)
  2. generate a wave of changes -> events sit unread in the WAL
  3. `pg_replication_slot_advance(slot, pg_current_wal_lsn())` -- exactly what a
     stray operator / another consumer / a `pg_recvlogical --start` would do
  4. run the pipeline again and see whether the skipped changes are noticed
"""

from __future__ import annotations

import json
import subprocess

from _common import PROJECT_DIR, Probe, executable, query, reseed


def main() -> None:
    p = Probe("p04_offset_mismatch")
    reseed()

    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    p.findings["slot_after_snapshot"] = None

    changes = subprocess.run(
        [executable("cdc-datagen"), "changes", "--scale", "1", "--seed", "11"],
        capture_output=True,
        text=True,
        env=p.env,
        cwd=PROJECT_DIR,
        check=True,
        timeout=120,
    )
    p.findings["changes_generated"] = json.loads(changes.stdout)

    before = query(
        "SELECT restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots "
        "WHERE slot_name = %s",
        (p.slot,),
    )
    p.findings["slot_before_advance"] = before

    advanced = query(
        "SELECT end_lsn::text FROM pg_replication_slot_advance(%s, pg_current_wal_lsn())",
        (p.slot,),
    )
    p.findings["advance_result"] = advanced

    after = query(
        "SELECT restart_lsn::text, confirmed_flush_lsn::text FROM pg_replication_slots "
        "WHERE slot_name = %s",
        (p.slot,),
    )
    p.findings["slot_after_advance"] = after

    run = p.run_pipeline(max_seconds=60, idle_seconds=6, expect_success=False)
    p.findings["run_after_advance"] = run
    p.findings["customers_op_counts"] = p.rows(
        "SELECT dbz_op, count(*) FROM cdc_raw.cdcflight_app_customers GROUP BY 1 ORDER BY 1"
    )
    p.findings["pg_customers_count"] = query("SELECT count(*) FROM app.customers")[0][0]
    p.findings["verdict_note"] = (
        "If run_after_advance.records is 0 (or << changes_generated.total) and the "
        "process still exits 0, the pipeline silently lost data."
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
