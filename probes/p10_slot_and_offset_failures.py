"""Probe: replication slot dropped underneath us, and a corrupt offset store.

Rubric items answered: 4.1 (recover from a failed/lost slot), 4.3 (recover from
a problematic WAL/offset state without hanging).

Case A: slot dropped externally while the Debezium offset file still points at
        an old LSN -> is the loss detected, or is it silently papered over?
Case B: the offset file is overwritten with a far-future LSN -> does the
        connector hang, fail, or backfill?
"""

from __future__ import annotations

import shutil
from pathlib import Path

from _common import Probe, drop_slot, query, reseed, sql


def main() -> None:
    p = Probe("p10_slot_and_offset_failures")
    reseed()
    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    state_dir = Path(p.env["CDC_STATE_DIR"])
    offsets = state_dir / "offsets.dat"
    p.findings["offsets_exists"] = offsets.exists()
    backup = state_dir / "offsets.backup"
    if offsets.exists():
        shutil.copy(offsets, backup)

    # ---- Case A: slot dropped externally ---------------------------------
    sql(
        "INSERT INTO app.customers (name, email) VALUES "
        "('lost slot 1', 'lostslot1@example.com'), ('lost slot 2', 'lostslot2@example.com')"
    )
    drop_slot(p.slot)
    p.findings["slot_present_after_drop"] = query(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (p.slot,)
    )[0][0]
    sql("INSERT INTO app.customers (name, email) VALUES ('after drop', 'afterdrop@example.com')")

    runA = p.run_pipeline(max_seconds=90, idle_seconds=8, expect_success=False)
    p.findings["runA_after_slot_drop"] = runA
    p.findings["runA_rows"] = p.rows(
        "SELECT name, dbz_op FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'lost slot%' OR name = 'after drop' ORDER BY name"
    )
    p.findings["runA_total_rows"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers"
    )
    p.findings["runA_note"] = (
        "'lost slot 1/2' missing => silent data loss when the slot disappears; "
        "everything duplicated => an unannounced re-snapshot"
    )

    # ---- Case B: corrupt / far-future offset ------------------------------
    sql("INSERT INTO app.customers (name, email) VALUES ('corrupt offset', 'corrupt@example.com')")
    raw = offsets.read_bytes() if offsets.exists() else b""
    p.findings["offset_file_head"] = raw[:400].decode("utf-8", "replace")
    if offsets.exists():
        text = raw.decode("utf-8", "replace")
        # bump every lsn-looking number by 10^12 -> far past the end of the WAL
        import re

        def bump(m):
            return m.group(1) + str(int(m.group(2)) + 10**12)

        patched = re.sub(r'("lsn":\s*)(\d+)', bump, text)
        p.findings["offset_patched"] = patched != text
        offsets.write_bytes(patched.encode())

    runB = p.run_pipeline(max_seconds=90, idle_seconds=8, timeout=200, expect_success=False)
    p.findings["runB_corrupt_offset"] = runB
    p.findings["runB_rows"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name = 'corrupt offset'"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
