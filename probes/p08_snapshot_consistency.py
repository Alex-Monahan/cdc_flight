"""Probe: are rows changed *during* the initial snapshot captured exactly once?

Rubric items answered: 1.6 (snapshot/backfill consistent with CDC), 3.3
(existing tables keep receiving CDC during a snapshot).

Sequence
  1. seed a table big enough that the snapshot takes several seconds
  2. start a fresh pipeline run (snapshot + stream)
  3. while it runs, insert rows tagged `during-snapshot-N` and delete some
     pre-existing rows
  4. drain with a second run and check for gaps / duplicates
"""

from __future__ import annotations

import threading
import time

from _common import Probe, dsn, query, reseed, sql

PRELOAD_ROWS = 120_000
CONCURRENT_ROWS = 300


def main() -> None:
    p = Probe("p08_snapshot_consistency")
    reseed()
    sql(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        f"SELECT 'preload '||i, 'preload'||i||'@example.com', (i % 500)::numeric "
        f"FROM generate_series(1, {PRELOAD_ROWS}) i"
    )
    p.findings["preload_rows"] = query("SELECT count(*) FROM app.customers")[0][0]

    stop = threading.Event()
    inserted: list[int] = []

    def churn():
        import psycopg

        # start once the snapshot has had a moment to begin
        time.sleep(6)
        with psycopg.connect(dsn(), autocommit=True) as conn:
            for i in range(CONCURRENT_ROWS):
                if stop.is_set():
                    break
                row = conn.execute(
                    "INSERT INTO app.customers (name, email) VALUES (%s, %s) RETURNING id",
                    (f"during-snapshot-{i}", f"during{i}@example.com"),
                ).fetchone()
                inserted.append(row[0])
                time.sleep(0.03)

    th = threading.Thread(target=churn)
    th.start()
    p.findings["snapshot_run"] = p.run_pipeline(reset_state=True, max_seconds=900, idle_seconds=12, timeout=1200)
    stop.set()
    th.join()
    p.findings["concurrent_inserted"] = len(inserted)

    # drain anything that arrived after the run stopped
    p.findings["drain_run"] = p.run_pipeline(max_seconds=300, idle_seconds=10, timeout=600)

    p.findings["preload_landed"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'preload %'"
    )
    p.findings["during_landed"] = p.rows(
        "SELECT count(*), count(DISTINCT id) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'during-snapshot-%'"
    )
    p.findings["during_by_op"] = p.rows(
        "SELECT dbz_op, count(*) FROM cdc_raw.cdcflight_app_customers "
        "WHERE name LIKE 'during-snapshot-%' GROUP BY 1 ORDER BY 1"
    )
    p.findings["expected_during"] = CONCURRENT_ROWS
    p.findings["verdict_note"] = (
        "consistent == every concurrently-inserted id present at least once "
        "(no gap between snapshot and stream); exactly-once == present exactly once"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
