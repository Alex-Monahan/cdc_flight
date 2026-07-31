"""Probe: primary-key UPDATE, TRUNCATE, and pg_logical_emit_message.

Rubric items answered: 1.4 (PK update), 1.5 (truncate/drop), 7.4 (logical messages).

Sequence
  1. reseed + snapshot run (baseline destination state)
  2. one transaction that:
       - updates a primary key (customers.id 1 -> 9001)
       - TRUNCATEs app.orders
       - emits a transactional and a non-transactional logical message
  3. stream run
  4. report exactly what landed
"""

from __future__ import annotations

from _common import Probe, query, sql


def main() -> None:
    p = Probe("p01_dml_edge_cases")
    from _common import reseed

    reseed()

    snap = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    p.findings["snapshot_run"] = snap
    p.findings["customers_after_snapshot"] = p.rows(
        'SELECT id, name, dbz_op FROM cdc_raw.cdcflight_app_customers ORDER BY id'
    )

    # --- the interesting transaction ---------------------------------------
    sql(
        [
            "SELECT pg_logical_emit_message(true, 'cdc_flight', 'transactional-hello')",
            "SELECT pg_logical_emit_message(false, 'cdc_flight', 'nontransactional-hello')",
            # PK update: 1 -> 9001. orders.customer_id FKs to it with ON DELETE CASCADE,
            # so update the child first to keep Postgres happy.
            "ALTER TABLE app.orders DROP CONSTRAINT orders_customer_id_fkey",
            "UPDATE app.customers SET id = 9001 WHERE id = 1",
            "TRUNCATE TABLE app.orders",
        ]
    )
    p.findings["pg_customers_ids"] = query("SELECT id FROM app.customers ORDER BY id")
    p.findings["pg_orders_count"] = query("SELECT count(*) FROM app.orders")[0][0]

    stream = p.run_pipeline(max_seconds=120, idle_seconds=8)
    p.findings["stream_run"] = stream

    p.findings["destination_tables"] = p.tables()
    p.findings["customers_after_stream"] = p.rows(
        "SELECT id, name, dbz_op, deleted FROM cdc_raw.cdcflight_app_customers ORDER BY id, dbz_op"
    )
    p.findings["customers_op_counts"] = p.rows(
        "SELECT dbz_op, count(*) FROM cdc_raw.cdcflight_app_customers GROUP BY 1 ORDER BY 1"
    )
    p.findings["orders_op_counts"] = p.rows(
        "SELECT dbz_op, count(*) FROM cdc_raw.cdcflight_app_orders GROUP BY 1 ORDER BY 1"
    )
    # Did a truncate event land in ANY form?
    p.findings["any_op_t"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_orders WHERE dbz_op = 't'"
    )[0][0]
    # Did the logical messages land anywhere?
    p.findings["message_tables"] = [t for t in p.tables() if "message" in t.lower()]

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
