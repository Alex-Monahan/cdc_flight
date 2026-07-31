"""Probe: new table discovery, DROP TABLE, and partition detach/drop.

Rubric items answered: 2.3 (new tables/schemas), 1.5 (drop), 7.3 (partitions).
"""

from __future__ import annotations

from _common import Probe, reseed, sql, try_sql


def main() -> None:
    p = Probe("p03_table_lifecycle")
    reseed()

    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)

    # -- A. brand-new table, NOT in the publication nor the include list ----
    p.findings["ddl_new_table"] = try_sql(
        [
            "CREATE TABLE app.newcomer (id int PRIMARY KEY, v text)",
            "INSERT INTO app.newcomer VALUES (1, 'before publication'), (2, 'before publication')",
        ]
    )
    p.findings["runA_new_table_unpublished"] = p.run_pipeline(max_seconds=60, idle_seconds=6)
    p.findings["tablesA"] = p.tables()

    # -- B. added to the publication, but the include list is still static --
    sql(
        [
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.newcomer",
            "INSERT INTO app.newcomer VALUES (3, 'after publication')",
        ]
    )
    p.findings["runB_published_not_included"] = p.run_pipeline(max_seconds=60, idle_seconds=6)
    p.findings["tablesB"] = p.tables()

    # -- C. include list widened (config change + restart), no re-snapshot --
    sql("INSERT INTO app.newcomer VALUES (4, 'after include list')")
    p.findings["runC_included"] = p.run_pipeline(
        max_seconds=60,
        idle_seconds=6,
        extra_env={
            "CDC_TABLES": "customers,orders,sensor_readings,documents,wide_types,audit_log,newcomer"
        },
    )
    p.findings["tablesC"] = p.tables()
    if "cdcflight_app_newcomer" in p.tables():
        p.findings["newcomer_rows"] = p.rows(
            "SELECT id, v, dbz_op FROM cdc_raw.cdcflight_app_newcomer ORDER BY id"
        )

    # -- D. partition detach + drop ----------------------------------------
    p.findings["ddl_partitions"] = try_sql(
        [
            "INSERT INTO app.audit_log (occurred_at, actor, action) "
            "VALUES ('2026-06-15', 'probe', 'pre-detach')",
            "ALTER TABLE app.audit_log DETACH PARTITION app.audit_log_2026_06",
            "INSERT INTO app.audit_log_2026_06 (id, occurred_at, actor, action) "
            "VALUES (999001, '2026-06-16', 'probe', 'post-detach')",
            "DROP TABLE app.audit_log_2026_08",
            "INSERT INTO app.audit_log (occurred_at, actor, action) "
            "VALUES ('2026-07-15', 'probe', 'after-partition-drop')",
        ]
    )
    p.findings["runD_partitions"] = p.run_pipeline(
        max_seconds=60,
        idle_seconds=6,
        expect_success=False,
        extra_env={
            "CDC_TABLES": "customers,orders,sensor_readings,documents,wide_types,audit_log,newcomer"
        },
    )
    p.findings["audit_rows"] = p.rows(
        "SELECT action, dbz_op, count(*) FROM cdc_raw.cdcflight_app_audit_log "
        "GROUP BY 1,2 ORDER BY 1,2"
    )

    # -- E. DROP a replicated table ----------------------------------------
    p.findings["ddl_drop_table"] = try_sql(
        [
            "DROP TABLE app.documents",
            "INSERT INTO app.customers (name, email) VALUES ('After Drop', 'afterdrop@example.com')",
        ]
    )
    p.findings["runE_drop_table"] = p.run_pipeline(
        max_seconds=60,
        idle_seconds=6,
        expect_success=False,
        extra_env={
            "CDC_TABLES": "customers,orders,sensor_readings,documents,wide_types,audit_log,newcomer"
        },
    )
    p.findings["documents_rows_after_drop"] = p.rows(
        "SELECT dbz_op, count(*) FROM cdc_raw.cdcflight_app_documents GROUP BY 1"
    )
    p.findings["customers_after_drop"] = p.rows(
        "SELECT count(*) FROM cdc_raw.cdcflight_app_customers WHERE name = 'After Drop'"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
