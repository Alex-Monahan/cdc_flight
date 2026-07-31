"""Probe: column add / drop / rename and column type changes.

Rubric items answered: 2.1 (add & drop), 2.2 (rename), 2.5 (type change).

Each DDL step gets its own pipeline run so we can see the destination schema
evolve step by step.
"""

from __future__ import annotations

from _common import Probe, reseed, sql


def main() -> None:
    p = Probe("p02_schema_evolution")
    reseed()

    p.findings["run0_snapshot"] = p.run_pipeline(reset_state=True, max_seconds=120, idle_seconds=6)
    p.findings["cols0"] = p.columns("cdcflight_app_customers")

    # -- A. ADD COLUMN ------------------------------------------------------
    sql(
        [
            "ALTER TABLE app.customers ADD COLUMN loyalty_tier text DEFAULT 'bronze'",
            "INSERT INTO app.customers (name, email, loyalty_tier) "
            "VALUES ('Added Col', 'addedcol@example.com', 'gold')",
            "UPDATE app.customers SET loyalty_tier = 'silver' WHERE id = 2",
        ]
    )
    p.findings["runA_add"] = p.run_pipeline(max_seconds=90, idle_seconds=6)
    p.findings["colsA"] = p.columns("cdcflight_app_customers")
    p.findings["valuesA"] = p.rows(
        "SELECT id, name, loyalty_tier, dbz_op FROM cdc_raw.cdcflight_app_customers "
        "WHERE dbz_op <> 'r' ORDER BY id"
    )

    # -- B. DROP COLUMN -----------------------------------------------------
    sql(
        [
            "ALTER TABLE app.customers DROP COLUMN is_active",
            "INSERT INTO app.customers (name, email) VALUES ('Dropped Col', 'droppedcol@example.com')",
        ]
    )
    p.findings["runB_drop"] = p.run_pipeline(max_seconds=90, idle_seconds=6)
    p.findings["colsB"] = p.columns("cdcflight_app_customers")
    p.findings["valuesB_is_active"] = p.rows(
        "SELECT dbz_op, is_active, count(*) FROM cdc_raw.cdcflight_app_customers "
        "GROUP BY 1,2 ORDER BY 1,2"
    )

    # -- C. RENAME COLUMN ---------------------------------------------------
    sql(
        [
            "ALTER TABLE app.customers RENAME COLUMN name TO full_name",
            "INSERT INTO app.customers (full_name, email) "
            "VALUES ('Renamed Col', 'renamedcol@example.com')",
        ]
    )
    p.findings["runC_rename"] = p.run_pipeline(max_seconds=90, idle_seconds=6)
    p.findings["colsC"] = p.columns("cdcflight_app_customers")
    p.findings["valuesC"] = p.rows(
        "SELECT id, name, full_name, dbz_op FROM cdc_raw.cdcflight_app_customers "
        "WHERE dbz_op = 'c' ORDER BY id"
    )

    # -- D. TYPE CHANGES ----------------------------------------------------
    # widening (integer -> bigint) and an incompatible change (smallint -> text)
    sql(
        [
            "ALTER TABLE app.wide_types ALTER COLUMN col_integer TYPE bigint",
            "ALTER TABLE app.wide_types ALTER COLUMN col_smallint TYPE text",
            "INSERT INTO app.wide_types (id, col_integer, col_smallint, col_text) "
            "VALUES (2, 9223372036854775807, 'now-a-string', 'after type change')",
        ]
    )
    p.findings["runD_typechange"] = p.run_pipeline(max_seconds=90, idle_seconds=6)
    p.findings["colsD_wide_types"] = p.columns("cdcflight_app_wide_types")
    p.findings["valuesD"] = p.rows(
        "SELECT * EXCLUDE (_dlt_load_id, _dlt_id) FROM cdc_raw.cdcflight_app_wide_types "
        "WHERE id = 2"
    )

    p.cleanup()
    p.emit()


if __name__ == "__main__":
    main()
