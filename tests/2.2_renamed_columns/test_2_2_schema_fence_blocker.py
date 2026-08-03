"""Reproduction for the per-source-unit schema-fence blocker."""

from __future__ import annotations

from applier_lab import Lab, data, end, keyed

from cdc_flight.catalog import CHANGE_SCHEMA, CatalogChange, CatalogWatcher, SourceRelation
from cdc_flight.schema_evolution import COLUMN_RENAMED, ColumnChange, SourceColumn


def test_schema_fence_is_between_source_units_not_at_group_final_lsn(tmp_path):
    """BLOCKER reproduction: the group-final-LSN implementation renames too early."""

    old_columns = (
        SourceColumn(1, "id", 20, "bigint", True),
        SourceColumn(2, "name", 25, "text", True),
    )
    new_columns = (
        SourceColumn(1, "id", 20, "bigint", True),
        SourceColumn(2, "full_name", 25, "text", True),
    )
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        include={"app.customers"},
        poll_seconds=0,
        known={
            "app.customers": SourceRelation(
                "app", "customers", 1, True, "d", old_columns
            )
        },
    )
    box = Lab(tmp_path / "epoch.duckdb", catalog=watcher)
    try:
        box.run(
            [
                keyed("seed", 1, 10, 1, "initial"),
                end("seed", 1, 11, {"app.customers": 1}),
            ]
        )
        watcher.queue(
            CatalogChange(
                kind=CHANGE_SCHEMA,
                schema="app",
                table="customers",
                detected_lsn=200,
                old_oid=1,
                new_oid=1,
                new_relation=SourceRelation(
                    "app", "customers", 1, True, "d", new_columns
                ),
                column_changes=(
                    ColumnChange(
                        COLUMN_RENAMED,
                        2,
                        "name",
                        "full_name",
                        25,
                        "text",
                        True,
                    ),
                ),
                state="marked",
            )
        )
        box.run(
            [
                keyed("old", 1, 100, 1, "before"),
                end("old", 1, 101, {"app.customers": 1}),
                data(
                    "new",
                    1,
                    300,
                    table="customers",
                    key={"id": 2},
                    after={"id": 2, "full_name": "after"},
                ),
                end("new", 1, 301, {"app.customers": 1}),
            ]
        )
        assert box.rows(box.target("customers"), "id, full_name", "id") == [
            (1, "before"),
            (2, "after"),
        ]
    finally:
        box.close()
