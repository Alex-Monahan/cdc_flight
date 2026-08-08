"""Existing rows in newly created tables and newly created schemas are snapshotted."""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


@pytest.fixture(scope="module")
def discovery_scenario(sandbox):
    box = sandbox
    box.reseed()
    # Deliberately do not add the tables below to this setting.  Discovery must be
    # driven by the publication/catalog, not by a process restart or config edit.
    box.env["CDC_TABLES"] = "customers"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    proc = None
    try:
        box.run(reset_state=True, max_seconds=180)
        # Keep one bounded pipeline process alive while the source DDL happens. The
        # watcher must hand the running engine through the existing re-snapshot path;
        # a second `box.run()` would only prove restart-time discovery.
        proc = box.spawn(max_seconds=300, idle_seconds=60)
        time.sleep(3)
        box.sql(
            [
                "CREATE TABLE app.discovered_rows (id bigint PRIMARY KEY, value text)",
                "INSERT INTO app.discovered_rows VALUES (1, 'before-stream'), (2, 'also-before')",
                "CREATE SCHEMA discovered_schema",
                "CREATE TABLE discovered_schema.rows (id bigint PRIMARY KEY, value text)",
                "INSERT INTO discovered_schema.rows VALUES (10, 'schema-before')",
            ],
            one_transaction=True,
        )
        # DuckDB holds the writer connection for the lifetime of the pipeline process,
        # so a concurrent read-only connection cannot observe the committed image while
        # the live engine is still running. The throwaway `_rs` slot is the source-side
        # completion fence instead: it is created for this handoff and retired only
        # after both destination images have swapped in. Only then write these rows;
        # they must arrive through the resumed main stream, not hide in the image.
        deadline = time.monotonic() + 90
        resnapshot_slot = f"{box.slot}_rs"
        saw_resnapshot_slot = False
        while time.monotonic() < deadline and proc.poll() is None:
            active_resnapshot = box.pg_query(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (resnapshot_slot,),
            )
            saw_resnapshot_slot |= bool(active_resnapshot)
            if saw_resnapshot_slot and not active_resnapshot:
                break
            time.sleep(0.5)
        assert saw_resnapshot_slot, "live discovery never started its throwaway snapshot"
        assert not box.pg_query(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
            (resnapshot_slot,),
        ), "live discovery snapshot did not retire before the CDC check"
        assert proc.poll() is None, "the process exited before the post-snapshot CDC check"
        box.sql(
            [
                "INSERT INTO app.discovered_rows VALUES (3, 'after-discovery')",
                "INSERT INTO discovered_schema.rows VALUES (11, 'schema-after-discovery')",
            ],
            one_transaction=True,
        )
        returncode = proc.wait(timeout=180)
        streamed_after_discovery = (
            box.duck_query(
                f"SELECT id, value FROM {box.table('cdcflight_app_discovered_rows')} "
                "WHERE id = 3"
            )
            == [(3, "after-discovery")]
            and box.duck_query(
                f"SELECT id, value FROM {box.table('cdcflight_discovered_schema_rows')} "
                "WHERE id = 11"
            )
            == [(11, "schema-after-discovery")]
        )
        assert streamed_after_discovery, {
            "returncode": returncode,
            "summary": box.last_summary(),
            "app_rows": box.duck_query(
                f"SELECT id, value FROM {box.table('cdcflight_app_discovered_rows')} "
                "ORDER BY id"
            ),
            "schema_rows": box.duck_query(
                f"SELECT id, value FROM {box.table('cdcflight_discovered_schema_rows')} "
                "ORDER BY id"
            ),
        }
        discovered = box.last_summary()
        assert returncode == 0, discovered
        yield {
            "box": box,
            "discovered": discovered,
            "streamed_after_discovery": streamed_after_discovery,
        }
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=30)
        box.sql(
            [
                "DROP TABLE IF EXISTS app.discovered_rows",
                "DROP SCHEMA IF EXISTS discovered_schema CASCADE",
            ]
        )
        box.reseed()


def test_discovery_run_completed_without_config_or_restart(discovery_scenario):
    summary = discovery_scenario["discovered"]
    assert summary["ok"] is True
    assert summary["live_discovery_handoffs"] >= 1
    assert set(summary["live_discovered_relations"]) >= {
        "app.discovered_rows",
        "discovered_schema.rows",
    }
    assert discovery_scenario["streamed_after_discovery"] is True


def test_new_table_existing_rows_were_snapshotted(discovery_scenario):
    box = discovery_scenario["box"]
    assert box.duck_query(
        f"SELECT id, value FROM {box.table('cdcflight_app_discovered_rows')} ORDER BY id"
    ) == [
        (1, "before-stream"),
        (2, "also-before"),
        (3, "after-discovery"),
    ]
    assert box.pg_query(
        "SELECT count(*) FROM pg_publication_rel pr "
        "JOIN pg_class c ON c.oid = pr.prrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_publication p ON p.oid = pr.prpubid "
        "WHERE p.pubname = 'cdc_flight_pub' AND n.nspname = 'app' "
        "AND c.relname = 'discovered_rows'"
    ) == [(1,)]


def test_new_schema_existing_rows_were_snapshotted(discovery_scenario):
    box = discovery_scenario["box"]
    assert box.duck_query(
        f"SELECT id, value FROM {box.table('cdcflight_discovered_schema_rows')}"
    ) == [(10, "schema-before"), (11, "schema-after-discovery")]


def test_discovery_is_auditable(discovery_scenario):
    box = discovery_scenario["box"]
    events = box.duck_query(
        "SELECT source_schema, source_table, applied FROM _cdc_flight.table_events "
        "WHERE event = 'new' AND applied AND ((source_schema = 'app' AND source_table = "
        "'discovered_rows') OR (source_schema = 'discovered_schema' AND "
        "source_table = 'rows')) ORDER BY source_schema, source_table"
    )
    assert events == [("app", "discovered_rows", True), ("discovered_schema", "rows", True)]
