"""Fast unit tests - no Postgres, no JVM. These keep the suite honest about the
pieces that are pure logic."""

from __future__ import annotations

from cdc_flight.config import ReplicationConfig, SourceConfig
from cdc_flight.debezium_props import METADATA_PREFIX, build_properties
from cdc_flight.handler import resolve_table_name


def test_source_dsn_and_table_list():
    src = SourceConfig()
    assert src.dsn.startswith("postgresql://")
    assert f":{src.port}/" in src.dsn
    assert "app.customers" in src.tables
    assert all("." in t for t in src.tables)


def test_properties_use_pgoutput_and_a_version_controlled_publication(tmp_path):
    props = build_properties(SourceConfig(), ReplicationConfig(state_dir=tmp_path))
    # Rubric 7.1: no Postgres extension - pgoutput only.
    assert props["plugin.name"] == "pgoutput"
    assert props["publication.autocreate.mode"] == "disabled"
    assert props["connector.class"].endswith("PostgresConnector")
    assert props["offset.storage.file.filename"].startswith(str(tmp_path))
    assert props["transforms.unwrap.add.fields.prefix"] == METADATA_PREFIX
    # Deprecated Debezium 1.x/2.x spellings from the blog must not come back.
    for removed in ("table.whitelist", "schema.whitelist", "database.whitelist"):
        assert removed not in props
    assert "transforms.unwrap.delete.handling.mode" not in props


def test_snapshot_mode_override(tmp_path):
    props = build_properties(
        SourceConfig(), ReplicationConfig(state_dir=tmp_path), snapshot_mode="never"
    )
    assert props["snapshot.mode"] == "never"


def test_table_name_prefers_payload_schema_over_topic():
    # Debezium 3.6 omits the schema from the topic; the payload still carries it.
    payload = {f"{METADATA_PREFIX}schema": "app", f"{METADATA_PREFIX}table": "customers"}
    assert resolve_table_name("cdcflight.customers", payload) == "cdcflight_app_customers"
    # Two same-named tables in different schemas must not collide.
    other = {f"{METADATA_PREFIX}schema": "billing", f"{METADATA_PREFIX}table": "customers"}
    assert resolve_table_name("cdcflight.customers", other) == "cdcflight_billing_customers"


def test_table_name_falls_back_to_topic():
    assert resolve_table_name("cdcflight.app.customers", None) == "cdcflight_app_customers"
    assert resolve_table_name("cdcflight.customers", {}) == "cdcflight_customers"
