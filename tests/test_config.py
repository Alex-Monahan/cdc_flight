"""Fast unit tests - no Postgres, no JVM. These keep the suite honest about the
pieces that are pure logic."""

from __future__ import annotations

from cdc_flight.config import ReplicationConfig, SourceConfig, applier_settings
from cdc_flight.debezium_props import build_properties
from cdc_flight.naming import destination_table, normalize, shadow_table


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
    # Deprecated Debezium 1.x/2.x spellings from the blog must not come back.
    for removed in ("table.whitelist", "schema.whitelist", "database.whitelist"):
        assert removed not in props
    assert "transforms.unwrap.delete.handling.mode" not in props


def test_properties_configure_the_full_envelope(tmp_path):
    """ADR 0001 D5. Each of these is load-bearing for a specific rubric item."""
    props = build_properties(SourceConfig(), ReplicationConfig(state_dir=tmp_path))
    # No SMT: the `before` image, the truncate/message ops and the transaction
    # block must all survive to Python.
    assert "transforms" not in props
    assert not any(k.startswith("transforms.") for k in props)
    # ADR 0001 §3.2: without this there is no END marker, so no commit group can
    # be proven to contain whole Postgres transactions.
    assert props["provide.transaction.metadata"] == "true"
    assert props["tombstones.on.delete"] == "false"
    assert props["replace.null.with.default"] == "false"
    # ADR 0001 §4.2 / Opus B2: a flush that did not happen must be observable.
    assert props["offset.flush.interval.ms"] == "0"


def test_snapshot_mode_override(tmp_path):
    props = build_properties(
        SourceConfig(), ReplicationConfig(state_dir=tmp_path), snapshot_mode="never"
    )
    assert props["snapshot.mode"] == "never"


def test_destination_table_name_includes_the_source_schema():
    # Two same-named tables in different schemas must not collide.
    assert destination_table("cdcflight", "app", "customers") == "cdcflight_app_customers"
    assert destination_table("cdcflight", "billing", "customers") == "cdcflight_billing_customers"


def test_identifier_normalisation_is_dlts():
    """ADR 0001 D10: dlt stays as a *library*, and this is why - the names have to
    keep matching the ones every existing probe and RUBRIC_STATUS entry uses."""
    assert normalize("Col Name") == "col_name"
    assert normalize("lifetime_value") == "lifetime_value"
    assert shadow_table("cdcflight_app_customers") == "cdcflight_app_customers__cdcf_tmp"


def test_applier_defaults_follow_the_adr():
    settings = applier_settings()
    assert settings["commit_max_age"] == 5.0
    assert settings["commit_max_events"] == 200_000
    assert settings["repair_offset_file"] is True
