"""Fast unit tests - no Postgres, no JVM. These keep the suite honest about the
pieces that are pure logic."""

from __future__ import annotations

import pytest

from cdc_flight.config import (
    DestinationConfig,
    ReplicationConfig,
    SourceConfig,
    applier_settings,
)
from cdc_flight.debezium_props import (
    assert_no_internal_topic_collision,
    build_properties,
)
from cdc_flight.errors import UnsafeDebeziumProperty
from cdc_flight.naming import destination_table, normalize, shadow_table


def test_source_dsn_and_table_list():
    src = SourceConfig()
    assert src.dsn.startswith("postgresql://")
    assert src.primary_dsn == src.dsn
    assert f":{src.port}/" in src.dsn
    assert "app.customers" in src.tables
    assert all("." in t for t in src.tables)


def test_source_primary_dsn_is_an_explicit_write_route(monkeypatch):
    monkeypatch.setenv("CDC_PRIMARY_DSN", "postgresql://writer:pw@primary:15432/db")
    source = SourceConfig()
    assert source.dsn != source.primary_dsn
    assert source.primary_dsn == "postgresql://writer:pw@primary:15432/db"


def test_default_runtime_artifacts_are_disjoint_per_instance(monkeypatch):
    for name in (
        "CDC_STATE_DIR",
        "CDC_PIPELINES_DIR",
        "CDC_DUCKDB_PATH",
        "CDC_PIPELINE_NAME",
    ):
        monkeypatch.delenv(name, raising=False)

    paths = {}
    for instance in ("pg15432", "pg15436"):
        monkeypatch.setenv("CDC_TEST_INSTANCE_ID", instance)
        replication = ReplicationConfig()
        destination = DestinationConfig()
        paths[instance] = {
            replication.state_dir,
            destination.pipelines_dir,
            destination.duckdb_path,
        }
        assert instance in destination.pipeline_name

    assert paths["pg15432"].isdisjoint(paths["pg15436"])


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


def test_the_lsn_flush_mode_is_pinned_to_connector(tmp_path):
    """Invariant O depends on it, and the safe value is only a Debezium *default*.

    With `lsn.flush.mode=connector_and_driver`,
    `PostgresReplicationConnection.java:1114-1123` sets `.withAutomaticFlush(true)`
    and the shipped pgjdbc then advances the flushed LSN to the server-supplied
    `lastServerLSN` on keepalives, **never consulting the offset store**. That
    confirms WAL to Postgres outside the invariant, i.e. it is the withdrawn P2's
    shape: an argument that holds because a default happens to be safe. Pin it
    (Opus B-2).
    """
    props = build_properties(SourceConfig(), ReplicationConfig(state_dir=tmp_path))
    assert props["lsn.flush.mode"] == "connector"


def test_an_unsafe_lsn_flush_mode_is_refused(tmp_path):
    with pytest.raises(UnsafeDebeziumProperty, match=r"lsn\.flush\.mode"):
        build_properties(
            SourceConfig(),
            ReplicationConfig(state_dir=tmp_path),
            overrides={"lsn.flush.mode": "connector_and_driver"},
        )


def test_an_override_that_would_break_invariant_o_is_refused(tmp_path):
    """The other two properties the whole design rests on."""
    for key, value in (
        ("provide.transaction.metadata", "false"),
        ("offset.flush.interval.ms", "60000"),
    ):
        with pytest.raises(UnsafeDebeziumProperty, match=key.replace(".", r"\.")):
            build_properties(
                SourceConfig(), ReplicationConfig(state_dir=tmp_path), overrides={key: value}
            )


def test_no_captured_table_can_collide_with_an_internal_topic():
    """`internal_topic_prefixes()` was dead code left behind by the deleted
    handler, which reads as protection that is not there (Opus MINOR-6). It is now
    an assertion the run makes at start-up."""
    assert_no_internal_topic_collision("cdcflight", ["app.customers", "app.orders"])
    with pytest.raises(UnsafeDebeziumProperty, match="transaction"):
        assert_no_internal_topic_collision("cdcflight", ["app.customers", "cdcflight.transaction"])


def test_snapshot_mode_override(tmp_path):
    props = build_properties(
        SourceConfig(), ReplicationConfig(state_dir=tmp_path), snapshot_mode="never"
    )
    assert props["snapshot.mode"] == "never"


def test_non_pinned_property_overrides_are_applied(tmp_path):
    props = build_properties(
        SourceConfig(),
        ReplicationConfig(state_dir=tmp_path),
        overrides={"max.batch.size": "17"},
    )
    assert props["max.batch.size"] == "17"


def test_source_and_money_contract_pins_cannot_be_clobbered_by_overrides(tmp_path):
    replication = ReplicationConfig(state_dir=tmp_path, slot_name="r15_slot")
    source = SourceConfig()
    protected = {
        "driver.options": "different",
        "snapshot.mode": "never",
        "slot.name": "other_slot",
        "plugin.name": "wal2json",
    }
    for key, value in protected.items():
        with pytest.raises(UnsafeDebeziumProperty, match=key.replace(".", r"\.")):
            build_properties(source, replication, overrides={key: value})

    # Matching the effective caller-selected snapshot mode remains a harmless
    # no-op; this keeps recovery's explicit snapshot-mode argument usable.
    props = build_properties(
        source,
        replication,
        snapshot_mode="never",
        overrides={
            "driver.options": "-c lc_monetary=C",
            "snapshot.mode": "never",
            "slot.name": "r15_slot",
            "plugin.name": "pgoutput",
        },
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
