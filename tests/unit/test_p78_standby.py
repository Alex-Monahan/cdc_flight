from __future__ import annotations

from dataclasses import replace

import pytest

from cdc_flight.config import SourceConfig
from cdc_flight.source_health import _SLOT_SQL, _SLOT_SQL_FAST
from cdc_flight.source_routes import SourceRoutePolicy
from cdc_flight.standby import (
    StandbyCapabilityError,
    StandbyObservation,
    assert_supported,
    unsupported_reasons,
)


def _healthy() -> StandbyObservation:
    return StandbyObservation(
        server_version_num=160000,
        in_recovery=True,
        wal_level="replica",
        primary_wal_level="logical",
        hot_standby_feedback=True,
        receiver_status="streaming",
        receiver_slot_name="p78_physical",
        expected_physical_slot_name="p78_physical",
        local_slot_name="p78_local",
        local_slot_type="logical",
        local_slot_plugin="pgoutput",
        local_slot_active=False,
        # PostgreSQL 16 does not expose the later failover-slot flags.  False is
        # the explicit positive witness for versions that do expose them.
        local_slot_synced=False,
        local_slot_failover=False,
        local_slot_wal_status="reserved",
        local_slot_catalog_xmin="123",
        local_slot_invalidation_reason=None,
        system_identifier="7001",
        timeline_id=1,
    )


@pytest.mark.parametrize(
    ("field", "value", "needle"),
    [
        ("server_version_num", 150000, "PostgreSQL 16+"),
        ("in_recovery", False, "not in recovery"),
        ("primary_wal_level", "replica", "wal_level=logical"),
        ("hot_standby_feedback", False, "hot_standby_feedback"),
        ("receiver_status", "catchup", "receiver is not streaming"),
        ("receiver_slot_name", "other", "different slot"),
        ("local_slot_type", "physical", "not logical"),
        ("local_slot_plugin", "wal2json", "not 'pgoutput'"),
        ("local_slot_synced", True, "synchronized failover slot"),
        ("local_slot_failover", True, "failover slot"),
        ("local_slot_wal_status", "lost", "WAL status"),
        ("local_slot_invalidation_reason", "rows_removed", "invalidated"),
    ],
)
def test_each_standby_guard_has_a_negative_witness(field, value, needle):
    observation = replace(_healthy(), **{field: value})
    reasons = unsupported_reasons(observation)
    assert any(needle in reason for reason in reasons), reasons
    with pytest.raises(StandbyCapabilityError, match=needle):
        assert_supported(observation)


def test_a_healthy_local_slot_is_supported_and_synced_slots_are_not():
    assert assert_supported(_healthy()).local_slot_name == "p78_local"
    assert "synchronized" in unsupported_reasons(
        replace(_healthy(), local_slot_synced=True)
    )[0]


def test_source_config_requires_explicit_standby_opt_in(monkeypatch):
    monkeypatch.delenv("CDC_SOURCE_ROLE", raising=False)
    assert SourceConfig().role == "primary"
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    assert SourceConfig().role == "standby"
    monkeypatch.setenv("CDC_SOURCE_ROLE", "snapshot-only")
    with pytest.raises(ValueError, match="CDC_SOURCE_ROLE"):
        _ = SourceConfig().role


def test_standby_source_fails_closed_without_a_primary_write_route(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    monkeypatch.delenv("CDC_PRIMARY_DSN", raising=False)
    with pytest.raises(ValueError, match="CDC_PRIMARY_DSN"):
        _ = SourceConfig().primary_dsn


def test_standby_source_uses_only_the_explicit_primary_write_route(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    monkeypatch.setenv("CDC_PRIMARY_DSN", "postgresql://writer:pw@primary:15432/db")
    source = SourceConfig()
    assert source.primary_dsn == "postgresql://writer:pw@primary:15432/db"


def test_standby_route_policy_keeps_local_slot_admin_on_the_read_endpoint(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ROLE", "standby")
    monkeypatch.setenv("CDC_PRIMARY_DSN", "postgresql://writer:pw@primary:15432/db")
    source = SourceConfig(host="replica", port=15435)
    assert source.route_policy == SourceRoutePolicy(
        role="standby",
        read_replication_dsn=source.dsn,
        source_write_dsn=source.primary_dsn,
        slot_owner_dsn=source.dsn,
    )


def test_standby_identity_mismatch_is_a_capability_failure():
    observation = replace(
        _healthy(),
        primary_system_identifier="different-system",
    )
    assert any("system identifiers differ" in reason for reason in unsupported_reasons(observation))
    with pytest.raises(StandbyCapabilityError, match="system identifiers differ"):
        assert_supported(observation)


def test_source_health_wal_position_is_recovery_safe():
    for sql in (_SLOT_SQL, _SLOT_SQL_FAST):
        assert "pg_is_in_recovery()" in sql
        assert "pg_last_wal_receive_lsn()" in sql
        assert "pg_current_wal_lsn()" in sql


def test_resnapshot_empty_fence_wal_position_is_recovery_safe():
    from cdc_flight.resnapshot_source_policy import SOURCE_WAL_LSN_SQL

    assert "pg_is_in_recovery()" in SOURCE_WAL_LSN_SQL
    assert "pg_last_wal_receive_lsn()" in SOURCE_WAL_LSN_SQL
    assert "CASE WHEN" in SOURCE_WAL_LSN_SQL
