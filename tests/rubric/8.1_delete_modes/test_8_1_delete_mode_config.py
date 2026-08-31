"""Rubric §8.1 configuration and durable policy identity."""

from __future__ import annotations

import pytest

from cdc_flight.config import ApplierConfig, applier_settings
from cdc_flight.delete_modes import (
    DeleteModeConfigurationError,
    DeleteModeResolver,
)


def test_default_is_hard_and_explicit_table_override_wins(monkeypatch):
    for name in (
        "CDC_DELETE_MODE",
        "CDC_DELETE_MODE_RULES",
        "CDC_DELETE_POLICY_EPOCH",
    ):
        monkeypatch.delenv(name, raising=False)
    resolver = DeleteModeResolver.from_environment()
    assert resolver.resolve("app.customers") == "hard"
    assert resolver.canonical_manifest()["global_mode"] == "hard"

    monkeypatch.setenv("CDC_DELETE_MODE", "soft")
    monkeypatch.setenv(
        "CDC_DELETE_MODE_RULES",
        '{"app.customers":"hard", "app.orders":"soft"}',
    )
    monkeypatch.setenv("CDC_DELETE_POLICY_EPOCH", "7")
    resolver = DeleteModeResolver.from_environment()
    assert resolver.resolve("app.other") == "soft"
    assert resolver.resolve("APP.CUSTOMERS") == "hard"
    assert resolver.resolve("app.orders") == "soft"
    assert resolver.epoch == 7
    assert len(resolver.digest) == 64
    assert resolver.digest == DeleteModeResolver.from_environment().digest


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("CDC_DELETE_MODE", "archive"),
        ("CDC_DELETE_MODE_RULES", "[]"),
        ("CDC_DELETE_MODE_RULES", '{"app.customers":"archive"}'),
        ("CDC_DELETE_MODE_RULES", '{"app.customers":"soft", "APP.CUSTOMERS":"hard"}'),
    ],
)
def test_invalid_delete_configuration_refuses_before_consumption(monkeypatch, field, value):
    monkeypatch.setenv(field, value)
    with pytest.raises(DeleteModeConfigurationError):
        DeleteModeResolver.from_environment()


def test_applier_config_and_shared_construction_use_one_delete_policy(monkeypatch):
    monkeypatch.setenv("CDC_DELETE_MODE", "soft")
    monkeypatch.setenv("CDC_DELETE_MODE_RULES", '{"app.orders":"hard"}')
    monkeypatch.setenv("CDC_DELETE_POLICY_EPOCH", "11")
    settings = applier_settings()
    cfg = ApplierConfig(**settings)
    resolver = settings["delete_policy"]

    assert cfg.delete_policy is resolver
    assert cfg.delete_policy.resolve("app.customers") == "soft"
    assert cfg.delete_policy.resolve("app.orders") == "hard"
    # Discovery and the throwaway re-snapshot receive the same settings mapping;
    # equality here is the durable version/digest contract, not object identity.
    assert cfg.delete_policy.epoch == 11
    assert cfg.delete_policy.digest == resolver.digest
    assert cfg.delete_policy.canonical_manifest() == resolver.canonical_manifest()
