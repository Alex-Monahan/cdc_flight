"""Structural guards for the test-tree refactor.

The lane tests subtract the guard module itself before comparing against the
baseline. That keeps the expected values tied to the pre-refactor suite while
allowing later module-surface guards in this file to be counted explicitly.
"""

from __future__ import annotations

import pytest

_BASELINE_SELECTED = {
    "not motherduck and not slow": 1203,
    "slow and not motherduck": 119,
    "motherduck": 24,
}


def _assert_lane_baseline(request, expression: str) -> None:
    if request.config.getoption("markexpr") != expression:
        return
    guard_path = "tests/unit/test_structure_guards.py::"
    guard_count = sum(item.nodeid.startswith(guard_path) for item in request.session.items)
    selected_without_guards = len(request.session.items) - guard_count
    assert selected_without_guards == _BASELINE_SELECTED[expression]


def test_default_lane_composition(request):
    _assert_lane_baseline(request, "not motherduck and not slow")


@pytest.mark.slow
def test_slow_lane_composition(request):
    _assert_lane_baseline(request, "slow and not motherduck")


@pytest.mark.motherduck
def test_motherduck_lane_composition(request):
    _assert_lane_baseline(request, "motherduck")


def test_catalog_helpers_share_one_public_module():
    import importlib.util

    from cdc_flight import catalog, catalog_poll, catalog_support, state_matrix

    assert catalog_support.CATALOG_SQL
    assert callable(catalog_support.summary)
    assert callable(catalog_support.observe_unit)
    assert callable(catalog_support.read_columns)
    assert catalog.catalog_support is catalog_support
    assert catalog_poll.observation_mod is catalog_support
    assert state_matrix.catalog_support is catalog_support
    for removed in ("catalog_observation", "catalog_reporting", "catalog_runtime"):
        assert importlib.util.find_spec(f"cdc_flight.{removed}") is None


def test_snapshot_protocol_and_notifications_share_one_public_module():
    import importlib.util

    from cdc_flight import snapshot_completion

    assert callable(snapshot_completion.notification_topic)
    assert callable(snapshot_completion.decode_notification)
    assert snapshot_completion.SnapshotNotification
    assert snapshot_completion.SnapshotCompletion
    assert importlib.util.find_spec("cdc_flight.snapshot_notifications") is None


def test_applier_config_has_one_canonical_module_and_compatibility_surface():
    import importlib.util

    from cdc_flight.applier import ApplierConfig as ApplierPublicConfig
    from cdc_flight.config import ApplierConfig

    assert ApplierPublicConfig is ApplierConfig
    assert importlib.util.find_spec("cdc_flight.applier_config") is None
