"""Structural guards for the test-tree refactor.

The lane tests subtract the guard module itself before comparing against the
baseline. That keeps the expected values tied to the pre-refactor suite while
allowing later module-surface guards in this file to be counted explicitly.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_BASELINE_SELECTED = {
    "not motherduck and not slow": 1371,
    "slow and not motherduck": 124,
    "motherduck": 30,
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


def test_offset_codec_reconciliation_and_resume_share_one_module():
    import importlib.util

    from cdc_flight import offsets

    for name in (
        "encode_key",
        "read",
        "write",
        "parse_offsets",
        "lsn_of",
        "file_lsn",
        "capture_offset_file",
        "point_for",
        "reconcile",
    ):
        assert callable(getattr(offsets, name))
    assert offsets.Reconciliation
    for removed in ("offset_file", "offset_reconcile", "resume"):
        assert importlib.util.find_spec(f"cdc_flight.{removed}") is None


def test_runtime_state_filesystem_module_owns_atomic_publication():
    import importlib.util
    import sys

    root = Path(__file__).resolve().parents[2]
    path = root / "scripts" / "runtime_state_fs.py"
    spec = importlib.util.spec_from_file_location("runtime_state_fs_guard", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    assert callable(module.rename_noreplace)
    assert not (root / "scripts" / "runtime_state_publish.py").exists()


def test_runtime_state_wrapper_executes_the_renamed_cli():
    root = Path(__file__).resolve().parents[2]
    wrapper = root / "scripts" / "runtime_state.sh"
    text = wrapper.read_text()
    assert "runtime_state_cli.py" in text
    assert "runtime_state.py" not in text
    result = subprocess.run(
        [str(wrapper), "--help"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
