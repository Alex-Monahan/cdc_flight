"""Structural guards for the test-tree refactor.

The lane tests subtract the guard module itself before comparing against the
baseline. That keeps the expected values tied to the pre-refactor suite while
allowing later module-surface guards in this file to be counted explicitly.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

#: RE-MEASURED FROM THE REAL LANE COLLECTIONS after the §3 gap coverage and the
#: detection/alerting proofs. These are collection counts, not a module-size rule.
#: The expected values below subtract only this guard module.
_BASELINE_SELECTED = {
    "not motherduck and not slow": 2218,
    "slow and not motherduck": 237,
    "motherduck": 51,
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


def test_type_apply_owners_are_split_by_cohesion_not_line_count():
    """The round-3 type split has explicit owners with real dependency boundaries."""
    from cdc_flight import identity_codec, schema_registry, typed_materialization

    assert callable(identity_codec._identity_value)
    assert callable(identity_codec.canonical_jsonb_identity)
    assert schema_registry.SchemaRegistry.__module__ == "cdc_flight.schema_registry"
    assert typed_materialization.insert_rows.__module__ == "cdc_flight.typed_materialization"


def test_type_registry_owners_have_real_dependency_boundaries():
    """The guard checks ownership/import direction, not an arbitrary line count."""
    from cdc_flight import schema_backfill, schema_ddl, schema_registry, schema_shadow

    assert schema_registry.SchemaRegistry._create_strict.__module__ == "cdc_flight.schema_ddl"
    assert schema_registry.SchemaRegistry._create.__module__ == "cdc_flight.schema_ddl"
    assert schema_registry.SchemaRegistry.convert_column_to_union.__module__ == "cdc_flight.schema_shadow"
    assert schema_registry.SchemaRegistry.backfill_columns.__module__ == "cdc_flight.schema_backfill"
    assert schema_registry.SchemaRegistry.backfill_constant_columns.__module__ == "cdc_flight.schema_backfill"
    assert schema_ddl.OWNER == "destination-ddl"
    assert schema_shadow.OWNER == "typed-shadow"
    assert schema_backfill.OWNER == "destination-backfill"
    registry_source = Path(schema_registry.__file__).read_text()
    assert "typed_materialization" not in registry_source
    assert "identity_descriptors" not in registry_source
    assert "def _create(" not in registry_source
    assert "def _create_strict(" not in registry_source
    assert "def convert_column_to_union(" not in registry_source
    assert "def backfill_columns(" not in registry_source
    assert "CREATE TABLE" in Path(schema_ddl.__file__).read_text()
    assert "_copy_rows_with_identity" in Path(schema_shadow.__file__).read_text()
    assert "SchemaBackfillRefused" in Path(schema_backfill.__file__).read_text()
    for owner in (schema_ddl, schema_shadow, schema_backfill):
        owner_source = Path(owner.__file__).read_text()
        assert "schema_registry" not in owner_source


def test_commit_protocol_owns_the_durability_boundary():
    """Applier lifecycle and commit durability are separate real owners."""
    from cdc_flight import applier, commit_protocol

    assert commit_protocol.commit_group.__module__ == "cdc_flight.commit_protocol"
    assert applier.Applier.commit_group.__module__ == "cdc_flight.applier"
    applier_source = Path(applier.__file__).read_text()
    protocol_source = Path(commit_protocol.__file__).read_text()
    assert 'self.con.execute("BEGIN TRANSACTION")' not in applier_source
    assert 'self.con.execute("COMMIT")' not in applier_source
    assert 'self.con.execute("BEGIN TRANSACTION")' in protocol_source
    assert 'self.con.execute("COMMIT")' in protocol_source
    assert commit_protocol.OWNER == "commit-durability"


def test_commit_ack_window_has_no_crash_matrix_persistence():
    """The COMMIT-to-ack region contains no alert, log, or flush I/O.

    This is deliberately a source guard: putting the timeout alert back in the
    watchdog callback or adding a log statement between ``COMMIT`` and
    ``COMMIT_ACK.leave`` makes this test fail before a timing-sensitive integration
    run can hide the violation.
    """
    from cdc_flight import commit_protocol

    source = Path(commit_protocol.__file__).read_text()
    commit = source.index('self.con.execute("COMMIT")')
    window_end = source.index("COMMIT_ACK.leave()", commit)
    boundary = source[commit:window_end]
    assert "runtime_state" not in boundary
    assert "os.replace" not in boundary
    assert "fsync" not in boundary
    for forbidden in (
        "raise_alert",
        "AlertSink",
        "clear_alert",
        "log.",
        "logging.",
        "record_log",
        "flush(",
        "_arm_commit_timeout_alert",
        "_clear_commit_timeout_alert",
    ):
        assert forbidden not in boundary, forbidden


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
