"""§3.7 tests: durable keyed cursor, retained shadow, and automatic resume."""

from __future__ import annotations

import json
import signal
import uuid

import pytest
from support.backfill_lab import require_backfill


def test_keyed_chunk_progress_is_stable_and_type_aware():
    """Progress identity is signal/table/key, never process arrival order or text casts."""
    backfill = require_backfill()
    first = backfill.incremental_identity(
        "signal-1", "app.customers", {"id": 7, "tenant": 2}
    )
    second = backfill.incremental_identity(
        "signal-1", "app.customers", {"id": 7, "tenant": 2}
    )
    different_type = backfill.incremental_identity(
        "signal-1", "app.customers", {"id": "7", "tenant": 2}
    )
    assert first == second
    assert first.startswith("inc:signal-1:app.customers:")
    assert different_type != first
    assert "::7" not in first


def test_composite_uuid_and_schema_qualified_keys_are_resume_identities():
    """The cursor covers composite names, UUID values, and qualified relations."""
    backfill = require_backfill()
    key = {"tenant_id": 7, "row_id": uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")}
    reordered = {"row_id": key["row_id"], "tenant_id": 7}
    identity = backfill.incremental_identity("signal-typed", "billing.events", key)
    assert identity == backfill.incremental_identity(
        "signal-typed", "billing.events", reordered
    )
    assert identity != backfill.incremental_identity(
        "signal-typed",
        "billing.events",
        {"tenant_id": "7", "row_id": str(key["row_id"])},
    )
    encoded = json.loads(backfill.canonical_key_json(key))
    assert encoded["row_id"]["type"] == "uuid"
    assert "billing.events" in identity


@pytest.mark.parametrize(
    "fault_point",
    [
        "incremental_chunk_before_shadow_write",
        "incremental_chunk_after_shadow_write_before_progress",
        "incremental_chunk_after_progress_before_md_commit",
        "after_md_commit_before_markProcessed",
    ],
)
def test_each_incremental_chunk_crash_cut_retains_a_resumable_partial_shadow(
    tmp_path, fault_point
):
    """A real fault boundary leaves the durable cursor and shadow for restart."""
    backfill = require_backfill()
    lab = backfill.ResumableBackfillLab(tmp_path, chunks=20)
    clean = lab.run_clean()
    crashed = lab.run_with_fault(fault_point)
    assert crashed.returncode in {137, -signal.SIGKILL}
    fired = json.loads((lab.state_dir / "fault_fired.json").read_text())
    assert fired["point"] == fault_point
    assert fired["nth"] == 2
    assert lab.partial_shadow_exists()
    assert lab.durable_cursor() > 0
    resumed = lab.resume()
    assert lab.identity_set(resumed.rows) == lab.identity_set(clean.rows)
    assert lab.value_multiset(resumed.rows) == lab.value_multiset(clean.rows)
    assert resumed.duplicate_keys == 0


def test_real_crash_matrix_child_is_used_and_not_armable_from_package_install():
    """The source-tree crash harness is the only hard-exit implementation."""
    backfill = require_backfill()
    child = backfill.crash_matrix_child_path()
    assert child.name == "crash_matrix_child.py"
    assert child.exists()
    assert backfill.production_fault_handler_available() is False


def test_keyless_stock_boundary_is_fallback_not_fake_resume():
    """Keyless tables retain the honest 4-level boundary."""
    backfill = require_backfill()
    result = backfill.keyless_resume_result(status="NO_PRIMARY_KEY")
    assert result.effective_mode == "full"
    assert result.ceiling == 4
    assert result.cursor is None
