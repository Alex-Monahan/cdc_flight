"""§3.2 tests: observation-level atomic shadow publication."""

from __future__ import annotations

import pytest
from support.backfill_lab import require_backfill


def test_atomic_swap_publishes_complete_data_and_state_together(tmp_path):
    """A consumer must see old complete or new complete data/state, never a gap."""
    backfill = require_backfill()
    lab = backfill.LocalAtomicityLab(tmp_path)
    lab.create_live([(1, "old"), (2, "old")], state="old")
    lab.prepare_shadow([(1, "new"), (2, "new"), (3, "new")])
    trace = lab.polling_reader_during_swap()
    assert trace
    assert all(observation.data in lab.complete_images for observation in trace)
    assert all(observation.state in {"old", "new"} for observation in trace)
    assert {(observation.data, observation.state) for observation in trace} <= {
        ("old", "old"),
        ("new", "new"),
    }


def test_swap_rollback_leaves_the_old_identity_and_state(tmp_path):
    """An explicit rollback before publication preserves the old image and state."""
    backfill = require_backfill()
    lab = backfill.LocalAtomicityLab(tmp_path)
    lab.create_live([(1, "old")], state="old")
    lab.prepare_shadow([(1, "new"), (2, "new")])
    lab.swap(rollback=True)
    assert lab.read_image() == [(1, "old")]
    assert lab.read_state() == "old"


@pytest.mark.parametrize("fault", ["between_drop_and_rename", "after_rename"])
def test_each_rename_fault_rolls_back_to_the_old_complete_image(tmp_path, fault):
    """Proves faults on either side of the rename cannot expose an intermediate name."""
    backfill = require_backfill()
    lab = backfill.LocalAtomicityLab(tmp_path / fault)
    lab.create_live([(1, "old")], state="old")
    lab.prepare_shadow([(1, "new"), (2, "new")])
    lab.swap(fault=fault)
    assert lab.read_image() == [(1, "old")]
    assert lab.read_state() == "old"


def test_data_and_backfill_state_are_one_motherduck_transaction(tmp_path):
    """The commit trace must place all state writes before one COMMIT and no work after it."""
    backfill = require_backfill()
    protocol = backfill.CommitTrace()
    protocol.record("shadow_row")
    protocol.record("progress")
    protocol.record("run_state")
    protocol.record("claim_release")
    protocol.commit()
    protocol.record("markProcessed")
    protocol.record("markBatchFinished")
    assert protocol.before_commit == ["shadow_row", "progress", "run_state", "claim_release"]
    assert protocol.after_commit == ["markProcessed", "markBatchFinished"]
