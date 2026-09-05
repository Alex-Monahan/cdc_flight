"""State-directory ownership is independent of physical-destination ownership."""

from __future__ import annotations

import pytest

from cdc_flight.config import DestinationConfig, ReplicationConfig
from cdc_flight.errors import LeaseLost
from cdc_flight.state_directory_lease import StateDirectoryLease


def test_different_destinations_cannot_share_one_state_directory(tmp_path):
    state_dir = tmp_path / "shared-state"
    first_replication = ReplicationConfig(state_dir=state_dir)
    second_replication = ReplicationConfig(state_dir=state_dir)
    first_destination = DestinationConfig(
        kind="duckdb", duckdb_path=tmp_path / "first.duckdb"
    )
    second_destination = DestinationConfig(
        kind="duckdb", duckdb_path=tmp_path / "second.duckdb"
    )

    assert first_destination.lease_key != second_destination.lease_key
    first = StateDirectoryLease(first_replication.state_dir)
    second = StateDirectoryLease(second_replication.state_dir)
    try:
        first.acquire()
        with pytest.raises(LeaseLost, match="state directory"):
            second.acquire()
    finally:
        second.release()
        first.release()

    # Process death releases the kernel lock; a successor can acquire the same
    # state path after the first owner has released it.
    second.acquire()
    second.release()


def test_state_directory_lock_is_a_sidecar_outside_recovery_tree(tmp_path):
    state_dir = tmp_path / "state"
    lease = StateDirectoryLease(state_dir)
    lease.acquire()
    try:
        assert lease.lock_path.parent == state_dir.parent
        assert lease.lock_path != state_dir / ".cdc_flight.lock"
    finally:
        lease.release()
