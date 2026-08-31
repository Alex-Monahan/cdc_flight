"""Delete, policy state, and a peer table share one commit-group transaction."""

from __future__ import annotations

import pytest
from support.applier_lab import Lab, data, end


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_delete_policy_and_peer_write_are_one_atomic_group(tmp_path, mode):
    box = Lab(tmp_path / f"atomic-{mode}.duckdb", delete_mode=mode)
    try:
        box.run(
            [
                data("seed", 1, 10, table="atomic_delete", key={"id": 1}, after={"id": 1, "name": "delete-me"}),
                data("seed", 2, 11, table="atomic_peer", key={"id": 1}, after={"id": 1, "name": "peer-before"}),
                end("seed", 2, 20, {"app.atomic_delete": 1, "app.atomic_peer": 1}),
            ]
        )
        box.run(
            [
                data("atomic", 1, 30, table="atomic_delete", op="d", key={"id": 1}, before={"id": 1, "name": "delete-me"}),
                data("atomic", 2, 31, table="atomic_peer", key={"id": 2}, after={"id": 2, "name": "peer-after"}),
                end("atomic", 2, 40, {"app.atomic_delete": 1, "app.atomic_peer": 1}),
            ]
        )
        delete_target = box.target("atomic_delete")
        peer_target = box.target("atomic_peer")
        assert box.q(
            f'SELECT id, cdcf_deleted FROM "cdc_raw"."{delete_target}"'
        ) == ([(1, True)] if mode == "soft" else [])
        assert box.q(
            f'SELECT id, name FROM "cdc_raw"."{peer_target}" ORDER BY id'
        ) == [(1, "peer-before"), (2, "peer-after")]
        assert box.q(
            "SELECT commit_id, event_count FROM _cdc_flight.commit_log ORDER BY commit_id"
        ) == [(1, 2), (2, 2)]
        assert box.q(
            "SELECT target_table, delete_mode, effect_state FROM _cdc_flight.delete_ledger"
        ) == [(delete_target, mode, "applied")]
        assert box.q(
            "SELECT target_table, delete_mode FROM _cdc_flight.table_state "
            "WHERE target_table IN (?, ?) ORDER BY target_table",
            [delete_target, peer_target],
        ) == [(delete_target, mode), (peer_target, mode)]
    finally:
        box.close()
