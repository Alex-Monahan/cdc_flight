"""Delete-policy changes are fenced to complete source transactions."""

from __future__ import annotations

import pytest
from support.applier_lab import Lab, begin, data, end

from cdc_flight.delete_modes import DeleteModeResolver

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _delete(txn: str, order: int, lsn: int, table: str, ident: int):
    return data(
        txn,
        order,
        lsn,
        table=table,
        op="d",
        key={"id": ident},
        before={"id": ident, "name": f"row-{ident}"},
    )


def test_hard_to_soft_and_soft_to_hard_are_boundary_fenced(tmp_path):
    box = Lab(tmp_path / "mode-boundary.duckdb", delete_mode="hard")
    try:
        box.run(
            [
                data("seed", 1, 10, table="mode_boundary", key={"id": 1}, after={"id": 1, "name": "row-1"}),
                data("seed", 2, 11, table="mode_boundary", key={"id": 2}, after={"id": 2, "name": "row-2"}),
                data("seed", 3, 12, table="mode_boundary", key={"id": 3}, after={"id": 3, "name": "row-3"}),
                end("seed", 3, 20, {"app.mode_boundary": 3}),
            ]
        )

        # The request is made while the source transaction is open.  Its delete
        # must still use hard mode; activation is allowed only before the next
        # BEGIN, after the old unit has been committed.
        box.feed([begin("hard-delete", 2), _delete("hard-delete", 1, 30, "mode_boundary", 1)])
        box.applier.request_delete_policy(DeleteModeResolver(global_mode="soft", epoch=2))
        box.feed([end("hard-delete", 1, 40, {"app.mode_boundary": 1})])

        box.feed([begin("soft-delete", 2)])
        box.feed([_delete("soft-delete", 1, 50, "mode_boundary", 2)])
        box.feed([end("soft-delete", 1, 60, {"app.mode_boundary": 1})])
        box.commit()

        target = box.target("mode_boundary")
        assert box.q(
            f'SELECT id FROM "cdc_raw"."{target}" ORDER BY id'
        ) == [(2,), (3,)]
        assert box.q(
            f'SELECT id, cdcf_deleted FROM "cdc_raw"."{target}" ORDER BY id'
        ) == [(2, True), (3, False)]
        assert box.scalar(
            f'SELECT count(*) FROM "cdc_raw"."{target}__live"'
        ) == 1
        assert box.q(
            "SELECT event_id, delete_mode, policy_epoch FROM _cdc_flight.delete_ledger "
            "WHERE target_table = ? ORDER BY event_id",
            [target],
        ) == [("30:hard-delete:1", "hard", 1), ("50:soft-delete:1", "soft", 2)]

        # Switching back to hard is another boundary.  The already marked row is
        # swept in the same destination transaction as the next source event; the
        # hard-deleted row is never reconstructed.
        box.applier.request_delete_policy(DeleteModeResolver(global_mode="hard", epoch=3))
        box.run(
            [
                _delete("hard-again", 1, 70, "mode_boundary", 3),
                end("hard-again", 1, 80, {"app.mode_boundary": 1}),
            ]
        )
        assert box.q(
            f'SELECT id FROM "cdc_raw"."{target}" ORDER BY id'
        ) == []
        assert box.q(
            "SELECT event_id, delete_mode, policy_epoch FROM _cdc_flight.delete_ledger "
            "WHERE target_table = ? ORDER BY event_id",
            [target],
        ) == [
            ("30:hard-delete:1", "hard", 1),
            ("50:soft-delete:1", "soft", 2),
            ("70:hard-again:1", "hard", 3),
        ]
    finally:
        box.close()
