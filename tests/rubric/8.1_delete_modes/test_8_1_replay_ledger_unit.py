"""Rubric §8.1 delete-ledger replay and collision fences."""

from __future__ import annotations

import pytest
from support.applier_lab import Lab, data, end

from cdc_flight.destination import ResumePoint
from cdc_flight.errors import DestinationIdentityCollision


@pytest.fixture(params=["hard", "soft"])
def delete_lab(request, tmp_path):
    box = Lab(tmp_path / f"ledger-{request.param}.duckdb", delete_mode=request.param)
    yield box
    box.close()


def _delete(box, *, table="ledger_rows", lsn=200, value="x"):
    box.run(
        [
            data(
                "delete",
                1,
                lsn,
                table=table,
                op="d",
                key={"id": 1},
                before={"id": 1, "name": value},
            ),
            end("delete", 1, lsn + 10, {f"app.{table}": 1}),
        ]
    )


def test_post_commit_replay_is_a_noop_in_both_modes(delete_lab):
    box = delete_lab
    box.run(
        [
            data("seed", 1, 100, table="ledger_rows", after={"id": 1, "name": "x"}),
            end("seed", 1, 110, {"app.ledger_rows": 1}),
        ]
    )
    _delete(box)
    first_ledger = box.q(
        "SELECT event_id, delete_mode, effect_digest FROM _cdc_flight.delete_ledger"
    )
    box.applier.resume_point = ResumePoint(last_lsn=0)
    _delete(box)
    assert box.q(
        "SELECT event_id, delete_mode, effect_digest FROM _cdc_flight.delete_ledger"
    ) == first_ledger
    target = box.target("ledger_rows")
    assert box.scalar(f'SELECT count(*) FROM "cdc_raw"."{target}"') == (
        0 if box.config.delete_mode == "hard" else 1
    )


def test_same_event_with_changed_identity_or_policy_is_rejected(delete_lab):
    box = delete_lab
    box.run(
        [
            data("seed", 1, 100, table="ledger_collision", after={"id": 1, "name": "x"}),
            end("seed", 1, 110, {"app.ledger_collision": 1}),
        ]
    )
    _delete(box, table="ledger_collision", lsn=200, value="x")
    target = box.target("ledger_collision")
    with pytest.raises(DestinationIdentityCollision):
        box.con.execute(
            "UPDATE _cdc_flight.delete_ledger SET identity_digest = 'different' "
            "WHERE target_table = ?",
            [target],
        )
        # The next claim sees the durable mismatch rather than treating the event
        # as a fresh delete. Use the public destination fence directly.
        from cdc_flight.destination import claim_delete_ledger

        claim_delete_ledger(
            box.con,
            pipeline="lab",
            target_table=target,
            event_id="200:delete:1",
            source_schema="app",
            source_table="ledger_collision",
            source_lsn=200,
            txn_id="delete",
            total_order=1,
            delete_mode=box.config.delete_policy.resolve("app.ledger_collision"),
            policy_epoch=1,
            policy_digest=box.config.delete_policy.digest,
            identity_digest="expected",
            effect_digest="expected",
        )


def test_hard_delete_keeps_reconciliation_evidence(delete_lab):
    box = delete_lab
    box.run(
        [
            data("seed", 1, 100, table="hard_evidence", after={"id": 1, "name": "x"}),
            end("seed", 1, 110, {"app.hard_evidence": 1}),
        ]
    )
    _delete(box, table="hard_evidence")
    target = box.target("hard_evidence")
    assert box.scalar(
        "SELECT count(*) FROM _cdc_flight.delete_ledger WHERE target_table = ?",
        [target],
    ) == 1
    assert box.scalar(
        "SELECT count(*) FROM _cdc_flight.commit_log WHERE pipeline = 'lab'"
    ) >= 2

