"""Rubric §8.1 current-state effects through the real Applier laboratory."""

from __future__ import annotations

import pytest
from support.applier_lab import Lab, data, end

from cdc_flight.destination import ResumePoint


@pytest.fixture
def make_lab(tmp_path):
    boxes = []

    def factory(**config):
        box = Lab(tmp_path / f"delete-{len(boxes)}.duckdb", **config)
        boxes.append(box)
        return box

    yield factory
    for box in boxes:
        box.close()


def _seed(box, table="delete_fold", ident=1, value="before"):
    box.run(
        [
            data("seed", 1, 100, table=table, after={"id": ident, "name": value}),
            end("seed", 1, 110, {f"app.{table}": 1}),
        ]
    )


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_keyed_delete_effect_and_live_view(make_lab, mode):
    box = make_lab(delete_mode=mode)
    _seed(box)
    box.run(
        [
            data(
                "delete",
                1,
                200,
                table="delete_fold",
                op="d",
                key={"id": 1},
                before={"id": 1, "name": "before"},
            ),
            end("delete", 1, 210, {"app.delete_fold": 1}),
        ]
    )
    target = box.target("delete_fold")
    assert box.q(
        f'SELECT cdcf_deleted, cdcf_delete_event_id, cdcf_delete_lsn '
        f'FROM "cdc_raw"."{target}"'
    ) == ([(True, "200:delete:1", "200")] if mode == "soft" else [])
    assert box.q(
        f'SELECT * FROM "cdc_raw"."{target}__live"'
    ) == []
    ledger = box.q(
        "SELECT delete_mode, policy_epoch, effect_state FROM _cdc_flight.delete_ledger "
        "WHERE target_table = ?",
        [target],
    )
    assert ledger == [(mode, 1, "applied")]


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_delete_then_source_insert_reuses_key_without_resurrection(make_lab, mode):
    box = make_lab(delete_mode=mode)
    _seed(box, table="delete_reuse")
    box.run(
        [
            data(
                "delete",
                1,
                200,
                table="delete_reuse",
                op="d",
                key={"id": 1},
                before={"id": 1, "name": "before"},
            ),
            end("delete", 1, 210, {"app.delete_reuse": 1}),
        ]
    )
    box.run(
        [
            data(
                "insert-after-delete",
                1,
                301,
                table="delete_reuse",
                op="c",
                key={"id": 1},
                after={"id": 1, "name": "after"},
            ),
            end("insert-after-delete", 1, 310, {"app.delete_reuse": 1}),
        ]
    )
    assert box.q(
        'SELECT id, name, cdcf_deleted FROM "cdc_raw"."cdcflight_app_delete_reuse"'
    ) == [(1, "after", False)]
    box.applier.resume_point = ResumePoint(last_lsn=0)
    # Replaying only the old delete must be a ledger no-op and must not erase or
    # re-tombstone the later source INSERT.
    old_delete = data(
        "delete",
        1,
        200,
        table="delete_reuse",
        op="d",
        key={"id": 1},
        before={"id": 1, "name": "before"},
    )
    box.run([old_delete, end("delete", 1, 210, {"app.delete_reuse": 1})])
    assert box.q(
        'SELECT id, name, cdcf_deleted FROM "cdc_raw"."cdcflight_app_delete_reuse"'
    ) == [(1, "after", False)]


def test_keyless_soft_delete_matches_one_complete_before_image(make_lab):
    box = make_lab(delete_mode="soft")
    table = "delete_keyless"
    before = {"id": 7, "name": "same"}
    box.run(
        [
            data("seed", 1, 400, table=table, after=before),
            end("seed", 1, 410, {f"app.{table}": 1}),
            data("delete", 1, 500, table=table, op="d", before=before),
            end("delete", 1, 510, {f"app.{table}": 1}),
        ]
    )
    assert box.scalar(
        f'SELECT count(*) FROM "cdc_raw"."{box.target(table)}" '
        "WHERE cdcf_deleted"
    ) == 1
    assert box.scalar(
        f'SELECT count(*) FROM "cdc_raw"."{box.target(table)}__live"'
    ) == 0
