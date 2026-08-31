"""Rubric §8.1 memory/spill parity through the real Applier."""

from __future__ import annotations

import pytest
from support.applier_lab import Lab, data, end


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_complete_delete_transaction_has_identical_memory_and_spill_results(tmp_path, mode):
    boxes = []
    try:
        for kind, config in (
            ("memory", {"delete_mode": mode}),
            ("spill", {"delete_mode": mode, "unit_spill_events": 1, "unit_spill_bytes": 1}),
        ):
            box = Lab(tmp_path / f"spill-{mode}-{kind}.duckdb", **config)
            boxes.append(box)
            box.run(
                [
                    data("seed", 1, 100, table="spill_delete", after={"id": 1, "name": "x"}),
                    end("seed", 1, 110, {"app.spill_delete": 1}),
                ]
            )
            box.run(
                [
                    data(
                        "delete",
                        1,
                        200,
                        table="spill_delete",
                        op="d",
                        key={"id": 1},
                        before={"id": 1, "name": "x"},
                    ),
                    end("delete", 1, 210, {"app.spill_delete": 1}),
                ]
            )
        target = boxes[0].target("spill_delete")
        projections = []
        for box in boxes:
            projections.append(
                (
                    box.q(
                        f'SELECT id, name, cdcf_deleted FROM "cdc_raw"."{target}"'
                    ),
                    box.q(
                        "SELECT event_id, delete_mode, effect_state FROM "
                        "_cdc_flight.delete_ledger"
                    ),
                )
            )
        assert projections[0] == projections[1]
    finally:
        for box in boxes:
            box.close()
