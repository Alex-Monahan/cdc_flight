"""Rubric 1.2 — the decisive test for the keyless-identity disagreement.

The two 1.1-1.3 reviews disagreed about `cdcf_event_id`:

* **Codex 4 (BLOCKER):** two *accepted* events can share
  `(lsn, txn_id, total_order)` and collapse, because the assembler validated only
  that `txn_id` exists. It reproduced two same-LSN events with duplicate ordinal 1
  both producing `100:7:1`.
* **Opus (attack log §3):** keyless identity is structurally immune —
  `total_order` is a 1-based per-transaction ordinal, unique by construction, and
  the event LSN separates transactions.

Both are right about different things, and this module is the executable
resolution. `test_two_events_that_share_an_ordinal_are_refused` shows Codex's
counterexample is real *at the boundary*; `test_identity_is_unique_and_replay_stable`
and `test_a_replay_recomputes_the_same_identity` show Opus is right *given valid
connector metadata*. The fix is therefore not a new identity but a validated one:
the ordinal contract is enforced where the unit is proven whole, so no colliding
input can ever reach the identity builder. See ADR 0001 §15/A18.
"""

from __future__ import annotations

import pytest
from support.applier_lab import DATASET, Lab, data, end

from cdc_flight.assembler import TransactionAssembler
from cdc_flight.destination import ResumePoint
from cdc_flight.envelope import PendingRecord
from cdc_flight.errors import TransactionAssemblyError

READINGS = "cdcflight_app_sensor_readings"


@pytest.fixture
def lab(tmp_path):
    boxes: list[Lab] = []

    def _make(**cfg) -> Lab:
        box = Lab(tmp_path / f"lab{len(boxes)}.duckdb", **cfg)
        boxes.append(box)
        return box

    yield _make
    for box in boxes:
        box.close()


def _reading(txn: str, order: int, lsn: int, value: float) -> PendingRecord:
    return data(txn, order, lsn, table="sensor_readings", after={"value": value})


# --------------------------------------------------------------------------- #
# Codex 4's counterexample, at the boundary where it belongs
# --------------------------------------------------------------------------- #
def test_two_events_that_share_an_ordinal_are_refused(lab):
    """Codex 4's exact probe: same LSN, duplicate ordinal 1, matching END(2).

    Before the fix the assembler emitted the unit and both events received the
    identity `100:7:1`, so one of them disappeared into the keyless table's
    identity dict. It is now refused at the boundary, which is the only place the
    proof of wholeness lives.
    """
    box = lab()
    records = [_reading("7", 1, 100, 1.0), _reading("7", 1, 100, 2.0)]
    with pytest.raises(TransactionAssemblyError, match="total_order 1 twice"):
        box.run([*records, end("7", 2, 101, {"app.sensor_readings": 2})])


def test_an_event_with_no_ordinal_is_refused(lab):
    """A missing ordinal made every event of the transaction share
    `<lsn>:<txId>:None`; spill made it look plausible by substituting a local
    sequence for `event_seq` while `cdcf_event_id` still contained `None`."""
    box = lab()
    rec = _reading("7", 1, 100, 1.0)
    rec.total_order = None
    with pytest.raises(TransactionAssemblyError, match="total_order"):
        box.run([rec, end("7", 1, 101, {"app.sensor_readings": 1})])


# --------------------------------------------------------------------------- #
# Opus's claim, made executable
# --------------------------------------------------------------------------- #
def test_identity_is_unique_for_distinct_events_sharing_one_lsn(lab):
    """Several events can share one LSN; the ordinal is what separates them."""
    box = lab()
    box.run(
        [
            _reading("7", 1, 100, 1.0),
            _reading("7", 2, 100, 2.0),
            _reading("7", 3, 100, 3.0),
            end("7", 3, 101, {"app.sensor_readings": 3}),
        ]
    )
    ids = box.q(f'SELECT cdcf_event_id FROM "{DATASET}"."{READINGS}" ORDER BY 1')
    assert [i[0] for i in ids] == ["100:7:1", "100:7:2", "100:7:3"], ids


def test_a_replay_recomputes_the_same_identity_and_cannot_duplicate(lab):
    """Ordinals restart at 1 for a replayed transaction, and that is the point.

    A resume point can only ever sit on a transaction boundary (the assembler
    never emits a partial unit and a control record inside a transaction is
    carried by it), so a replayed transaction renumbers from 1 and recomputes
    *identical* identities. The second delivery is deliberately admitted with a
    fresh test applier whose explicit resume point is zero, so the merge on
    `cdcf_event_id` is what this test proves.
    """
    box = lab()
    records = [
        _reading("7", 1, 100, 1.0),
        _reading("7", 2, 101, 2.0),
        end("7", 2, 102, {"app.sensor_readings": 2}),
    ]
    box.run(records)
    first = box.q(f'SELECT cdcf_event_id FROM "{DATASET}"."{READINGS}" ORDER BY 1')

    # The same WAL, delivered again, with a *fresh* record stream (the identity
    # must be derived, not remembered). Reset only this test's in-memory admission
    # point so the replay is above the fence rather than silently discarded by it.
    # This is an explicit test seam: production startup obtains this point from the
    # durable destination and never carries state between tests.
    box.applier.resume_point = ResumePoint(last_lsn=0)
    box.run(
        [
            _reading("7", 1, 100, 1.0),
            _reading("7", 2, 101, 2.0),
            end("7", 2, 102, {"app.sensor_readings": 2}),
        ]
    )
    assert box.applier.fenced_units == 0
    assert box.applier.applied_events == 4
    assert box.q(
        "SELECT event_count, fenced_units FROM _cdc_flight.commit_log "
        "WHERE pipeline = 'lab' ORDER BY commit_id DESC LIMIT 1"
    ) == [(2, 0)]
    second = box.q(
        f'SELECT cdcf_event_id FROM "{DATASET}"."{READINGS}" ORDER BY 1'
    )

    assert first == second == [("100:7:1",), ("101:7:2",)]
    assert box.scalar(f'SELECT count(*) FROM "{DATASET}"."{READINGS}"') == 2, (
        "a replay of the same transaction created extra keyless rows"
    )


def test_two_byte_identical_source_rows_are_two_rows(lab):
    """The assertion no content-deduplicating implementation can pass."""
    box = lab()
    box.run(
        [
            _reading("7", 1, 100, 42.5),
            _reading("7", 2, 101, 42.5),
            end("7", 2, 102, {"app.sensor_readings": 2}),
        ]
    )
    assert box.scalar(f'SELECT count(*) FROM "{DATASET}"."{READINGS}"') == 2


def test_keyless_delete_consumes_one_full_duplicate_and_replay_is_a_noop(lab):
    """A FULL before-image names one physical row, never an event-id row."""
    box = lab()
    seed = [
        data("seed", 1, 100, table="keyless_delete", after={"id": 7, "value": "same", "note": None}),
        data("seed", 2, 101, table="keyless_delete", after={"id": 7, "value": "same", "note": None}),
        data("seed", 3, 102, table="keyless_delete", after={"id": 7, "value": "same", "note": None}),
        end("seed", 3, 110, {"app.keyless_delete": 3}),
    ]
    box.run(seed)
    before = {"id": 7, "value": "same", "note": None}
    delete = data(
        "delete",
        1,
        200,
        table="keyless_delete",
        op="d",
        before=before,
    )
    box.run([delete, end("delete", 1, 210, {"app.keyless_delete": 1})])
    assert box.scalar(
        'SELECT count(*) FROM "cdc_raw"."cdcflight_app_keyless_delete"'
    ) == 2

    # Re-admit the exact source coordinates, bypassing only the lab's resume fence.
    # The durable keyless event state, not a process-local set, must make this a no-op.
    box.applier.resume_point = ResumePoint(last_lsn=0)
    box.run([delete, end("delete", 1, 210, {"app.keyless_delete": 1})])
    assert box.scalar(
        'SELECT count(*) FROM "cdc_raw"."cdcflight_app_keyless_delete"'
    ) == 2
    assert box.scalar(
        'SELECT count(*) FROM "cdc_raw"."cdcflight_app_keyless_delete" '
        "WHERE id = 7 AND value = 'same' AND note IS NULL"
    ) == 2


def test_keyless_delete_reinsert_and_update_fold_in_source_order(lab):
    """Delete/reinsert and a full-image UPDATE preserve physical-row semantics."""
    box = lab()
    original = {"id": 8, "value": "before", "note": "nullable"}
    box.run(
        [
            data("seed", 1, 300, table="keyless_order", after=original),
            end("seed", 1, 310, {"app.keyless_order": 1}),
        ]
    )
    old_id = box.scalar(
        'SELECT cdcf_event_id FROM "cdc_raw"."cdcflight_app_keyless_order"'
    )
    replacement = data(
        "reuse",
        2,
        401,
        table="keyless_order",
        op="c",
        after=original,
    )
    box.run(
        [
            data("reuse", 1, 400, table="keyless_order", op="d", before=original),
            replacement,
            end("reuse", 2, 410, {"app.keyless_order": 2}),
        ]
    )
    assert box.q(
        'SELECT cdcf_event_id, id, value, note FROM '
        '"cdc_raw"."cdcflight_app_keyless_order"'
    ) == [("401:reuse:2", 8, "before", "nullable")]
    assert old_id != "401:reuse:2"

    updated = {"id": 8, "value": "after", "note": None}
    box.run(
        [
            data(
                "update",
                1,
                500,
                table="keyless_order",
                op="u",
                before=original,
                after=updated,
            ),
            end("update", 1, 510, {"app.keyless_order": 1}),
        ]
    )
    assert box.q(
        'SELECT id, value, note, cdcf_event_id FROM '
        '"cdc_raw"."cdcflight_app_keyless_order"'
    ) == [(8, "after", None, "500:update:1")]


def test_the_ordinal_contract_is_enforced_before_the_identity_is_built():
    """A unit-level restatement: nothing that reaches the applier can collide.

    `TransactionAssembler` is the only producer of units, so validating the
    ordinal set there is what makes the identity structurally unique rather than
    conventionally unique.
    """
    a = TransactionAssembler()
    for order in (1, 2, 3):
        a.feed(_reading("9", order, 200 + order, float(order)))
    unit = a.feed(end("9", 3, 210, {"app.sensor_readings": 3}))[0]
    orders = [e.total_order for e in unit.events]
    assert orders == [1, 2, 3]
    assert len({(e.lsn, e.txn_id, e.total_order) for e in unit.events}) == 3
