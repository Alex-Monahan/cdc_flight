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
    *identical* identities. The fence is turned off here — `resume_lsn` stays 0 —
    so the merge on `cdcf_event_id` is what has to hold.
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
    # must be derived, not remembered).
    box.run(
        [
            _reading("7", 1, 100, 1.0),
            _reading("7", 2, 101, 2.0),
            end("7", 2, 102, {"app.sensor_readings": 2}),
        ]
    )
    second = box.q(f'SELECT cdcf_event_id FROM "{DATASET}"."{READINGS}" ORDER BY 1')

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
