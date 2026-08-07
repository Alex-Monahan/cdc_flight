"""Rubric 1.1 / 1.2 / 1.3 — the spill and snapshot paths, driven directly.

These are the default-suite guards for the four blockers the 1.1-1.3 reviews
reproduced (Opus B-1, Codex 1 / 2 / 5, Codex 6's mis-placed `mid_apply`). Every
one of them needs an exact interleaving of assembler and applier state, which a
subprocess run cannot pin down, and every one of them coexisted with a fully
green suite. They run against the shipped `Applier` and a real DuckDB file — see
`tests/support/applier_lab.py` — so they cost milliseconds rather than the ~40 s a
crash/recovery cycle costs.

The properties asserted here:

| property | why it is load-bearing |
|---|---|
| a spilled prefix is applied **before** the in-memory tail | otherwise the earlier value wins and the later change event is lost (Opus B-1) |
| total order is preserved across a whole group, not per storage mode | a group can hold `spilled+tail`, then a wholly in-memory unit (Opus B-1) |
| a table created inside the group gets **one** row per key | both passes skipped the DELETE half, so the same key landed twice |
| snapshot rows stage into the **shadow** with snapshot identity | staging into the live table exposes a partial snapshot and the swap then drops it (Codex 1) |
| a fenced unit's spilled prefix is **not** applied | the fence is set at `_add_unit`, long after staging (Codex 5) |
| `mid_apply` fires **between** two table writes | the anchor documented as "some tables written, others not" fired before the first one (Codex 6) |
"""

from __future__ import annotations

import pytest
from support.applier_lab import DATASET, Lab, data, end, keyed, snap

from cdc_flight import faults


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


def _three_updates_of_one_key(txn: str = "7") -> list:
    """One PG transaction, three UPDATEs of the same primary key: a -> b -> c."""
    return [
        keyed(txn, 1, 101, 1, "a"),
        keyed(txn, 2, 102, 1, "b", op="u"),
        keyed(txn, 3, 103, 1, "c", op="u"),
        end(txn, 3, 104, {"app.customers": 3}),
    ]


# --------------------------------------------------------------------------- #
# spill ordering (Opus B-1)
# --------------------------------------------------------------------------- #
def test_a_spilled_prefix_is_applied_before_the_in_memory_tail(lab):
    """The destination must end on the LAST value, not the last *storage mode*.

    With `unit_spill_events=2` the first two events of the transaction are staged
    in `_cdc_flight.spill_events` and the third stays in memory. Applying the
    in-memory tail first and then draining runs the merge's DELETE half against
    the row the tail just wrote, so the destination ends with `b` and the `c`
    change event is gone entirely.
    """
    box = lab(unit_spill_events=2)
    target = box.target("customers")

    # A pre-existing destination table: the merge's DELETE half is live.
    box.run([keyed("6", 1, 90, 1, "a0"), end("6", 1, 91, {"app.customers": 1})])
    assert box.rows(target, "id, name") == [(1, "a0")]

    box.run(_three_updates_of_one_key())

    assert box.applier.spilled_events == 2, "the spill never fired; the test is vacuous"
    assert box.rows(target, "id, name") == [(1, "c")], (
        "the spilled prefix was applied after the in-memory tail, so the earlier "
        "value won and the later change event was lost"
    )


def test_a_table_created_inside_the_group_gets_one_row_per_key(lab):
    """Both passes see `fresh=True`, so both skip the DELETE half of the merge."""
    box = lab(unit_spill_events=2)
    box.run(_three_updates_of_one_key())

    assert box.applier.spilled_events == 2, "the spill never fired; the test is vacuous"
    rows = box.rows(box.target("customers"), "id, name")
    assert rows == [(1, "c")], f"duplicate primary key survived a spill: {rows}"


def test_total_order_is_preserved_across_a_whole_group_not_per_unit(lab):
    """A group can hold `unit1 (spilled + tail)` then `unit2 (wholly in memory)`.

    Reordering the two phases cannot fix that: the correct order interleaves them.
    """
    box = lab(unit_spill_events=2)
    box.feed(_three_updates_of_one_key("7"))
    box.feed([keyed("8", 1, 110, 1, "d", op="u"), end("8", 1, 111, {"app.customers": 1})])
    box.commit()

    assert box.applier.spilled_events == 2
    assert box.rows(box.target("customers"), "id, name") == [(1, "d")]


def test_a_keyless_spilled_event_keeps_the_connector_identity(lab):
    """`cdcf_event_id` must stay `<lsn>:<txId>:<total_order>` through staging.

    Substituting a local spill sequence for a missing ordinal (Codex 4) would
    make a replay recompute a *different* identity, which is exactly what makes
    the keyless table de-duplicable.
    """
    box = lab(unit_spill_events=2)
    records = [
        data("7", i, 100 + i, table="sensor_readings", after={"value": float(i)})
        for i in range(1, 4)
    ]
    box.run([*records, end("7", 3, 110, {"app.sensor_readings": 3})])

    rows = box.rows(box.target("sensor_readings"), "cdcf_event_id, cdcf_total_order", "2")
    assert rows == [(f"{100 + i}:7:{i}", i) for i in range(1, 4)], rows


# --------------------------------------------------------------------------- #
# the fence and the spilled prefix (Codex 5)
# --------------------------------------------------------------------------- #
def test_a_fenced_units_spilled_prefix_is_not_applied(lab):
    """A9 claims "the fence alone prevents duplication". It has to include spill.

    Rows are staged while the unit is still open; the fence is set in
    `_add_unit`, which happens at its `END`. Draining unconditionally therefore
    re-applies the prefix of a transaction the destination already holds.
    """
    box = lab(unit_spill_events=2, resume_lsn=10_000)
    box.feed(_three_updates_of_one_key())
    assert box.applier.spilled_events == 2, "the spill never fired; the test is vacuous"
    box.commit()

    assert box.applier.fenced_units == 1, "the unit was not fenced; the test is vacuous"
    assert box.applier.applied_events == 0, (
        f"a fenced unit applied {box.applier.applied_events} events from its spilled prefix"
    )
    assert not box.exists(box.target("customers")), (
        "a fenced unit's spilled prefix was written to the destination"
    )
    assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0, (
        "staged rows of a fenced unit were left behind"
    )


def test_a_group_that_is_only_a_fenced_spill_is_not_a_data_group(lab):
    """`has_data` must not be forced true by staged rows a fence will discard."""
    box = lab(unit_spill_events=2, resume_lsn=10_000)
    box.feed(_three_updates_of_one_key())
    box.commit()
    assert box.applier.data_commit_groups == 0, (
        "a group whose only content was a fenced unit's spilled prefix was counted "
        "as a data group, so every `<nth>`-indexed fault anchor is off by one"
    )


# --------------------------------------------------------------------------- #
# snapshot spill (Codex 1)
# --------------------------------------------------------------------------- #
def test_a_spilled_snapshot_chunk_stages_into_the_shadow_table(lab):
    """Never the live table, and never with a streaming identity.

    `_spill_events` inferred the phase from `self._snapshot`, which
    `_apply_units` only populates later, so the *first* spilled chunk of every
    snapshot was routed to the live table with a `<lsn>:None:None` identity. A
    consumer could then see a partial snapshot, and the swap dropped those rows.
    """
    box = lab(full_snapshot=True, unit_spill_events=2)
    box.feed([snap("customers", 50, ident=i) for i in range(1, 5)])

    staged = box.q(
        "SELECT DISTINCT target_table FROM _cdc_flight.spill_events ORDER BY 1"
    )
    assert staged == [(box.shadow("customers"),)], (
        f"snapshot rows were staged into {staged}, not the shadow table"
    )
    ids = box.q("SELECT cdcf_event_id FROM _cdc_flight.spill_events ORDER BY event_seq")
    assert all(i[0].startswith("snap:") for i in ids), (
        f"spilled snapshot rows carry a streaming identity: {ids}"
    )
    assert not box.exists(box.target("customers")), (
        "the live table was created by the snapshot spill, before any swap"
    )


def test_every_chunk_of_a_spilling_snapshot_survives_the_swap(lab):
    """Multi-chunk, multi-spill, keyed: the live table holds every snapshot row."""
    box = lab(full_snapshot=True, unit_spill_events=2, snapshot_chunk_events=3)
    records = [snap("customers", 50, ident=i) for i in range(1, 6)]
    records.append(snap("customers", 50, ident=6, marker="last"))
    box.run(records)

    assert box.applier.snapshots.swaps == 1, "the shadow table was never swapped in"
    assert not box.exists(box.shadow("customers")), "the shadow table outlived the swap"
    rows = box.rows(box.target("customers"), "id", "1")
    assert [r[0] for r in rows] == list(range(1, 7)), (
        f"the swap dropped snapshot rows staged by an earlier chunk: {rows}"
    )
    ids = box.rows(box.target("customers"), "cdcf_event_id")
    assert all(i[0].startswith("snap:") for i in ids), ids
    assert len({i[0] for i in ids}) == 6, f"snapshot identities collided: {ids}"


def test_every_chunk_of_a_spilling_keyless_snapshot_survives_the_swap(lab):
    """The keyless shape: identity is `cdcf_event_id`, so a collision loses a row."""
    box = lab(full_snapshot=True, unit_spill_events=2, snapshot_chunk_events=3)
    records = [
        snap("sensor_readings", 50, value=f"v{i}", key=None) for i in range(1, 6)
    ]
    records.append(snap("sensor_readings", 50, value="v6", key=None, marker="last"))
    box.run(records)

    target = box.target("sensor_readings")
    assert box.scalar(f'SELECT count(*) FROM "{DATASET}"."{target}"') == 6
    assert box.scalar(f'SELECT count(DISTINCT cdcf_event_id) FROM "{DATASET}"."{target}"') == 6


# --------------------------------------------------------------------------- #
# the mid-apply anchor (Codex 6)
# --------------------------------------------------------------------------- #
def test_mid_apply_really_fires_between_two_table_writes(lab, monkeypatch):
    """The anchor documented as "some tables written, others not".

    It used to fire *before* the table-write loop, so it could not detect a
    transaction torn between table A and table B — the very interleaving rubric
    1.3 is about. The observation is taken inside the still-open destination
    transaction, immediately before the rollback.
    """
    monkeypatch.setenv(faults.ENV_VAR, "mid_apply:1:raise")
    faults.refresh()
    box = lab()

    observed: dict[str, int | None] = {}
    original = box.applier._rollback_quietly

    def _spy() -> None:
        for table in ("customers", "orders"):
            name = box.target(table)
            observed[table] = (
                box.scalar(f'SELECT count(*) FROM "{DATASET}"."{name}"')
                if box.exists(name)
                else None
            )
        original()

    box.applier._rollback_quietly = _spy

    records = [
        keyed("7", 1, 101, 1, "c1"),
        keyed("7", 2, 102, 2, "c2"),
        data("7", 3, 103, table="orders", key={"id": 1}, after={"id": 1, "total": 10}),
        end("7", 3, 104, {"app.customers": 2, "app.orders": 1}),
    ]
    with pytest.raises(faults.InjectedFault):
        box.run(records)

    assert observed.get("customers") == 2, (
        "the mid_apply anchor did not fire after the FIRST table write: "
        f"observed {observed}"
    )
    assert observed.get("orders") in (None, 0), (
        f"the mid_apply anchor fired after the SECOND table write too: {observed}"
    )
    # And the whole group rolled back, so the torn state is not durable.
    assert not box.exists(box.target("customers"))


def test_the_mid_apply_anchor_does_not_fire_on_a_control_only_group(lab, monkeypatch):
    """`faults.<nth>` counts data-carrying groups (`faults.py:35-41`)."""
    from applier_lab import heartbeat

    monkeypatch.setenv(faults.ENV_VAR, "mid_apply:1:raise")
    faults.refresh()
    box = lab()
    box.run([heartbeat(500)])  # must not raise
    assert box.applier.data_commit_groups == 0
