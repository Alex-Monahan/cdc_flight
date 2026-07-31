"""Rubric 1.5 — a truncate must mean the same thing in every storage mode.

Codex finding 2. There were two truncate dispatchers: in-memory events went through
`Applier._collect_truncate`, which applied `CDC_TRUNCATE_MODE`, appended the audit
marker and moved the counters; staged (spilled) events were loaded straight into
`_collect_prepared`, below that layer, and unconditionally emptied the table.

Measured with `unit_spill_events=1`:

| mode | rows | marker | counters |
|---|---|---|---|
| `log` | table **emptied**, contrary to policy | none | `0 / 0` |
| `replicate` | emptied (correct) | **none** | `0 / 0` |

Storage decides where bytes live. It must never decide semantics, so this file is a
matrix over `{memory, spill} x {replicate, log}` asserting rows, marker, counters and
`rows_removed` are identical — plus the repeated-truncate audit, which aliased one
mutable plan across both markers and reported the same `rows_removed` twice.
"""

from __future__ import annotations

import pytest
from applier_lab import DATASET, Lab, end, keyed, truncate

from cdc_flight import faults

CUSTOMERS = "cdcflight_app_customers"

#: `unit_spill_events=1` forces every unit through `_cdc_flight.spill_events`.
SPILL = {"unit_spill_events": 1, "unit_spill_bytes": 1}
STORAGE = [pytest.param({}, id="memory"), pytest.param(SPILL, id="spill")]


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


def txn(number: str, events: list) -> list:
    counts: dict[str, int] = {}
    for event in events:
        name = f"{event.schema}.{event.table}"
        counts[name] = counts.get(name, 0) + 1
    commit_lsn = max(e.lsn or 0 for e in events) + 1
    return [*events, end(number, len(events), commit_lsn, counts)]


def preload(box: Lab, count: int = 3) -> None:
    box.run(txn("1", [keyed("1", i + 1, 10 + i, i + 1, f"c{i + 1}") for i in range(count)]))


def rows(box: Lab) -> list[tuple]:
    return box.q(f'SELECT id FROM "{DATASET}"."{CUSTOMERS}" ORDER BY id')


def markers(box: Lab) -> list[tuple]:
    return box.q(
        "SELECT event, source_table, applied, rows_removed FROM _cdc_flight.table_events "
        "ORDER BY commit_id, seq"
    )


def spilled(box: Lab) -> bool:
    return box.applier.spilled_events > 0


# --------------------------------------------------------------------------- #
# the matrix
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("storage", STORAGE)
def test_replicate_mode_is_identical_in_every_storage_mode(lab, storage):
    box = lab(truncate_mode="replicate", **storage)
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 70, "after")]))
    assert rows(box) == [(70,)]
    assert markers(box) == [("truncate", "customers", True, 3)]
    assert (box.applier.truncates_applied, box.applier.truncates_logged) == (1, 0)
    if storage:
        assert spilled(box), "the test did not actually spill"


@pytest.mark.parametrize("storage", STORAGE)
def test_log_mode_keeps_the_rows_in_every_storage_mode(lab, storage):
    """`truncate_mode=log` under spill **emptied the table**: the staged path never
    saw the policy at all."""
    box = lab(truncate_mode="log", **storage)
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 70, "after")]))
    assert rows(box) == [(1,), (2,), (3,), (70,)], "log mode must keep every row"
    assert markers(box) == [("truncate", "customers", False, None)]
    assert (box.applier.truncates_applied, box.applier.truncates_logged) == (0, 1)
    if storage:
        assert spilled(box), "the test did not actually spill"


@pytest.mark.parametrize("storage", STORAGE)
def test_a_lone_spilled_truncate_records_its_marker_and_counter(lab, storage):
    """A unit whose *only* event is a truncate. Under spill it produced no marker and
    left both counters at zero."""
    box = lab(**storage)
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200)]))
    assert rows(box) == []
    assert markers(box) == [("truncate", "customers", True, 3)]
    assert box.applier.truncates_applied == 1


@pytest.mark.parametrize("storage", STORAGE)
def test_two_truncates_in_one_transaction_report_what_each_removed(lab, storage):
    """`TRUNCATE; INSERT 1 row; TRUNCATE` — both markers said 3.

    Every marker held a reference to the same mutable plan and `rows_removed` was
    read only after the final write, so the audit aliased. The honest numbers are:
    the first truncate removed the 3 rows the destination held, the second removed
    the 1 row the transaction had inserted after it (which therefore never landed).
    """
    box = lab(**storage)
    preload(box)
    box.run(
        txn(
            "2",
            [
                truncate("2", 1, 200),
                keyed("2", 2, 201, 50, "doomed"),
                truncate("2", 3, 202),
                keyed("2", 4, 203, 51, "survivor"),
            ],
        )
    )
    assert rows(box) == [(51,)]
    assert markers(box) == [
        ("truncate", "customers", True, 3),
        ("truncate", "customers", True, 1),
    ]
    assert box.applier.truncates_applied == 2


@pytest.mark.parametrize("storage", STORAGE)
def test_a_multi_table_truncate_reports_per_table_counts(lab, storage):
    box = lab(**storage)
    preload(box)
    box.run(
        txn(
            "0",
            [
                keyed("0", 1, 60, 7, "o7", table="orders"),
                keyed("0", 2, 61, 8, "o8", table="orders"),
            ],
        )
    )
    box.run(txn("2", [truncate("2", 1, 200), truncate("2", 2, 200, table="orders")]))
    assert [(m[1], m[3]) for m in markers(box)] == [("customers", 3), ("orders", 2)]


@pytest.mark.parametrize("storage", STORAGE)
def test_rows_removed_is_never_unknown_for_a_replicated_truncate(lab, storage):
    """Opus Q4: DuckDB and MotherDuck both return a count for a DELETE, so a marker
    that says "unknown" means the count silently degraded."""
    box = lab(**storage)
    preload(box)
    box.run(txn("2", [truncate("2", 1, 200)]))
    removed = box.q(
        "SELECT rows_removed FROM _cdc_flight.table_events WHERE event = 'truncate' "
        "AND applied"
    )
    assert removed and all(value is not None for (value,) in removed)


def test_a_keyless_table_truncate_records_how_many_rows_it_removed(lab):
    """A keyless table is a changelog; a truncate empties it, and the marker is the
    only surviving statement of what was lost (rubric 8.2 will replay it)."""
    from applier_lab import data

    box = lab()
    box.run(
        txn(
            "1",
            [
                data("1", 1, 10, table="sensor_readings", after={"value": 1.0}),
                data("1", 2, 11, table="sensor_readings", after={"value": 2.0}),
            ],
        )
    )
    box.run(txn("2", [truncate("2", 1, 200, table="sensor_readings")]))
    assert box.q(f'SELECT count(*) FROM "{DATASET}"."cdcflight_app_sensor_readings"') == [(0,)]
    assert markers(box) == [("truncate", "sensor_readings", True, 2)]


# --------------------------------------------------------------------------- #
# faults around a spilled truncate
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("point", ["spill", "mid_apply", "pre_commit"])
def test_a_fault_around_a_spilled_truncate_leaves_every_row(lab, monkeypatch, point):
    """Codex's 9-point item 9: a crash at each boundary of a *staged* truncate.

    The staged rows live in the group's own transaction, so a rollback discards them
    with everything else and the destination keeps every row.
    """
    box = lab(**SPILL)
    preload(box)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"{point}:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 70, "after")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert rows(box) == [(1,), (2,), (3,)], "a rolled-back truncate keeps every row"
    assert markers(box) == []

    # A fault at `spill` raises out of the assembler, so `commit_group` never runs and
    # the staging transaction is still open. `drain_on_shutdown()` is what a real run
    # does next, and it rolls that transaction back explicitly - the staged rows never
    # outlive it, which is the whole reason staging happens inside the group's own
    # transaction (ADR §3.4).
    box.applier.drain_on_shutdown()
    assert box.q("SELECT count(*) FROM _cdc_flight.spill_events") == [(0,)]
    assert rows(box) == [(1,), (2,), (3,)]

    box.run(txn("2", [truncate("2", 1, 200), keyed("2", 2, 201, 70, "after")]))
    assert rows(box) == [(70,)]
    assert markers(box) == [("truncate", "customers", True, 3)]
