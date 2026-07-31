"""Rubric 1.7's closure — the acquisition recovery has fault anchors of its own.

**The honest hold that kept 1.7 at 4.** The recovery mutates four independent durable
things (the to-do list, `offsets.dat`, the durable resume point, the replication slot)
and nothing can make two of them atomic. The 1.6—1.8 round proved a cut at every
boundary — but through a **test seam**: `recovery.resume(on_phase=...)` raises a Python
exception where a crash would happen. That proves the *resume logic* is re-entrant. It
does not prove a hard-killed process is, and the difference is not academic:

* `os._exit` runs no `except`, no `finally`, no `atexit` hook. The applier's
  `except BaseException: reassert_owed(...)` is exactly such a handler, and the
  architecture review's finding 1 was that stepping over it left a table owed work and
  invisible to every queue.
* the seam raises *inside* `resume()`, so the caller's own `try` still unwinds; a
  `SIGKILL` leaves the JVM, the destination connection and the offsets file wherever
  they were.

So each boundary is now a real `faults.maybe_crash` anchor with a real `os._exit`, and
this file is their **default-suite guard**: each one is reachable, fires exactly where it
says it fires, and leaves durable state the next attempt finishes from — in
milliseconds, with no JVM and no Postgres, because `recovery.resume()` takes the slot
drop as a parameter. The end-to-end pairing (a real `cdc-flight` process killed at the
anchor, against a real slot) is `tests/1.8_slot_mismatch/test_1_8_recovery_crash_e2e.py`
and the matrix under `-m slow`.

The exact-count recovery proof is the same one the rest of 1.7 uses: after the crash and
the next run, the destination equals the source exactly.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import faults
from cdc_flight import recovery as recovery_mod
from cdc_flight.recovery import (
    PHASE_ARMED,
    PHASE_FILE_DELETED,
    PHASE_REQUESTED,
    PHASE_ROW_DELETED,
)

PIPELINE = "recovery_anchors"
NAMESPACE = "recovery_anchors_ns"
DATASET = "cdc_raw"
TABLES = [
    ("app", "customers", "cdcflight_app_customers"),
    ("app", "orders", "cdcflight_app_orders"),
]

#: anchor -> the journal phase a crash there must leave behind.
#:
#: `recovery_requested` fires AFTER the journal row commits, so the surviving phase is
#: `requested`. Each later anchor fires after its own durable effect and BEFORE the
#: phase is recorded, which is the dangerous side of the cut: the effect has happened
#: and the journal does not know it. The resume ladder is written to be idempotent
#: precisely so that re-running the recorded phase is free.
ANCHOR_PHASE = {
    "recovery_requested": PHASE_REQUESTED,
    "recovery_offsets_file_deleted": PHASE_REQUESTED,
    "recovery_resume_point_deleted": PHASE_FILE_DELETED,
    "recovery_armed": PHASE_ROW_DELETED,
}


class _World:
    """A destination, an offsets file and a source slot: all fake, all real state."""

    def __init__(self, tmp_path: Path):
        self.con = duckdb.connect(str(tmp_path / "dest.duckdb"))
        dest_mod.ensure_control_schema(self.con)
        dest_mod.ensure_dataset(self.con, DATASET)
        self.offset_path = tmp_path / "offsets.dat"
        self.offset_path.write_bytes(b"\x00not-a-real-offset-map")
        self.slots = {"cdc_slot"}
        for schema, table, target in TABLES:
            self.con.execute(
                "INSERT INTO _cdc_flight.table_state (pipeline, source_schema, "
                "source_table, target_table, snapshot_state) VALUES (?,?,?,?,'complete')",
                [PIPELINE, schema, table, target],
            )
        self.con.execute(
            "INSERT INTO _cdc_flight.debezium_offsets (pipeline, namespace, resume_json, "
            "commit_id, last_lsn, snapshot_epoch, updated_at) "
            "VALUES (?,?,'{\"partition\":{},\"offset\":{},\"last_lsn\":4242}',7,4242,1,now())",
            [PIPELINE, NAMESPACE],
        )

    def drop_slot(self, dsn: str, slot_name: str) -> str:
        if slot_name in self.slots:
            self.slots.discard(slot_name)
            return "dropped"
        return "absent"

    def journal(self):
        return recovery_mod.read(self.con, pipeline=PIPELINE, namespace=NAMESPACE)

    def begin(self):
        return recovery_mod.begin(
            self.con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            decision="slot_ahead_of_destination",
            message="the slot is ahead of the destination",
            slot_name="cdc_slot",
            offset_path=self.offset_path,
            captured_tables=TABLES,
            forget_catalog=False,
        )

    def resume(self):
        record = self.journal()
        assert record is not None
        return recovery_mod.resume(
            self.con, pipeline=PIPELINE, namespace=NAMESPACE, record=record,
            dsn="postgresql://unused", drop_slot=self.drop_slot,
        )

    @property
    def durable_rows(self) -> int:
        return self.con.execute(
            "SELECT count(*) FROM _cdc_flight.debezium_offsets WHERE pipeline = ?",
            [PIPELINE],
        ).fetchone()[0]

    @property
    def owed(self) -> list[str]:
        return sorted(
            f"{s}.{t}" for s, t, _ in dest_mod.tables_awaiting_snapshot(self.con, PIPELINE)
        )

    def close(self):
        self.con.close()


@pytest.fixture
def world(tmp_path, monkeypatch):
    faults.reset_arrivals()
    monkeypatch.setenv("CDC_STATE_DIR", str(tmp_path / "state"))
    w = _World(tmp_path)
    try:
        yield w
    finally:
        w.close()
        faults.reset_arrivals()


def _arm(monkeypatch, point: str) -> None:
    monkeypatch.setenv(faults.ENV_VAR, f"{point}:1:raise")
    faults.refresh()


def _disarm(monkeypatch) -> None:
    monkeypatch.delenv(faults.ENV_VAR, raising=False)
    faults.refresh()


# --------------------------------------------------------------------------- #
# every recovery anchor exists, parses and is addressable
# --------------------------------------------------------------------------- #
def test_the_recovery_anchors_are_enumerated_in_ALL_POINTS():
    assert set(faults.RECOVERY_POINTS) <= set(faults.ALL_POINTS)
    assert set(ANCHOR_PHASE) <= set(faults.RECOVERY_POINTS)
    assert "table_rebuild_queued" in faults.RECOVERY_POINTS


@pytest.mark.parametrize("point", faults.RECOVERY_POINTS)
def test_every_recovery_anchor_parses(point, monkeypatch):
    monkeypatch.setenv(faults.ENV_VAR, f"{point}:2")
    assert faults.refresh() == (point, 2, faults.DEFAULT_EXIT_CODE)


# --------------------------------------------------------------------------- #
# each anchor fires where it says it fires, and the next attempt finishes the job
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("point", sorted(ANCHOR_PHASE))
def test_a_crash_at_a_recovery_anchor_leaves_a_resumable_journal(world, monkeypatch, point):
    world.begin() if point != "recovery_requested" else None
    if point == "recovery_requested":
        _arm(monkeypatch, point)
        with pytest.raises(faults.InjectedFault):
            world.begin()
        _disarm(monkeypatch)
    else:
        _arm(monkeypatch, point)
        with pytest.raises(faults.InjectedFault):
            world.resume()
        _disarm(monkeypatch)

    # The anchor recorded itself, fsynced, where a test can read it even after os._exit.
    fired = faults.read_fired_record(Path(world.offset_path).parent / "state")
    assert fired is not None and fired["point"] == point, fired

    surviving = world.journal()
    assert surviving is not None, "the journal is the whole point"
    assert surviving.phase == ANCHOR_PHASE[point]
    # The forced snapshot mode survived the cut: it used to live only in a local
    # variable, so a crash after the slot was dropped lost it (Codex B3).
    assert surviving.snapshot_mode == recovery_mod.FORCED_SNAPSHOT_MODE

    # ... and the next attempt finishes the job, from durable state alone.
    result = world.resume()
    assert result["phase"] == PHASE_ARMED
    assert world.offset_path.exists() is False
    assert world.durable_rows == 0
    assert world.slots == set(), "the slot must be gone before any snapshot starts"
    assert world.owed == ["app.customers", "app.orders"]


def test_the_journal_and_the_to_do_list_survive_or_die_together(world, monkeypatch):
    """`table_rebuild_queued` fires INSIDE `begin()`'s transaction.

    The to-do list and the journal that explains it are written in one destination
    transaction on purpose. A crash while the queue is being written must leave neither,
    or the next run finds tables marked owed with nothing recording why.
    """
    _arm(monkeypatch, "table_rebuild_queued")
    with pytest.raises(faults.InjectedFault):
        world.begin()
    _disarm(monkeypatch)

    assert world.journal() is None
    assert world.owed == [], "the to-do list must not outlive the journal"
    assert world.offset_path.exists(), "nothing destructive happened"
    assert world.durable_rows == 1
    assert world.slots == {"cdc_slot"}

    # And the retry works.
    world.begin()
    assert world.owed == ["app.customers", "app.orders"]


def test_the_nth_counter_addresses_the_second_recovery_of_a_process(world, monkeypatch):
    """`<nth>` is per-boundary-arrival, not per commit group.

    A run that resumes one recovery and then arms a second reaches the same boundary
    twice, and a test has to be able to name which. An index that is a function of the
    workload is one that silently stops firing (Opus M7).
    """
    world.begin()
    world.resume()  # first recovery: reaches every boundary once
    recovery_mod.clear(world.con, pipeline=PIPELINE, namespace=NAMESPACE)
    world.offset_path.write_bytes(b"\x00again")

    monkeypatch.setenv(faults.ENV_VAR, "recovery_offsets_file_deleted:2:raise")
    faults.refresh()
    world.begin()
    with pytest.raises(faults.InjectedFault):
        world.resume()
    _disarm(monkeypatch)
    assert world.journal().phase == PHASE_REQUESTED


def test_an_unarmed_recovery_runs_straight_through(world):
    """The anchors are inert unless `CDC_FAULT_INJECT` names one."""
    world.begin()
    assert world.resume()["phase"] == PHASE_ARMED
    assert world.journal().phase == PHASE_ARMED
