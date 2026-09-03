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
anchor, against a real slot) is `tests/rubric/1.8_slot_mismatch/test_1_8_recovery_crash_e2e.py`
and the matrix under `-m slow`.

The exact-count recovery proof is the same one the rest of 1.7 uses: after the crash and
the next run, the destination equals the source exactly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
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


DRIVER = Path(__file__).resolve().parents[2] / "support" / "recovery_crash_driver.py"


class _World:
    """A destination, an offsets file and a source slot: all fake, all real state."""

    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.duckdb_path = tmp_path / "dest.duckdb"
        self.slots_path = tmp_path / "slots.json"
        self.con = duckdb.connect(str(self.duckdb_path))
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
            self._save_slots()
            return "dropped"
        return "absent"

    # -- the hard-death seam ------------------------------------------------ #
    def _save_slots(self) -> None:
        self.slots_path.write_text(json.dumps(sorted(self.slots)))

    def crash_at(self, point: str, step: str, *, nth: int = 1, exit_code: int = 137):
        """Run one recovery step in a CHILD process that really dies at `point`.

        `os._exit` skips every `except`, `finally` and `atexit` hook, which is the
        difference between "the resume logic is re-entrant" and "a hard-killed process
        leaves resumable durable state" (Codex r1 MAJOR-6). The parent's DuckDB handle
        is closed first — one writer per file — and reopened afterwards.
        """
        self._save_slots()
        self.con.close()
        try:
            env = dict(os.environ)
            env["CDC_FAULT_INJECT"] = f"{point}:{nth}"
            env["CDC_STATE_DIR"] = str(self.root / "state")
            env["PYTHONPATH"] = os.pathsep.join(
                [str(Path(__file__).resolve().parents[3] / "src"), env.get("PYTHONPATH", "")]
            )
            proc = subprocess.run(
                [
                    sys.executable, str(DRIVER), str(self.duckdb_path),
                    str(self.offset_path), str(self.slots_path), step,
                ],
                env=env, capture_output=True, text=True, timeout=120, check=False,
            )
        finally:
            self.con = duckdb.connect(str(self.duckdb_path))
        self.slots = set(json.loads(self.slots_path.read_text()))
        assert proc.returncode == exit_code, (
            f"{point} at step {step!r} did not hard-exit {exit_code}: "
            f"rc={proc.returncode}\nstdout={proc.stdout[-2000:]}\n"
            f"stderr={proc.stderr[-2000:]}"
        )
        return proc

    def journal(self):
        return recovery_mod.read(self.con, pipeline=PIPELINE, namespace=NAMESPACE)

    def begin(self):
        slot_receipt = dest_mod.write_slot_state(
            self.con,
            pipeline=PIPELINE,
            slot_name="cdc_slot",
            observation={},
            verdict="slot_ahead_of_destination",
            verdict_message="the slot is ahead of the destination",
        )
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
            slot_receipt=slot_receipt,
            logical_message_dataset="cdc_raw",
        )

    def resume(self):
        record = self.journal()
        assert record is not None
        return recovery_mod.resume(
            self.con, pipeline=PIPELINE, namespace=NAMESPACE, record=record,
            dsn="postgresql://unused", drop_slot=self.drop_slot,
            logical_message_dataset="cdc_raw",
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
def test_a_hard_death_at_a_recovery_anchor_leaves_a_resumable_journal(world, point):
    """A REAL `os._exit` at every recovery boundary, in a child process.

    All four cuts used to be taken with `:raise`, which is Python exception unwinding:
    the caller's `try` runs, the destination connection closes cleanly, the interpreter
    tidies up. Only `recovery_armed` had a hard-death pairing, and it was in the slow
    lane. The claim under test is that DURABLE STATE ALONE is enough after a crash, and
    a raised exception does not test it (Codex r1 MAJOR-6).
    """
    if point != "recovery_requested":
        world.begin()
    world.crash_at(point, "begin" if point == "recovery_requested" else "resume")

    # The anchor recorded itself, fsynced, where a test can read it even after os._exit.
    fired = faults.read_fired_record(world.root / "state")
    assert fired is not None and fired["point"] == point, fired
    assert fired["pid"] != os.getpid(), "the cut has to happen in the process that dies"

    surviving = world.journal()
    assert surviving is not None, "the journal is the whole point"
    assert surviving.phase == ANCHOR_PHASE[point]
    # The forced snapshot mode survived the cut: it used to live only in a local
    # variable, so a crash after the slot was dropped lost it (Codex B3).
    assert surviving.snapshot_mode == recovery_mod.FORCED_SNAPSHOT_MODE
    # And the obligation itself, which is what the completion predicate is about.
    assert sorted(surviving.captured) == ["app.customers", "app.orders"]

    # ... and the next attempt finishes the job, from durable state alone.
    result = world.resume()
    assert result["phase"] == PHASE_ARMED
    assert world.offset_path.exists() is False
    assert world.durable_rows == 0
    assert world.slots == set(), "the slot must be gone before any snapshot starts"
    assert world.owed == ["app.customers", "app.orders"]


@pytest.mark.parametrize("point", sorted(ANCHOR_PHASE))
def test_the_same_anchor_also_unwinds_cleanly_as_an_exception(world, monkeypatch, point):
    """The `:raise` action, kept as the *error teardown* path rather than as the crash.

    ADR 0001 §1.2: an exception drives Debezium's error-teardown lifecycle, which is a
    different (and differently dangerous) path from hard death. Both are covered; the
    test above is the one that carries the rubric claim.
    """
    if point != "recovery_requested":
        world.begin()
    _arm(monkeypatch, point)
    with pytest.raises(faults.InjectedFault):
        world.begin() if point == "recovery_requested" else world.resume()
    _disarm(monkeypatch)
    surviving = world.journal()
    assert surviving is not None and surviving.phase == ANCHOR_PHASE[point]
    assert world.resume()["phase"] == PHASE_ARMED


def test_the_journal_and_the_to_do_list_survive_or_die_together(world):
    """`table_rebuild_queued` fires INSIDE `begin()`'s transaction, MID-WRITE.

    The anchor is now placed after the FIRST captured table has taken its
    `-> awaiting_snapshot` edge and before the second has (Codex r1 MAJOR-6); it used to
    fire before the loop, so it proved a pre-write rollback rather than a torn queue.
    The to-do list and the journal that explains it are written in one destination
    transaction on purpose: a hard death while the queue is half-written must leave
    neither, or the next run finds tables marked owed with nothing recording why.
    """
    world.crash_at("table_rebuild_queued", "begin")

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


# --------------------------------------------------------------------------- #
# rubric 1.9's catalog-baseline machine has crash cuts too (rev 14)
# --------------------------------------------------------------------------- #
def _baseline_row(world):
    return world.con.execute(
        "SELECT state FROM _cdc_flight.catalog_baseline WHERE pipeline = ?", [PIPELINE]
    ).fetchall()


def test_a_death_at_the_baseline_mark_leaves_the_obligation_durable(world):
    """The cut the whole machine exists for.

    The mark is written BEFORE the engine starts precisely so that a process which
    dies anywhere after it still says "this run did not confirm the catalog baseline".
    A hard death here is the strongest form of that claim: `os._exit` runs no `except`,
    no `finally` and no `atexit` hook, so nothing tidied up on the way out.
    """
    proc = world.crash_at("catalog_baseline_marked", "baseline")
    record = faults.read_fired_record(world.root / "state")
    assert record and record["point"] == "catalog_baseline_marked", (proc.stdout, record)
    assert [r[0] for r in _baseline_row(world)] == ["stale"], (
        "a hard-killed run left NO durable statement that the baseline was unconfirmed, "
        "which is exactly the state a later healthy run then adopts an oid over"
    )


def test_a_death_before_the_promotion_leaves_it_unconfirmed_and_repeatable(world):
    """The other cut: the learned relations are durable and the promotion is not.

    The promotion has to be idempotent rather than one-shot — the next run recomputes
    the same verdict from durable state — so re-running it after the crash must reach
    `valid` with no special handling.
    """
    from cdc_flight import catalog_baseline

    proc = world.crash_at("catalog_baseline_pre_valid", "baseline")
    record = faults.read_fired_record(world.root / "state")
    assert record and record["point"] == "catalog_baseline_pre_valid", (proc.stdout, record)
    assert [r[0] for r in _baseline_row(world)] == ["stale"]

    check = catalog_baseline.mark_unconfirmed(
        world.con, pipeline=PIPELINE, dataset=DATASET, runner_id="retry"
    )
    check = catalog_baseline.confirm(
        world.con, pipeline=PIPELINE, dataset=DATASET, check=check, successful_polls=1,
        runner_id="retry",
    )
    assert check.valid
    assert [r[0] for r in _baseline_row(world)] == ["valid"]
