"""Rubric 1.8 / 4.7 — the acquisition recovery survives a crash at EVERY step.

The reproducing tests for Codex B3 and Opus MAJOR-1. The recovery mutates four
independent durable things and nothing can make them atomic, so the question is not
"can we avoid an intermediate state" but "**can the Flight recognise its own**". It
could not:

* the old order deleted the durable resume row before `offsets.dat`, and a crash in
  between left `row absent / file present` — which the Flight diagnoses as
  `orphan_offset_file` and refuses to start on, for ever, until a human passes a CLI
  flag. Opus reproduced three consecutive refusals.
* a crash after the slot was dropped lost the forced `snapshot.mode='initial'`, because
  it only ever lived in a local variable. The next run then saw no row, no file and no
  slot and called it an ordinary fresh start (Codex B3).

Both are now closed by a durable journal written **before** any mutation, with every
step idempotent and re-entrant from the recorded phase. These tests cut at every phase
boundary and prove the next attempt finishes the job.

They are in the DEFAULT suite and cost milliseconds: `recovery.resume()` takes the slot
drop as a parameter, so the whole state machine is exercisable without a live cluster.
The end-to-end pairing (a real crash, a real Postgres slot) is
`test_1_8_recovery_crash_e2e.py`.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import recovery as recovery_mod
from cdc_flight.errors import ReconciliationRefused, RecoveryFailed
from cdc_flight.reconcile import reconcile
from cdc_flight.recovery import (
    PHASE_ARMED,
    PHASE_FILE_DELETED,
    PHASE_REQUESTED,
    PHASE_ROW_DELETED,
)

PIPELINE = "recovery_sm"
NAMESPACE = "recovery_sm_ns"
DATASET = "cdc_raw"
TABLES = [
    ("app", "customers", "cdcflight_app_customers"),
    ("app", "orders", "cdcflight_app_orders"),
    ("app", "audit_log", "cdcflight_app_audit_log"),
]


class _World:
    """A destination + an offsets file + a source slot, all fake but all real state."""

    def __init__(self, tmp_path: Path, *, slot_present: bool = True):
        self.con = duckdb.connect(str(tmp_path / "dest.duckdb"))
        dest_mod.ensure_control_schema(self.con)
        dest_mod.ensure_dataset(self.con, DATASET)
        self.offset_path = tmp_path / "offsets.dat"
        self.offset_path.write_bytes(b"\x00not-a-real-offset-map")
        self.slots = {"cdc_slot"} if slot_present else set()
        self.drop_calls = 0
        self.drop_raises: Exception | None = None
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

    # -- the injectable slot drop ---------------------------------------- #
    def drop_slot(self, dsn: str, slot_name: str) -> str:
        self.drop_calls += 1
        if self.drop_raises is not None:
            raise self.drop_raises
        if slot_name in self.slots:
            self.slots.discard(slot_name)
            return "dropped"
        return "absent"

    # -- observation ------------------------------------------------------ #
    @property
    def durable_rows(self) -> int:
        return self.con.execute(
            "SELECT count(*) FROM _cdc_flight.debezium_offsets WHERE pipeline = ?",
            [PIPELINE],
        ).fetchone()[0]

    @property
    def owed(self) -> list[str]:
        return [
            f"{s}.{t}"
            for s, t, _ in dest_mod.tables_awaiting_snapshot(self.con, PIPELINE)
        ]

    def journal(self):
        return recovery_mod.read(self.con, pipeline=PIPELINE, namespace=NAMESPACE)

    def begin(self, decision: str = "slot_ahead_of_destination"):
        return recovery_mod.begin(
            self.con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            decision=decision,
            message="the slot is ahead of the destination",
            slot_name="cdc_slot",
            offset_path=self.offset_path,
            captured_tables=TABLES,
            forget_catalog=False,
        )

    def resume(self, *, crash_before: str | None = None):
        record = self.journal()
        assert record is not None

        def _cut(phase: str) -> None:
            if phase == crash_before:
                raise _Crash(phase)

        return recovery_mod.resume(
            self.con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            record=record,
            dsn="postgresql://unused",
            drop_slot=self.drop_slot,
            on_phase=_cut,
        )

    def close(self):
        self.con.close()


class _Crash(RuntimeError):
    """Stands in for `kill -9` between two durable actions."""


@pytest.fixture
def world(tmp_path):
    w = _World(tmp_path)
    try:
        yield w
    finally:
        w.close()


# --------------------------------------------------------------------------- #
# the intent is durable before anything is destroyed
# --------------------------------------------------------------------------- #
def test_the_intent_is_written_before_any_mutation(world):
    record = world.begin()
    assert record.phase == PHASE_REQUESTED
    assert sorted(world.owed) == ["app.audit_log", "app.customers", "app.orders"]
    assert record.tables_marked == 3, "the count must be rows verified, not inputs"
    # NOTHING has been destroyed yet.
    assert world.offset_path.exists()
    assert world.durable_rows == 1
    assert world.slots == {"cdc_slot"}
    # And the forced snapshot mode is durable, not a local variable.
    assert world.journal().snapshot_mode == recovery_mod.FORCED_SNAPSHOT_MODE


def test_the_marking_and_the_journal_are_one_transaction(world, monkeypatch):
    """If the journal cannot be written, the tables must not be left marked."""
    real = dest_mod.request_snapshot

    def _mark_then_explode(*args, **kwargs):
        real(*args, **kwargs)
        raise RuntimeError("destination went away mid-transaction")

    monkeypatch.setattr(recovery_mod, "request_snapshot", _mark_then_explode)
    with pytest.raises(RuntimeError):
        world.begin()
    assert world.journal() is None
    assert world.owed == [], "the to-do list must not outlive the journal that explains it"


# --------------------------------------------------------------------------- #
# a crash at every phase boundary
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "cut", [PHASE_FILE_DELETED, PHASE_ROW_DELETED, PHASE_ARMED, None]
)
def test_a_crash_at_any_phase_boundary_is_resumable(world, cut):
    """Cut between every pair of durable actions; the next attempt finishes the job."""
    world.begin()
    if cut is not None:
        with pytest.raises(_Crash):
            world.resume(crash_before=cut)

    # --- what a *new process* sees, and what it must do with it ------------
    surviving = world.journal()
    assert surviving is not None, "the journal is the whole point"
    result = world.resume()

    assert result["phase"] == PHASE_ARMED
    assert world.offset_path.exists() is False
    assert world.durable_rows == 0
    assert world.slots == set(), "the slot must be gone before any snapshot starts"
    assert sorted(world.owed) == ["app.audit_log", "app.customers", "app.orders"]
    # The forced snapshot mode survived every cut.
    assert world.journal().snapshot_mode == recovery_mod.FORCED_SNAPSHOT_MODE


def test_resuming_twice_changes_nothing(world):
    world.begin()
    first = world.resume()
    calls = world.drop_calls
    second = world.resume()
    assert first["phase"] == second["phase"] == PHASE_ARMED
    assert world.drop_calls == calls, "an armed recovery does not re-drop the slot"
    assert world.durable_rows == 0


def test_the_stranding_window_no_longer_exists(world):
    """Opus MAJOR-1, reproduced and closed.

    The old order deleted the durable resume row and then the file. A crash between
    them left `row absent / file present`, which reconciliation refuses as
    `orphan_offset_file` for ever. The file now goes FIRST, so the one unjournalled
    intermediate state is `file absent / row present` — which reconciliation simply
    rebuilds.
    """
    world.begin()
    # `crash_before=X` cuts after X's durable effect and before X is recorded, so this
    # is the instant right after `offsets.dat` was unlinked.
    with pytest.raises(_Crash):
        world.resume(crash_before=PHASE_FILE_DELETED)

    # This is the exact cut that used to strand the pipeline — in the old order it was
    # `row absent / file present`, which is the orphan refusal.
    assert world.offset_path.exists() is False
    assert world.durable_rows == 1

    outcome = reconcile(
        world.con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        offset_path=world.offset_path,
        dsn=None,
        slot_name=None,
    )
    assert outcome.decision != "orphan_offset_file"
    assert outcome.decision.startswith("file_missing")


def test_the_old_order_is_what_produced_the_permanent_refusal(world):
    """The counter-proof: construct `row gone / file present` and watch it refuse.

    Kept as a test rather than a comment so the claim "this order matters" is checked
    rather than asserted. If someone ever restores the old order, the test above starts
    reproducing this behaviour and this test documents what they will get.
    """
    world.con.execute(
        "DELETE FROM _cdc_flight.debezium_offsets WHERE pipeline = ?", [PIPELINE]
    )
    assert world.offset_path.exists()
    with pytest.raises(ReconciliationRefused) as raised:
        reconcile(
            world.con,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
            offset_path=world.offset_path,
            dsn=None,
            slot_name=None,
        )
    assert "REFUSING TO START" in str(raised.value)


# --------------------------------------------------------------------------- #
# the one step that may not be stepped over (Codex B4)
# --------------------------------------------------------------------------- #
def test_a_slot_that_will_not_drop_fails_the_recovery(world):
    """It used to be recorded as the string `drop_failed: ...` and stepped over.

    A45: Debezium only pairs the snapshot with an exact WAL position when it creates
    the slot itself. Re-snapshotting against a surviving slot resumes the stream from a
    `confirmed_flush_lsn` we cannot account for — past the snapshot's consistent point,
    which is the loss window rubric 1.8 exists to close.
    """
    world.begin()
    world.drop_raises = RuntimeError('replication slot "cdc_slot" is active for PID 42')
    with pytest.raises(RecoveryFailed) as raised:
        world.resume()
    assert "cdc_slot" in str(raised.value)
    assert "consistent point" in str(raised.value)

    # The journal is intact at the phase that failed, so the next run retries it.
    assert world.journal().phase == PHASE_ROW_DELETED
    assert world.slots == {"cdc_slot"}

    # ... and it does, once the slot is free.
    world.drop_raises = None
    result = world.resume()
    assert result["phase"] == PHASE_ARMED
    assert world.slots == set()


def test_an_absent_slot_is_an_acceptable_drop_outcome(tmp_path):
    """`absent` proves the slot is gone just as well as `dropped` does."""
    world = _World(tmp_path, slot_present=False)
    try:
        world.begin()
        result = world.resume()
        assert result["slot"] == "absent"
        assert result["phase"] == PHASE_ARMED
    finally:
        world.close()


def test_a_forgotten_catalog_is_part_of_the_same_transaction(world):
    world.con.execute(
        "INSERT INTO _cdc_flight.source_relations (pipeline, source_schema, source_table, "
        "relation_oid, published, first_seen_at, last_seen_at) "
        "VALUES (?, 'app', 'customers', 1234, true, now(), now())",
        [PIPELINE],
    )
    recovery_mod.begin(
        world.con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        decision="source_timeline_changed",
        message="the timeline forked",
        slot_name="cdc_slot",
        offset_path=world.offset_path,
        captured_tables=TABLES,
        forget_catalog=True,
    )
    remaining = world.con.execute(
        "SELECT count(*) FROM _cdc_flight.source_relations WHERE pipeline = ?",
        [PIPELINE],
    ).fetchone()[0]
    assert remaining == 0
    assert world.journal().forget_catalog is True
