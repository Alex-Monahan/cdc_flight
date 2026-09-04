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

import json
from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import reconcile as reconcile_module
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
        slot_receipt = dest_mod.write_slot_state(
            self.con,
            pipeline=PIPELINE,
            slot_name="cdc_slot",
            observation={},
            verdict="fresh_start" if decision == recovery_mod.RESET_DECISION else decision,
            verdict_message="the slot is ahead of the destination",
        )
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
            slot_receipt=slot_receipt,
            logical_message_dataset="cdc_raw",
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
            logical_message_dataset=DATASET,
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


def _recovery_alerts(world):
    return world.con.execute(
        "SELECT code, context FROM _cdc_flight.alerts "
        "WHERE pipeline = ? AND code = 'operator_reset' ORDER BY raised_at",
        [PIPELINE],
    ).fetchall()


def test_prejournal_failure_that_is_never_retried_still_projects_one_real_alert(
    world, monkeypatch
):
    """A failed journal transaction may not make the operator signal disappear."""
    real_request_snapshot = recovery_mod.request_snapshot

    def _mark_then_fail(*args, **kwargs):
        real_request_snapshot(*args, **kwargs)
        raise RuntimeError("destination went away before the recovery journal")

    monkeypatch.setattr(recovery_mod, "request_snapshot", _mark_then_fail)
    with pytest.raises(RuntimeError, match="before the recovery journal"):
        world.begin(decision="operator_reset")

    rows = _recovery_alerts(world)
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    assert payload["recovery_begin_pending"] is True
    assert world.journal() is None
    assert world.owed == []


def test_prejournal_failure_then_successful_retry_projects_one_real_alert(
    world, monkeypatch
):
    """The pending pre-journal identity collapses a successful retry onto its alert."""
    real_request_snapshot = recovery_mod.request_snapshot

    def _mark_then_fail(*args, **kwargs):
        real_request_snapshot(*args, **kwargs)
        raise RuntimeError("destination went away before the recovery journal")

    monkeypatch.setattr(recovery_mod, "request_snapshot", _mark_then_fail)
    with pytest.raises(RuntimeError, match="before the recovery journal"):
        world.begin(decision="operator_reset")

    monkeypatch.setattr(recovery_mod, "request_snapshot", real_request_snapshot)
    record = world.begin(decision="operator_reset")

    rows = _recovery_alerts(world)
    assert record.phase == PHASE_REQUESTED
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    assert payload["recovery_begin_pending"] is False
    assert payload["recovery_journal_id"] == record.recovery_id
    assert world.journal().recovery_id == record.recovery_id


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


def test_operator_reset_resume_self_heals_a_malformed_durable_resume_row(
    world, monkeypatch
):
    """A reset successor must not reparse the row that reset intentionally deletes."""
    world.begin(decision=recovery_mod.RESET_DECISION)
    world.con.execute(
        "UPDATE _cdc_flight.debezium_offsets SET resume_json = ? WHERE pipeline = ?",
        ["{malformed reset state", PIPELINE],
    )

    def drop(_dsn, _slot_name, *, authorization):
        assert authorization.after_lsn is None
        return "dropped"

    monkeypatch.setattr(reconcile_module, "drop_slot", drop)
    result = recovery_mod.resume(
        world.con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        record=world.journal(),
        dsn="postgresql://unused",
        logical_message_dataset=DATASET,
        source_publication_name="cdc_flight_pub",
    )

    assert result["phase"] == PHASE_ARMED
    assert result["slot"] == "dropped"
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
    slot_receipt = dest_mod.write_slot_state(
        world.con,
        pipeline=PIPELINE,
        slot_name="cdc_slot",
        observation={},
        verdict="source_timeline_changed",
        verdict_message="the timeline forked",
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
        slot_receipt=slot_receipt,
        logical_message_dataset="cdc_raw",
    )
    remaining = world.con.execute(
        "SELECT count(*) FROM _cdc_flight.source_relations WHERE pipeline = ?",
        [PIPELINE],
    ).fetchone()[0]
    assert remaining == 0
    assert world.journal().forget_catalog is True


# --------------------------------------------------------------------------- #
# durable non-terminal states, and the phase domain
# (findings 1-3 of the parallel state-machine architecture review)
# --------------------------------------------------------------------------- #
def test_a_table_left_mid_snapshot_is_owed_work_after_ANY_crash(world):
    """`in_progress` is durable, non-terminal, and used to belong to no queue.

    It is written the instant a table's first snapshot record arrives and cleared only
    by the swap, so a process that dies inside a snapshot leaves it behind. The only
    thing that ever recovered from it was the applier's `except BaseException` — which
    `os._exit` (the fault injector, the commit watchdog) and `SIGKILL` both step over.

    The consequence was concrete: the recovery journal's "no table owes a snapshot any
    more" test could pass, and the run could log "recovery COMPLETE: every captured
    table has a fresh image", over a table that was half built.

    Rubric 1.9 closed the *class* as well as the case: `tables_awaiting_snapshot()` now
    selects every NON-TERMINAL `TableLifecycle` state rather than the one literal value,
    so the queue is complete even for a run that never called the promotion. The
    promotion still runs at start-up, because "owed and mid-snapshot" and "owed" should
    not be two different durable answers to the same question.
    """
    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'in_progress' "
        "WHERE pipeline = ? AND source_table = 'orders'",
        [PIPELINE],
    )
    assert world.owed == ["app.orders"], (
        "the queue selects every non-terminal lifecycle state; it used to select only "
        "`awaiting_snapshot`, which is how a half-snapshotted table belonged to no queue"
    )

    promoted = dest_mod.promote_interrupted_snapshots(world.con, PIPELINE)
    assert promoted == ["app.orders"]
    assert world.owed == ["app.orders"]

    # Idempotent, and a no-op when nothing was interrupted.
    assert dest_mod.promote_interrupted_snapshots(world.con, PIPELINE) == []


def test_the_owes_work_predicate_covers_both_non_terminal_states(world):
    assert {"awaiting_snapshot", "in_progress"} == dest_mod.SNAPSHOT_STATES_OWING_WORK
    assert dest_mod.SNAPSHOT_STATES_OWING_WORK <= dest_mod.SNAPSHOT_STATES


def test_a_snapshot_state_outside_the_frozen_domain_is_refused(world):
    """ADR §4.8 declared a domain that did not include the value everything uses.

    `failed` was declared and never written; `awaiting_snapshot` was written by three
    modules and never declared; nothing validated a read. A state outside the domain
    belongs to no queue and no recovery path, so it is refused rather than skipped.
    """
    states = dest_mod.read_snapshot_states(world.con, PIPELINE)
    assert set(states.values()) <= dest_mod.SNAPSHOT_STATES

    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'failed' "
        "WHERE pipeline = ? AND source_table = 'orders'",
        [PIPELINE],
    )
    with pytest.raises(ValueError) as raised:
        dest_mod.read_snapshot_states(world.con, PIPELINE)
    assert "app.orders" in str(raised.value)
    assert "failed" in str(raised.value)


def test_an_unknown_recovery_phase_is_loud_rather_than_a_silent_no_op(world):
    """`PHASES` was declared and never enforced.

    `read()` accepted any string and `resume()` then matched none of its branches, fell
    through every `if`, and logged "recovery is ARMED" while having done nothing at all.
    """
    world.begin()
    world.con.execute(
        "UPDATE _cdc_flight.recovery_state SET phase = 'halfway' WHERE pipeline = ?",
        [PIPELINE],
    )
    with pytest.raises(RecoveryFailed) as raised:
        world.journal()
    assert "halfway" in str(raised.value)


def test_the_heartbeat_table_declared_by_the_adr_actually_exists(world):
    """ADR §4.8 / D9.1 declared it and nothing ever created it."""
    columns = {
        str(row[0])
        for row in world.con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
        ).fetchall()
    }
    assert {"pipeline", "runner_id", "beat_at", "phase", "lag_seconds"} <= columns


# --------------------------------------------------------------------------- #
# Codex r2 BLOCKER-1 — a journal may only clear over POSITIVE terminal evidence
# --------------------------------------------------------------------------- #
def _armed_journal(world, *, decision: str, captured: list[tuple[str, str, str]]):
    slot_receipt = dest_mod.write_slot_state(
        world.con,
        pipeline=PIPELINE,
        slot_name="cdc_slot",
        observation={},
        verdict="fresh_start" if decision == recovery_mod.RESET_DECISION else decision,
        verdict_message="a test",
    )
    record = recovery_mod.begin(
        world.con,
        pipeline=PIPELINE,
        namespace=NAMESPACE,
        decision=decision,
        message="a test",
        slot_name="cdc_slot",
        offset_path=Path("/tmp/does-not-matter"),
        captured_tables=captured,
        forget_catalog=False,
        slot_receipt=slot_receipt,
        logical_message_dataset="cdc_raw",
    )
    world.con.execute(
        "UPDATE _cdc_flight.recovery_state SET phase = 'armed' WHERE pipeline = ?",
        [PIPELINE],
    )
    record.phase = "armed"
    return record


def _completion(world, record):
    return recovery_mod.complete_if_ready(
        world.con, pipeline=PIPELINE, namespace=NAMESPACE, record=record
    )


def test_a_reset_journal_marks_every_captured_table_as_owing_a_fresh_image(world):
    """`--reset-state`'s obligation has to BE an obligation (Codex r2 BLOCKER-1).

    The first cut of the journalled reset stopped at `reset_all()`, so every captured
    table ended at `none` and `tables_marked` was 0 — and `none` is not an owing state.
    The obligation was therefore satisfied by doing nothing at all.
    """
    tables = [("app", "customers", "cdcflight_app_customers"),
              ("app", "orders", "cdcflight_app_orders")]
    record = _armed_journal(world, decision=recovery_mod.RESET_DECISION, captured=tables)
    assert record.tables_marked == 2
    assert sorted(record.captured) == ["app.customers", "app.orders"]
    owed = sorted(f"{s}.{t}" for s, t, _ in dest_mod.tables_awaiting_snapshot(world.con, PIPELINE))
    assert owed == ["app.customers", "app.orders"]


@pytest.mark.parametrize("decision", ["operator_reset", "slot_ahead_of_destination"])
def test_none_and_a_missing_row_cannot_clear_a_journal(world, decision):
    """The exact predicate reproduction from the review, for BOTH decisions.

    `none` means "registered, never snapshotted" and a missing row means "nothing was
    ever written for it". Neither is evidence that a rebuild happened, and both used to
    pass because the test was `not in LIFECYCLE_OWING_WORK` rather than "is `complete`".
    """
    tables = [("app", "existing", "cdcflight_app_existing"),
              ("app", "missing", "cdcflight_app_missing")]
    record = _armed_journal(world, decision=decision, captured=tables)
    # Walk one table back to `none` and delete the other's row entirely, which is the
    # durable shape an empty source table leaves behind.
    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'none' "
        "WHERE pipeline = ? AND source_table = 'existing'", [PIPELINE],
    )
    world.con.execute(
        "DELETE FROM _cdc_flight.table_state WHERE pipeline = ? AND source_table = 'missing'",
        [PIPELINE],
    )
    completion = _completion(world, record)
    assert completion.cleared is False
    assert sorted(completion.still_owed) == ["app.existing", "app.missing"]
    assert recovery_mod.read(world.con, pipeline=PIPELINE, namespace=NAMESPACE) is not None


def test_no_resume_point_cannot_clear_a_reset_journal(world):
    """Every recovery needs the handoff evidence, reset included.

    `needs_resume = tables_marked > 0` exempted reset, and reset recorded 0.
    """
    tables = [("app", "customers", "cdcflight_app_customers")]
    record = _armed_journal(world, decision=recovery_mod.RESET_DECISION, captured=tables)
    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'complete' WHERE pipeline = ?",
        [PIPELINE],
    )
    world.con.execute("DELETE FROM _cdc_flight.debezium_offsets WHERE pipeline = ?", [PIPELINE])
    completion = _completion(world, record)
    assert completion.cleared is False
    assert completion.has_resume_point is False
    assert "resume point" in completion.reason


def test_a_journal_clears_only_when_every_captured_table_is_complete(world):
    tables = [("app", "customers", "cdcflight_app_customers"),
              ("app", "orders", "cdcflight_app_orders")]
    record = _armed_journal(world, decision=recovery_mod.RESET_DECISION, captured=tables)
    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'complete' "
        "WHERE pipeline = ? AND source_table = 'customers'", [PIPELINE],
    )
    assert _completion(world, record).cleared is False
    world.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'complete' WHERE pipeline = ?",
        [PIPELINE],
    )
    completion = _completion(world, record)
    assert completion.cleared is True
    assert recovery_mod.read(world.con, pipeline=PIPELINE, namespace=NAMESPACE) is None
