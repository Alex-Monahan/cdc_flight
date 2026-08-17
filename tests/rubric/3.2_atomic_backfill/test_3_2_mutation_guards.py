"""Mutation proofs for the §3 safety guards.

Each case temporarily replaces one production guard/owner with a deliberately
unsafe implementation, runs the real assertion, observes that it fails, then
leaves the patch context and runs the same assertion against the restored code.
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import Lab, begin, data, end
from support.backfill_lab import require_backfill

from cdc_flight.assembler import TransactionAssembler
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.envelope import PendingRecord
from cdc_flight.errors import TransactionAssemblyError
from cdc_flight.snapshot import SnapshotCoordinator


def _atomic_guard(backfill, tmp_path) -> None:
    lab = backfill.LocalAtomicityLab(tmp_path / "atomic")
    lab.create_live([(1, "old"), (2, "old")], state="old")
    lab.prepare_shadow([(1, "new"), (2, "new"), (3, "new")])
    observations = lab.polling_reader_during_swap()
    assert observations
    assert {(item.data, item.state) for item in observations} <= {
        ("old", "old"),
        ("new", "new"),
    }


def _invariant_o_guard(backfill) -> None:
    trace = backfill.CommitTrace()
    trace.record("shadow_row")
    trace.commit()
    try:
        trace.record("progress")
    except backfill.BackfillInvariantError:
        return
    raise AssertionError("the commit-to-ack guard accepted a post-commit progress write")


def _whole_transaction_guard() -> None:
    assembler = TransactionAssembler()
    assembler.feed(begin("mutation", 10))
    assembler.feed(data("mutation", 1, 11, key={"id": 1}, after={"id": 1, "name": "x"}))
    try:
        assembler.feed(end("mutation", 0, 12, {"app.customers": 1}))
    except TransactionAssemblyError:
        return
    raise AssertionError("the assembler accepted a transaction with a mismatched END count")


def _durable_cursor_guard(backfill) -> None:
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="cursor")
        run = coordinator.prepare(
            coordinator.request("app", "customers", mode="incremental")
        )
        event = PendingRecord(
            raw=object(),
            kind="data",
            topic="cdcflight.app.customers",
            nbytes=1,
            schema="app",
            table="customers",
            key={"id": 7},
            snapshot_identity="inc:cursor:app.customers:key-7",
            incremental=True,
            lsn=11,
        )
        coordinator.commit_progress(
            [SimpleNamespace(incremental=True, events=[event])]
        )
        persisted = coordinator.repository.get(run.run_id)
        assert persisted.last_processed_key_json == '{"id":{"type":"integer","value":7}}'
        assert persisted.chunk_count == 1
        assert persisted.row_count == 1
    finally:
        con.close()


def _stable_identity_guard(backfill) -> None:
    integer = backfill.incremental_identity("s", "app.customers", {"id": 7})
    text = backfill.incremental_identity("s", "app.customers", {"id": "7"})
    assert integer != text


def _retained_shadow_guard(backfill) -> None:
    class Registry:
        def forget(self, _name):
            return None

    coordinator = SnapshotCoordinator(
        duckdb.connect(":memory:"),
        dataset="cdc_raw",
        pipeline="mutation",
        topic_prefix="cdcflight",
        created_in_txn=lambda: set(),
        get_registry=lambda: Registry(),
        epoch=0,
        transactional_ddl=True,
    )
    state = coordinator.reattach(
        schema="app", table="customers", shadow="stable_shadow", run_id="run-1"
    )
    assert state.shadow == "stable_shadow"


def _claim_guard(backfill, con) -> None:
    claims = backfill.ShadowClaimRepository(con, pipeline="mutation")
    claims.acquire("app", "customers", owner_kind="backfill", owner_id="run-1")
    try:
        claims.acquire("app", "customers", owner_kind="typed_change", owner_id="other")
    except backfill.ClaimConflict:
        return
    raise AssertionError("the shadow claim guard allowed two owners")


def _queued_signal_guard(backfill) -> None:
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="queue-mutation")
        coordinator.request_tables(("app.customers",), signal_id="active-signal")
        queued, _runs = coordinator.request_tables(
            ("app.orders",), signal_id="successor-signal", request_id="successor-request"
        )
        assert queued.queued is True
        assert coordinator.signal_queue.queued(), "successor request was lost"
    finally:
        con.close()


def _empty_shadow_guard(backfill, path) -> None:
    lab = Lab(path)
    try:
        lab.run(
            [
                begin("old-empty", 10),
                data("old-empty", 1, 11, key={"id": 1}, after={"id": 1, "value": "old"}),
                end("old-empty", 1, 12, {"app.customers": 1}),
            ]
        )
        run = lab.applier.backfill.prepare(
            lab.applier.backfill.request("app", "customers", mode="incremental")
        )
        lab.applier._ensure_backfill_route("app", "customers", run)
        assert lab.exists(lab.shadow("customers")), "empty incremental shadow is absent"
    finally:
        lab.close()


def _full_swap_state_guard(backfill) -> None:
    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        coordinator = backfill.BackfillCoordinator(con, pipeline="full-swap")
        run = coordinator.prepare(
            coordinator.request("app", "orders", mode="full")
        )
        coordinator.complete_full_swap(
            SimpleNamespace(schema="app", table="orders"),
            snapshot_lsn=17,
            commit_id=1,
        )
        persisted = coordinator.repository.get(run.run_id)
        assert persisted.state == "complete"
        assert coordinator.claims.state("app", "orders")[0] == "free"
    finally:
        con.close()


def test_mutation_guards_fail_when_new_invariants_are_broken(tmp_path, monkeypatch):
    """Ten real production mutations fail their corresponding invariant guard."""
    backfill = require_backfill()
    proofs: list[str] = []

    original_read = backfill.LocalAtomicityLab._read

    def partial_read(self, _con):
        return backfill.Observation("partial", "old")

    with monkeypatch.context() as patch:
        patch.setattr(backfill.LocalAtomicityLab, "_read", partial_read)
        with pytest.raises(AssertionError):
            _atomic_guard(backfill, tmp_path)
    _atomic_guard(backfill, tmp_path)
    assert backfill.LocalAtomicityLab._read is original_read
    proofs.append("atomic_swap")

    original_record = backfill.CommitTrace.record

    def permissive_record(self, operation):
        self.after_commit.append(operation) if self.committed else self.before_commit.append(operation)

    with monkeypatch.context() as patch:
        patch.setattr(backfill.CommitTrace, "record", permissive_record)
        with pytest.raises(AssertionError):
            _invariant_o_guard(backfill)
    _invariant_o_guard(backfill)
    assert backfill.CommitTrace.record is original_record
    proofs.append("invariant_o")

    original_verify = TransactionAssembler._verify_complete
    with monkeypatch.context() as patch:
        patch.setattr(TransactionAssembler, "_verify_complete", lambda *_args: None)
        with pytest.raises(AssertionError):
            _whole_transaction_guard()
    _whole_transaction_guard()
    assert TransactionAssembler._verify_complete is original_verify
    proofs.append("whole_transactions")

    original_progress = backfill.BackfillRepository.update_progress

    def no_progress(self, run, **_kwargs):
        return self.get(run if isinstance(run, str) else run.run_id)

    with monkeypatch.context() as patch:
        patch.setattr(backfill.BackfillRepository, "update_progress", no_progress)
        with pytest.raises(AssertionError):
            _durable_cursor_guard(backfill)
    _durable_cursor_guard(backfill)
    assert backfill.BackfillRepository.update_progress is original_progress
    proofs.append("durable_cursor")

    original_canonical = backfill.canonical_key_json
    with monkeypatch.context() as patch:
        patch.setattr(backfill, "canonical_key_json", lambda _key: "same")
        with pytest.raises(AssertionError):
            _stable_identity_guard(backfill)
    _stable_identity_guard(backfill)
    assert backfill.canonical_key_json is original_canonical
    proofs.append("stable_identity")

    original_reattach = SnapshotCoordinator.reattach

    def unstable_reattach(self, *, schema, table, shadow, run_id=None, incremental=True):
        return original_reattach(
            self,
            schema=schema,
            table=table,
            shadow="different_shadow",
            run_id=run_id,
            incremental=incremental,
        )

    with monkeypatch.context() as patch:
        patch.setattr(SnapshotCoordinator, "reattach", unstable_reattach)
        with pytest.raises(AssertionError):
            _retained_shadow_guard(backfill)
    _retained_shadow_guard(backfill)
    assert SnapshotCoordinator.reattach is original_reattach
    proofs.append("retained_shadow")

    con = duckdb.connect(":memory:")
    try:
        ensure_control_schema(con)
        original_acquire = backfill.ShadowClaimRepository.acquire

        def permissive_acquire(self, *_args, **_kwargs):
            return "forged-lease"

        with monkeypatch.context() as patch:
            patch.setattr(backfill.ShadowClaimRepository, "acquire", permissive_acquire)
            with pytest.raises(AssertionError):
                _claim_guard(backfill, con)
        _claim_guard(backfill, con)
        assert backfill.ShadowClaimRepository.acquire is original_acquire
    finally:
        con.close()
    proofs.append("claim_ownership")

    original_enqueue = backfill.BackfillSignalQueueRepository.enqueue

    def drop_queue(self, **_kwargs):
        return None

    with monkeypatch.context() as patch:
        patch.setattr(backfill.BackfillSignalQueueRepository, "enqueue", drop_queue)
        with pytest.raises(AssertionError):
            _queued_signal_guard(backfill)
    _queued_signal_guard(backfill)
    assert backfill.BackfillSignalQueueRepository.enqueue is original_enqueue
    proofs.append("queued_signal_durability")

    original_state_for = SnapshotCoordinator.state_for

    def drop_empty_shadow(self, *args, **kwargs):
        state = original_state_for(self, *args, **kwargs)
        if state is not None and kwargs.get("incremental") and kwargs.get("retain_existing"):
            self.con.execute(
                f'DROP TABLE IF EXISTS "{self.dataset}"."{state.shadow}"'
            )
        return state

    with monkeypatch.context() as patch:
        patch.setattr(SnapshotCoordinator, "state_for", drop_empty_shadow)
        with pytest.raises(AssertionError):
            _empty_shadow_guard(backfill, tmp_path / "empty-shadow-mutated.duckdb")
    _empty_shadow_guard(backfill, tmp_path / "empty-shadow-restored.duckdb")
    assert SnapshotCoordinator.state_for is original_state_for
    proofs.append("empty_shadow_publication")

    original_full_swap = backfill.BackfillCoordinator.complete_full_swap
    with monkeypatch.context() as patch:
        patch.setattr(backfill.BackfillCoordinator, "complete_full_swap", lambda *_args, **_kwargs: None)
        with pytest.raises(AssertionError):
            _full_swap_state_guard(backfill)
    _full_swap_state_guard(backfill)
    assert backfill.BackfillCoordinator.complete_full_swap is original_full_swap
    proofs.append("full_swap_state_projection")

    print(f"P3 mutation proofs: {proofs}")
    assert len(proofs) == 10
