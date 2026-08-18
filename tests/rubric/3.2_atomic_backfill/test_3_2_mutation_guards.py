"""Mutation proofs for the §3 safety guards.

Each case temporarily replaces one production guard/owner with a deliberately
unsafe implementation, runs the real assertion, observes that it fails, then
leaves the patch context and runs the same assertion against the restored code.
"""

from __future__ import annotations

import contextlib
import threading
import time
from types import SimpleNamespace

import duckdb
import pytest
from support.applier_lab import Lab, begin, data, end, snap
from support.backfill_lab import require_backfill

from cdc_flight import destination
from cdc_flight.assembler import TransactionAssembler
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.envelope import KIND_SNAPSHOT_BOUNDARY, PendingRecord
from cdc_flight.errors import TransactionAssemblyError
from cdc_flight.snapshot import SnapshotCoordinator
from cdc_flight.snapshot_completion import SnapshotCompletion


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


def _production_publication_guard(tmp_path) -> None:
    """Exercise the shipped SnapshotCoordinator.swap callback boundary."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    lab = Lab(tmp_path / "production-publication.duckdb", pipeline="prod-publication")
    try:
        lab.run(
            [
                begin("old", 10),
                data("old", 1, 11, key={"id": 1}, after={"id": 1, "name": "old"}),
                end("old", 1, 12, {"app.customers": 1}),
            ]
        )
        snapshots = lab.applier.snapshots
        observed: list[tuple[list[tuple], list[tuple]]] = []

        def on_swap(state, _snapshot_lsn, _commit_id):
            image = lab.q(
                f'SELECT id, name FROM "{lab.dataset}"."{state.target}" ORDER BY id'
            )
            lifecycle = lab.q(
                "SELECT snapshot_state FROM \"_cdc_flight\".\"table_state\" "
                "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                ["prod-publication", "app", "customers"],
            )
            observed.append((image, lifecycle))
            assert image == [(2, "new")]
            assert lifecycle == [("complete",)]

        snapshots._on_swap = on_swap
        lab.con.execute("BEGIN TRANSACTION")
        try:
            state = snapshots.state_for("app", "customers")
            lab.con.execute(
                f'CREATE TABLE "{lab.dataset}"."{state.shadow}" AS '
                f'SELECT * FROM "{lab.dataset}"."{state.target}" WHERE FALSE'
            )
            lab.con.execute(
                f'INSERT INTO "{lab.dataset}"."{state.shadow}" (id, name) '
                "VALUES (2, 'new')"
            )
            assert snapshots.swap(state, commit_id=1, snapshot_lsn=99) is True
            lab.con.execute("COMMIT")
        except BaseException:
            with contextlib.suppress(Exception):
                lab.con.execute("ROLLBACK")
            raise
        assert observed == [([(2, "new")], [("complete",)])]
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{state.target}" ORDER BY id'
        ) == [(2, "new")]
    finally:
        lab.close()


def _production_commit_boundary_guard(tmp_path) -> None:
    """Readers must never see a production swap before its run state commits."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    lab = Lab(tmp_path / "production-commit.duckdb", pipeline="prod-commit")
    reader = None
    thread = None
    stop = threading.Event()
    observations: list[tuple[object, object]] = []
    errors: list[str] = []
    try:
        lab.run(
            [
                begin("old", 10),
                data("old", 1, 11, key={"id": 1}, after={"id": 1, "name": "old"}),
                end("old", 1, 12, {"app.customers": 1}),
            ]
        )
        backfill = lab.applier.backfill
        run = backfill.prepare(
            backfill.request("app", "customers", mode="incremental")
        )
        lab.applier._ensure_backfill_route("app", "customers", run)
        state = lab.applier.snapshots.states()[0]
        lab.con.execute(
            f'INSERT INTO "{lab.dataset}"."{state.shadow}" (id, name) '
            "VALUES (2, 'new')"
        )

        reader = duckdb.connect(
            str(lab.path), config=destination.DUCKDB_CONNECT_CONFIG
        )

        def sample() -> None:
            while not stop.is_set():
                try:
                    image = reader.execute(
                        f'SELECT id, name FROM "{lab.dataset}"."{state.target}" ORDER BY id'
                    ).fetchall()
                    run_state = reader.execute(
                        "SELECT state FROM \"_cdc_flight\".\"backfill_runs\" "
                        "WHERE pipeline = ? AND run_id = ?",
                        ["prod-commit", run.run_id],
                    ).fetchone()[0]
                    observations.append((image, run_state))
                except Exception as exc:  # pragma: no cover - mutation diagnostics
                    errors.append(f"{type(exc).__name__}: {exc}")
                time.sleep(0.002)

        thread = threading.Thread(target=sample, name="production-commit-reader")
        thread.start()
        commit_error: BaseException | None = None
        try:
            lab.con.execute("BEGIN TRANSACTION")
            lab.applier.snapshots.swap(state, commit_id=1, snapshot_lsn=99)
            lab.con.execute("COMMIT")
        except BaseException as exc:
            commit_error = exc
        time.sleep(0.05)
        stop.set()
        thread.join(timeout=2)
        if commit_error is not None:
            raise AssertionError(
                "production swap callback escaped the surrounding transaction: "
                f"{type(commit_error).__name__}: {commit_error}; observations={observations}"
            ) from commit_error
        assert not errors, errors
        invalid = [
            observation
            for observation in observations
            if observation[0] == [(2, "new")] and observation[1] != "complete"
        ]
        assert not invalid, f"new image was visible before complete state: {invalid}"
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{state.target}" ORDER BY id'
        ) == [(2, "new")]
        assert lab.q(
            "SELECT state FROM \"_cdc_flight\".\"backfill_runs\" "
            "WHERE pipeline = ? AND run_id = ?",
            ["prod-commit", run.run_id],
        ) == [("complete",)]
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=2)
        if reader is not None:
            reader.close()
        lab.close()


def _production_terminal_fence_guard(tmp_path) -> None:
    """A declared two-row terminal callback cannot publish one buffered row."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    lab = Lab(
        tmp_path / "production-terminal-fence.duckdb",
        pipeline="prod-terminal-fence",
        snapshot_chunk_events=1,
    )
    try:
        completion = SnapshotCompletion.full_snapshot({"app.customers"})
        completion.observe_notification("STARTED", {})
        completion.observe_notification(
            "TABLE_SCAN_COMPLETED",
            {
                "scanned_collection": "app.customers",
                "status": "SUCCEEDED",
                "total_rows_scanned": "2",
            },
        )
        completion.observe_notification("COMPLETED", {})
        lab.applier.snapshot_completion = completion
        lab.feed([snap("customers", 100, ident=1, marker="last")])
        boundary = PendingRecord(
            raw=None,
            kind=KIND_SNAPSHOT_BOUNDARY,
            topic="cdcflight.cdc_flight_snapshot_notifications",
            nbytes=0,
            lsn=201,
            source_partition={"server": "cdcflight"},
            source_offset={"lsn": 201, "lsn_proc": 201, "ts_usec": 201000},
        )
        for unit in lab.applier.assembler.feed_snapshot_boundary(boundary):
            lab.applier._add_unit(unit)
        lab.commit("production_terminal_fence")
        assert lab.applier.commit_groups == 0
        assert lab.committer.marked == 0
        assert not lab.exists(lab.target("customers"))
    finally:
        lab.close()


def test_mutation_guards_fail_when_new_invariants_are_broken(tmp_path, monkeypatch):
    """Thirteen production mutations fail their corresponding invariant guard."""
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

    # F-05(i): mutate the actual publication seam so its callback runs before the
    # shadow is live and the lifecycle is complete. The callback-boundary oracle must
    # see the new image and complete state together.
    original_swap = SnapshotCoordinator.swap

    def callback_before_publication(self, state, *, commit_id, snapshot_lsn):
        if self._on_swap is not None:
            self._on_swap(state, snapshot_lsn, commit_id)
        return original_swap(
            self, state, commit_id=commit_id, snapshot_lsn=snapshot_lsn
        )

    with monkeypatch.context() as patch:
        patch.setattr(SnapshotCoordinator, "swap", callback_before_publication)
        with pytest.raises(AssertionError) as failure:
            _production_publication_guard(tmp_path / "f05-swap-mutated")
        print(f"F-05 swap-order mutation failed as expected: {failure.value}")
    _production_publication_guard(tmp_path / "f05-swap-restored")
    assert SnapshotCoordinator.swap is original_swap
    proofs.append("production_swap_publication")

    # F-05(ii): commit from the production backfill callback before it projects the
    # run state. The independent reader and the surrounding COMMIT guard must reject
    # the split publication.
    original_complete_swap = backfill.BackfillCoordinator.complete_swap

    def early_callback_commit(self, state, **kwargs):
        self.con.execute("COMMIT")
        time.sleep(0.1)
        return original_complete_swap(self, state, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(
            backfill.BackfillCoordinator, "complete_swap", early_callback_commit
        )
        with pytest.raises(AssertionError) as failure:
            _production_commit_boundary_guard(tmp_path / "f05-commit-mutated")
        print(f"F-05 early-commit mutation failed as expected: {failure.value}")
    _production_commit_boundary_guard(tmp_path / "f05-commit-restored")
    assert backfill.BackfillCoordinator.complete_swap is original_complete_swap
    proofs.append("production_one_transaction")

    # F-05(iii): remove the declared-row terminal fence from the production
    # completion object. One buffered row for a two-row declaration must not publish.
    original_terminal_fence = SnapshotCompletion._will_complete_after

    def permissive_terminal_fence(self, *_args, **_kwargs):
        return True

    with monkeypatch.context() as patch:
        patch.setattr(
            SnapshotCompletion, "_will_complete_after", permissive_terminal_fence
        )
        with pytest.raises(AssertionError) as failure:
            _production_terminal_fence_guard(tmp_path / "f05-terminal-mutated")
        print(f"F-05 terminal-fence mutation failed as expected: {failure.value}")
    _production_terminal_fence_guard(tmp_path / "f05-terminal-restored")
    assert SnapshotCompletion._will_complete_after is original_terminal_fence
    proofs.append("production_declared_row_fence")

    print(f"P3 mutation proofs: {proofs}")
    assert len(proofs) == 13
