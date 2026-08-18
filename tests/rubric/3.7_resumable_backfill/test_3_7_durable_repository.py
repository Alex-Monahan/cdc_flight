"""Durable BACKFILL_RUN/SHADOW_CLAIM ownership proofs."""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight.backfill import BackfillCoordinator, ClaimConflict
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.states import IllegalTransition


def _coordinator():
    con = duckdb.connect(":memory:")
    ensure_control_schema(con)
    return con, BackfillCoordinator(con, pipeline="p3")


def test_run_cursor_claim_and_terminal_state_are_durable_and_idempotent():
    """The shadow claim and keyed cursor survive normal repository re-reads."""
    con, coordinator = _coordinator()
    try:
        run = coordinator.request("app", "customers", mode="incremental")
        run = coordinator.prepare(run)
        coordinator.repository.update_progress(
            run,
            last_key_json='{"id":7}',
            maximum_key_json='{"id":99}',
            chunks=1,
            rows=10,
            source_lsn=123,
        )
        persisted = coordinator.repository.get(run.run_id)
        assert persisted.state == "loading"
        assert persisted.last_processed_key_json == '{"id":7}'
        assert persisted.row_count == 10
        assert coordinator.claims.state("app", "customers")[0] == "backfill"
        with pytest.raises(IllegalTransition):
            coordinator.repository.transition(persisted, "complete")
    finally:
        con.close()


def test_claim_collision_is_refused_and_rollback_removes_uncommitted_request():
    """No competing typed/schema owner can overwrite a backfill claim."""
    con, coordinator = _coordinator()
    try:
        run = coordinator.request("app", "orders", mode="full")
        coordinator.prepare(run)
        with pytest.raises(ClaimConflict):
            coordinator.claims.acquire(
                "app", "orders", owner_kind="typed_change", owner_id="other"
            )
        con.execute("BEGIN TRANSACTION")
        coordinator.request("app", "documents", mode="incremental", in_transaction=True)
        con.execute("ROLLBACK")
        assert coordinator.repository.active("app", "documents") is None
    finally:
        con.close()


def test_maximum_cursor_uses_typed_numeric_order_not_json_text_order():
    """A composite/key cursor must rank integer 10 after integer 2 without text casts."""
    from types import SimpleNamespace

    from cdc_flight.envelope import PendingRecord

    con, coordinator = _coordinator()
    try:
        run = coordinator.prepare(
            coordinator.request("app", "customers", mode="incremental")
        )
        events = []
        for key in (2, 10):
            events.append(
                PendingRecord(
                    raw=object(),
                    kind="data",
                    topic="cdcflight.app.customers",
                    nbytes=1,
                    schema="app",
                    table="customers",
                    key={"id": key},
                    snapshot_identity=f"inc:s:app.customers:{key}",
                    incremental=True,
                )
            )
        coordinator.commit_progress(
            [SimpleNamespace(incremental=True, events=events)]
        )
        persisted = coordinator.repository.get(run.run_id)
        assert persisted.maximum_key_json == '{"id":{"type":"integer","value":10}}'
        assert persisted.last_processed_key_json == persisted.maximum_key_json
    finally:
        con.close()


def test_out_of_order_delivery_is_absorbed_by_the_monotonic_durable_cursor():
    """The restart cursor never regresses when a later key arrives first."""
    from types import SimpleNamespace

    from cdc_flight.envelope import PendingRecord

    con, coordinator = _coordinator()
    try:
        run = coordinator.prepare(
            coordinator.request("app", "customers", mode="incremental")
        )

        def event(key: int) -> PendingRecord:
            return PendingRecord(
                raw=object(),
                kind="data",
                topic="cdcflight.app.customers",
                nbytes=1,
                schema="app",
                table="customers",
                key={"id": key},
                snapshot_identity=f"inc:s:app.customers:{key}",
                incremental=True,
            )

        # The unit itself is out of order: 10 is delivered before 2.
        coordinator.commit_progress(
            [SimpleNamespace(incremental=True, events=[event(10), event(2)])]
        )
        persisted = coordinator.repository.get(run.run_id)
        high_water = '{"id":{"type":"integer","value":10}}'
        assert persisted.last_processed_key_json == high_water
        assert persisted.maximum_key_json == high_water

        # A later retry/progress write carrying a lower key is absorbed too.
        coordinator.repository.update_progress(
            run.run_id,
            last_key_json='{"id":{"type":"integer","value":2}}',
            maximum_key_json='{"id":{"type":"integer","value":2}}',
        )
        persisted = coordinator.repository.get(run.run_id)
        assert persisted.last_processed_key_json == high_water
        assert persisted.maximum_key_json == high_water
    finally:
        con.close()


def test_completed_run_is_retained_as_the_replay_fence_until_reconciliation():
    """A post-swap stock READ has durable terminal evidence and no live route."""
    con, coordinator = _coordinator()
    try:
        run = coordinator.request("app", "customers", mode="incremental")
        run = coordinator.repository.set_signal(run, "signal-terminal")
        run = coordinator.prepare(run)
        run = coordinator.repository.transition(run, "ready_to_swap")
        run = coordinator.repository.transition(run, "swapping")
        coordinator.repository.transition(
            run,
            "complete",
            terminal_source_point="123",
            notification_status="COMPLETED",
        )
        owner = coordinator.incremental_owner("app", "customers")
        assert owner is not None
        assert owner.state == "complete"
        assert owner.signal_id == "signal-terminal"
    finally:
        con.close()
