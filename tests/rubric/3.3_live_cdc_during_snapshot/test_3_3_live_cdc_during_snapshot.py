"""§3.3 tests: stock incremental rows and ordinary CDC share one shadow route."""

from __future__ import annotations

from itertools import permutations

import duckdb
import pytest
from support.applier_lab import Lab, begin, data, end, heartbeat, snap
from support.backfill_lab import incremental_record, notification, require_backfill

from cdc_flight.assembler import UNIT_SNAPSHOT_CHUNK, CompleteUnit, TransactionAssembler
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.envelope import KIND_SNAPSHOT, PendingRecord

_NOTIFICATION_PERMUTATIONS = tuple(
    permutations(("STARTED", "IN_PROGRESS", "COMPLETED", "TABLE_SCAN_COMPLETED"))
)
_NOTIFICATION_GROUPS = tuple(
    _NOTIFICATION_PERMUTATIONS[offset:offset + 6] for offset in range(0, 24, 6)
)


def test_incremental_snapshot_records_are_accepted_as_bounded_units():
    """Proves the deliberate current refusal is replaced by stock notification units."""
    backfill = require_backfill()
    assembler = TransactionAssembler(incremental_enabled=True, snapshot_chunk_events=2)
    records = [
        backfill.decode_incremental_record(incremental_record(key=1)),
        backfill.decode_incremental_record(incremental_record(key=2)),
    ]
    units = [unit for record in records for unit in assembler.feed(record)]
    assert units and units[0].kind == "snapshot_chunk"
    assert units[0].incremental is True
    assert units[0].events[0].snapshot_identity.startswith("inc:")


def test_ordinary_cdc_and_incremental_rows_route_to_the_same_shadow():
    """A target table is shadow-routed only while its durable run is loading."""
    backfill = require_backfill()
    route = backfill.TableRoute("app", "customers", live="customers")
    route.start_loading(shadow="customers__cdcf_tmp")
    assert route.target_for("incremental") == "customers__cdcf_tmp"
    assert route.target_for("cdc") == "customers__cdcf_tmp"
    assert route.target_for("cdc", table="orders") == "orders"
    route.finish()
    assert route.target_for("cdc") == "customers"


def test_stock_incremental_notification_is_decoded_as_progress_not_initial_completion():
    """Proves stock aggregate notifications retain table/status/chunk evidence."""
    backfill = require_backfill()
    decoded = backfill.decode_incremental_notification(
        notification("IN_PROGRESS", table="billing.audit_log", chunk=12)
    )
    assert decoded is not None
    assert decoded.observation == "IN_PROGRESS"
    assert decoded.table == "billing.audit_log"
    assert decoded.signal_id == "signal-1"
    assert decoded.chunk_id == "12"
    assert decoded.last_processed_key == "12"


@pytest.mark.parametrize("orderings", _NOTIFICATION_GROUPS)
def test_stock_notification_interleavings_keep_one_loading_state(orderings):
    """All 24 notification permutations stay durable until the real swap."""
    backfill = require_backfill()
    for ordering in orderings:
        con = duckdb.connect(":memory:")
        try:
            ensure_control_schema(con)
            coordinator = backfill.BackfillCoordinator(
                con, pipeline="notification-order"
            )
            coordinator.request_tables(
                ("app.customers",), request_id="request-order", signal_id="signal-order"
            )
            for observation in ordering:
                raw = notification(
                    observation,
                    table="app.customers",
                    rows=0 if observation == "TABLE_SCAN_COMPLETED" else 1,
                    status="EMPTY" if observation == "TABLE_SCAN_COMPLETED" else "SUCCEEDED",
                    signal_id="signal-order",
                )
                decoded = backfill.decode_incremental_notification(raw)
                coordinator.observe_notification(decoded)
            run = coordinator.active("app", "customers")
            assert run is not None
            assert run.state == "ready_to_swap", ordering
            assert coordinator.claims.state("app", "customers")[0] == "backfill"
            coordinator.complete_swap(
                type("State", (), {"schema": "app", "table": "customers"})(),
                snapshot_lsn=99,
                commit_id=1,
            )
            assert coordinator.repository.get(run.run_id).state == "complete", ordering
        finally:
            con.close()


def test_incremental_chunk_boundary_never_splits_an_open_postgres_transaction():
    """A stock READ waits behind a whole PG transaction, then closes as a chunk."""
    require_backfill()
    assembler = TransactionAssembler(incremental_enabled=True, snapshot_chunk_events=50_000)
    assembler.feed(
        PendingRecord(
            raw=object(), kind="txn_begin", topic="p.transaction", nbytes=1,
            txn_id="7", txn_status="BEGIN", lsn=10,
        )
    )
    assembler.feed(
        PendingRecord(
            raw=object(), kind="data", topic="p.customers", nbytes=1,
            schema="app", table="customers", txn_id="7", total_order=1,
            after={"id": 9},
        )
    )
    assert assembler.feed(
        PendingRecord(
            raw=object(), kind=KIND_SNAPSHOT, topic="p.customers", nbytes=1,
            schema="app", table="customers", snapshot="incremental", op="r",
            after={"id": 1},
        )
    ) == []
    txn_units = assembler.feed(
        PendingRecord(
            raw=object(), kind="txn_end", topic="p.transaction", nbytes=1,
            txn_id="7", txn_status="END", lsn=13, txn_event_count=1,
            txn_data_collections={"app.customers": 1},
        )
    )
    assert [unit.kind for unit in txn_units] == ["txn"]
    released = assembler.feed(heartbeat(14))
    assert released[0].kind == UNIT_SNAPSHOT_CHUNK
    assert released[0].incremental is True


def test_real_applier_routes_incremental_and_cdc_to_one_retained_shadow(tmp_path):
    """The shipped planner, writer, and commit protocol share the loading target."""
    backfill = require_backfill()
    lab = Lab(tmp_path / "route.duckdb")
    try:
        # Establish an old live image through the ordinary production row path.
        lab.run(
            [
                begin("old", 10),
                data("old", 1, 11, key={"id": 1}, after={"id": 1, "name": "old"}),
                data("old", 2, 11, key={"id": 2}, after={"id": 2, "name": "old-2"}),
                end("old", 2, 12, {"app.customers": 2}),
            ]
        )
        run = lab.applier.backfill.prepare(
            lab.applier.backfill.request("app", "customers", mode="incremental")
        )
        lab.applier._ensure_backfill_route("app", "customers", run)

        incremental = snap(
            "customers", 20, marker="incremental", ident=1, value="snapshot"
        )
        incremental.incremental = True
        incremental.incremental_signal_id = "signal-1"
        incremental.snapshot_identity = backfill.incremental_identity(
            "signal-1", "app.customers", {"id": 1}
        )
        incremental_two = snap(
            "customers", 21, marker="incremental", ident=2, value="snapshot-2"
        )
        incremental_two.incremental = True
        incremental_two.incremental_signal_id = "signal-1"
        incremental_two.snapshot_identity = backfill.incremental_identity(
            "signal-1", "app.customers", {"id": 2}
        )
        # This ordinary CDC transaction arrives after the stock READ. Both events
        # must land in the same shadow, with the CDC value winning by source order.
        lab.run(
            [
                incremental,
                incremental_two,
                begin("cdc", 30),
                data(
                    "cdc", 1, 31, op="u", key={"id": 1},
                    before={"id": 1, "name": "old"},
                    after={"id": 1, "name": "live-cdc"},
                ),
                data(
                    "cdc", 2, 31, op="d", key={"id": 2},
                    before={"id": 2, "name": "snapshot-2"},
                ),
                data(
                    "cdc", 3, 31, key={"id": 3},
                    after={"id": 3, "name": "inserted-during"},
                ),
                data(
                    "cdc", 4, 31, table="orders", key={"id": 8},
                    after={"id": 8, "name": "unrelated-live"},
                ),
                end("cdc", 4, 32, {"app.customers": 3, "app.orders": 1}),
            ]
        )
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{lab.shadow("customers")}" ORDER BY id'
        ) == [(1, "live-cdc"), (3, "inserted-during")]
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{lab.target("customers")}" ORDER BY id'
        ) == [(1, "old"), (2, "old-2")]
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{lab.target("orders")}" ORDER BY id'
        ) == [(8, "unrelated-live")]
        persisted = lab.applier.backfill.repository.get(run.run_id)
        assert persisted.state == "loading"
        assert persisted.last_processed_key_json == '{"id":{"type":"integer","value":2}}'
        assert persisted.row_count == 2

        terminal = backfill.IncrementalNotification(
            observation="TABLE_SCAN_COMPLETED",
            data={"scanned_collection": "app.customers", "status": "SUCCEEDED"},
            signal_id="signal-1",
            table="app.customers",
            status="SUCCEEDED",
            rows=1,
            chunk_id="terminal",
            last_processed_key='{"id":1}',
        )
        lab.applier._pending_backfill_notifications.append(terminal)
        lab.applier._add_unit(
            CompleteUnit(
                kind=UNIT_SNAPSHOT_CHUNK,
                records=[heartbeat(40)],
                schema="app",
                table="customers",
                nbytes=10,
                incremental=True,
                snapshot_last_for_table=True,
            )
        )
        lab.commit("incremental_terminal")
        assert lab.q(
            f'SELECT id, name FROM "{lab.dataset}"."{lab.target("customers")}" ORDER BY id'
        ) == [(1, "live-cdc"), (3, "inserted-during")]
        assert not lab.exists(lab.shadow("customers"))
        assert lab.applier.backfill.repository.get(run.run_id).state == "complete"
        assert lab.applier.backfill.claims.state("app", "customers")[0] == "free"
    finally:
        lab.close()
