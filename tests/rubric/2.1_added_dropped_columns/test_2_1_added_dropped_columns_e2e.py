"""Ongoing CDC around a real ADD COLUMN and DROP COLUMN."""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


@pytest.fixture(scope="module")
def add_drop_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = "customers"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        box.run(reset_state=True, max_seconds=180)

        # CDC is live before the DDL, so this is not a snapshot-only assertion.
        box.sql("UPDATE app.customers SET name = 'before-ddl' WHERE id = 1")
        box.run(max_seconds=120, min_records=1)

        box.sql("ALTER TABLE app.customers ADD COLUMN evolution_marker text DEFAULT 'bronze'")
        box.sql(
            [
                "UPDATE app.customers SET evolution_marker = 'gold' WHERE id = 1",
                "INSERT INTO app.customers (name, email, evolution_marker) "
                "VALUES ('after-add', 'after-add@example.com', 'silver')",
            ],
            one_transaction=True,
        )
        after_add = box.run(max_seconds=150, min_records=1)
        source_after_add = box.pg_query(
            "SELECT id, evolution_marker FROM app.customers ORDER BY id"
        )
        target_after_add = box.duck_query(
            f"SELECT id, evolution_marker FROM {box.table('cdcflight_app_customers')} "
            "ORDER BY id"
        )

        box.sql("ALTER TABLE app.customers DROP COLUMN evolution_marker")
        box.sql("UPDATE app.customers SET name = 'after-drop' WHERE id = 1")
        after_drop = box.run(max_seconds=150, min_records=1)
        settled = box.run(max_seconds=120)
        yield {
            "box": box,
            "after_add": after_add,
            "after_add_source": source_after_add,
            "after_add_target": target_after_add,
            "after_drop": after_drop,
            "settled": settled,
        }
    finally:
        box.reseed()


def test_add_and_drop_runs_complete(add_drop_scenario):
    assert add_drop_scenario["after_add"]["ok"] is True
    assert add_drop_scenario["after_drop"]["ok"] is True


def test_added_column_matches_postgres_existing_and_new_rows(add_drop_scenario):
    assert add_drop_scenario["after_add_target"] == add_drop_scenario["after_add_source"]


def test_dropped_column_is_gone_and_non_evolved_data_kept(add_drop_scenario):
    box = add_drop_scenario["box"]
    assert box.duck_query(
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? AND column_name = 'evolution_marker'",
        [box.DATASET, "cdcflight_app_customers"],
    ) == [(0,)]
    assert box.duck_query(
        f"SELECT id, name FROM {box.table('cdcflight_app_customers')} ORDER BY id"
    ) == box.pg_query("SELECT id, name FROM app.customers ORDER BY id")


def test_schema_changes_are_one_auditable_event_each(add_drop_scenario):
    box = add_drop_scenario["box"]
    events = box.duck_query(
        "SELECT event, applied FROM _cdc_flight.table_events "
        "WHERE source_table = 'customers' AND event IN ('column_added', 'column_dropped') "
        "ORDER BY commit_id, seq"
    )
    assert events == [("column_added", True), ("column_dropped", True)]


def _slot_metrics(box):
    # Capture both the global physical-WAL view and the slot-local retention window.
    # xdist workers share one physical cluster, so pg_current_wal_lsn() includes
    # unrelated databases; confirmed_flush_lsn - restart_lsn is the uncontaminated
    # bound for this slot.
    box.sql("CHECKPOINT")
    rows = box.pg_query(
        "SELECT restart_lsn::text, confirmed_flush_lsn::text, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
        "pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn)::bigint, "
        "restart_lsn - '0/0', confirmed_flush_lsn - '0/0' "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    assert rows, f"replication slot {box.slot!r} disappeared"
    restart_lsn, confirmed_flush_lsn, global_wal, slot_window, restart_pos, confirmed_pos = rows[0]
    return {
        "restart_lsn": str(restart_lsn),
        "confirmed_flush_lsn": str(confirmed_flush_lsn),
        "global_retained_wal": int(global_wal),
        "slot_wal_window": int(slot_window),
        "restart_pos": int(restart_pos),
        "confirmed_pos": int(confirmed_pos),
    }


@pytest.fixture(scope="module")
def inet_add_column_containment(sandbox):
    """Real ADD COLUMN inet path: the refusal boundary must not stop the Flight."""
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = "customers,orders"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        baseline = box.run(reset_state=True, max_seconds=180)
        assert baseline["ok"] is True, baseline
        before = _slot_metrics(box)

        box.sql(
            "ALTER TABLE app.customers ADD COLUMN v inet "
            "DEFAULT '192.0.2.1'::inet"
        )
        box.sql(
            "INSERT INTO app.orders "
            "(customer_id, placed_at, status, total_amount, line_items, quantities, note) "
            "VALUES (1, '2026-08-10T12:00:00Z', 'paid', 17.25, "
            "'[{}]'::jsonb, ARRAY[1], 'healthy peer')"
        )

        runs = []
        metrics = [before]
        for iteration in range(3):
            # `min_records` is a WAIT, and a wait must be for a condition that can
            # become true. Exactly one INSERT was made above, so only the first
            # run can ever see a record; asking the second and third for one made
            # each of them sit out its whole `--max-seconds` and pass anyway.
            # Measured: 298.3 s on this node, the single largest cost in the slow
            # lane (`codex_logs/slowlane_rootcause.md` §3.2 A2). The proof this
            # fixture exists for — containment, a healthy peer, a slot that keeps
            # advancing — never needed the wait; it needs the run to END, which
            # the assertions below now check explicitly.
            result = box.run(
                max_seconds=60,
                min_records=1 if iteration == 0 else 0,
                expect_success=False,
            )
            runs.append(result)
            metrics.append(_slot_metrics(box))

        source = box.pg_query(
            "SELECT id, format('%s', v) FROM app.customers ORDER BY id"
        )
        target = box.duck_query(
            f"SELECT id, v FROM {box.table('cdcflight_app_customers')} ORDER BY id"
        )
        yield {"box": box, "runs": runs, "metrics": metrics, "source": source, "target": target}
    finally:
        box.reseed()


def test_add_column_inet_keeps_the_healthy_peer_and_advances_the_slot(
    inet_add_column_containment,
):
    scenario = inet_add_column_containment
    assert all(run["ok"] is True for run in scenario["runs"]), scenario["runs"]
    # The wait that WAS satisfiable really was satisfied, and every run ended
    # because it had finished rather than because its deadline expired. Both were
    # previously invisible: a run that delivered nothing and timed out looked
    # exactly like a run that delivered everything.
    assert scenario["runs"][0]["records"] >= 1, scenario["runs"][0]
    assert [run["stop_reason"] for run in scenario["runs"]] == ["idle"] * 3, (
        "a run that reaches --max-seconds has not proved a complete delivery; "
        f"{[run['stop_reason'] for run in scenario['runs']]}"
    )
    assert scenario["target"] == scenario["source"]
    assert scenario["box"].duck_query(
        "SELECT count(*) FROM _cdc_flight.schema_refusals "
        "WHERE source_table='customers'"
    ) == [(0,)]
    metrics = scenario["metrics"]
    # A small transaction need not cross a WAL-segment boundary on every run, so
    # the retention horizon may repeat.  It must nevertheless move monotonically
    # from the baseline at least once, while the confirmed position proves the
    # healthy peer was acknowledged.
    assert metrics[-1]["restart_pos"] >= metrics[0]["restart_pos"], metrics
    assert any(
        metric["restart_pos"] > metrics[0]["restart_pos"] for metric in metrics[1:]
    ), metrics
    assert max(metric["confirmed_pos"] for metric in metrics[1:]) > metrics[0]["confirmed_pos"]
    assert all(metric["slot_wal_window"] >= 0 for metric in metrics[1:]), metrics
    if "PYTEST_XDIST_WORKER" not in os.environ:
        assert max(metric["slot_wal_window"] for metric in metrics[1:]) < 1_000_000, metrics
    print(f"round10 ADD COLUMN inet slot metrics: {metrics}")


def test_add_column_measurements_are_recorded_for_round10(inet_add_column_containment):
    metrics = inet_add_column_containment["metrics"]
    # Keep the measured values in the test report; the assertions above are the
    # bounded-WAL contract, while this makes an accidental fixture with no slot
    # progress impossible to hide in a green run.
    assert all(
        metric["restart_lsn"]
        and metric["confirmed_flush_lsn"]
        and metric["global_retained_wal"] >= 0
        and metric["slot_wal_window"] >= 0
        for metric in metrics
    ), metrics
