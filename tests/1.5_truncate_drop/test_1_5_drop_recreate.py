"""Rubric 1.5, the expensive half: the baseline gap, a crash, and a recreate.

Marked `slow` (seven pipeline runs, ~2-3 min). The default-suite guards for the same
properties are `test_1_5_truncate_fold.py` (in-process, exact interleavings) and
`test_1_5_truncate_drop_e2e.py` (one 33 s end-to-end scenario).

Four things only a real run can show:

1. **The gap, live.** `CDC_TRUNCATE_MODE=ignore` restores Debezium's own default
   (`skipped.operations=t`) and the TRUNCATE becomes invisible again — the exact
   baseline behaviour `RUBRIC_STATUS.md` scored 1 for, reproduced on demand.
2. **A truncate is replicated** once the default is overridden.
3. **A crash in the commit→ack window of a truncating group** replays that
   transaction: the destination must end up empty exactly once, never re-populated
   and never half-emptied.
4. **Drop, then recreate with a different schema.** The destination table must not
   survive the drop, and the recreated table's events must land in a table with the
   NEW shape rather than being merged into the old one.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

TR_DEMO = "cdcflight_app_tr_demo"
RC_DEMO = "cdcflight_app_rc_demo"
TABLES = (
    "customers,orders,sensor_readings,documents,wide_types,audit_log,tr_demo,rc_demo"
)


@pytest.fixture(scope="module")
def drop_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = TABLES
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    box.sql(
        [
            "CREATE TABLE app.tr_demo (id bigint PRIMARY KEY, label text NOT NULL)",
            "CREATE TABLE app.rc_demo (id bigint PRIMARY KEY, label text NOT NULL)",
            "INSERT INTO app.tr_demo VALUES (1, 'a'), (2, 'b')",
            "INSERT INTO app.rc_demo VALUES (1, 'old-shape')",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.tr_demo, app.rc_demo",
        ]
    )
    phases = {"snapshot": box.run(reset_state=True, max_seconds=150)}

    # 1. the baseline gap: Debezium's own default skips truncates outright.
    box.sql("TRUNCATE TABLE app.tr_demo")
    phases["ignored"] = box.run(
        max_seconds=150, idle_seconds=8, extra_env={"CDC_TRUNCATE_MODE": "ignore"}
    )
    phases["rows_after_ignored"] = box.duck_query(
        f"SELECT id FROM {box.table(TR_DEMO)} ORDER BY id"
    )
    # Captured HERE: later phases record truncate markers of their own, so a count
    # taken at assertion time would be measuring the wrong thing.
    phases["markers_after_ignored"] = box.duck_query(
        "SELECT count(*) FROM _cdc_flight.table_events WHERE event = 'truncate'"
    )[0][0]

    # 2. the same statement, replicated.
    box.sql(["INSERT INTO app.tr_demo VALUES (3, 'c'), (4, 'd')"])
    box.sql("TRUNCATE TABLE app.tr_demo")
    phases["replicated"] = box.run(max_seconds=150, idle_seconds=8, min_records=1)
    phases["rows_after_replicated"] = box.duck_query(
        f"SELECT id FROM {box.table(TR_DEMO)} ORDER BY id"
    )

    # 3. a crash in the commit->ack window of a group that carries a truncate.
    box.sql("INSERT INTO app.tr_demo VALUES (5, 'e'), (6, 'f')")
    box.sql("TRUNCATE TABLE app.tr_demo")
    phases["crashed"] = box.run(
        max_seconds=150,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
    )
    phases["recovered"] = box.run(max_seconds=150, idle_seconds=8)
    phases["rows_after_crash"] = box.duck_query(
        f"SELECT id FROM {box.table(TR_DEMO)} ORDER BY id"
    )

    # 4. drop, then recreate with a different schema.
    box.sql("DROP TABLE app.rc_demo")
    phases["dropped"] = box.run(max_seconds=150, idle_seconds=10)
    phases["exists_after_drop"] = box.duck_query(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? "
        "AND table_name = ?",
        [box.DATASET, RC_DEMO],
    )[0][0]
    box.sql(
        [
            "CREATE TABLE app.rc_demo (id bigint PRIMARY KEY, payload jsonb NOT NULL, "
            "amount bigint NOT NULL)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.rc_demo",
            "INSERT INTO app.rc_demo VALUES (1, '{\"new\": true}', 42)",
        ]
    )
    phases["recreated"] = box.run(max_seconds=150, idle_seconds=10, min_records=1)
    try:
        yield {"box": box, **phases}
    finally:
        box.reseed()


# --------------------------------------------------------------------------- #
# 1 + 2: the gap, and the fix
# --------------------------------------------------------------------------- #
def test_ignore_mode_reproduces_the_baseline_gap(drop_scenario):
    """`skipped.operations=t` is Debezium's default, and it is the whole of the
    baseline's 1 for truncate: Postgres emptied the table, the destination did not
    notice, and no counter moved."""
    assert drop_scenario["rows_after_ignored"] == [(1,), (2,)]
    assert drop_scenario["markers_after_ignored"] == 0, (
        "nothing was even recorded, which is exactly the baseline behaviour"
    )


def test_the_same_truncate_is_replicated_with_the_default_policy(drop_scenario):
    assert drop_scenario["rows_after_replicated"] == []
    assert drop_scenario["replicated"]["truncates_applied"] >= 1


# --------------------------------------------------------------------------- #
# 3: a crash around a truncating group
# --------------------------------------------------------------------------- #
def test_the_crash_actually_fired_in_the_ack_window(drop_scenario):
    if drop_scenario["crashed"]["returncode"] != 137:
        pytest.fail(
            "the fault did not fire at post_commit_pre_ack:1 (returncode "
            f"{drop_scenario['crashed']['returncode']}); the crash case is vacuous"
        )


def test_a_replayed_truncate_leaves_the_table_empty(drop_scenario):
    """The whole transaction replays after the crash. A truncate that re-populated
    the table, or one that left the pre-truncate rows behind, would show here."""
    assert drop_scenario["rows_after_crash"] == []
    assert drop_scenario["recovered"]["ok"] is True
    box = drop_scenario["box"]
    assert box.pg_query("SELECT count(*) FROM app.tr_demo") == [(0,)]


# --------------------------------------------------------------------------- #
# 4: drop, then recreate with a different schema
# --------------------------------------------------------------------------- #
def test_the_destination_table_does_not_outlive_the_source_table(drop_scenario):
    diagnosis = " ".join(
        f"{k}={v}"
        for k, v in sorted(drop_scenario["dropped"].items())
        if "catalog" in k or k in ("records", "commit_groups", "durable_lsn", "tables_dropped")
    )
    assert drop_scenario["exists_after_drop"] == 0, diagnosis
    assert drop_scenario["dropped"]["tables_dropped"] >= 1, diagnosis


def test_the_recreated_table_lands_with_its_new_shape(drop_scenario):
    """Same name, new relation oid, different columns. Merging the new events into
    the old destination table is the corruption this case exists to rule out."""
    box = drop_scenario["box"]
    columns = {
        row[0]
        for row in box.duck_query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [box.DATASET, RC_DEMO],
        )
    }
    assert {"id", "payload", "amount"} <= columns
    assert "label" not in columns, "the old table's shape survived the drop"
    assert box.duck_query(f"SELECT id, amount FROM {box.table(RC_DEMO)} ORDER BY id") == [
        (1, 42)
    ]


def test_every_run_after_the_drop_was_clean(drop_scenario):
    for name in ("dropped", "recreated"):
        assert drop_scenario[name]["ok"] is True, name
