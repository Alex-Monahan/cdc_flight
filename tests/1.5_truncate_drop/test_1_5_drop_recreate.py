"""Rubric 1.5, the expensive half: the baseline gap, a crash, and a recreate.

Marked `slow` (seven pipeline runs, ~2-3 min). The default-suite guards for the same
properties are `test_1_5_truncate_fold.py` (in-process, exact interleavings) and
`test_1_5_truncate_drop_e2e.py` (one 33 s end-to-end scenario).

Four things only a real run can show:

1. **The gap, live.** `CDC_TRUNCATE_MODE=ignore` preserves the externally visible
   baseline (the destination rows and marker count do not change), while the pipeline
   retains the raw TRUNCATE internally for the destination policy; lifecycle
   convergence is owned by the asynchronous catalog token.
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
from conftest import Sandbox

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

    # 1. the baseline gap: ignore is still a destination no-op, but the raw event is
    #    retained for policy/audit while the complete catalog token distinguishes an
    #    ordinary relfilenode rewrite from a replacement lifecycle.
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
    phases["recreated"] = box.run(
        max_seconds=150,
        idle_seconds=10,
        min_records=1,
        expect_success=False,
    )
    phases["recreated_healed"] = box.run(max_seconds=220, idle_seconds=10)
    try:
        yield {"box": box, **phases}
    finally:
        box.reseed()


# --------------------------------------------------------------------------- #
# 1 + 2: the gap, and the fix
# --------------------------------------------------------------------------- #
def test_ignore_mode_reproduces_the_baseline_gap(drop_scenario):
    """Ignore preserves the baseline destination behavior: Postgres emptied the
    table, the destination did not notice, and no truncate marker was published."""
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
    assert drop_scenario["recreated_healed"]["ok"] is True
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
    for name in ("dropped", "recreated", "recreated_healed"):
        assert drop_scenario[name]["ok"] is True, name


@pytest.mark.slow
def test_a_quiet_run_persists_what_it_learned_so_the_next_recreate_is_seen(
    tmp_path_factory, postgres_cluster
):
    """The exact silent inconsistency the round-3 review reproduced (Codex r3 BLOCKER-1).

    `source_relations` is the ONLY thing that makes a drop-and-recreate detectable across
    a restart, and it was persisted exclusively through `CatalogCoordinator.apply()` —
    inside a commit group. A run that committed **no groups** therefore persisted
    nothing, and everything the watcher had learned vanished at shutdown. Then:

    1. a healthy populated destination under `CDC_DROP_MODE=ignore` (no baseline);
    2. switch to `replicate` and run with no source changes at all: `ok=true`, zero
       records, zero commit groups — and, before the fix, zero persisted relations;
    3. offline, drop and recreate the table and insert one replacement row;
    4. the next run applied the insert, reported no catalog change, and persisted the NEW
       oid as though it had always owned that relation — leaving the OLD relation's rows
       beside the new one's, permanently, because from then on the oid agrees.

    The flush now happens once per run, after the watcher is proved quiesced.
    """
    table = "quiet_learn"
    target = f"cdcflight_app_{table}"
    box = Sandbox("quiet_learn", tmp_path_factory.mktemp("sbx_quiet"), postgres_cluster)
    box.env["CDC_TABLES"] = f"customers,{table}"
    try:
        box.reseed()
        box.sql(
            [
                f"DROP TABLE IF EXISTS app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (1, 'old-1'), (2, 'old-2')",
            ]
        )
        box.run(reset_state=True, max_seconds=180, extra_env={"CDC_DROP_MODE": "ignore"})
        assert box.scalar(f"SELECT count(*) FROM {box.table(target)}") == 2

        # A completely quiet run under the default `replicate` mode: nothing to apply.
        quiet = box.run(max_seconds=120)
        assert quiet["ok"] is True, quiet
        assert int(quiet.get("applied_events") or 0) == 0, quiet
        learned = dict(
            box.duck_query(
                "SELECT source_table, relation_oid FROM _cdc_flight.source_relations"
            )
        )
        assert table in learned, (
            "a run that committed no groups persisted nothing it had learned, so the "
            f"next recreate is undetectable: {learned}"
        )
        old_oid = learned[table]

        # Offline drop + recreate, with a replacement row.
        box.sql(
            [
                f"DROP TABLE app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (999, 'replacement-only')",
            ]
        )
        # Two runs: the first detects and drops, the second rebuilds from the source.
        box.run(max_seconds=200, idle_seconds=10, expect_success=False)
        healed = box.run(max_seconds=220, idle_seconds=10)
        assert healed["ok"] is True, healed

        source = {
            (int(r[0]), str(r[1]))
            for r in box.pg_query(f"SELECT id, label FROM app.{table}")
        }
        landed = {
            (int(r[0]), str(r[1]))
            for r in box.duck_query(f"SELECT id, label FROM {box.table(target)}")
        }
        assert landed == source, (
            f"the old relation's rows survived the recreate: destination={landed} "
            f"source={source}"
        )
        assert dict(
            box.duck_query(
                "SELECT source_table, relation_oid FROM _cdc_flight.source_relations"
            )
        )[table] != old_oid, "the new oid was never learned"
    finally:
        box.cleanup()
        box.sql(f"DROP TABLE IF EXISTS app.{table}")
        box.reseed()
