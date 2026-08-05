"""Rubric 1.5 end to end: real TRUNCATE and a real DROP TABLE, real destination.

The fold and the detection are pinned in isolation by the other two modules. This
one pins the things that cannot be constructed:

* that `skipped.operations=none` really does bring `op="t"` events through pgoutput,
  Debezium's transaction metadata and the assembler's completeness rule (a truncate
  is counted in `END.event_count`, so getting that wrong makes every truncating
  transaction fail hard rather than quietly);
* that `TRUNCATE parent CASCADE` really arrives as one event per relation inside one
  transaction, and that both destination tables are emptied by one COMMIT;
* that a `DROP TABLE` — which is nowhere in the replication stream — is detected,
  fenced and applied, so the destination table does not outlive the source table;
* that the whole thing is auditable afterwards from `_cdc_flight.table_events`.

Three tables are created by the scenario (`app.trunc_parent`, `app.trunc_child`,
`app.drop_demo`) rather than reusing seeded ones, because dropping and truncating a
seeded table would leak into every other module sharing this cluster.

`CDC_CATALOG_POLL_SECONDS=1` shortens the catalog poll for the test; the shipped
default is 10 s.
"""

from __future__ import annotations

import pytest

#: **Moved to the `slow` lane in the 1.6-1.8 review round** (Opus Q5). 1.5 keeps 98
#: deterministic tests in the default suite (catalog detection, the six guards, the fold,
#: ownership, key reuse, every storage mode); what moves out is the three-run end-to-end
#: scenario, which is the expensive part and the part the unit tests already pin. Nothing
#: was deleted.
pytestmark = [pytest.mark.slow, pytest.mark.e2e]

PARENT = "cdcflight_app_trunc_parent"
CHILD = "cdcflight_app_trunc_child"
DROPPED = "cdcflight_app_drop_demo"
TABLES = (
    "customers,orders,sensor_readings,documents,wide_types,audit_log,"
    "trunc_parent,trunc_child,drop_demo"
)


@pytest.fixture(scope="module")
def truncate_scenario(sandbox):
    box = sandbox
    box.reseed()
    box.env["CDC_TABLES"] = TABLES
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    box.sql(
        [
            "CREATE TABLE app.trunc_parent (id bigint PRIMARY KEY, label text NOT NULL)",
            "CREATE TABLE app.trunc_child (id bigint PRIMARY KEY, "
            "parent_id bigint NOT NULL REFERENCES app.trunc_parent (id), note text)",
            "CREATE TABLE app.drop_demo (id bigint PRIMARY KEY, label text NOT NULL)",
            "INSERT INTO app.trunc_parent VALUES (1, 'p1'), (2, 'p2')",
            "INSERT INTO app.trunc_child VALUES (10, 1, 'c10'), (11, 2, 'c11')",
            "INSERT INTO app.drop_demo VALUES (1, 'gone soon'), (2, 'also gone')",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.trunc_parent, "
            "app.trunc_child, app.drop_demo",
        ]
    )
    snapshot = box.run(reset_state=True, max_seconds=150)

    # T1 — `TRUNCATE parent CASCADE` truncates the child too: one transaction, one
    # `op="t"` event per relation. Inserts after the truncate in the SAME transaction
    # must survive it.
    box.sql(
        [
            "TRUNCATE TABLE app.trunc_parent CASCADE",
            "INSERT INTO app.trunc_parent VALUES (3, 'after-truncate')",
            "INSERT INTO app.trunc_child VALUES (30, 3, 'c30')",
        ],
        one_transaction=True,
    )
    # T2 — a DROP TABLE, which logical decoding does not report at all.
    box.sql("DROP TABLE app.drop_demo")
    # Something for the stream to carry, so the run has events to commit.
    box.sql("INSERT INTO app.customers (id, name, email) VALUES (8001, 'x', 'x@example.com')")

    streamed = box.run(max_seconds=150, min_records=1, idle_seconds=10)
    # A second, quiet run: the drop's fence needs a resume point past the LSN at which
    # the drop was detected, and asserting after two runs keeps the test from
    # depending on how many poll intervals fit inside the first one.
    settled = box.run(max_seconds=120, idle_seconds=8)
    try:
        yield {
            "box": box,
            "snapshot": snapshot,
            "streamed": streamed,
            "settled": settled,
        }
    finally:
        box.reseed()


def _events(box, event: str | None = None) -> list[tuple]:
    sql = (
        "SELECT event, source_table, applied, rows_removed FROM _cdc_flight.table_events "
        "WHERE 1 = 1"
    )
    params: list = []
    if event is not None:
        sql += " AND event = ?"
        params.append(event)
    return box.duck_query(sql + " ORDER BY commit_id, seq", params)


# --------------------------------------------------------------------------- #
# truncate
# --------------------------------------------------------------------------- #
def test_the_snapshot_landed_the_rows_the_truncate_will_remove(truncate_scenario):
    """Without this the truncate assertions below would pass vacuously."""
    box = truncate_scenario["box"]
    assert truncate_scenario["snapshot"]["ok"] is True
    assert box.duck_query(f"SELECT count(*) FROM {box.table(PARENT)}")[0][0] >= 1


def test_a_truncate_reaches_the_applier_at_all(truncate_scenario):
    """The pipeline pins `skipped.operations=none` so the ordered TRUNCATE reaches
    the applier; destination policy still decides whether it mutates or logs it."""
    box = truncate_scenario["box"]
    truncates = _events(box, "truncate")
    assert {(row[1], row[2]) for row in truncates} == {
        ("trunc_parent", True),
        ("trunc_child", True),
    }


def test_both_truncated_tables_hold_exactly_what_postgres_holds(truncate_scenario):
    box = truncate_scenario["box"]
    assert box.pg_query("SELECT id, label FROM app.trunc_parent ORDER BY id") == [
        (3, "after-truncate")
    ]
    assert box.duck_query(f"SELECT id, label FROM {box.table(PARENT)} ORDER BY id") == [
        (3, "after-truncate")
    ]
    assert box.duck_query(f"SELECT id, note FROM {box.table(CHILD)} ORDER BY id") == [
        (30, "c30")
    ]


def test_the_multi_table_truncate_was_one_commit_group(truncate_scenario):
    """`TRUNCATE parent CASCADE` is one Postgres transaction, so it must be one
    destination transaction: no observer can see the parent empty while the child is
    still full (rubric 1.3 applied to 1.5)."""
    box = truncate_scenario["box"]
    groups = box.duck_query(
        "SELECT DISTINCT commit_id FROM _cdc_flight.table_events WHERE event = 'truncate'"
    )
    assert len(groups) == 1, f"the truncate spanned {len(groups)} commit groups"


def test_the_truncate_marker_records_what_the_destination_lost(truncate_scenario):
    box = truncate_scenario["box"]
    removed = dict(
        (row[1], row[3]) for row in _events(box, "truncate")
    )
    assert removed == {"trunc_parent": 2, "trunc_child": 2}


# --------------------------------------------------------------------------- #
# drop
# --------------------------------------------------------------------------- #
def test_the_dropped_table_is_gone_from_the_destination(truncate_scenario):
    box = truncate_scenario["box"]
    assert box.pg_query(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'app' AND c.relname = 'drop_demo'"
    ) == [(0,)]
    assert (
        box.duck_query(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? "
            "AND table_name = ?",
            [box.DATASET, DROPPED],
        )
        == [(0,)]
    )


def test_the_drop_is_recorded_with_the_relation_it_lost(truncate_scenario):
    box = truncate_scenario["box"]
    dropped = box.duck_query(
        "SELECT source_table, applied, lsn FROM _cdc_flight.table_events "
        "WHERE event = 'dropped'"
    )
    assert [(row[0], row[1]) for row in dropped] == [("drop_demo", True)]
    assert dropped[0][2] > 0, "the detection LSN is what fences the drop"


def test_the_dropped_table_leaves_no_state_behind(truncate_scenario):
    box = truncate_scenario["box"]
    assert box.duck_query(
        "SELECT count(*) FROM _cdc_flight.source_relations WHERE source_table = 'drop_demo'"
    ) == [(0,)]
    assert box.duck_query(
        "SELECT count(*) FROM _cdc_flight.table_state WHERE source_table = 'drop_demo'"
    ) == [(0,)]


def test_the_catalog_watcher_recorded_the_relations_it_is_watching(truncate_scenario):
    """The persisted generation token makes a drop (or a drop-and-recreate)
    detectable across a restart."""
    box = truncate_scenario["box"]
    rows = box.duck_query(
        "SELECT source_table, relation_oid, published FROM _cdc_flight.source_relations "
        "ORDER BY source_table"
    )
    watched = {row[0]: row for row in rows}
    assert "trunc_parent" in watched and watched["trunc_parent"][1] > 0
    assert watched["trunc_parent"][2] is True


def test_the_fence_marker_did_not_break_the_transaction_assembler(truncate_scenario):
    """The watcher writes a **transactional** `pg_logical_emit_message(true, …)` to
    guarantee an LSN past the DDL flows (`cdc_flight.source_marker` records the
    measurement that makes `true` load-bearing: a non-transactional message does not
    end `WalPositionLocator.resumeFromLsn`'s search after a restart, so a quiet run
    delivered `records=0` and never applied the drop).

    A transactional message arrives as BEGIN + `op="m"` + END and Debezium DOES count
    it in `END.event_count`, so the assembler has to prove that unit whole through its
    `message_count` pseudo-entry - if it did not, every fenced run would fail hard on
    the completeness rule. It did not."""
    assert truncate_scenario["streamed"]["ok"] is True
    assert truncate_scenario["settled"]["ok"] is True
    assert truncate_scenario["streamed"].get("catalog_markers", 0) >= 1


def test_the_rest_of_the_stream_kept_flowing(truncate_scenario):
    """A truncate and a drop must not cost unrelated events."""
    box = truncate_scenario["box"]
    assert box.duck_query(
        f"SELECT name FROM {box.table('cdcflight_app_customers')} WHERE id = 8001"
    ) == [("x",)]


def test_no_run_reported_an_error(truncate_scenario):
    for name in ("snapshot", "streamed", "settled"):
        assert truncate_scenario[name]["ok"] is True, name
        assert truncate_scenario[name]["returncode"] == 0, name
