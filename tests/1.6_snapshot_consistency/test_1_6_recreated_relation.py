"""Rubric 1.5's last mark, through 1.6's machinery: a recreated relation rebuilds itself.

1.5 was deliberately held at 4 with one sentence: *"TO REACH 5: automatic re-snapshot of
a recreated relation (`awaiting_snapshot` flag exists; machinery owned by 2.3/3.4)."* The
machinery now exists, so this is the test that decides whether that 4 becomes a 5, and it
is written to be decisive rather than indicative:

* the relation is dropped and recreated **with rows already in it**, at the source, while
  the pipeline is down — so CDC alone cannot possibly rebuild it (there are no events for
  rows that existed before the table joined the publication);
* the destination table for the OLD relation must not survive, because it holds a
  different relation's rows;
* and the *next* run must produce a destination table whose contents equal the source's,
  automatically, with no operator step and no `--reset-state`.

If the last bullet did not hold, 1.5 would stay at 4: a destination table that looks
healthy while being silently short of every pre-publication row is the exact failure the
`awaiting_snapshot` flag was invented to make loud.
"""

from __future__ import annotations

import pytest
from conftest import Sandbox

#: Whole module `slow`: five pipeline runs, ~90 s. It is the decisive evidence for
#: 1.5 = 5, so it runs in `make test-slow` on every review round rather than never.
pytestmark = pytest.mark.slow

TABLE = "rs_demo"
TARGET = f"cdcflight_app_{TABLE}"
TABLES = "customers,orders,sensor_readings,documents,wide_types,audit_log,rs_demo"


@pytest.fixture(scope="module")
def recreated(tmp_path_factory, postgres_cluster):
    box = Sandbox("recreated", tmp_path_factory.mktemp("sbx_recreated"), postgres_cluster)
    box.env["CDC_TABLES"] = TABLES
    try:
        box.reseed()
        box.sql(
            [
                f"DROP TABLE IF EXISTS app.{TABLE}",
                f"CREATE TABLE app.{TABLE} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{TABLE}",
                f"INSERT INTO app.{TABLE} SELECT i, 'old-' || i FROM generate_series(1, 5) i",
            ]
        )
        first = box.run(reset_state=True, max_seconds=180)
        landed_old = box.scalar(f"SELECT count(*) FROM {box.table(TARGET)}")

        # Drop and recreate at the source, WITH ROWS, while the pipeline is down. Those
        # rows produce no change events at all - the table was not published when they
        # were written - so only a snapshot can ever see them.
        box.sql(
            [
                f"DROP TABLE app.{TABLE}",
                f"CREATE TABLE app.{TABLE} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{TABLE}",
                f"INSERT INTO app.{TABLE} SELECT i, 'new-' || i FROM generate_series(1, 9) i",
            ]
        )
        detected = box.run(
            max_seconds=200, idle_seconds=10, expect_success=False
        )
        state_after_detection = box.duck_query(
            "SELECT snapshot_state FROM _cdc_flight.table_state WHERE source_table = ?",
            [TABLE],
        )
        retained_after_detection = box.duck_query(
            f"SELECT label FROM {box.table(TARGET)} ORDER BY id"
        )
        healed = box.run(max_seconds=220, idle_seconds=10)
        yield {
            "box": box,
            "first": first,
            "landed_old": landed_old,
            "detected": detected,
            "state_after_detection": state_after_detection,
            "retained_after_detection": retained_after_detection,
            "healed": healed,
        }
    finally:
        box.cleanup()
        box.sql(f"DROP TABLE IF EXISTS app.{TABLE}")
        box.reseed()


def test_the_first_run_replicated_the_old_relation(recreated):
    assert recreated["first"]["ok"] is True, recreated["first"]
    assert recreated["landed_old"] == 5, recreated["landed_old"]


def test_the_recreation_is_detected_and_the_stale_table_is_quarantined(recreated):
    detected = recreated["detected"]
    assert detected["tables_dropped"] == 0, detected
    assert detected["tables_quarantined"] >= 1, detected
    assert detected["tables_awaiting_snapshot"] == [f"app.{TABLE}"], detected
    # The old image is retained as a recovery image until the replacement snapshot
    # commits its atomic swap. It is untrusted and cannot be admitted as success.
    assert recreated["retained_after_detection"], (
        "the physical recovery image must still exist at quarantine time"
    )


def test_the_incompleteness_was_recorded_before_it_was_repaired(recreated):
    """The flag is the durable to-do list; without it the repair cannot be automatic."""
    assert recreated["state_after_detection"] == [("awaiting_snapshot",)], (
        recreated["state_after_detection"]
    )


def test_the_next_run_rebuilds_it_automatically(recreated):
    """1.5's condition for a 5, and 1.6's claim about a re-snapshot's consistency."""
    box, healed = recreated["box"], recreated["healed"]
    assert healed["ok"] is True, healed
    assert healed["resnapshot_swapped"] == [f"app.{TABLE}"], healed

    source = {
        (int(r[0]), str(r[1]))
        for r in box.pg_query(f"SELECT id, label FROM app.{TABLE}")
    }
    dest = {
        (int(r[0]), str(r[1]))
        for r in box.duck_query(f"SELECT id, label FROM {box.table(TARGET)}")
    }
    assert dest == source, f"missing={sorted(source - dest)} extra={sorted(dest - source)}"
    assert len(source) == 9, source


def test_the_repair_does_not_repeat(recreated):
    """`awaiting_snapshot` must be cleared, or every run re-snapshots for ever."""
    box = recreated["box"]
    state = box.duck_query(
        "SELECT snapshot_state FROM _cdc_flight.table_state WHERE source_table = ?",
        [TABLE],
    )
    assert state == [("complete",)], state
    again = box.run(max_seconds=180, idle_seconds=10)
    assert again["ok"] is True
    assert "resnapshot_swapped" not in again, again.get("resnapshot_swapped")


def test_the_other_tables_kept_streaming_through_all_of_it(recreated):
    """A rebuild of one table must not disturb the rest (3.3/3.4's minimal core)."""
    box = recreated["box"]
    box.sql("INSERT INTO app.customers (name, email) VALUES ('post-rebuild', 'p@x.com')")
    after = box.run(max_seconds=180, idle_seconds=10)
    assert after["ok"] is True, after
    names = {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }
    assert "post-rebuild" in names
