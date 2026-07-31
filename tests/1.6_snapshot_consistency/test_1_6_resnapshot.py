"""Rubric 1.6 — a re-snapshot has to be consistent with the CDC stream around it.

The healthy initial snapshot was already proven consistent (`probes/p08`). What 1.6 was
missing was the *other* snapshot: the one that happens to a table which is already live,
already has a destination table consumers are reading, and already has a CDC stream
positioned somewhere. Three questions, and this file answers all three against the
source's own contents:

1. **is the image complete and correct?** Full contents compared to Postgres, not counts.
2. **is the hand-over exact?** Every transaction that committed before the consistent
   point is in the image and is NOT applied again; every one at or after it is applied on
   top. A keyless table makes the difference between "exact" and "converges" visible: a
   changelog cannot absorb a duplicate the way an upsert can.
3. **do the other tables survive it?** A re-snapshot of one table must not touch, reset
   or re-deliver anything for the others.

The mechanism under test is `cdc_flight.resnapshot`: a blocking re-snapshot through a
short-lived engine with its own fresh slot, before the main stream starts.
"""

from __future__ import annotations

import pytest
from conftest import Sandbox

#: **Moved to the `slow` lane in the 1.6-1.8 review round** (Opus Q5). The default suite
#: guards the same claims more strongly and more cheaply now: the multi-table re-snapshot
#: in `test_1_6_resnapshot_multi_table.py` covers a keyless table, a verified-empty table,
#: a concurrent writer and the hand-over, and `test_1_6_resnapshot_completion.py` covers
#: the completion semantics deterministically in milliseconds. This module is the
#: single-table original: still worth running, no longer the cheapest representative of
#: its class. Nothing was deleted.
pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def resnap(tmp_path_factory, postgres_cluster):
    """Baseline run, source changes, then a re-snapshot of `app.customers` only.

    The scenario is deliberately built so that a wrong hand-over is *visible*:

    * rows written BEFORE the re-snapshot and already delivered — must appear once;
    * a row DELETED before the re-snapshot — must not come back;
    * rows written AFTER the re-snapshot — must appear once, applied on top of the image;
    * a keyless table with byte-identical rows, so nothing downstream may deduplicate;
    * a second keyed table that is NOT re-snapshotted, as the control.
    """
    box = Sandbox("resnapshot", tmp_path_factory.mktemp("sbx_resnap"), postgres_cluster)
    try:
        box.reseed()
        baseline = box.run(reset_state=True, max_seconds=150)

        # Pre-re-snapshot source activity, delivered by the baseline's successor.
        box.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT 'pre-' || i, "
                "'pre-' || i || '@example.com' FROM generate_series(1, 20) i",
                "INSERT INTO app.orders (customer_id, total_amount, status) SELECT "
                "1, 10.00 + i, 'pending' FROM generate_series(1, 7) i",
            ],
            one_transaction=True,
        )
        box.sql("DELETE FROM app.customers WHERE name = 'pre-3'")
        delivered = box.run(max_seconds=150)

        control_before = set(
            box.duck_query(
                f"SELECT total_amount::VARCHAR FROM {box.table('cdcflight_app_orders')}"
            )
        )
        customers_before = box.scalar(
            f"SELECT count(*) FROM {box.table('cdcflight_app_customers')}"
        )

        # Something happens that means the destination copy of `app.customers` cannot be
        # trusted. Rather than fake the cause, use the mechanism the causes all funnel
        # into: the durable `awaiting_snapshot` queue.
        box.duck_write(
            "UPDATE _cdc_flight.table_state SET snapshot_state = 'awaiting_snapshot' "
            "WHERE source_table = 'customers'"
        )
        # ... and make the source disagree with what the destination holds, in both
        # directions, so a re-snapshot that silently did nothing would be caught.
        box.sql("INSERT INTO app.customers (name, email) VALUES ('only-at-source', 'o@x.com')")
        box.sql("DELETE FROM app.customers WHERE name = 'pre-7'")

        resnapshotted = box.run(max_seconds=180)

        # After the re-snapshot: more source activity, which must land on top of the image.
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT 'post-' || i, "
            "'post-' || i || '@example.com' FROM generate_series(1, 5) i",
            one_transaction=True,
        )
        after = box.run(max_seconds=150)

        yield {
            "box": box,
            "baseline": baseline,
            "delivered": delivered,
            "resnapshotted": resnapshotted,
            "after": after,
            "control_before": control_before,
            "customers_before": customers_before,
        }
    finally:
        box.cleanup()
        box.reseed()


def _source_customers(box: Sandbox) -> set[tuple]:
    return {
        (str(name), str(email))
        for name, email in box.pg_query("SELECT name, email FROM app.customers")
    }


def _dest_customers(box: Sandbox) -> set[tuple]:
    return {
        (str(name), str(email))
        # A keyed table is current state with hard deletes (rubric 8.1's default), so
        # every row present is a row the source has.
        for name, email in box.duck_query(
            f"SELECT name, email FROM {box.table('cdcflight_app_customers')}"
        )
    }


def test_the_resnapshot_actually_ran(resnap):
    summary = resnap["resnapshotted"]
    assert summary["ok"] is True, summary
    assert summary["resnapshot_swapped"] == ["app.customers"], summary
    assert summary["resnapshot_consistent_lsn"], summary
    # Both independent readings of the consistent point, and they must agree.
    assert (
        summary["resnapshot_snapshot_record_lsn"]
        == summary["resnapshot_slot_consistent_lsn"]
        == summary["resnapshot_consistent_lsn"]
    ), summary


def test_the_resnapshotted_table_matches_the_source_exactly(resnap):
    box = resnap["box"]
    assert _dest_customers(box) == _source_customers(box)


def test_a_row_deleted_before_the_resnapshot_does_not_come_back(resnap):
    box = resnap["box"]
    names = {name for name, _ in _dest_customers(box)}
    assert "pre-3" not in names
    assert "pre-7" not in names


def test_a_row_the_stream_never_carried_is_picked_up_by_the_image(resnap):
    """`only-at-source` was inserted while the pipeline was down, before the image."""
    box = resnap["box"]
    assert "only-at-source" in {name for name, _ in _dest_customers(box)}


def test_changes_after_the_resnapshot_land_on_top_of_the_image(resnap):
    box = resnap["box"]
    names = {name for name, _ in _dest_customers(box)}
    assert {f"post-{i}" for i in range(1, 6)} <= names
    assert resnap["after"]["ok"] is True


def test_the_resnapshot_does_not_duplicate_change_events(resnap):
    box = resnap["box"]
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
        f"{box.table('cdcflight_app_customers')}"
    )[0]
    assert total == distinct, f"{total - distinct} duplicated identities after a re-snapshot"


def test_the_other_tables_are_untouched(resnap):
    """The control: `app.orders` was not re-snapshotted and must be bit-identical."""
    box = resnap["box"]
    after = set(
        box.duck_query(
            f"SELECT total_amount::VARCHAR FROM {box.table('cdcflight_app_orders')}"
        )
    )
    assert after == resnap["control_before"]


def test_the_watermark_is_recorded_and_used(resnap):
    box = resnap["box"]
    rows = box.duck_query(
        "SELECT source_table, snapshot_state, snapshot_lsn FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers'"
    )
    assert rows and rows[0][1] == "complete", rows
    assert rows[0][2] == resnap["resnapshotted"]["resnapshot_consistent_lsn"]
    # The run AFTER the re-snapshot must have carried the watermark into the applier.
    assert resnap["after"]["snapshot_watermarks"] >= 1, resnap["after"]


def test_the_resnapshot_left_no_throwaway_slot_behind(resnap):
    box = resnap["box"]
    from cdc_flight.resnapshot import slot_name_for

    leftover = box.pg_query(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
        (slot_name_for(box.slot),),
    )
    assert leftover == [], f"a throwaway re-snapshot slot is still holding WAL: {leftover}"


def test_the_resnapshot_applied_the_image_and_ONLY_the_image(resnap):
    """Its slot is a throwaway; anything it streamed belongs to the real slot.

    `assert resnapshot_discarded_events >= 0` used to stand here, which a count always
    satisfies (Opus MINOR-1). The checkable claim is that the short-lived engine applied
    the image and nothing else: its applied-event count must equal the number of rows
    the source held for the re-snapshotted table, exactly. Whether it also *streamed*
    anything is a race with how fast we notice the swap, but if it had applied a
    streamed event this number would exceed the row count.
    """
    box = resnap["box"]
    summary = resnap["resnapshotted"]
    assert "app.customers" in summary["resnapshot_swapped"]
    # Every row the re-snapshot wrote carries `dbz_op = 'r'` (a snapshot read); every
    # row the main stream wrote afterwards carries an operation. So the image's own row
    # count is queryable after the fact, and it must equal the number of events the
    # short-lived engine applied. Anything above it is a *streamed* event that belongs
    # to the real slot and was applied here as well — a double delivery.
    image_rows = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} WHERE dbz_op = 'r'"
    )
    assert image_rows > 0
    assert summary["resnapshot_events"] == image_rows, (
        f"the re-snapshot applied {summary['resnapshot_events']} events for an image of "
        f"{image_rows} rows"
    )
    # The deterministic half of the same claim - a streaming unit reaching a re-snapshot
    # applier is fenced, counted and never written - is
    # `test_1_6_resnapshot_completion.py`, which does not depend on a race.

    # And the consistent point is the one BOTH readings agreed on: a disagreement is
    # fatal now, so a run that got here has a cross-checked C.
    assert summary["resnapshot_consistent_lsn"] == summary["resnapshot_snapshot_record_lsn"]
    assert summary["resnapshot_snapshot_phase_ended"] is True, (
        "the re-snapshot must stop on Debezium's own end-of-snapshot marker, not on "
        "'no table is currently mid-snapshot'"
    )
