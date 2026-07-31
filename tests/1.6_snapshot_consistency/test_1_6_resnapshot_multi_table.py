"""Rubric 1.6 — a re-snapshot of MORE THAN ONE table, end to end.

Every re-snapshot test on this branch marked exactly **one** table `awaiting_snapshot`,
and the worst defect the review round found lives entirely in the gap between one table
and two: completion was inferred from "no table is currently mid-snapshot", which is
true at a Debezium batch boundary between table A's last record and table B's first, and
every unreached table was then classified empty and had its live destination rows
deleted (Codex B1 / Opus BLOCKER-1). Opus could not force the window black-box in the
shapes tried, which is the finding rather than a mitigation: whether it fires is a
function of Debezium's batching against table boundaries.

So this file exercises the shapes the proof depends on, against a real engine:

* **three tables at once**, one of them keyless, so a swap-ordering defect shows up as a
  wrong row set rather than as a converging upsert;
* **a genuinely empty table in the same request**, which is the only path that may run
  the swap-less delete — and, as far as the review could tell, a path that had never
  been executed by the suite at all;
* **a concurrent writer throughout**, so the hand-over is exercised rather than assumed;
* **a crash inside the re-snapshot** (slow), after the first table has swapped, proving
  the remaining tables are still owed rather than declared empty.

The deterministic, engine-free half of the same claims is
`test_1_6_resnapshot_completion.py`.
"""

from __future__ import annotations

import threading
import time

import pytest
from conftest import Sandbox

#: `documents` is emptied at the source before the re-snapshot, so it is the
#: verified-empty case. `sensor_readings` is keyless. `customers` is keyed and busy.
REQUESTED = ("customers", "orders", "documents", "sensor_readings")


def _mark(box: Sandbox, tables) -> None:
    names = ", ".join(f"'{t}'" for t in tables)
    box.duck_write(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'awaiting_snapshot' "
        f"WHERE source_table IN ({names})"
    )


class _Writer:
    """A concurrent source writer, running for the whole of the re-snapshot run."""

    def __init__(self, box: Sandbox, seconds: float):
        self.box = box
        self.seconds = seconds
        self.written = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def _loop(self):
        deadline = time.monotonic() + self.seconds
        i = 0
        while not self._stop.is_set() and time.monotonic() < deadline:
            i += 1
            try:
                self.box.sql(
                    [
                        f"INSERT INTO app.customers (name, email) VALUES "
                        f"('during-{i}', 'during-{i}@example.com')",
                        "INSERT INTO app.sensor_readings (sensor_id, value, unit) "
                        f"VALUES ('during', {i}.5, 'C')",
                    ],
                    one_transaction=True,
                )
                self.written += 1
            except Exception:  # pragma: no cover - the source may be busy
                pass
            time.sleep(0.2)

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=10)


@pytest.fixture(scope="module")
def multi(tmp_path_factory, postgres_cluster):
    box = Sandbox("resnap_multi", tmp_path_factory.mktemp("sbx_resnap_multi"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=180)

        # Make the four requested tables disagree with the destination in four
        # different ways, so a re-snapshot that skipped any of them is visible.
        box.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT 'multi-' || i, "
                "'multi-' || i || '@example.com' FROM generate_series(1, 40) i",
                "INSERT INTO app.orders (customer_id, total_amount, status) SELECT "
                "1, 3.00 + i, 'paid' FROM generate_series(1, 15) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                "'multi', i * 2.0, 'C' FROM generate_series(1, 25) i",
                # ... and one table that really is empty at the source.
                "DELETE FROM app.documents",
            ],
            one_transaction=True,
        )
        # The destination still holds the documents rows from the baseline, so the
        # empty classification has something to destroy if it gets it wrong.
        documents_before = box.scalar(
            f"SELECT count(*) FROM {box.table('cdcflight_app_documents')}"
        )
        _mark(box, REQUESTED)

        writer = _Writer(box, seconds=45).start()
        try:
            resnapshotted = box.run(max_seconds=240, timeout=320)
        finally:
            writer.stop()
        settled = box.run(max_seconds=180)

        yield {
            "box": box,
            "resnapshotted": resnapshotted,
            "settled": settled,
            "documents_before": documents_before,
            "concurrent_writes": writer.written,
        }
    finally:
        box.cleanup()
        box.reseed()


def test_every_requested_table_reached_a_terminal_state(multi):
    """Completion means all four, not "all the ones the engine happened to reach"."""
    summary = multi["resnapshotted"]
    requested = set(summary["resnapshot_requested"])
    assert requested == {f"app.{t}" for t in REQUESTED}, summary
    terminal = set(summary["resnapshot_swapped"]) | set(summary["resnapshot_emptied"])
    assert terminal == requested, (
        f"these requested tables reached no terminal state: {sorted(requested - terminal)}"
    )
    assert summary["resnapshot_snapshot_phase_ended"] is True, (
        "the engine must have been stopped by Debezium's own end-of-snapshot marker"
    )
    assert summary["ok"] is True, summary


def test_the_three_non_empty_tables_were_swapped_not_emptied(multi):
    summary = multi["resnapshotted"]
    assert set(summary["resnapshot_swapped"]) == {
        "app.customers", "app.orders", "app.sensor_readings"
    }, summary
    assert set(summary["resnapshot_tables_scanned"]) == {
        "app.customers", "app.orders", "app.sensor_readings"
    }, "the empty table produces no records, and the other three must all be scanned"


def test_only_the_verified_empty_table_was_emptied(multi):
    """The one path that is allowed to delete without a swap, executed on purpose.

    A51 row 24 claimed this behaviour was AUTO; as far as the review could establish,
    this is the first time the suite has run it.
    """
    box, summary = multi["box"], multi["resnapshotted"]
    assert summary["resnapshot_emptied"] == ["app.documents"], summary
    assert multi["documents_before"] > 0, "the fixture must give it something to destroy"
    assert box.scalar(f"SELECT count(*) FROM {box.table('cdcflight_app_documents')}") == 0
    assert box.pg_query("SELECT count(*) FROM app.documents")[0][0] == 0

    # And the audit row says it was VERIFIED, with the LSN it was fenced at.
    events = box.duck_query(
        "SELECT event, rows_removed, lsn, detail FROM _cdc_flight.table_events "
        "WHERE source_table = 'documents' AND event = 'resnapshot_empty'"
    )
    assert len(events) == 1, events
    _event, removed, lsn, detail = events[0]
    assert removed == multi["documents_before"]
    assert lsn == summary["resnapshot_empty_check_lsn"] > 0
    assert "VERIFIED" in detail


def test_the_empty_table_is_fenced_at_its_own_lsn_not_at_the_images(multi):
    """An empty table has no `source.lsn`, so it gets a WAL position we sampled.

    Sampling the throwaway slot for one is a race with no upper bound: for an all-empty
    capture set the engine can finish the image, enter streaming and advance
    `confirmed_flush_lsn` before the first poll lands, and fencing above the image is
    silent loss (Codex B2).
    """
    box, summary = multi["box"], multi["resnapshotted"]
    image_lsn = summary["resnapshot_consistent_lsn"]
    empty_lsn = summary["resnapshot_empty_check_lsn"]
    assert image_lsn and empty_lsn
    watermarks = dict(
        box.duck_query(
            "SELECT source_table, snapshot_lsn FROM _cdc_flight.table_state "
            "WHERE source_table IN ('documents', 'customers')"
        )
    )
    assert watermarks["documents"] == empty_lsn
    assert watermarks["customers"] == image_lsn


def test_the_destination_equals_the_source_afterwards(multi):
    """Including the rows written WHILE the re-snapshot was running."""
    box = multi["box"]
    assert multi["concurrent_writes"] > 0, "the concurrent writer never ran"

    source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
    dest = {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }
    assert dest == source, {
        "missing": sorted(source - dest)[:8], "extra": sorted(dest - source)[:8]
    }

    for table in ("orders", "sensor_readings", "documents"):
        src = box.pg_query(f"SELECT count(*) FROM app.{table}")[0][0]
        dst = box.scalar(f"SELECT count(*) FROM {box.table(f'cdcflight_app_{table}')}")
        assert dst == src, f"app.{table}: destination {dst} vs source {src}"


def test_nothing_was_delivered_twice(multi):
    box = multi["box"]
    for table in ("customers", "orders", "sensor_readings"):
        total, distinct = box.duck_query(
            f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM "
            f"{box.table(f'cdcflight_app_{table}')}"
        )[0]
        assert total == distinct, f"{table}: {total - distinct} duplicated change events"


def test_no_throwaway_slot_survived(multi):
    box = multi["box"]
    from cdc_flight.resnapshot import slot_name_for

    leftover = box.pg_query(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
        (slot_name_for(box.slot),),
    )
    assert leftover == [], f"a throwaway re-snapshot slot is still holding WAL: {leftover}"


# --------------------------------------------------------------------------- #
# the engine dies after the first table
# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_crash_after_the_first_swap_leaves_the_rest_owed_not_emptied(
    tmp_path_factory, postgres_cluster
):
    """The shape that used to delete live tables, forced with a fault anchor.

    `swap:1` kills the process between the DROP and the RENAME of the FIRST swap of the
    re-snapshot, which is the closest thing to "the engine stopped after the first
    table" that can be produced deterministically. What must be true afterwards: the
    tables the engine never reached still hold their rows, are still `awaiting_snapshot`,
    and the next run finishes the job.
    """
    box = Sandbox(
        "resnap_crash", tmp_path_factory.mktemp("sbx_resnap_crash"), postgres_cluster
    )
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=180)
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT 'crash-' || i, "
            "'crash-' || i || '@example.com' FROM generate_series(1, 30) i",
            one_transaction=True,
        )
        box.run(max_seconds=150)

        before = {
            t: box.scalar(f"SELECT count(*) FROM {box.table(f'cdcflight_app_{t}')}")
            for t in REQUESTED
        }
        assert all(v > 0 for k, v in before.items() if k != "documents"), before
        _mark(box, REQUESTED)

        box.clear_fired_fault()
        crashed = box.run(
            max_seconds=200,
            timeout=280,
            expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "swap:1"},
        )
        fired = box.fired_fault()
        assert fired is not None and fired["point"] == "swap", (
            f"the swap anchor did not fire, so this test proves nothing: {fired!r} "
            f"rc={crashed['returncode']}"
        )
        assert crashed["returncode"] != 0

        # NOTHING may have been emptied, and every table is still owed.
        after_crash = {
            t: box.scalar(f"SELECT count(*) FROM {box.table(f'cdcflight_app_{t}')}")
            for t in REQUESTED
        }
        assert after_crash == before, (
            "a crash inside the re-snapshot deleted destination rows: "
            f"{ {k: (before[k], after_crash[k]) for k in before if before[k] != after_crash[k]} }"
        )
        owed = {
            str(r[0])
            for r in box.duck_query(
                "SELECT source_table FROM _cdc_flight.table_state "
                "WHERE snapshot_state = 'awaiting_snapshot'"
            )
        }
        assert set(REQUESTED) <= owed, owed

        # ... and the next run finishes it.
        finished = box.run(max_seconds=240, timeout=320)
        assert finished["ok"] is True, finished
        assert set(finished["resnapshot_swapped"]) | set(finished["resnapshot_emptied"]) == {
            f"app.{t}" for t in REQUESTED
        }, finished
        source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
        dest = {
            str(r[0])
            for r in box.duck_query(
                f"SELECT name FROM {box.table('cdcflight_app_customers')}"
            )
        }
        assert dest == source
    finally:
        box.cleanup()
        box.reseed()
