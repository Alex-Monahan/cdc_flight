"""Rubric 1.6 — the boundary between the snapshot image and the CDC stream.

`inconsistent=1, consistent=5`. "Consistent" has a precise meaning here and it is the
one this file asserts: **every source row is on exactly one side of the boundary.** A row
written while the snapshot is running must appear either in the snapshot image or in the
subsequent stream, once — never in both (a duplicate) and never in neither (loss).

Postgres makes that provable rather than probable, and only on one path: Debezium's
`CREATE_REPLICATION_SLOT` exports a snapshot, the snapshot transaction adopts it with
`SET TRANSACTION SNAPSHOT`, and the streaming start LSN is the slot's `consistent_point`.
A transaction is then visible in the image **iff** it committed before that LSN. The
writer thread here commits ~15 transactions per second for the whole of the snapshot, so
the boundary is crossed many times over rather than being a lucky miss.

`dbz_op` is what makes the "never both" half checkable at all: `r` is a snapshot read,
`c` a streamed insert, and a row that somehow arrived twice would have to show up under
two identities.
"""

from __future__ import annotations

import contextlib
import os
import re
import threading
import time

import psycopg
import pytest
from support.fixtures import Sandbox

TAG = "boundary"
#: How long the writer keeps committing for. The snapshot of the seeded schema takes a
#: couple of seconds; this comfortably spans it, including the slot creation.
WRITER_SECONDS = 9.0
WRITER_INTERVAL = 0.06


def _lsn_value(lsn: str) -> int:
    """Convert PostgreSQL's ``X/Y`` WAL position to the comparable integer form."""
    high, low = str(lsn).split("/")
    return (int(high, 16) << 32) + int(low, 16)


def _committed_customer_lsns(box, slot: str) -> dict[str, int]:
    """Read the source-side commit LSN for each boundary row.

    The writer's list is deliberately only a statement-completion ledger: it proves
    that the client observed each INSERT return, but it does not say whether that
    transaction was before or after this bounded pipeline run's completion watermark.
    A throwaway ``test_decoding`` slot gives this fixture the exact source commit LSN
    needed to make that distinction without treating post-watermark work as loss.
    """
    changes = box.pg_query(
        "SELECT lsn::text, xid::text, data "
        "FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
        (slot,),
    )
    transactions: dict[str, dict[str, int]] = {}
    committed: dict[str, int] = {}
    for lsn, xid, data in changes:
        xid = str(xid)
        text = str(data)
        if text.startswith("BEGIN "):
            transactions[xid] = {}
            continue
        match = re.search(r"name\[text\]:'(boundary-\d+)'", text)
        if match:
            transactions.setdefault(xid, {})[match.group(1)] = 0
            continue
        if text.startswith("COMMIT"):
            for name in transactions.pop(xid, {}):
                committed[name] = _lsn_value(lsn)
    return committed


def _destination_counts(box) -> dict[str, int]:
    rows = box.duck_query(
        f"SELECT name, count(*) FROM {box.table('cdcflight_app_customers')} "
        f"WHERE name LIKE '{TAG}-%' GROUP BY name"
    )
    return {str(name): int(count) for name, count in rows}


class Writer:
    """Commits one small transaction at a time into a captured table."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.committed: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.error: BaseException | None = None

    def start(self) -> Writer:
        self._thread = threading.Thread(target=self._loop, name="boundary-writer", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def _loop(self) -> None:
        index = 0
        try:
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                while not self._stop.is_set():
                    index += 1
                    name = f"{TAG}-{index}"
                    conn.execute(
                        "INSERT INTO app.customers (name, email) VALUES (%s, %s)",
                        (name, f"{name}@example.com"),
                    )
                    # Appended only AFTER the commit returned, so the ledger never
                    # claims a row Postgres might not have.
                    self.committed.append(name)
                    self._stop.wait(WRITER_INTERVAL)
        except BaseException as exc:
            self.error = exc


@pytest.fixture(scope="module")
def boundary(tmp_path_factory, postgres_cluster):
    """A fresh snapshot with a concurrent writer committing throughout it."""
    box = Sandbox("snapshot_boundary", tmp_path_factory.mktemp("sbx_boundary"), postgres_cluster)
    writer = Writer(postgres_cluster.dsn)
    diagnostic_slot = f"p16_boundary_{os.getpid()}_{time.time_ns()}"
    try:
        box.reseed()
        box.pg_query(
            "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')",
            (diagnostic_slot,),
        )
        # Start the workload before the engine can reach its completion watermark.
        # Under a loaded xdist worker, starting the writer after ``spawn`` can let a
        # fast initial snapshot finish and arm the watermark before the writer has
        # committed its first batch; those later rows are then correctly outside this
        # run, but no longer exercise the snapshot/stream boundary this fixture names.
        writer.start()
        proc = box.spawn(max_seconds=180, idle_seconds=8)
        time.sleep(WRITER_SECONDS)
        writer.stop()
        returncode = proc.wait(timeout=240)
        summary = box.last_summary()

        source_commit_lsns = _committed_customer_lsns(box, diagnostic_slot)
        missing_source_lsns = set(writer.committed) - set(source_commit_lsns)
        assert not missing_source_lsns, (
            "the source observer did not decode writer ledger entries: "
            f"{sorted(missing_source_lsns)[:10]}"
        )
        completion_lsn = summary.get("completion_watermark_lsn")
        assert isinstance(completion_lsn, int) and completion_lsn > 0, summary
        committed_through_run = {
            name for name in writer.committed if source_commit_lsns[name] <= completion_lsn
        }
        first_run_landed = _destination_counts(box)

        # The completion watermark is a source position, not a promise that the
        # source stopped.  Finish the source workload first, then run the normal
        # pipeline once more so the final source/destination equality assertion really
        # means "when the dust settles".  The first-run assertion below retains its
        # pre-catch-up destination view and checks the exact watermark-scoped ledger.
        catchup = box.spawn(max_seconds=180, idle_seconds=8)
        catchup_returncode = catchup.wait(timeout=240)
        assert catchup_returncode == 0, box.last_summary()
        yield {
            "box": box,
            "writer": writer,
            "returncode": returncode,
            "summary": summary,
            "committed_through_run": committed_through_run,
            "first_run_landed": first_run_landed,
            "catchup_returncode": catchup_returncode,
        }
    finally:
        writer.stop()
        with contextlib.suppress(Exception):
            box.pg_query("SELECT pg_drop_replication_slot(%s)", (diagnostic_slot,))
        box.cleanup()
        box.reseed()


def test_the_run_succeeded_and_snapshotted(boundary):
    assert boundary["writer"].error is None, boundary["writer"].error
    assert boundary["returncode"] == 0, boundary["summary"]
    assert boundary["summary"]["ok"] is True
    assert boundary["summary"]["snapshot_swaps"] >= 1, boundary["summary"]
    # The writer really did commit across this run's watermark, or this test proves
    # nothing.  Later writer commits are checked after the catch-up run below.
    assert len(boundary["committed_through_run"]) > 40, len(
        boundary["committed_through_run"]
    )


def test_every_concurrent_write_appears_exactly_once(boundary):
    """Every row committed through this run's source watermark lands exactly once."""
    landed = boundary["first_run_landed"]
    committed = boundary["committed_through_run"]

    missing = sorted(committed - set(landed))
    duplicated = sorted(name for name, count in landed.items() if count > 1)
    # A row the destination holds that the writer never *reported* committing is not an
    # error: the last insert may have committed while `stop()` was interrupting it.
    assert not missing, (
        f"{len(missing)} rows committed through the run watermark reached NEITHER the "
        f"image "
        f"nor the stream: {missing[:10]}"
    )
    assert not duplicated, (
        f"{len(duplicated)} rows reached BOTH the image and the stream: {duplicated[:10]}"
    )

    # The second run is not a relaxation of the property: it closes the source-side
    # interval that the first bounded run intentionally leaves open.  Every ledger row
    # must still be present exactly once once the writer has stopped and the catch-up
    # has settled.
    final_landed = _destination_counts(boundary["box"])
    all_committed = set(boundary["writer"].committed)
    final_missing = sorted(all_committed - set(final_landed))
    final_duplicated = sorted(
        name for name, count in final_landed.items() if name in all_committed and count > 1
    )
    assert not final_missing, (
        f"{len(final_missing)} writer commits remained absent after catch-up: "
        f"{final_missing[:10]}"
    )
    assert not final_duplicated, (
        f"{len(final_duplicated)} writer commits landed more than once after catch-up: "
        f"{final_duplicated[:10]}"
    )


def test_no_row_is_on_both_sides_of_the_boundary(boundary):
    """`r` is a snapshot read and `c` a streamed insert; no name may have both."""
    box = boundary["box"]
    both = box.duck_query(
        f"SELECT name FROM {box.table('cdcflight_app_customers')} "
        f"WHERE name LIKE '{TAG}-%' GROUP BY name "
        "HAVING count(DISTINCT dbz_op) > 1"
    )
    assert both == [], f"rows delivered as both snapshot and stream: {both[:10]}"


def test_the_boundary_is_a_queryable_lsn(boundary):
    """`table_state.snapshot_lsn` is the consistent point, and the stream starts there.

    ADR 0001 §7: "`snapshot_lsn` is what makes 1.6 provable". It is only provable if
    every streamed event for the table really is at or after it, so that is asserted
    rather than described.
    """
    box = boundary["box"]
    rows = box.duck_query(
        "SELECT snapshot_lsn FROM _cdc_flight.table_state "
        "WHERE source_table = 'customers' AND snapshot_state = 'complete'"
    )
    assert rows and rows[0][0], rows
    consistent = int(rows[0][0])

    snapshot_lsns = box.duck_query(
        f"SELECT DISTINCT dbz_lsn FROM {box.table('cdcflight_app_customers')} "
        "WHERE dbz_op = 'r'"
    )
    assert [int(r[0]) for r in snapshot_lsns] == [consistent], (
        "every snapshot record must carry the exported snapshot's consistent point"
    )

    below = box.duck_query(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} "
        "WHERE dbz_op <> 'r' AND dbz_lsn < ?",
        [consistent],
    )[0][0]
    # Streamed events may carry an event LSN below the consistent point ONLY if their
    # transaction committed at or after it - the straddling case, and the single thing
    # this whole fixture exists to exercise.
    #
    # `assert below >= 0` used to stand here, which a count always satisfies (Opus
    # MINOR-1). The real claim is stronger and checkable: every such event's own
    # transaction must appear on the STREAM side of the fence, i.e. its commit LSN is at
    # or above the consistent point. If that is ever false the event is a re-delivery of
    # something already in the image, which is the duplication 1.2 is scored on.
    straddling = box.duck_query(
        f"SELECT count(*) FROM {box.table('cdcflight_app_customers')} c "
        f"JOIN _cdc_flight.commit_log l ON l.commit_id = c.cdcf_commit_id "
        "WHERE c.dbz_op <> 'r' AND c.dbz_lsn < ? AND l.last_lsn < ?",
        [consistent, consistent],
    )[0][0]
    assert straddling == 0, (
        f"{straddling} streamed event(s) carry an LSN below the consistent point AND "
        "belong to a group that committed below it, so they are a re-delivery of rows "
        f"the image already holds ({below} events are below C in total)"
    )
    assert boundary["summary"]["snapshot_consistent_lsn"] == consistent, boundary["summary"]


def test_the_destination_matches_the_source_when_the_dust_settles(boundary):
    box = boundary["box"]
    source = {str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")}
    dest = {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }
    assert dest == source
