"""Rubric 1.5 — who owns a destination table, and when a run may call itself ok.

Two findings that are not about the fold and not about DDL, but about *bookkeeping*,
and both make a zombie destination table possible or a failure invisible.

**Ownership (Codex 5, ADR §18/A39).** "A replicated table absent from `pg_class` is
always detected" was not durably true. The watcher learns which names it owns from
`_cdc_flight.table_state`, and that row was written only by the snapshot coordinator —
so a table first materialised by streaming DML had no durable row at all, and a
`DROP TABLE` while the pipeline was down left an orphan destination table that no later
poll could ever report (`_compare` skips a name it has no oid for and does not believe
is replicated). `--reset-state` DELETEing the same table made the zombie **permanent**:
a source-dropped table produces no events, so nothing re-teaches the watcher.

**Honesty (Codex 6, ADR §18/A43).** Deferring a destructive action whose fence has not
opened is the correct safety choice. Calling that run successful is not: `poll()`
cleared `last_error` unconditionally and the alert was raised only when a change was
*applied*, so a run could report `ok: true`, `catalog_error: null` and
`catalog_pending > 0` together.
"""

from __future__ import annotations

import pytest
from applier_lab import Lab, end, keyed

from cdc_flight import catalog as catalog_mod
from cdc_flight import destination as dest_mod
from cdc_flight.catalog import CHANGE_DROPPED, CatalogChange, CatalogWatcher, SourceRelation
from cdc_flight.config import RunConfig
from cdc_flight.errors import EngineFailure
from cdc_flight.pipeline import run_engine_bounded


@pytest.fixture
def lab(tmp_path):
    boxes: list[Lab] = []

    def _make(**cfg) -> Lab:
        box = Lab(tmp_path / f"lab{len(boxes)}.duckdb", **cfg)
        boxes.append(box)
        return box

    yield _make
    for box in boxes:
        box.close()


def txn(number: str, events: list) -> list:
    counts: dict[str, int] = {}
    for event in events:
        counts[f"{event.schema}.{event.table}"] = (
            counts.get(f"{event.schema}.{event.table}", 0) + 1
        )
    return [*events, end(number, len(events), max(e.lsn or 0 for e in events) + 1, counts)]


# --------------------------------------------------------------------------- #
# ownership
# --------------------------------------------------------------------------- #
def test_streaming_dml_that_creates_a_table_records_the_ownership(lab):
    """No snapshot anywhere in this run: the table exists only because a change event
    created it, and that is exactly the case that had no durable row."""
    box = lab()
    box.run(txn("1", [keyed("1", 1, 10, 1, "a"), keyed("1", 2, 11, 7, "o", table="orders")]))
    owned = box.q(
        "SELECT source_schema, source_table, target_table FROM _cdc_flight.table_state "
        "ORDER BY source_table"
    )
    assert owned == [
        ("app", "customers", "cdcflight_app_customers"),
        ("app", "orders", "cdcflight_app_orders"),
    ]


def test_the_ownership_row_is_written_in_the_same_transaction_as_the_table(lab, monkeypatch):
    """A rolled-back group must leave neither the table nor the claim on it."""
    from cdc_flight import faults

    box = lab()
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("1", [keyed("1", 1, 10, 1, "a")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert not box.exists("cdcflight_app_customers")
    assert box.q("SELECT count(*) FROM _cdc_flight.table_state") == [(0,)]


def test_the_ownership_row_is_not_duplicated_by_later_groups(lab):
    box = lab()
    box.run(txn("1", [keyed("1", 1, 10, 1, "a")]))
    box.run(txn("2", [keyed("2", 1, 20, 2, "b")]))
    assert box.q("SELECT count(*) FROM _cdc_flight.table_state") == [(1,)]


def test_a_watcher_seeded_from_table_state_can_detect_a_drop_it_never_saw(lab):
    """The restart path end to end, in miniature: a run creates a table by streaming
    only, a *later* process seeds its watcher from the destination, and the drop that
    happened while nothing was running is detected."""
    box = lab()
    box.run(txn("1", [keyed("1", 1, 10, 1, "a")]))
    replicated = catalog_mod.seed_from_table_state(box.con, "lab")
    assert replicated == {"app.customers"}

    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(),
        known=catalog_mod.read_known_relations(box.con, "lab"),
        replicated=replicated, poll_seconds=0, confirm_polls=1,
    )
    added = w._compare({"app.orders": SourceRelation("app", "orders", 9, True, "d")}, lsn=99)
    assert [(c.kind, c.qualified) for c in added] == [(CHANGE_DROPPED, "app.customers")]


def test_reset_state_keeps_the_ownership_and_resets_only_the_snapshot_state(lab):
    """Opus MAJOR-4. `--reset-state` used to DELETE `table_state`, which permanently
    disabled drop detection for any table the source had already dropped: it produces
    no events, so `observe_replicated` never re-learns it and `_compare` skips it.

    Asserted against the statements `pipeline.run` issues, so the fix cannot drift
    apart from the test.
    """
    box = lab()
    box.run(txn("1", [keyed("1", 1, 10, 1, "a")]))
    box.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'complete', "
        "snapshot_epoch = 4, snapshot_lsn = 1234, last_commit_id = 7"
    )
    dest_mod.upsert_source_relation(
        box.con, pipeline="lab", source_schema="app", source_table="customers",
        relation_oid=16384, published=True, replica_identity="d",
    )

    # exactly what `pipeline.run(reset_state=True)` does to these two tables
    box.con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'none', snapshot_epoch = 0, "
        "snapshot_lsn = NULL, last_commit_id = NULL WHERE pipeline = ?",
        ["lab"],
    )
    box.con.execute("DELETE FROM _cdc_flight.source_relations WHERE pipeline = ?", ["lab"])

    assert catalog_mod.seed_from_table_state(box.con, "lab") == {"app.customers"}, (
        "ownership must survive --reset-state, or a source-dropped table is a "
        "permanent zombie"
    )
    assert catalog_mod.read_known_relations(box.con, "lab") == {}, (
        "the oids must NOT survive: a rebuilt source would otherwise read as a "
        "recreate of every table"
    )
    assert box.q(
        "SELECT snapshot_state, snapshot_epoch, snapshot_lsn, last_commit_id "
        "FROM _cdc_flight.table_state"
    ) == [("none", 0, None, None)]


# --------------------------------------------------------------------------- #
# honesty: the run summary and the drain barrier
# --------------------------------------------------------------------------- #
class _Handler:
    """The two things `run_engine_bounded` asks of the applier."""

    def __init__(self) -> None:
        self.record_count = 5
        self.error = None
        self.busy = False
        self.batch_count = 1
        self.data_batch_count = 1
        self.skipped_count = 0
        self.seconds_since_last_batch = 999.0

    def snapshot_counts(self) -> dict:
        return {}

    def stats(self) -> dict:
        return {}

    def drain_on_shutdown(self) -> int:
        return 0


class _Engine:
    failure = None
    offset_flushes_verified = 0
    suppressed_message = None
    completed_success = True

    def run(self) -> None:
        import time

        time.sleep(1.0)

    def close(self, intentional: bool = True) -> None:
        pass


def _watcher_with_pending(polls: list) -> CatalogWatcher:
    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0
    )
    w.queue(
        CatalogChange(kind=CHANGE_DROPPED, schema="app", table="gone", detected_lsn=10**9)
    )
    w.poll_quietly = lambda: polls.append(1) or []  # type: ignore[method-assign]
    return w


def test_a_quiet_run_polls_the_catalog_once_more_before_shutting_down():
    """Codex 6: the watcher polls every 10 s while the idle window is 8 s, so a DROP on
    a quiet source normally could not be seen until the *next* scheduled run. The final
    synchronous poll is also what completes `CDC_DROP_CONFIRM_POLLS` on a short run."""
    polls: list = []
    w = _watcher_with_pending(polls)
    with pytest.raises(EngineFailure):
        run_engine_bounded(
            _Engine(), _Handler(), RunConfig(max_seconds=6, idle_seconds=0.1),
            catalog=w, catalog_drain_seconds=0.2,
        )
    assert polls, "the final catalog poll did not happen"


def test_an_unresolved_destructive_change_makes_the_run_fail():
    """`ok: true` with `catalog_pending > 0` is the dishonest combination."""
    w = _watcher_with_pending([])
    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(
            _Engine(), _Handler(), RunConfig(max_seconds=6, idle_seconds=0.1),
            catalog=w, catalog_drain_seconds=0.2,
        )
    assert "unresolved" in str(excinfo.value)
    summary = excinfo.value.summary
    assert summary["stop_reason"] == "catalog_unresolved"
    assert summary["catalog_unresolved_tables"] == ["app.gone"]
    assert summary.get("ok") is not True


def test_a_run_with_nothing_pending_still_succeeds():
    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0
    )
    w.poll_quietly = lambda: []  # type: ignore[method-assign]
    summary = run_engine_bounded(
        _Engine(), _Handler(), RunConfig(max_seconds=6, idle_seconds=0.1),
        catalog=w, catalog_drain_seconds=0.2,
    )
    assert summary["ok"] is True
    assert summary["stop_reason"] == "idle"


def test_a_marker_failure_is_preserved_in_the_summary():
    """`poll()` used to clear `last_error` unconditionally, so the one state in which a
    destructive change *cannot* be applied reported no error at all."""
    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0
    )
    w.queue(
        CatalogChange(kind=CHANGE_DROPPED, schema="app", table="gone", detected_lsn=1)
    )

    class Broken:
        def execute(self, *_args):
            raise RuntimeError("cannot write to a read-only source")

    w._emit_marker(Broken(), w.pending())
    summary = w.summary()
    assert summary["catalog_marker_error"]
    assert summary["catalog_marker_capable"] is False
    assert summary["catalog_pending_destructive"] == 1
    assert summary["catalog_error"]


def test_the_marker_write_budget_is_bounded():
    """Opus MINOR-1: a fence that never opens would otherwise write one WAL record per
    poll for ever against a source cdc_flight otherwise only reads."""
    from cdc_flight.source_marker import CATALOG_FENCE, SourceMarker

    class Fine:
        def execute(self, *_args):
            return None

    marker = SourceMarker(prefix="cdcf", max_writes=3)
    conn = Fine()
    assert [marker.emit(conn, CATALOG_FENCE, {}) for _ in range(5)] == [
        True, True, True, False, False,
    ]
    assert marker.writes == 3
    assert marker.suppressed == 2
    assert "budget exhausted" in marker.last_error


def test_a_poll_that_finishes_at_shutdown_still_fails_the_run():
    """The sampling gap the first cut left (Codex r2 MAJOR-3).

    `run_engine_bounded` checked `machine_error` once and could return success, while
    the polling thread was stopped and joined only later in `pipeline.run()`'s
    `finally`. A poll already in flight could take an undeclared transition *after* the
    check, and nobody re-read the field — the same "success over an undeclared edge"
    policy the fix claims is impossible, moved into a timing gap. The watcher is now
    quiesced by the supervisor itself, before any verdict is taken.
    """

    class _LatePoller(CatalogWatcher):
        """Completes its in-flight poll — and discovers the illegal edge — on `stop()`."""

        def __init__(self):
            super().__init__(
                dsn="", publication="pub", schema="app", include=set(), poll_seconds=0
            )
            self.stopped = False

        def poll_quietly(self):  # the supervisor's final synchronous poll: clean
            return []

        def stop(self):
            self.stopped = True
            self.machine_error = "IllegalTransition: catalog_change: 'due' -> 'marked'"

    w = _LatePoller()
    with pytest.raises(EngineFailure) as excinfo:
        run_engine_bounded(
            _Engine(), _Handler(), RunConfig(max_seconds=6, idle_seconds=0.1),
            catalog=w, catalog_drain_seconds=0.2,
        )
    assert w.stopped, "the supervisor must quiesce the watcher before it judges the run"
    assert "undeclared transition" in str(excinfo.value)
    assert excinfo.value.summary["stop_reason"] == "engine_error"
    assert excinfo.value.summary.get("ok") is not True
