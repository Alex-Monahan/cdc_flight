"""Rubric 1.9 — `table_state.snapshot_state` has exactly one writer, and it checks.

This is the machine the architecture review called "THE BIG ONE": one durable column
written from five modules, a declared domain that had drifted from the ADR in both
directions, and one durable non-terminal value (`in_progress`) that no durable queue
selected — with a measured consequence, because the recovery journal's "does anything
still owe work?" test could pass over a half-snapshotted table and log *"recovery
COMPLETE: every captured table has a fresh image"*.

Milliseconds, DuckDB in a tmp dir, no engine.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination as dest_mod
from cdc_flight import table_lifecycle as tl
from cdc_flight.states import IllegalTransition, UnknownState

PIPELINE = "lifecycle"
SRC = Path(__file__).resolve().parents[2] / "src" / "cdc_flight"


@pytest.fixture
def con(tmp_path):
    connection = duckdb.connect(str(tmp_path / "dest.duckdb"))
    dest_mod.ensure_control_schema(connection)
    try:
        yield connection
    finally:
        connection.close()


def _to(con, table: str, state: str, **kw):
    return tl.transition(
        con, pipeline=PIPELINE, source_schema="app", source_table=table,
        to=state, reason="a test", target_table=f"cdcflight_app_{table}", **kw
    )


# --------------------------------------------------------------------------- #
# one writer
# --------------------------------------------------------------------------- #
def test_nothing_outside_table_lifecycle_writes_the_column():
    """A machine with two writers is a machine with one writer and one bug pending.

    Greps the shipped source. The 1.6-1.8 round froze the *domain* and validated reads;
    what stayed open was that five modules still wrote the column with their own SQL, so
    "every write goes through the machine" was a convention rather than a property.
    """
    offenders: list[str] = []
    # A *write* context: `SET snapshot_state = ...`, or the column named in an INSERT
    # list. Reads (`WHERE snapshot_state = 'complete'`) are fine — they go through
    # `read()`/`read_all()` where it matters and are harmless where it does not — and a
    # Python local that happens to be called `snapshot_state` is not this column at all.
    write = re.compile(
        r"SET\s+snapshot_state|snapshot_state\s*,\s*$|snapshot_state\s*\)", re.IGNORECASE
    )
    for path in sorted(SRC.glob("*.py")):
        if path.name in ("table_lifecycle.py", "control_schema.py", "machines.py"):
            continue  # the writer, the DDL, and the declaration
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith("*"):
                continue
            if not write.search(line):
                continue
            if not re.search(r"table_state|INSERT|UPDATE|VALUES", line, re.IGNORECASE):
                continue
            offenders.append(f"{path.name}:{number}: {stripped}")
    assert not offenders, (
        "these lines write `table_state.snapshot_state` outside "
        "`cdc_flight.table_lifecycle`, so they bypass the declared transition table:\n"
        + "\n".join(offenders)
    )


# --------------------------------------------------------------------------- #
# the edges
# --------------------------------------------------------------------------- #
def test_a_row_is_created_by_the_transition_that_needs_it(con):
    assert tl.read(con, pipeline=PIPELINE, source_schema="app", source_table="c") == tl.ABSENT
    assert _to(con, "c", tl.NONE) == tl.ABSENT
    assert tl.read(con, pipeline=PIPELINE, source_schema="app", source_table="c") == tl.NONE


def test_the_full_happy_path(con):
    _to(con, "c", tl.NONE)
    _to(con, "c", tl.IN_PROGRESS, epoch=3)
    _to(con, "c", tl.COMPLETE, snapshot_lsn=999, last_commit_id=7)
    row = con.execute(
        "SELECT snapshot_state, snapshot_epoch, snapshot_lsn, last_commit_id "
        "FROM _cdc_flight.table_state WHERE pipeline = ?", [PIPELINE]
    ).fetchone()
    assert row == ("complete", 3, 999, 7)


def test_an_undeclared_transition_is_refused_and_alerted(con):
    """Loud, and on the connection that survives a rollback.

    Refusing leaves the previous state, which is always the more conservative of the
    two: every edge INTO a terminal state is declared, so only an undeclared edge can
    lose owed work.
    """
    _to(con, "c", tl.NONE)

    class _Sink:
        def __init__(self):
            self.raised = []

        def raise_alert(self, **kw):
            self.raised.append(kw)

    sink = _Sink()
    with pytest.raises(IllegalTransition):
        _to(con, "c", tl.COMPLETE, alerts=sink)
    assert tl.read(con, pipeline=PIPELINE, source_schema="app", source_table="c") == tl.NONE
    assert sink.raised, "an illegal transition must reach an operator"
    assert sink.raised[0]["code"] == "illegal_table_lifecycle_transition"
    assert sink.raised[0]["severity"] == "critical"


def test_a_second_snapshot_over_a_durable_half_finished_one_is_refused(con):
    """The `in_progress` residue, as a test rather than as a comment.

    `SnapshotCoordinator.state_for()` writes `in_progress` the instant a table's first
    snapshot record arrives. A process killed inside a snapshot leaves it there. If a
    later run could simply open a second shadow over it, nothing anywhere would record
    that an image was abandoned; the declared route is start-up promotion to
    `awaiting_snapshot`, and there is no other.
    """
    _to(con, "c", tl.IN_PROGRESS)
    with pytest.raises(IllegalTransition) as raised:
        _to(con, "c", tl.IN_PROGRESS)
    assert "in_progress" in str(raised.value)

    # The declared route works, and then the snapshot may start.
    dest_mod.promote_interrupted_snapshots(con, PIPELINE)
    assert tl.read(
        con, pipeline=PIPELINE, source_schema="app", source_table="c"
    ) == tl.AWAITING
    _to(con, "c", tl.IN_PROGRESS)


def test_the_owed_queue_selects_every_non_terminal_state(con):
    _to(con, "c", tl.IN_PROGRESS)
    _to(con, "o", tl.AWAITING)
    _to(con, "a", tl.NONE)
    _to(con, "d", tl.AWAITING)
    _to(con, "d", tl.IN_PROGRESS)
    _to(con, "d", tl.COMPLETE, snapshot_lsn=1)
    owed = {f"{s}.{t}" for s, t, _ in dest_mod.tables_awaiting_snapshot(con, PIPELINE)}
    assert owed == {"app.c", "app.o"}
    assert tl.owing_work(con, PIPELINE) == ["app.c", "app.o"]


def test_a_state_outside_the_domain_is_refused_on_read(con):
    """ADR §4.8 declared `failed` and nothing ever wrote it; `awaiting_snapshot` was
    written by three modules and never declared. A value in neither belongs to no
    queue."""
    _to(con, "c", tl.NONE)
    con.execute(
        "UPDATE _cdc_flight.table_state SET snapshot_state = 'failed' WHERE pipeline = ?",
        [PIPELINE],
    )
    with pytest.raises(UnknownState) as raised:
        tl.read_all(con, PIPELINE)
    assert "failed" in str(raised.value)
    assert "app.c" in str(raised.value)


def test_reset_state_walks_every_row_back_to_none_through_declared_edges(con):
    _to(con, "c", tl.IN_PROGRESS, epoch=4)
    _to(con, "o", tl.AWAITING)
    _to(con, "a", tl.NONE)
    _to(con, "d", tl.AWAITING)
    _to(con, "d", tl.COMPLETE, snapshot_lsn=5, last_commit_id=6)
    moved = tl.reset_all(con, pipeline=PIPELINE, reason="--reset-state")
    assert moved == ["app.c", "app.d", "app.o"]
    rows = con.execute(
        "SELECT DISTINCT snapshot_state, snapshot_epoch, snapshot_lsn, last_commit_id "
        "FROM _cdc_flight.table_state WHERE pipeline = ?", [PIPELINE]
    ).fetchall()
    assert rows == [("none", 0, None, None)]
    # And the ownership registry survives: deleting it produced a permanent zombie
    # destination table (Opus MAJOR-4, measured).
    assert con.execute(
        "SELECT count(*) FROM _cdc_flight.table_state WHERE pipeline = ?", [PIPELINE]
    ).fetchone()[0] == 4


def test_forgetting_a_table_is_an_edge_too(con):
    _to(con, "c", tl.NONE)
    dest_mod.forget_table_state(
        con, pipeline=PIPELINE, source_schema="app", source_table="c"
    )
    assert tl.read(
        con, pipeline=PIPELINE, source_schema="app", source_table="c"
    ) == tl.ABSENT
    # Idempotent: forgetting an absent table is not an illegal `absent -> absent`.
    dest_mod.forget_table_state(
        con, pipeline=PIPELINE, source_schema="app", source_table="c"
    )


def test_register_table_does_not_overwrite_a_rebuild_that_is_owed(con):
    """`absent -> none` and nothing else. A table that already has a row is already
    registered, and re-registering it would overwrite "a rebuild is owed" with "never
    snapshotted" — which is the durable to-do list quietly losing an entry."""
    _to(con, "c", tl.AWAITING)
    dest_mod.register_table(
        con, pipeline=PIPELINE, source_schema="app", source_table="c",
        target_table="cdcflight_app_c",
    )
    assert tl.read(
        con, pipeline=PIPELINE, source_schema="app", source_table="c"
    ) == tl.AWAITING


def test_the_real_coordinator_refuses_the_residue_before_it_touches_anything(con):
    """The coordinator-level regression the review asked for (Codex r2 MINOR-2).

    `test_a_second_snapshot_over_a_durable_half_finished_one_is_refused` drives the
    lifecycle *writer* directly, and would stay green if `SnapshotCoordinator.state_for`
    went back to dropping the shadow, forgetting it in the registry and adding it to
    `created_in_txn` **before** asking whether the edge is legal — which is exactly what
    it used to do. This drives the real coordinator and asserts that none of those three
    side effects happened.
    """
    from cdc_flight.snapshot import SnapshotCoordinator

    con.execute("CREATE SCHEMA IF NOT EXISTS cdc_raw")
    con.execute("CREATE TABLE cdc_raw.cdcflight_app_c__cdcf_tmp (id BIGINT)")
    _to(con, "c", tl.IN_PROGRESS)  # a process died inside this table's snapshot

    created: set[str] = set()
    coordinator = SnapshotCoordinator(
        con,
        dataset="cdc_raw",
        pipeline=PIPELINE,
        topic_prefix="cdcflight",
        created_in_txn=lambda: created,
        get_registry=lambda: _Registry(),
        epoch=0,
        transactional_ddl=True,
    )
    with pytest.raises(IllegalTransition):
        coordinator.state_for("app", "c")

    assert created == set(), "created_in_txn was mutated for a refused snapshot"
    assert coordinator._tables == {}, "the coordinator recorded a table it refused"
    survived = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'cdc_raw' "
        "AND table_name = 'cdcflight_app_c__cdcf_tmp'"
    ).fetchone()[0]
    assert survived == 1, (
        "the shadow table was dropped before the edge was checked; the refusal then "
        "leaves the coordinator's view and the destination disagreeing"
    )
    # ... and the declared route still works, through the real coordinator.
    dest_mod.promote_interrupted_snapshots(con, PIPELINE)
    assert coordinator.state_for("app", "c") is not None


class _Registry:
    """The two calls `state_for` makes on the registry, recorded rather than performed."""

    def __init__(self) -> None:
        self.forgotten: list[str] = []

    def forget(self, name: str) -> None:
        self.forgotten.append(name)

    def drop(self, name: str) -> None:  # pragma: no cover - not reached here
        self.forgotten.append(name)
