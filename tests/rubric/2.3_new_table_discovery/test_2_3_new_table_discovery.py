"""Rubric 2.3: publication-backed table/schema discovery without config changes."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from cdc_flight import destination
from cdc_flight.catalog import (
    CHANGE_NEW,
    CatalogChange,
    CatalogWatcher,
    SourceRelation,
    read_known_relations,
)
from cdc_flight.catalog_apply import CatalogCoordinator
from cdc_flight.config import CatalogConfig
from cdc_flight.machines import (
    ADMISSION_ERROR,
    ADMISSION_EXTERNAL,
    ADMISSION_REFUSED,
    SCHEMA_EMPTY,
    SCHEMA_ERROR,
    SCHEMA_UNAVAILABLE,
    SCHEMA_VISIBLE,
)
from cdc_flight.schema_evolution import COLUMN_TYPE_CHANGED, ColumnChange


def relation(schema: str, table: str, oid: int, *, published: bool = False):
    return SourceRelation(schema, table, oid, published, "d")


def test_unconfigured_relations_in_all_watched_schemas_are_discovery_candidates():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        schemas={"app", "new_schema"},
        auto_discover=True,
        include={"app.customers"},
        known={"app.customers": relation("app", "customers", 1, published=True)},
        replicated={"app.customers"},
        poll_seconds=0,
    )

    changes = watcher._compare(
        {
            "app.customers": relation("app", "customers", 1, published=True),
            "app.arrival": relation("app", "arrival", 2),
            "new_schema.arrival": relation("new_schema", "arrival", 3),
        },
        lsn=100,
    )

    assert [(change.kind, change.qualified) for change in changes] == [
        (CHANGE_NEW, "app.arrival"),
        (CHANGE_NEW, "new_schema.arrival"),
    ]


def test_completed_resnapshot_closes_new_hook_without_duplicate_marker():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        auto_discover=True,
        include={"app.customers"},
        known={"app.customers": relation("app", "customers", 1, published=True)},
        replicated={"app.customers"},
        poll_seconds=0,
    )
    changes = watcher._compare(
        {
            "app.customers": relation("app", "customers", 1, published=True),
            "app.arrival": relation("app", "arrival", 2, published=True),
        },
        lsn=100,
    )

    assert [change.qualified for change in changes] == ["app.arrival"]
    assert watcher.complete_discoveries({"app.arrival"}) == ["app.arrival"]
    assert watcher.new_relations() == ()
    assert watcher.replicated == {"app.customers", "app.arrival"}
    assert changes[0].history[-2:] == ["due", "applied"]


def test_catalog_poll_default_is_short_and_configurable():
    assert 0 < CatalogConfig().poll_seconds <= 60


def test_discovery_ownership_modules_stay_below_the_maintainability_boundary():
    root = Path(__file__).resolve().parents[3]
    for relative in (
        "src/cdc_flight/catalog.py",
        "src/cdc_flight/pipeline.py",
        "src/cdc_flight/applier.py",
        "src/cdc_flight/resnapshot.py",
        "src/cdc_flight/resnapshot_compat.py",
        "src/cdc_flight/resnapshot_projection.py",
        "src/cdc_flight/state_interactions.py",
    ):
        assert len((root / relative).read_text().splitlines()) < 1000, relative


def test_liveness_is_per_schema_and_error_states_never_mean_mass_drop():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        schemas={"app", "new_schema"},
        include={"app.customers"},
        known={
            "app.customers": relation("app", "customers", 1, published=True),
            "new_schema.legacy": relation("new_schema", "legacy", 2, published=True),
        },
        replicated={"app.customers", "new_schema.legacy"},
        poll_seconds=0,
    )
    observed = {
        "app.customers": relation("app", "customers", 1, published=True),
    }
    for state in (SCHEMA_EMPTY, SCHEMA_UNAVAILABLE, SCHEMA_ERROR):
        watcher._schema_liveness = {"app": SCHEMA_VISIBLE, "new_schema": state}
        changes = watcher._compare(observed, lsn=100)
        assert not [change for change in changes if change.kind != CHANGE_NEW], state


def test_catalog_query_error_enters_the_declared_liveness_error_state():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        schemas={"app", "new_schema"},
        include=set(),
        poll_seconds=0,
    )

    def fail():
        raise RuntimeError("catalog connection lost")

    watcher.poll = fail
    assert watcher.poll_quietly() == []
    assert watcher.summary()["catalog_schema_liveness"] == {
        "app": SCHEMA_ERROR,
        "new_schema": SCHEMA_ERROR,
    }


@pytest.mark.parametrize(
    "changed", [
        {"type_name": "varchar(128)", "nullable": True},
        {"type_name": "varchar(32)", "nullable": False},
    ],
)
def test_confirmation_fingerprint_includes_full_column_identity(changed):
    """A type modifier or nullability change restarts the confirmation streak."""
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        include={"app.customers"},
        confirm_polls=2,
        poll_seconds=0,
    )

    def observation(type_name: str, nullable: bool):
        return CatalogChange(
            kind="schema_changed",
            schema="app",
            table="customers",
            detected_lsn=100,
            old_oid=1,
            new_oid=1,
            column_changes=(
                ColumnChange(
                    COLUMN_TYPE_CHANGED,
                    2,
                    old_name="name",
                    new_name="name",
                    type_oid=1043,
                    type_name=type_name,
                    nullable=nullable,
                ),
            ),
        )

    assert watcher._confirm("app.customers", observation("varchar(64)", True)) is None
    assert watcher._confirm(
        "app.customers", observation(changed["type_name"], changed["nullable"])
    ) is None
    assert watcher._unconfirmed["app.customers"].confirmations == 1
    confirmed = watcher._confirm(
        "app.customers", observation(changed["type_name"], changed["nullable"])
    )
    assert confirmed is not None
    assert confirmed.confirmations == 2


def test_mass_discovery_adds_are_safe_but_alerted():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        auto_discover=True,
        include=set(),
        poll_seconds=0,
    )
    changes = [
        CatalogChange(kind=CHANGE_NEW, schema="app", table=name, detected_lsn=100)
        for name in ("one", "two")
    ]
    for change in changes:
        watcher.queue(change)
    coordinator = CatalogCoordinator(
        catalog=watcher,
        pipeline="test",
        topic_prefix="cdcflight",
        drop_mode="replicate",
        registry_of=lambda: None,
    )

    plan = coordinator.plan(100)

    assert len(plan.actions) == 2
    assert all(not action.destructive for action in plan.actions)
    assert [alert["code"] for alert in plan.alerts] == ["mass_add_observed"]
    assert plan.alerts[0]["context"]["safe_default"] is True


def test_failed_publication_admission_is_retried_and_remains_visible():
    """BLOCKER reproduction: a failed ADD must not disappear on the next poll."""

    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        auto_discover=True,
        include=set(),
        poll_seconds=0,
    )
    change = CatalogChange(
        kind=CHANGE_NEW,
        schema="app",
        table="arrival",
        detected_lsn=100,
        new_relation=relation("app", "arrival", 2, published=False),
    )
    watcher.known["app.arrival"] = change.new_relation
    watcher.queue(change)

    class FailingConnection:
        attempts = 0

        def execute(self, sql):
            if "ALTER PUBLICATION" in sql:
                self.attempts += 1
                raise RuntimeError("publication is read-only")

    conn = FailingConnection()
    observed = {"app.arrival": change.new_relation}
    watcher._ensure_published(conn, observed, [change])
    watcher._ensure_published(conn, observed, [])

    assert conn.attempts == 2
    assert watcher.known["app.arrival"].admission_state == ADMISSION_ERROR
    assert watcher.new_relations() == ()
    assert watcher.pending_admission() == ("app.arrival",)


def test_external_publication_ownership_refuses_then_admits_only_after_membership():
    """The owner is policy, and an external owner can recover without ALTER from Flight."""

    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        auto_discover=True,
        publication_ownership="external",
        include=set(),
        poll_seconds=0,
    )
    change = CatalogChange(
        kind=CHANGE_NEW,
        schema="app",
        table="arrival",
        detected_lsn=100,
        new_relation=relation("app", "arrival", 2, published=False),
    )
    watcher.known[change.qualified] = change.new_relation
    watcher.queue(change)

    class ForbiddenConnection:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            raise AssertionError(f"external ownership must not execute {sql}")

    conn = ForbiddenConnection()
    unpublished = {change.qualified: change.new_relation}
    watcher._ensure_published(conn, unpublished, [change])
    assert conn.calls == 0
    assert watcher.known[change.qualified].admission_state == ADMISSION_REFUSED
    assert watcher.pending_admission() == (change.qualified,)
    assert watcher.new_relations() == ()

    published = {change.qualified: relation("app", "arrival", 2, published=True)}
    watcher._ensure_published(conn, published, [])
    assert watcher.known[change.qualified].admission_state == ADMISSION_EXTERNAL
    assert watcher.pending_admission() == ()
    assert tuple(item.qualified for item in watcher.new_relations()) == (change.qualified,)


def test_failed_admission_is_durable_and_retried_after_watcher_restart(tmp_path):
    """The admission ERROR survives a quiet failed run and a new process."""
    con = duckdb.connect(str(tmp_path / "admission.duckdb"))
    try:
        destination.ensure_control_schema(con)
        watcher = CatalogWatcher(
            dsn="",
            publication="cdc_flight_pub",
            schema="app",
            auto_discover=True,
            include=set(),
            poll_seconds=0,
        )
        change = CatalogChange(
            kind=CHANGE_NEW,
            schema="app",
            table="arrival",
            detected_lsn=100,
            new_relation=relation("app", "arrival", 2, published=False),
        )
        watcher.known[change.qualified] = change.new_relation
        watcher.queue(change)

        class FailingConnection:
            def execute(self, sql):
                raise RuntimeError("publication is read-only")

        observed = {change.qualified: change.new_relation}
        watcher._ensure_published(FailingConnection(), observed, [change])
        destination.flush_learned_relations(con, pipeline="p", catalog=watcher)
        assert con.execute(
            "SELECT admission_state FROM _cdc_flight.source_relations "
            "WHERE pipeline = 'p' AND source_table = 'arrival'"
        ).fetchone()[0] == ADMISSION_ERROR

        restarted = CatalogWatcher(
            dsn="",
            publication="cdc_flight_pub",
            schema="app",
            auto_discover=True,
            include=set(),
            known=read_known_relations(con, "p"),
            poll_seconds=0,
        )
        changes = restarted._compare(
            {"app.arrival": relation("app", "arrival", 2, published=False)},
            lsn=200,
        )
        assert [item.kind for item in changes] == [CHANGE_NEW]
        restarted._ensure_published(
            FailingConnection(),
            {"app.arrival": relation("app", "arrival", 2, published=False)},
            changes,
        )
        assert restarted.known["app.arrival"].admission_state == ADMISSION_ERROR

        published = {"app.arrival": relation("app", "arrival", 2, published=True)}
        changes = restarted._compare(published, lsn=300)
        restarted._ensure_published(FailingConnection(), published, changes)
        assert restarted.known["app.arrival"].admission_state == ADMISSION_EXTERNAL
    finally:
        con.close()


def test_live_handoff_requires_a_dead_watcher_before_restart():
    watcher = CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        include=set(),
        poll_seconds=1,
    )

    class LiveThread:
        def is_alive(self):
            return True

        def join(self, timeout):
            return None

    watcher._thread = LiveThread()
    watcher.quiesce_timeout = 0
    assert watcher.stop() is False
    with pytest.raises(RuntimeError, match="previous polling thread"):
        watcher.start()
