"""Rubric 2.3: publication-backed table/schema discovery without config changes."""

from __future__ import annotations

from cdc_flight.catalog import CHANGE_NEW, CatalogChange, CatalogWatcher, SourceRelation
from cdc_flight.catalog_apply import CatalogCoordinator
from cdc_flight.config import CatalogConfig


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
