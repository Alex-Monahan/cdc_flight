"""Rubric 1.5 — the four guards between "the table is gone" and `DROP TABLE`.

A detected drop and the DDL that acts on it are separated in time by the LSN fence,
and on a quiet source that gap is unbounded. The review round found that the code
crossed it without ever asking whether the fact still held, and that one poll could
destroy every destination table it owned. So:

| guard | what it refuses | finding |
|---|---|---|
| the fence | applying before the destination has consumed everything before the DDL | (already held) |
| confirmation | acting on a single observation | Opus Q5 |
| supersession | acting on an observation a later poll contradicted | Codex 4 |
| revalidation | acting without re-reading the source, and acting when it cannot be read | Codex 4 |
| the circuit breaker | destroying more than one relation at once | Opus MAJOR-3 / Q2 |

Everything here drives the shipped `CatalogWatcher` and `CatalogCoordinator`; the
source query itself is stubbed, because a watcher with no DSN must never fall back to
libpq defaults (that would reach `:5432`, which this project never touches).
"""

from __future__ import annotations

import pytest
from applier_lab import DATASET, Lab, end, keyed

from cdc_flight import faults
from cdc_flight.catalog import (
    CHANGE_DROPPED,
    CHANGE_RECREATED,
    CatalogChange,
    CatalogWatcher,
    SourceRelation,
)

CUSTOMERS = "cdcflight_app_customers"
ORDERS = "cdcflight_app_orders"


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


def relation(table: str, oid: int, *, published: bool = True) -> SourceRelation:
    return SourceRelation(
        schema="app", table=table, oid=oid, published=published, replica_identity="d"
    )


def watcher(*, present=None, fail_revalidation: bool = False, **kw) -> CatalogWatcher:
    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0, **kw
    )
    oids = dict(present or {})

    def relation_oids(names):
        if fail_revalidation:
            raise RuntimeError("the source could not be reached")
        return {f"{s}.{t}": oids.get(f"{s}.{t}") for s, t in names}

    w.relation_oids = relation_oids  # type: ignore[method-assign]
    return w


def queue(w: CatalogWatcher, table: str, *, kind: str = CHANGE_DROPPED, **kw) -> CatalogChange:
    change = CatalogChange(
        kind=kind, schema="app", table=table, detected_lsn=kw.pop("lsn", 100),
        fenced=True, **kw,
    )
    w._pending.append(change)
    return change


def txn(number: str, events: list) -> list:
    counts: dict[str, int] = {}
    for event in events:
        name = f"{event.schema}.{event.table}"
        counts[name] = counts.get(name, 0) + 1
    return [*events, end(number, len(events), max(e.lsn or 0 for e in events) + 1, counts)]


def preload(box: Lab) -> None:
    box.run(
        txn(
            "1",
            [
                keyed("1", 1, 10, 1, "c1"),
                keyed("1", 2, 11, 7, "o7", table="orders"),
            ],
        )
    )


def tick(box: Lab, number: str = "2", lsn: int = 300) -> None:
    """One ordinary commit group, which is when catalog changes are applied."""
    box.run(txn(number, [keyed(number, 1, lsn, 9, "unrelated", table="orders")]))


def alerts(box: Lab) -> list[tuple]:
    return box.q("SELECT severity, code FROM _cdc_flight.alerts ORDER BY raised_at, code")


# --------------------------------------------------------------------------- #
# guard: confirmation (Opus Q5)
# --------------------------------------------------------------------------- #
def test_one_observation_is_not_enough_to_queue_a_drop():
    w = watcher(known={"app.customers": relation("customers", 1)}, replicated={"app.customers"})
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    assert w._compare({"app.orders": relation("orders", 2)}, lsn=10) == []
    assert w.pending() == [], "a single poll must not queue a destructive action"
    added = w._compare({"app.orders": relation("orders", 2)}, lsn=20)
    assert [c.kind for c in added] == [CHANGE_DROPPED]
    assert added[0].confirmations == 2


def test_confirm_polls_one_restores_single_poll_detection():
    w = watcher(confirm_polls=1)
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    added = w._compare({"app.orders": relation("orders", 2)}, lsn=10)
    assert [c.kind for c in added] == [CHANGE_DROPPED]


def test_a_relation_that_reappears_resets_the_confirmation():
    w = watcher()
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    assert w._compare({"app.orders": relation("orders", 2)}, lsn=10) == []
    # It was a transient catalog read (a concurrent DDL, a mid-migration moment).
    assert w._compare(
        {"app.customers": relation("customers", 1), "app.orders": relation("orders", 2)},
        lsn=20,
    ) == []
    assert w._compare({"app.orders": relation("orders", 2)}, lsn=30) == [], "streak reset"
    assert w.pending() == []


# --------------------------------------------------------------------------- #
# guard: supersession (Codex 4)
# --------------------------------------------------------------------------- #
def test_a_relation_that_comes_back_cancels_its_pending_drop():
    """Codex 4's sharper race: poll P sees the name absent and queues `dropped`; the
    source recreates and republishes it before P's action is applied. `_compare` used
    to append `new`/`recreated` without superseding the pending drop, and `due()` then
    returned both in detection order - so the group landed the replacement's rows and
    then dropped their destination table."""
    w = watcher(confirm_polls=1)
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    assert [c.kind for c in w._compare({"app.orders": relation("orders", 2)}, lsn=10)] == [
        CHANGE_DROPPED
    ]
    assert len(w.pending_destructive()) == 1
    added = w._compare(
        {"app.customers": relation("customers", 5), "app.orders": relation("orders", 2)},
        lsn=20,
    )
    assert w.superseded == 1, "the queued drop describes a world that is gone"
    # The name is back under a DIFFERENT oid, so what is pending now is a `recreated` -
    # the destination table holds the old relation's rows - and never the stale drop.
    assert [c.kind for c in w.pending_destructive()] == [CHANGE_RECREATED]
    assert [c.kind for c in added] == [CHANGE_RECREATED]


def test_a_relation_that_comes_back_unchanged_leaves_nothing_pending():
    """The transient-catalog case: one poll missed it, the next sees the same oid."""
    w = watcher(confirm_polls=1)
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    assert [c.kind for c in w._compare({}, lsn=10)] == [CHANGE_DROPPED]
    assert w._compare({"app.customers": relation("customers", 1)}, lsn=20) == []
    assert w.pending_destructive() == []
    assert "app.customers" in w.replicated, "and it is still ours"


def test_a_relation_that_goes_away_cancels_its_pending_recreate():
    w = watcher(confirm_polls=1)
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers"}
    assert [c.kind for c in w._compare({"app.customers": relation("customers", 2)}, lsn=10)] == [
        CHANGE_RECREATED
    ]
    added = w._compare({"app.orders": relation("orders", 9)}, lsn=20)
    assert [c.kind for c in added] == [CHANGE_DROPPED]
    assert [c.kind for c in w.pending_destructive()] == [CHANGE_DROPPED]


# --------------------------------------------------------------------------- #
# guard: revalidation (Codex 4)
# --------------------------------------------------------------------------- #
def test_a_relation_that_exists_again_is_never_dropped(lab):
    """The queued fact is stale by the time the fence opens. The destination table now
    belongs to a LIVE relation, and dropping it would destroy rows this pipeline has
    already captured for it."""
    w = watcher(present={"app.customers": 4711})
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    tick(box)
    assert box.exists(CUSTOMERS), "a live relation's destination table must survive"
    assert len(w.pending_destructive()) == 1, "and the change stays pending"
    assert ("warning", "destructive_change_deferred") in alerts(box)


def test_a_source_that_cannot_be_re_read_fails_closed(lab):
    w = watcher(fail_revalidation=True)
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    tick(box)
    assert box.exists(CUSTOMERS), "'I could not ask' is not 'it is gone'"
    assert len(w.pending_destructive()) == 1


def test_a_recreate_whose_oid_changed_again_is_not_applied(lab):
    w = watcher(present={"app.customers": 12345})
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", kind=CHANGE_RECREATED, old_oid=1, new_oid=99999)
    tick(box)
    assert box.exists(CUSTOMERS)
    assert len(w.pending_destructive()) == 1


def test_revalidation_can_be_switched_off(lab):
    """`CDC_DROP_REVALIDATE=0` is for a deployment that cannot afford the extra source
    read; it removes guard 3 and nothing else."""
    w = watcher(present={"app.customers": 4711})
    box = lab(catalog=w, drop_revalidate=False)
    preload(box)
    queue(w, "customers", old_oid=1)
    tick(box)
    assert not box.exists(CUSTOMERS)


def test_a_watcher_with_no_dsn_refuses_to_query_rather_than_using_libpq_defaults():
    """An empty DSN makes libpq connect to its own defaults, which on this machine is
    `:5432` - a cluster this project must never touch."""
    w = CatalogWatcher(
        dsn="", publication="pub", schema="app", include=set(), poll_seconds=0
    )
    with pytest.raises(ValueError, match="no DSN"):
        w.relation_oids({("app", "customers")})


# --------------------------------------------------------------------------- #
# guard: the circuit breaker (Opus MAJOR-3 / Q2)
# --------------------------------------------------------------------------- #
def test_two_drops_in_one_group_are_both_refused(lab):
    """`DROP SCHEMA app CASCADE` yields one `dropped` per table. Applying the first N
    and stopping halfway would be the worst of both, so none of the set is applied."""
    w = watcher()
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    queue(w, "orders", old_oid=2)
    box.run(txn("2", [keyed("2", 1, 300, 5, "x", table="wide_types")]))
    assert box.exists(CUSTOMERS) and box.exists(ORDERS)
    assert len(w.pending_destructive()) == 2
    assert ("critical", "mass_drop_refused") in alerts(box)
    assert box.applier.stats()["catalog_destructive_refused"] >= 2


def test_a_single_drop_stays_fully_automatic(lab):
    w = watcher()
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    tick(box)
    assert not box.exists(CUSTOMERS)
    assert box.exists(ORDERS)


def test_the_breaker_can_be_raised_or_authorised(lab):
    w = watcher()
    box = lab(catalog=w, drop_allow_mass=True)
    preload(box)
    queue(w, "customers", old_oid=1)
    queue(w, "orders", old_oid=2)
    box.run(txn("2", [keyed("2", 1, 300, 5, "x", table="wide_types")]))
    assert not box.exists(CUSTOMERS)
    assert not box.exists(ORDERS)


def test_a_poll_that_sees_an_empty_schema_is_discarded(lab):
    """Opus Q2's absolute guard. A DSN pointed at the wrong database, a failover target
    that has not been migrated, or a source mid-`pg_restore` all look exactly like
    "every table was dropped", and that can never legitimately be what it means."""
    w = watcher(confirm_polls=1)
    w.known = {"app.customers": relation("customers", 1)}
    w.replicated = {"app.customers", "app.orders"}

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, sql, params=None):
            class R:
                def fetchall(self_inner):
                    return []

                def fetchone(self_inner):
                    return (777,)

            return R()

    import psycopg

    original = psycopg.connect
    psycopg.connect = lambda *a, **k: Conn()
    try:
        assert w.poll() == []
    finally:
        psycopg.connect = original
    assert w.pending() == []
    assert w.empty_polls == 1
    assert "no tables at all" in (w.last_error or "")


# --------------------------------------------------------------------------- #
# the alert has to survive the rollback it is about (Codex 7 / Opus M-2)
# --------------------------------------------------------------------------- #
def test_a_destructive_change_that_cannot_be_applied_still_alerts_after_a_rollback(
    lab, monkeypatch
):
    """Measured before the fix: inject `pre_commit:raise` after a detected drop and the
    DDL correctly rolls back while `_cdc_flight.alerts` is EMPTY - so a destructive
    change that keeps failing to apply produced no signal at all, which is precisely
    the case ADR §9.1 introduces the out-of-transaction alert for."""
    w = watcher()
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    queue(w, "orders", old_oid=2)  # refused by the breaker -> a critical alert
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        box.run(txn("2", [keyed("2", 1, 300, 5, "x", table="wide_types")]))
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert box.applier.alerts.independent, "the sink must be a separate connection"
    assert ("critical", "mass_drop_refused") in alerts(box), (
        "an alert about a refusal must not be discarded by the rollback"
    )
    assert box.q(f'SELECT count(*) FROM "{DATASET}"."{CUSTOMERS}"') == [(1,)]
    assert box.q("SELECT count(*) FROM _cdc_flight.table_events") == [(0,)]


def test_an_alert_about_an_applied_drop_does_not_outlive_a_rollback(lab, monkeypatch):
    """The other direction, and it matters just as much: "your destination table was
    dropped" must not be reported for a drop the rollback undid."""
    w = watcher()
    box = lab(catalog=w)
    preload(box)
    queue(w, "customers", old_oid=1)
    nth = box.applier.data_commit_groups + 1
    monkeypatch.setenv("CDC_FAULT_INJECT", f"pre_commit:{nth}:raise")
    faults.refresh()
    with pytest.raises(faults.InjectedFault):
        tick(box)
    monkeypatch.delenv("CDC_FAULT_INJECT")
    faults.refresh()
    assert box.exists(CUSTOMERS)
    assert alerts(box) == []
    assert len(w.pending_destructive()) == 1

    tick(box, "3", 400)
    assert not box.exists(CUSTOMERS)
    assert alerts(box) == [("warning", "table_dropped")]
