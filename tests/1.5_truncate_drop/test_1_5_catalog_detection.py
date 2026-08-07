"""Rubric 1.5 — detecting the DDL the replication stream does not carry.

pgoutput carries INSERT / UPDATE / DELETE / TRUNCATE and logical-decoding messages.
It does not carry DDL, and the Postgres connector has no DDL event source, so a
`DROP TABLE` is indistinguishable from a table that stopped changing — which is
exactly what the baseline measured (`cdcflight_app_documents` kept its two rows
forever after `DROP TABLE app.documents`).

`cdc_flight.catalog.CatalogWatcher` closes that by polling the source catalog for
two facts logical decoding cannot give us: the relation `oid` (which distinguishes a
dropped-and-recreated table from the one we were replicating) and publication
membership. This module tests the comparison and the fence in isolation — no
Postgres, no JVM — and `test_1_5_truncate_drop_e2e.py` proves the same against a
real cluster.
"""

from __future__ import annotations

import pytest

from cdc_flight import catalog_generation
from cdc_flight.catalog import (
    CHANGE_DROPPED,
    CHANGE_NEW,
    CHANGE_RECREATED,
    CHANGE_REPUBLISHED,
    CHANGE_UNPUBLISHED,
    CatalogChange,
    CatalogWatcher,
    SourceRelation,
)


def relation(table: str, oid: int, *, published: bool = True, identity: str = "d"):
    return SourceRelation(
        schema="app", table=table, oid=oid, published=published, replica_identity=identity
    )


def watcher(*, known=None, replicated=None, include=("app.customers",), **kw) -> CatalogWatcher:
    return CatalogWatcher(
        dsn="",
        publication="cdc_flight_pub",
        schema="app",
        include=set(include),
        known={r.qualified: r for r in (known or ())},
        replicated=set(replicated or ()),
        poll_seconds=0,
        **kw,
    )


def kinds(changes) -> list[tuple[str, str]]:
    return [(c.kind, c.qualified) for c in changes]


def confirmed(w: CatalogWatcher, observed: dict, lsn: int) -> list:
    """Poll until a destructive observation is confirmed (`CDC_DROP_CONFIRM_POLLS`).

    Returns the changes the *confirming* poll added, and asserts every poll before it
    added nothing - a destructive action must not be queued on a single observation
    (Opus Q5). The lsn advances between polls, exactly as a real source's would.
    """
    for attempt in range(w.confirm_polls - 1):
        assert w._compare(observed, lsn=lsn + attempt) == [], (
            f"poll {attempt + 1} of {w.confirm_polls} must not queue anything yet"
        )
    return w._compare(observed, lsn=lsn + w.confirm_polls - 1)


# --------------------------------------------------------------------------- #
# comparison
# --------------------------------------------------------------------------- #
def test_a_table_that_disappeared_is_a_drop():
    w = watcher(known=[relation("customers", 16384)], replicated=["app.customers"])
    changes = confirmed(w, {}, lsn=500)
    assert kinds(changes) == [(CHANGE_DROPPED, "app.customers")]
    assert changes[0].detected_lsn == 501
    assert changes[0].old_oid == 16384
    # The oid and the membership are deliberately KEPT while the action is pending: a
    # queued drop can still be refused (guard 3) or cancelled (guard 2), and forgetting
    # them made a cancelled drop indistinguishable from a table we never had. They are
    # forgotten by `forget()`, which the applier calls only after the DROP has COMMITTED.
    assert "app.customers" in w.known
    w.forget("app.customers")
    assert "app.customers" not in w.known


def test_a_replicated_table_with_no_persisted_oid_that_is_gone_is_still_a_drop():
    """The restart case, and the one that matters most.

    A table dropped while the pipeline was down - or one that was replicated before
    `source_relations` existed - has no oid on file. MEASURED: the first cut of this
    watcher skipped that combination, and a `DROP TABLE` performed between two runs
    was never noticed at all.
    """
    w = watcher(replicated=["app.customers"])
    changes = confirmed(w, {}, lsn=900)
    assert kinds(changes) == [(CHANGE_DROPPED, "app.customers")]
    assert changes[0].old_oid is None
    assert w._compare({}, lsn=910) == [], "and it is reported once, not every poll"
    assert w._compare({}, lsn=911) == []


def test_a_table_we_never_replicated_and_that_does_not_exist_is_not_a_drop():
    w = watcher(include=["app.customers"])
    assert w._compare({}, lsn=1) == []


def test_the_same_name_with_a_new_oid_is_a_recreate():
    w = watcher(known=[relation("customers", 16384)], replicated=["app.customers"])
    changes = confirmed(w, {"app.customers": relation("customers", 99999)}, lsn=600)
    assert kinds(changes) == [(CHANGE_RECREATED, "app.customers")]
    assert (changes[0].old_oid, changes[0].new_oid) == (16384, 99999)


def test_an_unchanged_table_produces_nothing():
    w = watcher(known=[relation("customers", 16384)], replicated=["app.customers"])
    assert w._compare({"app.customers": relation("customers", 16384)}, lsn=700) == []


def test_a_complete_type_token_makes_a_relfilenode_rewrite_an_ordinary_truncate():
    """TRUNCATE rewrites the physical file but does not create a new relation."""
    old = SourceRelation(
        "app", "customers", 16384, True, "d",
        relfilenode=90001, relation_type_oid=70001,
    )
    truncated = SourceRelation(
        "app", "customers", 16384, True, "d",
        relfilenode=90002, relation_type_oid=70001,
    )
    w = watcher(known=[old], replicated=["app.customers"])
    assert w._compare({"app.customers": truncated}, lsn=700) == []
    assert w.known["app.customers"].relfilenode == 90002


def test_a_complete_type_token_change_is_still_a_same_oid_recreate():
    old = SourceRelation(
        "app", "customers", 16384, True, "d",
        relfilenode=90001, relation_type_oid=70001,
    )
    replacement = SourceRelation(
        "app", "customers", 16384, True, "d",
        relfilenode=90002, relation_type_oid=70002,
    )
    w = watcher(known=[old], replicated=["app.customers"], confirm_polls=1)
    changes = w._compare({"app.customers": replacement}, lsn=700)
    assert [(change.kind, change.qualified) for change in changes] == [
        (CHANGE_RECREATED, "app.customers")
    ]


def test_confirmation_streak_tracks_the_complete_same_oid_lifecycle_token():
    old = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=90001,
        relation_type_oid=70001,
    )
    replacement_b = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=90002,
        relation_type_oid=70002,
    )
    replacement_c = SourceRelation(
        "app", "customers", 16384, True, "d", relfilenode=90003,
        relation_type_oid=70003,
    )
    w = watcher(
        known=[old], replicated=["app.customers"], confirm_polls=2
    )

    assert w._compare({"app.customers": replacement_b}, lsn=700) == []
    assert w._compare({"app.customers": replacement_c}, lsn=701) == []
    tracked = w._unconfirmed["app.customers"]
    assert tracked.new_identity == catalog_generation.identity_for(replacement_c)
    confirmed_change = w._compare({"app.customers": replacement_c}, lsn=702)
    assert len(confirmed_change) == 1
    assert confirmed_change[0].new_identity == tracked.new_identity


def test_leaving_and_rejoining_the_publication_is_reported_but_not_a_drop():
    w = watcher(known=[relation("customers", 1)], replicated=["app.customers"])
    left = w._compare({"app.customers": relation("customers", 1, published=False)}, lsn=1)
    assert kinds(left) == [(CHANGE_UNPUBLISHED, "app.customers")]
    back = w._compare({"app.customers": relation("customers", 1, published=True)}, lsn=2)
    assert kinds(back) == [(CHANGE_REPUBLISHED, "app.customers")]


def test_first_sight_of_a_replicated_table_records_its_oid_silently():
    """A table that predates this mechanism has no persisted oid. Recording it is the
    honest behaviour - we cannot report a change in a value we never saw - and only
    *later* changes are reported."""
    w = watcher(replicated=["app.customers"])
    assert w._compare({"app.customers": relation("customers", 16384)}, lsn=1) == []
    assert w.known["app.customers"].oid == 16384
    assert [r.qualified for r in w.dirty()] == ["app.customers"]


def test_a_table_in_the_include_list_we_have_never_replicated_is_new():
    """Rubric 2.3's hook: the same poll that finds drops finds new tables. 1.5 only
    records it."""
    w = watcher(
        include=["app.customers", "app.late_arrival"],
        known=[relation("customers", 1)],
        replicated=["app.customers"],
    )
    changes = w._compare(
        {
            "app.customers": relation("customers", 1),
            "app.late_arrival": relation("late_arrival", 2),
        },
        lsn=1,
    )
    assert kinds(changes) == [(CHANGE_NEW, "app.late_arrival")]


def test_a_table_outside_the_include_list_is_ignored():
    """Partitions of a published partitioned table are ordinary `pg_class` rows that
    are not themselves published; they must not each become a discovery event."""
    w = watcher(include=["app.customers"], known=[relation("customers", 1)],
                replicated=["app.customers"])
    changes = w._compare(
        {
            "app.customers": relation("customers", 1),
            "app.audit_log_2026_07": relation("audit_log_2026_07", 7, published=False),
        },
        lsn=1,
    )
    assert changes == []


# --------------------------------------------------------------------------- #
# the fence
# --------------------------------------------------------------------------- #
def _pending(w: CatalogWatcher, lsn: int, **kw) -> CatalogChange:
    change = CatalogChange(
        kind=CHANGE_DROPPED, schema="app", table="customers", detected_lsn=lsn, **kw
    )
    w.queue(change)
    return change


def test_a_change_is_not_due_until_the_destination_reaches_its_lsn():
    w = watcher()
    _pending(w, 1000)
    assert w.due(durable_lsn=999) == []
    assert len(w.due(durable_lsn=1000)) == 1
    assert len(w.due(durable_lsn=5000)) == 1


def test_a_non_destructive_change_needs_no_fence():
    """A `new` / `unpublished` / `republished` change removes nothing, so there is
    nothing for the fence to protect. Fencing them anyway left them pending on an idle
    stream, which kept the watcher writing marker records to the source for no reason.
    """
    w = watcher()
    for kind in (CHANGE_NEW, CHANGE_UNPUBLISHED, CHANGE_REPUBLISHED):
        w.queue(
            CatalogChange(kind=kind, schema="app", table="t", detected_lsn=10**9)
        )
    assert len(w.due(durable_lsn=0)) == 3


def test_grace_zero_never_forces_an_unfenced_change():
    """The default. Forcing a drop the fence has not cleared can leave a zombie: an
    in-flight event for the table re-creates it with pre-drop rows."""
    w = watcher()
    change = _pending(w, 1000)
    change.detected_at -= 3600
    assert w.due(durable_lsn=1) == []
    assert change.deferrals >= 1


def test_a_configured_grace_eventually_forces_it():
    w = watcher(grace_seconds=30)
    change = _pending(w, 1000)
    change.detected_at -= 31
    assert w.due(durable_lsn=1) == [change]


def test_resolve_removes_only_what_was_applied():
    w = watcher()
    first = _pending(w, 10)
    second = _pending(w, 20)
    w.resolve([first])
    assert w.pending() == [second]


def test_dirty_state_is_not_forgotten_until_the_caller_says_so():
    """`dirty()` is non-destructive on purpose: the applier writes those rows inside
    its transaction, and a rolled-back group must not have lost the observation."""
    w = watcher(replicated=["app.customers"])
    w._compare({"app.customers": relation("customers", 1)}, lsn=1)
    assert len(w.dirty()) == 1
    assert len(w.dirty()) == 1, "reading must not consume it"
    w.clear_dirty(["app.customers"])
    assert w.dirty() == []


def test_a_relation_with_an_unapplied_change_is_excluded_from_persistence():
    """Persisted state must never run ahead of the action it implies: writing the new
    oid before the recreate is applied would make the next run agree with the source
    and never notice the drop."""
    w = watcher(known=[relation("customers", 1)], replicated=["app.customers"])
    confirmed(w, {"app.customers": relation("customers", 2)}, lsn=1)
    assert [r.qualified for r in w.dirty()] == ["app.customers"]
    assert w.dirty(exclude={"app.customers"}) == []


def test_marker_failure_leaves_the_change_unfenced_rather_than_applied():
    """A source that cannot be written to (a replica, no permission) means the fence
    may never open. That is reported, not worked around: `fenced` stays False and the
    applier will not apply it until an event past the LSN arrives."""

    class Broken:
        def execute(self, *_args):
            raise RuntimeError("cannot write to a read-only source")

    w = watcher()
    change = _pending(w, 1000)
    w._emit_marker(Broken(), [change])
    assert change.fenced is False
    assert w.markers_emitted == 0


def test_a_successful_marker_fences_every_pending_change():
    """The marker is TRANSACTIONAL: a non-transactional one does not end Debezium's
    WAL-position search after a restart, so every record - including the marker
    itself - is skipped (measured; see `catalog._emit_marker`)."""
    class Fine:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))

    w = watcher()
    change = _pending(w, 1000)
    conn = Fine()
    w._emit_marker(conn, [change])
    assert change.fenced is True
    assert w.markers_emitted == 1
    sql, params = conn.calls[0]
    assert "pg_logical_emit_message(true" in sql
    assert params[0] == "cdcf_catalog_fence", "the reason is in the prefix (Opus Q3)"


def test_polling_can_be_switched_off():
    w = watcher()
    assert w.start() is w
    assert w._thread is None


@pytest.mark.parametrize("kind", [CHANGE_DROPPED, CHANGE_RECREATED])
def test_change_context_is_json_ready_for_an_alert(kind):
    change = CatalogChange(kind=kind, schema="app", table="t", detected_lsn=5)
    assert change.context()["table"] == "app.t"
