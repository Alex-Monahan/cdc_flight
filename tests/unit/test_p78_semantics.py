from __future__ import annotations

import duckdb
import pytest

from cdc_flight import destination, event_ledger, scd2
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.errors import AmbiguousDelete, DestinationIdentityCollision
from cdc_flight.snapshot import SnapshotCoordinator


def _event(
    operation: str,
    commit_lsn: int,
    total_order: int = 1,
    *,
    key: dict | None = None,
    before: dict | None = None,
    after: dict | None = None,
    snapshot_identity: str | None = None,
) -> PendingRecord:
    return PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="cdcflight.app.rows",
        nbytes=1,
        op=operation,
        schema="app",
        table="rows",
        lsn=commit_lsn,
        commit_lsn=commit_lsn,
        txn_id="tx-1",
        total_order=total_order,
        source_cluster_id="cluster-A",
        source_timeline=1,
        relation_generation="42:43:44",
        key=key,
        before=before,
        after=after,
        snapshot_identity=snapshot_identity,
        incremental=bool(snapshot_identity and snapshot_identity.startswith("inc:")),
    )


def _identity(event: PendingRecord, *, event_id: str | None = None):
    return event_ledger.identity_for(event, event_id=event_id)


def _con():
    con = duckdb.connect(":memory:")
    ensure_control_schema(con)
    return con


def _claim(con, identity, *, target="keyless_rows"):
    return destination.claim_event_ledger(
        con,
        identity,
        pipeline="p78",
        target_table=target,
        source_lsn=identity.source_lsn,
    )


def _bundle() -> scd2.SCD2RelationBundle:
    return scd2.SCD2RelationBundle(
        pipeline="p78",
        source_schema="app",
        source_table="rows",
        target_table="cdcflight_app_rows",
        columns={"id": "INTEGER", "value": "INTEGER"},
        key_columns=("id",),
        relation_generation="42:43:44",
    )


def _scd_event(operation, lsn, *, before=None, after=None, event_id=None):
    raw = _event(
        operation,
        lsn,
        key={"id": 1},
        before=before,
        after=after,
    )
    identity = _identity(raw, event_id=event_id)
    return scd2.SCD2Event(
        pipeline="p78",
        target_table="cdcflight_app_rows",
        source_schema="app",
        source_table="rows",
        event_id=identity.event_id,
        operation=operation,
        key={"id": 1},
        before=before,
        after=after,
        identity=identity,
    )


def _apply(con, event, bundle=None):
    con.execute("BEGIN")
    try:
        result = scd2.apply_event(con, event, bundle=bundle or _bundle())
        con.execute("COMMIT")
        return result
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _truncate_event(lsn: int) -> scd2.SCD2TableEvent:
    raw = _event("t", lsn, key=None)
    identity = _identity(raw)
    return scd2.SCD2TableEvent(
        pipeline="p78",
        target_table="cdcflight_app_rows",
        source_schema="app",
        source_table="rows",
        event_id=identity.event_id,
        identity=identity,
    )


def _apply_truncate(con, event, bundle=None):
    con.execute("BEGIN")
    try:
        result = scd2.apply_truncate(
            con, bundle or _bundle(), event, control_schema=None
        )
        con.execute("COMMIT")
        return result
    except BaseException:
        con.execute("ROLLBACK")
        raise


def test_strong_identity_excludes_table_and_key_but_digest_detects_payload_conflict():
    first = _event("c", 100, key={"id": 1}, after={"id": 1, "value": 10})
    same_source_facts = _event("c", 100, key={"id": 999}, after={"id": 999, "value": 10})
    changed_payload = _event("c", 100, key={"id": 1}, after={"id": 1, "value": 11})
    first_identity = _identity(first)
    assert first_identity.strong
    assert first_identity.event_id == _identity(same_source_facts).event_id
    assert first_identity.payload_digest != _identity(changed_payload).payload_digest
    assert first_identity.key_guard_digest != _identity(same_source_facts).key_guard_digest
    assert ".app." not in first_identity.event_id


def test_snapshot_and_incremental_identities_are_ledger_eligible():
    snapshot = _event("r", 0, key={"id": 1}, after={"id": 1}, snapshot_identity=None)
    initial = event_ledger.identity_for(snapshot, event_id="snap:7:app.rows:1")
    incremental = _event(
        "r", 0, key={"id": 1}, after={"id": 1}, snapshot_identity="inc:run-7:app.rows:1"
    )
    inc_identity = _identity(incremental)
    assert initial.ledger_eligible and initial.snapshot_epoch == 7
    assert inc_identity.ledger_eligible and inc_identity.snapshot_epoch == 0


def test_snapshot_epoch_comes_from_durable_ledger_and_ignores_unscoped_rows():
    con = _con()
    try:
        con.execute(
            "INSERT INTO _cdc_flight.event_ledger "
            "(pipeline, target_table, event_id, payload_digest, state, "
            "applied_at, snapshot_epoch) VALUES (?, ?, ?, ?, ?, current_timestamp, ?)",
            ["p78", "target", "snap:7:app.rows:1", "digest", "applied", 7],
        )
        con.execute(
            "INSERT INTO _cdc_flight.event_ledger "
            "(pipeline, target_table, event_id, payload_digest, state, applied_at) "
            "VALUES (?, ?, ?, ?, ?, current_timestamp)",
            ["p78", "target", "v2:stream", "digest", "applied"],
        )
        coordinator = SnapshotCoordinator(
            con,
            dataset="cdc_raw",
            pipeline="p78",
            topic_prefix="cdcflight",
            created_in_txn=lambda: set(),
            get_registry=lambda: None,
            epoch=1,
            transactional_ddl=True,
        )
        assert coordinator.epoch == 7
        assert event_ledger.latest_snapshot_epoch(con, pipeline="missing") == 0
    finally:
        con.close()


def test_shared_ledger_rolls_back_with_keyless_data_and_replays_after_commit():
    con = _con()
    try:
        event = _event("c", 100, key=None, after={"value": "one"})
        identity = _identity(event)
        con.execute("CREATE TABLE keyless_rows(value VARCHAR)")

        con.execute("BEGIN")
        assert not _claim(con, identity)
        con.execute("INSERT INTO keyless_rows VALUES (?)", ["one"])
        con.execute("ROLLBACK")
        assert destination.read_event_ledger(
            con, pipeline="p78", target_table="keyless_rows", event_id=identity.event_id
        ) is None
        assert con.execute("SELECT count(*) FROM keyless_rows").fetchone()[0] == 0

        con.execute("BEGIN")
        assert not _claim(con, identity)
        con.execute("INSERT INTO keyless_rows VALUES (?)", ["one"])
        con.execute("COMMIT")

        con.execute("BEGIN")
        assert _claim(con, identity)
        con.execute("COMMIT")
        assert con.execute("SELECT count(*) FROM keyless_rows").fetchone()[0] == 1
    finally:
        con.close()


def test_shared_ledger_rejects_same_id_with_conflicting_payload():
    con = _con()
    try:
        first = _event("c", 100, key=None, after={"value": "one"})
        second = _event("c", 100, key=None, after={"value": "two"})
        first_identity = _identity(first)
        second_identity = _identity(second, event_id=first_identity.event_id)
        con.execute("BEGIN")
        assert not _claim(con, first_identity)
        con.execute("COMMIT")
        con.execute("BEGIN")
        with pytest.raises(DestinationIdentityCollision):
            _claim(con, second_identity)
        con.execute("ROLLBACK")
    finally:
        con.close()


def test_scd2_close_insert_tombstone_and_duplicate_replay():
    con = _con()
    try:
        bundle = _bundle()
        created = _scd_event("c", 100, after={"id": 1, "value": 10})
        updated = _scd_event(
            "u", 200, before={"id": 1, "value": 10}, after={"id": 1, "value": 20}
        )
        deleted = _scd_event("d", 300, before={"id": 1, "value": 20})
        assert not _apply(con, created, bundle).replayed
        assert not _apply(con, updated, bundle).replayed
        assert _apply(con, updated, bundle).replayed
        assert not _apply(con, deleted, bundle).replayed
        assert con.execute('SELECT count(*) FROM "cdcflight_app_rows__current"').fetchone()[0] == 0
        rows = con.execute(
            'SELECT "__cdcf_scd2_operation", "__cdcf_scd2_is_current" '
            'FROM "cdcflight_app_rows__scd2_history" ORDER BY "__cdcf_scd2_valid_from"'
        ).fetchall()
        assert rows == [("c", False), ("u", False), ("d", True)]
    finally:
        con.close()


def test_scd2_late_event_is_inserted_between_verified_predecessor_and_successor():
    con = _con()
    try:
        bundle = _bundle()
        _apply(con, _scd_event("c", 100, after={"id": 1, "value": 1}), bundle)
        _apply(
            con,
            _scd_event("u", 300, before={"id": 1, "value": 1}, after={"id": 1, "value": 3}),
            bundle,
        )
        late = _scd_event(
            "u", 200, before={"id": 1, "value": 1}, after={"id": 1, "value": 2}
        )
        result = _apply(con, late, bundle)
        assert not result.current
        history = con.execute(
            'SELECT value, "__cdcf_scd2_valid_to", "__cdcf_scd2_is_current" '
            'FROM "cdcflight_app_rows__scd2_history" '
            'WHERE "__cdcf_scd2_source_identity" = ? '
            'ORDER BY "__cdcf_scd2_valid_from"',
            [event_ledger.canonical_json({"id": 1})],
        ).fetchall()
        assert [row[0] for row in history] == [1, 2, 3]
        assert history[1][1] is not None and not history[1][2]
        assert con.execute('SELECT value FROM "cdcflight_app_rows__current"').fetchone()[0] == 3
    finally:
        con.close()


def test_scd2_truncate_is_structural_and_allows_a_new_post_truncate_current():
    con = _con()
    try:
        bundle = _bundle()
        _apply(con, _scd_event("c", 100, after={"id": 1, "value": 1}), bundle)
        marker = _truncate_event(200)
        assert not _apply_truncate(con, marker, bundle).replayed
        assert con.execute('SELECT count(*) FROM "cdcflight_app_rows__current"').fetchone()[0] == 0
        _apply(con, _scd_event("c", 300, after={"id": 1, "value": 3}), bundle)
        assert con.execute('SELECT value FROM "cdcflight_app_rows__current"').fetchone()[0] == 3
        assert con.execute(
            'SELECT count(*) FROM "cdcflight_app_rows__scd2_history" '
            'WHERE "__cdcf_scd2_operation" = \'t\''
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_scd2_refuses_keyless_and_missing_predecessor_without_ledger_commit():
    con = _con()
    try:
        raw = _event("c", 100, key=None, after={"id": 1, "value": 1})
        with pytest.raises(AmbiguousDelete):
            scd2.SCD2Event.from_pending(
                raw,
                event_id="unused",
                pipeline="p78",
                target_table="cdcflight_app_rows",
                source_cluster_id="cluster-A",
                source_timeline=1,
                relation_generation="42:43:44",
                commit_lsn=100,
            )
        missing = _scd_event("d", 100, before={"id": 1, "value": 1})
        with pytest.raises(AmbiguousDelete):
            _apply(con, missing)
        assert con.execute('SELECT count(*) FROM "_cdc_flight"."event_ledger"').fetchone()[0] == 0
    finally:
        con.close()


def test_current_only_refresh_is_explicitly_refused():
    with pytest.raises(scd2.HistoryRefreshRefused, match="current-only"):
        scd2.refuse_current_only_refresh(
            source_schema="app",
            source_table="rows",
            target_table="cdcflight_app_rows",
            history_boundary=100,
        )


def test_real_applier_routes_a_history_mode_relation_through_the_shared_scd2_bundle(tmp_path):
    from support.applier_lab import Lab, data, end

    box = Lab(tmp_path / "scd2-applier.duckdb")
    try:
        box.con.execute(
            "INSERT INTO _cdc_flight.table_state "
            "(pipeline, source_schema, source_table, target_table, history_mode, "
            "key_strategy, key_columns) VALUES (?,?,?,?,?,?,?)",
            [
                "lab",
                "app",
                "scd_rows",
                "cdcflight_app_scd_rows",
                "scd2",
                "pk",
                ["id"],
            ],
        )
        box.applier.source_cluster_id = "cluster-A"
        box.applier.source_timeline = 1
        box.applier.strict_event_identity = True
        record = data(
            "scd",
            1,
            100,
            table="scd_rows",
            key={"id": 1},
            after={"id": 1, "name": "one"},
        )
        record.relation_generation = "42:43:44"
        box.run([record, end("scd", 1, 110, {"app.scd_rows": 1})])
        assert box.con.execute(
            'SELECT name FROM "cdc_raw"."cdcflight_app_scd_rows__current"'
        ).fetchall() == [("one",)]
        assert box.con.execute(
            'SELECT count(*) FROM "cdc_raw"."cdcflight_app_scd_rows__scd2_history"'
        ).fetchone()[0] == 1
        assert box.con.execute(
            'SELECT count(*) FROM "_cdc_flight"."event_ledger" '
            'WHERE pipeline = ? AND target_table = ?',
            ["lab", "cdc_raw.cdcflight_app_scd_rows"],
        ).fetchone()[0] == 1
    finally:
        box.close()
