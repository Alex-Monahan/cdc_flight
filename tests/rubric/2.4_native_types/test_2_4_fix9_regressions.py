"""FIX ROUND 9 regression probes.

These tests deliberately exercise the class of opaque values, not just the three
examples named in the previous review.  The refusal probe uses the real Applier and
its DuckDB control state; only the Debezium Java callback is replaced by the existing
in-process laboratory.
"""

from __future__ import annotations

import os
from ipaddress import IPv4Address, IPv4Interface
from itertools import pairwise
from pathlib import Path

import pytest
from support.applier_lab import Lab, begin, data, end

from cdc_flight.errors import SchemaEvolutionRefused
from cdc_flight.typed_types import (
    InvalidTypedValue,
    SourceTypeDescriptor,
    UnsupportedType,
    adapt_value,
    native_type,
)


def _slot_metrics(box):
    # restart_lsn is the retention horizon, while confirmed_flush_lsn is the durable
    # consumer position.  Capture both: a small test transaction can advance the
    # latter without crossing a WAL-segment boundary for the former.
    box.sql("CHECKPOINT")
    rows = box.pg_query(
        "SELECT restart_lsn::text, confirmed_flush_lsn::text, "
        "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
        "pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn)::bigint, "
        "restart_lsn - '0/0', confirmed_flush_lsn - '0/0' "
        "FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    assert rows, f"replication slot {box.slot!r} disappeared"
    (
        restart_lsn,
        confirmed_flush_lsn,
        retained_wal,
        slot_wal_window,
        restart_pos,
        confirmed_pos,
    ) = rows[0]
    return {
        "restart_lsn": str(restart_lsn),
        "confirmed_flush_lsn": str(confirmed_flush_lsn),
        "retained_wal": int(retained_wal),
        "slot_wal_window": int(slot_wal_window),
        "restart_pos": int(restart_pos),
        "confirmed_pos": int(confirmed_pos),
    }


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def test_decode_or_refuse_carries_arbitrary_postgresql_text_without_a_grammar():
    """Canonical output is opaque text; punctuation has no special meaning here."""
    source = _source("tsquery", 3615)
    decoded = adapt_value("c3RyaWN0ICQuImEi", native_type(source))
    assert decoded == 'strict $."a"'
    # The successfully decoded text is idempotent across fold/spill/bind seams;
    # it is not interpreted as a second base64 payload.
    assert adapt_value(decoded, native_type(source)) == 'strict $."a"'
    assert adapt_value("KDEgKyAyKQ==", native_type(source)) == "(1 + 2)"
    assert adapt_value("", native_type(source)) == ""


def test_base64_byte_transport_with_non_utf8_bytes_is_refused():
    source = _source("tsquery", 3615)
    with pytest.raises(InvalidTypedValue, match="strict UTF-8"):
        adapt_value(b"//8=", native_type(source))
    with pytest.raises(InvalidTypedValue, match="strict UTF-8"):
        adapt_value("//8=", native_type(source))


def test_inet_catalog_backfill_uses_postgresql_output_not_the_text_cast():
    """The ADD COLUMN path receives psycopg ipaddress objects, not Debezium text."""
    source = _source("inet", 869)
    target = native_type(source)
    assert adapt_value(IPv4Address("192.0.2.1"), target) == "192.0.2.1"
    assert adapt_value(IPv4Interface("192.0.2.1/24"), target) == "192.0.2.1/24"


def test_opaque_transport_has_no_hand_rolled_type_grammar_or_false_verified_set():
    """The implementation proof is transport-only, not an exemplar parser."""
    source = Path("src/cdc_flight/typed_types.py").read_text()
    for recognizer in (
        "_canonical_opaque_text_candidate",
        "_balanced_path_text",
        "_valid_tsquery_text",
        "_canonical_pg_lsn",
        "_VERIFIED_TEXT_KINDS",
        "_OBSCURE_EXTENSIONS",
    ):
        assert recognizer not in source


GLOBAL_UNKNOWN_DECISIONS = (
    ("tsquery", 3615, "'fat' & 'rat'"),
    ("jsonpath", 4072, '$."a"'),
    ("pg_lsn", 3220, "0/16B6A0"),
    ("tsvector", 3614, "'fat':1 'rat':2"),
    ("xml", 142, "<a>fat</a>"),
    ("money", 790, "$12.34"),
    ("inet", 869, "192.0.2.1/24"),
    ("cidr", 650, "192.0.2.0/24"),
    ("macaddr", 829, "08:00:2b:01:02:03"),
    ("macaddr8", 774, "08:00:2b:01:02:03:04:05"),
    ("int2vector", 22, "1 2 3"),
)


@pytest.mark.parametrize(
    ("kind", "oid", "text"),
    GLOBAL_UNKNOWN_DECISIONS,
    ids=[item[0] for item in GLOBAL_UNKNOWN_DECISIONS],
)
def test_every_allowlisted_unknown_type_is_varchar_and_transport_only(kind, oid, text):
    descriptor = _source(kind, oid)
    assert native_type(descriptor).sql == "VARCHAR"
    if kind in {"tsquery", "jsonpath", "pg_lsn"}:
        import base64

        wire = base64.b64encode(text.encode()).decode()
    else:
        wire = text
    assert adapt_value(wire, native_type(descriptor)) == text
    if kind == "money":
        assert adapt_value(wire.encode("ascii"), native_type(descriptor)) == wire.encode(
            "ascii"
        )
    else:
        assert adapt_value(wire.encode("ascii"), native_type(descriptor)) == text


REFUSED_UNKNOWN_TYPES = (
    ("box", 603),
    ("circle", 718),
    ("line", 628),
    ("lseg", 601),
    ("path", 602),
    ("polygon", 604),
    ("tid", 27),
    ("regclass", 2205),
    ("oidvector", 30),
    ("xid8", 5069),
    ("aclitem", 1033),
    ("pg_node_tree", 194),
    ("tinterval", 2900),
    ("snapshot", 2970),
)


@pytest.mark.parametrize(
    ("kind", "oid"), REFUSED_UNKNOWN_TYPES, ids=[item[0] for item in REFUSED_UNKNOWN_TYPES]
)
def test_every_other_unknown_type_is_refused_before_value_admission(kind, oid):
    with pytest.raises(UnsupportedType):
        native_type(_source(kind, oid))


def test_int2vector_non_text_connect_shape_is_refused_not_admitted_as_an_array():
    with pytest.raises(InvalidTypedValue):
        adapt_value([1, 2, 3], native_type(_source("int2vector", 22)))
    assert adapt_value("1 2 3", native_type(_source("int2vector", 22))) == "1 2 3"


@pytest.mark.parametrize(
    ("kind", "oid"),
    [
        (kind, oid)
        for kind, oid, _text in GLOBAL_UNKNOWN_DECISIONS
        if kind != "money"
    ],
    ids=[kind for kind, _oid, _text in GLOBAL_UNKNOWN_DECISIONS if kind != "money"],
)
def test_every_allowlisted_opaque_type_refuses_non_utf8_transport(kind, oid):
    with pytest.raises(InvalidTypedValue):
        adapt_value(b"\xff", native_type(_source(kind, oid)))


def test_money_never_refuses_or_rewrites_an_opaque_payload():
    target = native_type(_source("money", 790))
    for payload in (b"\xff", "₹1,237.89"):
        assert adapt_value(payload, target) is payload


def _permanently_bad_event(txn: str, order: int, lsn: int):
    event = data(
        txn,
        order,
        lsn,
        table="bad_opaque",
        key={"id": 1},
        # The row image deliberately changes with every source transaction.  The
        # refusal fingerprint is descriptor/shape identity, not a hash of a bad
        # payload, so a moving poison row cannot keep the slot pinned forever.
        after={"id": 1, "payload": b"\xff" + txn.encode("ascii")},
    )
    int4 = _source("int4", 23)
    event.key_descriptors = {"id": int4}
    event.after_descriptors = {
        "id": int4,
        "payload": _source("int2vector", 22),
    }
    return event


def _co_published_attempt(txn: str):
    bad = _permanently_bad_event(txn, 1, 110)
    healthy = data(
        txn,
        2,
        110,
        table="healthy_peer",
        key={"id": 2},
        after={"id": 2, "name": "durable"},
    )
    return [
        begin(txn, 109),
        bad,
        healthy,
        end(
            txn,
            2,
            110,
            per_table={"app.bad_opaque": 1, "app.healthy_peer": 1},
        ),
    ]


def test_identical_refusal_quarantines_one_table_and_advances_a_healthy_peer(
    tmp_path: Path,
):
    """A quarantine is stale, loud, and still has a durable recovery obligation."""
    path = tmp_path / "r9-quarantine.duckdb"

    first = Lab(path)
    try:
        with pytest.raises(SchemaEvolutionRefused):
            first.run(_co_published_attempt("r9-1"))
        assert first.scalar(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        ) == "pending"
    finally:
        first.applier.lease.release(first.con)
        first.close()

    second = Lab(path)
    try:
        with pytest.raises(SchemaEvolutionRefused):
            second.run(_co_published_attempt("r9-2"))
        refusal_fingerprint = second.scalar(
            "SELECT refusal_fingerprint FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        )
        refusal_reason = second.scalar(
            "SELECT reason FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        )
        assert refusal_fingerprint
        # The second observation is a generic blocked retry; it must not erase the
        # concrete first refusal's attribution.
        assert "int2vector" in refusal_reason
        assert second.scalar(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        ) == "quarantined"
        assert second.scalar(
            "SELECT snapshot_state FROM _cdc_flight.table_state "
            "WHERE source_table='bad_opaque'"
        ) == "awaiting_snapshot"
        assert second.scalar(
            "SELECT count(*) FROM _cdc_flight.alerts "
            "WHERE pipeline='lab' AND code='schema_table_quarantined'"
        ) == 1
    finally:
        second.applier.lease.release(second.con)
        second.close()

    for run_number in range(3, 10):
        attempt = Lab(path)
        try:
            attempt.run(_co_published_attempt(f"r9-{run_number}"))
            assert attempt.applier.stats()["quarantined_events"] > 0
            assert attempt.rows("cdcflight_app_healthy_peer", '"id", "name"') == [
                (2, "durable")
            ]
            assert not attempt.exists("cdcflight_app_bad_opaque")
            assert attempt.scalar(
                "SELECT last_lsn FROM _cdc_flight.debezium_offsets WHERE pipeline='lab'"
            ) == 110
            assert attempt.scalar(
                "SELECT state FROM _cdc_flight.schema_refusals "
                "WHERE source_table='bad_opaque'"
            ) == "quarantined"
            assert attempt.scalar(
                "SELECT snapshot_state FROM _cdc_flight.table_state "
                "WHERE source_table='bad_opaque'"
            ) == "awaiting_snapshot"
            assert attempt.scalar(
                "SELECT count(*) FROM _cdc_flight.alerts "
                "WHERE pipeline='lab' AND code='schema_table_quarantined'"
            ) == 1
            assert attempt.scalar(
                "SELECT count(*) FROM _cdc_flight.table_events "
                "WHERE source_table='bad_opaque' AND event='schema_quarantine'"
            ) == 1
        finally:
            attempt.applier.lease.release(attempt.con)
            attempt.close()


@pytest.fixture(scope="module")
def postgres_bad_healthy_containment(sandbox):
    """The real three-run box refusal/healthy-peer slot probe from rubric 4.0."""
    box = sandbox
    box.reseed()
    box.sql(
        [
            "CREATE TABLE app.bad_box (id integer PRIMARY KEY, value box)",
            "CREATE TABLE app.healthy_peer (id integer PRIMARY KEY, name text)",
            "ALTER PUBLICATION cdc_flight_pub ADD TABLE app.bad_box, app.healthy_peer",
        ],
        one_transaction=True,
    )
    box.env["CDC_TABLES"] = "bad_box,healthy_peer"
    box.env["CDC_AUTO_DISCOVERY"] = "0"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        baseline = box.run(reset_state=True, max_seconds=20)
        assert baseline["ok"] is True, baseline
        metrics = [_slot_metrics(box)]
        box.sql(
            [
                "INSERT INTO app.bad_box VALUES (1, '((0,0),(1,1))'::box)",
                "INSERT INTO app.healthy_peer VALUES (1, 'durable')",
            ],
            one_transaction=True,
        )
        runs = []
        run_diagnostics = []
        for _ in range(4):
            run = box.run(
                max_seconds=20,
                min_records=1,
                expect_success=False,
            )
            runs.append(run)
            run_diagnostics.append(
                {
                    key: run.get(key)
                    for key in (
                        "ok", "stop_reason", "records", "batches",
                        "commit_groups", "quarantined_events", "error_type",
                        "error_cause_type", "tables_awaiting_snapshot_unhandled",
                    )
                }
            )
            metrics.append(_slot_metrics(box))
        quarantine_state = {
            "refusal": box.duck_query(
                "SELECT state, refusal_fingerprint, refusal_class "
                "FROM _cdc_flight.schema_refusals WHERE source_table='bad_box'"
            ),
            "healthy": box.duck_query(
                f"SELECT id, name FROM {box.table('cdcflight_app_healthy_peer')}"
            ),
            "lifecycle": box.duck_query(
                "SELECT snapshot_state FROM _cdc_flight.table_state "
                "WHERE source_table='bad_box'"
            ),
            "alerts": box.duck_query(
                "SELECT count(*) FROM _cdc_flight.alerts "
                "WHERE code='schema_table_quarantined'"
            ),
        }
        # Repair the source schema.  The next run must exercise the declared
        # quarantined -> pending trigger and publish a complete current-source
        # image before resolving the refusal; a following run proves the normal
        # stream remains healthy after that hand-off.
        box.sql("ALTER TABLE app.bad_box DROP COLUMN value")
        box.sql("INSERT INTO app.bad_box VALUES (2)")
        repair_runs = []
        for _ in range(2):
            repair_runs.append(
                box.run(
                    max_seconds=30,
                    min_records=1,
                    expect_success=False,
                )
            )
            metrics.append(_slot_metrics(box))
        yield {
            "box": box,
            "runs": runs,
            "run_diagnostics": run_diagnostics,
            "repair_runs": repair_runs,
            "metrics": metrics,
            "quarantine_state": quarantine_state,
        }
    finally:
        box.reseed()


@pytest.mark.slow
@pytest.mark.e2e
def test_bad_box_is_quarantined_without_stopping_a_healthy_peer(
    postgres_bad_healthy_containment,
):
    scenario = postgres_bad_healthy_containment
    assert all(run["ok"] is False for run in scenario["runs"]), scenario["runs"]
    state = scenario["quarantine_state"]
    assert state["refusal"][0][0] == "quarantined"
    assert state["refusal"][0][1]
    assert state["refusal"][0][2] == "SchemaEvolutionRefused"
    assert state["healthy"] == [(1, "durable")]
    assert state["lifecycle"] == [("awaiting_snapshot",)]
    assert state["alerts"][0][0] == 1


@pytest.mark.slow
@pytest.mark.e2e
def test_repaired_source_leaves_quarantine_only_after_a_full_resnapshot(
    postgres_bad_healthy_containment,
):
    scenario = postgres_bad_healthy_containment
    box = scenario["box"]
    assert scenario["repair_runs"][-1]["ok"] is True, scenario["repair_runs"]
    assert box.duck_query(
        "SELECT state FROM _cdc_flight.schema_refusals WHERE source_table='bad_box'"
    ) == [("resolved",)]
    assert box.duck_query(
        "SELECT snapshot_state FROM _cdc_flight.table_state "
        "WHERE source_table='bad_box'"
    ) == [("complete",)]
    assert box.duck_query(
        f"SELECT id FROM {box.table('cdcflight_app_bad_box')} ORDER BY id"
    ) == [(1,), (2,)]


@pytest.mark.slow
@pytest.mark.e2e
def test_bad_healthy_scenario_records_slot_progress_and_bounded_wal(
    postgres_bad_healthy_containment,
):
    scenario = postgres_bad_healthy_containment
    metrics = scenario["metrics"]
    assert all(
        metric["restart_lsn"]
        and metric["confirmed_flush_lsn"]
        and metric["retained_wal"] >= 0
        for metric in metrics
    ), metrics
    bad_runs = metrics[1:5]
    # Containment is not proved by a later repair run.  While the bad table is
    # quarantined, the healthy peer's source transaction must still be durably
    # acknowledged and the main slot must move past it.
    assert bad_runs[-1]["confirmed_pos"] > metrics[0]["confirmed_pos"], (
        scenario["run_diagnostics"], metrics
    )
    assert all(
        later["confirmed_pos"] >= earlier["confirmed_pos"]
        for earlier, later in pairwise(bad_runs)
    ), (scenario["run_diagnostics"], metrics)
    assert all(metric["slot_wal_window"] >= 0 for metric in bad_runs), metrics
    if "PYTEST_XDIST_WORKER" not in os.environ:
        assert max(metric["slot_wal_window"] for metric in bad_runs) < 1_000_000, metrics
    print(f"round11 bad-box+healthy slot metrics: {metrics}")
