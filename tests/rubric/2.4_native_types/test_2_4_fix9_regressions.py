"""FIX ROUND 9 regression probes.

These tests deliberately exercise the class of opaque values, not just the three
examples named in the previous review.  The refusal probe uses the real Applier and
its DuckDB control state; only the Debezium Java callback is replaced by the existing
in-process laboratory.
"""

from __future__ import annotations

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
    ("money", 790, "12.34"),
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
    if kind in {"money", "inet"}:
        with pytest.raises(InvalidTypedValue):
            adapt_value(text, native_type(descriptor))
        return
    if kind in {"tsquery", "jsonpath", "pg_lsn"}:
        import base64

        wire = base64.b64encode(text.encode()).decode()
    else:
        wire = text
    assert adapt_value(wire, native_type(descriptor)) == text
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
    [(kind, oid) for kind, oid, _text in GLOBAL_UNKNOWN_DECISIONS],
    ids=[kind for kind, _oid, _text in GLOBAL_UNKNOWN_DECISIONS],
)
def test_every_allowlisted_opaque_type_refuses_non_utf8_transport(kind, oid):
    with pytest.raises(InvalidTypedValue):
        adapt_value(b"\xff", native_type(_source(kind, oid)))


def _permanently_bad_event(txn: str, order: int, lsn: int):
    event = data(
        txn,
        order,
        lsn,
        table="bad_opaque",
        key={"id": 1},
        after={"id": 1, "payload": b"\xff"},
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
    """Three consecutive attempts cannot turn one permanent row into an outage."""
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
        refusal_reason = second.scalar(
            "SELECT reason FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        )
        assert refusal_reason.count("input_fingerprint=") == 1
        assert second.scalar(
            "SELECT state FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque'"
        ) == "quarantined"
        assert second.scalar(
            "SELECT snapshot_state FROM _cdc_flight.table_state "
            "WHERE source_table='bad_opaque'"
        ) == "none"
    finally:
        second.applier.lease.release(second.con)
        second.close()

    third = Lab(path)
    try:
        third.run(_co_published_attempt("r9-3"))
        assert third.rows("cdcflight_app_healthy_peer", '"id", "name"') == [(2, "durable")]
        assert not third.exists("cdcflight_app_bad_opaque")
        assert third.scalar(
            "SELECT last_lsn FROM _cdc_flight.debezium_offsets WHERE pipeline='lab'"
        ) == 110
        assert third.scalar(
            "SELECT count(*) FROM _cdc_flight.schema_refusals "
            "WHERE source_table='bad_opaque' AND state='quarantined'"
        ) == 1
        assert third.scalar(
            "SELECT count(*) FROM _cdc_flight.table_events "
            "WHERE source_table='bad_opaque' AND event='schema_quarantine'"
        ) == 1
    finally:
        third.applier.lease.release(third.con)
        third.close()
