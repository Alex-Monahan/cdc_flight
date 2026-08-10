"""FIX ROUND 8 probes for opaque unknown values and generation-fenced caching."""

from __future__ import annotations

import base64
from dataclasses import replace

import pytest

from cdc_flight.catalog import CatalogWatcher
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.config import ReplicationConfig, SourceConfig
from cdc_flight.debezium_props import build_properties
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.typed_types import (
    InvalidTypedValue,
    SourceTypeDescriptor,
    UnsupportedType,
    adapt_value,
    native_type,
)


def _source(kind: str, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind)


def _opaque_wire(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# The first three are the stock Debezium opaque/base64 cases from the r7 probe.
# The lossless plain-text members arrive as ordinary PostgreSQL canonical text;
# money and inet are retained here as explicit refusal cases because stock
# Debezium drops formatting present in PostgreSQL's literal ``::text``.
OPAQUE_TEN_TYPE_PROBE = (
    ("tsquery", 3615, _opaque_wire("'fat' & 'rat'"), "'fat' & 'rat'"),
    ("jsonpath", 4072, _opaque_wire('$."a"'), '$."a"'),
    ("pg_lsn", 3220, _opaque_wire("0/16B6A0"), "0/16B6A0"),
    ("tsvector", 3614, "'fat':1 'rat':2", "'fat':1 'rat':2"),
    ("xml", 142, "<a>fat</a>", "<a>fat</a>"),
    ("money", 790, "12.34", "12.34"),
    ("inet", 869, "192.0.2.1", "192.0.2.1"),
    ("cidr", 650, "192.0.2.0/24", "192.0.2.0/24"),
    ("macaddr", 829, "08:00:2b:01:02:03", "08:00:2b:01:02:03"),
    ("macaddr8", 774, "08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:05"),
)


@pytest.mark.parametrize(
    ("kind", "oid", "wire", "canonical"),
    OPAQUE_TEN_TYPE_PROBE,
    ids=[case[0] for case in OPAQUE_TEN_TYPE_PROBE],
)
def test_stock_unknown_wire_is_stored_as_source_canonical_text(
    kind, oid, wire, canonical
):
    """The r7 ten-type wire values must not be admitted as literal base64."""
    source = _source(kind, oid)
    if kind in {"money", "inet"}:
        with pytest.raises(InvalidTypedValue):
            adapt_value(wire, native_type(source))
        return
    assert adapt_value(wire, native_type(source)) == canonical


def test_unclassified_opaque_types_fail_closed_instead_of_becoming_varchar():
    with pytest.raises(UnsupportedType, match="verified native destination"):
        native_type(_source("opaque", 999999))


@pytest.mark.parametrize(
    ("kind", "oid", "canonical"),
    [(case[0], case[1], case[3]) for case in OPAQUE_TEN_TYPE_PROBE[:3]],
    ids=[case[0] for case in OPAQUE_TEN_TYPE_PROBE[:3]],
)
def test_opaque_decoder_is_lossless_for_byte_and_string_transport(kind, oid, canonical):
    source = _source(kind, oid)
    wire = _opaque_wire(canonical).encode("ascii")
    assert adapt_value(wire, native_type(source)) == canonical


@pytest.mark.parametrize(
    ("kind", "oid", "invalid"),
    [
        ("tsquery", 3615, b"\xff"),
        ("jsonpath", 4072, b"\xff"),
        ("pg_lsn", 3220, b"\xff"),
    ],
)
def test_known_opaque_type_refuses_non_utf8_payload(kind, oid, invalid):
    with pytest.raises(InvalidTypedValue):
        adapt_value(invalid, native_type(_source(kind, oid)))


@pytest.mark.parametrize("include_unknown", ["true", "false"])
def test_stock_unknown_property_is_explicit_in_both_probe_modes(
    monkeypatch, include_unknown
):
    monkeypatch.setenv("CDC_INCLUDE_UNKNOWN_DATATYPES", include_unknown)
    props = build_properties(SourceConfig(), ReplicationConfig())
    assert props["include.unknown.datatypes"] == include_unknown


def _relation(*, relfilenode: int) -> SourceRelation:
    text = _source("text", 25)
    return SourceRelation(
        schema="app",
        table="generation_cache",
        oid=42,
        relfilenode=relfilenode,
        relation_type_oid=4200,
        published=True,
        replica_identity="f",
        columns=(SourceColumn(1, "payload", 25, "text", descriptor=text),),
    )


def test_toast_policy_cache_does_not_cross_relation_generation_or_epoch():
    relation = _relation(relfilenode=100)
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        schemas={"app"},
        include={relation.qualified},
        known={relation.qualified: relation},
        emit_marker=False,
        confirm_polls=1,
    )

    first = watcher.toast_policy_for(relation.qualified)
    watcher.known[relation.qualified] = replace(relation, relfilenode=200)
    watcher._epoch += 1
    second = watcher.toast_policy_for(relation.qualified)

    assert second is not first
    assert watcher.toast_policy_builds == 2
    assert watcher.toast_policy_cache_hits == 0


def test_toast_policy_cache_keys_each_generation_token_component():
    relation = _relation(relfilenode=100)
    watcher = CatalogWatcher(
        dsn="",
        publication="pub",
        schema="app",
        schemas={"app"},
        include={relation.qualified},
        known={relation.qualified: relation},
        emit_marker=False,
        confirm_polls=1,
    )

    first = watcher.toast_policy_for(relation.qualified)
    watcher.known[relation.qualified] = replace(relation, relation_type_oid=4201)
    second = watcher.toast_policy_for(relation.qualified)
    watcher.known[relation.qualified] = replace(relation, relation_type_oid=4201)
    watcher._epoch += 1
    third = watcher.toast_policy_for(relation.qualified)

    assert first is not second
    assert second is not third
    assert watcher.toast_policy_builds == 3
    assert watcher.toast_policy_cache_hits == 0
