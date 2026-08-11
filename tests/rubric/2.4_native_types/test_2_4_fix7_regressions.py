"""FIX ROUND 7 regression tests for unknown PostgreSQL type delivery."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cdc_flight.catalog_state import SourceRelation
from cdc_flight.catalog_support import observe_unit
from cdc_flight.config import ReplicationConfig, SourceConfig
from cdc_flight.debezium_props import build_properties
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.errors import SchemaShapeUnexplained
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.typed_types import (
    CanonicalRangeText,
    SourceTypeDescriptor,
    UnsupportedType,
    adapt_value,
    mark_canonical_range_text,
    native_type,
)


def _schema(*names: str) -> dict:
    return {"type": "struct", "fields": [{"field": name, "type": "string"} for name in names]}


def test_stock_connector_is_configured_to_deliver_unknown_datatypes():
    props = build_properties(SourceConfig(), ReplicationConfig())
    assert props["include.unknown.datatypes"] == "true"


def test_opaque_multirange_base64_unwraps_to_source_varchar_text():
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    int4range = SourceTypeDescriptor(
        3904, "pg_catalog.int4range", "range", range_subtype=integer
    )
    multirange = SourceTypeDescriptor(
        4451, "pg_catalog.int4multirange", "multirange", range_subtype=int4range
    )
    value = mark_canonical_range_text("e1sxLDMpfQ==", multirange)
    assert value == CanonicalRangeText("{[1,3)}")
    assert native_type(multirange).kind == "VARCHAR"
    assert native_type(multirange).indexable is True
    assert adapt_value(value, native_type(multirange)) == "{[1,3)}"

    incomplete = SourceTypeDescriptor(4451, "pg_catalog.int4multirange", "multirange")
    with pytest.raises(UnsupportedType):
        native_type(incomplete)


def test_catalog_event_shape_gate_refuses_an_omitted_catalog_column():
    """The inverse catalog-minus-event check must run before any table write."""
    integer = SourceTypeDescriptor(23, "pg_catalog.int4", "int4")
    text = SourceTypeDescriptor(25, "pg_catalog.text", "text")
    relation = SourceRelation(
        schema="app",
        table="multirange_probe",
        oid=9911,
        published=True,
        replica_identity="d",
        columns=(
            SourceColumn(1, "id", 23, "integer", descriptor=integer),
            SourceColumn(2, "mr", 4451, "int4multirange", descriptor=text),
            SourceColumn(3, "note", 25, "text", descriptor=text),
        ),
    )
    watcher = SimpleNamespace(
        dsn=None,
        known={relation.qualified: relation},
        _lock=__import__("threading").Lock(),
        _live=lambda: (),
        allowed_event_fields=lambda _name: {"id", "mr", "note"},
    )
    event = PendingRecord(
        raw=None,
        kind=KIND_DATA,
        topic="cdcflight.app.multirange_probe",
        nbytes=1,
        op="c",
        schema="app",
        table="multirange_probe",
        key={"id": 1},
        after={"id": 1, "note": "x"},
        key_schema=_schema("id"),
        after_schema=_schema("id", "note"),
    )

    with pytest.raises(SchemaShapeUnexplained, match="mr"):
        observe_unit(watcher, SimpleNamespace(events=[event]))
