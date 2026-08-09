"""Round-3 regressions for identity stability across current-version swaps."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import duckdb
import pytest
from support.type_matrix import nested_matrix, scalar_matrix

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.identity_codec import identity_value
from cdc_flight.typed_types import SourceTypeDescriptor


def _source(kind: str, oid: int, **kwargs) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind, **kwargs)


def _wrapped(child: SourceTypeDescriptor, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        oid,
        f"app.shadow_key_{oid}",
        "composite",
        composite_fields=(("value", child),),
    )


def _full_type_cases():
    """Return every declared 2.4 shape plus the recursive key shapes from the spec."""
    values = {
        "int2": 7,
        "int4": 7,
        "int8": 7,
        "float4": 1.23,
        "float8": 1.23,
        "bool": True,
        "text": "unicode-é-🙂",
        "bytea": b"\x00abc",
        "date": "2024-03-10",
        "time": "12:34:56.123456",
        "timestamp": "2024-03-10T01:30:00",
        "timestamptz": "2024-03-10T01:30:00-07:00",
        "timetz": "12:34:56.123456+02:00",
        "interval": "P1Y2M3DT4H5M6S",
        "uuid": "A0B1C2D3-E4F5-4678-9012-ABCDEFABCDEF",
        "json": '{"b":2,"a":1}',
        "jsonb": '{"b":2,"a":1}',
        "enum": "paid",
        "inet": "192.0.2.1",
        "money": "12.34",
    }
    values_by_precision = {
        (12, 4): Decimal("1.0000"),
        (50, 8): Decimal("123456789.12345678"),
    }
    cases = []
    for source in scalar_matrix() + nested_matrix():
        if source.kind == "numeric":
            value = values_by_precision[(source.precision, source.scale)]
        elif source.kind == "array":
            value = (
                [[1, None], []]
                if source.array_element and source.array_element.kind == "array"
                else [1, None]
            )
        elif source.kind == "map":
            value = {"a": "é🙂", "b": None}
        elif source.kind == "composite":
            value = {"id": 7, "label": "é🙂"}
        elif source.kind == "domain":
            value = [1, None]
        else:
            value = values[source.kind]
        cases.append((f"{source.kind}-{source.oid}-{source.precision or ''}", source, value))
        if source.kind == "array" and source.array_element and source.array_element.kind != "array":
            cases.append((f"{source.kind}-{source.oid}-empty", source, []))
        if source.kind == "timestamptz":
            cases[-1] = (f"{source.kind}-{source.oid}-spring-dst", source, "2024-03-10T01:30:00-07:00")
            cases.append((f"{source.kind}-{source.oid}-fall-dst", source, "2023-11-05T01:30:00-06:00"))

    integer = _source("int4", 23)
    text = _source("text", 25)
    jsonb = _source("jsonb", 3802)
    range_type = SourceTypeDescriptor(
        3904, "pg_catalog.int4range", "range", range_subtype=integer
    )
    cases.extend(
        [
            (
                "nested-composite-array-jsonb",
                SourceTypeDescriptor(
                    9900,
                    "app.outer_key",
                    "composite",
                    composite_fields=(
                        (
                            "items",
                            SourceTypeDescriptor(
                                9901,
                                "app.inner_key[]",
                                "array",
                                array_element=SourceTypeDescriptor(
                                    9902,
                                    "app.inner_key",
                                    "composite",
                                    composite_fields=(
                                        ("amount", _source("numeric", 1700, precision=12, scale=4)),
                                        ("label", text),
                                    ),
                                ),
                            ),
                        ),
                        ("doc", jsonb),
                    ),
                ),
                {
                    "items": [{"amount": "1.0000", "label": "é🙂"}, {"amount": None, "label": "z"}],
                    "doc": '{"b":[1.0],"a":2}',
                },
            ),
            (
                "range",
                range_type,
                "[1,5)",
            ),
            (
                "multirange",
                SourceTypeDescriptor(
                    4451,
                    "pg_catalog.int4multirange",
                    "multirange",
                    range_subtype=range_type,
                ),
                ["[1,5)", "[10,12)"],
            ),
            (
                "domain-over-jsonb",
                SourceTypeDescriptor(
                    9903,
                    "app.json_domain",
                    "domain",
                    domain_base=jsonb,
                ),
                '{"v":1.0}',
            ),
        ]
    )
    return tuple(cases)


_FULL_TYPE_CASES = _full_type_cases()


@pytest.mark.parametrize(
    ("name", "source", "source_value"),
    _FULL_TYPE_CASES,
    ids=[case[0] for case in _FULL_TYPE_CASES],
)
def test_full_24_type_list_source_identity_survives_typed_shadow_swap(
    name, source, source_value
):
    """The ID after a typed swap is the source identity, never a readback identity."""
    new_source = replace(
        source,
        oid=(source.oid or 1) + 50000,
        qualified_name=f"{source.qualified_name}.shadow",
    )
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "full_type_identity",
            columns={"key": source, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        insert_rows(
            con,
            registry.get("full_type_identity"),
            ["key", "payload"],
            [[source_value, "kept"]],
        )
        source_identity = identity_value(
            registry.get("full_type_identity"),
            (source_value,),
            key_columns=("key",),
        )
        registry.convert_column_to_union(
            "full_type_identity", "key", source, new_source
        )
        stored_identity = con.execute(
            'SELECT "cdcf_internal_id" FROM typed."full_type_identity"'
        ).fetchone()[0]
        assert stored_identity == source_identity, name
    finally:
        con.close()


@pytest.mark.parametrize(
    ("old_child", "new_child", "source_value"),
    [
        (_source("real", 700), _source("double", 701), {"value": 1.23}),
        (
            _source("numeric", 1700, precision=12, scale=4),
            _source("numeric", 1701, precision=12, scale=4),
            {"value": Decimal("1.0")},
        ),
        (
            _source("timestamptz", 1184),
            _source("timestamp", 1114),
            {"value": "2024-01-01T00:00:00+02:00"},
        ),
        (
            _source(
                "array", 9302,
                array_element=_source("numeric", 1700, precision=12, scale=4),
            ),
            _source(
                "array", 9303,
                array_element=_source("numeric", 1701, precision=12, scale=4),
            ),
            {"value": [Decimal("1.0"), None]},
        ),
    ],
)
def test_shadow_swap_carries_source_identity_verbatim(
    old_child, new_child, source_value
):
    old_key = _wrapped(old_child, 9400 + old_child.oid % 100)
    new_key = _wrapped(new_child, 9500 + new_child.oid % 100)
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "stable_identity",
            columns={"key": old_key, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        insert_rows(
            con, registry.get("stable_identity"), ["key", "payload"],
            [[source_value, "kept"]],
        )
        before = con.execute(
            'SELECT "cdcf_internal_id" FROM typed."stable_identity"'
        ).fetchone()[0]
        registry.convert_column_to_union("stable_identity", "key", old_key, new_key)
        after = con.execute(
            'SELECT "cdcf_internal_id" FROM typed."stable_identity"'
        ).fetchone()[0]
        assert after == before
        delete_keys(con, registry.get("stable_identity"), ("key",), [(source_value,)])
        assert con.execute('SELECT count(*) FROM typed."stable_identity"').fetchone()[0] == 0
    finally:
        con.close()
