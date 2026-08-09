"""Round-3 regressions for identity stability across current-version swaps."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import duckdb
import pytest
from support.type_matrix import nested_matrix, scalar_matrix

from cdc_flight.apply_sql import SchemaRegistry, delete_keys, insert_rows
from cdc_flight.identity_codec import _identity_tree, identity_value
from cdc_flight.typed_types import CanonicalRangeText, SourceTypeDescriptor


def _source(kind: str, oid: int, **kwargs) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(oid, f"pg_catalog.{kind}", kind, **kwargs)


def _wrapped(child: SourceTypeDescriptor, oid: int) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        oid,
        f"app.shadow_key_{oid}",
        "composite",
        composite_fields=(("value", child),),
    )


def test_range_and_multirange_identity_preserves_postgres_equality_classes():
    """Supported range values must keep source semantics across destination readback."""
    int4 = _source("int4", 23)
    int4range = SourceTypeDescriptor(
        3904, "pg_catalog.int4range", "range", range_subtype=int4
    )
    int4multirange = SourceTypeDescriptor(
        4451, "pg_catalog.int4multirange", "multirange", range_subtype=int4range
    )
    float4 = _source("float4", 700)
    float4range = SourceTypeDescriptor(
        3906, "pg_catalog.float4range", "range", range_subtype=float4
    )
    tstz = _source("timestamptz", 1184)
    tstzrange = SourceTypeDescriptor(
        3910, "pg_catalog.tstzrange", "range", range_subtype=tstz
    )

    # This is the exact r4 float4 readback pair: DuckDB exposes the FLOAT value
    # through Python as a widened double.
    source_float4 = {
        "is_empty": False,
        "lower": 1.23,
        "upper": 2.34,
        "lower_inclusive": True,
        "upper_inclusive": False,
    }
    readback_float4 = {
        "is_empty": False,
        "lower": 1.2300000190734863,
        "upper": 2.3399999141693115,
        "lower_inclusive": True,
        "upper_inclusive": False,
    }
    assert _identity_tree(source_float4, float4range) == _identity_tree(
        readback_float4, float4range
    )

    # PostgreSQL canonicalizes discrete ranges to [): these are equal ranges.
    assert _identity_tree("[1,3]", int4range) == _identity_tree(
        "[1,4)", int4range
    )
    assert _identity_tree("(1,2)", int4range) == _identity_tree(
        "empty", int4range
    )
    assert _identity_tree(None, int4range) != _identity_tree("empty", int4range)

    # PostgreSQL compares timestamptz endpoints by instant, not by offset spelling.
    minus_seven = timezone(timedelta(hours=-7))
    first_zone = {
        "is_empty": False,
        "lower": datetime(2024, 1, 1, 0, 0, tzinfo=minus_seven),
        "upper": datetime(2024, 1, 1, 8, 0, tzinfo=minus_seven),
        "lower_inclusive": True,
        "upper_inclusive": False,
    }
    utc = {
        "is_empty": False,
        "lower": datetime(2024, 1, 1, 7, 0, tzinfo=UTC),
        "upper": datetime(2024, 1, 1, 15, 0, tzinfo=UTC),
        "lower_inclusive": True,
        "upper_inclusive": False,
    }
    assert _identity_tree(first_zone, tstzrange) == _identity_tree(utc, tstzrange)

    # Unbounded flags are syntax, not values: PostgreSQL treats every spelling
    # without a lower/upper bound as the same infinite endpoint.
    assert _identity_tree("[,3)", int4range) == _identity_tree("(,3)", int4range)
    assert _identity_tree("[1,)", int4range) == _identity_tree("[1,]", int4range)
    assert _identity_tree("empty", int4range) != _identity_tree("[,)", int4range)

    # Multirange equality is order-independent and merges overlapping/adjacent
    # ranges before identity is formed.
    assert _identity_tree(
        ["[10,12)", "[1,3]"], int4multirange
    ) == _identity_tree(["[1,4)", "[10,12)"], int4multirange)


def test_r5_range_residuals_use_special_infinity_and_continuous_multirange_equality():
    """The r5 residual pair must remain in PostgreSQL's equality classes."""
    timestamp = _source("timestamp", 1114)
    tsrange = SourceTypeDescriptor(
        3908, "pg_catalog.tsrange", "range", range_subtype=timestamp
    )
    numeric = _source("numeric", 1700)
    numrange = SourceTypeDescriptor(
        3906, "pg_catalog.numrange", "range", range_subtype=numeric
    )
    nummultirange = SourceTypeDescriptor(
        4532,
        "pg_catalog.nummultirange",
        "multirange",
        range_subtype=numrange,
    )

    special = _identity_tree("[-infinity,infinity]", tsrange)
    unbounded = _identity_tree("(,)", tsrange)
    assert special == _identity_tree("[-infinity,infinity]", tsrange)
    assert special != unbounded

    assert _identity_tree(
        ["[10,20)", "[2,10)"], nummultirange
    ) == _identity_tree(["[2,20)"], nummultirange)


def test_canonical_range_text_survives_a_typed_shadow_key_swap():
    """A native STRUCT readback must not turn the source text into Python repr."""
    timestamp = _source("timestamp", 1114)
    tsrange = SourceTypeDescriptor(
        3908, "pg_catalog.tsrange", "range", range_subtype=timestamp
    )
    text = _source("text", 25)
    value = CanonicalRangeText("[-infinity,infinity]")
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "range_shadow_key",
            columns={"key": tsrange, "payload": text},
            key_columns=("key",),
        )
        insert_rows(con, registry.get("range_shadow_key"), ["key", "payload"], [[value, "kept"]])
        registry.convert_column_to_union("range_shadow_key", "key", tsrange, text)
        table = registry.get("range_shadow_key")
        assert con.execute(
            'SELECT "cdcf_internal_id" FROM typed."range_shadow_key"'
        ).fetchone()[0] == identity_value(
            table, (value,), key_columns=("key",)
        )
        delete_keys(con, table, ("key",), [(value,)])
        assert con.execute('SELECT count(*) FROM typed."range_shadow_key"').fetchone() == (0,)
    finally:
        con.close()


def test_matrix_has_no_parallel_physical_row_reachability_predicates():
    """Reachability must come from the owner, not a second production table."""
    from cdc_flight import machines

    assert not hasattr(machines, "PHYSICAL_ROW_PRECONDITIONS")
    assert not hasattr(machines, "physical_row_unreachable_reason")


def test_range_and_multirange_key_gain_deletes_equivalent_keys_after_readback():
    """The production key path uses the range identity, not STRUCT display text."""
    integer = _source("int4", 23)
    text = _source("text", 25)
    range_type = SourceTypeDescriptor(
        3904, "pg_catalog.int4range", "range", range_subtype=integer
    )
    multirange_type = SourceTypeDescriptor(
        4451,
        "pg_catalog.int4multirange",
        "multirange",
        range_subtype=range_type,
    )
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        cases = (
            ("range_key", range_type, "[1,3]", "[1,4)"),
            (
                "multirange_key",
                multirange_type,
                ["[10,12)", "[1,3]", "[3,5)"],
                ["[1,5)", "[10,12)"],
            ),
        )
        for name, descriptor, source_value, equivalent_value in cases:
            table, _ = registry.ensure_typed(
                name,
                columns={"key": descriptor, "payload": text},
                key_columns=("key",),
            )
            insert_rows(con, table, ["key", "payload"], [[source_value, "kept"]])
            readback = con.execute(
                f'SELECT "key" FROM typed."{name}"'
            ).fetchone()[0]
            assert identity_value(table, (source_value,), key_columns=("key",)) == identity_value(
                table, (readback,), key_columns=("key",)
            )
            delete_keys(con, table, ("key",), [(equivalent_value,)])
            assert con.execute(f'SELECT count(*) FROM typed."{name}"').fetchone() == (0,)
    finally:
        con.close()


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

_SPECIAL_ROUND_TRIPS = (
    ("float4-nan", _source("real", 700), "NaN"),
    ("float4-positive-infinity", _source("real", 700), "Infinity"),
    ("float8-negative-infinity", _source("double", 701), "-Infinity"),
    ("float8-signed-zero", _source("double", 701), -0.0),
    ("numeric-special", _source("numeric", 1700, precision=12, scale=4), "NaN"),
    ("numeric-variable-special", _source("numeric", 1700, precision=50, scale=8), "Infinity"),
)


@pytest.mark.parametrize(
    ("name", "source", "source_value"),
    _FULL_TYPE_CASES,
    ids=[case[0] for case in _FULL_TYPE_CASES],
)
def test_full_24_type_list_source_identity_survives_typed_shadow_swap(
    name, source, source_value
):
    """A typed swap rewrites the row to the one current source identity."""
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
        registry.convert_column_to_union(
            "full_type_identity", "key", source, new_source
        )
        current = registry.get("full_type_identity")
        row = con.execute(
            'SELECT "key", "cdcf_internal_id" FROM typed."full_type_identity"'
        ).fetchone()
        assert row[1] == identity_value(current, (source_value,), key_columns=("key",)), name
        assert row[1] == identity_value(current, (row[0],), key_columns=("key",)), name
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
def test_shadow_swap_rewrites_to_current_source_identity(
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
        registry.convert_column_to_union("stable_identity", "key", old_key, new_key)
        row = con.execute(
            'SELECT "key", "cdcf_internal_id" FROM typed."stable_identity"'
        ).fetchone()
        after = row[1]
        current = registry.get("stable_identity")
        assert after == identity_value(current, (row[0],), key_columns=("key",))
        # For an incompatible source-type change, an existing old UNION member
        # is addressed by its destination readback value.  The int4 -> int8
        # source-value path above proves the lossless widening case directly.
        delete_keys(con, registry.get("stable_identity"), ("key",), [(row[0],)])
        assert con.execute('SELECT count(*) FROM typed."stable_identity"').fetchone()[0] == 0
    finally:
        con.close()


def test_float4_key_delete_binds_the_target_native_type():
    """The exact r3 FLOAT/DOUBLE predicate miss must be a red regression."""
    real = _source("real", 700)
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "float4_key", columns={"key": real, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        insert_rows(con, registry.get("float4_key"), ["key", "payload"], [[1.23, "kept"]])
        delete_keys(con, registry.get("float4_key"), ("key",), [(1.23,)])
        assert con.execute('SELECT count(*) FROM typed."float4_key"').fetchone() == (0,)
    finally:
        con.close()


def test_interval_identity_is_stable_from_source_text_to_duckdb_readback():
    """P1Y2M3DT4H5M6S must address the timedelta DuckDB returns."""
    interval = _source("interval", 1186)
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "interval_readback", columns={"key": interval, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        table = registry.get("interval_readback")
        source_value = "P1Y2M3DT4H5M6S"
        insert_rows(con, table, ["key", "payload"], [[source_value, "kept"]])
        readback = con.execute('SELECT "key" FROM typed."interval_readback"').fetchone()[0]
        assert identity_value(table, (source_value,), key_columns=("key",)) == identity_value(
            table, (readback,), key_columns=("key",)
        )
        delete_keys(con, table, ("key",), [(source_value,)])
        assert con.execute('SELECT count(*) FROM typed."interval_readback"').fetchone() == (0,)
    finally:
        con.close()


def test_numeric_union_readback_normalizes_to_the_source_numeric_tree():
    numeric12 = _source("numeric", 1700, precision=12, scale=4)
    numeric18 = _source("numeric", 1701, precision=18, scale=4)
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "numeric_key", columns={"key": numeric12, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        insert_rows(
            con, registry.get("numeric_key"), ["key", "payload"], [[Decimal("1.0000"), "kept"]]
        )
        registry.convert_column_to_union("numeric_key", "key", numeric12, numeric18)
        table = registry.get("numeric_key")
        readback = con.execute('SELECT "key" FROM typed."numeric_key"').fetchone()[0]
        assert identity_value(table, (Decimal("1.0000"),), key_columns=("key",)) == identity_value(
            table, (readback,), key_columns=("key",)
        )
        delete_keys(con, table, ("key",), [(Decimal("1.0000"),)])
        assert con.execute('SELECT count(*) FROM typed."numeric_key"').fetchone() == (0,)
    finally:
        con.close()


def test_signed_zero_has_one_internal_identity():
    float8 = _source("double", 701)
    composite = SourceTypeDescriptor(
        9701,
        "app.zero_key",
        "composite",
        composite_fields=(("value", float8),),
    )
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "signed_zero", columns={"key": composite, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        table = registry.get("signed_zero")
        insert_rows(con, table, ["key", "payload"], [[{"value": 0.0}, "kept"]])
        delete_keys(con, table, ("key",), [({"value": -0.0},)])
        assert con.execute('SELECT count(*) FROM typed."signed_zero"').fetchone() == (0,)
    finally:
        con.close()


def test_interval_identity_uses_exact_integer_units_not_float_microseconds():
    interval = _source("interval", 1186)
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "interval_collision", columns={"key": interval}, key_columns=("key",)
        )
        table = registry.get("interval_collision")
        left = timedelta(days=200_000_000)
        right = timedelta(days=200_000_000, microseconds=1)
        assert identity_value(table, (left,), key_columns=("key",)) != identity_value(
            table, (right,), key_columns=("key",)
        )
    finally:
        con.close()


def test_current_identity_has_no_historical_descriptor_sidecar_or_candidates():
    """Identity history is forbidden once the canonical swap rewrite is in place."""
    from cdc_flight import identity_codec

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        table, _ = registry.ensure_typed(
            "current_identity", columns={"key": _source("int4", 23)}, key_columns=("key",)
        )
        assert not hasattr(table, "identity_descriptors")
        assert not hasattr(identity_codec, "_identity_candidates")
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='typed' AND table_name='__cdcf_key_metadata'"
        ).fetchall()
        assert "identity_descriptors" not in {row[0] for row in columns}
    finally:
        con.close()


@pytest.mark.parametrize(
    ("name", "source", "source_value"),
    _FULL_TYPE_CASES,
    ids=[case[0] for case in _FULL_TYPE_CASES],
)
def test_full_declared_type_identity_matches_readback_and_current_swap(
    name, source, source_value
):
    """Every declared source value has one identity before/after a typed swap."""
    new_source = replace(
        source,
        oid=(source.oid or 1) + 70000,
        qualified_name=f"{source.qualified_name}.current",
    )
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "identity_property", columns={"key": source, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        table = registry.get("identity_property")
        insert_rows(con, table, ["key", "payload"], [[source_value, "kept"]])
        source_id = identity_value(table, (source_value,), key_columns=("key",))
        readback = con.execute('SELECT "key" FROM typed."identity_property"').fetchone()[0]
        assert source_id == identity_value(table, (readback,), key_columns=("key",)), name

        registry.convert_column_to_union("identity_property", "key", source, new_source)
        current = registry.get("identity_property")
        swapped = con.execute('SELECT "key", "cdcf_internal_id" FROM typed."identity_property"').fetchone()
        current_id = identity_value(current, (source_value,), key_columns=("key",))
        assert swapped[1] == current_id, name
        assert current_id == identity_value(current, (swapped[0],), key_columns=("key",)), name
    finally:
        con.close()


@pytest.mark.parametrize(
    ("name", "source", "source_value"),
    _SPECIAL_ROUND_TRIPS,
    ids=[case[0] for case in _SPECIAL_ROUND_TRIPS],
)
def test_special_values_are_stable_through_readback_and_shadow_swap(
    name, source, source_value
):
    """NaN, infinities, and signed zero share one current canonical tree."""
    new_source = replace(
        source,
        oid=(source.oid or 1) + 80000,
        qualified_name=f"{source.qualified_name}.special",
    )
    con = duckdb.connect(":memory:", config={
        "storage_compatibility_version": "v1.5.0",
        "variant_minimum_shredding_size": "-1",
    })
    try:
        con.execute("CREATE SCHEMA typed")
        registry = SchemaRegistry(con, "typed")
        registry.ensure_typed(
            "special_identity",
            columns={"key": source, "payload": _source("text", 25)},
            key_columns=("key",),
        )
        table = registry.get("special_identity")
        insert_rows(con, table, ["key", "payload"], [[source_value, "kept"]])
        readback = con.execute(
            'SELECT "key" FROM typed."special_identity"'
        ).fetchone()[0]
        assert identity_value(table, (source_value,), key_columns=("key",)) == identity_value(
            table, (readback,), key_columns=("key",)
        ), name
        registry.convert_column_to_union("special_identity", "key", source, new_source)
        current = registry.get("special_identity")
        row = con.execute(
            'SELECT "key", "cdcf_internal_id" FROM typed."special_identity"'
        ).fetchone()
        assert row[1] == identity_value(current, (source_value,), key_columns=("key",)), name
        assert row[1] == identity_value(current, (row[0],), key_columns=("key",)), name
    finally:
        con.close()
