"""Default-lane contract tests for rubric 2.4 native type handling."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from cdc_flight.typed_types import (
    FieldState,
    FieldValue,
    SourceTypeDescriptor,
    TypedImage,
    UnsupportedType,
    encode_value,
    native_type,
    numeric_value,
)
from support.type_matrix import nested_matrix, scalar_matrix


def test_descriptor_is_recursive_and_has_stable_fingerprint():
    original = nested_matrix()[2]
    restored = SourceTypeDescriptor.from_dict(original.to_dict())

    assert restored == original
    assert restored.fingerprint == original.fingerprint
    assert restored.array_element is None
    assert restored.map_key.qualified_name == "pg_catalog.text"


@pytest.mark.parametrize("source", scalar_matrix())
def test_every_core_scalar_resolves_to_a_native_destination(source):
    target = native_type(source)
    assert target.sql
    assert target.kind not in {"VARCHAR_FALLBACK", "JSON_FALLBACK"}


@pytest.mark.parametrize("source", nested_matrix())
def test_nested_values_are_not_stringified(source):
    target = native_type(source)
    assert target.kind in {"LIST", "STRUCT", "MAP"}
    assert "JSON" not in target.sql.upper()
    assert target.sql != "VARCHAR"


@pytest.mark.parametrize("value, expected", [
    ("NaN", "special"),
    ("Infinity", "special"),
    ("-Infinity", "special"),
    ("12.3400", "finite"),
])
def test_numeric_specials_use_the_declared_numeric_union(value, expected):
    source = SourceTypeDescriptor(
        oid=1700,
        qualified_name="pg_catalog.numeric",
        kind="numeric",
        precision=12,
        scale=4,
    )
    encoded = numeric_value(value, source)
    assert encoded.member == expected
    if expected == "finite":
        assert encoded.value == Decimal("12.3400")
    else:
        assert encoded.value in {float("inf"), float("-inf")} or encoded.value != encoded.value


@pytest.mark.parametrize("value", [date(2026, 8, 7), time(1, 2, 3), datetime(2026, 8, 7, 1, 2, 3)])
def test_temporal_values_remain_temporal(value):
    kind = {date: "date", time: "time", datetime: "timestamp"}[type(value)]
    target = native_type(SourceTypeDescriptor(oid=1, qualified_name=f"pg_catalog.{kind}", kind=kind))
    assert encode_value(value, target.source or target) == value


def test_typed_image_distinguishes_null_from_absent_and_round_trips():
    integer = SourceTypeDescriptor(oid=23, qualified_name="pg_catalog.int4", kind="int4")
    image = TypedImage.from_mapping({"a": None, "b": 3}, {"a": integer, "b": integer})

    assert image.field("a").state is FieldState.EXPLICIT_NULL
    assert image.field("b").state is FieldState.VALUE
    assert image.field("c").state is FieldState.ABSENT
    assert TypedImage.from_dict(image.to_dict()) == image
    assert FieldValue.unchanged_toast().state is FieldState.UNCHANGED_TOAST


def test_unknown_types_fail_closed_instead_of_becoming_text():
    with pytest.raises(UnsupportedType):
        native_type(SourceTypeDescriptor(oid=999999, qualified_name="ext.secret", kind="unknown"))

