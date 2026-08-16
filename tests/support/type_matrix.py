"""Small, deterministic fixtures shared by the 2.4 and 2.5 rubric suites.

The production codec deliberately consumes descriptors rather than inferring a
destination type from a Python value.  Keeping these fixtures in ``tests/support``
makes that rule visible to both the unit and MotherDuck lanes.
"""

from __future__ import annotations

from cdc_flight.typed_types import SourceTypeDescriptor


def descriptor(type_name: str, *, oid: int = 1, **kwargs) -> SourceTypeDescriptor:
    return SourceTypeDescriptor(
        oid=oid,
        qualified_name=kwargs.pop("qualified_name", f"pg_catalog.{type_name}"),
        kind=kwargs.pop("kind", type_name),
        **kwargs,
    )


def scalar_matrix() -> tuple[SourceTypeDescriptor, ...]:
    return (
        descriptor("int2", oid=21),
        descriptor("int4", oid=23),
        descriptor("int8", oid=20),
        descriptor("float4", oid=700),
        descriptor("float8", oid=701),
        descriptor("bool", oid=16),
        descriptor("text", oid=25),
        descriptor("bytea", oid=17),
        descriptor("date", oid=1082),
        descriptor("time", oid=1083),
        descriptor("timestamp", oid=1114),
        descriptor("timestamptz", oid=1184),
        descriptor("timetz", oid=1266),
        descriptor("interval", oid=1186),
        descriptor("uuid", oid=2950),
        descriptor("json", oid=114),
        descriptor("jsonb", oid=3802),
        descriptor("numeric", oid=1700, precision=12, scale=4),
        descriptor("numeric", oid=1700, precision=50, scale=8),
        descriptor("enum", oid=9100, kind="enum", enum_labels=("pending", "paid")),
        descriptor("inet", oid=869),
        descriptor("money", oid=790),
    )


def nested_matrix() -> tuple[SourceTypeDescriptor, ...]:
    integer = descriptor("int4", oid=23)
    text = descriptor("text", oid=25)
    item = descriptor("composite", oid=9000, kind="composite", composite_fields=(
        ("id", integer),
        ("label", text),
    ))
    domain_array = descriptor(
        "app.int_list_domain",
        oid=9200,
        kind="domain",
        domain_base=descriptor("_int4", oid=1007, kind="array", array_element=integer),
    )
    return (
        descriptor("_int4", oid=1007, kind="array", array_element=integer),
        descriptor("_int4", oid=1007, kind="array", array_element=descriptor(
            "_int4", oid=1007, kind="array", array_element=integer
        )),
        descriptor("hstore", oid=9999, kind="map", map_key=text, map_value=text),
        item,
        domain_array,
    )
