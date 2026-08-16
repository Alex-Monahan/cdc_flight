"""Regression probes for mutable PostgreSQL type facts."""

from __future__ import annotations

from cdc_flight.catalog_descriptors import CatalogDescriptorReader


class _MutableCatalog:
    """Small catalog surface whose enum/composite facts change in place."""

    def __init__(self):
        self.epoch = 0

    def execute(self, sql, _params):
        if "FROM pg_type" in sql:
            if "9000" in str(_params):
                return _Rows(
                    [
                        (9000, "app", "mood", "e", "E", 0, 0, 0, 0, 0),
                    ]
                )
            if "9001" in str(_params):
                return _Rows(
                    [
                        (9001, "app", "address", "c", "C", 0, 0, 9101, 0, 0),
                    ]
                )
            return _Rows([(25, "pg_catalog", "text", "b", "S", 0, 0, 0, 0, 0)])
        if "FROM pg_enum" in sql:
            return _Rows(
                [(9000, "happy"), (9000, "sad")]
                if self.epoch == 0
                else [(9000, "happy"), (9000, "sad"), (9000, "sleepy")]
            )
        if "FROM pg_attribute" in sql:
            return _Rows(
                [(9101, "street", 25)]
                if self.epoch == 0
                else [(9101, "street", 25), (9101, "postal_code", 25)]
            )
        raise AssertionError(f"unexpected catalog query: {sql}")


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def test_enum_labels_are_refreshed_after_an_in_place_add_value():
    catalog = _MutableCatalog()
    reader = CatalogDescriptorReader(catalog)
    assert reader.resolve([9000])[9000].enum_labels == ("happy", "sad")
    catalog.epoch = 1
    assert reader.resolve([9000])[9000].enum_labels == ("happy", "sad", "sleepy")


def test_composite_fields_are_refreshed_after_an_in_place_add_attribute():
    catalog = _MutableCatalog()
    reader = CatalogDescriptorReader(catalog)
    assert tuple(name for name, _ in reader.resolve([9001])[9001].composite_fields) == (
        "street",
    )
    catalog.epoch = 1
    assert tuple(name for name, _ in reader.resolve([9001])[9001].composite_fields) == (
        "street",
        "postal_code",
    )
