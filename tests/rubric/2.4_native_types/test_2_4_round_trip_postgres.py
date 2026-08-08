"""Slow real-PostgreSQL/Debezium evidence for rubric 2.4."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_real_postgres_native_arrays_specials_and_obscure_text(sandbox):
    """A real schema-bearing stream reaches native nested destinations."""
    assert sandbox.source.port == 15432
    sandbox.reseed()
    initial = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert initial["ok"] is True, initial

    sandbox.sql(
        [
            "UPDATE app.wide_types SET "
            "col_int_array = ARRAY[]::integer[], "
            "col_text_array = ARRAY[]::text[], "
            "col_numeric_array = ARRAY[]::numeric(12,2)[] "
            "WHERE id = 1",
            "UPDATE app.wide_types SET "
            "col_double_inf = '-Infinity'::double precision, "
            "col_double_nan = 'NaN'::double precision "
            "WHERE id = 1",
        ],
        one_transaction=True,
    )
    streamed = sandbox.run(max_seconds=150, idle_seconds=6)
    assert streamed["ok"] is True, streamed

    types = dict(
        sandbox.duck_query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' AND table_name = 'cdcflight_app_wide_types'"
        )
    )
    assert types["col_int_array"] == "INTEGER[]"
    assert types["col_text_array"] == "VARCHAR[]"
    assert types["col_numeric_array"].endswith("[]")
    assert types["col_numeric_array"].startswith("UNION(")
    assert types["col_jsonb"] == "JSON"
    assert types["col_inet"] == "VARCHAR"
    assert types["col_money"] == "VARCHAR"

    row = sandbox.duck_query(
        "SELECT col_int_array, col_text_array, col_numeric_array, "
        "isinf(col_double_inf), isnan(col_double_nan) "
        "FROM cdc_raw.cdcflight_app_wide_types WHERE id = 1"
    )[0]
    assert row[:3] == ([], [], [])
    assert row[3:] == (True, True)
