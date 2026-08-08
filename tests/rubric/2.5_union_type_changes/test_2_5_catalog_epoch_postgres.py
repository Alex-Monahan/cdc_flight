"""Slow real-PostgreSQL/Debezium evidence for rubric 2.5."""

from __future__ import annotations

import os

import pytest

from cdc_flight.apply_sql import _union_members

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_real_postgres_type_change_keeps_old_and_new_members(sandbox):
    """The source DDL/data epoch crosses one atomic typed shadow swap."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    sandbox.reseed()
    initial = sandbox.run(reset_state=True, max_seconds=150, idle_seconds=6)
    assert initial["ok"] is True, initial

    # Let the catalog fence marker itself become durable before the first
    # post-change row.  A DDL+row transaction is deliberately a mixed-epoch
    # refusal (covered by the unit gate); the production round trip below proves
    # the normal ordered DDL -> post-boundary data path.
    sandbox.sql(
        "ALTER TABLE app.customers ALTER COLUMN lifetime_value "
        "TYPE double precision USING lifetime_value::double precision"
    )
    observed = sandbox.run(max_seconds=150, idle_seconds=6)
    assert observed["ok"] is True, observed
    sandbox.sql(
        "UPDATE app.customers SET lifetime_value = lifetime_value + 0.125 "
        "WHERE id = 1"
    )
    streamed = sandbox.run(max_seconds=180, idle_seconds=6)
    assert streamed["ok"] is True, streamed
    assert (
        observed["catalog_changes_applied"] + streamed["catalog_changes_applied"] >= 1
    ), (observed, streamed)
    assert streamed["catalog_pending_schema"] == 0, streamed

    physical = sandbox.duck_query(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = 'cdc_raw' AND table_name = 'cdcflight_app_customers' "
        "AND column_name = 'lifetime_value'"
    )[0][0]
    members = _union_members(physical)
    assert physical.startswith("UNION(")
    assert len(members) == 2, physical
    assert any("DECIMAL(18,4)" in member_type for _, member_type in members)
    assert any(member_type == "DOUBLE" for _, member_type in members)

    tags = dict(
        sandbox.duck_query(
            "SELECT id, union_tag(lifetime_value) "
            "FROM cdc_raw.cdcflight_app_customers ORDER BY id"
        )
    )
    assert len(set(tags.values())) == 2, tags
    assert tags[1] != tags[2]
