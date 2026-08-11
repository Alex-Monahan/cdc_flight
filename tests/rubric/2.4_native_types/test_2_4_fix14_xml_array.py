"""FIX ROUND 14: stock Debezium's omitted opaque xml[] field is recovered."""

from __future__ import annotations

import os

import pytest

from cdc_flight.config import ReplicationConfig

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_xml_array_is_loaded_from_the_source_without_blocking_a_peer(sandbox):
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    table = "app.fix14_xml_array"
    peer = "app.fix14_xml_array_peer"
    publication = ReplicationConfig().publication_name
    sandbox.reseed()
    try:
        sandbox.sql(
            [
                f"CREATE TABLE {table} (id integer PRIMARY KEY, x xml[], note text)",
                f"CREATE TABLE {peer} (id integer PRIMARY KEY, note text)",
                f"ALTER PUBLICATION {publication} ADD TABLE {table}",
                f"ALTER PUBLICATION {publication} ADD TABLE {peer}",
            ],
            one_transaction=True,
        )
        capture = {
            "CDC_AUTO_DISCOVERY": "0",
            "CDC_TABLES": "fix14_xml_array,fix14_xml_array_peer",
        }
        initial = sandbox.run(
            reset_state=True, extra_env=capture, max_seconds=180, idle_seconds=6
        )
        assert initial["ok"] is True, initial

        sandbox.sql(
            [
                "INSERT INTO app.fix14_xml_array VALUES "
                "(1, ARRAY['<a>1</a>'::xml, '<b>2</b>'::xml], 'xml-1')",
                "INSERT INTO app.fix14_xml_array_peer VALUES (1, 'peer-1')",
                "INSERT INTO app.fix14_xml_array VALUES "
                "(2, ARRAY['<c>3</c>'::xml], 'xml-2')",
                "INSERT INTO app.fix14_xml_array_peer VALUES (2, 'peer-2')",
            ],
            one_transaction=True,
        )
        streamed = sandbox.run(extra_env=capture, max_seconds=180, idle_seconds=6)
        assert streamed["ok"] is True, streamed

        source = sandbox.pg_query(
            "SELECT id, x::text, note FROM app.fix14_xml_array ORDER BY id"
        )
        destination = sandbox.duck_query(
            "SELECT id, x, note FROM cdc_raw.cdcflight_app_fix14_xml_array ORDER BY id"
        )
        assert source == [
            (1, "{<a>1</a>,<b>2</b>}", "xml-1"),
            (2, "{<c>3</c>}", "xml-2"),
        ]
        assert destination == [
            (1, ["<a>1</a>", "<b>2</b>"], "xml-1"),
            (2, ["<c>3</c>"], "xml-2"),
        ]
        assert sandbox.duck_query(
            "SELECT id, note FROM cdc_raw.cdcflight_app_fix14_xml_array_peer ORDER BY id"
        ) == [(1, "peer-1"), (2, "peer-2")]
    finally:
        sandbox.sql(
            [
                f"ALTER PUBLICATION {publication} DROP TABLE {table}",
                f"ALTER PUBLICATION {publication} DROP TABLE {peer}",
                f"DROP TABLE IF EXISTS {table}",
                f"DROP TABLE IF EXISTS {peer}",
            ],
            one_transaction=True,
        )
