"""Real stock-Debezium FIX ROUND 7 multirange sequence."""

from __future__ import annotations

import os

import psycopg
import pytest

from cdc_flight.config import ReplicationConfig

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def test_real_multirange_four_step_sequence_never_truncates_the_column(sandbox):
    """The r6 empty/insert/update/delete probe must retain ``mr`` end to end."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = ReplicationConfig().publication_name
    qualified = "app.multirange_probe"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "multirange_probe",
    }
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS app.multirange_probe")
        conn.execute(
            "CREATE TABLE app.multirange_probe ("
            "id integer PRIMARY KEY, mr int4multirange, note text)"
        )
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")

    try:
        empty = sandbox.run(reset_state=True, extra_env=capture)
        assert empty["ok"] is True, empty

        sandbox.sql("INSERT INTO app.multirange_probe VALUES (1, '{[1,3)}', 'first')")
        inserted = sandbox.run(extra_env=capture)
        assert inserted["ok"] is True, inserted
        assert sandbox.duck_query(
            'SELECT "id", "mr", "note" FROM cdc_raw.cdcflight_app_multirange_probe'
        ) == [(1, "{[1,3)}", "first")]

        sandbox.sql(
            "UPDATE app.multirange_probe SET mr = '{[4,6)}', "
            "note = 'updated' WHERE id = 1"
        )
        updated = sandbox.run(extra_env=capture)
        assert updated["ok"] is True, updated
        assert sandbox.duck_query(
            'SELECT "id", "mr", "note" FROM cdc_raw.cdcflight_app_multirange_probe'
        ) == [(1, "{[4,6)}", "updated")]

        sandbox.sql("DELETE FROM app.multirange_probe WHERE id = 1")
        deleted = sandbox.run(extra_env=capture)
        assert deleted["ok"] is True, deleted
        columns = dict(
            sandbox.duck_query(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'cdc_raw' AND "
                "table_name = 'cdcflight_app_multirange_probe'"
            )
        )
        assert columns["mr"] == "VARCHAR"
        assert sandbox.duck_query(
            'SELECT count(*) FROM cdc_raw.cdcflight_app_multirange_probe'
        ) == [(0,)]
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            conn.execute("DROP TABLE IF EXISTS app.multirange_probe")


def test_real_multirange_four_step_omission_is_a_loud_automatic_refusal(sandbox):
    """The r6 silent-drop schedule must remain refused if the connector omits ``mr``."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = ReplicationConfig().publication_name
    qualified = "app.multirange_probe_refusal"
    capture = {
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_TABLES": "multirange_probe_refusal",
        # Exercise stock Debezium's documented omission mode.  Production defaults
        # to true; this proves the inverse source-catalog/event-shape gate is still
        # the safety net when an operator runs the connector with false.
        "CDC_INCLUDE_UNKNOWN_DATATYPES": "false",
    }
    sandbox.reseed()
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS app.multirange_probe_refusal")
        conn.execute(
            "CREATE TABLE app.multirange_probe_refusal ("
            "id integer PRIMARY KEY, mr int4multirange, note text)"
        )
        conn.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")

    def run_refused() -> dict:
        # This four-run probe is intentionally a real JVM/source-task sequence.  The
        # slow lane can have another worker holding the host under load, and the
        # sandbox's ordinary 120-second run bound is then close enough to Debezium's
        # finite task-start bound that a refusal probe can fail before the connector
        # ever reaches the table.  Give this probe a larger, still finite budget so
        # its assertion remains about the omission/refusal rather than host startup
        # scheduling.
        result = sandbox.run(
            max_seconds=240,
            timeout=360,
            extra_env=capture,
            expect_success=False,
        )
        assert result["returncode"] != 0, result
        assert result.get("ok") is False, result
        assert "multirange_probe_refusal" in result.get("output", ""), result
        return result

    try:
        # Empty, insert, update, delete: every attempt is loud; none can create a
        # partial destination image or advance the durable source point.
        empty = sandbox.run(
            reset_state=True,
            max_seconds=240,
            timeout=360,
            extra_env=capture,
        )
        assert empty["ok"] is True, empty

        sandbox.sql(
            "INSERT INTO app.multirange_probe_refusal VALUES "
            "(1, '{[1,3)}', 'first')"
        )
        insert = run_refused()
        sandbox.sql(
            "UPDATE app.multirange_probe_refusal SET mr = '{[4,6)}', "
            "note = 'updated' WHERE id = 1"
        )
        update = run_refused()
        sandbox.sql("DELETE FROM app.multirange_probe_refusal WHERE id = 1")
        delete = run_refused()
        assert all(item["returncode"] != 0 for item in (insert, update, delete))

        assert sandbox.duck_query(
            "SELECT state, refusal_class FROM _cdc_flight.schema_refusals WHERE "
            "source_schema = 'app' AND source_table = 'multirange_probe_refusal'"
        ) == [("quarantined", "SchemaEvolutionRefused")]
        assert sandbox.duck_query(
            "SELECT snapshot_state FROM _cdc_flight.table_state WHERE "
            "source_schema = 'app' AND source_table = 'multirange_probe_refusal'"
        ) == [("awaiting_snapshot",)]
        assert sandbox.duck_query(
            "SELECT count(*) FROM information_schema.tables WHERE "
            "table_schema = 'cdc_raw' AND "
            "table_name = 'cdcflight_app_multirange_probe_refusal'"
        ) == [(0,)]
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            conn.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            conn.execute("DROP TABLE IF EXISTS app.multirange_probe_refusal")
