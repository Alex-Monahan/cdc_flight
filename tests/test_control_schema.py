"""Control-schema migration coverage on the local DuckDB destination."""

from __future__ import annotations

import duckdb
import pytest

from cdc_flight.catalog_state import read_known_relations
from cdc_flight.control_schema import ControlSchemaFailed, ensure_control_schema
from cdc_flight.states import UnknownState


def _legacy_source_relations(con) -> None:
    """Create the pre-2.3 table shape that needs the additive migration."""
    con.execute("CREATE SCHEMA _cdc_flight")
    con.execute(
        """
        CREATE TABLE _cdc_flight.source_relations (
            pipeline        VARCHAR NOT NULL,
            source_schema   VARCHAR NOT NULL,
            source_table    VARCHAR NOT NULL,
            relation_oid    BIGINT NOT NULL,
            published       BOOLEAN NOT NULL,
            replica_identity VARCHAR,
            first_seen_at   TIMESTAMPTZ NOT NULL,
            last_seen_at    TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pipeline, source_schema, source_table)
        )
        """
    )
    con.execute(
        """
        INSERT INTO _cdc_flight.source_relations
        VALUES ('p', 'app', 'customers', 42, true, 'd', now(), now())
        """
    )


def test_legacy_source_relations_migration_backfills_and_is_idempotent(tmp_path):
    path = str(tmp_path / "legacy.duckdb")
    con = duckdb.connect(path)
    try:
        _legacy_source_relations(con)
        ensure_control_schema(con)

        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'source_relations'"
            ).fetchall()
        }
        assert {"columns_json", "admission_state"} <= columns
        assert con.execute(
            "SELECT admission_state FROM _cdc_flight.source_relations"
        ).fetchall() == [("external",)]

        # The second call must neither re-ADD nor change the backfilled value.
        ensure_control_schema(con)
        assert con.execute(
            "SELECT admission_state FROM _cdc_flight.source_relations"
        ).fetchall() == [("external",)]

        # The additive column is necessarily nullable on the destination. A NULL is
        # still refused by the owner machine instead of being silently defaulted.
        con.execute("UPDATE _cdc_flight.source_relations SET admission_state = NULL")
        with pytest.raises(UnknownState, match="NULL"):
            read_known_relations(con, "p")
    finally:
        con.close()


def test_control_schema_rolls_back_ddl_when_the_additive_backfill_fails(tmp_path):
    path = str(tmp_path / "atomic.duckdb")
    con = duckdb.connect(path)
    _legacy_source_relations(con)

    class _FailsAdmissionBackfill:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            if "UPDATE _cdc_flight.source_relations SET admission_state" in str(sql):
                raise RuntimeError("backfill interrupted")
            return self._real.execute(sql, *args, **kwargs)

    try:
        with pytest.raises(ControlSchemaFailed, match="backfill"):
            ensure_control_schema(_FailsAdmissionBackfill(con))

        columns = {
            row[0]
            for row in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = '_cdc_flight' AND table_name = 'source_relations'"
            ).fetchall()
        }
        assert "admission_state" not in columns
        assert con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = '_cdc_flight' AND table_name = 'heartbeat'"
        ).fetchone()[0] == 0
    finally:
        con.close()
