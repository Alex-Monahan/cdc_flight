"""Real PostgreSQL admission/proof checks for rubric 2.6.

The Debezium wire scenarios live behind the existing slow/e2e fixtures.  These
tests keep the catalog and PostgreSQL NUL invariants independently executable in
the slow lane, using only the project-local port selected by CDC_TEST_PGPORT.
"""

from __future__ import annotations

import os
import time

import psycopg
import pytest

from cdc_flight.catalog import CatalogWatcher
from cdc_flight.catalog_poll import _ensure_toast_policies
from cdc_flight.catalog_state import SourceRelation
from cdc_flight.debezium_props import UNAVAILABLE_VALUE_PLACEHOLDER
from cdc_flight.schema_evolution import SourceColumn
from cdc_flight.typed_types import SourceTypeDescriptor

pytestmark = [pytest.mark.slow, pytest.mark.e2e]


def _dsn():
    return (
        f"host=127.0.0.1 port={os.environ.get('CDC_TEST_PGPORT', '15434')} "
        "dbname=cdc_source user=postgres password=postgres"
    )


def test_postgres_event_before_full_activation_is_fenced_from_current_policy(sandbox):
    """A real pre-FULL bytea event cannot be admitted as an explicit NULL."""
    assert sandbox.source.port == int(os.environ["CDC_TEST_PGPORT"])
    publication = "cdc_flight_pub"
    qualified = "app.p2b_toast_race"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS app.p2b_toast_race")
        con.execute(
            "CREATE TABLE app.p2b_toast_race "
            "(id integer PRIMARY KEY, payload bytea)"
        )
        con.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")
        con.execute(
            "INSERT INTO app.p2b_toast_race VALUES (1, decode('00ff','hex'))"
        )
        sample_lsn = int(
            con.execute(
                "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"
            ).fetchone()[0]
        )
        con.execute(
            "UPDATE app.p2b_toast_race SET payload = decode('010203','hex') WHERE id = 1"
        )
        event_lsn = int(
            con.execute(
                "SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint"
            ).fetchone()[0]
        )
        relation_oid = int(
            con.execute(
                "SELECT c.oid FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='app' AND c.relname='p2b_toast_race'"
            ).fetchone()[0]
        )
        relation = SourceRelation(
            schema="app",
            table="p2b_toast_race",
            oid=relation_oid,
            published=True,
            replica_identity="d",
            columns=(
                SourceColumn(
                    attnum=1,
                    name="id",
                    type_oid=23,
                    type_name="integer",
                    descriptor=SourceTypeDescriptor(23, "pg_catalog.int4", "int4"),
                    attstorage="p",
                ),
                SourceColumn(
                    attnum=2,
                    name="payload",
                    type_oid=17,
                    type_name="bytea",
                    descriptor=SourceTypeDescriptor(17, "pg_catalog.bytea", "bytea"),
                    attstorage="x",
                ),
            ),
        )
        watcher = CatalogWatcher(
            dsn=sandbox.source.dsn,
            primary_dsn=sandbox.source.dsn,
            publication=publication,
            schema="app",
            schemas={"app"},
            include={qualified},
            emit_marker=False,
            confirm_polls=1,
        )
        observed = _ensure_toast_policies(
            watcher,
            con,
            {qualified: relation},
            activation_lsn=sample_lsn,
        )
        policy = observed[qualified].toast_policy
        assert policy.full_activation_lsn > event_lsn
        assert policy.accepts_event(event_lsn) is False
        assert policy.accepts_event(policy.full_activation_lsn) is True
        assert con.execute(
            "SELECT payload FROM app.p2b_toast_race WHERE id = 1"
        ).fetchone()[0] == b"\x01\x02\x03"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as con:
        con.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
        con.execute("DROP TABLE IF EXISTS app.p2b_toast_race")


def test_two_connection_identity_downgrade_after_verification_is_not_admitted(sandbox):
    """Reproduce r3's exact verification-to-sample TOCTOU on two PostgreSQL connections."""
    publication = "cdc_flight_pub"
    qualified = "app.p2b_toast_identity_toc"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as setup:
        setup.execute(f"DROP TABLE IF EXISTS {qualified}")
        setup.execute(
            "CREATE TABLE app.p2b_toast_identity_toc "
            "(id integer PRIMARY KEY, payload bytea)"
        )
        setup.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")
        row = setup.execute(
            "SELECT c.oid, c.relfilenode, c.reltype FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='app' AND c.relname='p2b_toast_identity_toc'"
        ).fetchone()
        relation_oid, relfilenode, relation_type_oid = map(int, row)

    residual = SourceTypeDescriptor(
        17,
        "pg_catalog.bytea",
        "bytea",
    )
    relation = SourceRelation(
        schema="app",
        table="p2b_toast_identity_toc",
        oid=relation_oid,
        relfilenode=relfilenode,
        relation_type_oid=relation_type_oid,
        published=True,
        replica_identity="d",
        columns=(
            SourceColumn(
                attnum=1,
                name="id",
                type_oid=23,
                type_name="integer",
                descriptor=SourceTypeDescriptor(23, "pg_catalog.int4", "int4"),
                attstorage="p",
            ),
            SourceColumn(
                attnum=2,
                name="payload",
                type_oid=17,
                type_name="bytea",
                descriptor=residual,
                attstorage="x",
            ),
        ),
    )

    class PausingConnection:
        def __init__(self, inner, racer):
            self.inner = inner
            self.racer = racer
            self.paused = False

        def execute(self, statement, params=None):
            result = self.inner.execute(statement, params)
            if "SELECT relreplident" in statement and not self.paused:
                self.paused = True
                # The verification has returned FULL.  A second connection now
                # downgrades the relation before the first connection samples WAL.
                self.racer.execute("SET lock_timeout = '100ms'")
                self.racer.execute(
                    "ALTER TABLE app.p2b_toast_identity_toc "
                    "REPLICA IDENTITY DEFAULT"
                )
            return result

        def __getattr__(self, name):
            return getattr(self.inner, name)

    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        primary_dsn=sandbox.source.dsn,
        publication=publication,
        schema="app",
        schemas={"app"},
        include={qualified},
        emit_marker=False,
        confirm_polls=1,
    )
    try:
        with (
            psycopg.connect(sandbox.source.dsn, autocommit=True) as write,
            psycopg.connect(sandbox.source.dsn, autocommit=True) as racer,
        ):
            observed = _ensure_toast_policies(
                watcher,
                PausingConnection(write, racer),
                {qualified: relation},
                activation_lsn=1,
            )
            policy = observed[qualified].toast_policy
            assert observed[qualified].replica_identity == "d"
            assert observed[qualified].full_activation_lsn is None
            assert policy.accepts_event(11777429008) is False
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as cleanup:
            cleanup.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            cleanup.execute(f"DROP TABLE IF EXISTS {qualified}")


def test_two_connection_downgrade_after_post_sample_verification_is_not_admitted(sandbox):
    """The r4 pause after verification #2 must fail closed under a held relation lock."""
    publication = "cdc_flight_pub"
    qualified = "app.p2b_toast_identity_toc_r4"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as setup:
        setup.execute(f"DROP TABLE IF EXISTS {qualified}")
        setup.execute(
            "CREATE TABLE app.p2b_toast_identity_toc_r4 "
            "(id integer PRIMARY KEY, payload bytea)"
        )
        setup.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")
        relation_oid, relfilenode, relation_type_oid = map(
            int,
            setup.execute(
                "SELECT c.oid, c.relfilenode, c.reltype FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='app' AND c.relname='p2b_toast_identity_toc_r4'"
            ).fetchone(),
        )

    relation = SourceRelation(
        schema="app",
        table="p2b_toast_identity_toc_r4",
        oid=relation_oid,
        relfilenode=relfilenode,
        relation_type_oid=relation_type_oid,
        published=True,
        replica_identity="d",
        columns=(
            SourceColumn(
                attnum=1,
                name="id",
                type_oid=23,
                type_name="integer",
                descriptor=SourceTypeDescriptor(23, "pg_catalog.int4", "int4"),
                attstorage="p",
            ),
            SourceColumn(
                attnum=2,
                name="payload",
                type_oid=17,
                type_name="bytea",
                descriptor=SourceTypeDescriptor(17, "pg_catalog.bytea", "bytea"),
                attstorage="x",
            ),
        ),
    )

    class PausingAfterSecondVerification:
        def __init__(self, inner, racer):
            self.inner = inner
            self.racer = racer
            self.verify_count = 0
            self.racer_error = None

        def execute(self, statement, params=None):
            result = self.inner.execute(statement, params)
            if "SELECT relreplident" in statement:
                self.verify_count += 1
                if self.verify_count == 2:
                    self.racer.execute("SET lock_timeout = '100ms'")
                    try:
                        self.racer.execute(
                            "ALTER TABLE app.p2b_toast_identity_toc_r4 "
                            "REPLICA IDENTITY DEFAULT"
                        )
                    except psycopg.errors.LockNotAvailable as exc:
                        self.racer_error = exc
                        raise
            return result

        def __getattr__(self, name):
            return getattr(self.inner, name)

    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        primary_dsn=sandbox.source.dsn,
        publication=publication,
        schema="app",
        schemas={"app"},
        include={qualified},
        emit_marker=False,
        confirm_polls=1,
    )
    try:
        with (
            psycopg.connect(sandbox.source.dsn, autocommit=True) as write,
            psycopg.connect(sandbox.source.dsn, autocommit=True) as racer,
        ):
            conn = PausingAfterSecondVerification(write, racer)
            observed = _ensure_toast_policies(
                watcher,
                conn,
                {qualified: relation},
                activation_lsn=1,
            )
            assert conn.verify_count == 2
            assert conn.racer_error is not None
            assert observed[qualified].replica_identity == "d"
            assert observed[qualified].full_activation_lsn is None
            assert observed[qualified].toast_policy.accepts_event(11777429008) is False
            assert write.execute(
                "SELECT relreplident FROM pg_class WHERE oid = %s", [relation_oid]
            ).fetchone()[0] == "d"
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as cleanup:
            cleanup.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            cleanup.execute(f"DROP TABLE IF EXISTS {qualified}")


def test_activation_lock_timeout_refuses_behind_a_streaming_transaction(sandbox):
    """A DML-style relation lock cannot deadlock or stall the catalog poll."""
    publication = "cdc_flight_pub"
    qualified = "app.p2b_toast_identity_lock_timeout"
    with psycopg.connect(sandbox.source.dsn, autocommit=True) as setup:
        setup.execute(f"DROP TABLE IF EXISTS {qualified}")
        setup.execute(
            "CREATE TABLE app.p2b_toast_identity_lock_timeout "
            "(id integer PRIMARY KEY, payload bytea)"
        )
        setup.execute(f"ALTER PUBLICATION {publication} ADD TABLE {qualified}")
        relation_oid, relfilenode, relation_type_oid = map(
            int,
            setup.execute(
                "SELECT c.oid, c.relfilenode, c.reltype FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='app' AND c.relname='p2b_toast_identity_lock_timeout'"
            ).fetchone(),
        )

    residual = SourceTypeDescriptor(17, "pg_catalog.bytea", "bytea")
    relation = SourceRelation(
        schema="app",
        table="p2b_toast_identity_lock_timeout",
        oid=relation_oid,
        relfilenode=relfilenode,
        relation_type_oid=relation_type_oid,
        published=True,
        replica_identity="d",
        columns=(
            SourceColumn(
                1, "id", 23, "integer",
                descriptor=SourceTypeDescriptor(23, "pg_catalog.int4", "int4"),
                attstorage="p",
            ),
            SourceColumn(
                2, "payload", 17, "bytea", descriptor=residual, attstorage="x"
            ),
        ),
    )
    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        primary_dsn=sandbox.source.dsn,
        publication=publication,
        schema="app",
        schemas={"app"},
        include={qualified},
        emit_marker=False,
        confirm_polls=1,
    )
    try:
        # A streaming/DML transaction normally holds ROW EXCLUSIVE.  The poll's
        # ACCESS EXCLUSIVE NOWAIT must refuse immediately and leave the relation on
        # automatic refetch, rather than waiting for that transaction or deadlocking.
        with (
            psycopg.connect(sandbox.source.dsn, autocommit=False) as blocker,
            psycopg.connect(sandbox.source.dsn, autocommit=True) as poll_conn,
        ):
            blocker.execute(
                "LOCK TABLE app.p2b_toast_identity_lock_timeout "
                "IN ROW EXCLUSIVE MODE"
            )
            started = time.monotonic()
            observed = _ensure_toast_policies(
                watcher,
                poll_conn,
                {qualified: relation},
                activation_lsn=1,
            )
            elapsed = time.monotonic() - started
            assert elapsed < 1.0
            assert observed[qualified].replica_identity == "d"
            assert observed[qualified].full_activation_lsn is None
            assert observed[qualified].toast_policy.accepts_event(11777429008) is False
            blocker.rollback()
    finally:
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as cleanup:
            cleanup.execute(f"ALTER PUBLICATION {publication} DROP TABLE {qualified}")
            cleanup.execute(f"DROP TABLE IF EXISTS {qualified}")


def test_postgres_rejects_nul_for_structural_string_types_and_accepts_bytea():
    with psycopg.connect(_dsn(), autocommit=True) as con:
        cases = (
            "text", "varchar", "character(4)", 'pg_catalog."char"',
            "json", "jsonb", "xml",
        )
        for cast in cases:
            with pytest.raises(psycopg.Error):
                con.execute(f"SELECT %s::{cast}", ("prefix\x00suffix",))
        assert con.execute("SELECT decode('00ff','hex')::bytea").fetchone()[0] == b"\x00\xff"


def test_catalog_query_exposes_column_storage_facts():
    with psycopg.connect(_dsn(), autocommit=True) as con:
        rows = con.execute(
            "SELECT a.attname, a.attstorage, format_type(a.atttypid,a.atttypmod) "
            "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='app' AND c.relname='documents' AND a.attnum > 0 "
            "AND NOT a.attisdropped ORDER BY a.attnum"
        ).fetchall()
    assert rows
    assert any(name == "body" and storage != "p" for name, storage, _ in rows)


def test_structural_marker_is_the_configured_debezium_value():
    assert UNAVAILABLE_VALUE_PLACEHOLDER == "hex:00"
