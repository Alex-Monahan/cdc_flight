"""Source catalog reads use the same policy boundary as streamed records."""

from __future__ import annotations

import contextlib
import json

import psycopg
import pytest

from cdc_flight.catalog import CatalogWatcher
from cdc_flight.catalog_support import read_columns, read_event_columns_from_connection
from cdc_flight.envelope import KIND_DATA, PendingRecord
from cdc_flight.policy import PIIPolicy, PolicyGate

pytestmark = pytest.mark.e2e


TABLE = "app.p8_source_read_redaction"
TARGET = "p8_source_read_redaction"
TABLE_PREFIX = r"^app\.p8_source_read_redaction"


def _policy(tmp_path):
    salt = tmp_path / "source-read-salt"
    salt.write_bytes(b"source-read-private-salt")
    salt.chmod(0o600)
    return PIIPolicy.from_manifest(
        [
            {"column_regex": rf"{TABLE_PREFIX}\.id$", "action": "replicate"},
            {"column_regex": rf"{TABLE_PREFIX}\.email$", "action": "hash", "algorithm": "HMAC-SHA-256", "salt_id": "source-v1"},
            {"column_regex": rf"{TABLE_PREFIX}\.notes$", "action": "truncate", "max_chars": 5},
            {"column_regex": rf"{TABLE_PREFIX}\.secret$", "action": "exclude"},
        ],
        unmatched="exclude",
        salt_file=salt,
    )


@pytest.fixture(scope="module")
def source_read_case(sandbox, tmp_path_factory):
    sandbox.reseed()
    sandbox.sql(
        f"CREATE TABLE {TABLE} (id integer PRIMARY KEY, email text, notes text, secret text)"
    )
    sandbox.sql(
        "ALTER PUBLICATION cdc_flight_pub ADD TABLE " + TABLE
    )
    sandbox.sql(
        "INSERT INTO " + TABLE + " VALUES "
        "(1, 'email-sentinel@example.invalid', 'notes-sentinel-value', 'secret-sentinel-value')"
    )
    try:
        yield sandbox, tmp_path_factory.mktemp("p8_source_read")
    finally:
        with contextlib.suppress(Exception):
            sandbox.sql("ALTER PUBLICATION cdc_flight_pub DROP TABLE " + TABLE)
        with contextlib.suppress(Exception):
            sandbox.sql("DROP TABLE IF EXISTS " + TABLE)
        sandbox.reseed()


def test_backfill_and_omitted_value_reads_are_sanitized_before_return(
    source_read_case,
):
    sandbox, tmp_path = source_read_case
    policy = _policy(tmp_path)
    watcher = CatalogWatcher(
        dsn=sandbox.source.dsn,
        publication="cdc_flight_pub",
        schema="app",
        include={TABLE},
        auto_discover=False,
        poll_seconds=0,
    )
    try:
        watcher.poll_quietly()
        relation = watcher.known[TABLE]
        gate = PolicyGate(policy)
        rows = read_columns(
            watcher,
            relation,
            key_columns=("id",),
            value_columns=("email", "notes", "secret"),
            policy_gate=gate,
        )
        assert len(rows) == 1
        # The excluded field is absent, truncation is over the source OUTPUT text,
        # and the hash is a stable VARCHAR rather than the source email.
        assert len(rows[0]) == 3
        assert rows[0][2] == "notes"
        assert "email-sentinel@example.invalid" not in json.dumps(rows)
        assert "secret-sentinel-value" not in json.dumps(rows)

        event = PendingRecord(
            raw=None,
            kind=KIND_DATA,
            topic="cdcflight.app.p8_source_read_redaction",
            nbytes=1,
            op="u",
            schema="app",
            table=TABLE.rsplit(".", 1)[1],
            key={"id": 1},
            after={"id": 1},
        )
        descriptors = {
            str(column.name): column.descriptor for column in relation.columns
        }
        with psycopg.connect(sandbox.source.dsn, autocommit=True) as conn:
            recovered = read_event_columns_from_connection(
                conn,
                event,
                ("email", "notes", "secret"),
                policy_gate=gate,
                descriptors=descriptors,
            )
        assert recovered is not None
        assert set(recovered) == {"email", "notes"}
        assert recovered["notes"] == "notes"
        assert "email-sentinel@example.invalid" not in json.dumps(recovered)
        assert "secret-sentinel-value" not in json.dumps(recovered)
    finally:
        watcher.stop()
