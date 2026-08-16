"""Fast unit tests for `envelope.decode` — no JVM, no Postgres.

`decode` is the first place a malformed or unexpected payload can turn into a
*wrong* record rather than a loud failure, and one of those fail-open edges was
load-bearing: any payload on `<prefix>.transaction` that was not a `BEGIN`
became a `KIND_TXN_END` with `event_count = None`, which then terminated the open
transaction without any completeness check at all (Opus M-1).
"""

from __future__ import annotations

import json

import pytest

from cdc_flight.envelope import (
    KIND_DATA,
    KIND_MESSAGE,
    KIND_SNAPSHOT,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    decode,
)


def test_schema_enabled_envelope_and_key_retain_connect_descriptors():
    from support.typed_events import schema_enabled_event, schema_enabled_key

    record = FakeEvent(
        topic="cdc.app.typed_rows",
        value=schema_enabled_event(value="__debezium_unavailable_value"),
        key=schema_enabled_key(),
    )
    decoded = decode(record, topic_prefix="cdc")

    assert decoded.after == {"id": 1, "payload": "__debezium_unavailable_value"}
    assert decoded.after_descriptors["id"].kind == "int4"
    assert decoded.after_descriptors["payload"].kind == "text"
    assert decoded.key_descriptors["id"].kind == "int4"
    # The 2.6 marker identity gate is deliberately not inferred here: an ordinary
    # source string equal to the configured token remains a normal VALUE.
    assert decoded.typed_after.field("payload").state.value == "value"
from cdc_flight.errors import EnvelopeDecodeError

TOPIC_PREFIX = "cdcflight"


class FakeEvent:
    """The three `ChangeEvent` methods `decode` uses."""

    def __init__(self, topic: str, value, key=None, offset: dict | None = None):
        self._topic = topic
        self._value = value if value is None or isinstance(value, str) else json.dumps(value)
        self._key = key if key is None or isinstance(key, str) else json.dumps(key)
        self._offset = offset or {"lsn": 500}

    def destination(self):
        return self._topic

    def value(self):
        return self._value

    def key(self):
        return self._key

    def sourceRecord(self):
        offset = self._offset

        class _Record:
            def sourcePartition(inner):
                return {"server": TOPIC_PREFIX}

            def sourceOffset(inner):
                return offset

        return _Record()


def _decode(event: FakeEvent):
    return decode(event, topic_prefix=TOPIC_PREFIX)


def test_a_begin_marker_decodes_as_a_begin():
    rec = _decode(FakeEvent(f"{TOPIC_PREFIX}.transaction", {"status": "BEGIN", "id": "77:1"}))
    assert rec.kind == KIND_TXN_BEGIN
    assert rec.txn_id == "77"


def test_an_end_marker_carries_its_counts():
    rec = _decode(
        FakeEvent(
            f"{TOPIC_PREFIX}.transaction",
            {
                "status": "END",
                "id": "77:9",
                "event_count": 3,
                "data_collections": [{"data_collection": "app.customers", "event_count": 3}],
            },
        )
    )
    assert rec.kind == KIND_TXN_END
    assert rec.txn_event_count == 3
    assert rec.txn_data_collections == {"app.customers": 3}


def test_an_unrecognised_transaction_status_is_fatal_not_an_end():
    """`kind = BEGIN if status == "BEGIN" else END` failed open in the one
    direction that skips the boundary check."""
    with pytest.raises(EnvelopeDecodeError, match="status"):
        _decode(FakeEvent(f"{TOPIC_PREFIX}.transaction", {"id": "77:9"}))
    with pytest.raises(EnvelopeDecodeError, match="status"):
        _decode(FakeEvent(f"{TOPIC_PREFIX}.transaction", {"status": "SOMETHING", "id": "77:9"}))


def test_a_data_event_prefers_the_stable_source_tx_id():
    """ADR §15/A1: the envelope's `transaction.id` is `<txId>:<lsn>` and changes
    per event; `source.txId` is the stable identifier."""
    rec = _decode(
        FakeEvent(
            f"{TOPIC_PREFIX}.app.customers",
            {
                "op": "c",
                "after": {"id": 1},
                "source": {"schema": "app", "table": "customers", "lsn": 900, "txId": 77},
                "transaction": {"id": "77:900", "total_order": 2},
            },
            key={"id": 1},
        )
    )
    assert rec.kind == KIND_DATA
    assert rec.txn_id == "77"

    assert rec.total_order == 2
    assert rec.lsn == 900
    assert rec.key == {"id": 1}


def test_a_logical_message_retains_the_nested_marker_prefix():
    rec = _decode(
        FakeEvent(
            f"{TOPIC_PREFIX}.message",
            {
                "op": "m",
                "source": {"schema": "", "table": "", "txId": 78, "lsn": 901},
                "transaction": {"id": "78:901", "total_order": 1},
                "message": {
                    "prefix": "cdcf_completion_watermark",
                    "content": "payload",
                },
            },
        )
    )
    assert rec.kind == KIND_MESSAGE
    assert rec.message_prefix == "cdcf_completion_watermark"


def test_a_snapshot_record_never_carries_transaction_metadata():
    rec = _decode(
        FakeEvent(
            f"{TOPIC_PREFIX}.app.customers",
            {
                "op": "r",
                "after": {"id": 1},
                "source": {
                    "schema": "app",
                    "table": "customers",
                    "lsn": 900,
                    "txId": 77,
                    "snapshot": "true",
                },
            },
            key={"id": 1},
        )
    )
    assert rec.kind == KIND_SNAPSHOT
    assert rec.txn_id is None
    assert rec.total_order is None


def test_a_malformed_payload_is_a_decode_error_not_a_data_event():
    with pytest.raises(EnvelopeDecodeError):
        _decode(FakeEvent(f"{TOPIC_PREFIX}.app.customers", "{not json"))
