"""Deterministic contract tests for rubric §7.4 logical messages.

These tests deliberately stop at the destination transaction boundary.  The slow
and MotherDuck modules exercise the real connector and cloud destination; this
module proves the byte, routing, source-unit, ledger, and consumer contracts without
claiming a live round trip.
"""

from __future__ import annotations

import base64
import json

import duckdb
import pytest
from support.applier_lab import Lab as ApplierLab
from support.applier_lab import begin as lab_begin
from support.applier_lab import end as lab_end
from support.applier_lab import keyed as lab_keyed

from cdc_flight import event_ledger
from cdc_flight.assembler import (
    UNIT_MESSAGE,
    UNIT_TXN,
    CompleteUnit,
    TransactionAssembler,
)
from cdc_flight.commit_protocol import _unit_has_delivery_data
from cdc_flight.control_schema import ensure_control_schema
from cdc_flight.destination import ensure_dataset
from cdc_flight.envelope import (
    KIND_DATA,
    KIND_MESSAGE,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    PendingRecord,
    decode,
)
from cdc_flight.errors import (
    DestinationIdentityCollision,
    EnvelopeDecodeError,
    SchemaEvolutionRefused,
    TransactionAssemblyError,
)
from cdc_flight.logical_messages import (
    LOGICAL_MESSAGE_HEARTBEAT_PREFIX,
    MessagePrefixPolicy,
    message_prefix_include_list,
    read_logical_messages,
)
from cdc_flight.planner import GroupPlan
from cdc_flight.policy import AcknowledgementHandle
from cdc_flight.spill import SpillBuffer, StagedEvent

TOPIC_PREFIX = "cdcflight"
DATASET = "cdc_raw"
PIPELINE = "p74-contract"
POLICY = MessagePrefixPolicy(
    application_patterns=("app_.*",), marker_prefixes=("cdcf",)
)


class FakeEvent:
    """The small ChangeEvent surface consumed by ``envelope.decode``."""

    def __init__(self, topic: str, value: dict, *, offset: int = 500):
        self._topic = topic
        self._value = json.dumps(value)
        self._offset = {"lsn": offset}

    def destination(self):
        return self._topic

    def value(self):
        return self._value

    def key(self):
        return None

    def sourceRecord(self):
        offset = self._offset

        class _Record:
            def sourcePartition(inner):
                return {"server": TOPIC_PREFIX}

            def sourceOffset(inner):
                return offset

        return _Record()


def _decoded_message(
    content: bytes | str,
    *,
    prefix: str = "app_contract",
    offset: int = 100,
    transactional: bool = False,
):
    value = {
        "op": "m",
        "message": {
            "prefix": prefix,
            "content": (
                content
                if isinstance(content, str)
                else base64.b64encode(content).decode("ascii")
            ),
        },
        "source": {
            "schema": "",
            "table": "",
            "lsn": offset,
            "sequence": f"seq-{offset}",
            "ts_ms": 1234,
        },
        "ts_ms": 1235,
    }
    if transactional:
        value["source"]["txId"] = 77
        value["transaction"] = {"id": f"77:{offset}", "total_order": 1}
    return decode(
        FakeEvent(f"{TOPIC_PREFIX}.message", value, offset=offset),
        topic_prefix=TOPIC_PREFIX,
    )


def _begin(txn: str, lsn: int = 10) -> PendingRecord:
    return PendingRecord(
        raw=object(),
        kind=KIND_TXN_BEGIN,
        topic=f"{TOPIC_PREFIX}.transaction",
        nbytes=1,
        txn_id=txn,
        lsn=lsn,
        txn_status="BEGIN",
    )


def _end(txn: str, count: int, lsn: int = 30, per_table=None) -> PendingRecord:
    return PendingRecord(
        raw=object(),
        kind=KIND_TXN_END,
        topic=f"{TOPIC_PREFIX}.transaction",
        nbytes=1,
        txn_id=txn,
        lsn=lsn,
        txn_status="END",
        txn_event_count=count,
        txn_data_collections=dict(per_table or {}),
    )


def _message(
    txn: str | None,
    order: int | None,
    lsn: int,
    content: bytes,
    *,
    prefix: str = "app_contract",
    transactional: bool | None = None,
) -> PendingRecord:
    return PendingRecord(
        raw=object(),
        kind=KIND_MESSAGE,
        topic=f"{TOPIC_PREFIX}.message",
        nbytes=len(content),
        op="m",
        message_prefix=prefix,
        message_content=content,
        message_transactional=transactional,
        schema=None,
        table=None,
        lsn=lsn,
        txn_id=txn,
        total_order=order,
        source_sequence=f"seq-{lsn}",
        source_ts_ms=1234,
        event_ts_ms=1235,
    )


def _data(txn: str, order: int, lsn: int) -> PendingRecord:
    return PendingRecord(
        raw=object(),
        kind=KIND_DATA,
        topic=f"{TOPIC_PREFIX}.app.customers",
        nbytes=1,
        op="c",
        schema="app",
        table="customers",
        lsn=lsn,
        txn_id=txn,
        total_order=order,
        after={"id": order},
    )


def _plan_connection():
    con = duckdb.connect(":memory:")
    ensure_dataset(con, DATASET)
    ensure_control_schema(con)
    return con


class _Registry:
    dataset = DATASET

    def get(self, _target):
        return None


class _Snapshots:
    def states(self):
        return []

    def target_table(self, schema, table):
        return f"{DATASET}.{schema}_{table}"


class _Spill:
    def load(self, **_kwargs):
        return []

    def clear(self, _commit_id):
        return None


def _apply_message_plan(con, messages: list[PendingRecord], commit_id: int):
    plan = GroupPlan(
        con,
        commit_id=commit_id,
        registry_of=lambda: _Registry(),
        snapshots=_Snapshots(),
        spill=_Spill(),
        truncate_mode="replicate",
        created_in_txn=set(),
        pipeline=PIPELINE,
        control_schema=None,
        source_cluster_id="cluster-contract",
        source_timeline=1,
        strict_event_identity=True,
        message_prefix_policy=POLICY,
    )
    for message in messages:
        plan.add_unit(
            CompleteUnit(
                kind=UNIT_MESSAGE,
                events=[message],
                records=[message],
                last_lsn=message.lsn or 0,
                commit_lsn=message.lsn,
                nbytes=message.nbytes,
                delivery_events=0,
            )
        )
    stats = plan.write()
    return stats


def test_decode_is_strict_and_preserves_empty_and_non_utf8_bytes():
    non_utf8 = _decoded_message(b"\x00\xff\x80")
    empty = _decoded_message(b"", offset=101)

    assert non_utf8.message_content == b"\x00\xff\x80"
    assert type(non_utf8.message_content) is bytes
    assert empty.message_content == b""
    assert non_utf8.message_prefix == "app_contract"
    assert non_utf8.lsn == 100
    assert non_utf8.source_sequence == "seq-100"
    assert non_utf8.source_ts_ms == 1234
    assert non_utf8.event_ts_ms == 1235
    assert non_utf8.message_transactional is False

    with pytest.raises(EnvelopeDecodeError, match="invalid base64"):
        _decoded_message("not-base64")
    with pytest.raises(EnvelopeDecodeError, match="non-string base64"):
        decode(
            FakeEvent(
                f"{TOPIC_PREFIX}.message",
                {
                    "op": "m",
                    "message": {"prefix": "app_contract", "content": 7},
                    "source": {"lsn": 102},
                },
            ),
            topic_prefix=TOPIC_PREFIX,
        )


def test_capture_policy_includes_internal_routes_but_excludes_them_from_consumer():
    include = message_prefix_include_list(
        ("app_.*",), marker_prefixes=("cdcf",),
    )
    assert "message.prefix.include.list" not in include
    assert "app_.*" in include
    assert "cdc_flight_heartbeat" in include
    assert "^cdcf(?:_|$).*" in include
    assert POLICY.classify("app_contract") == "application"
    assert POLICY.classify("cdcf_completion_watermark") == "internal"
    assert POLICY.classify(LOGICAL_MESSAGE_HEARTBEAT_PREFIX) == "internal"
    assert POLICY.classify("other_namespace") == "rejected"


def test_transactional_message_is_a_delivery_event_inside_the_whole_source_unit():
    message = _message("77", 2, 20, b"payload", transactional=True)
    assembler = TransactionAssembler(keep_all_records=True)

    assert assembler.feed(_begin("77")) == []
    assert assembler.feed(_data("77", 1, 19)) == []
    assert assembler.feed(message) == []
    units = assembler.feed(_end("77", 2, lsn=21, per_table={"app.customers": 1}))

    assert len(units) == 1
    unit = units[0]
    assert unit.kind == UNIT_TXN
    assert unit.event_count == 2
    assert unit.commit_lsn == 21
    assert unit.delivery_events == 1
    assert [event.kind for event in unit.events] == [KIND_DATA, KIND_MESSAGE]
    assert unit.events[-1].message_content == b"payload"
    assert _unit_has_delivery_data(unit)


def test_non_transactional_message_is_its_own_unit_and_never_refreshes_liveness():
    message = _message(None, None, 40, b"", transactional=False)
    units = TransactionAssembler().feed(message)

    assert len(units) == 1
    unit = units[0]
    assert unit.kind == UNIT_MESSAGE
    assert unit.txn_id is None
    assert unit.event_count == 1
    assert unit.delivery_events == 0
    assert not _unit_has_delivery_data(unit)
    assert not message.is_data
    assert not message.is_delivery_data

    assembler = TransactionAssembler()
    assert assembler.feed(_begin("78")) == []
    with pytest.raises(TransactionAssemblyError, match="non-transactional"):
        assembler.feed(_message(None, None, 41, b"adjacent", transactional=False))


def test_message_counts_and_ordinals_are_part_of_end_proof():
    assembler = TransactionAssembler()
    assembler.feed(_begin("79"))
    assembler.feed(_message("79", 2, 50, b"gap", transactional=True))
    with pytest.raises(TransactionAssemblyError, match="ordinal"):
        assembler.feed(_end("79", 1, lsn=51))

    assembler = TransactionAssembler()
    assembler.feed(_begin("80"))
    assembler.feed(_message("80", 1, 60, b"one", transactional=True))
    with pytest.raises(TransactionAssemblyError, match="END declares 2 events"):
        assembler.feed(_end("80", 2, lsn=61))


def test_consumer_materialization_is_exact_and_replay_is_a_noop_with_collision_guard():
    con = _plan_connection()
    try:
        first = _message(None, None, 100, b"\x00\xff", transactional=False)
        second = _message(None, None, 101, b"", transactional=False)
        internal = _message(
            "900", 1, 102, b"", prefix=LOGICAL_MESSAGE_HEARTBEAT_PREFIX,
            transactional=True,
        )
        _apply_message_plan(con, [first, second, internal], 1)

        rows = read_logical_messages(con, dataset=DATASET, pipeline=PIPELINE)
        assert [row["content"] for row in rows] == [b"\x00\xff", b""]
        assert all(type(row["content"]) is bytes for row in rows)
        assert all(row["prefix"] == "app_contract" for row in rows)
        assert all(row["is_transactional"] is False for row in rows)
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.logical_message_audit"
        ).fetchone()[0] == 3
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.event_ledger "
            "WHERE target_table = ?",
            [f"{DATASET}.cdcflight_logical_messages"],
        ).fetchone()[0] == 3
        assert con.execute(
            "SELECT status FROM _cdc_flight.logical_message_audit "
            "WHERE prefix = ?",
            [LOGICAL_MESSAGE_HEARTBEAT_PREFIX],
        ).fetchone()[0] == "internal"

        _apply_message_plan(con, [first, second, internal], 2)
        rows_after_replay = read_logical_messages(
            con, dataset=DATASET, pipeline=PIPELINE
        )
        assert [row["content"] for row in rows_after_replay] == [b"\x00\xff", b""]
        assert con.execute(
            "SELECT count(*) FROM _cdc_flight.logical_message_audit"
        ).fetchone()[0] == 3
        assert set(
            con.execute(
                "SELECT status FROM _cdc_flight.logical_message_audit"
            ).fetchall()
        ) == {("replayed",)}

        collision = _message(None, None, 100, b"different", transactional=False)
        with pytest.raises(DestinationIdentityCollision, match="collision"):
            con.execute("BEGIN")
            try:
                _apply_message_plan(con, [collision], 3)
            finally:
                con.execute("ROLLBACK")
    finally:
        con.close()


def test_message_only_applier_commit_excludes_ordinary_data_accounting(tmp_path):
    """A durable message is not a row event in the legacy data counters."""
    box = ApplierLab(
        tmp_path / "message-accounting.duckdb",
        pipeline="p74-accounting",
        ack_every_record=True,
    )
    try:
        message = _message(
            "tx-only", 1, 1020, b"", prefix="app_accounting", transactional=True
        )
        message.source_cluster_id = "cluster-accounting"
        message.source_timeline = 1
        box.run(
            [
                lab_begin("tx-only", 1010),
                message,
                lab_end("tx-only", 1, 1030),
            ]
        )

        assert box.applier.applied_events == 0
        assert box.applier.data_commit_groups == 0
        assert box.applier.snapshot_counts() == {}
        assert box.q(
            "SELECT event_count, tables_touched FROM _cdc_flight.commit_log"
        ) == [(0, [])]
        assert box.q(
            "SELECT content FROM cdc_raw.cdcflight_logical_messages"
        ) == [(b"",)]
        # BEGIN, message, and END are all acknowledgeable in this conservative
        # harness mode. The separate spill-boundary test covers the opaque handle
        # required before a message can be staged.
        assert box.committer.marked == 3
    finally:
        box.close()


def test_message_plus_row_counts_only_the_ordinary_row(tmp_path):
    """A mixed source transaction keeps message and data counters separate."""
    box = ApplierLab(
        tmp_path / "message-plus-row-accounting.duckdb",
        pipeline="p74-mixed-accounting",
    )
    try:
        message = _message(
            "mixed", 2, 3020, b"message", prefix="app_accounting", transactional=True
        )
        message.source_cluster_id = "cluster-mixed"
        message.source_timeline = 1
        box.run(
            [
                lab_begin("mixed", 3010),
                lab_keyed("mixed", 1, 3015, 1, "ordinary"),
                message,
                lab_end("mixed", 2, 3030, {"app.customers": 1}),
            ]
        )

        assert box.applier.applied_events == 1
        assert box.applier.data_commit_groups == 1
        assert box.applier.snapshot_counts() == {"cdcflight_app_customers": 1}
        assert box.q(
            "SELECT event_count, tables_touched FROM _cdc_flight.commit_log"
        ) == [(1, ["cdcflight_app_customers"])]
    finally:
        box.close()


def test_transactional_message_spills_as_bytes_and_is_acknowledged(tmp_path):
    """A message may cross spill without losing its bytes or ack token."""
    box = ApplierLab(
        tmp_path / "message-spill.duckdb",
        pipeline="p74-message-spill",
        unit_spill_events=1,
        ack_every_record=True,
    )
    try:
        content = b"\x00\xff\x80"
        message = _message(
            "spill-tx", 1, 2020, content,
            prefix="app_spill", transactional=True,
        )
        message.source_cluster_id = "cluster-spill"
        message.source_timeline = 1

        box.feed(
            [
                lab_begin("spill-tx", 2010),
                message,
                lab_end("spill-tx", 1, 2030),
            ]
        )

        assert box.applier.spilled_events == 1
        assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 1
        assert isinstance(message.raw, AcknowledgementHandle)
        assert box.q(
            "SELECT message_content FROM _cdc_flight.spill_events"
        ) == [(content,)]

        box.commit()

        row = box.q(
            "SELECT content, prefix, is_transactional, txn_id, total_order, commit_lsn "
            "FROM cdc_raw.cdcflight_logical_messages"
        )
        assert row == [(content, "app_spill", True, "spill-tx", 1, 2030)]
        assert type(row[0][0]) is bytes
        assert box.applier.applied_events == 0
        assert box.applier.data_commit_groups == 0
        assert box.committer.marked == 3
        # The handle consumed by the post-COMMIT acknowledgement no longer owns the
        # connector token; calling consume again is therefore harmless and empty.
        assert message.raw.consume() is None
    finally:
        box.close()


def test_spill_boundary_refuses_a_sanitized_message_with_raw_mapping(tmp_path):
    """§8.3: sanitized metadata cannot bless a raw connector object."""
    box = ApplierLab(tmp_path / "message-raw-refusal.duckdb")
    try:
        message = _message(None, None, 2040, b"secret", transactional=False)
        message.raw = {"decoded": "source"}
        message.sanitized = True
        message.policy_digest = box.applier.policy_gate.policy.digest
        assert not isinstance(message.raw, AcknowledgementHandle)

        spill = SpillBuffer(
            box.con,
            policy_gate=box.applier.policy_gate,
            require_sanitized=True,
        )
        with pytest.raises(SchemaEvolutionRefused, match="decoded source mapping"):
            spill.stage(
                commit_id=1,
                unit_seq=1,
                prepared=[
                    StagedEvent(
                        event=message,
                        event_id="message-raw-refusal",
                        target="cdc_raw.cdcflight_logical_messages",
                        seq=1,
                    )
                ],
            )
        assert box.scalar("SELECT count(*) FROM _cdc_flight.spill_events") == 0
    finally:
        box.close()


def test_message_identity_is_source_based_and_digest_is_byte_sensitive():
    first = _message(None, None, 200, b"\x00", transactional=False)
    same_source = _message(None, None, 200, b"\x01", transactional=False)
    one = event_ledger.message_identity_for(
        first,
        source_cluster_id="cluster-contract",
        source_timeline=1,
        require_strong=True,
    )
    two = event_ledger.message_identity_for(
        same_source,
        source_cluster_id="cluster-contract",
        source_timeline=1,
        require_strong=True,
    )
    assert one.event_id == two.event_id
    assert one.payload_digest != two.payload_digest

    replay = _message(None, None, 200, b"\x00", transactional=False)
    replay.event_ts_ms = 987654321
    replay_identity = event_ledger.message_identity_for(
        replay,
        source_cluster_id="cluster-contract",
        source_timeline=1,
        require_strong=True,
    )
    assert replay_identity.event_id == one.event_id
    assert replay_identity.payload_digest == one.payload_digest

    with pytest.raises(DestinationIdentityCollision, match="missing stable"):
        event_ledger.message_identity_for(
            _message("tx", 1, 201, b"x", transactional=True),
            source_timeline=1,
        )
