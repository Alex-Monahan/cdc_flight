"""`offsets.dat` codec (ADR 0001 §4.3, §4.5).

Start-up reconciliation can only rebuild the offsets file if we can write
Debezium's format exactly. Kafka's `FileOffsetBackingStore` looks the partition
up by **exact `ByteBuffer`**, so a key that differs by one byte is not "close
enough" - it reads as "no offset", which means a full re-snapshot.

These tests pin the format that was read off a live file rather than assumed:
`ObjectOutputStream.writeObject(HashMap<byte[], byte[]>)` with compact-JSON keys
and values. They need a JVM (JPype) but no Postgres.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cdc_flight import offsets
from cdc_flight.destination import ResumePoint
from cdc_flight.errors import OffsetUnusable

NAMESPACE = "cdc-flight-engine"
PARTITION = {"server": "cdcflight"}
OFFSET = {
    "lsn_proc": 30105392,
    "messageType": "UPDATE",
    "lsn": 30105392,
    "txId": 795,
    "ts_usec": 1785460478558410,
}

#: A file **Debezium 3.6.0.Final wrote**, preserved byte for byte from a live
#: `.cdc_state/offsets.dat` on 2026-07-30. The previous "byte identical to one
#: Debezium wrote" test created both files with our own writer and was therefore
#: tautological (Codex 3); this fixture is the evidence it claimed to be.
DEBEZIUM_FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "support"
    / "fixtures"
    / "offsets_debezium_3.6.dat"
)


def test_key_and_value_encoding_match_the_observed_bytes():
    """These literals came out of a real `.cdc_state/offsets.dat`."""
    assert offsets.encode_key(NAMESPACE, PARTITION) == (
        b'["cdc-flight-engine",{"server":"cdcflight"}]'
    )
    assert offsets.encode_value({"lsn": 1, "txId": 2}) == b'{"lsn":1,"txId":2}'


def test_write_then_read_round_trips(tmp_path):
    path = tmp_path / "offsets.dat"
    entries = {offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(OFFSET)}
    offsets.write(path, entries)
    assert offsets.read(path) == entries

    (partition, offset), = offsets.parse_offsets(offsets.read(path))
    assert partition == PARTITION
    assert offset == OFFSET
    assert offsets.file_lsn(path) == 30105392


def test_a_file_we_wrote_is_byte_identical_to_one_debezium_wrote(tmp_path):
    """The strongest form of "compatible": read a file **Debezium wrote**, write
    it back with our writer, compare the bytes.

    The fixture is a preserved live `offsets.dat`, so if Debezium ever changes the
    serialisation this fails on the next run rather than silently causing a
    re-snapshot months later. (The earlier version of this test created both files
    with our own writer, which proved only that our writer is deterministic.)
    """
    assert DEBEZIUM_FIXTURE.exists(), f"the Debezium-written fixture is missing: {DEBEZIUM_FIXTURE}"
    entries = offsets.read(DEBEZIUM_FIXTURE)
    assert entries, "the fixture did not decode; the codec or the format changed"
    copy = tmp_path / "roundtrip.dat"
    offsets.write(copy, entries)
    assert copy.read_bytes() == DEBEZIUM_FIXTURE.read_bytes()


def test_the_debezium_fixture_decodes_to_the_expected_typed_offset():
    """Field *types* are load-bearing: `transaction_id` and `messageType` are
    Strings and `lsn`/`txId`/`ts_usec` are Longs, and Debezium casts them on
    start-up (see `envelope._coerce`)."""
    (partition, offset), = offsets.parse_offsets(offsets.read(DEBEZIUM_FIXTURE))
    assert partition == PARTITION
    assert offset == OFFSET
    assert isinstance(offset["messageType"], str)
    assert isinstance(offset["lsn"], int)


def test_offsets_that_share_an_lsn_but_differ_in_progress_do_not_round_trip_equal(tmp_path):
    """Two offsets can agree on `lsn` and still describe different positions.

    `lsn_proc`, `txId` and `transaction_id` all move within a shared commit LSN,
    which is why start-up reconciliation compares the whole typed map rather than
    a scalar (Codex 3).
    """
    a = {"lsn": 30105392, "lsn_proc": 30105392, "txId": 795, "transaction_id": "795:30105392"}
    b = {"lsn": 30105392, "lsn_proc": 30105999, "txId": 795, "transaction_id": "795:30105999"}
    assert offsets.lsn_of(a) == offsets.lsn_of(b)
    assert a != b

    path = tmp_path / "snap.dat"
    snapshot_offset = {"lsn": 42, "snapshot": True, "snapshot_completed": False}
    offsets.write(
        path,
        {offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(snapshot_offset)},
    )
    (_partition, decoded), = offsets.parse_offsets(offsets.read(path))
    assert decoded == snapshot_offset
    assert decoded["snapshot"] is True


def test_a_truncated_file_reads_as_empty_rather_than_raising(tmp_path):
    """ADR §4.5's "corrupt/truncated" row: a crash during Kafka's (non-atomic)
    save must resolve to "rebuild from the table", not to a stack trace."""
    path = tmp_path / "offsets.dat"
    offsets.write(
        path, {offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(OFFSET)}
    )
    raw = path.read_bytes()
    path.write_bytes(raw[: len(raw) // 2])
    assert offsets.read(path) == {}
    assert offsets.file_lsn(path) is None


def test_a_missing_file_reads_as_empty(tmp_path):
    assert offsets.read(tmp_path / "nope.dat") == {}


def test_the_write_is_atomic(tmp_path):
    """Ours renames into place, so the repair cannot reintroduce the corruption
    it exists to repair."""
    path = tmp_path / "offsets.dat"
    entries = {offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(OFFSET)}
    offsets.write(path, entries)
    offsets.write(path, entries)
    assert not (tmp_path / "offsets.dat.tmp").exists()
    assert offsets.read(path) == entries


def _durable_point() -> ResumePoint:
    return ResumePoint(
        partition=dict(PARTITION),
        offset=dict(OFFSET),
        last_lsn=30105392,
        commit_id=7,
    )


def test_replay_intent_is_durable_but_is_not_an_offset_record(tmp_path):
    path = offsets.replay_intent_path(tmp_path)
    point = _durable_point()

    intent = offsets.arm_replay_intent(
        path,
        pipeline="pipeline",
        namespace=NAMESPACE,
        durable_point=point,
    )

    payload = json.loads(path.read_text())
    assert offsets.read_replay_intent(path) == intent
    assert payload["phase"] == "pending"
    assert payload["durable_commit_id"] == point.commit_id
    assert "offset" not in payload
    assert "partition" not in payload
    offsets.validate_replay_intent(
        intent,
        pipeline="pipeline",
        namespace=NAMESPACE,
        durable_point=point,
    )


def test_pending_intent_wins_even_when_canonical_file_agrees(tmp_path):
    point = _durable_point()
    target = tmp_path / "offsets.dat"
    offsets.write(
        target,
        {offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(OFFSET)},
    )
    intent = offsets.arm_replay_intent(
        offsets.replay_intent_path(tmp_path),
        pipeline="pipeline",
        namespace=NAMESPACE,
        durable_point=point,
    )

    assert offsets.replay_install_is_durable(
        intent, target=target, durable_point=point
    ) is False


def test_installing_intent_self_heals_after_atomic_install(tmp_path):
    point = _durable_point()
    intent_path = offsets.replay_intent_path(tmp_path)
    source = tmp_path / offsets.REPLAY_OFFSET_FILE_NAME
    target = tmp_path / "offsets.dat"
    entries = {
        offsets.encode_key(NAMESPACE, PARTITION): offsets.encode_value(OFFSET)
    }
    offsets.write(source, entries)
    intent = offsets.arm_replay_intent(
        intent_path,
        pipeline="pipeline",
        namespace=NAMESPACE,
        durable_point=point,
    )
    fingerprint = offsets.replay_offset_fingerprint(source)
    installing = offsets.mark_replay_installing(
        intent_path,
        intent,
        source_size=fingerprint[0],
        source_sha256=fingerprint[1],
    )
    offsets.install_replay_offset(
        source,
        target,
        expected_fingerprint=fingerprint,
        durable_point=point,
        namespace=NAMESPACE,
    )

    assert offsets.replay_install_is_durable(
        installing, target=target, durable_point=point
    ) is True
    offsets.clear_replay_intent(intent_path)
    assert offsets.read_replay_intent(intent_path) is None


def test_replay_install_missing_or_empty_source_fails_closed(tmp_path):
    with pytest.raises(OffsetUnusable, match="usable replay offset"):
        offsets.install_replay_offset(
            tmp_path / offsets.REPLAY_OFFSET_FILE_NAME,
            tmp_path / "offsets.dat",
        )
    empty = tmp_path / offsets.REPLAY_OFFSET_FILE_NAME
    empty.touch()
    with pytest.raises(OffsetUnusable, match="usable replay offset"):
        offsets.install_replay_offset(empty, tmp_path / "offsets.dat")
