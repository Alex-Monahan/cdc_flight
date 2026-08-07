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

from pathlib import Path

from cdc_flight import offset_file

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
    Path(__file__).resolve().parent
    / "support"
    / "fixtures"
    / "offsets_debezium_3.6.dat"
)


def test_key_and_value_encoding_match_the_observed_bytes():
    """These literals came out of a real `.cdc_state/offsets.dat`."""
    assert offset_file.encode_key(NAMESPACE, PARTITION) == (
        b'["cdc-flight-engine",{"server":"cdcflight"}]'
    )
    assert offset_file.encode_value({"lsn": 1, "txId": 2}) == b'{"lsn":1,"txId":2}'


def test_write_then_read_round_trips(tmp_path):
    path = tmp_path / "offsets.dat"
    entries = {offset_file.encode_key(NAMESPACE, PARTITION): offset_file.encode_value(OFFSET)}
    offset_file.write(path, entries)
    assert offset_file.read(path) == entries

    (partition, offset), = offset_file.parse_offsets(offset_file.read(path))
    assert partition == PARTITION
    assert offset == OFFSET
    assert offset_file.file_lsn(path) == 30105392


def test_a_file_we_wrote_is_byte_identical_to_one_debezium_wrote(tmp_path):
    """The strongest form of "compatible": read a file **Debezium wrote**, write
    it back with our writer, compare the bytes.

    The fixture is a preserved live `offsets.dat`, so if Debezium ever changes the
    serialisation this fails on the next run rather than silently causing a
    re-snapshot months later. (The earlier version of this test created both files
    with our own writer, which proved only that our writer is deterministic.)
    """
    assert DEBEZIUM_FIXTURE.exists(), f"the Debezium-written fixture is missing: {DEBEZIUM_FIXTURE}"
    entries = offset_file.read(DEBEZIUM_FIXTURE)
    assert entries, "the fixture did not decode; the codec or the format changed"
    copy = tmp_path / "roundtrip.dat"
    offset_file.write(copy, entries)
    assert copy.read_bytes() == DEBEZIUM_FIXTURE.read_bytes()


def test_the_debezium_fixture_decodes_to_the_expected_typed_offset():
    """Field *types* are load-bearing: `transaction_id` and `messageType` are
    Strings and `lsn`/`txId`/`ts_usec` are Longs, and Debezium casts them on
    start-up (see `envelope._coerce`)."""
    (partition, offset), = offset_file.parse_offsets(offset_file.read(DEBEZIUM_FIXTURE))
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
    assert offset_file.lsn_of(a) == offset_file.lsn_of(b)
    assert a != b

    path = tmp_path / "snap.dat"
    snapshot_offset = {"lsn": 42, "snapshot": True, "snapshot_completed": False}
    offset_file.write(
        path,
        {offset_file.encode_key(NAMESPACE, PARTITION): offset_file.encode_value(snapshot_offset)},
    )
    (_partition, decoded), = offset_file.parse_offsets(offset_file.read(path))
    assert decoded == snapshot_offset
    assert decoded["snapshot"] is True


def test_a_truncated_file_reads_as_empty_rather_than_raising(tmp_path):
    """ADR §4.5's "corrupt/truncated" row: a crash during Kafka's (non-atomic)
    save must resolve to "rebuild from the table", not to a stack trace."""
    path = tmp_path / "offsets.dat"
    offset_file.write(
        path, {offset_file.encode_key(NAMESPACE, PARTITION): offset_file.encode_value(OFFSET)}
    )
    raw = path.read_bytes()
    path.write_bytes(raw[: len(raw) // 2])
    assert offset_file.read(path) == {}
    assert offset_file.file_lsn(path) is None


def test_a_missing_file_reads_as_empty(tmp_path):
    assert offset_file.read(tmp_path / "nope.dat") == {}


def test_the_write_is_atomic(tmp_path):
    """Ours renames into place, so the repair cannot reintroduce the corruption
    it exists to repair."""
    path = tmp_path / "offsets.dat"
    entries = {offset_file.encode_key(NAMESPACE, PARTITION): offset_file.encode_value(OFFSET)}
    offset_file.write(path, entries)
    offset_file.write(path, entries)
    assert not (tmp_path / "offsets.dat.tmp").exists()
    assert offset_file.read(path) == entries
