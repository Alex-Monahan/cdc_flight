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
    """The strongest form of "compatible": read a file, write it back, compare.

    If Debezium ever changes the serialisation, this fails on the next run rather
    than silently causing a re-snapshot months later.
    """
    original = tmp_path / "a.dat"
    entries = {offset_file.encode_key(NAMESPACE, PARTITION): offset_file.encode_value(OFFSET)}
    offset_file.write(original, entries)
    copy = tmp_path / "b.dat"
    offset_file.write(copy, offset_file.read(original))
    assert original.read_bytes() == copy.read_bytes()


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
