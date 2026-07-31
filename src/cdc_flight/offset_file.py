"""Read and write Debezium's `offsets.dat` (ADR 0001 §4.3, §4.5).

`offsets.dat` is **never a source of truth** (ADR §4.5). It is a scratch
serialisation buffer that Debezium requires on disk, and start-up reconciliation
rebuilds it from `_cdc_flight.debezium_offsets`. To rebuild it we need its
format, which was read off the file rather than guessed:

```
ObjectOutputStream.writeObject(HashMap<byte[], byte[]>)
  key   = ["<engine name>",{"server":"<topic.prefix>"}]      (compact JSON)
  value = {"lsn":30105392,"txId":795,"lsn_proc":...,...}     (compact JSON)
```

(`org.apache.kafka.connect.storage.FileOffsetBackingStore.save()`; the key is
`OffsetStorageWriter`'s `[namespace, partition]` pair rendered by a
`JsonConverter` with `schemas.enable=false`.) Verified empirically against a
live `.cdc_state/offsets.dat` on 2026-07-30 - see `tests/test_offset_file.py`,
which round-trips a real file.

Byte-exactness matters for the **key** and not for the value: Kafka looks the
partition up by exact `ByteBuffer`, but the value is parsed back through the
same converter. Compact separators reproduce the key exactly.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger("cdc_flight.offset_file")

_SEPARATORS = (",", ":")


def encode_key(namespace: str, partition: dict[str, Any]) -> bytes:
    return json.dumps([namespace, partition], separators=_SEPARATORS).encode("utf-8")


def encode_value(offset: dict[str, Any]) -> bytes:
    return json.dumps(offset, separators=_SEPARATORS).encode("utf-8")


def _jvm():
    import jpype
    import pydbzengine._jvm  # noqa: F401  - importing this is what starts the JVM

    return jpype


def read(path: Path | str) -> dict[bytes, bytes]:
    """`{key_bytes: value_bytes}`; `{}` if the file is missing, empty or corrupt."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return {}
    jpype = _jvm()
    FileInputStream = jpype.JClass("java.io.FileInputStream")
    ObjectInputStream = jpype.JClass("java.io.ObjectInputStream")
    stream = None
    try:
        stream = ObjectInputStream(FileInputStream(str(path)))
        obj = stream.readObject()
        out: dict[bytes, bytes] = {}
        for entry in obj.entrySet():
            key = entry.getKey()
            value = entry.getValue()
            out[bytes(key)] = b"" if value is None else bytes(value)
        return out
    except Exception as exc:
        # A crash during `FileOffsetBackingStore.save()` leaves a truncated file;
        # ADR §4.5 resolves that to "rebuild from the table", not to a failure.
        log.warning("offsets file %s is unreadable (%s); treating it as corrupt", path, exc)
        return {}
    finally:
        if stream is not None:
            with contextlib.suppress(Exception):  # pragma: no cover
                stream.close()


def write(path: Path | str, entries: dict[bytes, bytes]) -> None:
    """Write `entries` in `FileOffsetBackingStore`'s format, atomically.

    Kafka's own writer is *not* atomic, which is why ADR §4.5 has a
    "corrupt/truncated" row. Ours writes to a sibling temp file, `fsync`s it and
    the directory, then renames - so the failure mode it repairs cannot be
    reintroduced by the repair itself, and the guarantee holds against machine
    death and not only against process death (Opus MINOR-4).

    The file is still never a source of truth (ADR §4.5): if the rename is lost
    entirely, start-up reconciliation rebuilds it from the destination.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    jpype = _jvm()
    HashMap = jpype.JClass("java.util.HashMap")
    FileOutputStream = jpype.JClass("java.io.FileOutputStream")
    ObjectOutputStream = jpype.JClass("java.io.ObjectOutputStream")
    JByteArray = jpype.JArray(jpype.JByte)

    java_map = HashMap()
    for key, value in entries.items():
        java_map.put(JByteArray(_signed(key)), JByteArray(_signed(value)))

    tmp = path.with_suffix(path.suffix + ".tmp")
    stream = ObjectOutputStream(FileOutputStream(str(tmp)))
    try:
        stream.writeObject(java_map)
        stream.flush()
    finally:
        stream.close()
    _fsync(tmp)
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync(target: Path) -> None:
    try:
        fd = os.open(str(target), os.O_RDONLY)
    except OSError:  # pragma: no cover
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - not every filesystem supports it
        log.debug("fsync of %s failed", target, exc_info=True)
    finally:
        os.close(fd)


def _fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:  # pragma: no cover
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - macOS/Linux differ on directory fsync
        log.debug("fsync of directory %s failed", directory, exc_info=True)
    finally:
        os.close(fd)


def _signed(data: bytes) -> list[int]:
    return [b - 256 if b > 127 else b for b in data]


def parse_offsets(entries: dict[bytes, bytes]) -> list[tuple[dict, dict]]:
    """`[(partition, offset)]` decoded from raw entries."""
    out: list[tuple[dict, dict]] = []
    for key, value in entries.items():
        try:
            decoded_key = json.loads(key.decode("utf-8"))
            decoded_value = json.loads(value.decode("utf-8")) if value else {}
        except (ValueError, UnicodeDecodeError):  # pragma: no cover
            continue
        partition = decoded_key[1] if isinstance(decoded_key, list) and len(decoded_key) > 1 else {}
        out.append((partition, decoded_value))
    return out


def lsn_of(offset: dict[str, Any]) -> int | None:
    for key in ("lsn", "lsn_proc", "lsn_commit"):
        value = offset.get(key)
        if isinstance(value, int):
            return value
    return None


def file_lsn(path: Path | str) -> int | None:
    """The highest LSN `offsets.dat` currently claims, or None."""
    best: int | None = None
    for _partition, offset in parse_offsets(read(path)):
        value = lsn_of(offset)
        if value is not None and (best is None or value > best):
            best = value
    return best
