"""Offset-file codec, reconciliation, and resume-point recovery (ADR 0001 §4.3-§4.5).

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
live `.cdc_state/offsets.dat` on 2026-07-30 - see `tests/unit/test_offsets.py`,
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .assembler import CompleteUnit
from .destination import ResumePoint, raise_alert, read_offset_blobs
from .envelope import PendingRecord
from .envelope import offsets_of as envelope_offsets
from .errors import ReconciliationRefused, ResumePointDrift
from .machines import RECONCILE_DECISIONS

log = logging.getLogger("cdc_flight.offsets")

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


# --------------------------------------------------------------------------- #
# ADR §4.5 — offsets.dat versus the durable resume point
# --------------------------------------------------------------------------- #
@dataclass
class Reconciliation:
    """What the file turned out to be. `decision` is parsed through the frozen domain.

    Validated in production rather than only in a test (Codex r1 MAJOR-5): the domain
    existed and this class accepted any string, so it froze nothing.
    """

    decision: str
    resume_point: ResumePoint
    file_lsn: int | None = None
    repaired: bool = False
    message: str = ""

    def __post_init__(self) -> None:
        self.decision = RECONCILE_DECISIONS.parse(self.decision)


def reconcile(
    con,
    *,
    pipeline: str,
    namespace: str,
    offset_path: Path,
    accept_orphan: bool = False,
    repair: bool = True,
    dsn: str | None = None,
    slot_name: str | None = None,
) -> Reconciliation:
    """Classify. **This function destroys nothing** (Codex r1 BLOCKER-1).

    `dsn` and `slot_name` are accepted and ignored; they were the parameters the
    orphan-acceptance slot drop needed, and the signature is kept so that a caller
    passing them is not silently mis-wired.
    """
    from .destination import read_resume_point

    row = read_resume_point(con, pipeline, namespace)
    entries = read(offset_path)
    file_present = Path(offset_path).exists() and Path(offset_path).stat().st_size > 0
    file_decoded = bool(entries)
    parsed = parse_offsets(entries)
    file_lsn = None
    if parsed:
        file_lsn = lsn_of(parsed[0][1])

    if row is None:
        if not file_present:
            return Reconciliation("fresh_start", ResumePoint(), None, False,
                                  "no offsets file and no destination row")
        if accept_orphan:
            # CLASSIFY ONLY. This branch used to drop the replication slot and unlink
            # the file, and `pipeline.run()` journalled the recovery *afterwards* -
            # deliberately, and wrongly. A hard exit in that gap left no resume row, no
            # offsets file, no slot and no journal, which the next run reads as an
            # ordinary `fresh_start`: the operator's authorised rebuild is forgotten and
            # a configured non-data `snapshot.mode` then streams onto a destination
            # nobody rebuilt. That is the exact B3/A53 shape the journal exists to
            # prevent, recreated on the one route an operator reaches for when something
            # has already gone wrong (Codex r1 BLOCKER-1).
            #
            # The caller now writes the recovery intent and the full table obligation
            # FIRST, with `recovery.begin()`, and lets the one idempotent
            # `recovery.resume()` ladder remove the file, delete the (already absent)
            # row and drop the slot - each step anchored, each step re-entrant, each
            # step recognisable from durable state alone after a crash.
            return Reconciliation(
                "orphan_accepted_resnapshot", ResumePoint(), file_lsn, False,
                "an operator authorised --accept-orphan-offsets; NOTHING has been "
                "destroyed yet and the recovery journal owns the sequence",
            )
        raise_alert(
            con, pipeline=pipeline, severity="critical", code="orphan_offset_file",
            message=(
                f"{offset_path} exists but _cdc_flight.debezium_offsets has no row for "
                f"pipeline={pipeline!r} namespace={namespace!r}"
            ),
            context={"file_lsn": file_lsn},
        )
        raise ReconciliationRefused(
            f"REFUSING TO START: {offset_path} exists (lsn={file_lsn}) but there is no "
            f"_cdc_flight.debezium_offsets row for pipeline={pipeline!r}. The file may be "
            "arbitrarily ahead of anything durable in the destination, so trusting it is "
            "silent data loss (ADR 0001 §4.5). Point at the right destination database, "
            "or pass --accept-orphan-offsets to delete the file and force a re-snapshot."
        )

    # A destination row exists: it is the truth. Everything below only decides
    # whether the *file* needs repairing so Debezium starts where we say.
    if not file_present:
        repaired = _repair(con, pipeline, namespace, offset_path, row, repair)
        # MINOR-3: do not claim a rebuild that did not happen. With no offset map on
        # the durable row there is nothing to rebuild *from*, and an absent file then
        # silently means "start with no offset", i.e. a full re-snapshot.
        decision = "file_missing_rebuilt" if repaired else (
            "file_missing_no_durable_offset" if not row.offset else "file_missing_repair_disabled"
        )
        return Reconciliation(
            decision, row, None, repaired,
            "offsets file rebuilt from the destination" if repaired
            else "offsets file is absent and was NOT rebuilt; Debezium starts with no offset",
        )
    if not file_decoded:
        repaired = _repair(con, pipeline, namespace, offset_path, row, repair)
        return Reconciliation("file_corrupt_rebuilt", row, None, repaired,
                              "offsets file was unreadable and was rebuilt")

    if file_lsn is not None and row.last_lsn and file_lsn > row.last_lsn:
        raise_alert(
            con, pipeline=pipeline, severity="warning", code="offset_file_ahead",
            message=(
                f"offsets.dat claims lsn {file_lsn}, ahead of the durable destination "
                f"offset {row.last_lsn}; the extra offset was never durable"
            ),
        )
        repaired = _repair(con, pipeline, namespace, offset_path, row, repair)
        return Reconciliation("file_ahead_rebuilt", row, file_lsn, repaired,
                              "offsets file was ahead of the destination")
    if file_lsn is not None and row.last_lsn and file_lsn < row.last_lsn:
        repaired = _repair(con, pipeline, namespace, offset_path, row, repair)
        return Reconciliation("file_behind_rebuilt", row, file_lsn, repaired,
                              "offsets file lagged the destination")

    difference = _offset_map_difference(entries, namespace, row)
    if difference is not None:
        repaired = _repair(con, pipeline, namespace, offset_path, row, repair)
        return Reconciliation("file_offset_mismatch_rebuilt", row, file_lsn, repaired,
                              f"offsets file disagrees with the destination: {difference}")
    return Reconciliation("resume", row, file_lsn, False, "offsets file agrees")


def _offset_map_difference(
    entries: dict[bytes, bytes], namespace: str, row: ResumePoint
) -> str | None:
    """Compare the file against the destination's **whole typed offset map**.

    Not a scalar LSN (Codex 3). `lsn_of()` returns the first of
    `("lsn", "lsn_proc", "lsn_commit")` that is present, and several events share
    one commit LSN, so a file at `{lsn: 100, lsn_proc: 999}` compared equal to a
    durable `{lsn: 100, lsn_proc: 1}` and reconciliation said "resume" while the
    file was genuinely ahead within that LSN. Also checks the *key*: Kafka looks the
    partition up by exact `ByteBuffer`, so a file carrying somebody else's
    namespace/partition is not our resume point at all, and only `parsed[0]` was
    ever consulted, so a second entry was invisible.

    Returns a human-readable difference, or None when the file agrees exactly.
    """
    if not row.offset:
        # Nothing canonical to compare against; `_repair` declines too.
        return None
    expected_key = encode_key(namespace, row.partition or {})
    if len(entries) != 1:
        return f"{len(entries)} entries, expected exactly 1 for {expected_key!r}"
    (actual_key, actual_value), = entries.items()
    if actual_key != expected_key:
        return f"key {actual_key!r}, expected {expected_key!r}"
    decoded = parse_offsets({actual_key: actual_value})
    if not decoded:
        return "the offset value did not decode"
    _partition, offset = decoded[0]
    if offset != row.offset:
        keys = sorted(set(offset) | set(row.offset))
        deltas = [
            f"{k}: file={offset.get(k)!r} durable={row.offset.get(k)!r}"
            for k in keys
            if offset.get(k) != row.offset.get(k)
        ]
        return "; ".join(deltas)
    return None


def _repair(con, pipeline, namespace, offset_path: Path, row: ResumePoint, repair: bool) -> bool:
    if not repair:
        log.warning(
            "offsets file repair disabled (CDC_OFFSET_FILE_REPAIR=0): resuming from "
            "whatever the file says and relying on the applier's fence"
        )
        return False
    if not row.offset:
        log.warning("destination row carries no Debezium offset map; leaving the file alone")
        return False
    _blob, key_blob = read_offset_blobs(con, pipeline, namespace)
    key = key_blob or encode_key(namespace, row.partition or {})
    write(offset_path, {key: encode_value(row.offset)})
    log.info("rebuilt %s from the destination (lsn=%s)", offset_path, row.last_lsn)
    return True


# --------------------------------------------------------------------------- #
# The resume point a commit group writes (ADR 0001 §4.3)
# --------------------------------------------------------------------------- #
def point_for(
    group: list[CompleteUnit],
    *,
    previous: ResumePoint,
    commit_id: int,
    snapshot_epoch: int,
) -> ResumePoint:
    """The resume point `group` will make durable inside its own transaction."""
    terminal: PendingRecord | None = None
    for unit in reversed(group):
        if unit.records:
            terminal = unit.records[-1]
            break
    if terminal is not None and terminal.source_offset is None and terminal.raw is not None:
        # Decoded lazily: only this one record's Connect offset is needed, and
        # reading it for all 200 000 of them is what made decode the bottleneck.
        terminal.source_partition, terminal.source_offset = envelope_offsets(terminal.raw)
    last_unit = group[-1]
    last_lsn = max([previous.last_lsn] + [u.last_lsn or 0 for u in group])
    if last_lsn > previous.last_lsn and (terminal is None or not terminal.source_offset):
        raise ResumePointDrift(
            f"commit group would advance last_lsn to {last_lsn} but the terminal "
            "record's Connect offset could not be read, so the resume point would "
            "pair a newer LSN with an older offset map (ADR 0001 §4.3)"
        )
    total_order = None
    for unit in reversed(group):
        if unit.events:
            total_order = unit.events[-1].total_order
            break
    return ResumePoint(
        partition=(terminal.source_partition if terminal else previous.partition) or {},
        offset=(terminal.source_offset if terminal else previous.offset) or {},
        last_lsn=last_lsn,
        last_txn_id=last_unit.txn_id or previous.last_txn_id,
        last_total_order=total_order,
        # The group being written, not the previous one. `ResumePoint.to_json` omits it
        # and `read_resume_point` takes it from its own column, so this was dead but
        # looked live (Opus MINOR-16).
        commit_id=commit_id,
        snapshot_epoch=snapshot_epoch,
    )


def capture_offset_file(path, point: ResumePoint) -> tuple[bytes | None, bytes | None]:
    """`(key blob, entries blob)` of `offsets.dat` after the acknowledgement.

    The bytes belong to the group that has *just* been acknowledged, so they can only
    ride on the *next* group's transaction. They are redundant - `resume_json` is the
    source of truth - but they let start-up rebuild a byte-exact file, and they make
    format drift visible immediately.

    Raises `ResumePointDrift` if the file claims an LSN ahead of what we committed.
    Invariant O says that cannot happen: nothing enters the offset store before COMMIT.
    If it ever does, it is the ADR-rev-2 bug class.
    """
    try:
        entries = read(path)
    except Exception:  # pragma: no cover
        return None, None
    if not entries:
        return None, None
    key = next(iter(entries))
    blob = _serialise_entries(entries)
    file_offsets = parse_offsets(entries)
    if file_offsets:
        _partition, offset = file_offsets[0]
        file_lsn = lsn_of(offset)
        if file_lsn is not None and point.last_lsn and file_lsn > point.last_lsn:
            raise ResumePointDrift(
                f"offsets.dat claims lsn {file_lsn}, ahead of the durable resume "
                f"point {point.last_lsn}. Invariant O is violated (ADR 0001 §4.3)."
            )
    return key, blob


def _serialise_entries(entries: dict[bytes, bytes]) -> bytes:
    return json.dumps(
        {k.decode("utf-8", "replace"): v.decode("utf-8", "replace") for k, v in entries.items()},
        separators=(",", ":"),
    ).encode("utf-8")
