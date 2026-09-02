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
import hashlib
import json
import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import faults
from .assembler import CompleteUnit
from .destination import ResumePoint, raise_alert, read_offset_blobs
from .envelope import PendingRecord
from .envelope import offsets_of as envelope_offsets
from .errors import OffsetUnusable, ReconciliationRefused, ResumePointDrift
from .machines import RECONCILE_DECISIONS

log = logging.getLogger("cdc_flight.offsets")

_SEPARATORS = (",", ":")
REPLAY_OFFSET_FILE_NAME = ".offsets.replay.dat"
REPLAY_INTENT_FILE_NAME = ".offsets.replay.intent"
REPLAY_INTENT_VERSION = 1
REPLAY_INTENT_KIND = "cdc-flight-replay-intent"
REPLAY_INTENT_PENDING = "pending"
REPLAY_INTENT_INSTALLING = "installing"
REPLAY_INTENT_PHASES = frozenset({REPLAY_INTENT_PENDING, REPLAY_INTENT_INSTALLING})


@dataclass(frozen=True)
class ReplayIntent:
    """Durable *decision* to replay, deliberately without an offset value.

    The sidecar is not another offset store.  It binds a replay to the destination
    row that caused it, using that row's commit id and a digest of its resume JSON,
    but it cannot tell Debezium where to resume.  The source slot and stock
    Debezium's disposable file remain the only replay-position mechanism.
    """

    pipeline: str
    namespace: str
    durable_commit_id: int
    durable_resume_digest: str
    phase: str = REPLAY_INTENT_PENDING
    source_size: int | None = None
    source_sha256: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": REPLAY_INTENT_KIND,
            "version": REPLAY_INTENT_VERSION,
            "pipeline": self.pipeline,
            "namespace": self.namespace,
            "durable_commit_id": self.durable_commit_id,
            "durable_resume_digest": self.durable_resume_digest,
            "phase": self.phase,
            "source_size": self.source_size,
            "source_sha256": self.source_sha256,
        }


def replay_intent_path(state_dir: Path | str) -> Path:
    """Return the durable replay-decision sidecar inside the state directory."""
    return Path(state_dir) / REPLAY_INTENT_FILE_NAME


def _resume_digest(point: ResumePoint) -> str:
    """Hash the durable row without placing its offset contents in the sidecar."""
    return hashlib.sha256(point.to_json().encode("utf-8")).hexdigest()


def _atomic_json_write(path: Path, payload: dict[str, object]) -> None:
    """Publish a small sidecar with the same fsync/rename discipline as offsets.dat."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=_SEPARATORS)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _intent_from_payload(payload: object, path: Path) -> ReplayIntent:
    if not isinstance(payload, dict):
        raise OffsetUnusable(f"replay intent {path} is not a JSON object")
    if payload.get("kind") != REPLAY_INTENT_KIND or payload.get("version") != REPLAY_INTENT_VERSION:
        raise OffsetUnusable(f"replay intent {path} has an unknown kind or version")
    pipeline = payload.get("pipeline")
    namespace = payload.get("namespace")
    commit_id = payload.get("durable_commit_id")
    digest = payload.get("durable_resume_digest")
    phase = payload.get("phase")
    source_size = payload.get("source_size")
    source_sha256 = payload.get("source_sha256")
    if not isinstance(pipeline, str) or not pipeline:
        raise OffsetUnusable(f"replay intent {path} has no pipeline identity")
    if not isinstance(namespace, str) or not namespace:
        raise OffsetUnusable(f"replay intent {path} has no namespace identity")
    if isinstance(commit_id, bool) or not isinstance(commit_id, int) or commit_id < 0:
        raise OffsetUnusable(f"replay intent {path} has an invalid durable commit id")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        raise OffsetUnusable(f"replay intent {path} has an invalid durable-row digest")
    if phase not in REPLAY_INTENT_PHASES:
        raise OffsetUnusable(f"replay intent {path} has an invalid phase {phase!r}")
    if phase == REPLAY_INTENT_PENDING:
        if source_size is not None or source_sha256 is not None:
            raise OffsetUnusable(
                f"pending replay intent {path} must not carry an installed-file fingerprint"
            )
    else:
        if (
            isinstance(source_size, bool)
            or not isinstance(source_size, int)
            or source_size <= 0
            or not isinstance(source_sha256, str)
            or len(source_sha256) != 64
            or any(char not in "0123456789abcdef" for char in source_sha256)
        ):
            raise OffsetUnusable(
                f"installing replay intent {path} has an invalid source fingerprint"
            )
    return ReplayIntent(
        pipeline=pipeline,
        namespace=namespace,
        durable_commit_id=commit_id,
        durable_resume_digest=digest,
        phase=phase,
        source_size=source_size,
        source_sha256=source_sha256,
    )


def read_replay_intent(path: Path | str) -> ReplayIntent | None:
    """Read the durable decision; malformed state fails closed rather than resuming."""
    path = Path(path)
    if not path.exists():
        return None
    if path.stat().st_size <= 0:
        raise OffsetUnusable(f"replay intent {path} is empty")
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise OffsetUnusable(f"replay intent {path} is unreadable: {exc}") from exc
    return _intent_from_payload(payload, path)


def validate_replay_intent(
    intent: ReplayIntent,
    *,
    pipeline: str,
    namespace: str,
    durable_point: ResumePoint | None,
) -> None:
    """Bind a marker to the durable row before allowing it to select replay."""
    if intent.pipeline != pipeline or intent.namespace != namespace:
        raise OffsetUnusable(
            "replay intent belongs to a different pipeline/namespace: "
            f"{intent.pipeline!r}/{intent.namespace!r}"
        )
    if durable_point is None or not durable_point.offset:
        raise OffsetUnusable(
            "replay intent exists but its durable destination resume row is absent or empty"
        )
    if durable_point.commit_id < intent.durable_commit_id:
        raise OffsetUnusable(
            "replay intent names a durable commit newer than the destination resume row"
        )
    if (
        durable_point.commit_id == intent.durable_commit_id
        and _resume_digest(durable_point) != intent.durable_resume_digest
    ):
        raise OffsetUnusable(
            "replay intent does not match the durable destination resume row"
        )


def arm_replay_intent(
    path: Path | str,
    *,
    pipeline: str,
    namespace: str,
    durable_point: ResumePoint,
) -> ReplayIntent:
    """Durably arm replay before a canonical offset repair is attempted."""
    if not durable_point.offset:
        raise OffsetUnusable("cannot arm replay without a typed durable offset map")
    path = Path(path)
    existing = read_replay_intent(path)
    candidate = ReplayIntent(
        pipeline=pipeline,
        namespace=namespace,
        durable_commit_id=durable_point.commit_id,
        durable_resume_digest=_resume_digest(durable_point),
    )
    if existing is not None:
        validate_replay_intent(
            existing,
            pipeline=pipeline,
            namespace=namespace,
            durable_point=durable_point,
        )
        # An installing marker contains the only evidence that lets a restart
        # distinguish a completed install from a pre-install crash. Never replace it
        # with a weaker pending marker.
        return existing
    _atomic_json_write(path, candidate.as_dict())
    return candidate


def mark_replay_installing(
    path: Path | str,
    intent: ReplayIntent,
    *,
    source_size: int,
    source_sha256: str,
) -> ReplayIntent:
    """Record the exact disposable file expected to be atomically installed."""
    if intent.phase not in REPLAY_INTENT_PHASES:
        raise OffsetUnusable(f"cannot advance replay intent from phase {intent.phase!r}")
    if source_size <= 0 or len(source_sha256) != 64:
        raise OffsetUnusable("cannot arm replay installation without a valid file fingerprint")
    installing = ReplayIntent(
        pipeline=intent.pipeline,
        namespace=intent.namespace,
        durable_commit_id=intent.durable_commit_id,
        durable_resume_digest=intent.durable_resume_digest,
        phase=REPLAY_INTENT_INSTALLING,
        source_size=source_size,
        source_sha256=source_sha256,
    )
    if intent.phase == REPLAY_INTENT_INSTALLING:
        if (
            intent.source_size != source_size
            or intent.source_sha256 != source_sha256
        ):
            raise OffsetUnusable("replay installation fingerprint changed after it was armed")
        return intent
    _atomic_json_write(Path(path), installing.as_dict())
    return installing


def clear_replay_intent(path: Path | str) -> None:
    """Remove the marker only after the canonical replay offset is installed."""
    path = Path(path)
    path.unlink(missing_ok=True)
    _fsync_dir(path.parent)


def _file_fingerprint(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return path.stat().st_size, digest.hexdigest()


def replay_offset_fingerprint(path: Path | str) -> tuple[int, str]:
    """Validate and fingerprint a stock replay file; absence is a hard failure."""
    path = Path(path)
    if not path.exists() or path.stat().st_size <= 0:
        raise OffsetUnusable(
            f"stock Debezium did not leave a usable replay offset file at {path}"
        )
    entries = read(path)
    if not entries or len(parse_offsets(entries)) != 1:
        raise OffsetUnusable(f"stock Debezium produced an unreadable replay offset file {path}")
    return _file_fingerprint(path)


def _offset_file_is_not_ahead(
    path: Path | str,
    *,
    namespace: str,
    durable_point: ResumePoint,
) -> bool:
    """Return true only when a canonical candidate is durable or strictly older."""
    entries = read(path)
    if len(entries) != 1 or not durable_point.offset:
        return False
    expected_key = encode_key(namespace, durable_point.partition or {})
    actual_key, actual_value = next(iter(entries.items()))
    if actual_key != expected_key:
        return False
    decoded = parse_offsets({actual_key: actual_value})
    if not decoded:
        return False
    _partition, actual = decoded[0]
    actual_lsn = lsn_of(actual)
    durable_lsn = lsn_of(durable_point.offset)
    if actual_lsn is None or durable_lsn is None:
        return actual == durable_point.offset
    if actual_lsn > durable_lsn:
        return False
    if actual_lsn == durable_lsn:
        return actual == durable_point.offset
    return _offset_map_is_behind(actual, durable_point.offset)


def replay_install_is_durable(
    intent: ReplayIntent,
    *,
    target: Path | str,
    durable_point: ResumePoint,
) -> bool:
    """Recognize an already-finished install after a kill before marker clearing."""
    if intent.phase != REPLAY_INTENT_INSTALLING:
        return False
    try:
        size, digest = replay_offset_fingerprint(target)
    except OffsetUnusable:
        return False
    return (
        size == intent.source_size
        and digest == intent.source_sha256
        and _offset_file_is_not_ahead(
            target,
            namespace=intent.namespace,
            durable_point=durable_point,
        )
    )


def replay_offset_path(state_dir: Path | str) -> Path:
    """Return the disposable offset path used for a slot-replay recovery.

    A destination commit can precede the connector's file/slot acknowledgement.  On
    the next run the durable destination row is the truth, but giving that row back to
    Debezium's normal WAL-position search can skip a non-transactional message between
    the stored commit and the next transaction boundary.  A recovery therefore uses a
    fresh file so stock Debezium starts at the slot's confirmed position and Flight's
    durable fence/ledger handles the replay.
    """
    return Path(state_dir) / REPLAY_OFFSET_FILE_NAME


def prepare_replay_offset(path: Path | str) -> Path:
    """Remove any stale disposable replay file and return its path."""
    target = Path(path)
    target.unlink(missing_ok=True)
    return target


def install_replay_offset(
    source: Path | str,
    target: Path | str,
    *,
    expected_fingerprint: tuple[int, str] | None = None,
    durable_point: ResumePoint | None = None,
    namespace: str | None = None,
) -> bool:
    """Atomically install a validated replay offset, or fail the run.

    The old ``False`` result made a missing/empty source look like a successful
    pipeline return.  That is unsafe: the next run then sees the canonical file
    equal to MotherDuck and can take stock resume.  A replay install is therefore
    fail-closed and returns ``True`` only for compatibility with existing callers.
    """
    source_path = Path(source)
    source_fingerprint = replay_offset_fingerprint(source_path)
    if expected_fingerprint is not None and source_fingerprint != expected_fingerprint:
        raise OffsetUnusable(
            f"replay offset {source_path} changed before installation: "
            f"expected {expected_fingerprint}, got {source_fingerprint}"
        )
    if durable_point is not None:
        if namespace is None:
            raise ValueError("namespace is required when checking a durable replay point")
        if not _offset_file_is_not_ahead(
            source_path, namespace=namespace, durable_point=durable_point
        ):
            raise OffsetUnusable(
                f"replay offset {source_path} is not at or behind durable destination "
                f"commit {durable_point.commit_id}"
            )

    target_path = Path(target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = target_path.with_name(f".{target_path.name}.replay.tmp")
    temporary.unlink(missing_ok=True)
    try:
        shutil.copyfile(source_path, temporary)
        # This is the real partial-install crash row: the temporary file exists but
        # the canonical path has not been replaced. A restart discards the sibling
        # temp and replays from the still-durable marker.
        faults.matrix_crash("source_replay_during_copy_before_fsync")
        _fsync(temporary)
        if _file_fingerprint(temporary) != source_fingerprint:
            raise OffsetUnusable(
                f"replay offset temporary {temporary} did not match its validated source"
            )
        # The cut is immediately at the atomic boundary. Either the old canonical
        # file survives, or the new complete file does; neither state is torn.
        faults.matrix_crash("source_replay_at_os_replace")
        os.replace(temporary, target_path)
        faults.matrix_crash("source_replay_after_os_replace")
        _fsync_dir(target_path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return True


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
    before_repair: Callable[[ResumePoint, str], None] | None = None,
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
        repaired = _repair_with_intent(
            con, pipeline, namespace, offset_path, row, repair,
            before_repair=before_repair, decision="file_missing_rebuilt",
        )
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
        repaired = _repair_with_intent(
            con, pipeline, namespace, offset_path, row, repair,
            before_repair=before_repair, decision="file_corrupt_rebuilt",
        )
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
        repaired = _repair_with_intent(
            con, pipeline, namespace, offset_path, row, repair,
            before_repair=before_repair, decision="file_ahead_rebuilt",
        )
        return Reconciliation("file_ahead_rebuilt", row, file_lsn, repaired,
                              "offsets file was ahead of the destination")
    if file_lsn is not None and row.last_lsn and file_lsn < row.last_lsn:
        repaired = _repair_with_intent(
            con, pipeline, namespace, offset_path, row, repair,
            before_repair=before_repair, decision="file_behind_rebuilt",
        )
        return Reconciliation("file_behind_rebuilt", row, file_lsn, repaired,
                              "offsets file lagged the destination")

    difference = _offset_map_difference(entries, namespace, row)
    if difference is not None:
        repaired = _repair_with_intent(
            con, pipeline, namespace, offset_path, row, repair,
            before_repair=before_repair, decision="file_offset_mismatch_rebuilt",
        )
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


def _repair_with_intent(
    con,
    pipeline: str,
    namespace: str,
    offset_path: Path,
    row: ResumePoint,
    repair: bool,
    *,
    before_repair: Callable[[ResumePoint, str], None] | None,
    decision: str,
) -> bool:
    """Arm any caller-owned recovery decision before touching ``offsets.dat``."""
    if before_repair is not None and row.offset:
        # This callback is deliberately before _repair, including when repair=False.
        # A process can die after this decision and before a canonical write, and the
        # restart must still select the slot-replay path rather than stock resume.
        before_repair(row, decision)
    return _repair(con, pipeline, namespace, offset_path, row, repair)


def _offset_map_is_behind(actual: dict[str, Any], durable: dict[str, Any]) -> bool:
    """Return whether a valid file is an older Debezium point, not an alien one."""
    actual_lsn = lsn_of(actual)
    durable_lsn = lsn_of(durable)
    if actual_lsn is None or durable_lsn is None or actual_lsn >= durable_lsn:
        return False
    # A file flush can legitimately trail the destination transaction for a short
    # interval after markBatchFinished().  Once the primary LSN is behind, fields
    # belonging to the previous source transaction may also be absent/present in
    # different combinations; the destination remains the authority and the next
    # reconciliation can rebuild the file.  Any file ahead of the durable LSN is
    # never accepted by this helper.
    return all(
        not isinstance(value, int)
        or not isinstance(durable.get(key), int)
        or value <= durable[key]
        for key, value in actual.items()
        if key in durable
    )


def verify_service_offset(
    con,
    *,
    pipeline: str,
    namespace: str,
    offset_path: Path,
    control_schema: str | None = None,
) -> dict[str, object]:
    """Fail closed when a live service loses its usable Debezium offset.

    Startup reconciliation may repair a missing file.  During service life a
    missing, corrupt, foreign, or ahead file is a mid-stream invariant failure:
    silently allowing the engine to continue would turn a lost resume point into
    an unbounded replay/skip decision.  This read-only check is called while the
    applier's destination-operation lock is held and never enters COMMIT_ACK.
    """
    from .destination import read_resume_point

    row = read_resume_point(
        con, pipeline, namespace, control_schema=control_schema
    )
    if row is None or not row.offset:
        return {"checked": False, "reason": "no durable typed offset yet"}
    path = Path(offset_path)
    if not path.exists() or path.stat().st_size <= 0:
        raise OffsetUnusable(
            f"service offset file {path} disappeared while durable destination "
            f"offset {row.last_lsn} exists"
        )
    entries = read(path)
    difference = _offset_map_difference(entries, namespace, row)
    if difference is not None:
        parsed = parse_offsets(entries)
        if len(entries) == 1 and parsed and _offset_map_is_behind(parsed[0][1], row.offset):
            return {
                "checked": True,
                "behind": True,
                "last_lsn": row.last_lsn,
                "entries": len(entries),
                "reason": "offset file flush trails the durable destination point",
            }
        raise OffsetUnusable(
            f"service offset file no longer matches durable destination offset "
            f"{row.last_lsn}: {difference}"
        )
    return {
        "checked": True,
        "last_lsn": row.last_lsn,
        "entries": len(entries),
    }


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
