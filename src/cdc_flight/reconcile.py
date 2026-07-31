"""Start-up reconciliation and the Invariant-O guard (ADR 0001 §4.5, §4.7).

**Rule: `offsets.dat` is never a source of truth.** It is a scratch
serialisation buffer that Debezium happens to require on disk. The truth is
`_cdc_flight.debezium_offsets`, written inside the same transaction as the data.

| `offsets.dat` | table row | decision |
|---|---|---|
| absent | absent | fresh start (snapshot per `snapshot.mode`) |
| absent | present | write the file from the table |
| present, **identical typed offset map** | present | resume |
| present, **ahead of** table on the scalar LSN | present | overwrite from the table; `warning offset_file_ahead` |
| present, differs in *any* typed offset field, key or entry count | present | overwrite from the table |
| present, corrupt | present | overwrite from the table |
| present (any state) | **absent** | **REFUSE TO START** (`orphan_offset_file`) |
| any | present, but `slot.confirmed_flush_lsn > last_lsn` | `critical slot_ahead_of_destination` -> rubric 1.8 |
| any | **absent**, but the slot exists and has advanced | **REFUSE TO START** (`no_durable_destination_row`) unless `snapshot.mode` re-reads every table |

Two of those rows are corrections from the 1.1-1.3 review (Codex 3). The
comparison is on the **whole typed offset map**, not on a scalar LSN: several
events share one commit LSN, so `{lsn: 100, lsn_proc: 999}` and a durable
`{lsn: 100, lsn_proc: 1}` are *different positions* that a scalar guard called
"agrees". And an empty destination is only safe to start from when the configured
snapshot mode is about to re-read every captured table; otherwise the connector
streams from the slot's confirmed position and everything before it is gone.

The refusal row is the one that matters and the one that is easy to get wrong: a
file with no matching destination row may be arbitrarily *ahead* of anything
durable, so trusting it silently loses every event in between. `--accept-orphan-
offsets` is the deliberate escape hatch, and it deletes the file and forces a
re-snapshot rather than trusting it.

Correctness does **not** depend on the repair: under Invariant O the file can
only ever lag the table, and a lagging file replays units the applier then
fences (ADR §4.4). `CDC_OFFSET_FILE_REPAIR=0` turns the repair off precisely so
that the fence can be exercised on its own, and the suite runs the crash
scenario both ways.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from . import offset_file
from .destination import CONTROL_SCHEMA, ResumePoint, raise_alert, read_offset_blobs
from .errors import (
    NoDurableDestinationRow,
    ReconciliationRefused,
    SlotAheadOfDestination,
)

log = logging.getLogger("cdc_flight.reconcile")


@dataclass
class Reconciliation:
    decision: str
    resume_point: ResumePoint
    file_lsn: int | None = None
    repaired: bool = False
    message: str = ""


def reconcile(
    con,
    *,
    pipeline: str,
    namespace: str,
    offset_path: Path,
    accept_orphan: bool = False,
    repair: bool = True,
) -> Reconciliation:
    from .destination import read_resume_point

    row = read_resume_point(con, pipeline, namespace)
    entries = offset_file.read(offset_path)
    file_present = Path(offset_path).exists() and Path(offset_path).stat().st_size > 0
    file_decoded = bool(entries)
    parsed = offset_file.parse_offsets(entries)
    file_lsn = None
    if parsed:
        file_lsn = offset_file.lsn_of(parsed[0][1])

    if row is None:
        if not file_present:
            return Reconciliation("fresh_start", ResumePoint(), None, False,
                                  "no offsets file and no destination row")
        if accept_orphan:
            Path(offset_path).unlink(missing_ok=True)
            raise_alert(
                con, pipeline=pipeline, severity="warning", code="orphan_offset_file",
                message="orphan offsets.dat deleted on operator request; re-snapshotting",
                context={"offset_file": str(offset_path), "file_lsn": file_lsn},
            )
            return Reconciliation("orphan_accepted_resnapshot", ResumePoint(), file_lsn,
                                  True, "orphan offsets file deleted")
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

    Not a scalar LSN (Codex 3). `offset_file.lsn_of()` returns the first of
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
    expected_key = offset_file.encode_key(namespace, row.partition or {})
    if len(entries) != 1:
        return f"{len(entries)} entries, expected exactly 1 for {expected_key!r}"
    (actual_key, actual_value), = entries.items()
    if actual_key != expected_key:
        return f"key {actual_key!r}, expected {expected_key!r}"
    decoded = offset_file.parse_offsets({actual_key: actual_value})
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
    key = key_blob or offset_file.encode_key(namespace, row.partition or {})
    offset_file.write(offset_path, {key: offset_file.encode_value(row.offset)})
    log.info("rebuilt %s from the destination (lsn=%s)", offset_path, row.last_lsn)
    return True


# --------------------------------------------------------------------------- #
# ADR §4.7 — the Invariant-O guard
# --------------------------------------------------------------------------- #
def slot_position(dsn: str, slot_name: str) -> int | None:
    """`confirmed_flush_lsn` of the slot, as an integer, or None if it is gone."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        rows = conn.execute(
            "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (slot_name,),
        ).fetchall()
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


#: `snapshot.mode` values that re-read every captured table's data in full, so an
#: empty destination is about to be repopulated rather than skipped over.
SNAPSHOT_MODES_WITH_DATA = frozenset(
    {"initial", "initial_only", "always", "when_needed"}
)


def check_invariant_o(
    con,
    *,
    pipeline: str,
    namespace: str,
    dsn: str,
    slot_name: str,
    snapshot_mode: str | None = None,
    raise_on_violation: bool = True,
) -> dict:
    """`slot.confirmed_flush_lsn <= debezium_offsets.last_lsn`, plus the cell it lacked.

    The only detector for the bug class that produced ADR revision 2 (a Debezium
    lifecycle path confirming an LSN the destination never committed). Sampled at
    start-up and at shutdown; a violation means WAL that is not in the
    destination has already been discarded, which routes to rubric 1.8's
    automatic re-snapshot.

    It used to return `ok=True` whenever `durable is None`, which reported ADR
    §4.5's "absent/absent but slot exists" cell as healthy (Codex 3). An existing
    slot with an advanced `confirmed_flush_lsn` and an **empty** destination means
    the WAL between them is not in the destination and may already be unrecoverable;
    that is only safe to proceed from when `snapshot.mode` will re-read every
    captured table in full, and it is never "healthy".
    """
    rows = con.execute(
        f"SELECT last_lsn FROM {CONTROL_SCHEMA}.debezium_offsets "
        "WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    ).fetchall()
    durable = int(rows[0][0]) if rows else None
    try:
        confirmed = slot_position(dsn, slot_name)
    except Exception as exc:  # pragma: no cover - the source may be down
        log.warning("could not read the slot position: %s", exc)
        return {"checked": False, "reason": str(exc)}

    result = {
        "checked": True,
        "slot_confirmed_flush_lsn": confirmed,
        "durable_lsn": durable,
        "snapshot_mode": snapshot_mode,
        "ok": True,
    }
    if durable is None and confirmed is not None:
        backfills = (snapshot_mode or "") in SNAPSHOT_MODES_WITH_DATA
        message = (
            f"replication slot {slot_name!r} exists with confirmed_flush_lsn={confirmed} "
            f"but {CONTROL_SCHEMA}.debezium_offsets has no row for pipeline={pipeline!r}: "
            "nothing before that position is durable here"
        )
        if backfills:
            result["decision"] = "no_durable_row_full_snapshot"
            log.warning(
                "%s; proceeding because snapshot.mode=%s re-reads every captured table",
                message, snapshot_mode,
            )
            return result
        result["ok"] = False
        result["decision"] = "no_durable_destination_row"
        raise_alert(
            con, pipeline=pipeline, severity="critical",
            code="no_durable_destination_row", message=message, context=result,
        )
        if raise_on_violation:
            raise NoDurableDestinationRow(
                f"REFUSING TO START: {message}. snapshot.mode={snapshot_mode!r} does not "
                f"re-read table data, so the connector would stream from {confirmed} and "
                "every change before it would be silently missing (ADR 0001 §4.5). Use a "
                "snapshot mode that backfills, or point at the right destination."
            )
        return result
    if confirmed is None or durable is None:
        return result
    if confirmed > durable:
        result["ok"] = False
        message = (
            f"slot {slot_name!r} confirmed_flush_lsn={confirmed} is AHEAD of the durable "
            f"destination offset {durable}: WAL in between can no longer be replayed"
        )
        raise_alert(
            con, pipeline=pipeline, severity="critical",
            code="slot_ahead_of_destination", message=message, context=result,
        )
        if raise_on_violation:
            raise SlotAheadOfDestination(message)
    return result
