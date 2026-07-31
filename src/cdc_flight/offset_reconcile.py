"""`offsets.dat` versus the durable resume point (ADR 0001 §4.5).

Split out of `reconcile.py` at the 1.6-1.8 review round (Codex B6): offset-file
forensics, a pure slot-decision table and a destructive slot mutation were three
ownerships in one module, and the destructive one had no journal because nothing owned
it. This module is the first of the three and touches nothing but the file, the durable
row, and (for `--accept-orphan-offsets`) the slot it must prove gone before it deletes
anything.

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
| present (any state) | **absent** | **REFUSE TO START** (`orphan_offset_file`), unless `--accept-orphan-offsets`, which classifies `orphan_accepted_resnapshot` and hands the destructive sequence to the recovery journal |
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
offsets` is the deliberate escape hatch, and it forces a re-snapshot rather than
trusting the file.

**This module classifies; it does not mutate** (Codex r1 BLOCKER-1). The escape hatch
used to drop the slot and unlink the file here and journal the intent afterwards, which
put a crash window between destroying the evidence and recording why. `cdc_flight.
recovery` owns the sequence now, journal first, exactly like the acquisition recovery.

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
from .destination import ResumePoint, raise_alert, read_offset_blobs
from .errors import ReconciliationRefused
from .machines import RECONCILE_DECISIONS

log = logging.getLogger("cdc_flight.offset_reconcile")


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
