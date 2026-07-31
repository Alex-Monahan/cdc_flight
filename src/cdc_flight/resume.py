"""The resume point a commit group writes, and the `offsets.dat` forensics (ADR §4.3).

Extracted from `applier.py` (ADR §15/A29, §18/A44) so that module owns the commit
protocol and only that. Nothing here has behaviour of its own: it is the two questions
"what does this group make durable?" and "does the file Debezium keeps agree with what
we committed?", both of which are about the *resume point* rather than about the
transaction.

The load-bearing rule is `point_for`'s refusal. `envelope.offsets_of()` returns
`(None, None)` for every bridge failure, and pairing a NEWER `last_lsn` with the
PREVIOUS (or an empty) Connect offset map is silent loss: Debezium would resume from the
older offset while our idempotency fence claimed the newer LSN was durable, so the
replay would be fenced away. Refusing the commit is free — a rollback replays.
"""

from __future__ import annotations

import json
import logging

from . import offset_file
from .assembler import CompleteUnit
from .destination import ResumePoint
from .envelope import PendingRecord
from .envelope import offsets_of as envelope_offsets
from .errors import ResumePointDrift

log = logging.getLogger("cdc_flight.resume")


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
        entries = offset_file.read(path)
    except Exception:  # pragma: no cover
        return None, None
    if not entries:
        return None, None
    key = next(iter(entries))
    blob = _serialise_entries(entries)
    file_offsets = offset_file.parse_offsets(entries)
    if file_offsets:
        _partition, offset = file_offsets[0]
        file_lsn = offset_file.lsn_of(offset)
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
