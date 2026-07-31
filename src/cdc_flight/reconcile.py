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
from dataclasses import dataclass, field
from pathlib import Path

from . import offset_file
from .destination import (
    CONTROL_SCHEMA,
    ResumePoint,
    raise_alert,
    read_offset_blobs,
    request_snapshot,
)
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


# --------------------------------------------------------------------------- #
# rubric 1.8 — the slot, and the cluster it lives in, on every acquisition
# --------------------------------------------------------------------------- #
#: `pg_current_wal_lsn()` **errors** on a standby ("recovery is in progress"), and
#: rubric 7.2 wants CDC to be able to read from a replica, so the write position has to
#: be asked for in a way that answers on both. `pg_last_wal_receive_lsn()` is the
#: standby's equivalent and, like the primary's write position, is never behind
#: anything the slot has already decoded - which is all the regression check needs.
_SLOT_OBSERVATION_SQL = """
SELECT (s.restart_lsn - '0/0')::bigint,
       (s.confirmed_flush_lsn - '0/0')::bigint,
       s.active,
       ((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()
              ELSE pg_current_wal_lsn() END) - '0/0')::bigint,
       (SELECT system_identifier::text FROM pg_control_system()),
       (SELECT timeline_id FROM pg_control_checkpoint())
FROM (SELECT 1) one
LEFT JOIN pg_replication_slots s ON s.slot_name = %s
"""


@dataclass
class SlotObservation:
    """One look at the slot *and at the cluster it belongs to*.

    `system_identifier` and `timeline_id` are the two facts that separate "my source,
    quiet" from "a different source wearing the same DSN": a base-backup restore, a
    promoted standby, a DSN repointed at a clone. Neither is visible in the slot alone,
    and both are one cheap catalog function away.
    """

    slot_exists: bool = False
    active: bool = False
    restart_lsn: int | None = None
    confirmed_flush_lsn: int | None = None
    current_wal_lsn: int | None = None
    system_identifier: str | None = None
    timeline_id: int | None = None
    error: str | None = None

    @property
    def observable(self) -> bool:
        return self.error is None

    def as_dict(self) -> dict:
        return {
            "slot_exists": self.slot_exists,
            "slot_active": self.active,
            "restart_lsn": self.restart_lsn,
            "confirmed_flush_lsn": self.confirmed_flush_lsn,
            "current_wal_lsn": self.current_wal_lsn,
            "system_identifier": self.system_identifier,
            "timeline_id": self.timeline_id,
            "error": self.error,
        }


def observe_slot(dsn: str, slot_name: str, *, connect_timeout: int = 10) -> SlotObservation:
    """Everything rubric 1.8 needs, in one round trip on its own connection."""
    try:
        import psycopg

        with psycopg.connect(dsn, autocommit=True, connect_timeout=connect_timeout) as conn:
            row = conn.execute(_SLOT_OBSERVATION_SQL, (slot_name,)).fetchone()
    except Exception as exc:  # pragma: no cover - the source may be down
        return SlotObservation(error=f"{type(exc).__name__}: {exc}")
    if row is None:  # pragma: no cover - the LEFT JOIN always returns one row
        return SlotObservation(error="no row from pg_replication_slots")
    restart, confirmed, active, current, system_id, timeline = row
    return SlotObservation(
        slot_exists=restart is not None or confirmed is not None or bool(active),
        active=bool(active),
        restart_lsn=int(restart) if restart is not None else None,
        confirmed_flush_lsn=int(confirmed) if confirmed is not None else None,
        current_wal_lsn=int(current) if current is not None else None,
        system_identifier=str(system_id) if system_id is not None else None,
        timeline_id=int(timeline) if timeline is not None else None,
    )


#: Decisions that mean "WAL we needed is unreachable, so the destination has to be
#: rebuilt from the source". Every one of them triggers an automatic re-snapshot
#: (rubric 1.8's 5) rather than the hard error that was worth a 4.
RESNAPSHOT_DECISIONS = (
    "slot_ahead_of_destination",
    "slot_missing",
    "slot_recreated",
    "source_identity_changed",
    "source_lsn_regressed",
    "no_durable_destination_row",
)


@dataclass
class SlotVerdict:
    """What the slot check concluded, and what the run must do about it."""

    decision: str
    ok: bool
    resnapshot: bool = False
    refuse: bool = False
    message: str = ""
    context: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "decision": self.decision,
            "ok": self.ok,
            "resnapshot": self.resnapshot,
            "refuse": self.refuse,
            "message": self.message,
            **self.context,
        }


def check_slot(
    *,
    durable_lsn: int | None,
    observation: SlotObservation,
    previous: dict | None,
) -> SlotVerdict:
    """The rubric-1.8 decision table, as a pure function (ADR 0001 §19/A45).

    Pure so that every cell is a unit test rather than a Postgres it would take a
    base-backup restore to produce. The caller supplies the durable resume point, one
    observation, and the previous observation; nothing here connects to anything.

    | condition | decision | action |
    |---|---|---|
    | source unobservable | `source_unobservable` | proceed; the engine will fail on its own |
    | no durable row, no slot | `fresh_start` | nothing to reconcile |
    | no durable row, slot has a position | `no_durable_destination_row` | re-snapshot |
    | durable row, slot gone | `slot_missing` | re-snapshot |
    | `system_identifier` changed | `source_identity_changed` | re-snapshot |
    | `pg_current_wal_lsn() < durable` | `source_lsn_regressed` | re-snapshot |
    | `restart_lsn` regressed vs the last observation | `slot_recreated` | re-snapshot |
    | `confirmed_flush_lsn > durable` | `slot_ahead_of_destination` | re-snapshot |
    | otherwise | `ok` | stream |

    Order matters: the *cause* is reported, not the symptom. A restored cluster also
    shows a regressed LSN and often a recreated slot, and "your source is a different
    cluster" is the sentence an operator needs.
    """
    context = observation.as_dict() | {"durable_lsn": durable_lsn}
    if not observation.observable:
        return SlotVerdict(
            "source_unobservable", ok=False, message=str(observation.error), context=context
        )

    if durable_lsn is None:
        if not observation.slot_exists or observation.confirmed_flush_lsn is None:
            return SlotVerdict("fresh_start", ok=True, context=context)
        return SlotVerdict(
            "no_durable_destination_row",
            ok=False,
            resnapshot=True,
            message=(
                f"the slot exists with confirmed_flush_lsn="
                f"{observation.confirmed_flush_lsn} but the destination has no resume "
                "point: nothing before that position is durable here, so the WAL that "
                "would have carried it is already discarded"
            ),
            context=context,
        )

    previous = previous or {}
    prev_system = previous.get("system_identifier")
    if (
        prev_system
        and observation.system_identifier
        and str(prev_system) != str(observation.system_identifier)
    ):
        return SlotVerdict(
            "source_identity_changed",
            ok=False,
            resnapshot=True,
            message=(
                f"the source cluster's system_identifier changed from {prev_system} to "
                f"{observation.system_identifier}: this is not the database the "
                "destination was built from (a restore, a clone, or a repointed DSN)"
            ),
            context=context | {"previous_system_identifier": str(prev_system)},
        )

    if (
        observation.current_wal_lsn is not None
        and observation.current_wal_lsn < durable_lsn
    ):
        return SlotVerdict(
            "source_lsn_regressed",
            ok=False,
            resnapshot=True,
            message=(
                f"pg_current_wal_lsn()={observation.current_wal_lsn} is BEHIND the "
                f"durable destination offset {durable_lsn}: the source has been rewound "
                "(a base-backup restore or a timeline change), so the events the "
                "destination already holds are no longer the source's history"
            ),
            context=context,
        )

    if not observation.slot_exists:
        return SlotVerdict(
            "slot_missing",
            ok=False,
            resnapshot=True,
            message=(
                "the replication slot is gone. A new slot starts at the *current* WAL "
                f"position, so every change since the durable offset {durable_lsn} "
                "would be silently missing"
            ),
            context=context,
        )

    prev_restart = previous.get("restart_lsn")
    if (
        prev_restart is not None
        and observation.restart_lsn is not None
        and observation.restart_lsn < int(prev_restart)
    ):
        return SlotVerdict(
            "slot_recreated",
            ok=False,
            resnapshot=True,
            message=(
                f"the slot's restart_lsn went BACKWARDS, from {prev_restart} to "
                f"{observation.restart_lsn}: a slot only ever advances, so this slot is "
                "not the slot we were streaming from"
            ),
            context=context | {"previous_restart_lsn": int(prev_restart)},
        )

    confirmed = observation.confirmed_flush_lsn
    if confirmed is not None and confirmed > durable_lsn:
        return SlotVerdict(
            "slot_ahead_of_destination",
            ok=False,
            resnapshot=True,
            message=(
                f"the slot's confirmed_flush_lsn={confirmed} is AHEAD of the durable "
                f"destination offset {durable_lsn}: something else advanced the slot, "
                "and the WAL in between can no longer be replayed"
            ),
            context=context,
        )

    return SlotVerdict("ok", ok=True, context=context)


def drop_slot(dsn: str, slot_name: str) -> str:
    """Drop the slot so the next start creates a fresh one. Returns what happened.

    The re-snapshot **needs** a slot Debezium creates itself, and this is why. Debezium
    only uses Postgres's exported snapshot - `CREATE_REPLICATION_SLOT` returning a
    `consistent_point` plus a `snapshot_name`, then `SET TRANSACTION SNAPSHOT` - when it
    creates the slot as part of the same start-up (`PostgresSnapshotChangeEventSource.
    getTransactionStartLsn`: "if any SQL operations occur mid-snapshot ... otherwise
    they'll be lost"). With a pre-existing slot it falls back to an ordinary snapshot
    plus `pg_current_wal_lsn()`, and the snapshot/stream boundary is then only as exact
    as that pairing happens to be. VERIFIED in the engine log: a fresh slot gets
    `SET TRANSACTION SNAPSHOT '…'`, a pre-existing one does not.

    Keeping the stale slot would also be wrong for a different reason: its
    `confirmed_flush_lsn` is by definition *ahead* of what we can account for, so a
    stream resumed from it starts past the snapshot's consistent point and the window
    in between is lost twice over.
    """
    import psycopg

    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        rows = conn.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (slot_name,),
        ).fetchall()
    return "dropped" if rows else "absent"


def recover_by_full_resnapshot(
    con,
    *,
    pipeline: str,
    namespace: str,
    dsn: str,
    slot_name: str,
    offset_path: Path,
    verdict: SlotVerdict,
    captured_tables: list[tuple[str, str, str]],
    forget_catalog: bool = False,
) -> dict:
    """Rubric 1.8's automatic recovery: rebuild every captured table from the source.

    "Affected tables = all captured tables, unless provable otherwise", and it is not
    provable otherwise: a slot advanced past our position discarded WAL for *every*
    relation in the publication, and nothing in the destination records which relations
    the discarded WAL touched. So the whole capture set is re-snapshotted.

    Nothing is destroyed here. The destination tables stay exactly as they are, still
    queryable, and each one is replaced only when its shadow is complete, in one
    transaction (D7). If this run dies half way, the tables it did not reach are still
    `awaiting_snapshot` and the next run finishes the job - which is why the marking
    happens *before* anything else.

    The steps, in the order they have to happen:

    1. mark every captured table `awaiting_snapshot` - the durable to-do list;
    2. delete the destination resume point, so reconciliation sees a fresh start;
    3. delete `offsets.dat`, so Debezium does not resume from a position we have just
       declared unusable;
    4. drop the replication slot, so Debezium creates one and the snapshot is
       coordinated by Postgres's exported snapshot rather than by a race (see
       `drop_slot`).

    Order 2-before-3 matters: with the row gone and the file present, reconciliation
    REFUSES to start (`orphan_offset_file`), which is the correct refusal for an
    operator pointing at the wrong database and the wrong outcome for us. Doing both,
    in this order, in one function is how that stays true.
    """
    detail = f"{verdict.decision}: {verdict.message}"
    raise_alert(
        con, pipeline=pipeline, severity="critical", code=verdict.decision,
        message=(
            f"{verdict.message}. Rebuilding every captured table from the source "
            f"({len(captured_tables)} tables); the destination stays queryable until "
            "each table's snapshot is complete and swapped in one transaction."
        ),
        context=verdict.as_dict(),
    )
    marked = request_snapshot(con, pipeline=pipeline, tables=captured_tables, detail=detail)
    con.execute(
        f"DELETE FROM {CONTROL_SCHEMA}.debezium_offsets WHERE pipeline = ? AND namespace = ?",
        [pipeline, namespace],
    )
    if forget_catalog:
        # A different cluster's oids are not our relations' oids, and comparing them
        # would make the catalog watcher conclude that every table was dropped and
        # recreated - which the mass-drop circuit breaker then refuses, correctly and
        # unhelpfully. Forget what we knew about a catalog that no longer exists.
        con.execute(
            f"DELETE FROM {CONTROL_SCHEMA}.source_relations WHERE pipeline = ?", [pipeline]
        )
    file_removed = Path(offset_path).exists()
    Path(offset_path).unlink(missing_ok=True)
    try:
        slot_action = drop_slot(dsn, slot_name)
    except Exception as exc:  # pragma: no cover - a slot held by another backend
        log.error("could not drop the replication slot %r: %s", slot_name, exc)
        slot_action = f"drop_failed: {exc}"
    log.warning(
        "rubric 1.8 recovery armed (%s): %s tables awaiting a snapshot, resume point "
        "deleted, offsets file %s, slot %s",
        verdict.decision, marked, "removed" if file_removed else "absent", slot_action,
    )
    return {
        "decision": verdict.decision,
        "tables_marked": marked,
        "offset_file": "removed" if file_removed else "absent",
        "slot": slot_action,
        "message": verdict.message,
    }


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
