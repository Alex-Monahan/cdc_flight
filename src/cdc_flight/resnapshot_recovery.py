"""Durable recovery ownership for an interrupted blocking re-snapshot.

The re-snapshot orchestrator owns image construction. This module owns the separate
cross-process protocol which survives a callback that cannot be proved quiescent:
marker persistence and validation, declared transitions, restart discharge, and the
terminal slot/offset/marker cleanup boundary.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from . import destination as dest_mod
from . import faults
from . import reconcile as reconcile_mod
from .errors import EngineFailure
from .machines import INTERRUPTION_MARKER as INTERRUPTION_MARKER_MACHINE
from .machines import MARKER_ABSENT, MARKER_ARMED, MARKER_CONSUMED

# Keep the established operational logger while moving implementation ownership.
log = logging.getLogger("cdc_flight.resnapshot")

INTERRUPTION_MARKER = "interrupted.json"
TableIdentity = tuple[str, str, str]


def interruption_marker(state_dir) -> Path:
    return Path(state_dir) / INTERRUPTION_MARKER


def interruption_marker_state(state_dir) -> str:
    """Return the declared durable interruption-marker state for matrix observers."""
    return _read_interruption_marker(state_dir)[0]


def _write_interruption_marker(marker: Path, payload: dict) -> None:
    """Atomically replace and fsync one durable marker state."""
    marker.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as sink:
        json.dump(payload, sink, sort_keys=True)
        sink.flush()
        os.fsync(sink.fileno())
    os.replace(temporary, marker)
    directory_fd = os.open(marker.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _read_interruption_marker(state_dir) -> tuple[str, dict | None]:
    marker = interruption_marker(state_dir)
    if not marker.exists():
        return MARKER_ABSENT, None
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
        # Markers written by rev 18 had no explicit state and were always armed.
        state = INTERRUPTION_MARKER_MACHINE.parse(payload.get("state", MARKER_ARMED))
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise EngineFailure(
            f"invalid interrupted re-snapshot recovery marker {marker}: {exc}"
        ) from exc
    if state == MARKER_ABSENT:
        raise EngineFailure(
            f"invalid interrupted re-snapshot recovery marker {marker}: an existing "
            "marker cannot declare the absent state"
        )
    return state, payload


def arm_interruption_marker(
    state_dir, *, pipeline: str, tables: list[TableIdentity]
) -> Path:
    """Durably arm next-run owed-state repair before a callback can start."""
    state, _payload = _read_interruption_marker(state_dir)
    INTERRUPTION_MARKER_MACHINE.check(state, MARKER_ARMED)
    marker = interruption_marker(state_dir)
    payload = {
        "state": MARKER_ARMED,
        "pipeline": pipeline,
        "tables": [list(table) for table in tables],
    }
    _write_interruption_marker(marker, payload)
    faults.runtime_state(interruption_marker=MARKER_ARMED)
    return marker


def consume_interruption_marker(state_dir) -> Path:
    """Publish that a safe owner discharged the armed destination obligation."""
    state, payload = _read_interruption_marker(state_dir)
    INTERRUPTION_MARKER_MACHINE.check(state, MARKER_CONSUMED)
    assert payload is not None
    payload["state"] = MARKER_CONSUMED
    marker = interruption_marker(state_dir)
    _write_interruption_marker(marker, payload)
    faults.runtime_state(interruption_marker=MARKER_CONSUMED)
    return marker


def discard_consumed_interruption_marker(state_dir) -> None:
    """Retire terminal marker and offset state through the declared machine edge."""
    state, _payload = _read_interruption_marker(state_dir)
    if state == MARKER_ABSENT:
        return
    if state != MARKER_CONSUMED:
        raise EngineFailure(
            "refusing to delete armed interrupted re-snapshot recovery marker "
            f"{interruption_marker(state_dir)}"
        )
    INTERRUPTION_MARKER_MACHINE.check(state, MARKER_ABSENT)
    retired_dir = interruption_marker(state_dir).parent
    parent = retired_dir.parent
    shutil.rmtree(retired_dir)
    directory_fd = os.open(parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    faults.runtime_state(interruption_marker=MARKER_ABSENT)


def _validated_record(
    *, pipeline: str, state_dir
) -> tuple[str, list[TableIdentity]] | None:
    marker = interruption_marker(state_dir)
    state, payload = _read_interruption_marker(state_dir)
    if state == MARKER_ABSENT:
        return None
    try:
        assert payload is not None
        recorded_pipeline = str(payload["pipeline"])
        tables = [tuple(str(value) for value in row) for row in payload["tables"]]
    except (KeyError, TypeError, ValueError) as exc:
        raise EngineFailure(
            f"invalid interrupted re-snapshot recovery marker {marker}: {exc}"
        ) from exc
    if recorded_pipeline != pipeline or any(len(table) != 3 for table in tables):
        raise EngineFailure(
            f"interrupted re-snapshot marker {marker} belongs to pipeline "
            f"{recorded_pipeline!r} or has an invalid table identity; refusing to "
            "discard recovery intent"
        )
    return state, [(schema, table, target) for schema, table, target in tables]


def requeue_interrupted(
    con,
    *,
    pipeline: str,
    state_dir,
    control_schema: str | None = None,
) -> list[str]:
    """Consume a prior hard-exit marker and re-assert its snapshot obligation."""
    record = _validated_record(pipeline=pipeline, state_dir=state_dir)
    if record is None:
        return []
    state, tables = record
    if state == MARKER_ARMED:
        dest_mod.request_snapshot(
            con,
            pipeline=pipeline,
            tables=tables,
            detail=(
                "a previous re-snapshot could not prove callback quiescence; its "
                "durable image is requeued before this run starts"
            ),
            control_schema=control_schema,
        )
        consume_interruption_marker(state_dir)
    discard_consumed_interruption_marker(state_dir)
    names = sorted(f"{schema}.{table}" for schema, table, _target in tables)
    if state == MARKER_CONSUMED:
        log.warning(
            "retired a consumed interrupted re-snapshot marker for %s table(s)",
            len(names),
        )
        return []
    log.warning(
        "requeued %s table(s) from interrupted re-snapshot recovery: %s",
        len(names), ", ".join(names),
    )
    return names


@dataclass(frozen=True)
class InterruptionRecovery:
    """One typed owner for a running re-snapshot's durable recovery instance."""

    state_dir: Path
    pipeline: str
    tables: list[TableIdentity]

    @classmethod
    def prepare(
        cls, state_dir, *, pipeline: str, tables: list[TableIdentity]
    ) -> InterruptionRecovery:
        recovery = cls(Path(state_dir), pipeline, tables)
        state, _payload = _read_interruption_marker(recovery.state_dir)
        if state == MARKER_ARMED:
            raise EngineFailure(
                "refusing to replace armed interrupted re-snapshot recovery marker "
                f"{recovery.marker}; discharge it through restart recovery first"
            )
        if state == MARKER_CONSUMED:
            # Validate ownership before retiring the previous terminal instance. The
            # cleanup itself checks the declared `consumed -> absent` edge.
            _validated_record(pipeline=pipeline, state_dir=recovery.state_dir)
            discard_consumed_interruption_marker(recovery.state_dir)
        elif recovery.state_dir.exists():
            # No marker means no logical recovery instance. Sibling files are orphaned
            # state from an unarmed/crashed preparation and may be removed, but failure
            # must remain fatal: Debezium cannot consume replacement offsets beside it.
            shutil.rmtree(recovery.state_dir)
        recovery.state_dir.mkdir(parents=True, exist_ok=True)
        recovery.arm()
        return recovery

    @property
    def marker(self) -> Path:
        return interruption_marker(self.state_dir)

    @property
    def state(self) -> str:
        state, _payload = _read_interruption_marker(self.state_dir)
        return state

    @property
    def consumed(self) -> bool:
        return self.state == MARKER_CONSUMED

    def arm(self) -> Path:
        return arm_interruption_marker(
            self.state_dir, pipeline=self.pipeline, tables=self.tables
        )

    def consume(self) -> Path:
        return consume_interruption_marker(self.state_dir)

    def retain_in(self, summary: dict) -> None:
        summary["resnapshot_recovery"] = "armed"
        summary["resnapshot_recovery_marker"] = str(self.marker)
        summary["destination_owner"] = "live_resnapshot_callback"

    def retire_terminal_resources(
        self, *, dsn: str, slot: str, authorization=None
    ) -> None:
        """Drop the throwaway slot and destroy terminal marker plus offset state."""
        try:
            reconcile_mod.drop_slot(dsn, slot, authorization=authorization)
        except Exception:  # pragma: no cover - the source may be unreachable
            log.error(
                "could not drop the throwaway re-snapshot slot %r; it is holding WAL "
                "on the source and the next run of this pipeline will sweep it",
                slot,
                exc_info=True,
            )
        discard_consumed_interruption_marker(self.state_dir)
