"""One owner for the positive evidence that a snapshot phase has ended.

The state is deliberately per engine invocation. A full snapshot reaches a terminal
state through exactly one of two edges:

* ``pending -> record_complete`` after Debezium's ``snapshot='last'`` unit has
  committed and no shadow remains active;
* ``pending -> empty_complete`` after no snapshot unit was observed and the source
  health gate has proved that the connector reached streaming.

Streaming-only runs begin in ``not_required``. The applier records committed units;
the supervisor supplies the source-side empty evidence. Neither caller redefines the
meaning of completion.
"""

from __future__ import annotations

from .assembler import UNIT_SNAPSHOT_CHUNK
from .machines import (
    SNAPSHOT_COMPLETION,
    SNAPSHOT_EMPTY_COMPLETE,
    SNAPSHOT_NOT_REQUIRED,
    SNAPSHOT_PENDING,
    SNAPSHOT_RECORD_COMPLETE,
)


class SnapshotCompletion:
    """The one mutable completion record shared by applier and supervisor."""

    def __init__(self, *, required: bool) -> None:
        self._required = required
        self._state = SNAPSHOT_PENDING if required else SNAPSHOT_NOT_REQUIRED
        self._marker_seen = False
        self._snapshot_units = 0
        self._tables_seen: set[str] = set()

    @classmethod
    def full_snapshot(cls) -> SnapshotCompletion:
        return cls(required=True)

    @classmethod
    def streaming_only(cls) -> SnapshotCompletion:
        return cls(required=False)

    @property
    def required(self) -> bool:
        return self._required

    @property
    def state(self) -> str:
        return self._state

    @property
    def phase_ended(self) -> bool:
        return SNAPSHOT_COMPLETION.is_terminal(self._state)

    @property
    def completed(self) -> bool:
        return self.phase_ended

    @property
    def marker_seen(self) -> bool:
        return self._marker_seen

    @property
    def tables_seen(self) -> set[str]:
        return set(self._tables_seen)

    @property
    def snapshot_units(self) -> int:
        return self._snapshot_units

    def observe_committed_group(self, units, *, snapshot_active: bool) -> None:
        """Record snapshot evidence only after the destination group committed."""
        for unit in units:
            if unit.kind != UNIT_SNAPSHOT_CHUNK or unit.fenced:
                continue
            self._snapshot_units += 1
            if unit.schema and unit.table:
                self._tables_seen.add(f"{unit.schema}.{unit.table}")
            if unit.snapshot_last:
                self._marker_seen = True
        if (
            self._state == SNAPSHOT_PENDING
            and self._marker_seen
            and not snapshot_active
        ):
            self._transition(SNAPSHOT_RECORD_COMPLETE)

    def observe_source_streaming(self) -> None:
        """Use the source health gate as positive evidence for an all-empty phase."""
        if self._state != SNAPSHOT_PENDING or self._snapshot_units:
            return
        self._transition(SNAPSHOT_EMPTY_COMPLETE)

    def as_dict(self) -> dict:
        return {
            "snapshot_completion_required": self.required,
            "snapshot_completion_state": self.state,
            "snapshot_completed": self.completed,
            "snapshot_final_seen": self.marker_seen,
            "snapshot_tables_seen": sorted(self._tables_seen),
        }

    def _transition(self, target: str) -> None:
        SNAPSHOT_COMPLETION.check(self._state, target)
        self._state = target
