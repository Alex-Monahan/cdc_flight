"""Closed snapshot-callback completion model shared by applier and supervisor.

Debezium's ``sink`` notification channel places initial-snapshot notifications in the
same ordered ``ChangeEventQueue`` as row records.  A delivered ``COMPLETED`` notification
therefore proves that every earlier snapshot callback crossed the Python boundary.  The
per-table ``TABLE_SCAN_COMPLETED`` notifications also cover empty relations, which have
no row record on which a ``snapshot='last'`` marker could ride.

Source slot state is deliberately outside this model.  Reaching streaming says where the
connector is; it says nothing about callbacks already waiting in its delivery queue.
"""

from __future__ import annotations

from .assembler import UNIT_SNAPSHOT_CHUNK
from .machines import (
    SNAPSHOT_AWAITING_CALLBACKS,
    SNAPSHOT_CALLBACK_OBSERVATIONS,
    SNAPSHOT_CALLBACKS_COMPLETE,
    SNAPSHOT_CALLBACKS_STARTED,
    SNAPSHOT_COMPLETION,
    SNAPSHOT_COMPLETION_NOTIFIED,
    SNAPSHOT_NOT_REQUIRED,
)
from .states import IllegalTransition, UnknownState


class SnapshotObservationError(RuntimeError):
    """A callback or non-callback observation outside the closed protocol."""


def _qualified(value: str) -> str:
    parts = [part.strip().strip('"') for part in str(value).split(".")]
    if len(parts) != 2 or not all(parts):
        raise SnapshotObservationError(
            f"snapshot callback named invalid relation {value!r}; expected schema.table"
        )
    return ".".join(parts)


class SnapshotCompletion:
    """The one mutable record of ordered snapshot callback evidence."""

    def __init__(self, *, required: bool, expected_tables=()) -> None:
        self._required = required
        self._state = (
            SNAPSHOT_AWAITING_CALLBACKS if required else SNAPSHOT_NOT_REQUIRED
        )
        self._expected_tables = {_qualified(table) for table in expected_tables}
        self._callback_tables: set[str] = set()
        self._callback_rows: dict[str, int] = {}
        self._committed_rows: dict[str, int] = {}
        self._marker_seen = False
        self._snapshot_units = 0
        self._tables_seen: set[str] = set()

    @classmethod
    def full_snapshot(cls, expected_tables=()) -> SnapshotCompletion:
        return cls(required=True, expected_tables=expected_tables)

    @classmethod
    def streaming_only(cls) -> SnapshotCompletion:
        return cls(required=False)

    @classmethod
    def for_capture(cls, required: bool, *, schema: str, tables) -> SnapshotCompletion:
        if not required:
            return cls.streaming_only()
        expected = {table if "." in table else f"{schema}.{table}" for table in tables}
        return cls.full_snapshot(expected)

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
    def callback_tables(self) -> set[str]:
        return set(self._callback_tables)

    @property
    def snapshot_units(self) -> int:
        return self._snapshot_units

    def observe_committed_group(self, units, *, snapshot_active: bool) -> None:
        """Retain row diagnostics; row markers are not a completion edge."""
        del snapshot_active
        snapshot_units = [
            unit
            for unit in units
            if unit.kind == UNIT_SNAPSHOT_CHUNK and not unit.fenced
        ]
        if snapshot_units and self._state == SNAPSHOT_CALLBACKS_COMPLETE:
            raise SnapshotObservationError(
                "snapshot row callback arrived after Debezium's ordered COMPLETED "
                "callback; refusing an impossible callback order"
            )
        for unit in snapshot_units:
            self._snapshot_units += 1
            if unit.schema and unit.table:
                table = _qualified(f"{unit.schema}.{unit.table}")
                self._tables_seen.add(table)
                self._committed_rows[table] = (
                    self._committed_rows.get(table, 0) + unit.event_count
                )
                declared = self._callback_rows.get(table)
                if declared is not None and self._committed_rows[table] > declared:
                    raise SnapshotObservationError(
                        f"committed {self._committed_rows[table]} snapshot row callbacks "
                        f"for {table}, beyond Debezium's declared {declared}"
                    )
            if unit.snapshot_last:
                self._marker_seen = True
        if snapshot_units:
            if self._state == SNAPSHOT_COMPLETION_NOTIFIED and self._counts_match():
                self._transition(SNAPSHOT_CALLBACKS_COMPLETE)
            elif self._state in {
                SNAPSHOT_AWAITING_CALLBACKS,
                SNAPSHOT_CALLBACKS_STARTED,
                SNAPSHOT_COMPLETION_NOTIFIED,
            }:
                self._transition(self._state)

    def observe_notification(self, observation: str, data: dict[str, str]) -> None:
        """Consume one Debezium ``Initial Snapshot`` notification, exhaustively."""
        try:
            observation = SNAPSHOT_CALLBACK_OBSERVATIONS.parse(observation)
        except UnknownState as exc:
            raise SnapshotObservationError(str(exc)) from exc

        if self._state == SNAPSHOT_NOT_REQUIRED:
            if observation != "SKIPPED":
                raise SnapshotObservationError(
                    f"unexpected snapshot callback {observation} in not_required state"
                )
            self._transition(SNAPSHOT_NOT_REQUIRED)
            return

        if observation == "STARTED":
            if self._state != SNAPSHOT_AWAITING_CALLBACKS:
                self._unexpected(observation)
            self._transition(SNAPSHOT_CALLBACKS_STARTED)
            return

        if observation in {
            "IN_PROGRESS",
            "TABLE_CHUNK_IN_PROGRESS",
            "TABLE_CHUNK_COMPLETED",
        }:
            if self._state != SNAPSHOT_CALLBACKS_STARTED:
                self._unexpected(observation)
            self._validate_progress_table(observation, data)
            self._transition(SNAPSHOT_CALLBACKS_STARTED)
            return

        if observation == "TABLE_SCAN_COMPLETED":
            if self._state != SNAPSHOT_CALLBACKS_STARTED:
                self._unexpected(observation)
            if data.get("status") != "SUCCEEDED":
                raise SnapshotObservationError(
                    "TABLE_SCAN_COMPLETED did not report status=SUCCEEDED: "
                    f"{data.get('status')!r}"
                )
            table = _qualified(data.get("scanned_collection", ""))
            if table not in self._expected_tables:
                raise SnapshotObservationError(
                    f"snapshot callback completed unexpected table {table!r}; expected "
                    f"{sorted(self._expected_tables)}"
                )
            if table in self._callback_tables:
                raise SnapshotObservationError(
                    f"snapshot callback completed table {table!r} more than once"
                )
            self._callback_tables.add(table)
            try:
                rows = int(data["total_rows_scanned"])
            except (KeyError, TypeError, ValueError) as exc:
                raise SnapshotObservationError(
                    f"TABLE_SCAN_COMPLETED for {table} has invalid total_rows_scanned"
                ) from exc
            if rows < 0:
                raise SnapshotObservationError(
                    f"TABLE_SCAN_COMPLETED for {table} has negative row count {rows}"
                )
            self._callback_rows[table] = rows
            committed = self._committed_rows.get(table, 0)
            if committed > rows:
                raise SnapshotObservationError(
                    f"committed {committed} snapshot row callbacks for {table}, beyond "
                    f"Debezium's declared {rows}"
                )
            self._transition(SNAPSHOT_CALLBACKS_STARTED)
            return

        if observation == "COMPLETED":
            if self._state != SNAPSHOT_CALLBACKS_STARTED:
                self._unexpected(observation)
            missing = self._expected_tables - self._callback_tables
            extra = self._callback_tables - self._expected_tables
            if missing or extra:
                raise SnapshotObservationError(
                    "Debezium reported snapshot COMPLETED without the exact per-table "
                    f"terminal set; missing={sorted(missing)}, extra={sorted(extra)}"
                )
            self._transition(
                SNAPSHOT_CALLBACKS_COMPLETE
                if self._counts_match()
                else SNAPSHOT_COMPLETION_NOTIFIED
            )
            return

        # Required snapshots never accept SKIPPED or ABORTED, and the Domain.parse
        # above makes any future Debezium observation equally loud.
        self._unexpected(observation)

    def observe_source_streaming(self) -> None:
        """Refuse the Round-8 false edge explicitly for callers/tests."""
        raise SnapshotObservationError(
            "source streaming is not a snapshot callback completion observation; "
            "callbacks may still be queued behind that source-side state"
        )

    def as_dict(self) -> dict:
        return {
            "snapshot_completion_required": self.required,
            "snapshot_completion_state": self.state,
            "snapshot_completed": self.completed,
            "snapshot_final_seen": self.marker_seen,
            "snapshot_tables_seen": sorted(self._tables_seen),
            "snapshot_callback_tables_expected": sorted(self._expected_tables),
            "snapshot_callback_tables_completed": sorted(self._callback_tables),
            "snapshot_callback_rows_declared": dict(sorted(self._callback_rows.items())),
            "snapshot_callback_rows_committed": dict(
                sorted(self._committed_rows.items())
            ),
        }

    def _validate_progress_table(self, observation: str, data: dict[str, str]) -> None:
        key = (
            "scanned_collection"
            if observation == "TABLE_CHUNK_COMPLETED"
            else "current_collection_in_progress"
        )
        value = data.get(key)
        if value is None:
            raise SnapshotObservationError(
                f"{observation} callback is missing {key!r}"
            )
        table = _qualified(value)
        if table not in self._expected_tables:
            raise SnapshotObservationError(
                f"{observation} callback named unexpected table {table!r}; expected "
                f"{sorted(self._expected_tables)}"
            )

    def _counts_match(self) -> bool:
        return all(
            self._committed_rows.get(table, 0) == self._callback_rows.get(table)
            for table in self._expected_tables
        )

    def _unexpected(self, observation: str) -> None:
        raise SnapshotObservationError(
            f"unexpected snapshot callback {observation} in {self._state} state"
        )

    def _transition(self, target: str) -> None:
        try:
            SNAPSHOT_COMPLETION.check(self._state, target)
        except IllegalTransition as exc:
            raise SnapshotObservationError(str(exc)) from exc
        self._state = target
