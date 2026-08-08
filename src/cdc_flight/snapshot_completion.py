"""Closed snapshot-callback completion model shared by applier and supervisor.

Debezium's ``sink`` notification channel places initial-snapshot notifications in the
same ordered ``ChangeEventQueue`` as row records.  A delivered ``COMPLETED`` notification
therefore proves that every earlier snapshot callback crossed the Python boundary.  The
per-table ``TABLE_SCAN_COMPLETED`` notifications also cover empty relations, which
have no row record but still have direct completion evidence.

Source slot state is deliberately outside this model.  Reaching streaming says where the
connector is; it says nothing about callbacks already waiting in its delivery queue.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .assembler import UNIT_SNAPSHOT_CHUNK
from .envelope import KIND_SNAPSHOT_BOUNDARY
from .machines import (
    SNAPSHOT_AWAITING_CALLBACKS,
    SNAPSHOT_CALLBACK_OBSERVATIONS,
    SNAPSHOT_CALLBACKS_COMPLETE,
    SNAPSHOT_CALLBACKS_STARTED,
    SNAPSHOT_COMPLETION,
    SNAPSHOT_COMPLETION_NOTIFIED,
    SNAPSHOT_NOT_REQUIRED,
    SNAPSHOT_STREAMING,
)
from .states import IllegalTransition, UnknownState


class SnapshotObservationError(RuntimeError):
    """A callback or non-callback observation outside the closed protocol."""


NOTIFICATION_SUFFIX = "cdc_flight_snapshot_notifications"
INITIAL_SNAPSHOT = "Initial Snapshot"


def notification_topic(topic_prefix: str) -> str:
    return f"{topic_prefix}.{NOTIFICATION_SUFFIX}"


@dataclass(frozen=True)
class SnapshotNotification:
    observation: str
    data: dict[str, str]


def decode_notification(raw, *, topic_prefix: str) -> SnapshotNotification | None:
    """Return a typed snapshot notification, or ``None`` for another topic."""
    topic = str(raw.destination())
    expected_topic = notification_topic(topic_prefix)
    if topic != expected_topic:
        return None
    text = "" if raw.value() is None else str(raw.value())
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SnapshotObservationError(
            f"snapshot notification on {topic} is not JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise SnapshotObservationError(
            f"snapshot notification on {topic} is not an object"
        )
    # JsonConverter with schemas.enable=true wraps the sink notification in the
    # same ``{"schema": ..., "payload": ...}`` envelope as row records.  The
    # notification schema is transport metadata; the payload remains the closed
    # callback protocol validated below.
    if (
        isinstance(payload.get("schema"), dict)
        and "payload" in payload
        and isinstance(payload.get("payload"), dict)
    ):
        payload = payload["payload"]
    aggregate = payload.get("aggregate_type")
    if aggregate != INITIAL_SNAPSHOT:
        raise SnapshotObservationError(
            f"unexpected notification aggregate {aggregate!r} on reserved topic {topic}"
        )
    observation = payload.get("type")
    if not isinstance(observation, str) or not observation:
        raise SnapshotObservationError("snapshot notification has no string type")
    raw_data = payload.get("additional_data")
    if not isinstance(raw_data, dict):
        raise SnapshotObservationError(
            f"snapshot notification {observation} has non-object additional_data"
        )
    data = {str(key): str(value) for key, value in raw_data.items()}
    return SnapshotNotification(observation=observation, data=data)


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

    def validate_committed_group(self, units) -> None:
        """Validate snapshot-row observations before their destination transaction.

        ``observe_committed_group`` records the rows only after ``COMMIT``. Its
        validation therefore has to be available before the commit as well, or an
        illegal row/state or an over-counted callback can advance the resume point
        before the refusal is raised.
        """
        self._validate_snapshot_units(self._snapshot_units_in(units))

    def commit_ready(self, units) -> bool:
        """Whether a group may carry a terminal snapshot offset to ``COMMIT``.

        A valid ``COMPLETED`` callback can arrive while its final row units are still
        buffered. Keep the boundary with those rows until their declared counts match;
        otherwise the destination could durably skip the queued rows while the model
        still says ``completion_notified``.
        """
        self.validate_committed_group(units)
        if not self._has_boundary(units):
            return True
        return self._will_complete_after(self._snapshot_units_in(units), boundary=True)

    def will_complete_after_commit(self, units) -> bool:
        """Predict the post-COMMIT completion edge without mutating the model."""
        snapshot_units = self._snapshot_units_in(units)
        self._validate_snapshot_units(snapshot_units)
        return self._will_complete_after(snapshot_units)

    def check_streaming_admission(self) -> None:
        """Check the phase edge before a streaming unit can affect a group.

        This is deliberately non-mutating.  The applier uses it before closing an
        open snapshot group, so an illegal edge cannot first commit snapshot rows and
        only then be reported.  Once the model is already in ``streaming``, repeated
        streaming units are admitted without inventing a second transition.
        """
        if self._state == SNAPSHOT_STREAMING:
            return
        self._check_streaming_edge()

    def enter_streaming(self) -> None:
        """Take the only phase-changing edge out of the snapshot model."""
        self.check_streaming_admission()
        self._state = SNAPSHOT_STREAMING

    def _check_streaming_edge(self) -> None:
        try:
            SNAPSHOT_COMPLETION.check(self._state, SNAPSHOT_STREAMING)
        except (IllegalTransition, UnknownState) as exc:
            raise SnapshotObservationError(
                f"snapshot phase transition to streaming refused from {self._state}: "
                f"{exc}"
            ) from exc

    def observe_committed_group(self, units, *, snapshot_active: bool) -> None:
        """Record rows after ``COMMIT``; direct callbacks prove completion.

        The old shadow/marker model is gone. A row marker remains diagnostic, while
        the per-table and global notifications plus declared/committed row counts
        are the only completion proof.
        """
        del snapshot_active
        snapshot_units = self._snapshot_units_in(units)
        self._validate_snapshot_units(snapshot_units)
        for unit in snapshot_units:
            self._snapshot_units += 1
            table = _qualified(f"{unit.schema}.{unit.table}")
            self._tables_seen.add(table)
            self._committed_rows[table] = (
                self._committed_rows.get(table, 0) + unit.event_count
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

    @staticmethod
    def _snapshot_units_in(units):
        return [
            unit
            for unit in units
            if unit.kind == UNIT_SNAPSHOT_CHUNK and not unit.fenced
        ]

    @staticmethod
    def _has_boundary(units) -> bool:
        return any(
            record.kind == KIND_SNAPSHOT_BOUNDARY
            for unit in units
            for record in unit.records
        )

    def _projected_rows(self, snapshot_units) -> dict[str, int]:
        projected = dict(self._committed_rows)
        for unit in snapshot_units:
            table = _qualified(f"{unit.schema}.{unit.table}")
            projected[table] = projected.get(table, 0) + unit.event_count
        return projected

    def _validate_snapshot_units(self, snapshot_units) -> None:
        if not snapshot_units:
            return
        legal_states = {
            SNAPSHOT_CALLBACKS_STARTED,
            SNAPSHOT_COMPLETION_NOTIFIED,
        }
        if self._state not in legal_states:
            raise SnapshotObservationError(
                f"committed snapshot row callback arrived in {self._state} state; "
                "rows are legal only after STARTED and before callbacks_complete"
            )
        # Keep the observation on the declared machine, including its self-edges.
        # The explicit state set above is necessary because not_required also has a
        # self-edge for repeated SKIPPED notifications, not for rows.
        try:
            SNAPSHOT_COMPLETION.check(self._state, self._state)
        except IllegalTransition as exc:  # pragma: no cover - declaration guard
            raise SnapshotObservationError(str(exc)) from exc

        for unit in snapshot_units:
            if not unit.schema or not unit.table:
                raise SnapshotObservationError(
                    "committed snapshot row callback has no schema.table identity"
                )
            table = _qualified(f"{unit.schema}.{unit.table}")
            if table not in self._expected_tables:
                raise SnapshotObservationError(
                    f"committed snapshot row callback named unexpected table {table!r}; "
                    f"expected {sorted(self._expected_tables)}"
                )

        projected = self._projected_rows(snapshot_units)
        for unit in snapshot_units:
            table = _qualified(f"{unit.schema}.{unit.table}")
            declared = self._callback_rows.get(table)
            if declared is not None and projected[table] > declared:
                raise SnapshotObservationError(
                    f"committed {projected[table]} snapshot row callbacks for {table}, "
                    f"beyond Debezium's declared {declared}"
                )

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

    def _counts_match(self, committed_rows: dict[str, int] | None = None) -> bool:
        committed_rows = self._committed_rows if committed_rows is None else committed_rows
        return all(
            committed_rows.get(table, 0) == self._callback_rows.get(table)
            for table in self._expected_tables
        )

    def _will_complete_after(self, snapshot_units, *, boundary: bool = False) -> bool:
        if self.phase_ended:
            return True
        if self._state != SNAPSHOT_COMPLETION_NOTIFIED:
            if not boundary:
                return False
            raise SnapshotObservationError(
                f"snapshot boundary reached {self._state} without a validated "
                "COMPLETED observation"
            )
        return self._counts_match(self._projected_rows(snapshot_units))

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
