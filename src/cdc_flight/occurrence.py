"""Opaque alert-occurrence identities and their state-owned inputs.

An exception message is useful for grouping a condition, but it is not an
occurrence. Occurrences come from a durable row, a run, or another state
machine that owns the lifetime of the incident. This module keeps that
distinction at the API boundary: ``OccurrenceKey`` has no public constructor
for text, and durable-backed factories accept only an opaque receipt returned
after their state owner has committed or fsynced/installed its state. A raw
state value is never enough to mint an occurrence.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


def _require_text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{name} must be a non-empty string")
    return value


class RunState:
    """The run-owned identity allocated at the beginning of one pipeline run.

    There is deliberately no public constructor accepting a caller-supplied
    identifier.  ``new()`` is the only way production code obtains a run state;
    existing consumers use the read-only ``runner_id`` property.
    """

    __slots__ = ("_pipeline", "_runner_id")

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise TypeError("RunState is allocated by RunState.new()")

    @classmethod
    def new(cls, pipeline: str) -> RunState:
        pipeline = _require_text(pipeline, name="pipeline")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_pipeline", pipeline)
        object.__setattr__(instance, "_runner_id", uuid.uuid4().hex)
        return instance

    @property
    def pipeline(self) -> str:
        return self._pipeline

    @property
    def runner_id(self) -> str:
        return self._runner_id

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("RunState is immutable")


@dataclass(frozen=True, slots=True)
class EpisodeState:
    """The value carried by a source-health or fallback-alert durable owner."""

    pipeline: str
    episode_id: int
    state: str
    observed_at: Any = None

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        if not isinstance(self.episode_id, int) or self.episode_id < 0:
            raise TypeError("episode_id must be a non-negative integer")
        _require_text(self.state, name="state")

    def as_dict(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "state": self.state,
            "observed_at": self.observed_at,
        }

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]


@dataclass(frozen=True, slots=True)
class RecoveryGeneration:
    """The value carried by a committed recovery journal."""

    pipeline: str
    namespace: str
    recovery_id: str
    decision: str

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        _require_text(self.namespace, name="namespace")
        _require_text(self.recovery_id, name="recovery_id")
        _require_text(self.decision, name="decision")


@dataclass(frozen=True, slots=True)
class SlotState:
    """The value carried by a durable source-slot observation."""

    decision: str
    slot_name: str
    system_identifier: Any = None
    timeline_id: Any = None
    restart_lsn: Any = None
    confirmed_flush_lsn: Any = None
    current_wal_lsn: Any = None
    durable_lsn: Any = None

    def __post_init__(self) -> None:
        _require_text(self.decision, name="decision")
        _require_text(self.slot_name, name="slot_name")

    @classmethod
    def from_mapping(
        cls,
        state: Mapping[str, Any],
        *,
        decision: str,
        slot_name: str,
    ) -> SlotState:
        """Capture the actual slot verdict mapping, not an exception string."""
        return cls(
            decision=decision,
            slot_name=slot_name,
            system_identifier=state.get("system_identifier"),
            timeline_id=state.get("timeline_id"),
            restart_lsn=state.get("restart_lsn"),
            confirmed_flush_lsn=state.get("confirmed_flush_lsn"),
            current_wal_lsn=state.get("current_wal_lsn"),
            durable_lsn=state.get("durable_lsn"),
        )


@dataclass(frozen=True, slots=True)
class OffsetRowState:
    """The value carried by a durable offset row."""

    pipeline: str
    namespace: str
    resume_json: str
    commit_id: int
    snapshot_epoch: int
    last_lsn: int
    updated_at: Any

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        _require_text(self.namespace, name="namespace")
        if not isinstance(self.resume_json, str):
            raise TypeError("resume_json must be a string")
        for name, value in (
            ("commit_id", self.commit_id),
            ("snapshot_epoch", self.snapshot_epoch),
            ("last_lsn", self.last_lsn),
        ):
            if not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")


class CommitReservation:
    """Opaque run-owned reservation used by the pre-COMMIT watchdog.

    This is deliberately not a durability receipt: A-3 arms the watchdog before
    COMMIT. The reservation is allocated from the current run's monotone commit
    id, so it has the same intentional pre-durability ownership as ``RunState``.
    """

    __slots__ = ("_commit_id", "_pipeline")

    def __new__(cls, *args: object, **kwargs: object) -> CommitReservation:
        raise TypeError(
            "CommitReservation is opaque; use the current run's reserved commit id"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("CommitReservation is immutable")

    @property
    def commit_id(self) -> int:
        return self._commit_id


def _commit_reservation(pipeline: str, commit_id: int) -> CommitReservation:
    _require_text(pipeline, name="pipeline")
    if not isinstance(commit_id, int) or commit_id < 0:
        raise TypeError("commit_id must be a non-negative integer")
    instance = object.__new__(CommitReservation)
    object.__setattr__(instance, "_pipeline", pipeline)
    object.__setattr__(instance, "_commit_id", commit_id)
    return instance


@dataclass(frozen=True, slots=True)
class LeaseState:
    """The value carried by a durable lease owner observed by a failure."""

    pipeline: str
    owner_id: str
    operation: str

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        _require_text(self.owner_id, name="owner_id")
        _require_text(self.operation, name="operation")


class _DurableReceipt:
    """Opaque proof that a state owner has crossed its durability boundary.

    Receipts are deliberately not constructible from caller data.  The private
    issue functions below are called only after their owning writer has committed
    or fsynced/installed the state.  A process that wants to forge one would have
    to deliberately subvert this type with ``object.__new__``; that is the same
    accepted residual as the opaque ``OccurrenceKey`` itself.
    """

    __slots__ = ("_details", "_state")

    def __new__(cls, *args: object, **kwargs: object) -> _DurableReceipt:
        raise TypeError(
            f"{cls.__name__} is opaque; use the receipt returned by its durable writer"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    @property
    def state(self) -> object:
        return self._state

    @property
    def details(self) -> Mapping[str, Any]:
        return self._details


class EpisodeReceipt(_DurableReceipt):
    """Proof that an episode row or sidecar episode is durable."""

    __slots__ = ()

    @property
    def episode_id(self) -> int:
        return self._state.episode_id

    @property
    def pipeline(self) -> str:
        return self._state.pipeline

    @property
    def state_name(self) -> str:
        return self._state.state

    @property
    def observed_at(self) -> Any:
        return self._state.observed_at

    def as_dict(self) -> dict[str, Any]:
        return self._state.as_dict()

    def __getitem__(self, key: str) -> Any:
        return self.as_dict()[key]


class RecoveryJournalReceipt(_DurableReceipt):
    """Proof that the recovery journal transaction committed."""

    __slots__ = ()

    @property
    def recovery_id(self) -> str:
        return self._state.recovery_id

    @property
    def pipeline(self) -> str:
        return self._state.pipeline

    @property
    def namespace(self) -> str:
        return self._state.namespace


class SlotStateReceipt(_DurableReceipt, Mapping[str, Any]):
    """Proof that the slot observation row committed."""

    __slots__ = ()

    def __getitem__(self, key: str) -> Any:
        return self._details[key]

    def __iter__(self):
        return iter(self._details)

    def __len__(self) -> int:
        return len(self._details)

    def as_dict(self) -> dict[str, Any]:
        return dict(self._details)


class OffsetRowReceipt(_DurableReceipt):
    """Proof that the unusable offset row was read from durable state."""

    __slots__ = ()


class LeaseReceipt(_DurableReceipt):
    """Proof that the competing lease row is durable."""

    __slots__ = ()


def _issue_receipt(receipt_type: type[_DurableReceipt], state: object, details=None):
    """Issue a receipt at a first-party durable-write/read boundary only."""
    expected = {
        EpisodeReceipt: EpisodeState,
        RecoveryJournalReceipt: RecoveryGeneration,
        SlotStateReceipt: SlotState,
        OffsetRowReceipt: OffsetRowState,
        LeaseReceipt: LeaseState,
    }[receipt_type]
    _require_state(state, expected, receipt_type.__name__)
    instance = object.__new__(receipt_type)
    object.__setattr__(instance, "_state", state)
    object.__setattr__(
        instance,
        "_details",
        MappingProxyType(dict(details or {})),
    )
    return instance


# These are intentionally private: there is no public ``from_state`` or string
# helper on any receipt.  The callers are the state-owning write/read boundaries,
# and they invoke these only after the durable operation has succeeded.
def _episode_receipt_after_durable(state: EpisodeState) -> EpisodeReceipt:
    return _issue_receipt(EpisodeReceipt, state)


def _recovery_journal_receipt_after_commit(
    state: RecoveryGeneration,
) -> RecoveryJournalReceipt:
    return _issue_receipt(RecoveryJournalReceipt, state)


def _slot_state_receipt_after_commit(
    state: SlotState, details: Mapping[str, Any]
) -> SlotStateReceipt:
    return _issue_receipt(SlotStateReceipt, state, details)


def _offset_row_receipt_from_durable(
    state: OffsetRowState,
) -> OffsetRowReceipt:
    return _issue_receipt(OffsetRowReceipt, state)


def _lease_receipt_from_durable(state: LeaseState) -> LeaseReceipt:
    return _issue_receipt(LeaseReceipt, state)


class OccurrenceKey:
    """Opaque nominal identity for exactly one durable alert occurrence.

    Calling ``OccurrenceKey(...)`` is intentionally always an error.  The only
    construction paths below validate a receipt from a state owner and mint the
    value internally. In particular, there is no constructor, ``from_string``,
    or ``unsafe`` path that can turn exception text or an undurable state into an
    occurrence. ``from_run`` is the intentional run-owned exception.
    """

    __slots__ = ("_text",)

    def __new__(cls, *args: object, **kwargs: object) -> OccurrenceKey:
        raise TypeError(
            "OccurrenceKey is opaque; use a named factory with durable or run-owned state"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("OccurrenceKey is immutable")

    @classmethod
    def from_episode(cls, receipt: EpisodeReceipt) -> OccurrenceKey:
        _require_receipt(receipt, EpisodeReceipt, "from_episode")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"episode:{receipt.state.episode_id}")
        return instance

    @classmethod
    def from_recovery_generation(
        cls, receipt: RecoveryJournalReceipt
    ) -> OccurrenceKey:
        _require_receipt(
            receipt, RecoveryJournalReceipt, "from_recovery_generation"
        )
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"recovery:{receipt.state.recovery_id}")
        return instance

    @classmethod
    def from_run(cls, run: RunState) -> OccurrenceKey:
        _require_state(run, RunState, "from_run")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"run:{run.runner_id}")
        return instance

    @classmethod
    def from_slot_state(cls, receipt: SlotStateReceipt) -> OccurrenceKey:
        _require_receipt(receipt, SlotStateReceipt, "from_slot_state")
        state = receipt.state
        parts = (
            state.decision,
            state.slot_name,
            state.system_identifier,
            state.timeline_id,
            state.restart_lsn,
            state.confirmed_flush_lsn,
            state.current_wal_lsn,
            state.durable_lsn,
        )
        instance = object.__new__(cls)
        object.__setattr__(
            instance, "_text", "slot-state:" + ":".join(str(value) for value in parts)
        )
        return instance

    @classmethod
    def from_offset_row(cls, receipt: OffsetRowReceipt) -> OccurrenceKey:
        _require_receipt(receipt, OffsetRowReceipt, "from_offset_row")
        row = receipt.state
        resume_digest = hashlib.sha256(row.resume_json.encode("utf-8")).hexdigest()
        updated = (
            row.updated_at.isoformat()
            if hasattr(row.updated_at, "isoformat")
            else str(row.updated_at)
        )
        instance = object.__new__(cls)
        object.__setattr__(
            instance,
            "_text",
            f"offset-row:{row.pipeline}:{row.namespace}:commit:{row.commit_id}:"
            f"snapshot:{row.snapshot_epoch}:last-lsn:{row.last_lsn}:updated:{updated}:"
            f"state:{resume_digest}",
        )
        return instance

    @classmethod
    def from_commit(cls, reservation: CommitReservation) -> OccurrenceKey:
        _require_receipt(reservation, CommitReservation, "from_commit")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"commit:{reservation.commit_id}")
        return instance

    @classmethod
    def from_lease(cls, receipt: LeaseReceipt) -> OccurrenceKey:
        _require_receipt(receipt, LeaseReceipt, "from_lease")
        lease = receipt.state
        instance = object.__new__(cls)
        object.__setattr__(
            instance, "_text", f"{lease.operation}:{lease.pipeline}:{lease.owner_id}"
        )
        return instance

    @property
    def text(self) -> str:
        return self._text

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return f"OccurrenceKey({self._text!r})"

    def __hash__(self) -> int:
        return hash(self._text)

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and self._text == other._text


def _require_state(value: object, expected: type, factory: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"OccurrenceKey.{factory} requires {expected.__name__}")


def _require_receipt(
    value: object, expected: type, factory: str
) -> None:
    if type(value) is not expected:
        raise TypeError(
            f"OccurrenceKey.{factory} requires {expected.__name__}; "
            "the durable write receipt is missing"
        )


def occurrence_text(value: object) -> str:
    """Runtime gate used by alert sinks before they touch a connection."""
    if type(value) is not OccurrenceKey:
        raise TypeError("occurrence_key must be an OccurrenceKey")
    return value.text


__all__ = [
    "CommitReservation",
    "EpisodeReceipt",
    "EpisodeState",
    "LeaseReceipt",
    "LeaseState",
    "OccurrenceKey",
    "OffsetRowReceipt",
    "OffsetRowState",
    "RecoveryGeneration",
    "RecoveryJournalReceipt",
    "RunState",
    "SlotState",
    "SlotStateReceipt",
    "occurrence_text",
]
