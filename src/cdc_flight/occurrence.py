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
    # Added after the original state shape so raw-state callers that only want to
    # describe a slot remain source-compatible.  A receipt can only be issued when
    # this is populated by the pipeline-owned writer/read boundary.
    pipeline: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.decision, name="decision")
        _require_text(self.slot_name, name="slot_name")
        if self.pipeline is not None:
            _require_text(self.pipeline, name="pipeline")

    @classmethod
    def from_mapping(
        cls,
        state: Mapping[str, Any],
        *,
        decision: str,
        slot_name: str,
        pipeline: str | None = None,
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
            pipeline=pipeline,
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

    @property
    def pipeline(self) -> str:
        return self._pipeline


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
    if receipt_type is SlotStateReceipt and not isinstance(state.pipeline, str):
        raise ValueError("SlotStateReceipt requires a pipeline-bound slot state")
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
def _episode_receipt_after_durable(
    state: EpisodeState, details: Mapping[str, Any] | None = None
) -> EpisodeReceipt:
    return _issue_receipt(EpisodeReceipt, state, details)


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


def _lease_receipt_from_durable(
    state: LeaseState, details: Mapping[str, Any] | None = None
) -> LeaseReceipt:
    return _issue_receipt(LeaseReceipt, state, details)


class OccurrenceKey:
    """Opaque nominal identity for exactly one durable alert occurrence.

    Calling ``OccurrenceKey(...)`` is intentionally always an error.  The only
    construction paths below validate a receipt from a state owner and mint the
    value internally. In particular, there is no constructor, ``from_string``,
    or ``unsafe`` path that can turn exception text or an undurable state into an
    occurrence. ``from_run`` is the intentional run-owned exception.
    """

    __slots__ = ("_binding", "_text")

    def __new__(cls, *args: object, **kwargs: object) -> OccurrenceKey:
        raise TypeError(
            "OccurrenceKey is opaque; use a named factory with durable or run-owned state"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("OccurrenceKey is immutable")

    @classmethod
    def _from_binding(cls, text: str, binding: tuple[Any, ...]) -> OccurrenceKey:
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", text)
        object.__setattr__(instance, "_binding", binding)
        return instance

    @classmethod
    def from_episode(
        cls, receipt: EpisodeReceipt, *, pipeline: str | None = None
    ) -> OccurrenceKey:
        _require_receipt(receipt, EpisodeReceipt, "from_episode")
        bound_pipeline = _bound_pipeline(receipt.state.pipeline, pipeline, "from_episode")
        owner = str(receipt.details.get("owner") or "source_health")
        path = receipt.details.get("path")
        return cls._from_binding(
            f"episode:{receipt.state.episode_id}",
            (
                "episode",
                owner,
                bound_pipeline,
                receipt.state.episode_id,
                receipt.state.state,
                str(path) if path is not None else None,
            ),
        )

    @classmethod
    def from_recovery_generation(
        cls,
        receipt: RecoveryJournalReceipt,
        *,
        pipeline: str | None = None,
        namespace: str | None = None,
    ) -> OccurrenceKey:
        _require_receipt(
            receipt, RecoveryJournalReceipt, "from_recovery_generation"
        )
        bound_pipeline = _bound_pipeline(
            receipt.state.pipeline, pipeline, "from_recovery_generation"
        )
        bound_namespace = _bound_text(
            receipt.state.namespace, namespace, "namespace", "from_recovery_generation"
        )
        return cls._from_binding(
            f"recovery:{receipt.state.recovery_id}",
            (
                "recovery",
                bound_pipeline,
                bound_namespace,
                receipt.state.recovery_id,
                receipt.state.decision,
            ),
        )

    @classmethod
    def from_run(
        cls, run: RunState, *, pipeline: str | None = None
    ) -> OccurrenceKey:
        _require_state(run, RunState, "from_run")
        bound_pipeline = _bound_pipeline(run.pipeline, pipeline, "from_run")
        return cls._from_binding(
            f"run:{run.runner_id}", ("run", bound_pipeline, run.runner_id)
        )

    @classmethod
    def from_slot_state(
        cls,
        receipt: SlotStateReceipt,
        *,
        pipeline: str | None = None,
        slot_name: str | None = None,
    ) -> OccurrenceKey:
        _require_receipt(receipt, SlotStateReceipt, "from_slot_state")
        state = receipt.state
        bound_pipeline = _bound_pipeline(state.pipeline, pipeline, "from_slot_state")
        bound_slot = _bound_text(
            state.slot_name, slot_name, "slot_name", "from_slot_state"
        )
        parts = (
            state.decision,
            bound_slot,
            state.system_identifier,
            state.timeline_id,
            state.restart_lsn,
            state.confirmed_flush_lsn,
            state.current_wal_lsn,
            state.durable_lsn,
        )
        return cls._from_binding(
            "slot-state:" + ":".join(str(value) for value in parts),
            (
                "slot",
                bound_pipeline,
                bound_slot,
                state.decision,
                state.system_identifier,
                state.timeline_id,
                state.restart_lsn,
                state.confirmed_flush_lsn,
                state.current_wal_lsn,
                state.durable_lsn,
            ),
        )

    @classmethod
    def from_offset_row(
        cls,
        receipt: OffsetRowReceipt,
        *,
        pipeline: str | None = None,
        namespace: str | None = None,
    ) -> OccurrenceKey:
        _require_receipt(receipt, OffsetRowReceipt, "from_offset_row")
        row = receipt.state
        bound_pipeline = _bound_pipeline(row.pipeline, pipeline, "from_offset_row")
        bound_namespace = _bound_text(
            row.namespace, namespace, "namespace", "from_offset_row"
        )
        resume_digest = hashlib.sha256(row.resume_json.encode("utf-8")).hexdigest()
        updated = (
            row.updated_at.isoformat()
            if hasattr(row.updated_at, "isoformat")
            else str(row.updated_at)
        )
        return cls._from_binding(
            f"offset-row:{row.pipeline}:{row.namespace}:commit:{row.commit_id}:"
            f"snapshot:{row.snapshot_epoch}:last-lsn:{row.last_lsn}:updated:{updated}:"
            f"state:{resume_digest}",
            (
                "offset",
                bound_pipeline,
                bound_namespace,
                row.commit_id,
                row.snapshot_epoch,
                row.last_lsn,
                updated,
                resume_digest,
            ),
        )

    @classmethod
    def from_commit(
        cls, reservation: CommitReservation, *, pipeline: str | None = None
    ) -> OccurrenceKey:
        _require_receipt(reservation, CommitReservation, "from_commit")
        bound_pipeline = _bound_pipeline(
            reservation.pipeline, pipeline, "from_commit"
        )
        return cls._from_binding(
            f"commit:{reservation.commit_id}",
            ("commit", bound_pipeline, reservation.commit_id),
        )

    @classmethod
    def from_lease(
        cls,
        receipt: LeaseReceipt,
        *,
        pipeline: str | None = None,
        owner_id: str | None = None,
    ) -> OccurrenceKey:
        _require_receipt(receipt, LeaseReceipt, "from_lease")
        lease = receipt.state
        bound_pipeline = _bound_pipeline(lease.pipeline, None, "from_lease")
        alert_pipeline = str(
            receipt.details.get("alert_pipeline") or lease.pipeline
        )
        bound_alert_pipeline = _bound_text(
            alert_pipeline, pipeline, "pipeline", "from_lease"
        )
        bound_owner = _bound_text(
            lease.owner_id, owner_id, "owner_id", "from_lease"
        )
        lease_stamp = _stable_value(receipt.details.get("acquired_at"))
        return cls._from_binding(
            f"{lease.operation}:{lease.pipeline}:{lease.owner_id}",
            (
                "lease",
                bound_alert_pipeline,
                bound_pipeline,
                bound_owner,
                lease.operation,
                lease_stamp,
            ),
        )

    @property
    def text(self) -> str:
        return self._text

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return f"OccurrenceKey({self._text!r})"

    def __hash__(self) -> int:
        return hash((self._text, self._binding))

    def __eq__(self, other: object) -> bool:
        return (
            type(other) is type(self)
            and self._text == other._text
            and self._binding == other._binding
        )


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


def _stable_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _bound_text(
    actual: str, expected: str | None, name: str, factory: str
) -> str:
    if expected is not None:
        _require_text(expected, name=name)
        if expected != actual:
            raise ValueError(
                f"OccurrenceKey.{factory} receipt names {name}={actual!r}, "
                f"not {expected!r}"
            )
    return actual


def _bound_pipeline(actual: str | None, expected: str | None, factory: str) -> str:
    if not isinstance(actual, str) or not actual.strip():
        raise TypeError(
            f"OccurrenceKey.{factory} requires a receipt bound to a pipeline"
        )
    return _bound_text(actual, expected, "pipeline", factory)


def _occurrence_binding(value: object) -> tuple[Any, ...]:
    """Return the factory-owned binding for the alert boundary.

    This is intentionally an internal nominal gate.  It exposes no constructor or
    setter; ordinary callers can only obtain the tuple from a key already minted by
    one of the factories above.
    """
    if type(value) is not OccurrenceKey:
        raise TypeError("occurrence_key must be an OccurrenceKey")
    return value._binding


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
    "_occurrence_binding",
    "occurrence_text",
]
