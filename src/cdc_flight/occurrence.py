"""Opaque alert-occurrence identities and their state-owned inputs.

An exception message is useful for grouping a condition, but it is not an
occurrence.  Occurrences come from a durable row, a run, or another state
machine that owns the lifetime of the incident.  This module keeps that
distinction at the API boundary: ``OccurrenceKey`` has no public constructor
for text, and the alert sink accepts only an instance minted by one of its
named state factories.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
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
    """A durable source-health or fallback-alert episode."""

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
    """The durable recovery journal generation being reported."""

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
    """The source-slot state used to decide whether recovery is required."""

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
    """The durable offset row that owns an unusable resume-point occurrence."""

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


@dataclass(frozen=True, slots=True)
class CommitState:
    """The run-owned commit identity used by the commit watchdog."""

    pipeline: str
    commit_id: int

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        if not isinstance(self.commit_id, int) or self.commit_id < 0:
            raise TypeError("commit_id must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class LeaseState:
    """The durable lease owner observed by an acquire or renew failure."""

    pipeline: str
    owner_id: str
    operation: str

    def __post_init__(self) -> None:
        _require_text(self.pipeline, name="pipeline")
        _require_text(self.owner_id, name="owner_id")
        _require_text(self.operation, name="operation")


class OccurrenceKey:
    """Opaque nominal identity for exactly one durable alert occurrence.

    Calling ``OccurrenceKey(...)`` is intentionally always an error.  The only
    construction paths below validate a state-owner object and mint the value
    internally.  In particular, there is no constructor, ``from_string``, or
    ``unsafe`` path that can turn exception text into an occurrence.
    """

    __slots__ = ("_text",)

    def __new__(cls, *args: object, **kwargs: object) -> OccurrenceKey:
        raise TypeError(
            "OccurrenceKey is opaque; use a named factory with durable or run-owned state"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("OccurrenceKey is immutable")

    @classmethod
    def from_episode(cls, episode: EpisodeState) -> OccurrenceKey:
        _require_state(episode, EpisodeState, "from_episode")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"episode:{episode.episode_id}")
        return instance

    @classmethod
    def from_recovery_generation(
        cls, generation: RecoveryGeneration
    ) -> OccurrenceKey:
        _require_state(generation, RecoveryGeneration, "from_recovery_generation")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"recovery:{generation.recovery_id}")
        return instance

    @classmethod
    def from_run(cls, run: RunState) -> OccurrenceKey:
        _require_state(run, RunState, "from_run")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"run:{run.runner_id}")
        return instance

    @classmethod
    def from_slot_state(cls, state: SlotState) -> OccurrenceKey:
        _require_state(state, SlotState, "from_slot_state")
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
    def from_offset_row(cls, row: OffsetRowState) -> OccurrenceKey:
        _require_state(row, OffsetRowState, "from_offset_row")
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
    def from_commit(cls, commit: CommitState) -> OccurrenceKey:
        _require_state(commit, CommitState, "from_commit")
        instance = object.__new__(cls)
        object.__setattr__(instance, "_text", f"commit:{commit.commit_id}")
        return instance

    @classmethod
    def from_lease(cls, lease: LeaseState) -> OccurrenceKey:
        _require_state(lease, LeaseState, "from_lease")
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


def occurrence_text(value: object) -> str:
    """Runtime gate used by alert sinks before they touch a connection."""
    if type(value) is not OccurrenceKey:
        raise TypeError("occurrence_key must be an OccurrenceKey")
    return value.text


__all__ = [
    "CommitState",
    "EpisodeState",
    "LeaseState",
    "OccurrenceKey",
    "OffsetRowState",
    "RecoveryGeneration",
    "RunState",
    "SlotState",
    "occurrence_text",
]
