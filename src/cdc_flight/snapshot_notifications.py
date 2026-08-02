"""Decode Debezium's ordered snapshot notification records."""

from __future__ import annotations

import json
from dataclasses import dataclass

from .snapshot_completion import SnapshotObservationError

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
