"""Versioned hard/soft delete policy.

Delete mode is a policy decision, not a property inferred from a row image.  This
module keeps parsing and precedence in one place so the stream, snapshot, backfill,
discovery, and re-snapshot callers all resolve the same answer.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from . import naming
from .errors import AdmissionError

DELETE_MODES = ("hard", "soft")


class DeleteModeConfigurationError(AdmissionError):
    """The delete-mode manifest cannot be applied safely."""


def _canonical_table(value: str) -> str:
    parts = str(value).strip().split(".")
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise DeleteModeConfigurationError(
            "delete-mode table names must be exactly schema.table"
        )
    return ".".join(naming.normalize(part.strip()) for part in parts)


@dataclass(frozen=True, repr=False)
class DeleteModeResolver:
    """Resolve a global mode plus exact fully-qualified table overrides."""

    global_mode: str = "hard"
    overrides: tuple[tuple[str, str], ...] = ()
    epoch: int = 1
    digest: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        mode = str(self.global_mode).strip().lower()
        if mode not in DELETE_MODES:
            raise DeleteModeConfigurationError(
                f"delete mode must be one of {DELETE_MODES}, got {self.global_mode!r}"
            )
        object.__setattr__(self, "global_mode", mode)
        normalized: dict[str, str] = {}
        for table, override in self.overrides:
            canonical = _canonical_table(table)
            resolved = str(override).strip().lower()
            if resolved not in DELETE_MODES:
                raise DeleteModeConfigurationError(
                    f"delete mode override for {canonical} is not one of {DELETE_MODES}"
                )
            previous = normalized.get(canonical)
            if previous is not None and previous != resolved:
                raise DeleteModeConfigurationError(
                    f"conflicting delete mode overrides for {canonical}"
                )
            normalized[canonical] = resolved
        canonical_overrides = tuple(sorted(normalized.items()))
        object.__setattr__(self, "overrides", canonical_overrides)
        if int(self.epoch) < 1:
            raise DeleteModeConfigurationError("delete policy epoch must be positive")
        if not self.digest:
            payload = {
                "version": 1,
                "global_mode": mode,
                "overrides": list(canonical_overrides),
                "epoch": int(self.epoch),
            }
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            object.__setattr__(self, "digest", hashlib.sha256(encoded.encode()).hexdigest())

    @classmethod
    def from_environment(cls) -> DeleteModeResolver:
        raw_rules = os.environ.get("CDC_DELETE_MODE_RULES", "")
        overrides: list[tuple[str, str]] = []
        if raw_rules.strip():
            try:
                parsed = json.loads(raw_rules)
            except json.JSONDecodeError as exc:
                raise DeleteModeConfigurationError(
                    "CDC_DELETE_MODE_RULES must be valid JSON"
                ) from exc
            if not isinstance(parsed, Mapping):
                raise DeleteModeConfigurationError(
                    "CDC_DELETE_MODE_RULES must be a JSON object of schema.table to mode"
                )
            overrides = [(str(table), str(mode)) for table, mode in parsed.items()]
        return cls(
            global_mode=os.environ.get("CDC_DELETE_MODE", "hard"),
            overrides=tuple(overrides),
            epoch=int(os.environ.get("CDC_DELETE_POLICY_EPOCH", "1")),
        )

    def resolve(self, qualified_table: str | None) -> str:
        if not qualified_table:
            return self.global_mode
        table = _canonical_table(qualified_table)
        return dict(self.overrides).get(table, self.global_mode)

    def canonical_manifest(self) -> dict[str, Any]:
        return {
            "version": 1,
            "global_mode": self.global_mode,
            "overrides": dict(self.overrides),
            "epoch": self.epoch,
            "digest": self.digest,
        }

    def __repr__(self) -> str:
        return (
            "DeleteModeResolver("  # no secret material exists in this object
            f"global_mode={self.global_mode!r}, overrides={dict(self.overrides)!r}, "
            f"epoch={self.epoch}, digest={self.digest!r})"
        )


__all__ = ["DELETE_MODES", "DeleteModeConfigurationError", "DeleteModeResolver"]
