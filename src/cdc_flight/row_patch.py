"""Typed sparse row patches for the 2.6 physical-row fold."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from . import naming
from .toast import STRUCTURAL_MARKER, field_value
from .typed_types import FieldState, FieldValue, SourceTypeDescriptor, TypedImage, encode_value

PATCH_VERSION = 1


def _jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}
    if isinstance(value, bytearray):
        return {"__bytes__": bytes(value).hex()}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return {"__iso__": value.isoformat()}
        except (AttributeError, TypeError, ValueError):
            pass
    return value


def _field_dict(value: FieldValue) -> dict[str, Any]:
    return value.to_dict()


def _marker_candidate(value: Any) -> bool:
    """Cheap gate before invoking the descriptor-aware marker classifier."""
    if isinstance(value, str):
        return STRUCTURAL_MARKER in value or r"\u0000" in value
    if isinstance(value, dict):
        return any(_marker_candidate(key) or _marker_candidate(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_marker_candidate(item) for item in value)
    return False


@dataclass
class RowPatch:
    """A closed, composable sparse image.

    ``fields`` retains marker dispositions for the digest and spill codec, but
    ``bindable_values`` filters them before any Arrow or SQL construction.  A
    marker therefore cannot accidentally become a NULL/string/blob assignment.
    """

    fields: dict[str, FieldValue] = field(default_factory=dict)
    absent: tuple[str, ...] = ()
    complete: bool = False
    _encoded_cache: dict[str, Any] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.fields = {str(name): value for name, value in self.fields.items()}
        self.absent = tuple(sorted({str(name) for name in self.absent}))
        for name, value in self.fields.items():
            if not isinstance(value, FieldValue):
                raise TypeError(f"RowPatch field {name!r} is not a FieldValue")
            if value.state is FieldState.UNCHANGED_TOAST and value.value is not None:
                raise ValueError(
                    f"RowPatch field {name!r} carries both a marker disposition and a value"
                )

    @classmethod
    def from_image(
        cls,
        image: dict[str, Any] | None,
        descriptors: dict[str, SourceTypeDescriptor] | None = None,
        *,
        typed: TypedImage | None = None,
        complete: bool = False,
        binary_mode: str = "base64",
        hstore_mode: str = "map",
    ) -> RowPatch:
        descriptors = descriptors or {}
        image = image or {}
        typed_fields = dict(typed.fields) if typed is not None else {}
        fields: dict[str, FieldValue] = {}
        for raw_name, raw_value in image.items():
            name = naming.normalize(str(raw_name))
            descriptor = descriptors.get(str(raw_name)) or descriptors.get(name)
            existing = typed_fields.get(str(raw_name)) or typed_fields.get(name)
            # The JSON schema image is built before the catalog descriptor is
            # enriched.  Re-run the narrow marker classifier for ordinary VALUE
            # fields so a U+0000 SourceRecord survives that enrichment boundary;
            # explicit marker/null states already carried by a spill image win.
            if existing is not None and existing.state is not FieldState.VALUE:
                fields[name] = existing
            elif not _marker_candidate(raw_value):
                fields[name] = FieldValue.of(raw_value, descriptor)
            else:
                fields[name] = field_value(
                    raw_value,
                    descriptor,
                    binary_mode=binary_mode,
                    hstore_mode=hstore_mode,
                )
        for raw_name, existing in typed_fields.items():
            name = naming.normalize(str(raw_name))
            if name not in fields:
                fields[name] = existing
        # Catalog-enriched descriptors describe the complete source row, while a
        # sparse UPDATE image contains only the fields present on the wire.  Retain
        # that distinction explicitly so spill/replay and the idempotency digest do
        # not silently turn ABSENT into an untracked omission.
        for raw_name, _descriptor in descriptors.items():
            name = naming.normalize(str(raw_name))
            if name not in fields:
                # The event descriptor map may be the previous catalog epoch while
                # a schema fence is being applied.  ABSENT is state, not evidence
                # that this stale column belongs in the destination DDL; keep the
                # descriptor out of the field so a post-rename row cannot recreate
                # the old physical column.
                fields[name] = FieldValue.absent()
        return cls(fields, complete=complete)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RowPatch:
        if value.get("version") not in (None, PATCH_VERSION):
            raise ValueError(f"unsupported RowPatch version {value.get('version')!r}")
        raw_fields = value.get("fields", {})
        return cls(
            {str(name): FieldValue.from_dict(raw) for name, raw in raw_fields.items()},
            tuple(value.get("absent", ())),
            bool(value.get("complete", False)),
        )

    @property
    def marker_columns(self) -> tuple[str, ...]:
        return tuple(sorted(name for name, value in self.fields.items() if value.state is FieldState.UNCHANGED_TOAST))

    @property
    def explicit_null_columns(self) -> tuple[str, ...]:
        return tuple(sorted(name for name, value in self.fields.items() if value.state is FieldState.EXPLICIT_NULL))

    def field(self, name: str) -> FieldValue:
        return self.fields.get(naming.normalize(name), FieldValue.absent())

    def bindable_values(self) -> dict[str, Any]:
        return {
            name: value.value
            for name, value in self.fields.items()
            if value.state in {FieldState.VALUE, FieldState.EXPLICIT_NULL}
        }

    def encoded_values(self) -> dict[str, Any]:
        if self._encoded_cache is None:
            result: dict[str, Any] = {}
            for name, value in self.fields.items():
                if value.state is FieldState.VALUE:
                    result[name] = (
                        encode_value(value.value, value.descriptor)
                        if value.descriptor
                        else value.value
                    )
                elif value.state is FieldState.EXPLICIT_NULL:
                    result[name] = None
            self._encoded_cache = result
        return dict(self._encoded_cache)

    def has_marker(self) -> bool:
        return bool(self.marker_columns)

    def compose(self, later: RowPatch) -> RowPatch:
        """Compose source-order patches; marker/absent are no-ops."""

        fields = dict(self.fields)
        absent = set(self.absent) | set(later.absent)
        for name, value in later.fields.items():
            if value.state in {FieldState.VALUE, FieldState.EXPLICIT_NULL}:
                fields[name] = value
                absent.discard(name)
            elif name not in fields:
                # Retain the disposition so the digest records that the source
                # carried an unchanged marker even though it has no bindable value.
                fields[name] = value
        return RowPatch(fields, tuple(absent), complete=self.complete or later.complete)

    @property
    def digest(self) -> str:
        payload = {
            "version": PATCH_VERSION,
            "complete": self.complete,
            "absent": list(self.absent),
            "fields": {
                name: {
                    "state": value.state.value,
                    "value": _jsonable(value.value),
                }
                for name, value in sorted(self.fields.items())
            },
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": PATCH_VERSION,
            "complete": self.complete,
            "absent": list(self.absent),
            "fields": {name: _field_dict(value) for name, value in sorted(self.fields.items())},
            "digest": self.digest,
        }

    @classmethod
    def from_event(
        cls,
        event,
        *,
        commit_id: int,
        event_id: str,
        snapshot: bool = False,
        binary_mode: str = "base64",
        hstore_mode: str = "map",
    ) -> RowPatch:
        image = event.before if event.op == "d" else event.after
        descriptors = (
            event.before_descriptors if event.op == "d" else event.after_descriptors
        )
        typed = event.typed_before if event.op == "d" else event.typed_after
        patch = cls.from_image(
            image,
            descriptors,
            typed=typed,
            complete=event.op in {"c", "r"} or snapshot,
            binary_mode=binary_mode,
            hstore_mode=hstore_mode,
        )
        # CDC metadata is part of the physical update.  It is not a source field,
        # but last-real-value-wins must update it alongside the sparse source image.
        metadata = {
            naming.CDCF_COMMIT_ID: commit_id,
            naming.CDCF_EVENT_ID: event_id,
            naming.CDCF_TOTAL_ORDER: None if snapshot else event.total_order,
            "dbz_op": event.op,
            "dbz_lsn": event.lsn,
            "dbz_tx_id": None if snapshot else _as_int(event.txn_id),
            "dbz_schema": event.schema,
            "dbz_table": event.table,
            "dbz_source_ts_ms": event.source_ts_ms,
        }
        for name, value in metadata.items():
            patch.fields[name] = FieldValue.of(value)
        return patch


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = ["PATCH_VERSION", "RowPatch"]
