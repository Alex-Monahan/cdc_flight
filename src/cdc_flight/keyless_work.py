"""Keyless physical-row folding.

Keyless tables do not have a source key fold.  This owner keeps their event
identity ledger and source-order physical operations separate from keyed table
folding, while leaving destination SQL execution to ``table_writer``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .envelope import PendingRecord
from .event_ledger import payload_digest
from .row_patch import RowPatch
from .typed_types import FieldState

OWNER = "keyless-physical-fold"


@dataclass(frozen=True)
class KeylessOperation:
    """One source-order physical operation for a keyless table."""

    event_id: str
    operation: str
    after: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    image_digest: str | None = None
    delete_mode: str = "hard"
    source_lsn: int | None = None
    txn_id: str | None = None
    total_order: int | None = None


def collect(
    item,
    event: PendingRecord,
    row: dict[str, Any],
    event_id: str,
    *,
    probe=None,
    patch: RowPatch | None = None,
) -> None:
    """Fold one keyless event into source-order physical operations."""
    if (
        not item.snapshot
        and probe is not None
        and hasattr(probe, "keyless_event_applied")
        and probe.keyless_event_applied(item, event_id)
    ):
        return
    if event_id in item.keyless_event_ids:
        from .errors import AmbiguousDelete

        raise AmbiguousDelete(
            f"{item.target}: keyless event identity {event_id!r} appeared twice "
            "in one commit group; refusing to apply an un-attributable physical "
            "operation",
            source_schema=item.source_schema,
            source_table=item.source_table,
            target=item.target,
        )
    patch = patch or RowPatch.from_event(
        event,
        commit_id=0,
        event_id=event_id,
        snapshot=item.snapshot,
    )
    if event.op in {"d", "u"}:
        before_patch = (
            patch
            if event.op == "d"
            else RowPatch.from_image(
                event.before,
                event.before_descriptors,
                typed=event.typed_before,
                complete=True,
            )
        )
        before = complete_image(item, before_patch, after=False)
        after = patch.encoded_values() if event.op == "u" else None
        if event.op == "u":
            # A sparse after-image cannot reconstruct a replacement row without
            # reading and composing the matched physical row.  Refuse that
            # impossible image rather than inventing unchanged values.
            complete_image(item, patch, after=True)
        operation = KeylessOperation(
            event_id=event_id,
            operation=event.op,
            before=before,
            after=after,
            image_digest=payload_digest(event),
            delete_mode=getattr(event, "delete_mode", None) or "hard",
            source_lsn=event.lsn,
            txn_id=event.txn_id,
            total_order=event.total_order,
        )
        item.keyless_operations.append(operation)
        if event.op == "d":
            item.delete_effects[event_id] = operation
    else:
        if patch.has_marker():
            _missing_toast_base(item, reason="keyless sparse change has no safe base")
        item.keyless_operations.append(
            KeylessOperation(
                event_id=event_id,
                operation=event.op,
                after=patch.encoded_values(),
                image_digest=payload_digest(event),
                delete_mode=getattr(event, "delete_mode", None) or "hard",
                source_lsn=event.lsn,
                txn_id=event.txn_id,
                total_order=event.total_order,
            )
        )
    item.keyless_event_ids.add(event_id)
    if not item.snapshot:
        item.keyless_ledger.append((event_id, event.op, payload_digest(event)))


def complete_image(item, patch: RowPatch, *, after: bool) -> dict[str, Any]:
    """Return a complete, bindable source image for a physical match."""
    from .table_work import APPLIER_COLUMN_TYPES

    source_columns = tuple(
        column for column in item.native_columns if column not in APPLIER_COLUMN_TYPES
    )
    if not source_columns:
        _missing_toast_base(
            item,
            reason="keyless physical operation has no catalog-authoritative source columns",
        )
    encoded = patch.encoded_values()
    result: dict[str, Any] = {}
    for column in source_columns:
        field = patch.field(column)
        if field.state in {FieldState.UNCHANGED_TOAST, FieldState.ABSENT}:
            _missing_toast_base(
                item,
                reason=(
                    "keyless UPDATE after-image"
                    if after
                    else "keyless DELETE before-image"
                )
                + f" has no complete value for column {column!r}",
            )
        if field.state not in {FieldState.VALUE, FieldState.EXPLICIT_NULL}:
            _missing_toast_base(item, reason="keyless image has no safe base")
        # None is a source NULL only after the field-state check above; it is
        # never used to stand in for an absent value.
        result[column] = encoded.get(column)
    return result


def _missing_toast_base(item, *, reason: str = "") -> None:
    from .errors import ToastBaseMissing

    detail = reason or "a sparse TOAST patch has no verified physical-row base"
    raise ToastBaseMissing(
        f"{item.target}: {detail}; refusing the commit group so automatic "
        "table-scoped refetch/resnapshot can repair the base",
        source_schema=item.source_schema,
        source_table=item.source_table,
        target=item.target,
    )
