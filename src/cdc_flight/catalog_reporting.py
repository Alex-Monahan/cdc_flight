"""Diagnostics projection for the catalog watcher.

The watcher owns observations and transitions; this module owns the read-only report
assembled from them. Keeping that projection out of ``catalog.py`` leaves the polling
and change-folding code below the maintainability boundary without hiding state writes
behind a generic utility.
"""

from __future__ import annotations

from .catalog_state import CHANGE_NEW, CHANGE_SCHEMA, CHANGE_UNPUBLISHED, DESTRUCTIVE
from .machines import ADMISSION_ADMITTED, ADMISSION_EXTERNAL


def summary(watcher) -> dict:
    """Return the stable operational summary for one quiesced or live watcher."""
    with watcher._lock:
        pending = watcher._live()
        pending_admission = [
            change.qualified
            for change in pending
            if change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
            and (
                watcher.known.get(change.qualified) is None
                or watcher.known[change.qualified].admission_state
                not in {ADMISSION_ADMITTED, ADMISSION_EXTERNAL}
                or not watcher.known[change.qualified].published
            )
        ]
        admission_errors = dict(watcher._admission_errors)
        schema_liveness = dict(watcher._schema_liveness)
    return {
        "catalog_polls": watcher.polls,
        "catalog_successful_polls": watcher.successful_polls,
        "catalog_unrelatable": sorted(watcher.unrelatable),
        "catalog_machine_error": watcher.machine_error,
        "catalog_empty_polls": watcher.empty_polls,
        "catalog_markers": watcher.marker.writes,
        "catalog_pending": len(pending),
        "catalog_pending_destructive": sum(
            1 for change in pending if change.kind in DESTRUCTIVE
        ),
        "catalog_pending_schema": sum(
            1 for change in pending if change.kind == CHANGE_SCHEMA
        ),
        "catalog_superseded": watcher.superseded,
        "catalog_error": watcher.last_error,
        "catalog_marker_error": watcher.marker.last_error,
        "catalog_marker_capable": watcher.marker.capable,
        "catalog_publication_ownership": watcher.publication_ownership,
        "catalog_pending_admission": sorted(pending_admission),
        "catalog_admission_errors": admission_errors,
        "catalog_schema_liveness": schema_liveness,
    }
