"""Publication admission policy for catalog discovery.

Observation says that a relation exists; this module owns the separate decision that
the relation is streamable.  Keeping it out of ``catalog.py`` makes the two facts and
their failure/retry state explicit and keeps the watcher an observation coordinator.
"""

from __future__ import annotations

import logging
from dataclasses import replace

from .catalog_state import CHANGE_NEW, CHANGE_UNPUBLISHED
from .machines import (
    ADMISSION_ADMITTED,
    ADMISSION_ERROR,
    ADMISSION_EXTERNAL,
    ADMISSION_PENDING,
    ADMISSION_REFUSED,
    PUBLICATION_ADMISSION,
)
from .naming import quote

log = logging.getLogger("cdc_flight.catalog_admission")


def ensure_published(watcher, conn, observed, changes) -> None:
    """Retry every live discovery admission and update its durable candidate."""
    if not watcher.auto_discover:
        return
    with watcher._lock:
        pending = [
            change
            for change in watcher._live()
            if change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
            and change.qualified in observed
        ]
    by_name = {change.qualified: change for change in pending}
    by_name.update(
        {
            change.qualified: change
            for change in changes
            if change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
        }
    )
    for change in by_name.values():
        relation = observed.get(change.qualified)
        if relation is None or relation.is_partition:
            continue
        if relation.published or relation.publication_all_tables:
            state = relation.admission_state
            if state not in {ADMISSION_ADMITTED, ADMISSION_EXTERNAL}:
                state = ADMISSION_EXTERNAL
            updated = _with_state(relation, state)
            _record(watcher, change, updated, error=None)
            observed[change.qualified] = updated
            continue
        if watcher.publication_ownership == "external":
            reason = (
                f"discovered relation {change.qualified} is not a member of "
                f"publication {watcher.publication!r}; publication ownership is "
                "external, so cdc_flight will not issue ALTER PUBLICATION"
            )
            updated = _with_state(relation, ADMISSION_REFUSED)
            _record(watcher, change, updated, error=reason)
            observed[change.qualified] = updated
            continue
        try:
            conn.execute(
                f"ALTER PUBLICATION {quote(watcher.publication)} ADD TABLE "
                f"{quote(relation.schema)}.{quote(relation.table)}"
            )
        except Exception as exc:
            reason = (
                f"could not add discovered relation {change.qualified} to "
                f"publication {watcher.publication!r}: {exc}"
            )
            updated = _with_state(relation, ADMISSION_ERROR)
            _record(watcher, change, updated, error=reason)
            observed[change.qualified] = updated
            log.warning(reason)
            continue
        updated = _with_state(relation, ADMISSION_ADMITTED, published=True)
        _record(watcher, change, updated, error=None)
        observed[change.qualified] = updated
        log.info(
            "added discovered relation %s to publication %s",
            change.qualified,
            watcher.publication,
        )


def _with_state(relation, state: str, *, published: bool | None = None):
    return replace(
        relation,
        published=relation.published if published is None else published,
        admission_state=state,
    )


def _record(watcher, change, relation, *, error: str | None) -> None:
    with watcher._lock:
        previous = change.new_relation or watcher.known.get(change.qualified)
        previous_state = (
            previous.admission_state if previous is not None else ADMISSION_PENDING
        )
        target = relation.admission_state or ADMISSION_PENDING
        if previous_state != target:
            PUBLICATION_ADMISSION.check(previous_state, target)
        change.new_relation = relation
        watcher.known[change.qualified] = relation
        watcher._dirty[change.qualified] = relation
        if error is None:
            watcher._admission_errors.pop(change.qualified, None)
        else:
            watcher._admission_errors[change.qualified] = error
