"""dlt change handler for the Debezium embedded engine.

Structurally this is the blog's `DltChangeHandler`, with two additions that the
baseline needs to be testable:

* per-batch bookkeeping (record counts, per-table counts, last-batch timestamp)
  so a bounded run can decide when the stream has gone quiet;
* skipping Debezium's internal topics (heartbeat / transaction metadata) instead
  of materialising them as destination tables.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import dlt
from pydbzengine import BasePythonChangeHandler, ChangeEvent

from .debezium_props import METADATA_PREFIX
from .faults import maybe_crash

log = logging.getLogger("cdc_flight.handler")


def resolve_table_name(topic: str, payload: dict[str, Any] | None = None) -> str:
    """Destination table name for a change event.

    Debezium 3.6's default topic for this connector is `<prefix>.<table>` - the
    source *schema* is not part of the topic, so two same-named tables in
    different schemas would silently collide. The unwrapped payload does carry
    `dbz_schema`/`dbz_table`, so prefer those and fall back to the topic.
    """
    prefix = topic.split(".", 1)[0]
    if payload:
        schema = payload.get(f"{METADATA_PREFIX}schema")
        table = payload.get(f"{METADATA_PREFIX}table")
        if schema and table:
            return f"{prefix}_{schema}_{table}".replace(".", "_")
    return topic.replace(".", "_")


def _decode(event: ChangeEvent) -> dict[str, Any] | None:
    value = event.value()
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        # A bare tombstone (key, null value). With
        # `delete.tombstone.handling.mode=rewrite` these should not appear, but
        # be defensive.
        return None
    return json.loads(text)


@dlt.source(name="debezium")
def debezium_source_events(records_by_table: dict[str, list[dict[str, Any]]]):
    """Yield one dlt resource per source table for a single Debezium batch."""
    for table_name, events in records_by_table.items():
        yield dlt.resource(events, name=table_name, write_disposition="append")


class DltChangeHandler(BasePythonChangeHandler):
    """Load each Debezium batch into a dlt destination."""

    def __init__(self, dlt_pipeline: dlt.Pipeline, internal_topic_prefixes: tuple[str, ...] = ()):
        self.dlt_pipeline = dlt_pipeline
        self.internal_topic_prefixes = internal_topic_prefixes
        self._lock = threading.Lock()
        self._in_flight = 0
        self.record_count = 0
        self.batch_count = 0
        self.skipped_count = 0
        self.table_counts: dict[str, int] = {}
        self.last_batch_at: float = time.monotonic()
        self.error: BaseException | None = None

    # -- Debezium callback ---------------------------------------------------
    def handleJsonBatch(self, records: list[ChangeEvent]):
        with self._lock:
            self._in_flight += 1
        try:
            self._handle(records)
        finally:
            with self._lock:
                self._in_flight -= 1
                # The idle clock only starts once the load has actually finished,
                # otherwise a slow destination looks like an idle stream.
                self.last_batch_at = time.monotonic()

    def _handle(self, records: list[ChangeEvent]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        skipped = 0
        for event in records:
            topic = str(event.destination())
            if any(topic.startswith(p) for p in self.internal_topic_prefixes):
                skipped += 1
                continue
            payload = _decode(event)
            if payload is None:
                skipped += 1
                continue
            grouped.setdefault(resolve_table_name(topic, payload), []).append(payload)

        n = sum(len(v) for v in grouped.values())
        nth = self.batch_count + 1
        if grouped:
            log.info(
                "loading batch: %s records across %s tables (%s)",
                n,
                len(grouped),
                ", ".join(f"{k}={len(v)}" for k, v in sorted(grouped.items())),
            )
            maybe_crash("before_load", nth)
            try:
                self.dlt_pipeline.run(debezium_source_events(grouped))
            except BaseException as exc:  # surfaced to the caller by the engine
                self.error = exc
                raise
            # The at-least-once window: rows are committed at the destination,
            # the Debezium offset is not yet flushed. Inert unless a test asks
            # for it (see cdc_flight.faults).
            maybe_crash("after_load", nth)

        with self._lock:
            self.record_count += n
            self.skipped_count += skipped
            self.batch_count += 1
            for table, rows in grouped.items():
                self.table_counts[table] = self.table_counts.get(table, 0) + len(rows)

    # -- introspection used by the runner / tests ----------------------------
    @property
    def busy(self) -> bool:
        """True while a batch is being loaded - never stop the engine now."""
        with self._lock:
            return self._in_flight > 0

    @property
    def seconds_since_last_batch(self) -> float:
        with self._lock:
            return time.monotonic() - self.last_batch_at

    def snapshot_counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self.table_counts)
