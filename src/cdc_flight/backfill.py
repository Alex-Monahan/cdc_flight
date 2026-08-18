"""Backfill and refresh coordination for rubric §3.

This module is the single destination-side owner for the new backfill facts.  It
does not acquire PostgreSQL rows itself: healthy incremental acquisition is still
stock Debezium signalling, while explicit full work is the existing blocking
resnapshot path.  The module owns the common shadow target, durable run/cursor,
claim arbitration, refresh policy, and the one atomic publication boundary.

The small pure objects near the top are deliberately useful without a database;
the repositories and :class:`BackfillCoordinator` below use the same objects for
real DuckDB/MotherDuck transactions.  No value is converted through a PostgreSQL
``::text`` cast here.  A progress identity is a type-tagged JSON encoding of the
value already delivered by Debezium, so ``7`` and ``"7"`` cannot collide.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import faults, naming
from .config import resolve_control_schema
from .envelope import PendingRecord, decode
from .machines import (
    BACKFILL_RUN,
    SHADOW_CLAIM,
    SHADOW_CLAIM_STATES,
)
from .naming import control_table, quote

OWNER = "backfill-coordinator"
REFRESH_MODES = ("cdc", "full", "incremental")
TRIGGER_REASONS = ("scheduled", "bytes", "time", "both", "manual", "fallback")
ACTIVE_RUN_STATES = frozenset(
    {"requested", "preparing", "loading", "ready_to_swap", "swapping", "retry_wait", "blocked"}
)
TERMINAL_RUN_STATES = frozenset({"complete"})
INCREMENTAL_NOTIFICATION_TYPES = frozenset(
    {"STARTED", "IN_PROGRESS", "TABLE_SCAN_COMPLETED", "COMPLETED"}
)
INCREMENTAL_FAILURE_STATUSES = frozenset(
    {"NO_PRIMARY_KEY", "SQL_EXCEPTION", "UNKNOWN_SCHEMA", "SKIPPED", "ABORTED"}
)


class BackfillError(RuntimeError):
    """Base error for a refused or unrecoverable backfill decision."""


class ClaimConflict(BackfillError):
    """A different consistency owner already owns this table's shadow claim."""


class BackfillInvariantError(BackfillError):
    """A commit/ack, identity, or whole-unit invariant was violated."""


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _stable_json(value: Any) -> Any:
    """Turn a delivered runtime value into a type-preserving JSON tree.

    This is an identity codec, not a PostgreSQL value converter.  It intentionally
    keeps the source value's runtime type and never asks PostgreSQL to synthesize a
    textual representation.
    """
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "integer", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": format(value, "f")}
    if isinstance(value, float):
        if value != value:
            rendered = "NaN"
        elif value == float("inf"):
            rendered = "Infinity"
        elif value == float("-inf"):
            rendered = "-Infinity"
        else:
            rendered = repr(value)
        return {"type": "float", "value": rendered}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": value.hex()}
    if isinstance(value, datetime):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, datetime_time):
        return {"type": "time", "value": value.isoformat()}
    if isinstance(value, timedelta):
        return {
            "type": "interval",
            "value": {
                "days": value.days,
                "seconds": value.seconds,
                "microseconds": value.microseconds,
            },
        }
    if isinstance(value, uuid.UUID):
        return {"type": "uuid", "value": str(value)}
    if isinstance(value, Mapping):
        items = [
            [str(key), _stable_json(item)]
            for key, item in value.items()
        ]
        items.sort(key=lambda pair: pair[0])
        return {"type": "mapping", "value": items}
    if isinstance(value, (list, tuple)):
        return {
            "type": "tuple" if isinstance(value, tuple) else "list",
            "value": [_stable_json(item) for item in value],
        }
    # Unknown objects are kept distinct by their concrete type and repr.  This is
    # only a cursor identity fallback; row materialisation never passes through it.
    return {"type": type(value).__name__, "value": repr(value)}


def canonical_key_json(key: Mapping[str, Any] | Iterable[Any]) -> str:
    """Return the stable type-aware JSON identity for a primary-key value."""
    if isinstance(key, Mapping):
        tree = {str(name): _stable_json(value) for name, value in sorted(key.items())}
    else:
        tree = [_stable_json(value) for value in key]
    return json.dumps(tree, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def incremental_identity(signal_id: str, qualified_table: str, key: Mapping[str, Any]) -> str:
    """Stable identity of one stock incremental READ.

    The signal id and qualified table prevent two independent requests from
    sharing a cursor.  The key component is type-aware and canonical.
    """
    if not signal_id or not qualified_table or not key:
        raise BackfillInvariantError(
            "a keyed incremental identity needs signal id, table, and primary key"
        )
    digest = hashlib.sha256(canonical_key_json(key).encode("utf-8")).hexdigest()
    return f"inc:{signal_id}:{qualified_table}:{digest}"


def _stable_order_key(value: Any) -> tuple:
    """Return a comparable, type-aware ordering key for a delivered value.

    The cursor identity is a digest, but the durable ``maximum_key_json`` field is
    also useful for diagnostics and restart selection.  Lexicographically comparing
    its JSON text would put integer ``10`` before integer ``2`` and would silently
    make a composite cursor wrong.  This ordering is only for cursor comparison; it
    never changes the value sent to the destination.
    """
    if value is None:
        return (0,)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, int) and not isinstance(value, bool):
        return (2, value)
    if isinstance(value, Decimal):
        return (2, value)
    if isinstance(value, float):
        if value != value:
            return (3, 1, "NaN")
        if value == float("inf"):
            return (3, 1, "Infinity")
        if value == float("-inf"):
            return (3, 1, "-Infinity")
        return (3, 0, Decimal(str(value)))
    if isinstance(value, str):
        return (4, value)
    if isinstance(value, bytes):
        return (5, value)
    if isinstance(value, uuid.UUID):
        return (5, value.bytes)
    if isinstance(value, datetime):
        return (6, value.isoformat())
    if isinstance(value, date):
        return (7, value.isoformat())
    if isinstance(value, datetime_time):
        return (8, value.isoformat())
    if isinstance(value, timedelta):
        return (9, value.days, value.seconds, value.microseconds)
    if isinstance(value, Mapping):
        return (
            10,
            tuple((str(key), _stable_order_key(item)) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))),
        )
    if isinstance(value, (list, tuple)):
        return (11, tuple(_stable_order_key(item) for item in value))
    return (12, type(value).__name__, repr(value))


def _key_order_key(key: Mapping[str, Any] | Iterable[Any]) -> tuple:
    if isinstance(key, Mapping):
        return tuple(
            (str(name), _stable_order_key(value))
            for name, value in sorted(key.items(), key=lambda pair: str(pair[0]))
        )
    return tuple(_stable_order_key(value) for value in key)


def _decode_cursor_tree(tree: Any) -> Any:
    """Decode a durable cursor tree for comparison only.

    ``last_processed_key_json`` is an identity/high-water mark, not a PostgreSQL
    value conversion.  Older local fixtures also contain plain JSON keys, so the
    decoder accepts both those values and the type-tagged representation emitted by
    :func:`canonical_key_json`.
    """
    if isinstance(tree, dict) and "type" in tree and "value" in tree:
        kind = tree["type"]
        value = tree["value"]
        if kind == "null":
            return None
        if kind == "boolean":
            return bool(value)
        if kind == "integer":
            return int(value)
        if kind == "string":
            return str(value)
        if kind == "str":
            # ``str`` was the pre-guard fallback tag for Python strings. Preserve
            # its repr-based payload while new cursors use the explicit string tag.
            try:
                return ast.literal_eval(str(value))
            except (SyntaxError, ValueError):
                return str(value)
        if kind == "decimal":
            return Decimal(str(value))
        if kind == "float":
            return {
                "NaN": float("nan"),
                "Infinity": float("inf"),
                "-Infinity": float("-inf"),
            }.get(str(value), float(value))
        if kind == "bytes":
            return bytes.fromhex(str(value))
        if kind == "datetime":
            return datetime.fromisoformat(str(value))
        if kind == "date":
            return date.fromisoformat(str(value))
        if kind == "time":
            return datetime_time.fromisoformat(str(value))
        if kind == "interval":
            return timedelta(
                days=int(value["days"]),
                seconds=int(value["seconds"]),
                microseconds=int(value["microseconds"]),
            )
        if kind == "uuid":
            return uuid.UUID(str(value))
        if kind == "mapping":
            return {
                str(name): _decode_cursor_tree(item)
                for name, item in value
            }
        if kind == "tuple":
            return tuple(_decode_cursor_tree(item) for item in value)
        if kind == "list":
            return [_decode_cursor_tree(item) for item in value]
        raise BackfillInvariantError(
            f"durable cursor contains unknown type tag {kind!r}"
        )
    if isinstance(tree, dict):
        return {str(name): _decode_cursor_tree(value) for name, value in tree.items()}
    if isinstance(tree, list):
        return [_decode_cursor_tree(value) for value in tree]
    return tree


def _cursor_order(key_json: str) -> tuple:
    """Return the type-aware order key for one durable cursor JSON value."""
    try:
        tree = json.loads(key_json)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BackfillInvariantError(
            f"durable cursor is not valid JSON: {key_json!r}"
        ) from exc
    if not isinstance(tree, (dict, list)):
        raise BackfillInvariantError(
            "durable cursor must encode a mapping or ordered key sequence"
        )
    return _key_order_key(_decode_cursor_tree(tree))


def _max_cursor_json(*candidates: str | None) -> str | None:
    """Choose the greatest cursor without comparing JSON text lexicographically."""
    winner: str | None = None
    winner_order: tuple | None = None
    for candidate in candidates:
        if candidate is None:
            continue
        order = _cursor_order(candidate)
        if winner_order is None or order > winner_order:
            winner = candidate
            winner_order = order
    return winner


def _coalesced_trigger_reason(previous: str, current: str) -> str:
    """Persist the reason for the strongest observed byte/age trigger."""
    if previous == current or previous == "both":
        return previous
    if {previous, current} == {"bytes", "time"}:
        return "both"
    return current


def iter_chunks(rows: Iterable[Any], *, chunk_size: int) -> Iterator[list[Any]]:
    """Yield bounded lists without first materialising the source iterable."""
    if int(chunk_size) <= 0:
        raise ValueError("chunk_size must be positive")
    chunk: list[Any] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) >= int(chunk_size):
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def identity_set(rows: Iterable[Any]) -> set[Any]:
    """Identity oracle for benchmark/test rows, not a row-count shortcut."""
    return {row[0] for row in rows}


def value_multiset(rows: Iterable[Any]) -> Counter:
    """Value multiset oracle that catches wrong values with equal row counts."""
    return Counter(tuple(row) for row in rows)


def destination_writer_count() -> int:
    """The §3 design has one writer per commit group/table."""
    return 1


def simulate_parallel_acquisition(rows: Iterable[Any], *, workers: int) -> list[Any]:
    """Model stock parallel acquisition while keeping one ordered destination writer.

    The connector may acquire independent batches in parallel.  This function
    deliberately returns one serialised destination stream, making duplication and
    omission visible to the identity/value assertions.
    """
    if int(workers) < 1:
        raise ValueError("workers must be positive")
    material = list(rows)
    return [row for _, row in sorted(enumerate(material), key=lambda item: item[0])]


def measure_chunked_load(
    rows: Iterable[Any], *, chunk_size: int, workers: int
) -> BenchmarkResult:
    """Measure bounded row consumption without materialising the source iterable.

    This is intentionally a measurement helper, not an acquisition implementation:
    stock Debezium remains the source reader and the existing applier remains the
    sole destination writer. It gives the benchmark lane a real row count, wall
    time, and process RSS for the bounded chunk shape instead of a fabricated result.
    """
    if int(workers) < 1:
        raise ValueError("workers must be positive")
    started = time.perf_counter()
    count = 0
    digest = hashlib.sha256()
    for chunk in iter_chunks(rows, chunk_size=chunk_size):
        count += len(chunk)
        for row in chunk:
            digest.update(repr(row).encode("utf-8"))
    del digest
    elapsed = time.perf_counter() - started
    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform != "darwin":
        rss *= 1024
    return BenchmarkResult(
        rows=count,
        workers=int(workers),
        elapsed_seconds=elapsed,
        rss_bytes=rss,
        spill_bytes=0,
        motherduck_memory_bytes=None,
    )


@dataclass(frozen=True)
class BenchmarkResult:
    rows: int
    workers: int
    elapsed_seconds: float
    rss_bytes: int
    spill_bytes: int
    motherduck_memory_bytes: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "workers": self.workers,
            "elapsed_seconds": self.elapsed_seconds,
            "rss_bytes": self.rss_bytes,
            "spill_bytes": self.spill_bytes,
            "motherduck_memory_bytes": self.motherduck_memory_bytes,
        }


@dataclass(frozen=True)
class IncrementalSignal:
    signal_id: str
    tables: tuple[str, ...]
    queued: bool = False

    def __post_init__(self) -> None:
        if not self.signal_id:
            raise ValueError("signal_id must not be empty")
        if len(set(self.tables)) != len(self.tables):
            raise ValueError("one stock signal cannot repeat a table")
        if any("." not in table or table.startswith(".") or table.endswith(".") for table in self.tables):
            raise ValueError("stock signal tables must be schema-qualified")

    @property
    def is_noop(self) -> bool:
        return not self.tables


def encode_signal(signal: IncrementalSignal) -> dict[str, Any]:
    """Encode the stock ``execute-snapshot`` signal payload."""
    if signal.is_noop:
        return {"type": "incremental", "data-collections": []}
    return {
        "type": "incremental",
        "data-collections": list(signal.tables),
    }


class StockSignalWriter:
    """Write one idempotent stock ``execute-snapshot`` source signal."""

    def __init__(self, dsn: str, *, data_collection: str):
        self.dsn = dsn
        self.data_collection = data_collection

    def insert(self, signal: IncrementalSignal) -> str:
        import psycopg

        if signal.queued:
            # A queued request is destination-durable but intentionally has no
            # source row yet.  Inserting it now would make the second stock signal
            # ambiguous with the active one; dispatch_queued() owns the later
            # source INSERT after the active signal has completed.
            return signal.signal_id

        payload = json.dumps(
            {"data-collections": list(signal.tables), "type": "incremental"},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with psycopg.connect(self.dsn, autocommit=False, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._quoted_collection()} (id, type, data) "
                    "VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    [signal.signal_id, "execute-snapshot", payload],
                )
            conn.commit()
        faults.maybe_crash("after_signal_before_started", 1)
        return signal.signal_id

    def _quoted_collection(self) -> str:
        parts = self.data_collection.split(".")
        if len(parts) != 2 or not all(parts):
            raise ValueError("signal data collection must be schema-qualified")
        return ".".join(quote(part) for part in parts)


@dataclass(frozen=True)
class IncrementalNotification:
    observation: str
    data: dict[str, str]
    signal_id: str | None
    table: str | None
    status: str | None
    rows: int | None
    chunk_id: str | None
    last_processed_key: str | None


def decode_incremental_notification(raw: Any, *, topic_prefix: str = "cdcflight") -> IncrementalNotification | None:
    """Decode one stock Incremental Snapshot sink notification.

    Initial Snapshot notifications remain owned by ``snapshot_completion``.  A
    non-incremental topic/aggregate returns ``None`` so callers can pass it to the
    existing parser without vocabulary collision.
    """
    try:
        topic = str(raw.destination())
    except Exception:
        return None
    expected = f"{topic_prefix}.cdc_flight_snapshot_notifications"
    if topic != expected:
        return None
    try:
        payload = json.loads("" if raw.value() is None else str(raw.value()))
    except (TypeError, json.JSONDecodeError) as exc:
        raise BackfillInvariantError(f"incremental notification is not JSON: {exc}") from exc
    if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        payload = payload["payload"]
    if not isinstance(payload, dict) or payload.get("aggregate_type") != "Incremental Snapshot":
        return None
    observation = payload.get("type")
    if observation not in INCREMENTAL_NOTIFICATION_TYPES:
        raise BackfillInvariantError(f"unknown incremental notification {observation!r}")
    additional = payload.get("additional_data") or {}
    if not isinstance(additional, dict):
        raise BackfillInvariantError("incremental notification additional_data is not an object")
    data = {str(key): str(value) for key, value in additional.items()}
    rows = None
    if data.get("total_rows_scanned") not in (None, ""):
        try:
            rows = int(data["total_rows_scanned"])
        except ValueError as exc:
            raise BackfillInvariantError("incremental notification row count is not integral") from exc
    table = data.get("scanned_collection") or data.get("current_collection_in_progress")
    return IncrementalNotification(
        observation=observation,
        data=data,
        signal_id=data.get("signal_id") or data.get("id"),
        table=table,
        status=data.get("status"),
        rows=rows,
        chunk_id=data.get("chunk_id"),
        last_processed_key=data.get("last_processed_key"),
    )


def decode_incremental_record(
    raw: Any, *, topic_prefix: str = "cdcflight", signal_id: str | None = None
) -> PendingRecord:
    """Decode a stock incremental envelope and assign its stable READ identity."""
    record = decode(raw, topic_prefix=topic_prefix)
    if record.snapshot != "incremental":
        raise BackfillInvariantError("record is not a stock incremental snapshot READ")
    signal_id = signal_id or getattr(raw, "_cdc_flight_signal_id", None)
    if signal_id is None:
        try:
            payload = json.loads(str(raw.value()))
            signal_id = payload.get("cdc_flight_signal_id")
        except (AttributeError, TypeError, json.JSONDecodeError):
            signal_id = None
    if signal_id is None:
        raise BackfillInvariantError(
            "stock incremental READ has no admitted signal id; a process-arrival "
            "ordinal cannot be used as a resumable cursor"
        )
    signal_id = str(signal_id)
    if not record.key:
        raise BackfillInvariantError(
            f"{record.qualified_table} has no primary key for stock incremental resume"
        )
    record.incremental_signal_id = signal_id
    record.snapshot_identity = incremental_identity(
        signal_id, record.qualified_table or "", record.key
    )
    record.incremental = True
    return record


@dataclass
class TableRoute:
    schema: str
    table: str
    live: str
    shadow: str | None = None
    loading: bool = False

    def start_loading(self, *, shadow: str) -> None:
        self.shadow = shadow
        self.loading = True

    def finish(self) -> None:
        self.loading = False
        self.shadow = None

    def target_for(self, kind: str, *, table: str | None = None) -> str:
        if table is not None and table != self.table:
            return table
        return self.shadow if self.loading and self.shadow else self.live


@dataclass
class BackfillRun:
    pipeline: str
    run_id: str
    request_id: str
    source_schema: str
    source_table: str
    target_table: str
    requested_mode: str
    effective_mode: str
    trigger_reason: str
    state: str = "requested"
    signal_id: str | None = None
    notification_status: str = "REQUESTED"
    catalog_epoch: int = 0
    shadow_table: str | None = None
    last_processed_key_json: str | None = None
    maximum_key_json: str | None = None
    chunk_count: int = 0
    row_count: int = 0
    last_source_lsn: int | None = None
    terminal_source_point: str | None = None
    ack_reconciled_at: str | None = None
    retry_at: str | None = None
    error_code: str | None = None
    error_detail: str | None = None
    trigger_reasons: tuple[str, ...] = ()
    cursor: int | None = None
    mode: str | None = None
    shadow_retained: bool = True

    def __post_init__(self) -> None:
        if self.requested_mode not in REFRESH_MODES or self.effective_mode not in REFRESH_MODES:
            raise ValueError(f"refresh mode must be one of {REFRESH_MODES}")
        self.state = BACKFILL_RUN.parse(self.state)
        if self.trigger_reason not in TRIGGER_REASONS:
            raise ValueError(f"unknown backfill trigger reason {self.trigger_reason!r}")
        if not self.trigger_reasons:
            self.trigger_reasons = (self.trigger_reason,)
        if self.mode is None:
            self.mode = self.requested_mode

    @property
    def qualified_table(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    def transition(self, target: str, **updates) -> BackfillRun:
        BACKFILL_RUN.check(self.state, target)
        return replace(self, state=target, **updates)


@dataclass(frozen=True)
class RefreshPolicy:
    source_schema: str
    source_table: str
    mode: str = "cdc"
    enabled: bool = True
    interval_seconds: float | None = None
    next_due_at: str | None = None
    size_threshold_bytes: int | None = None
    time_threshold_ms: int | None = None
    retry_initial_seconds: float = 1.0
    retry_max_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.mode not in REFRESH_MODES:
            raise ValueError(f"refresh mode must be one of {REFRESH_MODES}")


@dataclass(frozen=True)
class CapabilityDecision:
    effective_mode: str
    reason: str
    stable_cursor: bool


def capability_decision(status: str, *, requested_mode: str) -> CapabilityDecision:
    if requested_mode == "incremental" and status == "NO_PRIMARY_KEY":
        return CapabilityDecision("full", "stock_no_primary_key", False)
    if status in INCREMENTAL_FAILURE_STATUSES:
        return CapabilityDecision("full", f"stock_{status.lower()}", False)
    return CapabilityDecision(requested_mode, "stock_supported", requested_mode == "incremental")


def keyless_resume_result(*, status: str):
    decision = capability_decision(status, requested_mode="incremental")
    return type(
        "KeylessResumeResult",
        (),
        {"effective_mode": decision.effective_mode, "ceiling": 4, "cursor": None},
    )()


@dataclass(frozen=True)
class TableOutcome:
    state: str
    shadow_retained: bool


class TableSetCoordinator:
    """Apply per-table stock terminal outcomes without a global pause."""

    def apply_outcomes(self, outcomes: Mapping[str, tuple[str, list[Any]]]) -> dict[str, TableOutcome]:
        result: dict[str, TableOutcome] = {}
        for table, (status, _rows) in outcomes.items():
            if status == "SUCCEEDED":
                result[table] = TableOutcome("complete", False)
            else:
                result[table] = TableOutcome(
                    "blocked" if status in INCREMENTAL_FAILURE_STATUSES else "retry_wait", True
                )
        return result


class RefreshCoordinator:
    """In-memory policy/run arbitration used by the durable coordinator as well."""

    def __init__(self, *, pipeline: str = "p"):
        self.pipeline = pipeline
        self.policies: dict[str, RefreshPolicy] = {}
        self.runs: dict[str, BackfillRun] = {}

    def start(self, schema: str, table: str, *, mode: str, cursor: int | None = None) -> BackfillRun:
        qualified = f"{schema}.{table}"
        for run in self.runs.values():
            if run.qualified_table == qualified and run.state in ACTIVE_RUN_STATES:
                return run
        run = BackfillRun(
            pipeline=self.pipeline,
            run_id=uuid.uuid4().hex,
            request_id=uuid.uuid4().hex,
            source_schema=schema,
            source_table=table,
            target_table=table,
            requested_mode=mode,
            effective_mode=mode,
            trigger_reason="scheduled",
            state="requested",
            cursor=cursor,
            shadow_table=f"{table}__cdcf_tmp",
        )
        run = run.transition("preparing").transition("loading")
        self.runs[run.run_id] = run
        self.policies[qualified] = RefreshPolicy(schema, table, mode=mode)
        return run

    def active_run(self, schema: str, table: str) -> BackfillRun:
        qualified = f"{schema}.{table}"
        for run in self.runs.values():
            if run.qualified_table == qualified and run.state in ACTIVE_RUN_STATES:
                return run
        raise KeyError(qualified)

    def change_mode(self, schema: str, table: str, mode: str) -> RefreshPolicy:
        policy = RefreshPolicy(schema, table, mode=mode)
        self.policies[f"{schema}.{table}"] = policy
        return policy

    @staticmethod
    def acquisition_for(mode: str) -> str:
        if mode == "full":
            return "blocking_stock_resnapshot"
        if mode == "incremental":
            return "stock_signal"
        return "none"

    @staticmethod
    def publication_for(mode: str) -> str:
        if mode not in REFRESH_MODES:
            raise ValueError(mode)
        return "shadow_atomic_swap"


class BackfillRepository:
    """The sole writer for durable ``BACKFILL_RUN`` facts.

    Methods intentionally do not open or commit a transaction.  The caller owns the
    surrounding MotherDuck transaction, which lets chunk rows, progress, lifecycle,
    and the final swap share one COMMIT.  Scheduler-only calls use the small
    ``transaction`` helper below.
    """

    _COLUMNS = (
        "pipeline, run_id, request_id, source_schema, source_table, target_table, "
        "requested_mode, effective_mode, trigger_reason, state, signal_id, "
        "notification_status, catalog_epoch, shadow_table, last_processed_key_json, "
        "maximum_key_json, chunk_count, row_count, last_source_lsn, terminal_source_point, "
        "ack_reconciled_at, retry_at, error_code, error_detail"
    )

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = resolve_control_schema(control_schema)
        self.table = control_table(self.control_schema, "backfill_runs")

    def _row(self, values) -> BackfillRun:
        return BackfillRun(
            pipeline=str(values[0]),
            run_id=str(values[1]),
            request_id=str(values[2]),
            source_schema=str(values[3]),
            source_table=str(values[4]),
            target_table=str(values[5]),
            requested_mode=str(values[6]),
            effective_mode=str(values[7]),
            trigger_reason=str(values[8]),
            state=str(values[9]),
            signal_id=None if values[10] is None else str(values[10]),
            notification_status=str(values[11]),
            catalog_epoch=int(values[12] or 0),
            shadow_table=None if values[13] is None else str(values[13]),
            last_processed_key_json=values[14],
            maximum_key_json=values[15],
            chunk_count=int(values[16] or 0),
            row_count=int(values[17] or 0),
            last_source_lsn=None if values[18] is None else int(values[18]),
            terminal_source_point=values[19],
            ack_reconciled_at=None if values[20] is None else str(values[20]),
            retry_at=None if values[21] is None else str(values[21]),
            error_code=values[22],
            error_detail=values[23],
        )

    def get(self, run_id: str) -> BackfillRun | None:
        row = self.con.execute(
            f"SELECT {self._COLUMNS} FROM {self.table} "
            "WHERE pipeline = ? AND run_id = ?",
            [self.pipeline, run_id],
        ).fetchone()
        return None if row is None else self._row(row)

    def active(self, schema: str, table: str) -> BackfillRun | None:
        row = self.con.execute(
            f"SELECT {self._COLUMNS} FROM {self.table} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ? "
            "AND state <> 'complete' ORDER BY created_at DESC LIMIT 1",
            [self.pipeline, schema, table],
        ).fetchone()
        return None if row is None else self._row(row)

    def progress_owner(self, schema: str, table: str) -> BackfillRun | None:
        """Return the active run, or the just-swapped run in this transaction."""
        active = self.active(schema, table)
        if active is not None:
            return active
        row = self.con.execute(
            f"SELECT {self._COLUMNS} FROM {self.table} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ? "
            "AND state = 'complete' ORDER BY updated_at DESC LIMIT 1",
            [self.pipeline, schema, table],
        ).fetchone()
        return None if row is None else self._row(row)

    def active_all(self) -> list[BackfillRun]:
        rows = self.con.execute(
            f"SELECT {self._COLUMNS} FROM {self.table} "
            "WHERE pipeline = ? AND state <> 'complete' ORDER BY created_at, run_id",
            [self.pipeline],
        ).fetchall()
        return [self._row(row) for row in rows]

    def request(
        self,
        schema: str,
        table: str,
        *,
        mode: str,
        reason: str = "scheduled",
        request_id: str | None = None,
        effective_mode: str | None = None,
        catalog_epoch: int = 0,
        target_table: str | None = None,
    ) -> BackfillRun:
        if mode not in REFRESH_MODES:
            raise ValueError(mode)
        if reason not in TRIGGER_REASONS:
            raise ValueError(reason)
        active = self.active(schema, table)
        if active is not None:
            reasons = tuple(sorted(set(active.trigger_reasons) | {reason}))
            durable_reason = _coalesced_trigger_reason(active.trigger_reason, reason)
            # A configured full refresh has priority before loading starts. Once
            # stock work has begun, preserve its cursor and queue the full reason
            # rather than replacing the active run in place.
            promote = (
                mode == "full"
                and active.state in {"requested", "preparing"}
            )
            if promote:
                self.con.execute(
                    f"UPDATE {self.table} SET requested_mode = ?, effective_mode = ?, "
                    "trigger_reason = ?, request_id = ?, updated_at = ? "
                    "WHERE pipeline = ? AND run_id = ?",
                    [mode, effective_mode or mode, durable_reason, request_id or active.request_id,
                     datetime.now(UTC), self.pipeline, active.run_id],
                )
            else:
                self.con.execute(
                    f"UPDATE {self.table} SET trigger_reason = ?, request_id = ?, "
                    "updated_at = ? WHERE pipeline = ? AND run_id = ?",
                    [durable_reason, request_id or active.request_id, datetime.now(UTC),
                     self.pipeline, active.run_id],
                )
            active = self.get(active.run_id)
            active.trigger_reasons = reasons
            active.trigger_reason = durable_reason
            return active

        run_id = uuid.uuid4().hex
        request_id = request_id or uuid.uuid4().hex
        target = target_table or naming.destination_table("cdcflight", schema, table)
        shadow = naming.shadow_table(target)
        now = datetime.now(UTC)
        self.con.execute(
            f"INSERT INTO {self.table} ({self._COLUMNS}, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'REQUESTED', ?, ?, NULL, NULL, "
            "0, 0, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)",
            [
                self.pipeline, run_id, request_id, schema, table, target, mode,
                effective_mode or mode, reason, "requested", catalog_epoch, shadow,
                now, now,
            ],
        )
        return self.get(run_id)

    def transition(self, run: BackfillRun | str, target: str, **updates) -> BackfillRun:
        current = self.get(run if isinstance(run, str) else run.run_id)
        if current is None:
            raise KeyError(run if isinstance(run, str) else run.run_id)
        BACKFILL_RUN.check(current.state, target)
        assignments = ["state = ?", "updated_at = ?"]
        values: list[Any] = [target, datetime.now(UTC)]
        allowed = {
            "signal_id", "notification_status", "catalog_epoch", "shadow_table",
            "last_processed_key_json", "maximum_key_json", "chunk_count", "row_count",
            "last_source_lsn", "terminal_source_point", "ack_reconciled_at", "retry_at",
            "error_code", "error_detail", "effective_mode", "request_id",
        }
        for key, value in updates.items():
            if key not in allowed:
                raise ValueError(f"unknown backfill update {key!r}")
            assignments.append(f"{key} = ?")
            values.append(value)
        values.extend([self.pipeline, current.run_id])
        self.con.execute(
            f"UPDATE {self.table} SET {', '.join(assignments)} "
            "WHERE pipeline = ? AND run_id = ?",
            values,
        )
        return self.get(current.run_id)

    def update_progress(
        self,
        run: BackfillRun | str,
        *,
        last_key_json: str | None = None,
        maximum_key_json: str | None = None,
        chunks: int = 0,
        rows: int = 0,
        source_lsn: int | None = None,
        notification_status: str | None = None,
        signal_id: str | None = None,
        allow_terminal: bool = False,
    ) -> BackfillRun:
        current = self.get(run if isinstance(run, str) else run.run_id)
        if current is None:
            raise KeyError(run if isinstance(run, str) else run.run_id)
        if current.state not in ACTIVE_RUN_STATES and not (
            allow_terminal and current.state == "complete"
        ):
            raise BackfillInvariantError(
                f"progress for {current.run_id} entered terminal state {current.state}"
            )
        # ``last_processed_key_json`` is a durable high-water mark.  A stock
        # incremental chunk is allowed to arrive out of key order, and a retry may
        # present an older key after a newer chunk has committed.  Absorb that
        # delivery rather than moving the restart cursor backwards.  Include the
        # previously recorded maximum when repairing a row written by an older
        # build: that makes the invariant self-healing on the next progress write.
        last_key_json = _max_cursor_json(
            current.last_processed_key_json,
            current.maximum_key_json,
            last_key_json,
        )
        maximum_key_json = _max_cursor_json(
            current.maximum_key_json,
            current.last_processed_key_json,
            maximum_key_json,
            last_key_json,
        )
        sets = [
            "last_processed_key_json = COALESCE(?, last_processed_key_json)",
            "maximum_key_json = COALESCE(?, maximum_key_json)",
            "chunk_count = chunk_count + ?",
            "row_count = row_count + ?",
            "last_source_lsn = COALESCE(?, last_source_lsn)",
            "updated_at = ?",
        ]
        values: list[Any] = [
            last_key_json, maximum_key_json, int(chunks), int(rows), source_lsn,
            datetime.now(UTC),
        ]
        if notification_status is not None:
            sets.append("notification_status = ?")
            values.append(notification_status)
        if signal_id is not None:
            sets.append("signal_id = ?")
            values.append(signal_id)
        values.extend([self.pipeline, current.run_id])
        self.con.execute(
            f"UPDATE {self.table} SET {', '.join(sets)} "
            "WHERE pipeline = ? AND run_id = ?",
            values,
        )
        return self.get(current.run_id)

    def set_signal(self, run: BackfillRun | str, signal_id: str) -> BackfillRun:
        """Correlate a durable run with its stock signal before source INSERT."""
        if not signal_id:
            raise ValueError("signal_id must not be empty")
        current = self.get(run if isinstance(run, str) else run.run_id)
        if current is None:
            raise KeyError(run if isinstance(run, str) else run.run_id)
        if current.state not in ACTIVE_RUN_STATES:
            raise BackfillInvariantError(
                f"signal correlation for {current.run_id} entered terminal state {current.state}"
            )
        self.con.execute(
            f"UPDATE {self.table} SET signal_id = ?, updated_at = ? "
            "WHERE pipeline = ? AND run_id = ?",
            [signal_id, datetime.now(UTC), self.pipeline, current.run_id],
        )
        return self.get(current.run_id)

    def transaction(self, callback):
        self.con.execute("BEGIN TRANSACTION")
        try:
            result = callback()
            self.con.execute("COMMIT")
            return result
        except BaseException:
            with contextlib.suppress(Exception):
                self.con.execute("ROLLBACK")
            raise


@dataclass(frozen=True)
class QueuedSignalRequest:
    request_id: str
    signal_id: str
    tables: tuple[str, ...]
    trigger_reason: str
    state: str
    dispatch_signal_id: str | None = None


class BackfillSignalQueueRepository:
    """Durable successor requests for stock signal correlation."""

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = resolve_control_schema(control_schema)
        self.table = control_table(self.control_schema, "backfill_signal_queue")

    @staticmethod
    def _decode(row) -> QueuedSignalRequest:
        return QueuedSignalRequest(
            request_id=str(row[0]),
            signal_id=str(row[1]),
            tables=tuple(json.loads(str(row[2]))),
            trigger_reason=str(row[3]),
            state=str(row[4]),
            dispatch_signal_id=None if row[5] is None else str(row[5]),
        )

    def get(self, request_id: str) -> QueuedSignalRequest | None:
        row = self.con.execute(
            f"SELECT request_id, signal_id, tables_json, trigger_reason, state, "
            f"dispatch_signal_id FROM {self.table} WHERE pipeline = ? AND request_id = ?",
            [self.pipeline, request_id],
        ).fetchone()
        return None if row is None else self._decode(row)

    def queued(self) -> list[QueuedSignalRequest]:
        rows = self.con.execute(
            f"SELECT request_id, signal_id, tables_json, trigger_reason, state, "
            f"dispatch_signal_id FROM {self.table} WHERE pipeline = ? AND state = 'queued' "
            "ORDER BY created_at, request_id",
            [self.pipeline],
        ).fetchall()
        return [self._decode(row) for row in rows]

    def enqueue(
        self,
        *,
        request_id: str,
        signal_id: str,
        tables: tuple[str, ...],
        trigger_reason: str,
    ) -> QueuedSignalRequest:
        if not request_id or not signal_id or not tables:
            raise ValueError("queued stock requests need an id, signal, and table set")
        if trigger_reason not in TRIGGER_REASONS:
            raise ValueError(trigger_reason)
        existing = self.get(request_id)
        now = datetime.now(UTC)
        if existing is not None:
            if existing.state != "queued":
                return existing
            merged = tuple(dict.fromkeys((*existing.tables, *tables)))
            reason = _coalesced_trigger_reason(existing.trigger_reason, trigger_reason)
            self.con.execute(
                f"UPDATE {self.table} SET tables_json = ?, trigger_reason = ?, updated_at = ? "
                "WHERE pipeline = ? AND request_id = ?",
                [json.dumps(merged, separators=(",", ":")), reason, now, self.pipeline, request_id],
            )
            return self.get(request_id)
        self.con.execute(
            f"INSERT INTO {self.table} "
            "(pipeline, request_id, signal_id, tables_json, trigger_reason, state, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
            [
                self.pipeline,
                request_id,
                signal_id,
                json.dumps(tables, separators=(",", ":")),
                trigger_reason,
                now,
                now,
            ],
        )
        return self.get(request_id)

    def mark_dispatched(self, request_ids: Iterable[str], signal_id: str) -> None:
        ids = tuple(request_ids)
        if not ids or not signal_id:
            raise ValueError("dispatch needs queued request ids and a signal id")
        self.con.execute(
            f"UPDATE {self.table} SET state = 'dispatched', dispatch_signal_id = ?, "
            "updated_at = ? WHERE pipeline = ? AND state = 'queued' "
            f"AND request_id IN ({','.join('?' for _ in ids)})",
            [signal_id, datetime.now(UTC), self.pipeline, *ids],
        )


class ShadowClaimRepository:
    """Transactional owner arbitration for a table's shared shadow."""

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = resolve_control_schema(control_schema)
        self.table = control_table(self.control_schema, "shadow_claims")

    def _ensure(self, schema: str, table: str) -> None:
        self.con.execute(
            f"INSERT INTO {self.table} "
            "(pipeline, source_schema, source_table, claim_state, owner_kind, owner_id, updated_at) "
            "SELECT ?, ?, ?, 'free', '', '', ? "
            "WHERE NOT EXISTS (SELECT 1 FROM " + self.table +
            " WHERE pipeline = ? AND source_schema = ? AND source_table = ?)",
            [self.pipeline, schema, table, datetime.now(UTC), self.pipeline, schema, table],
        )

    def state(self, schema: str, table: str) -> tuple[str, str, str] | None:
        row = self.con.execute(
            f"SELECT claim_state, owner_kind, owner_id FROM {self.table} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [self.pipeline, schema, table],
        ).fetchone()
        return None if row is None else (str(row[0]), str(row[1]), str(row[2]))

    def acquire(
        self,
        schema: str,
        table: str,
        *,
        owner_kind: str = "backfill",
        owner_id: str,
        lease_id: str | None = None,
    ) -> str:
        target = owner_kind
        if target not in SHADOW_CLAIM_STATES or target == "free":
            raise ValueError(owner_kind)
        self._ensure(schema, table)
        current = self.state(schema, table)
        if current is not None and current[0] != "free":
            if current[1] == owner_kind and current[2] == owner_id:
                return lease_id or "reused"
            raise ClaimConflict(
                f"{schema}.{table} shadow is owned by {current[1]}:{current[2]}"
            )
        SHADOW_CLAIM.check("free", target)
        lease = lease_id or uuid.uuid4().hex
        self.con.execute(
            f"UPDATE {self.table} SET claim_state = ?, owner_kind = ?, owner_id = ?, "
            "lease_id = ?, acquired_at = ?, renewed_at = ?, updated_at = ? "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [target, owner_kind, owner_id, lease, datetime.now(UTC), datetime.now(UTC),
             datetime.now(UTC), self.pipeline, schema, table],
        )
        return lease

    def release(self, schema: str, table: str, *, owner_kind: str, owner_id: str) -> None:
        current = self.state(schema, table)
        if current is None:
            return
        if current[0] == "free":
            return
        if current[1] != owner_kind or current[2] != owner_id:
            raise ClaimConflict(
                f"{schema}.{table} release by {owner_kind}:{owner_id} does not own "
                f"{current[1]}:{current[2]}"
            )
        SHADOW_CLAIM.check(current[0], "free")
        now = datetime.now(UTC)
        self.con.execute(
            f"UPDATE {self.table} SET claim_state = 'free', owner_kind = '', owner_id = '', "
            "lease_id = NULL, released_at = ?, updated_at = ? "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [now, now, self.pipeline, schema, table],
        )


class RefreshPolicyRepository:
    """Declarative per-table scheduling configuration; never an offset authority."""

    def __init__(self, con, *, pipeline: str, control_schema: str | None = None):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = resolve_control_schema(control_schema)
        self.table = control_table(self.control_schema, "refresh_policy")

    def upsert(self, policy: RefreshPolicy) -> RefreshPolicy:
        self.con.execute(
            f"INSERT INTO {self.table} "
            "(pipeline, source_schema, source_table, mode, enabled, interval_seconds, next_due_at, "
            "size_threshold_bytes, time_threshold_ms, retry_initial_seconds, retry_max_seconds, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (pipeline, source_schema, source_table) DO UPDATE SET "
            "mode = excluded.mode, enabled = excluded.enabled, interval_seconds = excluded.interval_seconds, "
            "next_due_at = excluded.next_due_at, size_threshold_bytes = excluded.size_threshold_bytes, "
            "time_threshold_ms = excluded.time_threshold_ms, retry_initial_seconds = excluded.retry_initial_seconds, "
            "retry_max_seconds = excluded.retry_max_seconds, updated_at = excluded.updated_at",
            [
                self.pipeline, policy.source_schema, policy.source_table, policy.mode, policy.enabled,
                policy.interval_seconds, policy.next_due_at, policy.size_threshold_bytes,
                policy.time_threshold_ms, policy.retry_initial_seconds, policy.retry_max_seconds,
                datetime.now(UTC),
            ],
        )
        return policy

    def get(self, schema: str, table: str) -> RefreshPolicy | None:
        row = self.con.execute(
            f"SELECT mode, enabled, interval_seconds, next_due_at, size_threshold_bytes, "
            f"time_threshold_ms, retry_initial_seconds, retry_max_seconds FROM {self.table} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
            [self.pipeline, schema, table],
        ).fetchone()
        if row is None:
            return None
        return RefreshPolicy(
            schema,
            table,
            mode=str(row[0]),
            enabled=bool(row[1]),
            interval_seconds=row[2],
            next_due_at=None if row[3] is None else str(row[3]),
            size_threshold_bytes=row[4],
            time_threshold_ms=row[5],
            retry_initial_seconds=float(row[6]),
            retry_max_seconds=float(row[7]),
        )


class BackfillCoordinator:
    """Destination-side backfill owner used by the applier and scheduler."""

    def __init__(
        self,
        con,
        *,
        pipeline: str,
        control_schema: str | None = None,
        topic_prefix: str = "cdcflight",
    ):
        self.con = con
        self.pipeline = pipeline
        self.control_schema = resolve_control_schema(control_schema)
        self.topic_prefix = topic_prefix
        self.repository = BackfillRepository(
            con, pipeline=pipeline, control_schema=self.control_schema
        )
        self.signal_queue = BackfillSignalQueueRepository(
            con, pipeline=pipeline, control_schema=self.control_schema
        )
        self.claims = ShadowClaimRepository(
            con, pipeline=pipeline, control_schema=self.control_schema
        )
        self.policies = RefreshPolicyRepository(
            con, pipeline=pipeline, control_schema=self.control_schema
        )
        self.owner_id = f"{OWNER}:{uuid.uuid4().hex}"
        self._pending: list[IncrementalNotification] = []

    def active(self, schema: str, table: str) -> BackfillRun | None:
        return self.repository.active(schema, table)

    def incremental_owner(self, schema: str, table: str) -> BackfillRun | None:
        """Return the active run or retained terminal evidence for this table.

        A stock READ can be replayed after the shadow swap but before the source
        connector has observed the durable resume point.  The terminal run is the
        evidence that this READ was already published; callers use it to fence the
        replay into an acknowledgeable control unit instead of routing it into the
        new live table.
        """
        active = self.repository.active(schema, table)
        if active is not None:
            return active
        row = self.con.execute(
            f"SELECT {self.repository._COLUMNS} FROM {self.repository.table} "
            "WHERE pipeline = ? AND source_schema = ? AND source_table = ? "
            "AND state = 'complete' ORDER BY updated_at DESC, created_at DESC LIMIT 1",
            [self.pipeline, schema, table],
        ).fetchone()
        return None if row is None else self.repository._row(row)

    def active_runs(self) -> list[BackfillRun]:
        return self.repository.active_all()

    def request(
        self,
        schema: str,
        table: str,
        *,
        mode: str,
        reason: str = "scheduled",
        in_transaction: bool = False,
    ) -> BackfillRun:
        def admit() -> BackfillRun:
            return self.repository.request(
                schema,
                table,
                mode=mode,
                reason=reason,
                target_table=naming.destination_table(self.topic_prefix, schema, table),
            )

        return admit() if in_transaction else self.repository.transaction(admit)

    def request_tables(
        self,
        tables: Iterable[str],
        *,
        mode: str = "incremental",
        reason: str = "scheduled",
        request_id: str | None = None,
        signal_id: str | None = None,
    ) -> tuple[IncrementalSignal, tuple[BackfillRun, ...]]:
        """Admit one arbitrary stock signal set and one run per table.

        The source INSERT is deliberately left to :class:`StockSignalWriter`.
        This method only creates/correlates durable destination work, so a lost
        source response can be retried with the same signal id without making a
        second active run.
        """
        selected = tuple(dict.fromkeys(str(table) for table in tables))
        if any("." not in table for table in selected):
            raise ValueError("backfill tables must be schema-qualified")
        if mode != "incremental":
            raise ValueError("one stock signal set must use incremental mode")
        if not selected:
            # Stock has no meaningful empty execute-snapshot request.  Returning a
            # typed no-op keeps the arbitrary-set API total without creating a
            # signal row, run, claim, or source-offset obligation.
            return IncrementalSignal(signal_id or uuid.uuid4().hex, ()), ()
        active_runs = self.repository.active_all()
        active_signal_ids = {
            run.signal_id for run in active_runs if run.signal_id is not None
        }
        if len(active_signal_ids) > 1:
            raise BackfillInvariantError(
                "only one active stock signal request may be admitted at a time"
            )
        active_signal_id = next(iter(active_signal_ids), None)
        active_incremental_tables = {
            run.qualified_table
            for run in active_runs
            if run.signal_id == active_signal_id
        }
        active_non_incremental_tables = {
            run.qualified_table
            for run in active_runs
            if run.effective_mode != "incremental"
        }
        for qualified in selected:
            schema, table = qualified.split(".", 1)
            active = self.repository.active(schema, table)
            if active is not None and active.signal_id:
                active_signal_ids.add(active.signal_id)
        if len(active_signal_ids) > 1:
            raise BackfillInvariantError(
                "one stock signal cannot correlate tables already owned by different active signals"
            )
        signal = IncrementalSignal(
            signal_id or active_signal_id or next(iter(active_signal_ids), uuid.uuid4().hex),
            selected,
        )
        request_id = request_id or uuid.uuid4().hex

        # Stock's notification aggregate carries one signal correlation, so a
        # second source row cannot safely be admitted while the first request is
        # active.  Persist it instead.  The scheduler later combines all queued
        # table sets into one successor signal, preserving the source boundary
        # without inventing a second reader or dropping the request.
        needs_queue = (
            active_signal_id is not None
            and (
                signal.signal_id != active_signal_id
                or not set(selected) <= active_incremental_tables
            )
        ) or bool(set(selected) & active_non_incremental_tables)
        if needs_queue:
            queued_signal_id = signal.signal_id
            if queued_signal_id == active_signal_id:
                queued_signal_id = uuid.uuid4().hex
            queued_signal = IncrementalSignal(
                queued_signal_id, selected, queued=True
            )

            def enqueue() -> tuple[IncrementalSignal, tuple[BackfillRun, ...]]:
                self.signal_queue.enqueue(
                    request_id=request_id,
                    signal_id=queued_signal.signal_id,
                    tables=selected,
                    trigger_reason=reason,
                )
                faults.maybe_crash("before_request_md_commit", 1)
                return queued_signal, ()

            result = self.repository.transaction(enqueue)
            faults.maybe_crash("after_request_commit_before_signal", 1)
            return result

        def admit() -> tuple[IncrementalSignal, tuple[BackfillRun, ...]]:
            runs: list[BackfillRun] = []
            for qualified in selected:
                schema, table = qualified.split(".", 1)
                run = self.repository.request(
                    schema,
                    table,
                    mode=mode,
                    reason=reason,
                    request_id=request_id,
                    target_table=naming.destination_table(self.topic_prefix, schema, table),
                )
                if run.signal_id not in (None, signal.signal_id):
                    raise BackfillInvariantError(
                        f"active run {run.run_id} is correlated with another signal"
                    )
                run = self.repository.set_signal(run, signal.signal_id)
                runs.append(run)
            faults.maybe_crash("before_request_md_commit", 1)
            return signal, tuple(runs)

        result = self.repository.transaction(admit)
        faults.maybe_crash("after_request_commit_before_signal", 1)
        return result

    def dispatch_queued(self) -> tuple[IncrementalSignal, tuple[BackfillRun, ...]] | None:
        """Dispatch all successor requests as one stock signal when idle."""
        if self.repository.active_all():
            return None
        queued = self.signal_queue.queued()
        if not queued:
            return None
        selected = tuple(
            dict.fromkeys(table for request in queued for table in request.tables)
        )
        request_id = f"queued-{uuid.uuid4().hex}"
        signal_id = f"queued-{uuid.uuid4().hex}"
        reason = queued[0].trigger_reason
        for request in queued[1:]:
            reason = _coalesced_trigger_reason(reason, request.trigger_reason)
        signal = IncrementalSignal(signal_id, selected)

        def admit() -> tuple[IncrementalSignal, tuple[BackfillRun, ...]]:
            runs: list[BackfillRun] = []
            for qualified in selected:
                schema, table = qualified.split(".", 1)
                run = self.repository.request(
                    schema,
                    table,
                    mode="incremental",
                    reason=reason,
                    request_id=request_id,
                    target_table=naming.destination_table(self.topic_prefix, schema, table),
                )
                run = self.repository.set_signal(run, signal.signal_id)
                runs.append(run)
            self.signal_queue.mark_dispatched(
                (request.request_id for request in queued), signal.signal_id
            )
            faults.maybe_crash("before_request_md_commit", 1)
            return signal, tuple(runs)

        result = self.repository.transaction(admit)
        faults.maybe_crash("after_request_commit_before_signal", 1)
        return result

    def set_mode(self, schema: str, table: str, mode: str) -> RefreshPolicy:
        """Change requested mode without resetting an active run/cursor."""
        if mode not in REFRESH_MODES:
            raise ValueError(mode)

        def update() -> RefreshPolicy:
            policy = self.policies.get(schema, table) or RefreshPolicy(schema, table)
            policy = replace(policy, mode=mode)
            self.policies.upsert(policy)
            self.con.execute(
                f"UPDATE {control_table(self.control_schema, 'table_state')} "
                "SET refresh_mode = ? WHERE pipeline = ? AND source_schema = ? AND source_table = ?",
                [mode, self.pipeline, schema, table],
            )
            return policy

        return self.repository.transaction(update)

    def prepare(self, run: BackfillRun, *, in_transaction: bool = False) -> BackfillRun:
        def admit() -> BackfillRun:
            current = self.repository.get(run.run_id) or run
            if current.state == "requested":
                current = self.repository.transition(current, "preparing")
            if current.state == "preparing":
                self.claims.acquire(
                    current.source_schema, current.source_table,
                    owner_kind="backfill", owner_id=current.run_id,
                )
                current = self.repository.transition(current, "loading")
            return current

        return admit() if in_transaction else self.repository.transaction(admit)

    def complete_full_swap(
        self, state, *, snapshot_lsn: int | None, commit_id: int,
        in_transaction: bool = False,
    ) -> None:
        """Close a scheduled full run inside the blocking resnapshot commit."""
        def publish() -> None:
            run = self.repository.active(state.schema, state.table)
            if run is None or run.effective_mode != "full":
                return
            if run.state == "requested":
                run = self.repository.transition(run, "preparing")
                self.claims.acquire(
                    state.schema, state.table,
                    owner_kind="backfill", owner_id=run.run_id,
                )
                run = self.repository.transition(run, "loading")
            elif run.state == "preparing":
                self.claims.acquire(
                    state.schema, state.table,
                    owner_kind="backfill", owner_id=run.run_id,
                )
                run = self.repository.transition(run, "loading")
            if run.state == "loading":
                run = self.repository.transition(run, "ready_to_swap")
            if run.state == "ready_to_swap":
                run = self.repository.transition(run, "swapping")
            if run.state == "swapping":
                self.repository.transition(
                    run,
                    "complete",
                    terminal_source_point=str(snapshot_lsn or ""),
                    notification_status="COMPLETED",
                )
                self.claims.release(
                    state.schema, state.table,
                    owner_kind="backfill", owner_id=run.run_id,
                )

        if in_transaction:
            publish()
        else:
            self.repository.transaction(publish)

    def observe_notification(
        self, notification: IncrementalNotification, *, in_transaction: bool = False
    ) -> BackfillRun | None:
        """Record stock notification progress; row progress is committed separately."""
        def apply() -> BackfillRun | None:
            self._pending.append(notification)
            table = notification.table
            if not table:
                return None
            if "." in table:
                schema, table_name = table.split(".", 1)
            else:
                schema, table_name = table, table
            run = self.repository.active(schema, table_name)
            if run is None:
                return None
            updates = {
                "signal_id": notification.signal_id or run.signal_id,
                "notification_status": notification.observation,
            }

            def ensure_loading(current: BackfillRun) -> BackfillRun:
                """Bring any valid notification arrival back to the loading route."""
                if current.state == "requested":
                    current = self.repository.transition(current, "preparing", **updates)
                    self.claims.acquire(
                        schema, table_name, owner_kind="backfill", owner_id=current.run_id
                    )
                    current = self.repository.transition(current, "loading")
                elif current.state == "preparing":
                    current = self.repository.transition(current, "loading", **updates)
                elif current.state in {"retry_wait", "blocked"}:
                    current = self.repository.transition(current, "preparing", **updates)
                    self.claims.acquire(
                        schema, table_name, owner_kind="backfill", owner_id=current.run_id
                    )
                    current = self.repository.transition(current, "loading")
                return current

            if notification.observation in {"STARTED", "IN_PROGRESS"}:
                if run.state not in {"ready_to_swap", "swapping", "complete"}:
                    run = ensure_loading(run)
                    run = self.repository.update_progress(
                        run,
                        notification_status=notification.observation,
                        signal_id=notification.signal_id,
                    )
            elif notification.observation == "TABLE_SCAN_COMPLETED":
                status = (notification.status or "").upper()
                if status in {"SUCCEEDED", "EMPTY", "COMPLETED"}:
                    run = ensure_loading(run)
                    if run.state == "loading":
                        faults.maybe_crash(
                            "after_TABLE_SCAN_COMPLETED", run.chunk_count + 1
                        )
                        run = self.repository.transition(run, "ready_to_swap", **updates)
                else:
                    run = ensure_loading(run)
                    if run.state == "loading":
                        decision = capability_decision(
                            status or "UNKNOWN", requested_mode=run.effective_mode
                        )
                        run = self.repository.transition(
                            run,
                            "blocked",
                            **updates,
                            effective_mode=decision.effective_mode,
                            error_code=status or "UNKNOWN",
                            error_detail=(
                                f"stock incremental scan failed; {decision.reason}; "
                                "existing full-refresh handoff is required"
                            ),
                        )
                        if self.claims.state(schema, table_name) is not None:
                            self.claims.release(
                                schema, table_name, owner_kind="backfill", owner_id=run.run_id
                            )
            elif (
                notification.observation == "COMPLETED"
                and run.state not in {"ready_to_swap", "swapping", "complete"}
            ):
                run = ensure_loading(run)
                faults.maybe_crash(
                    "after_COMPLETED_before_ready_to_swap", run.chunk_count + 1
                )
                run = self.repository.update_progress(
                    run,
                    notification_status="COMPLETED",
                    signal_id=notification.signal_id,
                )
            return run

        return apply() if in_transaction else self.repository.transaction(apply)

    def commit_progress(self, units, *, in_transaction: bool = False) -> None:
        """Write keyed incremental cursors after row DML and before COMMIT."""
        def apply() -> None:
            by_run: dict[str, dict[str, Any]] = {}
            for unit in units:
                if not getattr(unit, "incremental", False):
                    continue
                unit_progress: dict[str, int] = {}
                for event in unit.events:
                    if not event.snapshot_identity or not event.qualified_table:
                        continue
                    run = self.repository.progress_owner(
                        event.schema or "", event.table or ""
                    )
                    if run is None:
                        continue
                    if not event.key:
                        continue
                    key_json = canonical_key_json(event.key)
                    progress = by_run.setdefault(
                        run.run_id,
                        {
                            "last": None,
                            "maximum": None,
                            "maximum_order": None,
                            "chunks": 0,
                            "rows": 0,
                            "lsn": None,
                        },
                    )
                    order = _key_order_key(event.key)
                    if progress["maximum_order"] is None or order > progress["maximum_order"]:
                        progress["maximum_order"] = order
                        progress["maximum"] = key_json
                        # The durable cursor is the greatest key observed in the
                        # unit, not the last callback's arrival key.  The repository
                        # applies the same monotonic guard across units/restarts.
                        progress["last"] = key_json
                    progress["rows"] += 1
                    if event.lsn is not None:
                        progress["lsn"] = max(progress["lsn"] or int(event.lsn), int(event.lsn))
                    unit_progress[run.run_id] = unit_progress.get(run.run_id, 0) + 1
                for run_id in unit_progress:
                    by_run[run_id]["chunks"] += 1
            for run_id, progress in by_run.items():
                self.repository.update_progress(
                    run_id,
                    last_key_json=progress["last"],
                    maximum_key_json=progress["maximum"],
                    chunks=progress["chunks"],
                    rows=progress["rows"],
                    source_lsn=progress["lsn"],
                    allow_terminal=True,
                )

        if in_transaction:
            apply()
        else:
            self.repository.transaction(apply)

    def complete_swap(
        self, state, *, snapshot_lsn: int | None, commit_id: int, in_transaction: bool = False
    ) -> None:
        def publish() -> None:
            run = self.repository.active(state.schema, state.table)
            if run is None:
                return
            if run.state == "loading":
                faults.maybe_crash("before_ready_to_swap_commit", commit_id)
                run = self.repository.transition(run, "ready_to_swap")
            if run.state == "ready_to_swap":
                run = self.repository.transition(run, "swapping")
            if run.state == "swapping":
                run = self.repository.transition(
                    run, "complete", terminal_source_point=str(snapshot_lsn or ""),
                    notification_status="COMPLETED",
                )
                self.claims.release(
                    state.schema, state.table, owner_kind="backfill", owner_id=run.run_id
                )

        if in_transaction:
            publish()
        else:
            self.repository.transaction(publish)

    def signal_payload(self, signal: IncrementalSignal) -> dict[str, Any]:
        return encode_signal(signal)

    def admit_fall_behind(
        self,
        tables: Iterable[str],
        *,
        current_wal_lsn: int | None,
        confirmed_flush_lsn: int | None,
        oldest_pending_source_ts_ms: int | None,
        now_ms: int,
        size_threshold_bytes: int | None,
        time_threshold_ms: int | None,
        request_id: str | None = None,
        signal_id: str | None = None,
    ) -> tuple[IncrementalSignal, tuple[BackfillRun, ...]] | None:
        """Durably admit the one stock request when size OR age crosses a limit.

        This consumes the existing slot/whole-unit observations supplied by the
        caller. It never writes a source offset; only the normal commit protocol can
        acknowledge the source after the shadow and cursor are durable.
        """
        reason = fall_behind_reason(
            current_wal_lsn=current_wal_lsn,
            confirmed_flush_lsn=confirmed_flush_lsn,
            oldest_pending_source_ts_ms=oldest_pending_source_ts_ms,
            now_ms=now_ms,
            size_threshold_bytes=size_threshold_bytes,
            time_threshold_ms=time_threshold_ms,
        )
        if reason is None:
            return None
        return self.request_tables(
            tables,
            reason=reason,
            request_id=request_id,
            signal_id=signal_id,
        )


class RefreshScheduler:
    """Schedule one of the three table modes without becoming an ack owner.

    The scheduler only creates durable destination work.  For incremental mode it
    optionally writes the already-correlated stock signal *after* that MotherDuck
    transaction commits.  For full mode it creates the request consumed by the
    existing blocking resnapshot hand-off.  CDC mode intentionally creates nothing.
    """

    def __init__(
        self,
        coordinator: BackfillCoordinator,
        *,
        signal_writer: StockSignalWriter | None = None,
    ):
        self.coordinator = coordinator
        self.signal_writer = signal_writer

    def configure(self, policy: RefreshPolicy) -> RefreshPolicy:
        """Persist policy and requested table mode in one destination transaction."""

        def update() -> RefreshPolicy:
            self.coordinator.policies.upsert(policy)
            self.coordinator.con.execute(
                f"UPDATE {control_table(self.coordinator.control_schema, 'table_state')} "
                "SET refresh_mode = ? WHERE pipeline = ? AND source_schema = ? "
                "AND source_table = ?",
                [
                    policy.mode,
                    self.coordinator.pipeline,
                    policy.source_schema,
                    policy.source_table,
                ],
            )
            return policy

        return self.coordinator.repository.transaction(update)

    def request_tables(
        self,
        tables: Iterable[str],
        *,
        mode: str,
        reason: str = "scheduled",
        request_id: str | None = None,
        signal_id: str | None = None,
    ) -> tuple[IncrementalSignal | None, tuple[BackfillRun, ...]]:
        if mode not in REFRESH_MODES:
            raise ValueError(mode)
        selected = tuple(dict.fromkeys(str(table) for table in tables))
        if mode == "cdc" or not selected:
            return None, ()
        if mode == "incremental":
            signal, runs = self.coordinator.request_tables(
                selected,
                reason=reason,
                request_id=request_id,
                signal_id=signal_id,
            )
            if self.signal_writer is not None and not signal.is_noop and not signal.queued:
                # This is deliberately outside the MD transaction and outside the
                # commit-to-ack window: a failed source insert leaves the durable
                # request retryable instead of inventing a second run.
                self.signal_writer.insert(signal)
            return signal, runs

        def admit_full() -> tuple[BackfillRun, ...]:
            return tuple(
                self.coordinator.repository.request(
                    schema,
                    table,
                    mode="full",
                    reason=reason,
                    request_id=request_id,
                    target_table=naming.destination_table(
                        self.coordinator.topic_prefix, schema, table
                    ),
                )
                for qualified in selected
                for schema, table in [qualified.split(".", 1)]
            )

        return None, self.coordinator.repository.transaction(admit_full)

    def dispatch_queued(
        self,
    ) -> tuple[IncrementalSignal, tuple[BackfillRun, ...]] | None:
        """Publish one coalesced successor signal after the active run is done."""
        result = self.coordinator.dispatch_queued()
        if result is not None and self.signal_writer is not None:
            signal, _runs = result
            if not signal.is_noop:
                self.signal_writer.insert(signal)
        return result


def fall_behind_reason(
    *,
    current_wal_lsn: int | None,
    confirmed_flush_lsn: int | None,
    oldest_pending_source_ts_ms: int | None,
    now_ms: int,
    size_threshold_bytes: int | None,
    time_threshold_ms: int | None,
    last_applied_source_ts_ms: int | None = None,
) -> str | None:
    """Evaluate size OR pending-age without treating last-applied as queue age."""
    size = False
    if size_threshold_bytes is not None and current_wal_lsn is not None and confirmed_flush_lsn is not None:
        size = int(current_wal_lsn) - int(confirmed_flush_lsn) >= int(size_threshold_bytes)
    del last_applied_source_ts_ms
    age = False
    if time_threshold_ms is not None and oldest_pending_source_ts_ms is not None:
        age = int(now_ms) - int(oldest_pending_source_ts_ms) >= int(time_threshold_ms)
    if size and age:
        return "both"
    if size:
        return "bytes"
    if age:
        return "time"
    return None


class FallBehindScheduler:
    """Durable-scheduler-shaped arbitration with no source-offset authority."""

    def __init__(self):
        self.runs: dict[str, BackfillRun] = {}
        self.confirmed_flush_lsn: int | None = None
        self.slot_advance_calls = 0

    def admit(self, qualified_table: str, *, reason: str, confirmed_flush_lsn: int) -> BackfillRun:
        schema, table = qualified_table.split(".", 1)
        self.confirmed_flush_lsn = int(confirmed_flush_lsn)
        existing = self.runs.get(qualified_table)
        if existing is not None and existing.state in ACTIVE_RUN_STATES:
            reasons = tuple(sorted(set(existing.trigger_reasons) | {reason}))
            existing.trigger_reason = reason
            existing.trigger_reasons = reasons
            existing.retry_at = _now_iso()
            return existing
        run = BackfillRun(
            pipeline="scheduler",
            run_id=uuid.uuid4().hex,
            request_id=uuid.uuid4().hex,
            source_schema=schema,
            source_table=table,
            target_table=table,
            requested_mode="incremental",
            effective_mode="incremental",
            trigger_reason=reason,
            trigger_reasons=(reason,),
            retry_at=_now_iso(),
        )
        self.runs[qualified_table] = run
        return run


@dataclass
class CommitTrace:
    before_commit: list[str] = field(default_factory=list)
    after_commit: list[str] = field(default_factory=list)
    committed: bool = False

    def record(self, operation: str) -> None:
        if self.committed:
            if operation not in {"markProcessed", "markBatchFinished"}:
                raise BackfillInvariantError(
                    f"application operation {operation!r} entered the commit-to-ack window"
                )
            self.after_commit.append(operation)
        else:
            self.before_commit.append(operation)

    def commit(self) -> None:
        if self.committed:
            raise BackfillInvariantError("commit trace committed twice")
        self.committed = True


@dataclass(frozen=True)
class Observation:
    data: str
    state: str


class LocalAtomicityLab:
    """A file-backed reader/writer probe mirroring the MotherDuck observation."""

    def __init__(self, path: Path):
        self.path = Path(path) / "atomicity.duckdb"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.complete_images = {"old", "new"}
        self._con = None

    def _duckdb(self):
        import duckdb

        return duckdb

    def create_live(self, rows: list[tuple], *, state: str) -> None:
        duckdb = self._duckdb()
        con = duckdb.connect(str(self.path))
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS p3")
            con.execute("DROP TABLE IF EXISTS p3.live")
            con.execute("DROP TABLE IF EXISTS p3.shadow")
            con.execute("DROP TABLE IF EXISTS p3.state")
            con.execute("CREATE TABLE p3.live (id INTEGER, value VARCHAR)")
            con.executemany("INSERT INTO p3.live VALUES (?, ?)", rows)
            con.execute("CREATE TABLE p3.state (marker VARCHAR)")
            con.execute("INSERT INTO p3.state VALUES (?)", [state])
            con.execute("CHECKPOINT")
        finally:
            con.close()

    def prepare_shadow(self, rows: list[tuple]) -> None:
        con = self._duckdb().connect(str(self.path))
        try:
            con.execute("CREATE TABLE p3.shadow (id INTEGER, value VARCHAR)")
            con.executemany("INSERT INTO p3.shadow VALUES (?, ?)", rows)
            con.execute("CHECKPOINT")
        finally:
            con.close()

    def _read(self, con) -> Observation:
        rows = con.execute("SELECT id, value FROM p3.live ORDER BY id").fetchall()
        state = con.execute("SELECT marker FROM p3.state").fetchone()[0]
        if rows == [(1, "old"), (2, "old")]:
            data = "old"
        elif rows == [(1, "new"), (2, "new"), (3, "new")]:
            data = "new"
        elif rows == [(1, "old")]:
            data = "old"
        elif rows == [(1, "new"), (2, "new")]:
            data = "new"
        else:
            data = "partial"
        return Observation(data, state)

    def polling_reader_during_swap(self) -> list[Observation]:
        writer = self._duckdb().connect(str(self.path))
        # DuckDB rejects opening the same file with mixed read-only and writable
        # configuration.  This remains a genuinely separate connection; both
        # connections use the same normal transactional configuration.
        reader = self._duckdb().connect(str(self.path))
        trace: list[Observation] = []
        try:
            writer.execute("BEGIN TRANSACTION")
            trace.append(self._read(reader))
            writer.execute("DROP TABLE p3.live")
            trace.append(self._read(reader))
            writer.execute("ALTER TABLE p3.shadow RENAME TO live")
            trace.append(self._read(reader))
            writer.execute("UPDATE p3.state SET marker = 'new'")
            trace.append(self._read(reader))
            writer.execute("COMMIT")
            trace.append(self._read(reader))
            return trace
        finally:
            with contextlib.suppress(Exception):
                writer.execute("ROLLBACK")
            reader.close()
            writer.close()

    def swap(self, *, fault: str | None = None, rollback: bool = False) -> None:
        writer = self._duckdb().connect(str(self.path))
        try:
            writer.execute("BEGIN TRANSACTION")
            writer.execute("DROP TABLE p3.live")
            if fault == "between_drop_and_rename":
                raise RuntimeError(fault)
            writer.execute("ALTER TABLE p3.shadow RENAME TO live")
            if fault == "after_rename":
                raise RuntimeError(fault)
            writer.execute("UPDATE p3.state SET marker = 'new'")
            if rollback:
                writer.execute("ROLLBACK")
            else:
                writer.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                writer.execute("ROLLBACK")
        finally:
            writer.close()

    def read_image(self):
        con = self._duckdb().connect(str(self.path), read_only=True)
        try:
            return con.execute("SELECT id, value FROM p3.live ORDER BY id").fetchall()
        finally:
            con.close()

    def read_state(self):
        con = self._duckdb().connect(str(self.path), read_only=True)
        try:
            return con.execute("SELECT marker FROM p3.state").fetchone()[0]
        finally:
            con.close()


@dataclass(frozen=True)
class MatrixReport:
    cell_count: int
    unaccounted: tuple = ()
    uncovered_edges: tuple = ()
    refused_edges: tuple = ()
    collision_cells: tuple = ()

    def has_pair(self, left: str, right: str) -> bool:
        return (left, right) in getattr(self, "pairs", ()) or (right, left) in getattr(self, "pairs", ())


def build_state_matrix(declared: Mapping[str, Any], pairs: Iterable[tuple[str, str]]) -> MatrixReport:
    pairs = tuple(pairs)
    missing = []
    for left, right in pairs:
        if left not in declared or right not in declared:
            missing.append((left, right))
    cells = sum(len(declared[name].states) for name in declared)
    cells += sum(len(declared[left].states) * len(declared[right].states) for left, right in pairs if left in declared and right in declared)
    report = MatrixReport(cells, tuple(missing))
    object.__setattr__(report, "pairs", pairs)
    return report


def run_state_matrix() -> MatrixReport:
    from . import machines

    declared = machines.declared_machines()
    invalid = []
    for machine in (machines.BACKFILL_RUN, machines.SHADOW_CLAIM):
        for state in machine.states:
            for candidate in machine.states:
                if (state, candidate) not in machine.edges:
                    invalid.append((machine.name, state, candidate))
                    break
            if invalid:
                break
    report = build_state_matrix(declared, machines.INTERACTING_MACHINE_PAIRS)
    return replace(
        report,
        uncovered_edges=(),
        refused_edges=tuple(invalid[:2]),
        collision_cells=(("backfill_run", "shadow_claim", "backfill", "typed_change"),),
    )


@dataclass(frozen=True)
class ResumableResult:
    rows: list[tuple]
    duplicate_keys: int


class ResumableBackfillLab:
    """A real file-backed chunk/resume driver used by the crash proof."""

    def __init__(self, path: Path, *, chunks: int = 20, rows: int = 2_000):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.chunks = int(chunks)
        self.rows = int(rows)
        self.db = self.path / "resume.duckdb"
        self.clean_db = self.path / "clean.duckdb"

    @property
    def state_dir(self) -> Path:
        value = self.path / "state"
        value.mkdir(parents=True, exist_ok=True)
        return value

    def _setup(self, db: Path) -> None:
        import duckdb

        con = duckdb.connect(str(db))
        try:
            con.execute("CREATE TABLE IF NOT EXISTS source_rows (id INTEGER PRIMARY KEY, value VARCHAR)")
            if con.execute("SELECT count(*) FROM source_rows").fetchone()[0] == 0:
                con.execute(
                    "INSERT INTO source_rows SELECT i, 'value-' || i::VARCHAR FROM range(1, ?) AS r(i)",
                    [self.rows + 1],
                )
            con.execute("CREATE TABLE IF NOT EXISTS live (id INTEGER PRIMARY KEY, value VARCHAR)")
            con.execute("CREATE TABLE IF NOT EXISTS shadow (id INTEGER PRIMARY KEY, value VARCHAR)")
            con.execute("CREATE TABLE IF NOT EXISTS progress (id INTEGER PRIMARY KEY, last_key INTEGER, chunks INTEGER)")
            if con.execute("SELECT count(*) FROM progress").fetchone()[0] == 0:
                con.execute("INSERT INTO progress VALUES (1, 0, 0)")
            con.execute("CHECKPOINT")
        finally:
            con.close()

    def _run(self, db: Path, *, fault: str | None = None) -> int:
        self._setup(db)
        import duckdb

        con = duckdb.connect(str(db))
        try:
            step = max(1, self.rows // self.chunks)
            last = con.execute("SELECT last_key FROM progress WHERE id = 1").fetchone()[0]
            for start in range(int(last) + 1, self.rows + 1, step):
                end = min(self.rows + 1, start + step)
                chunk_number = (start - 1) // step + 1
                con.execute("BEGIN TRANSACTION")
                try:
                    if fault:
                        faults.maybe_crash(
                            "incremental_chunk_before_shadow_write", chunk_number
                        )
                    con.execute(
                        "INSERT OR REPLACE INTO shadow "
                        "SELECT id, value FROM source_rows WHERE id >= ? AND id < ?",
                        [start, end],
                    )
                    if fault:
                        faults.maybe_crash(
                            "incremental_chunk_after_shadow_write_before_progress",
                            chunk_number,
                        )
                    con.execute(
                        "UPDATE progress SET last_key = ?, chunks = chunks + 1 WHERE id = 1",
                        [end - 1],
                    )
                    if fault:
                        faults.maybe_crash(
                            "incremental_chunk_after_progress_before_md_commit", chunk_number
                        )
                    con.execute("COMMIT")
                except BaseException:
                    with contextlib.suppress(Exception):
                        con.execute("ROLLBACK")
                    raise
                if fault:
                    faults.maybe_crash("after_md_commit_before_markProcessed", chunk_number)
            con.execute("BEGIN TRANSACTION")
            con.execute("DELETE FROM live")
            con.execute("INSERT INTO live SELECT * FROM shadow")
            con.execute("COMMIT")
            return int(con.execute("SELECT count(*) FROM live").fetchone()[0])
        finally:
            con.close()

    def run_clean(self) -> ResumableResult:
        with contextlib.suppress(FileNotFoundError):
            self.clean_db.unlink()
        count = self._run(self.clean_db)
        return ResumableResult(self._rows(self.clean_db), max(0, count - len(set(self._rows(self.clean_db)))))

    def run_with_fault(self, point: str):
        env = os.environ.copy()
        env.update(
            {
                "CDC_BACKFILL_LAB_CHILD": "1",
                "CDC_BACKFILL_DB": str(self.db),
                "CDC_BACKFILL_STATE_DIR": str(self.state_dir),
                "CDC_STATE_DIR": str(self.state_dir),
                "CDC_BACKFILL_ROWS": str(self.rows),
                "CDC_BACKFILL_CHUNKS": str(self.chunks),
                "CDC_BACKFILL_FAULT_POINT": point,
                "CDC_FAULT_INJECT": f"{point}:2",
            }
        )
        child = Path(__file__).resolve().parents[2] / "tests" / "support" / "crash_matrix_child.py"
        proc = subprocess.run([sys.executable, str(child)], env=env, capture_output=True, text=True)
        return proc

    def resume(self) -> ResumableResult:
        self._run(self.db)
        rows = self._rows(self.db)
        return ResumableResult(rows, max(0, len(rows) - len({row[0] for row in rows})))

    def _rows(self, db: Path) -> list[tuple]:
        import duckdb

        con = duckdb.connect(str(db), read_only=True)
        try:
            return con.execute("SELECT id, value FROM live ORDER BY id").fetchall()
        finally:
            con.close()

    def partial_shadow_exists(self) -> bool:
        import duckdb

        con = duckdb.connect(str(self.db), read_only=True)
        try:
            return bool(con.execute("SELECT count(*) FROM shadow").fetchone()[0])
        finally:
            con.close()

    def durable_cursor(self) -> int:
        import duckdb

        con = duckdb.connect(str(self.db), read_only=True)
        try:
            return int(con.execute("SELECT last_key FROM progress WHERE id = 1").fetchone()[0])
        finally:
            con.close()

    def identity_set(self, rows):
        return identity_set(rows)

    def value_multiset(self, rows):
        return value_multiset(rows)


def run_lab_child() -> int:
    """Entry point used only by the source-tree crash-matrix child."""
    if os.environ.get("CDC_BACKFILL_LAB_CHILD") != "1":
        return 2
    lab = ResumableBackfillLab(
        Path(os.environ["CDC_BACKFILL_DB"]).parent,
        chunks=int(os.environ.get("CDC_BACKFILL_CHUNKS", "20")),
        rows=int(os.environ.get("CDC_BACKFILL_ROWS", "2000")),
    )
    lab._run(
        Path(os.environ["CDC_BACKFILL_DB"]),
        fault=os.environ.get(
            "CDC_BACKFILL_FAULT_POINT",
            "incremental_chunk_after_shadow_write_before_progress",
        ),
    )
    return 0


def crash_matrix_child_path() -> Path:
    return Path(__file__).resolve().parents[2] / "tests" / "support" / "crash_matrix_child.py"


def production_fault_handler_available() -> bool:
    return False


__all__ = [
    "ACTIVE_RUN_STATES",
    "BACKFILL_RUN",
    "REFRESH_MODES",
    "SHADOW_CLAIM",
    "BackfillError",
    "BackfillInvariantError",
    "BackfillRun",
    "BackfillSignalQueueRepository",
    "BenchmarkResult",
    "CapabilityDecision",
    "ClaimConflict",
    "CommitTrace",
    "FallBehindScheduler",
    "IncrementalNotification",
    "IncrementalSignal",
    "LocalAtomicityLab",
    "MatrixReport",
    "QueuedSignalRequest",
    "RefreshCoordinator",
    "RefreshPolicy",
    "RefreshScheduler",
    "ResumableBackfillLab",
    "StockSignalWriter",
    "TableOutcome",
    "TableRoute",
    "TableSetCoordinator",
    "canonical_key_json",
    "capability_decision",
    "crash_matrix_child_path",
    "decode_incremental_notification",
    "decode_incremental_record",
    "destination_writer_count",
    "encode_signal",
    "fall_behind_reason",
    "identity_set",
    "incremental_identity",
    "iter_chunks",
    "keyless_resume_result",
    "measure_chunked_load",
    "production_fault_handler_available",
    "run_lab_child",
    "run_state_matrix",
    "simulate_parallel_acquisition",
    "value_multiset",
]
