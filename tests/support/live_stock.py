"""Bounded live-stock support shared by the Round A/B rubric scenarios.

This module owns only test support.  It drives the real PostgreSQL source, the
production ``StockSignalWriter``, and a normal ``cdc-flight`` child; it never
constructs a Debezium callback or inserts a synthetic notification.  The
scan-boundary helpers are deliberately limited to the live-stock proof seam that
Round A and Round B share.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import time
import uuid
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def lsn_value(lsn: str) -> int:
    """Convert PostgreSQL's ``X/Y`` WAL spelling to an ordered integer."""
    high, low = str(lsn).split("/")
    return (int(high, 16) << 32) + int(low, 16)


@dataclass(frozen=True)
class DurableBackfillObservation:
    """One state observed by querying the committed destination control tables."""

    state: str
    notification_status: str
    table_state: str | None
    row_count: int
    chunk_count: int
    shadow_table: str | None
    observed_at_monotonic_ns: int
    polls: int


@dataclass(frozen=True)
class SourceCommit:
    """A source transaction observed after PostgreSQL reported its commit."""

    label: str
    operation: str
    committed_at_monotonic_ns: int


@dataclass(frozen=True)
class DecodedSourceCommit:
    """A ``test_decoding`` commit LSN associated with one bounded writer label."""

    label: str
    xid: str
    commit_lsn: int


class BoundedSourceCommitLedger:
    """Read a bounded, watermark-scoped source commit ledger exactly once.

    The non-consuming peek is intentionally polled until all expected labels are
    visible.  ``pg_logical_slot_get_changes`` is then called once, with the captured
    upper watermark.  A single consuming read would incorrectly turn "not decoded
    yet" into "does not exist" and would make this evidence race the writer.
    """

    def __init__(self, box, slot: str, lower_watermark: str, consistent_point: str):
        self.box = box
        self.slot = slot
        self.lower_watermark = lower_watermark
        self.consistent_point = consistent_point
        self.upper_watermark: str | None = None
        self.peek_polls = 0
        self.consuming_reads = 0
        self._closed = False

    @classmethod
    def open(
        cls, box, *, slot_prefix: str = "p3a_ledger"
    ) -> BoundedSourceCommitLedger:
        """Create a throwaway ``test_decoding`` slot at the writer boundary.

        The prefix is part of the shared A/B harness surface.  Keeping it
        caller-selectable makes the diagnostic slot name identify the scenario
        that owns it while preserving Round A's default.
        """
        lower = str(box.pg_query("SELECT pg_current_wal_flush_lsn()::text")[0][0])
        if not slot_prefix:
            raise ValueError("slot_prefix must not be empty")
        slot = f"{slot_prefix}_{os.getpid()}_{uuid.uuid4().hex[:12]}"
        created = box.pg_query(
            "SELECT slot_name, lsn::text FROM "
            "pg_create_logical_replication_slot(%s, 'test_decoding')",
            (slot,),
        )
        if not created:
            raise AssertionError(f"PostgreSQL did not return diagnostic slot {slot!r}")
        return cls(box, slot, lower, str(created[0][1]))

    def close(self) -> None:
        """Drop only this uniquely named diagnostic slot."""
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            self.box.pg_query("SELECT pg_drop_replication_slot(%s)", (self.slot,))

    def seal(self) -> str:
        """Capture the exclusive upper source watermark after all writers commit."""
        if self.upper_watermark is not None:
            return self.upper_watermark
        self.upper_watermark = str(
            self.box.pg_query("SELECT pg_current_wal_lsn()::text")[0][0]
        )
        if lsn_value(self.upper_watermark) < lsn_value(self.consistent_point):
            raise AssertionError(
                "source upper watermark moved behind the diagnostic slot point: "
                f"{self.upper_watermark} < {self.consistent_point}"
            )
        return self.upper_watermark

    @staticmethod
    def _commits(
        changes: Iterable[Sequence[Any]], evidence: dict[str, str]
    ) -> dict[str, DecodedSourceCommit]:
        transactions: dict[str, set[str]] = {}
        commits: dict[str, DecodedSourceCommit] = {}
        for lsn, xid_raw, data_raw in changes:
            xid = str(xid_raw)
            data = str(data_raw)
            if data.startswith("BEGIN"):
                transactions[xid] = set()
                continue
            for label, marker in evidence.items():
                if marker in data:
                    transactions.setdefault(xid, set()).add(label)
            if data.startswith("COMMIT"):
                for label in transactions.pop(xid, set()):
                    if label in commits:
                        raise AssertionError(
                            f"test_decoding observed source label {label!r} in more than "
                            "one transaction"
                        )
                    commits[label] = DecodedSourceCommit(
                        label=label,
                        xid=xid,
                        commit_lsn=lsn_value(str(lsn)),
                    )
        return commits

    def read(
        self,
        expected: set[str],
        *,
        evidence: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, DecodedSourceCommit]:
        """Poll a non-consuming peek, then perform the one bounded consuming read."""
        if not expected:
            raise ValueError("the bounded source ledger needs at least one expected label")
        evidence = evidence or {label: label for label in expected}
        if set(evidence) != expected:
            raise ValueError("source ledger evidence must cover exactly the expected labels")
        deadline = time.monotonic() + timeout
        seen: set[str] = set()
        peek_commits: dict[str, DecodedSourceCommit] = {}
        while True:
            self.peek_polls += 1
            peeked = self.box.pg_query(
                "SELECT lsn::text, xid::text, data "
                "FROM pg_logical_slot_peek_changes(%s, NULL, NULL)",
                (self.slot,),
            )
            peek_commits = self._commits(peeked, evidence)
            seen = set(peek_commits)
            if expected <= seen:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "test_decoding did not expose every bounded writer label after "
                    f"{self.peek_polls} peek polls; missing={sorted(expected - seen)}"
                )
            # This is only the retry cadence between durable slot peeks; the peeked
            # source state, not this delay, is the synchronization predicate.
            time.sleep(0.25)

        minimum_upper = max(
            fact.commit_lsn for label, fact in peek_commits.items() if label in expected
        )
        upper_deadline = min(deadline, time.monotonic() + 5.0)
        while True:
            candidate = str(
                self.box.pg_query("SELECT pg_current_wal_flush_lsn()::text")[0][0]
            )
            if lsn_value(candidate) >= minimum_upper:
                self.upper_watermark = candidate
                break
            if time.monotonic() >= upper_deadline:
                raise AssertionError(
                    "PostgreSQL's flush watermark did not cover the decoded writer "
                    f"commits: watermark={candidate}, minimum={minimum_upper}"
                )
            time.sleep(0.05)

        upper = self.upper_watermark
        assert upper is not None
        self.consuming_reads += 1
        changes = self.box.pg_query(
            "SELECT lsn::text, xid::text, data "
            "FROM pg_logical_slot_get_changes(%s, %s::pg_lsn, NULL)",
            (self.slot, upper),
        )
        commits = self._commits(changes, evidence)

        missing = expected - set(commits)
        if missing:
            raise AssertionError(
                "the one bounded consuming test_decoding read omitted writer labels: "
                f"{sorted(missing)}; peek_seen={sorted(seen)}"
            )
        extra = set(commits) - expected
        if extra:
            raise AssertionError(
                "the bounded source ledger crossed an unexpected labeled transaction: "
                f"{sorted(extra)}"
            )
        lower = lsn_value(self.lower_watermark)
        upper_value = lsn_value(upper)
        outside = {
            label: fact.commit_lsn
            for label, fact in commits.items()
            if not lower <= fact.commit_lsn <= upper_value
        }
        if outside:
            raise AssertionError(
                "bounded writer commit LSN escaped its source watermark window: "
                f"{outside}; window=[{self.lower_watermark}, {upper}]"
            )
        return {label: commits[label] for label in expected}


class SourceTransactionWriter:
    """Commit named source DML in separate, complete PostgreSQL transactions."""

    def __init__(self, dsn: str, *, label_prefix: str = "p3a-"):
        self.dsn = dsn
        if not label_prefix:
            raise ValueError("label_prefix must not be empty")
        self.label_prefix = label_prefix
        self.commits: list[SourceCommit] = []

    def commit(self, label: str, operation: str, statement: str, params: Sequence[Any] = ()) -> SourceCommit:
        if not label.startswith(self.label_prefix):
            raise ValueError(
                f"source labels must start with {self.label_prefix!r}"
            )
        import psycopg

        with psycopg.connect(self.dsn) as conn, conn.transaction():
            conn.execute(statement, params)
        fact = SourceCommit(
            label=label,
            operation=operation,
            committed_at_monotonic_ns=time.monotonic_ns(),
        )
        self.commits.append(fact)
        return fact


class LiveStockHarness:
    """Real source/stock/process lifecycle plus durable scan-boundary polling.

    The surface is intentionally the small live-stock portion authorized for
    Round A/B.  Later rubric items must not use this class as a substitute for
    their own production scheduling, health, or crash semantics.
    """

    def __init__(
        self,
        box,
        *,
        control_schema: str,
        dataset: str,
        pipeline: str,
        source_table: str,
        signal_tables: Sequence[str],
        request_id: str,
        signal_id: str,
        data_collection: str = "app.cdc_flight_signal",
    ):
        self.box = box
        self.control_schema = control_schema
        self.dataset = dataset
        self.pipeline = pipeline
        self.source_table = source_table
        self.signal_tables = tuple(signal_tables)
        self.request_id = request_id
        self.signal_id = signal_id
        self.run_ids: tuple[str, ...] = ()
        self.data_collection = data_collection
        self.process = None
        self.live_state_path = self.box.dir / "backfill_live_state.jsonl"
        self.boundary_barrier_path = self.box.dir / "backfill_scan_boundary_barrier"

    @property
    def _runs_table(self) -> str:
        return f'"{self.control_schema}"."backfill_runs"'

    @property
    def _state_table(self) -> str:
        return f'"{self.control_schema}"."table_state"'

    def admit_and_publish(self) -> tuple[str, tuple[str, ...]]:
        """Durably admit one stock run, then publish it through production code."""
        import duckdb

        from cdc_flight import destination
        from cdc_flight.backfill import BackfillCoordinator, StockSignalWriter

        with duckdb.connect(str(self.box.duckdb_path)) as con:
            destination.ensure_control_schema(con, self.control_schema)
            destination.ensure_dataset(con, self.dataset)
            coordinator = BackfillCoordinator(
                con,
                pipeline=self.pipeline,
                control_schema=self.control_schema,
                topic_prefix="cdcflight",
            )
            signal, runs = coordinator.request_tables(
                self.signal_tables,
                request_id=self.request_id,
                signal_id=self.signal_id,
            )
            if signal.signal_id != self.signal_id:
                raise AssertionError(signal)
            coordinator.reconcile_signal_effects(
                StockSignalWriter(
                    self.box.source.dsn,
                    data_collection=self.data_collection,
                )
            )
            self.run_ids = tuple(run.run_id for run in runs)
        return signal.signal_id, self.run_ids

    def start_normal_stock(self, *, captured_tables: str, timeout: float = 60.0) -> None:
        """Start the ordinary stock process and wait for its real source slot."""
        self.live_state_path.unlink(missing_ok=True)
        Path(f"{self.boundary_barrier_path}.ready").unlink(missing_ok=True)
        Path(f"{self.boundary_barrier_path}.release").unlink(missing_ok=True)
        self.process = self.box.spawn(
            max_seconds=240,
            idle_seconds=90,
            extra_env={
                "CDC_AUTO_DISCOVERY": "0",
                "CDC_TABLES": captured_tables,
                "CDC_BACKFILL_LIVE_STATE_PATH": str(self.live_state_path),
                "CDC_BACKFILL_BOUNDARY_BARRIER_PATH": str(self.boundary_barrier_path),
            },
            capture=True,
        )
        self.box.wait_for_slot_active(process=self.process, timeout=timeout)

    def _durable_state(self) -> tuple[str, str, str | None, int, int, str | None] | None:
        if not self.live_state_path.exists():
            return None
        observations = []
        for line in self.live_state_path.read_text(encoding="utf-8").splitlines():
            with contextlib.suppress(json.JSONDecodeError):
                observations.append(json.loads(line))
        matching = [
            entry
            for entry in observations
            if entry.get("pipeline") == self.pipeline
            and entry.get("request_id") == self.request_id
            and entry.get("signal_id") == self.signal_id
            and entry.get("table") == f"app.{self.source_table}"
            and (not self.run_ids or entry.get("run_id") in self.run_ids)
        ]
        if not matching:
            return None
        entry = matching[-1]
        return (
            str(entry["state"]),
            str(entry["notification_status"]),
            entry.get("shadow_table"),
            int(entry.get("row_count") or 0),
            int(entry.get("chunk_count") or 0),
            entry.get("table_state"),
        )

    def wait_for_scan_boundary(
        self,
        *,
        timeout: float = 90.0,
        poll_seconds: float = 0.1,
    ) -> DurableBackfillObservation:
        """Wait for committed ``STARTED``/``IN_PROGRESS`` and in-progress lifecycle."""
        deadline = time.monotonic() + timeout
        polls = 0
        while True:
            polls += 1
            state = self._durable_state()
            if state is not None:
                run_state, notification_status, shadow, rows, chunks, lifecycle = state
                if (
                    run_state == "loading"
                    and notification_status in {"STARTED", "IN_PROGRESS"}
                    and lifecycle == "in_progress"
                    and Path(f"{self.boundary_barrier_path}.ready").exists()
                ):
                    return DurableBackfillObservation(
                        state=run_state,
                        notification_status=notification_status,
                        table_state=lifecycle,
                        row_count=rows,
                        chunk_count=chunks,
                        shadow_table=shadow,
                        observed_at_monotonic_ns=time.monotonic_ns(),
                        polls=polls,
                    )
            if self.process is not None and self.process.poll() is not None:
                raise AssertionError(
                    "normal stock process exited before a durable scan boundary: "
                    f"returncode={self.process.returncode}, state={state}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "no durable STARTED/IN_PROGRESS scan boundary appeared within "
                    f"{timeout:.1f}s; last_state={state}"
                )
            time.sleep(poll_seconds)

    def release_scan_boundary(self) -> None:
        """Durably release the child after the harness has admitted the seam."""
        ready = Path(f"{self.boundary_barrier_path}.ready")
        if not ready.exists():
            raise AssertionError("cannot release a live scan boundary before READY")
        release = Path(f"{self.boundary_barrier_path}.release")
        temporary = release.with_name(f".{release.name}.{os.getpid()}.tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(
                {
                    "event": "BACKFILL_SCAN_BOUNDARY_RELEASED",
                    "pid": os.getpid(),
                    "released_at_monotonic_ns": time.monotonic_ns(),
                },
                stream,
                sort_keys=True,
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, release)

    def wait_for_terminal(
        self,
        *,
        timeout: float = 120.0,
        poll_seconds: float = 0.1,
    ) -> DurableBackfillObservation:
        """Wait for the committed final image and terminal run state."""
        deadline = time.monotonic() + timeout
        polls = 0
        while True:
            polls += 1
            state = self._durable_state()
            if state is not None:
                run_state, notification_status, shadow, rows, chunks, lifecycle = state
                if (
                    run_state == "complete"
                    and notification_status == "COMPLETED"
                    and lifecycle == "complete"
                ):
                    return DurableBackfillObservation(
                        state=run_state,
                        notification_status=notification_status,
                        table_state=lifecycle,
                        row_count=rows,
                        chunk_count=chunks,
                        shadow_table=shadow,
                        observed_at_monotonic_ns=time.monotonic_ns(),
                        polls=polls,
                    )
            if self.process is not None and self.process.poll() is not None:
                stdout, stderr = self.process.communicate()
                raise AssertionError(
                    "normal stock process exited before durable terminal publication: "
                    f"returncode={self.process.returncode}, state={state}, "
                    f"stdout={stdout[-3000:]!r}, stderr={stderr[-6000:]!r}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "no durable terminal backfill publication appeared within "
                    f"{timeout:.1f}s; last_state={state}"
                )
            time.sleep(poll_seconds)

    def stop_process(self) -> tuple[str, str]:
        """Bound cleanup of the normal child and return captured output."""
        if self.process is None:
            return "", ""
        if self.process.poll() is None:
            self.process.terminate()
        stdout, stderr = self.process.communicate(timeout=120)
        return stdout or "", stderr or ""


def _freeze(value: Any) -> Any:
    """Make nested source/destination values hashable without converting types to text."""
    if isinstance(value, dict):
        return ("mapping", tuple(sorted((str(k), _freeze(v)) for k, v in value.items())))
    if isinstance(value, (list, tuple)):
        return (type(value).__name__, tuple(_freeze(v) for v in value))
    if isinstance(value, float) and math.isnan(value):
        return ("float", "NaN")
    try:
        hash(value)
    except TypeError:
        return (type(value).__name__, repr(value))
    return (type(value).__name__, value)


def identity_set(rows: Iterable[Sequence[Any]], identity_columns: Sequence[int] = (0,)) -> set[Any]:
    """Return exact typed identity tuples, including support for composite keys."""
    columns = tuple(identity_columns)
    if not columns:
        raise ValueError("identity_columns must not be empty")
    return {
        _freeze(tuple(row[index] for index in columns))
        for row in rows
    }


def value_multiset(rows: Iterable[Sequence[Any]]) -> Counter:
    """Return an exact multiplicity oracle for complete row values."""
    return Counter(_freeze(tuple(row)) for row in rows)


def assert_exact_rows(
    source_rows: Iterable[Sequence[Any]],
    destination_rows: Iterable[Sequence[Any]],
    *,
    label: str,
    identity_columns: Sequence[int] = (0,),
) -> None:
    """Assert identity sets, value multisets, row counts, and physical-key uniqueness."""
    source = list(source_rows)
    destination = list(destination_rows)
    source_ids = identity_set(source, identity_columns)
    destination_ids = identity_set(destination, identity_columns)
    assert destination_ids == source_ids, f"{label} identity mismatch"
    assert value_multiset(destination) == value_multiset(source), f"{label} value mismatch"
    assert len(destination) == len(source), f"{label} multiplicity mismatch"
    physical_ids = [
        _freeze(tuple(row[index] for index in identity_columns))
        for row in destination
    ]
    duplicates = [key for key, count in Counter(physical_ids).items() if count > 1]
    assert not duplicates, f"{label} has duplicate physical identities: {duplicates[:10]}"


__all__ = [
    "BoundedSourceCommitLedger",
    "DecodedSourceCommit",
    "DurableBackfillObservation",
    "LiveStockHarness",
    "SourceCommit",
    "SourceTransactionWriter",
    "assert_exact_rows",
    "identity_set",
    "lsn_value",
    "value_multiset",
]
