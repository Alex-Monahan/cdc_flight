"""Rubric 4.7 — the repairs the applier reaches for but does not own.

Two things that used to live inside `applier.py` and are not the commit protocol
(A44 assigned it "the commit protocol, and only that"; Codex B6 found it back over a
thousand lines owning ambiguity-rebuild policy, recovery alert semantics, commit-watchdog
state and re-snapshot completion): the policy that converts an undecidable fold into a
durable re-snapshot request, and the watchdog that bounds a `COMMIT`. Both are *recovery*
semantics. The applier calls them; it does not decide them.
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time

from . import naming
from .errors import AmbiguousDelete, DestinationIdentityCollision

log = logging.getLogger("cdc_flight.self_heal")

# This is deliberately a closed vocabulary.  Operation names are diagnostic
# evidence, not an event stream: an unexpected name is folded into ``other`` so
# a long-lived Flight cannot grow a dictionary from untrusted or newly added
# call sites.
DESTINATION_OPERATION_CATEGORIES = (
    "alert_write",
    "catalog_work",
    "group_write",
    "ledger_claims_batch",
    "lease_refresh",
    "motherduck_commit",
    "motherduck_round_trip",
    "other",
)
_DESTINATION_OPERATION_CATEGORY_SET = frozenset(DESTINATION_OPERATION_CATEGORIES)


class DestinationOperationProgress:
    """Memory-only completion evidence for one pre-COMMIT destination operation.

    A watchdog must distinguish a sequence of completed destination operations from
    one native call that never returns.  The caller owns the operation context, so
    the only progress edge accepted here is the context manager's successful exit;
    there is deliberately no free-standing ``touch`` method that a polling thread
    could use to make a blocked operation appear healthy.

    Every successful destination operation is a progress edge.  The state is bounded
    to one active stack and one fixed operation table; it is not an event buffer and
    never performs I/O.  Unknown operation names are aggregated into ``other``.
    """

    def __init__(self, *, on_start=None, on_finish=None):
        self._lock = threading.Lock()
        self._active: list[tuple[int, str, float]] = []
        self._next_token = 0
        self._progress_sequence = 0
        self._last_progress = time.monotonic()
        self._operation_totals = {
            category: {"starts": 0, "completed": 0, "elapsed_sec": 0.0}
            for category in DESTINATION_OPERATION_CATEGORIES
        }
        self._on_start = on_start
        self._on_finish = on_finish

    @contextlib.contextmanager
    def operation(self, name: str, *, progressed: bool = True):
        """Track one named operation and publish progress only after success."""
        if not isinstance(name, str) or not name:
            raise ValueError("destination operation name must be non-empty")
        started = time.monotonic()
        # Keep the service-level aggregate outside this witness' lock.  The
        # service watchdog reads the two witnesses in the opposite order
        # (service context, then this progress object); invoking the callback
        # while holding this lock would create a lock-order inversion during
        # shutdown and could prevent the terminal service summary from being
        # written.
        if self._on_start is not None:
            self._on_start(name)
        with self._lock:
            self._next_token += 1
            token = self._next_token
            self._active.append((token, name, started))
            category = (
                name
                if name in _DESTINATION_OPERATION_CATEGORY_SET
                else "other"
            )
            self._operation_totals[category]["starts"] += 1
        succeeded = False
        try:
            yield
            succeeded = True
        finally:
            finished = time.monotonic()
            elapsed = max(0.0, finished - started)
            with self._lock:
                # The operation stack is private and strictly nested.  Refusing to
                # silently repair a mismatched close keeps a future instrumentation
                # mutation from manufacturing a progress edge.
                if not self._active or self._active[-1][0] != token:
                    raise RuntimeError(
                        f"destination operation stack mismatch while closing {name!r}"
                    )
                self._active.pop()
                if succeeded and progressed:
                    self._progress_sequence += 1
                    self._last_progress = finished
                self._operation_totals[category]["completed"] += 1
                self._operation_totals[category]["elapsed_sec"] += elapsed
            if self._on_finish is not None:
                self._on_finish(name, elapsed, succeeded, bool(succeeded and progressed))

    @property
    def progress_sequence(self) -> int:
        """Return the count of successful destination completion edges."""
        with self._lock:
            return self._progress_sequence

    @property
    def last_progress(self) -> float:
        """Return the monotonic time of the last successful completion edge."""
        with self._lock:
            return self._last_progress

    @property
    def active_operation_started_at(self) -> float | None:
        """Return the current operation's start time, or ``None`` while quiet."""
        with self._lock:
            return self._active[-1][2] if self._active else None

    def snapshot(self) -> dict[str, object]:
        """Return bounded diagnostic state without touching a destination."""
        with self._lock:
            active = self._active[-1] if self._active else None
            now = time.monotonic()
            return {
                "active_operation": active[1] if active else None,
                "active_operation_age_sec": (
                    round(now - active[2], 3) if active else None
                ),
                "progress_sequence": self._progress_sequence,
                "progress_age_sec": round(now - self._last_progress, 3),
            }

    def operation_table(self) -> dict[str, dict[str, int | float]]:
        """Return fixed-size operation evidence for the current group."""
        with self._lock:
            return {
                category: {
                    "starts": int(values["starts"]),
                    "completed": int(values["completed"]),
                    "elapsed_sec": round(float(values["elapsed_sec"]), 6),
                }
                for category, values in self._operation_totals.items()
            }


def request_resnapshot_for(
    ambiguous: AmbiguousDelete | DestinationIdentityCollision,
    *,
    alerts,
    pipeline: str,
    topic_prefix: str,
    enabled: bool,
) -> tuple[bool, dict | None]:
    """Turn an undecidable fold into a durable re-snapshot request (rubric 4.7).

    Returns `(queued, alert_or_None)`. The caller raises either way: the run still
    fails with a non-zero exit, because "I could not fold this and I have queued a
    rebuild" is information an operator wants even though no human action is required.

    The request is written on the alert sink's **independent** connection, so it
    survives the rollback of the group that could not be folded. The re-snapshot's
    consistent point is necessarily after the offending transaction (we already
    received it, so it is already in WAL), so the per-table watermark fences it and the
    loop terminates after exactly one re-snapshot (A47).

    The honesty note that belongs with it, and which `resnapshot` records in
    `table_events`: a re-snapshot replaces **current state**. The individual change
    events of the fenced span are not delivered, so a changelog (rubric 8.2) sees a
    discontinuity there — an image at the consistent point rather than the events that
    produced it. Current state is exact; per-event history for that span is not
    recoverable, because the ambiguity was precisely that the events did not say what
    they did.

    The three ways this can fail to queue anything are A51 row 42: the operator turned
    it off, the exception did not name a table, or the request could not be recorded.
    All three are permanent, all three say so in the alert, and all three are counted as
    manual-intervention cases rather than described as self-healing.
    """
    if not enabled:
        log.error(
            "CDC_AMBIGUOUS_RESNAPSHOT=0: not queueing a re-snapshot for the fold "
            "that could not be decided, so this failure will repeat on every run "
            "until a human intervenes"
        )
        return False, None
    schema, table = ambiguous.source_schema, ambiguous.source_table
    if not schema or not table:
        log.error(
            "an undecidable fold did not name its table, so no re-snapshot can be "
            "queued for it: %s", ambiguous,
        )
        return False, None
    target = ambiguous.target or naming.destination_table(topic_prefix, schema, table)
    recorded = alerts.request_snapshot(
        pipeline=pipeline, schema=schema, table=table, target=target
    )
    alert = {
        "severity": "critical",
        "code": "ambiguous_delete_resnapshot",
        "on_rollback": True,
        "message": (
            f"the fold for {schema}.{table} could not be decided, so the commit "
            "group was refused. "
            + (
                "The table is now marked awaiting_snapshot and the next run "
                "rebuilds it automatically; no human action is required, but "
                "per-event history for the rebuilt span is replaced by the "
                "snapshot image (rubric 4.7 / ADR 0001 §19/A47)."
                if recorded
                else "The re-snapshot request could NOT be recorded, so this "
                "failure WILL repeat until a human intervenes."
            )
        ),
        "context": {
            "source_schema": schema,
            "source_table": table,
            "target_table": target,
            "resnapshot_queued": recorded,
            "detail": str(ambiguous),
        },
    }
    return recorded, alert


@contextlib.contextmanager
def commit_watchdog(timeout: float, commit_id: int, stage=None, on_timeout=None):
    """Bound the post-COMMIT protocol. A hung commit or acknowledgement kills the process.

    Rubric 1.7 requires every injected fault to end in a clean recovery or a loud
    failure. A `COMMIT` that never returns is neither, and nothing in DuckDB or the
    MotherDuck client imposes a deadline of its own, so the run would hang for ever
    holding the lease (which is also rubric 4.5's "hanging or locking that prevents
    recovery").

    Hard-exiting is safe precisely *because* of Invariant O. The commit is ambiguous
    - it may already have been durable server-side - but nothing has entered
    Debezium's offset store, so the next run reads whichever of `W` / `W-prime` the
    destination actually holds and resumes from exactly there (ADR 0001 §4.6 F5).
    Exit code 75 (`EX_TEMPFAIL`) rather than the fault injector's 137: this is a real
    operational failure and a supervisor should retry it, and a test that cannot tell
    the two codes apart cannot tell the watchdog from a kill (A54).
    """
    if not timeout or timeout <= 0:
        yield
        return

    def _fire() -> None:  # pragma: no cover - exercised by the fault test in a child
        # This callback can run while COMMIT_ACK is active. It must not inspect the
        # destination, write an alert, log, or flush a stream. The commit protocol
        # pre-arms a durable alert before opening that window; leaving it in place is
        # the timeout's observable record. The hard exit is the only operation here.
        os._exit(75)

    timer = threading.Timer(timeout, _fire)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


@contextlib.contextmanager
def destination_operation_watchdog(
    timeout: float, progress: DestinationOperationProgress | None = None
):
    """Bound pre-COMMIT destination work without adding work to COMMIT_ACK.

    The Flight is a hard process boundary, so terminating the whole instance is the
    only reliable cancellation for a native DuckDB/MotherDuck call that does not
    return.  This guard is intentionally stopped immediately before the commit/ack
    protocol; the existing ``commit_watchdog`` owns that smaller window and its
    callback is likewise I/O-free.
    """
    if not timeout or timeout <= 0:
        yield lambda: None
        return

    stopped = threading.Event()
    observed_progress = progress.progress_sequence if progress is not None else None
    observed_active_started = None
    deadline = time.monotonic() + timeout if progress is None else None
    paused_deadline = None

    def _watch() -> None:  # pragma: no cover - exercised by a real service child
        nonlocal deadline, observed_progress, observed_active_started, paused_deadline
        while not stopped.wait(min(0.05, max(timeout / 10.0, 0.01))):
            now = time.monotonic()
            if progress is not None:
                current_progress = progress.progress_sequence
                active_started = progress.active_operation_started_at
                progress_changed = current_progress != observed_progress
                if progress_changed:
                    # A successful completion resets the active-operation budget.
                    # Carry that reset across a quiet gap without running a timer
                    # while no destination operation is active.
                    paused_deadline = now + timeout
                if active_started is None:
                    # A completed destination call leaves a quiet gap. There is
                    # no native operation to be hung in that gap, so the guard is
                    # deliberately unarmed until the next operation enters.
                    deadline = None
                    observed_active_started = None
                elif active_started != observed_active_started:
                    # Start a fresh per-operation budget from the operation's
                    # actual entry edge, or resume the reset deadline from the
                    # preceding completed operation. A quiet gap itself is never
                    # timed.
                    observed_active_started = active_started
                    if paused_deadline is None or paused_deadline <= now:
                        paused_deadline = active_started + timeout
                    deadline = paused_deadline
                elif progress_changed:
                    # A successful nested/completed operation is real progress;
                    # reset the active-operation budget without polling heartbeats.
                    deadline = now + timeout
                observed_progress = current_progress
            if deadline is not None and now >= deadline:
                # This callback can run on a thread that is unrelated to the
                # destination operation, and it may race COMMIT_ACK in a future
                # refactor.  It therefore remains strictly I/O-free.
                os._exit(75)

    timer = threading.Thread(
        target=_watch,
        name="cdc-flight-destination-operation-watchdog",
        daemon=True,
    )
    timer.start()

    def stop() -> None:
        stopped.set()

    try:
        yield stop
    finally:
        stop()
