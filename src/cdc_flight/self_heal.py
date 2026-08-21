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

from . import naming
from .errors import AmbiguousDelete, DestinationIdentityCollision

log = logging.getLogger("cdc_flight.self_heal")


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
def destination_operation_watchdog(timeout: float):
    """Bound pre-COMMIT destination work without adding work to COMMIT_ACK.

    The service worker is a hard process boundary, so terminating it is the only
    reliable cancellation for a native DuckDB/MotherDuck call that does not return.
    This guard is intentionally stopped immediately before the commit/ack protocol;
    the existing ``commit_watchdog`` owns that smaller window and its callback is
    likewise I/O-free.
    """
    if not timeout or timeout <= 0:
        yield lambda: None
        return

    def _fire() -> None:  # pragma: no cover - exercised by a real service child
        os._exit(75)

    timer = threading.Timer(timeout, _fire)
    timer.daemon = True
    timer.start()
    stopped = False

    def stop() -> None:
        nonlocal stopped
        if not stopped:
            stopped = True
            timer.cancel()

    try:
        yield stop
    finally:
        stop()
