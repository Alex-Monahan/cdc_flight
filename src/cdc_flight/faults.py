"""Deterministic fault injection.

Rubric 1.7 asks for "robust injection of failures in testing" (=5). Racing a
`kill -9` against a load is not robust - `probes/p07_crash_duplication.py` lost
that race outright and `probes/p13` only won it by inflating the workload to
400 000 rows. This module makes the interesting crash points *exact*.

It is inert unless `CDC_FAULT_INJECT` is set, and the only thing it can do is
kill this process. Nothing here alters the data path.

    CDC_FAULT_INJECT="<point>:<nth>[:<exit_code>]"

Points:

* `before_load` - the batch has been decoded but nothing has been written to the
  destination. A crash here must lose nothing (Debezium replays the batch).
* `after_load` - the destination transaction has committed but
  `RecordCommitter.markProcessed()` / `markBatchFinished()` have *not* run, so
  the Debezium offset on disk still points before this batch. This is the
  at-least-once window: today a crash here duplicates the whole batch.

`<nth>` is 1-based over the batches this process handles.

Example:

    CDC_FAULT_INJECT=after_load:1 cdc-flight --destination duckdb
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("cdc_flight.faults")

ENV_VAR = "CDC_FAULT_INJECT"
POINTS = ("before_load", "after_load")
DEFAULT_EXIT_CODE = 137  # what a SIGKILL looks like to a shell


def _spec() -> tuple[str, int, int] | None:
    raw = os.environ.get(ENV_VAR)
    if not raw:
        return None
    parts = raw.split(":")
    point = parts[0].strip()
    if point not in POINTS:
        raise ValueError(f"{ENV_VAR}: unknown point {point!r}; expected one of {POINTS}")
    nth = int(parts[1]) if len(parts) > 1 and parts[1] else 1
    code = int(parts[2]) if len(parts) > 2 and parts[2] else DEFAULT_EXIT_CODE
    return point, nth, code


def maybe_crash(point: str, nth: int) -> None:
    """Hard-kill this process if the configured fault point has been reached.

    `os._exit` on purpose: no atexit hooks, no JVM shutdown, no flushing of
    Debezium's offset file - exactly what a `kill -9` leaves behind.
    """
    spec = _spec()
    if spec is None:
        return
    want_point, want_nth, code = spec
    if point != want_point or nth != want_nth:
        return
    log.error("FAULT INJECTION: crashing at %s (batch %s) with exit code %s", point, nth, code)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)
