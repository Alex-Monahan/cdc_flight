"""Deterministic fault injection at *protocol* anchor points.

Rubric 1.7 asks for "robust injection of failures in testing" (=5). Racing a
`kill -9` against a load is not robust - `probes/p07_crash_duplication.py` lost
that race outright and `probes/p13` only won it by inflating the workload to
400 000 rows. This module makes the interesting crash points *exact*.

It is inert unless `CDC_FAULT_INJECT` is set.

    CDC_FAULT_INJECT="<point>:<nth>[:<action>]"

**The points are named after the transactional protocol, not after the current
implementation** (review feedback: Opus M7 / Codex 9). The baseline's dlt handler
and ADR 0001's applier reach the same three anchors, so a fault test written
today keeps working after D5/D9/D1 land:

* `pre_commit` - the batch has been decoded and applied to the destination
  *within an open destination transaction*, which has not committed. A crash here
  must lose nothing and duplicate nothing: the transaction rolls back and
  Debezium replays. (Baseline: immediately before `dlt.run()`, which is where the
  baseline's "transaction" begins and ends.)
* `post_commit_pre_ack` - the destination transaction has **committed** and
  Debezium has **not** been acknowledged (`markProcessed()` /
  `markBatchFinished()` have not run), so the offset on disk still points before
  this batch. This is the at-least-once window: today a crash here duplicates the
  whole batch. Under ADR 0001's Invariant O this is the *normal* steady state
  between `COMMIT` and the acknowledgement, and it must be safe.
* `post_ack` - Debezium has been acknowledged and `offsets.dat` has been flushed,
  but the replication slot has not been confirmed yet (that happens on the next
  `poll()`). Under Invariant O a crash here loses nothing and duplicates nothing.

Legacy aliases `before_load` / `after_load` map onto `pre_commit` /
`post_commit_pre_ack` so existing scenarios keep working.

`<nth>` is 1-based over the **data-carrying commit groups** this process
performs (for `decode`, over data-carrying Debezium batches). Batches that
contain only internal/skipped records (Debezium heartbeats, transaction-metadata
markers) are deliberately not counted: once `provide.transaction.metadata=true`
and `heartbeat.interval.ms` land (ADR 0001 D5/D9), the first batch of a run is
frequently metadata-only, and counting it would silently disarm every fault test
(Opus M7).

`<action>` is either an exit code (default 137, what a SIGKILL looks like to a
shell - note that a *real* SIGKILL surfaces as `returncode == -9` through
`subprocess`, so do not unify the two assertions) or the literal `raise`, which
raises `InjectedFault` instead of hard-exiting. `raise` exercises the error
teardown path (ADR 0001 L3); the exit code exercises hard process death.

Examples:

    CDC_FAULT_INJECT=post_commit_pre_ack:1 cdc-flight --destination duckdb
    CDC_FAULT_INJECT=pre_commit:2:raise   cdc-flight --destination duckdb
"""

from __future__ import annotations

import logging
import os
import sys

log = logging.getLogger("cdc_flight.faults")

ENV_VAR = "CDC_FAULT_INJECT"

#: Protocol anchor points, in the order the applier reaches them.
#:
#: `decode`, `begin`, `mid_apply` and `spill` were added with the transactional
#: applier (Codex 9 carry-forward): before it, those code paths did not exist, so
#: the three original anchors were the whole protocol. They now bracket every
#: state the commit group passes through:
#:
#: * `decode`      - records decoded and assembled, no transaction open yet
#: * `begin`       - `BEGIN TRANSACTION` issued, nothing applied
#: * `spill`       - a unit's events staged into `_cdc_flight.spill_events`
#: * `mid_apply`   - the FIRST destination table of the group has been written and
#:                   the next has not; the transaction is still open. (It used to
#:                   fire *before* the table-write loop, so it could not detect a
#:                   transaction torn between table A and table B - Codex 6.)
#: * `pre_commit`  - everything (data + commit_log + resume point) written, not committed
#: * `post_commit_pre_ack` - committed, Debezium NOT acknowledged
#: * `post_ack`    - acknowledged, slot not confirmed (that is the next poll())
POINTS = (
    "decode",
    "begin",
    "spill",
    "mid_apply",
    "pre_commit",
    "post_commit_pre_ack",
    "post_ack",
)

#: Names kept working from the first fault-injection cut.
ALIASES = {"before_load": "pre_commit", "after_load": "post_commit_pre_ack"}

DEFAULT_EXIT_CODE = 137
RAISE = "raise"


class InjectedFault(RuntimeError):
    """Raised by `maybe_crash(...)` when the configured action is `raise`."""


class FaultSpecError(ValueError):
    """The `CDC_FAULT_INJECT` value is malformed or names an unknown point."""


def parse_spec(raw: str | None) -> tuple[str, int, int | str] | None:
    """Parse and validate `CDC_FAULT_INJECT`. Returns None when unset.

    Raises `FaultSpecError` for anything malformed, so a typo fails the run
    instead of leaving a fault test vacuously green (Codex 9).
    """
    if not raw:
        return None
    parts = raw.split(":")
    point = ALIASES.get(parts[0].strip(), parts[0].strip())
    if point not in POINTS:
        raise FaultSpecError(
            f"{ENV_VAR}: unknown point {parts[0]!r}; expected one of "
            f"{POINTS} (or aliases {tuple(ALIASES)})"
        )
    try:
        nth = int(parts[1]) if len(parts) > 1 and parts[1] else 1
    except ValueError as exc:
        raise FaultSpecError(f"{ENV_VAR}: <nth> must be an integer, got {parts[1]!r}") from exc
    if nth < 1:
        raise FaultSpecError(f"{ENV_VAR}: <nth> is 1-based, got {nth}")

    action: int | str = DEFAULT_EXIT_CODE
    if len(parts) > 2 and parts[2]:
        if parts[2].strip().lower() == RAISE:
            action = RAISE
        else:
            try:
                action = int(parts[2])
            except ValueError as exc:
                raise FaultSpecError(
                    f"{ENV_VAR}: <action> must be an exit code or {RAISE!r}, got {parts[2]!r}"
                ) from exc
    if len(parts) > 3:
        raise FaultSpecError(f"{ENV_VAR}: too many fields in {raw!r}")
    return point, nth, action


#: Sentinel distinguishing "not parsed yet" from "parsed, and there is no fault".
_UNPARSED = object()
_spec_cache: object = _UNPARSED


def validate_env() -> tuple[str, int, int | str] | None:
    """Parse the environment once, at start-up, so a bad spec fails loudly."""
    return refresh()


def refresh() -> tuple[str, int, int | str] | None:
    """Re-read `CDC_FAULT_INJECT` and cache the result. Tests call this after
    changing the environment."""
    global _spec_cache
    _spec_cache = parse_spec(os.environ.get(ENV_VAR))
    return _spec_cache  # type: ignore[return-value]


def _spec() -> tuple[str, int, int | str] | None:
    """The parsed spec, cached.

    Cached because `maybe_crash` is called from inside the commit->ack window, and
    the binding principle says that window contains nothing but the
    acknowledgement (Codex 7). Re-reading and re-parsing an environment variable
    there is exactly the kind of unrelated work the principle excludes; after the
    first call this is a tuple comparison.
    """
    if _spec_cache is _UNPARSED:
        return refresh()
    return _spec_cache  # type: ignore[return-value]


def maybe_crash(point: str, nth: int) -> None:
    """Fire the configured fault if this is the configured point + data batch.

    `os._exit` on purpose for the exit-code action: no atexit hooks, no JVM
    shutdown, no flushing of Debezium's offset file - exactly what a `kill -9`
    leaves behind. The `raise` action instead drives Debezium's error-teardown
    path, which is a different (and, per ADR 0001 §1.2, differently dangerous)
    lifecycle path.
    """
    spec = _spec()
    if spec is None:
        return
    want_point, want_nth, action = spec
    if ALIASES.get(point, point) != want_point or nth != want_nth:
        return
    log.error("FAULT INJECTION: firing at %s (data batch %s) action=%s", point, nth, action)
    sys.stdout.flush()
    sys.stderr.flush()
    if action == RAISE:
        raise InjectedFault(f"injected fault at {point} (data batch {nth})")
    os._exit(int(action))
