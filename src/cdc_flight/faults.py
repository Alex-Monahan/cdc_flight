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

**Destination and network faults (rubric 1.7's route to 5).** The anchors above are
all places *we* stand; they cannot express "the destination refused the write" or
"the connection went away". Those live in `DESTINATION_POINTS` and are injected by
`wrap_destination()`, which wraps the single connection the applier writes through:

    CDC_FAULT_INJECT=destination_write:2    # a data write is rejected mid-transaction
    CDC_FAULT_INJECT=destination_commit:1   # COMMIT raises BEFORE executing
    CDC_FAULT_INJECT=destination_commit_late:1  # COMMIT EXECUTES, then raises -
                                            # genuinely AMBIGUOUS (§4.6 F5)
    CDC_FAULT_INJECT=destination_hang:1     # COMMIT never returns (bounded by
                                            # CDC_COMMIT_TIMEOUT, then exit 75)
    CDC_FAULT_INJECT=destination_close:1    # the connection is severed mid-transaction
    CDC_FAULT_INJECT=swap:1                 # between the DROP and the RENAME of a
                                            # backfill swap (1.6)

`destination_hang`'s duration is `CDC_FAULT_HANG_SECONDS` (default 3600). It used to be
read out of `<action>`, so `destination_hang:1` hung for 137 seconds - the *default exit
code* reinterpreted as a duration - which is undocumented, surprising, and (with the
shipped `CDC_COMMIT_TIMEOUT=300`) would have ended the hang before the watchdog it
exists to test could fire (Opus MAJOR-5).

**Every fired anchor writes a machine-readable record** to `$CDC_STATE_DIR/fault_fired.json`
before it does anything else, including before `os._exit`. A fault test that only checks
an exit code cannot tell "the watchdog bounded a hung COMMIT" from "the harness killed
the process"; with the record it can assert the exact anchor that fired (Codex M2).

The *network* fault that matters most cannot be injected from inside the process at
all - a source whose packets simply stop arriving, with the sockets left open - so it
is injected from outside by a TCP relay the test owns (`tests/tcp_relay.py`).

Every one of these must end in exactly one of two states, and `tests/1.7_fault_
injection/` asserts which for each: a clean recovery with the ledger unchanged, or a
non-zero exit with an accurate `last_run.json`. Silence, `ok: true`, a hang or a
duplicate are all failures of the item.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

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
    "swap",
    "pre_commit",
    "post_commit_pre_ack",
    "post_ack",
)

#: Points that are **not** reached by `maybe_crash`: they describe something the
#: destination does to us rather than somewhere we can stand. They are injected by
#: `FaultyConnection`, which wraps the one destination connection, and their action
#: is fixed by the point (an error, a severed connection, a hang) rather than
#: chosen with `<action>`. See `wrap_destination`.
#:
#: * `destination_write`  - the Nth data group's first data-modifying statement
#:   raises, with the destination transaction open. Rolls back; must lose nothing.
#: * `destination_commit` - `COMMIT` itself raises. **Ambiguous**: the destination
#:   may or may not have committed, so recovery has to be right either way (ADR
#:   0001 §4.6 F5).
#: * `destination_hang`   - `COMMIT` never returns. Bounded by
#:   `CDC_COMMIT_TIMEOUT`; the run must die loudly rather than hang for ever.
#: * `destination_close`  - the connection is severed mid-transaction, which is
#:   what a dropped network route to MotherDuck looks like.
#: * `destination_commit_late` - `COMMIT` is EXECUTED and then raises. This is the
#:   genuinely ambiguous shape: the server committed, the client saw an error, and the
#:   two are indistinguishable from where we stand. `destination_commit` raises
#:   *before* the statement runs, which is an ordinary uncommitted failure wearing an
#:   ambiguous name (Codex M2); both are kept, because the recovery has to be right
#:   either way and only having the easy one proved half the claim.
DESTINATION_POINTS = (
    "destination_write",
    "destination_commit",
    "destination_commit_late",
    "destination_hang",
    "destination_close",
)

#: **Recovery anchors** (rubric 1.7's route from 4 to 5, added with rubric 1.9's state
#: machines). The acquisition recovery is the one durable sequence in the tree that
#: cannot be made atomic — it mutates the to-do list, `offsets.dat`, the durable resume
#: point and the replication slot, in that order, and a crash between any two of them
#: used to leave a state the Flight diagnosed as an operator error and refused to start
#: on, for ever (Codex B3 / Opus MAJOR-1). The 1.6—1.8 round proved those cuts through a
#: **test seam** (`recovery.resume(on_phase=...)`), which proves the *logic* resumes and
#: not that a hard-killed process does: `os._exit` skips every `except`, every `finally`
#: and every atexit hook, and the seam runs on none of those paths. These anchors put a
#: real `os._exit` at each boundary, so the crash-cut table in A53 is measured rather
#: than modelled — which is the difference the 1.7 hold was about.
#:
#: * `recovery_requested`             - the journal row and the to-do list are durable
#:   and NOTHING has been destroyed. The next run must resume, not re-diagnose.
#: * `recovery_offsets_file_deleted`  - `offsets.dat` is gone, the journal still says
#:   `requested`. A53's benign cut: `file absent / row present` -> rebuilt.
#: * `recovery_resume_point_deleted`  - the durable resume point is gone, the slot is
#:   not. The next run re-runs the drop from `offsets_file_deleted`.
#: * `recovery_armed`                 - the slot is dropped and the journal has not
#:   recorded it. The dangerous one: the forced `snapshot.mode` lives only in the row.
#: * `table_rebuild_queued`           - the durable to-do list is mid-write.
RECOVERY_POINTS = (
    "recovery_requested",
    "recovery_offsets_file_deleted",
    "recovery_resume_point_deleted",
    "recovery_armed",
    "table_rebuild_queued",
)

ALL_POINTS = POINTS + DESTINATION_POINTS + RECOVERY_POINTS

#: Names kept working from the first fault-injection cut.
ALIASES = {"before_load": "pre_commit", "after_load": "post_commit_pre_ack"}

DEFAULT_EXIT_CODE = 137
RAISE = "raise"


class DestinationFault(RuntimeError):
    """Raised by `FaultyConnection` for a `destination_*` point.

    Deliberately not an `InjectedFault`: it stands in for a real destination error
    (a rejected write, a severed connection), so it must travel the same path a
    genuine `duckdb.Error` travels - through `_rollback_quietly` and out of the run
    with a non-zero exit - rather than a path that only fault tests take.
    """


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
    if point not in ALL_POINTS:
        raise FaultSpecError(
            f"{ENV_VAR}: unknown point {parts[0]!r}; expected one of "
            f"{ALL_POINTS} (or aliases {tuple(ALIASES)})"
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


#: Which data-carrying commit group the applier is currently building, 1-based.
#: Set by the applier at the top of `commit_group`, read by `FaultyConnection`.
#: A module-level int rather than a callback because the alternative is for the
#: connection wrapper to *infer* the group index from the SQL it sees, and an
#: inferred index is exactly how a fault test goes vacuously green (Opus M7).
_current_group = 0


#: How many times this process has reached each NON-group anchor, 1-based. The
#: protocol anchors index by commit group, which is the right `<nth>` for them; a
#: recovery phase boundary is not a commit group, and an index that is a function of the
#: workload is one that silently stops firing (Opus M7). `arrival()` is the counter for
#: anchors that happen once (or twice) per RUN rather than once per group.
_arrivals: dict[str, int] = {}


def arrival(point: str) -> int:
    """The 1-based count of times this process has reached `point`."""
    _arrivals[point] = _arrivals.get(point, 0) + 1
    return _arrivals[point]


def reset_arrivals() -> None:
    """Test seam: forget how many times each non-group anchor has been reached."""
    _arrivals.clear()


def arm_group(nth: int) -> None:
    global _current_group
    _current_group = nth


def current_group() -> int:
    return _current_group


#: Statements that count as "writing data" for `destination_write`. Bookkeeping
#: (BEGIN, SELECT, the lease, the control schema) is deliberately excluded: the
#: point is documented as "the destination rejected a write *of this group's data*",
#: and firing on the lease renewal would test a different thing under the same name.
_DATA_STATEMENTS = ("insert into", "update ", "delete from", "create table", "create or replace")


def _is_data_statement(sql: str) -> bool:
    lowered = sql.lstrip().lower()
    if lowered.startswith(("insert into _cdc_flight", "delete from _cdc_flight",
                           "update _cdc_flight", "insert or replace into _cdc_flight")):
        return False
    return lowered.startswith(_DATA_STATEMENTS)


class FaultyConnection:
    """A destination connection that fails the way a destination really fails.

    Wraps the single DuckDB/MotherDuck connection the applier writes through and
    injects one of `DESTINATION_POINTS` at the configured data-carrying commit
    group. Everything else is delegated untouched, including `cursor()` (the alert
    sink's independent connection is deliberately **not** made faulty: "the write
    failed" and "you cannot even be told the write failed" are different faults, and
    conflating them would hide which one the test proved).
    """

    def __init__(self, con, point: str, nth: int, *, hang_seconds: float = 3600.0):
        self._con = con
        self._point = point
        self._nth = nth
        self._hang_seconds = hang_seconds
        self.fired = False

    # -- the injected surface ---------------------------------------------- #
    def execute(self, sql, *args, **kwargs):
        late = False
        if not self.fired and _current_group == self._nth:
            statement = sql if isinstance(sql, str) else str(sql)
            self._maybe_fire(statement)
            late = self.fired and self._point == "destination_commit_late"
        result = self._con.execute(sql, *args, **kwargs)
        if late:
            # The COMMIT really ran, and THEN the client lost the answer. This is the
            # shape §4.6 F5 is about and the shape `destination_commit` is not.
            log.error(
                "FAULT INJECTION: destination COMMIT executed, then the client lost "
                "the answer (group %s)", self._nth,
            )
            raise DestinationFault(
                "injected destination COMMIT failure AFTER the statement executed "
                "(genuinely ambiguous: the destination committed and we cannot know it)"
            )
        return result

    def _maybe_fire(self, statement: str) -> None:
        lowered = statement.lstrip().lower()
        if self._point == "destination_write" and _is_data_statement(statement):
            self.fired = True
            record_fired(self._point, self._nth, "raise")
            log.error("FAULT INJECTION: destination rejects %r (group %s)",
                      statement[:60], self._nth)
            raise DestinationFault(
                f"injected destination write failure on {statement[:80]!r}"
            )
        if lowered.startswith("commit"):
            if self._point == "destination_commit":
                self.fired = True
                record_fired(self._point, self._nth, "raise")
                log.error("FAULT INJECTION: destination COMMIT fails (group %s)", self._nth)
                raise DestinationFault(
                    "injected destination COMMIT failure before the statement ran"
                )
            if self._point == "destination_commit_late":
                self.fired = True
                record_fired(self._point, self._nth, "raise")
                return
            if self._point == "destination_hang":
                self.fired = True
                record_fired(self._point, self._nth, f"hang:{self._hang_seconds}")
                log.error(
                    "FAULT INJECTION: destination COMMIT hangs for %ss (group %s)",
                    self._hang_seconds, self._nth,
                )
                sys.stdout.flush()
                time.sleep(self._hang_seconds)
        if self._point == "destination_close" and _is_data_statement(statement):
            self.fired = True
            record_fired(self._point, self._nth, "close")
            log.error("FAULT INJECTION: severing the destination connection (group %s)",
                      self._nth)
            try:
                self._con.close()
            except Exception:  # pragma: no cover - closing a broken handle
                log.debug("closing the destination connection raised", exc_info=True)
            raise DestinationFault("injected severed destination connection")

    # -- everything else --------------------------------------------------- #
    def __getattr__(self, name):
        return getattr(self._con, name)


#: How long `destination_hang` blocks inside `COMMIT`. Its OWN environment variable.
#: It used to be `<action>` reinterpreted as seconds, so a bare `destination_hang:1`
#: hung for 137 s (the default exit code) - undocumented, and shorter than the shipped
#: `CDC_COMMIT_TIMEOUT` of 300 s, which means the anchor would have quietly failed to
#: exercise the watchdog it exists for (Opus MAJOR-5).
HANG_SECONDS_ENV = "CDC_FAULT_HANG_SECONDS"
DEFAULT_HANG_SECONDS = 3600.0


def hang_seconds() -> float:
    raw = os.environ.get(HANG_SECONDS_ENV)
    if not raw:
        return DEFAULT_HANG_SECONDS
    try:
        return float(raw)
    except ValueError as exc:
        raise FaultSpecError(
            f"{HANG_SECONDS_ENV}: expected a number of seconds, got {raw!r}"
        ) from exc


def wrap_destination(con, *, hang_seconds: float | None = None):
    """Return `con`, or a `FaultyConnection` when a `destination_*` fault is armed."""
    spec = _spec()
    if spec is None:
        return con
    point, nth, _action = spec
    if point not in DESTINATION_POINTS:
        return con
    hang = hang_seconds if hang_seconds is not None else globals()["hang_seconds"]()
    log.warning(
        "destination fault armed: %s at data group %s%s",
        point, nth, f" (hang {hang}s)" if point == "destination_hang" else "",
    )
    return FaultyConnection(con, point, nth, hang_seconds=hang)


#: Where a fired anchor records itself. Inside `CDC_STATE_DIR` so a test that owns a
#: sandbox owns the record too, and so a hard `os._exit` cannot outrun it.
FIRED_FILENAME = "fault_fired.json"


def fired_record_path() -> str | None:
    state_dir = os.environ.get("CDC_STATE_DIR")
    if not state_dir:
        return None
    return os.path.join(state_dir, FIRED_FILENAME)


def record_fired(point: str, nth: int, action) -> None:
    """Write the machine-readable "this anchor fired" record. Never raises.

    Rubric 1.7's claim is that a fault produced a specific outcome, and an exit code
    alone cannot carry that: `test_a_hung_commit_...` accepted 75, -9, 137 or 1, so it
    passed if the run died of anything at all (Opus MAJOR-5), and the matrix accepted
    any non-zero run without establishing that the selected fault had fired (Codex M2).
    A test can now assert the exact anchor.
    """
    path = fired_record_path()
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as handle:
            json.dump(
                {"point": point, "nth": nth, "action": str(action), "pid": os.getpid()},
                handle,
            )
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:  # pragma: no cover - reporting must never mask the fault
        log.debug("could not write the fired-fault record", exc_info=True)


def read_fired_record(state_dir) -> dict | None:
    """The anchor that fired in a run whose state directory is `state_dir`, or None."""
    path = os.path.join(str(state_dir), FIRED_FILENAME)
    try:
        with open(path) as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


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
    # BEFORE the exit, and fsynced: `os._exit` runs no atexit hook, so this is the only
    # evidence a hard-killed run leaves behind that the anchor it was armed at is the
    # one that fired.
    record_fired(point, nth, action)
    sys.stdout.flush()
    sys.stderr.flush()
    if action == RAISE:
        raise InjectedFault(f"injected fault at {point} (data batch {nth})")
    os._exit(int(action))
