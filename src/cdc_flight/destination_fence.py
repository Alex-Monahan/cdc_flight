"""Structural epoch fencing for every service-owned destination write.

The service path deliberately has one destination handle.  This module makes that
fact enforceable at the handle boundary: a service connection and every cursor it
creates route mutations through the current lease epoch immediately before the
mutation.  Callers therefore cannot accidentally omit a fence when they add a new
control, recovery, alert, catalog, or data writer.

The lease implementation receives the raw DuckDB handle while the destination write
stays in the same transaction.  That avoids recursively fencing the lease's own
conditional UPDATE while preserving the important ordering:

    BEGIN -> lease epoch CAS -> destination mutation -> COMMIT

An explicit transaction keeps that ordering for a whole commit group.  A standalone
mutation gets its own short transaction, so independent run-log/alert cursors are
covered as well.
"""

from __future__ import annotations

import re
from collections.abc import Callable

_LEADING_COMMENT = re.compile(r"^(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.DOTALL)


def _first_keyword(statement: object) -> str:
    text = str(statement)
    text = _LEADING_COMMENT.sub("", text).lstrip().lower()
    return text.split(None, 1)[0] if text else ""


def _is_transaction_control(statement: object) -> str | None:
    keyword = _first_keyword(statement)
    text = _LEADING_COMMENT.sub("", str(statement)).strip().lower().rstrip(";").strip()
    if keyword in {"begin", "start"}:
        return "begin"
    if keyword in {"commit", "end"}:
        return "commit"
    if keyword == "rollback" and not text.startswith("rollback to"):
        return "rollback"
    return None


def is_destination_mutation(statement: object) -> bool:
    """Classify SQL conservatively for the service write boundary.

    Reads and session/catalog inspection are allowed without a lease write.  Every
    other SQL form is treated as a mutation, including ``WITH`` (which may contain
    DML) and DDL.  A conservative false positive costs one tiny fence; a false
    negative would reopen the stale-generation write path this boundary exists to
    close.
    """
    if _is_transaction_control(statement) is not None:
        return False
    keyword = _first_keyword(statement)
    return keyword not in {
        "",
        "select",
        "show",
        "describe",
        "desc",
        "explain",
        "pragma",
        "set",
        "reset",
        "use",
        "load",
        "install",
    }


def unwrap_destination_handle(handle):
    """Return the raw driver handle hidden by one or more fence wrappers."""
    current = handle
    seen: set[int] = set()
    while id(current) not in seen:
        seen.add(id(current))
        raw = getattr(current, "_epoch_fence_raw", None)
        if raw is None:
            break
        current = raw
    return current


class _FencedOperations:
    """Common transaction/fence implementation for connections and cursors."""

    def __init__(self, raw, lease, context) -> None:
        self._epoch_fence_raw = raw
        self._epoch_fence_lease = lease
        self._epoch_fence_context = context
        self._epoch_fence_in_transaction = False

    @property
    def _raw(self):
        return self._epoch_fence_raw

    def _assert_and_fence(self) -> None:
        self._epoch_fence_context.assert_writable()
        self._epoch_fence_lease.fence(self._raw)

    def _raw_execute(self, sql, *args, **kwargs):
        return self._raw.execute(sql, *args, **kwargs)

    def _run_mutation(self, operation: Callable[[], object]):
        if self._epoch_fence_in_transaction:
            self._assert_and_fence()
            return operation()

        self._raw_execute("BEGIN TRANSACTION")
        self._epoch_fence_in_transaction = True
        try:
            self._assert_and_fence()
            result = operation()
            self._raw_execute("COMMIT")
            self._epoch_fence_in_transaction = False
            return result
        except BaseException:
            try:
                self._raw_execute("ROLLBACK")
            finally:
                self._epoch_fence_in_transaction = False
            raise

    def execute(self, sql, *args, **kwargs):
        control = _is_transaction_control(sql)
        if control == "begin":
            result = self._raw_execute(sql, *args, **kwargs)
            self._epoch_fence_in_transaction = True
            return result
        if control == "commit":
            result = self._raw_execute(sql, *args, **kwargs)
            self._epoch_fence_in_transaction = False
            return result
        if control == "rollback":
            result = self._raw_execute(sql, *args, **kwargs)
            self._epoch_fence_in_transaction = False
            return result
        if not is_destination_mutation(sql):
            return self._raw_execute(sql, *args, **kwargs)
        return self._run_mutation(lambda: self._raw_execute(sql, *args, **kwargs))

    def executemany(self, sql, parameters, *args, **kwargs):
        return self._run_mutation(
            lambda: self._raw.executemany(sql, parameters, *args, **kwargs)
        )

    def append(self, *args, **kwargs):
        return self._run_mutation(lambda: self._raw.append(*args, **kwargs))

    def sql(self, sql, *args, **kwargs):
        if not is_destination_mutation(sql):
            return self._raw.sql(sql, *args, **kwargs)
        return self._run_mutation(lambda: self._raw.sql(sql, *args, **kwargs))

    def commit(self):
        result = self._raw.commit()
        self._epoch_fence_in_transaction = False
        return result

    def rollback(self):
        result = self._raw.rollback()
        self._epoch_fence_in_transaction = False
        return result

    def __getattr__(self, name):
        return getattr(self._raw, name)


class EpochFencedCursor(_FencedOperations):
    """A cursor whose independent transaction is also epoch fenced."""

    def __enter__(self):
        self._raw.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        return self._raw.__exit__(exc_type, exc, traceback)


class EpochFencedConnection(_FencedOperations):
    """The only service destination connection exposed to pipeline code."""

    def cursor(self, *args, **kwargs):
        return EpochFencedCursor(
            self._raw.cursor(*args, **kwargs),
            self._epoch_fence_lease,
            self._epoch_fence_context,
        )

    def __enter__(self):
        self._raw.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        return self._raw.__exit__(exc_type, exc, traceback)


__all__ = [
    "EpochFencedConnection",
    "EpochFencedCursor",
    "is_destination_mutation",
    "unwrap_destination_handle",
]
