"""Exception types shared across cdc_flight.

Kept in its own (import-cheap) module: `engine.py` imports pydbzengine, which
boots a JVM as a side effect of being imported, and the CLI's error handling must
not pay that cost just to name an exception.
"""

from __future__ import annotations


class EngineFailure(RuntimeError):
    """The Debezium engine terminated abnormally.

    Carries a partial run summary so the CLI can still write a machine-readable
    `last_run.json` for the failed run (rubric 6.1/6.2 depend on it).
    """

    def __init__(self, message: str, summary: dict | None = None):
        super().__init__(message)
        self.summary: dict = summary or {}


class OffsetFlushFailed(RuntimeError):
    """`markBatchFinished()` returned normally but did not flush the offset.

    Debezium swallows every non-timeout flush failure and discards the boolean
    (`AsyncEmbeddedEngine.java:894-932`, `:1369-1382`), so "the offset is now
    durable" is not something a normal return can be taken to mean. See
    `cdc_flight.consumer` and ADR 0001 §4.2.
    """


class SourceNotStreaming(RuntimeError):
    """The connector stopped streaming without the supervisor asking it to.

    Raised when a run would otherwise report a quiet stream as `idle` while the
    replication slot says the connector is not actually connected - Debezium's
    retriable-restart backoff looks exactly like an idle stream from the Python
    side (ADR 0001 §9.1; review finding Opus B5).
    """


# --------------------------------------------------------------------------- #
# transactional applier (ADR 0001 §3, §4)
# --------------------------------------------------------------------------- #
class TransactionAssemblyError(RuntimeError):
    """Debezium's transaction metadata is not self-consistent.

    ADR 0001 §3.2: a `txId` change without an intervening `END`, a `BEGIN` while
    another transaction is open, or an `END` whose `event_count` disagrees with
    what we buffered. Every one of these means a commit group could contain part
    of a Postgres transaction, so the applier refuses rather than guessing.
    """


class ResumePointDrift(RuntimeError):
    """`offsets.dat` does not agree with the resume point we just committed.

    ADR 0001 §4.3. Raised *after* the destination COMMIT, so the data is already
    durable; the process exits non-zero and start-up reconciliation (§4.5)
    repairs the file from `_cdc_flight.debezium_offsets`.
    """


class ReconciliationRefused(RuntimeError):
    """Start-up reconciliation cannot establish a safe resume point.

    ADR 0001 §4.5. The load-bearing case is *file present / table row missing*:
    the file may be arbitrarily ahead of anything durable in the destination, so
    trusting it is silent data loss.
    """


class SlotAheadOfDestination(RuntimeError):
    """`slot.confirmed_flush_lsn > debezium_offsets.last_lsn` (ADR 0001 §4.7).

    The Invariant-O guard. Under Invariant O this should be unfalsifiable; if it
    ever fires, WAL that the destination never committed has already been
    discarded by Postgres, and the only recovery is a re-snapshot (rubric 1.8).
    """


class LeaseLost(RuntimeError):
    """Another runner owns `_cdc_flight.lease` for this pipeline (rubric 4.2)."""
