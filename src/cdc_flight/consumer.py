"""Our own Debezium `ChangeConsumer`, so the offset flush stops being invisible.

Why this exists (ADR 0001 §3.7, §4.2; review finding Opus B2)
-------------------------------------------------------------
`RecordCommitter.markBatchFinished()` **discards** the result of Debezium's
`commitOffsets(...)`:

```java
private static boolean commitOffsets(...) {
    if (!offsetWriter.beginFlush(...)) { return false; }          // nothing to flush
    try { final Future<Void> flush = offsetWriter.doFlush(...);
          if (flush == null) { offsetWriter.cancelFlush(); return false; }
          flush.get(...); task.commit(); }
    catch (InterruptedException e) { ...; throw e; }
    catch (Exception e) { LOGGER.warn("Flush of the offsets failed, ...");
                          offsetWriter.cancelFlush(); return false; }   // swallowed
    return true;
}

public void markBatchFinished() {
    if (offsetCommitPolicy.performCommit(...)) {
        try { if (commitOffsets(...)) { recordsSinceLastCommit = 0; ... } }  // boolean dropped
        catch (TimeoutException e) { throw new DebeziumException(...); }
    }
}
```
(`repos/debezium/debezium-embedded/.../async/AsyncEmbeddedEngine.java:894-932`,
`:1369-1382`.)

Only a `TimeoutException` is rethrown. Every other flush failure - disk full, an
I/O error, a null future - returns `false`, logs a WARN, and `markBatchFinished()`
returns **normally**. `pydbzengine.PythonChangeConsumer` then also discards it
(`repos/pydbzengine/pydbzengine/_jvm.py:109-130`).

The transactional applier will rely on "the acknowledgement actually happened",
so the failure has to be observable *now*, before the applier is written. This
consumer replaces pydbzengine's: it does everything the original did (call the
handler, mark every record, finish the batch, capture Python exceptions and
interrupt the engine thread) and additionally **verifies that `offsets.dat`
really changed** when a flush was expected.

Under ADR 0001's Invariant O the acknowledgement runs *after* the MotherDuck
`COMMIT`, so a failure here can never lose data - it can only make the on-disk
offset lag the durable one, which start-up reconciliation repairs (ADR §4.5).
It is still a fatal condition, because it is the canary for a broken offset
store, and because silently replaying is not free.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from pathlib import Path

from .errors import OffsetFlushFailed

log = logging.getLogger("cdc_flight.consumer")

_consumer_class = None


def _fingerprint(path: Path) -> tuple[int, int, str] | None:
    """(size, mtime_ns, sha256) of the offset file, or None if it does not exist."""
    try:
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None
    except OSError:  # pragma: no cover - unreadable file is itself a failure signal
        return None
    return (stat.st_size, stat.st_mtime_ns, digest)


class OffsetFlushVerifier:
    """Detects a `markBatchFinished()` that returned normally without flushing."""

    def __init__(self, offset_file: Path, *, always_commit: bool):
        self.offset_file = Path(offset_file)
        # Only meaningful with AlwaysCommitOffsetPolicy: with a periodic policy a
        # no-op markBatchFinished() is expected and correct.
        self.always_commit = always_commit
        self.flushes_verified = 0
        self.last_error: str | None = None

    def before(self) -> tuple[int, int, str] | None:
        return _fingerprint(self.offset_file)

    def after(self, before: tuple[int, int, str] | None, *, marked: int) -> None:
        """Raise unless the offset file demonstrably moved.

        "Moved" means any of (size, mtime_ns, sha256) changed: `FileOffsetBackingStore`
        rewrites the whole file on every successful flush, so an unchanged mtime
        means the write never happened. Comparing the digest alone would be wrong
        for a flush whose serialised bytes happen to be identical.
        """
        if marked == 0 or not self.always_commit:
            return
        now = _fingerprint(self.offset_file)
        if now is None:
            self.last_error = f"offset file {self.offset_file} does not exist after a flush"
        elif before is not None and now == before:
            self.last_error = (
                f"markBatchFinished() returned normally but {self.offset_file} did not "
                f"change after {marked} markProcessed() calls - Debezium's "
                "commitOffsets() returns false and swallows the failure "
                "(AsyncEmbeddedEngine.java:894-932, :1369-1382)"
            )
        else:
            self.flushes_verified += 1
            return
        raise OffsetFlushFailed(self.last_error)


def verifying_consumer_class():
    """Build the JPype proxy class lazily - it needs a running JVM."""
    global _consumer_class
    if _consumer_class is not None:
        return _consumer_class

    # Importing pydbzengine._jvm is what *starts* the JVM. `@jpype.JImplements`
    # needs it running, and `DebeziumJsonEngine.run()` touches `self.consumer`
    # before `self.engine`, so we cannot rely on the engine property to have
    # booted it for us.
    import jpype
    import pydbzengine._jvm  # noqa: F401

    @jpype.JImplements("io/debezium/engine/DebeziumEngine$ChangeConsumer")
    class _VerifyingChangeConsumer:
        """Drop-in replacement for `pydbzengine.PythonChangeConsumer`."""

        def __init__(self):
            self.handler = None
            self.verifier: OffsetFlushVerifier | None = None
            self._exception: BaseException | None = None
            self._lock = threading.Lock()
            self.batches_acked = 0
            self.records_acked = 0

        # -- pydbzengine callback surface ----------------------------------- #
        def set_change_handler(self, handler) -> None:
            self.handler = handler

        @jpype.JOverride
        def supportsTombstoneEvents(self):
            return True

        def interrupt(self):
            from pydbzengine._jvm import JavaLangThread

            JavaLangThread.currentThread().interrupt()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.interrupt()

        # -- the callback --------------------------------------------------- #
        @jpype.JOverride
        def handleBatch(self, records, committer):
            from pydbzengine._jvm import JavaLangThread

            try:
                # ADR 0001 D3 / Invariant O: the handler owns the
                # acknowledgement, because `markProcessed()` may only run
                # AFTER the destination COMMIT, and a commit group can span
                # several Debezium batches. `SourceRecordCommitter` is created
                # once per poll loop (`AsyncEmbeddedEngine.java:1300`), so the
                # committer handed to us is the same object every batch and is
                # safe to hold across batches on this (single) poll thread.
                self.handler.handle_batch(records, self._wrap(committer))
            except BaseException as exc:
                log.error("failed to consume events in python", exc_info=True)
                self._exception = exc
                # How pydbzengine propagates a handler error to the engine thread.
                JavaLangThread.currentThread().interrupt()

        def _wrap(self, committer):
            """Count acknowledgements so the run summary can report them."""
            consumer = self

            class _Counting:
                def markProcessed(self, record):
                    committer.markProcessed(record)
                    with consumer._lock:
                        consumer.records_acked += 1

                def markBatchFinished(self):
                    committer.markBatchFinished()
                    with consumer._lock:
                        consumer.batches_acked += 1

            return _Counting()

    _consumer_class = _VerifyingChangeConsumer
    return _consumer_class
