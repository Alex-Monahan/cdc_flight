"""Exception types shared across cdc_flight.

Kept in its own (import-cheap) module: `engine.py` imports pydbzengine, which
boots a JVM as a side effect of being imported, and the CLI's error handling must
not pay that cost just to name an exception.
"""

from __future__ import annotations

from .occurrence import LeaseState, OffsetRowState

# A refusal's durable class is deliberately not an origin label.  Origins are
# useful diagnostics, but letting each raise site choose the durable class made
# the same source condition alternate between ``pending`` and ``quarantined``.
# This declaration is the one authority shared by the exception boundary and the
# structural test: adding a production refusal module requires adding its origin
# here and declaring that origin at every raise site.
REFUSAL_ORIGIN_BY_MODULE = {
    "catalog": "catalog_state",
    "catalog_descriptors": "catalog_descriptor",
    "catalog_poll": "catalog_poll",
    "catalog_state": "catalog_state",
    "catalog_support": "catalog_shape",
    "planner": "typed_planner",
    "schema_backfill": "schema_backfill",
    "schema_ddl": "schema_ddl",
    "schema_epoch": "schema_epoch",
    "schema_evolution": "schema_evolution",
    "schema_registry": "schema_registry",
    "schema_shadow": "schema_shadow",
    "spill_protocol": "spill_protocol",
    "table_work": "table_work",
    "table_writer": "table_writer",
}
REFUSAL_ORIGINS = frozenset(REFUSAL_ORIGIN_BY_MODULE.values())
CANONICAL_REFUSAL_CLASS = "SchemaEvolutionRefused"


class EngineFailure(RuntimeError):
    """The Debezium engine terminated abnormally.

    Carries a partial run summary so the CLI can still write a machine-readable
    `last_run.json` for the failed run (rubric 6.1/6.2 depend on it).
    """

    def __init__(self, message: str, summary: dict | None = None):
        super().__init__(message)
        self.summary: dict = summary or {}


class AlertPersistenceFailure(RuntimeError):
    """The run's failure signal could not be written to its durable alert surface.

    A failed alert write is not safe to downgrade to a log-only continuation. The
    original failure is retained as an attribute, while this exception makes the
    CLI's non-zero outcome explicitly say that alerting itself is broken.
    """

    def __init__(
        self,
        *,
        code: str,
        original_failure: BaseException,
        alert_failure: BaseException,
        summary: dict | None = None,
    ):
        self.code = code
        self.original_failure = original_failure
        self.alert_failure = alert_failure
        self.summary = dict(summary or {})
        self.summary.update(
            {
                "alerting_broken": True,
                "alerting_code": code,
                "alerting_error": f"{type(alert_failure).__name__}: {alert_failure}",
                "original_failure": (
                    f"{type(original_failure).__name__}: {original_failure}"
                ),
            }
        )
        super().__init__(
            f"ALERTING BROKEN: could not persist {code} while reporting the original "
            f"failure {type(original_failure).__name__}: {original_failure}; "
            f"alert write failed with {type(alert_failure).__name__}: {alert_failure}"
        )


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


class UnsafeDebeziumProperty(RuntimeError):
    """A Debezium property that Invariant O depends on was given an unsafe value.

    ADR 0001 §4.10. `lsn.flush.mode=connector_and_driver` makes pgjdbc advance the
    flushed LSN from server keepalives without ever consulting the offset store,
    which confirms WAL to Postgres outside the invariant (Opus B-2); a false
    `provide.transaction.metadata` removes the END marker the boundary rule needs;
    a non-zero `offset.flush.interval.ms` makes a missing flush unobservable.
    Also raised when a captured table's topic would collide with one of Debezium's
    own internal topics.
    """


class AdmissionError(ValueError):
    """A source value or schema admission cannot cross a safe boundary.

    Typed-value diagnostics and schema-evolution refusals deliberately remain
    different concrete errors, but they share this root.  Every containment
    boundary catches the root so a new admission sibling cannot bypass the
    refusal architecture merely because the boundary predates that sibling.
    """


# --------------------------------------------------------------------------- #
# transactional applier (ADR 0001 §3, §4)
# --------------------------------------------------------------------------- #
class EnvelopeDecodeError(RuntimeError):
    """A Debezium payload cannot be decoded into a record we are willing to act on.

    ADR 0001 §3.2. The load-bearing case is the transaction topic: a payload whose
    `status` is neither `BEGIN` nor `END` used to become an `END` with no
    `event_count`, which terminated the open transaction with no completeness
    check at all (Opus M-1). Failing loud is the only safe reading of "unknown
    control message".
    """


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


class OffsetUnusable(RuntimeError):
    """The durable resume row is present but cannot be parsed safely.

    A malformed resume point is not the same as a repairable offsets.dat scratch
    file: the destination row is the source of truth, and there is no trustworthy
    position from which Debezium may start.  The caller must alert and exit before
    the engine is constructed.
    """

    def __init__(self, message: str, *, offset_row: OffsetRowState | None = None):
        super().__init__(message)
        if offset_row is not None and type(offset_row) is not OffsetRowState:
            raise TypeError("offset_row must be an OffsetRowState")
        # The alert condition may be an exception fingerprint, but its occurrence
        # comes from the durable offset row that could not be parsed.  The exception
        # never accepts an occurrence string, so a failure message cannot become one.
        self.offset_row = offset_row


class AmbiguousDelete(RuntimeError):
    """The fold cannot say which physical row a delete removed (ADR 0001 §18/A35).

    Rubric 1.4's hard case. Inside one Postgres transaction a deferred unique
    constraint lets several rows wear one key, and only the delete's before-image
    says which of them the event describes. A deferrable key is not a valid replica
    identity, so that image is always a full row where the shape is reachable - but
    if it ever is not, the honest outcome is a refused commit group, not a guess:
    the rubric's own scale puts an error (=1) above silent loss, the group rolls
    back, and the transaction replays for free under Invariant O.

    **And then it replays into the same ambiguity, for ever** — which is a permanent
    manual-intervention case, and rubric 4.7 scores those. So the exception carries the
    table it could not fold, and the applier turns it into a durable re-snapshot request
    for exactly that table (ADR 0001 §19/A47). The re-snapshot's consistent point is
    necessarily *after* the offending transaction — we had already received it, so it is
    already in WAL — so the per-table watermark fences the transaction that cannot be
    folded, and the loop terminates after exactly one re-snapshot.
    """

    def __init__(
        self,
        message: str,
        *,
        source_schema: str | None = None,
        source_table: str | None = None,
        target: str | None = None,
    ):
        super().__init__(message)
        self.source_schema = source_schema
        self.source_table = source_table
        self.target = target


class ToastBaseMissing(AmbiguousDelete):
    """A sparse TOAST patch has no verified physical row to update.

    This is deliberately an ``AmbiguousDelete`` subclass so the existing automatic
    recovery path queues the table-scoped refetch/resnapshot and never turns a
    missing sparse base into a manual repair or a fabricated NULL/value.
    """


class DestinationIdentityCollision(RuntimeError):
    """Two destination rows share one identity (ADR 0001 §15/A21, Opus M-2).

    Raised inside the commit group's transaction, so the group rolls back and the
    events replay. Only reachable on a destination that cannot express the
    `PRIMARY KEY` the identity columns normally carry; where it can, the destination
    itself rejects the INSERT.
    """

    def __init__(
        self,
        message: str,
        *,
        source_schema: str | None = None,
        source_table: str | None = None,
        target: str | None = None,
    ):
        super().__init__(message)
        #: rubric 4.7: set by `table_writer.write` so the applier can queue the rebuild
        #: that turns this from a permanent failure into a self-healing one.
        self.source_schema = source_schema
        self.source_table = source_table
        self.target = target


class DestinationExecutionFailure(RuntimeError):
    """A destination SQL operation invalidated the current table transaction.

    This is intentionally separate from ``DestinationFault`` (the test injector's
    protocol failure) and from engine-level commit/connection failures.  The planner
    raises it only for a concrete destination execution error while writing one table;
    the commit owner can then roll back, durably quarantine that table independently,
    and replay the same source transaction with healthy tables still eligible.
    """

    def __init__(self, refused: SchemaEvolutionRefused, original: Exception, target: str):
        super().__init__(str(original))
        self.refused = refused
        self.original = original
        self.target = target


class TableWriteFailure(RuntimeError):
    """A table materializer failed after the fold reached the destination.

    DuckDB does not expose the savepoint syntax this applier would need to roll back
    one table while retaining the source transaction's healthy peers.  The commit
    owner therefore rolls the whole group back, records this table's refusal through
    the independent sink, and replays the source transaction with that table held out.
    ``table_writer`` creates this only for an exception raised inside the data
    materializer. Control-plane helpers are outside that boundary and propagate
    unchanged.
    """

    def __init__(self, refused: SchemaEvolutionRefused | None, original: Exception, target: str):
        super().__init__(str(original))
        self.refused = refused
        self.original = original
        self.target = target


class SchemaEvolutionRefused(AdmissionError):
    """A catalog schema transition cannot be applied without guessing.

    This is intentionally distinct from row-shape inference.  A fenced catalog
    baseline is authoritative, so an unsupported or failed destination ALTER must
    abort the whole group and leave the source relation pending rather than persisting
    a post-DDL baseline that the destination does not actually have.
    """

    def __init__(
        self,
        message: str,
        *,
        source_schema: str | None = None,
        source_table: str | None = None,
        target: str | None = None,
        detected_lsn: int | None = None,
        input_fingerprint: str | None = None,
        source_fingerprint: str | None = None,
        refusal_origin: str,
        source_tables: tuple[tuple[str, str, str | None], ...] = (),
    ):
        if refusal_origin not in REFUSAL_ORIGINS:
            raise ValueError(
                f"unregistered schema refusal origin {refusal_origin!r}; "
                "add it to REFUSAL_ORIGIN_BY_MODULE before raising"
            )
        super().__init__(message)
        self.source_schema = source_schema
        self.source_table = source_table
        self.target = target
        self.detected_lsn = detected_lsn
        #: Stable source-row evidence used to distinguish a deterministic retry from
        #: a genuinely new refusal.  It deliberately excludes operation/transaction
        #: metadata so a snapshot ``r`` can be compared with the original ``c``.
        self.input_fingerprint = input_fingerprint
        #: A catalog-authority refusal can cover more than the first relation in a
        #: batched source query.  Every affected relation must receive the same
        #: scoped quarantine; choosing rows[0] would leave its siblings uncontained.
        self.source_tables = tuple(source_tables)
        #: Stable source-schema evidence used to decide whether a quarantined table's
        #: blocking condition has changed.  It is separate from the retry identity:
        #: row-image changes must not create an unbounded retry loop.
        self.source_fingerprint = source_fingerprint
        #: Diagnostics only.  The durable class below is centrally fixed and cannot
        #: be supplied by a call site.
        self.refusal_origin = refusal_origin
        self.refusal_class = CANONICAL_REFUSAL_CLASS
        #: Spill handling can durably record before the outer commit owner sees the
        #: same exception.  The flag prevents one failure from counting twice.
        self.refusal_recorded = False


class SchemaBackfillRefused(SchemaEvolutionRefused):
    """A schema ADD has no safe value/identity mapping for existing rows."""


class SchemaShapeUnexplained(SchemaEvolutionRefused):
    """A row contains a column shape absent from every observed schema epoch."""


def as_schema_refusal(
    error: AdmissionError,
    *,
    refusal_origin: str,
    source_schema: str | None = None,
    source_table: str | None = None,
    target: str | None = None,
    detected_lsn: int | None = None,
) -> SchemaEvolutionRefused:
    """Normalize a common-base error at a containment boundary.

    The normal typed/schema paths preserve their richer
    ``SchemaEvolutionRefused`` instance.  This fallback is for a future sibling
    raised below a boundary that has not yet learned its richer context: it still
    becomes the durable refusal class, rolls back, and cannot escape as an
    uncategorized ``ValueError``.
    """
    if isinstance(error, SchemaEvolutionRefused):
        return error
    return SchemaEvolutionRefused(
        str(error),
        source_schema=source_schema,
        source_table=source_table,
        target=target,
        detected_lsn=detected_lsn,
        refusal_origin=refusal_origin,
    )


class NoDurableDestinationRow(RuntimeError):
    """The replication slot exists and has advanced, but nothing is durable here.

    ADR 0001 §4.5's "absent/absent but slot exists" cell (Codex 3). The slot's
    `confirmed_flush_lsn` may be arbitrarily far ahead of the empty destination, so
    a start-up mode that streams from it without re-reading the tables silently
    skips all the history in between. Refused unless the configured
    `snapshot.mode` is one that re-reads every captured table in full.
    """


class SlotAheadOfDestination(RuntimeError):
    """`slot.confirmed_flush_lsn > debezium_offsets.last_lsn` (ADR 0001 §4.7).

    The Invariant-O guard. Under Invariant O this should be unfalsifiable; if it
    ever fires, WAL that the destination never committed has already been
    discarded by Postgres, and the only recovery is a re-snapshot (rubric 1.8).
    """


class RecoveryFailed(RuntimeError):
    """A step of the durable acquisition recovery could not be completed.

    Rubric 1.8 / ADR 0001 §19/A53. The recovery is a journalled state machine, and a
    step that cannot be completed must **stop the run** with the journal intact rather
    than continue into a state the design calls unsafe. The load-bearing case is a
    replication slot that will not drop: A45 shows that Debezium only pairs the
    snapshot with an exact WAL position when it creates the slot itself, so a
    re-snapshot started against a surviving slot has an uncoordinated image/stream
    boundary — precisely the loss window rubric 1.8 exists to close. It used to be
    logged as `drop_failed: ...` and stepped over (Codex B4).
    """


class LeaseLost(RuntimeError):
    """Another runner owns `_cdc_flight.lease` for this pipeline (rubric 4.2)."""

    def __init__(self, message: str, *, lease_state: LeaseState | None = None):
        super().__init__(message)
        if lease_state is not None and type(lease_state) is not LeaseState:
            raise TypeError("lease_state must be a LeaseState")
        # Expiry timestamps and runner wording change on every retry.  The alert
        # occurrence comes from the durable ownership identity instead, so repeated
        # contenders for one live lease remain one operator incident.
        self.lease_state = lease_state
