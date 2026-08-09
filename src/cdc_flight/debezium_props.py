"""Build the Debezium embedded-engine configuration.

Starting point is the dlthub blog post (`dlthub.com/blog/debezium-and-dlt`), but
the blog's property set is written against a much older Debezium and uses several
properties that are removed in Debezium 3.x. Deviations are called out inline and
in `research/NOTES.md`.
"""

from __future__ import annotations

import os

from .config import ReplicationConfig, SourceConfig
from .errors import UnsafeDebeziumProperty
from .snapshot_completion import notification_topic
from .toast import UNAVAILABLE_VALUE_PLACEHOLDER

# Re-exported for configuration callers.  The value is owned by ``toast.py`` so
# the connector property and the decoder cannot drift apart.

# Prefix applied to the CDC metadata fields injected by ExtractNewRecordState.
METADATA_PREFIX = "dbz_"

# Flush on every `markBatchFinished()` rather than on a timer, so that a flush
# that did not happen is a detectable event (see `cdc_flight.consumer`).
#
# MEASURED: `offset.commit.policy=…$AlwaysCommitOffsetPolicy` does NOT work -
# Debezium instantiates the policy reflectively and requires a
# `<init>(java.util.Properties)` constructor, which that class does not have
# (`NoSuchMethodException`, reproduced 2026-07-30). The default
# `PeriodicCommitOffsetPolicy` with a **zero** interval is equivalent:
# `performCommit()` is `timeSinceLastCommit >= Duration.ZERO`, which is always
# true (`repos/debezium/debezium-api/.../spi/OffsetCommitPolicy.java:36-53`).
OFFSET_FLUSH_INTERVAL_MS_ALWAYS = "0"

#: ADR 0001 §4.10 / Opus B-2. Invariant O holds because
#: `PostgresConnectorTask.performCommit()` re-reads the offset *backing store*
#: rather than the task's in-memory offset context. `lsn.flush.mode` is the one
#: documented bypass: with `connector_and_driver`,
#: `PostgresReplicationConnection.java:1114-1123` sets `.withAutomaticFlush(true)`
#: and the shipped pgjdbc (`V3PGReplicationStream.processKeepAliveMessage`) then
#: advances the flushed LSN to the **server-supplied** `lastServerLSN` on
#: keepalives, never consulting the offset store - confirming WAL to Postgres
#: outside the invariant. Debezium's default is already `connector`, and that is
#: precisely the problem: "the default happens to be safe" is a conditional
#: argument, which is the shape of the withdrawn P2. So it is pinned.
LSN_FLUSH_MODE_SAFE = "connector"

#: Properties whose value the exactly-once argument depends on. An override that
#: changes any of them is refused rather than logged.
INVARIANT_O_PINS = {
    "lsn.flush.mode": LSN_FLUSH_MODE_SAFE,
    "provide.transaction.metadata": "true",
    "offset.flush.interval.ms": OFFSET_FLUSH_INTERVAL_MS_ALWAYS,
}

INVARIANT_O_REASONS = {
    "lsn.flush.mode": (
        "any value but 'connector' lets pgjdbc flush the LSN from server keepalives "
        "without consulting the offset store, which advances the slot past data the "
        "destination never committed (ADR 0001 §4.10, Opus B-2)"
    ),
    "provide.transaction.metadata": (
        "without the transaction END marker there is no way to prove a Postgres "
        "transaction whole, so no commit group can be formed (ADR 0001 §3.2)"
    ),
    "offset.flush.interval.ms": (
        "a non-zero interval makes `markBatchFinished()` a no-op most of the time, so "
        "'the offset did not flush' becomes unobservable (ADR 0001 §4.2)"
    ),
}


def internal_topic_prefixes(topic_prefix: str) -> tuple[str, ...]:
    """Topics that must never become destination tables.

    Debezium's own topics are `__debezium-heartbeat.<prefix>` and, with
    `provide.transaction.metadata=true`, `<prefix>.transaction`.
    """
    return ("__debezium", f"{topic_prefix}.transaction", f"{topic_prefix}.heartbeat")


def assert_no_internal_topic_collision(topic_prefix: str, tables: list[str]) -> None:
    """A captured table whose topic equals an internal topic would be silently lossy.

    With the pinned `DefaultTopicNamingStrategy` a Postgres table named
    `transaction` lands on `<prefix>.<schema>.transaction`, so it does not collide
    with `<prefix>.transaction` today. This is asserted rather than reasoned about,
    because `internal_topic_prefixes()` had become dead code after the old handler
    was deleted, which reads as protection that is not there (Opus MINOR-6).
    """
    internal = set(internal_topic_prefixes(topic_prefix))
    for table in tables:
        topic = f"{topic_prefix}.{table}"
        if topic in internal or table in internal:
            raise UnsafeDebeziumProperty(
                f"captured table {table!r} would publish to {topic!r}, which is one of "
                f"Debezium's internal topics ({sorted(internal)}). Its records would be "
                "decoded as transaction metadata or heartbeats and never applied "
                "(ADR 0001 §3.2)."
            )

# Metadata columns as they appear in the destination (after dlt normalisation).
METADATA_COLUMNS = (
    "dbz_op",
    "dbz_deleted",
    "dbz_table",
    "dbz_schema",
    "dbz_lsn",
    "dbz_tx_id",
    "dbz_source_ts_ms",
    "dbz_ts_ms",
)


#: Debezium's own default is `"t"`: **truncates are skipped unless you say
#: otherwise** (`CommonConnectorConfig.java:865-875`, "By default, only truncate
#: operations will be skipped"), and the pgoutput decoder then drops the 'T' message
#: before it is ever decoded (`PgOutputMessageDecoder.isTruncateEventsIncluded`).
#: That single default is the whole of rubric 1.5's truncate half, and it is why the
#: baseline's `skipped` counter did not even increment for a TRUNCATE.
SKIP_NOTHING = "none"
SKIP_TRUNCATE = "t"


def build_properties(
    source: SourceConfig,
    replication: ReplicationConfig,
    *,
    snapshot_mode: str | None = None,
    max_batch_size: int = 2048,
    poll_interval_ms: int = 500,
    overrides: dict[str, str] | None = None,
    truncate_mode: str = "replicate",
) -> dict[str, str]:
    """Return Debezium engine properties as a plain dict.

    `DebeziumJsonEngine` converts a dict into `java.util.Properties` for us, so we
    keep the Python side dict-shaped and unit-testable.

    `overrides` exists so the pins that Invariant O depends on have something to
    refuse: an operator (or a future config surface) that tries to change one of
    `INVARIANT_O_PINS` gets `UnsafeDebeziumProperty`, not a warning.
    """
    replication.state_dir.mkdir(parents=True, exist_ok=True)
    snapshot = snapshot_mode or replication.snapshot_mode
    for key, value in (overrides or {}).items():
        pinned = INVARIANT_O_PINS.get(key)
        if pinned is not None and str(value) != pinned:
            raise UnsafeDebeziumProperty(
                f"refusing to set {key}={value!r}: it is pinned to {pinned!r} because "
                f"{INVARIANT_O_REASONS[key]}"
            )

    props: dict[str, str] = {
        "name": "cdc-flight-engine",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        # --- source connection ------------------------------------------------
        "database.hostname": source.host,
        "database.port": str(source.port),
        "database.user": source.user,
        "database.password": source.password,
        "database.dbname": source.dbname,
        # --- logical decoding -------------------------------------------------
        # pgoutput is built into Postgres: no wal2json / decoderbufs extension.
        # (Rubric 7.1 wants exactly this.)
        "plugin.name": "pgoutput",
        "slot.name": replication.slot_name,
        "slot.drop.on.stop": "false",
        "publication.name": replication.publication_name,
        # The publication is created by sql/01_schema.sql and version controlled,
        # so Debezium must not invent its own.
        "publication.autocreate.mode": "disabled",
        "topic.prefix": replication.topic_prefix,
        "snapshot.mode": snapshot,
        # --- offsets ----------------------------------------------------------
        # File-backed offsets: the simplest Kafka-less store. This is a known
        # baseline weakness (offsets live outside the destination transaction,
        # which is what breaks exactly-once - see research/NOTES.md).
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": replication.offset_file.as_posix(),
        # ADR 0001 §4.10: every `markBatchFinished()` must actually attempt a
        # flush, otherwise "the offset advanced" is unobservable and
        # `cdc_flight.consumer.OffsetFlushVerifier` cannot tell a policy no-op
        # from Debezium swallowing a flush failure (Opus B2).
        "offset.flush.interval.ms": OFFSET_FLUSH_INTERVAL_MS_ALWAYS,
        # PINNED, not left to the default. See LSN_FLUSH_MODE_SAFE above: this is
        # the one path that can confirm WAL to Postgres without ever reading the
        # offset store, i.e. the one thing that could break Invariant O from
        # outside our code (Opus B-2).
        "lsn.flush.mode": LSN_FLUSH_MODE_SAFE,
        # ADR 0001 §4.10 / Opus m10: `stopSourceTasks()` waits only
        # `task.management.timeout.ms` before `taskService.shutdownNow()`
        # (`AsyncEngineConfig.java:25,76-80`), so a flush timeout larger than it
        # would be hard-killed mid-write during shutdown. Keep the pair aligned,
        # with task management the larger of the two.
        "offset.flush.timeout.ms": "5000",
        "task.management.timeout.ms": "30000",
        # --- batching / latency ----------------------------------------------
        "max.batch.size": str(max_batch_size),
        "max.queue.size": str(max_batch_size * 4),
        "poll.interval.ms": str(poll_interval_ms),
        # --- payload shape (ADR 0001 D5) --------------------------------------
        # `ExtractNewRecordState` is GONE. The applier consumes the full Debezium
        # envelope, because the SMT discards - before Python ever sees it - the
        # `before` image (1.2, 1.4, 2.6), the truncate/message operations (1.5,
        # 7.4) and, decisively, the `transaction` block that ADR 0001 §3.2 and §6
        # both depend on. Nothing downstream of here can recover those.
        # 2.4/2.5 consume the schema-bearing wrapper.  The envelope decoder accepts
        # the old schema-disabled shape for replay fixtures, but production records
        # must retain Connect logical names/parameters so a NULL-only or empty nested
        # value cannot be inferred as VARCHAR/JSON.
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        "decimal.handling.mode": "string",
        "time.precision.mode": "microseconds",
        "interval.handling.mode": "string",
        "binary.handling.mode": "base64",
        # Stock PostgreSQL 3.6 otherwise drops JDBC-1111 columns before the
        # envelope reaches Python.  Unknown values are opaque bytes at the
        # connector boundary; the strict catalog descriptor and the one native
        # resolver decide whether/how they can be stored.
        # The production default is TRUE.  A false value is retained as a safe
        # diagnostic mode: the completeness gate must refuse an omitted source
        # column rather than allowing Debezium's historical silent-drop behavior.
        "include.unknown.datatypes": os.environ.get(
            "CDC_INCLUDE_UNKNOWN_DATATYPES", "true"
        ).lower(),
        "hstore.handling.mode": "map",
        # Gate2 Option N: Debezium accepts the documented hex form and emits a
        # U+0000 marker.  PostgreSQL text-like domains cannot contain that code
        # point, so the Python decoder may recognize it only with a matching
        # source descriptor.
        "unavailable.value.placeholder": UNAVAILABLE_VALUE_PLACEHOLDER,
        "topic.naming.strategy": "io.debezium.schema.DefaultTopicNamingStrategy",
        # ADR 0001 §3.2: MANDATORY, not optional. Without it there is no `END`
        # marker and therefore no way to prove a Postgres transaction whole, so
        # the applier cannot form a commit group at all.
        "provide.transaction.metadata": "true",
        # Debezium's sink notification is enqueued in the same ChangeEventQueue as
        # snapshot rows. Its Initial Snapshot COMPLETED record is therefore the direct
        # post-callback barrier used by SnapshotCompletion; source slot streaming is
        # deliberately not completion evidence.
        "notification.enabled.channels": "sink",
        "notification.sink.topic.name": notification_topic(replication.topic_prefix),
        # A tombstone is a (key, null value) record; with the envelope there is
        # nothing to gain from one and a null payload must never be mistaken for
        # a data event.
        "tombstones.on.delete": "false",
        # p01 finding: with the default, a delete's `before` image is fabricated
        # zeros and empty strings rather than the real NULLs.
        "replace.null.with.default": "false",
        # Postgres DDL events would arrive on the bare `<prefix>` topic and are
        # rubric 2.x work; keep them off until there is code that handles them.
        # (For Postgres this is a no-op anyway: the connector has no DDL event
        # source, which is exactly why rubric 1.5's DROP detection has to poll the
        # catalog - see `cdc_flight.catalog`.)
        "include.schema.changes": "false",
        # Always retain pgoutput TRUNCATE records for the destination policy.  They
        # are not generation authority: the asynchronous catalog token and the
        # complete-image resnapshot own lifecycle convergence.  Applying the old
        # Debezium `skipped.operations=t` setting would lose the operation before
        # the planner sees it.
        "skipped.operations": SKIP_NOTHING,
        # --- resilience -------------------------------------------------------
        "errors.max.retries": "3",
        "errors.retry.delay.initial.ms": "300",
        "errors.retry.delay.max.ms": "10000",
        # NOTE (baseline gap): no `heartbeat.interval.ms` / `heartbeat.action.query`
        # yet. Rubric 4.4/4.5/4.6 require an idle-slot heartbeat; that is Phase 4
        # work and is deliberately absent from the baseline so the gap is visible.
    }
    # In discovery mode the publication is the capture contract. A static table or
    # schema include list would make a catalog watcher capable of observing a new
    # relation but Debezium incapable of delivering its rows. The explicit opt-out
    # retains the old bounded-capture behaviour for deployments that want it.
    if not source.auto_discovery:
        props["schema.include.list"] = source.schema
        props["table.include.list"] = ",".join(source.tables)
    return props
