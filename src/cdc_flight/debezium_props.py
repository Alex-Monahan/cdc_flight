"""Build the Debezium embedded-engine configuration.

Starting point is the dlthub blog post (`dlthub.com/blog/debezium-and-dlt`), but
the blog's property set is written against a much older Debezium and uses several
properties that are removed in Debezium 3.x. Deviations are called out inline and
in `research/NOTES.md`.
"""

from __future__ import annotations

from .config import ReplicationConfig, SourceConfig

# Debezium's marker for a TOASTed column whose value was not present in the WAL.
UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value"

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


def internal_topic_prefixes(topic_prefix: str) -> tuple[str, ...]:
    """Topics that must never become destination tables.

    Debezium's own topics are `__debezium-heartbeat.<prefix>` and, once
    `provide.transaction.metadata=true` lands, `<prefix>.transaction`. The
    previous literal `("__debezium", "__cdcflight")` could never match the second
    one - `topic.prefix` is `cdcflight`, so the transaction topic is
    `cdcflight.transaction`, and it would have been materialised as a data table
    (Opus m1). Derive it from the configured prefix instead.
    """
    return ("__debezium", f"{topic_prefix}.transaction", f"{topic_prefix}.heartbeat")

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


def build_properties(
    source: SourceConfig,
    replication: ReplicationConfig,
    *,
    snapshot_mode: str | None = None,
    max_batch_size: int = 2048,
    poll_interval_ms: int = 500,
) -> dict[str, str]:
    """Return Debezium engine properties as a plain dict.

    `DebeziumJsonEngine` converts a dict into `java.util.Properties` for us, so we
    keep the Python side dict-shaped and unit-testable.
    """
    replication.state_dir.mkdir(parents=True, exist_ok=True)
    snapshot = snapshot_mode or replication.snapshot_mode

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
        "schema.include.list": source.schema,
        "table.include.list": ",".join(source.tables),
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
        "key.converter.schemas.enable": "false",
        # DEVIATION from ADR 0001 §5, recorded in the ADR amendment: the Connect
        # *schema* stays off for now. The applier needs the *envelope*; the schema
        # is what rubric 2.4/2.6 need, and turning it on inflates every payload
        # 3-5x, which §5.1 flags as an unmeasured throughput risk owned by 5.3.
        "value.converter.schemas.enable": "false",
        "topic.naming.strategy": "io.debezium.schema.DefaultTopicNamingStrategy",
        # ADR 0001 §3.2: MANDATORY, not optional. Without it there is no `END`
        # marker and therefore no way to prove a Postgres transaction whole, so
        # the applier cannot form a commit group at all.
        "provide.transaction.metadata": "true",
        # A tombstone is a (key, null value) record; with the envelope there is
        # nothing to gain from one and a null payload must never be mistaken for
        # a data event.
        "tombstones.on.delete": "false",
        # p01 finding: with the default, a delete's `before` image is fabricated
        # zeros and empty strings rather than the real NULLs.
        "replace.null.with.default": "false",
        # Postgres DDL events would arrive on the bare `<prefix>` topic and are
        # rubric 2.x work; keep them off until there is code that handles them.
        "include.schema.changes": "false",
        # --- resilience -------------------------------------------------------
        "errors.max.retries": "3",
        "errors.retry.delay.initial.ms": "300",
        "errors.retry.delay.max.ms": "10000",
        # NOTE (baseline gap): no `heartbeat.interval.ms` / `heartbeat.action.query`
        # yet. Rubric 4.4/4.5/4.6 require an idle-slot heartbeat; that is Phase 4
        # work and is deliberately absent from the baseline so the gap is visible.
    }
    return props
