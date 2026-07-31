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
        # --- payload shape ----------------------------------------------------
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "topic.naming.strategy": "io.debezium.schema.DefaultTopicNamingStrategy",
        # Flatten the Debezium envelope to the "after" image and carry the
        # operation metadata as `__`-prefixed fields.
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.add.fields": "op,table,schema,lsn,txId,source.ts_ms,ts_ms",
        # DEVIATION: the blog keeps Debezium's default `__` prefix. dlt's snake_case
        # naming convention strips leading underscores, which would land the
        # metadata as bare `op` / `table` / `schema` - names that can collide with
        # real source columns and are reserved words in SQL. An explicit `dbz_`
        # prefix survives normalisation untouched.
        "transforms.unwrap.add.fields.prefix": METADATA_PREFIX,
        # DEVIATION: the blog uses `delete.handling.mode`, removed in Debezium 3.x.
        # `rewrite` keeps deletes as rows carrying `__deleted=true`.
        "transforms.unwrap.delete.tombstone.handling.mode": "rewrite",
        # --- resilience -------------------------------------------------------
        "errors.max.retries": "3",
        "errors.retry.delay.initial.ms": "300",
        "errors.retry.delay.max.ms": "10000",
        # NOTE (baseline gap): no `heartbeat.interval.ms` / `heartbeat.action.query`
        # yet. Rubric 4.4/4.5/4.6 require an idle-slot heartbeat; that is Phase 4
        # work and is deliberately absent from the baseline so the gap is visible.
    }
    return props
