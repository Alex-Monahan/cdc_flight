"""Decode one Debezium `ChangeEvent` into a `PendingRecord` (ADR 0001 D5, §3.2).

The baseline flattened every event with `ExtractNewRecordState` before it ever
reached Python. ADR 0001 D5 drops that transform, so what arrives here is the
**full Debezium envelope**:

```json
{"before": null,
 "after":  {"id": 1, "name": "Ada", ...},
 "source": {"schema": "app", "table": "customers", "lsn": 26289304,
            "txId": 771, "ts_ms": 1..., "snapshot": "false", ...},
 "transaction": {"id": "771", "total_order": 3, "data_collection_order": 3},
 "op": "c", "ts_ms": 1...}
```

plus two *control* topics that the baseline deliberately threw away and the
applier depends on:

* `<prefix>.transaction` — `{"status": "BEGIN"|"END", "id": ..., "event_count":
  ..., "data_collections": [...]}`. This is the only authoritative statement of
  where a Postgres transaction ends (ADR 0001 §3.2).
* `__debezium-heartbeat.<prefix>` — offset-bearing, data-free.

Deviation from ADR 0001 §5, recorded in the ADR's amendment section: the
envelope is consumed with `value.converter.schemas.enable=false`. The
*envelope* (before/after/source/transaction/op) is what rubric 1.1/1.2/1.3
need; the *Connect schema* is what rubric 2.4/2.6 need, and it lands with them,
after §5.1's decode-throughput measurement.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .errors import EnvelopeDecodeError
from .typed_types import SourceTypeDescriptor, TypedImage

#: `source.snapshot` values that mean "this record came out of a snapshot".
#: Debezium serialises `SnapshotRecord` lowercased.
SNAPSHOT_VALUES = frozenset(
    {
        "true",
        "first",
        "first_in_data_collection",
        "last_in_data_collection",
        "last",
        "incremental",
    }
)
#: `source.snapshot` values that close a table's snapshot.
SNAPSHOT_TABLE_LAST = frozenset({"last_in_data_collection", "last"})
#: `source.snapshot` value that closes the whole snapshot.
SNAPSHOT_LAST = "last"
#: `source.snapshot` value of an *incremental* snapshot chunk. Refused until
#: rubric 3.3 owns it: those records interleave with streaming events, never carry
#: a `last` marker and carry no `txId`/`lsn` (Opus M-7).
SNAPSHOT_INCREMENTAL = "incremental"

#: Debezium operation codes the applier treats specially.
OP_DELETE = "d"
OP_UPDATE = "u"
OP_TRUNCATE = "t"
OP_MESSAGE = "m"

KIND_DATA = "data"
KIND_SNAPSHOT = "snapshot"
#: `op="t"`. A truncate is a *data* event for every purpose Debezium counts
#: (`EventDispatcher` sends it through the same `changeRecord` path, so
#: `TransactionMonitor.dataEvent` counts it in `END.event_count`, it occupies a
#: `transaction.total_order` ordinal and it gets a `data_collections` entry), and it
#: is a *table* event for every purpose we count: it carries no key and no row, and
#: it empties the destination table (rubric 1.5). Its own kind, because giving it
#: `KIND_DATA` made `work_for` read its absent key as "this table is keyless".
KIND_TRUNCATE = "truncate"
KIND_TXN_BEGIN = "txn_begin"
KIND_TXN_END = "txn_end"
KIND_HEARTBEAT = "heartbeat"
KIND_MESSAGE = "logical_message"
KIND_SCHEMA_CHANGE = "schema_change"
KIND_UNKNOWN = "unknown"
# A validated Initial Snapshot COMPLETED boundary. It carries the source offset
# into the destination resume point but deliberately has no Debezium raw handle:
# the notification is acknowledged only after the completion machine reaches its
# terminal state.
KIND_SNAPSHOT_BOUNDARY = "snapshot_boundary"


@dataclass
class PendingRecord:
    """One decoded record, optionally retaining the Java object to acknowledge it.

    Snapshot-boundary records are synthetic: their decoded Connect offset is real,
    but ``raw`` is intentionally ``None`` because the corresponding notification is
    acknowledged only after the closed completion proof succeeds.
    """

    raw: Any
    kind: str
    topic: str
    nbytes: int
    op: str | None = None
    #: Logical-decoding message prefix. Flight-owned source markers use the
    #: configured ``<marker-prefix>_<reason>`` namespace; retaining it lets the
    #: applier report exactly which marker records crossed callback admission.
    message_prefix: str | None = None
    schema: str | None = None
    table: str | None = None
    lsn: int | None = None
    txn_id: str | None = None
    total_order: int | None = None
    #: Source lineage facts are optional on the stock envelope.  The applier fills
    #: the cluster/timeline from the durable slot observation and the relation
    #: generation from the catalog when the converter does not carry them.
    source_cluster_id: str | None = None
    source_timeline: int | None = None
    relation_generation: str | None = None
    #: PostgreSQL transaction commit LSN, distinct from the event's own WAL LSN.
    commit_lsn: int | None = None
    policy_epoch: int = 0
    #: Opaque post-decode policy identity. The digest is safe to persist; the
    #: source mapping is deliberately not retained here.
    policy_digest: str | None = None
    sanitized: bool = False
    policy_alerts: list[dict[str, Any]] = field(default_factory=list)
    #: The mode is attached at admission so a source transaction cannot mix a
    #: configuration epoch while it is open.
    delete_mode: str | None = None
    delete_policy_epoch: int = 1
    delete_policy_digest: str | None = None
    #: Proven PostgreSQL OUTPUT-function text supplied by a source adapter/read.
    #: It is consumed by the policy gate and never written to diagnostics.
    output_texts: dict[str, Any] = field(default_factory=dict)
    data_collection_order: int | None = None
    source_ts_ms: int | None = None
    snapshot: str | None = None
    key: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    txn_status: str | None = None
    txn_event_count: int | None = None
    txn_data_collections: dict[str, int] = field(default_factory=dict)
    source_partition: dict[str, Any] | None = None
    source_offset: dict[str, Any] | None = None
    #: 1-based arrival ordinal within the current snapshot of this table, assigned
    #: by `TransactionAssembler`. A snapshot record has no transaction and
    #: therefore no `total_order`, so this is what gives it a stable identity
    #: (`snap:<epoch>:<schema>.<table>:<ordinal>`) in **both** storage modes. It is
    #: assigned where the records arrive, so the ordinal is arrival order whether
    #: the record is later spilled or kept in memory (Codex 1).
    snapshot_ordinal: int | None = None
    #: Schema-enabled Connect JSON facts.  The legacy `key/before/after` mappings are
    #: retained for the transaction assembler and for the unchanged-TOAST path; these
    #: descriptors are the authoritative type input for 2.4/2.5 encoding.
    value_schema: dict[str, Any] | None = None
    key_schema: dict[str, Any] | None = None
    before_schema: dict[str, Any] | None = None
    after_schema: dict[str, Any] | None = None
    key_descriptors: dict[str, SourceTypeDescriptor] = field(default_factory=dict)
    before_descriptors: dict[str, SourceTypeDescriptor] = field(default_factory=dict)
    after_descriptors: dict[str, SourceTypeDescriptor] = field(default_factory=dict)
    typed_key: TypedImage | None = None
    typed_before: TypedImage | None = None
    typed_after: TypedImage | None = None
    #: A value-free admission decision made while sealing this record. The
    #: assembler carries the decision to the complete-unit boundary even if the
    #: row image itself is moved to spill storage; it is never serialized as a
    #: source image.
    admission_refusal: Any | None = None
    #: Stock incremental READ metadata.  These fields are deliberately separate
    #: from snapshot_ordinal: an arrival ordinal is not a resumable cursor.
    incremental: bool = False
    incremental_signal_id: str | None = None
    snapshot_identity: str | None = None
    incremental_chunk_id: str | None = None
    #: True when the applier identifies this data record as the Flight's own
    #: control-plane relation.  It remains a normal data event for transaction
    #: assembly and offset proof; this separate fact only prevents it from
    #: refreshing service delivery liveness.
    ignored_source_record: bool = False
    #: Stock's TABLE_SCAN_COMPLETED notification may overtake READ records on the
    #: embedded-engine callback stream.  Preserve its declared row total so the
    #: destination can defer the atomic swap until the shadow has received them.
    incremental_rows: int | None = None

    def __repr__(self) -> str:
        """Return an audit-safe description of the record.

        ``PendingRecord`` is present in assembler and spill exceptions often enough
        that the dataclass-generated representation would be a source-value leak:
        it includes ``key``, both row images, typed fields, and the raw connector
        object.  The acknowledgement handle is intentionally not represented either
        (its delegate is the connector's private callback token).  Operational
        diagnostics can use the identity and policy facts below without ever
        serialising a source value.
        """
        return (
            "PendingRecord("
            f"kind={self.kind!r}, topic={self.topic!r}, op={self.op!r}, "
            f"schema={self.schema!r}, table={self.table!r}, lsn={self.lsn!r}, "
            f"txn_id={self.txn_id!r}, total_order={self.total_order!r}, "
            f"sanitized={self.sanitized!r}, policy_epoch={self.policy_epoch!r}, "
            f"policy_digest={self.policy_digest!r}, delete_mode={self.delete_mode!r})"
        )

    @property
    def is_data(self) -> bool:
        return self.kind in (KIND_DATA, KIND_SNAPSHOT, KIND_TRUNCATE)

    @property
    def is_delivery_data(self) -> bool:
        """Whether this data event is evidence of this Flight's delivery.

        The signal relation is deliberately excluded only from this liveness view.
        It remains ``is_data`` and therefore remains counted by the transaction
        assembler's Debezium END/event_count reconciliation.
        """
        return self.is_data and not self.ignored_source_record

    @property
    def qualified_table(self) -> str | None:
        if self.schema and self.table:
            return f"{self.schema}.{self.table}"
        return self.table


def _java_map_to_dict(m: Any) -> dict[str, Any] | None:
    """Convert a `java.util.Map` (source partition / offset) into plain Python.

    Values are Long / Integer / String / Boolean; JPype hands them over as the
    matching Python types already, but they are still Java objects, so they are
    coerced explicitly - a `java.lang.Long` survives `json.dumps` only by
    accident.
    """
    if m is None:
        return None
    out: dict[str, Any] = {}
    try:
        for entry in m.entrySet():
            key = str(entry.getKey())
            value = entry.getValue()
            out[key] = _coerce(value)
    except Exception:  # pragma: no cover - defensive around the JVM bridge
        return None
    return out


#: Java classes we round-trip through JSON into Debezium's offset store.
_JAVA_LONGS = frozenset({"java.lang.Long", "java.lang.Integer", "java.lang.Short"})
_JAVA_FLOATS = frozenset({"java.lang.Double", "java.lang.Float"})


def _coerce(value: Any) -> Any:
    """Convert one Connect offset value, PRESERVING its Java type.

    This is not cosmetic. The offset map is re-serialised into `offsets.dat` by
    start-up reconciliation, and Debezium casts several fields by type:
    `transaction_id` and `messageType` are `String`, `lsn`/`txId`/`ts_usec` are
    `Long`, `snapshot_completed` is `Boolean`. Guessing "looks like a number ⇒
    int" turns `transaction_id` into a Long and the connector dies on start-up
    with `ClassCastException: java.lang.Long cannot be cast to java.lang.String`
    (measured, 2026-07-30).
    """
    if value is None:
        return None
    try:
        java_class = str(value.getClass().getName())
    except (AttributeError, TypeError):
        java_class = None
    if java_class in _JAVA_LONGS:
        return int(value)
    if java_class in _JAVA_FLOATS:
        return float(value)
    if java_class == "java.lang.Boolean":
        return bool(value)
    if java_class == "java.lang.String":
        return str(value)
    # Plain Python values (unit tests) or an unfamiliar Java type: keep bools and
    # numbers as they are, everything else as text.
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    return str(value)


def offsets_of(raw: Any) -> tuple[dict | None, dict | None]:
    """`(sourcePartition, sourceOffset)` of a Debezium `ChangeEvent`.

    `EmbeddedEngineChangeEvent.sourceRecord()` is public and is retained by the
    JSON-converting engine path
    (`debezium-embedded/.../ConverterBuilder.java:133`), so the Connect offset
    maps are reachable without a fork. They are what ADR 0001 §4.3 persists as
    the resume point.
    """
    try:
        record = raw.sourceRecord()
    except Exception:  # pragma: no cover - a non-embedded event shape
        return None, None
    try:
        return _java_map_to_dict(record.sourcePartition()), _java_map_to_dict(
            record.sourceOffset()
        )
    except Exception:  # pragma: no cover
        return None, None


def decode(raw: Any, *, topic_prefix: str, want_offsets: bool = False) -> PendingRecord:
    """Decode one `ChangeEvent`.

    **Raises `EnvelopeDecodeError` rather than guessing.** The earlier docstring
    said this function "never raises for an unexpected payload shape", which was
    both untrue (`json.loads` raises on malformed JSON) and the wrong goal: the
    one place it did fail open — any non-`BEGIN` payload on the transaction topic
    became an `END` with no `event_count` — bypassed the completeness rule the
    whole of rubric 1.3 rests on (Opus M-1, MINOR-5). A payload we cannot classify
    is a consistency error, not a record.

    `want_offsets` is **off** by default and that is a measured decision, not a
    micro-optimisation. Reading the Connect offset maps costs ~20 JPype calls per
    record (`sourceRecord()`, `sourcePartition()`, `sourceOffset()`, then an
    `entrySet()` walk), and it retains two dicts per record. On a 200 000-event
    transaction that dominated decode and made throughput *degrade* as the buffer
    grew. Only two records per transaction actually need them - the `BEGIN`/`END`
    markers, whose payload carries no `source` block and therefore no LSN - plus
    the one terminal record the resume point is built from, which
    `Applier._resume_point_for` fetches on demand.
    """
    topic = str(raw.destination())
    value = raw.value()
    text = "" if value is None else str(value)
    # The JSON *text* length, not the retained Python size of the `PendingRecord`
    # plus its decoded dicts, which is several times larger. The spill and chunk
    # thresholds are therefore looser in real memory than the constants read
    # (Opus MINOR-12); they are calibrated by measurement (ADR §15/A16), so the
    # numbers are right and the units are a proxy.
    nbytes = len(text)

    rec = PendingRecord(raw=raw, kind=KIND_UNKNOWN, topic=topic, nbytes=nbytes)
    if want_offsets:
        rec.source_partition, rec.source_offset = offsets_of(raw)

    if topic.startswith("__debezium-heartbeat"):
        rec.source_partition, rec.source_offset = offsets_of(raw)
        rec.kind = KIND_HEARTBEAT
        rec.lsn = _offset_lsn(rec.source_offset)
        return rec

    if not text.strip():
        # A tombstone (key, null value). `tombstones.on.delete=false` should stop
        # these, but a null payload must never become a data event.
        rec.kind = KIND_UNKNOWN
        rec.lsn = _offset_lsn(rec.source_offset)
        return rec

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise EnvelopeDecodeError(
            f"payload on {topic} is not JSON ({exc}); a record we cannot decode "
            "must not become a data event or a boundary (ADR 0001 §3.2)"
        ) from exc
    if not isinstance(payload, dict):
        raise EnvelopeDecodeError(
            f"payload on {topic} decoded to {type(payload).__name__}, not an object"
        )

    value_schema, payload = _unwrap_schema_payload(payload)
    if topic == f"{topic_prefix}.transaction":
        # The only records whose LSN is NOT in the payload: the transaction value
        # schema is {status, id, ts_ms, event_count, data_collections}, so the
        # commit LSN has to come from the Connect offset.
        if rec.source_offset is None:
            rec.source_partition, rec.source_offset = offsets_of(raw)
        status = str(payload.get("status") or "").upper()
        # NOT `BEGIN if status == "BEGIN" else END`. That made every unrecognised
        # control payload an END with `event_count = None`, which then closed the
        # open transaction without any completeness check (Opus M-1).
        if status == "BEGIN":
            rec.kind = KIND_TXN_BEGIN
        elif status == "END":
            rec.kind = KIND_TXN_END
        else:
            raise EnvelopeDecodeError(
                f"transaction marker on {topic} has status {status!r}, expected "
                "'BEGIN' or 'END'. Treating it as an END would terminate the open "
                "transaction without proving it whole (ADR 0001 §3.2)."
            )
        rec.txn_status = status
        rec.txn_id = _txn_id(payload.get("id"))
        rec.txn_event_count = payload.get("event_count")
        rec.source_ts_ms = payload.get("ts_ms")
        for entry in payload.get("data_collections") or []:
            if isinstance(entry, dict):
                rec.txn_data_collections[str(entry.get("data_collection"))] = int(
                    entry.get("event_count") or 0
                )
        rec.lsn = _offset_lsn(rec.source_offset)
        return rec

    source = payload.get("source") or {}
    rec.op = payload.get("op")
    message = payload.get("message")
    rec.message_prefix = _as_str(
        message.get("prefix") if isinstance(message, dict) else payload.get("prefix")
    )
    rec.schema = source.get("schema")
    rec.table = source.get("table")
    rec.lsn = source.get("lsn") or _offset_lsn(rec.source_offset)
    rec.source_cluster_id = _as_str(
        source.get("system_identifier", source.get("system_id", source.get("cluster_id")))
    )
    rec.source_timeline = _as_int(
        source.get("timeline_id", source.get("timeline"))
    )
    rec.relation_generation = _as_str(
        source.get("relation_generation", source.get("relation_gen"))
    )
    rec.commit_lsn = _as_int(source.get("commit_lsn"))
    rec.source_ts_ms = source.get("ts_ms")
    rec.snapshot = _as_str(source.get("snapshot"))
    rec.value_schema = value_schema
    rec.before_schema = _schema_for_field(value_schema, "before")
    rec.after_schema = _schema_for_field(value_schema, "after")
    rec.before = payload.get("before")
    rec.after = payload.get("after")
    rec.before_descriptors = _field_descriptors(rec.before_schema)
    rec.after_descriptors = _field_descriptors(rec.after_schema)
    rec.typed_before = TypedImage.from_mapping(rec.before, rec.before_descriptors)
    rec.typed_after = TypedImage.from_mapping(rec.after, rec.after_descriptors)

    txn = payload.get("transaction")
    if isinstance(txn, dict):
        # MEASURED, 2026-07-30, Debezium 3.6.0.Final + pgoutput: the envelope's
        # `transaction.id` is NOT a transaction identifier. It is
        # `"<txId>:<lsn at the moment this struct was built>"`, so every event of
        # one transaction carries a DIFFERENT id, and BEGIN's differs from END's:
        #
        #   BEGIN  {"id":"11115:937926432"}
        #   data   {"id":"11115:937926432","total_order":1}
        #   data   {"id":"11115:937926736","total_order":2}
        #   END    {"id":"11115:937927152","event_count":3}
        #
        # ADR 0001 §3.2/§6 assumed it was stable. Taking it literally makes every
        # multi-event transaction look like a `txId` change without an END - which
        # is exactly the fatal error the assembler raises. The stable identifier is
        # the prefix, which equals `source.txId`, and that is what is used.
        rec.txn_id = _txn_id(txn.get("id"))
        rec.total_order = txn.get("total_order")
        rec.data_collection_order = txn.get("data_collection_order")
    if source.get("txId") is not None:
        rec.txn_id = _as_str(source.get("txId"))

    key_text = raw.key()
    if key_text is not None:
        key_str = str(key_text).strip()
        if key_str:
            try:
                parsed = json.loads(key_str)
                rec.key_schema, parsed = _unwrap_schema_payload(parsed)
                rec.key = parsed if isinstance(parsed, dict) else None
                rec.key_descriptors = _field_descriptors(rec.key_schema)
                rec.typed_key = TypedImage.from_mapping(rec.key, rec.key_descriptors)
            except json.JSONDecodeError:  # pragma: no cover
                rec.key = None

    if rec.op == OP_TRUNCATE:
        # `skipped.operations=none` is what lets these through at all; the pgoutput
        # decoder drops the 'T' message outright while TRUNCATE is skipped
        # (`PgOutputMessageDecoder.isTruncateEventsIncluded`). One event per relation
        # of a `TRUNCATE a, b CASCADE`, all inside one transaction.
        rec.kind = KIND_TRUNCATE
    elif rec.op == OP_MESSAGE:
        rec.kind = KIND_MESSAGE
    elif rec.op is None and "ddl" in payload:
        rec.kind = KIND_SCHEMA_CHANGE
    elif rec.snapshot in SNAPSHOT_VALUES:
        rec.kind = KIND_SNAPSHOT
        # A snapshot record has no transaction metadata (its dispatch path never
        # reaches `TransactionMonitor.dataEvent`, verified in
        # `EventDispatcher.java:324` vs `dispatchSnapshotEvent`). Anything the
        # `source` block happens to carry must not be mistaken for one, or the
        # assembler would try to close a transaction that has no END.
        rec.txn_id = None
        rec.total_order = None
        rec.incremental = rec.snapshot == SNAPSHOT_INCREMENTAL
    else:
        rec.kind = KIND_DATA
    return rec


def _as_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _txn_id(value: Any) -> str | None:
    """The stable part of Debezium 3.6's `"<txId>:<lsn>"` transaction id."""
    if value is None:
        return None
    return str(value).split(":", 1)[0]


def _offset_lsn(offset: dict[str, Any] | None) -> int | None:
    if not offset:
        return None
    for key in ("lsn", "lsn_proc", "lsn_commit"):
        value = offset.get(key)
        if isinstance(value, int):
            return value
    return None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _unwrap_schema_payload(payload: Any) -> tuple[dict[str, Any] | None, Any]:
    """Accept both schema-disabled envelopes and Connect schema/payload wrappers."""
    if (
        isinstance(payload, dict)
        and isinstance(payload.get("schema"), dict)
        and "payload" in payload
    ):
        return payload["schema"], payload.get("payload")
    return None, payload


def _schema_for_field(schema: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    if not schema:
        return None
    for field_schema in schema.get("fields", ()) or ():
        if not isinstance(field_schema, dict):
            continue
        field_name = field_schema.get("field", field_schema.get("name"))
        if field_name == name:
            nested = field_schema.get("schema")
            return nested if isinstance(nested, dict) else field_schema
    return None


_FIELD_DESCRIPTOR_CACHE: dict[tuple, dict[str, SourceTypeDescriptor]] = {}
_FIELD_DESCRIPTOR_CACHE_MAX = 128


def _schema_cache_key(schema: dict[str, Any]) -> tuple:
    """Stable, type-bearing key that avoids serialising the full envelope schema."""
    nested = {"fields", "items", "value_schema", "keys", "key_schema", "values"}
    scalar = tuple(
        sorted(
            (str(name), json.dumps(value, sort_keys=True, separators=(",", ":")))
            for name, value in schema.items()
            if name not in nested and not isinstance(value, (dict, list))
        )
    )
    fields = tuple(
        (
            str(field_schema.get("field", field_schema.get("name", ""))),
            _schema_cache_key(
                field_schema.get("schema")
                if isinstance(field_schema.get("schema"), dict)
                else field_schema.get("type")
                if isinstance(field_schema.get("type"), dict)
                else field_schema,
            ),
        )
        for field_schema in schema.get("fields", ()) or ()
        if isinstance(field_schema, dict)
    )
    children = tuple(
        (name, _schema_cache_key(value))
        for name in ("items", "value_schema", "keys", "key_schema", "values")
        if isinstance((value := schema.get(name)), dict)
    )
    enum_values = schema.get("values", schema.get("enum"))
    return scalar, fields, children, tuple(enum_values or ()) if isinstance(enum_values, list) else enum_values


def _field_descriptors(schema: dict[str, Any] | None) -> dict[str, SourceTypeDescriptor]:
    if not schema:
        return {}
    key = _schema_cache_key(schema)
    cached = _FIELD_DESCRIPTOR_CACHE.get(key)
    if cached is None:
        cached = _field_descriptors_uncached(schema)
        if len(_FIELD_DESCRIPTOR_CACHE) >= _FIELD_DESCRIPTOR_CACHE_MAX:
            _FIELD_DESCRIPTOR_CACHE.pop(next(iter(_FIELD_DESCRIPTOR_CACHE)))
        _FIELD_DESCRIPTOR_CACHE[key] = cached
    # Catalog enrichment may add fields to an event-local mapping; share only the
    # immutable descriptor objects across repeated schema instances.
    return dict(cached)


def _field_descriptors_uncached(
    schema: dict[str, Any] | None,
) -> dict[str, SourceTypeDescriptor]:
    if not schema:
        return {}
    result: dict[str, SourceTypeDescriptor] = {}
    for field_schema in schema.get("fields", ()) or ():
        if not isinstance(field_schema, dict):
            continue
        name = field_schema.get("field", field_schema.get("name"))
        nested = field_schema.get("schema")
        if nested is None and isinstance(field_schema.get("type"), dict):
            nested = field_schema["type"]
        if name is None or not isinstance(nested if nested is not None else field_schema, dict):
            continue
        try:
            result[str(name)] = SourceTypeDescriptor.from_connect_schema(
                nested if isinstance(nested, dict) else field_schema
            )
        except (TypeError, ValueError):
            # A malformed optional field is not allowed to erase the whole envelope;
            # the strict resolver will refuse the field if the caller attempts to
            # materialize it.  Existing transaction metadata still remains usable.
            continue
    return result
