"""Source-catalog SQL and observation-shape constants.

The watcher owns state transitions; this module owns the read-only source projection.
Keeping the SQL here makes the observation boundary independently reviewable and
prevents publication policy from being mixed into catalog reads.
"""

from __future__ import annotations

from . import naming
from .catalog_state import CHANGE_NEW, CHANGE_SCHEMA, CHANGE_UNPUBLISHED, DESTRUCTIVE
from .errors import SchemaShapeUnexplained
from .machines import ADMISSION_ADMITTED, ADMISSION_EXTERNAL
from .toast import ToastRoute, classify_relation

CATALOG_SQL = """
SELECT n.nspname                                  AS source_schema,
       c.relname                                  AS source_table,
       c.oid::bigint                              AS relation_oid,
       c.relfilenode::bigint                      AS relation_filenode,
       c.reltype::bigint                          AS relation_type_oid,
       c.relreplident                             AS replica_identity,
       (
           COALESCE(p.puballtables, false)
           OR pr.prrelid IS NOT NULL
           OR parent_pr.prrelid IS NOT NULL
       )                                           AS published,
       COALESCE(p.puballtables, false)             AS publication_all_tables,
       COALESCE(inh.inhparent IS NOT NULL, false)  AS is_partition,
       COALESCE(
           jsonb_agg(
               jsonb_build_object(
                   'attnum', a.attnum,
                   'name', a.attname,
                   'type_oid', a.atttypid::bigint,
                   'type_name', format_type(a.atttypid, a.atttypmod),
                   'typmod', a.atttypmod,
                   'attstorage', a.attstorage,
                   'type_schema', typ_ns.nspname,
                   'type_kind', typ.typtype,
                   'typelem', typ.typelem::bigint,
                   'typbasetype', typ.typbasetype::bigint,
                   'typrelid', typ.typrelid::bigint,
                   'nullable', NOT a.attnotnull,
                   'has_missing_default', COALESCE(a.atthasmissing, false),
                   'missing_value_text', CASE WHEN a.atthasmissing
                       THEN a.attmissingval::text ELSE NULL END
               ) ORDER BY a.attnum
           ) FILTER (WHERE a.attnum IS NOT NULL),
           '[]'::jsonb
       )                                           AS columns_json
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_publication p ON p.pubname = %s
LEFT JOIN pg_publication_rel pr ON pr.prrelid = c.oid AND pr.prpubid = p.oid
LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid
LEFT JOIN pg_publication_rel parent_pr
    ON parent_pr.prrelid = inh.inhparent AND parent_pr.prpubid = p.oid
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
LEFT JOIN pg_type typ ON typ.oid = a.atttypid
LEFT JOIN pg_namespace typ_ns ON typ_ns.oid = typ.typnamespace
WHERE c.relkind IN ('r', 'p')
  AND (
      (%s::text[] IS NULL AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_cdc_flight'))
      OR n.nspname = ANY(%s::text[])
  )
GROUP BY n.nspname, c.relname, c.oid, c.relfilenode, c.reltype, c.relreplident,
         p.puballtables,
         pr.prrelid, inh.inhparent, parent_pr.prrelid
"""

# A catalog read may run on a hot standby.  `pg_current_wal_lsn()` is primary-only;
# the receive position is the corresponding upper bound for the read-side WAL fence.
LSN_SQL = """
SELECT ((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()
              ELSE pg_current_wal_lsn() END) - '0/0'::pg_lsn)::bigint
"""

# A primary's flushed LSN can lag a catalog DDL even after the statement returned.
# The activation fence is sampled on the same write connection after FULL was
# verified, so use the insert position there; on a standby the receive position is
# the available upper bound.
ACTIVATION_LSN_SQL = """
SELECT ((CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()
              ELSE pg_current_wal_insert_lsn() END) - '0/0'::pg_lsn)::bigint
"""

SCHEMA_LIVENESS_SQL = """
SELECT n.nspname,
       count(c.oid)::bigint AS relation_count
FROM pg_namespace n
LEFT JOIN pg_class c
  ON c.relnamespace = n.oid
 AND c.relkind IN ('r', 'p')
WHERE (
    (%s::text[] IS NULL AND n.nspname NOT IN
        ('pg_catalog', 'information_schema', '_cdc_flight'))
    OR n.nspname = ANY(%s::text[])
)
GROUP BY n.nspname
"""

PARTITION_SQL = """
SELECT child_n.nspname, child.relname
FROM pg_inherits i
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_namespace child_n ON child_n.oid = child.relnamespace
JOIN pg_class parent ON parent.oid = i.inhparent
JOIN pg_namespace parent_n ON parent_n.oid = parent.relnamespace
LEFT JOIN pg_publication p ON p.pubname = %s
LEFT JOIN pg_publication_rel pr ON pr.prrelid = parent.oid AND pr.prpubid = p.oid
WHERE parent.relkind IN ('r', 'p')
  AND (
      (%s::text[] IS NULL AND parent_n.nspname NOT IN ('pg_catalog', 'information_schema', '_cdc_flight'))
      OR parent_n.nspname = ANY(%s::text[])
  )
  AND (COALESCE(p.puballtables, false) OR pr.prrelid IS NOT NULL)
"""


def summary(watcher) -> dict:
    """Return the stable operational summary for one quiesced or live watcher."""
    with watcher._lock:
        pending = watcher._live()
        pending_admission = [
            change.qualified
            for change in pending
            if change.kind in {CHANGE_NEW, CHANGE_UNPUBLISHED}
            and (
                watcher.known.get(change.qualified) is None
                or watcher.known[change.qualified].admission_state
                not in {ADMISSION_ADMITTED, ADMISSION_EXTERNAL}
                or not watcher.known[change.qualified].published
            )
        ]
        admission_errors = dict(watcher._admission_errors)
        schema_liveness = dict(watcher._schema_liveness)
        toast_policies = [
            classify_relation(
                relation.qualified,
                relation.columns,
                replica_identity=relation.replica_identity,
                binary_mode=getattr(watcher, "binary_handling_mode", "base64"),
                hstore_mode=getattr(watcher, "hstore_handling_mode", "map"),
                full_activation_lsn=relation.full_activation_lsn,
                full_invalidation_lsn=relation.full_invalidation_lsn,
            )
            for relation in watcher.known.values()
        ]
    return {
        "catalog_polls": watcher.polls,
        "catalog_successful_polls": watcher.successful_polls,
        "catalog_unrelatable": sorted(watcher.unrelatable),
        "catalog_machine_error": watcher.machine_error,
        "catalog_empty_polls": watcher.empty_polls,
        "catalog_markers": watcher.marker.writes,
        "catalog_pending": len(pending),
        "catalog_pending_destructive": sum(
            1 for change in pending if change.kind in DESTRUCTIVE
        ),
        "catalog_pending_schema": sum(
            1 for change in pending if change.kind == CHANGE_SCHEMA
        ),
        "catalog_superseded": watcher.superseded,
        "catalog_error": watcher.last_error,
        "catalog_marker_error": watcher.marker.last_error,
        "catalog_marker_capable": watcher.marker.capable,
        "catalog_publication_ownership": watcher.publication_ownership,
        "catalog_pending_admission": sorted(pending_admission),
        "catalog_admission_errors": admission_errors,
        "catalog_schema_refusals": sorted(watcher._schema_refusals),
        "catalog_schema_liveness": schema_liveness,
        "toast_efficient_tables": sum(policy.efficient for policy in toast_policies),
        "toast_fallback_tables": sum(policy.route is ToastRoute.FALLBACK for policy in toast_policies),
        "toast_residual_columns": sum(len(policy.residual_columns) for policy in toast_policies),
        "toast_policy_builds": watcher.toast_policy_builds,
        "toast_policy_cache_hits": watcher.toast_policy_cache_hits,
        "toast_admission_checks": watcher.toast_admission_checks,
        "toast_source_revalidations": watcher.toast_source_revalidations,
        "toast_admission_rejections": watcher.toast_admission_rejections,
    }


def observe_unit(watcher, unit) -> None:
    """Fence a late schema event by probing before its unit is appended."""
    candidates: list[str] = []
    field_sets: dict[str, set[str]] = {}
    shape_records: dict[str, list[object]] = {}
    for record in unit.events:
        if not record.schema or not record.table:
            continue
        name = f"{record.schema}.{record.table}"
        fields = set()
        for image in (record.before, record.after, record.key):
            if image:
                fields.update(image)
        fields.update(delivered_event_fields(record))
        field_sets.setdefault(name, set()).update(fields)
        shape_records.setdefault(name, []).append(record)
        with watcher._lock:
            relation = watcher.known.get(name)
            known_names = (
                {column.destination_name for column in relation.columns}
                if relation
                else set()
            )
            known_descriptors = {
                column.destination_name: column.descriptor
                for column in relation.columns
                if relation and column.descriptor is not None
            } if relation else {}
        descriptor_changed = any(
            _descriptor_changed(known_descriptors, record_descriptors, fields)
            for record_descriptors in (
                getattr(record, "key_descriptors", {}),
                getattr(record, "before_descriptors", {}),
                getattr(record, "after_descriptors", {}),
            )
        )
        if fields - known_names or descriptor_changed:
            candidates.append(name)
        if relation and relation.columns:
            catalog_names = {
                column.destination_name for column in relation.columns
            }
            # Probe only the record currently being collected.  Re-scanning
            # ``shape_records[name]`` here made a 5,000-row source transaction
            # quadratic; the complete per-unit pass below rechecks every record
            # once after any synchronous catalog poll.
            if event_shape_missing(watcher, record, catalog_names):
                candidates.append(name)
    if candidates and watcher.dsn:
        # Never suppress a second probe after a failed or empty observation. A source
        # may have committed another DDL between callbacks; a one-shot guard would
        # turn a transient catalog miss into a permanently unexplained row shape.
        watcher.poll_quietly()
    for name, fields in field_sets.items():
        # A relation without column metadata is the startup/legacy shape: the
        # applier may already have a durable table while the first catalog
        # observation is still being established.  It cannot prove that a row
        # field is unexplained, so keep the unit in the ordinary catalog path.
        # Once a relation has a column-bearing epoch, an unknown field is a
        # closed-model violation and must be refused rather than guessed.
        with watcher._lock:
            relation = watcher.known.get(name)
            has_column_epoch = bool(relation and relation.columns)
        if not has_column_epoch:
            continue
        allowed = watcher.allowed_event_fields(name)
        unknown = sorted(fields - allowed)
        if unknown:
            schema, _, table = name.partition(".")
            raise SchemaShapeUnexplained(
                f"row shape for {name} contains {unknown}, but the source catalog "
                "has no current or fenced schema epoch containing those fields; "
                "an intermediate DDL history was hidden between polls, so the "
                "unit is refused rather than folded against the wrong identity",
                source_schema=schema,
                source_table=table,
                target=name,
                refusal_origin="catalog_shape",
            )
        catalog_names = {
            column.destination_name for column in relation.columns
        }
        for record in shape_records.get(name, ()):
            missing = event_shape_missing(watcher, record, catalog_names)
            if missing:
                schema, _, table = name.partition(".")
                raise SchemaShapeUnexplained(
                    f"source catalog/event shape is incomplete for {name}: the "
                    f"connector delivered no schema field(s) {missing!r}; refusing "
                    "table creation/commit rather than truncating the source row",
                    source_schema=schema,
                    source_table=table,
                    target=name,
                    detected_lsn=getattr(record, "lsn", None),
                    refusal_origin="catalog_shape",
                )


def delivered_event_fields(record) -> set[str]:
    """Return fields the connector delivered, including schema-only NULL fields."""
    fields: set[str] = set()
    for image, schema in (
        (getattr(record, "key", None), getattr(record, "key_schema", None)),
        (getattr(record, "before", None), getattr(record, "before_schema", None)),
        (getattr(record, "after", None), getattr(record, "after_schema", None)),
    ):
        if image:
            fields.update(naming.normalize(str(name)) for name in image)
        if isinstance(schema, dict):
            for field_schema in schema.get("fields", ()) or ():
                if not isinstance(field_schema, dict):
                    continue
                field_name = field_schema.get("field", field_schema.get("name"))
                if field_name is not None:
                    fields.add(naming.normalize(str(field_name)))
    return fields


def event_shape_missing(watcher, record, catalog_names: set[str]) -> tuple[str, ...]:
    """Return source columns absent from one event, honoring fenced epochs."""
    # The inverse gate is about the connector's *published schema*, not a sparse
    # value image.  Synthetic replay records and older embedded callers may carry
    # only ``before``/``after`` mappings; those have no claim about omitted fields.
    # Stock schema-enabled Debezium records do carry a Connect struct schema, and
    # that is the evidence required to distinguish an omitted unknown datatype from
    # an ordinary sparse update.
    if not has_event_schema(record):
        return ()
    delivered = delivered_event_fields(record)
    if not catalog_names or not getattr(record, "is_data", True):
        return ()
    with watcher._lock:
        changes = tuple(
            change
            for change in watcher._live()
            if change.qualified == record.qualified_table
            and change.kind == CHANGE_SCHEMA
        )
    # A catalog schema change is a two-epoch window.  The watcher may still hold the
    # old relation while the fenced change is live, or may already hold the new
    # relation while an older WAL unit is being replayed.  Both shapes are valid until
    # the fence is settled; treating the currently held relation as the only valid
    # shape would reject a replayed unit from the other side of the fence.
    expected = set(catalog_names)
    event_lsn = getattr(record, "lsn", None)
    for change in changes:
        new_relation = getattr(change, "new_relation", None)
        new_names = (
            {column.destination_name for column in new_relation.columns}
            if new_relation is not None
            else set()
        )
        if not new_names:
            continue
        old_names = set(catalog_names)
        if new_names == old_names:
            # The catalog has already advanced. Reconstruct the prior epoch from the
            # attnum-preserving diff, including add/drop/rename transitions.
            old_names = set(new_names)
            for column in change.column_changes:
                old_name = column.destination_old_name
                new_name = column.destination_new_name
                if column.kind in {"added", "renamed"} and new_name:
                    old_names.discard(new_name)
                if column.kind in {"dropped", "renamed"} and old_name:
                    old_names.add(old_name)
        old_only = old_names - new_names
        new_only = new_names - old_names
        has_old_shape = bool(delivered & old_only)
        has_new_shape = bool(delivered & new_only)
        # The Connect struct is stronger evidence than the watcher's polling LSN:
        # ``detected_lsn`` is sampled when the poll notices the DDL and can therefore
        # be *after* a post-DDL DML event.  A complete struct that exactly matches one
        # epoch identifies that epoch even when the catalog poll learned about it late.
        if delivered == new_names:
            expected = new_names
        elif delivered == old_names:
            expected = old_names
        elif has_new_shape and not has_old_shape:
            expected = new_names
        elif has_old_shape and not has_new_shape:
            expected = old_names
        else:
            detected = getattr(change, "detected_lsn", None)
            if event_lsn is not None and detected is not None:
                expected = (
                    old_names if int(event_lsn) < int(detected) else new_names
                )
    missing = expected - delivered
    # Stock Debezium 3.x omits schema/value fields whose JDBC type is an opaque
    # PostgreSQL array element (notably xml[]), even with
    # include.unknown.datatypes=true.  Those fields are recoverable from the
    # source catalog connection by key; they are not permission to create a
    # partial destination table, so keep them out of the hard completeness refusal
    # and let the typed planner hydrate them before folding the row.
    missing -= set(omitted_xml_array_fields(watcher, record, catalog_names))
    return tuple(sorted(missing))


def has_event_schema(record) -> bool:
    """Whether a record carries an explicit Connect struct shape to gate."""
    return any(
        isinstance(getattr(record, name, None), dict)
        and isinstance(getattr(record, name, {}).get("fields"), list)
        for name in ("key_schema", "before_schema", "after_schema")
    )


def omitted_xml_array_fields(
    watcher, record, catalog_descriptors: set[str] | dict[str, object]
) -> tuple[str, ...]:
    """Return omitted source fields that can be read back without synthesis.

    This is deliberately narrower than "all missing fields": only an array whose
    catalog descriptor has an XML element is eligible.  The source SELECT is the
    PostgreSQL value boundary, and the planner still refuses every other unexplained
    omission rather than guessing a type or silently dropping a column.
    """
    delivered = delivered_event_fields(record)
    names = set(catalog_descriptors)
    descriptors = (
        catalog_descriptors
        if isinstance(catalog_descriptors, dict)
        else {}
    )
    if not descriptors:
        with watcher._lock:
            relation = watcher.known.get(getattr(record, "qualified_table", None))
            descriptors = {
                column.destination_name: column.descriptor
                for column in (relation.columns if relation is not None else ())
            }
    omitted: list[str] = []
    for name in sorted(names - delivered):
        descriptor = descriptors.get(name)
        if descriptor is None:
            continue
        kind = str(getattr(descriptor, "kind", "")).lower()
        element = getattr(descriptor, "array_element", None)
        element_kind = str(getattr(element, "kind", "")).lower()
        if kind == "array" and element_kind == "xml":
            omitted.append(name)
    return tuple(omitted)


def _descriptor_changed(known: dict, incoming: dict, fields: set[str]) -> bool:
    """Detect a same-name type epoch change before row admission."""
    for raw_name, descriptor in (incoming or {}).items():
        name = naming.normalize(str(raw_name))
        if name not in fields or descriptor is None or name not in known:
            continue
        previous = known[name]
        if previous is not None and getattr(previous, "fingerprint", None) != getattr(
            descriptor, "fingerprint", None
        ):
            return True
    return False


def _policy_projection(relation, columns, policy_gate, *, key_columns=()):
    """Build a source projection that does not select excluded columns."""
    from .policy import PolicyValueRefused

    descriptors = {
        naming.normalize(column.name): column.descriptor
        for column in relation.columns
    }
    normalized_keys = {naming.normalize(column) for column in key_columns}
    allowed: list[str] = []
    expressions: list[str] = []
    kinds: dict[str, str] = {}
    for name in columns:
        normalized = naming.normalize(name)
        rule = policy_gate.policy.rule_for(relation.qualified, normalized)
        if normalized in normalized_keys and rule.action != "replicate":
            raise PolicyValueRefused(
                "a transformed or excluded source key cannot identify a backfill row"
            )
        if rule.action == "exclude":
            continue
        source_name = next(
            column.name
            for column in relation.columns
            if naming.normalize(column.name) == normalized
        )
        if rule.action in {"hash", "truncate"}:
            # PostgreSQL format(%s, value) invokes the value's type OUTPUT
            # function. It is deliberately not a ::text cast or Python rendering.
            # Build the percent marker without a literal ``%s`` token: psycopg
            # reserves that spelling for bind parameters even when the query has
            # no selector parameters. PostgreSQL's format() still dispatches the
            # value through its type OUTPUT function.
            expressions.append(
                f"format(chr(37) || 's', {naming.quote(source_name)})"
            )
            kinds[normalized] = "output"
        else:
            expressions.append(naming.quote(source_name))
            kinds[normalized] = "raw"
        allowed.append(normalized)
    return allowed, expressions, kinds, descriptors


def _sanitize_source_rows(
    relation,
    columns,
    rows,
    policy_gate,
    *,
    key_columns=(),
    kinds=None,
    descriptors=None,
) -> list[tuple]:
    from .policy import PostgreSQLOutputText

    normalized_columns = [naming.normalize(column) for column in columns]
    kinds = kinds or {}
    descriptors = descriptors or {}
    output = []
    for row in rows:
        mapping = dict(zip(normalized_columns, row, strict=True))
        output_texts = {}
        for name, kind in kinds.items():
            if kind != "output" or mapping.get(name) is None:
                continue
            value = mapping[name]
            if not isinstance(value, str):
                raise TypeError("PostgreSQL output projection did not return text")
            descriptor = descriptors.get(name)
            output_texts[name] = PostgreSQLOutputText(
                value,
                getattr(descriptor, "output_function_oid", None),
            )
        sanitized = policy_gate.sanitize_mapping(
            relation.qualified,
            mapping,
            descriptors,
            output_texts=output_texts,
            key_columns=tuple(key_columns),
        )
        output.append(tuple(sanitized[name] for name in normalized_columns if name in sanitized))
    return output


def read_columns(
    watcher,
    relation,
    key_columns,
    value_columns,
    *,
    policy_gate=None,
) -> list[tuple]:
    """Read current source values for a fenced add-column backfill.

    With a policy gate, excluded fields are not part of the SELECT and hash/truncate
    fields are projected through PostgreSQL's OUTPUT-function boundary before the
    resulting row is returned to the destination backfill path.
    """
    from .naming import normalize, quote

    source_names = {
        normalize(column.name): column.name for column in relation.columns
    }
    destinations = tuple(key_columns) + tuple(value_columns)
    missing = [name for name in destinations if name not in source_names]
    if missing:
        raise ValueError(
            f"source relation {relation.qualified} has no catalog columns for "
            f"{missing}"
        )
    if policy_gate is not None and policy_gate.policy.enabled:
        allowed, expressions, kinds, descriptors = _policy_projection(
            relation, destinations, policy_gate, key_columns=key_columns
        )
        if not expressions:
            return []
        select_list = ", ".join(expressions)
    else:
        allowed = [normalize(name) for name in destinations]
        select_list = ", ".join(quote(source_names[name]) for name in allowed)
    with watcher._connect() as conn:
        rows = conn.execute(
            f"SELECT {select_list} FROM {quote(relation.schema)}."
            f"{quote(relation.table)}"
        ).fetchall()
    if policy_gate is None or not policy_gate.policy.enabled:
        return rows
    return _sanitize_source_rows(
        relation,
        allowed,
        rows,
        policy_gate,
        key_columns=key_columns,
        kinds=kinds,
        descriptors=descriptors,
    )


def read_event_columns(watcher, event, value_columns) -> dict[str, object] | None:
    """Read omitted opaque-array values for one event from PostgreSQL.

    A primary key is the normal selector.  For a keyless table, the delivered
    non-omitted image is used as a NULL-safe selector; an unidentifiable multi-row
    result is refused rather than assigning one source row to another.  The query
    selects the source columns directly -- no Python rendering or type synthesis.
    """
    from .naming import normalize

    with watcher._lock:
        relation = watcher.known.get(event.qualified_table)
        source_names = {
            normalize(column.name): column.name
            for column in (relation.columns if relation is not None else ())
        }
        descriptors = {
            normalize(column.name): column.descriptor
            for column in (relation.columns if relation is not None else ())
        }
    if not source_names:
        source_names = {normalize(name): str(name) for name in value_columns}
        for image in (event.key, event.before, event.after):
            for name in (image or {}):
                source_names.setdefault(normalize(name), str(name))
    with watcher._connect() as conn:
        return _read_event_columns(
            conn,
            event,
            value_columns,
            source_names,
            policy_gate=getattr(watcher, "policy_gate", None),
            descriptors=descriptors,
        )


def read_event_columns_from_connection(
    con, event, value_columns, *, policy_gate=None, descriptors=None
) -> dict[str, object] | None:
    """Connection-backed variant used by the bounded resnapshot descriptor provider."""
    from .naming import normalize

    source_names = {normalize(name): str(name) for name in value_columns}
    for image in (event.key, event.before, event.after):
        for name in (image or {}):
            source_names.setdefault(normalize(name), str(name))
    return _read_event_columns(
        con,
        event,
        value_columns,
        source_names,
        policy_gate=policy_gate,
        descriptors=descriptors,
    )


def _read_event_columns(
    con,
    event,
    value_columns,
    source_names,
    *,
    policy_gate=None,
    descriptors=None,
) -> dict[str, object] | None:
    from .naming import normalize, quote

    values = tuple(normalize(name) for name in value_columns)
    missing = [name for name in values if name not in source_names]
    if missing:
        raise SchemaShapeUnexplained(
            f"source catalog has no column(s) {missing!r} needed to recover "
            f"{event.qualified_table}",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
            refusal_origin="catalog_shape",
        )
    predicates: list[str] = []
    params: list[object] = []
    key = event.key or {}
    for raw_name, value in key.items():
        name = normalize(raw_name)
        if name in source_names:
            if policy_gate is not None and policy_gate.policy.enabled:
                rule = policy_gate.policy.rule_for(event.qualified_table, name)
                if rule.action != "replicate":
                    from .policy import PolicyValueRefused

                    raise PolicyValueRefused(
                        "a transformed or excluded source key cannot identify a "
                        "source recovery row"
                    )
            predicates.append(f"{quote(source_names[name])} IS NOT DISTINCT FROM %s")
            params.append(value)
    if not predicates:
        image = event.before if event.op == "d" else event.after
        for raw_name, value in (image or {}).items():
            name = normalize(raw_name)
            if name not in source_names or name in values:
                continue
            predicates.append(f"{quote(source_names[name])} IS NOT DISTINCT FROM %s")
            params.append(value)
    kinds: dict[str, str] = {}
    if policy_gate is not None and policy_gate.policy.enabled:
        descriptors = descriptors or {}
        expressions = []
        for name in values:
            rule = policy_gate.policy.rule_for(event.qualified_table, name)
            if rule.action == "exclude":
                continue
            if rule.action in {"hash", "truncate"}:
                expressions.append(
                    f"format(chr(37) || 's', {quote(source_names[name])})"
                )
                kinds[name] = "output"
            else:
                expressions.append(quote(source_names[name]))
                kinds[name] = "raw"
        select_list = ", ".join(expressions)
        if not select_list:
            return {}
    else:
        select_list = ", ".join(quote(source_names[name]) for name in values)
    query = (
        f"SELECT {select_list} FROM {quote(event.schema)}.{quote(event.table)}"
        + (" WHERE " + " AND ".join(predicates) if predicates else "")
        + " LIMIT 2"
    )
    rows = con.execute(query, params).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise SchemaShapeUnexplained(
            f"source row for {event.qualified_table} is not uniquely identifiable "
            "while recovering an omitted opaque array",
            source_schema=event.schema,
            source_table=event.table,
            target=event.qualified_table,
            refusal_origin="catalog_shape",
        )
    if policy_gate is None or not policy_gate.policy.enabled:
        return dict(zip(values, rows[0], strict=True))
    selected = [name for name in values if policy_gate.policy.rule_for(event.qualified_table, name).action != "exclude"]
    raw = dict(zip(selected, rows[0], strict=True))
    from .policy import PostgreSQLOutputText

    output_texts = {
        name: PostgreSQLOutputText(
            raw[name],
            getattr((descriptors or {}).get(name), "output_function_oid", None),
        )
        for name, kind in kinds.items()
        if kind == "output" and raw.get(name) is not None
    }
    return policy_gate.sanitize_mapping(
        event.qualified_table,
        raw,
        descriptors or {},
        output_texts=output_texts,
    )
