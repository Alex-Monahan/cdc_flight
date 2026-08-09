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
        "catalog_schema_liveness": schema_liveness,
        "toast_efficient_tables": sum(policy.efficient for policy in toast_policies),
        "toast_fallback_tables": sum(policy.route is ToastRoute.FALLBACK for policy in toast_policies),
        "toast_residual_columns": sum(len(policy.residual_columns) for policy in toast_policies),
    }


def observe_unit(watcher, unit) -> None:
    """Fence a late schema event by probing before its unit is appended."""
    candidates: list[str] = []
    field_sets: dict[str, set[str]] = {}
    for record in unit.events:
        if not record.schema or not record.table:
            continue
        name = f"{record.schema}.{record.table}"
        fields = set()
        for image in (record.before, record.after, record.key):
            if image:
                fields.update(image)
        field_sets.setdefault(name, set()).update(fields)
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
            )


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


def read_columns(watcher, relation, key_columns, value_columns) -> list[tuple]:
    """Read current source values for a fenced add-column backfill."""
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
    select_list = ", ".join(quote(source_names[name]) for name in destinations)
    with watcher._connect() as conn:
        return conn.execute(
            f"SELECT {select_list} FROM {quote(relation.schema)}."
            f"{quote(relation.table)}"
        ).fetchall()
