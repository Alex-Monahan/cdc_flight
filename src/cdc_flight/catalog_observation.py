"""Source-catalog SQL and observation-shape constants.

The watcher owns state transitions; this module owns the read-only source projection.
Keeping the SQL here makes the observation boundary independently reviewable and
prevents publication policy from being mixed into catalog reads.
"""

from __future__ import annotations

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
WHERE c.relkind IN ('r', 'p')
  AND (
      (%s::text[] IS NULL AND n.nspname NOT IN ('pg_catalog', 'information_schema', '_cdc_flight'))
      OR n.nspname = ANY(%s::text[])
  )
GROUP BY n.nspname, c.relname, c.oid, c.relfilenode, c.reltype, c.relreplident,
         p.puballtables,
         pr.prrelid, inh.inhparent, parent_pr.prrelid
"""

OID_SQL = """
SELECT n.nspname, c.relname, c.oid::bigint, c.relfilenode::bigint,
       c.reltype::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND (n.nspname, c.relname) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
"""

LSN_SQL = "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint"

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
