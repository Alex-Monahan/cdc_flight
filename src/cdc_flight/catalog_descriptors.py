"""Memoized PostgreSQL type-descriptor tree reader.

The relation observation remains one batch query.  This reader follows the distinct
type OIDs from that batch with a handful of targeted catalog queries and memoizes the
result for later polls.  It deliberately does not use a recursive SQL CTE or issue a
query per column/type.
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field, replace

from .config import source_connection_kwargs
from .errors import AdmissionError, SchemaEvolutionRefused
from .naming import normalize
from .schema_evolution import descriptor_from_type_name
from .typed_types import SourceTypeDescriptor, native_type


@dataclass
class CatalogDescriptorReader:
    con: object
    cache: dict[int, SourceTypeDescriptor] = field(default_factory=dict)

    def resolve(self, oids: Iterable[int]) -> dict[int, SourceTypeDescriptor]:
        wanted = {int(oid) for oid in oids if oid}
        facts: dict[int, dict] = {}
        pending = set(wanted)
        while pending:
            rows = self.con.execute(
                "SELECT t.oid::bigint, n.nspname, t.typname, t.typtype, "
                "t.typcategory, t.typbasetype::bigint, t.typelem::bigint, "
                "t.typrelid::bigint, COALESCE(r.rngsubtype, mr.rngsubtype, 0)::bigint, "
                "COALESCE(r.rngmultitypid, 0)::bigint, "
                "p.oid::bigint, pn.nspname, p.proname "
                "FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace "
                "LEFT JOIN pg_range r ON r.rngtypid=t.oid "
                "LEFT JOIN pg_range mr ON mr.rngmultitypid=t.oid "
                "LEFT JOIN pg_proc p ON p.oid=t.typoutput "
                "LEFT JOIN pg_namespace pn ON pn.oid=p.pronamespace "
                "WHERE t.oid = ANY(%s::oid[])",
                [sorted(pending)],
            ).fetchall()
            for row in rows:
                if len(row) == 10:
                    # Compatibility with small catalog fakes and pre-§8 test
                    # adapters. They do not model pg_proc, so the policy gate will
                    # require an explicit output-text proof for transforms.
                    (
                        oid, schema, name, typtype, category, base, element, relid,
                        subtype, multirange_oid,
                    ) = row
                    output_oid = output_schema = output_name = None
                else:
                    (
                        oid, schema, name, typtype, category, base, element, relid,
                        subtype, multirange_oid, output_oid, output_schema, output_name,
                    ) = row
                facts[int(oid)] = {
                    "oid": int(oid),
                    "schema": str(schema),
                    "name": str(name),
                    "typtype": str(typtype),
                    "category": str(category),
                    "base": int(base or 0),
                    "element": int(element or 0),
                    "relid": int(relid or 0),
                    "subtype": int(subtype or 0),
                    "multirange_oid": int(multirange_oid or 0),
                    "output_function_oid": int(output_oid or 0),
                    "output_function_schema": str(output_schema or ""),
                    "output_function_name": str(output_name or ""),
                }
            children = set()
            for fact in facts.values():
                children.update(item for item in (fact["base"], fact["element"], fact["subtype"]) if item)
                if fact["typtype"] == "m" and fact["multirange_oid"]:
                    children.add(fact["multirange_oid"])
            # OID identity does not make enum labels or composite attributes
            # immutable.  Revisit every descendant in every catalog epoch so an
            # ADD VALUE/ADD ATTRIBUTE cannot hide behind a process-local cache.
            pending = children - set(facts)
        enum_labels = self._enum_labels([fact["oid"] for fact in facts.values() if fact["typtype"] == "e"])
        composite_fields = self._composite_fields(
            [fact["relid"] for fact in facts.values() if fact["typtype"] == "c" and fact["relid"]]
        )
        child_oids = {
            child
            for fields in composite_fields.values()
            for _, child in fields
        }
        # A child can itself be mutable (for example a composite field whose type
        # gained an attribute).  Do not let its process-local OID cache suppress a
        # fresh recursive catalog read for this epoch.
        child_oids -= set(facts)
        if child_oids:
            self.resolve(child_oids)
        building: set[int] = set()

        def build(oid: int) -> SourceTypeDescriptor:
            if oid in building:
                raise ValueError(f"recursive PostgreSQL type descriptor at OID {oid}")
            fact = facts.get(oid)
            if fact is None:
                if oid in self.cache:
                    return self.cache[oid]
                raise KeyError(f"PostgreSQL type OID {oid} was not returned by the catalog")
            building.add(oid)
            kind = _kind_for_fact(fact)
            range_subtype = fact["subtype"]
            if kind == "multirange" and not range_subtype:
                range_fact = next(
                    (
                        candidate
                        for candidate in facts.values()
                        if candidate["oid"] == fact["multirange_oid"]
                    ),
                    None,
                )
                range_subtype = range_fact["subtype"] if range_fact else 0
            map_key = map_value = None
            if kind == "map" and fact["name"] == "hstore":
                map_key = SourceTypeDescriptor(25, "pg_catalog.text", "text")
                map_value = SourceTypeDescriptor(25, "pg_catalog.text", "text")
            descriptor = SourceTypeDescriptor(
                oid=oid,
                qualified_name=f"{fact['schema']}.{fact['name']}",
                kind=kind,
                domain_base=build(fact["base"]) if fact["typtype"] == "d" and fact["base"] else None,
                array_element=build(fact["element"]) if fact["element"] and fact["typtype"] != "d" else None,
                enum_labels=tuple(enum_labels.get(oid, ())),
                composite_fields=tuple(
                    (name, build(child_oid))
                    for name, child_oid in composite_fields.get(fact["relid"], ())
                ),
                range_subtype=(build(range_subtype) if range_subtype else None),
                map_key=map_key,
                map_value=map_value,
                output_function_oid=(fact["output_function_oid"] or None),
                output_function_schema=fact["output_function_schema"] or None,
                output_function_name=fact["output_function_name"] or None,
            )
            building.discard(oid)
            self.cache[oid] = descriptor
            return descriptor

        for oid in sorted(wanted):
            build(oid)
        return {oid: self.cache[oid] for oid in wanted if oid in self.cache}

    def _enum_labels(self, oids: list[int]) -> dict[int, list[str]]:
        if not oids:
            return {}
        result: dict[int, list[str]] = {oid: [] for oid in oids}
        for oid, label in self.con.execute(
            "SELECT enumtypid::bigint, enumlabel FROM pg_enum "
            "WHERE enumtypid = ANY(%s::oid[]) ORDER BY enumtypid, enumsortorder",
            [oids],
        ).fetchall():
            result.setdefault(int(oid), []).append(str(label))
        return result

    def _composite_fields(self, relids: list[int]) -> dict[int, list[tuple[str, int]]]:
        if not relids:
            return {}
        result: dict[int, list[tuple[str, int]]] = {relid: [] for relid in relids}
        for relid, name, type_oid in self.con.execute(
            "SELECT attrelid::bigint, attname, atttypid::bigint FROM pg_attribute "
            "WHERE attrelid = ANY(%s::oid[]) AND attnum > 0 AND NOT attisdropped "
            "ORDER BY attrelid, attnum",
            [relids],
        ).fetchall():
            result.setdefault(int(relid), []).append((str(name), int(type_oid)))
        return result


@dataclass
class RelationDescriptorProvider:
    """A memoized descriptor map for a bounded set of source relations.

    Re-snapshot intentionally has no live :class:`CatalogWatcher`: it must not
    discover or apply DDL while building a replacement image.  It still needs the
    same source type authority as the streaming path, so this provider performs one
    relation-column projection and delegates all recursive type work to the shared
    ``CatalogDescriptorReader`` above.
    """

    relations: dict[str, dict[str, SourceTypeDescriptor]]
    source_dsn: str | None = None
    relation_generations: dict[str, str] = field(default_factory=dict)
    _event_read_lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _event_read_conn: object | None = field(default=None, init=False, repr=False)
    #: Applier-owned mandatory policy attachment.  Keeping it on the provider
    #: object (rather than a bound ``descriptors_for`` method) makes the resnapshot
    #: acquisition seam inspectable and prevents an immutable-method assignment
    #: from being silently ignored.
    policy_gate: object | None = field(default=None, init=False, repr=False)

    @classmethod
    def from_tables(
        cls,
        con,
        tables: Iterable[tuple[str, str, str]],
        *,
        source_dsn: str | None = None,
    ) -> RelationDescriptorProvider:
        # psycopg intentionally removes the password from ``ConnectionInfo.dsn``.
        # Callers that already own the configured source DSN must pass it through so
        # a later opaque-array recovery connection does not silently lose credentials.
        source_dsn = source_dsn or getattr(getattr(con, "info", None), "dsn", None)
        requested = sorted(
            {(str(schema), str(table), str(target)) for schema, table, target in tables}
        )
        if not requested:
            return cls({}, source_dsn=source_dsn)
        predicates = " OR ".join(
            "(n.nspname = %s AND c.relname = %s)" for _ in requested
        )
        params = [value for schema, table, _target in requested for value in (schema, table)]
        rows = con.execute(
            "SELECT n.nspname, c.relname, a.attname, a.atttypid::bigint, "
            "a.atttypmod, format_type(a.atttypid, a.atttypmod) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            f"WHERE c.relkind IN ('r', 'p') AND ({predicates}) "
            "ORDER BY n.nspname, c.relname, a.attnum",
            params,
        ).fetchall()
        oids = {int(row[3]) for row in rows}
        reader = CatalogDescriptorReader(con)
        try:
            descriptors = reader.resolve(oids)
        except AdmissionError as exc:
            source_tables = tuple(
                (str(schema), str(table), str(target))
                for schema, table, target in requested
            )
            scoped = source_tables[0] if len(source_tables) == 1 else None
            raise SchemaEvolutionRefused(
                f"catalog descriptor authority failed for {len(source_tables)} "
                f"source relation(s): {exc}",
                source_schema=(scoped[0] if scoped else None),
                source_table=(scoped[1] if scoped else None),
                target=(scoped[2] if scoped else None),
                refusal_origin="catalog_descriptor",
                source_tables=source_tables,
            ) from exc
        missing = sorted(oids - set(descriptors))
        if missing:
            scoped = requested[0] if len(requested) == 1 else None
            raise SchemaEvolutionRefused(
                "catalog descriptor authority is incomplete for OID(s) "
                + ", ".join(str(oid) for oid in missing),
                source_schema=(str(scoped[0]) if scoped else None),
                source_table=(str(scoped[1]) if scoped else None),
                target=(str(scoped[2]) if scoped else None),
                refusal_origin="catalog_descriptor",
                source_tables=tuple(
                    (str(schema), str(table), str(target))
                    for schema, table, target in requested
                ),
            )
        generations: dict[str, str] = {}
        try:
            generation_rows = con.execute(
                "SELECT n.nspname, c.relname, c.oid::bigint, "
                "c.relfilenode::bigint, c.reltype::bigint "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE ({' OR '.join('(n.nspname = %s AND c.relname = %s)' for _ in requested)})",
                params,
            ).fetchall()
        except Exception:
            generation_rows = ()
        for row in generation_rows:
            if len(row) != 5:
                continue
            schema, table, oid, filenode, type_oid = row
            generations[f"{schema}.{table}"] = (
                f"{int(oid)}:{'' if filenode is None else int(filenode)}:"
                f"{'' if type_oid is None else int(type_oid)}"
            )
        result: dict[str, dict[str, SourceTypeDescriptor]] = {}
        for schema, table, name, oid, typmod, formatted in rows:
            oid = int(oid)
            descriptor = descriptors[oid]
            descriptor = _column_facts(descriptor, str(formatted), typmod)
            try:
                # Resolving the complete tree is the authority check.  An empty
                # composite, missing array child, incomplete map, or unsupported
                # descendant must not be converted into a guessed VARCHAR.
                native_type(descriptor)
            except (AdmissionError, ValueError) as exc:
                raise SchemaEvolutionRefused(
                    f"catalog descriptor authority is incomplete for {schema}.{table}.{name}",
                    source_schema=str(schema),
                    source_table=str(table),
                    target=f"{schema}.{table}",
                    refusal_origin="catalog_descriptor",
                ) from exc
            result.setdefault(f"{schema}.{table}", {})[normalize(str(name))] = descriptor
        return cls(
            result,
            source_dsn=source_dsn,
            relation_generations=generations,
        )

    def descriptors_for(self, qualified: str) -> dict[str, SourceTypeDescriptor]:
        return dict(self.relations.get(str(qualified), {}))

    def __call__(self, qualified: str) -> dict[str, SourceTypeDescriptor]:
        """Remain callable for planners while retaining provider ownership."""
        return self.descriptors_for(qualified)

    def relation_generation_for(self, qualified: str) -> str | None:
        return self.relation_generations.get(str(qualified))

    def read_event_columns(self, event, value_columns):
        """Recover omitted opaque-array fields through one bounded source session."""
        policy_gate = self.policy_gate
        if policy_gate is None:
            raise SchemaEvolutionRefused(
                "the bounded descriptor provider has no attached policy gate for "
                f"opaque-array recovery of {event.qualified_table}",
                source_schema=event.schema,
                source_table=event.table,
                target=event.qualified_table,
                refusal_origin="catalog_descriptor",
            )
        if not self.source_dsn:
            raise SchemaEvolutionRefused(
                "the bounded descriptor provider has no source connection for "
                f"opaque-array recovery of {event.qualified_table}",
                source_schema=event.schema,
                source_table=event.table,
                target=event.qualified_table,
                refusal_origin="catalog_descriptor",
            )
        import psycopg

        from . import catalog_support

        with self._event_read_lock:
            con = self._event_read_conn
            if con is None or con.closed:
                con = psycopg.connect(
                    self.source_dsn,
                    autocommit=True,
                    **source_connection_kwargs(),
                )
                self._event_read_conn = con
            try:
                return catalog_support.read_event_columns_from_connection(
                    con,
                    event,
                    value_columns,
                    policy_gate=policy_gate,
                    descriptors=self.relations.get(event.qualified_table, {}),
                )
            except Exception:
                con.close()
                self._event_read_conn = None
                raise

    def close(self) -> None:
        """Release the bounded opaque-array recovery connection."""
        with self._event_read_lock:
            con = self._event_read_conn
            self._event_read_conn = None
            if con is not None:
                con.close()


def relation_descriptor_fingerprint(
    relation_oid: int,
    columns: Iterable[tuple[str, int, int, int | None, str, SourceTypeDescriptor]],
) -> str:
    """Hash the source relation facts used by the quarantine retry gate.

    The relation OID catches drop/recreate.  Column names, order, type OIDs, typmods,
    formatted names, and the complete recursively resolved descriptor catch source
    schema/type changes.  It intentionally does not inspect row values: a changing
    bad row image is not permission to retry an unchanged schema forever.
    """
    payload = {
        "relation_oid": int(relation_oid),
        "columns": [
            {
                "name": str(name),
                "attnum": int(attnum),
                "type_oid": int(type_oid),
                "typmod": int(typmod) if typmod is not None else None,
                "formatted": str(formatted),
                "descriptor": descriptor.fingerprint,
            }
            for name, attnum, type_oid, typmod, formatted, descriptor in columns
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_relation_fingerprint(
    dsn: str, source_schema: str, source_table: str
) -> tuple[bool, str | None]:
    """Read one current source relation for the quarantine reactivation decision.

    ``(False, None)`` is positive source-absence evidence.  A read/authority error
    returns ``(True, None)`` so an uncertain source state cannot reactivate a stale
    table; the next ordinary catalog poll remains the repair trigger.
    """
    import psycopg

    try:
        with psycopg.connect(
            dsn,
            autocommit=True,
            **source_connection_kwargs(),
        ) as con:
            rows = con.execute(
                "SELECT c.oid::bigint, a.attname, a.attnum, a.atttypid::bigint, "
                "a.atttypmod, format_type(a.atttypid, a.atttypmod) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_attribute a ON a.attrelid = c.oid "
                "AND a.attnum > 0 AND NOT a.attisdropped "
                "WHERE n.nspname = %s AND c.relname = %s "
                "AND c.relkind IN ('r', 'p', 'f', 'm') "
                "ORDER BY a.attnum",
                [source_schema, source_table],
            ).fetchall()
            if not rows:
                exists = con.execute(
                    "SELECT 1 FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace WHERE n.nspname = %s "
                    "AND c.relname = %s AND c.relkind IN ('r', 'p', 'f', 'm')",
                    [source_schema, source_table],
                ).fetchone()
                return bool(exists), None
            reader = CatalogDescriptorReader(con)
            descriptors = reader.resolve({int(row[3]) for row in rows})
            facts = []
            for _relation_oid, name, attnum, type_oid, typmod, formatted in rows:
                descriptor = _column_facts(
                    descriptors[int(type_oid)], str(formatted), typmod
                )
                facts.append(
                    (str(name), int(attnum), int(type_oid), typmod, str(formatted), descriptor)
                )
            return True, relation_descriptor_fingerprint(int(rows[0][0]), facts)
    except Exception:
        return True, None


def provider_for_source(source, *, routes=None) -> object:
    """Build the bounded no-catalog provider from the source catalog once."""
    requested: list[tuple[str, str, str]] = []
    for qualified in source.tables:
        schema, separator, table = str(qualified).partition(".")
        if not separator or not schema or not table:
            raise ValueError(
                f"configured source table {qualified!r} is not qualified; "
                "catalog descriptor authority cannot be established"
            )
        requested.append((schema, table, ""))
    import psycopg

    read_dsn = (routes or source.route_policy).read_dsn

    with psycopg.connect(
        read_dsn,
        autocommit=True,
        **source_connection_kwargs(),
    ) as descriptor_con:
        return RelationDescriptorProvider.from_tables(
            descriptor_con, requested, source_dsn=read_dsn
        )


def _column_facts(
    descriptor: SourceTypeDescriptor, formatted: str, typmod: int | None
) -> SourceTypeDescriptor:
    """Apply column typmod/precision facts to an OID-resolved descriptor tree."""

    result = replace(descriptor, typmod=int(typmod) if typmod is not None else descriptor.typmod)
    text = formatted.strip()
    lowered = text.lower()
    if result.kind in {"numeric", "decimal"} and lowered.startswith(("numeric(", "decimal(")):
        parsed = descriptor_from_type_name(
            text, oid=result.oid, typmod=result.typmod, nullable=result.nullable
        )
        result = replace(result, precision=parsed.precision, scale=parsed.scale)
    elif result.kind == "array" and result.array_element is not None and lowered.endswith("[]"):
        result = replace(
            result,
            array_element=_column_facts(result.array_element, text[:-2].strip(), None),
        )
    return result


def _kind_for_fact(fact: dict) -> str:
    if fact["typtype"] == "d":
        return "domain"
    if fact["typtype"] == "e":
        return "enum"
    if fact["typtype"] == "c":
        return "composite"
    if fact["typtype"] == "r":
        return "range"
    if fact["typtype"] == "m":
        return "multirange"
    if fact["name"] == "hstore":
        return "map"
    if fact["element"] and fact["category"] == "A":
        return "array"
    return fact["name"]


__all__ = [
    "CatalogDescriptorReader",
    "RelationDescriptorProvider",
    "provider_for_source",
    "relation_descriptor_fingerprint",
    "source_relation_fingerprint",
]
