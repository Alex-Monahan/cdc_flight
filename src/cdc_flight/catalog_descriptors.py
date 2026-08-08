"""Memoized PostgreSQL type-descriptor tree reader.

The relation observation remains one batch query.  This reader follows the distinct
type OIDs from that batch with a handful of targeted catalog queries and memoizes the
result for later polls.  It deliberately does not use a recursive SQL CTE or issue a
query per column/type.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace

from .naming import normalize
from .schema_evolution import descriptor_from_type_name
from .typed_types import SourceTypeDescriptor


@dataclass
class CatalogDescriptorReader:
    con: object
    cache: dict[int, SourceTypeDescriptor] = field(default_factory=dict)

    def resolve(self, oids: Iterable[int]) -> dict[int, SourceTypeDescriptor]:
        wanted = {int(oid) for oid in oids if oid}
        missing = wanted - set(self.cache)
        facts: dict[int, dict] = {}
        while missing:
            rows = self.con.execute(
                "SELECT t.oid::bigint, n.nspname, t.typname, t.typtype, "
                "t.typcategory, t.typbasetype::bigint, t.typelem::bigint, "
                "t.typrelid::bigint, COALESCE(r.rngsubtype, 0)::bigint, "
                "COALESCE(r.rngmultitypid, 0)::bigint "
                "FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace "
                "LEFT JOIN pg_range r ON r.rngtypid=t.oid "
                "WHERE t.oid = ANY(%s::oid[])",
                [sorted(missing)],
            ).fetchall()
            for row in rows:
                (
                    oid, schema, name, typtype, category, base, element, relid,
                    subtype, multirange_oid,
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
                }
            children = set()
            for fact in facts.values():
                children.update(item for item in (fact["base"], fact["element"], fact["subtype"]) if item)
                if fact["typtype"] == "m" and fact["multirange_oid"]:
                    children.add(fact["multirange_oid"])
            missing = children - set(self.cache) - set(facts)
        enum_labels = self._enum_labels([fact["oid"] for fact in facts.values() if fact["typtype"] == "e"])
        composite_fields = self._composite_fields(
            [fact["relid"] for fact in facts.values() if fact["typtype"] == "c" and fact["relid"]]
        )
        child_oids = {
            child
            for fields in composite_fields.values()
            for _, child in fields
        }
        child_oids -= set(self.cache) | set(facts)
        if child_oids:
            self.resolve(child_oids)
        building: set[int] = set()

        def build(oid: int) -> SourceTypeDescriptor:
            if oid in self.cache:
                return self.cache[oid]
            if oid in building:
                raise ValueError(f"recursive PostgreSQL type descriptor at OID {oid}")
            fact = facts.get(oid)
            if fact is None:
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
            )
            building.remove(oid)
            self.cache[oid] = descriptor
            return descriptor

        for oid in sorted(wanted):
            if oid not in self.cache:
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

    @classmethod
    def from_tables(cls, con, tables: Iterable[tuple[str, str, str]]) -> RelationDescriptorProvider:
        requested = sorted({(str(schema), str(table)) for schema, table, _target in tables})
        if not requested:
            return cls({})
        predicates = " OR ".join("(n.nspname = %s AND c.relname = %s)" for _ in requested)
        params = [value for pair in requested for value in pair]
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
        descriptors = reader.resolve(oids)
        result: dict[str, dict[str, SourceTypeDescriptor]] = {}
        for schema, table, name, oid, typmod, formatted in rows:
            oid = int(oid)
            descriptor = descriptors.get(oid) or descriptor_from_type_name(
                str(formatted), oid=oid, typmod=int(typmod)
            )
            descriptor = _column_facts(descriptor, str(formatted), typmod)
            result.setdefault(f"{schema}.{table}", {})[normalize(str(name))] = descriptor
        return cls(result)

    def descriptors_for(self, qualified: str) -> dict[str, SourceTypeDescriptor]:
        return dict(self.relations.get(str(qualified), {}))


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


__all__ = ["CatalogDescriptorReader", "RelationDescriptorProvider"]
