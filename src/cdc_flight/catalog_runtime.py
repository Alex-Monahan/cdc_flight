"""Runtime catalog probes that are shared by the watcher and applier."""

from __future__ import annotations

from .errors import SchemaShapeUnexplained


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
        if fields - known_names:
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
