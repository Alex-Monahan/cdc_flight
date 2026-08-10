"""Bounded source-catalog polling coordinator."""

from __future__ import annotations

import json
import logging
from dataclasses import replace

from . import catalog_support as observation_mod
from . import faults as faults_mod
from .catalog_descriptors import CatalogDescriptorReader
from .catalog_generation import identities_equal, identity_for
from .catalog_state import FENCED, SourceRelation, _missing_value
from .errors import SchemaEvolutionRefused
from .machines import (
    CATALOG_SCHEMA_LIVENESS,
    SCHEMA_EMPTY,
    SCHEMA_ERROR,
    SCHEMA_UNAVAILABLE,
    SCHEMA_VISIBLE,
)
from .schema_evolution import SourceColumn, descriptor_from_type_name
from .states import IllegalTransition, UnknownState
from .toast import classify_relation
from .typed_types import SourceTypeDescriptor, UnsupportedType

log = logging.getLogger("cdc_flight.catalog_poll")


def _column_descriptor(raw: dict, descriptors: dict[int, SourceTypeDescriptor]) -> SourceTypeDescriptor:
    oid = int(raw["type_oid"])
    descriptor = descriptors.get(oid)
    if descriptor is None:
        raise SchemaEvolutionRefused(
            f"catalog descriptor authority is incomplete for source type OID {oid}; "
            "refusing to infer a type from its display name",
            refusal_origin="catalog_poll",
        )
    typmod = raw.get("typmod")
    if typmod is not None:
        descriptor = replace(descriptor, typmod=int(typmod))
    descriptor = _apply_formatted_precision(descriptor, str(raw["type_name"]))
    return descriptor


def _apply_formatted_precision(
    descriptor: SourceTypeDescriptor, formatted_name: str
) -> SourceTypeDescriptor:
    """Recover column/array numeric precision from PostgreSQL's formatted type."""
    text = formatted_name.strip()
    if descriptor.kind in {"numeric", "decimal"} and text.lower().startswith(
        ("numeric(", "decimal(")
    ):
        parsed = descriptor_from_type_name(
            text,
            oid=descriptor.oid,
            typmod=descriptor.typmod,
            nullable=descriptor.nullable,
        )
        return replace(descriptor, precision=parsed.precision, scale=parsed.scale)
    if descriptor.kind == "array" and descriptor.array_element is not None and text.endswith(
        "[]"
    ):
        element_text = text[:-2].strip()
        element = _apply_formatted_precision(descriptor.array_element, element_text)
        return replace(descriptor, array_element=element)
    return descriptor


def connect(watcher, *, autocommit: bool = True, dsn: str | None = None):
    import psycopg

    return psycopg.connect(
        dsn or watcher.dsn,
        autocommit=autocommit,
        connect_timeout=watcher.connect_timeout,
        options=f"-c statement_timeout={watcher.query_timeout_ms}",
        keepalives=1,
        keepalives_idle=1,
        keepalives_interval=1,
        keepalives_count=2,
        tcp_user_timeout=watcher.query_timeout_ms,
    )


def _positive_lsn(value):
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return None
    return candidate if candidate > 0 else None


def _closed_full_relation(relation, activation_lsn, invalidation_lsn):
    """Close one FULL evidence interval without manufacturing an open one."""
    activation = _positive_lsn(activation_lsn)
    if activation is None:
        return replace(
            relation,
            full_activation_lsn=None,
            full_invalidation_lsn=None,
        )
    invalidation = _positive_lsn(invalidation_lsn)
    # If an external downgrade is observed without an event LSN, the only safe
    # point we can name from this observation is the first LSN after activation.
    # That is deliberately conservative: it routes the rest through refetch rather
    # than admitting an event whose generation cannot be proven.
    if invalidation is None or invalidation <= activation:
        invalidation = activation + 1
    return replace(
        relation,
        full_activation_lsn=activation,
        full_invalidation_lsn=invalidation,
    )


def _post_commit_full_state(conn, relation, boundary):
    """Re-check after COMMIT and close the interval if a racer downgraded it."""
    verified = conn.execute(
        "SELECT relreplident FROM pg_class WHERE oid = %s", [relation.oid]
    ).fetchone()
    if verified and str(verified[0]).lower() == "f":
        return replace(
            relation,
            replica_identity="f",
            full_activation_lsn=boundary,
            full_invalidation_lsn=None,
        )
    # The post-COMMIT check is deliberately not treated as an atomic boundary: a
    # second connection may have changed identity between COMMIT and this read, and
    # a WAL sample taken now could be after unrelated DML.  Close immediately after
    # activation and route every later event through the existing automatic
    # refetch/resnapshot path until a fresh FULL interval is established.
    return _closed_full_relation(
        replace(relation, replica_identity="d"), boundary, int(boundary) + 1
    )


def _ensure_toast_policies(
    watcher,
    conn,
    observed: dict[str, SourceRelation],
    *,
    activation_lsn: int | None = None,
):
    """Make residual TOAST tables FULL and bound each proven FULL interval.

    The relation lock proves the lower bound only while the activation transaction
    is open.  The explicit post-COMMIT recheck is a *closing* observation: if another
    connection downgrades the relation in the interval after COMMIT, the fresh WAL
    sample becomes the exclusive upper bound and the policy routes later events to
    fallback.  A downgrade seen by the next poll is closed conservatively at the
    first LSN after the old activation until an event-level LSN can provide a tighter
    boundary; it is never treated as an open FULL interval.
    """
    updated = dict(observed)
    for qualified, relation in observed.items():
        previous = getattr(watcher, "known", {}).get(qualified)
        current_boundary = _positive_lsn(relation.full_activation_lsn)
        previous_boundary = _positive_lsn(
            getattr(previous, "full_activation_lsn", None)
        )
        previous_invalidation = _positive_lsn(
            getattr(previous, "full_invalidation_lsn", None)
        )
        previous_identity = identity_for(previous) if previous is not None else None
        current_identity = identity_for(relation)
        same_complete_generation = (
            previous_identity is not None
            and current_identity.complete
            and previous_identity.complete
            and identities_equal(current_identity, previous_identity)
        )
        current_full = str(relation.replica_identity).lower() == "f"

        # A source generation owns one evidence interval.  Never carry either end
        # across a DROP/CREATE or an incomplete identity token.
        if not same_complete_generation:
            relation = replace(
                relation,
                full_activation_lsn=None,
                full_invalidation_lsn=None,
            )
            current_boundary = None
            previous_boundary = None
            previous_invalidation = None
        elif current_full:
            # A previously closed interval followed by an external re-enable needs a
            # fresh lower bound; the next activation transaction establishes it.
            if previous_invalidation is not None:
                relation = replace(
                    relation,
                    full_activation_lsn=None,
                    full_invalidation_lsn=None,
                )
                current_boundary = None
            elif current_boundary is None and previous_boundary is not None:
                relation = replace(
                    relation,
                    full_activation_lsn=previous_boundary,
                    full_invalidation_lsn=None,
                )
                current_boundary = previous_boundary
        elif previous_boundary is not None and previous_invalidation is None:
            # The next poll has observed the downgrade.  Close the old interval and
            # stop here; a later poll may establish a new FULL interval, but no event
            # in this gap is admitted on the strength of the old lower bound.
            relation = _closed_full_relation(
                relation, previous_boundary, previous_boundary + 1
            )
            updated[qualified] = relation
            continue
        else:
            relation = replace(
                relation,
                full_activation_lsn=None,
                full_invalidation_lsn=None,
            )
            current_boundary = None

        updated[qualified] = relation
        policy = classify_relation(
            qualified,
            relation.columns,
            replica_identity=relation.replica_identity,
            binary_mode=watcher.binary_handling_mode,
            hstore_mode=watcher.hstore_handling_mode,
            full_activation_lsn=relation.full_activation_lsn,
            full_invalidation_lsn=relation.full_invalidation_lsn,
        )
        if not policy.residual_columns:
            continue
        try:
            schema, _, table = qualified.partition(".")
            if not schema or not table:
                raise ValueError(f"unqualified source relation {qualified!r}")
            from .naming import quote

            needs_activation = not (
                str(relation.replica_identity).lower() == "f"
                and _positive_lsn(relation.full_activation_lsn) is not None
            )
            lock_mode = "ACCESS EXCLUSIVE" if needs_activation else "ACCESS SHARE"
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                f"LOCK TABLE {quote(schema)}.{quote(table)} "
                f"IN {lock_mode} MODE NOWAIT"
            )
            if not needs_activation:
                locked_identity = conn.execute(
                    "SELECT relreplident FROM pg_class WHERE oid = %s", [relation.oid]
                ).fetchone()
                if not locked_identity or str(locked_identity[0]).lower() != "f":
                    raise RuntimeError(
                        "source replica identity changed before the held-lock admission"
                    )
                conn.execute("COMMIT")
                updated[qualified] = _post_commit_full_state(
                    conn, relation, _positive_lsn(relation.full_activation_lsn)
                )
                continue

            conn.execute(
                f"ALTER TABLE {quote(schema)}.{quote(table)} "
                "REPLICA IDENTITY FULL"
            )
            verified = conn.execute(
                "SELECT relreplident FROM pg_class WHERE oid = %s", [relation.oid]
            ).fetchone()
            if not verified or str(verified[0]).lower() != "f":
                raise RuntimeError(
                    f"source reported replica identity {verified[0] if verified else None!r}"
                )
            post_alter = conn.execute(observation_mod.ACTIVATION_LSN_SQL).fetchone()
            boundary = _positive_lsn(post_alter[0] if post_alter else None)
            pre_alter = _positive_lsn(activation_lsn)
            if boundary is None or (pre_alter is not None and boundary <= pre_alter):
                raise RuntimeError(
                    f"post-ALTER WAL boundary {boundary!r} did not prove it follows "
                    f"the pre-ALTER sample {pre_alter!r}"
                )
            verified_after_sample = conn.execute(
                "SELECT relreplident FROM pg_class WHERE oid = %s", [relation.oid]
            ).fetchone()
            if (
                not verified_after_sample
                or str(verified_after_sample[0]).lower() != "f"
            ):
                raise RuntimeError(
                    "source replica identity changed while sampling activation WAL; "
                    "discarding the boundary and requiring refetch"
                )
            conn.execute("COMMIT")
            updated[qualified] = _post_commit_full_state(
                conn,
                replace(
                    relation,
                    replica_identity="f",
                    full_activation_lsn=boundary,
                    full_invalidation_lsn=None,
                ),
                boundary,
            )
            if updated[qualified].full_invalidation_lsn is None:
                log.info(
                    "TOAST residual table %s admitted with verified REPLICA IDENTITY "
                    "FULL: %s",
                    qualified,
                    ", ".join(policy.residual_columns),
                )
            else:
                log.warning(
                    "TOAST residual table %s lost FULL immediately after COMMIT; "
                    "closed validity interval at LSN %s",
                    qualified,
                    updated[qualified].full_invalidation_lsn,
                )
        except Exception as exc:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                log.debug("could not roll back source activation transaction", exc_info=True)
            log.warning(
                "could not establish REPLICA IDENTITY FULL for residual TOAST table %s; "
                "events will take automatic refetch/resnapshot recovery: %s",
                qualified,
                exc,
            )
            updated[qualified] = replace(
                relation,
                full_activation_lsn=None,
                full_invalidation_lsn=None,
            )
    return updated


def poll_quietly(watcher):
    try:
        # Keep the watcher method as the seam for tests and embedders that need to
        # substitute one observation. The method itself delegates back here in the
        # normal implementation.
        return watcher.poll()
    except (IllegalTransition, UnknownState) as illegal:
        _mark_liveness_error(watcher)
        watcher.machine_error = f"{type(illegal).__name__}: {illegal}"
        log.critical(
            "catalog state machine violated during a poll; this run cannot be "
            "reported successful: %s",
            watcher.machine_error,
        )
        return []
    except Exception as exc:  # pragma: no cover - exercised through the thread
        if isinstance(exc, SchemaEvolutionRefused) and hasattr(
            watcher, "remember_schema_refusal"
        ):
            watcher.remember_schema_refusal(exc)
        _mark_liveness_error(watcher)
        watcher.last_error = f"{type(exc).__name__}: {exc}"
        log.warning("catalog poll failed: %s", watcher.last_error)
        return []


def _mark_liveness_error(watcher) -> None:
    """Keep source-query failure in the declared per-schema liveness machine."""
    with watcher._lock:
        names = set(watcher.schemas) or {
            qualified.partition(".")[0] for qualified in watcher.known
        }
        for name in names:
            watcher._schema_liveness[name] = CATALOG_SCHEMA_LIVENESS.parse(SCHEMA_ERROR)


def poll(watcher):
    faults_mod.maybe_fail_repeatedly("catalog_poll")
    with connect(watcher) as conn:
        schema_array = None if watcher.all_schemas else sorted(watcher.schemas)
        rows = conn.execute(
            observation_mod.CATALOG_SQL,
            (watcher.publication, schema_array, schema_array),
        ).fetchall()
        liveness_rows = conn.execute(
            observation_mod.SCHEMA_LIVENESS_SQL,
            (schema_array, schema_array),
        ).fetchall()
        observed_liveness = {
            str(schema): (SCHEMA_VISIBLE if int(count) > 0 else SCHEMA_EMPTY)
            for schema, count in liveness_rows
        }
        expected_schemas = set(watcher.schemas)
        if watcher.all_schemas:
            expected_schemas |= {name.partition(".")[0] for name in watcher.known}
        for name in expected_schemas:
            observed_liveness.setdefault(name, SCHEMA_UNAVAILABLE)
        for state in observed_liveness.values():
            CATALOG_SCHEMA_LIVENESS.parse(state)
        with watcher._lock:
            watcher._schema_liveness = observed_liveness
        partition_rows = conn.execute(
            observation_mod.PARTITION_SQL,
            (watcher.publication, schema_array, schema_array),
        ).fetchall()
        lsn = int(conn.execute(observation_mod.LSN_SQL).fetchone()[0])
        observed: dict[str, SourceRelation] = {}
        descriptor_reader = CatalogDescriptorReader(
            conn, cache=getattr(watcher, "_descriptor_cache", {})
        )
        watcher._descriptor_cache = descriptor_reader.cache
        parsed_columns: dict[int, list[dict]] = {}
        all_type_oids: set[int] = set()
        for index, row in enumerate(rows):
            raw_columns = row[9] if len(row) > 9 else []
            if isinstance(raw_columns, str):
                try:
                    raw_columns = json.loads(raw_columns)
                except (TypeError, ValueError):
                    raw_columns = []
            if not isinstance(raw_columns, list):
                raw_columns = []
            parsed_columns[index] = [raw for raw in raw_columns if isinstance(raw, dict)]
            all_type_oids.update(
                int(raw["type_oid"])
                for raw in parsed_columns[index]
                if raw.get("type_oid")
            )
        try:
            descriptors = descriptor_reader.resolve(all_type_oids)
        except (SchemaEvolutionRefused, UnsupportedType, ValueError, KeyError) as exc:
            source_tables = tuple(dict.fromkeys(
                (str(row[0]), str(row[1]), f"{row[0]}.{row[1]}")
                for row in rows
                if len(row) >= 2
            ))
            explicit_schema = getattr(exc, "source_schema", None)
            explicit_table = getattr(exc, "source_table", None)
            if explicit_schema and explicit_table:
                source_schema = str(explicit_schema)
                source_table = str(explicit_table)
                target = getattr(exc, "target", None) or f"{source_schema}.{source_table}"
            elif len(source_tables) == 1:
                source_schema, source_table, target = source_tables[0]
            else:
                # A batch failure is not evidence that the first row caused it.
                # Keep the complete affected-relation set for the durable router,
                # but leave the wrapper itself unscoped so no healthy table is
                # guessed as the origin.
                source_schema = source_table = target = None
            raise SchemaEvolutionRefused(
                f"source catalog descriptor authority failed for {target}: {exc}",
                source_schema=source_schema,
                source_table=source_table,
                target=target,
                detected_lsn=lsn,
                refusal_origin="catalog_poll",
                source_tables=source_tables,
            ) from exc
        for index, row in enumerate(rows):
            raw_columns = parsed_columns[index]
            columns = tuple(
                SourceColumn(
                    attnum=int(raw["attnum"]),
                    name=str(raw["name"]),
                    type_oid=int(raw["type_oid"]),
                    type_name=str(raw["type_name"]),
                    typmod=(int(raw["typmod"]) if raw.get("typmod") is not None else None),
                    nullable=bool(raw.get("nullable", True)),
                    has_missing_default=bool(raw.get("has_missing_default", False)),
                    missing_value=_missing_value(
                        raw.get("missing_value_text"), str(raw["type_name"])
                    ),
                    attstorage=(str(raw["attstorage"]) if raw.get("attstorage") else None),
                    descriptor=(
                        SourceTypeDescriptor.from_dict(raw["descriptor"])
                        if raw.get("descriptor")
                        else _column_descriptor(raw, descriptors)
                    ),
                )
                for raw in (raw_columns or [])
            )
            # Catalog observation records the complete source shape even when a
            # descriptor has no native destination representation.  Admission owns
            # the refusal: preserving the column list prevents a later row from
            # becoming an unscoped ``SchemaShapeUnexplained`` failure and lets the
            # refusal be quarantined table-by-table while healthy co-published tables
            # continue.  Descriptor authority is still strict in ``CatalogWatcher`` /
            # planner before any value is admitted.
            observed[f"{row[0]}.{row[1]}"] = SourceRelation(
                schema=row[0],
                table=row[1],
                oid=int(row[2]),
                relfilenode=(int(row[3]) if row[3] is not None else None),
                relation_type_oid=(int(row[4]) if row[4] is not None else None),
                replica_identity=str(row[5]),
                published=bool(row[6]),
                columns=columns,
                publication_all_tables=bool(row[7]) if len(row) > 7 else False,
                is_partition=bool(row[8]) if len(row) > 8 else False,
            )
        if not observed:
            with watcher._lock:
                watcher.polls += 1
                watcher.empty_polls += 1
                watcher.last_lsn = lsn
            watcher.last_error = (
                f"the polled schema {watcher.schema!r} contains no tables at all; "
                "this observation was DISCARDED rather than read as a mass drop"
            )
            log.error("catalog poll: %s", watcher.last_error)
            return []
        with watcher._lock:
            watcher._snapshot_partitions = {
                f"{row[0]}.{row[1]}" for row in partition_rows
            }
    # Keep source writes off the catalog read connection.  In replica mode this
    # route is the primary; in a primary-only deployment it is the same DSN as the
    # read side.  The connection is opened only after the read transaction is closed.
    with connect(watcher, dsn=watcher.primary_dsn) as write_conn:
        # Reclassify this exact catalog epoch before `_compare` can admit its events.
        # A residual table is either verified FULL or remains explicitly on the
        # automatic refetch/resnapshot route; an unverified ALTER is never treated as
        # success.
        observed = _ensure_toast_policies(
            watcher, write_conn, observed, activation_lsn=lsn
        )
        added = watcher._compare(observed, lsn)
        watcher._ensure_published(write_conn, observed, added)
        with watcher._lock:
            watcher.successful_polls += 1
        unfenced = [change for change in watcher.pending() if change.kind in FENCED]
        if unfenced:
            watcher._emit_marker(
                write_conn,
                [change for change in added if change.kind in FENCED] or unfenced,
            )
    if watcher.marker.last_error is None and not watcher._admission_errors:
        watcher.last_error = None
    return added
