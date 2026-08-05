"""Source-side generation proof acquisition.

This is the I/O half of generation proofing.  The pure token classifier remains in
``catalog_generation``; this module owns the bounded PostgreSQL query and the
short-lived source lock held through the destination commit boundary.
"""

from __future__ import annotations

from dataclasses import replace

from . import catalog_generation, catalog_observation, catalog_poll
from .naming import quote


def read_rows(watcher, conn, names):
    schemas = [schema for schema, _ in names]
    tables = [table for _, table in names]
    rows = conn.execute(
        catalog_observation.OID_SQL, (schemas, tables)
    ).fetchall()
    found = {
        f"{schema}.{table}": catalog_generation.RelationIdentity(
            int(oid),
            int(relfilenode) if relfilenode is not None else None,
            int(reltype_oid) if reltype_oid is not None else None,
        )
        for schema, table, oid, relfilenode, reltype_oid in rows
    }
    return {
        f"{schema}.{table}": found.get(f"{schema}.{table}")
        for schema, table in names
    }


def relation_oids(watcher, names):
    if not watcher.dsn:
        raise ValueError("this watcher has no DSN, so the source cannot be re-read")
    with watcher._connect() as conn:
        return read_rows(watcher, conn, names)


def _legacy_lease(watcher, names):
    try:
        values = watcher.relation_oids(names)
    except Exception as exc:
        return catalog_generation.GenerationProofLease(
            {
                f"{schema}.{table}": catalog_generation.GenerationProof.unknown(
                    str(exc)
                )
                for schema, table in names
            }
        )
    proofs = {}
    for schema, table in names:
        name = f"{schema}.{table}"
        raw = values.get(name)
        proof = catalog_generation.coerce_proof(raw)
        # Explicit GenerationProof values are useful test/embedding seams and retain
        # their declared boundary state. Bare values have no source lock or WAL LSN.
        proofs[name] = (
            proof
            if isinstance(raw, catalog_generation.GenerationProof)
            else replace(proof, legacy=True)
        )
    return catalog_generation.GenerationProofLease(proofs)


def acquire(watcher, names):
    names = set(names)
    if not names:
        return catalog_generation.GenerationProofLease({})
    if not watcher.dsn:
        return _legacy_lease(watcher, names)

    conn = None
    try:
        conn = catalog_poll.connect(watcher, autocommit=False)
        conn.execute("BEGIN TRANSACTION")
        initial = read_rows(watcher, conn, names)
        for schema, table in sorted(names):
            if initial.get(f"{schema}.{table}") is None:
                continue
            conn.execute(
                f"LOCK TABLE {quote(schema)}.{quote(table)} IN ACCESS SHARE MODE"
            )
        final = read_rows(watcher, conn, names)
        source_lsn = int(
            conn.execute(catalog_observation.LSN_SQL).fetchone()[0]
        )
        proofs = {
            name: catalog_generation.GenerationProof(
                identity=catalog_generation.coerce_identity(value),
                source_lsn=source_lsn,
            )
            for name, value in final.items()
        }

        def release() -> None:
            try:
                conn.commit()
            finally:
                conn.close()

        return catalog_generation.GenerationProofLease(proofs, release)
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            finally:
                conn.close()
        return catalog_generation.GenerationProofLease(
            {
                f"{schema}.{table}": catalog_generation.GenerationProof.unknown(
                    f"could not establish source generation proof: {exc}"
                )
                for schema, table in names
            }
        )
