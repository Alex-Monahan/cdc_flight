"""Deterministic change generator for the cdc_flight source database.

Produces inserts, updates and deletes across every seeded table (including the
no-PK table, the TOAST table, and the partitioned table) so a CDC run has
something interesting to capture.

    cdc-datagen changes --scale 1 --seed 42
    cdc-datagen truncate-demo
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg

from .config import SourceConfig

STATUSES = ["pending", "paid", "shipped", "cancelled", "refunded"]
TAGS = ["vip", "beta", "new", "churn-risk", "enterprise", "self-serve"]


@dataclass
class ChangeReport:
    inserts: int = 0
    updates: int = 0
    deletes: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "total": self.inserts + self.updates + self.deletes,
        }


def connect(source: SourceConfig | None = None) -> psycopg.Connection:
    source = source or SourceConfig()
    return psycopg.connect(source.dsn, autocommit=False)


def _big_body(rng: random.Random, kb: int = 64) -> str:
    """Incompressible text, guaranteed to be pushed out of line into TOAST."""
    chunks = [hashlib.md5(str(rng.random()).encode()).hexdigest() for _ in range(kb * 32)]
    return "".join(chunks)


def generate_changes(conn: psycopg.Connection, *, scale: int = 1, seed: int = 42) -> ChangeReport:
    """One deterministic wave of changes. `scale` multiplies row counts."""
    rng = random.Random(seed)
    report = ChangeReport()
    now = datetime.now(UTC)

    with conn.cursor() as cur:
        # -- customers: insert, update, delete --------------------------------
        for i in range(3 * scale):
            cur.execute(
                """
                INSERT INTO app.customers (name, email, lifetime_value, is_active, prefs, tags)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id
                """,
                (
                    f"gen-customer-{seed}-{i}",
                    f"gen-{seed}-{i}@example.com",
                    round(rng.uniform(0, 5000), 4),
                    rng.random() > 0.2,
                    json.dumps({"tier": rng.choice(["bronze", "silver", "gold"]), "gen": True}),
                    rng.sample(TAGS, k=2),
                ),
            )
            report.inserts += 1

        cur.execute(
            """
            UPDATE app.customers
               SET lifetime_value = lifetime_value + 10.5,
                   updated_at = %s,
                   prefs = jsonb_set(prefs, '{touched}', 'true')
             WHERE id IN (SELECT id FROM app.customers ORDER BY id LIMIT %s)
            """,
            (now, 2 * scale),
        )
        report.updates += cur.rowcount

        cur.execute(
            """
            DELETE FROM app.customers
             WHERE email LIKE %s
               AND id IN (SELECT id FROM app.customers WHERE email LIKE %s ORDER BY id DESC LIMIT %s)
            """,
            (f"gen-{seed}-%", f"gen-{seed}-%", 1 * scale),
        )
        report.deletes += cur.rowcount

        # -- orders: insert + status transitions ------------------------------
        cur.execute("SELECT id FROM app.customers ORDER BY id LIMIT 5")
        customer_ids = [r[0] for r in cur.fetchall()]
        for i in range(4 * scale):
            cur.execute(
                """
                INSERT INTO app.orders
                       (customer_id, placed_at, status, total_amount, line_items, quantities, note)
                VALUES (%s, %s, %s::app.order_status, %s, %s::jsonb, %s, %s)
                """,
                (
                    rng.choice(customer_ids),
                    now - timedelta(minutes=i),
                    rng.choice(STATUSES),
                    round(rng.uniform(5, 2000), 2),
                    json.dumps([{"sku": f"SKU-{rng.randint(1, 99)}", "qty": rng.randint(1, 4)}]),
                    [rng.randint(1, 4) for _ in range(rng.randint(1, 3))],
                    None if rng.random() < 0.5 else f"generated note {i}",
                ),
            )
            report.inserts += 1

        cur.execute(
            """
            UPDATE app.orders SET status = 'shipped'
             WHERE id IN (SELECT id FROM app.orders WHERE status = 'paid' ORDER BY id LIMIT %s)
            """,
            (2 * scale,),
        )
        report.updates += cur.rowcount

        # -- sensor_readings: the no-PK table ---------------------------------
        for i in range(6 * scale):
            cur.execute(
                """
                INSERT INTO app.sensor_readings (sensor_id, reading_at, value, unit, meta)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    f"sensor-{rng.choice('abc')}",
                    now - timedelta(seconds=30 * i),
                    round(rng.uniform(-20, 120), 3),
                    rng.choice(["C", "%", "hPa"]),
                    json.dumps({"site": rng.choice(["hq", "warehouse"]), "gen": True}),
                ),
            )
            report.inserts += 1

        # UPDATE/DELETE on a table with no PK: only possible for logical
        # decoding because REPLICA IDENTITY is FULL.
        cur.execute(
            "UPDATE app.sensor_readings SET value = value + 1 WHERE sensor_id = 'sensor-a'"
        )
        report.updates += cur.rowcount
        cur.execute(
            "DELETE FROM app.sensor_readings WHERE ctid IN "
            "(SELECT ctid FROM app.sensor_readings ORDER BY reading_at DESC LIMIT %s)",
            (2 * scale,),
        )
        report.deletes += cur.rowcount

        # -- documents: TOAST insert, then an update that does NOT touch body --
        for i in range(1 * scale):
            body = _big_body(rng)
            cur.execute(
                """
                INSERT INTO app.documents (title, body, body_bytes, revision)
                VALUES (%s, %s, %s, 1)
                """,
                (f"gen-doc-{seed}-{i}", body, len(body)),
            )
            report.inserts += 1

        # Classic unchanged-TOAST case: only the metadata changes, so the WAL
        # carries `__debezium_unavailable_value` for `body`.
        cur.execute(
            "UPDATE app.documents SET revision = revision + 1, updated_at = %s "
            "WHERE id IN (SELECT id FROM app.documents ORDER BY id LIMIT %s)",
            (now, 2 * scale),
        )
        report.updates += cur.rowcount

        # -- audit_log: partitioned inserts across two partitions --------------
        for i in range(2 * scale):
            cur.execute(
                """
                INSERT INTO app.audit_log (occurred_at, actor, action, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                """,
                (
                    datetime(2026, 7, 15, tzinfo=UTC) + timedelta(hours=i),
                    f"gen-actor-{i}",
                    rng.choice(["login", "update", "delete"]),
                    json.dumps({"gen": True, "i": i}),
                ),
            )
            report.inserts += 1

        # -- wide_types: update the single pinned row ---------------------------
        cur.execute(
            "UPDATE app.wide_types SET col_integer = col_integer - 1, col_text = %s WHERE id = 1",
            (f"updated-{seed}",),
        )
        report.updates += cur.rowcount

    conn.commit()
    return report


def counts(conn: psycopg.Connection) -> dict[str, int]:
    tables = [
        "customers",
        "orders",
        "sensor_readings",
        "documents",
        "wide_types",
        "audit_log",
    ]
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT count(*) FROM app.{t}")
            out[t] = cur.fetchone()[0]
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-datagen", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_changes = sub.add_parser("changes", help="emit one deterministic wave of DML")
    p_changes.add_argument("--scale", type=int, default=1)
    p_changes.add_argument("--seed", type=int, default=42)
    p_changes.add_argument("--waves", type=int, default=1)

    sub.add_parser("counts", help="print current source row counts")

    args = parser.parse_args(argv)

    with connect() as conn:
        if args.command == "changes":
            totals = ChangeReport()
            for w in range(args.waves):
                r = generate_changes(conn, scale=args.scale, seed=args.seed + w)
                totals.inserts += r.inserts
                totals.updates += r.updates
                totals.deletes += r.deletes
            print(json.dumps(totals.as_dict(), indent=2))
        elif args.command == "counts":
            print(json.dumps(counts(conn), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
