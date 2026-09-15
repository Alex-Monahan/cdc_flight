"""Sustained source-TPS evidence harness for rubric 5.3.

This is an evidence probe, not a production path or a pytest test.  It has three
deliberate properties that the older throughput probes did not have:

* source throughput is derived from PostgreSQL-committed marker rows and the
  source database's own transaction counter, never from the requested pacing
  target;
* the primary arm starts the packaged ``cdc-flight-service`` and drives the
  normal SingleProcessFlight -> ServiceContext -> run_engine_bounded -> Applier
  path; and
* a repetition is not a keep-up result unless the source/destination oracle is
  exact and the **published-data-only** backlog is non-growing during a sustained
  source window.

Each measured product transaction inserts one application row in the existing
published ``app.customers`` relation and one marker row in a deliberately
unpublished measurement table.  Because the workload has exactly one published
customer row per transaction, the source customer-row count minus durable
destination customer-row count is a direct published-data backlog witness.  The
unpublished marker remains the independent source-TPS clock witness: its
``clock_timestamp()`` is PostgreSQL's clock, not the requested rate or a Python
event counter.  The unpublished marker's WAL is retained in the evidence but is
never part of the keep-up backlog.  PostgreSQL ``pg_stat_database.xact_commit`` is
used for the separate capability check.  Product destination delivery is reported
separately as durable destination rows per source-start-to-durable-boundary wall
second.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import sql as pg_sql

PROJECT_DIR = Path(__file__).resolve().parents[1]
SWARM_DIR = PROJECT_DIR.parent
DEFAULT_EVIDENCE_DIR = SWARM_DIR / "codex_logs" / "p53_runs"
SAMPLER = SWARM_DIR / "tools" / "contention_sampler.sh"
BASE_SHA = "ac73967181dc66f68a3ffe936eb985b788ab193f"
CANDIDATE_BRANCH = "feature/5-3-band-three"
SOURCE_PORT = 15432
SOURCE_DATABASE = "cdc_source"
SOURCE_ADMIN_DATABASE = "postgres"
SOURCE_PUBLICATION = "cdc_flight_pub"
MAX_GENERATOR_TPS = 500.0
DEFAULT_TARGET_TPS = 350.0
SOURCE_BAND_LOWER = 300.0
SOURCE_BAND_UPPER = 1000.0
DEFAULT_REPETITIONS = 5
DEFAULT_SUSTAINED_WINDOW_SECONDS = 180.0
DEFAULT_WORKERS = 8
SAMPLE_INTERVAL_SECONDS = 2.0


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _source_connect(database: str, *, application_name: str = "p53-harness"):
    return psycopg.connect(
        host="127.0.0.1",
        port=SOURCE_PORT,
        user=_env("PGUSER", "postgres"),
        password=_env("PGPASSWORD", "postgres"),
        dbname=database,
        application_name=application_name,
    )


def _duck_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _lsn_value(value: str | None) -> int | None:
    if value is None:
        return None
    high, low = str(value).split("/")
    return (int(high, 16) << 32) + int(low, 16)


def _json_default(value: Any) -> str:
    return str(value)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _database_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"[:63]


@contextmanager
def _scratch_postgres_database(prefix: str) -> Iterable[str]:
    name = _database_name(prefix)
    with _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-admin") as con:
        con.autocommit = True
        con.execute(pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(name)))
    try:
        yield name
    finally:
        with suppress(Exception), _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-admin") as con:
            con.autocommit = True
            con.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (name,),
            )
            con.execute(
                pg_sql.SQL("DROP DATABASE IF EXISTS {}").format(pg_sql.Identifier(name))
            )


def _create_marker_table(database: str, table: str) -> None:
    with _source_connect(database, application_name="p53-setup") as con:
        con.autocommit = True
        con.execute("CREATE SCHEMA IF NOT EXISTS p53_measurement")
        con.execute(
            pg_sql.SQL(
                "CREATE TABLE {}.{} ("
                "transaction_no bigint PRIMARY KEY, "
                "marker text NOT NULL, "
                "observed_at timestamptz NOT NULL DEFAULT clock_timestamp()"
                ")"
            ).format(pg_sql.Identifier("p53_measurement"), pg_sql.Identifier(table))
        )


def _create_published_marker_table(table: str, publication: str = SOURCE_PUBLICATION) -> None:
    """Create a per-run source marker relation and make it part of the publication.

    The table is included in ``CDC_TABLES`` by the service arm, so every generated
    transaction has a published marker record that can be counted at the durable
    destination.  It is removed from the publication before the table is dropped.
    """
    with _source_connect(SOURCE_DATABASE, application_name="p53-published-setup") as con:
        con.autocommit = True
        con.execute(
            pg_sql.SQL(
                "CREATE TABLE {}.{} ("
                "transaction_no bigint PRIMARY KEY, "
                "marker text NOT NULL, "
                "observed_at timestamptz NOT NULL DEFAULT clock_timestamp()"
                ")"
            ).format(pg_sql.Identifier("app"), pg_sql.Identifier(table))
        )
        con.execute(
            pg_sql.SQL("ALTER PUBLICATION {} ADD TABLE {}.{}").format(
                pg_sql.Identifier(publication),
                pg_sql.Identifier("app"),
                pg_sql.Identifier(table),
            )
        )


def _drop_published_marker_table(table: str, publication: str = SOURCE_PUBLICATION) -> None:
    """Remove the per-run marker from the publication and then drop its table."""
    with suppress(Exception), _source_connect(
        SOURCE_DATABASE, application_name="p53-published-cleanup"
    ) as con:
        con.autocommit = True
        member = con.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_publication_tables "
            "WHERE pubname = %s AND schemaname = %s AND tablename = %s)",
            (publication, "app", table),
        ).fetchone()[0]
        if member:
            # ALTER PUBLICATION DROP TABLE has no IF EXISTS variant; check the
            # live publication catalog first so cleanup remains idempotent.
            con.execute(
                pg_sql.SQL("ALTER PUBLICATION {} DROP TABLE {}.{}").format(
                    pg_sql.Identifier(publication),
                    pg_sql.Identifier("app"),
                    pg_sql.Identifier(table),
                )
            )
        con.execute(
            pg_sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                pg_sql.Identifier("app"), pg_sql.Identifier(table)
            )
        )


def _drop_marker_table(database: str, table: str) -> None:
    with suppress(Exception), _source_connect(database, application_name="p53-cleanup") as con:
        con.autocommit = True
        con.execute(
            pg_sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                pg_sql.Identifier("p53_measurement"), pg_sql.Identifier(table)
            )
        )


def _database_stats(database: str) -> dict[str, int]:
    with _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-stats") as con:
        con.autocommit = True
        with suppress(Exception):
            con.execute("SELECT pg_stat_force_next_flush()")
        row = con.execute(
            "SELECT xact_commit, tup_inserted FROM pg_stat_database WHERE datname = %s",
            (database,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"PostgreSQL did not expose pg_stat_database row for {database!r}")
    return {"xact_commit": int(row[0]), "tup_inserted": int(row[1])}


def _postgres_database_exists(database: str) -> bool:
    with _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-cleanup-check") as con:
        con.autocommit = True
        return bool(
            con.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (database,),
            ).fetchone()[0]
        )


def _source_relation_exists(
    schema: str, table: str, database: str = SOURCE_DATABASE
) -> bool:
    with _source_connect(database, application_name="p53-cleanup-check") as con:
        con.autocommit = True
        return bool(
            con.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s)",
                (schema, table),
            ).fetchone()[0]
        )


def _source_publication_contains_table(
    publication: str, schema: str, table: str
) -> bool:
    with _source_connect(SOURCE_DATABASE, application_name="p53-publication-check") as con:
        con.autocommit = True
        return bool(
            con.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_publication_tables "
                "WHERE pubname = %s AND schemaname = %s AND tablename = %s)",
                (publication, schema, table),
            ).fetchone()[0]
        )


def _published_data_facts(prefix: str) -> dict[str, Any]:
    """Count the generated rows in the already-published application table."""
    with _source_connect(SOURCE_DATABASE, application_name="p53-published-facts") as con:
        con.autocommit = True
        count = int(
            con.execute(
                "SELECT count(*) FROM app.customers WHERE name LIKE %s",
                (f"{prefix}-%",),
            ).fetchone()[0]
        )
    return {
        "committed_published_data_rows": count,
        "published_source_relation": "app.customers",
        "published_data_measurement": (
            "source committed app.customers rows for this run"
        ),
    }


def _marker_facts(database: str, table: str) -> dict[str, Any]:
    with _source_connect(database, application_name="p53-marker-facts") as con:
        con.autocommit = True
        row = con.execute(
            pg_sql.SQL(
                "SELECT count(*), min(observed_at), max(observed_at) "
                "FROM {}.{}"
            ).format(pg_sql.Identifier("p53_measurement"), pg_sql.Identifier(table))
        ).fetchone()
    count = int(row[0])
    first = row[1]
    last = row[2]
    span = (last - first).total_seconds() if first is not None and last is not None else 0.0
    return {
        "committed_transactions": count,
        "first_postgres_observed_at": first,
        "last_postgres_observed_at": last,
        "postgres_clock_span_sec": round(max(0.0, span), 6),
        "source_tps_from_postgres_clock": round(count / max(span, 0.001), 3),
    }


def _source_lsn(database: str = SOURCE_ADMIN_DATABASE) -> str:
    with _source_connect(database, application_name="p53-lsn") as con:
        con.autocommit = True
        return str(con.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])


def _slot_snapshot(slot: str) -> dict[str, Any]:
    with _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-slot") as con:
        con.autocommit = True
        row = con.execute(
            "SELECT pg_current_wal_lsn()::text, restart_lsn::text, "
            "confirmed_flush_lsn::text, active "
            "FROM pg_replication_slots WHERE slot_name = %s",
            (slot,),
        ).fetchone()
    if row is None:
        return {"exists": False, "slot": slot}
    current, restart, confirmed, active = row
    current_value = _lsn_value(current)
    restart_value = _lsn_value(restart)
    confirmed_value = _lsn_value(confirmed)
    return {
        "exists": True,
        "slot": slot,
        "active": bool(active),
        "current_lsn": current,
        "restart_lsn": restart,
        "confirmed_flush_lsn": confirmed,
        "lag_bytes": (
            max(0, current_value - restart_value)
            if current_value is not None and restart_value is not None
            else None
        ),
        "confirmed_lag_bytes": (
            max(0, current_value - confirmed_value)
            if current_value is not None and confirmed_value is not None
            else None
        ),
    }


class HostSampler:
    """Retain the contention sampler output and evaluate every sample."""

    def __init__(self, path: Path):
        self.path = path
        self.process: subprocess.Popen[str] | None = None

    def __enter__(self) -> HostSampler:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("w")
        self.process = subprocess.Popen(
            [str(SAMPLER), "1"],
            stdout=handle,
            stderr=subprocess.DEVNULL,
            text=True,
            cwd=SWARM_DIR,
        )
        # The sampler owns this descriptor after Popen; retaining the path is the
        # evidence contract, and closing our copy avoids a descriptor leak.
        handle.close()
        time.sleep(1.1)
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            with suppress(subprocess.TimeoutExpired):
                self.process.wait(timeout=5)
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)

    def verdict(self) -> dict[str, Any]:
        rows: list[dict[str, str]] = []
        if self.path.exists():
            with self.path.open(newline="") as stream:
                rows = list(csv.DictReader(stream))
        invalid: list[dict[str, Any]] = []
        for row in rows:
            try:
                avail = float(row["avail_mb"])
                wired = float(row["wired_mb"])
                swapins = float(row["swapins_d"])
            except (KeyError, TypeError, ValueError) as exc:
                invalid.append({"sample": row, "reason": f"unparseable gate fields: {exc}"})
                continue
            if not (avail > 1500 and wired < 4000 and swapins < 50):
                invalid.append(
                    {
                        "sample": row,
                        "reason": (
                            "host gate violation: require avail_mb > 1500, "
                            "wired_mb < 4000, swapins_d < 50"
                        ),
                    }
                )
        return {
            "csv": str(self.path),
            "sample_count": len(rows),
            "valid": bool(rows) and not invalid,
            "invalid_samples": invalid,
            "gate": "avail_mb > 1500 AND wired_mb < 4000 AND swapins_d < 50",
        }


def _pace_limit(advertised_target_tps: float) -> tuple[float, str | None]:
    if not math.isfinite(advertised_target_tps) or advertised_target_tps <= 0:
        raise ValueError("advertised target TPS must be positive and finite")
    if advertised_target_tps > MAX_GENERATOR_TPS:
        return MAX_GENERATOR_TPS, "requested rate is above the harness's 500 TPS cap"
    return advertised_target_tps, None


def _transactions_for_sustained_window(
    advertised_target_tps: float,
    duration_seconds: float,
    requested_transactions: int | None,
) -> int:
    """Choose enough commits to cover the required PostgreSQL-clock window.

    An explicit transaction count is retained for mutation probes and for a caller
    that wants a deliberately failing short run.  The normal score path leaves it
    unset, so the source window is derived from the requested pacing limit rather
    than from an arbitrary old burst size.
    """
    if not math.isfinite(duration_seconds) or duration_seconds < 0:
        raise ValueError("duration_seconds must be finite and non-negative")
    if requested_transactions is not None:
        if requested_transactions < 1:
            raise ValueError("requested_transactions must be positive")
        return requested_transactions
    effective_target_tps, _reason = _pace_limit(advertised_target_tps)
    # The first scheduled commit is one pacing interval after release; the extra
    # second leaves the PostgreSQL clock span at least the requested window even
    # when commits land on their due times.
    return max(1, math.ceil(effective_target_tps * (duration_seconds + 1.0)))


def _insert_one_customer(con, prefix: str, row_no: int) -> None:
    name = f"{prefix}-{row_no}"
    con.execute(
        "INSERT INTO app.customers (name, email, lifetime_value) VALUES (%s, %s, %s)",
        (name, f"{name}@example.com", row_no % 100),
    )


def _insert_customer_batch(con, prefix: str, first: int, last: int) -> None:
    con.execute(
        "INSERT INTO app.customers (name, email, lifetime_value) "
        "SELECT %s || '-' || i, %s || '-' || i || '@example.com', "
        "(i %% 100)::numeric FROM generate_series(%s::bigint, %s::bigint) i",
        (prefix, prefix, first, last),
    )


def _generate_transactions(
    database: str,
    marker_table: str,
    prefix: str,
    transactions: int,
    rows_per_transaction: int,
    advertised_target_tps: float,
    workers: int,
    *,
    include_customers: bool,
    published_marker_table: str | None = None,
    on_start: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    if transactions <= 0 or rows_per_transaction <= 0 or workers <= 0:
        raise ValueError("transactions, rows_per_transaction, and workers must be positive")
    effective_target_tps, cap_reason = _pace_limit(advertised_target_tps)
    ready = threading.Barrier(workers + 1)
    release = threading.Event()
    errors: list[str] = []
    commit_times: list[float] = []
    commit_lock = threading.Lock()
    start_box: dict[str, float] = {}

    marker_insert = pg_sql.SQL("INSERT INTO {}.{} (transaction_no, marker) VALUES (%s, %s)").format(
        pg_sql.Identifier("p53_measurement"), pg_sql.Identifier(marker_table)
    )
    published_marker_insert = None
    if published_marker_table is not None:
        published_marker_insert = pg_sql.SQL(
            "INSERT INTO {}.{} (transaction_no, marker) VALUES (%s, %s)"
        ).format(pg_sql.Identifier("app"), pg_sql.Identifier(published_marker_table))

    def write_worker(worker: int) -> None:
        try:
            with _source_connect(database, application_name=f"p53-writer-{worker}") as con:
                con.autocommit = False
                ready.wait(timeout=30)
                release.wait(timeout=30)
                start = start_box["monotonic"]
                for transaction_no in range(worker, transactions, workers):
                    due = start + (transaction_no + 1) / effective_target_tps
                    delay = due - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    try:
                        if include_customers:
                            first = transaction_no * rows_per_transaction
                            if rows_per_transaction == 1:
                                _insert_one_customer(con, prefix, first)
                            else:
                                _insert_customer_batch(
                                    con, prefix, first, first + rows_per_transaction - 1
                                )
                        con.execute(marker_insert, (transaction_no, prefix))
                        if published_marker_insert is not None:
                            con.execute(published_marker_insert, (transaction_no, prefix))
                        con.commit()
                    except BaseException:
                        con.rollback()
                        raise
                    with commit_lock:
                        commit_times.append(time.monotonic())
        except BaseException as exc:
            with commit_lock:
                errors.append(f"worker {worker}: {type(exc).__name__}: {exc}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(write_worker, worker) for worker in range(workers)]
        ready.wait(timeout=30)
        started = time.monotonic()
        start_box["monotonic"] = started
        if on_start is not None:
            on_start(started)
        release.set()
        for future in futures:
            future.result()
    ended = time.monotonic()
    if errors:
        raise RuntimeError("; ".join(errors))
    return {
        "advertised_target_tps": advertised_target_tps,
        "effective_pacing_limit_tps": effective_target_tps,
        "harness_cap_tps": MAX_GENERATOR_TPS,
        "cap_reason": cap_reason,
        "transactions_requested": transactions,
        "rows_requested": transactions * rows_per_transaction,
        "workers": workers,
        "rows_per_transaction": rows_per_transaction,
        "client_wall_sec": round(ended - started, 6),
        "first_client_commit_after_start_sec": (
            round(min(commit_times) - started, 6) if commit_times else None
        ),
        "last_client_commit_after_start_sec": (
            round(max(commit_times) - started, 6) if commit_times else None
        ),
    }


def _source_cleanup(prefix: str) -> None:
    with suppress(Exception), _source_connect(SOURCE_DATABASE, application_name="p53-cleanup") as con:
        con.autocommit = True
        con.execute("DELETE FROM app.customers WHERE name LIKE %s", (f"{prefix}-%",))


def _run_source_capability(
    evidence_dir: Path,
    *,
    advertised_target_tps: float = DEFAULT_TARGET_TPS,
    transactions: int = 4000,
    label: str = "capability",
    minimum_sustained_window_seconds: float = DEFAULT_SUSTAINED_WINDOW_SECONDS,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "kind": "source_capability",
        "label": label,
        "database_port": SOURCE_PORT,
        "measurement": (
            "PostgreSQL committed marker rows, PostgreSQL clock span, and "
            "pg_stat_database.xact_commit delta"
        ),
    }
    scratch_database: str | None = None
    with _scratch_postgres_database("cdc_p53_cap") as database:
        scratch_database = database
        marker_table = f"commits_{uuid.uuid4().hex[:12]}"
        _create_marker_table(database, marker_table)
        before_stats = _database_stats(database)
        csv_path = evidence_dir / f"{label}.host.csv"
        with HostSampler(csv_path) as sampler:
            generated = _generate_transactions(
                database,
                marker_table,
                f"p53-{label}-{uuid.uuid4().hex[:8]}",
                transactions,
                1,
                advertised_target_tps,
                DEFAULT_WORKERS,
                include_customers=False,
            )
            # Let PostgreSQL publish the final cumulative counter before the
            # post-window observation.  This sleep is inside the host-gated run.
            time.sleep(0.25)
            after_stats = _database_stats(database)
            facts = _marker_facts(database, marker_table)
        result.update(generated)
        result["postgres_stats_before"] = before_stats
        result["postgres_stats_after"] = after_stats
        result["postgres_xact_commit_delta"] = (
            after_stats["xact_commit"] - before_stats["xact_commit"]
        )
        result.update(facts)
        result["host_gate"] = sampler.verdict()
    result["scratch_database"] = scratch_database
    try:
        result["scratch_database_left_after_cleanup"] = _postgres_database_exists(
            scratch_database
        )
    except BaseException as exc:
        result["scratch_database_cleanup_check_error"] = f"{type(exc).__name__}: {exc}"
    result["actual_source_tps"] = facts["source_tps_from_postgres_clock"]
    result["minimum_sustained_window_seconds"] = minimum_sustained_window_seconds
    result["source_clock_window_pass"] = (
        facts["postgres_clock_span_sec"] >= minimum_sustained_window_seconds
    )
    result["xact_commit_cross_check_pass"] = (
        result["postgres_xact_commit_delta"] >= transactions
    )
    result["source_capability_pass"] = (
        result["host_gate"]["valid"]
        and result["committed_transactions"] == transactions
        and result["xact_commit_cross_check_pass"]
        and result["source_clock_window_pass"]
        and SOURCE_BAND_LOWER <= result["actual_source_tps"] <= SOURCE_BAND_UPPER
    )
    return result


def _motherduck_token() -> str | None:
    return os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")  # noqa: SIM112


def _create_motherduck_database(token: str, database: str) -> None:
    with duckdb.connect(f"md:?motherduck_token={token}") as con:
        con.execute(f"CREATE DATABASE {_duck_identifier(database)}")


def _prepare_motherduck_destination(token: str, database: str, dataset: str) -> None:
    """Create the non-data schemas required before service admission.

    ``SingleProcessFlight`` deliberately refuses to create destination state before
    it owns a fencing epoch.  The test harness owns this one-time fixture setup;
    every measured row and every state transition after admission still uses the
    service's one fenced connection.
    """
    from cdc_flight.control_schema import ensure_control_schema
    from cdc_flight.destination import ensure_dataset

    with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
        ensure_control_schema(con, "_cdc_flight")
        ensure_dataset(con, dataset)


def _drop_motherduck_database(token: str, database: str) -> None:
    with suppress(Exception), duckdb.connect(f"md:?motherduck_token={token}") as con:
        con.execute(f"DROP DATABASE {_duck_identifier(database)}")


def _motherduck_database_exists(token: str, database: str) -> bool:
    with duckdb.connect(f"md:?motherduck_token={token}") as con:
        return database in {str(row[0]) for row in con.execute("SHOW DATABASES").fetchall()}


def _destination_table_name(source_table: str) -> str:
    from cdc_flight.naming import destination_table

    return destination_table("cdcflight", "app", source_table)


def _motherduck_count(token: str, database: str, dataset: str, prefix: str) -> int:
    try:
        with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
            table = f"{_duck_identifier(dataset)}.{_duck_identifier('cdcflight_app_customers')}"
            return int(
                con.execute(
                    f"SELECT count(*) FROM {table} WHERE name LIKE ?",
                    [f"{prefix}-%"],
                ).fetchone()[0]
            )
    except Exception:
        return 0


def _motherduck_table_count(
    token: str,
    database: str,
    dataset: str,
    table: str,
    column: str,
    value: str,
) -> int:
    """Count a published marker with missing-table and network failures distinct.

    A marker destination table legitimately does not exist during the first few
    observations of initial admission; that means zero durable marker rows.  Any
    other MotherDuck failure invalidates the backlog observation instead of being
    silently converted into zero.
    """
    try:
        with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
            table_ref = (
                f"{_duck_identifier(dataset)}.{_duck_identifier(table)}"
            )
            return int(
                con.execute(
                    f"SELECT count(*) FROM {table_ref} "
                    f"WHERE {_duck_identifier(column)} = ?",
                    [value],
                ).fetchone()[0]
            )
    except duckdb.CatalogException as exc:
        if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
            return 0
        raise


def _motherduck_like_count(
    token: str,
    database: str,
    dataset: str,
    table: str,
    column: str,
    pattern: str,
) -> int:
    """Count published destination rows without hiding non-catalog failures."""
    try:
        with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
            table_ref = f"{_duck_identifier(dataset)}.{_duck_identifier(table)}"
            return int(
                con.execute(
                    f"SELECT count(*) FROM {table_ref} "
                    f"WHERE {_duck_identifier(column)} LIKE ?",
                    [pattern],
                ).fetchone()[0]
            )
    except duckdb.CatalogException as exc:
        if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
            return 0
        raise


def _source_published_data_sample(
    unpublished_table: str,
    prefix: str,
) -> dict[str, int]:
    """Return published application rows and the separate unpublished marker count."""
    with _source_connect(SOURCE_DATABASE, application_name="p53-published-backlog") as con:
        con.autocommit = True
        row = con.execute(
            pg_sql.SQL(
                "SELECT "
                "(SELECT count(*) FROM app.customers WHERE name LIKE %s) AS published_count, "
                "(SELECT count(*) FROM {}.{} WHERE marker = %s) AS unpublished_count"
            ).format(
                pg_sql.Identifier("p53_measurement"),
                pg_sql.Identifier(unpublished_table),
            ),
            (f"{prefix}-%", prefix),
        ).fetchone()
    return {
        "source_published_data_rows": int(row[0]),
        "source_unpublished_marker_rows": int(row[1]),
    }


def _published_backlog_sample(
    token: str,
    database: str,
    dataset: str,
    unpublished_marker_table: str,
    prefix: str,
) -> dict[str, Any]:
    source = _source_published_data_sample(unpublished_marker_table, prefix)
    destination_rows = _motherduck_like_count(
        token,
        database,
        dataset,
        _destination_table_name("customers"),
        "name",
        f"{prefix}-%",
    )
    pending = source["source_published_data_rows"] - destination_rows
    return {
        **source,
        "destination_published_data_rows": destination_rows,
        "pending_published_records": pending,
        "overdelivered_published_records": max(0, -pending),
        "measure": (
            "source committed app.customers rows minus durable destination "
            "app.customers rows, both filtered to this run's marker prefix"
        ),
        "unpublished_marker_in_backlog": False,
    }


def _motherduck_total_count(token: str, database: str, dataset: str) -> int:
    try:
        with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
            table = f"{_duck_identifier(dataset)}.{_duck_identifier('cdcflight_app_customers')}"
            return int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    except Exception:
        return 0


def _source_customer_rows(prefix: str) -> list[tuple[Any, ...]]:
    with _source_connect(SOURCE_DATABASE, application_name="p53-oracle-source") as con:
        con.autocommit = True
        return list(
            con.execute(
                "SELECT id, external_ref, name, email, signup_at, lifetime_value, "
                "is_active, prefs, tags, updated_at FROM app.customers "
                "WHERE name LIKE %s ORDER BY name",
                (f"{prefix}-%",),
            ).fetchall()
        )


def _destination_customer_rows(
    token: str, database: str, dataset: str, prefix: str
) -> list[tuple[Any, ...]]:
    with duckdb.connect(f"md:{database}?motherduck_token={token}") as con:
        table = f"{_duck_identifier(dataset)}.{_duck_identifier('cdcflight_app_customers')}"
        return list(
            con.execute(
                f"SELECT id, external_ref, name, email, signup_at, lifetime_value, "
                f"is_active, prefs, tags, updated_at FROM {table} "
                "WHERE name LIKE ? ORDER BY name",
                [f"{prefix}-%"],
            ).fetchall()
        )


def _wait_for_slot(slot: str, process: subprocess.Popen[str], timeout: float = 120.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = _slot_snapshot(slot)
        if snapshot.get("exists") and snapshot.get("active"):
            return snapshot
        if process.poll() is not None:
            raise RuntimeError(f"service exited before slot activation: {process.returncode}")
        time.sleep(0.25)
    raise TimeoutError(f"slot {slot!r} did not become active within {timeout:.1f}s")


def _wait_for_warmup(
    token: str,
    database: str,
    dataset: str,
    slot: str,
    process: subprocess.Popen[str],
    warmup_prefix: str,
    timeout: float = 120.0,
) -> dict[str, Any]:
    initial_deadline = time.monotonic() + timeout
    while time.monotonic() < initial_deadline:
        if process.poll() is not None:
            raise RuntimeError(f"service exited during initial snapshot: {process.returncode}")
        # cdc_source is the fixed, seeded source fixture.  Initial mode must
        # first publish those five baseline rows; only then is the isolated
        # warm-up row written and excluded from the measured prefix.
        if _motherduck_total_count(token, database, dataset) >= 5:
            break
        time.sleep(0.5)
    else:
        raise TimeoutError("initial source snapshot did not publish the seeded customers")
    _insert_source_warmup(warmup_prefix)
    target_lsn = _source_lsn()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"service exited during warm-up: {process.returncode}")
        count = _motherduck_count(token, database, dataset, warmup_prefix)
        slot_state = _slot_snapshot(slot)
        confirmed = _lsn_value(slot_state.get("confirmed_flush_lsn"))
        target = _lsn_value(target_lsn)
        if count == 1 and confirmed is not None and target is not None and confirmed >= target:
            return {
                "warmup_rows": count,
                "warmup_source_lsn": target_lsn,
                "warmup_slot_confirmation": slot_state,
            }
        time.sleep(0.5)
    raise TimeoutError("warm-up row did not become durable with post-commit slot confirmation")


def _insert_source_warmup(prefix: str) -> None:
    with _source_connect(SOURCE_DATABASE, application_name="p53-warmup") as con:
        con.autocommit = False
        _insert_one_customer(con, prefix, 0)
        con.commit()


class SlotMonitor:
    def __init__(
        self,
        slot: str,
        interval: float = SAMPLE_INTERVAL_SECONDS,
        published_sample: Callable[[], dict[str, Any]] | None = None,
    ):
        self.slot = slot
        self.interval = interval
        self.published_sample = published_sample
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._lock = threading.Lock()
        self.stop = threading.Event()
        self.start = threading.Event()
        self.source_started_at: float | None = None
        self.thread = threading.Thread(target=self._run, name="p53-slot-monitor", daemon=True)

    def source_started(self, at: float) -> None:
        self.source_started_at = at
        self.start.set()

    def __enter__(self) -> SlotMonitor:
        self.thread.start()
        return self

    def __exit__(self, _type, _value, _traceback) -> None:
        self.stop.set()
        self.thread.join(timeout=10)

    def capture_now(self, observed_at: float | None = None) -> None:
        """Capture the endpoint after the generator, labeled at its source edge."""
        observed_at = time.monotonic() if observed_at is None else observed_at
        try:
            sample = _slot_snapshot(self.slot)
        except BaseException as exc:
            with self._lock:
                self.errors.append(f"slot sample: {type(exc).__name__}: {exc}")
            return
        sample["observed_monotonic"] = observed_at
        if self.source_started_at is not None:
            sample["source_window_sec"] = round(observed_at - self.source_started_at, 6)
        if self.published_sample is not None:
            try:
                sample["published_backlog"] = self.published_sample()
            except BaseException as exc:
                sample["published_backlog_error"] = f"{type(exc).__name__}: {exc}"
                with self._lock:
                    self.errors.append(
                        f"published backlog sample: {type(exc).__name__}: {exc}"
                    )
        with self._lock:
            self.samples.append(sample)

    def _run(self) -> None:
        if not self.start.wait(timeout=120):
            return
        while not self.stop.is_set():
            self.capture_now()
            self.stop.wait(self.interval)

    def source_window_samples(self, end: float) -> list[dict[str, Any]]:
        start = self.source_started_at
        if start is None:
            return []
        with self._lock:
            samples = list(self.samples)
        return [
            sample
            for sample in samples
            if start <= float(sample["observed_monotonic"]) <= end
        ]

    def published_window_samples(self, end: float) -> list[dict[str, Any]]:
        return [
            sample
            for sample in self.source_window_samples(end)
            if isinstance(sample.get("published_backlog"), dict)
        ]


def _backlog_verdict(samples: list[dict[str, Any]]) -> dict[str, Any]:
    values = [
        (float(sample["source_window_sec"]), int(sample["confirmed_lag_bytes"]))
        for sample in samples
        if sample.get("confirmed_lag_bytes") is not None
    ]
    if len(values) < 3:
        return {
            "valid_observations": len(values),
            "keep_up_backlog": False,
            "reason": "fewer than three source-window slot-lag observations",
        }
    first = values[0][1]
    last = values[-1][1]
    minimum = min(item[1] for item in values)
    maximum = max(item[1] for item in values)
    mean_x = statistics.fmean(item[0] for item in values)
    mean_y = statistics.fmean(item[1] for item in values)
    denominator = sum((x - mean_x) ** 2 for x, _y in values)
    slope = (
        sum((x - mean_x) * (y - mean_y) for x, y in values) / denominator
        if denominator
        else 0.0
    )
    # Definition used for this round: a keep-up window has at least three slot
    # observations, ends no farther behind than it began, and has no positive
    # fitted backlog trend. A transient rise that drains before the end is
    # visible in max/min but is not called monotonically growing.
    strictly_increasing = all(values[index][1] > values[index - 1][1] for index in range(1, len(values)))
    return {
        "valid_observations": len(values),
        "lag_start_bytes": first,
        "lag_end_bytes": last,
        "lag_min_bytes": minimum,
        "lag_max_bytes": maximum,
        "lag_linear_slope_bytes_per_sec": round(slope, 3),
        "strictly_increasing": strictly_increasing,
        "definition": (
            "keep-up requires >=3 source-window samples, final slot lag <= initial "
            "lag, and non-positive fitted lag trend"
        ),
        "keep_up_backlog": last <= first and slope <= 0 and not strictly_increasing,
    }


def _published_backlog_verdict(samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate the backlog that the publication can actually deliver.

    The source and destination counts are both rows from the already-published
    ``app.customers`` relation.  This deliberately does not use a slot byte delta: WAL
    from the separate unpublished clock marker (and any other unpublished WAL)
    cannot become destination rows and therefore is not application backlog.
    """
    observations: list[tuple[float, int, int, int, int]] = []
    for sample in samples:
        backlog = sample.get("published_backlog")
        if not isinstance(backlog, dict):
            continue
        try:
            observations.append(
                (
                    float(sample["source_window_sec"]),
                    int(backlog["pending_published_records"]),
                    int(backlog["source_published_data_rows"]),
                    int(backlog["destination_published_data_rows"]),
                    int(backlog["overdelivered_published_records"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    if len(observations) < 3:
        return {
            "valid_observations": len(observations),
            "keep_up_backlog": False,
            "reason": "fewer than three published-data backlog observations",
            "definition": (
                "published backlog is source committed app.customers rows minus "
                "durable destination app.customers rows; keep-up requires >=3 observations, "
                "non-negative bounded backlog, final <= initial, and OLS slope <= 0"
            ),
        }
    pending_values = [item[1] for item in observations]
    source_values = [item[2] for item in observations]
    overdelivered_values = [item[4] for item in observations]
    mean_x = statistics.fmean(item[0] for item in observations)
    mean_y = statistics.fmean(item[1] for item in observations)
    denominator = sum((x - mean_x) ** 2 for x, *_rest in observations)
    slope = (
        sum((x - mean_x) * (pending - mean_y) for x, pending, *_rest in observations)
        / denominator
        if denominator
        else 0.0
    )
    first = pending_values[0]
    last = pending_values[-1]
    non_negative = all(value >= 0 for value in pending_values)
    source_non_decreasing = all(
        source_values[index] >= source_values[index - 1]
        for index in range(1, len(source_values))
    )
    return {
        "valid_observations": len(observations),
        "pending_start_records": first,
        "pending_end_records": last,
        "pending_min_records": min(pending_values),
        "pending_max_records": max(pending_values),
        "pending_linear_slope_records_per_sec": round(slope, 6),
        "source_published_data_rows_start": observations[0][2],
        "source_published_data_rows_end": observations[-1][2],
        "destination_published_data_rows_start": observations[0][3],
        "destination_published_data_rows_end": observations[-1][3],
        "overdelivered_observations": sum(value > 0 for value in overdelivered_values),
        "source_count_non_decreasing": source_non_decreasing,
        "positive_trend": slope > 0,
        "definition": (
            "published backlog is source committed app.customers rows minus "
            "durable destination app.customers rows; keep-up requires >=3 observations, "
            "non-negative bounded backlog, final <= initial, and OLS slope <= 0"
        ),
        "keep_up_backlog": (
            non_negative
            and source_non_decreasing
            and not any(value > 0 for value in overdelivered_values)
            and last <= first
            and slope <= 0
        ),
    }


def _stop_service(process: subprocess.Popen[str], timeout: float = 90.0) -> dict[str, Any]:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=30)
    return {"returncode": process.returncode}


def _drop_source_slot(slot: str) -> None:
    with suppress(Exception), _source_connect(SOURCE_ADMIN_DATABASE, application_name="p53-slot-cleanup") as con:
        con.autocommit = True
        con.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (slot,))


def _source_slot_exists(slot: str) -> bool:
    return bool(_slot_snapshot(slot).get("exists"))


def _service_environment(
    run_dir: Path,
    *,
    database: str,
    dataset: str,
    pipeline: str,
    slot: str,
    publication: str = SOURCE_PUBLICATION,
    stall: bool = False,
) -> dict[str, str]:
    token = _motherduck_token()
    if not token:
        raise RuntimeError("motherduck_token/MOTHERDUCK_TOKEN is required for the product arm")
    environment = {
        **os.environ,
        "PGHOST": "127.0.0.1",
        "PGPORT": str(SOURCE_PORT),
        "PGUSER": _env("PGUSER", "postgres"),
        "PGPASSWORD": _env("PGPASSWORD", "postgres"),
        "PGDATABASE": SOURCE_DATABASE,
        "CDC_TEST_PGPORT": str(SOURCE_PORT),
        "CDC_TEST_PGDATABASE": SOURCE_DATABASE,
        "CDC_STATE_DIR": str(run_dir / "state"),
        "CDC_PIPELINES_DIR": str(run_dir / "state" / "dlt_pipelines"),
        "CDC_SLOT_NAME": slot,
        "CDC_PIPELINE_NAME": pipeline,
        "CDC_SERVICE_ID": pipeline,
        "CDC_PUBLICATION": publication,
        "CDC_DESTINATION": "motherduck",
        "CDC_MD_DATABASE": database,
        "CDC_DATASET": dataset,
        "CDC_TABLES": "customers",
        "CDC_AUTO_DISCOVERY": "0",
        "CDC_SNAPSHOT_MODE": "initial",
        "MAX_RUNTIME_SEC": "0",
        "motherduck_token": token,
        "MOTHERDUCK_TOKEN": token,
        "RUNTIME__DLTHUB_TELEMETRY": "false",
        "ARROW_DEFAULT_MEMORY_POOL": "system",
    }
    if stall:
        arm = run_dir / "destination_fault.arm"
        environment.update(
            {
                # Group 1 is the initial snapshot and group 2 is the isolated
                # post-snapshot warm-up transaction.  Group 3 is therefore the
                # first measured source-data group after the external arm.
                "CDC_FAULT_INJECT": "destination_hang:3",
                "CDC_FAULT_HANG_PHASE": "pre_commit",
                # The initial snapshot is a real destination commit too, so its
                # bound must exceed the observed MotherDuck startup latency.  The
                # injected 40s live-data hang is still terminated by this 20s
                # commit watchdog after the arm is opened.
                "CDC_FAULT_HANG_SECONDS": "40",
                "CDC_SERVICE_COMMIT_TIMEOUT": "20",
                "CDC_COMMIT_TIMEOUT": "20",
                # MotherDuck admission and the initial snapshot are allowed to
                # finish before the test arms the live data fault.  The
                # destination-operation watchdog remains 3s once the fault is
                # armed; a short service idle timeout here would kill startup
                # before the source window existed.
                "CDC_SERVICE_STALL_TIMEOUT_SECONDS": "30",
                "CDC_CLOSE_TIMEOUT": "5",
                "CDC_SERVICE_CLOSE_TIMEOUT": "5",
                "CDC_TEST_DESTINATION_FAULT_ARM": str(arm),
                "CDC_TEST_CALLBACK_ENTERED": str(run_dir / "callback_entered.json"),
            }
        )
    return environment


def _start_service(environment: dict[str, str], log_path: Path) -> tuple[subprocess.Popen[str], Any]:
    executable = PROJECT_DIR / ".venv" / "bin" / "cdc-flight-service"
    if not executable.exists():
        raise RuntimeError(f"missing installed service executable: {executable}")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("w")
    process = subprocess.Popen(
        [str(executable), "--destination", "motherduck", "--log-level", "INFO"],
        cwd=PROJECT_DIR,
        env=environment,
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, handle


def _run_service_repetition(
    evidence_dir: Path,
    *,
    arm: str,
    repetition: int,
    target_tps: float,
    transactions: int,
    rows_per_transaction: int = 1,
    sustained_window_seconds: float = DEFAULT_SUSTAINED_WINDOW_SECONDS,
    stall: bool = False,
    positive_backlog_mutation: bool = False,
) -> dict[str, Any]:
    if not math.isfinite(sustained_window_seconds) or sustained_window_seconds < 0:
        raise ValueError("sustained_window_seconds must be finite and non-negative")
    token = _motherduck_token()
    if not token:
        raise RuntimeError("motherduck_token/MOTHERDUCK_TOKEN is required")
    run_id = f"{arm}_{repetition}_{uuid.uuid4().hex[:8]}"
    run_dir = evidence_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    md_database = _database_name(f"cdc_p53_{arm}_{repetition}")
    dataset = f"data_{uuid.uuid4().hex[:8]}"
    pipeline = f"p53_{arm}_{repetition}_{uuid.uuid4().hex[:8]}"
    slot = f"p53_{arm}_{repetition}_{os.getpid()}_{uuid.uuid4().hex[:8]}"[:63]
    prefix = f"p53-{arm}-{repetition}-{uuid.uuid4().hex[:8]}"
    warmup_prefix = f"p53-warm-{arm}-{repetition}-{uuid.uuid4().hex[:8]}"
    marker_table = f"run_{uuid.uuid4().hex[:12]}"
    publication = _env("CDC_PUBLICATION", SOURCE_PUBLICATION)
    result: dict[str, Any] = {
        "kind": "service_repetition",
        "arm": arm,
        "repetition": repetition,
        "production_source_port": SOURCE_PORT,
        "source_database": SOURCE_DATABASE,
        "destination_kind": "motherduck",
        "motherduck_database": md_database,
        "destination_dataset": dataset,
        "pipeline": pipeline,
        "slot": slot,
        "source_prefix": prefix,
        "warmup_prefix": warmup_prefix,
        "unpublished_marker_table": f"p53_measurement.{marker_table}",
        "published_source_relation": "app.customers",
        "published_destination_relation": _destination_table_name("customers"),
        "publication": publication,
        "transactions": transactions,
        "rows_per_transaction": rows_per_transaction,
        "target_tps_advertised": target_tps,
        "harness_cap_tps": MAX_GENERATOR_TPS,
        "stall_mutation": stall,
        "positive_backlog_mutation": positive_backlog_mutation,
        "minimum_sustained_window_seconds": sustained_window_seconds,
        "product_owner_path": (
            "SingleProcessFlight -> lease/ServiceContext -> pipeline.run -> "
            "run_engine_bounded -> Applier -> commit_protocol.commit_group"
        ),
    }
    process: subprocess.Popen[str] | None = None
    log_handle = None
    environment: dict[str, str] | None = None
    generator_error: BaseException | None = None
    try:
        _create_motherduck_database(token, md_database)
        _prepare_motherduck_destination(token, md_database, dataset)
        _create_marker_table(SOURCE_DATABASE, marker_table)
        environment = _service_environment(
            run_dir,
            database=md_database,
            dataset=dataset,
            pipeline=pipeline,
            slot=slot,
            publication=publication,
            stall=stall,
        )
        process, log_handle = _start_service(environment, run_dir / "service.log")
        admission_started = time.monotonic()
        active_slot = _wait_for_slot(slot, process)
        result["service_slot_admission_sec"] = round(time.monotonic() - admission_started, 3)
        result["slot_at_admission"] = active_slot
        result["warmup"] = _wait_for_warmup(
            token, md_database, dataset, slot, process, warmup_prefix
        )
        if stall:
            arm_path = Path(environment["CDC_TEST_DESTINATION_FAULT_ARM"])
            arm_path.touch()
            result["destination_fault_arm"] = {
                "path": str(arm_path),
                "armed_after_warmup": True,
            }
        lower_lsn = _source_lsn()
        source_started_box: dict[str, float] = {}

        def on_start(at: float) -> None:
            source_started_box["at"] = at

        def sample_published_backlog() -> dict[str, Any]:
            return _published_backlog_sample(
                token,
                md_database,
                dataset,
                marker_table,
                prefix,
            )

        csv_path = evidence_dir / run_id / "host.csv"
        with HostSampler(csv_path) as sampler:
            # The sampler starts before the measured generator.  It continues
            # through destination durability, final oracle, and service stop.
            with SlotMonitor(slot, published_sample=sample_published_backlog) as monitor:
                generated = _generate_transactions(
                    SOURCE_DATABASE,
                    marker_table,
                    prefix,
                    transactions,
                    rows_per_transaction,
                    target_tps,
                    DEFAULT_WORKERS,
                    include_customers=True,
                    on_start=lambda at: (on_start(at), monitor.source_started(at)),
                )
                source_finished_at = time.monotonic()
                # Label this endpoint at the last generator commit even though
                # the query itself is performed immediately afterward.
                monitor.capture_now(observed_at=source_finished_at)
                source_facts = _marker_facts(SOURCE_DATABASE, marker_table)
                published_source_facts = _published_data_facts(prefix)
                upper_lsn = _source_lsn()
                result["source"] = {**generated, **source_facts}
                result["source"]["published_data"] = published_source_facts
                result["source"]["source_upper_lsn"] = upper_lsn
                result["source"]["source_lower_lsn"] = lower_lsn
                result["source"]["actual_source_tps"] = source_facts[
                    "source_tps_from_postgres_clock"
                ]
                expected_rows = transactions * rows_per_transaction
                durable_at: float | None = None
                destination_count = 0
                published_destination_count = 0
                deadline = time.monotonic() + (20.0 if stall else 300.0)
                while time.monotonic() < deadline:
                    slot_state = _slot_snapshot(slot)
                    confirmed = _lsn_value(slot_state.get("confirmed_flush_lsn"))
                    target = _lsn_value(upper_lsn)
                    if (
                        not stall
                        and confirmed is not None
                        and target is not None
                        and confirmed >= target
                    ):
                        durable_at = time.monotonic()
                        result["slot_at_durable_boundary"] = slot_state
                        break
                    if process.poll() is not None:
                        break
                    time.sleep(1.0)
                # A confirmed source position is the product's durable boundary:
                # the applier only acknowledges after the one MotherDuck commit.
                # Read the destination after that boundary, rather than polling a
                # second MotherDuck connection while the service owns its commit
                # connection; the latter can manufacture cloud write conflicts.
                destination_count = _motherduck_count(
                    token, md_database, dataset, prefix
                )
                published_destination_count = _motherduck_like_count(
                    token,
                    md_database,
                    dataset,
                    _destination_table_name("customers"),
                    "name",
                    f"{prefix}-%",
                )
                if (
                    not stall
                    and durable_at is not None
                    and (
                        destination_count != expected_rows
                        or published_destination_count != expected_rows
                    )
                ):
                    destination_deadline = time.monotonic() + 60.0
                    while time.monotonic() < destination_deadline:
                        destination_count = _motherduck_count(
                            token, md_database, dataset, prefix
                        )
                        published_destination_count = _motherduck_like_count(
                            token,
                            md_database,
                            dataset,
                            _destination_table_name("customers"),
                            "name",
                            f"{prefix}-%",
                        )
                        if (
                            destination_count == expected_rows
                            and published_destination_count == expected_rows
                        ):
                            break
                        time.sleep(1.0)
                source_window_end = source_finished_at
                source_window_samples = monitor.source_window_samples(source_window_end)
                published_window_samples = monitor.published_window_samples(source_window_end)
                backlog = _backlog_verdict(source_window_samples)
                published_backlog = _published_backlog_verdict(published_window_samples)
                published_backlog_final_pending = (
                    published_source_facts["committed_published_data_rows"]
                    - published_destination_count
                )
                if positive_backlog_mutation:
                    original_published_backlog = published_backlog
                    mutated_samples: list[dict[str, Any]] = []
                    for index, sample in enumerate(published_window_samples):
                        mutated = dict(sample)
                        observed_backlog = dict(sample["published_backlog"])
                        source_rows = max(
                            int(observed_backlog["source_published_data_rows"]),
                            index + 1,
                        )
                        pending = index + 1
                        observed_backlog.update(
                            {
                                "source_published_data_rows": source_rows,
                                "destination_published_data_rows": source_rows - pending,
                                "pending_published_records": pending,
                                "overdelivered_published_records": 0,
                            }
                        )
                        mutated["published_backlog"] = observed_backlog
                        mutated_samples.append(mutated)
                    published_backlog = _published_backlog_verdict(mutated_samples)
                    published_backlog["mutation_applied"] = True
                    published_backlog["observed_before_mutation"] = original_published_backlog
                result["published_backlog_samples"] = published_window_samples
                result["published_backlog"] = published_backlog
                result["slot_backlog_samples"] = source_window_samples
            host_gate = sampler.verdict()
        result["host_gate"] = host_gate
        result["slot_backlog"] = backlog
        result["source"]["source_window_sec_client"] = round(
            source_finished_at - source_started_box["at"], 6
        )
        result["destination_rows_observed_before_stop"] = destination_count
        result["published_destination_rows_observed_before_stop"] = published_destination_count
        result["published_backlog_final_pending_records"] = published_backlog_final_pending
        result["published_backlog_sample_errors"] = list(monitor.errors)
        if durable_at is not None:
            result["delivery"] = {
                "durable_boundary_sec_after_source_start": round(
                    durable_at - source_started_box["at"], 6
                ),
                "durable_rows": transactions * rows_per_transaction,
                "delivered_rows_per_sec": round(
                    transactions * rows_per_transaction
                    / max(durable_at - source_started_box["at"], 0.001),
                    3,
                ),
            }
        else:
            result["delivery"] = {
                "durable_boundary_sec_after_source_start": None,
                "durable_rows": destination_count,
                "delivered_rows_per_sec": None,
            }
        if not stall and durable_at is not None:
            source_rows = _source_customer_rows(prefix)
            destination_rows = _destination_customer_rows(token, md_database, dataset, prefix)
            try:
                sys.path.insert(0, str(PROJECT_DIR / "tests"))
                from support.live_stock import assert_exact_rows

                assert_exact_rows(
                    source_rows,
                    destination_rows,
                    label=f"{arm} repetition {repetition} source/destination",
                )
                published_data_exact = bool(
                    published_source_facts["committed_published_data_rows"]
                    == expected_rows
                    and published_destination_count == expected_rows
                    and published_backlog_final_pending == 0
                )
                result["oracle"] = {
                    "passed": published_data_exact,
                    "source_rows": len(source_rows),
                    "destination_rows": len(destination_rows),
                    "published_data_source_rows": published_source_facts[
                        "committed_published_data_rows"
                    ],
                    "published_data_source_rows_exact": published_data_exact,
                    "published_data_destination_rows": published_destination_count,
                    "published_data_count_exact": published_data_exact,
                    "identity_value_multiplicity": "exact",
                    "columns": [
                        "id",
                        "external_ref",
                        "name",
                        "email",
                        "signup_at",
                        "lifetime_value",
                        "is_active",
                        "prefs",
                        "tags",
                        "updated_at",
                    ],
                }
                if not published_data_exact:
                    result["oracle"]["error"] = (
                        "published data count or final published backlog was not exact"
                    )
            except BaseException as exc:
                result["oracle"] = {
                    "passed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "source_rows": len(source_rows),
                    "destination_rows": len(destination_rows),
                    "published_data_source_rows": published_source_facts[
                        "committed_published_data_rows"
                    ],
                    "published_data_destination_rows": published_destination_count,
                }
        elif stall:
            result["oracle"] = {
                "passed": False,
                "reason": "destination-stall mutation intentionally prevents a durable exact oracle",
            }
        else:
            result["oracle"] = {
                "passed": False,
                "reason": "no durable source/destination boundary was observed",
            }
        source_window_sustained = bool(
            result["source"].get("postgres_clock_span_sec", 0)
            >= sustained_window_seconds
            and result["source"].get("source_window_sec_client", 0)
            >= sustained_window_seconds
        )
        result["source_window_sustained"] = source_window_sustained
        result["keep_up"] = bool(
            not stall
            and result["host_gate"]["valid"]
            and result["source"]["actual_source_tps"] >= SOURCE_BAND_LOWER
            and result["source"]["actual_source_tps"] <= SOURCE_BAND_UPPER
            and source_window_sustained
            and result["published_backlog_sample_errors"] == []
            and result["published_backlog"].get("keep_up_backlog") is True
            and result["published_backlog_final_pending_records"] == 0
            and result["oracle"].get("passed") is True
        )
        result["valid_for_score"] = bool(result["keep_up"])
        if stall:
            result["mutation_verdict"] = {
                "passed": not result["keep_up"],
                "keep_up_check_rejected": not result["keep_up"],
                "reason": "destination was stalled while source marker commits continued",
                "callback_witness": str(run_dir / "callback_entered.json"),
                "fault_record": str(run_dir / "state" / "fault_fired.json"),
            }
    except BaseException as exc:
        generator_error = exc
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["valid_for_score"] = False
    finally:
        if process is not None:
            result["service_stop"] = _stop_service(process)
        if log_handle is not None:
            log_handle.close()
        service_summary_path = run_dir / "state" / "last_run.json"
        service_summary = _read_json(service_summary_path)
        if service_summary is not None:
            result["service_summary"] = service_summary
        _drop_source_slot(slot)
        _source_cleanup(prefix)
        _source_cleanup(warmup_prefix)
        _drop_marker_table(SOURCE_DATABASE, marker_table)
        _drop_motherduck_database(token, md_database)
        cleanup: dict[str, Any] = {}
        try:
            cleanup["slot_exists_after_cleanup"] = _source_slot_exists(slot)
        except BaseException as exc:
            cleanup["slot_cleanup_check_error"] = f"{type(exc).__name__}: {exc}"
        try:
            cleanup["source_prefix_rows_after_cleanup"] = len(_source_customer_rows(prefix))
            cleanup["warmup_prefix_rows_after_cleanup"] = len(
                _source_customer_rows(warmup_prefix)
            )
        except BaseException as exc:
            cleanup["source_cleanup_check_error"] = f"{type(exc).__name__}: {exc}"
        try:
            cleanup["marker_table_exists_after_cleanup"] = _source_relation_exists(
                "p53_measurement", marker_table
            )
        except BaseException as exc:
            cleanup["marker_cleanup_check_error"] = f"{type(exc).__name__}: {exc}"
        try:
            cleanup["motherduck_database_exists_after_cleanup"] = _motherduck_database_exists(
                token, md_database
            )
        except BaseException as exc:
            cleanup["motherduck_cleanup_check_error"] = f"{type(exc).__name__}: {exc}"
        result["cleanup"] = cleanup
    if result.get("keep_up"):
        service_summary = result.get("service_summary") or {}
        service_stop = result.get("service_stop") or {}
        if (
            service_stop.get("returncode") != 0
            or service_summary.get("service_mode") is not True
            or service_summary.get("ok") is not True
        ):
            result["keep_up"] = False
            result["valid_for_score"] = False
            result["keep_up_rejection_reason"] = (
                "service did not publish a successful service-mode summary after the "
                "durable boundary"
            )
    if stall:
        callback_path = run_dir / "callback_entered.json"
        fault_path = run_dir / "state" / "fault_fired.json"
        fault_record = _read_json(fault_path)
        source = result.get("source") or {}
        mutation_passed = bool(
            source.get("committed_transactions") == transactions
            and SOURCE_BAND_LOWER <= float(source.get("actual_source_tps", 0)) <= SOURCE_BAND_UPPER
            and callback_path.exists()
            and fault_record
            and fault_record.get("point") == "destination_hang"
            and not result.get("keep_up", False)
        )
        result["mutation_verdict"] = {
            "passed": mutation_passed,
            "source_commits_continued": source.get("committed_transactions") == transactions,
            "source_actual_tps": source.get("actual_source_tps"),
            "destination_stall_witness": callback_path.exists(),
            "fault_record": fault_record,
            "keep_up_check_rejected": not result.get("keep_up", False),
            "reason": (
                "destination was stalled while source marker commits continued; the "
                "keep-up decision also requires an exact destination/oracle boundary"
            ),
        }
    if positive_backlog_mutation:
        mutated_backlog = result.get("published_backlog") or {}
        result["mutation_verdict"] = {
            "passed": bool(
                mutated_backlog.get("positive_trend") is True
                and mutated_backlog.get("keep_up_backlog") is False
                and not result.get("keep_up", False)
            ),
            "positive_trend_observed": mutated_backlog.get("positive_trend"),
            "fitted_slope_records_per_sec": mutated_backlog.get(
                "pending_linear_slope_records_per_sec"
            ),
            "keep_up_check_rejected": not result.get("keep_up", False),
            "reason": (
                "the recorded published-only backlog samples were given a positive "
                "trend; the keep-up predicate rejected the mutated evidence"
            ),
        }
    _write_json(run_dir / "result.json", result)
    if generator_error is not None and not result.get("stall_mutation"):
        # The caller records the failed product repetition and decides whether
        # the run is a product failure or an environmental discard.
        return result
    return result


def _aggregate(repetitions: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [item for item in repetitions if item.get("valid_for_score")]
    host_valid = [
        item
        for item in repetitions
        if (item.get("host_gate") or {}).get("valid")
        and item.get("source", {}).get("actual_source_tps") is not None
    ]
    rates = [
        float(item["delivery"]["delivered_rows_per_sec"])
        for item in valid
        if item.get("delivery", {}).get("delivered_rows_per_sec") is not None
    ]
    source_rates = [float(item["source"]["actual_source_tps"]) for item in host_valid]
    score_source_rates = [
        float(item["source"]["actual_source_tps"])
        for item in valid
        if item.get("source", {}).get("actual_source_tps") is not None
    ]
    durations = [
        float(item["delivery"]["durable_boundary_sec_after_source_start"])
        for item in valid
        if item.get("delivery", {}).get("durable_boundary_sec_after_source_start") is not None
    ]

    def stats(values: list[float]) -> dict[str, Any]:
        if not values:
            return {"median": None, "min": None, "max": None, "p95_small_sample_max": None}
        return {
            "median": round(statistics.median(values), 3),
            "min": round(min(values), 3),
            "max": round(max(values), 3),
            "p95_small_sample_max": round(max(values), 3),
        }

    discarded = []
    for item in repetitions:
        host_gate = item.get("host_gate") or {}
        if not item.get("valid_for_score"):
            discarded.append(
                {
                    "repetition": item.get("repetition"),
                    "host_gate_valid": host_gate.get("valid"),
                    "host_invalid_samples": host_gate.get("invalid_samples", []),
                    "keep_up": item.get("keep_up", False),
                    "oracle_passed": (item.get("oracle") or {}).get("passed"),
                    "source_window_sustained": item.get("source_window_sustained"),
                    "published_backlog": item.get("published_backlog"),
                    "error": item.get("error") or item.get("keep_up_rejection_reason"),
                }
            )
    return {
        "repetitions_total": len(repetitions),
        "valid_repetitions": len(valid),
        "discarded_or_failed_repetitions": len(repetitions) - len(valid),
        "host_valid_repetitions": len(host_valid),
        "discarded_host_repetitions": sum(
            not bool((item.get("host_gate") or {}).get("valid"))
            for item in repetitions
        ),
        "delivered_rows_per_sec": stats(rates),
        "source_tps": stats(source_rates),
        "source_tps_score_valid": stats(score_source_rates),
        "durable_boundary_sec": stats(durations),
        "host_gate_verdicts": [item.get("host_gate") for item in repetitions],
        "discarded_repetitions": discarded,
        "full_spread_repetition_ids": [
            {
                "repetition": item.get("repetition"),
                "valid_for_score": item.get("valid_for_score", False),
                "source_tps": item.get("source", {}).get("actual_source_tps"),
                "delivered_rows_per_sec": item.get("delivery", {}).get("delivered_rows_per_sec"),
                "backlog_start_bytes": item.get("slot_backlog", {}).get("lag_start_bytes"),
                "backlog_end_bytes": item.get("slot_backlog", {}).get("lag_end_bytes"),
                "published_backlog_start_records": item.get("published_backlog", {}).get(
                    "pending_start_records"
                ),
                "published_backlog_end_records": item.get("published_backlog", {}).get(
                    "pending_end_records"
                ),
                "published_backlog_slope_records_per_sec": item.get(
                    "published_backlog", {}
                ).get("pending_linear_slope_records_per_sec"),
                "published_backlog_observations": item.get("published_backlog", {}).get(
                    "valid_observations"
                ),
                "published_backlog_final_pending_records": item.get(
                    "published_backlog_final_pending_records"
                ),
                "source_window_sec_client": item.get("source", {}).get(
                    "source_window_sec_client"
                ),
                "postgres_clock_span_sec": item.get("source", {}).get(
                    "postgres_clock_span_sec"
                ),
                "source_window_sustained": item.get("source_window_sustained"),
                "oracle_passed": item.get("oracle", {}).get("passed"),
                "host_csv": item.get("host_gate", {}).get("csv"),
            }
            for item in repetitions
        ],
    }


def _run_arm(
    evidence_dir: Path,
    arm: str,
    *,
    repetitions: int,
    target_tps: float,
    transactions: int,
    sustained_window_seconds: float,
) -> dict[str, Any]:
    results = []
    for repetition in range(1, repetitions + 1):
        results.append(
            _run_service_repetition(
                evidence_dir,
                arm=arm,
                repetition=repetition,
                target_tps=target_tps,
                transactions=transactions,
                sustained_window_seconds=sustained_window_seconds,
            )
        )
    return {"arm": arm, "repetitions": results, "aggregate": _aggregate(results)}


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=PROJECT_DIR, check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


def _run_mutations(evidence_dir: Path, target_tps: float) -> dict[str, Any]:
    advertised = _run_source_capability(
        evidence_dir,
        advertised_target_tps=2000.0,
        transactions=2500,
        label="mutation_advertised_2000_actual_cap_500",
        minimum_sustained_window_seconds=0.0,
    )
    advertised["harness_decision"] = {
        "accepted_claimed_band": False,
        "advertised_band_tps": 2000.0,
        "actual_source_tps": advertised.get("actual_source_tps"),
        "actual_band": (
            3
            if SOURCE_BAND_LOWER <= float(advertised.get("actual_source_tps", 0)) <= SOURCE_BAND_UPPER
            else None
        ),
        "reason": (
            "the requested 2000 TPS was capped at 500 and the score decision uses "
            "the PostgreSQL-measured actual rate, never the advertisement"
        ),
    }
    stalled = _run_service_repetition(
        evidence_dir,
        arm="mutation_destination_stall",
        repetition=1,
        target_tps=target_tps,
        transactions=1200,
        sustained_window_seconds=0.0,
        stall=True,
    )
    stalled["harness_decision"] = {
        "accepted_keep_up": False,
        "reason": (
            "destination stall left the source slot backlog/oracle unsatisfied; "
            "a source-only rate is not a keep-up result"
        ),
    }
    positive = _run_service_repetition(
        evidence_dir,
        arm="mutation_positive_backlog_trend",
        repetition=1,
        target_tps=target_tps,
        transactions=1200,
        sustained_window_seconds=0.0,
        positive_backlog_mutation=True,
    )
    positive["harness_decision"] = {
        "accepted_keep_up": False,
        "positive_trend_rejected": (positive.get("published_backlog") or {}).get(
            "positive_trend"
        )
        is True
        and (positive.get("published_backlog") or {}).get("keep_up_backlog") is False,
        "reason": (
            "a positive fitted trend in the published-only pending-record series "
            "must make keep-up false, even if the unmutated run reached the oracle"
        ),
    }
    return {
        "advertised_vs_actual": advertised,
        "destination_stall": stalled,
        "positive_backlog_trend": positive,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path(os.environ.get("P53_EVIDENCE_DIR", DEFAULT_EVIDENCE_DIR)),
    )
    parser.add_argument("--target-tps", type=float, default=DEFAULT_TARGET_TPS)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--duration-seconds", type=float, default=DEFAULT_SUSTAINED_WINDOW_SECONDS)
    parser.add_argument("--transactions", type=int, default=None)
    parser.add_argument("--capability-only", action="store_true")
    parser.add_argument("--mutations-only", action="store_true")
    args = parser.parse_args(argv)
    if int(_env("CDC_TEST_PGPORT", str(SOURCE_PORT))) != SOURCE_PORT:
        raise SystemExit("p53 harness is intentionally fixed to CDC_TEST_PGPORT=15432")
    if args.repetitions < 1:
        raise SystemExit("repetitions must be positive")
    if (
        not math.isfinite(args.duration_seconds)
        or args.duration_seconds < DEFAULT_SUSTAINED_WINDOW_SECONDS
    ):
        raise SystemExit(
            f"duration-seconds must be at least {DEFAULT_SUSTAINED_WINDOW_SECONDS:.0f}"
        )
    if args.transactions is not None and args.transactions < 1:
        raise SystemExit("transactions must be positive when supplied")
    measurement_transactions = _transactions_for_sustained_window(
        args.target_tps,
        args.duration_seconds,
        args.transactions,
    )
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "harness": "p53_sustained_tps",
        "base_sha": BASE_SHA,
        "branch_at_start": _git("branch", "--show-current"),
        "source_port": SOURCE_PORT,
        "target_tps_advertised": args.target_tps,
        "sustained_window_seconds": args.duration_seconds,
        "transactions_for_measurement": measurement_transactions,
        "source_measurement": (
            "one PostgreSQL marker row per generator transaction, PostgreSQL clock "
            "span, and pg_stat_database.xact_commit delta"
        ),
        "keep_up_definition": (
            "published-only pending records = source committed rows in the published "
            "marker table minus durable destination marker rows; over >=180 seconds "
            "it must be bounded and have non-positive OLS slope, then the final "
            "published marker count and exact identity/value/multiplicity oracle must "
            "hold at the durable boundary"
        ),
    }
    capability = _run_source_capability(
        args.evidence_dir,
        advertised_target_tps=args.target_tps,
        transactions=measurement_transactions,
        minimum_sustained_window_seconds=args.duration_seconds,
    )
    report["source_capability"] = capability
    _write_json(args.evidence_dir / "source_capability.json", capability)
    if args.capability_only:
        print(json.dumps(report, indent=2, sort_keys=True, default=_json_default))
        return 0 if capability["source_capability_pass"] else 2
    if not capability["source_capability_pass"] and not args.mutations_only:
        report["stopped"] = "source capability check failed before product measurement"
        print(json.dumps(report, indent=2, sort_keys=True, default=_json_default))
        return 2
    token = _motherduck_token()
    if not token:
        raise SystemExit("motherduck_token/MOTHERDUCK_TOKEN is required")
    if args.mutations_only:
        report["mutations"] = _run_mutations(args.evidence_dir, args.target_tps)
    else:
        report["candidate"] = _run_arm(
            args.evidence_dir,
            "candidate",
            repetitions=args.repetitions,
            target_tps=args.target_tps,
            transactions=measurement_transactions,
            sustained_window_seconds=args.duration_seconds,
        )
        _git("checkout", BASE_SHA)
        try:
            if _git("rev-parse", "HEAD") != BASE_SHA:
                raise RuntimeError("control arm did not start at the required base SHA")
            report["control"] = _run_arm(
                args.evidence_dir,
                "control",
                repetitions=args.repetitions,
                target_tps=args.target_tps,
                transactions=measurement_transactions,
                sustained_window_seconds=args.duration_seconds,
            )
        finally:
            _git("switch", CANDIDATE_BRANCH)
        if _git("rev-parse", "HEAD") != _git("rev-parse", CANDIDATE_BRANCH):
            raise RuntimeError("candidate branch was not restored after control arm")
        report["mutations"] = _run_mutations(args.evidence_dir, args.target_tps)
    report["branch_at_end"] = _git("branch", "--show-current")
    report["sha_at_end"] = _git("rev-parse", "HEAD")
    _write_json(args.evidence_dir / "p53_results.json", report)
    print(json.dumps(report, indent=2, sort_keys=True, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
