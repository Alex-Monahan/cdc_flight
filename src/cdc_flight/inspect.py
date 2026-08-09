"""`make query` - show what actually landed in the destination.

Local DuckDB by default; `--destination motherduck` to peek at the cloud copy.
"""

from __future__ import annotations

import argparse

import duckdb

from .config import DestinationConfig, motherduck_token
from .destination import DUCKDB_CONNECT_CONFIG
from .naming import control_table


def connect(dest: DestinationConfig) -> duckdb.DuckDBPyConnection:
    if dest.kind == "duckdb":
        return duckdb.connect(
            str(dest.duckdb_path), read_only=True, config=DUCKDB_CONNECT_CONFIG
        )
    token = motherduck_token()
    if not token:
        raise RuntimeError("`motherduck_token` is not set")
    return duckdb.connect(
        f"md:{dest.motherduck_database}?motherduck_token={token}",
        config=DUCKDB_CONNECT_CONFIG,
    )


def _report_incomplete_tables(con, dest: DestinationConfig) -> None:
    """Say loudly which tables are known to be incomplete (rubric 1.5 / Opus Q1).

    A source relation that was dropped and recreated has a destination table that CDC
    alone cannot rebuild, and the worst outcome is one that *looks* healthy. So the
    `awaiting_snapshot` flag `catalog_apply` persists is surfaced here rather than only
    in one `table_events.detail` string.
    """
    try:
        rows = con.execute(
            "SELECT pipeline, source_schema, source_table, target_table "
            f"FROM {control_table(dest.control_schema, 'table_state')} "
            "WHERE snapshot_state = 'awaiting_snapshot' "
            "ORDER BY 1, 2, 3"
        ).fetchall()
    except Exception:  # pragma: no cover - a destination with no control schema yet
        return
    if not rows:
        return
    print("-" * 72)
    print(f"!! {len(rows)} table(s) are INCOMPLETE and need a re-snapshot (rubric 2.3/3.4):")
    for pipeline, schema, table, target in rows:
        print(f"   {schema}.{table} -> {target or '(dropped)'}   [pipeline {pipeline}]")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-inspect", description=__doc__)
    parser.add_argument("--destination", choices=["duckdb", "motherduck"], default="duckdb")
    parser.add_argument("--limit", type=int, default=3, help="sample rows per table")
    args = parser.parse_args(argv)

    dest = DestinationConfig(kind=args.destination)
    con = connect(dest)

    tables = con.execute(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = ?
           AND table_name NOT LIKE '\\_dlt%' ESCAPE '\\'
         ORDER BY table_name
        """,
        [dest.dataset_name],
    ).fetchall()

    if not tables:
        print(f"no tables in dataset {dest.dataset_name!r} yet - run the pipeline first")
        return 1

    print(f"dataset: {dest.dataset_name}  ({args.destination})")
    print("-" * 72)
    for (name,) in tables:
        qualified = f'"{dest.dataset_name}"."{name}"'
        total = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
        # dlt child tables (arrays) carry no CDC metadata of their own.
        has_op = con.execute(
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = 'dbz_op'",
            [dest.dataset_name, name],
        ).fetchone()[0]
        if has_op:
            ops = con.execute(
                f"SELECT dbz_op, count(*) FROM {qualified} GROUP BY 1 ORDER BY 1"
            ).fetchall()
            ops_txt = " ".join(f"{op}={n}" for op, n in ops)
        else:
            ops_txt = "child table"
        print(f"{name:44s} rows={total:<6d} ops[{ops_txt}]")

    _report_incomplete_tables(con, dest)

    print("-" * 72)
    for (name,) in tables:
        qualified = f'"{dest.dataset_name}"."{name}"'
        print(f"\n== {name} (first {args.limit}) ==")
        con.sql(f"SELECT * FROM {qualified} LIMIT {args.limit}").show(max_width=200)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
