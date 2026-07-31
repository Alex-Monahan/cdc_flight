"""`make query` - show what actually landed in the destination.

Local DuckDB by default; `--destination motherduck` to peek at the cloud copy.
"""

from __future__ import annotations

import argparse

import duckdb

from .config import DestinationConfig, motherduck_token


def connect(dest: DestinationConfig) -> duckdb.DuckDBPyConnection:
    if dest.kind == "duckdb":
        return duckdb.connect(str(dest.duckdb_path), read_only=True)
    token = motherduck_token()
    if not token:
        raise RuntimeError("`motherduck_token` is not set")
    return duckdb.connect(f"md:{dest.motherduck_database}?motherduck_token={token}")


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
        ops = con.execute(
            f"SELECT __op, count(*) FROM {qualified} GROUP BY 1 ORDER BY 1"
        ).fetchall()
        ops_txt = " ".join(f"{op}={n}" for op, n in ops)
        print(f"{name:42s} rows={total:<6d} ops[{ops_txt}]")

    print("-" * 72)
    for (name,) in tables:
        qualified = f'"{dest.dataset_name}"."{name}"'
        print(f"\n== {name} (first {args.limit}) ==")
        con.sql(f"SELECT * FROM {qualified} LIMIT {args.limit}").show(max_width=200)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
