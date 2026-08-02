"""Small run-summary projection kept out of pipeline orchestration."""

from __future__ import annotations


def decorate(result: dict, *, extra: dict, destination, runner_id: str) -> dict:
    result.update(extra)
    result["destination"] = destination.kind
    result["dataset"] = destination.dataset_name
    result["runner_id"] = runner_id
    if destination.kind == "duckdb":
        result["duckdb_path"] = str(destination.duckdb_path)
    else:
        result["motherduck_database"] = destination.motherduck_database
    return result
