"""Destination identifier naming — dlt used as a *library* (ADR 0001 D10).

ADR 0001 D10 demotes dlt from framework to library: the pipeline/load path
leaves the apply path, but the layers that are genuinely tested and genuinely
load-bearing stay. This module is one of the two places where that decision is
cashed in (the other is `dlt.common.schema`, which arrives with rubric 2.1/2.5).

Why the naming convention specifically earns its keep: every destination table
name in this repo (`cdcflight_app_customers`, …), every probe, every existing
test and `RUBRIC_STATUS.md` were produced by dlt's `snake_case` normaliser.
Re-deriving those names by hand is exactly how a silent rename happens the first
time a source column is called `Col Name` or `select`. Calling dlt's normaliser
keeps the names byte-identical across the applier migration, which is a property
the migrated e2e tests actually assert.

The adapter is deliberately tiny (ADR §10's exit criterion is "more than ~100
lines of adapter ⇒ drop it"): 3 calls, no dlt pipeline, no dlt state.
"""

from __future__ import annotations

from functools import lru_cache

from .config import DEFAULT_CONTROL_SCHEMA

#: Reserved for the applier's own bookkeeping; never taken from a source column.
CDCF_COMMIT_ID = "cdcf_commit_id"
CDCF_EVENT_ID = "cdcf_event_id"
CDCF_TOTAL_ORDER = "cdcf_total_order"
CDCF_DELETED = "cdcf_deleted"
CDCF_DELETE_EVENT_ID = "cdcf_delete_event_id"
CDCF_DELETE_LSN = "cdcf_delete_lsn"
DBZ_COLUMNS = (
    "dbz_op",
    "dbz_lsn",
    "dbz_tx_id",
    "dbz_schema",
    "dbz_table",
    "dbz_source_ts_ms",
)
#: Every column the applier adds to a replicated table (ADR 0001 §4.9).
APPLIER_COLUMNS = (
    CDCF_COMMIT_ID,
    CDCF_EVENT_ID,
    CDCF_TOTAL_ORDER,
    CDCF_DELETED,
    CDCF_DELETE_EVENT_ID,
    CDCF_DELETE_LSN,
    *DBZ_COLUMNS,
)

#: Suffix of the shadow table a snapshot loads into before the swap (ADR D7).
SHADOW_SUFFIX = "__cdcf_tmp"


@lru_cache(maxsize=1)
def _convention():
    from dlt.common.normalizers.naming.snake_case import NamingConvention

    return NamingConvention()


@lru_cache(maxsize=4096)
def normalize(identifier: str) -> str:
    """dlt's `snake_case` normalisation of a single identifier."""
    return _convention().normalize_identifier(identifier)


@lru_cache(maxsize=1024)
def destination_table(topic_prefix: str, schema: str, table: str) -> str:
    """`cdcflight` + `app` + `customers` -> `cdcflight_app_customers`.

    The source *schema* is part of the name on purpose: Debezium's default topic
    for this connector does not always carry it, so two same-named tables in
    different schemas would otherwise collide silently.
    """
    return normalize(f"{topic_prefix}_{schema}_{table}".replace(".", "_"))


def shadow_table(name: str) -> str:
    return f"{name}{SHADOW_SUFFIX}"


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def control_table(schema: str, table: str) -> str:
    """Return an injection-safe qualified control-table identifier."""
    if schema == DEFAULT_CONTROL_SCHEMA:
        # Preserve the published default SQL spelling. The default is a fixed,
        # trusted identifier; every configured non-default schema takes the quoted
        # path below.
        return f"{schema}.{table}"
    return f"{quote(schema)}.{quote(table)}"
