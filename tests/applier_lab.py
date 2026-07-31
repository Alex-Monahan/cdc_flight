"""An in-process laboratory for the *real* `Applier`: no JVM, no Postgres.

Why this exists
---------------
The 1.1/1.2/1.3 reviews found four accepted-bad-state defects (spill ordering,
snapshot spill routing, fenced spill prefixes, a mid-apply anchor that is not
mid-apply) that the whole subprocess suite could not see, because every one of
them needs a *specific interleaving* of assembler and applier state. Driving the
shipped `Applier` against a shipped DuckDB file directly makes those
interleavings exact and costs milliseconds instead of the ~40 s a crash/recovery
cycle costs, so every blocker gets a guard in the **default** suite.

What is real here: `TransactionAssembler`, `Applier`, `apply_sql`,
`destination`, the control schema, the spill table, the snapshot shadow/swap and
the commit protocol. What is faked: the Debezium `ChangeEvent` (a
`PendingRecord` is constructed directly, exactly as `envelope.decode` would) and
the `RecordCommitter`.

Records carry `source_partition` / `source_offset` on purpose: that is what
`decode(..., want_offsets=True)` produces for the offset-bearing records, and the
applier refuses to advance a resume point whose Connect offset it cannot read.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Any

import duckdb

from cdc_flight import destination as dest_mod
from cdc_flight.applier import Applier, ApplierConfig
from cdc_flight.destination import Lease, ResumePoint
from cdc_flight.envelope import (
    KIND_DATA,
    KIND_HEARTBEAT,
    KIND_SNAPSHOT,
    KIND_TXN_BEGIN,
    KIND_TXN_END,
    PendingRecord,
)

TOPIC_PREFIX = "cdcflight"
DATASET = "cdc_raw"
PARTITION = {"server": TOPIC_PREFIX}


class _Raw:
    """Stands in for the Java `ChangeEvent`. Only its identity matters here: the
    applier passes it to `markProcessed()` and otherwise only checks it for None
    (that is how it releases superseded JPype references)."""

    __slots__ = ("topic",)

    def __init__(self, topic: str) -> None:
        self.topic = topic

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<raw {self.topic}>"


def _offset(lsn: int, txn_id: str | None = None) -> dict[str, Any]:
    offset: dict[str, Any] = {"lsn": lsn, "lsn_proc": lsn, "ts_usec": lsn * 1000}
    if txn_id is not None:
        offset["transaction_id"] = f"{txn_id}:{lsn}"
    return offset


class FakeCommitter:
    """`SourceRecordCommitter` reduced to the two calls Invariant O cares about."""

    def __init__(self) -> None:
        self.marked = 0
        self.batches = 0

    def markProcessed(self, record) -> None:
        self.marked += 1

    def markBatchFinished(self) -> None:
        self.batches += 1


# --------------------------------------------------------------------------- #
# record constructors (exactly the shapes `envelope.decode` produces)
# --------------------------------------------------------------------------- #
def begin(txn: str, lsn: int) -> PendingRecord:
    return PendingRecord(
        raw=_Raw(f"{TOPIC_PREFIX}.transaction"),
        kind=KIND_TXN_BEGIN,
        topic=f"{TOPIC_PREFIX}.transaction",
        nbytes=40,
        txn_id=txn,
        lsn=lsn,
        txn_status="BEGIN",
        source_partition=dict(PARTITION),
        source_offset=_offset(lsn, txn),
    )


def end(txn: str, count: int, lsn: int, per_table: dict[str, int] | None = None) -> PendingRecord:
    rec = PendingRecord(
        raw=_Raw(f"{TOPIC_PREFIX}.transaction"),
        kind=KIND_TXN_END,
        topic=f"{TOPIC_PREFIX}.transaction",
        nbytes=40,
        txn_id=txn,
        lsn=lsn,
        txn_status="END",
        txn_event_count=count,
        source_partition=dict(PARTITION),
        source_offset=_offset(lsn, txn),
    )
    rec.txn_data_collections = dict(per_table or {})
    return rec


def data(
    txn: str,
    order: int,
    lsn: int,
    *,
    table: str = "customers",
    op: str = "c",
    key: dict | None = None,
    after: dict | None = None,
    before: dict | None = None,
    nbytes: int = 100,
) -> PendingRecord:
    return PendingRecord(
        raw=_Raw(f"{TOPIC_PREFIX}.app.{table}"),
        kind=KIND_DATA,
        topic=f"{TOPIC_PREFIX}.app.{table}",
        nbytes=nbytes,
        op=op,
        schema="app",
        table=table,
        lsn=lsn,
        txn_id=txn,
        total_order=order,
        source_ts_ms=1_760_000_000_000 + order,
        key=key,
        before=before,
        after=after,
        source_partition=dict(PARTITION),
        source_offset=_offset(lsn, txn),
    )


def keyed(txn: str, order: int, lsn: int, ident: int, value: str, **kw) -> PendingRecord:
    """One keyed UPDATE/INSERT of `id = ident`."""
    return data(
        txn,
        order,
        lsn,
        key={"id": ident},
        after={"id": ident, "name": value},
        **kw,
    )


def snap(
    table: str,
    lsn: int,
    *,
    marker: str = "true",
    ident: int | None = None,
    value: str = "s",
    key: dict | None = None,
    nbytes: int = 100,
) -> PendingRecord:
    after = {"id": ident, "name": value} if ident is not None else {"name": value}
    return PendingRecord(
        raw=_Raw(f"{TOPIC_PREFIX}.app.{table}"),
        kind=KIND_SNAPSHOT,
        topic=f"{TOPIC_PREFIX}.app.{table}",
        nbytes=nbytes,
        op="r",
        schema="app",
        table=table,
        lsn=lsn,
        snapshot=marker,
        key=key if key is not None else ({"id": ident} if ident is not None else None),
        after=after,
        source_partition=dict(PARTITION),
        source_offset=_offset(lsn),
    )


def heartbeat(lsn: int) -> PendingRecord:
    return PendingRecord(
        raw=_Raw(f"__debezium-heartbeat.{TOPIC_PREFIX}"),
        kind=KIND_HEARTBEAT,
        topic=f"__debezium-heartbeat.{TOPIC_PREFIX}",
        nbytes=10,
        lsn=lsn,
        source_partition=dict(PARTITION),
        source_offset=_offset(lsn),
    )


# --------------------------------------------------------------------------- #
# the lab
# --------------------------------------------------------------------------- #
class Lab:
    """One `Applier` over one DuckDB file, driven record by record."""

    def __init__(self, path: Path, *, resume_lsn: int = 0, **cfg: Any) -> None:
        self.path = Path(path)
        self.con = duckdb.connect(str(self.path))
        dest_mod.ensure_control_schema(self.con)
        dest_mod.ensure_dataset(self.con, DATASET)
        self.committer = FakeCommitter()
        cfg.setdefault("verify_offset_file", False)
        self.config = ApplierConfig(**cfg)
        self.lease = Lease("lab", ttl_seconds=600)
        self.lease.acquire(self.con)
        self.applier = Applier(
            self.con,
            pipeline="lab",
            namespace="lab-namespace",
            dataset=DATASET,
            topic_prefix=TOPIC_PREFIX,
            offset_path=self.path.parent / "offsets.dat",
            resume_point=ResumePoint(last_lsn=resume_lsn),
            config=self.config,
            lease=self.lease,
            runner_id="lab-runner",
        )
        self.applier._committer = self.committer

    # -- driving ---------------------------------------------------------- #
    def feed(self, records: list[PendingRecord]) -> None:
        """Assemble records into units and buffer them, exactly as `_handle` does."""
        for rec in records:
            for unit in self.applier.assembler.feed(rec):
                self.applier._add_unit(unit)

    def commit(self, trigger: str = "test") -> None:
        self.applier._committer = self.committer
        self.applier.commit_group(trigger)

    def run(self, records: list[PendingRecord], trigger: str = "test") -> None:
        self.feed(records)
        self.commit(trigger)

    # -- inspecting ------------------------------------------------------- #
    def q(self, sql: str, params: list | None = None) -> list[tuple]:
        return self.con.execute(sql, params or []).fetchall()

    def scalar(self, sql: str, params: list | None = None):
        return self.q(sql, params)[0][0]

    def rows(self, table: str, columns: str = "*", order: str = "1") -> list[tuple]:
        return self.q(f'SELECT {columns} FROM "{DATASET}"."{table}" ORDER BY {order}')

    def exists(self, table: str) -> bool:
        return bool(
            self.scalar(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [DATASET, table],
            )
        )

    def target(self, table: str) -> str:
        return f"{TOPIC_PREFIX}_app_{table}"

    def shadow(self, table: str) -> str:
        return f"{self.target(table)}__cdcf_tmp"

    def close(self) -> None:
        self.applier.shutdown()
        with contextlib.suppress(Exception):  # pragma: no cover
            self.con.close()
