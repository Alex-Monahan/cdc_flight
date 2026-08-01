"""The r5 BLOCKER, end to end against the real cluster (rubric 1.9 · 1.5 · 1.7 · 4.7).

`test_1_9_catalog_baseline.py` proves the *decision* in process, in under a second, and
that is where the fine-grained assertions live. This proves the **composition** — four
real pipeline runs against Postgres on :15432 with a real Debezium engine — because the
whole finding was that each piece was individually defensible and the sequence was not:

1. a destination populated with **no relation registry** (`CDC_DROP_MODE=ignore`, which
   is exactly what a destination built before `source_relations` existed looks like);
2. a run in which **every** catalog poll fails (`CDC_FAULT_INJECT=catalog_poll:1`). It
   must fail loudly — round 5 already fixed that — and it must leave a *durable* record
   that the baseline was never confirmed, which is what round 5 did not fix;
3. the relation is dropped and recreated at the source while the pipeline is down, with
   a single replacement row;
4. the healthy retries. Round 5 measured both of them reporting `ok=true` while the
   destination permanently held `1, 2` (the old relation's rows) **beside** `999`.

The assertion this file exists for is the last one: **no successful run may ever observe
the old rows beside the new**, and the destination must converge on the source.
"""

from __future__ import annotations

import json

import pytest
from conftest import Sandbox

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

TABLE = "baseline_demo"
TARGET = f"cdcflight_app_{TABLE}"


@pytest.fixture(scope="module")
def unchecked_catalog(tmp_path_factory, postgres_cluster):
    box = Sandbox("baseline", tmp_path_factory.mktemp("sbx_baseline"), postgres_cluster)
    box.env["CDC_TABLES"] = f"customers,{TABLE}"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    phases: dict = {"box": box}
    try:
        box.reseed()
        box.sql(
            [
                f"DROP TABLE IF EXISTS app.{TABLE}",
                f"CREATE TABLE app.{TABLE} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{TABLE}",
                f"INSERT INTO app.{TABLE} VALUES (1, 'old-1'), (2, 'old-2')",
            ]
        )
        # 1. a populated destination with no relation registry at all.
        box.run(reset_state=True, max_seconds=180, extra_env={"CDC_DROP_MODE": "ignore"})
        phases["rows_after_populate"] = box.scalar(
            f"SELECT count(*) FROM {box.table(TARGET)}"
        )
        phases["registry_after_populate"] = box.duck_query(
            "SELECT source_table FROM _cdc_flight.source_relations"
        )

        # 2. every catalog poll fails. The run must die AND leave the obligation.
        phases["unchecked"] = box.run(
            max_seconds=120,
            expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "catalog_poll:1"},
        )
        phases["baseline_after_unchecked"] = box.duck_query(
            "SELECT state, unreconciled_json FROM _cdc_flight.catalog_baseline"
        )

        # 3. drop + recreate while the pipeline is down, with a replacement row.
        box.sql(
            [
                f"DROP TABLE app.{TABLE}",
                f"CREATE TABLE app.{TABLE} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{TABLE}",
                f"INSERT INTO app.{TABLE} VALUES (999, 'replacement-only')",
            ]
        )

        # 4. the healthy retry. It reads the durable mark, finds it cannot relate the
        #    rows it holds to any identity at the source, marks those relations
        #    `awaiting_snapshot` BEFORE the owed queue is read — and rubric 1.6's
        #    blocking re-snapshot rebuilds them from the source in this same run.
        phases["retry"] = box.run(max_seconds=260, idle_seconds=10)
        phases["rows_after_retry"] = box.duck_query(
            f"SELECT id, label FROM {box.table(TARGET)} ORDER BY id"
        ) if box.scalar(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? "
            "AND table_name = ?", [box.DATASET, TARGET],
        ) else []
        phases["healed"] = box.run(max_seconds=220, idle_seconds=10)
        phases["rows_after_healed"] = box.duck_query(
            f"SELECT id, label FROM {box.table(TARGET)} ORDER BY id"
        )
        phases["source"] = box.pg_query(f"SELECT id, label FROM app.{TABLE} ORDER BY id")
        phases["baseline_after_healed"] = box.duck_query(
            "SELECT state FROM _cdc_flight.catalog_baseline"
        )
        yield phases
    finally:
        box.reseed()


def test_the_precondition_is_the_reviewers_one(unchecked_catalog):
    """A populated destination that has never recorded a single relation identity."""
    assert unchecked_catalog["rows_after_populate"] == 2
    assert unchecked_catalog["registry_after_populate"] == [], (
        "the precondition is a destination with rows and NO baseline; a registry here "
        "would make this test prove the thing that already worked"
    )


def test_a_run_that_never_read_the_catalog_fails_and_says_why(unchecked_catalog):
    summary = unchecked_catalog["unchecked"]
    assert summary.get("ok") is not True, summary
    assert int(summary.get("catalog_successful_polls") or 0) == 0, summary


def test_that_run_leaves_a_DURABLE_record_that_the_baseline_is_unconfirmed(unchecked_catalog):
    """The whole finding in one assertion.

    `successful_polls` is process memory: it can reject the run that failed, and it
    cannot carry that run's obligation across the failure into the next one. Round 5
    reproduced exactly that — the failed run left the destination registry empty and
    nothing else, so the next healthy run had no reason not to adopt.
    """
    rows = unchecked_catalog["baseline_after_unchecked"]
    assert rows, "the failed run left NOTHING behind saying the baseline was never confirmed"
    state, _unreconciled = rows[0]
    assert state in ("stale", "invalidated"), state


def test_no_successful_run_ever_shows_the_old_rows_beside_the_new(unchecked_catalog):
    """THE consistency assertion. Round 5 measured `[1, 2, 999]`, twice, with `ok=true`."""
    assert unchecked_catalog["retry"]["ok"] is True, unchecked_catalog["retry"]
    ids = {int(r[0]) for r in unchecked_catalog["rows_after_retry"]}
    assert not ({1, 2} & ids), (
        f"the old relation's rows survived a successful run beside the new one's: {ids}"
    )


def test_the_retry_rebuilds_the_unrelatable_relations_in_ITS_OWN_run(unchecked_catalog):
    """One run, not two, and no destructive DDL to get there.

    The relation exists at the source; what could not be done was relating the rows we
    held to it. So the action is `awaiting_snapshot` and the actor is rubric 1.6's
    blocking re-snapshot, which runs before the main stream — not a drop, which would
    trip the mass-drop circuit breaker the moment more than one relation is unrelatable
    (measured: a destination built without a registry has ALL of them unrelatable).
    """
    summary = unchecked_catalog["retry"]
    unreconciled = summary.get("catalog_baseline_unreconciled") or []
    assert f"app.{TABLE}" in unreconciled, json.dumps(summary, default=str)[:1500]
    rebuilt = set(summary.get("resnapshot_swapped") or []) | set(
        summary.get("resnapshot_emptied") or []
    )
    assert f"app.{TABLE}" in rebuilt, (
        "the relation was not rebuilt on the run that found it unrelatable: "
        + json.dumps(summary, default=str)[:1500]
    )
    landed = {(int(a), str(b)) for a, b in unchecked_catalog["rows_after_retry"]}
    source = {(int(a), str(b)) for a, b in unchecked_catalog["source"]}
    assert landed == source, (
        f"the retry did not converge on its own: source={sorted(source)} "
        f"destination={sorted(landed)}"
    )


def test_the_destination_stays_converged(unchecked_catalog):
    assert unchecked_catalog["healed"]["ok"] is True, unchecked_catalog["healed"]
    source = {(int(a), str(b)) for a, b in unchecked_catalog["source"]}
    landed = {(int(a), str(b)) for a, b in unchecked_catalog["rows_after_healed"]}
    assert landed == source, f"source={sorted(source)} destination={sorted(landed)}"


def test_the_baseline_is_confirmed_once_the_destination_is_related(unchecked_catalog):
    """And only then. A `valid` baseline is a claim with evidence behind it."""
    assert [r[0] for r in unchecked_catalog["baseline_after_healed"]] == ["valid"]
    summary = unchecked_catalog["healed"]
    assert summary.get("catalog_baseline") == "valid", json.dumps(summary, default=str)[:800]
