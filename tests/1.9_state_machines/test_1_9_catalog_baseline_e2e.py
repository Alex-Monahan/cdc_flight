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
    # ...what START-UP found. The shutdown key is the other fact, and on a converged run
    # it is the empty list — two names, because one key carrying both was terminally
    # contradictory (Codex r6 MINOR-1).
    detected = summary.get("catalog_baseline_unreconciled_at_start") or []
    assert f"app.{TABLE}" in detected, json.dumps(summary, default=str)[:1500]
    assert summary.get("catalog_baseline_unreconciled") == [], (
        "a converged run still reported relations it could not reconcile"
    )
    assert summary.get("catalog_baseline") == "valid"
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


# --------------------------------------------------------------------------- #
# the two round-6 compositions, against the real cluster
# --------------------------------------------------------------------------- #
@pytest.mark.slow
def test_a_destination_that_predates_the_control_table_is_rebuilt_not_adopted(
    tmp_path_factory, postgres_cluster
):
    """Codex r6 BLOCKER-1, reproduced: the legacy-migration shape.

    A populated destination with no relation registry, and then the
    `_cdc_flight.catalog_baseline` table **dropped** — which is exactly what a
    destination that predates this migration looks like to the first upgraded run. Under
    rev 14 that run read `absent`, trusted it, adopted the replacement oid and reported
    `catalog_baseline='valid'` over `[1, 2, 999]` against a source holding `[999]`.
    Twice.
    """
    table = "r6_absent"
    target = f"cdcflight_app_{table}"
    box = Sandbox("r6absent", tmp_path_factory.mktemp("sbx_r6absent"), postgres_cluster)
    box.env["CDC_TABLES"] = f"customers,{table}"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        box.reseed()
        box.sql(
            [
                f"DROP TABLE IF EXISTS app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (1, 'old-1'), (2, 'old-2')",
            ]
        )
        box.run(reset_state=True, max_seconds=180, extra_env={"CDC_DROP_MODE": "ignore"})
        assert box.scalar(f"SELECT count(*) FROM {box.table(target)}") == 2

        # THE PRE-MIGRATION SHAPE: rows, no registry, and no baseline table at all.
        box.duck_write("DROP TABLE IF EXISTS _cdc_flight.catalog_baseline")
        assert box.duck_query(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = "
            "'_cdc_flight' AND table_name = 'catalog_baseline'"
        )[0][0] == 0

        box.sql(
            [
                f"DROP TABLE app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (999, 'replacement-only')",
            ]
        )

        upgraded = box.run(max_seconds=260, idle_seconds=10)
        assert upgraded.get("catalog_baseline_was") == "absent", upgraded
        landed = {int(r[0]) for r in box.duck_query(f"SELECT id FROM {box.table(target)}")}
        assert not ({1, 2} & landed), (
            f"the first upgraded run adopted a replacement relation: {sorted(landed)}"
        )
        settled = box.run(max_seconds=220, idle_seconds=10)
        assert settled["ok"] is True, settled
        source = {(int(a), str(b)) for a, b in box.pg_query(
            f"SELECT id, label FROM app.{table}"
        )}
        final = {(int(a), str(b)) for a, b in box.duck_query(
            f"SELECT id, label FROM {box.table(target)}"
        )}
        assert final == source, f"source={sorted(source)} destination={sorted(final)}"
    finally:
        box.reseed()


@pytest.mark.slow
def test_a_skipped_rebuild_cannot_be_reported_as_a_successful_run(
    tmp_path_factory, postgres_cluster
):
    """Codex r6 BLOCKER-2, reproduced: `CDC_RESNAPSHOT=0` over an unrelatable relation.

    Under rev 14 the run marked the relation `awaiting_snapshot`, skipped the rebuild,
    logged it as unhandled, streamed replacement row 999 beside old rows 1 and 2,
    persisted the replacement oid, promoted the baseline to `valid` and returned 0.

    `CDC_RESNAPSHOT=0`'s own contract is detect, alert, exit non-zero, mutate nothing —
    and that is what it must do here, before the engine can adopt anything.
    """
    table = "r6_noresnap"
    target = f"cdcflight_app_{table}"
    box = Sandbox("r6noresnap", tmp_path_factory.mktemp("sbx_r6nr"), postgres_cluster)
    box.env["CDC_TABLES"] = f"customers,{table}"
    box.env["CDC_CATALOG_POLL_SECONDS"] = "1"
    try:
        box.reseed()
        box.sql(
            [
                f"DROP TABLE IF EXISTS app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (1, 'old-1'), (2, 'old-2')",
            ]
        )
        box.run(reset_state=True, max_seconds=180, extra_env={"CDC_DROP_MODE": "ignore"})
        box.sql(
            [
                f"DROP TABLE app.{table}",
                f"CREATE TABLE app.{table} (id bigint PRIMARY KEY, label text NOT NULL)",
                f"ALTER PUBLICATION cdc_flight_pub ADD TABLE app.{table}",
                f"INSERT INTO app.{table} VALUES (999, 'replacement-only')",
            ]
        )

        refused = box.run(
            max_seconds=200, expect_success=False, extra_env={"CDC_RESNAPSHOT": "0"}
        )
        assert refused.get("ok") is not True, refused
        landed = {int(r[0]) for r in box.duck_query(f"SELECT id FROM {box.table(target)}")}
        assert landed == {1, 2}, (
            f"the refusal mutated the destination it was protecting: {sorted(landed)}"
        )
        assert box.duck_query(
            "SELECT count(*) FROM _cdc_flight.source_relations WHERE source_table = ?",
            [table],
        )[0][0] == 0, "the replacement identity was persisted by a run that refused"

        # ...and with repair enabled again it heals, which is what makes the refusal a
        # gate rather than a wedge.
        healed = box.run(max_seconds=260, idle_seconds=10)
        assert healed["ok"] is True, healed
        source = {(int(a), str(b)) for a, b in box.pg_query(
            f"SELECT id, label FROM app.{table}"
        )}
        final = {(int(a), str(b)) for a, b in box.duck_query(
            f"SELECT id, label FROM {box.table(target)}"
        )}
        assert final == source, f"source={sorted(source)} destination={sorted(final)}"
    finally:
        box.reseed()
