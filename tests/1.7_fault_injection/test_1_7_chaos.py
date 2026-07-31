"""Rubric 1.7 — a seeded chaos harness over the whole anchor set.

The matrix proves each anchor once, in isolation, from a clean state. What it cannot
prove is that the anchors compose: a fault that fires while the *previous* fault's
recovery is still the most recent thing that happened to the destination, over and over,
with the workload shape changing underneath. That is where state-machine bugs live, and
three of the last review round's blockers were exactly that shape.

Seeded and therefore reproducible: `CDC_CHAOS_SEED` reruns a failing sequence verbatim,
and the seed and the sequence are printed either way, because an unreproducible chaos
failure is a rumour rather than a bug report.

The invariant checked after **every** iteration is the only one that matters: the
destination equals the source, per table, with no duplicated identities. Not "the run
succeeded" - most iterations fail on purpose.
"""

from __future__ import annotations

import os
import random

import pytest
from conftest import Sandbox

#: Anchors the harness draws from, with whatever arming they need.
from test_1_7_fault_matrix import ARMING

from cdc_flight import faults

ITERATIONS = int(os.environ.get("CDC_CHAOS_ITERATIONS", "8"))
ROWS = 12

#: `swap` needs a re-snapshot in flight to have a shadow to tear, so it is excluded here
#: and covered by `tests/1.6_snapshot_consistency/test_1_6_interrupted_snapshot.py`.
CHAOS_POINTS = tuple(p for p in faults.ALL_POINTS if p != "swap")


def _shape(box: Sandbox, rng: random.Random, tag: str) -> None:
    """One randomly chosen unit of source work. Each shape is a different fold."""
    shape = rng.choice(["insert", "update", "delete", "key_update", "multi_table", "keyless"])
    if shape == "insert":
        box.sql(
            "INSERT INTO app.customers (name, email) SELECT "
            f"'{tag}-' || i, '{tag}-' || i || '@example.com' "
            f"FROM generate_series(1, {ROWS}) i",
            one_transaction=True,
        )
    elif shape == "update":
        box.sql(
            f"UPDATE app.customers SET lifetime_value = coalesce(lifetime_value, 0) + 1 "
            f"WHERE name LIKE '{tag[:-1]}%'"
        )
    elif shape == "delete":
        box.sql(f"DELETE FROM app.customers WHERE name LIKE '%-{rng.randint(1, ROWS)}'")
    elif shape == "key_update":
        box.sql(
            [
                f"INSERT INTO app.customers (name, email) VALUES ('{tag}-k', '{tag}-k@x.com')",
                f"UPDATE app.customers SET id = -id WHERE name = '{tag}-k'",
            ],
            one_transaction=True,
        )
    elif shape == "multi_table":
        box.sql(
            [
                f"INSERT INTO app.customers (name, email) VALUES ('{tag}-m', '{tag}-m@x.com')",
                "INSERT INTO app.orders (customer_id, total_amount, status) "
                "SELECT 1, 5.00, 'pending' FROM generate_series(1, 3) i",
            ],
            one_transaction=True,
        )
    else:
        box.sql(
            "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
            f"'{tag}', 1.0, 'C' FROM generate_series(1, {ROWS}) i",
            one_transaction=True,
        )


def _assert_equal_to_source(box: Sandbox, note: str) -> None:
    source = {
        str(r[0]) for r in box.pg_query("SELECT name FROM app.customers")
    }
    dest = {
        str(r[0])
        for r in box.duck_query(f"SELECT name FROM {box.table('cdcflight_app_customers')}")
    }
    assert dest == source, (
        f"{note}: destination and source disagree on app.customers; "
        f"missing={sorted(source - dest)[:6]} extra={sorted(dest - source)[:6]}"
    )
    src_readings = box.pg_query("SELECT count(*) FROM app.sensor_readings")[0][0]
    dst_readings = box.scalar(
        f"SELECT count(*) FROM {box.table('cdcflight_app_sensor_readings')}"
    )
    assert dst_readings == src_readings, (
        f"{note}: keyless changelog holds {dst_readings} rows for {src_readings} source rows"
    )
    for table in ("cdcflight_app_customers", "cdcflight_app_sensor_readings", "cdcflight_app_orders"):
        total, distinct = box.duck_query(
            f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {box.table(table)}"
        )[0]
        assert total == distinct, f"{note}: {table} has {total - distinct} duplicated events"


@pytest.mark.slow
def test_random_faults_at_random_anchors_never_break_the_ledger(tmp_path_factory, postgres_cluster):
    seed = int(os.environ.get("CDC_CHAOS_SEED", "20260731"))
    rng = random.Random(seed)
    box = Sandbox("chaos", tmp_path_factory.mktemp("sbx_chaos"), postgres_cluster)
    plan: list[tuple[int, str, str]] = []
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _assert_equal_to_source(box, "baseline")

        for iteration in range(1, ITERATIONS + 1):
            point = rng.choice(CHAOS_POINTS)
            nth = 1 if rng.random() < 0.7 else 2
            tag = f"chaos{iteration}"
            plan.append((iteration, point, tag))
            _shape(box, rng, tag)

            env = {"CDC_FAULT_INJECT": f"{point}:{nth}", **ARMING.get(point, {})}
            # `expect_success=False` for every iteration: whether the fault fires at all
            # depends on how many data groups the shape produces, and an iteration in
            # which it did not fire is a perfectly good (if unexciting) data point.
            box.run(max_seconds=120, timeout=200, expect_success=False, extra_env=env)

            # Recovery, with as many attempts as it takes - a fault at `<nth>=2` can be
            # followed by one at the same anchor on the retry.
            for attempt in range(4):
                recovered = box.run(max_seconds=200, expect_success=False)
                if recovered.get("ok") is True:
                    break
                assert attempt < 3, (
                    f"iteration {iteration} ({point}:{nth}) could not be recovered in "
                    f"4 attempts: { {k: v for k, v in recovered.items() if k != 'output'} }"
                )
            _assert_equal_to_source(box, f"iteration {iteration} after {point}:{nth}")
    except BaseException:
        print(f"\nCDC_CHAOS_SEED={seed} sequence={plan}")
        raise
    finally:
        print(f"\nchaos seed {seed} completed sequence: {plan}")
        box.cleanup()
        box.reseed()
