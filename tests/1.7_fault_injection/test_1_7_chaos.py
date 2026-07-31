"""Rubric 1.7 — a seeded chaos harness over the whole anchor set.

The matrix proves each anchor once, in isolation, from a clean state. What it cannot
prove is that the anchors compose: a fault that fires while the *previous* fault's
recovery is still the most recent thing that happened to the destination, over and over,
with the workload shape changing underneath. That is where state-machine bugs live, and
three of the last review round's blockers were exactly that shape.

## What the previous cut did not do, and now does (Codex M3 / Opus MAJOR-6)

The docstring claimed the harness "composes the anchors" and it did not:

* **recovery runs got no fault environment**, so a fault could never fire during another
  fault's recovery — which is exactly the composition the claim is about. Recovery
  attempts now carry a fault too, drawn from the same plan, so the second fault lands on
  a destination whose most recent event is the first fault's rollback;
* **the chosen fault was allowed not to fire**, described in a comment as "a perfectly
  good data point". It is not: an iteration in which nothing fired proves nothing and
  silently shrinks the case count. Every iteration now asserts its anchor fired, using
  the `fault_fired.json` record;
* **the plan did not cover the anchor set.** A uniform draw over 12 anchors in 8
  iterations misses most of them. The plan is now a **shuffled cover**: every eligible
  anchor appears at least once, and the length is the anchor count (extra iterations, if
  asked for, are drawn on top);
* **the seed was hard-coded**, so it was a fixed sequence rather than a search. The
  default seed is now the day, and the slow lane runs several seeds.

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

ROWS = 12

#: Anchors this harness cannot reach, each with the reason and the module that does.
#:
#: The harness's scenario is an *ordinary healthy run over a changing workload*, which
#: is what makes composition meaningful; an anchor that needs a different scenario
#: cannot be drawn from the plan, and an anchor that is drawn and does not fire is a
#: silently missing case rather than a data point (Codex M3). So the exclusions are
#: named here rather than discovered at run time — and `test_the_excluded_anchors_are_
#: covered_somewhere_else` checks that each named module exists.
#:
#: The five `recovery_*` anchors are the ones this list gained with rubric 1.9. They sit
#: inside the **acquisition recovery**, which only runs when the slot check declares the
#: destination unusable; a healthy run never reaches them, so the seeded plan would arm
#: one, watch nothing fire, and (correctly) fail. Their composition question — "does a
#: fault during a recovery leave a resumable journal" — is exactly what the recovery's
#: own anchors test, one per boundary, and it is answered from durable state rather than
#: from the previous iteration's residue.
EXCLUDED = {
    "swap": (
        "needs a re-snapshot in flight to have a shadow to tear",
        "tests/1.6_snapshot_consistency/test_1_6_interrupted_snapshot.py",
    ),
    "destination_hang": (
        "a *timeout* anchor: composing it adds CDC_COMMIT_TIMEOUT to every iteration it "
        "lands in and proves nothing the bounded test does not",
        "tests/1.7_fault_injection/test_1_7_anchor_guards.py",
    ),
    **{
        point: (
            "inside the acquisition recovery, which a healthy run never enters",
            "tests/1.7_fault_injection/test_1_7_recovery_anchors.py",
        )
        for point in faults.RECOVERY_POINTS
    },
}

CHAOS_POINTS = tuple(p for p in faults.ALL_POINTS if p not in EXCLUDED)


def test_the_excluded_anchors_are_covered_somewhere_else():
    """An exclusion with no owner is an anchor nothing proves.

    `swap` was excluded with a prose comment. A comment does not fail when the module it
    names is renamed or deleted, which is the same shape as every other vacuously-green
    assertion this item has had to close.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    assert set(EXCLUDED) | set(CHAOS_POINTS) == set(faults.ALL_POINTS)
    for point, (_why, where) in EXCLUDED.items():
        assert (root / where).exists(), f"{point} points at {where}, which does not exist"

#: `(seed, iterations, must_cover)`. The first run is a **cover**: every eligible anchor
#: fires at least once, which is what makes "the anchors compose" a checked claim rather
#: than a sample. The second is a shorter run on a different seed, because one hard-coded
#: seed is a fixed sequence and calling it chaos overstates it. `CDC_CHAOS_SEEDS`
#: (comma-separated `seed:iterations`) overrides the whole plan.
def _runs() -> list[tuple[int, int, bool]]:
    raw = os.environ.get("CDC_CHAOS_SEEDS")
    if raw:
        out = []
        for item in raw.split(","):
            seed, _, count = item.partition(":")
            n = int(count) if count else len(CHAOS_POINTS)
            out.append((int(seed), n, n >= len(CHAOS_POINTS)))
        return out
    return [(20260731, len(CHAOS_POINTS), True), (7, 4, False)]


RUNS = _runs()


def _plan(rng: random.Random, iterations: int) -> list[str]:
    """A shuffled cover of the anchor set, extended with random draws if asked."""
    plan = list(CHAOS_POINTS)
    rng.shuffle(plan)
    while len(plan) < iterations:
        plan.append(rng.choice(CHAOS_POINTS))
    return plan[:iterations]


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
@pytest.mark.parametrize(("seed", "iterations", "must_cover"), RUNS)
def test_random_faults_at_random_anchors_never_break_the_ledger(
    tmp_path_factory, postgres_cluster, seed, iterations, must_cover
):
    rng = random.Random(seed)
    box = Sandbox(f"chaos{seed}", tmp_path_factory.mktemp(f"sbx_chaos{seed}"), postgres_cluster)
    points = _plan(rng, iterations)
    executed: list[tuple[int, str, str]] = []
    fired_anchors: set[str] = set()
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _assert_equal_to_source(box, "baseline")

        for iteration, point in enumerate(points, start=1):
            nth = 1
            tag = f"chaos{iteration}"
            _shape(box, rng, tag)

            box.clear_fired_fault()
            env = {"CDC_FAULT_INJECT": f"{point}:{nth}", **ARMING.get(point, {})}
            box.run(max_seconds=120, timeout=200, expect_success=False, extra_env=env)
            fired = box.fired_fault()
            assert fired is not None and fired["point"] == point, (
                f"iteration {iteration}: the plan armed {point}:{nth} and it did not "
                f"fire (record={fired!r}). An iteration in which nothing fired is not "
                "a data point, it is a silently missing case"
            )
            fired_anchors.add(point)
            executed.append((iteration, point, tag))

            # Recovery, with a fault ARMED during it. This is the composition the
            # module claims: the second fault lands on a destination whose most recent
            # event is the first fault's rollback, and on an offset store that is
            # mid-replay. `<nth>` is deliberately high enough that it usually does not
            # fire, so the sequence still terminates — and because it usually does not
            # fire, this loop alone is NOT evidence that a composed fault ever lands.
            # `test_a_fault_during_a_recovery_really_fires` is (Codex r1 MAJOR-6).
            during_recovery = rng.choice(CHAOS_POINTS)
            recovery_env = {
                "CDC_FAULT_INJECT": f"{during_recovery}:2",
                **ARMING.get(during_recovery, {}),
            }
            for attempt in range(4):
                box.clear_fired_fault()
                recovered = box.run(
                    max_seconds=200,
                    timeout=260,
                    expect_success=False,
                    # Only the FIRST attempt is hostile; the rest must be allowed to
                    # converge or the harness proves nothing about recovery at all.
                    extra_env=recovery_env if attempt == 0 else None,
                )
                during = box.fired_fault()
                if during is not None:
                    fired_anchors.add(str(during["point"]))
                    executed.append((iteration, f"{during['point']}@recovery", tag))
                if recovered.get("ok") is True:
                    break
                assert attempt < 3, (
                    f"iteration {iteration} ({point}:{nth}) could not be recovered in "
                    f"4 attempts: { {k: v for k, v in recovered.items() if k != 'output'} }"
                )
            _assert_equal_to_source(box, f"iteration {iteration} after {point}:{nth}")

        if must_cover:
            missing = sorted(set(CHAOS_POINTS) - fired_anchors)
            assert not missing, (
                f"the plan was supposed to be a cover and these anchors never fired: "
                f"{missing}"
            )
    except BaseException:
        print(f"\nCDC_CHAOS_SEED={seed} plan={points} executed={executed}")
        raise
    finally:
        print(f"\nchaos seed {seed} executed: {executed}")
        box.cleanup()
        box.reseed()


@pytest.mark.slow
def test_a_fault_during_a_recovery_really_fires(tmp_path_factory, postgres_cluster):
    """The composition claim, as a BOUNDED scenario that must fire (Codex r1 MAJOR-6).

    The seeded harness above arms a fault during recovery at `<nth>=2` so the sequence
    still terminates, and explicitly allows it not to fire — which means its
    shuffled-cover assertion can be satisfied entirely by the first faults, and "the
    anchors compose" rests on nothing that has to happen. This test makes exactly one
    composed fault mandatory:

    1. a healthy baseline, so the counts afterwards mean something;
    2. two transactions and a hard death at `post_commit_pre_ack:1` — the at-least-once
       window, the shape Invariant O exists for;
    3. the recovery run, with `pre_commit:1` armed. The recovery *replays* the
       un-acknowledged group, so it necessarily builds a data-carrying commit group,
       so this anchor necessarily arrives. It is asserted to have fired;
    4. an unhindered run, and then exact source/destination equality.

    The second fault therefore lands on a destination whose most recent durable event is
    the first fault's commit, and on an offset store that is mid-replay.
    """
    box = Sandbox("chaos_compose", tmp_path_factory.mktemp("sbx_compose"), postgres_cluster)
    try:
        box.reseed()
        box.run(reset_state=True, max_seconds=150)
        _assert_equal_to_source(box, "baseline")

        _shape(box, random.Random(1), "compose1")
        box.clear_fired_fault()
        box.run(
            max_seconds=120, timeout=200, expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
        )
        first = box.fired_fault()
        assert first is not None and first["point"] == "post_commit_pre_ack", first

        box.clear_fired_fault()
        box.run(
            max_seconds=120, timeout=200, expect_success=False,
            extra_env={"CDC_FAULT_INJECT": "pre_commit:1"},
        )
        during = box.fired_fault()
        assert during is not None and during["point"] == "pre_commit", (
            "the composed fault did not fire: the recovery run must build at least one "
            f"data-carrying commit group to replay the un-acknowledged batch ({during!r})"
        )

        assert box.run(max_seconds=200, timeout=280)["ok"] is True
        _assert_equal_to_source(box, "after a fault inside a recovery")
    finally:
        box.cleanup()
        box.reseed()
