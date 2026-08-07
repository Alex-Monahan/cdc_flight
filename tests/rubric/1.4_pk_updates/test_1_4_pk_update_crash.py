"""Rubric 1.4 x 1.7: a hard crash in the commit→ack window of a PK-update group.

`post_commit_pre_ack` is the at-least-once window made exact: the destination
transaction that moved the row has committed, and Debezium has *not* been
acknowledged, so the whole transaction replays on the next run. For a primary-key
update that replay is the dangerous one — the `d` of the old key and the `c` of the
new key both come back, and a fold that mishandled either would leave the row under
two keys or under none.

Marked `slow`: a crash plus two recovery runs is ~90 s. The default-suite guard for
the same property is `test_1_4_pk_update_fold.py`'s fault anchors, which reach
`begin` / `mid_apply` / `pre_commit` in-process.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.slow, pytest.mark.e2e]

CUSTOMERS = "cdcflight_app_customers"


@pytest.fixture(scope="module")
def crashed_pk_update(sandbox):
    box = sandbox
    box.reseed()
    box.run(reset_state=True, max_seconds=150)

    # Customer 3 has no orders, so Postgres allows the key change (the FK on
    # app.orders has no ON UPDATE clause).
    box.sql("UPDATE app.customers SET id = 9001 WHERE id = 3")

    crashed = box.run(
        max_seconds=150,
        expect_success=False,
        extra_env={"CDC_FAULT_INJECT": "post_commit_pre_ack:1"},
    )
    if crashed["returncode"] != 137:
        raise RuntimeError(
            "the fault did not fire at post_commit_pre_ack:1 (returncode "
            f"{crashed['returncode']}); the PK-update crash case is vacuous without it"
            f"\n--- tail ---\n{crashed.get('output', '')[-2000:]}"
        )
    recovered = box.run(max_seconds=150)
    try:
        yield {"box": box, "crashed": crashed, "recovered": recovered}
    finally:
        box.reseed()


def test_the_replayed_transaction_leaves_one_row_under_the_new_key(crashed_pk_update):
    box = crashed_pk_update["box"]
    assert box.duck_query(f"SELECT id FROM {box.table(CUSTOMERS)} WHERE id IN (3, 9001)") == [
        (9001,)
    ]


def test_the_replay_did_not_duplicate_any_key(crashed_pk_update):
    box = crashed_pk_update["box"]
    assert (
        box.duck_query(
            f"SELECT id, count(*) FROM {box.table(CUSTOMERS)} GROUP BY id HAVING count(*) > 1"
        )
        == []
    )


def test_the_destination_still_matches_postgres(crashed_pk_update):
    box = crashed_pk_update["box"]
    assert box.duck_query(f"SELECT id, name FROM {box.table(CUSTOMERS)} ORDER BY id") == (
        box.pg_query("SELECT id, name FROM app.customers ORDER BY id")
    )


def test_the_recovery_run_was_clean(crashed_pk_update):
    assert crashed_pk_update["recovered"]["ok"] is True
