"""Rubric 1.7 / 1.1 / 1.2 — a crash at EVERY protocol anchor loses nothing and
duplicates nothing.

`crash_replay` proves the single most dangerous window
(`post_commit_pre_ack`). This module walks the whole commit-group protocol, one
anchor at a time, and asserts the same two things at each: the destination holds
**exactly** the change events the source produced, and every one of them is
there.

| anchor | destination transaction | Debezium offset store | expected |
|---|---|---|---|
| `begin` | open, empty | untouched | rolled back, replay, no dup |
| `mid_apply` | open, partially written | untouched | rolled back, replay, no dup |
| `pre_commit` | fully written, uncommitted | untouched | rolled back, replay, no dup |
| `post_commit_pre_ack` | **committed** | untouched (stale) | already durable, resume point rebuilds the file, NO replay |
| `post_ack` | committed | flushed, slot not confirmed | nothing to redo |

The "no duplicates" half is what the baseline could not do: it duplicated a
whole batch at `post_commit_pre_ack` (measured: 402 048 rows / 400 000 distinct,
`probes/p13`). The "no loss" half is what a naive fix breaks.

Every scenario writes its rows in ONE Postgres transaction across a keyed table
and a keyless one, so a single run exercises 1.1 and 1.2 together. The keyless
table is the decisive evidence: a current-state merge on a primary key can hide a
double delivery, an append-keyed-on-event-identity table cannot.
"""

from __future__ import annotations

import pytest

CUSTOMERS = '"cdc_raw"."cdcflight_app_customers"'
READINGS = '"cdc_raw"."cdcflight_app_sensor_readings"'

#: (anchor, tag, rows). Kept small on purpose: the point is the *anchor*, not the
#: volume, and the suite has a 10-minute budget.
ANCHORS = [
    ("begin", "fb", 12),
    ("mid_apply", "fm", 12),
    ("pre_commit", "fp", 12),
    ("post_commit_pre_ack", "fa", 12),
    ("post_ack", "fk", 12),
]


@pytest.fixture(scope="module")
def crashed_at(sandbox) -> dict:
    """One baseline snapshot, then one crash + recovery cycle per anchor."""
    sandbox.reseed()
    sandbox.run(reset_state=True, max_seconds=150)

    results: dict[str, dict] = {}
    for anchor, tag, rows in ANCHORS:
        sandbox.sql(
            [
                "INSERT INTO app.customers (name, email) SELECT "
                f"'{tag}-c-' || i, '{tag}-c-' || i || '@example.com' "
                f"FROM generate_series(1, {rows}) i",
                "INSERT INTO app.sensor_readings (sensor_id, value, unit) SELECT "
                f"'{tag.upper()}', i * 1.5, 'C' FROM generate_series(1, {rows}) i",
            ],
            one_transaction=True,
        )
        crashed = sandbox.run(
            max_seconds=150,
            expect_success=False,
            extra_env={"CDC_FAULT_INJECT": f"{anchor}:1"},
        )
        recovered = sandbox.run(max_seconds=150)
        results[anchor] = {"crashed": crashed, "recovered": recovered, "rows": rows, "tag": tag}
    return {"box": sandbox, "results": results}


@pytest.mark.parametrize("anchor", [a for a, _, _ in ANCHORS])
def test_fault_actually_fired(crashed_at, anchor):
    """Guard: a fault that did not fire makes every assertion below vacuous."""
    outcome = crashed_at["results"][anchor]
    assert outcome["crashed"]["returncode"] == 137, (
        f"the fault at {anchor} did not fire; the run exited "
        f"{outcome['crashed']['returncode']}"
    )
    assert outcome["recovered"]["returncode"] == 0, outcome["recovered"]


@pytest.mark.parametrize("anchor", [a for a, _, _ in ANCHORS])
def test_no_loss_at_anchor(crashed_at, anchor):
    """NO LOSS - every source row reached the destination after the restart."""
    box = crashed_at["box"]
    outcome = crashed_at["results"][anchor]
    tag, rows = outcome["tag"], outcome["rows"]
    missing = box.scalar(
        f"SELECT count(*) FROM generate_series(1, {rows}) g(i) "
        f"WHERE NOT EXISTS (SELECT 1 FROM {CUSTOMERS} c WHERE c.name = '{tag}-c-' || g.i)"
    )
    assert missing == 0, f"crash at {anchor} lost {missing} of {rows} keyed rows"
    readings = box.scalar(
        f"SELECT count(DISTINCT value) FROM {READINGS} WHERE sensor_id = '{tag.upper()}'"
    )
    assert readings == rows, f"crash at {anchor} lost {rows - readings} keyless rows"


@pytest.mark.parametrize("anchor", [a for a, _, _ in ANCHORS])
def test_no_duplicates_at_anchor(crashed_at, anchor):
    """NO DUPLICATES - and measured on the table where a merge cannot hide one.

    `cdcflight_app_sensor_readings` has no primary key, so its rows are keyed on
    `cdcf_event_id` and every delivery of a change event is a row. If the crash
    caused the batch to be applied twice, this count is `2 * rows`.
    """
    box = crashed_at["box"]
    outcome = crashed_at["results"][anchor]
    tag, rows = outcome["tag"], outcome["rows"]

    keyless = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE sensor_id = '{tag.upper()}'")
    assert keyless == rows, (
        f"crash at {anchor} delivered {keyless} keyless change events, expected {rows}"
    )
    keyed, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT id) FROM {CUSTOMERS} WHERE name LIKE '{tag}-c-%'"
    )[0]
    assert (keyed, distinct) == (rows, rows), (
        f"crash at {anchor}: {keyed} rows / {distinct} distinct ids, expected {rows}/{rows}"
    )
    dupe_events = box.scalar(
        f"SELECT count(*) FROM (SELECT cdcf_event_id FROM {READINGS} "
        f"WHERE sensor_id = '{tag.upper()}' GROUP BY 1 HAVING count(*) > 1)"
    )
    assert dupe_events == 0, f"crash at {anchor} applied {dupe_events} events twice"


@pytest.mark.parametrize("anchor", [a for a, _, _ in ANCHORS])
def test_slot_never_outruns_the_destination_at_anchor(crashed_at, anchor):
    """ADR 0001 §4.7 - Invariant O, sampled after every crash/recovery cycle."""
    box = crashed_at["box"]
    durable = box.scalar(
        "SELECT last_lsn FROM _cdc_flight.debezium_offsets LIMIT 1"
    )
    confirmed = box.pg_query(
        "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots WHERE slot_name = %s",
        (box.slot,),
    )
    assert confirmed and confirmed[0][0] is not None, f"slot {box.slot} is gone"
    assert confirmed[0][0] <= durable, (
        f"after a crash at {anchor} the slot confirmed {confirmed[0][0]}, ahead of the "
        f"durable destination offset {durable}"
    )


def test_uncommitted_anchors_leave_nothing_behind(crashed_at):
    """The three pre-COMMIT anchors must not have committed a partial group.

    A crash before `COMMIT` rolls the destination transaction back, so the rows
    the crashed run had written are gone and the *recovery* run is what put them
    there. `commit_log` is the audit trail: the events of one Postgres
    transaction may never straddle two commit groups, whatever the crash did.
    """
    box = crashed_at["box"]
    for anchor, tag, _rows in ANCHORS:
        commits = box.duck_query(
            f"SELECT DISTINCT cdcf_commit_id FROM {CUSTOMERS} WHERE name LIKE '{tag}-c-%' "
            f"UNION SELECT DISTINCT cdcf_commit_id FROM {READINGS} "
            f"WHERE sensor_id = '{tag.upper()}'"
        )
        assert len(commits) == 1, (
            f"the transaction written before the {anchor} crash is spread over "
            f"{len(commits)} commit groups: {commits}"
        )


def test_the_committed_anchors_did_not_replay(crashed_at):
    """`post_commit_pre_ack` and `post_ack` are the Invariant-O payoff.

    The crashed run had already committed, so the resume point went to the
    destination with the rows. Start-up reconciliation rebuilds `offsets.dat`
    from it and the connector resumes *after* the batch: the recovery run has
    nothing to re-apply. Under the baseline this was the case that duplicated a
    whole batch.
    """
    for anchor in ("post_commit_pre_ack", "post_ack"):
        recovered = crashed_at["results"][anchor]["recovered"]
        assert recovered["applied_events"] == 0, (
            f"after a crash at {anchor} the recovery run re-applied "
            f"{recovered['applied_events']} events; they were already durable"
        )
