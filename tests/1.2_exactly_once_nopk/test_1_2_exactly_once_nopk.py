"""Rubric 1.2 - exactly-once delivery for tables WITHOUT a primary key.

See README.md for the failure mode and the test conventions.

Review note (Opus M6 / Codex 8). Every assertion of the form "count(*) == N"
over rows whose values happen to be distinct is satisfiable by a `SELECT
DISTINCT`. For a keyless table that is not merely a weak test, it is a test that
passes on a **wrong** implementation: two legitimately identical sensor readings
are two facts, and collapsing them is data loss. The decisive case is therefore
`test_target_identical_source_rows_both_survive` - two byte-identical rows
inserted on purpose, both of which must exist, while the crash-replay copies of
them must not. Nothing that deduplicates by row content can satisfy both halves.
"""

from __future__ import annotations

import pytest

READINGS = '"cdc_raw"."cdcflight_app_sensor_readings"'
REPLAY_FILTER = "sensor_id = 'REPLAY'"


def _identical(crash_replay) -> str:
    """SQL predicate selecting the deliberately-identical source rows.

    Read off the fixture rather than imported from `conftest`: `tests/` is not a
    package, so a cross-module import here depends on pytest's sys.path
    insertion order.
    """
    return f"sensor_id = '{crash_replay['identical_sensor']}'"

TARGET = (
    "rubric 1.2: exactly-once for keyless tables needs the transactional applier "
    "plus a stable event identity from the envelope's transaction block (ADR 0001 §6)"
)
TARGET_KEY = (
    "rubric 1.2: keyless tables need a stable synthetic identity "
    "(txn commit lsn, txId, transaction.total_order) - ADR 0001 §6"
)


def test_gap_replay_duplicates_keyless_rows(crash_replay):
    """PIN OF TODAY'S BROKEN BEHAVIOUR - delete once the applier lands."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {REPLAY_FILTER}")
    assert rows > n, (
        f"expected more than {n} rows after the offset rollback (at-least-once); "
        f"got {rows}"
    )


def test_gap_dedup_would_destroy_real_data(crash_replay):
    """The duplicates carry no distinguishing field - AND neither do two real rows.

    Renamed and strengthened from `test_gap_dedup_is_impossible_without_a_key`,
    which used rows whose values were all distinct and therefore only showed
    replay duplication, not the claimed ambiguity (Codex 13).

    Here both halves are demonstrated at once:

    * the replayed rows are byte-identical to their originals, including every
      CDC metadata column, so nothing downstream can tell them apart;
    * the source *also* contains two genuinely identical rows, so the only
      available downstream fix (`SELECT DISTINCT`) would delete real data.
    """
    box = crash_replay["box"]
    all_cols = "sensor_id, reading_at, value, unit, dbz_op, dbz_lsn, dbz_tx_id"
    user_cols = "sensor_id, reading_at, value, unit"

    # (a) A replayed event is byte-identical to its original in EVERY column,
    #     CDC metadata included, so nothing can tell the copies apart.
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT ({all_cols})) FROM {READINGS} WHERE {REPLAY_FILTER}"
    )[0]
    assert total > distinct, (
        "expected byte-identical duplicates; if they now differ, a synthetic key "
        "may have been added - update RUBRIC_STATUS"
    )

    # (b) The two deliberately identical source rows are indistinguishable in
    #     every column a consumer could reasonably dedupe on, so `SELECT DISTINCT`
    #     would delete one of them. They are two facts, not one.
    #
    #     MEASURED (2026-07-30): they DO differ in `dbz_lsn` - two INSERTs in one
    #     transaction got two WAL positions. That is not a general guarantee:
    #     several Debezium events can share one LSN, which is why the connector
    #     keeps a separate `lsn_proc` offset field
    #     (`PostgresOffsetContext.java:38`) and why ADR 0001 §6 builds the event
    #     identity from `transaction.total_order` rather than from the LSN alone.
    ident_total, ident_user_shapes = box.duck_query(
        f"SELECT count(*), count(DISTINCT ({user_cols})) FROM {READINGS} "
        f"WHERE {_identical(crash_replay)}"
    )[0]
    assert ident_total >= crash_replay["identical"], ident_total
    assert ident_user_shapes == 1, (
        "the two deliberately identical source rows should be indistinguishable "
        f"in their user-visible columns; got {ident_user_shapes} distinct shapes"
    )


def test_no_readings_are_lost(crash_replay):
    """Regression guard: at-least-once must never decay into at-most-once."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    distinct_values = box.scalar(
        f"SELECT count(DISTINCT value) FROM {READINGS} WHERE {REPLAY_FILTER}"
    )
    assert distinct_values == n, f"{n - distinct_values} readings never arrived"


def test_identical_source_rows_are_never_lost(crash_replay):
    """Both deliberately-identical rows must exist, today and after the applier lands.

    Not xfail: this must be true of *every* implementation, including the
    baseline. It is the assertion that fails the day someone "fixes" 1.2 with a
    `SELECT DISTINCT`.
    """
    box = crash_replay["box"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {_identical(crash_replay)}")
    assert rows >= crash_replay["identical"], (
        f"expected at least {crash_replay['identical']} identical readings "
        f"(the source inserted that many on purpose), got {rows}"
    )


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_identical_source_rows_both_survive(crash_replay):
    """TARGET BEHAVIOUR - the decisive keyless case.

    Two genuinely identical rows were inserted in the source. After a crash in
    the at-least-once window and a replay:

    * both must still exist (deduplication by content is WRONG here);
    * there must be exactly two (the replayed copies must NOT).

    An implementation that deduplicates fails the first half; today's
    at-least-once pipeline fails the second. Only exactly-once delivery of the
    change events passes both.
    """
    box = crash_replay["box"]
    expected = crash_replay["identical"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {_identical(crash_replay)}")
    assert rows == expected, (
        f"expected exactly {expected} identical readings - both real rows and no "
        f"crash-replay copies - got {rows}"
    )


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_change_event_ledger_balances(crash_replay):
    """TARGET BEHAVIOUR - the destination holds exactly the events the source produced."""
    box = crash_replay["box"]
    expected = crash_replay["readings"] + crash_replay["identical"]
    events = box.scalar(
        f"SELECT count(*) FROM {READINGS} "
        f"WHERE ({REPLAY_FILTER} OR {_identical(crash_replay)}) AND dbz_op = 'c'"
    )
    assert events == expected, (
        f"source produced {expected} keyless INSERT events, destination holds {events}"
    )


@pytest.mark.xfail(reason=TARGET, strict=True)
def test_target_exactly_once_nopk(crash_replay):
    """TARGET BEHAVIOUR - each keyless change event lands exactly once."""
    box = crash_replay["box"]
    n = crash_replay["readings"]
    rows = box.scalar(f"SELECT count(*) FROM {READINGS} WHERE {REPLAY_FILTER}")
    assert rows == n, f"expected exactly {n} rows, got {rows}"


@pytest.mark.xfail(reason=TARGET_KEY, strict=True)
def test_target_synthetic_key_is_present_and_unique(crash_replay):
    """TARGET BEHAVIOUR - a keyless table still gets a unique row identity."""
    box = crash_replay["box"]
    columns = {
        c
        for (c,) in box.duck_query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'cdc_raw' "
            "AND table_name = 'cdcflight_app_sensor_readings'"
        )
    }
    assert "cdcf_event_id" in columns, sorted(columns)
    total, distinct = box.duck_query(
        f"SELECT count(*), count(DISTINCT cdcf_event_id) FROM {READINGS}"
    )[0]
    assert total == distinct, "synthetic event id is not unique"

    # Uniqueness alone is satisfied by a random id, which would break the very
    # thing the id exists for. The two identical source rows must have *different*
    # ids (they are different events) ...
    ident_ids = box.scalar(
        f"SELECT count(DISTINCT cdcf_event_id) FROM {READINGS} WHERE {_identical(crash_replay)}"
    )
    assert ident_ids == crash_replay["identical"], (
        "two identical source rows must be two distinct events, so they need two "
        f"distinct cdcf_event_ids; got {ident_ids}"
    )


@pytest.mark.xfail(reason=TARGET_KEY, strict=True)
def test_target_event_identity_is_derived_not_random(crash_replay):
    """TARGET BEHAVIOUR - the identity is reproducible from the envelope.

    Replay stability is the whole reason for the synthetic id: a replayed event
    must be recognisable as the same event. Uniqueness in the final table cannot
    show that (random ids are unique too). Assert instead that the id is a
    function of the source coordinates that ADR 0001 §6 specifies, so it can be
    recomputed - and therefore matched - on a replay.
    """
    box = crash_replay["box"]
    mismatched = box.scalar(
        f"SELECT count(*) FROM {READINGS} "
        "WHERE cdcf_event_id IS NULL "
        "   OR NOT ends_with(cdcf_event_id, "
        "        dbz_tx_id::VARCHAR || ':' || cdcf_total_order::VARCHAR)"
    )
    assert mismatched == 0, (
        f"{mismatched} rows have a cdcf_event_id that does not end in "
        "(txId, transaction.total_order), so it is not recomputable from the "
        "envelope and cannot be matched on a replay (ADR 0001 §6)"
    )
