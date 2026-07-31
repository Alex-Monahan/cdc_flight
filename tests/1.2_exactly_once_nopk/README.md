# tests/1.2_exactly_once_nopk

**Rubric 1.2** — *Delivery guarantees for tables WITHOUT a primary key.*
`at-most-once=1, at-least-once=3, exactly-once=5`. Baseline score: **3**.

Table under test: `app.sensor_readings` (no PK, `REPLICA IDENTITY FULL`).

## Why this is a separate item from 1.1

The delivery machinery is identical, so the same offset-flush-window crash
duplicates rows here too. What is *different* is that there is **no key to
deduplicate on afterwards**: a replayed batch of sensor readings is
indistinguishable from six genuinely identical readings. Any fix that leans on
`INSERT … ON CONFLICT` or a merge key therefore does not apply, and a downstream
`SELECT DISTINCT` cannot rescue the data either — it would also collapse
legitimately identical readings, which is *data loss*.
`test_gap_dedup_would_destroy_real_data` pins both halves of that at once.

The applier must give keyless tables a **stable synthetic identity**. Review
correction: it cannot be built from `source.sequence` — that field is
`[lastCommitLsn, currentLsn]`, not an event ordinal
(`repos/debezium/.../postgresql/SourceInfo.java:180-196`), and several events can
share one LSN. The ordinal is `transaction.total_order` from the envelope's
transaction block, available once `provide.transaction.metadata=true`
(`AbstractTransactionStructMaker.java:41-43`). See
`docs/adr/0001-transactional-applier.md` §6.

## The decisive case

The scenario inserts **two byte-identical rows on purpose** — same `sensor_id`,
`reading_at`, `value` and `unit`. Every other assertion in this directory is
satisfiable by a `SELECT DISTINCT`; this one is not:

* both real rows must survive (`test_identical_source_rows_are_never_lost`, which
  is **not** xfail — it must hold of every implementation, including today's);
* and after a crash+replay there must be exactly two of them
  (`test_target_identical_source_rows_both_survive`).

An implementation that deduplicates by row content fails the first; today's
at-least-once pipeline fails the second. Only exactly-once delivery passes both.

## Fault injection used here

Shared with 1.1 via the session-scoped `crash_replay` fixture in
`tests/conftest.py` (see `tests/1.1_exactly_once_pk/README.md` for what it does
and why the deterministic offset rollback is equivalent to the `kill -9` that
`probes/p13_offset_replay.py` recorded).

## What the tests assert

| test | today | after the applier lands |
|---|---|---|
| `test_gap_replay_duplicates_keyless_rows` | **passes** — the 60 replayed readings appear twice | starts failing; delete it |
| `test_gap_dedup_would_destroy_real_data` | **passes** — duplicates are byte-identical, *and so are two real rows* | delete it |
| `test_no_readings_are_lost` | passes | must keep passing |
| `test_identical_source_rows_are_never_lost` | passes | **must keep passing** — this is the anti-dedup guard |
| `test_target_identical_source_rows_both_survive` | **xfail(strict)** | must pass, then drop the marker |
| `test_target_change_event_ledger_balances` | **xfail(strict)** | must pass, then drop the marker |
| `test_target_exactly_once_nopk` | **xfail(strict)** | must pass, then drop the marker |
| `test_target_synthetic_key_is_present_and_unique` | **xfail(strict)** | must pass, then drop the marker |
| `test_target_event_identity_is_derived_not_random` | **xfail(strict)** — uniqueness alone is satisfied by a random id, which would break replay matching | must pass, then drop the marker |

Conventions are described in `tests/1.0_engine_error_propagation/README.md`.
