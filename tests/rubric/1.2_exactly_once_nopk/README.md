# tests/rubric/1.2_exactly_once_nopk

**Rubric 1.2** — *Delivery guarantees for tables WITHOUT a primary key.*
`at-most-once=1, at-least-once=3, exactly-once=5`. Baseline score: **3**.
**Status: the target tests pass** — the transactional applier landed (ADR 0001).

Table under test: `app.sensor_readings` (no PK, `REPLICA IDENTITY FULL`).

## Why this is a separate item from 1.1

The delivery machinery is identical, so the same offset-flush-window crash
duplicates rows here too. What is *different* is that there is **no key to
deduplicate on afterwards**: a replayed batch of sensor readings is
indistinguishable from six genuinely identical readings. Any fix that leans on
`INSERT … ON CONFLICT` or a merge key therefore does not apply, and a downstream
`SELECT DISTINCT` cannot rescue the data either — it would also collapse
legitimately identical readings, which is *data loss*.
`test_gap_dedup_would_destroy_real_data` pinned both halves of that at once; it
was deleted when the applier landed, because it asserted the *broken* behaviour.

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
`tests/support/fixtures.py` (see `tests/rubric/1.1_exactly_once_pk/README.md` for what it does
and why the deterministic offset rollback is equivalent to the `kill -9` that
`probes/p13_offset_replay.py` recorded).

## What the tests assert

| test | status |
|---|---|
| ~~`test_gap_replay_duplicates_keyless_rows`~~ | DELETED — it asserted duplication, which no longer happens |
| ~~`test_gap_dedup_would_destroy_real_data`~~ | DELETED — same reason |
| `test_no_readings_are_lost` | passes |
| `test_identical_source_rows_are_never_lost` | passes — the anti-dedup guard |
| `test_target_identical_source_rows_both_survive` | **passes** (marker removed) |
| `test_target_change_event_ledger_balances` | **passes** (marker removed) |
| `test_target_exactly_once_nopk` | **passes** (marker removed) |
| `test_target_synthetic_key_is_present_and_unique` | **passes** (marker removed) |
| `test_target_event_identity_is_derived_not_random` | **passes** (marker removed) |

## How it is met

Keyless tables are keyed on `cdcf_event_id` =
`"<event lsn>:<source.txId>:<transaction.total_order>"` and applied as an
append-only changelog, so:

* two genuinely identical source rows are two different *events* and keep two
  different ids — deduplication by content is impossible by construction;
* a replayed event recomputes the *same* id, and the applier's unit-level fence
  drops the whole replayed unit before it is ever applied.

ADR 0001 §15/A1 records the correction that made this work: Debezium 3.6's
envelope `transaction.id` is `"<txId>:<lsn>"` and changes per event, so the
stable transaction identifier is `source.txId`. `transaction.total_order` is a
genuine 1-based ordinal and is used as specified.

Conventions are described in `tests/rubric/1.0_engine_error_propagation/README.md`.
