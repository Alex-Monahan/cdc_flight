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
`SELECT DISTINCT` cannot rescue the data either — `test_gap_dedup_is_impossible`
pins exactly that.

The applier must give keyless tables a **synthetic identity**. The envelope
already carries everything needed: `(lsn, tx_id, ordinal-within-transaction)` is
unique per change event and free. See `docs/adr/0001-transactional-applier.md`.

## Fault injection used here

Shared with 1.1 via the session-scoped `crash_replay` fixture in
`tests/conftest.py` (see `tests/1.1_exactly_once_pk/README.md` for what it does
and why the deterministic offset rollback is equivalent to the `kill -9` that
`probes/p13_offset_replay.py` recorded).

## What the tests assert

| test | today | after the applier lands |
|---|---|---|
| `test_gap_replay_duplicates_keyless_rows` | **passes** — the 60 replayed readings appear twice | starts failing; delete it |
| `test_gap_dedup_is_impossible_without_a_key` | **passes** — duplicates are byte-identical | delete it |
| `test_no_readings_are_lost` | passes | must keep passing |
| `test_target_exactly_once_nopk` | **xfail(strict)** | must pass, then drop the marker |
| `test_target_synthetic_key_is_present` | **xfail(strict)** | must pass, then drop the marker |

Conventions are described in `tests/1.0_engine_error_propagation/README.md`.
