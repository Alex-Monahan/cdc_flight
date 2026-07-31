# tests/1.0_engine_error_propagation

**TODO item 1.0(b).** Not a rubric item on its own; it *gates the measurement* of
rubric 1.8, 4.1, 4.2, 4.3 and 6.2, because every one of those failure modes
currently exits 0.

## The defect

`DebeziumJsonEngine.run()` (pydbzengine) builds the engine with
`DebeziumEngine.create(...).using(props).notifying(consumer).build()` — it never
registers a `CompletionCallback`. Debezium 3.6's `AsyncEmbeddedEngine` reports a
startup or streaming failure by calling
`completionCallback.handle(false, message, error)`
(`repos/debezium/debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java:796-804`)
and then returning normally from `run()`. Nothing is thrown on the caller's
thread, so `run_engine_bounded`'s `error_box` stays empty, the loop falls out of
`while thread.is_alive()` into the `else:` branch (`stop_reason:
"engine_finished"`), and the process prints a success summary and exits 0.

Probe `probes/p11_dropped_slot_logs.py` proved it: with the replication slot
dropped, the engine logs

```
WARN  BaseSourceTask - Last recorded offset is no longer available on the server.
ERROR AsyncEmbeddedEngine - 1 task(s) out of 1 failed to start.
```

while the pipeline reports `{"records": 0, "stop_reason": "engine_finished",
"returncode": 0}`.

## What these tests assert

| test | asserts |
|---|---|
| `test_healthy_run_reports_success` | a healthy run still exits 0 with a summary — guards against an over-eager failure detector |
| `test_dropped_slot_surfaces_as_a_failure` | slot dropped externally ⇒ non-zero exit, `stop_reason: "engine_error"`, and the Debezium message ("no longer available on the server") in the run summary and on stderr |
| `test_corrupt_offset_surfaces_as_a_failure` (`slow`) | a corrupted offset file ⇒ non-zero exit and a Debezium error message |

These are **target-behaviour** tests for a defect that is fixed in the same
commit range, so they are expected to *pass*. They are not xfail-marked.

## Conventions used by all `tests/<rubric item>_*/` suites

1. **Gap pins** — plain tests whose assertions encode *today's broken*
   behaviour, named `test_gap_*`. They pass now. When the fix lands they start
   failing, which is the signal to delete them and flip the matching target
   test.
2. **Target behaviour** — tests named `test_target_*`, marked
   `@pytest.mark.xfail(reason=..., strict=True)`. `strict=True` means an
   unexpected pass is a *failure*, so the moment the applier implements the
   behaviour, CI forces the marker (and the paired gap pin) to be removed.
   Nothing silently drifts.
3. **Cost** — the scenario (several 20-30 s pipeline runs) is built once per
   module in a module-scoped fixture; individual tests only query it.
4. **`slow`** — real `kill -9` / long fault-injection runs carry
   `@pytest.mark.slow` and are deselected by `make test`. Run them with
   `make test-slow`. Every slow test has a fast deterministic counterpart in the
   default suite so regressions are still caught.
