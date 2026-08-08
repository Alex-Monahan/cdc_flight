# P2b capability-gate record

Date: 2026-08-07  
Base: `main @ 2d4276e`  
Branch: `feature/2.4-2.6-type-handling`  
Environment: Homebrew PostgreSQL 18.1 on port `15432`; Debezium 3.6.0.Final;
MotherDuck/DuckDB runtime `v1.5.4`.

This is a stop record, not a production implementation. The reviewed P2b plan
requires stopping when raw unchanged-TOAST marker identity is unavailable. No
placeholder heuristic, source refetch, `REPLICA IDENTITY FULL` requirement, or
text/JSON fallback was added.

## Debezium wire capture

The probe used the real embedded engine against `cdc_source`, `app.documents`,
`pgoutput`, and a disposable replication slot. It enabled both
`key.converter.schemas.enable=true` and `value.converter.schemas.enable=true`.
The writer produced, after the connector started:

1. `UPDATE app.documents SET title = ... WHERE id = 2`, where `body` is a
   64,000-byte TOAST value omitted from the WAL.
2. An ordinary insert whose real PostgreSQL `body` value was exactly
   `__debezium_unavailable_value`.

Observed `SourceRecord` data for both events:

| event | operation | body class | body value | marker identity | source-record headers | change-event headers |
| --- | --- | --- | --- | --- | ---: | ---: |
| unchanged TOAST | `u` | `java.lang.String` | `__debezium_unavailable_value` | `false` | 4 context headers only | 0 |
| ordinary source string | `c` | `java.lang.String` | `__debezium_unavailable_value` | `false` | 4 context headers only | 0 |

The body Connect schema was `STRING` with no name, documentation, or
parameters in both cases. The four source-record headers were only
`__debezium.context.connectorLogicalName`, `taskId`, `connectorName`, and
`runId`; none carried column-level TOAST state. The direct Java check
`UnchangedToastedReplicationMessageColumn.isUnchangedToastedValue(body)` was
false for both values.

The result matches the inspected Debezium implementation: PostgreSQL's
identity marker is consumed by `PostgresValueConverter` and converted to
`UnchangedToastedPlaceholder.getToastPlaceholderString()` before the
`SourceRecord` is constructed. A converter downstream of that point cannot
recover identity.

The disposable probe slot was dropped after capture, and probe rows were
removed. No `cdc_p2b_*` slots remain.

## MotherDuck/native probes

The actual runtime accepted native `LIST`, nested `LIST`, `STRUCT`, `MAP`,
`ENUM`, `UNION`, `UNION(...)[]`, and `BIGNUM` declarations. Tagged `UNION`
values and tagged NULLs preserved their member tags. A numeric inner union
preserved finite `DECIMAL` values and `NaN`, `+Inf`, and `-Inf` in its `DOUBLE`
member. A `UNION` primary key was rejected, and a transactional shadow
drop/rename rollback preserved the original live table. Array and mixed-union
inserts required explicit casts to the declared union type.

These probes pass the target-engine capability checks, but they do not justify
shipping 2.4/2.5/2.6 without a lossless TOAST marker representation.

## Gate decision

**REDESIGN GATE HIT: raw Debezium marker identity unavailable.** The reviewed
plan's §10 stop condition applies. Work therefore stopped before production
type machinery, RowPatch, UNION fencing, or rubric suites were added. Rubric
2.4, 2.5, and 2.6 are not claimed as implemented.

