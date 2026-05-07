# Phase I.6 string MIN/MAX hardening plan

## Status

**S.1 through S.6 shipped.**  The follow-up fixes scoped by this
document have all landed.  Remaining work is broader-surface coverage
(join aggregation, redistribute aggregation, and CTE materialisation
consumption of string MIN/MAX outputs) tracked under the F.4
follow-up plan rather than this document.

| Phase | Commit | Scope |
|-------|--------|-------|
| Initial test path | `9efdc033e9c` | First end-to-end string MIN/MAX path: AggInterpreter assertion-allow for string types, JoinAggInterpreter parallel relax, kernel optimizer no longer rewrites string `kOpMax` / `kOpMin` to BIGINT variants, NdbAggregator parse path tolerant of string types, narrow single-partition `testVarcharMinMax` block test |
| S.1 grouped ownership | `d96e01add1d` | Grouped `AGG_CHAR_RESULT` rows now resolve string slots before insert/merge; `ResultRecord::result_records_.len` is exactly `n_agg_results × sizeof(AggResItem)` regardless of appended payload; ownership cleanup symmetric across new-group / existing-group paths; `testVarcharMinMax` grouped section added |
| S.2 multi-source merge | `47841e2ca5e` | Real charset-aware string MIN/MAX merge on the API side using `NdbSqlUtil::getType(typeId).m_cmp`; deep-copy of winners into API-owned memory; replaced buffers freed; scalar and grouped paths share the same string-merge helper; multi-partition test no longer single-partition pinned |
| S.3 scratch-buffer guard | `7f1ced100f2` | `AggInterpreter` and `JoinAggInterpreter` bound `m_attr_read_pos` against the scratch buffer before advancing; clean interpreter-level error on overflow rather than silent corruption |
| S.4 builder validation | `81d1832a099` | API rejects unsupported string aggregate operations (`SUM`, etc.) at finalize time with `kErrUnsupportedStringOperation`; `LoadColumn(string)` still allowed because MIN/MAX is the supported destination |
| S.5 RonSQL printing | `a8db55676b7` | RonSQL aggregate result printer dispatches on aggregate output type and renders string MIN/MAX results via `NdbAggregator::Result::data_str()`; matches GROUP BY string column rendering; new `ronsql_minmax_string.test` |
| S.6 coverage extension | `09e5a8b90cd` | CHAR padding, VARCHAR varying lengths, Longvarchar payloads, NULL handling, scalar + grouped surfaces, RonSQL direct aggregate, multi-partition merge in `testVarcharMinMax`; mirrored in `ronsql_minmax_string` |

## Goal

Make string MIN/MAX correct for the surfaces already being enabled:

- scalar and grouped NdbAggregator results,
- single-source and multi-source / multi-partition result merging,
- CHAR, VARCHAR, and Longvarchar payloads,
- normal aggregation and join aggregation interpreter execution,
- RonSQL CTE use cases and direct string aggregate result printing.

The implementation must not rely on "first source wins" behavior, and
tests must not hide merge correctness by forcing a single partition
unless the test is explicitly scoped to a single-source path.

## Phase I.6-S.1: Fix grouped API result ownership and iteration  *(shipped, `d96e01add1d`)*

Problem:

- Grouped `AGG_CHAR_RESULT` rows currently store the full incoming
  aggregate result byte length as `ResultRecord::result_records_.len`.
  That length includes appended string payload bytes, so
  `FetchAggregationResult()` can walk past the real `AggResItem[]`
  slots and decode payload as bogus aggregate results.
- Grouped merge can copy the wire-format `AggResItem` for string slots
  before resolving the appended payload into API-owned memory.  That can
  preserve a kernel-local pointer value in the API process.

Fix:

- Resolve string slots for every incoming grouped string row before
  copying or merging it.
- Store only `n_agg_results * sizeof(AggResItem)` as the grouped public
  result length.
- Keep appended payload bytes out of the public iteration surface after
  they have been resolved.
- Ensure string ownership cleanup is correct for both new-group and
  existing-group paths.

Tests:

- Add grouped API tests for `MIN/MAX(VARCHAR)` and `MIN/MAX(CHAR)`.
- Verify that `FetchAggregationResult()` returns exactly the expected
  number of aggregate slots and then `end()`.
- Include NULL and empty-string rows so ownership and NULL-vs-empty
  handling are exercised.

## Phase I.6-S.2: Implement API-side string MIN/MAX merge  *(shipped, `47841e2ca5e`)*

Problem:

- Scalar and grouped merge paths currently preserve the first string
  result when more than one source contributes.  This is incorrect for
  multi-partition, multi-fragment, and redistributed execution.

Fix:

- Port the kernel string comparison semantics to the API merge path.
- Compare using the column charset/collation associated with the
  aggregate slot, not bytewise `memcmp`.
- Merge both scalar and grouped results:
  - NULL incoming value does not replace a non-NULL current winner.
  - Non-NULL incoming value replaces NULL current winner.
  - For MIN, replace when incoming is less than current.
  - For MAX, replace when incoming is greater than current.
- Deep-copy the winning string into API-owned memory and free replaced
  buffers.
- Make ownership explicit so scalar, grouped, and destructor cleanup all
  follow the same rule.

Tests:

- Remove the single-partition-only assumption from at least one string
  MIN/MAX test.
- Add a multi-partition table where the global MIN and MAX reside in
  different partitions/sources.
- Add grouped multi-partition tests where different groups get winners
  from different sources.
- Run the tests repeatedly enough to catch source-order dependence.

## Phase I.6-S.3: Add interpreter string scratch-buffer bounds checks  *(shipped, `7f1ced100f2`)*

Problem:

- The interpreters now advance `m_attr_read_pos` after loading string
  attributes so later loads do not overwrite captured string pointers.
  The advancement needs a capacity check against the attribute read
  buffer.

Fix:

- Add a shared or parallel bounds check in `AggInterpreter` and
  `JoinAggInterpreter` before advancing `m_attr_read_pos`.
- Return a clear interpreter error when the string load sequence would
  exceed the scratch buffer.
- Keep the behavior identical between both interpreter variants.

Tests:

- Add a test with several string aggregate slots in one program.
- Add a Longvarchar-heavy case that reaches the guard path.
- Verify the error path is deterministic and does not corrupt later
  result handling.

## Phase I.6-S.4: Narrow API builder validation by operation  *(shipped, `81d1832a099`)*

Problem:

- `TypeSupported()` now accepts string columns globally so
  `LoadColumn(string)` can feed MIN/MAX.  That also allows API users to
  build unsupported programs such as `SUM(string)`, which then fail only
  at kernel execution time.

Fix:

- Keep `LoadColumn()` support for CHAR / VARCHAR / Longvarchar.
- Reject string registers at build/finalize time for arithmetic
  aggregates and operations that do not define string semantics.
- Allow string registers only for MIN and MAX unless a future phase adds
  another explicit string operation.
- Return a clear API error instead of a late generic kernel failure.

Tests:

- Add negative builder tests for `SUM(CHAR)`, `SUM(VARCHAR)`, and
  `SUM(Longvarchar)`.
- Verify MIN/MAX over the same registers still finalizes and executes.

## Phase I.6-S.5: Add RonSQL string aggregate result printing  *(shipped, `a8db55676b7`)*

Problem:

- RonSQL prepares CTE virtual table string MIN/MAX outputs and accepts
  string CTE lookup filters, but aggregate result printing still only
  handles integer, unsigned integer, and double aggregate result types.

Fix:

- Extend RonSQL aggregate result printing to handle string aggregate
  results through `NdbAggregator::Result::data_str()`.
- Use the resolved aggregate output type and charset metadata for
  formatting, matching existing group-by string printing behavior.
- Ensure NULL string aggregate results print as `NULL`.

Tests:

- Add direct RonSQL tests for `SELECT MIN(varchar_col)`,
  `SELECT MAX(varchar_col)`, `SELECT MIN(char_col)`, and
  `SELECT MAX(char_col)`.
- Add grouped RonSQL string MIN/MAX result tests.
- Defer CTE string MIN/MAX consumption tests to S.6.  Re-aggregating a
  string CTE result currently exercises the wider CTE virtual-table
  string aggregate path and can hang instead of failing cleanly; S.5 is
  scoped to the RonSQL aggregate result printer itself.

## Phase I.6-S.6: Complete type and surface coverage  *(shipped for in-scope surfaces, `09e5a8b90cd`)*

Problem:

- Current coverage is intentionally narrow.  It does not sufficiently
  cover Longvarchar, grouped aggregation, join aggregation, CTE/RonSQL
  final output, NULL handling, empty strings, or collation-sensitive
  ordering.

Fix:

- Extend coverage after the correctness fixes above are in place.

Tests:

- CHAR:
  - trailing-space padding,
  - empty-ish values represented by spaces,
  - NULL ignored by MIN/MAX unless all rows are NULL.
- VARCHAR:
  - empty string,
  - varying lengths,
  - high-byte / collation-sensitive values.
- Longvarchar:
  - short value,
  - value large enough to exercise multi-word payload handling,
  - multiple Longvarchar aggregate slots in one program.
- Execution surfaces:
  - scalar scan aggregation,
  - grouped scan aggregation,
  - join aggregation,
  - RonSQL direct aggregate,
  - RonSQL CTE materialisation and lookup/scan consumption.
- Negative paths:
  - unsupported string arithmetic aggregate rejected at build time,
  - scratch-buffer overflow rejected cleanly.

## Completion criteria  *(met for the in-scope surfaces)*

- No `first-source winner` shortcut remains for enabled string MIN/MAX
  paths — S.2 ported real charset-aware compare.
- Grouped result iteration returns exactly the declared aggregate slots
  — S.1 fixed `result_records_.len`.
- String result memory is API-owned after receive and freed exactly
  once — S.1 + S.2 unified ownership.
- RonSQL can print string aggregate results — S.5.
- Tests include multi-partition cases that would fail if merge order
  were used as the result — S.6 covers this.
- Full RonSQL suite and the relevant `ndb_push_agg` tests pass with the
  new `testVarcharMinMax` and `ronsql_minmax_string` files added.

## Open follow-ups (not covered by this document)

The S.1-S.6 fixups cover scalar / grouped scan aggregation against a
single table and direct RonSQL aggregate printing.  String MIN/MAX
results that need to flow through additional execution surfaces remain
gated and are tracked separately:

- **Join aggregation linked-attr strings** — `JoinAggInterpreter::
  kOpLoadCol` still rejects linked-attr CHAR / VARCHAR / Longvarchar
  loads (`attrDescriptor == nullptr` arm returns
  `ZAGG_LOAD_COL_WRONG_TYPE`).  Encoding charset and prefix bytes in
  the linked-attr stream is required before MIN / MAX over a CTE
  string output can feed a downstream aggregator.
- **CTE_LOOKUP delivery substitution** — `Dblqh::cteLookupEmitResult`
  and the per-row CTE feed paths today `memcpy(..., 8)` per
  `AggResItem` slot.  For string slots they ship `val_ptr` bits, not
  the payload, so any consumer reading CTE output directly sees
  garbage on the receiver.  Needs the same per-slot type-aware
  substitution that AGG_CHAR_RESULT introduced for the kernel-to-API
  drain path, but applied to the kernel-internal CTE delivery path.
- **CTE materialisation chain consumption** — Once linked-attr strings
  and CTE delivery substitution are wired, RonSQL queries of the form
  `WITH cte AS (... MIN/MAX(string) ...) SELECT ... FROM cte JOIN ...`
  can be exercised end-to-end.  Today they hang or fail with
  `ZCTE_LOOKUP_OUTPUT_OVERFLOW`.

These are tracked under the F.4 plan (`cte_filter_phase_i6_string_minmax_f4.md`,
to be written) and are deliberately out of scope for the S.1-S.6
hardening sweep.
