# Phase I.6 string MIN/MAX hardening plan

## Status

The current implementation checkpoint was committed first as requested:

- `9efdc033e9c` - `RONDB-1050: add initial string MIN/MAX aggregation test path`

This document plans the follow-up fixes from the review of the
CHAR / VARCHAR / Longvarchar MIN/MAX work.  The feature currently has
the kernel wire path and a narrow single-partition API test, but it is
not yet safe to expose broadly because several merge, grouped-result,
and RonSQL result paths are incomplete.

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

## Phase I.6-S.1: Fix grouped API result ownership and iteration

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

## Phase I.6-S.2: Implement API-side string MIN/MAX merge

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

## Phase I.6-S.3: Add interpreter string scratch-buffer bounds checks

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

## Phase I.6-S.4: Narrow API builder validation by operation

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

## Phase I.6-S.5: Add RonSQL string aggregate result printing

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

## Phase I.6-S.6: Complete type and surface coverage

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

## Completion criteria

- No `first-source winner` shortcut remains for enabled string MIN/MAX
  paths.
- Grouped result iteration returns exactly the declared aggregate slots.
- String result memory is API-owned after receive and freed exactly
  once.
- RonSQL can print string aggregate results.
- Tests include at least one multi-partition case that would fail if
  merge order were used as the result.
- Full RonSQL suite and the relevant `ndb_push_agg` tests pass before
  the phase is considered complete.
