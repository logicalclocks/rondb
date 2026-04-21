# Phase 4 — `testCteNdbApiOuterJoin.cpp` consolidated tests

## Goal

New NDB-API test binary covering every CTE outer-join shape touched
by Phases 1-3. Modeled on `testCteNdbApiFilter.cpp`: shared schema
(`CTE_SRC_TABLE`, `CTE_VIRTUAL_TABLE`), verification via
`nextResult(true)` loop with row-count and per-column assertions.

## Registration

Add to `storage/ndb/block_unit_test/CMakeLists.txt`:
- `testCteNdbApiOuterJoin` binary, linked like `testCteNdbApiFilter`.

## Test roster

| # | Name | Shape | Verifies |
|---|---|---|---|
| 1 | `testMainLookupInner` | `scanTable LEFT JOIN lookupCte(0)`, all parents match, but child uses `MatchNonNull` | Baseline (inner path still works after Phase 1's REF refactor) |
| 2 | `testMainLookupLeftJoinBasic` | `scanTable LEFT JOIN lookupCte(0)`, one parent's grp not in CTE | Phase 1 API null-row path |
| 3 | `testMainLookupLeftJoinMultiBatch` | Same as 2, ~1000 parents, half unmatched, `setBatchSize(100)` | Phase 1 + SCAN_NEXTREQ interaction |
| 4 | `testMainLookupLeftJoinFilter` | Mirror of existing `testCteNdbApiFilter` Test 10 | Phase 1 filter-reject path |
| 5 | `testCteSubtreeLeftJoinLookup` | `beginCteSubtree(1) { scanTable LEFT JOIN lookupCte(0) + aggregation }` | Phase 1 agg-feed null-row path |
| 6 | `testScanCteAsLeftParent` | `scanCte(0) LEFT JOIN readTuple(t)`, some CTE groups unmatched | Phase 2 (verification, likely no code change) |
| 7 | `testScanCteChildEmpty` | `scanTable LEFT JOIN scanCte(0)` with empty CTE — every parent gets NULL row | Phase 3 null path |
| 8 | `testScanCteChildNonEmpty` | Same as 7, non-empty CTE, Cartesian cross-product semantics | Phase 3 non-null path |
| 9 | `testChainedCteLeftJoin` | `beginCteSubtree(2) { scanCte(0) LEFT JOIN lookupCte(1) + aggregation }` | Phase 1 + subtree composition |

Tests 1-5 exercise Phase 1. Test 6 is Phase 2. Tests 7-9 exercise
Phase 3. Test 4 duplicates intent with Test 10 of
`testCteNdbApiFilter`; keep for end-to-end binary coverage.

## Schema reuse

- `CTE_SRC_TABLE` (`cte_src`, real table) — `pk`, `grp`, `val`.
- `CTE_VIRTUAL_TABLE` (`cte_virtual`) — `grp`, `total`.
- `CTE_SCALAR_VIRTUAL_TABLE` (if needed for scalar aggregates).

Setup via `sqlExec` following existing test pattern.

## Verification helpers

- Count rows returned.
- For null-row assertions: `NdbRecAttr::isNULL() > 0` on CTE columns
  when parent grp not in CTE.
- For aggregation-feeding tests: use `NdbAggregator::FetchResultRecord`
  to verify the null-row-injected groups produce COUNT including-null
  and SUM=0.

## Build / run (user runs)

```
cd debug_build
make -j$(sysctl -n hw.ncpu) testCteNdbApiOuterJoin
./runtime_output_directory/testCteNdbApiOuterJoin -c <cs> -m 3306 -v
```

Each phase adds tests incrementally; Phase 4 confirms the full
suite passes.
