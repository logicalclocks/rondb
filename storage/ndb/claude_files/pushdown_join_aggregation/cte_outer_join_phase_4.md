# Phase 4 — `testCteNdbApiOuterJoin.cpp` consolidated tests

## Goal

End-to-end NDB-API coverage of CTE outer-join shapes shipped in
Phases 1 and 2. Phase 3 is dropped (see phase 3 doc), so tests for
CTE_SCAN as outer-join child are not included. The CTE-subtree
agg-feed NULL-injection path is also deferred; see
`next_steps.md` for the pending work.

## Binary registration

`storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` registered
in:
- `storage/ndb/block_unit_test/CMakeLists.txt` (build target)
- `mysql-test/suite/ndb_push_agg/t/testCteNdbApiOuterJoin.test`
- `mysql-test/suite/ndb_push_agg/r/testCteNdbApiOuterJoin.result`

## Test roster

| # | Name | Shape | Verifies |
|---|---|---|---|
| 1 | `testScanCteInnerJoin` | `scanCte(0) INNER JOIN readTuple(oj_rhs)` | Phase 2 baseline plumbing |
| 2 | `testScanCteLeftJoin` | `scanCte(0) LEFT JOIN readTuple(oj_rhs)` — grp=2 unmatched on rhs side | Phase 2 canary |
| 3 | `testMainLookupCteLeftJoinDefaultBatch` | `scanTable(oj_rhs) LEFT JOIN lookupCte(0)` — id=5 unmatched on cte side | Phase 1 API-direct path |
| 4 | `testMainLookupCteLeftJoinSmallBatch` | Test 3 with `setBatchSize(1)` | Phase 1 + SCAN_NEXTREQ interaction |

All verify outer-join NULL-padding via
`NdbQueryOperation::isRowNULL()` (operation-level), not via
`NdbRecAttr::isNULL()` (column-level — stale from prior matched
row, false negatives).

## Shared schema

| Table | Purpose |
|---|---|
| `oj_cte_src` | CTE source (pk, grp, val); rows cover grp=1,2,3 |
| `oj_cte_virtual` | Virtual table for `lookupCte`/`scanCte` (grp PK, total) |
| `oj_rhs` | Outer-join RHS (id PK, label). id=1,3 match CTE groups; id=2,5 don't, to force NULL on each side |

## Build / run (user runs)

```
cd debug_build
make -j$(sysctl -n hw.ncpu) testCteNdbApiOuterJoin
cd ../mysql-test
./mtr --suite=ndb_push_agg testCteNdbApiOuterJoin
```

Expected: all 4 tests PASSED.
