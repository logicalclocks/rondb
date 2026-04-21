# Phase 2 — scanCte as LEFT-side parent of outer join

## Goal

Confirm that when `scanCte(cteId)` is the root (or ancestor) and a
regular `readTuple` child is attached in outer-join mode (no
`setMatchType(MatchNonNull)`), NULL-row handling for the child works.

## What shipped

### One latent bug fixed

Phase 2's tests uncovered a pre-existing bug: `cte_scan_build` read
`ctx.m_start_signal` (for `batch_size_rows`) but never cleared it, so
a subsequent child `lookup_build` at `DbspjMain.cpp:7735` would see
the leftover `SCAN_FRAGREQ` and wrongly interpret it as its *own*
root start signal — setting `TreeNode::T_ONE_SHOT` on the child. At
`lookup_send` time the child then took the T_ONE_SHOT branch with
stale `m_send.m_keyInfoPtrI` / `m_attrInfoPtrI` pointers, crashing on
`getSection` (ArrayPool assertion).

The fix mirrors the existing guard in `cte_lookup_build`
(`DbspjMain.cpp:5901-5908`): clear `ctx.m_start_signal` when the
CTE_SCAN is the main-query root (no parent, not inside a CTE
subtree).

Triggered by Test 1 in `testCteNdbApiOuterJoin.cpp` (scanCte INNER
JOIN readTuple), pre-existing bug — not introduced by this phase.

### No other DBSPJ changes

The scan-root + lookup-child outer-join NULL-row machinery (API
`NdbResultStream::prepareResultSet` auto-fill) works identically
whether the parent emits rows via `cte_scan` or regular `scanFrag`,
once the `T_ONE_SHOT` bug above is fixed.

### Test gotcha worth recording

The outer-join "unmatched child" case is reported at the **operation
level** via `NdbQueryOperation::isRowNULL()`, NOT at the
**column level** via `NdbRecAttr::isNULL()`. An unmatched outer-join
child leaves every column's NdbRecAttr untouched (possibly carrying
a stale value from a previous matched row); only the operation-level
row-is-NULL flag reflects the no-match. Tests that check column
NULLness for outer-join detection will give false negatives.

## Tests in the new binary (`testCteNdbApiOuterJoin.cpp`)

- **Test 1** — `scanCte(0) INNER JOIN readTuple(oj_rhs)`: 2 rows
  (grp=1,3 match; grp=2 dropped). Baseline for the scanCte-as-parent
  plumbing.
- **Test 2** — `scanCte(0) LEFT JOIN readTuple(oj_rhs)`: 3 rows
  (grp=1,3 matched; grp=2 outer-join NULL-padded). Phase 2 canary;
  verified via `rhsOp->isRowNULL()`.

Schema (`oj_cte_src`, `oj_cte_virtual`, `oj_rhs`) is self-contained
in the new binary and won't collide with tests in the existing CTE
binaries.

## Files touched

| File | Change |
|---|---|
| `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp::cte_scan_build` | Clear `ctx.m_start_signal` when CTE_SCAN is main-query root |
| `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` | New — Tests 1 and 2 |
| `storage/ndb/block_unit_test/CMakeLists.txt` | Register binary |
| `mysql-test/suite/ndb_push_agg/t/testCteNdbApiOuterJoin.test` | MTR wrapper |
| `mysql-test/suite/ndb_push_agg/r/testCteNdbApiOuterJoin.result` | Expected `PASSED` |

## Build / run (user runs)

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd testCteNdbApiOuterJoin
cd ../mysql-test
./mtr --suite=ndb_push_agg testCteNdbApiOuterJoin
```

Expected: PASSED; 2 tests, 3 total rows in Test 2 with grp=2
null-padded.
