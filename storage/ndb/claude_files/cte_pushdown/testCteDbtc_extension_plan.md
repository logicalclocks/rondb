# Plan: Extend testCteDbtc with CTE_LOOKUP, CTE_SCAN, Multi-Phase, and Negative Tests

## Context

The current testCteDbtc has 4 test cases that materialize CTEs but
the main SELECT doesn't USE them — it's an independent self-join.
We need tests that exercise the full CTE query path including
CTE_LOOKUP_REQ and CTE_SCAN_REQ from the main query and between
CTEs, plus negative tests for invalid query trees.

## File to Modify

`storage/ndb/block_unit_test/testCteDbtc.cpp`

## New Test Cases

### Test 5: Main SELECT with CTE_LOOKUP (CTE_LOOKUP_REQ path)

**SQL equivalent:**
```sql
WITH cte0 AS (
  SELECT grp, SUM(val) AS total
  FROM src t1 JOIN src t2 ON t1.pk = t2.pk
  GROUP BY grp
)
SELECT COUNT(*), SUM(cte0.total)
FROM src t1
JOIN cte0 ON t1.grp = cte0.grp;
```

**Query Tree (9 nodes):**
```
Node 0: QN_CTE_SUBTREE (cteId=0, numNodes=2)
Node 1:   QN_SCAN_FRAG (CTE 0 scan on src, linked: pk)
Node 2:   QN_LOOKUP (CTE 0 agg leaf, self-join by pk)
Node 3: QN_SCAN_FRAG (main scan on src, linked: grp)
Node 4: QN_CTE_LOOKUP (cteId=0, parent=node 3, key=grp)
```

**What it tests:**
- CTE materialization into hash table (nodes 1-2)
- CTE_LOOKUP_REQ from DBSPJ to DBLQH (node 4)
- Main query joins CTE results by GROUP BY key
- The CTE must be in CTE_READY state before lookups execute

**CTE definition:** 1 CTE with GROUP BY (grp), SUM(val)
**Main agg:** COUNT(*), SUM of CTE result column
**Expected:** each group's SUM(val) joined back, then aggregated

**Note:** This requires a 3-column table (pk, grp, val) which the
test already creates as `cte_dbtc_test3`.

### Test 6: Two CTEs with Dependency (CTE_SCAN_REQ + Multi-Phase)

**SQL equivalent:**
```sql
WITH
  cte0 AS (
    SELECT grp, SUM(val) AS total
    FROM src t1 JOIN src t2 ON t1.pk = t2.pk
    GROUP BY grp
  ),
  cte1 AS (
    -- CTE 1 depends on CTE 0: scans CTE 0's hash table
    SELECT COUNT(*) AS grp_count, SUM(total) AS grand_total
    FROM cte0
  )
SELECT COUNT(*), SUM(t2.val)
FROM src t1 JOIN src t2 ON t1.pk = t2.pk;
```

**Query Tree (11 nodes):**
```
Node 0: QN_CTE_SUBTREE (cteId=0, numNodes=2)
Node 1:   QN_SCAN_FRAG (CTE 0 scan, linked: pk)
Node 2:   QN_LOOKUP (CTE 0 agg leaf)
Node 3: QN_CTE_SUBTREE (cteId=1, numNodes=2)
Node 4:   QN_CTE_SCAN (cteId=0, reads CTE 0's hash table)
Node 5:   QN_LOOKUP (CTE 1 agg leaf, keyed by CTE_SCAN output)
Node 6: QN_SCAN_FRAG (main scan, linked: pk)
Node 7: QN_LOOKUP (main agg leaf)
```

**What it tests:**
- 2-phase CTE execution: phase 0 = CTE 0, phase 1 = CTE 1
- CTE_SCAN_REQ from DBSPJ to DBLQH (node 4 scans CTE 0)
- CTE dependency via depMask: CTE 1 has depMask=0x1 (depends on CTE 0)
- Phase advancement: CTE_PHASE_COMPLETE_REP → redistribution →
  CTE_PHASE_START_REQ
- CTE 1's QN_CTE_SCAN node NOT marked T_CTE_SCAN (our Part A fix)

**CTE definitions:**
- CTE 0: depMask=0, phase=0, GROUP BY grp, SUM(val)
- CTE 1: depMask=0x1, phase=1, no GROUP BY, COUNT+SUM

### Test 7-11: Negative Tests (Invalid Query Trees)

Each negative test sends an intentionally malformed query tree and
verifies that DBTC or DBSPJ returns SCAN_TABREF with an error code
instead of crashing.

**Test 7: scanParallelism < fragCount for CTE query**
- Send SCAN_TABREQ with scanParallelism=1 but CTE definitions
- Expected: SCAN_TABREF with ZINVALID_KEY from DBTC
  (enforced in execDIH_SCAN_TAB_CONF)

**Test 8: numCtes > 64**
- Send CTE_DEFS_MARKER followed by numCtes=65
- Expected: SCAN_TABREF with ZINVALID_KEY from DBTC

**Test 9: CTE_SUBTREE with numNodes=0**
- Send QN_CTE_SUBTREE with numNodes=0 (empty subtree)
- Expected: SCAN_TABREF or SCAN_FRAGREF with error

**Test 10: CTE_LOOKUP with invalid cteId**
- Send QN_CTE_LOOKUP with cteId=99 (no matching CTE subtree)
- Expected: error from DBSPJ (cteIdx not found)

**Test 11: Missing CTE_DEFS_MARKER**
- Send CTE definitions without the 0xCDE00000 marker
- Expected: CTEs not parsed, main query runs without CTEs
  (graceful degradation, not crash)

## Implementation Notes

### Building New Query Trees

Add new `buildQueryTree*` functions for each topology. Follow the
existing pattern in `buildQueryTreeWithCtes()`:
- Tree section: node headers with optype, flags, parent list, key pattern
- Parameter section: one param block per node

### CTE_LOOKUP Node Construction

```cpp
/* QN_CTE_LOOKUP node */
Uint32 n_len = 0;
QueryNode::setOpLen(n_len, QueryNode::QN_CTE_LOOKUP, 7);
ai.push_back(n_len);
ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED);
ai.push_back(cteId);           // cteId (instead of tableId)
ai.push_back(numResultCols);   // numResultCols (instead of tableVersion)
ai.push_back((parentNodeIdx << 16) | 1);  // parent list
ai.push_back((0 << 16) | 1);             // key pattern: 1 word
ai.push_back(QueryPattern::col(0));       // key from parent linked attr
```

### CTE_SCAN Node Construction

```cpp
/* QN_CTE_SCAN node */
Uint32 n_len = 0;
QueryNode::setOpLen(n_len, QueryNode::QN_CTE_SCAN, 4);
ai.push_back(n_len);
ai.push_back(DABits::NI_LINKED_ATTR);  // passes results to children
ai.push_back(cteId);           // source CTE to scan
ai.push_back(numResultCols);   // GROUP BY cols + agg results
```

### Dependency Mask in CTE Definitions

For Test 6, CTE 1 depends on CTE 0:
```cpp
aggSection.push_back(0xCDE00000);  // CTE_DEFS_MARKER
aggSection.push_back(2);           // numCtes
// CTE 0: no deps
aggSection.push_back(depMask_lo=0); aggSection.push_back(depMask_hi=0);
// CTE 1: depends on CTE 0
aggSection.push_back(depMask_lo=1); aggSection.push_back(depMask_hi=0);
```

### Negative Test Pattern

```cpp
static int testNegative_ScanParallelism(TestCtx &ctx) {
  // ... setup ...
  // Override scanParallelism to 1 (too small)
  data[15] = 1;
  // Send and expect SCAN_TABREF
  rc = sendScanTabReqWithCtes(...);
  if (rc != 0) return -1;
  SimpleSignal *resp = waitForSignal(...);
  if (getGsn(resp) != GSN_SCAN_TABREF) {
    printf("FAIL: expected SCAN_TABREF, got GSN %d\n", gsn);
    return -1;
  }
  printf("PASS\n");
  return 0;
}
```

## Verification

1. Build: `make -j$(nproc) ndbmtd testCteDbtc`
2. Run: `./mtr --suite=ndb_push_agg testCteDbtc`
3. Verify: all positive tests produce correct results, all negative
   tests receive error responses without crashes
4. Regression: `./mtr --suite=ndb_push_agg testJoinAggSpj`
5. Full suite: `./mtr --suite=ndb_push_agg`
