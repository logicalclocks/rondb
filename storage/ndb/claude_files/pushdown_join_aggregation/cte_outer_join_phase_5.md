# Phase 5 — CTE_LOOKUP agg-feed NULL injection (was next_steps 6a)

## Context

When a `lookupCte` inside a CTE subtree is built with outer-join
semantics (no `setMatchType(MatchNonNull)`) and DBLQH returns
`CteLookupRef(GROUP_NOT_FOUND)`, DBSPJ today silently drops the
parent row. The enclosing CTE's aggregator never sees that parent's
contribution, so chained-CTE queries like

```sql
WITH cte0 AS (SELECT grp, SUM(val) FROM t1 GROUP BY grp),
     cte1 AS (SELECT COUNT(*), SUM(cte0.total)
              FROM t2 LEFT JOIN cte0 ON cte0.grp = t2.id)
SELECT * FROM cte1;
```

silently under-count rows whose `t2.id` has no matching group in
`cte0`. The Phase 1 doc and comments already flag this as "not yet
wired"; the earlier pivot to next_steps (6a) deferred it, but the
shape is common enough in real SQL that we should ship it.

## Target behaviour

For each `lookupCte` REF that meets all three conditions:

1. `ref->errorCode == CteLookupRef::GROUP_NOT_FOUND`
2. `(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) == 0`
3. `(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) != 0`
   — i.e. agg-feed path, result goes into the downstream
   `JoinAggInterpreter`, not the API

DBSPJ should inject a NULL row into the enclosing CTE's aggregator
via the existing `sendJoinAggNullRow(...)` helper
(`DbspjMain.cpp:9410+`), using the parent row identified by the
correlation already echoed back in `CteLookupRef::correlation`
(Phase 1 signal extension).

The main-SELECT direct-to-API path is untouched (the API
auto-fills NULL; Phase 1).

## Design

### Why REF-time injection, not completion-time sweep

Regular outer-join lookups use `handleAggAncestorComplete` at
treeNode completion: iterate buffered parent rows, for each whose
`m_matched` bit is unset, fire `sendJoinAggNullRow`. That design
relies on `execTRANSID_AI` walking up scan ancestors and setting
`m_matched` bits on each match.

CTE_LOOKUP does not emit TRANSID_AI back to DBSPJ on match — DBLQH
calls `cteLookupAggFeed` which feeds the result straight into the
target `JoinAggInterpreter` and sends CONF only. So there's no
natural hook for setting the `m_matched` bit. Wiring that would
mean either calling the match-walk loop from `execCTE_LOOKUP_CONF`
(duplicating execTRANSID_AI's logic) or plumbing a "matched" flag
through the CteLookupConf signal.

REF-time injection is simpler: each REF unambiguously identifies
one unmatched parent via the correlation we already echo. The
parent row is in the scan ancestor's buffered-rows map; fetch via
`getBufferedRow`, call `sendJoinAggNullRow`.

### Step 1 — enable correlation-keyed parent lookup

`sendJoinAggNullRow` takes a `const RowPtr &`. To construct one from
the REF-time correlation we need `getBufferedRow(scanAncestorPtr,
rowId, &parentRow)`, which needs the scan ancestor to have both
`T_BUFFER_ROW` (row data buffered) and `T_BUFFER_MAP` (COLLECTION_MAP
layout so the correlation → row lookup works).

The build-plan pass that sets these bits lives in
`validateAggregateFlags` (not `appendTreeNode`). `appendTreeNode` is
only called via `planSequentialExec`; for scan requests with
multi-leaf aggregation (e.g. chained-CTE queries with two `defineCte`
calls), `buildExecPlan` routes through `planParallelExec` which does
NOT call `appendTreeNode`. So the logic must live in
`validateAggregateFlags` which runs unconditionally before plan
dispatch.

Existing pass at `DbspjMain.cpp:2445-2511` handles outer-join
agg-leaf **scans** (`treeNodePtr.p->isScan()`). CTE_LOOKUP is a
lookup, so that pass skips it. Add a **new pass** right after it,
specifically for CTE_LOOKUP agg-leaves:

```cpp
for (list.first(treeNodePtr); !treeNodePtr.isNull();
     list.next(treeNodePtr)) {
  if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) &&
      !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) &&
      treeNodePtr.p->m_info == &g_CteLookupOpInfo) {
    jam();
    Ptr<TreeNode> scanAncestorPtr;
    Uint32 parentPtrI = treeNodePtr.p->m_parentPtrI;
    bool found = false;
    while (parentPtrI != RNIL) {
      jam();
      ndbrequire(m_treenode_pool.getPtr(scanAncestorPtr, parentPtrI));
      if (scanAncestorPtr.p->isScan()) {
        found = true;
        break;
      }
      parentPtrI = scanAncestorPtr.p->m_parentPtrI;
    }
    if (found) {
      jam();
      scanAncestorPtr.p->m_bits |=
          TreeNode::T_BUFFER_ROW | TreeNode::T_BUFFER_MAP;
    }
  }
}
```

Note: no `T_BUFFER_MATCH` and no batch_size cap — REF-time injection
doesn't need match tracking, and CTE_LOOKUP has no cross-fragment
range fan-out on the leaf side.

### Step 2 — REF-time injection in `execCTE_LOOKUP_REF`

Extend the `GROUP_NOT_FOUND` branch:

```cpp
if (errorCode != CteLookupRef::GROUP_NOT_FOUND) { ... abort ... }

const bool isOuterJoin =
    (treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) == 0;
const bool isAggFeed =
    (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) != 0;

if (isOuterJoin && isAggFeed) {
  jam();
  Ptr<TreeNode> scanAncestorPtr;
  ndbrequire(m_treenode_pool.getPtr(scanAncestorPtr,
                                     treeNodePtr.p->m_scanAncestorPtrI));
  ndbassert(scanAncestorPtr.p->m_bits & TreeNode::T_BUFFER_ANY);
  ndbassert(scanAncestorPtr.p->m_bits & TreeNode::T_BUFFER_MAP);

  RowPtr parentRow;
  getBufferedRow(scanAncestorPtr, (refCorrelation >> 16), &parentRow);

  Uint32 err = sendJoinAggNullRow(signal, requestPtr, treeNodePtr,
                                   parentRow, /*parentLevelAdjust=*/0,
                                   /*nullNodes=*/0);
  if (unlikely(err != 0)) {
    jam();
    abort(signal, requestPtr, err);
    return;
  }
}
// outer + direct-to-API: nothing (API auto-fills, Phase 1)
// inner + GROUP_NOT_FOUND: silently drop (current behaviour)

maybeResumeCongestedNodes(signal, requestPtr, treeNodePtr);
checkBatchComplete(signal, requestPtr);
```

### Step 3 — synthesize CTE NULL columns in the linked buffer

`sendJoinAggNullRow`'s `expand()` produces parent linked columns only.
On the success path, `cteLookupAggFeed` appends the source CTE's GB-key
and aggregate-result columns to the linked buffer via
`buildCteLinkedBuffer`.  The downstream `mainAgg` program's
`LoadLinkedColumn` instructions encode those positions at build time;
if they're missing at execution, `ProcessRec` returns
`ZAGG_OTHER_ERROR` and DBLQH's `execJOIN_AGG_NULL_ROW_REQ` crashes on
`ndbrequire(ret == 0)`.

Fix: in `sendJoinAggNullRow`, after `expand()`, if
`treeNodePtr.p->m_info == &g_CteLookupOpInfo`, append
`m_cteLookup_data.m_numResultCols` synthetic NULL
`[tableId=0, schemaVersion=0, AttributeHeader(attrId=i, byteSize=0)]`
triples (matching `buildCteLinkedBuffer`'s layout).  The agg
interpreter's position-walk treats `byteSize=0` as NULL, so
`LoadLinkedColumn` loads NULL → `SUM` skips the row and `COUNT`
(driven by `LoadUint64(1)`) still increments.

### Step 4 — test

Add **Test 5** to `testCteNdbApiOuterJoin.cpp`:

```
cte0: SELECT grp, SUM(val) FROM oj_cte_src GROUP BY grp
      → {(1,30),(2,70),(3,50)}
cte1: SELECT COUNT(*), SUM(cte0.total)
      FROM oj_rhs LEFT JOIN cte0 ON cte0.grp = oj_rhs.id
      -- scalar aggregate, no GROUP BY
      -- expect COUNT=3, SUM=80 (id=1→30, id=3→50, id=5→NULL/0)
main: SELECT * FROM cte1
```

Expected with fix applied: COUNT=3, SUM=80.
Expected today (pre-fix): COUNT=2, SUM=80 (id=5 silently dropped).

Verify via `NdbAggregator::FetchResultRecord` on the main query
(cte1 is scalar → one result row).

## Risks / unknowns

1. **`T_BUFFER_MAP` memory cost**: `COLLECTION_MAP` uses more
   memory than `COLLECTION_LIST` (inline hash bucket array). Only
   activated when `m_info == &g_CteLookupOpInfo` + outer-join +
   agg-leaf — a narrow slice. Acceptable.

2. **Scan ancestor populated by REF time**: the REF-time call
   assumes the scan ancestor's `m_rows` map contains the parent row
   that triggered the REQ. Parent rows are stored when the scan
   ancestor buffers them (before firing child REQs). Verified by
   the existing `T_CHK_CONGESTION + T_BUFFER_MAP` path at
   `DbspjMain.cpp:2986-2989` which is already set for
   scan-parent-of-lookup shapes.

3. **Multiple REFs per parent row?** No. CTE_LOOKUP sends one REQ
   per parent row; DBLQH sends exactly one CONF or one REF. No
   duplicate-null-row concern.

4. **Correlation echo regression**: Phase 1's CteLookupRef
   extension plumbs `correlation`. Verified already in use via
   Phase 1 debug trace.

5. **Interaction with `cte_lookup_start` pre-READY fast-path**
   (`DbspjMain.cpp:5926`): when CTE is already READY at main-query
   start, `cte_lookup_start` calls `cte_lookup_send` directly
   without queueing. REFs from that fast path still land in
   `execCTE_LOOKUP_REF` same as queued-then-flushed REFs, so the
   new null-injection branch applies uniformly.

## Files touched (expected)

| File | Change |
|---|---|
| `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` | Build-plan: set `T_BUFFER_MAP` on scan ancestor for CTE_LOOKUP outer-join agg-leaf. `execCTE_LOOKUP_REF`: new outer-join + agg-feed branch calls `sendJoinAggNullRow`. |
| `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` | New Test 5 — CTE subtree LEFT JOIN with agg-feed. |
| `storage/ndb/claude_files/pushdown_join_aggregation/next_steps.md` | Remove the 6a entry; 6b stays. |
| `storage/ndb/claude_files/pushdown_join_aggregation/cte_outer_join_plan.md` | Update in-scope list; phase index. |

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd testCteNdbApiOuterJoin
cd ../mysql-test
./mtr --suite=ndb_push_agg testCteNdbApiOuterJoin
```

All 5 tests PASSED. Test 5 specifically verifies cte1's scalar
aggregate is `{3, 80}` rather than the `{2, 80}` you'd get without
the fix.
