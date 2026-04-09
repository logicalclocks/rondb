# Plan: CTE-to-CTE Lookup + Single-Row CTE Optimization

## Context

Two related improvements to the CTE pushdown framework:

**A. CTE-to-CTE can use both CTE_LOOKUP and CTE_SCAN.** Currently a CTE that
depends on another CTE can only use QN_CTE_SCAN (full scan of the predecessor's
hash table). But some dependent CTEs only need point lookups by key into the
predecessor — they should use QN_CTE_LOOKUP instead. The query planner decides
which node type to use based on the query structure: scan when the dependent CTE
needs all rows, lookup when it joins by key.

**B. Single-row CTE optimization.** When a CTE produces exactly one row (e.g., an
aggregate without GROUP BY — a scalar subquery rewritten as a CTE), three
optimizations apply:
1. Materialize on only **one** data node (round-robin selection), not all nodes
2. Skip redistribution entirely (only one node has data)
3. After materialization, **upload the single row to DBSPJ** so subsequent
   lookups/scans serve it from memory without CTE_LOOKUP_REQ signals to DBLQH

## Files to Modify

### Part A: CTE-to-CTE Lookup
1. `storage/ndb/include/kernel/signaldata/QueryTree.hpp` — no changes needed
   (QN_CTE_LOOKUP already supports parent-linked keys)
2. `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp` — allow `lookupCte()` inside
   CTE subtrees (currently may be restricted to main query)
3. `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` — ensure `cte_parent_row()`
   works when the parent is a CTE_SCAN node (keys from CTE scan rows)
4. `storage/ndb/src/ronsql/QueryPlanner.cpp` — emit QN_CTE_LOOKUP inside CTE
   subtrees when the dependent CTE joins by key

### Part B: Single-Row CTE
5. `storage/ndb/include/kernel/signaldata/QueryTree.hpp` — define
   `CTE_SINGLE_ROW` flag in `QN_CteSubtreeNode::requestInfo`
6. `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` — add `m_singleRow` flag to
   `CteInfo`
7. `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — 
   - Parse single-row flag from CTE definitions
   - `sendJoinAggSetupReqs()`: send to one node only for single-row CTEs
   - `sendCteCompleteReqsForPhase()`: skip redistribution for single-row CTEs
8. `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` — add `m_singleRow` flag
   and cached row fields to `CteContext`
9. `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` —
   - Parse single-row flag from aggKeys
   - `execCTE_PHASE_START_REQ` / `execCTE_START_MAIN_REQ`: for single-row
     CTEs, fetch the one row via CTE_SCAN_REQ and cache it in DBSPJ
   - `cte_lookup_send()` / `cte_parent_row()`: serve cached row directly
     instead of sending CTE_LOOKUP_REQ to DBLQH
10. `storage/ndb/src/ronsql/QueryPlanner.cpp` — set single-row flag when CTE
    has aggregation but no GROUP BY
11. `storage/ndb/block_unit_test/testCteDbtc.cpp` — wire format update for
    single-row flag

## Detailed Changes

### Part A: CTE-to-CTE Lookup

#### A1. Ensure QN_CTE_LOOKUP works inside CTE subtrees

The QN_CTE_LOOKUP node type already has full key pattern support (parent list,
linked operands). When placed inside a QN_CTE_SUBTREE, the build loop marks it
with T_CTE_SCAN flag (line 1880). The `cte_build()` function registers the
CteContext and `cte_parent_row()` handles parent rows by checking CTE state.

**What needs verification/change:**
- `cte_parent_row()` (line 5369): When the CTE_LOOKUP's parent is itself a
  CTE_SCAN node inside the same subtree, parent rows arrive as TRANSID_AI
  from the CTE_SCAN. The key expansion via `expand()` should work since the
  row data is in standard AttributeHeader format. Verify this path works.
- The build loop at line 1880 sets T_CTE_SCAN on all nodes inside a subtree.
  A QN_CTE_LOOKUP inside a subtree should NOT be marked T_CTE_SCAN (it's not
  a scan node for materialization purposes — it's a lookup child). The flag
  assignment needs to distinguish: only mark scan/lookup leaf nodes that
  drive the CTE's own materialization, not CTE_LOOKUP nodes that read from
  other CTEs.

#### A2. NDB API: lookupCte() inside CTE subtrees

`NdbQueryBuilder::lookupCte()` validates that the operation is not the root
(line 970). Inside a CTE subtree, the CTE_LOOKUP would be a child of the
CTE's scan root — this should already satisfy the "not root" check. No API
changes expected, but verify serialization order: CTE_SUBTREE header →
embedded SCAN_FRAG → embedded CTE_LOOKUP → ... main query.

#### A3. Query Planner: Emit CTE_LOOKUP inside dependent CTE

In `QueryPlanner.cpp`, when planning a CTE that depends on another CTE:
- If the dependency is a full scan (no join key): emit QN_CTE_SCAN (current)
- If the dependency is by key (e.g., CTE 1 joins CTE 0 on CTE 0's GROUP BY
  columns): emit QN_CTE_LOOKUP with linked operands from the parent scan

The planner needs to detect when the dependent CTE's FROM clause references
the predecessor CTE with an equi-join condition matching the predecessor's
GROUP BY columns.

### Part B: Single-Row CTE

#### B1. QueryTree flag

Add `CTE_SINGLE_ROW = 0x1` to `QN_CteSubtreeNode::requestInfo`:

```cpp
struct QN_CteSubtreeNode {
  Uint32 len;
  Uint32 requestInfo;   // Bit 0: CTE_SINGLE_ROW
  Uint32 cteId;
  Uint32 numNodes;
  static constexpr Uint32 NodeSize = 4;
  
  enum RequestInfoBits {
    CTE_SINGLE_ROW = 0x1  // CTE produces exactly one row (no GROUP BY)
  };
};
```

#### B2. DBTC: Single-node materialization

Add `bool m_singleRow` to `CteInfo` (Dbtc.hpp). Parse from wire format.

In `sendJoinAggSetupReqs()`, for single-row CTEs:
- Select one node via round-robin: `Uint32 targetNode = cteRoundRobin++ % numDbNodes`
- Send JOIN_AGG_SETUP_REQ only to that node
- Set `m_cteAggNodeState[c]->m_aggNodes` with only that node

In `sendCteCompleteReqsForPhase()`, for single-row CTEs:
- Send JOIN_AGG_COMPLETE_REQ to the single node
- The node has all data locally → no redistribution needed → immediate
  COMPLETE_CONF with state transition to CTE_READY

#### B3. DBTC wire format

In the CTE definition section (SCAN_TABINFO), add a flags word per CTE:

```
[tableId] [schemaVersion] [depMask_lo] [depMask_hi] [flags] [progLen] [prog...]
```

Where `flags` bit 0 = single-row CTE. This requires updating the parsing
in both DBTC and the test.

#### B4. DBSPJ: Cache single row

Add to `CteContext`:

```cpp
bool m_singleRow;          // This CTE produces exactly one row
Uint32 m_cachedRowPtrI;    // SegmentedSection containing cached result (RNIL if none)
```

**Cache population:** After all CTEs transition to CTE_READY in
`execCTE_START_MAIN_REQ()` (or `execCTE_PHASE_START_REQ()`), for each
single-row CTE:
1. Send one CTE_SCAN_REQ (batchSize=1) to the single node that has the data
2. The TRANSID_AI response contains the one row
3. Store it in `m_cachedRowPtrI` as a SegmentedSection
4. Mark CTE as `CTE_CACHED` (new state, or reuse CTE_READY with cached flag)

**Cache serving:** In `cte_parent_row()`, when state is CTE_READY and
`m_cachedRowPtrI != RNIL`:
- Construct the result directly from the cached section
- Skip CTE_LOOKUP_REQ entirely
- Send TRANSID_AI to the API using the cached data
- This avoids the signal round-trip to DBLQH for every parent row

For CTE_SCAN of a single-row CTE: the scan returns the cached row immediately
without sending CTE_SCAN_REQ.

#### B5. Query Planner: Set single-row flag

In `QueryPlanner.cpp`, when emitting QN_CTE_SUBTREE:
- If the CTE's aggregation program has no GROUP BY columns (pure aggregate:
  COUNT(*), SUM, etc. without GROUP BY), set `CTE_SINGLE_ROW` in requestInfo
- This can be detected from the aggregation program metadata

## Verification

### Part A
1. Build and run: `testCteDbtc` (exercises CTE-to-CTE paths)
2. Manual test: CTE 1 that does `lookupCte(cte0)` inside its subtree
3. Verify TRANSID_AI row format from CTE_SCAN feeds correctly into CTE_LOOKUP
   key expansion

### Part B
1. Build and run: `testCteDbtc` with a single-row CTE test case
2. Verify only one node receives JOIN_AGG_SETUP_REQ for single-row CTEs
3. Verify redistribution is skipped (no REDISTRIBUTE_REQ signals)
4. Verify cached row serves lookups without CTE_LOOKUP_REQ
5. Run regression: `testJoinAgg`, `testJoinAggSpj`, `testJoinAggNdbApi`
