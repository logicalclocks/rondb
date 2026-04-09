# Plan: Part B — Single-Row CTE Optimization

## Context

When a CTE produces exactly one row (aggregate without GROUP BY — e.g., a scalar
subquery rewritten as a CTE), three optimizations apply:

1. **Single-node materialization** — build the hash table on only one data node
   (round-robin), not all nodes
2. **Skip redistribution** — the single node already has the complete result,
   no REDISTRIBUTE_REQ needed
3. **Cache result in DBSPJ** — after materialization completes, fetch the one
   row via CTE_SCAN_REQ(batchSize=1) and cache it in DBSPJ. Subsequent lookups
   serve the cached row directly without CTE_LOOKUP_REQ signals to DBLQH.

## Files to Modify

1. `storage/ndb/include/kernel/signaldata/QueryTree.hpp` — CTE_SINGLE_ROW flag
2. `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` — CteInfo.m_flags
3. `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — parse flags, single-node setup, skip redist, pack flags in keyData
4. `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` — CteContext.m_flags, cached row fields
5. `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` — parse flags, cache fetch, serve cached row
6. `storage/ndb/block_unit_test/testCteDbtc.cpp` — wire format: add per-CTE flags word

## Detailed Changes

### Step 1: QueryTree.hpp — CTE_SINGLE_ROW flag

Add flag enum to `QN_CteSubtreeNode`:

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

### Step 2: Dbtc.hpp — CteInfo.m_flags

Add `m_flags` to CteInfo (after phase, same alignment group):

```cpp
struct CteInfo {
  Uint64 depMask;
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 aggProgramPtrI;
  Uint32 phase;
  Uint32 m_flags;    // Bit 0 = CTE_SINGLE_ROW
};
```

### Step 3: DbtcMain.cpp — Parse flags, single-node setup, skip redist

**3a. Parse per-CTE flags (line ~28527)**

Add flags read after depMask, before progLen:

```
Wire format per CTE:
[tableId] [schemaVersion] [depMask_lo] [depMask_hi] [flags] [progLen] [prog...]
```

```cpp
Uint32 cteFlags;
ndbrequire(reader.getWord(&cteFlags));
scanptr.p->m_cteInfos[c].m_flags = cteFlags;
consumed += 6;  // was 5
```

Initialize `m_flags = 0` in the allocation init loop.

**3b. Single-node setup in sendJoinAggSetupReqs() (line ~28835)**

For single-row CTEs, select one node via round-robin instead of all nodes:

```cpp
for (Uint32 c = 0; c < scanptr.p->m_numCtes; c++) {
  // ... allocate JoinAggNodeState ...

  if (scanptr.p->m_cteInfos[c].m_flags & QN_CteSubtreeNode::CTE_SINGLE_ROW) {
    // Single-row CTE: send SETUP to one node only (round-robin)
    Uint32 targetNode = selectRoundRobinDbNode();
    // send SETUP_REQ to targetNode only
    cteNodes->m_aggNodes.set(targetNode);
    cteNodes->m_aggNodesPending.set(targetNode);
    scanptr.p->m_cteSetupOutstanding++;
  } else {
    // Normal CTE: send to all connected DB nodes (existing code)
    for (Uint32 nodeId = 1; nodeId < MAX_NDB_NODES; nodeId++) { ... }
  }
}
```

For round-robin: reuse the existing `Thostptr.p->m_round_robin_instance`
pattern, or add a simple `m_cteRoundRobin` counter on ScanRecord that
wraps around the connected DB node list.

**3c. Skip redistribution in sendCteCompleteReqsForPhase() (line ~29357)**

For single-row CTEs, skip sending JOIN_AGG_COMPLETE_REQ (no redistribution
needed since only one node has data):

```cpp
for (Uint32 c = 0; c < scanptr.p->m_numCtes; c++) {
  if (scanptr.p->m_cteInfos[c].phase != phase) continue;

  if (scanptr.p->m_cteInfos[c].m_flags & QN_CteSubtreeNode::CTE_SINGLE_ROW) {
    // Single-row CTE: no redistribution needed — skip COMPLETE_REQ.
    // The single node's hash table is already the final result.
    continue;
  }
  // ... existing redistribution logic for normal CTEs ...
}
```

When all normal CTEs in this phase have been skipped or completed,
`m_cteCompleteOutstanding == 0` at line 29407, which triggers
`cteAdvancePhase()` immediately.

**3d. Pack per-CTE flags in keyData (line ~29031)**

Add per-CTE flags word in the aggKeys section packing:

```cpp
keyData[idx++] = c;                  // cteId
keyData[idx++] = depMask_lo;
keyData[idx++] = depMask_hi;
keyData[idx++] = scanptr.p->m_cteInfos[c].m_flags;  // NEW
keyData[idx++] = scanptr.p->m_cteInfos[c].phase;
keyData[idx++] = cteNodeCount;
```

Update keyData size calculation to account for the extra word per CTE:
`numCtes * (6 + maxNodes * 2)` (was 5).

### Step 4: Dbspj.hpp — CteContext extensions

Add flags and cached row fields:

```cpp
struct CteContext {
  enum State { CTE_NOT_STARTED=0, CTE_MATERIALIZING=1,
               CTE_READY=2, CTE_FAILED=3 };
  Uint64 m_depMask;
  Uint32 m_cteId;
  Uint32 m_state;
  Uint32 m_numResultCols;
  Uint32 m_scanTreeNodeNo;
  Uint32 m_phase;
  Uint32 m_flags;              // Bit 0 = CTE_SINGLE_ROW
  Uint32 m_cachedRowPtrI;      // RNIL or section with cached result row
  Uint32 m_cachedRowLen;       // Word count of cached row
  Uint32 m_singleNodeId;       // Node ID where single-row CTE lives
};
```

### Step 5: DbspjMain.cpp — Parse flags, cache, serve

**5a. Parse per-CTE flags in aggKeys (line ~1471)**

Add flags read in per-CTE loop, matching DBTC packing order:

```cpp
ndbrequire(reader.getWord(&depLo));
ndbrequire(reader.getWord(&depHi));
Uint32 perCteFlags;
ndbrequire(reader.getWord(&perCteFlags));  // NEW
ndbrequire(reader.getWord(&phase));
ndbrequire(reader.getWord(&cteNodeCount));

// Store in CteContext
requestPtr.p->m_cteContexts[i].m_depMask = depMask;
requestPtr.p->m_cteContexts[i].m_phase = phase;
requestPtr.p->m_cteContexts[i].m_flags = perCteFlags;  // NEW
```

Initialize new fields in `cte_build()`:
```cpp
cctx.m_flags = 0;
cctx.m_cachedRowPtrI = RNIL;
cctx.m_cachedRowLen = 0;
cctx.m_singleNodeId = 0;
```

Resolve single-node ID during aggKeys parsing: for single-row CTEs,
find the one node with a non-zero aggStateKey:

```cpp
if (perCteFlags & QN_CteSubtreeNode::CTE_SINGLE_ROW) {
  for (Uint32 n = 0; n < cteNodeCount; n++) {
    // ... read nodeId, cteAggKey ...
    if (cteAggKey != 0) {
      requestPtr.p->m_cteContexts[i].m_singleNodeId = nodeId;
    }
  }
}
```

**5b. Cache single-row result after CTE becomes READY**

In `execCTE_PHASE_START_REQ()` (line 6071) and `execCTE_START_MAIN_REQ()`
(line 6116), after transitioning a CTE to CTE_READY, if it's single-row:

```cpp
if (ctx.m_flags & QN_CteSubtreeNode::CTE_SINGLE_ROW) {
  // Fetch the one row from the single node's hash table
  fetchSingleRowCte(signal, requestPtr, ctx);
}
```

`fetchSingleRowCte()` sends CTE_SCAN_REQ(batchSize=1) to the single node.
The TRANSID_AI response is intercepted and stored in `m_cachedRowPtrI`
via `appendToSection()`. The CTE_SCAN_CONF(EndOfData) confirms the row
is cached.

This is asynchronous — the main query hasn't started yet (we're still in
CTE phase transitions), so the cache fetch completes before the main query
begins. Increment `m_outstanding` to prevent premature batch completion.

**5c. Serve cached row in cte_parent_row() (line 5398)**

Before calling `cte_lookup_send()`, check for cached row:

```cpp
case CteContext::CTE_READY:
  if (cteCtx->m_cachedRowPtrI != RNIL) {
    // Single-row CTE: serve cached result directly
    cte_serve_cached_row(signal, requestPtr, treeNodePtr, *cteCtx);
  } else {
    cte_lookup_send(signal, requestPtr, treeNodePtr, rowRef);
  }
  break;
```

`cte_serve_cached_row()` constructs a TRANSID_AI signal from the cached
section and delivers it to the tree node's row processing pipeline. It
then calls `checkBatchComplete()`. No CTE_LOOKUP_REQ to DBLQH needed.

This is the key performance win: every parent row that looks up the
single-row CTE gets the cached result without a signal round-trip.

**5d. Cleanup cached row**

In `cleanup()` (line 3773), free the cached section:

```cpp
for (Uint32 i = 0; i < requestPtr.p->m_numCtes; i++) {
  if (requestPtr.p->m_cteContexts[i].m_cachedRowPtrI != RNIL) {
    releaseSection(requestPtr.p->m_cteContexts[i].m_cachedRowPtrI);
    requestPtr.p->m_cteContexts[i].m_cachedRowPtrI = RNIL;
  }
}
```

### Step 6: testCteDbtc.cpp — Wire format update

Add per-CTE flags word (0 = not single-row) to the test signal construction:

```cpp
aggSection.push_back(meta.tableId);
aggSection.push_back(meta.schemaVersion);
aggSection.push_back(0);            /* depMask lo */
aggSection.push_back(0);            /* depMask hi */
aggSection.push_back(0);            /* flags = 0 (not single-row) */  // NEW
aggSection.push_back((Uint32)cteAggProgram.size());
```

Also update the aggKeys packing in the test to include per-CTE flags.

## Verification

1. Build: `make -j$(sysctl -n hw.ncpu) testCteDbtc testJoinAggSpj testJoinAggNdbApi`
2. Run CTE tests: `testCteDbtc -c <cs> -m <port>`
3. Run regression: `testJoinAggSpj`, `testJoinAgg`, `testJoinAggNdbApi`
4. Add a single-row CTE test case to testCteDbtc verifying:
   - Only one node receives JOIN_AGG_SETUP_REQ
   - No REDISTRIBUTE_REQ signals sent
   - Cached row served without CTE_LOOKUP_REQ
