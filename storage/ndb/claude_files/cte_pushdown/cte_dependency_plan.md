# CTE-to-CTE Dependencies: Implementation Plan

## Overview

Enable CTEs to reference earlier CTEs as source tables:

```sql
WITH
  cte1 AS (SELECT region, SUM(amount) FROM orders GROUP BY region),
  cte2 AS (SELECT region, AVG(total)  FROM cte1   GROUP BY region),
  cte3 AS (SELECT ...                 FROM cte1 JOIN products ...)
SELECT ... FROM cte2 JOIN cte3 ...
```

Dependency DAG: cte2→cte1, cte3→cte1, main→{cte2,cte3}.

## Architecture Decision: Single Request, Multi-Phase

Keep one DBSPJ Request per SCAN_FRAGREQ. Extend the existing two-phase
model (CTE scans → main query) to N phases for CTE dependency chains.

**Why not separate Requests per CTE:**
1. Hash lookup (senderData+transId) maps 1:1 to Requests — splitting breaks every signal handler
2. SCAN_NEXTREQ only targets one Request — CTE Requests would never receive it
3. CTE2 lookups into CTE1 need CTE1's aggStateKeys — cross-Request sharing is fragile
4. One abort() + one cleanup() handles everything — cascading across Requests is complex
5. Single arena is efficient — N Requests = N arenas with no benefit
6. One transId, one error path, one transaction — separate Requests blur ownership

**Why single Request works:**
`m_outstanding` naturally reaches 0 between phases. `batchComplete()` already
routes on `RT_CTE_PHASE`. Extending to N phases is a straightforward generalization.

## Signal Flow

```
API
 │  SCAN_TABREQ (QueryTree + main agg + CTE defs with depMasks)
 ▼
DBTC ──────────────────────────────────────────────────────────────
 │
 │  SETUP: JOIN_AGG_SETUP_REQ (CTE_MODE_FLAG) for ALL CTEs → DBLQH
 │  Collect SETUP_CONFs, pack aggStateKeys with CTE_KEYS_MARKER
 │
 │  SCAN_FRAGREQ → DBSPJ instances (QueryTree + all aggStateKeys)
 │
 │  ┌─── Phase 0 ──────────────────────────────────────────────┐
 │  │  DBSPJ starts CTE scans with depMask=0 (no deps)        │
 │  │  Scans complete → CTE_PHASE_COMPLETE_REP(phase=0) → DBTC│
 │  └──────────────────────────────────────────────────────────┘
 │
 │  DBTC: JOIN_AGG_COMPLETE_REQ for Phase 0 CTEs → DBLQH
 │         (redistribution)
 │  DBTC: CTE_PHASE_START_REQ(phase=1) → all DBSPJ instances
 │
 │  ┌─── Phase 1 ──────────────────────────────────────────────┐
 │  │  DBSPJ marks Phase 0 CTEs as CTE_READY                  │
 │  │  DBSPJ starts CTE scans with phase=1                    │
 │  │  (these may use CTE_SCAN_REQ to read Phase 0 CTEs)      │
 │  │  Scans complete → CTE_PHASE_COMPLETE_REP(phase=1) → DBTC│
 │  └──────────────────────────────────────────────────────────┘
 │
 │  DBTC: JOIN_AGG_COMPLETE_REQ for Phase 1 CTEs → DBLQH
 │  DBTC: CTE_START_MAIN_REQ → all DBSPJ instances
 │
 │  ┌─── Main Query ───────────────────────────────────────────┐
 │  │  DBSPJ marks all CTEs as CTE_READY, clears RT_CTE_PHASE │
 │  │  Normal scan + lookup + CTE_LOOKUP execution             │
 │  │  SCAN_TABCONF → API                                      │
 │  └──────────────────────────────────────────────────────────┘
```

## Phase 1: Dependency Metadata and Phase Computation

**Goal:** Parse CTE dependency info, compute execution phases, no behavioral
change yet (all current queries have depMask=0, single phase).

### Step 1.1: Extend KeyInfo CTE Definition Format

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`** (parseJoinAggKeyInfo)

Extend the per-CTE definition in section 2 of SCAN_TABREQ:

```
Current:
  [numCtes]
  For each CTE: [tableId] [schemaVersion] [progLen] [prog...]

New:
  [numCtes]
  For each CTE: [tableId] [schemaVersion] [depMask] [progLen] [prog...]
```

`depMask` is a Uint32 bitmask — bit c set means this CTE depends on cteId c.
For backward compatibility, depMask=0 means no dependencies (Phase 0).

Store in `ScanRecord::CteInfo`:
```cpp
struct CteInfo {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 aggProgramPtrI;
  Uint32 depMask;         // NEW: which CTEs must be READY first
};
```

Parse change: read one extra word per CTE in the KeyInfo loop.

### Step 1.2: Compute Phases in DBTC

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**

After parsing all CTE definitions, compute phases:
```
phase[c] = 0                             if depMask[c] == 0
phase[c] = 1 + max(phase[d] for d in depMask[c])  otherwise
```

Store `m_ctePhaseCount` = max phase + 1 in ScanRecord.

Add to ScanRecord:
```cpp
Uint32 m_ctePhases[MAX_CTES];    // Phase number per CTE
Uint32 m_ctePhaseCount;          // Total phases (1 for no deps)
Uint32 m_cteCurrentPhase;        // Phase being waited on
```

### Step 1.3: Forward depMask and Phase to DBSPJ

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`** (aggStateKeys packing)

Extend the CTE_KEYS_MARKER section in SCAN_FRAGREQ:

```
Current:
  CTE_KEYS_MARKER
  [numCtes]
  For each CTE: [cteId] [numNodes] [nodeId, aggKey]...

New:
  CTE_KEYS_MARKER
  [numCtes]
  For each CTE: [cteId] [depMask] [phase] [numNodes] [nodeId, aggKey]...
```

### Step 1.4: Parse Phase Info in DBSPJ

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`** (CTE aggKeys parsing)
**File: `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`** (CteContext)

Extend CteContext:
```cpp
struct CteContext {
  Uint32 m_cteId;
  Uint32 m_state;
  Uint32 m_numResultCols;
  Uint32 m_scanTreeNodeNo;
  Uint32 m_depMask;       // NEW
  Uint32 m_phase;         // NEW
};
```

Extend Request:
```cpp
Uint32 m_cteCurrentPhase;       // NEW: phase being executed
Uint32 m_ctePhaseCount;         // NEW: total CTE phases
```

Parse the new fields from the CTE_KEYS_MARKER section.

### Step 1.5: Test

Verify existing tests pass unchanged — all current CTEs have depMask=0
and phase=0. Extend testCteDbtc to verify phase metadata is parsed
correctly (add verbose logging of parsed depMask/phase values).

**No behavioral change in this phase.**

---

## Phase 2: Per-Phase CTE Execution in DBSPJ

**Goal:** Start only Phase 0 CTEs initially. Report per-phase completion
instead of all-CTEs completion. Handle new phase-start signal.

### Step 2.1: Phase-Aware CTE Start

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`** (checkPrepareComplete)

Change from starting all T_CTE_SCAN nodes to starting only Phase 0:

```cpp
if (requestPtr.p->m_numCtes > 0) {
  requestPtr.p->m_bits |= Request::RT_CTE_PHASE;
  requestPtr.p->m_cteCurrentPhase = 0;

  Local_TreeNode_list list(m_treenode_pool, requestPtr.p->m_nodes);
  Ptr<TreeNode> treeNodePtr;
  for (list.first(treeNodePtr); !treeNodePtr.isNull();
       list.next(treeNodePtr)) {
    if (!(treeNodePtr.p->m_bits & TreeNode::T_CTE_SCAN)) continue;

    // Find this node's CTE context
    for (Uint32 i = 0; i < requestPtr.p->m_numCtes; i++) {
      if (requestPtr.p->m_cteContexts[i].m_cteId == treeNodePtr.p->m_cteId &&
          requestPtr.p->m_cteContexts[i].m_phase == 0) {
        (this->*(treeNodePtr.p->m_info->m_start))(
            signal, requestPtr, treeNodePtr);
        break;
      }
    }
  }
}
```

For current queries (all CTEs have phase=0), behavior is unchanged.

### Step 2.2: Phase-Aware batchComplete

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`** (batchComplete)

Replace `handleCteScansComplete()` call with phase-aware reporting:

```cpp
if (requestPtr.p->m_bits & Request::RT_CTE_PHASE) {
  jam();
  // Report current phase complete to DBTC
  Uint32 phase = requestPtr.p->m_cteCurrentPhase;
  sendCtePhaseCompleteRep(signal, requestPtr, phase);
  return;
}
```

### Step 2.3: New Signal — CTE_PHASE_COMPLETE_REP

**File: `storage/ndb/include/kernel/signaldata/CteScan.hpp`**

```cpp
struct CtePhaseCompleteRep {
  Uint32 senderRef;
  Uint32 senderData;    // ScanFragRec.i for DBTC lookup
  Uint32 phase;         // Which CTE phase completed
  static constexpr Uint32 SignalLength = 3;
};
```

**File: `storage/ndb/include/kernel/GlobalSignalNumbers.h`**

Allocate GSN for CTE_PHASE_COMPLETE_REP.

### Step 2.4: New Signal — CTE_PHASE_START_REQ

**File: `storage/ndb/include/kernel/signaldata/CteScan.hpp`**

```cpp
struct CtePhaseStartReq {
  Uint32 senderRef;
  Uint32 senderData;    // ScanFragRec.i for DBSPJ hash lookup
  Uint32 transId1;
  Uint32 transId2;
  Uint32 phase;         // Which CTE phase to start
  static constexpr Uint32 SignalLength = 5;
};
```

### Step 2.5: DBSPJ Handler — execCTE_PHASE_START_REQ

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`**

```cpp
void Dbspj::execCTE_PHASE_START_REQ(Signal *signal) {
  // Hash lookup by senderData + transId
  Ptr<Request> requestPtr;
  ndbrequire(m_scan_request_hash.find(requestPtr, key));

  Uint32 phase = req->phase;

  // Transition previous phase's CTEs to CTE_READY
  for (Uint32 i = 0; i < requestPtr.p->m_numCtes; i++) {
    CteContext &ctx = requestPtr.p->m_cteContexts[i];
    if (ctx.m_phase < phase && ctx.m_state == CteContext::CTE_MATERIALIZING) {
      ctx.m_state = CteContext::CTE_READY;
      requestPtr.p->m_ctesReady++;
    }
  }

  // Update current phase
  requestPtr.p->m_cteCurrentPhase = phase;

  // Start this phase's CTE scan nodes
  Local_TreeNode_list list(m_treenode_pool, requestPtr.p->m_nodes);
  Ptr<TreeNode> treeNodePtr;
  for (list.first(treeNodePtr); !treeNodePtr.isNull();
       list.next(treeNodePtr)) {
    if (!(treeNodePtr.p->m_bits & TreeNode::T_CTE_SCAN)) continue;
    for (Uint32 i = 0; i < requestPtr.p->m_numCtes; i++) {
      if (requestPtr.p->m_cteContexts[i].m_cteId == treeNodePtr.p->m_cteId &&
          requestPtr.p->m_cteContexts[i].m_phase == phase) {
        (this->*(treeNodePtr.p->m_info->m_start))(
            signal, requestPtr, treeNodePtr);
        break;
      }
    }
  }

  checkBatchComplete(signal, requestPtr);
}
```

### Step 2.6: Backward Compatibility

For single-phase CTE queries (m_ctePhaseCount == 1), the new code produces
the same signal flow as before: one CTE_PHASE_COMPLETE_REP (phase=0) is
equivalent to the old CTE_SCAN_COMPLETE_REP.

Deprecate CTE_SCAN_COMPLETE_REP in favor of CTE_PHASE_COMPLETE_REP.
Keep CTE_START_MAIN_REQ unchanged for the final transition.

### Step 2.7: Test

Verify all existing CTE tests pass (single-phase behavior unchanged).
Add a test that manually constructs a 2-phase CTE definition (depMask=0
for cte0, depMask=0x1 for cte1) and verifies that CTE_PHASE_COMPLETE_REP
arrives before CTE_PHASE_START_REQ for phase 1.

---

## Phase 3: DBTC Per-Phase Orchestration

**Goal:** DBTC handles multi-phase CTE lifecycle: receive per-phase
completion reports, redistribute per phase, send phase-start signals.

### Step 3.1: Per-Phase Tracking in ScanRecord

**File: `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`**

Replace the flat CTE completion counters with per-phase tracking:

```cpp
// Remove:
//   Uint32 m_cteScanReportsExpected;
//   Uint32 m_cteScanReportsReceived;

// Add:
Uint32 m_ctePhaseCount;                // Total CTE phases
Uint32 m_cteCurrentPhase;              // Phase being waited on
Uint32 m_ctePhaseReportsExpected;      // DBSPJ instances in flight
Uint32 m_ctePhaseReportsReceived;      // Per-phase completion count
Uint32 m_ctePhases[MAX_CTES];          // Phase number per CTE
```

### Step 3.2: execCTE_PHASE_COMPLETE_REP Handler

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**

Replace `execCTE_SCAN_COMPLETE_REP` with:

```cpp
void Dbtc::execCTE_PHASE_COMPLETE_REP(Signal *signal) {
  // Look up ScanFragRec → ScanRecord (same as current handler)
  scanptr.p->m_ctePhaseReportsReceived++;

  if (scanptr.p->m_ctePhaseReportsReceived !=
      scanptr.p->m_ctePhaseReportsExpected)
    return;

  // All DBSPJ instances completed this phase.
  // Redistribute this phase's CTEs.
  scanptr.p->scanState = ScanRecord::WAIT_CTE_COMPLETE;
  sendCteCompleteReqsForPhase(signal, scanptr,
                              scanptr.p->m_cteCurrentPhase);
}
```

### Step 3.3: Per-Phase Redistribution

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**

Factor `sendCteCompleteReqs()` to accept a phase parameter:

```cpp
void Dbtc::sendCteCompleteReqsForPhase(Signal *signal,
                                        ScanRecordPtr scanptr,
                                        Uint32 phase) {
  scanptr.p->m_cteCompleteOutstanding = 0;

  for (Uint32 c = 0; c < scanptr.p->m_numCtes; c++) {
    if (scanptr.p->m_ctePhases[c] != phase) continue;
    // Send JOIN_AGG_COMPLETE_REQ for CTE c to each node
    // (same logic as current sendCteCompleteReqs, filtered by phase)
    ...
  }
}
```

### Step 3.4: Phase Advancement After Redistribution

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**
(execJOIN_AGG_COMPLETE_CONF in WAIT_CTE_COMPLETE)

```cpp
if (scanptr.p->m_cteCompleteOutstanding == 0) {
  Uint32 nextPhase = scanptr.p->m_cteCurrentPhase + 1;
  if (nextPhase < scanptr.p->m_ctePhaseCount) {
    // More CTE phases — advance
    scanptr.p->m_cteCurrentPhase = nextPhase;
    scanptr.p->m_ctePhaseReportsReceived = 0;
    scanptr.p->scanState = ScanRecord::RUNNING;
    sendCtePhaseStartReqs(signal, scanptr, nextPhase);
  } else {
    // All CTE phases complete — start main query
    sendCteStartMainReqs(signal, scanptr);
  }
}
```

### Step 3.5: sendCtePhaseStartReqs

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**

Send CTE_PHASE_START_REQ to all DBSPJ instances (same iteration as
sendCteStartMainReqs but with phase number):

```cpp
void Dbtc::sendCtePhaseStartReqs(Signal *signal, ScanRecordPtr scanptr,
                                  Uint32 phase) {
  CtePhaseStartReq *req = ...;
  req->senderRef = reference();
  req->transId1 = apiPtr.p->transid[0];
  req->transId2 = apiPtr.p->transid[1];
  req->phase = phase;

  // Iterate running + queued ScanFragRecs
  for (each fragPtr) {
    req->senderData = fragPtr.i;
    sendSignal(fragPtr.p->lqhBlockref, GSN_CTE_PHASE_START_REQ, ...);
  }
  scanptr.p->scanState = ScanRecord::RUNNING;
}
```

### Step 3.6: Test

SignalSender test with 2 CTEs where cte1 has depMask=0 (Phase 0) and
cte2 has depMask=0x1 (Phase 1). Verify signal ordering:
1. CTE_PHASE_COMPLETE_REP(phase=0) from each DBSPJ instance
2. JOIN_AGG_COMPLETE_REQ for cte0
3. CTE_PHASE_START_REQ(phase=1) to each DBSPJ instance
4. CTE_PHASE_COMPLETE_REP(phase=1) from each DBSPJ instance
5. JOIN_AGG_COMPLETE_REQ for cte1
6. CTE_START_MAIN_REQ to each DBSPJ instance

---

## Phase 4: CTE Hash Table Scan (QN_CTE_SCAN)

**Goal:** Enable a CTE to read all groups from an earlier CTE's hash table,
so cte2 can scan cte1 as a source table.

### Step 4.1: New Node Type — QN_CTE_SCAN

**File: `storage/ndb/include/kernel/signaldata/QueryTree.hpp`**

```cpp
enum OpType {
  ...
  QN_CTE_SCAN = 0x8,  // Scan all groups from a materialized CTE hash table
};

struct QN_CteScanNode {
  Uint32 len;
  Uint32 requestInfo;   // DABits flags (NI_AGGREGATE, NI_LINKED_ATTR, etc.)
  Uint32 cteId;         // Which CTE's hash table to scan
  Uint32 numResultCols; // GROUP BY cols + aggregate result cols
  static constexpr Uint32 NodeSize = 4;
  Uint32 optional[1];   // NI_LINKED_ATTR etc.
};

struct QN_CteScanParameters {
  Uint32 len;
  Uint32 requestInfo;
  Uint32 resultData;
  Uint32 batch_size_rows;
  Uint32 batch_size_bytes;
  static constexpr Uint32 NodeSize = 5;
  Uint32 optional[1];
};
```

### Step 4.2: New Signal — CTE_SCAN_REQ/CONF/REF

**File: `storage/ndb/include/kernel/signaldata/CteScan.hpp`**

```cpp
struct CteScanReq {
  Uint32 senderRef;       // DBSPJ block reference
  Uint32 senderData;      // TreeNode pointer
  Uint32 aggStateKey;     // CTE hash table to scan
  Uint32 batchSize;       // Max groups to return per batch
  static constexpr Uint32 SignalLength = 4;
};

struct CteScanConf {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 numRows;         // Groups returned in this batch
  Uint32 flags;           // EndOfData flag
  static constexpr Uint32 SignalLength = 4;
  enum { EndOfData = 0x1 };
};

struct CteScanRef {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 errorCode;
  static constexpr Uint32 SignalLength = 3;
};
```

### Step 4.3: DBLQH Handler — execCTE_SCAN_REQ

**File: `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`**

Iterate groups in the CTE's JoinAggregationState hash table:
- For each group up to batchSize:
  - Send TRANSID_AI with the group key + aggregate values
- Send CTE_SCAN_CONF with numRows and EndOfData flag
- If not EndOfData, save iteration position for SCAN_NEXTREQ equivalent

This reuses the existing hash table iteration from JOIN_AGG_COMPLETE_REQ
redistribution code (which already iterates all groups).

### Step 4.4: DBSPJ OpInfo — g_CteScanOpInfo

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`**

New operation handlers:
- `cte_scan_build()` — parse QN_CTE_SCAN node, initialize ScanFragData-like state
- `cte_scan_start()` — send CTE_SCAN_REQ to local DBLQH
- `execCTE_SCAN_CONF()` — process returned groups, propagate to children
- `cte_scan_execSCAN_NEXTREQ()` — send next CTE_SCAN_REQ batch

The CTE scan behaves like a fragment scan but reads from a hash table
instead of an NDB table. It uses a single "fragment" (the local DBLQH's
CTE hash table) and iterates groups in batches.

### Step 4.5: QueryTree Construction for Dependent CTEs

For a CTE that reads from an earlier CTE:

```sql
WITH
  cte1 AS (SELECT grp, SUM(val) FROM orders GROUP BY grp),
  cte2 AS (SELECT grp, COUNT(*) FROM cte1   GROUP BY grp)
```

QueryTree for cte2's subtree:
```
QN_CTE_SUBTREE(cteId=1, numNodes=1)
  QN_CTE_SCAN(cteId=0)    -- scan cte1's hash table
    NI_AGGREGATE | NI_AGGREGATE_LEAF
```

Note: cte2's subtree has only 1 embedded node (the CTE_SCAN). There is no
lookup child because the CTE_SCAN directly iterates groups — each group
IS a complete row for cte2's aggregation input.

### Step 4.6: Test

SignalSender test:
- cte1: GROUP BY grp, SUM(val) from test table
- cte2: GROUP BY grp, COUNT(*) from cte1 (via QN_CTE_SCAN)
- Main query: standard scan+lookup self-join

Verify cte2's hash table contains the correct COUNT(*) per group
(matching the number of groups in cte1).

---

## Phase 5: End-to-End Integration

### Step 5.1: MySQL Integration

**File: `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`**

Extend the NDB query builder to:
- Accept CTE dependency information from the MySQL optimizer
- Generate depMask per CTE based on FROM clause references
- Emit QN_CTE_SCAN nodes when a CTE's source is another CTE

### Step 5.2: MySQL Optimizer

Detect CTE-to-CTE references in the WITH clause and pass dependency
metadata through the NDB pushdown interface.

### Step 5.3: MTR Tests

SQL-level tests:
```sql
-- Basic chain: cte2 reads cte1
WITH
  c1 AS (SELECT dept, SUM(salary) s FROM emp GROUP BY dept),
  c2 AS (SELECT dept, s*2 AS doubled FROM c1)
SELECT * FROM c2;

-- Diamond: c3 reads both c1 and c2
WITH
  c1 AS (...),
  c2 AS (... FROM c1 ...),
  c3 AS (... FROM c1 JOIN c2 ...)
SELECT * FROM c3;

-- Independent parallel CTEs
WITH
  c1 AS (... FROM t1 ...),
  c2 AS (... FROM t2 ...)     -- no dep on c1
SELECT * FROM c1 JOIN c2 ...;
```

### Step 5.4: Error Handling

Verify that:
- If Phase 0 CTE fails, Phase 1 CTEs never start, main query gets error
- If Phase 1 CTE fails, main query gets error
- Node failure during CTE materialization aborts entire query
- Timeout during CTE redistribution propagates correctly

---

## Summary

| Phase | What | Blocks Changed | New Signals |
|-------|------|----------------|-------------|
| 1 | Dependency metadata parsing | DBTC, DBSPJ | — |
| 2 | Per-phase CTE execution in DBSPJ | DBSPJ | CTE_PHASE_COMPLETE_REP, CTE_PHASE_START_REQ |
| 3 | Per-phase orchestration in DBTC | DBTC | (handlers for above) |
| 4 | QN_CTE_SCAN node type | DBSPJ, DBLQH | CTE_SCAN_REQ/CONF/REF |
| 5 | MySQL integration + MTR tests | NdbApi, MySQL | — |

Phases 1-3 can be implemented and tested independently using SignalSender
tests with manually constructed QueryTrees. Phase 4 adds the CTE-reads-CTE
capability. Phase 5 connects everything to the SQL layer.
