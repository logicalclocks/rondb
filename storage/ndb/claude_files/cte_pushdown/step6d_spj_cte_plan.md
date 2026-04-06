# Phase 6D: DBTC + DBSPJ CTE Implementation Plan

## Signal Flow

```
API
 │  SCAN_TABREQ (compound QueryTree + CTE agg programs)
 ▼
DBTC ─────────────────────────────────────────────────────────────
 │
 │  Step A: SETUP (for CTEs)
 │  JOIN_AGG_SETUP_REQ (CTE_MODE_FLAG) ──► DBLQH (all nodes)
 │  JOIN_AGG_SETUP_CONF ◄────────────────── DBLQH (aggStateKeys)
 │
 │  Step B: Forward compound query to DBSPJ
 │  SCAN_FRAGREQ ──► DBSPJ instances (QueryTree + CTE aggStateKeys)
 │
 │     DBSPJ executes CTE scans:
 │     SCAN_FRAGREQ ──► DBLQH (CTE source table, with CTE aggStateKey)
 │     SCAN_FRAGCONF ◄── DBLQH
 │     ...all CTE fragments scanned...
 │     Reports CTE completion ──► DBTC
 │
 │  Step C: COMPLETE (when all DBSPJ instances report CTEs done)
 │  JOIN_AGG_COMPLETE_REQ ──► DBLQH (all nodes, CTE aggStateKeys)
 │  JOIN_AGG_COMPLETE_CONF ◄── DBLQH (redistribution done, CTE_READY)
 │
 │  Step D: Start main SELECT
 │  Signal to DBSPJ instances: CTEs are READY, execute main query
 │
 │     DBSPJ executes main query:
 │     SCAN_FRAGREQ ──► DBLQH (root table scan)
 │     LQHKEYREQ ──► DBLQH (PK lookups for feature tables)
 │     CTE_LOOKUP_REQ ──► DBLQH (CTE hash table lookups)
 │     TRANSID_AI ──► API (result rows)
 │     CTE_LOOKUP_CONF ──► DBSPJ
 │
 │  Step E: Completion
 │  SCAN_TABCONF ──► API (end of data)
 │
 │  Step F: Cleanup
 │  JOIN_AGG_RELEASE_REQ ──► DBLQH (free CTE hash tables)
 │
```

## Part 1: DBTC Changes

### 1.1 Parse CTE definitions from SCAN_TABREQ

The KeyInfo section (section 2) is extended with CTE definitions after the
existing fields:

```
Existing:
  [boundsLen] [bounds...] [aggReceiverId] [mainAggProgram...]

Extended for CTE:
  [boundsLen] [bounds...] [aggReceiverId] [mainAggProgram...]
  [numCtes]
  For each CTE:
    [cteTableId] [cteSchemaVersion] [cteAggProgramLen] [cteAggProgram...]
```

If numCtes > 0, DBTC enters the CTE flow.

**File: `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`**

Add to ScanRecord:
```cpp
static constexpr Uint32 MAX_CTES = 4;

struct CteInfo {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 aggProgramPtrI;    // Section ptr to this CTE's agg program
  Uint32 fragCount;         // Fragments in CTE source table
  Uint32 fragsStarted;      // CTE scan frags sent
  Uint32 fragsCompleted;    // CTE scan frags done
};

Uint32 m_numCtes;
CteInfo m_cteInfos[MAX_CTES];
Uint32 m_currentCte;          // Which CTE is currently being scanned
Uint32 m_ctesCompleted;       // Number of CTEs fully scanned

// Per-CTE aggStateKeys: m_cteAggStateKeys[cteIdx][nodeId]
// Stored in a single flat array: [cte0_node0, cte0_node1, ..., cte1_node0, ...]
Uint32 *m_cteAggStateKeys;    // Allocated: MAX_CTES * MAX_NDB_NODES
```

Add ScanRecord states:
```cpp
CTE_SCANNING = ...,           // Scanning CTE source table fragments
WAIT_CTE_COMPLETE = ...,      // Sent COMPLETE_REQ, waiting for COMPLETE_CONF
WAIT_CTE_MAIN_START = ...,    // CTEs ready, signaling DBSPJ to start main query
```

**File: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`**

Extend `parseJoinAggKeyInfo()`:
- After reading main agg program, check if more data remains
- If so, read numCtes and per-CTE definitions
- Store in ScanRecord::m_cteInfos[]

### 1.2 CTE SETUP phase

Modify `sendJoinAggSetupReqs()`:
- For each CTE (0..numCtes-1): send JOIN_AGG_SETUP_REQ with CTE_MODE_FLAG
  to DBLQH on every data node, using the CTE's agg program
- Track outstanding SETUP requests across all CTEs

Modify `execJOIN_AGG_SETUP_CONF()`:
- Store aggStateKey in m_cteAggStateKeys[cteIdx * MAX_NDB_NODES + nodeId]
- When all SETUP_CONFs received (for all CTEs + main agg if any):
  Pack CTE aggStateKeys into aggKeys section for DBSPJ
  Transition to CTE_SCANNING (or RUNNING if no CTEs)

### 1.3 CTE scan phase

New function: `sendCteScans(Signal*, ScanRecordPtr)`

When entering CTE_SCANNING state:
- For each CTE (sequentially or in parallel):
  - Get fragment info for CTE source table from DBDIH
  - Send SCAN_FRAGREQ to DBLQH for each fragment, with:
    - JoinAggFlag set
    - CTE aggStateKey for that node in variableData
    - Minimal AttrInfo (just ExitOK interpreter program)
  - Track fragments started/completed per CTE

Handle SCAN_FRAGCONF for CTE scans:
- Increment fragsCompleted for the CTE
- When all fragments done for current CTE: move to next CTE or transition

When all CTEs fully scanned:
- Transition to WAIT_CTE_COMPLETE

**Note:** The CTE scans go directly from DBTC to DBLQH (not through DBSPJ),
because DBTC is coordinating the materialization phase. The scan is simple
(full table scan with aggregation, no join tree needed). DBSPJ only handles
the compound main query.

**Alternative:** DBTC sends CTE scans to DBSPJ instances, which distribute
them to DBLQH fragments. This uses DBSPJ's existing scan distribution
infrastructure. The compound QueryTree would include QN_CTE_SUBTREE nodes
that DBSPJ processes as simple scan operations.

**Recommendation:** Use DBSPJ for CTE scans. Reasons:
- DBSPJ already handles scan distribution across fragments
- Fragment-to-node mapping is handled by DBSPJ's existing infrastructure
- Multiple DBSPJ instances can scan different fragments in parallel
- Consistent with the design plan (DBSPJ starts CTE scans)

Flow with DBSPJ handling CTE scans:
1. DBTC sends SCAN_FRAGREQ to DBSPJ with compound QueryTree
2. DBSPJ builds compound tree (QN_CTE_SUBTREE + main query nodes)
3. DBSPJ starts CTE scan operations (using existing scan infrastructure)
4. When CTE scans complete on a DBSPJ instance, it reports to DBTC
5. DBTC waits for all DBSPJ instances, then sends COMPLETE_REQ

### 1.4 CTE COMPLETE phase

New function: `sendCteCompleteReqs(Signal*, ScanRecordPtr)`

When entering WAIT_CTE_COMPLETE:
- For each CTE: send JOIN_AGG_COMPLETE_REQ to DBLQH on every node
  with the CTE's aggStateKey and CTE_MODE_FLAG
- Track outstanding COMPLETE requests

Handle JOIN_AGG_COMPLETE_CONF:
- When all COMPLETE_CONFs received for all CTEs:
  - CTE hash tables are now READY (redistribution done on multi-node)
  - Signal DBSPJ instances to start main SELECT execution
  - Transition to RUNNING

### 1.5 Pack CTE aggStateKeys for DBSPJ

When forwarding to DBSPJ (SCAN_FRAGREQ), include CTE aggStateKeys in the
aggKeys section. Extended format:

```
Existing: [nodeId, baseAggStateKey] pairs for main aggregation

Extended:
  [nodeId1, mainAggStateKey1] ...          // Main agg (may be empty)
  [CTE_KEYS_MARKER = 0xCCEE0000]          // Sentinel
  [numCtes]
  For each CTE:
    [cteId]
    [numNodes]
    [nodeId1, cteAggStateKey1] [nodeId2, cteAggStateKey2] ...
```

DBSPJ parses this extended format and stores CTE keys in CteContext.

### 1.6 Release CTE hash tables

Extend `sendJoinAggReleaseReqs()`:
- After releasing main agg states, also release CTE agg states
- For each CTE, for each node: send RELEASE_REQ with CTE aggStateKey

## Part 2: DBSPJ Changes

### 2.1 Parse CTE aggStateKeys

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`**

In the existing aggKeys section parser (SCAN_FRAGREQ handler, ~line 1400):
- After reading main agg keys, check for CTE_KEYS_MARKER
- If present, read CTE keys and populate CteContext per cteId
- Set CteContext state based on phase:
  - During CTE scan phase: CTE_NOT_STARTED or CTE_MATERIALIZING
  - After DBTC signals CTEs READY: CTE_READY

### 2.2 Handle QN_CTE_SUBTREE in build phase

**File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`**

Add to `getOpInfo()`:
```cpp
case QueryNode::QN_CTE_SUBTREE:
  return &Dbspj::g_CteSubtreeOpInfo;
```

Implement `g_CteSubtreeOpInfo` handlers:
- `cte_subtree_build()`: Parse QN_CteSubtreeNode, extract cteId and embedded
  scan node. Create TreeNode for the CTE scan using existing scan_frag_build()
  infrastructure. Mark with CTE-specific flags so results go to hash table
  (via aggStateKey with CTE_MODE_FLAG, already set up by DBTC).
- `cte_subtree_start()`: Start the CTE scan (send SCAN_FRAGREQ to DBLQH
  fragments, same as a normal scan but with the CTE aggStateKey).
- CTE scan completion: when all fragments report SCAN_FRAGCONF, report
  back to DBTC that this CTE's scans are done on this DBSPJ instance.

### 2.3 CTE scan completion reporting to DBTC

New signal: CTE_SCAN_COMPLETE_REP (DBSPJ → DBTC)
```cpp
struct CteScanCompleteRep {
  Uint32 senderRef;
  Uint32 senderData;   // scanPtr.i in DBTC
  Uint32 cteId;        // Which CTE completed
};
```

When all fragments of a CTE subtree scan are done on a DBSPJ instance,
send CTE_SCAN_COMPLETE_REP to DBTC.

DBTC tracks: for each CTE, how many DBSPJ instances have reported completion.
When all instances report all CTEs done → proceed to COMPLETE phase.

### 2.4 Main SELECT start signal from DBTC

New signal: CTE_START_MAIN_REQ (DBTC → DBSPJ)
```cpp
struct CteStartMainReq {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestPtrI;  // DBSPJ's Request pool index
};
```

When DBTC receives all COMPLETE_CONFs, it sends CTE_START_MAIN_REQ to
each DBSPJ instance. DBSPJ then starts executing the main query tree
(root scan + PK lookups + CTE lookups).

### 2.5 Complete cte_parent_row() — send CTE_LOOKUP_REQ

When main SELECT is running and parent rows arrive at CTE_LOOKUP nodes:

```cpp
case CteContext::CTE_READY:
  cte_lookup_send(signal, requestPtr, treeNodePtr, rowRef, cteCtx);
  break;
```

`cte_lookup_send()`:
1. Build lookup key from parent row using key pattern (parsed in cte_build)
2. Find target node:
   - Single-node: only node in CteContext
   - Multi-node: hash key to find owner node
3. Get aggStateKey for target node from CteContext
4. Build CTE_LOOKUP_REQ with AttrInfo:
   - 5-word header + ExitOK
   - Final read: virtual columns for GB keys + agg results
   - FLUSH_AI → API resultRef/resultData
   - CORR_FACTOR32 for correlation
5. Send CTE_LOOKUP_REQ to DBLQH
6. Track outstanding count

### 2.6 CTE_LOOKUP_CONF / REF handlers

Register in DbspjInit.cpp:
```cpp
addRecSignal(GSN_CTE_LOOKUP_CONF, &Dbspj::execCTE_LOOKUP_CONF);
addRecSignal(GSN_CTE_LOOKUP_REF, &Dbspj::execCTE_LOOKUP_REF);
```

`execCTE_LOOKUP_CONF()`:
- TRANSID_AI already sent by DBLQH to API via FLUSH_AI
- Decrement outstanding, check batch complete

`execCTE_LOOKUP_REF()`:
- GROUP_NOT_FOUND + LEFT JOIN: send NULL row to API
- GROUP_NOT_FOUND + INNER JOIN: skip row
- Other errors: abort

### 2.7 Extend CteContext

**File: `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`**

```cpp
struct CteContext {
  Uint32 m_cteId;
  Uint32 m_state;              // NOT_STARTED → MATERIALIZING → READY → FAILED
  Uint32 m_numResultCols;
  // Per-node CTE hash table handles (from DBTC via aggKeys section)
  Uint32 m_numNodes;
  Uint32 m_nodeIds[MAX_NDB_DATA_NODES];
  Uint32 m_aggStateKeys[MAX_NDB_DATA_NODES];
};
```

## Part 3: New Signal Definitions

| Signal | Direction | Purpose |
|--------|-----------|---------|
| CTE_SCAN_COMPLETE_REP | DBSPJ → DBTC | Report that a CTE's scans are done on this DBSPJ instance |
| CTE_START_MAIN_REQ | DBTC → DBSPJ | All CTEs ready, start main SELECT execution |

Existing signals reused:
- JOIN_AGG_SETUP_REQ/CONF (DBTC → DBLQH, with CTE_MODE_FLAG)
- JOIN_AGG_COMPLETE_REQ/CONF (DBTC → DBLQH)
- JOIN_AGG_RELEASE_REQ (DBTC → DBLQH)
- CTE_LOOKUP_REQ/CONF/REF (DBSPJ → DBLQH, already implemented in Phase 6B)
- SCAN_FRAGREQ/CONF (DBSPJ → DBLQH, existing scan infrastructure)

## Implementation Order

```
Step 1: DBTC CTE parsing + SETUP
  - Parse CTE defs from KeyInfo section
  - Send SETUP_REQ with CTE_MODE_FLAG for each CTE
  - Collect CTE aggStateKeys
  - Pack into DBSPJ aggKeys section

Step 2: DBSPJ QN_CTE_SUBTREE build + CTE scan execution
  - getOpInfo() dispatch for QN_CTE_SUBTREE
  - cte_subtree_build(): parse embedded scan, create TreeNode
  - cte_subtree_start(): start CTE scan fragments
  - CTE scan completion → CTE_SCAN_COMPLETE_REP to DBTC

Step 3: DBTC CTE COMPLETE coordination
  - Track CTE_SCAN_COMPLETE_REP from all DBSPJ instances
  - Send COMPLETE_REQ when all done
  - Handle COMPLETE_CONF
  - Send CTE_START_MAIN_REQ to DBSPJ instances

Step 4: DBSPJ main SELECT execution with CTE_LOOKUP
  - Handle CTE_START_MAIN_REQ: start main query tree
  - cte_parent_row() → cte_lookup_send()
  - execCTE_LOOKUP_CONF / execCTE_LOOKUP_REF

Step 5: DBTC cleanup
  - Release CTE aggStateKeys via RELEASE_REQ
```

## Files to Modify

| File | Changes |
|------|---------|
| `dbtc/Dbtc.hpp` | CteInfo struct, ScanRecord CTE fields, new states |
| `dbtc/DbtcMain.cpp` | Parse CTE, SETUP with CTE_MODE, COMPLETE coordination, RELEASE |
| `dbtc/DbtcInit.cpp` | Register CTE_SCAN_COMPLETE_REP handler |
| `dbspj/Dbspj.hpp` | Extend CteContext, add handler decls, g_CteSubtreeOpInfo |
| `dbspj/DbspjMain.cpp` | QN_CTE_SUBTREE handlers, cte_lookup_send, CONF/REF handlers |
| `dbspj/DbspjInit.cpp` | Register CTE_LOOKUP_CONF/REF, CTE_START_MAIN_REQ |
| `signaldata/CteScan.hpp` | New: CTE_SCAN_COMPLETE_REP, CTE_START_MAIN_REQ |
| `kernel/GlobalSignalNumbers.h` | New GSN numbers for CTE signals |
