# Design Plan: CTE Support with Materialized Aggregate Tables for NDB Pushdown

## Context

### The Problem

ML feature serving requires queries that combine:
- **Precomputed features**: hundreds of columns across multiple tables, joined by PK
- **Online aggregates**: real-time aggregations over event tables (purchases, page views, clicks)

A single query must return both in one result set. The challenge: multiple independent event tables each need GROUP BY aggregation, but joining them all produces a cross product (fan-out problem). And GROUP BY on hundreds of feature columns is impractical.

### The Solution: CTEs with Pushdown Materialization

SQL Common Table Expressions (WITH ... AS) allow each event table's aggregation to be expressed as a named subquery that materializes into a virtual 1:1 table. The main query then joins these CTE results with feature tables — all PK lookups, no GROUP BY needed.

```sql
WITH
  purchase_agg AS (
    SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
    FROM purchases WHERE ts > '2026-01-01' GROUP BY user_id
  ),
  view_agg AS (
    SELECT user_id, COUNT(*) AS views, SUM(duration) AS time
    FROM page_views WHERE ts > '2026-01-01' GROUP BY user_id
  )
SELECT f.*, d.*, p.cnt, p.total, v.views, v.time
FROM user_features f
JOIN user_demographics d ON d.user_id = f.user_id
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
LEFT JOIN view_agg v ON v.user_id = f.user_id
WHERE f.user_id IN (1001, 1002, 1003)
```

**Functional dependency** (SQL:2003): since `f.user_id` is PK of `user_features`, all `f.*` columns are valid in SELECT without GROUP BY. The CTE results join 1:1.

### Key Architecture: Single Compound Query Tree

The full query — CTEs and main query — is sent as **one compound Query Tree** to DBSPJ. Each CTE becomes an embedded sub-tree within the compound tree. DBSPJ orchestrates everything:

- Starts CTE materialization scans and main query PK lookups **in parallel**
- Only blocks when a CTE_LOOKUP node needs a result from a CTE that hasn't completed yet
- For latency-sensitive queries (1-10ms), this parallelism is critical: feature table lookups proceed while event table scans are still running

---

## Component Overview

| Component | Location | Change |
|-----------|----------|--------|
| RonSQL Lexer | `storage/ndb/src/ronsql/RonSQLLexer.l` | Add WITH keyword |
| RonSQL Parser | `storage/ndb/src/ronsql/RonSQLParser.y` | CTE grammar rules |
| RonSQL AST | `storage/ndb/src/ronsql/RonSQLCommon.hpp` | CTE data structures |
| RonSQL Preparer | `storage/ndb/src/ronsql/RonSQLPreparer.cpp` | CTE planning & compilation |
| QueryPlanner | `storage/ndb/src/ronsql/QueryPlanner.cpp` | CTE table references in join tree |
| NDB API QueryBuilder | `storage/ndb/src/ndbapi/NdbQueryBuilder*` | CTE sub-trees in query tree |
| QueryTree signal | `storage/ndb/include/kernel/signaldata/QueryTree.hpp` | QN_CTE_SCAN + QN_CTE_LOOKUP |
| DBSPJ | `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` | Compound tree execution |
| DBSPJ types | `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` | Request with embedded CTE sub-requests |
| JoinAggInterpreter | `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` | Hash table lookup interface |
| New signal | `storage/ndb/include/kernel/signaldata/CteLookup.hpp` | CTE_LOOKUP_REQ/CONF |

---

## Phase 1: RonSQL CTE Parsing

### 1.1 Lexer — Add WITH keyword

**File:** `storage/ndb/src/ronsql/Keywords.hpp`

Add `WITH` to the keyword list.

### 1.2 Parser — CTE Grammar Rules

**File:** `storage/ndb/src/ronsql/RonSQLParser.y`

```
statement:
    cte_list selectstatement SEMICOLON
  | selectstatement SEMICOLON
  ;

cte_list:
    WITH cte_def
  | cte_list COMMA cte_def
  ;

cte_def:
    IDENTIFIER AS T_LEFT selectstatement T_RIGHT
  ;
```

### 1.3 AST — CTE Data Structures

**File:** `storage/ndb/src/ronsql/RonSQLCommon.hpp`

```cpp
struct CteDefinition {
  const char* name;           // CTE alias (e.g., "purchase_agg")
  SelectStatement* stmt;      // The CTE's SELECT statement
  CteDefinition* next;        // Linked list
};
```

Extend `SelectStatement` with `CteDefinition* cte_list`.

### 1.4 Table Reference Resolution

In `load_join()` and `load_single_table()`, before `dict->getTable()`: check `m_ast.cte_list` for a matching name. If found, mark the join operation as a CTE reference and extract the virtual schema (GROUP BY columns = PK, aggregate outputs = value columns).

---

## Phase 2: RonSQL Query Planning for CTEs

### 2.1 CTE Analysis and Validation

New function `analyze_ctes()` in `RonSQLPreparer.cpp`:

For each CTE:
1. Validate it contains GROUP BY
2. Validate GROUP BY columns match the join key in the main query's ON condition
3. Record virtual schema: PK = GROUP BY columns, values = aggregate columns
4. Determine if the CTE is a single-table scan or a multi-table join
5. If the CTE itself contains JOINs, plan it as a full join tree (using existing `QueryPlanner::plan()`) with the aggregate applied at the leaf — this is the same pattern as existing pushdown join aggregation

### 2.2 Join Plan with CTE References

**File:** `storage/ndb/src/ronsql/QueryPlanner.cpp`

Extend `JoinOp` with `CTE_LOOKUP` type. When `plan()` encounters a CTE reference, create a `JoinOp` with `type = CTE_LOOKUP` and a pointer to the `CteDefinition`.

### 2.3 Compilation — Single Compound Query Tree

RonSQL compiles the entire query into **one** NdbQueryDef containing:

1. **CTE sub-trees**: Each CTE becomes an embedded scan+aggregate sub-tree. These are self-contained: source table scan, filter, aggregate program, GROUP BY specification.

2. **Main query tree**: The joined tree with PK lookups for feature tables and CTE_LOOKUP nodes for CTE references. Each CTE_LOOKUP node carries a reference (cteId) to its corresponding CTE sub-tree.

The NdbQueryDef serializes both the CTE sub-trees and the main tree into a single QueryTree signal sent to DBSPJ.

---

## Phase 3: NDB API — Compound Query Tree

### 3.1 QueryTree Signal Extension

**File:** `storage/ndb/include/kernel/signaldata/QueryTree.hpp`

New node types:

```cpp
enum OpType {
  QN_LOOKUP     = 0x1,
  QN_SCAN_FRAG  = 0x4,
  QN_CTE_SCAN   = 0x6,     // NEW: CTE materialization scan+aggregate
  QN_CTE_LOOKUP = 0x7,     // NEW: lookup into materialized CTE
  QN_END        = 0
};
```

**QN_CTE_SUBTREE** — a container that wraps an embedded sub-tree representing one CTE:
- `cteId` — unique ID linking this to corresponding QN_CTE_LOOKUP nodes
- `numNodes` — number of nodes in the embedded sub-tree
- Followed by a sequence of standard QueryTree nodes (QN_SCAN_FRAG, QN_LOOKUP, etc.) that form the CTE's own join tree
- The leaf node of the sub-tree carries the aggregate program and GROUP BY specification
- The aggregate results are materialized into a hash table keyed by GROUP BY columns

A CTE sub-tree can be:
- **Single-table**: one QN_SCAN_FRAG with aggregate (simple case)
- **Multi-table join**: a full join tree (QN_SCAN_FRAG root + QN_LOOKUP children) with the aggregate at the leaf — same pattern as existing pushdown join aggregation, but the results go to a hash table instead of the API

**QN_CTE_LOOKUP** carries:
- `cteId` — which CTE hash table to look up
- Key column mapping — how to extract the lookup key from parent row
- Expected result columns — number and types of aggregate values

### 3.2 NdbQueryBuilder Extension

**File:** `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`

New methods:
```cpp
NdbQueryOperationDef* scanCte(
    const NdbDictionary::Table* sourceTable,
    Uint32 cteId,
    const NdbQueryOperand* groupByKeys[],
    const AggregateProgram* aggProgram);

NdbQueryOperationDef* lookupCte(
    Uint32 cteId,
    const NdbQueryOperationDef* parent,
    const NdbQueryOperand* keys[]);
```

### 3.3 Serialization

In `doSend()`, serialize CTE sub-trees first (QN_CTE_SCAN nodes), then the main tree nodes. DBSPJ processes them in order, recognizing the compound structure.

---

## Phase 4: DBSPJ — Compound Query Tree Execution

### 4.1 Compound Request Structure

**File:** `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`

Each CTE sub-tree gets its own embedded Request-like context within the parent Request:

```cpp
struct CteContext {
  Uint32 m_cteId;                    // CTE identifier
  Uint32 m_state;                    // MATERIALIZING, READY, FAILED
  Uint32 m_requestPtrI;             // Pool index (.i) of CTE's Request
  // Use Request_pool::getValidPtr() to resolve to .p
  // The CTE's Request uses standard TreeNode/scan/aggregate machinery.
  // A flag on the Request (m_isCte) redirects aggregate results to the
  // hash table instead of sending them to the API.

  // Hash table handles per LDM thread (allocated via lc_ndbd_pool_malloc):
  Uint32 m_numPartitions;            // Actual partition count
  Uint32* m_hashTableHandles;        // Dynamically allocated array

  // Pending lookups queued while CTE is still materializing:
  // (flushed when m_state transitions to READY)
};

struct Request {
  // ... existing fields ...
  CteContext* m_cteContexts;          // NEW: allocated via lc_ndbd_pool_malloc
  Uint32 m_numCtes;                   // Number of CTE sub-trees
  Uint32 m_ctesReady;                 // Count of completed CTEs
  bool m_isCte;                       // NEW: true if this Request is a CTE sub-tree
  Uint32 m_parentCteContextIdx;      // NEW: index into parent's m_cteContexts (if m_isCte)
  Uint32 m_parentRequestPtrI;        // NEW: pool index (.i) of main query's Request (if m_isCte)
};
```

All cross-struct references use pool indices (`.i` values) resolved via `getValidPtr()`, ensuring magic number validation and safe pointer access.

This design reuses the entire Request infrastructure for CTEs:
- CTE sub-trees get their own Request with standard TreeNodes, scan state, aggregate handling
- The `m_isCte` flag tells aggregate result handlers to write to the hash table instead of sending TRANSID_AI to the API
- The main query's Request holds `m_cteContexts` pointing to the CTE Requests
- Each CTE Request stores pool indices for back-references to its parent
```

### 4.2 Build Phase — Parse Compound Tree

In `execSCAN_FRAGREQ()` / `build()`:

1. Parse the serialized QueryTree
2. When encountering a `QN_CTE_SUBTREE` container:
   - Allocate a new `CteContext` and a new `Request` for the CTE sub-tree
   - Set `cteRequest->m_isCte = true` and link back to parent
   - Parse the embedded nodes (QN_SCAN_FRAG, QN_LOOKUP, etc.) into the CTE Request's TreeNodes using standard build logic
   - The CTE Request reuses all existing OpInfo handlers (scan, lookup, aggregate) — the only difference is result routing
3. When encountering `QN_CTE_LOOKUP` nodes in the main tree, create a TreeNode with `g_CteOpInfo` and link it to the corresponding `CteContext` via `cteId`
4. Build the rest of the main query tree normally

### 4.3 Execution Phase — Parallel Start

**The critical latency optimization:**

When DBSPJ starts executing the compound tree:

1. **Immediately start all CTE scans** — send JOIN_AGG_SETUP_REQ for each CTE to the relevant LDM threads. These run asynchronously.

2. **Simultaneously start the main query tree** — begin the root scan/lookup. PK lookups against real NDB tables (feature tables, demographics, etc.) proceed immediately without waiting for CTEs.

3. **Block only at CTE_LOOKUP nodes** — when the join tree reaches a CTE_LOOKUP node and the parent has produced a row that needs CTE data:
   - If the CTE is already READY: send CTE_LOOKUP_REQ immediately
   - If the CTE is still MATERIALIZING: queue the lookup. When the CTE completes, flush all queued lookups.

This means for a query like:
```
SCAN user_features (root)
  ├── PK_LOOKUP user_demographics   ← starts immediately
  ├── CTE_LOOKUP purchase_agg       ← waits if CTE not ready
  └── CTE_LOOKUP view_agg           ← waits if CTE not ready
```

The PK lookups for `user_demographics` execute in parallel with the CTE materialization scans. For short queries (1-10ms), the CTE scans may complete by the time the first feature table lookup returns, eliminating any CTE wait entirely.

### 4.4 CTE_LOOKUP_REQ — Hash Table Lookup Signal

**New file:** `storage/ndb/include/kernel/signaldata/CteLookup.hpp`

```cpp
struct CteLookupReq {
  Uint32 senderRef;       // DBSPJ block reference
  Uint32 senderData;      // TreeNode pointer (for correlation)
  Uint32 cteHandle;       // Hash table handle from materialization
  Uint32 keyLen;          // Key length in words
  // Key data in long section
};

struct CteLookupConf {
  Uint32 senderRef;       // LDM block reference
  Uint32 senderData;      // TreeNode pointer (echo)
  Uint32 found;           // 1 = found, 0 = not found (LEFT JOIN → NULL)
  // Row data in long section (aggregate values)
};
```

### 4.5 Distribution — Simple Hash

CTE hash tables are partitioned implicitly by the scan: each LDM thread's hash table holds groups from the fragments it scanned. For partition-aligned GROUP BY keys (e.g., `user_id` on a table partitioned by `user_id`), the lookup key directly identifies the LDM thread via a simple hash — no DBDIH consultation needed.

Initial implementation requires partition alignment. Non-aligned keys need a merge phase (future work).

### 4.6 CTE Completion Handling

When a CTE scan completes (JOIN_AGG_COMPLETE_CONF received from all LDM threads):

1. Set `m_cteContexts[cteId].m_state = READY`
2. Increment `m_ctesReady`
3. Flush any queued CTE_LOOKUP requests that were waiting for this CTE
4. Continue main query tree execution for any blocked branches

### 4.7 TreeNode Operations for CTE_LOOKUP

```cpp
static const OpInfo g_CteOpInfo = {
  &Dbspj::cte_build,          // Parse QN_CTE_LOOKUP from tree
  &Dbspj::cte_prepare,        // Nothing to prepare (CTE scans start separately)
  nullptr,                     // No self-exec
  &Dbspj::cte_execNODE,       // Parent row arrived → do hash lookup or queue
  &Dbspj::cte_parent_batch_complete,
  &Dbspj::cte_cleanup         // Release hash table handles
};
```

`cte_execNODE()` logic:
```
if (cteContext.m_state == READY) {
  // CTE materialized — do lookup now
  send CTE_LOOKUP_REQ to appropriate LDM thread
} else {
  // CTE still materializing — queue this lookup
  add (parent_row, correlation) to cteContext.m_pending_lookups
}
```

---

## Phase 5: JoinAggInterpreter — Hash Table Lookup Interface

### 5.1 Lookup Method

**File:** `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp`

Add public lookup method:

```cpp
const char* lookupGroup(const char* key, Uint32 keyLen) const {
  return m_gb_map->find(key, keyLen);
}
```

`GBHashTable::find()` already exists in `AggHashTable.hpp`.

### 5.2 Result Extraction

New method to extract a single group's aggregates into TRANSID_AI format:

```cpp
Int32 extractGroupResult(const char* groupData, Uint32* outBuffer,
                         Uint32 bufferSize, Uint32* bytesWritten);
```

Similar to existing `getResultData()` but for a single group.

### 5.3 CTE_LOOKUP_REQ Handler in DBLQH

New signal handler in DBLQH that:
1. Receives CTE_LOOKUP_REQ with hash table handle + key
2. Calls `JoinAggInterpreter::lookupGroup(key, keyLen)`
3. If found: extract result, send CTE_LOOKUP_CONF with row data
4. If not found: send CTE_LOOKUP_CONF with `found = 0` (LEFT JOIN → NULL row)

---

## Phase 6: Signal Flow — End to End

### Single compound query: everything sent at once

```
RonSQL (API)
  │
  SCAN_FRAGREQ (compound query tree:
  │              QN_CTE_SUBTREE[0]: {scan purchases + agg} → hash table
  │              QN_CTE_SUBTREE[1]: {scan page_views + agg} → hash table
  │              (CTE sub-trees can be full join trees, e.g.:
  │               {scan purchases → lookup categories + agg})
  │              QN_SCAN_FRAG: user_features (root)
  │              QN_LOOKUP: user_demographics
  │              QN_CTE_LOOKUP[0]: purchase_agg
  │              QN_CTE_LOOKUP[1]: view_agg)
  ──────────────────────────────────────────────────► DBSPJ
                                                        │
                                   ┌────────────────────┼────────────────────┐
                                   │ PARALLEL START      │                    │
                                   ▼                     ▼                    ▼
                            CTE[0] sub-tree:      CTE[1] sub-tree:     Main query:
                            Execute pushed join   Execute pushed join  Root scan
                            (may be single scan   (may be single scan  user_features
                             or scan+lookups)      or scan+lookups)         │
                            → build hash table    → build hash table       │
                                   │                     │                 │
                                   │                     │          For each user row:
                                   │                     │          ├─ LQHKEYREQ → demographics
                                   │                     │          │  (proceeds immediately)
                                   │                     │          │
                            SETUP_CONF              SETUP_CONF      ├─ CTE_LOOKUP[0]:
                            CTE[0] READY            CTE[1] READY    │  if READY → CTE_LOOKUP_REQ
                                   │                     │          │  else → queue
                                   └─────────┬───────────┘          │
                                             │                      ├─ CTE_LOOKUP[1]:
                                        Flush queued                │  if READY → CTE_LOOKUP_REQ
                                        CTE lookups                 │  else → queue
                                             │                      │
                                             └──────────────────────┘
                                                        │
                                              Combine all results
                                              TRANSID_AI ──────────► API
```

### Cleanup

```
DBSPJ
  │ (query complete)
  ├── JOIN_AGG_RELEASE_REQ → DBLQH (for each CTE, each LDM thread)
  │   └── Free hash table memory (MemChunks deallocated)
  └── Release Request + all TreeNodes
```

---

## Phase 7: Implementation Order

### Step 1: RonSQL CTE Parsing (no execution)
- Add WITH keyword, parser rules, AST structures
- Parse CTEs and store in SelectStatement
- Validate but don't execute
- Test: parse CTE queries without error

### Step 2: RonSQL CTE Planning
- Resolve CTE table references in join tree
- Add CTE_LOOKUP to JoinOp types
- Compile CTE aggregate programs
- Generate compound query tree with embedded CTE sub-trees
- Test: EXPLAIN shows CTE materialization + main query plan

### Step 3: NDB API Compound Query Tree
- Add QN_CTE_SUBTREE and QN_CTE_LOOKUP to QueryTree
- QN_CTE_SUBTREE wraps a sequence of standard nodes (reusing existing QN_SCAN_FRAG, QN_LOOKUP) to form the CTE's join tree
- Extend NdbQueryBuilder with CTE operations
- Serialize compound tree in doSend(): CTE sub-trees first, then main tree
- Test: API sends valid compound query tree to DBSPJ

### Step 4: DBSPJ Compound Tree Build
- Parse compound tree: create CteContext with its own sub-tree of TreeNodes for each QN_CTE_SUBTREE
- CTE sub-trees reuse existing TreeNode types and OpInfo handlers (scans, lookups) but redirect aggregate results to hash table instead of API
- Create CTE_LOOKUP TreeNodes in main tree linked to CteContexts via cteId
- Test: compound tree builds correctly (verify via debug logging)

### Step 5: DBSPJ Parallel Execution
- Start CTE scans (JOIN_AGG_SETUP_REQ) and main query simultaneously
- Main query PK lookups proceed without waiting for CTEs
- CTE_LOOKUP nodes queue or execute based on CTE readiness
- Flush queued lookups when CTE completes
- Test: feature table lookups return before CTE scans complete

### Step 6: CTE_LOOKUP_REQ Signal + Handler
- Define signal in CteLookup.hpp
- DBLQH handler: hash table lookup, return result or NULL
- DBSPJ: send lookup, handle response, integrate into join tree
- Simple hash-based distribution (partition-aligned keys only)
- Test: single CTE + feature table join returns correct results

### Step 7: CTE with Internal Joins
- CTE sub-trees that are themselves multi-table join trees
- Reuse existing pushed join infrastructure: CTE sub-tree root is a scan, children are lookups
- Aggregate at the leaf of the CTE join tree, results go to hash table
- Test: CTE that joins two tables before aggregating (e.g., purchases JOIN categories)

### Step 8: Multi-CTE + End-to-End
- Multiple CTE sub-trees in one compound tree
- Parallel CTE scans + parallel CTE lookups
- LEFT JOIN NULL propagation for missing groups
- Test: feature store query pattern with 3+ CTEs, hundreds of columns

### Step 9: Optimization
- Partition-alignment detection and validation
- Batch CTE lookups (multiple keys per signal to reduce signal overhead)
- Memory limits for hash tables (eviction / spill to disk)
- Non-aligned GROUP BY support (merge phase between materialization and lookup)

### Step 10: Extended CTE Operations (future)

These extensions are not required for the core feature store use case but are
natural generalizations of the CTE infrastructure:

1. **CTE_SCAN in main query**: The main SELECT can use a CTE as its root table
   (scan over the full materialized CTE result). This enables queries like
   `SELECT * FROM purchase_agg WHERE cnt > 5` where the CTE is the driving
   table, not just a joined lookup target.

2. **Aggregation on top of CTEs**: The main SELECT can have its own GROUP BY
   and aggregate functions over CTE results. For example, aggregating across
   multiple CTE outputs: `SELECT country, SUM(p.total) FROM ... GROUP BY country`.
   The CTE materializes per-user aggregates, then the main query re-aggregates
   by country.

3. **Filters and aggregation on CTE_LOOKUP/CTE_SCAN nodes**: CTE result nodes
   in the main query tree can carry their own scan filters and aggregation
   programs, just like regular table nodes. This allows filtering CTE results
   (e.g., `WHERE p.cnt > 10`) and further aggregation without a second query
   round-trip.

---

## Key Design Decisions

### Single compound Query Tree (not two-phase)

The entire query — CTEs and main query — is sent as one Query Tree. Each CTE is an embedded sub-tree with its own scan/aggregate nodes. DBSPJ sees the full picture and can:

- Start CTE scans and main query PK lookups **in parallel**
- Only wait at CTE_LOOKUP nodes, not globally
- Optimize scheduling based on the full dependency graph
- Cancel CTEs early if the main query completes or errors first

This is fundamentally different from a two-phase approach (materialize all CTEs, then run main query). For 1-10ms queries, eliminating the serial wait between phases can halve the latency.

### New signal (CTE_LOOKUP_REQ) instead of LQHKEYREQ

LQHKEYREQ goes through DBLQH's full key operation path: DBDIH distribution, ACC hash index, TUP tuple storage, transaction coordinator. CTE lookups need none of this — the data is in an in-memory hash table. A dedicated signal is simpler and faster.

### Hash-based distribution instead of DBDIH

CTE hash tables don't exist in the NDB dictionary. Each LDM thread's hash table holds groups from its scanned fragments. For partition-aligned GROUP BY keys, a simple hash of the key maps to the correct LDM thread. No DBDIH needed.

### Partition alignment requirement (initial)

If the event table is partitioned by `user_id` and the CTE groups by `user_id`, each group exists in exactly one LDM thread. No cross-thread merge needed. Non-aligned keys need a merge phase between materialization and lookup — complex, deferred to Step 8.

---

## Verification

### Integration Tests (MTR)
- Single CTE + feature table: correct aggregate values joined with features
- Multiple CTEs: independent aggregation on different event tables
- LEFT JOIN with missing groups: NULL propagation
- Large batch: 1000+ user_ids with varied group counts
- Feature store pattern: hundreds of feature columns + 3 CTE aggregates
- Latency test: verify PK lookups start before CTE scans complete

### Performance Tests
- Compare CTE pushdown vs client-side aggregation + join
- Measure parallel execution: main query latency with/without CTE overlap
- Hash table build time vs scan time
- Verify no cross-product inflation
