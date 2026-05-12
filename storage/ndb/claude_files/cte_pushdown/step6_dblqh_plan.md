# Step 6 — DBLQH Phases: CTE Distributed Hash Table + Lookup

## Context

The aggregation hash table in DBLQH has lifecycle: SETUP → rows processed → COMPLETE → send results (erase) → RELEASE. For CTE lookups, COMPLETE must instead **build a distributed hash table** across data nodes, then serve point lookups via CTE_LOOKUP_REQ.

**Multi-node challenge:** With 2+ data nodes, each node has only partial aggregation results after its local scan. COMPLETE must redistribute group rows so each group lives on exactly one node (its hash-partition owner). CTE_LOOKUP_REQ then hashes the key to find the right node.

**Result delivery:** CTE_LOOKUP_REQ behaves like LQHKEYREQ — sends result as TRANSID_AI via flushAI to DBSPJ/API, then sends CTE_LOOKUP_CONF (or CTE_LOOKUP_REF if group not found).

## Signal Naming Conventions

- `_REQ/_CONF/_REF` — Request/response pair where sender waits for reply
- `_ORD` — Fire-and-forget order requiring action (no confirmation)
- `_REP` — Fire-and-forget report/notification requiring no action

## Critical Files

| File | Purpose |
|------|---------|
| `GlobalSignalNumbers.h` | GSN numbers for new signals |
| `CteLookup.hpp` (new) | CTE_LOOKUP_REQ/CONF/REF signal structs |
| `JoinAgg.hpp` | Extend with REDISTRIBUTE_ORD, FINAL_REP |
| `JoinAggregationState.hpp` | CTE mode flag, redistribution state |
| `DblqhMain.cpp` | CTE_LOOKUP_REQ handler, redistribution logic |
| `DblqhProxy.cpp` | SETUP with CTE mode |
| `DblqhInit.cpp` | Signal registration |
| `Dblqh.hpp` | Handler declarations |
| `JoinAggInterpreter.hpp` | lookupGroup(), mergeGroup() |
| `AggHashTable.hpp` | find() already exists |
| `storage/ndb/test/ndbapi/` | SignalSender test program |

## Phased Plan

### Phase 6A: Signal Definitions + CTE Mode Flag

**Goal:** Define all new signals and add CTE mode to JoinAggregationState.

**Signals:**

| GSN | Name | Type | Purpose |
|-----|------|------|---------|
| 965 | GSN_CTE_LOOKUP_REQ | REQ | Lookup group in CTE hash table |
| 966 | GSN_CTE_LOOKUP_CONF | CONF | Lookup result (found, with row via TRANSID_AI) |
| 967 | GSN_CTE_LOOKUP_REF | REF | Lookup failed (group not found, or error) |
| 968 | GSN_JOIN_AGG_REDISTRIBUTE_ORD | ORD | Send group row to hash-owner node (action) |
| 969 | GSN_JOIN_AGG_FINAL_REP | REP | "All my groups sent" notification (report) |

**New file `CteLookup.hpp`:**
```cpp
struct CteLookupReq {
  Uint32 senderRef;       // DBSPJ block reference
  Uint32 senderData;      // TreeNode pointer (for correlation)
  Uint32 aggStateKey;     // Hash table handle
  Uint32 keyLen;          // Key length in bytes
  // Long section 0: lookup key (AttributeHeader-encoded)
  static constexpr Uint32 SignalLength = 4;
};
struct CteLookupConf {
  Uint32 senderRef;
  Uint32 senderData;
  static constexpr Uint32 SignalLength = 2;
  // Result row already sent as TRANSID_AI via flushAI
};
struct CteLookupRef {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 errorCode;       // Not-found or internal error
  static constexpr Uint32 SignalLength = 3;
};
```

**Extend `JoinAgg.hpp`:**
```cpp
struct JoinAggRedistributeOrd {
  Uint32 aggStateKey;     // Destination state on receiving node
  Uint32 keyLen;          // Group key length in bytes
  Uint32 valueLen;        // Accumulator data length in bytes
  // Long section 0: [key_data] + [accumulator_data]
  static constexpr Uint32 SignalLength = 3;
};
struct JoinAggFinalRep {
  Uint32 aggStateKey;
  Uint32 senderNodeId;    // Which node finished sending
  static constexpr Uint32 SignalLength = 2;
};
```

**`JoinAggregationState.hpp`** — Add CTE fields:
```cpp
bool m_cte_mode;                    // Skip send-and-erase on COMPLETE
Uint32 m_cte_nodes_finalized;       // Nodes that sent FINAL_REP
Uint32 m_cte_total_nodes;           // Total data nodes participating
// New state values:
CTE_REDISTRIBUTING = 10,  // Sending groups to hash-owner nodes
CTE_READY = 11             // Distributed hash table ready for lookups
```

**Test:** Build only.

---

### Phase 6B: Local CTE_LOOKUP (Single Node)

**Goal:** On a single data node, COMPLETE in CTE mode skips send-and-erase, hash table stays alive, CTE_LOOKUP_REQ does point lookup and sends result via flushAI + CTE_LOOKUP_CONF.

**Changes:**

1. **`DblqhMain.cpp` — `continueJoinAggMerge()`:** After merge/finalize, if `state->m_cte_mode`: transition to `CTE_READY`, send COMPLETE_CONF, skip `continueJoinAggSend()`

2. **`DblqhMain.cpp` — `execCTE_LOOKUP_REQ()`:**
   - Decode aggStateKey → get JoinAggregationState
   - Validate state is CTE_READY
   - `lookupGroup(key, keyLen)` on the interpreter's hash table
   - **If found:** Format result as TRANSID_AI (same format as `continueJoinAggSend()` single-group), send via flushAI to DBSPJ, then send CTE_LOOKUP_CONF
   - **If not found:** Send CTE_LOOKUP_REF (LEFT JOIN → NULL row handled by DBSPJ)

3. **`JoinAggInterpreter.hpp`** — Add `lookupGroup()`:
   ```cpp
   const char* lookupGroup(const char* key, Uint32 keyLen) const {
     return m_gb_map ? m_gb_map->find(key, keyLen) : nullptr;
   }
   ```

4. **Signal registration** in `DblqhInit.cpp`.

**Test (SignalSender, 1 node):**
1. Create table, insert rows
2. SETUP_REQ with cte_mode=true, GROUP BY + COUNT(*) program
3. Scan rows into aggregation
4. COMPLETE_REQ → COMPLETE_CONF (no TRANSID_AI sent)
5. CTE_LOOKUP_REQ(known key) → TRANSID_AI with result + CTE_LOOKUP_CONF
6. CTE_LOOKUP_REQ(unknown key) → CTE_LOOKUP_REF
7. RELEASE_REQ → cleanup

---

### Phase 6C: Hash Redistribution (Multi-Node)

**Goal:** On COMPLETE, each node redistributes groups to build a distributed hash table.

**Algorithm:**
1. Node finalizes local aggregation (merge if MUTEX_FREE)
2. Iterates local hash table:
   - `hash(key) % num_data_nodes` → owner node
   - If self: keep
   - If other: send JOIN_AGG_REDISTRIBUTE_ORD, erase locally
3. When done: send JOIN_AGG_FINAL_REP to all other nodes
4. **Receiving side:**
   - Buffer incoming REDISTRIBUTE_ORD if local finalization not yet done
   - When ready: for each incoming group:
     - If key exists locally: **merge** accumulators (SUM→add, COUNT→add, MIN→min, MAX→max)
     - If key is new: insert into hash table
   - When FINAL_REP received from all other nodes AND own redistribution done → CTE_READY
5. CTE_LOOKUP_REQ from DBSPJ hashes key → routes to correct node

**Changes:**

1. **`DblqhMain.cpp`** — CTE COMPLETE path:
   - `continueJoinAggRedistribute()` — iterates hash table, batches REDISTRIBUTE_ORD via CONTINUEB
   - `execJOIN_AGG_REDISTRIBUTE_ORD()` — receives group, merges or inserts
   - `execJOIN_AGG_FINAL_REP()` — tracks finalization count, transitions to CTE_READY

2. **`JoinAggInterpreter`** — Add `mergeGroup(key, keyLen, incomingAccumulators)`:
   - Find existing group, update accumulators using merge logic

**Test (SignalSender, 2-node cluster):**
1. Table partitioned across 2 nodes, insert rows so groups span both
2. SETUP + scan + COMPLETE on both nodes → redistribution
3. CTE_LOOKUP_REQ(key) → routed to owner → correct merged result
4. RELEASE on both nodes

---

### Phase 6D: LIMIT Support in CTE Mode

**Goal:** CTE with `LIMIT N` limits groups materialized.

- Set `m_max_groups = N` during SETUP
- In CTE mode, evicted groups silently dropped (no TRANSID_AI)
- After redistribution, distributed table has ≤N groups per partition (approximate global limit)

**Test:** SignalSender: max_groups=5, 10 keys, verify ≤5 survive, lookups correct.

---

### Phase 6E: ORDER BY + LIMIT (Distributed Sort)

**Goal:** CTE with `ORDER BY ... LIMIT N` needs global sort.

**Approach (simple):** Route ALL groups to one designated node. That node sorts locally + applies LIMIT. For small result sets this is fine. Distributed sort deferred to optimization.

**Test:** Deferred to after basic CTE works.

---

## Implementation Order

| Phase | Deliverable | Test | Nodes |
|-------|-------------|------|-------|
| **6A** | Signal defs, CTE mode flag | Build only | — |
| **6B** | CTE_LOOKUP_REQ handler, flushAI result delivery | SignalSender | 1 |
| **6C** | REDISTRIBUTE_ORD, FINAL_REP, distributed hash table | SignalSender | 2 |
| **6D** | LIMIT in CTE mode | SignalSender | 1 |
| **6E** | ORDER BY + LIMIT (single-node sort) | Deferred | — |

**Critical path: 6A → 6B → 6C.** These deliver a working distributed CTE hash table with point lookups via flushAI.
