# DBLQH Pushdown Join Aggregation Architecture

## Overview

This document describes the architectural changes needed in DBLQH to support aggregation for pushdown join queries. Currently, DBLQH supports aggregation for simple scans (single table). For pushdown joins, DBSPJ coordinates multiple operations (LQHKEYREQ lookups and/or SCAN_FRAGREQ scans) across tables, and the leaf table must aggregate results across all these operations.

## Current Architecture

### Scan-Based Aggregation (Already Implemented)

For single-table scans with aggregation, DBLQH maintains state in `ScanRecord`:

```cpp
struct ScanRecord {
    Uint8 m_aggregation;                    // Flag: aggregation enabled
    Uint32 m_agg_curr_batch_size_rows;      // [0,1] indicates batch complete
    Uint32 m_agg_curr_batch_size_bytes;     // Bytes of aggregation results
    Uint32 m_agg_n_res_recs;                // Cached results count
    AggInterpreter* m_agg_interpreter;      // Aggregation interpreter instance
};
```

**Flow:**
1. `execSCAN_FRAGREQ` creates ScanRecord with aggregation program
2. DBTUP executes aggregation via `AggInterpreter::ProcessRec()` for each row
3. Results accumulate in `m_agg_interpreter->gb_map_` (grouped) or `agg_results_` (ungrouped)
4. When batch threshold reached, results sent via `TRANSID_AI`
5. `SCAN_FRAGCONF` reports aggregated row count

### Pushdown Join Flow (DBSPJ Perspective)

For pushdown joins with aggregation (see `CLAUDE.md` in dbspj):

```
CUSTOMER (scan)     → No FLUSH_AI (intermediate)
    |
    +--linked attrs--> ORDERS (lookup/scan) → No FLUSH_AI (intermediate)
                           |
                           +--linked attrs--> LINEITEM (lookup/scan, leaf)
                                                    |
                                                    v
                                           Execute aggregation
                                                    |
                                                    v
                                              FLUSH_AI → API
```

**Key Points:**
- DBSPJ sends multiple operations to leaf table:
  - `LQHKEYREQ` for key lookups (when joining on indexed columns)
  - `SCAN_FRAGREQ` for scans (range scans or full scans on leaf)
- Each operation contains:
  - Key/scan parameters for the operation
  - Linked attributes from parent tables (GROUP BY columns, aggregate source columns)
  - Aggregation program (passed through from API)
- Aggregation must accumulate across ALL operations in the batch
- Only the leaf table sends results to API

## Problem Statement

For pushdown join aggregation in DBLQH:

1. **Multiple Operations**: Unlike single-table scans where one ScanRecord handles all rows, join operations arrive as separate `LQHKEYREQ` or `SCAN_FRAGREQ` signals, potentially handled by different LDM threads

2. **Shared State Needed**: Aggregation state must persist across multiple operations that belong to the same SPJ request, and be accessible from any LDM thread

3. **Multi-Node Distribution**: The leaf table's fragments are distributed across data nodes; each node needs its own aggregation state

4. **Parent Data Access**: The aggregation must access columns from parent tables (received via linked attributes)

5. **Batch Coordination**: Must know when all operations are complete to send aggregated results

## Proposed Architecture

### Design Principles

1. **Dedicated Setup Signal**: A new signal (`JOIN_AGG_SETUP_REQ`) is sent to each data node before the batch starts, creating the aggregation state

2. **Node-Level State**: The `JoinAggregationState` is created once per data node (not per fragment) and is accessible by any thread that executes queries

3. **Selectable Concurrency Strategy**: Dbspj chooses one of two strategies per request:
   - **MUTEX_BASED**: One shared AggInterpreter with per-group mutex locking
   - **MUTEX_FREE**: One AggInterpreter per thread, no mutexes during operations; merge at completion

4. **Support Both Operations**: Both LQHKEYREQ and SCAN_FRAGREQ can participate in join aggregation

### New State Structure: JoinAggregationState

Create a new structure to hold aggregation state shared across multiple operations:

```cpp
struct JoinAggregationState {
    static constexpr Uint32 TYPE_ID = RT_DBLQH_JOIN_AGG_STATE;
    Uint32 m_magic;

    //------------------------------------------------------------------
    // Identification (immutable after creation)
    //------------------------------------------------------------------
    Uint32 m_transid[2];           // Transaction ID
    Uint32 m_senderData;           // SPJ request identifier
    Uint32 m_requestId;            // Unique request ID for this aggregation
    BlockReference m_senderRef;     // DBSPJ block reference
    BlockReference m_apiRef;        // API block reference for results

    //------------------------------------------------------------------
    // Aggregation Program (immutable after creation)
    //------------------------------------------------------------------
    Uint32* m_agg_program;              // Copy of aggregation program
    Uint32 m_agg_program_len;           // Program length in words

    //------------------------------------------------------------------
    // Concurrency Strategy (immutable after creation)
    //------------------------------------------------------------------
    enum ConcurrencyStrategy {
        MUTEX_BASED = 0,    // One shared AggInterpreter with per-group mutex
        MUTEX_FREE = 1      // One AggInterpreter per thread, no mutexes
    };
    ConcurrencyStrategy m_strategy;
    Uint32 m_num_threads;               // Number of query threads on this node

    //------------------------------------------------------------------
    // Aggregation State
    //------------------------------------------------------------------
    // MUTEX_BASED: single shared interpreter with per-group locking
    //   - m_group_map_mutex: protects GROUP BY hash map (find/insert)
    //   - group->m_mutex: per-group mutex for accumulator updates
    //
    // MUTEX_FREE: per-thread interpreters, no mutexes needed during
    //   operations since each thread writes only to its own interpreter.
    //   At completion, DblqhProxy merges all per-thread interpreters.
    //------------------------------------------------------------------
    AggInterpreter* m_agg_interpreter;  // MUTEX_BASED: shared interpreter
    AggInterpreter** m_per_thread_interpreters;
                                        // MUTEX_FREE: one per thread
                                        // Indexed by thread number
                                        // (each thread knows its own index)
                                        // Allocated via ndbd_malloc at setup:
                                        //   m_num_threads * sizeof(AggInterpreter*)

    //------------------------------------------------------------------
    // Operation Tracking (atomic operations)
    //------------------------------------------------------------------
    std::atomic<Uint32> m_outstanding_ops;  // Operations still in progress
    std::atomic<Uint32> m_completed_ops;    // Operations completed successfully
    std::atomic<Uint32> m_failed_ops;       // Operations that failed
    Uint32 m_total_ops_expected;            // Total operations in batch (0 = unknown)

    //------------------------------------------------------------------
    // Result Tracking (protected by m_interpreter_mutex)
    //------------------------------------------------------------------
    Uint32 m_agg_curr_batch_size_rows;      // Aggregated result rows (groups)
    Uint32 m_agg_curr_batch_size_bytes;     // Aggregated result bytes
    Uint32 m_rows_sent;                     // Rows already sent to API

    //------------------------------------------------------------------
    // Result Routing (immutable after creation)
    //------------------------------------------------------------------
    Uint32 m_resultRef;                 // API reference
    Uint32 m_resultData;                // API operation data reference (pointer to API object)
    Uint32 m_routeRef;                  // Route reference (TC block)

    //------------------------------------------------------------------
    // State Machine (atomic)
    //------------------------------------------------------------------
    enum State {
        IDLE = 0,
        SETUP_COMPLETE = 1,    // Ready to receive operations
        ACCUMULATING = 2,      // Receiving operations, accumulating results
        FINALIZING = 3,        // All ops done, preparing results
        SENDING_RESULTS = 4,   // Sending results to API
        COMPLETED = 5,         // All results sent
        ERROR = 6,
        ABORTING = 7
    };
    std::atomic<State> m_state;
    Uint32 m_error_code;                // Error code if m_state == ERROR

    //------------------------------------------------------------------
    // Key-based access
    //------------------------------------------------------------------
    Uint32 m_key;                       // Pool index assigned by DblqhProxy
                                        // at seize time. Returned to Dbspj in
                                        // JOIN_AGG_SETUP_CONF and carried in
                                        // every LQHKEYREQ / SCAN_FRAGREQ.
                                        // Used by Dblqh workers for O(1) lookup
                                        // via SimulatedBlock::getJoinAggState().

    //------------------------------------------------------------------
    // Timeout Management
    //------------------------------------------------------------------
    Uint32 m_creation_time;             // For timeout detection
    Uint32 m_last_activity_time;        // Last operation timestamp
};
```

### Shared State Management via SimulatedBlock

The shared state must be accessible from both `Dblqh` worker instances (which
process LQHKEYREQ/SCAN_FRAGREQ on different LDM threads) and `DblqhProxy`
(which handles setup/completion/release signals). Since both inherit from
`SimulatedBlock`, the state pool and access methods are defined there:

```cpp
class SimulatedBlock {
    // ...existing members...

    //------------------------------------------------------------------
    // Join Aggregation State Pool (node-level)
    // Shared across all block instances on the node.
    // Pool index serves as the Uint32 lookup key.
    //
    // Seize/release: called only from DblqhProxy (single-threaded),
    //   so no mutex needed for pool operations.
    // Access: getJoinAggState() is a direct pool index lookup (O(1)),
    //   safe to call from any thread. Thread-safety for the state
    //   contents is handled by mutexes within JoinAggregationState
    //   itself (group map mutex, per-group mutex).
    //------------------------------------------------------------------
    static ArrayPool<JoinAggregationState> s_joinAggStatePool;

    // Allocate a new state, returns pool index (the key).
    // Called only from DblqhProxy — no mutex needed.
    static Uint32 seizeJoinAggState();

    // Look up state by key (pool index) — O(1) array access,
    // safe from any LDM thread. Concurrent access to the returned
    // state is protected by the state's internal mutexes.
    static JoinAggregationState* getJoinAggState(Uint32 key);

    // Release state back to pool.
    // Called only from DblqhProxy — no mutex needed.
    static void releaseJoinAggState(Uint32 key);

    // Initialization (called once at node startup)
    static void initJoinAggStatePool();
};
```

**Key design points:**
- `DblqhProxy` is single-threaded, so `seizeJoinAggState()` and
  `releaseJoinAggState()` require no mutex for pool operations
- `getJoinAggState(key)` is a direct array index into the pool — O(1), no
  hash table, no mutex needed for the lookup itself
- The returned `JoinAggregationState*` is accessed concurrently by multiple
  LDM threads; thread-safety is ensured by the state's internal mutexes
  (group map mutex and per-group mutex) as described in the Thread-Safety
  section
- The Uint32 key is allocated by DblqhProxy during setup, returned to Dbspj
  in `JOIN_AGG_SETUP_CONF`, and then carried in every `LQHKEYREQ` and
  `SCAN_FRAGREQ` signal — eliminating the need for a hash table keyed on
  (transid, senderData, requestId)

### Signal Flow

#### Signal Routing via DblqhProxy

The setup, completion, and release signals are handled directly by `DblqhProxy`
itself (single-threaded), not forwarded to a Dblqh worker:

- `JOIN_AGG_SETUP_REQ` — DblqhProxy seizes state from pool, initializes it,
  returns `aggStateKey` in CONF
- `JOIN_AGG_COMPLETE_REQ` — DblqhProxy looks up state by key, finalizes and
  sends results
- `JOIN_AGG_RELEASE_REQ` — DblqhProxy releases state back to pool

Since DblqhProxy is single-threaded, no mutex is needed for pool
seize/release operations.

The operation signals (`LQHKEYREQ`, `SCAN_FRAGREQ`) continue to be routed
normally to Dblqh worker instances based on fragment ownership. Each carries
the `aggStateKey` (when `RI_JOIN_AGGREGATION` flag is set) so the worker
can look up the shared state via `getJoinAggState(key)`.

#### Phase 1: Setup (New Signal)

DBSPJ sends `JOIN_AGG_SETUP_REQ` to each data node that has fragments of the leaf table:

```cpp
// Signal: JOIN_AGG_SETUP_REQ
// Sent from: DBSPJ
// Handled by: DblqhProxy (one per data node with leaf table fragments)
// Purpose: Seize and initialize shared aggregation state before operations start

struct JoinAggSetupReq {
    static constexpr Uint32 SignalLength = 11;
    static constexpr Uint32 AggProgramSectionNum = 0;

    // Concurrency strategy values
    static constexpr Uint32 STRATEGY_MUTEX_BASED = 0;
    static constexpr Uint32 STRATEGY_MUTEX_FREE = 1;

    Uint32 senderRef;           // DBSPJ block reference
    Uint32 senderData;          // SPJ request identifier
    Uint32 requestId;           // Unique ID for this aggregation request
    Uint32 transid[2];          // Transaction ID
    Uint32 tableId;             // Leaf table ID
    Uint32 expectedOpCount;     // Expected number of operations (0 = unknown)
    Uint32 concurrencyStrategy; // STRATEGY_MUTEX_BASED or STRATEGY_MUTEX_FREE
    Uint32 resultRef;           // API reference for FLUSH_AI
    Uint32 resultData;          // API data for FLUSH_AI
    Uint32 routeRef;            // Route reference for FLUSH_AI

    // Long section 0: Aggregation program
};

// Response: JOIN_AGG_SETUP_CONF
struct JoinAggSetupConf {
    static constexpr Uint32 SignalLength = 4;

    Uint32 senderRef;           // DBLQH block reference
    Uint32 senderData;          // Echo from request
    Uint32 requestId;           // Echo from request
    Uint32 aggStateKey;         // Uint32 key (pool index) for the created state.
                                // Dbspj must store this and include it in every
                                // LQHKEYREQ and SCAN_FRAGREQ that participates
                                // in this join aggregation.
};

// Error response: JOIN_AGG_SETUP_REF
struct JoinAggSetupRef {
    static constexpr Uint32 SignalLength = 5;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 errorCode;
    Uint32 errorLine;
};
```

**execJOIN_AGG_SETUP_REQ Flow (runs in DblqhProxy, single-threaded):**

```
JOIN_AGG_SETUP_REQ arrives at DblqhProxy
    |
    v
Uint32 key = seizeJoinAggState()
    (No mutex needed — DblqhProxy is single-threaded)
    |
    v
JoinAggregationState* state = getJoinAggState(key)
    |
    v
state->m_key = key
state->m_strategy = req->concurrencyStrategy
state->m_num_threads = getNumQueryThreads()
    |
    v
Copy aggregation program from signal section
    |
    v
Store identification (transid, senderData, requestId, senderRef)
    |
    v
Store result routing info (resultRef, resultData, routeRef)
    |
    v
if (m_strategy == MUTEX_BASED):
    |   Create one shared AggInterpreter
    |   (with group map mutex, per-group mutexes)
    |   state->m_agg_interpreter = new AggInterpreter(...)
    |
else (MUTEX_FREE):
    |   Create one AggInterpreter per thread (no mutexes needed)
    |   for (i = 0; i < m_num_threads; i++):
    |       state->m_per_thread_interpreters[i] = new AggInterpreter(...)
    |
    v
Set m_state = SETUP_COMPLETE
    |
    v
Send JOIN_AGG_SETUP_CONF with aggStateKey = key
    (Dbspj stores this key and passes it in all subsequent
     LQHKEYREQ / SCAN_FRAGREQ operations for this aggregation)
```

#### Phase 2: Operations (LQHKEYREQ / SCAN_FRAGREQ)

Each operation carries the Uint32 key for the shared aggregation state.
A flag bit in the request signals that the extra word is present:

```cpp
// New flag bit in LqhKeyReq::requestInfo
namespace LqhKeyReq {
    // When set, the signal contains one additional word after the
    // standard signal: the aggStateKey (Uint32 pool index).
    static constexpr Uint32 RI_JOIN_AGGREGATION = 0x...;  // bit position TBD
}

// New flag bit in ScanFragReq::requestInfo
namespace ScanFragReq {
    // When set, the signal contains one additional word after the
    // standard signal: the aggStateKey (Uint32 pool index).
    static constexpr Uint32 RI_JOIN_AGGREGATION = 0x...;  // bit position TBD
}

// Signal layout when RI_JOIN_AGGREGATION is set:
//
// LQHKEYREQ:
//   word 0..N-1:  standard LqhKeyReq fields
//   word N:       aggStateKey   (Uint32, pool index from JOIN_AGG_SETUP_CONF)
//
// SCAN_FRAGREQ:
//   word 0..M-1:  standard ScanFragReq fields
//   word M:       aggStateKey   (Uint32, pool index from JOIN_AGG_SETUP_CONF)
//
// The aggStateKey is used by Dblqh to look up the shared
// JoinAggregationState via SimulatedBlock::getJoinAggState(key).
// Parent data (linked attributes) is in the AttrInfo section as before.
```

**LQHKEYREQ with Join Aggregation:**

```
LQHKEYREQ arrives (with RI_JOIN_AGGREGATION flag set)
    |
    v
Extract aggStateKey from extra word at end of signal
    |
    v
JoinAggregationState* state = getJoinAggState(aggStateKey)
    (O(1) pool index lookup, no mutex needed)
    |
    v
[state == nullptr?] --> Send LQHKEYREF (invalid key)
    |
    v
[state->m_state == ERROR?] --> Send LQHKEYREF (aggregation failed)
    |
    v
Execute normal lookup (TUPKEYREQ to DBTUP)
    |
    v
On TUPKEYCONF:
    |
    v
Extract parent row data from AttrInfo (linked attributes)
    |
    v
Get the AggInterpreter for this operation:
    if (state->m_strategy == MUTEX_BASED):
    |   interp = state->m_agg_interpreter   (shared, uses locking)
    else (MUTEX_FREE):
    |   interp = state->m_per_thread_interpreters[myThreadIndex]
    |       (thread-private, no locking needed)
    |
    v
interp->ProcessRecWithLinkedAttrs(...)
    |
    +-- MUTEX_BASED: two-level locking
    |   1. Brief lock on m_group_map_mutex to find/create group
    |   2. Lock group->m_mutex to update accumulators
    |   3. Unlock group->m_mutex
    |   (Multiple threads can aggregate different groups in parallel)
    |
    +-- MUTEX_FREE: no locking at all
    |   Thread-private interpreter, direct hash map access
    |
    v
Atomic increment: state->m_completed_ops++
    |
    v
Send LQHKEYCONF (DO NOT send row data - aggregation accumulates it)
```

**SCAN_FRAGREQ with Join Aggregation:**

```
SCAN_FRAGREQ arrives (with RI_JOIN_AGGREGATION flag set)
    |
    v
Extract aggStateKey from extra word at end of signal
    |
    v
JoinAggregationState* state = getJoinAggState(aggStateKey)
    (O(1) pool index lookup, no mutex needed)
    |
    v
[state == nullptr?] --> Send SCAN_FRAGREF (invalid key)
    |
    v
Create ScanRecord with reference to JoinAggregationState:
    scanPtr->m_join_agg_state_key = aggStateKey
    |
    v
For each row in scan:
    |
    v
    Extract parent row data from AttrInfo
    |
    v
    Get AggInterpreter (same strategy selection as LQHKEYREQ):
        MUTEX_BASED: state->m_agg_interpreter (with locking)
        MUTEX_FREE:  state->m_per_thread_interpreters[myThreadIndex]
        |
        v
    interp->ProcessRecWithLinkedAttrs(...)
        |
        +-- MUTEX_BASED: two-level locking (parallel across groups)
        +-- MUTEX_FREE: no locking (thread-private interpreter)
    |
    v
When scan batch/fragment complete:
    Atomic increment: state->m_completed_ops++
    |
    v
Send SCAN_FRAGCONF (with aggregation batch info, not individual rows)
```

#### Phase 3: Completion (New Signal)

DBSPJ sends completion signal when all operations for the batch are done:

```cpp
// Signal: JOIN_AGG_COMPLETE_REQ
// Sent when: DBSPJ has received CONF for all operations
// Purpose: Trigger finalization and result sending

struct JoinAggCompleteReq {
    static constexpr Uint32 SignalLength = 7;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
    Uint32 aggStateKey;         // Key from JOIN_AGG_SETUP_CONF
    Uint32 completedOps;        // Total completed ops (for verification)
};

// Response: JOIN_AGG_COMPLETE_CONF
struct JoinAggCompleteConf {
    static constexpr Uint32 SignalLength = 5;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 numResultRows;       // Number of aggregate result rows
    Uint32 resultBytes;         // Total bytes of results
};
```

**execJOIN_AGG_COMPLETE_REQ Flow (runs in DblqhProxy, single-threaded):**

```
JOIN_AGG_COMPLETE_REQ arrives at DblqhProxy
    |
    v
JoinAggregationState* state = getJoinAggState(aggStateKey)
    (aggStateKey carried in signal, originally from SETUP_CONF)
    |
    v
[state == nullptr?] --> Send JOIN_AGG_COMPLETE_REF
    |
    v
Verify: state->m_completed_ops == completedOps
    |
    v
Set state->m_state = FINALIZING
    |
    v
if (state->m_strategy == MUTEX_BASED):
    |   Finalize: m_agg_interpreter->FinalizeResults()
    |
else (MUTEX_FREE):
    |   Merge all per-thread interpreters into one:
    |       merged = state->m_per_thread_interpreters[0]
    |       for (i = 1; i < m_num_threads; i++):
    |           merged->MergeFrom(m_per_thread_interpreters[i])
    |   Finalize: merged->FinalizeResults()
    |
    v
Set state->m_state = SENDING_RESULTS
    |
    v
Send TRANSID_AI with aggregated results
    - Use stored m_resultRef, m_resultData, m_routeRef
    - May require multiple TRANSID_AI for large results
    - NO FLUSH_AI needed - results sent directly here
    |
    v
Send JOIN_AGG_COMPLETE_CONF
    |
    v
Set state->m_state = COMPLETED
    |
    v
Schedule state cleanup (or wait for explicit release signal)
```

**Note:** Results may also be sent during the accumulation phase if the aggregation buffer fills up. In this case, partial results are sent via TRANSID_AI and the buffer is cleared to continue accumulating.

#### Phase 4: Cleanup (Optional Signal)

```cpp
// Signal: JOIN_AGG_RELEASE_REQ
// Sent when: DBSPJ is done with the aggregation state
// Purpose: Release resources

struct JoinAggReleaseReq {
    static constexpr Uint32 SignalLength = 5;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
    Uint32 aggStateKey;         // Key from JOIN_AGG_SETUP_CONF
};
```

### Thread-Safety Details

#### Mutex Hierarchy

No node-level mutex is needed for state lookup — the Uint32 key gives O(1)
pool access without locking. Pool seize/release is done only by the
single-threaded DblqhProxy, so no mutex there either.

The mutexes that protect the shared state contents (accessed concurrently
by multiple LDM threads) must be acquired in this order to prevent deadlocks:

1. `state->m_group_map_mutex` (per-state, protects GROUP BY hash map)
2. `group->m_mutex` (per-group, protects aggregate accumulators)

#### Per-Group Locking Strategy

For high-concurrency scenarios, using a single mutex per AggInterpreter creates
contention when many LDM threads aggregate simultaneously. Instead, we use a
two-level locking strategy:

1. **Group Map Mutex**: Protects the GROUP BY hash map structure (find/insert)
2. **Per-Group Mutex**: Each GROUP BY row has its own mutex for accumulator updates

This allows multiple threads to aggregate different groups in parallel.

```cpp
// Extended GROUP BY entry with per-row mutex
struct GroupByEntry {
    // Group key (variable length, stored separately)
    Uint32 m_key_offset;        // Offset into key buffer
    Uint32 m_key_len;           // Key length in bytes

    // Per-group mutex for accumulator updates
    NdbMutex* m_mutex;

    // Aggregate accumulators (protected by m_mutex)
    // Dynamically allocated via ndbd_malloc: m_num_aggregates * sizeof(AggResItem)
    AggResItem* m_accumulators;
    Uint32 m_num_aggregates;    // Number of aggregate functions
    Uint64 m_row_count;         // Number of rows in this group
};

// Extended AggInterpreter with per-group locking
class AggInterpreter {
    // ...existing members...

    // GROUP BY hash map - protected by m_group_map_mutex
    NdbMutex* m_group_map_mutex;
    std::unordered_map<GroupKey, GroupByEntry*, GroupKeyHash> m_group_map;

    // Pool for GroupByEntry allocation
    // Allocated via ndbd_malloc, grows as needed
    GroupByEntry* m_group_pool;          // Array of pre-allocated entries
    Uint32 m_group_pool_size;           // Current pool capacity
    std::atomic<Uint32> m_next_group_slot;  // Next free slot
};
```

#### Two-Phase Row Processing

```cpp
int AggInterpreter::ProcessRecWithLinkedAttrs(
    Dbtup* block_tup,
    KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len)
{
    // Phase 1: Extract GROUP BY key from linked attrs + local columns
    GroupKey key;
    extractGroupByKey(linked_attr_data, req_struct, &key);

    // Phase 2: Find or create group entry (short critical section)
    GroupByEntry* group = nullptr;
    {
        NdbMutex_Lock(m_group_map_mutex);

        auto it = m_group_map.find(key);
        if (it != m_group_map.end()) {
            group = it->second;
        } else {
            // Allocate new group entry
            group = allocateGroupEntry(key);
            if (group == nullptr) {
                NdbMutex_Unlock(m_group_map_mutex);
                return ZAGG_GROUP_ALLOC_FAILED;
            }
            m_group_map[key] = group;
        }

        NdbMutex_Unlock(m_group_map_mutex);
    }

    // Phase 3: Update accumulators (per-group mutex, parallel across groups)
    {
        NdbMutex_Lock(group->m_mutex);

        // Extract aggregate source values
        // m_values_buf is pre-allocated at interpreter creation time
        // sized to m_num_aggregates (no MAX constant needed)
        extractAggregateInputs(linked_attr_data, req_struct, m_values_buf);

        // Update each accumulator
        for (Uint32 i = 0; i < m_num_aggregates; i++) {
            updateAccumulator(&group->m_accumulators[i], m_agg_functions[i], &m_values_buf[i]);
        }
        group->m_row_count++;

        NdbMutex_Unlock(group->m_mutex);
    }

    return 0;
}
```

#### Lock-Free Group Lookup Optimization

For read-heavy workloads where most rows hit existing groups, we can use
a lock-free lookup with fallback to locked insert:

```cpp
GroupByEntry* AggInterpreter::findOrCreateGroup(const GroupKey& key)
{
    // Fast path: lock-free lookup (read-only, no mutex)
    // Uses atomic load with acquire semantics
    GroupByEntry* group = m_group_map.find_lockfree(key);
    if (group != nullptr) {
        return group;  // Found existing group, no lock needed
    }

    // Slow path: acquire mutex for potential insert
    NdbMutex_Lock(m_group_map_mutex);

    // Double-check after acquiring lock (another thread may have inserted)
    auto it = m_group_map.find(key);
    if (it != m_group_map.end()) {
        group = it->second;
    } else {
        group = allocateGroupEntry(key);
        if (group != nullptr) {
            m_group_map.insert({key, group});
        }
    }

    NdbMutex_Unlock(m_group_map_mutex);
    return group;
}
```

#### Atomic Operations

Use atomics for frequently accessed counters:

```cpp
// Increment completed ops (called by each LDM thread)
state->m_completed_ops.fetch_add(1, std::memory_order_relaxed);

// Check state (lock-free read)
if (state->m_state.load(std::memory_order_acquire) == State::ERROR) {
    // Handle error
}

// State transition (compare-and-swap)
State expected = State::ACCUMULATING;
if (state->m_state.compare_exchange_strong(expected, State::FINALIZING)) {
    // Won the race to finalize
}

// Allocate group slot from pool (lock-free)
Uint32 slot = m_next_group_slot.fetch_add(1, std::memory_order_relaxed);
if (slot >= m_group_pool_size) {
    return nullptr;  // Pool exhausted
}
```

#### Complete Row Processing (Strategy-Dependent)

```cpp
void processRowForJoinAgg(JoinAggregationState* state,
                          Uint32 myThreadIndex,
                          const Uint32* rowData,
                          const Uint32* linkedData,
                          Uint32 linkedDataLen) {
    // Select interpreter based on strategy
    AggInterpreter* interp;
    if (state->m_strategy == JoinAggregationState::MUTEX_BASED) {
        interp = state->m_agg_interpreter;
        // Uses two-level locking internally:
        // 1. Brief lock on group map to find/create group
        // 2. Per-group lock to update accumulators
    } else {
        interp = state->m_per_thread_interpreters[myThreadIndex];
        // No locking — thread-private interpreter
    }

    int result = interp->ProcessRecWithLinkedAttrs(
        rowData, linkedData, linkedDataLen);

    if (result != 0) {
        // Handle error atomically
        state->m_failed_ops.fetch_add(1);
        State expected = State::ACCUMULATING;
        state->m_state.compare_exchange_strong(expected, State::ERROR);
    }
}
```

#### Finalization

Finalization runs single-threaded in DblqhProxy after all operations
have completed, so no concurrent updates are possible.

For MUTEX_FREE, `MergeFrom()` is called first (see "Merge Algorithm"
section), then `FinalizeResults()` on the merged interpreter.

```cpp
int AggInterpreter::FinalizeResults() {
    // Runs single-threaded in DblqhProxy — no locking needed.
    // All operations have completed before this is called.

    // Iterate all groups
    for (auto& [key, group] : m_group_map) {
        // Finalize each accumulator (e.g., compute AVG from SUM/COUNT)
        for (Uint32 i = 0; i < m_num_aggregates; i++) {
            finalizeAccumulator(&group->m_accumulators[i], m_agg_functions[i]);
        }
    }

    return 0;
}
```

#### Memory Layout for Cache Efficiency

Align GroupByEntry to cache line boundaries to prevent false sharing:

```cpp
// Cache-line aligned group entry (typically 64 bytes)
struct alignas(64) GroupByEntry {
    // Hot data: mutex and accumulators (accessed during aggregation)
    NdbMutex* m_mutex;                          // 8 bytes
    Uint64 m_row_count;                         // 8 bytes
    AggResItem* m_accumulators;                 // 8 bytes (pointer to ndbd_malloc'd array)
    Uint32 m_num_aggregates;                    // 4 bytes

    // Cold data: key info (accessed during lookup/finalization)
    Uint32 m_key_offset;
    Uint32 m_key_len;
};

// The m_accumulators array is allocated separately via ndbd_malloc
// with size m_num_aggregates * sizeof(AggResItem). This avoids
// baking MAX_AGGREGATES into the struct layout. The struct itself
// stays small and cache-line aligned; the accumulators are allocated
// contiguously and accessed sequentially during updates.
```

#### Contention Analysis

| Scenario | MUTEX_BASED (per-group) | MUTEX_FREE (per-thread) |
|----------|------------------------|------------------------|
| Low cardinality (e.g., 7 shipmodes) | Some contention on hot groups | Zero contention, cheap merge |
| High cardinality (e.g., 1M customers) | Parallel across groups | Zero contention, larger merge |
| Skewed distribution | Hot groups still serialize | Zero contention |
| Group map growth | Brief contention on insert | No contention (private maps) |
| Completion overhead | None (single interpreter) | Merge N interpreters |
| Memory footprint | 1 interpreter + mutexes | N interpreters, no mutexes |

**Trade-offs:**
- Per-group mutex adds ~8 bytes per group
- Lock-free lookup adds complexity but improves read-heavy workloads
- For very low cardinality (< 10 groups), single mutex may be simpler

### Mutex-Free Per-Thread Strategy

#### Overview

The mutex-free strategy eliminates all locking during the operations phase by
giving each thread its own private AggInterpreter. Each thread accumulates
into its own group map independently. At completion time, DblqhProxy merges
all per-thread results into one before sending.

#### Thread Addressing

Each thread that can execute queries has an index (0..N-1). The Dblqh worker
knows its own thread index. The per-thread interpreter is accessed by:

```cpp
// Two lookups to reach the thread-private interpreter:
// 1. aggStateKey → JoinAggregationState (pool index, O(1))
// 2. myThreadIndex → AggInterpreter (array index, O(1))

JoinAggregationState* state = getJoinAggState(aggStateKey);
AggInterpreter* interp = state->m_per_thread_interpreters[myThreadIndex];

// No mutex needed — this interpreter is private to this thread
interp->ProcessRecWithLinkedAttrs(...);
```

#### Per-Thread Interpreter Characteristics

Each per-thread AggInterpreter is a complete, independent instance:
- Has its own GROUP BY hash map (no `m_group_map_mutex` needed)
- Has its own GroupByEntry pool (no `m_mutex` per group needed)
- Has its own accumulators for each group
- Uses the same aggregation program (shared, immutable)
- Uses the **same hash function and initial hash table size** as all
  other per-thread interpreters — this is critical for efficient
  bucket-sequential merge (see Merge Algorithm)

The same group key (e.g., shipmode = 'AIR') may exist in multiple
per-thread interpreters simultaneously. This is correct — the merge
phase will combine them. Because all interpreters use the same hash
function, matching groups are in the same bucket index across all
interpreters.

#### Merge Algorithm

At completion time, DblqhProxy merges all per-thread interpreters.
This runs single-threaded — no locking needed since all operations
have completed before JOIN_AGG_COMPLETE_REQ arrives.

For high-cardinality GROUP BY (many groups), the merge must efficiently
find matching groups across interpreters. A naive approach — iterate
groups in interpreter[i] and probe the hash map of interpreter[0] —
causes random cache misses when the hash map is large.

##### Consistent Hash Table Structure

All per-thread interpreters use the **same hash function and same hash
table size**. This means a group with key K hashes to the **same bucket
index** in every interpreter. The merge exploits this:

```cpp
// All interpreters use the same hash table layout:
//   - Same hash function: hashGroupKey(key)
//   - Same number of buckets: m_hash_table_size
//   - A group with key K is in bucket hashGroupKey(K) % m_hash_table_size
//     in EVERY interpreter
```

##### Bucket-Sequential Merge

Instead of iterating all groups in one interpreter and probing another,
the merge iterates **by hash bucket index**. For each bucket, it processes
all entries from all interpreters that fall in that bucket:

```cpp
int AggInterpreter::MergeAllByBucket(
    AggInterpreter* interpreters[],
    Uint32 num_interpreters)
{
    // This interpreter (interpreters[0]) accumulates the merged result.
    // All interpreters have the same m_hash_table_size.

    for (Uint32 bucket = 0; bucket < m_hash_table_size; bucket++) {
        // For each source interpreter (1..N-1), merge entries
        // in this bucket into interpreter[0]'s same bucket.
        for (Uint32 t = 1; t < num_interpreters; t++) {
            GroupByEntry* other_entry = interpreters[t]->m_buckets[bucket];
            while (other_entry != nullptr) {
                // Look up in THIS interpreter's SAME bucket —
                // key must hash to same bucket, so search is
                // confined to this bucket's chain.
                GroupByEntry* my_entry = findInBucket(bucket,
                                                      other_entry->key());
                if (my_entry == nullptr) {
                    // New group: move entry to this bucket
                    my_entry = insertInBucket(bucket, other_entry->key());
                    initAccumulators(my_entry);
                }

                // Merge accumulators
                for (Uint32 i = 0; i < m_num_aggregates; i++) {
                    mergeAccumulator(&my_entry->m_accumulators[i],
                                     &other_entry->m_accumulators[i],
                                     m_agg_functions[i]);
                }
                my_entry->m_row_count += other_entry->m_row_count;

                other_entry = other_entry->m_next;
            }
        }
    }
    return 0;
}
```

**Why this is cache-friendly:**
- Processing one bucket at a time keeps the working set small
- All interpreters' entries for bucket B share the same hash prefix,
  so the bucket chains are short (typically 1-3 entries with good
  hash distribution)
- Sequential bucket iteration has a predictable access pattern
- The merged interpreter's bucket B is hot in cache while we merge
  all N source interpreters' bucket B entries into it

##### Sorted Merge Alternative

For extremely high cardinality (millions of groups), a sorted merge
may be more efficient than bucket-sequential merge:

```cpp
int AggInterpreter::MergeAllSorted(
    AggInterpreter* interpreters[],
    Uint32 num_interpreters)
{
    // Step 1: Sort each interpreter's groups by key
    // (can be done in-place on the group arrays)
    for (Uint32 t = 0; t < num_interpreters; t++) {
        interpreters[t]->sortGroupsByKey();
    }

    // Step 2: N-way merge using a priority queue
    // Groups with matching keys across interpreters
    // are adjacent in the merge order.
    PriorityQueue<MergeIterator> pq;
    for (Uint32 t = 0; t < num_interpreters; t++) {
        if (interpreters[t]->groupCount() > 0) {
            pq.push(MergeIterator(interpreters[t], 0));
        }
    }

    GroupByEntry* current_result = nullptr;
    while (!pq.empty()) {
        MergeIterator top = pq.pop();
        GroupByEntry* entry = top.currentEntry();

        if (current_result != nullptr &&
            current_result->key() == entry->key()) {
            // Same group: merge accumulators
            for (Uint32 i = 0; i < m_num_aggregates; i++) {
                mergeAccumulator(&current_result->m_accumulators[i],
                                 &entry->m_accumulators[i],
                                 m_agg_functions[i]);
            }
            current_result->m_row_count += entry->m_row_count;
        } else {
            // New group: start accumulating
            current_result = appendResultGroup(entry);
        }

        // Advance iterator, re-insert if more entries
        if (top.advance()) {
            pq.push(top);
        }
    }
    return 0;
}
```

**Sorted merge characteristics:**
- O(G_total * log N) — the log N factor from the priority queue is
  negligible since N (number of threads) is typically small (< 100)
- Purely sequential memory access through sorted arrays
- Groups with matching keys are encountered consecutively — the
  merged result is built in sorted order with no random lookups
- The sort step is O(G_per_thread * log G_per_thread) per interpreter
  but can reuse the existing group pool memory

##### Merge Approach Comparison

| Factor | Bucket-Sequential | Sorted Merge |
|--------|-------------------|--------------|
| Complexity | O(G_total) amortized | O(G_total * log G_max) |
| Cache behaviour | Good (bucket at a time) | Excellent (sequential) |
| Best for | Moderate group counts | Very high group counts |
| Extra memory | None (in-place) | Sort requires rearrangement |
| Implementation | Uses existing hash table | Requires sort + priority queue |

The bucket-sequential merge is the natural default since it reuses
the existing hash table structure. The sorted merge is an optimisation
for cases where GROUP BY cardinality is very high and the hash table
becomes too large for cache-friendly bucket processing.

##### Accumulator Merge Rules

```
SUM:   result.sum += other.sum
COUNT: result.count += other.count
MIN:   result.min = min(result.min, other.min)
MAX:   result.max = max(result.max, other.max)
AVG:   merge sum and count separately, compute avg at finalize
```

#### Complete Completion Flow (MUTEX_FREE)

```
JOIN_AGG_COMPLETE_REQ arrives at DblqhProxy
    |
    v
state = getJoinAggState(aggStateKey)
    |
    v
Verify: state->m_completed_ops == completedOps
    |
    v
Set state->m_state = FINALIZING
    |
    v
merged = state->m_per_thread_interpreters[0]
    |
    v
for i = 1 to m_num_threads - 1:
    |   merged->MergeFrom(m_per_thread_interpreters[i])
    |   (single-threaded, no locking — all ops already complete)
    |
    v
merged->FinalizeResults()
    (compute AVG from SUM/COUNT, etc.)
    |
    v
Send TRANSID_AI with aggregated results
    |
    v
Send JOIN_AGG_COMPLETE_CONF
```

### Strategy Selection by Dbspj

Dbspj selects the concurrency strategy in JOIN_AGG_SETUP_REQ. Both
strategies must produce identical results, making it possible to
verify correctness by running the same query with each strategy and
comparing output.

#### Selection Criteria

| Factor | Favors MUTEX_BASED | Favors MUTEX_FREE |
|--------|-------------------|-------------------|
| GROUP BY cardinality | High (many groups → low contention) | Low (few groups → high contention on hot groups) |
| Number of threads | Few threads | Many threads |
| Memory pressure | Tight (one interpreter) | Relaxed (N interpreters) |
| Merge cost tolerance | N/A | Acceptable (low-cardinality merge is cheap) |

#### Heuristic

```cpp
// In Dbspj, when preparing JOIN_AGG_SETUP_REQ:
Uint32 strategy;
if (estimated_group_count <= num_query_threads * 2) {
    // Low cardinality: mutex contention likely on hot groups.
    // Per-thread interpreters avoid all contention.
    // Merge cost is cheap (few groups to merge).
    strategy = JoinAggSetupReq::STRATEGY_MUTEX_FREE;
} else {
    // High cardinality: threads rarely collide on same group.
    // Mutex-based saves memory (one interpreter instead of N).
    // No merge overhead at completion.
    strategy = JoinAggSetupReq::STRATEGY_MUTEX_BASED;
}
```

The `estimated_group_count` can come from MySQL optimizer statistics
(index cardinality of GROUP BY columns). When no estimate is available,
MUTEX_FREE is a safe default — it has predictable performance regardless
of cardinality.

#### Verification Mode

For testing and correctness verification, Dbspj can be configured to:
1. Run with a forced strategy (always MUTEX_BASED or always MUTEX_FREE)
2. Compare results between strategies for the same query
3. Log which strategy was selected and the selection inputs

This can be controlled via a session variable or NDB configuration
parameter during development and testing.

### Attribute Information Handling

#### Simplified AttrInfo for Join Aggregation

Since the aggregation program is sent in `JOIN_AGG_SETUP_REQ` (not per-operation), and results are sent via `JOIN_AGG_COMPLETE_REQ` (not FLUSH_AI), the AttrInfo for leaf operations is simplified:

```
AttrInfo for leaf operation (lookup or scan) with join aggregation:
+------------------+----------------------------------------+
| 5-word header    | Standard header                        |
+------------------+----------------------------------------+
| Filter program   | Optional filter for leaf table         |
|                  | - Executed by filter interpreter       |
|                  | - Default: EXIT_OK if no filter        |
+------------------+----------------------------------------+
| Linked columns   | Columns from parent tables:            |
|                  | - GROUP BY column values               |
|                  | - Aggregate source column values       |
|                  | - Used by both filter and aggregation  |
|                  | - CORR_FACTOR32                        |
+------------------+----------------------------------------+
```

**Key simplifications:**
- **No aggregation program** - already sent in JOIN_AGG_SETUP_REQ
- **No FLUSH_AI marker** - results sent on buffer full or JOIN_AGG_COMPLETE_REQ
- **No user projection** - aggregation program defines output columns
- **No final reads for aggregation** - local columns to read are defined in aggregation program

The linked columns serve dual purpose:
1. Filter interpreter can reference parent columns for predicates
2. Aggregation interpreter reads them for GROUP BY and aggregate functions

#### Column ID Mapping

The aggregation program uses a simple scheme to distinguish local columns from linked (parent) columns. There's no need to distinguish which ancestor level a linked column came from - they all arrive in the linked attribute buffer in order.

```cpp
// Column ID encoding:
// Bit 15: Source indicator
//   0 = Local table column (read via DBTUP)
//   1 = Linked attribute (read from AttrInfo buffer)
// Bits 14-0: Column/offset identifier

static constexpr Uint32 COL_SOURCE_MASK = 0x8000;
static constexpr Uint32 COL_SOURCE_LOCAL = 0x0000;
static constexpr Uint32 COL_SOURCE_LINKED = 0x8000;
static constexpr Uint32 COL_NUMBER_MASK = 0x7FFF;

// Example: GROUP BY c.category, SUM(l.price)
// c.category = linked column (0x8000 | offset_in_buffer)
// l.price = local column (0x0005 if col 5)
```

**Why no parent level distinction?**
- Linked attributes from all ancestor tables are concatenated in the AttrInfo
- The aggregation program references them by offset in the linked buffer
- The specific ancestor table doesn't matter for aggregation execution
- MySQL supports joins of up to 64 tables, so tracking 63 parent levels would be complex and unnecessary

### Integration with DBTUP

DBTUP's `AggInterpreter` needs modifications to handle linked attribute data. The aggregation program (sent at setup time) specifies which local columns to read.

```cpp
class AggInterpreter {
public:
    // Existing interface for single-table scans
    int ProcessRec(Dbtup* block_tup, KeyReqStruct* req_struct);

    // New interface for join aggregation
    // Called with both local row data and linked attributes from parents
    int ProcessRecWithLinkedAttrs(
        Dbtup* block_tup,
        KeyReqStruct* req_struct,
        const Uint32* linked_attr_data,     // Parent row columns from AttrInfo
        Uint32 linked_attr_len);

    // Finalize and prepare results for sending
    int FinalizeResults();

    // Get result data for sending
    int GetResultData(Uint32* buffer, Uint32 buffer_size, Uint32* bytes_written);

private:
    // Buffer for linked attribute data (parent columns)
    // Allocated via ndbd_malloc at interpreter creation time,
    // sized to the actual linked attribute length from the
    // aggregation program.
    Uint32* m_linked_attr_buf;
    Uint32 m_linked_attr_buf_size;  // Allocated size in words
    Uint32 m_linked_attr_len;       // Current content length in words

    // Load column value - checks source bit to determine local vs linked
    int LoadColumnValue(Uint32 col_id, Register* reg);
};

// Implementation of LoadColumnValue
int AggInterpreter::LoadColumnValue(Uint32 col_id, Register* reg) {
    Uint32 source = col_id & COL_SOURCE_MASK;
    Uint32 col_num = col_id & COL_NUMBER_MASK;

    if (source == COL_SOURCE_LOCAL) {
        // Read from local row via DBTUP (column specified in aggregation program)
        return block_tup_->readAttribute(col_num, reg);
    } else {
        // Read from linked attribute buffer at offset
        return readLinkedAttribute(col_num, reg);
    }
}
```

### Changes to Existing Structures

#### ScanRecord

Add key for shared aggregation state lookup:

```cpp
struct ScanRecord {
    // ... existing fields ...

    // For join aggregation - key to shared state (RNIL if none)
    Uint32 m_join_agg_state_key;    // Pool index from aggStateKey in signal
};
```

#### TcConnectionrec

Add key for shared aggregation state lookup (key operations):

```cpp
struct TcConnectionrec {
    // ... existing fields ...

    // For join aggregation - key to shared state (RNIL if none)
    Uint32 m_join_agg_state_key;    // Pool index from aggStateKey in signal
};
```

### Error Handling

```cpp
enum JoinAggErrorCode {
    JAE_NO_ERROR = 0,
    JAE_STATE_ALLOC_FAILED = 4001,      // Cannot allocate JoinAggregationState
    JAE_STATE_NOT_FOUND = 4002,         // State not found for operation
    JAE_INTERPRETER_INIT_FAILED = 4003, // AggInterpreter initialization failed
    JAE_INTERPRETER_ERROR = 4004,       // AggInterpreter returned error
    JAE_OP_COUNT_MISMATCH = 4005,       // Completed ops != expected
    JAE_PARENT_DATA_ERROR = 4006,       // Invalid parent data format
    JAE_RESULT_TOO_LARGE = 4007,        // Aggregation result exceeds limits
    JAE_TIMEOUT = 4008,                 // State timed out
    JAE_ALREADY_FINALIZED = 4009,       // Duplicate complete signal
    JAE_MUTEX_ERROR = 4010              // Mutex acquisition failed
};
```

**Error Propagation:**

1. Error during setup (JOIN_AGG_SETUP_REQ):
   - Send JOIN_AGG_SETUP_REF
   - DBSPJ aborts the query

2. Error during operation (LQHKEYREQ/SCAN_FRAGREQ):
   - Atomically set `state->m_state = ERROR`
   - Send LQHKEYREF/SCAN_FRAGREF for this operation
   - Subsequent operations check state and fail fast
   - DBSPJ sends JOIN_AGG_COMPLETE_REQ (or ABORT)

3. Error during completion:
   - Send JOIN_AGG_COMPLETE_REF
   - DBSPJ handles based on error code

### Memory Management

All variable-sized arrays are allocated dynamically using NDB's internal
allocator (`ndbd_malloc`) with `QUERY_MEMORY` resource. This avoids
compile-time constants like `MAX_QUERY_THREADS`, `MAX_AGGREGATES`, or
`MAX_LINKED_ATTR_SIZE` in struct definitions. The actual sizes are known
at setup time from the aggregation program and node configuration.

**Dynamically allocated structures:**
- `m_per_thread_interpreters` — sized to actual number of query threads
- `GroupByEntry::m_accumulators` — sized to actual number of aggregate functions
- `AggInterpreter::m_linked_attr_buf` — sized to actual linked attribute length
- `AggInterpreter::m_group_pool` — initial allocation, can grow as needed
- `AggInterpreter::m_buckets` (hash table) — sized to chosen hash table size

```cpp
#include "vm/ndbd_malloc_impl.hpp"

// All variable-sized allocations use ndbd_malloc with QUERY_MEMORY:

// Per-thread interpreter array (at setup, MUTEX_FREE only)
state->m_per_thread_interpreters = (AggInterpreter**)
    ndbd_malloc(m_num_threads * sizeof(AggInterpreter*), RG_QUERY_MEMORY);

// Accumulators per group (when allocating a new GroupByEntry)
entry->m_accumulators = (AggResItem*)
    ndbd_malloc(m_num_aggregates * sizeof(AggResItem), RG_QUERY_MEMORY);

// Linked attribute buffer (at interpreter creation)
interp->m_linked_attr_buf = (Uint32*)
    ndbd_malloc(linked_attr_size * sizeof(Uint32), RG_QUERY_MEMORY);

// Deallocation
ndbd_free(ptr, size);

// Resource tracking (for monitoring)
struct JoinAggResources {
    Uint32 c_active_states;
    Uint32 c_peak_states;
    Uint64 c_total_states_created;
    Uint64 c_interpreter_memory_used;
};
```

The memory limits are governed by the overall QUERY_MEMORY pool size, which is already configured for query execution.

### Complete Signal Flow Diagram

```
DBSPJ                              DblqhProxy / Dblqh                     DBTUP
  |                                      |                                  |
  |====== Setup Phase (DblqhProxy, single-threaded) ====================  |
  |                                      |                                  |
  | JOIN_AGG_SETUP_REQ(strategy)         |                                  |
  |------------------------------------->| [DblqhProxy]                     |
  |                                      | key = seizeJoinAggState()        |
  |                                      | state->m_strategy = strategy     |
  |                                      | MUTEX_BASED: init 1 interpreter  |
  |                                      | MUTEX_FREE:  init N interpreters |
  |                                      | state->m_key = key               |
  | JOIN_AGG_SETUP_CONF(aggStateKey=key) |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | Dbspj stores aggStateKey for use     |                                  |
  | in all subsequent operations         |                                  |
  |                                      |                                  |
  |====== Operations Phase (Dblqh workers, parallel, any thread) ========  |
  |                                      |                                  |
  | LQHKEYREQ (+aggStateKey, thread 1)   |                                  |
  |------------------------------------->| [Dblqh worker]                   |
  |                                      | state = getJoinAggState(key)     |
  |                                      | MUTEX_BASED: interp = shared     |
  |                                      | MUTEX_FREE:  interp = my_thread  |
  |                                      | TUPKEYREQ                        |
  |                                      |--------------------------------->|
  |                                      |                                  | Read local row
  |                                      | TUPKEYCONF                       |
  |                                      |<---------------------------------|
  |                                      | interp->ProcessRecWithLinked():  |
  |                                      |   MUTEX_BASED: lock/unlock       |
  |                                      |   MUTEX_FREE:  no locking        |
  |                                      | Increment completed_ops          |
  | LQHKEYCONF                           |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | SCAN_FRAGREQ (+aggStateKey, thread 2)|                                  |
  |------------------------------------->| [Dblqh worker]                   |
  |                                      | (same strategy selection)        |
  |                                      | For each row: interp->Process()  |
  |                                      | Increment completed_ops          |
  | SCAN_FRAGCONF                        |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | ... more operations (parallel) ...   |                                  |
  |                                      |                                  |
  |====== Completion Phase (DblqhProxy, single-threaded) ================  |
  |                                      |                                  |
  | JOIN_AGG_COMPLETE_REQ(aggStateKey)   |                                  |
  |------------------------------------->| [DblqhProxy]                     |
  |                                      | getJoinAggState(aggStateKey)     |
  |                                      | Verify completed_ops             |
  |                                      | MUTEX_BASED:                     |
  |                                      |   FinalizeResults()              |
  |                                      | MUTEX_FREE:                      |
  |                                      |   MergeFrom(interp[1..N-1])     |
  |                                      |   FinalizeResults()              |
  |                                      |                                  |
  |                                      | TRANSID_AI (aggregated results)  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | JOIN_AGG_COMPLETE_CONF               |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | DBSPJ routes results to API          |                                  |
  |                                      |                                  |
  |====== Cleanup Phase (DblqhProxy, single-threaded) ==================  |
  |                                      |                                  |
  | JOIN_AGG_RELEASE_REQ(aggStateKey)    |                                  |
  |------------------------------------->| [DblqhProxy]                     |
  |                                      | releaseJoinAggState(aggStateKey) |
  |                                      | (frees 1 or N interpreters)      |
```

## File Changes Summary

| File | Changes |
|------|---------|
| `SimulatedBlock.hpp` | Add static `s_joinAggStatePool`, `seizeJoinAggState()`, `getJoinAggState()`, `releaseJoinAggState()` |
| `SimulatedBlock.cpp` | Implement pool init, seize, get, release |
| `Dblqh.hpp` | Add JoinAggregationState struct, add `m_join_agg_state_key` to ScanRecord and TcConnectionrec |
| `DblqhMain.cpp` | Modify execLQHKEYREQ, execSCAN_FRAGREQ to extract aggStateKey when RI_JOIN_AGGREGATION set |
| `DblqhProxy.cpp` | Add execJOIN_AGG_SETUP_REQ (seize + init), execJOIN_AGG_COMPLETE_REQ (finalize + send), execJOIN_AGG_RELEASE_REQ (release) |
| `LqhKeyReq.hpp` | Add RI_JOIN_AGGREGATION flag |
| `ScanFrag.hpp` | Add RI_JOIN_AGGREGATION flag |
| `signaldata/JoinAgg.hpp` (new) | Signal definitions for JOIN_AGG_* |
| `AggInterpreter.hpp` | Add ProcessRecWithLinkedAttrs, FinalizeResults, MergeFrom |
| `AggInterpreter.cpp` | Implement linked attribute handling, per-thread merge logic |

## Open Questions (Resolved)

1. **Batch Completion Mechanism**: ✓ Dedicated JOIN_AGG_COMPLETE_REQ signal

2. **Thread Safety**: ✓ Two selectable strategies: MUTEX_BASED (per-group mutex) or MUTEX_FREE (per-thread interpreters). Atomics for counters, O(1) key-based state lookup

3. **Multi-Node**: ✓ Each node has independent state, DBSPJ coordinates final aggregation if needed

## Remaining Design Decisions

1. **Result Aggregation Across Nodes**: If leaf table spans multiple nodes, does DBSPJ aggregate the per-node results, or is there node-to-node coordination?

2. **Partial Results**: Should DBLQH send partial results if aggregation buffer fills up, or wait for completion?

3. **Interpreter Pool**: Pre-allocate interpreter instances, or create on demand?

4. **State Timeout**: How long to keep state before automatic cleanup? Tied to transaction timeout?

## Performance Considerations

1. **MUTEX_BASED Operations**: Two-level locking minimizes contention
   - Group map mutex held briefly for find/insert
   - Per-group mutex allows parallel aggregation across different groups
   - For high-cardinality GROUP BY, threads rarely contend
   - For low-cardinality (< 10 groups), some contention on hot groups
   - Zero overhead at completion (single interpreter, no merge)

2. **MUTEX_FREE Operations**: Zero contention during operations
   - Each thread writes to its own private interpreter — no locking at all
   - No cache line bouncing between threads (each has own data)
   - Completion phase adds merge cost: O(G_total) across all per-thread
     interpreters, but this runs single-threaded in DblqhProxy
   - For low-cardinality GROUP BY, merge cost is negligible
   - For high-cardinality, merge iterates more groups but operations
     phase was fully contention-free

3. **Memory Usage**
   - MUTEX_BASED: one AggInterpreter + per-group mutex (~8 bytes/group)
   - MUTEX_FREE: N AggInterpreter instances (one per thread), no mutexes
   - MUTEX_FREE uses N times more memory for interpreter state, but
     GroupByEntry does not need mutex or cache-line alignment
   - Both use QUERY_MEMORY pool; released when JOIN_AGG_RELEASE_REQ processed

4. **State Lookup**: O(1) pool index access via aggStateKey
   - No hash table, no mutex, no hash computation for state lookup
   - Key carried in signal, used for direct array access
   - MUTEX_FREE adds one more array index (thread index) — still O(1)
   - State pointer cached in TcConnectionrec/ScanRecord after first lookup

5. **Single-Thread Phases**: Setup/completion/release handled by DblqhProxy
   - DblqhProxy is single-threaded: no mutex needed for seize/release
   - Operations phase fully parallel across threads
   - MUTEX_FREE completion includes merge before finalize
   - Both strategies produce identical results
