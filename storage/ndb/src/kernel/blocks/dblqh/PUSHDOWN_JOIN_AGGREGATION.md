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

1. **Dedicated Setup Signal**: A new signal (`JOIN_AGG_SETUP_REQ`) is sent to each data node before the batch starts, creating the shared aggregation state

2. **Node-Level Shared State**: The `JoinAggregationState` is created once per data node (not per fragment or per thread) and is accessible by any LDM thread

3. **Thread-Safe Access**: The state structure uses atomic operations or mutex protection for concurrent access

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
    // Aggregation State (thread-safe via per-group locking)
    //------------------------------------------------------------------
    AggInterpreter* m_agg_interpreter;  // Shared aggregation interpreter
    // Note: Fine-grained locking is inside AggInterpreter:
    //   - m_group_map_mutex: protects GROUP BY hash map (find/insert)
    //   - group->m_mutex: per-group mutex for accumulator updates

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
    // Hash Table Management
    //------------------------------------------------------------------
    Uint32 m_hash_next;                 // Next in hash chain
    Uint32 m_hash_prev;                 // Previous in hash chain

    //------------------------------------------------------------------
    // Timeout Management
    //------------------------------------------------------------------
    Uint32 m_creation_time;             // For timeout detection
    Uint32 m_last_activity_time;        // Last operation timestamp
};
```

### Node-Level State Management

The shared state is managed at the node level (not per DBLQH instance), accessible by all LDM threads:

```cpp
class Dblqh : public SimulatedBlock {
    // ...existing members...

    //------------------------------------------------------------------
    // Join Aggregation State Management (node-level, thread-safe)
    // These are STATIC - one per data node, shared across all DBLQH instances
    //------------------------------------------------------------------

    // Hash table for state lookup - protected by s_join_agg_hash_mutex
    static constexpr Uint32 JOIN_AGG_STATE_HASH_SIZE = 256;
    static NdbMutex* s_join_agg_hash_mutex;
    static Uint32 s_join_agg_state_hash[JOIN_AGG_STATE_HASH_SIZE];

    // Pool for JoinAggregationState records (uses QUERY_MEMORY via ndbd_malloc)
    static ArrayPool<JoinAggregationState> s_joinAggStatePool;

    // State management functions (operate on static data)
    static JoinAggregationState* createJoinAggState(
        const Uint32 transid[2],
        Uint32 senderData,
        Uint32 requestId,
        Signal* signal);

    static JoinAggregationState* findJoinAggState(
        const Uint32 transid[2],
        Uint32 senderData,
        Uint32 requestId);

    static void releaseJoinAggState(JoinAggregationState* state);

    // Hash function
    static Uint32 hashJoinAggState(const Uint32 transid[2], Uint32 senderData, Uint32 requestId);

    // Initialization (called once at node startup)
    static void initJoinAggStatePool();
};
```

### Signal Flow

#### Signal Routing via DblqhProxy

The setup, completion, and release signals only need to be executed by one thread per node. `DblqhProxy.cpp` can route these signals to a designated thread:

- `JOIN_AGG_SETUP_REQ` - routed to one thread, creates shared state
- `JOIN_AGG_COMPLETE_REQ` - routed to one thread, finalizes and sends results
- `JOIN_AGG_RELEASE_REQ` - routed to one thread, releases state

The operation signals (`LQHKEYREQ`, `SCAN_FRAGREQ`) continue to be routed normally based on fragment ownership.

#### Phase 1: Setup (New Signal)

DBSPJ sends `JOIN_AGG_SETUP_REQ` to each data node that has fragments of the leaf table:

```cpp
// Signal: JOIN_AGG_SETUP_REQ
// Sent from: DBSPJ
// Sent to: DBLQH (one per data node with leaf table fragments)
// Purpose: Create shared aggregation state before operations start

struct JoinAggSetupReq {
    static constexpr Uint32 SignalLength = 10;
    static constexpr Uint32 AggProgramSectionNum = 0;

    Uint32 senderRef;           // DBSPJ block reference
    Uint32 senderData;          // SPJ request identifier
    Uint32 requestId;           // Unique ID for this aggregation request
    Uint32 transid[2];          // Transaction ID
    Uint32 tableId;             // Leaf table ID
    Uint32 expectedOpCount;     // Expected number of operations (0 = unknown)
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
    Uint32 aggStatePtr;         // Handle to created state (for debugging)
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

**execJOIN_AGG_SETUP_REQ Flow:**

```
JOIN_AGG_SETUP_REQ arrives (from DBSPJ, routed by DblqhProxy)
    |
    v
Acquire s_join_agg_hash_mutex (static, node-level)
    |
    v
Allocate JoinAggregationState from s_joinAggStatePool
    (uses ndbd_malloc with QUERY_MEMORY)
    |
    v
Copy aggregation program from signal section
    |
    v
Create and initialize AggInterpreter
    |
    v
Initialize m_interpreter_mutex
    |
    v
Store result routing info (resultRef, resultData, routeRef)
    |
    v
Set m_state = SETUP_COMPLETE
    |
    v
Insert into s_join_agg_state_hash
    |
    v
Release s_join_agg_hash_mutex
    |
    v
Send JOIN_AGG_SETUP_CONF
```

#### Phase 2: Operations (LQHKEYREQ / SCAN_FRAGREQ)

Operations include a reference to the aggregation state:

```cpp
// New flags in LqhKeyReq::requestInfo
namespace LqhKeyReq {
    static constexpr Uint32 RI_JOIN_AGGREGATION = 0x...;  // Has join aggregation
}

// New flags in ScanFragReq::requestInfo
namespace ScanFragReq {
    static constexpr Uint32 RI_JOIN_AGGREGATION = 0x...;  // Has join aggregation
}

// Additional words in LQHKEYREQ/SCAN_FRAGREQ when RI_JOIN_AGGREGATION set:
// - requestId: matches JOIN_AGG_SETUP_REQ.requestId
// - Parent data in AttrInfo section (linked attributes)
```

**LQHKEYREQ with Join Aggregation:**

```
LQHKEYREQ arrives (with RI_JOIN_AGGREGATION flag)
    |
    v
Extract requestId from signal
    |
    v
findJoinAggState(transid, senderData, requestId)
    |
    v
[State not found?] --> Send LQHKEYREF (state not initialized)
    |
    v
[State found, m_state == ERROR?] --> Send LQHKEYREF (aggregation failed)
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
Process row through AggInterpreter with parent data:
    m_agg_interpreter->ProcessRecWithLinkedAttrs(...)
    |
    +-- Internally uses two-level locking:
    |   1. Brief lock on m_group_map_mutex to find/create group
    |   2. Lock group->m_mutex to update accumulators
    |   3. Unlock group->m_mutex
    |   (Multiple threads can aggregate different groups in parallel)
    |
    v
Atomic increment: state->m_completed_ops++
    |
    v
Send LQHKEYCONF (DO NOT send row data - aggregation accumulates it)
```

**SCAN_FRAGREQ with Join Aggregation:**

```
SCAN_FRAGREQ arrives (with RI_JOIN_AGGREGATION flag)
    |
    v
Extract requestId from signal
    |
    v
findJoinAggState(transid, senderData, requestId)
    |
    v
[State not found?] --> Send SCAN_FRAGREF (state not initialized)
    |
    v
Create ScanRecord with reference to JoinAggregationState:
    scanPtr->m_join_agg_state_ptr = state
    |
    v
For each row in scan:
    |
    v
    Extract parent row data from AttrInfo
    |
    v
    Process row through shared AggInterpreter:
        state->m_agg_interpreter->ProcessRecWithLinkedAttrs(...)
        |
        +-- Two-level locking (parallel across groups):
            1. Brief lock on m_group_map_mutex to find/create group
            2. Lock group->m_mutex to update accumulators
            3. Unlock group->m_mutex
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
    static constexpr Uint32 SignalLength = 6;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
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

**execJOIN_AGG_COMPLETE_REQ Flow:**

```
JOIN_AGG_COMPLETE_REQ arrives (from DBSPJ, routed by DblqhProxy)
    |
    v
findJoinAggState(transid, senderData, requestId)
    |
    v
[State not found?] --> Send JOIN_AGG_COMPLETE_REF
    |
    v
Verify: state->m_completed_ops == completedOps
    |
    v
Acquire state->m_interpreter_mutex
    |
    v
Set state->m_state = FINALIZING
    |
    v
Finalize aggregation: m_agg_interpreter->FinalizeResults()
    |
    v
Set state->m_state = SENDING_RESULTS
    |
    v
Release state->m_interpreter_mutex
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
    static constexpr Uint32 SignalLength = 4;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
};
```

### Thread-Safety Details

#### Basic Mutex Hierarchy

To prevent deadlocks, acquire mutexes in this order:

1. `s_join_agg_hash_mutex` (static, node-level, protects state hash table)
2. `state->m_group_map_mutex` (per-state, protects GROUP BY hash map)
3. `group->m_mutex` (per-group, protects aggregate accumulators)

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
    AggResItem m_accumulators[MAX_AGGREGATES];
    Uint64 m_row_count;         // Number of rows in this group
};

// Extended AggInterpreter with per-group locking
class AggInterpreter {
    // ...existing members...

    // GROUP BY hash map - protected by m_group_map_mutex
    NdbMutex* m_group_map_mutex;
    std::unordered_map<GroupKey, GroupByEntry*, GroupKeyHash> m_group_map;

    // Pool for GroupByEntry allocation
    GroupByEntry* m_group_pool;
    Uint32 m_group_pool_size;
    std::atomic<Uint32> m_next_group_slot;
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
        Register values[MAX_AGGREGATES];
        extractAggregateInputs(linked_attr_data, req_struct, values);

        // Update each accumulator
        for (Uint32 i = 0; i < m_num_aggregates; i++) {
            updateAccumulator(&group->m_accumulators[i], m_agg_functions[i], &values[i]);
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

#### Complete Row Processing with Per-Group Locking

```cpp
void processRowForJoinAgg(JoinAggregationState* state,
                          const Uint32* rowData,
                          const Uint32* linkedData,
                          Uint32 linkedDataLen) {
    AggInterpreter* interp = state->m_agg_interpreter;

    // Process row with two-level locking:
    // 1. Brief lock on group map to find/create group
    // 2. Per-group lock to update accumulators
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

#### Finalization with Per-Group Locking

During finalization, we need to ensure no concurrent updates:

```cpp
int AggInterpreter::FinalizeResults() {
    // Acquire group map mutex to prevent new group creation
    NdbMutex_Lock(m_group_map_mutex);

    // Set finalizing flag to reject new rows
    m_finalizing = true;

    // Iterate all groups - no per-group lock needed since
    // m_finalizing prevents concurrent updates
    for (auto& [key, group] : m_group_map) {
        // Finalize each accumulator (e.g., compute AVG from SUM/COUNT)
        for (Uint32 i = 0; i < m_num_aggregates; i++) {
            finalizeAccumulator(&group->m_accumulators[i], m_agg_functions[i]);
        }
    }

    NdbMutex_Unlock(m_group_map_mutex);
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
    AggResItem m_accumulators[MAX_AGGREGATES];  // variable

    // Cold data: key info (accessed during lookup/finalization)
    Uint32 m_key_offset;
    Uint32 m_key_len;

    // Padding to cache line boundary
    char m_padding[...];
};
```

#### Contention Analysis

| Scenario | Single Mutex | Per-Group Mutex |
|----------|--------------|-----------------|
| Low cardinality (e.g., 7 shipmodes) | Some contention | Minimal - only 7 groups |
| High cardinality (e.g., 1M customers) | Severe contention | Parallel across groups |
| Skewed distribution | Bottleneck on hot groups | Hot groups still serialize |
| Group map growth | N/A | Brief contention on insert |

**Trade-offs:**
- Per-group mutex adds ~8 bytes per group
- Lock-free lookup adds complexity but improves read-heavy workloads
- For very low cardinality (< 10 groups), single mutex may be simpler

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
    Uint32 m_linked_attr_buf[MAX_LINKED_ATTR_SIZE];
    Uint32 m_linked_attr_len;

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

Add reference to shared aggregation state:

```cpp
struct ScanRecord {
    // ... existing fields ...

    // For join aggregation - reference to shared state
    Uint32 m_join_agg_state_ptr;    // Ptr to JoinAggregationState (RNIL if none)
    Uint32 m_join_agg_request_id;   // Request ID for lookup
};
```

#### TcConnectionrec

Add reference to shared aggregation state for key operations:

```cpp
struct TcConnectionrec {
    // ... existing fields ...

    // For join aggregation operations
    Uint32 m_join_agg_state_ptr;    // Ptr to JoinAggregationState (RNIL if none)
    Uint32 m_join_agg_request_id;   // Request ID for lookup
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

Memory is allocated using NDB's internal allocator from `ndbd_malloc_impl.hpp` with `QUERY_MEMORY` resource. No configuration parameters needed.

```cpp
#include "vm/ndbd_malloc_impl.hpp"

// Allocation for JoinAggregationState and AggInterpreter
void* ptr = ndbd_malloc(size, RG_QUERY_MEMORY);

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
DBSPJ                              DBLQH                                 DBTUP
  |                                      |                                  |
  |====== Setup Phase (routed by DblqhProxy to one thread) ==============  |
  |                                      |                                  |
  | JOIN_AGG_SETUP_REQ                   |                                  |
  |------------------------------------->|                                  |
  |                                      | Create JoinAggregationState      |
  |                                      | Parse aggregation program        |
  |                                      | Initialize AggInterpreter        |
  |                                      | Insert in static hash table      |
  | JOIN_AGG_SETUP_CONF                  |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  |====== Operations Phase (parallel, any LDM thread) ===================  |
  |                                      |                                  |
  | LQHKEYREQ (join agg, thread 1)       |                                  |
  |------------------------------------->|                                  |
  |                                      | Find state in static hash        |
  |                                      | TUPKEYREQ                        |
  |                                      |--------------------------------->|
  |                                      |                                  | Read local row
  |                                      | TUPKEYCONF                       |
  |                                      |<---------------------------------|
  |                                      | ProcessRecWithLinkedAttrs():     |
  |                                      |   Lock group_map, find group     |
  |                                      |   Unlock group_map               |
  |                                      |   Lock group->mutex              |
  |                                      |   Update accumulators            |
  |                                      |   Unlock group->mutex            |
  |                                      | Increment completed_ops          |
  | LQHKEYCONF                           |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | SCAN_FRAGREQ (join agg, thread 2)    |                                  |
  |------------------------------------->|                                  |
  |                                      | Find state, create ScanRecord    |
  |                                      | For each row:                    |
  |                                      |   Read via DBTUP                 |
  |                                      |   Lock group_map, find group     |
  |                                      |   Unlock group_map               |
  |                                      |   Lock group->mutex              |
  |                                      |   Update accumulators            |
  |                                      |   Unlock group->mutex            |
  |                                      |   (parallel with other threads   |
  |                                      |    on different groups)          |
  |                                      | Increment completed_ops          |
  | SCAN_FRAGCONF                        |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | ... more operations (parallel) ...   |                                  |
  |                                      |                                  |
  |====== Completion Phase (routed by DblqhProxy to one thread) =========  |
  |                                      |                                  |
  | JOIN_AGG_COMPLETE_REQ                |                                  |
  |------------------------------------->|                                  |
  |                                      | Find state                       |
  |                                      | Verify completed_ops             |
  |                                      | Lock interpreter                 |
  |                                      | FinalizeResults()                |
  |                                      | Unlock                           |
  |                                      |                                  |
  |                                      | TRANSID_AI (aggregated results)  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | JOIN_AGG_COMPLETE_CONF               |                                  |
  |<-------------------------------------|                                  |
  |                                      |                                  |
  | DBSPJ routes results to API          |                                  |
  |                                      |                                  |
  |====== Cleanup Phase (routed by DblqhProxy to one thread) ============  |
  |                                      |                                  |
  | JOIN_AGG_RELEASE_REQ                 |                                  |
  |------------------------------------->|                                  |
  |                                      | Remove from hash, free memory    |
```

## File Changes Summary

| File | Changes |
|------|---------|
| `Dblqh.hpp` | Add JoinAggregationState struct, static pool/hash/mutex |
| `DblqhMain.cpp` | Add execJOIN_AGG_SETUP_REQ, execJOIN_AGG_COMPLETE_REQ, execJOIN_AGG_RELEASE_REQ, modify execLQHKEYREQ, execSCAN_FRAGREQ |
| `DblqhProxy.cpp` | Route JOIN_AGG_* signals to designated thread |
| `LqhKeyReq.hpp` | Add RI_JOIN_AGGREGATION flag |
| `ScanFrag.hpp` | Add RI_JOIN_AGGREGATION flag |
| `signaldata/JoinAgg.hpp` (new) | Signal definitions for JOIN_AGG_* |
| `AggInterpreter.hpp` | Add ProcessRecWithLinkedAttrs, FinalizeResults |
| `AggInterpreter.cpp` | Implement linked attribute handling |

## Open Questions (Resolved)

1. **Batch Completion Mechanism**: ✓ Dedicated JOIN_AGG_COMPLETE_REQ signal

2. **Thread Safety**: ✓ Mutex per interpreter, atomics for counters, hash mutex for lookups

3. **Multi-Node**: ✓ Each node has independent state, DBSPJ coordinates final aggregation if needed

## Remaining Design Decisions

1. **Result Aggregation Across Nodes**: If leaf table spans multiple nodes, does DBSPJ aggregate the per-node results, or is there node-to-node coordination?

2. **Partial Results**: Should DBLQH send partial results if aggregation buffer fills up, or wait for completion?

3. **Interpreter Pool**: Pre-allocate interpreter instances, or create on demand?

4. **State Timeout**: How long to keep state before automatic cleanup? Tied to transaction timeout?

## Performance Considerations

1. **Per-Group Locking**: Two-level locking minimizes contention
   - Group map mutex held briefly for find/insert
   - Per-group mutex allows parallel aggregation across different groups
   - For high-cardinality GROUP BY, threads rarely contend
   - For low-cardinality (< 10 groups), some contention on hot groups

2. **Memory Usage**: Each state includes an AggInterpreter (hash map for groups)
   - Uses QUERY_MEMORY pool, shared with other query execution
   - Per-group mutex adds ~8 bytes overhead per group
   - GroupByEntry aligned to cache lines to prevent false sharing
   - Memory released when JOIN_AGG_RELEASE_REQ processed

3. **Hash Table Lookup**: State lookup required once per operation start
   - State pointer cached in TcConnectionrec/ScanRecord after lookup
   - Static hash table with simple mutex - fast for typical concurrency
   - Lock-free group lookup optimization for read-heavy workloads

4. **Single-Thread Phases**: Setup/completion/release routed to one thread
   - Serializes state creation and result sending
   - Operations phase fully parallel across LDM threads
   - Finalization sets flag to reject new rows, then iterates groups
