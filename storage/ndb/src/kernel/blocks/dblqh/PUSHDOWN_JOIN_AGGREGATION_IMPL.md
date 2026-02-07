# DBLQH Pushdown Join Aggregation — Implementation Description

This document describes the concrete code changes needed to implement the
architecture defined in `PUSHDOWN_JOIN_AGGREGATION.md`. It lists every new
file, new class/struct, new method, modified file, and the implementation
order. No code is changed by this document — it serves as the bridge from
architecture to implementation.

## 1. New Global Signal Numbers

**File:** `storage/ndb/include/kernel/GlobalSignalNumbers.h`

Current MAX_GSN = 943 (line 36). Add after GSN_QUOTA_OVERLOAD_REP (943):

```
GSN_JOIN_AGG_SETUP_REQ       944
GSN_JOIN_AGG_SETUP_CONF      945
GSN_JOIN_AGG_SETUP_REF       946
GSN_JOIN_AGG_COMPLETE_REQ    947
GSN_JOIN_AGG_COMPLETE_CONF   948
GSN_JOIN_AGG_COMPLETE_REF    949
GSN_JOIN_AGG_RELEASE_REQ     950
GSN_JOIN_AGG_RELEASE_CONF    951
```

Update `MAX_GSN` from 943 to 951.

**File:** `storage/ndb/src/common/debugger/signaldata/SignalNames.cpp`

Add after the `GSN_QUOTA_OVERLOAD_REP` entry (line 972), before the closing `};`:

```cpp
  ,{ GSN_JOIN_AGG_SETUP_REQ, "JOIN_AGG_SETUP_REQ" }
  ,{ GSN_JOIN_AGG_SETUP_CONF, "JOIN_AGG_SETUP_CONF" }
  ,{ GSN_JOIN_AGG_SETUP_REF, "JOIN_AGG_SETUP_REF" }
  ,{ GSN_JOIN_AGG_COMPLETE_REQ, "JOIN_AGG_COMPLETE_REQ" }
  ,{ GSN_JOIN_AGG_COMPLETE_CONF, "JOIN_AGG_COMPLETE_CONF" }
  ,{ GSN_JOIN_AGG_COMPLETE_REF, "JOIN_AGG_COMPLETE_REF" }
  ,{ GSN_JOIN_AGG_RELEASE_REQ, "JOIN_AGG_RELEASE_REQ" }
  ,{ GSN_JOIN_AGG_RELEASE_CONF, "JOIN_AGG_RELEASE_CONF" }
```

The `NO_OF_SIGNAL_NAMES` constant (line 974) is computed from `sizeof(SignalNames)`
so it updates automatically.

## 2. New Record Type

**File:** `storage/ndb/src/kernel/blocks/record_types.hpp`

After RT_DBTUX_SCAN_BOUND = MAKE_TID(33, RG_TRANSACTION_MEMORY) (line 140), add:

```cpp
#define RT_DBLQH_JOIN_AGG_STATE  MAKE_TID(34, RG_TRANSACTION_MEMORY)
```

Uses `RG_TRANSACTION_MEMORY` to match existing DBLQH TransientPool record
types (RT_DBLQH_TC_CONNECT, RT_DBLQH_SCAN_RECORD, etc.). TID 34 is the
next available in this resource group.

## 3. New File: Signal Data — `JoinAgg.hpp`

**File:** `storage/ndb/include/kernel/signaldata/JoinAgg.hpp` (new)

Contains signal data structs for all JOIN_AGG signals. Follows the pattern
in `ScanFrag.hpp` (static SignalLength, static getter/setter methods).

### Structs

```cpp
struct JoinAggSetupReq {
    static constexpr Uint32 SignalLength = 11;
    static constexpr Uint32 AggProgramSectionNum = 0;
    static constexpr Uint32 STRATEGY_MUTEX_BASED = 0;
    static constexpr Uint32 STRATEGY_MUTEX_FREE = 1;

    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
    Uint32 tableId;
    Uint32 expectedOpCount;
    Uint32 concurrencyStrategy;
    Uint32 resultRef;
    Uint32 resultData;
    Uint32 routeRef;
    // Long section 0: Aggregation program
};

struct JoinAggSetupConf {
    static constexpr Uint32 SignalLength = 4;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 aggStateKey;     // Pool index for O(1) lookup
};

struct JoinAggSetupRef {
    static constexpr Uint32 SignalLength = 5;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 errorCode;
    Uint32 errorLine;
};

struct JoinAggCompleteReq {
    static constexpr Uint32 SignalLength = 7;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
    Uint32 aggStateKey;
    Uint32 completedOps;
};

struct JoinAggCompleteConf {
    static constexpr Uint32 SignalLength = 5;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 numResultRows;
    Uint32 resultBytes;
};

struct JoinAggCompleteRef {
    static constexpr Uint32 SignalLength = 5;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 errorCode;
    Uint32 errorLine;
};

struct JoinAggReleaseReq {
    static constexpr Uint32 SignalLength = 6;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
    Uint32 transid[2];
    Uint32 aggStateKey;
};

struct JoinAggReleaseConf {
    static constexpr Uint32 SignalLength = 3;
    Uint32 senderRef;
    Uint32 senderData;
    Uint32 requestId;
};
```

## 4. New File: State Structure — `JoinAggregationState.hpp`

**File:** `storage/ndb/src/kernel/blocks/dblqh/JoinAggregationState.hpp` (new)

Contains the `JoinAggregationState` struct exactly as specified in the
architecture document. Key points:

- `static constexpr Uint32 TYPE_ID = RT_DBLQH_JOIN_AGG_STATE;`
- `Uint32 m_magic;` (required by ArrayPool)
- All fields from architecture: identification, aggregation program,
  concurrency strategy, interpreter pointers, atomic counters, state machine,
  result routing, key, timeout management
- `ConcurrencyStrategy` enum: MUTEX_BASED=0, MUTEX_FREE=1
- `State` enum: IDLE=0 through ABORTING=7

Include `<atomic>` for `std::atomic<Uint32>` and `std::atomic<State>`.

## 5. Modified: LqhKey.hpp — Join Aggregation Flag Bit

**File:** `storage/ndb/include/kernel/signaldata/LqhKey.hpp`

Use bit 7 (currently `RI_CLEAR_SHIFT7`, marked "Currently unused" at line 245):

```cpp
// Replace RI_CLEAR_SHIFT7 = 7 with:
RI_JOIN_AGG_SHIFT = 7,    // Join aggregation: extra word with aggStateKey
```

Add getter/setter following the existing pattern (lines 481-692):

```cpp
static void setJoinAggFlag(UintR &requestInfo, UintR val) {
    ASSERT_BOOL(val, "LqhKeyReq::setJoinAggFlag");
    requestInfo |= (val << RI_JOIN_AGG_SHIFT);
}
static Uint8 getJoinAggFlag(const UintR &requestInfo) {
    return (Uint8)((requestInfo >> RI_JOIN_AGG_SHIFT) & 1);
}
```

When RI_JOIN_AGG_SHIFT is set, the signal contains one extra word after the
standard signal: the `aggStateKey` (Uint32 pool index).

## 6. Modified: ScanFrag.hpp — Join Aggregation Flag Bit

**File:** `storage/ndb/include/kernel/signaldata/ScanFrag.hpp`

Add after SF_TTL_IGNORE_SHIFT (24) at line 387:

```cpp
#define SF_JOIN_AGG_SHIFT (25)
```

Add getter/setter following the `setAggregationFlag` pattern (lines 558-568):

```cpp
static void setJoinAggFlag(Uint32 &requestInfo, UintR val);
static Uint32 getJoinAggFlag(const Uint32 &requestInfo);
```

Update the requestInfo bit map comment (lines 352-355) to include bit 25.

When SF_JOIN_AGG_SHIFT is set, the signal contains one extra word after the
standard signal: the `aggStateKey`.

## 7. Modified: Dblqh.hpp — New Fields in Existing Structs

**File:** `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp`

### ScanRecord (lines 603-759)

Add after `m_agg_interpreter` (line 755):

```cpp
Uint32 m_join_agg_state_key;    // Pool index for shared join agg state (RNIL if none)
```

Initialize to `RNIL` in constructor (add to initializer list at line 607).

### TcConnectionrec (lines 2642-2915)

Add after `m_query_thread` (line 2915):

```cpp
Uint32 m_join_agg_state_key;    // Pool index for shared join agg state (RNIL if none)
```

Initialize to `RNIL` in constructor.

### Signal Handler Declarations (after line 3204)

Add handler for JOIN_AGG_COMPLETE_REQ — this signal is routed via V_QUERY
to a Dblqh worker based on CPU load (not to DblqhProxy):

```cpp
void execJOIN_AGG_COMPLETE_REQ(Signal* signal);
```

Register in DblqhInit.cpp (addRecSignal section):

```cpp
addRecSignal(GSN_JOIN_AGG_COMPLETE_REQ, &Dblqh::execJOIN_AGG_COMPLETE_REQ);
```

## 8. Modified: SimulatedBlock — Static Pool and Access Methods

Use `TransientPool` instead of `ArrayPool`. TransientPool is the standard
DBLQH pattern for bounded-lifetime records (see `TcConnectionrec_pool`,
`ScanRecord_pool` in Dblqh.hpp). It provides lazy page initialization,
automatic shrinking, magic-validated access via `getValidPtr()`, and
debug memory poisoning (0xF4 on seize/release in VM_TRACE builds).

**File:** `storage/ndb/src/kernel/vm/SimulatedBlock.hpp`

Add near existing static members (lines 1716-1723):

```cpp
//------------------------------------------------------------------
// Join Aggregation State Pool (node-level, shared across all instances)
// Uses TransientPool for bounded-lifetime records with lazy init
// and automatic shrinking.
//------------------------------------------------------------------
typedef TransientPool<JoinAggregationState> JoinAggState_pool;
static JoinAggState_pool c_joinAggStatePool;

static Uint32 seizeJoinAggState();          // DblqhProxy only
static JoinAggregationState* getJoinAggState(Uint32 key);  // any thread
static void releaseJoinAggState(Uint32 key); // DblqhProxy only
static void initJoinAggStatePool(Uint32 min_recs, Uint32 max_recs);
```

Forward-declare `struct JoinAggregationState;` near top of file.
Include `TransientPool.hpp` if not already included.

**File:** `storage/ndb/src/kernel/vm/SimulatedBlock.cpp`

Add static member definition and method implementations:

```cpp
SimulatedBlock::JoinAggState_pool SimulatedBlock::c_joinAggStatePool;

void SimulatedBlock::initJoinAggStatePool(Uint32 min_recs, Uint32 max_recs) {
    Pool_context pc;
    // TransientPool::init(type_id, pool_ctx, min_recs, max_recs)
    c_joinAggStatePool.init(RT_DBLQH_JOIN_AGG_STATE, pc, min_recs, max_recs);
}

Uint32 SimulatedBlock::seizeJoinAggState() {
    Ptr<JoinAggregationState> ptr;
    if (c_joinAggStatePool.seize(ptr)) {
        ptr.p->m_key = ptr.i;
        return ptr.i;
    }
    return RNIL;
}

JoinAggregationState* SimulatedBlock::getJoinAggState(Uint32 key) {
    if (key == RNIL) return nullptr;
    return c_joinAggStatePool.getPtr(key);
}

void SimulatedBlock::releaseJoinAggState(Uint32 key) {
    Ptr<JoinAggregationState> ptr;
    ptr.i = key;
    c_joinAggStatePool.getPtr(ptr);
    c_joinAggStatePool.release(ptr);
}
```

Note: `TransientPool::release()` takes `Ptr<T>` (not bare Uint32), so we
must construct the Ptr first. The release call destructs the record and
poisons memory in debug builds.

## 9. Modified: DblqhProxy — Signal Handlers

**File:** `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.hpp`

Add signal handler declarations (after existing handlers, around line 181):

```cpp
// Join Aggregation signals (setup + release handled by proxy)
void execJOIN_AGG_SETUP_REQ(Signal*);
void execJOIN_AGG_RELEASE_REQ(Signal*);
```

Note: `JOIN_AGG_COMPLETE_REQ` is NOT handled by DblqhProxy — it is sent
to V_QUERY and handled by a Dblqh worker (see section 10).

**File:** `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp`

### Constructor (lines 51-183)

Add signal registration after existing addRecSignal calls:

```cpp
// JOIN_AGG signals (setup + release handled by proxy)
addRecSignal(GSN_JOIN_AGG_SETUP_REQ,    &DblqhProxy::execJOIN_AGG_SETUP_REQ);
addRecSignal(GSN_JOIN_AGG_RELEASE_REQ,  &DblqhProxy::execJOIN_AGG_RELEASE_REQ);
```

### New Method Descriptions

**execJOIN_AGG_SETUP_REQ:**
1. Parse signal into `JoinAggSetupReq`
2. `Uint32 key = seizeJoinAggState()`
3. `JoinAggregationState* state = getJoinAggState(key)`
4. Copy aggregation program from long section 0
5. Initialize based on `concurrencyStrategy`:
   - MUTEX_BASED: allocate one AggInterpreter via ndbd_malloc
   - MUTEX_FREE: allocate `getNumQueryThreads()` AggInterpreters via ndbd_malloc
6. Set state fields (identification, result routing, state machine)
7. Send `JOIN_AGG_SETUP_CONF` with `aggStateKey = key`

**execJOIN_AGG_RELEASE_REQ:**
1. Parse signal into `JoinAggReleaseReq`
2. `state = getJoinAggState(req->aggStateKey)`
3. Free all ndbd_malloc'd memory (interpreters, buffers)
4. `releaseJoinAggState(req->aggStateKey)`
5. Send `JOIN_AGG_RELEASE_CONF`

## 10. Modified: DblqhMain.cpp — Operation and Completion Handlers

**File:** `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`

### execJOIN_AGG_COMPLETE_REQ (new handler, routed via V_QUERY)

DBSPJ sends this signal to V_QUERY, which routes it to a Dblqh worker
based on current CPU load. The merge/finalize work is CPU-intensive, so
V_QUERY ensures it runs on a thread with available capacity.

```cpp
void Dblqh::execJOIN_AGG_COMPLETE_REQ(Signal* signal) {
    jamEntry();
    const JoinAggCompleteReq* req =
        (const JoinAggCompleteReq*)signal->getDataPtr();

    JoinAggregationState* state = getJoinAggState(req->aggStateKey);
    if (state == nullptr) {
        // Send JOIN_AGG_COMPLETE_REF
        return;
    }

    // Verify operation count
    ndbrequire(state->m_completed_ops == req->completedOps);

    // Set state to FINALIZING — single-threaded, all operations
    // already completed before DBSPJ sends this signal
    state->m_state.store(JoinAggregationState::FINALIZING);

    if (state->m_strategy == JoinAggregationState::MUTEX_BASED) {
        state->m_agg_interpreter->FinalizeResults();
    } else {
        // MUTEX_FREE: merge all per-thread interpreters, then finalize
        AggInterpreter::MergeAllByBucket(
            state->m_per_thread_interpreters, state->m_num_threads);
        state->m_per_thread_interpreters[0]->FinalizeResults();
    }

    // Send TRANSID_AI with aggregated results
    // ... (use state->m_resultRef, m_resultData, m_routeRef)

    // Send JOIN_AGG_COMPLETE_CONF
    state->m_state.store(JoinAggregationState::COMPLETED);
}
```

### execLQHKEYREQ (line 8722)

After extracting `Treqinfo` (requestInfo) around line 8731, add:

```cpp
const Uint32 joinAggFlag = LqhKeyReq::getJoinAggFlag(Treqinfo);
Uint32 aggStateKey = RNIL;
if (joinAggFlag) {
    // Extra word at end of signal contains aggStateKey
    aggStateKey = signal->theData[LqhKeyReq::FixedSignalLength + <offset>];
}
```

Later, when creating/initializing TcConnectionrec:

```cpp
regTcPtr->m_join_agg_state_key = aggStateKey;
```

After TUPKEYCONF (when row is read), if `aggStateKey != RNIL`:

```cpp
JoinAggregationState* state = getJoinAggState(aggStateKey);
AggInterpreter* interp;
if (state->m_strategy == JoinAggregationState::MUTEX_BASED) {
    interp = state->m_agg_interpreter;
} else {
    interp = state->m_per_thread_interpreters[getOwnThreadIndex()];
}
interp->ProcessRecWithLinkedAttrs(block_tup, req_struct, linked_data, linked_len);
state->m_completed_ops.fetch_add(1, std::memory_order_relaxed);
```

Send LQHKEYCONF without row data (aggregation accumulates it).

### execSCAN_FRAGREQ (line 17615)

After extracting requestInfo around line 17628, add:

```cpp
const Uint32 joinAggFlag = ScanFragReq::getJoinAggFlag(requestInfo);
Uint32 aggStateKey = RNIL;
if (joinAggFlag) {
    aggStateKey = signal->theData[ScanFragReq::SignalLength + <offset>];
}
```

When creating ScanRecord:

```cpp
scanPtr->m_join_agg_state_key = aggStateKey;
```

In the scan loop (where rows are processed), if `m_join_agg_state_key != RNIL`:
use the same strategy-dependent interpreter selection as LQHKEYREQ,
call `ProcessRecWithLinkedAttrs()` for each row.

## 11. Modified: AggInterpreter — New Methods

**File:** `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.hpp`

Add to public section (after `ProcessRec` at line 104):

```cpp
// Join aggregation: process row with linked attributes from parent tables
Int32 ProcessRecWithLinkedAttrs(
    Dbtup* block_tup,
    Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len);

// Finalize results (compute AVG from SUM/COUNT, etc.)
Int32 FinalizeResults();

// Get result data for sending via TRANSID_AI
Int32 GetResultData(Uint32* buffer, Uint32 buffer_size, Uint32* bytes_written);

// Merge all per-thread interpreters by bucket (MUTEX_FREE completion)
static Int32 MergeAllByBucket(
    AggInterpreter* interpreters[],
    Uint32 num_interpreters);

// Merge another interpreter's results into this one
Int32 MergeFrom(AggInterpreter* other);
```

Add to private section (after existing members at line 172):

```cpp
// Linked attribute buffer (ndbd_malloc'd at creation, not MAX-sized)
Uint32* m_linked_attr_buf;
Uint32 m_linked_attr_buf_size;  // Allocated size in words
Uint32 m_linked_attr_len;       // Current content length

// Column source resolution
Int32 LoadColumnValue(Uint32 col_id, Register* reg);
```

**File:** `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp` (or equivalent)

Implement:

- `ProcessRecWithLinkedAttrs`: Copy linked attrs to buffer, then run
  aggregation program using `LoadColumnValue` which checks bit 15 to
  distinguish local columns (read via DBTUP) from linked columns (read
  from buffer).

- `FinalizeResults`: Iterate all groups in `gb_map_`, finalize each
  accumulator (e.g., AVG = SUM/COUNT).

- `MergeFrom`: For each group in `other->gb_map_`, find or create in
  `this->gb_map_`, merge accumulators using rules: SUM+=SUM, COUNT+=COUNT,
  MIN=min(), MAX=max().

- `MergeAllByBucket`: Iterate buckets 0..hash_table_size-1. For each bucket,
  merge entries from interpreters[1..N-1] into interpreters[0]. Same-bucket
  constraint ensures cache-friendly access.

- `GetResultData`: Serialize finalized group results into TRANSID_AI format.

## 12. Modified: Dbspj — Orchestration

**File:** `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`

Add to TreeNode (near existing aggregation fields, T_AGGREGATE_LEAF at line 1052):

```cpp
Uint32 m_join_agg_state_key;    // aggStateKey from JOIN_AGG_SETUP_CONF (per node)
Uint32 m_join_agg_setup_pending; // Number of outstanding SETUP requests
Uint32 m_join_agg_strategy;     // Chosen concurrency strategy
```

Add signal handler declarations:

```cpp
void execJOIN_AGG_SETUP_CONF(Signal*);
void execJOIN_AGG_SETUP_REF(Signal*);
void execJOIN_AGG_COMPLETE_CONF(Signal*);
void execJOIN_AGG_COMPLETE_REF(Signal*);
void execJOIN_AGG_RELEASE_CONF(Signal*);
```

**File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

### Setup Integration

Before sending operations for a leaf node with T_AGGREGATE_LEAF flag:

1. Determine concurrency strategy using heuristic (estimated_group_count
   vs num_query_threads)
2. Send `JOIN_AGG_SETUP_REQ` to each data node with leaf table fragments
3. Wait for all `JOIN_AGG_SETUP_CONF` — store `aggStateKey` per node
4. Only then proceed to send LQHKEYREQ/SCAN_FRAGREQ operations

### Operation Integration

In `lookup_send()` (line 4632):
- If TreeNode has T_AGGREGATE_LEAF and m_join_agg_state_key != RNIL:
  - Set `LqhKeyReq::setJoinAggFlag(req->requestInfo, 1)`
  - Append aggStateKey as extra word after standard signal

In `exec_scan_frag_send()` (line 7080):
- If TreeNode has T_AGGREGATE_LEAF and m_join_agg_state_key != RNIL:
  - Set `ScanFragReq::setJoinAggFlag(req->requestInfo, 1)`
  - Append aggStateKey as extra word after standard signal

### Completion Integration

After receiving LQHKEYCONF/SCAN_FRAGCONF for all operations:
1. Send `JOIN_AGG_COMPLETE_REQ` to **V_QUERY** on each data node
   (V_QUERY routes to a Dblqh worker based on CPU load — the merge/finalize
   work is CPU-intensive so it should not block DblqhProxy)
   ```cpp
   BlockReference ref = numberToRef(V_QUERY, instance_no, nodeId);
   sendSignal(ref, GSN_JOIN_AGG_COMPLETE_REQ, signal, ...);
   ```
2. On `JOIN_AGG_COMPLETE_CONF`: results already sent via TRANSID_AI
3. Send `JOIN_AGG_RELEASE_REQ` to DblqhProxy on each data node
4. On `JOIN_AGG_RELEASE_CONF`: cleanup complete

## 13. Error Code Definitions

**File:** `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp`

Add error codes in the 12xx range used by DBLQH. Codes 1238-1242 are
already taken by database quota errors (see `ndberror.cpp`), so use
1250-1259:

```cpp
#define ZJOIN_AGG_STATE_ALLOC_FAILED       1250
#define ZJOIN_AGG_STATE_NOT_FOUND          1251
#define ZJOIN_AGG_INTERPRETER_INIT_FAILED  1252
#define ZJOIN_AGG_INTERPRETER_ERROR        1253
#define ZJOIN_AGG_OP_COUNT_MISMATCH        1254
#define ZJOIN_AGG_PARENT_DATA_ERROR        1255
#define ZJOIN_AGG_RESULT_TOO_LARGE         1256
#define ZJOIN_AGG_TIMEOUT                  1257
#define ZJOIN_AGG_ALREADY_FINALIZED        1258
#define ZJOIN_AGG_MUTEX_ERROR              1259
```

Also add corresponding entries in `storage/ndb/src/ndbapi/ndberror.cpp`
for NDB API error reporting.

## 14. CMake Build Integration

**File:** `storage/ndb/src/kernel/blocks/CMakeLists.txt` (or equivalent)

- Add `JoinAggregationState.hpp` to DBLQH sources
- Add `JoinAgg.hpp` to signaldata headers

## 15. Implementation Phases

### Phase 1: Signal Infrastructure
- GlobalSignalNumbers.h: Add GSN 944-951
- SignalNames.cpp: Add name entries
- signaldata/JoinAgg.hpp: Create signal data structs
- record_types.hpp: Add RT_DBLQH_JOIN_AGG_STATE

### Phase 2: State Structure
- JoinAggregationState.hpp: Create state struct
- SimulatedBlock.hpp/cpp: Add static pool and access methods
- Pool initialization in node startup path

### Phase 3: Flag Bits
- LqhKey.hpp: RI_JOIN_AGG_SHIFT = 7 with getter/setter
- ScanFrag.hpp: SF_JOIN_AGG_SHIFT = 25 with getter/setter

### Phase 4: DblqhProxy Signal Handlers (Setup + Release)
- DblqhProxy.hpp: Declare setup + release handlers
- DblqhProxy.cpp: Register signals, implement execJOIN_AGG_SETUP_REQ,
  execJOIN_AGG_RELEASE_REQ

### Phase 5: Dblqh Operation and Completion Integration
- Dblqh.hpp: Add m_join_agg_state_key to ScanRecord and TcConnectionrec,
  declare execJOIN_AGG_COMPLETE_REQ handler
- DblqhInit.cpp: Register GSN_JOIN_AGG_COMPLETE_REQ
- DblqhMain.cpp: Implement execJOIN_AGG_COMPLETE_REQ (merge + finalize,
  received via V_QUERY); extract aggStateKey in execLQHKEYREQ and
  execSCAN_FRAGREQ, call ProcessRecWithLinkedAttrs after row read

### Phase 6: AggInterpreter Extensions
- AggInterpreter.hpp: Add new methods and linked attr buffer
- Implementation: ProcessRecWithLinkedAttrs, FinalizeResults, MergeFrom,
  MergeAllByBucket, GetResultData

### Phase 7: Dbspj Orchestration
- Dbspj.hpp: Add state fields to TreeNode, declare handlers
- DbspjMain.cpp: Setup/complete/release flow, strategy selection heuristic,
  aggStateKey insertion in LQHKEYREQ and SCAN_FRAGREQ

### Phase 8: Testing & Verification
- Forced strategy mode (always MUTEX_BASED or always MUTEX_FREE)
- Compare results between strategies for same query
- Error injection tests (state alloc failure, interpreter error, timeout)
- Stress test: high concurrency with low and high cardinality GROUP BY

## 16. Files Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `include/kernel/GlobalSignalNumbers.h` | Modify | Add GSN 944-951, update MAX_GSN |
| `src/common/debugger/signaldata/SignalNames.cpp` | Modify | Add signal name entries |
| `src/kernel/blocks/record_types.hpp` | Modify | Add RT_DBLQH_JOIN_AGG_STATE |
| `include/kernel/signaldata/JoinAgg.hpp` | **New** | Signal data structs |
| `src/kernel/blocks/dblqh/JoinAggregationState.hpp` | **New** | State struct |
| `include/kernel/signaldata/LqhKey.hpp` | Modify | RI_JOIN_AGG_SHIFT bit 7 |
| `include/kernel/signaldata/ScanFrag.hpp` | Modify | SF_JOIN_AGG_SHIFT bit 25 |
| `src/kernel/vm/SimulatedBlock.hpp` | Modify | Static TransientPool + access methods |
| `src/kernel/vm/SimulatedBlock.cpp` | Modify | TransientPool implementation |
| `src/kernel/blocks/dblqh/Dblqh.hpp` | Modify | m_join_agg_state_key in ScanRecord/TcConnectionrec, error codes 1250-1259, handler declaration |
| `src/ndbapi/ndberror.cpp` | Modify | Error message entries for 1250-1259 |
| `src/kernel/blocks/dblqh/DblqhInit.cpp` | Modify | addRecSignal for GSN_JOIN_AGG_COMPLETE_REQ |
| `src/kernel/blocks/dblqh/DblqhMain.cpp` | Modify | execJOIN_AGG_COMPLETE_REQ (via V_QUERY), aggStateKey extraction |
| `src/kernel/blocks/dblqh/DblqhProxy.hpp` | Modify | Setup + release handler declarations |
| `src/kernel/blocks/dblqh/DblqhProxy.cpp` | Modify | Signal registration + setup/release handlers |
| `src/kernel/blocks/dbtup/AggInterpreter.hpp` | Modify | New methods + linked attr members |
| `src/kernel/blocks/dbtup/AggInterpreter.cpp` | Modify | Method implementations |
| `src/kernel/blocks/dbspj/Dbspj.hpp` | Modify | TreeNode fields + handler declarations |
| `src/kernel/blocks/dbspj/DbspjMain.cpp` | Modify | Setup/complete/release orchestration |

All paths are relative to `storage/ndb/`.
