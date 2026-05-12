# Phase 6C: Hash Redistribution (Multi-Node CTE)

## Context

With 2+ data nodes, each node's hash table only has groups from its local fragments. Phase 6C redistributes groups after COMPLETE so each group lives on exactly one node (its hash-owner). CTE_LOOKUP_REQ then hashes the key to route to the correct node.

## Design

### Node Selection

- Build a **live node array** at SETUP time: `Uint32 m_cte_node_list[MAX_DATA_NODES]` with `m_cte_num_nodes` entries, containing only ZNODE_UP data nodes sorted by node ID
- Hash owner: `rondb_xxhash_std(key, keyLen) % m_cte_num_nodes` → index into `m_cte_node_list`
- Store the node failure counter (or a snapshot of alive nodes) at SETUP time; on COMPLETE, verify all nodes are still alive — abort if any node went down

### Node Failure Handling

- Record live node bitmask at SETUP time in `m_cte_nodes_at_setup` (NdbNodeBitmask)
- At COMPLETE, before redistribution: check that all nodes in `m_cte_node_list` are still connected via `getNodeInfo(nodeId).m_connected`
- If any node failed: abort the CTE (set state to ERROR, send COMPLETE_REF)
- During redistribution: if a sendSignal fails or node goes down, abort
- No attempt to recover from mid-query node failures

### Redistribution Algorithm

On COMPLETE (in CTE mode, after local merge/finalize):

1. If `m_cte_num_nodes == 1`: skip redistribution, go directly to CTE_READY (single-node case, Phase 6B path)
2. Verify all nodes still alive → abort if not
3. Iterate local hash table:
   - For each group: `hash(key) % m_cte_num_nodes → ownerIdx`
   - If `m_cte_node_list[ownerIdx] == getOwnNodeId()`: keep (no action)
   - Else: send JOIN_AGG_REDISTRIBUTE_ORD to owner node, erase locally
4. Batch via CONTINUEB (256 groups or 64KB per batch, same as `continueJoinAggSend`)
5. When done: send JOIN_AGG_FINAL_REP to all other nodes in the list
6. State: CTE_REDISTRIBUTING during this phase

### Receiving Redistribution

On receiving JOIN_AGG_REDISTRIBUTE_ORD:

1. Get JoinAggregationState via aggStateKey
2. **Check if local finalization is complete:**
   - If state is CTE_REDISTRIBUTING or CTE_READY: process immediately
   - If state is FINALIZING (still merging per-thread interpreters for MUTEX_FREE): **queue the incoming group** in a buffer for later processing
3. Read key from section 0, accumulator data from section 1
4. Look up key in local hash table:
   - **Found:** merge incoming accumulators with local ones (accumulator-wise: SUM→add, COUNT→add, MIN→min, MAX→max)
   - **Not found:** allocate new group entry, copy key + accumulators
5. No response needed (fire-and-forget ORD)

### Queued Redistribution Processing

When local finalization completes (state transitions from FINALIZING):
- Process all queued REDISTRIBUTE_ORD groups before starting own redistribution
- This ensures the local hash table is complete before we decide which groups to send away
- Queue storage: linked list of (key, accumulators) pairs allocated from the aggregation memory budget

### Completion

On receiving JOIN_AGG_FINAL_REP:

1. Set the sender's bit in `m_cte_nodes_finalized` bitmask
2. Check: if all other nodes have sent FINAL_REP AND own redistribution is done → transition to CTE_READY
3. CTE_LOOKUP_REQ can now be served

### CTE_LOOKUP_REQ Routing (future, in DBSPJ)

DBSPJ (not DBLQH) will hash the lookup key and send CTE_LOOKUP_REQ to the correct node. DBLQH doesn't need to route — it just looks up in its own hash table.

## Files to Modify

### 1. `JoinAggregationState.hpp`

Add fields:
```cpp
// CTE node distribution (set at SETUP, immutable after)
Uint32 m_cte_node_list[MAX_NDB_DATA_NODES];  // Live data node IDs
Uint32 m_cte_num_nodes;                       // Number of live nodes
bool m_cte_redistribution_done;               // This node finished sending

// Queue for REDISTRIBUTE_ORD groups arriving before local finalization
struct RedistQueueEntry {
  Uint32 keyLen;
  Uint32 valueLen;
  Uint32 data[1];  // Variable: [key_data][value_data]
};
RedistQueueEntry* m_redist_queue_head;  // Linked list of queued groups
RedistQueueEntry* m_redist_queue_tail;
Uint32 m_redist_queue_count;
```

Initialize in constructor: `m_cte_num_nodes = 0`, `m_cte_redistribution_done = false`,
`m_redist_queue_head = nullptr`, `m_redist_queue_tail = nullptr`, `m_redist_queue_count = 0`.

### 2. `DblqhProxy.cpp` — SETUP_REQ handling

When `m_cte_mode == true`:
- Build `m_cte_node_list` from currently alive data nodes
- Set `m_cte_num_nodes`
- Clear `m_cte_nodes_finalized` bitmask
- Set `m_cte_redistribution_done = false`

### 3. `DblqhMain.cpp`

**a) Modify CTE COMPLETE path** (in `continueJoinAggMerge()`, after finalize):

Replace the simple CTE_READY transition with:
```
if (m_cte_num_nodes <= 1) {
  // Single node — skip redistribution
  state = CTE_READY;
  send COMPLETE_CONF;
} else {
  // Multi-node — verify nodes still alive, start redistribution
  for each node in m_cte_node_list:
    if (!getNodeInfo(node).m_connected): abort with error
  state = CTE_REDISTRIBUTING;
  continueJoinAggRedistribute(signal, aggStateKey, ...);
}
```

**b) New function: `continueJoinAggRedistribute()`**

Iterates the hash table, sends REDISTRIBUTE_ORD to non-local groups:
```
for each group in hash table:
  hash = rondb_xxhash_std(key, keyLen)
  ownerIdx = hash % m_cte_num_nodes
  ownerNode = m_cte_node_list[ownerIdx]
  if (ownerNode == getOwnNodeId()):
    skip (keep locally)
  else:
    send JOIN_AGG_REDISTRIBUTE_ORD to numberToRef(DBLQH, 1, ownerNode)
    erase group from local hash table
  batch_count++
  if batch_count >= 256: CONTINUEB yield, return
when done:
  send JOIN_AGG_FINAL_REP to all other nodes
  m_cte_redistribution_done = true
  checkCteReady(state)  // transition to CTE_READY if all finals received
```

**c) New handler: `execJOIN_AGG_REDISTRIBUTE_ORD()`**

```
1. Read aggStateKey, keyLen, valueLen from signal
2. getJoinAggState(aggStateKey) → state
3. Read key from section 0, accumulators from section 1
4. Get interpreter: getJoinAggResultInterpreter(state)
5. Look up key in hash table:
   - Found: merge accumulators (type-aware: SUM adds, COUNT adds, MIN takes min, MAX takes max)
   - Not found: allocate new group, copy key + accumulators, insert into hash table
6. No response signal needed
```

**d) New handler: `execJOIN_AGG_FINAL_REP()`**

```
1. Read aggStateKey, senderNodeId from signal
2. getJoinAggState(aggStateKey) → state
3. Set bit in m_cte_nodes_finalized for senderNodeId
4. checkCteReady(state): if all other nodes finalized AND own redistribution done → CTE_READY, send COMPLETE_CONF
```

**e) Helper: `checkCteReady()`**

```
if (!m_cte_redistribution_done) return;  // still sending
for each node in m_cte_node_list where node != ownNode:
  if (!m_cte_nodes_finalized.get(node)) return;  // still waiting
state = CTE_READY;
send JOIN_AGG_COMPLETE_CONF to senderRef;
```

### 4. `DblqhInit.cpp`

Register new signal handlers (both worker init blocks):
```cpp
addRecSignal(GSN_JOIN_AGG_REDISTRIBUTE_ORD, &Dblqh::execJOIN_AGG_REDISTRIBUTE_ORD);
addRecSignal(GSN_JOIN_AGG_FINAL_REP, &Dblqh::execJOIN_AGG_FINAL_REP);
```

### 5. `Dblqh.hpp`

Declare new handlers:
```cpp
void execJOIN_AGG_REDISTRIBUTE_ORD(Signal* signal);
void execJOIN_AGG_FINAL_REP(Signal* signal);
void continueJoinAggRedistribute(Signal*, Uint32 aggStateKey, ...);
void checkCteReady(Signal*, JoinAggregationState* state, Uint32 senderRef, Uint32 senderData, Uint32 requestId);
```

### 6. `JoinAggInterpreter.hpp/.cpp`

Add method to merge a single incoming group (not a full interpreter merge):
```cpp
Int32 mergeOneGroup(const char* key, Uint32 keyLen,
                    const char* accumulators, Uint32 accLen);
```

This either merges with existing group or inserts new. Uses the same accumulator merge logic as `mergeFrom()` but for a single group.

## Accumulator Merge Logic

For each AggResItem at position i:
- If incoming.is_null: skip (null doesn't affect merge)
- If local.is_null: copy incoming value
- Based on operation type stored in the cached agg ops:
  - SUM/COUNT: `local.value += incoming.value`
  - MIN: `local.value = min(local.value, incoming.value)`
  - MAX: `local.value = max(local.value, incoming.value)`

The operation type is determined by the aggregation program's opcodes, already parsed and cached in `m_cached_agg_ops` during Init().

## Verification

### testCteLookup update

Add a new test case (Test 7) that requires a 2-node cluster:
- Setup on both nodes with CTE mode
- Scan both nodes' fragments
- Complete on both nodes → redistribution happens
- Lookup keys that were originally on different nodes → both should work from the correct owner node

### MTR integration

Run the updated testCteLookup on a 2-node cluster config. This needs either:
- A separate MTR test file with a 2-node `.cnf`
- Or the test detects node count and skips multi-node tests on 1-node clusters
