# Star Schema Pushdown Join Aggregation Plan

## Problem Statement

RonSQL and DBSPJ currently support only **linear chain** join topologies for
pushdown aggregation (root → child1 → child2 → ... → leaf). Star schemas
require **fan-out** topologies where a central table joins to multiple child
tables, or an entity ID ties together multiple tables that share a composite
primary key (entity_id, timestamp).

### Target Use Case

Tables sharing an entity ID as part of their primary key, with optional
timestamp columns:

```sql
CREATE TABLE entity (
  entity_id INT PRIMARY KEY,
  name VARCHAR(100)
) ENGINE=NDB PARTITION BY KEY(entity_id);

CREATE TABLE measurements (
  entity_id INT,
  ts TIMESTAMP,
  value DOUBLE,
  PRIMARY KEY(entity_id, ts)
) ENGINE=NDB PARTITION BY KEY(entity_id);

CREATE TABLE events (
  entity_id INT,
  ts TIMESTAMP,
  event_type INT,
  PRIMARY KEY(entity_id, ts)
) ENGINE=NDB PARTITION BY KEY(entity_id);

-- Fan-out query: entity → measurements + events
SELECT e.name, SUM(m.value), COUNT(ev.event_type)
FROM entity e
JOIN measurements m ON m.entity_id = e.entity_id
JOIN events ev ON ev.entity_id = e.entity_id
WHERE m.ts BETWEEN '2025-01-01' AND '2025-12-31'
  AND ev.ts BETWEEN '2025-01-01' AND '2025-12-31'
GROUP BY e.name;
```

This query has two independent children of the root (entity), each needing
its own aggregation program — a **fan-out aggregation** pattern.

### Why This Matters

Without fan-out support, the query must either:
1. Execute without pushdown (all rows transferred to API node)
2. Split into multiple separate pushdown queries and merge results

Both options lose the main benefit of pushdown: reducing data transfer
between data nodes and API nodes.

---

## Scope

This plan covers **RonSQL integration only**. MySQL handler integration
will be handled as a separate task.

---

## Current State

### What Already Works

| Capability | Status |
|-----------|--------|
| DBSPJ TreeNode fan-out (m_child_nodes list) | Working |
| NI_REPEAT_SCAN_RESULT for star-joined scans | Working |
| Sibling branches (non-aggregation) alongside aggregate path | Working |
| Linear chain aggregation (up to 6+ tables) | Working |
| Linked projections (GROUP BY from ancestor tables) | Working |
| Outer join chains with aggregation | Working |
| Per-node independent aggregation with API merge | Working |

### What's Missing

| Gap | Impact |
|-----|--------|
| Single aggregate leaf constraint | Cannot aggregate across fan-out branches |
| Single aggregation program per JoinAggregationState | Cannot run different programs per leaf |
| Null propagation follows single path | Fan-out unmatched rows need multi-path handling |
| RonSQL planner: leaf-only aggregation columns | Cannot express multi-branch aggregates |

---

## Core Design: Shared State, Independent Programs

The central design principle is that **all aggregate leaves share a single
aggregation state** (one GROUP BY hash map) but each leaf has its **own
aggregation program** that writes to its own accumulator slots within the
shared group rows.

### Group Row Layout

Each group row in the hash map is sized for the **combined** accumulators
of all leaf programs:

```
┌─────────────────┬──────────────────────┬──────────────────────┐
│ GROUP BY key    │ Leaf 0 accumulators  │ Leaf 1 accumulators  │
│ (e.name)        │ SUM(m.value)         │ COUNT(ev.event_type) │
│ variable len    │ AggResItem[n0]       │ AggResItem[n1]       │
└─────────────────┴──────────────────────┴──────────────────────┘
                   ↑ offset 0             ↑ offset n0
```

- `m_n_agg_results = n0 + n1 + ... + nK` (total across all leaf programs)
- Each leaf program knows its accumulator offset within the combined row
- The GROUP BY columns are identical for all leaves (shared key space)
- One hash map, one set of group rows, all leaves operate on the same rows

### Aggregation Key Encoding

A single 32-bit aggStateKey encodes both the shared state reference and
the leaf identity:

```
  31        24 23                          0
 ┌───────────┬─────────────────────────────┐
 │ leaf index │     aggregation state key   │
 │  (8 bits)  │         (24 bits)           │
 └───────────┴─────────────────────────────┘
```

- **Lower 24 bits**: The aggregation state key, referencing the shared
  JoinAggregationState (hash map, group rows, all programs). This is the
  same value for all leaves in the same query.
- **Upper 8 bits**: The leaf index (0..255), selecting which aggregation
  program to execute for rows arriving from this leaf.

Extraction:
```cpp
Uint32 stateKey  = aggStateKey & 0x00FFFFFF;
Uint32 leafIndex = aggStateKey >> 24;
```

This encoding means DBTC and DBSPJ never need to manage multiple
aggStateKeys per query — they work with a single base key and OR in the
leaf index when populating each leaf's SCAN_FRAGREQ or LQHKEYREQ. The
DblqhProxy uses the lower 24 bits to find the JoinAggregationState and
the upper 8 bits to select the program.

A join can have at most 64 tables, so 256 leaf indexes is more than
sufficient.

```
aggStateKey = (leafIndex << 24) | baseStateKey

leaf 0: (0x00 << 24) | baseKey ──┐
                                  ├──→ JoinAggregationState (shared hash map)
leaf 1: (0x01 << 24) | baseKey ──┘     ├── program[0]: SUM(m.value)        → acc offset 0
                                        └── program[1]: COUNT(ev.event_type) → acc offset n0
```

JOIN_AGG_COMPLETE_REQ and JOIN_AGG_RELEASE_REQ receive the base state key
(leaf index 0 or any leaf — the lower 24 bits are the same). They operate
on the entire shared state, handling all leaf programs in one request.

### Concurrency Model (Unchanged)

The existing concurrency strategies apply to the shared hash map:

- **MUTEX_BASED**: One shared hash map, mutex protects per-group access.
  All leaf programs from all threads write to the same hash map.
- **MUTEX_FREE**: Per-thread hash map instances, each containing the
  combined accumulator layout. All leaf programs within one thread
  share that thread's hash map. Merge at completion as before.

### Result Flow (Minimal Change)

Because all accumulators live in the same group rows, JOIN_AGG_COMPLETE
produces a **single result stream** with combined accumulators:

```
TRANSID_AI result row:
  [AGG_RESULT header]
  [n_gb_cols | n_agg_results (combined total)]
  [group key data]
  [leaf 0 accumulators | leaf 1 accumulators | ...]
```

The NDB API receives this as a **single NdbAggregator** result set. No
cross-branch merging needed at the API level. This is why the design
has **very little impact on NDB API result handling** — only query tree
setup differs.

### Signal Protocol

```
DBTC ──── JOIN_AGG_SETUP_REQ (N programs, shared state)
           → DblqhProxy allocates baseStateKey, creates shared hash map
           → stores N programs with accumulator offsets

DBSPJ ──── SCAN_FRAGREQ (leaf 0, key = 0x00|baseKey) ──→ LDM → program[0]
       └── SCAN_FRAGREQ (leaf 1, key = 0x01|baseKey) ──→ LDM → program[1]
                                  │
           handleJoinAggRow:      │
             stateKey  = key & 0x00FFFFFF  → find JoinAggregationState
             leafIndex = key >> 24         → select program[leafIndex]

DBSPJ ──── JOIN_AGG_COMPLETE_REQ (baseStateKey)
           → merge (MUTEX_FREE) → finalize → single TRANSID_AI stream

DBSPJ ──── JOIN_AGG_RELEASE_REQ (baseStateKey)
           → releases shared state (hash map + all programs)
```

---

## Implementation Plan

### Phase 1: Multi-Program JoinAggregationState (DBLQH)

**Goal**: Extend JoinAggregationState to hold multiple aggregation programs
that share a single GROUP BY hash map with a combined accumulator layout.

#### 1a. JoinAggregationState Storage

Replace the single program pointer with a dynamically allocated array:

```cpp
// Current (single program):
Uint32* m_agg_program;
Uint32  m_agg_program_len;

// New (multi-program):
struct LeafProgram {
  Uint32* m_agg_program;       // lc_ndbd_pool_malloc'd per leaf
  Uint32  m_agg_program_len;
  Uint32  m_acc_offset;        // offset into combined accumulator array
  Uint32  m_n_agg_results;     // this leaf's accumulator count
};
Uint32       m_num_leaves;
LeafProgram* m_leaf_programs;  // lc_ndbd_pool_malloc'd array
Uint32       m_total_agg_results;  // sum of all leaf n_agg_results
```

**Memory allocation** in `execJOIN_AGG_SETUP_REQ`:
```cpp
state->m_num_leaves = numLeaves;
state->m_leaf_programs = (LeafProgram *)lc_ndbd_pool_malloc(
    numLeaves * sizeof(LeafProgram),
    RG_QUERY_MEMORY, getThreadId(), false);

Uint32 accOffset = 0;
for (Uint32 i = 0; i < numLeaves; i++) {
  Uint32 progLen = /* read from section */;
  state->m_leaf_programs[i].m_agg_program =
      (Uint32 *)lc_ndbd_pool_malloc(
          progLen * sizeof(Uint32),
          RG_QUERY_MEMORY, getThreadId(), false);
  state->m_leaf_programs[i].m_agg_program_len = progLen;
  state->m_leaf_programs[i].m_acc_offset = accOffset;
  state->m_leaf_programs[i].m_n_agg_results = /* from program header */;
  accOffset += state->m_leaf_programs[i].m_n_agg_results;
}
state->m_total_agg_results = accOffset;
```

**Deallocation** in `execJOIN_AGG_RELEASE_REQ`:
```cpp
for (Uint32 i = 0; i < state->m_num_leaves; i++) {
  lc_ndbd_pool_free(state->m_leaf_programs[i].m_agg_program);
}
lc_ndbd_pool_free(state->m_leaf_programs);
```

#### 1b. Section 0 Wire Format for Multiple Programs

Currently Section 0 of JOIN_AGG_SETUP_REQ carries a single aggregation
program as a flat array of Uint32. Extend it with a header:

```
Section 0 layout (multi-leaf):
┌──────────────────────────────────────────────────┐
│ Word 0: numLeaves                                │
├──────────────────────────────────────────────────┤
│ Word 1: progLen[0]  (leaf 0 program length)      │
│ Words 2..1+progLen[0]: program[0] words          │
├──────────────────────────────────────────────────┤
│ Word N: progLen[1]  (leaf 1 program length)      │
│ Words N+1..N+progLen[1]: program[1] words        │
├──────────────────────────────────────────────────┤
│ ... repeat for each leaf ...                     │
└──────────────────────────────────────────────────┘
```

**Backward compatibility**: For single-leaf queries, `numLeaves = 1`
and the format degrades to `[1, progLen, program...]`. The receiver
checks `numLeaves`: if the existing code encounters a section without
the header (old API), it treats the entire section as a single program
(leaf 0). Version-gated via `ndbd_support_xxx()` if needed.

**API-side construction** (NdbQueryOperation.cpp): When building
SCAN_TABREQ Section 2, concatenate all leaf programs with the length-
prefixed format above. The total section size is:
`1 + sum(1 + progLen[i]) for each leaf`.

**DBTC forwarding** (DbtcMain.cpp): `parseJoinAggKeyInfo()` extracts
the combined program section and stores it in `m_aggProgramPtrI` as
before (single section, variable length). `sendJoinAggSetupReqs()`
duplicates this section into Section 0 of JOIN_AGG_SETUP_REQ unchanged.
DBTC does not need to parse the multi-program format — it just forwards
the section opaquely to DblqhProxy.

**DblqhProxy parsing** (DblqhProxy.cpp): In `execJOIN_AGG_SETUP_REQ`,
after `copy(buf, section0Ptr)`:
```cpp
Uint32 pos = 0;
Uint32 numLeaves = buf[pos++];
for (Uint32 i = 0; i < numLeaves; i++) {
  Uint32 progLen = buf[pos++];
  // Allocate and copy program[i] from buf[pos..pos+progLen-1]
  memcpy(state->m_leaf_programs[i].m_agg_program, &buf[pos],
         progLen * sizeof(Uint32));
  pos += progLen;
}
```

The section is first copied into a temporary buffer via the existing
`copy()` function (handles segmented sections), then parsed sequentially.
Each program is allocated individually with `lc_ndbd_pool_malloc`.

#### 1c. AggInterpreter Program Switching

Single AggInterpreter per state (MUTEX_BASED) or per thread (MUTEX_FREE),
initialized with `m_total_agg_results` so hash map entries are sized for
the full combined accumulator space.

When a row arrives, the interpreter switches to the appropriate leaf
program based on the leaf index. Program switching is a pointer swap —
the hash map and group row storage are unchanged. Each leaf's program
writes only to its own accumulator offset range
`[m_acc_offset .. m_acc_offset + m_n_agg_results)`.

#### 1d. Encoded aggStateKey Dispatch

`handleJoinAggRow` extracts the leaf index and state key:
```cpp
Uint32 stateKey  = aggStateKey & 0x00FFFFFF;
Uint32 leafIndex = aggStateKey >> 24;
JoinAggregationState *state = getJoinAggState(stateKey);
LeafProgram &leaf = state->m_leaf_programs[leafIndex];
// switch interpreter to leaf.m_agg_program, leaf.m_acc_offset
```
DblqhProxy allocates a single base state key during setup. No mapping
table needed — the leaf index is always in the upper 8 bits of the
key carried in each LQHKEYREQ/SCAN_FRAGREQ.

#### 1e. Completion, Release, and Eviction

- **JOIN_AGG_COMPLETE_REQ** — receives the base state key (lower 24 bits).
  Merge and finalize operate on the single shared hash map as before.
  The result rows contain the full combined accumulators from all leaves.

- **JOIN_AGG_RELEASE_REQ** — receives the base state key. Frees each
  leaf program via `lc_ndbd_pool_free`, then frees the `m_leaf_programs`
  array, then the hash map and interpreter memory.

- **Eviction** — unchanged. When the hash map is full, a complete group
  row (with combined accumulators from all leaves) is evicted. The API
  merges evicted partial groups as before.

**Files**:
- `JoinAggregationState.hpp` — LeafProgram struct, dynamic array pointer
- `AggInterpreter.hpp/.cpp` — program switching, acc_offset per leaf
- `DblqhProxy.cpp` — multi-program parsing, lc_ndbd_pool_malloc allocation
- `DblqhMain.cpp` — handleJoinAggRow extracts leafIndex from upper bits
- `JoinAgg.hpp` — numLeaves field in signal header (or document section format)

### Phase 2: DBSPJ Fan-Out Aggregation Coordination

**Goal**: Allow multiple leaf nodes in the query tree to be marked as
aggregate leaves, each assigned its own aggStateKey, and coordinate
null propagation across fan-out branches.

**Changes**:

1. **NI_AGGREGATE_LEAF on multiple nodes** — relax the single-leaf
   constraint in QueryTree.hpp. Each leaf gets a unique leaf index
   (0, 1, 2, ...) that maps to its aggStateKey.

2. **setupAncestors()** — iterate all aggregate leaves and mark
   `T_AGGREGATE_ANCESTOR` on each leaf's ancestor path (union of all
   paths). Nodes that are ancestors of multiple leaves get a bitmask
   indicating which leaves they feed.

3. **aggStateKey construction** — DBTC/DBSPJ receives a single base
   state key from JOIN_AGG_SETUP_CONF. For each leaf's SCAN_FRAGREQ or
   LQHKEYREQ, it encodes: `aggStateKey = (leafIndex << 24) | baseKey`.
   No need to manage multiple keys — just OR in the leaf index.

4. **Null propagation for outer joins** — when an intermediate node has
   no match in a LEFT JOIN:
   - Identify which aggregate leaves are descendants of that node
   - Send null rows only to those leaves (using their aggStateKeys)
   - The shared hash map receives the null row through the correct
     leaf program, which updates only that leaf's accumulator slots
     (e.g., COUNT stays unchanged, SUM adds nothing)

5. **Completion and release** — DBSPJ sends JOIN_AGG_COMPLETE_REQ and
   JOIN_AGG_RELEASE_REQ with the base state key (leaf index 0). Since
   all leaves share the same state, one request handles everything.

**Files**:
- `DbspjMain.cpp` — ancestor marking, encoded aggStateKey construction
- `Dbspj.hpp` — per-TreeNode aggregate leaf descendant bitmask
- `QueryTree.hpp` — allow multiple NI_AGGREGATE_LEAF flags

### Phase 3: NDB API Query Tree Setup

**Goal**: NdbQueryBuilder accepts multiple aggregate leaf operations
in a single query definition.

**Changes**:

1. **Remove single-leaf validation** — in `NdbQueryDefImpl` constructor,
   remove the check that rejects a second `NI_AGGREGATE_LEAF`. Track
   `Vector<Uint32> m_aggregateLeafOpNos` instead of single
   `m_aggregateLeafOpNo`.

2. **Combined aggregation program** — when building the aggregation
   program for JOIN_AGG_SETUP_REQ, concatenate per-leaf programs with
   offset metadata so the data node can initialize the combined layout.

3. **setAggregation() per leaf** — each leaf operation calls
   `setAggregation(program)` independently. The NdbQueryBuilder
   validates that all leaves share the same GROUP BY columns.

4. **Result handling (minimal change)** — the NdbAggregator receives
   the combined result rows as before (single stream). The only
   difference is that the accumulator count is larger (sum of all
   leaves). The caller (RonSQL) knows the accumulator layout and
   extracts each leaf's results by offset.

**Files**:
- `NdbQueryBuilder.cpp` — multi-leaf validation, combined program
- `NdbQueryOperation.cpp` — per-leaf setAggregation
- `NdbAggregator.hpp` — no structural changes needed

### Phase 4: Scan-Scan with Timestamp Bounds

**Goal**: Support range scans on (entity_id, timestamp) ordered indexes
as child operations in the fan-out tree.

**Changes**:

1. **RonSQL planner** — when a child table has composite PK (entity_id, ts)
   and the WHERE clause constrains ts, generate an ordered index scan:
   - Equality bound on entity_id (linked from parent)
   - Range bounds on ts (from WHERE clause constants)

2. **DBSPJ** — already supports scan-scan pattern. Verify it works
   correctly with aggregation on scan children in the multi-leaf context.

3. **Scan filter** — push timestamp predicates as NDB interpreter scan
   bounds (existing mechanism).

**This may already work** for single-branch aggregation. The main effort
is testing it in the multi-leaf fan-out context from Phases 1-3.

**Files**:
- RonSQL join planner
- Test cases with (entity_id, ts) composite keys

### Phase 5: RonSQL Query Planner Extensions

**Goal**: RonSQL generates multi-leaf aggregate query trees for star schemas.

**Changes**:

1. **Fan-out detection** — when multiple tables join to the same parent
   on the same key, recognize this as a star schema pattern suitable
   for multi-leaf pushdown.

2. **Multi-leaf aggregate assignment** — allow aggregation functions to
   reference columns from different leaf tables. Each leaf's aggregation
   program is built independently with its own accumulator slots.

3. **GROUP BY validation** — GROUP BY columns must come from shared
   ancestors (typically the root table). This is the same rule as today,
   applied across all branches. All leaves share the same GROUP BY key.

4. **Query tree construction** — build NdbQueryDef with multiple children
   of the root, each marked as aggregate leaf with its own aggregation
   program. The combined accumulator layout is communicated to the API.

5. **Result interpretation** — RonSQL knows the per-leaf accumulator
   offsets and extracts results from the combined group rows. No
   cross-branch merging needed since the hash map already combined them.

**SQL decomposition example**:
```sql
-- Input:
SELECT e.name, SUM(m.value), COUNT(ev.event_type)
FROM entity e
JOIN measurements m ON m.entity_id = e.entity_id
JOIN events ev ON ev.entity_id = e.entity_id
GROUP BY e.name;

-- Query tree:
--   entity (scan, root)
--     ├── measurements (scan, agg leaf 0: SUM(value) → acc[0])
--     └── events (scan, agg leaf 1: COUNT(event_type) → acc[1])
-- GROUP BY: e.name (linked projection to both leaves)
-- Combined accumulators: [SUM(m.value), COUNT(ev.event_type)]
```

**Files**:
- RonSQL join planner
- RonSQL aggregate pushdown logic

### Phase 6: Distribution-Aware Optimization (Future)

**Goal**: When GROUP BY key is a superset of the distribution key, skip
cross-node merging.

When all tables share the same distribution key (entity_id) and GROUP BY
includes entity_id, aggregation results from each node are complete.
The API can concatenate results rather than merge.

This is a **pure optimization** — correctness doesn't depend on it.
Deferred to a future task.

---

## Key Design Details

### Accumulator Offset Assignment

At query build time, the NDB API assigns accumulator offsets:

```
Leaf 0 (measurements): n_agg_results = 1 (SUM), acc_offset = 0
Leaf 1 (events):       n_agg_results = 1 (COUNT), acc_offset = 1
Combined:              m_total_agg_results = 2
```

For a more complex query:
```sql
SELECT e.name, SUM(m.value), AVG(m.value), COUNT(ev.event_type), MAX(ev.ts)
```
```
Leaf 0: n_agg_results = 2 (SUM, AVG), acc_offset = 0
Leaf 1: n_agg_results = 2 (COUNT, MAX), acc_offset = 2
Combined: m_total_agg_results = 4
```

### Uninitialized Accumulator Slots

When a group row is first created by a row from leaf 0, only leaf 0's
accumulator slots are initialized. Leaf 1's slots remain in their default
state (zero/null). This is correct because:
- COUNT default is 0 (add nothing)
- SUM/MIN/MAX default is NULL (identity element)
- When leaf 1 eventually contributes a row to this group, it initializes
  its slots through the normal aggregation path

The AggInterpreter already handles first-row initialization per
accumulator slot. With combined layout, each leaf's program initializes
only its own slots on first encounter, leaving other leaves' slots
untouched.

### Eviction with Combined Accumulators

When the hash map reaches capacity, a complete group row is evicted with
ALL accumulators (from all leaves). The API-side merge logic handles this
correctly because:
- Evicted rows have the same format as completion rows
- The NdbAggregator merges by GROUP BY key, combining partial accumulators
- This works regardless of how many leaf programs contributed

### Interaction with Existing Single-Leaf Queries

The multi-leaf design is backward compatible:
- Single-leaf queries produce `m_num_leaves = 1`, `m_total_agg_results = n0`
- The aggStateKey has leaf index 0 in the upper bits (same as `baseKey`)
- JOIN_AGG_SETUP_REQ carries one program descriptor
- All existing behavior is preserved — `(0x00 << 24) | baseKey == baseKey`

---

## Testing Strategy

### Unit Tests (block_unit_test)

| Test | Description |
|------|-------------|
| testStarJoinAgg | Two-branch fan-out: root → leaf0 (SUM) + leaf1 (COUNT), shared hash map |
| testStarJoinAgg3Way | Three-branch: root → leaf0 + leaf1 + leaf2, combined accumulators |
| testStarJoinAggTimestamp | Fan-out with (entity_id, ts) composite keys, range scans on ts |
| testStarJoinAggOuter | Fan-out with LEFT JOIN, null propagation to multiple leaves |
| testStarJoinAggEviction | Forced eviction (ERROR_INSERT 5090), verify combined row eviction |
| testStarJoinAggMutexFree | MUTEX_FREE concurrency with multi-leaf, verify merge correctness |

### NDB API Tests

| Test | Description |
|------|-------------|
| testStarJoinAggNdbApi | NdbQueryBuilder with 2 aggregate leaves, verify combined results |
| testStarJoinAggGroupBy | Multi-column GROUP BY from root, fan-out to 2 leaves |
| testStarJoinAggLargeDataset | Many groups, verify eviction + merge produces correct totals |

### RonSQL Integration Tests

- Star schema queries through RonSQL, compare with non-pushdown execution
- Queries with timestamp range filters on fan-out branches
- Edge cases: empty branches (one leaf matches no rows), single-row groups

---

## Risk Analysis

| Risk | Mitigation |
|------|-----------|
| Combined accumulator row too wide | Limit MAX_LEAVES; accumulators are 32 bytes each, even 10 leaves × 5 aggs = 1.6 KB per group — manageable |
| Uninitialized slots cause wrong results | AggResItem default state (null/zero) is the identity for all aggregate functions; verify in tests |
| Eviction of partially-filled rows | API merge already handles partial groups; combined layout is transparent |
| Program switching overhead in hot path | Pointer swap only — no hash map restructuring; benchmark to confirm |
| MUTEX_FREE merge with combined layout | Merge operates on full group rows as before; combined layout doesn't change merge logic |

---

## Phase Summary

| Phase | Scope | Depends On | Complexity |
|-------|-------|-----------|------------|
| 1 | Multi-program JoinAggregationState (DBLQH) | — | High |
| 2 | DBSPJ fan-out aggregation coordination | Phase 1 | Medium-High |
| 3 | NDB API query tree setup (multi-leaf) | Phase 2 | Low-Medium |
| 4 | Scan-scan with timestamp bounds | Phase 1 | Low |
| 5 | RonSQL planner extensions | Phase 3 | Medium |
| 6 | Distribution-aware optimization | Phase 5 | Medium (future) |

Phases 1-2 are the kernel infrastructure. Phase 3 is the API surface.
Phase 4 can proceed in parallel with Phase 2. Phase 5 is the RonSQL
integration that makes this user-visible. Phase 6 is a future optimization.
