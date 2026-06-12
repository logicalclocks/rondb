# Star Schema Multi-Leaf Aggregation — Implementation Plan

This document describes the concrete code changes needed to implement
multi-leaf fan-out aggregation as designed in `star_schema_plan.md`.
All leaf programs share a single GROUP BY hash map with combined
accumulator layout. One AggInterpreter switches between programs.

## Overview of Changes by Layer

```
NDB API  →  builds multi-leaf program section, multiple setAggregation()
DBTC     →  forwards combined program section opaquely
DblqhProxy → parses multi-program section, allocates LeafProgram array
AggInterpreter → program switching + acc_offset per row
DBSPJ    →  encodes leaf index in aggStateKey upper 8 bits
```

---

## Step 1: JoinAggregationState — Multi-Program Storage

**File**: `storage/ndb/src/kernel/blocks/dblqh/JoinAggregationState.hpp`

### Current fields (lines 100-120):
```cpp
Uint32* m_agg_program;
Uint32  m_agg_program_len;
JoinAggInterpreter* m_agg_interpreter;           // MUTEX_BASED
JoinAggInterpreter** m_per_thread_interpreters;  // MUTEX_FREE
```

### New fields:
```cpp
struct LeafProgram {
  Uint32* m_agg_program;       // lc_ndbd_pool_malloc'd
  Uint32  m_agg_program_len;
  Uint32  m_acc_offset;        // first accumulator index for this leaf
  Uint32  m_n_agg_results;     // number of accumulators for this leaf
};

Uint32       m_num_leaves;         // 1 for single-leaf (backward compat)
LeafProgram* m_leaf_programs;      // lc_ndbd_pool_malloc'd array [m_num_leaves]
Uint32       m_total_agg_results;  // sum of all leaf m_n_agg_results
```

The old `m_agg_program` / `m_agg_program_len` fields are **replaced** by
`m_leaf_programs[0]` for single-leaf queries. The interpreter pointers
(`m_agg_interpreter`, `m_per_thread_interpreters`) remain unchanged —
there is still one interpreter per state (MUTEX_BASED) or one per thread
(MUTEX_FREE). The interpreter is initialized with `m_total_agg_results`
so hash map entries are sized for all leaves' accumulators combined.

### Initialization (after seize):
```cpp
state->m_num_leaves = 0;
state->m_leaf_programs = nullptr;
state->m_total_agg_results = 0;
```

### Helper methods:
```cpp
static Uint32 encodeAggStateKey(Uint32 baseKey, Uint32 leafIndex) {
  return (leafIndex << 24) | (baseKey & 0x00FFFFFF);
}
static Uint32 decodeBaseKey(Uint32 aggStateKey) {
  return aggStateKey & 0x00FFFFFF;
}
static Uint32 decodeLeafIndex(Uint32 aggStateKey) {
  return aggStateKey >> 24;
}
```

---

## Step 2: Section 0 Wire Format — Multi-Program Encoding

**File**: `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp`

### Wire format

Section 0 of JOIN_AGG_SETUP_REQ currently contains a flat aggregation
program. Extend with a numLeaves header:

```
Word 0:  numLeaves
Word 1:  progLen[0]
Word 2 .. 1+progLen[0]:         program[0] words
Word 2+progLen[0]:              progLen[1]
Word 3+progLen[0] .. :          program[1] words
...repeat...
```

For single-leaf backward compatibility: `numLeaves = 1`, followed by
`[progLen, program...]`. The total section size is
`1 + sum(1 + progLen[i])`.

### No JoinAgg.hpp or SCAN_TABREQ changes

`numLeaves` lives exclusively in Section 0 data — **not** in the signal
header. This means:
- `JoinAggSetupReq::SignalLength` stays at 11, no new field
- DBTC does not parse the multi-program format at all — it forwards
  Section 0 opaquely via `dupSection` / `sendSignal` as before
- No SCAN_TABREQ signal changes needed
- DblqhProxy reads `numLeaves` from `buf[0]` after `copy(buf, section0Ptr)`

The only code that parses the multi-program format is DblqhProxy
(receiver) and NdbQueryOperation (sender). DBTC is a pass-through.

### Why single section, not N sections

`SectionHandle` supports at most 3 sections. With N leaf programs +
receiver IDs, N > 2 would exceed the limit. A single length-prefixed
section avoids this constraint entirely. DBTC forwards Section 0
opaquely — it never parses the multi-program format.

---

## Step 3: DblqhProxy — SETUP_REQ Parsing and Allocation

**File**: `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp`

### execJOIN_AGG_SETUP_REQ changes (lines 2305-2541)

After seizing the JoinAggregationState:

```cpp
// 1. Copy Section 0 into temporary buffer
SegmentedSectionPtr progSectionPtr;
ndbrequire(handle.getSection(progSectionPtr,
                             JoinAggSetupReq::AggProgramSectionNum));
Uint32 totalSectionWords = progSectionPtr.sz;
Uint32 *buf = (Uint32 *)lc_ndbd_pool_malloc(
    totalSectionWords * sizeof(Uint32),
    RG_QUERY_MEMORY, getThreadId(), false);
copy(buf, progSectionPtr);

// 2. Read numLeaves from first word of section data
Uint32 numLeaves = buf[0];
if (numLeaves == 0) numLeaves = 1;  // backward compat with old API

// 3. Allocate LeafProgram array
state->m_num_leaves = numLeaves;
state->m_leaf_programs = (LeafProgram *)lc_ndbd_pool_malloc(
    numLeaves * sizeof(LeafProgram),
    RG_QUERY_MEMORY, getThreadId(), false);

// 4. Parse length-prefixed programs
Uint32 pos = 0;
Uint32 sectionNumLeaves = buf[pos++];
ndbrequire(sectionNumLeaves == numLeaves);

Uint32 accOffset = 0;
for (Uint32 i = 0; i < numLeaves; i++) {
  Uint32 progLen = buf[pos++];
  Uint32 *progCopy = (Uint32 *)lc_ndbd_pool_malloc(
      progLen * sizeof(Uint32),
      RG_QUERY_MEMORY, getThreadId(), false);
  memcpy(progCopy, &buf[pos], progLen * sizeof(Uint32));
  pos += progLen;

  state->m_leaf_programs[i].m_agg_program = progCopy;
  state->m_leaf_programs[i].m_agg_program_len = progLen;
  state->m_leaf_programs[i].m_acc_offset = accOffset;
  // m_n_agg_results is read from program header during Init()
  // (set after interpreter initialization below)
}

// 5. Free temporary buffer
lc_ndbd_pool_free(buf);

// 6. Initialize interpreter with leaf 0's program
//    (all leaves share same GROUP BY — leaf 0 establishes the hash map)
//    m_n_agg_results is set to m_total_agg_results after scanning all programs
```

### Interpreter initialization — combined accumulator count

The interpreter must be initialized knowing the **total** accumulator
count across all leaves so hash map entries are sized correctly. But the
current `Init()` reads `m_n_agg_results` from the program header.

**Approach**: Initialize the interpreter with leaf 0's program, then
override `m_n_agg_results` to `m_total_agg_results`. This requires a
new setter or Init parameter:

```cpp
// After Init with leaf 0's program:
interp->Init(state->m_leaf_programs[0].m_agg_program);

// Read each leaf's n_agg_results from its program header
for (Uint32 i = 0; i < numLeaves; i++) {
  Uint32 leafAggResults = readNAggResultsFromProgram(
      state->m_leaf_programs[i].m_agg_program);
  state->m_leaf_programs[i].m_n_agg_results = leafAggResults;
  accOffset += leafAggResults;
}
state->m_total_agg_results = accOffset;

// Override interpreter's accumulator count
interp->setTotalAggResults(state->m_total_agg_results);
```

The `setTotalAggResults()` method overrides `m_n_agg_results` so the
hash map allocates group rows large enough for all leaves' accumulators.

### Concurrency model — unchanged structure

- **MUTEX_BASED**: One shared interpreter, one hash map. All leaf
  programs execute against it (with mutex protection per group).
- **MUTEX_FREE**: Per-thread interpreter, per-thread hash map. All leaf
  programs on a given thread execute against that thread's interpreter.

The interpreter count does not change. Only the program loaded into the
interpreter changes per-row based on the leaf index.

---

## Step 4: AggInterpreter — Program Switching

**File**: `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.hpp`

### New method: switchProgram()

```cpp
/**
 * Switch to a different leaf's aggregation program.
 * The hash map and group row layout remain unchanged.
 * Only the program pointer, accumulator offset, and
 * per-program n_agg_results change.
 *
 * @param program     Pointer to leaf's program words
 * @param prog_len    Length of program
 * @param acc_offset  Accumulator start offset for this leaf
 * @param n_results   Number of accumulators for this leaf
 */
void switchProgram(const Uint32* program, Uint32 prog_len,
                   Uint32 acc_offset, Uint32 n_results);
```

**Implementation**: Store the active program pointer and accumulator
offset. When `processRecWithLinkedAttrs()` or `processRec()` runs the
aggregation bytecode, it uses the current program. Accumulator writes
add `acc_offset` to their slot index.

### New method: setTotalAggResults()

```cpp
/**
 * Override m_n_agg_results for hash map entry sizing.
 * Called during multi-leaf setup after Init() with leaf 0's program.
 * Must be called BEFORE any rows are processed.
 */
void setTotalAggResults(Uint32 total);
```

This sets `m_n_agg_results` to the combined count so `MemAlloc` for
group entries allocates `key_len + total * sizeof(AggResItem)`.

### Accumulator offset in bytecode execution

**File**: `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp`

In the aggregation bytecode interpreter (ProcessRec, around line 1337+),
when the program writes to accumulator slot `i`, it actually writes to
slot `i + m_acc_offset`. This is the key change that makes multiple
leaf programs share the same group row without conflict.

```cpp
// Current (single program):
agg_res_ptr[slot_index] = ...;

// New (multi-leaf):
agg_res_ptr[slot_index + m_current_acc_offset] = ...;
```

`m_current_acc_offset` is set by `switchProgram()` and defaults to 0
for single-leaf queries (backward compatible).

### First-row initialization per accumulator

Currently, when a group row is first created, all accumulators are
initialized. With combined layout, only the current leaf's accumulators
should be initialized on first encounter. Other leaves' accumulators
remain in their default state until those leaves contribute rows.

The default state for each accumulator type is its identity element:
- COUNT: 0
- SUM: NULL (0 when non-null values arrive)
- MIN: NULL (first non-null value becomes MIN)
- MAX: NULL (first non-null value becomes MAX)

This requires that `MemAlloc` for group entries zero-initializes the
accumulator portion (or the program handles first-row detection per
slot rather than per group).

---

## Step 5: DblqhMain — handleJoinAggRow Dispatch

**File**: `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`

### Encoded aggStateKey extraction

In the scan processing path where `handleJoinAggRow` (or equivalent)
is called, the aggStateKey from the LQHKEYREQ/SCAN_FRAGREQ carries the
encoded leaf index:

```cpp
// Current code path (around line 15213):
JoinAggregationState *state =
    getJoinAggState(scanPtr->m_join_agg_state_key);
JoinAggInterpreter *interp = getJoinAggInterpreter(state);

// New code:
Uint32 encodedKey = scanPtr->m_join_agg_state_key;
Uint32 baseKey = JoinAggregationState::decodeBaseKey(encodedKey);
Uint32 leafIndex = JoinAggregationState::decodeLeafIndex(encodedKey);

JoinAggregationState *state = getJoinAggState(baseKey);
ndbrequire(leafIndex < state->m_num_leaves);

JoinAggInterpreter *interp = getJoinAggInterpreter(state);

// Switch interpreter to this leaf's program
LeafProgram &leaf = state->m_leaf_programs[leafIndex];
interp->switchProgram(leaf.m_agg_program, leaf.m_agg_program_len,
                      leaf.m_acc_offset, leaf.m_n_agg_results);
```

For MUTEX_BASED: `switchProgram()` happens under the group mutex, so
program state is thread-safe. The program switch is a pointer swap —
no memory allocation.

For MUTEX_FREE: each thread has its own interpreter, so program
switching has no concurrency concern.

### ScanRecord field

`ScanRecord::m_join_agg_state_key` currently stores the base key. For
multi-leaf, it stores the **encoded** key (base + leaf index). This is
set when DBSPJ sends the SCAN_FRAGREQ with the encoded key.

---

## Step 6: DBSPJ — Leaf Index Encoding in aggStateKey

**File**: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

### TreeNode leaf index assignment

During query tree building, each TreeNode with `T_AGGREGATE_LEAF` gets
a leaf index (0, 1, 2, ...) assigned sequentially:

```cpp
// In build phase, after setupAncestors():
Uint32 leafIdx = 0;
for (each TreeNode in execution order) {
  if (treeNode.m_bits & TreeNode::T_AGGREGATE_LEAF) {
    treeNode.m_agg_leaf_index = leafIdx++;
  }
}
```

**New field in TreeNode** (Dbspj.hpp):
```cpp
Uint32 m_agg_leaf_index;  // 0..255, leaf index for aggStateKey encoding
```

### SCAN_FRAGREQ / LQHKEYREQ aggStateKey encoding

When DBSPJ populates the aggStateKey in a SCAN_FRAGREQ or LQHKEYREQ
for an aggregate leaf operation, it encodes the leaf index:

```cpp
// Current (lines 5120-5143 for lookup, 8395-8411 for scan):
Uint32 aggStateKey = requestPtr.p->m_aggStateKey;  // base key from SETUP_CONF
variableData[var_index + 4] = aggStateKey;

// New:
Uint32 baseKey = requestPtr.p->m_aggStateKey;
Uint32 leafIndex = treeNodePtr.p->m_agg_leaf_index;
Uint32 encodedKey = JoinAggregationState::encodeAggStateKey(baseKey, leafIndex);
variableData[var_index + 4] = encodedKey;
```

### setupAncestors() — multi-leaf ancestor marking

Currently `setupAncestors()` walks from the single aggregate leaf
upward, marking `T_AGGREGATE_ANCESTOR` on each node. For multi-leaf:

```cpp
// Walk from EACH aggregate leaf upward, marking ancestors
for (each TreeNode with T_AGGREGATE_LEAF) {
  TreeNodePtr curr = leafNode;
  while (curr.p->m_parentPtrI != RNIL) {
    curr = getTreeNode(curr.p->m_parentPtrI);
    curr.p->m_bits |= TreeNode::T_AGGREGATE_ANCESTOR;
    // Could also set a bitmask of which leaves this node feeds
  }
}
```

The union of all ancestor paths gets marked. This is correct because
any node that is an ancestor of ANY leaf must participate in the
aggregation flow (forwarding rows to its children on the aggregate
path).

### COMPLETE and RELEASE — use base key

DBSPJ sends JOIN_AGG_COMPLETE_REQ and JOIN_AGG_RELEASE_REQ with the
base key (leaf index 0), since all leaves share the same state:

```cpp
Uint32 baseKey = requestPtr.p->m_aggStateKey;  // unchanged
// Send COMPLETE/RELEASE with baseKey
```

---

## Step 7: DBTC — No Changes Required

**File**: `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`

DBTC requires **zero changes** for multi-leaf aggregation. The multi-
program format is entirely contained in Section 0 data, which DBTC
treats as an opaque blob:

- `parseJoinAggKeyInfo()` extracts the program section from SCAN_TABREQ
  Section 2 into `m_aggProgramPtrI` — unchanged, works for any content
- `sendJoinAggSetupReqs()` duplicates `m_aggProgramPtrI` into Section 0
  of JOIN_AGG_SETUP_REQ — unchanged, forwards the multi-program data
- `JoinAggSetupReq::SignalLength` stays at 11 — no new fields
- No new ScanRecord fields needed

The `numLeaves` value travels inside Section 0 data from the NDB API
to DblqhProxy without DBTC ever reading it.

---

## Step 8: NDB API — Multi-Leaf Query Building

**File**: `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`

### Remove single-leaf validation (lines 2485-2491)

Replace the check that rejects multiple `NI_AGGREGATE_LEAF` nodes:

```cpp
// Current: error if m_hasAggregation is already true when second leaf found
// New: track vector of leaf operation numbers
m_aggregateLeafOpNos.push_back(opNo);
```

### Validate shared GROUP BY

All leaves must use the same GROUP BY columns. At query finalization:

```cpp
if (m_aggregateLeafOpNos.size() > 1) {
  // Verify all leaves have identical GROUP BY column lists
  const NdbAggregator *first =
      m_operations[m_aggregateLeafOpNos[0]]->getAggregator();
  for (Uint32 i = 1; i < m_aggregateLeafOpNos.size(); i++) {
    const NdbAggregator *leaf =
        m_operations[m_aggregateLeafOpNos[i]]->getAggregator();
    if (!sameGroupByColumns(first, leaf)) {
      return QRY_WRONG_OPERATION_TYPE;  // GROUP BY mismatch
    }
  }
}
```

### setAggregation() — per-leaf

Each leaf operation independently calls `setAggregation(program)`.
No changes to the method signature. The NdbQueryBuilder tracks which
operations are aggregate leaves.

---

## Step 9: NDB API — Multi-Program Section Construction

**File**: `storage/ndb/src/ndbapi/NdbQueryOperation.cpp`

### prepareAggregation() changes (lines 3228-3280)

Build the combined program section:

```cpp
int NdbQueryImpl::prepareAggregation() {
  Uint32 numLeaves = m_queryDef->m_aggregateLeafOpNos.size();
  m_aggNumLeaves = numLeaves;

  // Build length-prefixed multi-program section
  m_aggProgram.clear();
  m_aggProgram.append(numLeaves);  // word 0: numLeaves

  for (Uint32 i = 0; i < numLeaves; i++) {
    Uint32 leafOpNo = m_queryDef->m_aggregateLeafOpNos[i];
    const NdbQueryOptionsImpl &opts =
        m_queryDef->getQueryOperation(leafOpNo).getOptions();
    const Uint32 *progBuf = opts.getAggProgramBuffer();
    Uint32 progLen = opts.getAggProgramLen();

    m_aggProgram.append(progLen);           // progLen[i]
    for (Uint32 w = 0; w < progLen; w++) {
      m_aggProgram.append(progBuf[w]);      // program[i] words
    }
  }
  // ... allocate receivers as before ...
}
```

### doSend() — pack into SCAN_TABREQ Section 2

The existing code packs `[boundsLen, bounds, receiverId, aggProgram]`
into Section 2. For multi-leaf, the `aggProgram` portion now contains
the length-prefixed multi-program format. No structural change to the
section layout — it's just a larger program section.

The `numAggLeaves` value must also be communicated to DBTC. Options:
1. Store it in SCAN_TABREQ (add a field or use existing reserved bits)
2. DBTC reads `numLeaves` from the first word of the program section

**Approach 2 is simpler**: DBTC reads `buf[0]` of the extracted
program section to get `numLeaves`, stores it in `m_aggNumLeaves`,
and forwards it in `JoinAggSetupReq::numAggLeaves`. No SCAN_TABREQ
signal changes needed.

---

## Step 10: DblqhProxy — COMPLETE_REQ and RELEASE_REQ

**File**: `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp`

### execJOIN_AGG_COMPLETE_REQ — unchanged merge logic

The merge phase (MUTEX_FREE) iterates per-thread interpreters and
merges group rows. Since all leaves share the same hash map, the merge
operates on complete group rows (with combined accumulators). No change
to merge logic — the group row layout is opaque to the merge function
(it merges by matching GROUP BY keys and combining accumulator values).

The finalize phase sends groups via TRANSID_AI with the full combined
accumulator layout. The API's NdbAggregator processes the combined row.

### execJOIN_AGG_RELEASE_REQ — free all leaf programs

```cpp
// Free each leaf program
for (Uint32 i = 0; i < state->m_num_leaves; i++) {
  lc_ndbd_pool_free(state->m_leaf_programs[i].m_agg_program);
}
// Free the LeafProgram array
lc_ndbd_pool_free(state->m_leaf_programs);

// Free interpreter(s) and hash map — unchanged
// ...
```

---

## Step 11: QueryTree.hpp — Allow Multiple NI_AGGREGATE_LEAF

**File**: `storage/ndb/include/kernel/signaldata/QueryTree.hpp`

Currently `NI_AGGREGATE_LEAF` (0x4000) can be set on exactly one leaf.
Relax this constraint: multiple nodes may have the flag set.

No new flags needed. The DBSPJ tree builder simply allows multiple
nodes with `NI_AGGREGATE_LEAF`. The leaf index is determined by
traversal order (same order as the NDB API's operation list).

---

## Backward Compatibility

All changes are backward compatible with single-leaf queries:

| Aspect | Single-leaf | Multi-leaf |
|--------|-------------|------------|
| `numLeaves` | 1 | N > 1 |
| Section 0 format | `[1, progLen, prog...]` | `[N, len0, prog0..., len1, prog1..., ...]` |
| `m_leaf_programs` | Array of 1 | Array of N |
| Encoded aggStateKey | `(0 << 24) | baseKey == baseKey` | `(leafIdx << 24) | baseKey` |
| `switchProgram()` | Called with leaf 0 (no-op if offset=0) | Called with leaf K's program |
| `m_total_agg_results` | Same as leaf 0's count | Sum of all leaves |
| COMPLETE/RELEASE | Base key | Same base key |

---

## Implementation Order

### Bottom-up: kernel first, test at each layer, API last

```
Steps 1-5:  DBLQH kernel changes
Step 6:     testStarJoinAgg — direct DBLQH via SignalSender
Step 7:     DBTC — no changes (opaque forwarding)
Step 8:     DBSPJ fan-out coordination
Step 9:     QueryTree.hpp multi-leaf flag
Step 10:    testStarJoinAggSpj — DBTC→DBSPJ→DBLQH via SignalSender
Step 11:    NdbQueryBuilder multi-leaf validation
Step 12:    NdbQueryOperation multi-program section
Step 13:    testStarJoinAggNdbApi — NdbQueryBuilder API tests
Step 14:    RonSQL planner extensions
Step 15:    RonSQL integration tests
Step 16:    Comprehensive test suite — all code paths + rejection tests
```

---

### Steps 1-5: DBLQH Kernel Changes

1. **Step 1**: JoinAggregationState — add LeafProgram struct and fields
2. **Step 4**: AggInterpreter — add switchProgram(), setTotalAggResults(),
   acc_offset support in bytecode execution
3. **Step 3**: DblqhProxy SETUP_REQ — parse multi-program section,
   allocate LeafProgram array, initialize interpreter with combined count
4. **Step 10**: DblqhProxy RELEASE_REQ — free leaf program array
5. **Step 5**: DblqhMain — decode encoded aggStateKey, call switchProgram()

---

### Step 6: testStarJoinAgg — Direct DBLQH Tests

**Pattern**: Same as `testJoinAgg.cpp` — uses `SignalSender` to send
signals directly to DBLQH, bypassing DBTC and DBSPJ entirely.

**New file**: `storage/ndb/block_unit_test/testStarJoinAgg.cpp`
**CMake target**: `testStarJoinAgg`
**MTR test**: `mysql-test/suite/ndb_push_agg/t/testStarJoinAgg.test`
**MTR result**: `mysql-test/suite/ndb_push_agg/r/testStarJoinAgg.result`

#### Test Schema

Create three tables via MySQL (same distribution key):

```sql
CREATE TABLE star_entity (
  entity_id INT PRIMARY KEY,
  name VARCHAR(40)
) ENGINE=NDB;

CREATE TABLE star_measurements (
  entity_id INT,
  ts INT,
  value INT,
  PRIMARY KEY(entity_id, ts)
) ENGINE=NDB;

CREATE TABLE star_events (
  entity_id INT,
  ts INT,
  event_type INT,
  PRIMARY KEY(entity_id, ts)
) ENGINE=NDB;
```

Insert test data: 5 entities, 3 measurements each, 2 events each.

#### Signal Flow per Test

Each test follows this sequence:

```
1. Build 2 aggregation programs (hand-crafted Uint32 vectors):
   - Program 0: SUM(value), GROUP BY entity_id
   - Program 1: COUNT(event_type), GROUP BY entity_id

2. Build multi-program Section 0:
   [numLeaves=2, progLen0, prog0..., progLen1, prog1...]

3. Send JOIN_AGG_SETUP_REQ to DBLQH (via SignalSender):
   - numAggLeaves = 2
   - Section 0 = multi-program section
   - Section 1 = receiverId
   → Receive SETUP_CONF with baseStateKey

4. Send SCAN_FRAGREQ for leaf 0 (measurements table):
   - aggStateKey = encodeAggStateKey(baseKey, 0)
   - JoinAggFlag = 1
   → Scan all measurements rows through handleJoinAggRow

5. Send SCAN_FRAGREQ for leaf 1 (events table):
   - aggStateKey = encodeAggStateKey(baseKey, 1)
   - JoinAggFlag = 1
   → Scan all events rows through handleJoinAggRow

6. Send JOIN_AGG_COMPLETE_REQ with baseStateKey:
   → Receive TRANSID_AI with combined results
   → Each group row has [entity_id, SUM(value), COUNT(event_type)]

7. Send JOIN_AGG_RELEASE_REQ with baseStateKey

8. Verify: compare results against expected values computed from
   test data (same pattern as testJoinAgg.cpp verifyResults())
```

#### Test Cases

| Test | Description |
|------|-------------|
| test_2leaf_sum_count | 2 leaves: SUM + COUNT, GROUP BY entity_id, verify combined accumulators |
| test_2leaf_no_groupby | 2 leaves: SUM + COUNT, no GROUP BY (single global result row) |
| test_3leaf_sum_count_max | 3 leaves: SUM + COUNT + MAX, verify acc_offset [0,1,2] |
| test_2leaf_eviction | 2 leaves with ERROR_INSERT 5090 (maxGroups=3), 5 groups → eviction of combined rows |
| test_2leaf_mutex_free | 2 leaves with MUTEX_FREE strategy, verify per-thread merge produces correct combined results |
| test_2leaf_partial_groups | Groups that exist in leaf 0 but not leaf 1 (some entities have measurements but no events) |
| test_2leaf_empty_leaf | One leaf produces 0 rows, other produces rows — verify accumulators default correctly |
| test_single_leaf_compat | Single leaf (numLeaves=1), verify backward compatibility with existing behavior |

#### CMakeLists.txt addition

```cmake
NDB_ADD_EXECUTABLE(testStarJoinAgg
  testStarJoinAgg.cpp
  NDBTEST NDBCLIENT MYSQLCLIENT)
```

#### MTR test file

```
--source include/have_ndb.inc

--exec $NDB_PUSH_AGG_DIR/testStarJoinAgg -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT 2>&1

exit;
```

#### MTR result file

```
PASSED: test_2leaf_sum_count
PASSED: test_2leaf_no_groupby
PASSED: test_3leaf_sum_count_max
PASSED: test_2leaf_eviction
PASSED: test_2leaf_mutex_free
PASSED: test_2leaf_partial_groups
PASSED: test_2leaf_empty_leaf
PASSED: test_single_leaf_compat
```

---

### Steps 7-9: Signal and DBSPJ Changes

7. **Step 7**: DBTC — no changes required (opaque forwarding)
8. **Step 6**: DBSPJ — leaf index assignment, encoded aggStateKey in
   SCAN_FRAGREQ/LQHKEYREQ, multi-leaf setupAncestors()
9. **Step 11**: QueryTree.hpp — allow multiple NI_AGGREGATE_LEAF

---

### Step 10: testStarJoinAggSpj — DBTC→DBSPJ→DBLQH Tests

**Pattern**: Same as `testJoinAggSpj.cpp` — uses `SignalSender` to send
SCAN_TABREQ to DBTC with a hand-built QueryTree. DBTC forwards to DBSPJ,
which expands the tree and sends SCAN_FRAGREQ/LQHKEYREQ to DBLQH.

**New file**: `storage/ndb/block_unit_test/testStarJoinAggSpj.cpp`
**CMake target**: `testStarJoinAggSpj`
**MTR test**: `mysql-test/suite/ndb_push_agg/t/testStarJoinAggSpj.test`
**MTR result**: `mysql-test/suite/ndb_push_agg/r/testStarJoinAggSpj.result`

#### Test Schema

Same three tables as Step 6 (star_entity, star_measurements, star_events).

#### Signal Flow per Test

```
1. TCSEIZEREQ → DBTC → get apiConnectPtr + tcRef

2. Build QueryTree with fan-out topology:
   Node 0: QN_ScanFragNode (star_entity, root scan)
   Node 1: QN_LookupNode (star_measurements, child of node 0,
            NI_AGGREGATE | NI_AGGREGATE_LEAF, leaf index 0)
   Node 2: QN_LookupNode (star_events, child of node 0,
            NI_AGGREGATE | NI_AGGREGATE_LEAF, leaf index 1)

   Each lookup node:
   - Join key: entity_id linked from parent (P_COL pattern)
   - Linked attributes: entity_id from parent for GROUP BY

3. Build multi-program Section 2:
   [boundsLen=0, receiverId, numLeaves=2, progLen0, prog0..., progLen1, prog1...]

4. Send SCAN_TABREQ to DBTC:
   - Section 0: receiver IDs
   - Section 1: QueryTree (3 nodes)
   - Section 2: combined section (bounds + programs)
   - viaSPJFlag = 1, JoinAggFlag = 1
   - scanParallelism = num_fragments

5. Receive interleaved TRANSID_AI + SCAN_TABCONF:
   - TRANSID_AI contains combined aggregation results
   - SCAN_TABCONF with EndOfData signals completion

6. Verify results against expected values
```

#### Key Differences from testJoinAggSpj

- QueryTree has **3 nodes** (root + 2 children) instead of 2 (root + 1 child)
- Both children have `NI_AGGREGATE_LEAF` set
- Section 2 contains multi-program format
- Two linked attribute patterns (one per child) reference parent columns
- DBSPJ must encode leaf index in aggStateKey when dispatching to DBLQH

#### QueryTree Construction Detail

```cpp
// Node 0: Root scan on star_entity
QN_ScanFragNode rootNode;
rootNode.tableId = entityTableId;
rootNode.requestInfo = NI_AGGREGATE;  // participates in aggregation

// Node 1: Lookup on star_measurements (leaf 0)
QN_LookupNode measNode;
measNode.tableId = measTableId;
measNode.requestInfo = NI_AGGREGATE | NI_AGGREGATE_LEAF;
measNode.parentId = 0;  // child of root
// Key pattern: entity_id = P_COL(parent, entity_id_col)
// Linked attrs: entity_id from parent for GROUP BY

// Node 2: Lookup on star_events (leaf 1)
QN_LookupNode evtNode;
evtNode.tableId = evtTableId;
evtNode.requestInfo = NI_AGGREGATE | NI_AGGREGATE_LEAF;
evtNode.parentId = 0;  // also child of root (fan-out)
// Key pattern: entity_id = P_COL(parent, entity_id_col)
// Linked attrs: entity_id from parent for GROUP BY
```

#### Test Cases

| Test | Description |
|------|-------------|
| test_spj_2leaf_lookup | Root scan → 2 lookup leaves, SUM + COUNT, GROUP BY entity_id |
| test_spj_2leaf_scan | Root scan → 2 scan leaves (ordered index on entity_id,ts), range bounds on ts |
| test_spj_2leaf_mixed | Root scan → 1 lookup leaf + 1 scan leaf |
| test_spj_2leaf_empty | One child table has no matching rows for some entity_ids |
| test_spj_2leaf_outer | Root scan → 2 LEFT JOIN leaves (outer join null propagation to both) |
| test_spj_3leaf | Root scan → 3 lookup leaves, SUM + COUNT + MAX |
| test_spj_single_leaf_compat | Single-leaf fan-out (1 child), verify backward compatibility |

#### CMakeLists.txt addition

```cmake
NDB_ADD_EXECUTABLE(testStarJoinAggSpj
  testStarJoinAggSpj.cpp
  NDBTEST NDBCLIENT MYSQLCLIENT)
```

#### MTR integration

Same pattern as testStarJoinAgg: `--exec $NDB_PUSH_AGG_DIR/testStarJoinAggSpj -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT 2>&1`

---

### Steps 11-12: NDB API Changes

11. **Step 8**: NdbQueryBuilder — remove single-leaf validation,
    validate shared GROUP BY
12. **Step 9**: NdbQueryOperation — build multi-program section

---

### Step 13: testStarJoinAggNdbApi — NDB API Level Tests

**Pattern**: Same as `testJoinAggNdbApi.cpp` — uses `NdbQueryBuilder`
to construct pushed join queries with aggregation, executes via
`NdbTransaction`, verifies results against MySQL reference queries.

**New file**: `storage/ndb/block_unit_test/testStarJoinAggNdbApi.cpp`
**CMake target**: `testStarJoinAggNdbApi`
**MTR test**: `mysql-test/suite/ndb_push_agg/t/testStarJoinAggNdbApi.test`
**MTR result**: `mysql-test/suite/ndb_push_agg/r/testStarJoinAggNdbApi.result`

#### Test Schema

Same three tables. Data inserted via NDB API (same pattern as
testJoinAggNdbApi.cpp).

#### Query Construction Pattern

```cpp
// 1. Build aggregation programs for each leaf
NdbAggregator aggMeas(measTab);
aggMeas.GroupByLinked(0, entityTab->getColumn("id"));
aggMeas.LoadColumn("value", 0);
aggMeas.Sum(0, 0);                          // agg[0] = SUM(value)
aggMeas.Finalize();

NdbAggregator aggEvt(evtTab);
aggEvt.GroupByLinked(0, entityTab->getColumn("id"));
aggEvt.LoadColumn("event_type", 0);
aggEvt.Count(0);                            // agg[0] = COUNT(event_type)
aggEvt.Finalize();

// 2. Build query tree with fan-out
NdbQueryBuilder *qb = NdbQueryBuilder::create();
const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(entityTab);

// Leaf 0: measurements
const NdbQueryOperand *measKey[] = {
  qb->linkedValue(rootOp, "entity_id"), nullptr
};
NdbQueryOptions measOpts;
measOpts.setAggregation(aggMeas);
measOpts.addLinkedProjection(qb->linkedValue(rootOp, "entity_id"));
const NdbQueryLookupOperationDef *measOp =
    qb->readTuple(measTab, measKey, &measOpts);

// Leaf 1: events
const NdbQueryOperand *evtKey[] = {
  qb->linkedValue(rootOp, "entity_id"), nullptr
};
NdbQueryOptions evtOpts;
evtOpts.setAggregation(aggEvt);
evtOpts.addLinkedProjection(qb->linkedValue(rootOp, "entity_id"));
const NdbQueryLookupOperationDef *evtOp =
    qb->readTuple(evtTab, evtKey, &evtOpts);

const NdbQueryDef *queryDef = qb->prepare(ndb);

// 3. Execute and verify
NdbTransaction *trans = ndb->startTransaction();
NdbQuery *query = trans->createQuery(queryDef);
trans->execute(NdbTransaction::NoCommit);

while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {}

NdbAggregator *resultAgg = query->getAggregator();
// Iterate combined results: each row has [entity_id, SUM(value), COUNT(event_type)]
```

#### MySQL Verification Pattern

```cpp
void verifyWithMysql(MYSQL *mysql) {
  mysql_query(mysql,
    "SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type) "
    "FROM star_entity e "
    "JOIN star_measurements m ON m.entity_id = e.entity_id "
    "JOIN star_events ev ON ev.entity_id = e.entity_id "
    "GROUP BY e.entity_id ORDER BY e.entity_id");
  // Compare with NDB API results
}
```

#### Test Cases

| Test | Description |
|------|-------------|
| test_api_2leaf_sum_count | 2 leaves via NdbQueryBuilder, SUM + COUNT, GROUP BY entity_id, verify against MySQL |
| test_api_2leaf_multi_agg | 2 leaves, each with 2 aggregates: SUM+MAX on measurements, COUNT+MIN on events |
| test_api_3leaf | 3 leaves (add a third table), verify 3 accumulator groups |
| test_api_2leaf_no_groupby | 2 leaves, no GROUP BY (single global row with all aggregates) |
| test_api_2leaf_large | 2 leaves, 1000 entities × 10 measurements × 5 events, verify correctness at scale |
| test_api_2leaf_eviction | 2 leaves with ERROR_INSERT 5090, verify partial eviction + merge |
| test_api_2leaf_outer | 2 leaves with LEFT JOIN, entities missing in one child table |
| test_api_2leaf_scan_scan | 2 scan leaves (ordered index), range bounds on timestamp |
| test_api_groupby_mismatch | 2 leaves with different GROUP BY columns → expect QRY_WRONG_OPERATION_TYPE error |
| test_api_single_leaf_compat | Single leaf via same code path, verify no regression |

#### CMakeLists.txt addition

```cmake
NDB_ADD_EXECUTABLE(testStarJoinAggNdbApi
  testStarJoinAggNdbApi.cpp
  NDBTEST NDBCLIENT MYSQLCLIENT)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi)
```

(The include for `src/ndbapi` is already present in CMakeLists.txt for
the existing testJoinAggNdbApi target.)

#### MTR integration

Same pattern: `--exec $NDB_PUSH_AGG_DIR/testStarJoinAggNdbApi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT 2>&1`

---

### Step 14: RonSQL Planner Extensions

RonSQL fan-out detection, multi-leaf tree construction, result
interpretation.

---

### Step 15: RonSQL Integration Tests

**Pattern**: RonSQL queries executed through the RDRS REST API or
RonSQL CLI, compared against equivalent MySQL queries for correctness.

**MTR test**: `mysql-test/suite/ronsql/t/ronsql_star_join.test`
**MTR result**: `mysql-test/suite/ronsql/r/ronsql_star_join.result`

#### Test Schema

Same three tables (star_entity, star_measurements, star_events).
Created and populated via MySQL in the MTR test file.

#### Test Cases

```sql
-- Test 1: Basic 2-way star join aggregation
RONSQL SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type)
FROM star_entity e
JOIN star_measurements m ON m.entity_id = e.entity_id
JOIN star_events ev ON ev.entity_id = e.entity_id
GROUP BY e.entity_id
ORDER BY e.entity_id;

-- Test 2: Star join with timestamp range filter
RONSQL SELECT e.name, SUM(m.value)
FROM star_entity e
JOIN star_measurements m ON m.entity_id = e.entity_id
  AND m.ts BETWEEN 100 AND 200
JOIN star_events ev ON ev.entity_id = e.entity_id
  AND ev.ts BETWEEN 100 AND 200
GROUP BY e.name
ORDER BY e.name;

-- Test 3: Star join without GROUP BY (global aggregation)
RONSQL SELECT SUM(m.value), COUNT(ev.event_type)
FROM star_entity e
JOIN star_measurements m ON m.entity_id = e.entity_id
JOIN star_events ev ON ev.entity_id = e.entity_id;

-- Test 4: Star join with LEFT JOIN (outer join)
RONSQL SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type)
FROM star_entity e
LEFT JOIN star_measurements m ON m.entity_id = e.entity_id
LEFT JOIN star_events ev ON ev.entity_id = e.entity_id
GROUP BY e.entity_id
ORDER BY e.entity_id;

-- Test 5: 3-way star join
RONSQL SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type), MAX(d.detail)
FROM star_entity e
JOIN star_measurements m ON m.entity_id = e.entity_id
JOIN star_events ev ON ev.entity_id = e.entity_id
JOIN star_details d ON d.entity_id = e.entity_id
GROUP BY e.entity_id
ORDER BY e.entity_id;

-- Test 6: Star join with multiple aggregates per leaf
RONSQL SELECT e.name, SUM(m.value), MAX(m.value), COUNT(ev.event_type), MIN(ev.ts)
FROM star_entity e
JOIN star_measurements m ON m.entity_id = e.entity_id
JOIN star_events ev ON ev.entity_id = e.entity_id
GROUP BY e.name
ORDER BY e.name;
```

Each RonSQL query is preceded by the equivalent MySQL query to establish
expected results. The MTR test compares outputs.

#### MTR test file structure

```
--source include/have_ndb.inc

# Create tables and insert data
CREATE TABLE star_entity (...) ENGINE=NDB;
CREATE TABLE star_measurements (...) ENGINE=NDB;
CREATE TABLE star_events (...) ENGINE=NDB;
INSERT INTO star_entity VALUES (1, 'alpha'), (2, 'beta'), ...;
INSERT INTO star_measurements VALUES (1, 100, 10), (1, 200, 20), ...;
INSERT INTO star_events VALUES (1, 100, 1), (1, 200, 2), ...;

# MySQL reference
SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type) ...;

# RonSQL pushdown
--exec $RONSQL_CLI "SELECT e.entity_id, SUM(m.value), COUNT(ev.event_type) ..."

# Compare outputs (--diff or inline verification)

# Cleanup
DROP TABLE star_events, star_measurements, star_entity;
```

---

## Files Modified — Summary

### Source files

| File | Change | Step |
|------|--------|------|
| `JoinAggregationState.hpp` | LeafProgram struct, encode/decode helpers | 1 |
| `AggInterpreter.hpp` | switchProgram(), setTotalAggResults() | 2 |
| `AggInterpreter.cpp` | acc_offset in bytecode execution | 2 |
| `DblqhProxy.cpp` | Multi-program parsing, allocation, release | 3, 4 |
| `DblqhMain.cpp` | Encoded key decoding, switchProgram() call | 5 |
| `DbspjMain.cpp` | Leaf index assignment, aggStateKey encoding | 8 |
| `Dbspj.hpp` | m_agg_leaf_index field in TreeNode | 8 |
| `QueryTree.hpp` | Allow multiple NI_AGGREGATE_LEAF | 9 |
| `NdbQueryBuilder.cpp` | Multi-leaf validation, GROUP BY check | 11 |
| `NdbQueryBuilderImpl.hpp` | m_aggregateLeafOpNos vector | 11 |
| `NdbQueryOperation.cpp` | Multi-program section construction | 12 |

### Test files

| File | Type | Step |
|------|------|------|
| `block_unit_test/testStarJoinAgg.cpp` | Direct DBLQH via SignalSender | 6 |
| `block_unit_test/testStarJoinAggSpj.cpp` | DBTC→DBSPJ via SignalSender | 10 |
| `block_unit_test/testStarJoinAggNdbApi.cpp` | NdbQueryBuilder API | 13 |
| `block_unit_test/CMakeLists.txt` | Build targets for all 3 tests | 6, 10, 13 |
| `mysql-test/suite/ndb_push_agg/t/testStarJoinAgg.test` | MTR wrapper | 6 |
| `mysql-test/suite/ndb_push_agg/t/testStarJoinAggSpj.test` | MTR wrapper | 10 |
| `mysql-test/suite/ndb_push_agg/t/testStarJoinAggNdbApi.test` | MTR wrapper | 13 |
| `mysql-test/suite/ndb_push_agg/r/testStarJoinAgg.result` | Expected output | 6 |
| `mysql-test/suite/ndb_push_agg/r/testStarJoinAggSpj.result` | Expected output | 10 |
| `mysql-test/suite/ndb_push_agg/r/testStarJoinAggNdbApi.result` | Expected output | 13 |
| `mysql-test/suite/ronsql/t/ronsql_star_join.test` | RonSQL integration | 15 |
| `mysql-test/suite/ronsql/r/ronsql_star_join.result` | Expected output | 15 |
| `block_unit_test/testStarJoinAggComprehensive.cpp` | Full path coverage + rejection | 16 |
| `mysql-test/suite/ndb_push_agg/t/testStarJoinAggComprehensive.test` | MTR wrapper | 16 |
| `mysql-test/suite/ndb_push_agg/r/testStarJoinAggComprehensive.result` | Expected output | 16 |

---

## Key Risks and Mitigations

### acc_offset correctness in bytecode

The aggregation bytecode uses slot indices (0, 1, 2...) to write to
accumulators. With `acc_offset`, slot 0 in leaf 1's program maps to
physical slot `n0` in the group row. This offset must be applied
consistently in:
- `ProcessRec()` — accumulator writes
- `PrepareAggResIfNeeded()` — result serialization (reads all slots)
- `mergeByBucket()` — MUTEX_FREE merge (reads all slots)
- `evictOneGroup()` — eviction (reads all slots)

For result serialization, merge, and eviction: these operate on the
**full combined row**, so they read slots 0..total-1 without any
offset. Only ProcessRec (bytecode execution) applies the offset.

**Mitigation**: Explicit unit test with 2 leaves, verify accumulator
values at correct offsets in dumped group rows.

### Hash map entry sizing

The hash map allocates entries as `key_len + m_n_agg_results * sizeof(AggResItem)`.
If `setTotalAggResults()` is called after `Init()` but before any rows,
the entry sizing is correct. If called too late, entries may be too
small. **Mitigation**: Assert no rows processed before
`setTotalAggResults()`.

### Eviction of partially-filled groups

A group may have accumulators from leaf 0 but not leaf 1 (leaf 1 hasn't
contributed rows to that group yet). Eviction sends the full row with
leaf 1's accumulators in their default state (null/zero). The API-side
merge correctly handles this because:
- Merging a null accumulator with a non-null one keeps the non-null
- Merging two partial results produces the correct combined result

**Mitigation**: Test eviction with forced small hash map (ERROR_INSERT
5090) where some groups have contributions from only one leaf.

---

## Step 16: Comprehensive Star Schema Test Suite

**Goal**: Exercise all code paths for multi-leaf aggregation and verify
that invalid QueryTree constructions are properly rejected.

**New file**: `storage/ndb/block_unit_test/testStarJoinAggComprehensive.cpp`
**CMake target**: `testStarJoinAggComprehensive`
**MTR test**: `mysql-test/suite/ndb_push_agg/t/testStarJoinAggComprehensive.test`

This test combines DBLQH-level (SignalSender), DBTC/DBSPJ-level
(SignalSender), and NDB API-level tests in a single binary, covering
all code paths and error cases.

### Part A: Code Path Coverage Tests

These tests verify that every code path touched by multi-leaf
aggregation is exercised and produces correct results.

#### A1: Concurrency Strategies

| Test | Path | Description |
|------|------|-------------|
| test_mutex_based_2leaf | DblqhProxy MUTEX_BASED | 2 leaves with shared interpreter, verify program switching under mutex |
| test_mutex_free_2leaf | DblqhProxy MUTEX_FREE | 2 leaves with per-thread interpreters, verify per-thread merge with combined agg_ops |
| test_mutex_free_3leaf | DblqhProxy MUTEX_FREE | 3 leaves, verify mergeFrom combines all 3 leaves' accumulators correctly |

#### A2: Aggregation Types

| Test | Path | Description |
|------|------|-------------|
| test_sum_count | ProcessRec kOpSum + kOpCount | Leaf 0: SUM, Leaf 1: COUNT, verify combined |
| test_max_min | ProcessRec kOpMax + kOpMin | Leaf 0: MAX, Leaf 1: MIN, verify combined |
| test_all_aggs | ProcessRec all ops | Leaf 0: SUM+MAX, Leaf 1: COUNT+MIN, 4 accumulators total |
| test_sum_bigint_double | ProcessRec type variants | Leaf 0: SumBigint, Leaf 1: SumDouble, verify type-specific accumulation |

#### A3: GROUP BY Variants

| Test | Path | Description |
|------|------|-------------|
| test_groupby_local | ProcessRec m_n_gb_cols>0 | GROUP BY on local (non-linked) column, both leaves |
| test_groupby_linked | ProcessRec + initGBTypes | GROUP BY on linked column (0x8000), via DBSPJ with NI_ATTR_LINKED |
| test_no_groupby | ProcessRec m_n_gb_cols==0 | No GROUP BY, single global result row |
| test_multicolumn_groupby | ProcessRec multi-GB | GROUP BY on 2 columns (e.g., entity_id + category) |

#### A4: Eviction and Flow Control

| Test | Path | Description |
|------|------|-------------|
| test_eviction_combined | evictOneGroup | ERROR_INSERT 5090 (maxGroups=3), 5+ groups, verify evicted rows have combined accumulators |
| test_eviction_partial | evictOneGroup | Groups with data from only one leaf are evicted, verify API merge handles null slots |
| test_flow_control | JOIN_AGG_SEND_REQ | Large result set triggers flow control, verify SEND_REQ/SEND_CONF handshake works with combined rows |

#### A5: Outer Join Paths

| Test | Path | Description |
|------|------|-------------|
| test_outer_join_2leaf | handleOuterJoinAggKeyNotFound | 2 LEFT JOIN leaves, some entities missing in one table, verify null propagation updates correct leaf's accumulators |
| test_outer_null_both | processNullExtendedRow | Entity exists in neither child table, verify both leaves get null row |
| test_inner_outer_mix | T_INNER_JOIN flag | Leaf 0: INNER JOIN, Leaf 1: LEFT JOIN, verify mixed join semantics |

#### A6: Encoded Key Paths

| Test | Path | Description |
|------|------|-------------|
| test_encoded_key_lqhkeyreq | DblqhMain LQHKEYREQ | Verify decodeBaseKey/decodeLeafIndex at LQHKEYREQ entry |
| test_encoded_key_scanfragreq | DblqhMain SCAN_FRAGREQ | Verify decoding at SCAN_FRAGREQ entry |
| test_encoded_key_node_fail | DblqhMain node fail check | Verify decoding in node failure abort path |

#### A7: DBSPJ Fan-Out Paths

| Test | Path | Description |
|------|------|-------------|
| test_spj_lookup_2leaf | lookup_send | Root scan → 2 lookup leaves, verify both get encoded aggStateKey |
| test_spj_scan_2leaf | scanFrag_send | Root scan → 2 scan leaves (ordered index), verify encoded key in SCAN_FRAGREQ |
| test_spj_mixed_2leaf | lookup_send + scanFrag_send | Root scan → 1 lookup leaf + 1 scan leaf |
| test_spj_ancestor_marking | validateAggregateFlags | 3-level tree: root → intermediate → 2 leaves, verify T_AGGREGATE_ANCESTOR on all ancestors |

#### A8: Multi-Node Cluster

| Test | Path | Description |
|------|------|-------------|
| test_multi_node_merge | JOIN_AGG_COMPLETE | 2+ data nodes, verify per-node SETUP with same program, independent hash maps, API merges cross-node results |
| test_multi_node_eviction | eviction + merge | Multi-node with eviction on different nodes, verify combined merge |

#### A9: Edge Cases

| Test | Path | Description |
|------|------|-------------|
| test_empty_table | ProcessRec 0 rows | No rows in any child table, verify empty result |
| test_single_row | ProcessRec 1 row | Single row in each child, verify single group with combined accumulators |
| test_single_leaf_compat | all paths | numLeaves=1, verify all code paths degrade to single-leaf behavior |
| test_max_leaves | validateAggregateFlags | 8 aggregate leaves (max reasonable), verify setup and combined accumulators |
| test_large_dataset | ProcessRec + merge | 10000 rows × 2 leaves, verify correctness and no memory issues |

### Part B: Invalid QueryTree Rejection Tests

These tests verify that DBSPJ and NDB API properly reject invalid
multi-leaf query constructions with appropriate error codes.

#### B1: DBSPJ Validation (via SignalSender)

| Test | Expected Error | Description |
|------|---------------|-------------|
| test_reject_different_parents | InvalidAggregateFlags | 2 aggregate leaves with different parents (not fan-out from single node) |
| test_reject_leaf_on_nonleaf | InvalidAggregateFlags | NI_AGGREGATE_LEAF set on a non-leaf node (has children) |
| test_reject_no_aggregate_leaf | InvalidAggregateFlags | NI_AGGREGATE set on all nodes but no NI_AGGREGATE_LEAF |
| test_reject_aggregate_without_ni | InvalidAggregateFlags | NI_AGGREGATE_LEAF set but NI_AGGREGATE not set on some nodes |

Validation is in `Dbspj::validateAggregateFlags()`.

#### B2: NDB API Validation (via NdbQueryBuilder)

| Test | Expected Error | Description |
|------|---------------|-------------|
| test_reject_groupby_mismatch | QRY_WRONG_OPERATION_TYPE | 2 leaves with different GROUP BY columns |
| test_reject_leaf_not_child | QRY_WRONG_OPERATION_TYPE | Aggregate leaf that is not a leaf node (has children in query tree) |
| test_reject_too_many_leaves | QRY_WRONG_OPERATION_TYPE | More than MAX_AGG_LEAVES aggregate leaves |

Validation is in `NdbQueryDefImpl` constructor and `NdbQueryBuilder::prepare()`.

#### B3: Wire Format Validation (via SignalSender)

| Test | Expected Error | Description |
|------|---------------|-------------|
| test_reject_bad_numleaves | SETUP_REF | Section 0 with numLeaves=0 and no program data |
| test_reject_truncated_section | SETUP_REF | Section 0 too short for declared numLeaves |
| test_reject_bad_program_magic | SETUP_REF | Valid numLeaves but corrupt program header (wrong magic) |

Validation is in `DblqhProxy::execJOIN_AGG_SETUP_REQ`.

### Implementation Notes

The test binary uses SignalSender for DBLQH and DBSPJ-level tests,
and NdbQueryBuilder for API-level tests, in a single binary. This
follows the pattern of testMultiOuterJoinAggNdbApi which combines
multiple test levels.

Each test function:
1. Creates a fresh table and inserts test data
2. Runs the multi-leaf aggregation via the appropriate path
3. Verifies results against expected values
4. For rejection tests: verifies the expected error code is returned
5. Cleans up (releases TC, drops table)

All tests print PASS/FAIL per test case. The MTR wrapper runs the
binary and expects "PASSED" as the final line.

### Files

| File | Type | Step |
|------|------|------|
| `block_unit_test/testStarJoinAggComprehensive.cpp` | Combined test | 16 |
| `block_unit_test/CMakeLists.txt` | Build target | 16 |
| `mysql-test/suite/ndb_push_agg/t/testStarJoinAggComprehensive.test` | MTR wrapper | 16 |
| `mysql-test/suite/ndb_push_agg/r/testStarJoinAggComprehensive.result` | Expected output | 16 |
