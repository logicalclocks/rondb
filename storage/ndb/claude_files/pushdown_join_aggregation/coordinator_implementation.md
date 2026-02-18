# Phase 7 DBSPJ Implementation Notes (Complete)

## Overview

**STATUS: ALL SECTIONS IMPLEMENTED AND TESTED**

DBTC passes aggStateKeys to DBSPJ as an extra section of SCAN_FRAGREQ
(not a separate signal). When the JoinAgg flag is set in SCAN_FRAGREQ,
DBTC appends a section containing (nodeId, aggStateKey) pairs. DBSPJ
extracts this section during execSCAN_FRAGREQ and stores the keys in
the Request struct. For each child lookup to an aggregate leaf node,
DBSPJ sets JoinAggFlag in LQHKEYREQ and appends the target node's
aggStateKey to the variable data. DBSPJ also suppresses
T_EXPECT_TRANSID_AI for aggregate leaf nodes so it doesn't wait for
per-row results that will never arrive.

This approach eliminates the need for a separate GSN_JOIN_AGG_STATE_KEYS
signal, avoids signal ordering concerns, and removes the hash lookup
problem (no need for transId in a separate signal).

### Linked Attribute Table Metadata (Recent Addition)

DBSPJ now conditionally prepends `(tableId, tableVersion)` to linked
attribute entries when expanding `m_attrParamPattern` patterns. This
enables type-aware column resolution at the aggregate leaf. Key constraint:
this metadata is only added for attribute parameter patterns, NOT for key
patterns (which would corrupt bounds data).

### Position-Based Linked Column Resolution

The aggregation program references linked columns by position index
(0, 1, 2...) rather than by column ID. The NdbAggregator `GroupBy()`
and `LoadColumn()` calls set bit 15 (0x8000) to indicate a linked column,
with bits 0-14 giving the position in the linked attribute buffer. The
AggInterpreter resolves these by scanning the AttributeHeader chain in
the linked data.

---

## 1. Dbspj.hpp: Request Struct Changes

### File: `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`

### Replace fixed arrays with pointers (line 1260)

Replace:
```cpp
Uint16 m_lookup_node_data[ABS_MAX_NDB_NODES];
```

With:
```cpp
Uint16 *m_lookup_node_data;   // Dynamically allocated [MAX_NDB_NODES]
Uint32 *m_aggStateKeys;       // Dynamically allocated [MAX_NDB_NODES]
NdbNodeBitmask m_aggNodes;
```

### Why dynamic allocation?

The fixed array `m_lookup_node_data[ABS_MAX_NDB_NODES]` (Uint16[145])
consumes 290 bytes in every Request. Adding `m_aggStateKeys[145]`
(Uint32[145] = 580 bytes) would push the total to 870+ bytes. With
alignment, this exceeds 1 KB per Request.

Request is allocated from an ArenaPool. ArenaPool allocates fixed-size
objects from contiguous arena segments. Shrinking each Request by >1 KB
lets many more Requests fit per arena segment, increasing the maximum
number of concurrent SCAN_FRAGREQs the system can handle.

The dynamic memory is allocated separately via `lc_ndbd_pool_malloc`
from `RG_QUERY_MEMORY` and freed explicitly during Request cleanup.

### Allocation: combined single block

Both arrays are allocated as a single block to minimize overhead
(one alloc/free call, 16-byte alignment rounding done once).

**Use `MAX_NDB_NODES` (runtime), not `ABS_MAX_NDB_NODES` (145):**
Data nodes can only be added at startup, so no node ID can exceed
`MAX_NDB_NODES - 1`. The runtime value is typically much smaller
(e.g., 48 for a standard cluster), saving further memory.

```cpp
// In do_init():
size_t alloc_size = MAX_NDB_NODES * sizeof(Uint32) +
                    MAX_NDB_NODES * sizeof(Uint16);
void *mem = lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                                getThreadId(), true);  // clear_flag=true
ndbrequire(mem != nullptr);
requestP->m_aggStateKeys = static_cast<Uint32 *>(mem);
requestP->m_lookup_node_data =
    reinterpret_cast<Uint16 *>(
        static_cast<char *>(mem) + MAX_NDB_NODES * sizeof(Uint32));
```

`clear_flag=true` zeroes both arrays, replacing the existing `memset`
of m_lookup_node_data and satisfying m_aggStateKeys' requirement
(though m_aggStateKeys entries are only read when m_aggNodes is set).

Uint32 array is placed first (naturally aligned from the 16-byte
aligned malloc), Uint16 array second (any alignment is fine).

### Deallocation: in cleanup()

In `cleanup_request()` or wherever Request resources are released
before the ArenaPool reclaims the memory:

```cpp
if (requestP->m_lookup_node_data != nullptr) {
    // m_aggStateKeys is part of the same allocation block —
    // lc_ndbd_pool_free only needs the base pointer.
    // If m_aggStateKeys is first (recommended layout), free that.
    lc_ndbd_pool_free(requestP->m_aggStateKeys);
    requestP->m_lookup_node_data = nullptr;
    requestP->m_aggStateKeys = nullptr;
}
```

**Important:** ArenaPool does not call destructors. The dynamic memory
must be freed explicitly before the arena segment is released. This
follows the same pattern as AggInterpreter (allocated via
`lc_ndbd_pool_malloc`, freed via `AggInterpreter::Destruct` before
pool release).

No new method declarations needed in Dbspj class (no separate signal
handler — extraction happens inline in execSCAN_FRAGREQ).

---

## 2. DbspjMain.cpp: Request Initialization and Cleanup

### File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

### In do_init() (line 1402): replace memset + add allocation

Replace the existing memset of m_lookup_node_data (lines 1421-1422):
```cpp
std::memset(requestP->m_lookup_node_data, 0,
            sizeof(requestP->m_lookup_node_data));
```

With dynamic allocation (as described in section 1):
```cpp
size_t alloc_size = MAX_NDB_NODES * sizeof(Uint32) +
                    MAX_NDB_NODES * sizeof(Uint16);
void *mem = lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                                getThreadId(), true);
ndbrequire(mem != nullptr);
requestP->m_aggStateKeys = static_cast<Uint32 *>(mem);
requestP->m_lookup_node_data =
    reinterpret_cast<Uint16 *>(
        static_cast<char *>(mem) + MAX_NDB_NODES * sizeof(Uint32));
requestP->m_aggNodes.clear();
```

The `clear_flag=true` zeroes the entire block, initializing both arrays.

**Note:** There are two do_init() functions — one for scan requests
(line 1402) and one for lookup requests (line 1105). Both must be
updated. The lookup version at line 1105 also does
`memset(requestP->m_lookup_node_data, 0, ...)`.

### Cleanup: free dynamic memory before pool release

In `cleanup_request()` (or equivalent Request teardown), before the
ArenaPool reclaims the Request:

```cpp
if (requestP->m_aggStateKeys != nullptr) {
    lc_ndbd_pool_free(requestP->m_aggStateKeys);
    requestP->m_aggStateKeys = nullptr;
    requestP->m_lookup_node_data = nullptr;
}
```

This must run for every Request, not just join-agg ones, because
m_lookup_node_data is always dynamically allocated.

---

## 3. DBTC: Build and Attach aggStateKeys Section

### Build section in execJOIN_AGG_SETUP_CONF (coordinator_research.md Sub-Phase 7B)

When all SETUP_CONFs have arrived (`m_aggNodesOutstanding == 0`), DBTC
builds a section containing the (nodeId, aggStateKey) pairs and stores
it in `scanptr.p->m_aggKeysSectionPtrI`. This happens once, before
calling sendFragScansLab.

```cpp
// In execJOIN_AGG_SETUP_CONF, when m_aggNodesOutstanding == 0:
{
    // Build aggStateKeys section: [nodeId1, key1, nodeId2, key2, ...]
    Uint32 keyData[MAX_NDB_NODES * 2];
    Uint32 idx = 0;
    NdbNodeBitmask nodes = scanptr.p->m_aggNodes;
    for (Uint32 nid = nodes.find_first();
         nid != NdbNodeBitmask::NotFound;
         nid = nodes.find_next(nid + 1)) {
        keyData[idx++] = nid;
        keyData[idx++] = scanptr.p->m_aggStateKeys[nid];
    }
    scanptr.p->m_aggKeysSectionPtrI = RNIL;
    ndbrequire(appendToSection(scanptr.p->m_aggKeysSectionPtrI,
                               keyData, idx));
}
scanptr.p->scanState = ScanRecord::RUNNING;
sendFragScansLab(signal, scanptr, apiConnectptr);
```

**Stack buffer size:** `MAX_NDB_NODES * 2` words. For a typical cluster
with MAX_NDB_NODES=48, that's 96 words = 384 bytes.

### Attach section in sendScanFragReq (DbtcMain.cpp)

In sendScanFragReq, after the existing section setup (line 18379-18388)
and before the MultiFrag block (line 18390):

```cpp
SectionHandle sections(this);
sections.m_ptr[0].i = scanP->scanAttrInfoPtr;
sections.m_ptr[1].i = scanP->scanKeyInfoPtr;
sections.m_cnt = 1;  // always attrInfo
if (scanP->scanKeyInfoPtr != RNIL) {
    jamDebug();
    sections.m_cnt = 2;  // sometimes keyinfo
}

// NEW: Attach aggStateKeys section for join-agg queries
if (scanP->m_joinAgg) {
    jam();
    sections.m_ptr[sections.m_cnt++].i = scanP->m_aggKeysSectionPtrI;
}

// MultiFrag adds fragIdList as the LAST section (existing code)
if (ScanFragReq::getMultiFragFlag(scanP->scanRequestInfo)) {
    // ... existing MultiFrag block appends fragIdPtrI ...
    sections.m_ptr[sections.m_cnt++].i = fragIdPtrI;
}
```

**Section ordering (DBTC sends, index increases):**
```
[0] AttrInfo          (always, shared across sends)
[1] KeyInfo           (optional — absent for join-agg)
[?] aggStateKeys      (if JoinAgg — shared across sends)
[?] fragIdList        (if MultiFrag — per-send unique, released after each non-last send)
```

For join-agg + MultiFrag (the common case):
```
[0] AttrInfo, [1] aggStateKeys, [2] fragIdList  → 3 sections (max)
```

For join-agg without MultiFrag:
```
[0] AttrInfo, [1] aggStateKeys  → 2 sections
```

**Section lifetime:** aggStateKeys is shared across all sends (same keys
for all SPJ workers), just like AttrInfo and KeyInfo. The existing
`sendBatchedFragmentedSignal` with `!isLastReq` keeps shared sections
alive. The existing MultiFrag cleanup (`release(sections.m_ptr[sections.m_cnt - 1])`)
still works because fragIdList remains the LAST section.

### Clear section pointer on last send (line 18476-18483)

```cpp
const bool isLastReq = (scanP->scanNextFragId >= scanP->scanNoFrag);
if (isLastReq) {
    jamDebug();
    scanP->scanKeyInfoPtr = RNIL;
    scanP->scanAttrInfoPtr = RNIL;
    if (scanP->m_joinAgg) scanP->m_aggKeysSectionPtrI = RNIL;
}
```

### Add m_aggKeysSectionPtrI to ScanRecord (Dbtc.hpp)

```cpp
Uint32 m_aggKeysSectionPtrI;  // Section: (nodeId, aggStateKey) pairs for DBSPJ
```

Initialize in initScanrec():
```cpp
scanptr.p->m_aggKeysSectionPtrI = RNIL;
```

Release in releaseScanResources():
```cpp
if (scanptr.p->m_aggKeysSectionPtrI != RNIL) {
    releaseSection(scanptr.p->m_aggKeysSectionPtrI);
    scanptr.p->m_aggKeysSectionPtrI = RNIL;
}
```

---

## 4. DbspjMain.cpp: Extract aggStateKeys from SCAN_FRAGREQ

### File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

### In execSCAN_FRAGREQ(), after MultiFrag extraction (line 1336)

The extraction follows the same pattern as MultiFrag: peel sections from
the end. DBSPJ processes sections in reverse order of how DBTC added them.

**Extraction order in DBSPJ (decreasing from end):**
1. If MultiFrag: fragIdList = `handle.m_ptr[--sectionCnt]` (already done, line 1325)
2. If JoinAgg: aggStateKeys = `handle.m_ptr[--sectionCnt]`
3. If sectionCnt > 1: KeyInfo = `handle.m_ptr[1]`
4. AttrInfo = `handle.m_ptr[0]`

```cpp
// After MultiFrag extraction (line 1336), before tree build:
Uint32 aggKeysPtrI = RNIL;
if (ScanFragReq::getJoinAggFlag(req->requestInfo)) {
    jam();
    sectionCnt--;
    aggKeysPtrI = handle.m_ptr[sectionCnt].i;
    SectionReader reader(aggKeysPtrI, getSectionSegmentPool());
    Uint32 numPairs = reader.getSize() / 2;
    for (Uint32 i = 0; i < numPairs; i++) {
        Uint32 nodeId, aggKey;
        ndbrequire(reader.getWord(&nodeId));
        ndbrequire(reader.getWord(&aggKey));
        ndbrequire(nodeId < MAX_NDB_NODES);
        requestPtr.p->m_aggStateKeys[nodeId] = aggKey;
        requestPtr.p->m_aggNodes.set(nodeId);
    }
}
```

### Release the aggStateKeys section (after line 1367)

Add after the existing `releaseSection(fragIdsPtrI)`:
```cpp
release(attrPtr);
releaseSection(fragIdsPtrI);   // MultiFrag list (existing)
releaseSection(aggKeysPtrI);   // aggStateKeys (new)
handle.clear();
```

`releaseSection(RNIL)` is a no-op, so this is safe when JoinAgg is not set.

### Why this works

- **No signal ordering concern:** The aggStateKeys arrive with the
  SCAN_FRAGREQ itself, not in a follow-up signal. No race possible.
- **No hash lookup needed:** The Request is being constructed in the
  same function where we extract the section. No need to look up
  the Request by senderData/transId.
- **Request already created:** At line 1304-1309, the Request is
  allocated and initialized via do_init(). By line 1336+, we can
  safely write to requestPtr.p.

---

## 5. DbspjMain.cpp: lookup_send() Modification

### File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

### Insertion point: after line 4756, before error injection (line 4883)

The aggStateKey must be placed at `variableData[var_index + 4]` because
DBLQH's variable data parsing expects it at that position:

**DBLQH variable data parsing order for SPJ LQHKEYREQ:**
1. `variableData[0]` = userId (if UserIdFlag, var_index++ to 1)
2. `variableData[var_index]` = applRef (SPJ reference, NormalProtocol)
3. `variableData[var_index + 1]` = applOprec (treeNodePtr.i)
4. `nextPos = 2 + var_index` (because ApplicationAddressFlag=1)
5. CorrFactorFlag=1: correlation + rootResultData at nextPos -> nextPos += 2
6. -> nextPos = `4 + var_index`
7. No Rowid, no GCI for SPJ lookups
8. **JoinAggFlag: aggStateKey at `variableData[4 + var_index]`**

This matches DBSPJ's write to `variableData[var_index + 4]`.

**Verification:** For SPJ LQHKEYREQ, the following flags are NOT set:
- SameClientAndTcFlag = 0 (no tcOprec word)
- lastReplicaNo == seqNoReplica (no nextReplicas word)
- ReturnedReadLenAIFlag = 0 (no readlenAi word)
- RowidFlag = 0 (no rowid words)
- GCIFlag = 0 (no GCI word)

So the aggStateKey immediately follows the CorrFactor pair.

### Code to add (after line 4756, before the error-insertion block):

```cpp
Uint32 agg_extra = 0;
if (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) {
    jam();
    Uint32 nodeId = refToNode(treeNodePtr.p->m_send.m_ref);
    ndbrequire(requestPtr.p->m_aggNodes.get(nodeId));
    LqhKeyReq::setJoinAggFlag(req->attrLen, 1);
    req->variableData[var_index + 4] = requestPtr.p->m_aggStateKeys[nodeId];
    agg_extra = 1;
}
```

**Notes:**
- `requestPtr` is already available (parameter of lookup_send)
- `treeNodePtr.p->m_send.m_ref` is the destination LQH ref, set during
  lookup preparation. `refToNode()` extracts the node ID to look up the
  aggStateKey. This ref may be modified later (V_QUERY handling at line
  4915-4931), but the node ID remains the same (line 4931 uses `Tnode`).
- `req->attrLen` is DATA 1 of LqhKeyReq. JoinAggFlag is at bit 28
  (SI_JOIN_AGG_SHIFT). This field also carries distributionKey and other
  flags, all set in the template. We OR in the JoinAggFlag.

### Modify sendSignal call at line 4932:

Change:
```cpp
sendSignal(ref, GSN_LQHKEYREQ, signal,
           NDB_ARRAY_SIZE(treeNodePtr.p->m_lookup_data.m_lqhKeyReq) + var_index,
           JBB,
           &handle);
```

To:
```cpp
sendSignal(ref, GSN_LQHKEYREQ, signal,
           NDB_ARRAY_SIZE(treeNodePtr.p->m_lookup_data.m_lqhKeyReq)
               + var_index + agg_extra,
           JBB,
           &handle);
```

### Signal length verification

- `NDB_ARRAY_SIZE(m_lqhKeyReq)` = FixedSignalLength + 4 = 11 + 4 = 15
- Without userId, without aggStateKey: 15 + 0 = 15 words
- With userId, without aggStateKey: 15 + 1 = 16 words
- Without userId, with aggStateKey: 15 + 0 + 1 = 16 words
- With userId, with aggStateKey: 15 + 1 + 1 = 17 words
- All within the 25-word signal limit (before sections)

### DBLQH signal length check (DblqhMain.cpp line 9370)

```cpp
if (unlikely((LqhKeyReq::FixedSignalLength + nextPos) !=
             signal->getLength())) {
```

This check validates that the signal length matches the parsed variable
data. With aggStateKey, nextPos = 4 + var_index + 1 = 5 + var_index,
and signal length = 11 + 5 + var_index = 16 + var_index. This matches
our send: 15 + var_index + 1 = 16 + var_index.

---

## 6. DbspjMain.cpp: Suppress T_EXPECT_TRANSID_AI

### Background

For aggregate leaf nodes, DBLQH normally does NOT send per-row
TRANSID_AI back to DBSPJ. Instead, rows are aggregated in-place using
the shared aggregation state. Final results are sent during the
COMPLETE phase (directly to API).

**Exception — group overflow (eviction):** When the AggInterpreter's
hash table is full, groups are evicted and sent as partial TRANSID_AI
results directly to the API. This happens during the operations phase,
not during COMPLETE. See section 8 for how this is tracked and how
flow control prevents API overload.

DBSPJ must NOT set T_EXPECT_TRANSID_AI on the aggregate leaf because
DBSPJ itself never receives TRANSID_AI from these nodes — the evicted
results go directly to the API, bypassing DBSPJ.

### Modification points

**Location 1: line 9805** (in the LINKED_ATTR/API result path)

Context (lines 9793-9806):
```cpp
        /**
         * Read correlation factor
         */
        dst[cnt++] = AttributeHeader::CORR_FACTOR32 << 16;

        err = DbspjErr::OutOfSectionMemory;
        if (!appendToSection(attrInfoPtrI, dst, cnt)) {
          jam();
          break;
        }
        sum_read += cnt;
        treeNodePtr.p->m_bits |= TreeNode::T_EXPECT_TRANSID_AI;
```

Change line 9805 to:
```cpp
        if (!(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF)) {
          treeNodePtr.p->m_bits |= TreeNode::T_EXPECT_TRANSID_AI;
        }
```

**Location 2: line 9832** (in the INNER_JOIN/FIRST_MATCH correlation-only path)

Context (lines 9816-9833):
```cpp
      else if (requestPtr.p->isScan() &&
               (treeNodePtr.p->m_bits &
                (TreeNode::T_INNER_JOIN | TreeNode::T_FIRST_MATCH))) {
        jam();
        Uint32 cnt = 0;
        /**
         * Only read correlation factor
         */
        dst[cnt++] = AttributeHeader::CORR_FACTOR32 << 16;

        err = DbspjErr::OutOfSectionMemory;
        if (!appendToSection(attrInfoPtrI, dst, cnt)) {
          jam();
          break;
        }
        sum_read += cnt;
        treeNodePtr.p->m_bits |= TreeNode::T_EXPECT_TRANSID_AI;
```

Change line 9832 to:
```cpp
        if (!(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF)) {
          treeNodePtr.p->m_bits |= TreeNode::T_EXPECT_TRANSID_AI;
        }
```

### Why this is correct

When T_AGGREGATE_LEAF is set:
- DBLQH receives LQHKEYREQ with JoinAggFlag -> calls handleJoinAggRow()
  instead of sending TRANSID_AI
- DBLQH sends LQHKEYCONF to DBSPJ (normal completion)
- DBSPJ receives LQHKEYCONF, decrements m_outstanding
- Without T_EXPECT_TRANSID_AI, DBSPJ doesn't wait for TRANSID_AI data
- Batch completion proceeds normally when m_outstanding reaches 0

The existing FLUSH_AI suppression (lines 9747-9760) already handles
intermediate aggregate nodes. This change handles the aggregate leaf.

---

## 7. Aggregate Leaf Correlation Factor

### Issue: Does the aggregate leaf need CORR_FACTOR?

Looking at the two T_EXPECT_TRANSID_AI locations, both add a
CORR_FACTOR32 read to the attrInfo. For aggregate leaf nodes, this
correlation factor is used by DBSPJ to match LQHKEYCONF responses
to their parent rows.

Even though we suppress T_EXPECT_TRANSID_AI (no data rows expected),
the CORR_FACTOR is still needed because:
- LQHKEYCONF carries the correlation factor back
- DBSPJ uses it to track which parent row's child lookup completed
- This is separate from TRANSID_AI data

So we should still append the CORR_FACTOR read attribute but NOT set
T_EXPECT_TRANSID_AI. The modified code achieves this: the dst/cnt
logic that adds CORR_FACTOR32 to attrInfo still runs, only the
T_EXPECT_TRANSID_AI bit is conditionally skipped.

---

## 8. Group Overflow: TRANSID_AI During Operations Phase

### Problem

During operations, each LQHKEYREQ with JoinAggFlag feeds a row into
the AggInterpreter's hash table. Normally no results are sent — all
aggregation happens in-place. But when the hash table is full, groups
must be evicted. Evicted groups are sent as partial TRANSID_AI results
directly to the API (via `m_resultRef`). This happens during the
operations phase, not during COMPLETE.

DBLQH already tracks evictions per scan batch in
`ScanRecord::m_join_agg_evict_rows` (Dblqh.hpp line 822). Each time
`handleJoinAggRow()` produces a non-zero `read_len` (indicating
TRANSID_AI was sent), the counter increments (DblqhMain.cpp line 19618).

### LQHKEYCONF: report evicted rows to DBSPJ

Currently LQHKEYCONF has `readLen` (bytes of TRANSID_AI data sent to
the requesting block). For aggregate leaf lookups, readLen is 0 because
TRANSID_AI goes to the API, not to DBSPJ. However, DBSPJ needs to know
that rows were sent to the API so it can track cumulative API load.

**Approach:** Extend LQHKEYCONF (or use the existing `readLen` field)
to carry the number of evicted group rows sent to API for this
operation. When `JoinAggFlag` is set:
- `readLen` = 0 (no TRANSID_AI to DBSPJ)
- Eviction info carried in an additional field or repurposed field

The exact field encoding is to be determined. Options:
- Use `numFiredTriggers` (unused for read-only aggregate lookups)
- Add a version-gated extended signal length with an extra word
- Repurpose `readLen` since DBSPJ ignores it for aggregate leaf

### DBSPJ: accumulate eviction counts

DBSPJ maintains a per-Request counter for evicted group rows:

```cpp
// In struct Request:
Uint32 m_agg_rows_sent_to_api;  // Cumulative evicted groups sent to API
```

In `lookup_execLQHKEYCONF`, when T_AGGREGATE_LEAF, add the reported
eviction count to `m_agg_rows_sent_to_api`.

### SCAN_FRAGCONF: report to DBTC

SCAN_FRAGCONF already carries `completedOps` (row/group count) and
`total_len` (TRANSID_AI word count). For join-agg queries, DBSPJ
reports the cumulative evicted rows in SCAN_FRAGCONF so DBTC can
track how much data the API has already received.

This is needed for flow control: if evicted rows are accumulating
at the API, DBTC may need to throttle further fragment sends or
signal the API to consume results before proceeding.

### Flow control concern

Without tracking, evictions from many concurrent lookups across
multiple data nodes could flood the API with TRANSID_AI signals.
The tracking chain is:

```
DBLQH (per-op eviction count in LQHKEYCONF)
  → DBSPJ (per-Request cumulative count)
    → DBTC (per-scan cumulative in SCAN_FRAGCONF)
      → API (flow control: pause sends if API backlogged)
```

DBTC already has scan flow control (SCAN_TABCONF batching). The
eviction count lets it factor in "early" result rows when deciding
whether to send more SCAN_NEXTREQs.

---

## 9. Examined Rows: 64-bit Tracking

### Problem

For pushdown join aggregation, the number of examined rows can be very
large — it represents the total rows read from the inner (leaf) table
across all join lookups. For example, a join of a 1M-row orders table
with a 6M-row lineitem table might examine millions of lineitem rows.
A 32-bit counter (max ~4 billion) could overflow for large queries.

### DBLQH → DBSPJ: examined rows in SCAN_FRAGCONF

SCAN_FRAGCONF already has a `rowsExamined` field (Uint32, version-gated
via `ndbd_support_scan_frag_rows_examined`). For join aggregation, the
leaf-level examined rows accumulate across all LQHKEYREQ operations
processed within a single scan batch.

The examined rows counter must be 64-bit. Since SCAN_FRAGCONF currently
uses Uint32 for `rowsExamined`, this needs extension:

**Option A:** Send as two Uint32 words (low + high), version-gated
to a new signal length (`SignalLength_v3`):
```cpp
static constexpr Uint32 SignalLength_v3 = 11;  // After SignalLength_v2 = 9
Uint32 rowsExaminedHigh;  // Upper 32 bits of 64-bit examined rows
Uint32 aggRowsSentToApi;  // Evicted group rows (see section 8)
```

**Option B:** Pack into existing words using unused bits or conditional
encoding when JoinAgg flag is set.

Option A is cleaner and follows the established version-gating pattern.

### DBSPJ → DBTC: accumulate in SCAN_FRAGCONF

DBSPJ accumulates examined rows across all child lookups in a Request.
When DBSPJ sends SCAN_FRAGCONF back to DBTC, it includes the
cumulative examined rows for this batch.

```cpp
// In struct Request:
Uint64 m_rows_examined;  // Cumulative examined rows across all lookups
```

### DBTC → API: report in SCAN_TABCONF

DBTC accumulates examined rows from all SCAN_FRAGCONFs across all SPJ
workers. The final count is sent to the API in SCAN_TABCONF (or via a
handler status signal) so the MySQL handler can report it in query
statistics (e.g., `SHOW STATUS LIKE 'Handler_read%'`).

### Counting in DBLQH

For join aggregation leaf lookups, each LQHKEYREQ with JoinAggFlag
represents one examined row. The existing `m_rows_examined` counter
in DBLQH's ScanRecord (incremented per scan row) doesn't apply because
join-agg leaf operations arrive as LQHKEYREQ, not scan operations.

Instead, the examined row count for the join-agg leaf is the number of
completed LQHKEYREQ operations with JoinAggFlag. DBSPJ already tracks
completed lookups per tree node — this count can be accumulated into
the 64-bit `m_rows_examined` on the Request.

---

## 10. Node Failure During Operations

### What happens if a data node fails while lookups are in flight?

DBSPJ already handles node failure for lookups:
- `execNODE_FAILREP()` iterates all active requests
- For each request with operations to the failed node, the outstanding
  count is adjusted and the request may be aborted

For join aggregation, the additional concern is that the failed node's
aggStateKey is now invalid. However, this doesn't require special
handling in DBSPJ because:
- DBTC detects node failure and aborts the scan
- DBTC sends RELEASE_REQ to surviving nodes
- The failed node's state is cleaned up by node restart

DBSPJ doesn't need to track which nodes have aggregation state — it
just needs the aggStateKey for each LQHKEYREQ it sends, and if the
target node is down, the lookup fails normally.

---

## 11. Build Verification

After each change, verify with:
```bash
make -j$(sysctl -n hw.ncpu) ndbmtd
```

### Expected compilation order:
1. Dbspj.hpp (Request fields) -> triggers recompile of DBSPJ sources
2. DbspjMain.cpp (section extraction + lookup_send + parseDA changes)

No changes to signal definitions or GlobalSignalNumbers.h for the
DBSPJ side — all delivered via SCAN_FRAGREQ sections.

---

## 12. Summary of All Changes

| File | Line | Change |
|------|------|--------|
| `Dbspj.hpp` | ~1260 | Replace fixed `m_lookup_node_data[]` array with pointer |
| `Dbspj.hpp` | ~1261 | Add `m_aggStateKeys` pointer and `m_aggNodes` bitmask |
| `Dbspj.hpp` | Request | Add `m_agg_rows_sent_to_api`, `m_rows_examined` counters |
| `DbspjMain.cpp` | do_init | Dynamic alloc via `lc_ndbd_pool_malloc` (both arrays, one block) |
| `DbspjMain.cpp` | cleanup | Free dynamic block via `lc_ndbd_pool_free` |
| `DbspjMain.cpp` | ~1336 | Extract aggStateKeys from SCAN_FRAGREQ section in execSCAN_FRAGREQ() |
| `DbspjMain.cpp` | ~1367 | Release aggStateKeys section after extraction |
| `DbspjMain.cpp` | ~4756 | Add aggStateKey to variableData in lookup_send() |
| `DbspjMain.cpp` | ~4932 | Add `+ agg_extra` to signal length |
| `DbspjMain.cpp` | LQHKEYCONF | Accumulate eviction count into `m_agg_rows_sent_to_api` |
| `DbspjMain.cpp` | SCAN_FRAGCONF | Report `m_agg_rows_sent_to_api` and `m_rows_examined` |
| `DbspjMain.cpp` | 9805 | Guard T_EXPECT_TRANSID_AI with !T_AGGREGATE_LEAF |
| `DbspjMain.cpp` | 9832 | Guard T_EXPECT_TRANSID_AI with !T_AGGREGATE_LEAF |
| `ScanFrag.hpp` | SignalLength | Add `SignalLength_v3` with 64-bit rowsExamined + aggRowsSent |
| `Dbtc.hpp` | ScanRecord | Add `m_aggKeysSectionPtrI` field |
| `DbtcMain.cpp` | initScanrec | Init `m_aggKeysSectionPtrI = RNIL` |
| `DbtcMain.cpp` | SETUP_CONF | Build aggStateKeys section when all nodes confirmed |
| `DbtcMain.cpp` | sendScanFragReq | Attach aggStateKeys section before MultiFrag |
| `DbtcMain.cpp` | sendScanFragReq | Clear `m_aggKeysSectionPtrI` on last send |
| `DbtcMain.cpp` | releaseScanResources | Release `m_aggKeysSectionPtrI` if non-RNIL |
| `DbtcMain.cpp` | SCAN_TABCONF | Accumulate 64-bit examined rows for API reporting |
