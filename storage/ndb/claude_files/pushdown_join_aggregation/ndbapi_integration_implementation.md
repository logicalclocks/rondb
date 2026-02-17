# NDB API Integration Implementation (RONDB-733)

This document describes the changes made to integrate pushdown join aggregation
into the NDB API (NdbQueryBuilder/NdbQueryOperation), enabling C++ applications
to build pushed join queries with GROUP BY and aggregate functions.

## Commits (ndbapi-integration branch)

| Step | Commit | Description |
|------|--------|-------------|
| Plan | `55a88ed` | Add NDB API integration plan |
| Docs | `54f66ac` | Add API documentation (NdbQueryAggregation.hpp) |
| 1 | `48e2327` | Add NdbQueryOptions::setAggregation() and addLinkedProjection() |
| 2 | `8d1cf2b` | Add m_isAggregateLeaf flag to NdbQueryOperationDefImpl |
| 3 | `821a65f` | Track aggregation at query definition level |
| 4 | `198e8ed` | Set NI_AGGREGATE/NI_AGGREGATE_LEAF DABits in serialization |
| 5 | `dd688bc` | Add aggregation fields to NdbQueryImpl and NDB_AGG_RECEIVER type |
| 6 | `88929a6` | Set JoinAgg flag and send agg program in doSend() section 2 |
| 7 | `916c28f` | Parse combined section 2 header for bounds + agg program |
| 8 | `b3bc3d8` | Add aggregation result handling (receiver routing + data processing) |
| 10 | `27ce99f` | Add testJoinAggNdbApi integration test |

Step 9 (NdbQuery::getAggregator()) was implemented as part of Step 5.

---

## Architecture

### Signal flow (API → kernel → API)

```
NdbQueryBuilder                    NdbQueryOperation::doSend()
  scanTable(parent)                  SCAN_TABREQ
  readTuple(child, key, &opts)         Section 0: aggReceiverIds[1]
  opts.setAggregation(agg)             Section 1: AttrInfo (QueryTree with NI_AGGREGATE DABits)
  opts.addLinkedProjection(link)        Section 2: [boundsLen, bounds..., aggReceiverId, aggProgram...]
  prepare(ndb)                         DATA 15:   scanParallelism
                                       requestInfo: JoinAggFlag set
         |
         v
DBTC (DbtcMain.cpp)
  Parse Section 2 header:
    boundsLen → scanKeyInfoPtr
    aggReceiverId → m_aggReceiverId
    aggProgram → m_aggProgramPtrI
  sendJoinAggSetupReqs():
    resultData = m_aggReceiverId  (routes TRANSID_AI to API receiver)
         |
         v
DBLQH → AggInterpreter → TRANSID_AI results
         |
         v
API (Ndbif.cpp)
  NDB_AGG_RECEIVER dispatch → NdbQueryImpl::execAggTRANSID_AI()
  Accumulates data in m_aggResultData buffer
         |
         v
API (NdbQueryOperation.cpp)
  EndOfData → processAggResults()
    → NdbAggregator::ProcessRes() for each batch
    → NdbAggregator::PrepareResults()
         |
         v
Application
  query->getAggregator()->FetchResultRecord()
```

### Section 2 combined format

For JoinAgg queries, SCAN_TABREQ Section 2 carries bounds (if index scan)
and aggregation data in a combined format:

```
Word 0:                boundsLen (0 if no bounds)
Words 1..boundsLen:    bounds data (for ordered index scans)
Word boundsLen+1:      aggReceiverId (API NdbReceiver object-map ID)
Remaining words:       aggregation program (NdbAggregator bytecode)
```

DBTC splits these apart: bounds → `scanKeyInfoPtr`, receiver ID → `m_aggReceiverId`,
remainder → `m_aggProgramPtrI`.

---

## Files Modified

### NDB API — Query Definition (Steps 1-4)

#### `NdbQueryBuilder.hpp` — Public API additions

```cpp
class NdbQueryOptions {
  // ... existing methods ...

  // Step 1: New methods for aggregation
  int setAggregation(const NdbAggregator &agg);
  int addLinkedProjection(const NdbLinkedOperand *operand);
};
```

- `setAggregation()`: Deep-copies NdbAggregator program buffer into options
- `addLinkedProjection()`: Registers parent column operands for SPJ projection

#### `NdbQueryBuilderImpl.hpp` — Internal state

**NdbQueryOptionsImpl** (Step 1):
- `Uint32 *m_aggProgramBuffer` — Deep-copied aggregation program
- `Uint32 m_aggProgramLen` — Program word count
- `Uint32 m_aggNGroupByCols` — GROUP BY column count
- `bool m_aggDiskColumns` — Whether disk columns are referenced
- `const NdbTableImpl *m_aggTable` — Table for result interpretation
- `Vector<const NdbLinkedOperandImpl *> m_linkedProjection` — Parent columns to project

**NdbQueryOperationDefImpl** (Step 2):
- `bool m_isAggregateLeaf` — True if this operation carries the aggregation
- `bool m_queryHasAggregation` — Set on all ops before serialization

**NdbQueryDefImpl** (Step 3):
- `bool m_hasAggregation` — Query-level flag
- `Uint32 m_aggregateLeafOpNo` — Index of the aggregate leaf operation
- Aggregation program data (transferred from options to definition)

#### `NdbQueryBuilder.cpp` — Builder logic

**Step 1** — `setAggregation()` and `addLinkedProjection()`:
- Validates NdbAggregator is finalized
- Deep-copies program buffer via `copyAggregation()`
- Stores linked operands for later processing

**Step 2** — `readTuple()`/`scanTable()`/`scanIndex()`:
- If options has aggregator, sets `m_isAggregateLeaf = true`
- Processes linked projection operands

**Step 3** — `prepare()`:
- Scans operations to find aggregate leaf
- Validates: exactly 0 or 1 leaf, not root
- Transfers aggregator ownership to NdbQueryDefImpl
- Sets `m_queryHasAggregation` on all operations

**Step 4** — `serializeOperation()` (all variants):
```cpp
if (m_queryHasAggregation) {
    requestInfo |= DABits::NI_AGGREGATE;
    if (m_isAggregateLeaf) {
        requestInfo |= DABits::NI_AGGREGATE_LEAF;
    }
}
```

### NDB API — Query Execution (Steps 5-6, 8-9)

#### `NdbQueryOperation.hpp` — Public result API

```cpp
class NdbQuery {
  // Step 9: Aggregation result access
  NdbAggregator *getAggregator() const;
};
```

#### `NdbQueryOperationImpl.hpp` — Execution state

**Step 5** — New fields in NdbQueryImpl:
```cpp
// Aggregation state
bool m_hasAggregation;
NdbAggregator *m_aggregator;        // Owned, deleted in postFetchRelease()

// Aggregation program (from definition)
Uint32Buffer m_aggProgram;

// Dedicated receivers for aggregation TRANSID_AI
NdbReceiver **m_aggReceivers;
Uint32 m_numAggReceivers;

// Raw result accumulation
Uint32Buffer m_aggResultData;        // Concatenated batch data
Uint32Buffer m_aggResultOffsets;     // Start offset for each batch
```

**Step 8** — New methods:
```cpp
// Called from Ndbif.cpp TRANSID_AI dispatch
void execAggTRANSID_AI(const Uint32 *data, Uint32 len);
void execAggTRANSID_AI_frag(const Uint32 *data, Uint32 len, Uint32 fragInfo);

// Called after scan completes
int processAggResults();
```

#### `NdbQueryOperation.cpp` — Signal building and result handling

**Step 5** — Constructor/destructor:
- Initialize aggregation fields (m_hasAggregation, m_aggregator, buffers)
- Allocate NDB_AGG_RECEIVER receivers during `prepareSend()`
- Cleanup in `postFetchRelease()` (delete aggregator, release buffers)

**Step 6** — `doSend()` scan path:
```cpp
if (m_hasAggregation) {
    // Set JoinAgg flag
    ScanTabReq::setJoinAggFlag(reqInfo, 1);
    scanTabReq->scanParallelism = scanParallel;  // DATA 15

    // Build combined Section 2
    Uint32Buffer combinedAggSec2;
    const Uint32 boundsLen = m_keyInfo.getSize();
    combinedAggSec2.append(boundsLen);
    if (boundsLen > 0) combinedAggSec2.append(m_keyInfo);
    combinedAggSec2.append(m_aggReceivers[0]->getId());  // receiver ID
    combinedAggSec2.append(m_aggProgram);                 // agg program

    // Section 0: receiver IDs (not worker IDs)
    secs[0].p = aggReceiverIds;
    secs[0].sz = m_numAggReceivers;
}
```

**Step 8** — Result handling:
```cpp
// Non-fragmented TRANSID_AI → store batch data
void NdbQueryImpl::execAggTRANSID_AI(const Uint32 *data, Uint32 len) {
    if (len == 0) return;
    m_aggResultOffsets.append(m_aggResultData.getSize());
    Uint32 *dst = m_aggResultData.alloc(len);
    if (likely(dst != nullptr))
        memcpy(dst, data, len * sizeof(Uint32));
}

// After EndOfData → feed accumulated data to NdbAggregator
int NdbQueryImpl::processAggResults() {
    if (!m_hasAggregation || m_aggregator == nullptr) return 0;
    const Uint32 numBatches = m_aggResultOffsets.getSize();
    for (Uint32 i = 0; i < numBatches; i++) {
        const Uint32 offset = m_aggResultOffsets.addr()[i];
        const Uint32 nextOffset = (i + 1 < numBatches)
            ? m_aggResultOffsets.addr()[i + 1]
            : m_aggResultData.getSize();
        const Uint32 batchLen = nextOffset - offset;
        const Uint32 *batchData = m_aggResultData.addr() + offset;
        if (batchLen > 1) {
            // Skip AttributeHeader word, ProcessRes expects data starting
            // from n_gb_cols|n_agg_results
            m_aggregator->ProcessRes(
                const_cast<char *>(reinterpret_cast<const char *>(batchData + 1)));
        }
    }
    m_aggregator->PrepareResults();
    return 0;
}
```

Integration at scan completion:
```cpp
case FetchResult_noMoreData:
    m_state = EndOfData;
    if (m_hasAggregation) {
        processAggResults();
    }
    postFetchRelease();
    return NdbQuery::NextResult_scanComplete;
```

#### `NdbReceiver.hpp` — New receiver type

```cpp
enum ReceiverType {
    NDB_UNINITIALIZED,
    NDB_OPERATION = 1,
    NDB_SCANRECEIVER = 2,
    NDB_INDEX_OPERATION = 3,
    NDB_QUERY_OPERATION = 4,
    NDB_AGG_RECEIVER = 5       // NEW: dedicated aggregation receiver
};
```

#### `Ndbif.cpp` — TRANSID_AI signal dispatch

Added `NDB_AGG_RECEIVER` handling in all 3 TRANSID_AI paths:

```cpp
// Non-fragmented long signal
} else if (type == NdbReceiver::NDB_AGG_RECEIVER) {
    NdbQueryImpl *queryImpl = (NdbQueryImpl *)tRec->getOwner();
    queryImpl->execAggTRANSID_AI(ptr[0].p, ptr[0].sz);
    com = 0;

// Fragmented long signal
} else if (type == NdbReceiver::NDB_AGG_RECEIVER) {
    NdbQueryImpl *queryImpl = (NdbQueryImpl *)tRec->getOwner();
    queryImpl->execAggTRANSID_AI_frag(ptr[0].p, ptr[0].sz,
                                       aSignal->m_fragmentInfo);
    com = 0;

// Short signal
} else if (type == NdbReceiver::NDB_AGG_RECEIVER) {
    NdbQueryImpl *queryImpl = (NdbQueryImpl *)tRec->getOwner();
    queryImpl->execAggTRANSID_AI(
        tDataPtr + TransIdAI::HeaderLength,
        tLen - TransIdAI::HeaderLength);
    com = 0;
```

#### `NdbImpl.hpp` — Transaction lookup for receiver type

```cpp
case NDB_AGG_RECEIVER:
    return &((NdbQueryImpl *)m_owner)->getNdbTransaction();
```

### DBTC — Section 2 parsing (Steps 7-8)

#### `Dbtc.hpp` — ScanRecord fields

```cpp
Uint32 m_aggReceiverId;     // API NdbReceiver ID for result routing
```

#### `DbtcMain.cpp` — execSCAN_TABREQ

**Step 7** — Parse combined section 2:
```cpp
if (ScanTabReq::getJoinAggFlag(ri) && keyLen) {
    scanptr.p->m_joinAgg = true;
    scanptr.p->scanKeyInfoPtr = RNIL;
    scanptr.p->m_aggProgramPtrI = RNIL;

    SectionReader reader(handle.m_ptr[KeyInfoSectionNum].i,
                         getSectionSegmentPool());
    Uint32 boundsLen;
    ndbrequire(reader.getWord(&boundsLen));
    ndbrequire(2 + boundsLen <= keyLen);

    if (boundsLen > 0) {
        // Extract bounds → scanKeyInfoPtr
        for (Uint32 w = 0; w < boundsLen; w++) {
            Uint32 word;
            ndbrequire(reader.getWord(&word));
            appendToSection(scanptr.p->scanKeyInfoPtr, &word, 1);
        }
    }

    // Extract receiver ID
    ndbrequire(reader.getWord(&scanptr.p->m_aggReceiverId));

    // Extract agg program
    const Uint32 aggLen = keyLen - 2 - boundsLen;
    if (aggLen > 0) {
        for (Uint32 w = 0; w < aggLen; w++) {
            Uint32 word;
            ndbrequire(reader.getWord(&word));
            appendToSection(scanptr.p->m_aggProgramPtrI, &word, 1);
        }
    }

    releaseSection(handle.m_ptr[KeyInfoSectionNum].i);
    handle.m_ptr[KeyInfoSectionNum].i = RNIL;
    handle.m_ptr[KeyInfoSectionNum].sz = 0;
}
```

**Step 8** — Route results to API receiver:
```cpp
// In sendJoinAggSetupReqs():
req->resultData = (scanptr.p->m_aggReceiverId != RNIL)
    ? scanptr.p->m_aggReceiverId
    : apiConnectptr.p->ndbapiConnect;
```

### Test (Step 10)

#### `testJoinAggNdbApi.cpp`

Integration test using the NdbQueryBuilder API (not raw signals).

**Schema:**
- `jagg_parent(id INT PK, grp INT)`
- `jagg_child(parent_id INT PK, amount BIGINT)`

**Test data:** 5 parent rows (3 groups), 5 child rows (amounts 100-500)

**Tests:**
1. `SELECT grp, SUM(amount) ... GROUP BY grp` — GROUP BY with SUM, linked parent column
2. `SELECT COUNT(*), SUM(amount) ...` — Non-GROUP-BY with multiple aggregates
3. `SELECT grp, COUNT(*), SUM(amount) ... GROUP BY grp` — Multiple aggregates with GROUP BY

**Build:** `make -j$(sysctl -n hw.ncpu) testJoinAggNdbApi` (from debug_build)

---

## Key Design Decisions

### 1. Combined Section 2 format with header

Rather than using a separate signal section for the aggregation program, we
pack bounds + receiver ID + agg program into Section 2 with a length header.
This avoids needing a 4th section in SCAN_TABREQ.

### 2. Dedicated NDB_AGG_RECEIVER type

Aggregation results from the kernel bypass normal scan result routing. A new
receiver type (NDB_AGG_RECEIVER = 5) in the NdbReceiver enum enables Ndbif.cpp
to route TRANSID_AI signals directly to NdbQueryImpl.

### 3. API-side receiver ID in Section 2

The kernel's DBTC sends `ndbapiConnect` (NdbTransaction ID) as `resultData` in
JOIN_AGG_SETUP_REQ by default. This would cause TRANSID_AI to be dispatched to
the wrong object. By embedding an NDB_AGG_RECEIVER object-map ID in Section 2,
DBTC extracts and uses it as `resultData`, ensuring results arrive at the
correct NdbReceiver on the API side.

### 4. Accumulate-then-process result strategy

Rather than processing aggregation results as they arrive (which would require
locking), we accumulate raw TRANSID_AI data in `m_aggResultData` and process
it all at once after the scan completes (EndOfData). This is simple and correct
since aggregation results are only meaningful after all scan batches are consumed.

### 5. Linked column flag (0x8000)

When an aggregation program references a parent column (e.g., GROUP BY on parent's
column), bit 15 is set in the column ID passed to `NdbAggregator::GroupBy()` or
`LoadColumn()`. The `addLinkedProjection()` call ensures SPJ includes the parent
column in the data sent to the child operation.
