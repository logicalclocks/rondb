# NDB API Integration for Pushdown Join Aggregation (RONDB-733)

## Context

Pushdown join aggregation is fully implemented at the kernel level (DBLQH, DBTUP, DBTC, DBSPJ) and tested via manual signal construction. The RONDB-733 branch added hash-partitioned receiver routing: multiple NdbReceiver IDs in SCAN_TABREQ section 0, with groups routed to `receiverIds[hash(key) % N]`. Each group always maps to exactly one receiver — no cross-receiver merge needed.

This plan adds aggregation support to the NDB API (NdbQueryBuilder/NdbQueryOperation) so C++ programs can build pushed join queries with GROUP BY and aggregate functions without manual signal construction.

**Scope**: NDB API only. No MySQL handler changes. Rebase onto `RONDB-733` branch first.

---

## Prerequisite: Git Rebase

```bash
git rebase RONDB-733
```

---

## Architecture (from RONDB-733 branch)

### Signal flow with multiple receivers
```
API:   SCAN_TABREQ
         Section 0: receiverIds[N]  (NdbReceiver IDs, hash-partitioned)
         Section 1: AttrInfo        (QueryTree with NI_AGGREGATE DABits)
         Section 2: Agg program     (NdbAggregator bytecode)
         DATA 15:   scanParallelism (explicit, independent of section 0 size)

DBTC:  Stores section 0 as m_aggReceiverIdsPtrI
       Forwards section 0 as section 1 of JOIN_AGG_SETUP_REQ
       Forwards section 2 as section 0 of JOIN_AGG_SETUP_REQ

DBLQH: selectReceiverData(key, key_len) → receiverIds[hash(key) % N]
       Each group's TRANSID_AI goes to one specific receiver

API:   Each NdbReceiver gets a disjoint subset of groups
       No cross-receiver merge needed — groups are pre-partitioned
```

### Key design insight
A group is **always** routed to exactly one receiver. With N receivers across M data nodes, the same group key hashes to the same receiver from all nodes. This means results from multiple data nodes for the same group key arrive at the same NdbReceiver. The NdbAggregator can process each receiver's data independently or merge across receivers trivially (no duplicate groups within a receiver).

---

## Key Design Decisions

### 1. Section 2: Bounds + Agg program coexist via header

For ordered index scan roots with bounds AND aggregation:
```
Section 2 when JoinAgg is set:
  Word 0:                boundsLen (0 if no bounds)
  Words 1..boundsLen:    bounds data
  Words boundsLen+1..:   agg program
```
DBTC splits: bounds → `scanKeyInfoPtr`, remainder → `m_aggProgramPtrI`.

### 2. API surface: NdbQueryOptions::setAggregation()

Follows the `setInterpretedCode()` pattern. User attaches NdbAggregator to the leaf operation.

### 3. Multiple receivers for result partitioning

NdbQuery allocates N `NdbReceiver` objects. Their object-map IDs go into SCAN_TABREQ section 0. DBTC forwards them to DBLQH. Each group's TRANSID_AI is hash-routed to one receiver. After scan completes, each receiver's data is fed to `NdbAggregator::ProcessRes()`.

### 4. Explicit scanParallelism

For JoinAgg queries, `scanTabReq->scanParallelism` (DATA 15) carries the scan parallelism, since section 0 size now means "number of receivers" not "parallelism".

---

## Files to Modify

### NDB API (definition + execution)
| File | Changes |
|------|---------|
| `storage/ndb/src/ndbapi/NdbQueryBuilder.hpp` | `setAggregation()`, `addLinkedProjection()` on NdbQueryOptions |
| `storage/ndb/src/ndbapi/NdbQueryBuilderImpl.hpp` | `m_aggregator` in NdbQueryOptionsImpl; `m_isAggregateLeaf` on op def; `m_hasAggregation` on query def |
| `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp` | Implement setAggregation(); set NI_AGGREGATE/NI_AGGREGATE_LEAF in serializeOperation() |
| `storage/ndb/src/ndbapi/NdbQueryOperation.hpp` | `getAggregator()` on NdbQuery |
| `storage/ndb/src/ndbapi/NdbQueryOperationImpl.hpp` | `m_aggregator`, `m_aggProgram`, `m_aggReceivers[]`, `m_numAggReceivers` on NdbQueryImpl |
| `storage/ndb/src/ndbapi/NdbQueryOperation.cpp` | Build sections 0/2 with receiver IDs and agg program; set JoinAgg flag + scanParallelism; handle aggregate TRANSID_AI |

### DBTC (Section 2 header parsing)
| File | Changes |
|------|---------|
| `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` | Parse Section 2 header: split bounds from agg program |

### New test
| File | Purpose |
|------|---------|
| `storage/ndb/block_unit_test/testJoinAggNdbApi.cpp` | NdbQueryBuilder-based join aggregation test |
| `storage/ndb/block_unit_test/CMakeLists.txt` | Build target |

### Existing code to reuse
- `NdbAggregator` (`storage/ndb/include/ndbapi/NdbAggregator.hpp`) — program builder, ProcessRes(), FetchResultRecord()
- `NdbQueryOptions::setInterpretedCode()` pattern — copy-on-set, stored in NdbQueryOptionsImpl
- `NdbReceiver` with `NDB_SCANRECEIVER` type — registered in Ndb object map, receives TRANSID_AI
- DABits `NI_AGGREGATE` (0x2000), `NI_AGGREGATE_LEAF` (0x4000) — already defined in QueryTree.hpp
- `ScanTabReq::scanParallelism` (DATA 15) — already added in RONDB-733

---

## Implementation Steps (All Complete)

### Step 1: NdbQueryOptions — setAggregation() and addLinkedProjection()

**`NdbQueryBuilder.hpp`**: Add to NdbQueryOptions:
```cpp
int setAggregation(const NdbAggregator &agg);
int addLinkedProjection(const NdbLinkedOperand *operand);
```

**`NdbQueryBuilderImpl.hpp`** — NdbQueryOptionsImpl: Add `NdbAggregator *m_aggregator{nullptr}` and `Vector<const NdbLinkedOperandImpl *> m_linkedProjection`. Deep-copy on set, delete in destructor.

**`NdbQueryBuilder.cpp`**: `setAggregation()` validates finalized, deep-copies buffer. `addLinkedProjection()` stores operand for later processing.

### Step 2: NdbQueryOperationDefImpl — Aggregate leaf flag

**`NdbQueryBuilderImpl.hpp`**: Add `bool m_isAggregateLeaf{false}` and accessor.

**`NdbQueryBuilder.cpp`**: In `readTuple()`/`scanTable()`/`scanIndex()`, if options has aggregator, set `m_isAggregateLeaf = true`. Process `m_linkedProjection` operands: call `addColumnRef()` on parent for each to ensure SPJ projection includes them.

### Step 3: NdbQueryDefImpl — Track aggregation

**`NdbQueryBuilderImpl.hpp`**: Add `m_hasAggregation`, `m_aggregateLeafOpNo`, `m_aggregator` to NdbQueryDefImpl.

**`NdbQueryBuilder.cpp` — `prepare()`**: Scan operations, find aggregate leaf, validate (exactly 0 or 1 leaf, not root), transfer aggregator ownership. Set `m_queryHasAggregation` flag on all operations for serialization access.

### Step 4: Serialization — NI_AGGREGATE / NI_AGGREGATE_LEAF

**`NdbQueryBuilder.cpp`** — In each `serializeOperation()` method:
```cpp
if (m_queryHasAggregation) {
    requestInfo |= DABits::NI_AGGREGATE;
    if (m_isAggregateLeaf) {
        requestInfo |= DABits::NI_AGGREGATE_LEAF;
    }
}
```

### Step 5: NdbQueryImpl — Receivers and agg program

**`NdbQueryOperationImpl.hpp`**: Add to NdbQueryImpl:
```cpp
NdbAggregator *m_aggregator;
Uint32Buffer m_aggProgram;
NdbReceiver **m_aggReceivers;     // Array of receiver pointers
Uint32 m_numAggReceivers;
bool m_hasAggregation;
```

**`NdbQueryOperation.cpp` — prepareSend()**: If aggregation:
- Copy agg program into `m_aggProgram`
- Allocate N receivers (N = configurable, default to m_workerCount or a reasonable number)
- Init each as `NDB_SCANRECEIVER` with NdbQueryImpl as owner
- Create NdbAggregator for result collection

### Step 6: doSend() — Section 0 (receivers), Section 2 (agg program), JoinAgg flag

**`NdbQueryOperation.cpp` — doSend() scan path (~line 3340)**:

For aggregation queries:
```cpp
if (m_hasAggregation) {
    // Section 0: aggregate receiver IDs (not worker IDs)
    Uint32 aggReceiverIds[m_numAggReceivers];
    for (i = 0; i < m_numAggReceivers; i++)
        aggReceiverIds[i] = m_aggReceivers[i]->getId();
    secs[0].p = aggReceiverIds;
    secs[0].sz = m_numAggReceivers;

    // Section 2: [boundsLen, bounds..., aggProgram...]
    Uint32Buffer combinedSec2;
    combinedSec2.append(m_keyInfo.getSize());
    if (m_keyInfo.getSize() > 0)
        combinedSec2.append(m_keyInfo);
    combinedSec2.append(m_aggProgram);
    secs[2].p = combinedSec2.addr();
    secs[2].sz = combinedSec2.getSize();
    numSections = 3;

    // Flags
    ScanTabReq::setJoinAggFlag(reqInfo, 1);
    scanTabReq->scanParallelism = scanParallel;  // DATA 15
}
```

### Step 7: DBTC — Parse Section 2 header

**`DbtcMain.cpp` — execSCAN_TABREQ (~line 16042)**:

Replace current JoinAgg section 2 handling:
```cpp
if (ScanTabReq::getJoinAggFlag(ri)) {
    jam();
    scanptr.p->m_joinAgg = true;
    // Parse section 2: [boundsLen, bounds..., aggProgram...]
    SectionReader reader(handle.m_ptr[KeyInfoSectionNum].i, getSectionSegmentPool());
    Uint32 boundsLen;
    ndbrequire(reader.getWord(&boundsLen));
    if (boundsLen > 0) {
        // Extract bounds into scanKeyInfoPtr
        ...importToSection(scanptr.p->scanKeyInfoPtr, reader, boundsLen);
    }
    // Remaining = agg program
    Uint32 aggLen = sec2Size - 1 - boundsLen;
    ...importToSection(scanptr.p->m_aggProgramPtrI, reader, aggLen);
    // Release combined section
    releaseSection(handle.m_ptr[KeyInfoSectionNum].i);
}
```

### Step 8: Result handling — Receiver → NdbAggregator

TRANSID_AI dispatch in `Ndbif.cpp:511`:
- `tFirstData` = connectPtr = receiver ID (from `selectReceiverData()`)
- `void2rec(tFirstData)` → NdbReceiver
- `tRec->execTRANSID_AI()` stores data in receiver buffer

After scan completes (`SCAN_TABCONF` with EndOfData):
- Iterate over all m_aggReceivers
- For each receiver, extract row data from buffer
- Feed to `m_aggregator->ProcessRes(rowData)` for each row
- Call `m_aggregator->PrepareResults()`
- User accesses via `NdbQuery::getAggregator()->FetchResultRecord()`

**Note**: Since groups are hash-partitioned across receivers, same-key groups from different data nodes arrive at the same receiver. ProcessRes() merges them in its internal hash map.

### Step 9: NdbQuery public API

**`NdbQueryOperation.hpp`**: Add to NdbQuery:
```cpp
NdbAggregator *getAggregator() const;
```

### Step 10: Test program

**`storage/ndb/block_unit_test/testJoinAggNdbApi.cpp`**:

Uses NdbQueryBuilder (not SignalSender):
1. Create tables via MySQL
2. Insert test data
3. Build query: `qb->scanTable(parent)` + `qb->readTuple(child, linkedKey, &opts)` with `opts.setAggregation(agg)`
4. Execute via NdbTransaction::createQuery()
5. After completion, iterate `query->getAggregator()->FetchResultRecord()`
6. Verify against expected values

Test cases: basic SUM/COUNT, empty table, single group, multiple groups, index scan root with bounds, eviction (ERROR_INSERT 5090).

---

## Implementation Order (All Complete)

1. **Steps 1-4**: NdbQueryBuilder changes (API + serialization) — `make ndb_client` ✓
2. **Step 7**: DBTC Section 2 header parsing — `make ndbmtd` ✓
3. **Steps 5-6**: NdbQueryOperation signal building — `make ndb_client` ✓
4. **Steps 8-9**: Result handling — `make ndb_client` ✓
5. **Step 10**: Test program (4 tests including 3-way join) ✓

See `ndbapi_integration_implementation.md` for full implementation details.

---

## Verification

```bash
# Build
make -j$(sysctl -n hw.ncpu) ndbmtd ndb_client testJoinAggNdbApi

# New test
./testJoinAggNdbApi -c <connect_string> -m <mysql_port> --verbose

# Regression
./testJoinAgg -c <connect_string> -m <mysql_port>
./testJoinAggSpj -c <connect_string> -m <mysql_port>
cd mysql-test && ./mtr ndb_join_pushdown_*
```
