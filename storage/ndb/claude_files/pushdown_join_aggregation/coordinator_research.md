# Phase 7: DBTC-Orchestrated Pushdown Join Aggregation

## Context

DBLQH join aggregation is fully implemented and tested (Phases 1-6). DBLQH
handles: SETUP_REQ (allocate shared state + interpreters), operations
(LQHKEYREQ with aggStateKey), COMPLETE_REQ (merge + finalize + send
TRANSID_AI results), and RELEASE_REQ (cleanup).

DBTC will orchestrate SETUP/COMPLETE/RELEASE for the entire query. This gives:
- Single SETUP per data node (not per fragment scan)
- Single COMPLETE phase when all fragments are done
- Fewer partial results at API → less merging
- Natural "all fragments done" detection (DBTC already tracks this)

DBSPJ handles the operations phase: receives aggStateKeys from DBTC, sets
JoinAggFlag + aggStateKey in each LQHKEYREQ, suppresses per-row TRANSID_AI
for aggregate leaf nodes.

### Agg Program Delivery: Section 2 of SCAN_TABREQ

When the JoinAgg flag is set in SCAN_TABREQ, **section 2 carries the agg
program exclusively** (not KeyInfo). DBTC reads it without query tree parsing.

All three root types are supported (full table scan, ordered index scan, key
lookup). Currently section 2 also carries KeyInfo (bounds) for ordered index
scan roots in pushed queries. For join-agg queries, the NDB API must move
bounds into the query tree parameters (AttrInfo), freeing section 2 for the
agg program. This NDB API change is a separate later phase — initial testing
uses SignalSender where we control signal construction directly.

### Result Routing

DBLQH sends aggregated TRANSID_AI results directly to `resultRef` (API) during
COMPLETE. Cross-node merging of same GROUP BY keys is the MySQL handler's
responsibility. Each data node sends one set of partial group results.

---

## Signal Flow

```
1. API → DBTC:   SCAN_TABREQ (JoinAgg flag, section 2 = agg program)
2. DBTC:         Extract agg program from section 2, store in ScanRecord
3. DBTC:         Proceed with DIH fragment location requests (normal path)
4. DBTC:         Before sendFragScansLab → send SETUP_REQ to alive data nodes
5. DBTC → DBLQH: JOIN_AGG_SETUP_REQ (agg program as section 0) to each node
6. DBLQH → DBTC: JOIN_AGG_SETUP_CONF (aggStateKey per node)
7. DBTC:         All SETUP_CONFs → sendFragScansLab (normal fragment sending)
8. DBTC → DBSPJ: SCAN_FRAGREQ (JoinAgg flag set) + JOIN_AGG_STATE_KEYS signal
9. DBSPJ:        Build query tree, store aggStateKeys in Request
10. DBSPJ → DBLQH: LQHKEYREQ with JoinAggFlag + aggStateKey (each child lookup)
11. DBLQH → DBSPJ: LQHKEYCONF (no per-row TRANSID_AI for aggregate leaf)
12. DBSPJ → DBTC: SCAN_FRAGCONF (fragmentCompleted=1 when fragment done)
13. DBTC:         When all fragments done (sendScanTabConf EndOfData path):
14. DBTC → DBLQH: JOIN_AGG_COMPLETE_REQ to all nodes
15. DBLQH → API:  TRANSID_AI (aggregated result rows, direct to API)
16. DBLQH → DBTC: JOIN_AGG_COMPLETE_CONF (numResultRows)
17. DBTC:         Handle JOIN_AGG_SEND_REQ flow control (reply immediately)
18. DBTC:         All COMPLETE_CONFs → RELEASE_REQ → SCAN_TABCONF(EndOfData)
```

---

## Files to Modify

| File | Purpose |
|------|---------|
| `storage/ndb/include/kernel/signaldata/ScanTab.hpp` | JoinAgg flag in SCAN_TABREQ (bit 0) |
| `storage/ndb/include/kernel/signaldata/JoinAgg.hpp` | JoinAggStateKeys signal struct |
| `storage/ndb/include/kernel/GlobalSignalNumbers.h` | GSN_JOIN_AGG_STATE_KEYS = 958 |
| `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` | ScanRecord: agg program, aggStateKeys, state |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcInit.cpp` | Register signal handlers |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` | SETUP/COMPLETE/RELEASE orchestration |
| `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` | Request: aggStateKeys |
| `storage/ndb/src/kernel/blocks/dbspj/DbspjInit.cpp` | Register JOIN_AGG_STATE_KEYS handler |
| `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` | Receive keys, set JoinAggFlag, suppress TRANSID_AI |

NDB API changes (later phase — for now, test via SignalSender):

| File | Purpose |
|------|---------|
| `storage/ndb/src/ndbapi/NdbQueryOperation.cpp` | Move bounds to query tree, agg program in section 2 |

---

## Sub-Phase 7A: DBTC Plumbing — Flag, State, Signal Registration

### ScanTab.hpp: Add JoinAgg flag (bit 0)

Bits 0-7 were parallelism (deprecated since 7.0.34, should be zero-filled).
Bits 2-7 have been reclaimed individually. **Bit 0 is available**.

```cpp
#define SCAN_JOIN_AGG_SHIFT (0)

// In the request info bitmap comment, update bit 0 line
// Add getter/setter declarations in ScanTabReq class
static Uint8 getJoinAggFlag(const UintR &requestInfo);
static void setJoinAggFlag(UintR &requestInfo, Uint32 val);

// Inline implementations
inline Uint8 ScanTabReq::getJoinAggFlag(const UintR &requestInfo) {
  return (Uint8)((requestInfo >> SCAN_JOIN_AGG_SHIFT) & 1);
}
inline void ScanTabReq::setJoinAggFlag(UintR &requestInfo, Uint32 val) {
  ASSERT_BOOL(val, "ScanTabReq::setJoinAggFlag");
  requestInfo = (requestInfo & ~(1 << SCAN_JOIN_AGG_SHIFT)) |
                (val << SCAN_JOIN_AGG_SHIFT);
}
```

### GlobalSignalNumbers.h: Add GSN_JOIN_AGG_STATE_KEYS

```cpp
#define GSN_JOIN_AGG_STATE_KEYS         958
// Update MAX_GSN from 957 to 958
```

### JoinAgg.hpp: Add JoinAggStateKeys struct

```cpp
class JoinAggStateKeys {
public:
  static constexpr Uint32 HeaderLength = 2;
  Uint32 senderData;   // Matches SCAN_FRAGREQ senderData
  Uint32 numKeys;      // Number of (nodeId, aggStateKey) pairs
  // Followed by: Uint32 pairs[numKeys * 2]
};
```

### Dbtc.hpp: Extend ScanRecord and ScanState

Add WAIT_JOIN_AGG_SETUP to ScanState enum (line 1912):
```cpp
enum ScanState {
  IDLE = 0,
  WAIT_SCAN_TAB_INFO = 1,
  WAIT_AI = 2,
  WAIT_FRAGMENT_COUNT = 3,
  RUNNING = 4,
  CLOSING_SCAN = 5,
  WAIT_JOIN_AGG_SETUP = 6,
  WAIT_JOIN_AGG_COMPLETE = 7
};
```

Add fields to ScanRecord (before line 1999 closing brace):
```cpp
Uint32 m_aggProgramPtrI;                   // Section ptr to agg program (RNIL if none)
Uint32 m_aggStateKeys[MAX_NDB_NODES];     // aggStateKey per data node (145 max)
NdbNodeBitmask m_aggNodes;                 // Data nodes with active agg state
Uint32 m_aggNodesOutstanding;              // Outstanding SETUP/COMPLETE signals
bool m_joinAgg;                            // Is this a join-agg query?
```

Note: Use `MAX_NDB_NODES` (= `ABS_MAX_NDB_NODES` = 145) for array sizing.

Add method declarations to Dbtc class:
```cpp
void execJOIN_AGG_SETUP_CONF(Signal *);
void execJOIN_AGG_SETUP_REF(Signal *);
void execJOIN_AGG_COMPLETE_CONF(Signal *);
void execJOIN_AGG_COMPLETE_REF(Signal *);
void execJOIN_AGG_RELEASE_CONF(Signal *);
void execJOIN_AGG_SEND_REQ(Signal *);
void sendJoinAggSetupReqs(Signal *, ScanRecordPtr, ApiConnectRecordPtr);
void sendJoinAggCompleteReqs(Signal *, ScanRecordPtr);
void sendJoinAggReleaseReqs(Signal *, ScanRecordPtr);
```

### DbtcInit.cpp: Register signal handlers

```cpp
#include <signaldata/JoinAgg.hpp>
addRecSignal(GSN_JOIN_AGG_SETUP_CONF, &Dbtc::execJOIN_AGG_SETUP_CONF);
addRecSignal(GSN_JOIN_AGG_SETUP_REF, &Dbtc::execJOIN_AGG_SETUP_REF);
addRecSignal(GSN_JOIN_AGG_COMPLETE_CONF, &Dbtc::execJOIN_AGG_COMPLETE_CONF);
addRecSignal(GSN_JOIN_AGG_COMPLETE_REF, &Dbtc::execJOIN_AGG_COMPLETE_REF);
addRecSignal(GSN_JOIN_AGG_RELEASE_CONF, &Dbtc::execJOIN_AGG_RELEASE_CONF);
addRecSignal(GSN_JOIN_AGG_SEND_REQ, &Dbtc::execJOIN_AGG_SEND_REQ);
```

### DbtcMain.cpp: Initialize new fields in initScanrec() (around line 16240)

```cpp
scanptr.p->m_aggProgramPtrI = RNIL;
scanptr.p->m_joinAgg = false;
scanptr.p->m_aggNodes.clear();
scanptr.p->m_aggNodesOutstanding = 0;
```

Also forward JoinAgg flag to ScanFragReq requestInfo (after line 16262):
```cpp
ScanFragReq::setJoinAggFlag(tmp, ScanTabReq::getJoinAggFlag(ri));
```

### DbtcMain.cpp: Stub signal handlers

```cpp
void Dbtc::execJOIN_AGG_SETUP_CONF(Signal *signal) { jamEntry(); }
void Dbtc::execJOIN_AGG_SETUP_REF(Signal *signal) { jamEntry(); }
void Dbtc::execJOIN_AGG_COMPLETE_CONF(Signal *signal) { jamEntry(); }
void Dbtc::execJOIN_AGG_COMPLETE_REF(Signal *signal) { jamEntry(); }
void Dbtc::execJOIN_AGG_RELEASE_CONF(Signal *signal) { jamEntry(); }
void Dbtc::execJOIN_AGG_SEND_REQ(Signal *signal) { jamEntry(); }
```

**Verify**: `make -j$(sysctl -n hw.ncpu) ndbmtd`

---

## Sub-Phase 7B: DBTC SETUP Phase

### In execSCAN_TABREQ(): Extract agg program from section 2

After section storage (line 16037-16041), when JoinAgg flag is set:
```cpp
if (ScanTabReq::getJoinAggFlag(ri)) {
    jam();
    scanptr.p->m_joinAgg = true;
    // Section 2 = agg program (not KeyInfo) for join-agg queries
    if (keyLen) {
        jamDebug();
        scanptr.p->m_aggProgramPtrI =
            handle.m_ptr[ScanTabReq::KeyInfoSectionNum].i;
        scanptr.p->scanKeyInfoPtr = RNIL;  // Not KeyInfo for join-agg
    }
}
```

Note: `keyLen` was extracted earlier in execSCAN_TABREQ and indicates section 2
is present. For join-agg, this section is the agg program, not key bounds.

### SETUP insertion point: before sendFragScansLab (line 16807)

In sendDihGetNodesLab(), at line 16807, replace:
```cpp
sendFragScansLab(signal, scanptr, apiConnectptr);
```
with:
```cpp
if (scanP->m_joinAgg) {
    jam();
    scanP->scanState = ScanRecord::WAIT_JOIN_AGG_SETUP;
    sendJoinAggSetupReqs(signal, scanptr, apiConnectptr);
} else {
    sendFragScansLab(signal, scanptr, apiConnectptr);
}
```

### Implement sendJoinAggSetupReqs()

```cpp
void Dbtc::sendJoinAggSetupReqs(Signal *signal, ScanRecordPtr scanptr,
                                 ApiConnectRecordPtr apiConnectptr) {
    scanptr.p->m_aggNodes.clear();
    scanptr.p->m_aggNodesOutstanding = 0;

    for (Uint32 nodeId = 1; nodeId < MAX_NDB_NODES; nodeId++) {
        if (!getNodeInfo(nodeId).m_connected) continue;
        if (getNodeInfo(nodeId).m_type != NodeInfo::DB) continue;

        JoinAggSetupReq *req = (JoinAggSetupReq *)signal->getDataPtrSend();
        req->senderRef = reference();
        req->senderData = scanptr.i;
        req->requestId = scanptr.p->scanApiRec;
        req->transid[0] = apiConnectptr.p->transid[0];
        req->transid[1] = apiConnectptr.p->transid[1];
        req->tableId = scanptr.p->scanTableref;
        req->expectedOpCount = 0;
        req->concurrencyStrategy = JoinAggSetupReq::STRATEGY_MUTEX_FREE;
        req->resultRef = apiConnectptr.p->ndbapiBlockref;
        req->resultData = apiConnectptr.p->ndbapiConnect;
        req->routeRef = reference();

        // Attach agg program as section 0
        SectionHandle handle(this);
        Uint32 aggPtrI = RNIL;
        ndbrequire(dupSection(aggPtrI, scanptr.p->m_aggProgramPtrI));
        getSection(handle.m_ptr[0], aggPtrI);
        handle.m_cnt = 1;

        Uint32 ref = numberToRef(DBLQH, nodeId);  // DblqhProxy
        sendSignal(ref, GSN_JOIN_AGG_SETUP_REQ, signal,
                   JoinAggSetupReq::SignalLength, JBB, &handle);
        scanptr.p->m_aggNodes.set(nodeId);
        scanptr.p->m_aggNodesOutstanding++;
    }

    if (scanptr.p->m_aggNodesOutstanding == 0) {
        // No data nodes → error
        jam();
        abortScanLab(signal, scanptr, ZNODE_FAILURE_ERROR, true, apiConnectptr);
    }
}
```

### Implement execJOIN_AGG_SETUP_CONF()

```cpp
void Dbtc::execJOIN_AGG_SETUP_CONF(Signal *signal) {
    jamEntry();
    const JoinAggSetupConf *conf =
        (const JoinAggSetupConf *)signal->getDataPtr();

    ScanRecordPtr scanptr;
    scanptr.i = conf->senderData;
    ndbrequire(scanRecordPool.getPtr(scanptr));

    if (scanptr.p->scanState == ScanRecord::CLOSING_SCAN) {
        jam();
        // Scan was closed while waiting for SETUP — ignore
        return;
    }
    ndbrequire(scanptr.p->scanState == ScanRecord::WAIT_JOIN_AGG_SETUP);

    Uint32 nodeId = refToNode(conf->senderRef);
    scanptr.p->m_aggStateKeys[nodeId] = conf->aggStateKey;
    scanptr.p->m_aggNodesOutstanding--;

    if (scanptr.p->m_aggNodesOutstanding == 0) {
        jam();
        // All nodes set up — proceed with normal fragment scanning
        scanptr.p->scanState = ScanRecord::RUNNING;
        ApiConnectRecordPtr apiConnectptr;
        apiConnectptr.i = scanptr.p->scanApiRec;
        ndbrequire(c_apiConnectRecordPool.getPtr(apiConnectptr));
        sendFragScansLab(signal, scanptr, apiConnectptr);
    }
}
```

### Implement execJOIN_AGG_SETUP_REF()

```cpp
void Dbtc::execJOIN_AGG_SETUP_REF(Signal *signal) {
    jamEntry();
    const JoinAggSetupRef *ref =
        (const JoinAggSetupRef *)signal->getDataPtr();

    ScanRecordPtr scanptr;
    scanptr.i = ref->senderData;
    ndbrequire(scanRecordPool.getPtr(scanptr));

    Uint32 nodeId = refToNode(ref->senderRef);
    scanptr.p->m_aggNodes.clear(nodeId);
    scanptr.p->m_aggNodesOutstanding--;

    ApiConnectRecordPtr apiConnectptr;
    apiConnectptr.i = scanptr.p->scanApiRec;
    ndbrequire(c_apiConnectRecordPool.getPtr(apiConnectptr));

    // Send RELEASE to nodes that already confirmed
    sendJoinAggReleaseReqs(signal, scanptr);

    // Abort the scan
    abortScanLab(signal, scanptr, ref->errorCode, true, apiConnectptr);
}
```

**Verify**: Build ndbmtd.

---

## Sub-Phase 7C: DBTC → DBSPJ: Pass aggStateKeys

### In sendScanFragReq() (DbtcMain.cpp, line 18330)

After sending SCAN_FRAGREQ (after sendBatchedFragmentedSignal at ~line 18627),
if m_joinAgg, send a follow-up signal with aggStateKeys:

```cpp
if (scanP->m_joinAgg) {
    jam();
    Uint32 *data = signal->getDataPtrSend();
    data[0] = scanFragP->lqhScanFragId;  // senderData matching SCAN_FRAGREQ
    Uint32 idx = 2;
    NdbNodeBitmask nodes = scanP->m_aggNodes;
    for (Uint32 nid = nodes.find_first();
         nid != NdbNodeBitmask::NotFound;
         nid = nodes.find_next(nid + 1)) {
        ndbrequire(idx + 1 < 25);  // Fits in signal body
        data[idx++] = nid;
        data[idx++] = scanP->m_aggStateKeys[nid];
    }
    data[1] = (idx - 2) / 2;  // numKeys
    sendSignal(scanFragP->lqhBlockref, GSN_JOIN_AGG_STATE_KEYS,
               signal, idx, JBB);
}
```

The JoinAgg flag in SCAN_FRAGREQ requestInfo is already set in initScanrec
(Sub-Phase 7A) so DBSPJ knows to expect the keys signal.

---

## Sub-Phase 7D: DBSPJ — Receive Keys, Operations Phase

### Dbspj.hpp: Add to Request (not TreeNode)

Store aggStateKeys at the Request level (shared across all TreeNodes):
```cpp
// In struct Request, add:
Uint32 m_aggStateKeys[MAX_NDB_NODES];
NdbNodeBitmask m_aggNodes;
```

### DbspjInit.cpp: Register new signal

```cpp
addRecSignal(GSN_JOIN_AGG_STATE_KEYS, &Dbspj::execJOIN_AGG_STATE_KEYS);
```

### DbspjMain.cpp: Handle JOIN_AGG_STATE_KEYS

```cpp
void Dbspj::execJOIN_AGG_STATE_KEYS(Signal *signal) {
    jamEntry();
    const Uint32 *data = signal->getDataPtr();
    Uint32 senderData = data[0];
    Uint32 numKeys = data[1];

    Ptr<Request> requestPtr;
    ndbrequire(m_scan_request_hash.find(requestPtr, senderData));

    for (Uint32 i = 0; i < numKeys; i++) {
        Uint32 nodeId = data[2 + i * 2];
        Uint32 key = data[2 + i * 2 + 1];
        requestPtr.p->m_aggStateKeys[nodeId] = key;
        requestPtr.p->m_aggNodes.set(nodeId);
    }
}
```

### Modify lookup_send() (line ~4704)

After existing variableData setup, add aggStateKey for aggregate leaf:
```cpp
Uint32 agg_extra = 0;
if (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) {
    jam();
    Uint32 nodeId = refToNode(ref);
    Ptr<Request> requestPtr;
    m_request_pool.getPtr(requestPtr, treeNodePtr.p->m_requestPtrI);
    ndbrequire(requestPtr.p->m_aggNodes.get(nodeId));
    LqhKeyReq::setJoinAggFlag(req->attrLen, 1);
    variableData[var_index + 4] = requestPtr.p->m_aggStateKeys[nodeId];
    agg_extra = 1;
}
```

Adjust signal length in the sendSignal call:
```cpp
sendSignal(ref, GSN_LQHKEYREQ, signal,
           LqhKeyReq::FixedSignalLength + var_index + agg_extra,
           JBB, &handle);
```

### Suppress T_EXPECT_TRANSID_AI for aggregate leaf

In parseDA() at lines 9805 and 9832 (where T_EXPECT_TRANSID_AI is set):
```cpp
if (!(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF)) {
    treeNodePtr.p->m_bits |= TreeNode::T_EXPECT_TRANSID_AI;
}
```

**Verify**: Build ndbmtd.

---

## Sub-Phase 7E: DBTC COMPLETE Phase

### Trigger point: sendScanTabConf EndOfData (line 18770)

In sendScanTabConf(), at lines 18770-18774, when all fragments are done:

```cpp
if (scanPtr.p->m_delivered_scan_frags.isEmpty() &&
    scanPtr.p->m_running_scan_frags.isEmpty()) {
    jam();
    if (scanPtr.p->m_joinAgg && !scanPtr.p->m_aggNodes.isclear()) {
        jam();
        // Enter COMPLETE phase — don't send EndOfData yet
        scanPtr.p->scanState = ScanRecord::WAIT_JOIN_AGG_COMPLETE;
        sendJoinAggCompleteReqs(signal, scanPtr);
        return;  // Will resume in execJOIN_AGG_COMPLETE_CONF
    }
    release = true;
    conf->requestInfo = op_count | ScanTabConf::EndOfData;
```

### Implement sendJoinAggCompleteReqs()

```cpp
void Dbtc::sendJoinAggCompleteReqs(Signal *signal, ScanRecordPtr scanptr) {
    scanptr.p->m_aggNodesOutstanding = 0;
    NdbNodeBitmask nodes = scanptr.p->m_aggNodes;
    for (Uint32 nodeId = nodes.find_first();
         nodeId != NdbNodeBitmask::NotFound;
         nodeId = nodes.find_next(nodeId + 1)) {
        JoinAggCompleteReq *req =
            (JoinAggCompleteReq *)signal->getDataPtrSend();
        req->senderRef = reference();
        req->senderData = scanptr.i;
        req->requestId = scanptr.p->scanApiRec;

        ApiConnectRecordPtr apiPtr;
        apiPtr.i = scanptr.p->scanApiRec;
        ndbrequire(c_apiConnectRecordPool.getPtr(apiPtr));
        req->transid[0] = apiPtr.p->transid[0];
        req->transid[1] = apiPtr.p->transid[1];

        req->aggStateKey = scanptr.p->m_aggStateKeys[nodeId];
        req->maxBatchRows = 256;

        Uint32 ref = numberToRef(DBLQH, 1, nodeId);
        sendSignal(ref, GSN_JOIN_AGG_COMPLETE_REQ, signal,
                   JoinAggCompleteReq::SignalLength, JBB);
        scanptr.p->m_aggNodesOutstanding++;
    }
}
```

### Implement execJOIN_AGG_COMPLETE_CONF()

```cpp
void Dbtc::execJOIN_AGG_COMPLETE_CONF(Signal *signal) {
    jamEntry();
    const JoinAggCompleteConf *conf =
        (const JoinAggCompleteConf *)signal->getDataPtr();

    ScanRecordPtr scanptr;
    scanptr.i = conf->senderData;
    ndbrequire(scanRecordPool.getPtr(scanptr));

    scanptr.p->m_aggNodesOutstanding--;

    if (scanptr.p->m_aggNodesOutstanding == 0) {
        jam();
        // All COMPLETE done — send RELEASE and final EndOfData
        sendJoinAggReleaseReqs(signal, scanptr);

        // Resume normal scan close: send SCAN_TABCONF(EndOfData)
        ApiConnectRecordPtr apiConnectptr;
        apiConnectptr.i = scanptr.p->scanApiRec;
        ndbrequire(c_apiConnectRecordPool.getPtr(apiConnectptr));

        scanptr.p->scanState = ScanRecord::CLOSING_SCAN;
        scanptr.p->m_close_scan_req = true;
        close_scan_req_send_conf(signal, scanptr, apiConnectptr);
    }
}
```

### Implement execJOIN_AGG_COMPLETE_REF()

```cpp
void Dbtc::execJOIN_AGG_COMPLETE_REF(Signal *signal) {
    jamEntry();
    const JoinAggCompleteRef *ref =
        (const JoinAggCompleteRef *)signal->getDataPtr();

    ScanRecordPtr scanptr;
    scanptr.i = ref->senderData;
    ndbrequire(scanRecordPool.getPtr(scanptr));

    // Release all nodes and abort
    sendJoinAggReleaseReqs(signal, scanptr);

    ApiConnectRecordPtr apiConnectptr;
    apiConnectptr.i = scanptr.p->scanApiRec;
    ndbrequire(c_apiConnectRecordPool.getPtr(apiConnectptr));
    abortScanLab(signal, scanptr, ref->errorCode, true, apiConnectptr);
}
```

### Implement execJOIN_AGG_SEND_REQ() — flow control

```cpp
void Dbtc::execJOIN_AGG_SEND_REQ(Signal *signal) {
    jamEntry();
    const JoinAggSendReq *req =
        (const JoinAggSendReq *)signal->getDataPtr();

    JoinAggSendConf *conf = (JoinAggSendConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = req->senderData;
    conf->requestId = req->requestId;
    conf->aggStateKey = req->aggStateKey;
    conf->maxBatchRows = 256;
    sendSignal(req->senderRef, GSN_JOIN_AGG_SEND_CONF, signal,
               JoinAggSendConf::SignalLength, JBB);
}
```

---

## Sub-Phase 7F: DBTC RELEASE + Cleanup

### Implement sendJoinAggReleaseReqs()

```cpp
void Dbtc::sendJoinAggReleaseReqs(Signal *signal, ScanRecordPtr scanptr) {
    NdbNodeBitmask nodes = scanptr.p->m_aggNodes;
    for (Uint32 nodeId = nodes.find_first();
         nodeId != NdbNodeBitmask::NotFound;
         nodeId = nodes.find_next(nodeId + 1)) {
        JoinAggReleaseReq *req =
            (JoinAggReleaseReq *)signal->getDataPtrSend();
        req->senderRef = reference();
        req->senderData = scanptr.i;
        req->requestId = scanptr.p->scanApiRec;

        ApiConnectRecordPtr apiPtr;
        apiPtr.i = scanptr.p->scanApiRec;
        ndbrequire(c_apiConnectRecordPool.getPtr(apiPtr));
        req->transid[0] = apiPtr.p->transid[0];
        req->transid[1] = apiPtr.p->transid[1];

        req->aggStateKey = scanptr.p->m_aggStateKeys[nodeId];

        Uint32 ref = numberToRef(DBLQH, nodeId);
        sendSignal(ref, GSN_JOIN_AGG_RELEASE_REQ, signal,
                   JoinAggReleaseReq::SignalLength, JBB);
    }
    scanptr.p->m_aggNodes.clear();
}
```

### Implement execJOIN_AGG_RELEASE_CONF()

```cpp
void Dbtc::execJOIN_AGG_RELEASE_CONF(Signal *signal) {
    jamEntry();
    // Fire-and-forget: nothing to do
}
```

### Release agg program section in releaseScanResources()

```cpp
if (scanptr.p->m_aggProgramPtrI != RNIL) {
    releaseSection(scanptr.p->m_aggProgramPtrI);
    scanptr.p->m_aggProgramPtrI = RNIL;
}
```

### Abort path

In scan abort paths, if m_joinAgg and m_aggNodes is non-empty:
```cpp
if (scanptr.p->m_joinAgg) {
    sendJoinAggReleaseReqs(signal, scanptr);
}
```

---

## Verification

### Build
```bash
make -j$(sysctl -n hw.ncpu) ndbmtd
```
After each sub-phase.

### Testing
Initial testing via modified bench_q12_tpch or new SignalSender test that:
1. Sends SCAN_TABREQ with JoinAgg flag + agg program in section 2
2. Verifies DBTC does SETUP (aggStateKeys returned)
3. Verifies DBSPJ receives aggStateKeys and sets JoinAggFlag in LQHKEYREQ
4. Verifies DBTC does COMPLETE after all fragments done
5. Verifies aggregated results arrive at API

### Regression
Run existing MTR NDB tests to ensure no regressions for non-join-agg queries.
