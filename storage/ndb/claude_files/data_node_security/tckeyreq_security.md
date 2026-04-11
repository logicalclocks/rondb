# Secure TCKEYREQ Signal Handling in Dbtc

## Problem

The `execTCKEYREQ` function in Dbtc handles malformed signals unsafely:

| Current mechanism | Behavior | Problem |
|---|---|---|
| `warningHandlerLab` | No-op in production, debug-only log | Malicious sender faces no consequences |
| `ndbrequire` | Crashes the data node | Malicious sender can take down the cluster |
| `ndbassert` | Crashes in debug only | Inconsistent between debug/release |

Both API nodes and compromised data nodes (`ndbmtd`) can send malformed signals. The correct response is to **disconnect the sender and log to the cluster log**, not crash.

## Key Files

- `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — main implementation
- `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` — declarations

## Existing Disconnect Mechanisms

| Node type | Mechanism | QMGR handler |
|---|---|---|
| API | DUMP_STATE_ORD 900 to QMGR | `api_failed()` → `API_FAILREQ` broadcast + transport close |
| Data | DUMP_STATE_ORD 939 to QMGR | `CLOSE_COMREQ` to TRPMAN → `DISCONNECT_REP` → `node_failed()` → full failure protocol |

Existing `handleSignalStateProblem` (DbtcMain.cpp:2451) only handles API nodes — has `ndbrequire(nodeType == API)` which would crash on data node senders.

---

## Step 1: Create `disconnectMaliciousNode` Function

### Declaration (Dbtc.hpp, after handleSignalStateProblem declaration at line 2568)

```cpp
void disconnectMaliciousNode(Signal *signal,
                             NodeId nodeId,
                             const char *reason,
                             int line);
```

### Implementation (DbtcMain.cpp, after handleSignalStateProblem at ~line 2508)

```cpp
void Dbtc::disconnectMaliciousNode(Signal *signal,
                                   NodeId nodeId,
                                   const char *reason,
                                   int line) {
  jam();
  g_eventLogger->warning(
      "TC %u : Malformed signal from node %u (type %u) at line %d: %s. "
      "Disconnecting.",
      instance(), nodeId, getNodeInfo(nodeId).getType(), line, reason);

  if (getNodeInfo(nodeId).getType() == NODE_TYPE_API) {
    jam();
    /* API node: ask QMGR to disconnect via api_failed() */
    signal->theData[0] = 900;
    signal->theData[1] = nodeId;
    sendSignal(QMGR_REF, GSN_DUMP_STATE_ORD, signal, 2, JBA);
  } else {
    jam();
    /* Data node: close transport, QMGR handles via node_failed() */
    signal->theData[0] = 939;
    signal->theData[1] = nodeId;
    sendSignal(QMGR_REF, GSN_DUMP_STATE_ORD, signal, 2, JBA);
  }
}
```

**Design decisions:**
- Handles both API and data nodes (no `ndbrequire` on node type)
- API: DUMP_STATE_ORD 900 → `api_failed()` (same as `handleSignalStateProblem`)
- Data: DUMP_STATE_ORD 939 → transport close → `DISCONNECT_REP` → `node_failed()` → full failure protocol
- Logs node type in warning so cluster admin knows whether sender was API or data
- Takes `NodeId` directly (not `ApiConnectRecordPtr`) — works even when apiConnectptr is invalid

---

## Step 2: Update execTCKEYREQ Validation Points

### 2a. Invalid API connect record pointer (DbtcMain.cpp:3576-3581)

**Current:** `ndbrequire(!passQueueingFlag)` + `warningHandlerLab` (crash / no-op)

```cpp
if (unlikely(!c_apiConnectRecordPool.getValidPtr(apiConnectptr))) {
    jam();
    ndbrequire(!passQueueingFlag);       // REMOVE: crashes on external input
    releaseSections(handle);
    warningHandlerLab(signal, __LINE__);  // REMOVE: no-op
    return;
}
```

**New:** Disconnect sender using `signal->getSendersBlockRef()` (no valid apiConnectptr available).

```cpp
if (unlikely(!c_apiConnectRecordPool.getValidPtr(apiConnectptr))) {
    jam();
    releaseSections(handle);
    NodeId senderNodeId = refToNode(signal->getSendersBlockRef());
    disconnectMaliciousNode(signal, senderNodeId,
        "invalid apiConnectPtr in TCKEYREQ", __LINE__);
    return;
}
```

### 2b. Table index out of bounds (DbtcMain.cpp:3605-3613)

**Current:** `ndbrequire(!passQueueingFlag)` + `warningHandlerLab` + conditional `TCKEY_abort(69)` (crash / no-op)

```cpp
if (unlikely(TtabIndex > TtabMaxIndex)) {
    jam();
    ndbrequire(!passQueueingFlag);       // REMOVE
    releaseSections(handle);
    warningHandlerLab(signal, __LINE__);  // REMOVE
    if (!is_transaction_to_start(regApiPtr, TstartFlag)) {
      TCKEY_abort(signal, 69, apiConnectptr);  // REMOVE: disconnect instead
    }
    return;
}
```

**New:** Disconnect sender (apiConnectptr is valid here, use `regApiPtr->ndbapiBlockref`).

```cpp
if (unlikely(TtabIndex > TtabMaxIndex)) {
    jam();
    releaseSections(handle);
    NodeId senderNodeId = refToNode(regApiPtr->ndbapiBlockref);
    disconnectMaliciousNode(signal, senderNodeId,
        "table index out of bounds in TCKEYREQ", __LINE__);
    return;
}
```

### 2c. User ID validation failure (DbtcMain.cpp:3628-3635)

**Current:** `ndbassert(false)` + fall through (crash in debug / silent fallback in release)

```cpp
} else if (unlikely(databaseRecordPtr.p->m_is_user == false ||
                    databaseRecordPtr.p->m_database_version !=
                      user_id_version)) {
  databaseRecordPtr.i = localTabptr.p->databaseRecord;
  databaseRecordPtr.p = nullptr;
  g_eventLogger->info("(%u) Sending incorrect user id: %u",
    instance(), user_id);
  ndbassert(false);  // REMOVE: crashes debug builds
}
```

**New:** Disconnect sender.

```cpp
} else if (unlikely(databaseRecordPtr.p->m_is_user == false ||
                    databaseRecordPtr.p->m_database_version !=
                      user_id_version)) {
  jam();
  releaseSections(handle);
  NodeId senderNodeId = refToNode(regApiPtr->ndbapiBlockref);
  disconnectMaliciousNode(signal, senderNodeId,
      "incorrect user id in TCKEYREQ", __LINE__);
  return;
}
```

### 2d. Reorg flag with invalid operation type (DbtcMain.cpp:4312)

**Current:** `ndbassert(false)` (crash in debug / silent in release)

```cpp
    else {
      ndbassert(false);  // REMOVE
    }
```

**New:** Disconnect sender + resource cleanup (TC record + cache already seized).

```cpp
    else {
      jam();
      terrorCode = ZSIGNAL_ERROR;
      NodeId senderNodeId = refToNode(regApiPtr->ndbapiBlockref);
      disconnectMaliciousNode(signal, senderNodeId,
          "reorg flag set with invalid operation type in TCKEYREQ",
          __LINE__);
      releaseAtErrorLab(signal, apiConnectptr);
      return;
    }
```

### 2e. UNLOCK without distribution key (DbtcMain.cpp:4357)

**Current:** `ndbassert(distributionKeyIndicator)` (crash in debug / bad state in release)

```cpp
  if (unlikely(TOperationType == ZUNLOCK)) {
    ndbassert(regCachePtr->distributionKeyIndicator);  // REMOVE
    regCachePtr->m_no_hash = 1;
```

**New:** Disconnect sender + resource cleanup.

```cpp
  if (unlikely(TOperationType == ZUNLOCK)) {
    if (unlikely(!regCachePtr->distributionKeyIndicator)) {
      jam();
      terrorCode = ZSIGNAL_ERROR;
      NodeId senderNodeId = refToNode(regApiPtr->ndbapiBlockref);
      disconnectMaliciousNode(signal, senderNodeId,
          "UNLOCK without distribution key in TCKEYREQ", __LINE__);
      releaseAtErrorLab(signal, apiConnectptr);
      return;
    }
    regCachePtr->m_no_hash = 1;
```

### 2f. CommitFlag without ExecFlag (DbtcMain.cpp:4608)

**Current:** `ndbrequire(TexecFlag)` (crash)

```cpp
  if (TcKeyReq::getCommitFlag(Treqinfo) == 1) {
    ndbrequire(TexecFlag);  // REMOVE: both flags from external input
    regApiPtr->apiConnectstate = CS_REC_COMMITTING;
```

**New:** Disconnect sender + resource cleanup.

```cpp
  if (TcKeyReq::getCommitFlag(Treqinfo) == 1) {
    if (unlikely(!TexecFlag)) {
      jam();
      terrorCode = ZSIGNAL_ERROR;
      NodeId senderNodeId = refToNode(regApiPtr->ndbapiBlockref);
      disconnectMaliciousNode(signal, senderNodeId,
          "CommitFlag set without ExecFlag in TCKEYREQ", __LINE__);
      releaseAtErrorLab(signal, apiConnectptr);
      return;
    }
    regApiPtr->apiConnectstate = CS_REC_COMMITTING;
```

### 2g. TCKEY_abort case 2 — start during active abort (DbtcMain.cpp:2637-2654)

**Current:** Send TCROLLBACKREP + `ndbassert(false)` (crash in debug)

```cpp
  case 2:{
    printState(signal, 6, apiConnectptr);
    const TcKeyReq * const tcKeyReq = (TcKeyReq *)&signal->theData[0];
    const Uint32 t1 = tcKeyReq->transId1;
    const Uint32 t2 = tcKeyReq->transId2;
    signal->theData[0] = apiConnectptr.p->ndbapiConnect;
    signal->theData[1] = t1;
    signal->theData[2] = t2;
    signal->theData[3] = ZABORT_ERROR;
    ndbassert(false);  // REMOVE
    sendSignal(apiConnectptr.p->ndbapiBlockref, GSN_TCROLLBACKREP, 
               signal, 4, JBB);
    return;
  }
```

**New:** Disconnect sender (no TCROLLBACKREP needed — connection being torn down).

```cpp
  case 2:{
    jam();
    NodeId senderNodeId = refToNode(apiConnectptr.p->ndbapiBlockref);
    disconnectMaliciousNode(signal, senderNodeId,
        "start flag during active abort (protocol error)", __LINE__);
    return;
  }
```

---

## Resource Cleanup Rules

| Change | Position in function | Cleanup needed |
|--------|---------------------|----------------|
| 2a, 2b, 2c | Before `seizeTcRecord` (line 4100) | `releaseSections(handle)` only |
| 2d, 2e, 2f | After `seizeTcRecord` | `terrorCode = ZSIGNAL_ERROR` + `releaseAtErrorLab()` |
| 2g | In `TCKEY_abort` | Resources managed by caller |

---

## What NOT to Change (Internal Invariants)

These `ndbrequire`/`ndbassert` calls guard internal state unreachable from external input. They should remain as crashes to detect genuine TC bugs:

| Line | Check | Why it's internal |
|------|-------|-------------------|
| 3663 | `m_num_queued_outstanding > 0` | Internal counter set by TC itself |
| 3665 | `m_outstanding_queries > 0` | Internal counter |
| 3876 | `apiCopyRecord != RNIL` | State machine guarantees this after CS_STARTED |
| 4067 | `TstartFlag == 1` | Switch-case above already validated this |
| 4250, 4255, 4259 | Trigger state fields | Internal trigger execution path |
| 4386 | `handle.getSection(keyInfoSection, 0)` | isLongTcKeyReq guarantees sections exist |
| 4392 | `handle.getSection(attrInfoSection, 1)` | attrlength>0 guarantees section exists |
| 4533 | Marker not in hash | Internal data structure |

---

## Verification

1. Build: `cmake -DWITH_NDB=1 -DWITH_NDB_TEST=1 ...` and `make`
2. Run MTR: `mtr ndb_basic`, `mtr ndb_ddl` for regression testing
3. Verify cluster log warnings appear with node type info
4. For each changed path, verify sender gets disconnected instead of data node crashing
