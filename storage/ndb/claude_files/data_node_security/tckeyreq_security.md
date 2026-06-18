# Secure TCKEYREQ Signal Handling in Dbtc

**Status: IMPLEMENTED** — all call sites migrated to `reportMaliciousSignal()` in the v2 framework. This doc is a historical reference for the original plan; see [audit_implementation_guide.md](audit_implementation_guide.md) for the current API.

---

## Problem

The `execTCKEYREQ` function in Dbtc handled malformed signals unsafely:

| Original mechanism | Behavior | Problem |
|---|---|---|
| `warningHandlerLab` | No-op in production, debug-only log | Malicious sender faces no consequences |
| `ndbrequire` | Crashes the data node | Malicious sender can take down the cluster |
| `ndbassert` | Crashes in debug only | Inconsistent between debug/release |

Both API nodes and compromised data nodes (`ndbmtd`) can send malformed signals. The correct response is to **disconnect the sender and log to the cluster log**, not crash.

## Key Files

- `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — main implementation
- `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` — declarations

## Implemented Approach (v2 framework)

All 23 DBTC call sites now call `reportMaliciousSignal(signal, offendingNodeId, VT_*)` — a protected method on `SimulatedBlock` that sends `GSN_MALICIOUS_SIGNAL_REPORT` to QMGR. QMGR then decides whether to disconnect based on the violation tier and `EnableSecurityDisconnect`.

The old `disconnectMaliciousNode()` function and `REPORT_MALICIOUS_SIGNAL` macro described in this plan were superseded by the framework. Call sites look like:

```cpp
if (unlikely(/* violation condition */)) {
  jam();
  releaseSections(handle);  // if before seizeTcRecord
  reportMaliciousSignal(signal, offendingNodeId,
                        ViolationType::VT_INVALID_APICONNECTPTR_IN_TCKEYREQ);
  return;
}
```

For call sites after `seizeTcRecord`, add `terrorCode = ZSIGNAL_ERROR` + `releaseAtErrorLab()` before the report (see resource cleanup rules below).

---

## Existing Disconnect Mechanisms

| Node type | Mechanism | QMGR handler |
|---|---|---|
| API | `api_failed()` via QMGR's Tier A path | API_FAILREQ broadcast + transport close |
| Data | `node_failed()` via QMGR's Tier A path | CLOSE_COMREQ → DISCONNECT_REP → full failure protocol |

QMGR handles both node types uniformly once `reportMaliciousSignal()` delivers the `GSN_MALICIOUS_SIGNAL_REPORT`.

---

## Migration: execTCKEYREQ Validation Points

Below is the original plan with the current implementation status. All sites are migrated.

### 2a. Invalid API connect record pointer

**Original issue:** `ndbrequire(!passQueueingFlag)` + `warningHandlerLab` (crash / no-op)

**Implemented:** `reportMaliciousSignal` with `VT_INVALID_APICONNECTPTR_IN_TCKEYREQ` (Tier A). NodeId from `signal->getSendersBlockRef()` (no valid apiConnectptr available at this point).

### 2b. Table index out of bounds

**Original issue:** `ndbrequire + warningHandlerLab + conditional TCKEY_abort(69)`

**Implemented:** `reportMaliciousSignal` with `VT_TABLE_INDEX_OOB_IN_TCKEYREQ` (Tier A). NodeId from `regApiPtr->ndbapiBlockref`.

### 2c. Signal length mismatch (long and short variants)

**Original issue:** Missing checks, silent

**Implemented:** `reportMaliciousSignal` with `VT_TCKEYREQ_LONG_LENGTH_MISMATCH` / `VT_TCKEYREQ_SHORT_LENGTH_MISMATCH` (Tier B — version-skew plausible).

### 2d. Reorg flag with invalid operation type

**Original issue:** `ndbassert(false)` (crash in debug / silent in release)

**Implemented:** `reportMaliciousSignal` with `VT_REORG_FLAG_INVALID_OP_TYPE` (Tier B). Includes `terrorCode = ZSIGNAL_ERROR` + `releaseAtErrorLab()`.

### 2e. UNLOCK without distribution key

**Original issue:** `ndbassert(distributionKeyIndicator)`

**Implemented:** `reportMaliciousSignal` with `VT_UNLOCK_WITHOUT_DIST_KEY` (Tier B). Includes resource cleanup.

### 2f. CommitFlag without ExecFlag

**Original issue:** `ndbrequire(TexecFlag)` (crash)

**Implemented:** `reportMaliciousSignal` with `VT_COMMITFLAG_WITHOUT_EXECFLAG` (Tier B). Includes resource cleanup.

### 2g. Start flag during active abort (TCKEY_abort case 2)

**Original issue:** `ndbassert(false)` in TCKEY_abort

**Implemented:** `reportMaliciousSignal` with `VT_START_FLAG_DURING_ABORT` (Tier A — state machine attack). No TCROLLBACKREP needed (connection being torn down).

---

## Resource Cleanup Rules

| Change | Position in execTCKEYREQ | Cleanup needed before reportMaliciousSignal |
|--------|--------------------------|---------------------------------------------|
| 2a, 2b, 2c | Before `seizeTcRecord` | `releaseSections(handle)` only |
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
2. Run MTR: `./mtr --suite=ndb ndb_security` — covers Tier A/B injection, counter increments, and kill-switch behavior
3. For each changed path, verify the sending node is disconnected (Tier A) or stays connected with a log entry (Tier B) instead of the data node crashing
