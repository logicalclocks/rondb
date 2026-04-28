# CTE filter Phase L — JOIN_AGG_COMPLETE robustness: idempotency, unified requestId correlation, single-owner COMPLETE routing

## Status

**Plan only.** No code yet. Targets a multi-batch chained-CTE race
discovered while running `ronsql_cte_multi_batch` post Phase K. The
race is timing-dependent and reproduced once in ~50 attempts during
Phase K verification (see commit `3bc18e41054` for the diagnostic
helpers).

## Context

`ronsql_cte_multi_batch` Test 3 — chained CTE with > 256 groups per
phase — sometimes crashes DBTC with:

```
DBTC (Line: 29314) Check scanptr.p->scanState ==
ScanRecord::WAIT_JOIN_AGG_COMPLETE failed
```

Investigation (full debug log + trace files) showed:

```
19.381800 send JOIN_AGG_COMPLETE_REQ aggKey=2 (CTE 'a', phase 0)
19.382612 CONF nodeId=2 → outstanding 2→1
19.382887 CONF nodeId=1 → outstanding 1→0 → cteAdvancePhase
19.383387 send JOIN_AGG_COMPLETE_REQ aggKey=0 (CTE 'b', phase 1)
19.383699 CONF nodeId=2 → outstanding 2→1
19.383703 CONF nodeId=2 → outstanding 1→0 (DUPLICATE!)
19.383703 sendCteStartMainReqs (state advances out of WAIT_CTE_COMPLETE)
19.383972 CONF nodeId=1 → scanState != WAIT_CTE_COMPLETE → ASSERT
```

Node 2 sent **two** `JOIN_AGG_COMPLETE_CONF`s for the same phase-1
request. DBTC counted both, advanced state, and then crashed when
the legitimate node-1 reply arrived.

The duplicate originates in DBLQH:

1. `checkCteReady` (`DblqhMain.cpp:20997`) is **non-idempotent**.
   Both its preconditions — `m_cte_redistribution_done == true` and
   "all remote nodes have set their bit in `m_cte_nodes_finalized`"
   — are *sticky*. Once met, every subsequent invocation re-fires
   the COMPLETE_CONF, even after state has transitioned to
   `CTE_READY`.

2. `continueJoinAggRedistribute` (`DblqhMain.cpp:20580`) is
   **non-idempotent**. Re-entry after `redistribution_done` already
   fired (via CONTINUEB or REDISTRIBUTE_CONF) re-traverses the
   now-empty `gb_map`, falls through to `redistribution_done`,
   re-broadcasts FINAL_REPs to every remote, and re-calls
   `checkCteReady`.

3. **DBTC `ScanRecord` shares state across all CTE phases.** A
   single `m_cteCompleteOutstanding` counter and a single
   `scanState` are used for every CTE in every phase. There is no
   way to distinguish a stale CONF (left over from a finished
   phase, a duplicate, or a re-issued REQ) from a legitimate one —
   DBTC just decrements the counter and advances.

4. **`JoinAggregationState` is accessed concurrently from multiple
   LDM threads** without consistent serialization.
   `m_redist_mutex` is held only inside
   `execJOIN_AGG_REDISTRIBUTE_REQ`; every other path
   (`JOIN_AGG_COMPLETE_REQ`, `JOIN_AGG_FINAL_REP`,
   `JOIN_AGG_REDISTRIBUTE_CONF/REF`, `CONTINUEB` continuations,
   `checkCteReady`) reads / mutates the same fields with no lock.
   The result is gb_map iterator invalidation, half-set bitmask
   reads, racing `m_state` transitions, and ultimately the
   duplicate CONFs that A and B describe.

Phase L addresses all four together. They are independent fixes but
each contributes to a single coherent story: the JOIN_AGG_COMPLETE
phase must be idempotent, scoped per aggregation-completion record,
and owned by one DBLQH thread per aggregation state.

Important correlation rule: every `JOIN_AGG_COMPLETE_REQ`, including
non-CTE main SELECT aggregation, uses the existing `requestId` field
to carry an encoded DBTC aggregation-completion record id instead of
`scanApiRec`. DBLQH already echoes `requestId` in CONF/REF, so no
COMPLETE wire-format extension is needed. DBTC resolves the encoded
`requestId` to the completion record, and the record then supplies
the owning `ScanRecord`, aggregation kind (main or CTE), CTE index
and phase when applicable, expected node, expected `aggStateKey`,
error state, and phase/main-query bookkeeping.

## Goal

Make `JOIN_AGG_COMPLETE` reliable under multi-batch chained-CTE
load. Concretely:

- DBLQH never sends duplicate `JOIN_AGG_COMPLETE_CONF` for a single
  CTE's `JOIN_AGG_COMPLETE_REQ` (A, B).
- DBLQH never broadcasts duplicate FINAL_REPs (B).
- DBTC tracks every aggregation completion independently using
  `requestId` as the completion record id; stale or duplicate CONFs
  are detected and dropped (C).
- `CTE_PHASE_COMPLETE_REP` is correlated to the active CTE phase
  record and deduplicated before DBTC sends any COMPLETE_REQs (C.2).
- All COMPLETE / REDISTRIBUTE / FINAL_REP actions for a given
  `aggStateKey` run on the same DBLQH worker thread (E).

## Five sub-phases — A, B, C, D, E

A and B are surgical kernel idempotency fixes. C is a DBTC
correlation cleanup. D is the test coverage. E is the single-owner
DBLQH routing needed to make the state transitions race-safe. Land
in commit order: 1=A+B+E core routing, 2=C+C.2+D7-D9, 3=D1-D6
MTR coverage, 4=D11 concurrency coverage, then 5=E cleanup/audit
follow-up.

---

### A — `checkCteReady` idempotency

`storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp:20997`

```cpp
void Dblqh::checkCteReady(Signal *signal, JoinAggregationState *state) {
  if (state->m_state.load() == JoinAggregationState::CTE_READY) {
    jam();
    return;  // already sent COMPLETE_CONF
  }
  if (!state->m_cte_redistribution_done) {
    jam();
    return;
  }
  // existing per-node loop, return early if any remote not finalized
  // ...
  state->m_state.store(JoinAggregationState::CTE_READY);
  // existing send COMPLETE_CONF
}
```

The state-load guard makes every subsequent invocation a no-op
once the CONF has been sent. Cheap, defensive, idempotent.

This is only the final guard. Every CTE state transition in the
COMPLETE path must check the current state before acting:

- `SETUP_COMPLETE -> FINALIZING` only from the initial
  `JOIN_AGG_COMPLETE_REQ`.
- `FINALIZING -> SENDING_RESULTS` only after merge/finalize.
- `SENDING_RESULTS -> CTE_REDISTRIBUTING` only for multi-node CTEs.
- `CTE_REDISTRIBUTING -> CTE_READY` only after local redistribution
  and all remote FINAL_REPs are observed.
- A duplicate entry into any earlier phase after `CTE_READY` returns
  without changing state and without sending another CONF.

The implementation should not move the state backwards. In
particular, `execJOIN_AGG_COMPLETE_REQ` must check the current state
before the current unconditional `FINALIZING` store.

### B — `continueJoinAggRedistribute` idempotency

`storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp:20580`

```cpp
void Dblqh::continueJoinAggRedistribute(Signal *signal,
                                         Uint32 aggStateKey) {
  JoinAggregationState *state = getJoinAggState(aggStateKey);
  ndbrequire(state != nullptr);
  if (state->m_state.load() != JoinAggregationState::CTE_REDISTRIBUTING) {
    jam();
    return;  // stale continuation or duplicate entry
  }
  if (state->m_cte_redistribution_done) {
    jam();
    return;  // redistribution already finished — duplicate entry
  }
  // existing node-failure check, gb_map iteration, etc.
  // ...
}
```

Prevents:
- Re-broadcasting FINAL_REPs to remote nodes (which would
  re-trigger their `checkCteReady` and propagate the duplicate-CONF
  bug across nodes).
- Re-calling `checkCteReady` from a stale code path.
- Re-entering redistribution from an old CONTINUEB /
  REDISTRIBUTE_CONF after the state has already left
  `CTE_REDISTRIBUTING`.

### C — Unified aggregation-completion state in DBTC

**Problem:** today DBTC has separate accounting paths for main
aggregation and CTE materialization. CTEs share
`m_cteCompleteOutstanding`, while main SELECT aggregation uses
`m_aggNodesOutstanding` / `m_joinAggNodes->m_aggNodesPending` and
`WAIT_JOIN_AGG_COMPLETE`. This leaves two subtly different reply
paths and makes it too easy for CTE fixes to miss the main path, or
vice versa.

**Fix:** introduce one aggregation-completion record type, linked
from `ScanRecord`, and use the existing
`JoinAggCompleteReq::requestId` as an encoded record id for **all**
JOIN_AGG_COMPLETE requests. CTE materialization records carry CTE
index and phase. Main SELECT aggregation records carry
`m_cteIndex = RNIL` and `m_phase = RNIL`. DBLQH echoes `requestId`
in `_CONF` / `_REF`. DBTC decodes the record id and always handles
the reply through the same record-state machine.

`WAIT_CTE_COMPLETE` and `WAIT_JOIN_AGG_COMPLETE` remain as logical
states, but they move from `ScanRecord::scanState` to the
aggregation-completion record. `scanState` can still be used as a
broad scan lifecycle guard; it is no longer the source of truth for
whether an individual COMPLETE reply is valid.

**New struct** in `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`:

```cpp
struct AggCompleteRecord {
  Uint32 m_magic;
  Uint32 m_nextI;                              // singly-linked, RNIL = tail
  Uint32 m_scanPtrI;                           // owning ScanRecord
  enum Kind {
    KIND_MAIN = 0,
    KIND_CTE = 1
  } m_kind;
  Uint32 m_cteIndex;                           // index into m_cteInfos
  Uint32 m_phase;                              // CTE phase
  Uint32 m_aggStateKeys[MAX_NDB_NODES];        // per-node aggKey
  NdbNodeBitmask m_aggNodesPending;            // CONFs still expected
  Uint32 m_outstanding;                        // |m_aggNodesPending|
  enum State {
    REC_IDLE = 0,
    REC_WAIT_COMPLETE = 1,
    REC_COMPLETE = 2,
    REC_FAILED = 3
  } m_state;
  Uint32 m_errorCode;
};
```

**Pool:** `TransientPool<AggCompleteRecord>` mirroring existing
scan/api pools.

**`ScanRecord` additions:**

```cpp
Uint32 m_aggRecordsHead;     // RNIL or first record (linked list head)
Uint32 m_aggRecordsCount;    // total live records on this scan
Uint32 m_mainAggRecI;        // main SELECT aggregation record, RNIL if none
Uint32 m_ctePhaseRemaining;  // records in current phase not yet complete
                             //   — phase-advance trigger
Uint32 m_cteActivePhaseRecI; // current CtePhaseRecord, RNIL outside CTE phase
```

Replaces `m_cteCompleteOutstanding`, `m_aggNodesPending`, and
`m_aggNodesOutstanding` for COMPLETE accounting. RELEASE accounting
can continue to use the existing per-scan node set until release is
separately refactored.

**`Dbtc::sendCteCompleteReqsForPhase`** (line 29522) changes:

- Set `m_ctePhaseRemaining = 0` (per phase, not per scan).
- For each CTE in the phase:
  - Allocate or reset an `AggCompleteRecord`.
  - Populate `m_kind = KIND_CTE`, `m_phase`, `m_cteIndex`,
    `m_aggStateKeys[]`,
    `m_aggNodesPending` from `cteNodes->m_aggNodes`,
    `m_outstanding = popcnt`, `m_scanPtrI = scanptr.i`,
    `m_state = REC_WAIT_COMPLETE`.
  - Append to `scanptr.p->m_aggRecordsHead`.
  - Increment `m_ctePhaseRemaining`.
- For each node in the CTE: send `JOIN_AGG_COMPLETE_REQ`
  with:

```cpp
req->senderData = scanptr.i;       // compatibility / validation
req->requestId = makeAggCompleteRequestId(aggRec.i);
req->aggStateKey = cteNodes->m_aggStateKeys[nodeId];
```

The aggregation-completion record owns the `m_outstanding` count instead of
`scanptr.p->`.

**`Dbtc::sendJoinAggCompleteReqs`** (main SELECT aggregation) changes
similarly:

- Allocate or reset one `AggCompleteRecord`.
- Populate `m_kind = KIND_MAIN`, `m_cteIndex = RNIL`,
  `m_phase = RNIL`, `m_scanPtrI = scanptr.i`, `m_aggStateKeys[]`
  from `scanptr.p->m_joinAggNodes`, `m_aggNodesPending` from
  `m_joinAggNodes->m_aggNodes`, and `m_state = REC_WAIT_COMPLETE`.
- Store it in `scanptr.p->m_mainAggRecI` and link it from
  `m_aggRecordsHead`.
- Send every main `JOIN_AGG_COMPLETE_REQ` with
  `req->requestId = makeAggCompleteRequestId(mainAggRec.i)`.

This means DBTC has one COMPLETE reply path for CTE and non-CTE
queries. The only difference is what completion of the record drives:
`KIND_CTE` decrements `m_ctePhaseRemaining`; `KIND_MAIN` triggers
`sendJoinAggReleaseReqs`.

**`Dbtc::execJOIN_AGG_COMPLETE_CONF`** (around line 29283):

```cpp
const Uint32 senderNodeId = refToNode(conf->senderRef);

AggCompleteRecordPtr rec;
if (isAggCompleteRequestId(conf->requestId)) {
  rec.i = decodeAggCompleteRequestId(conf->requestId);
  if (!getValidAggCompleteRecord(rec)) {
    return;  // stale record id
  }
  if (rec.p->m_state != AggCompleteRecord::REC_WAIT_COMPLETE) {
    // stale duplicate after this aggregation was already completed/failed
    return;
  }

  ScanRecordPtr scanptr;
  scanptr.i = rec.p->m_scanPtrI;
  if (!scanRecordPool.getValidPtr(scanptr) ||
      conf->senderData != scanptr.i ||
      rec.p->m_aggStateKeys[senderNodeId] == 0 ||
      !rec.p->m_aggNodesPending.get(senderNodeId)) {
    // stale, wrong scan, wrong node, or duplicate node reply
    return;
  }

  rec.p->m_aggNodesPending.clear(senderNodeId);
  rec.p->m_outstanding--;
  if (rec.p->m_outstanding == 0) {
    rec.p->m_state = AggCompleteRecord::REC_COMPLETE;
    if (rec.p->m_kind == AggCompleteRecord::KIND_CTE) {
      scanptr.p->m_ctePhaseRemaining--;
      if (scanptr.p->m_ctePhaseRemaining == 0) cteAdvancePhase(signal, scanptr);
    } else {
      sendJoinAggReleaseReqs(signal, scanptr);
    }
  }
  return;
}

// Unknown/non-record requestId. This is stale or malformed in Phase L.
return;
```

The scan-level `WAIT_CTE_COMPLETE` and `WAIT_JOIN_AGG_COMPLETE`
assertions go away for COMPLETE CONFs. The aggregation-completion
record state is the source of truth, and stale CONFs are detected by
record lookup miss, scan mismatch, wrong node, or already-cleared
pending bit. Phase-1 CONFs that arrive while scan state has already
advanced still find their own record and are handled or dropped
deterministically.

**`Dbtc::execJOIN_AGG_COMPLETE_REF`:** same lookup pattern. Set
`rec->m_state = REC_FAILED`, `rec->m_errorCode`, clear the node
pending bit if it was still pending, decrement counters, and bubble
up to the CTE or main aggregation abort/release path according to
`rec->m_kind`.

**Cleanup:** records stay linked across phase boundaries (needed
for late-CONF deduplication). `releaseAggCompleteRecords(scanptr)`
walks the list and releases all records back to the pool, called
from scan teardown and the abort path.

#### C.2 — Phase-complete correlation

`CTE_PHASE_COMPLETE_REP` is a separate race source. Today it is
correlated only by `ScanFragRec.i` and a phase number, then counted
in shared scan-level counters. Phase reports must be deduplicated
before DBTC sends any `JOIN_AGG_COMPLETE_REQ`s.

Add a lightweight per-phase record:

```cpp
struct CtePhaseRecord {
  Uint32 m_magic;
  Uint32 m_scanPtrI;
  Uint32 m_phase;
  // Or use a ScanFragRec list if node bitmask is not precise enough.
  NdbNodeBitmask m_spjReportsPending;
  Uint32 m_reportsOutstanding;
  Uint32 m_firstAggRecI;               // Agg records for this phase
  enum State {
    PHASE_WAIT_REPORTS = 0,
    PHASE_WAIT_CTE_COMPLETE = 1,
    PHASE_COMPLETE = 2
  } m_state;
};
```

`execCTE_PHASE_COMPLETE_REP` resolves `rep->senderData` to
`ScanFragRec`, then `ScanRecord`, then the active `CtePhaseRecord`.
It must verify:

- `rep->phase == phaseRec.m_phase`.
- The reporting DBSPJ / `ScanFragRec` was pending for this phase.
- The phase record is still in `PHASE_WAIT_REPORTS`.

Only the first valid report from each expected DBSPJ instance clears
a pending slot. Duplicate or stale reports are logged and dropped.
When all reports are received, DBTC transitions the phase record to
`PHASE_WAIT_CTE_COMPLETE`, initializes the CTE aggregation records
for that phase, and sends `JOIN_AGG_COMPLETE_REQ`s carrying each
aggregation record id in `requestId`.

### D — Test coverage

#### D-MTR (functional regressions)

**D1 — Three-level chained CTE:**

`mysql-test/suite/ronsql/t/ronsql_cte_chained.test` (new file):

```sql
WITH a AS (SELECT grp AS k, SUM(val) AS s FROM cte_big GROUP BY grp),
     b AS (SELECT k, MAX(s) AS m FROM a GROUP BY k),
     c AS (SELECT k, MIN(m) AS mn FROM b GROUP BY k)
SELECT k, SUM(mn) FROM c GROUP BY k;
```

Three CTE phases → three independent chances for stale CONF to
cross phase boundaries. Hits **C** directly.

**D2 — Sibling CTEs in the same phase:**

```sql
WITH a AS (SELECT grp AS k, SUM(val) AS s FROM cte_big GROUP BY grp),
     b AS (SELECT grp AS k, COUNT(*) AS n FROM cte_big GROUP BY grp)
SELECT a.k, SUM(a.s), SUM(b.n)
FROM cte_parent p
JOIN a ON a.k = p.grp
JOIN b ON b.k = p.grp
GROUP BY a.k;
```

Two independent CTEs at phase 0 → DBTC fans out 4
`JOIN_AGG_COMPLETE_REQ`s. With the legacy shared counter, a stale
CONF from `a` bleeds into `b`'s accounting.

**D3 — Skewed-key chain (asymmetric redistribution):**

Insert data so one node holds 95% of CTE 'a's groups, the other
5%. Build CTE 'b' from 'a'. Node 2's gb_map for 'b' starts almost
empty → hits the empty-gb_map → `goto redistribution_done` path
that **A**+**B** guard.

**D4 — Empty-intermediate chain:**

```sql
WITH a AS (SELECT grp AS k, SUM(val) AS s FROM cte_big WHERE val < 0
          GROUP BY grp),
     b AS (SELECT k, MAX(s) AS m FROM a GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k;
```

`a` is empty on every node → `b` is empty → both phases hit the
empty-gb_map fast path on every node. Maximum exposure to **A**.

**D5 — Many small batches:**

Bump `cte_big` to 5000 groups, set RonSQL batch size low (or use
default and let the kernel batching trigger). Many CONTINUEBs in
`continueJoinAggRedistribute` exercise **B**'s re-entry guard.

**D6 — Stress loop:**

Wrap existing `ronsql_cte_multi_batch` Tests 1-4 in a `while $i <=
50` MTR loop. With the original race needing ~50 attempts to
repro, this gives ≥ 1 hit per MTR run on an unfixed tree —
strong negative test against fixed trees.

#### D-block (deterministic, in `storage/ndb/block_unit_test/`)

New file `testJoinAggIdempotency.cpp`:

**D7 — `testJoinAggDuplicateFinalRep`:** drives a 2-node CTE
materialization, injects a duplicate `JOIN_AGG_FINAL_REP` after
the legitimate one. Asserts DBTC sees exactly one
`JOIN_AGG_COMPLETE_CONF`. Pure black-box test of **A**.

**D8 — `testJoinAggRedistributeReentrancy`:** synthetic
`CONTINUEB` injection after `redistribution_done`. Asserts no
duplicate FINAL_REP outbound and no duplicate CONF inbound. Tests
**B**.

**D9 — `testJoinAggCrossPhaseStaleConf`:** chained-CTE setup;
replay phase-0 CONF after phase 1 has started. With **C**, DBTC
decodes `requestId`, finds that the aggregation record is already
complete, and drops the stale reply. Without **C**, the shared scan
counter path asserts.

**D11 — `testJoinAggConcurrentRedistribute`:** 4-LDM-thread
cluster. Fire `JOIN_AGG_COMPLETE_REQ` and
`JOIN_AGG_REDISTRIBUTE_REQ` at different LDM instances
simultaneously, with ERROR_INSERT µs jitter between handlers. Run
1000 iterations. Without **E**, expect intermittent corruption;
with **E**, all 1000 pass.

### E — Single-owner DBLQH routing for COMPLETE state

`JoinAggregationState` is accessed concurrently by:

| Signal | Origin | Touches |
|---|---|---|
| `JOIN_AGG_COMPLETE_REQ` | DBTC | merge → finalize → `gb_map` → `m_cte_redistribution_done` → `checkCteReady` |
| `JOIN_AGG_FINAL_REP` | remote DBLQH | `m_cte_nodes_finalized` → `checkCteReady` |
| `JOIN_AGG_REDISTRIBUTE_REQ` | remote DBLQH | `gb_map` insert via `processRedistQueue`, `m_redist_queue_*` |
| `JOIN_AGG_REDISTRIBUTE_CONF/REF` | remote DBLQH | `m_cte_waiting_conf` → re-enter `continueJoinAggRedistribute` |
| `CONTINUEB(ZCONTINUE_JOIN_AGG_REDISTRIBUTE)` | self | full redistribution iteration |
| `CONTINUEB(ZCONTINUE_JOIN_AGG_MERGE)` | self | merge phase |

`m_redist_mutex` is held only inside `execJOIN_AGG_REDISTRIBUTE_REQ`
today. Every other path is unprotected. Rather than extending the
mutex across a large signal graph, Phase L makes a stricter routing
rule: for a given destination `aggStateKey`, all COMPLETE /
REDISTRIBUTE / FINAL_REP work runs on one owner LDM thread.

#### E.1 — Owner calculation

Each `JoinAggregationState` gets a stable owner instance at setup
time:

```cpp
ownerInstance = (aggStateKey % lqhWorkersOnNode) + 1;
```

`JOIN_AGG_SETUP_CONF` must return the owner instance together with
the `aggStateKey`, or DBTC must be able to recompute the exact same
owner from node worker count and `aggStateKey`. Prefer echoing it in
SETUP_CONF so routing does not depend on duplicate worker-count
logic in every sender.

DBTC stores per-node:

```cpp
cteNodes->m_aggStateKeys[nodeId]
cteNodes->m_aggOwnerInstances[nodeId]
```

The main aggregation path can keep existing routing initially, but
the CTE COMPLETE path must use owner routing from this phase onward.

#### E.2 — DBTC sends COMPLETE_REQ to the owner

`sendCteCompleteReqsForPhase` no longer sends CTE COMPLETE_REQ via
round-robin V_QUERY. For each node:

```cpp
Uint32 owner = cteNodes->m_aggOwnerInstances[nodeId];
BlockReference ref = numberToRef(DBLQH, owner, nodeId);
req->requestId = makeCteCompleteRequestId(cteRec.i);
req->aggStateKey = cteNodes->m_aggStateKeys[nodeId];
sendSignal(ref, GSN_JOIN_AGG_COMPLETE_REQ, ...);
```

This guarantees `execJOIN_AGG_COMPLETE_REQ`,
`continueJoinAggMerge`, `continueJoinAggRedistribute`, and
`checkCteReady` execute on the owner instance for that state.

#### E.3 — DBLQH sends REDISTRIBUTE / FINAL_REP to destination owners

For remote redistribution, route by the **destination** state key,
not by the sender's local key:

```cpp
Uint32 dstKey = state->m_cte_remote_aggKeys[ownerNode];
Uint32 dstOwner = state->m_cte_remote_ownerInstances[ownerNode];
BlockReference remoteRef = numberToRef(DBLQH, dstOwner, ownerNode);
req->aggStateKey = dstKey;
sendSignal(remoteRef, GSN_JOIN_AGG_REDISTRIBUTE_REQ, ...);
```

The same rule applies to `JOIN_AGG_FINAL_REP`: the receiver must get
a FINAL_REP addressed to its local `aggStateKey` and owner instance.
Do not rely on pool indexes being equal across data nodes.

#### E.4 — Mutex status after owner routing

With owner routing, `m_redist_mutex` should no longer be required
for CTE COMPLETE/redistribution state changes because only one LDM
instance touches a given `JoinAggregationState`. Keep existing
mutex use inside `execJOIN_AGG_REDISTRIBUTE_REQ` during the first
owner-routing implementation as a defensive assertion point, but
add debug checks that all CTE COMPLETE/REDISTRIBUTE/FINAL_REP
handlers execute on `state->m_cte_owner_instance`.

If any path cannot be routed to the owner in the first implementation
commit, either keep that path outside Phase L or protect it with a
small, local mutex section that does not include `sendSignal`.

## Files

- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` — A (line
  20997), B (line 20580), E owner-thread assertions and destination
  owner routing for REDISTRIBUTE / FINAL_REP.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp` — setup
  path stores / returns the owner LDM instance for each CTE
  `aggStateKey`.
- `storage/ndb/include/kernel/signaldata/JoinAgg.hpp` — if needed,
  extend SETUP_CONF with owner instance. COMPLETE_CONF/REF wire
  format is unchanged; `requestId` carries the aggregation-complete
  record id for both CTE and main SELECT complete requests.
- `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` —
  `AggCompleteRecord` and `CtePhaseRecord` structs, pool decls,
  per-node owner-instance storage, `ScanRecord` field additions.
- `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — pool init,
  `sendCteCompleteReqsForPhase` rewrite (line 29522),
  `execJOIN_AGG_COMPLETE_CONF/_REF` rewrite (around 29283),
  `execCTE_PHASE_COMPLETE_REP` deduplication, `cteAdvancePhase`
  driver swap, `releaseAggCompleteRecords` helper,
  `releaseCtePhaseRecords` helper.
- `mysql-test/suite/ronsql/t/ronsql_cte_chained.test` — new file
  with D1-D4.
- `mysql-test/suite/ronsql/r/ronsql_cte_chained.result` —
  recorded.
- `mysql-test/suite/ronsql/t/ronsql_cte_multi_batch.test` — append
  D5, D6 (stress loop).
- `mysql-test/suite/ronsql/r/ronsql_cte_multi_batch.result` —
  re-recorded.
- `storage/ndb/block_unit_test/testJoinAggIdempotency.cpp` — new
  file, hosts D7, D8, D9, D11.
- `storage/ndb/block_unit_test/CMakeLists.txt` — add target.
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md` —
  index entry for Phase L.

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2 \
                            testJoinAggIdempotency
cd debug_build/mysql-test

# A + B + E owner routing (commit 1)
./mtr --suite=ronsql ronsql_cte_multi_batch

# C + C.2 + D7-D9 (commit 2)
./mtr --suite=ndb_push_agg testJoinAggIdempotency

# D1-D6 (commit 3)
./mtr --suite=ronsql ronsql_cte_chained
./mtr --suite=ronsql ronsql_cte_multi_batch     # D6 stress wraps
                                                  # tests 1-4 × 50
./mtr --suite=ronsql                             # full suite — no
                                                  # regressions

# D11 (commit 4)
./mtr --suite=ndb_push_agg testJoinAggIdempotency \
      --mysqld=--ndb-num-lqh-workers=4

# E cleanup/audit follow-up (commit 5)
./mtr --suite=ronsql                             # full suite
./mtr --suite=ndb_push_agg                       # block tests
```

## Commit cadence

| Commit | Contents | Approx LoC |
|---|---|---|
| 1 | A + B + E owner routing for CTE COMPLETE/REDISTRIBUTE/FINAL_REP | ~200-350 |
| 2 | C + C.2 + D7-D9 (DBTC unified complete/phase state + idempotency block tests) | ~500-800 |
| 3 | D1-D6 (MTR functional regressions) | ~150 |
| 4 | D11 (concurrency block test) | ~150 |
| 5 | E cleanup/audit follow-up: debug owner assertions, remove redundant mutex use where proven | ~100-200 |

Plan-doc deliverables: this file (Phase L). Optionally a separate
`cte_agg_complete_state.md` for the deeper C details once
implementation starts.

## Risks

1. **C is a non-trivial DBTC refactor.** Pool init order,
   `requestId` compatibility, senderData validation, phase-record
   cleanup, and node-failure cleanup all need careful handling.
   Lands in its own commit so it's individually testable and
   revertable.

2. **Backward compatibility of signals.** Wire formats are
   mostly unchanged. COMPLETE_CONF/REF remain unchanged because
   `requestId` carries the aggregation-complete record id. SETUP_CONF may
   need an owner-instance field; if that is not acceptable for the
   branch, DBTC must recompute the owner deterministically from
   `aggStateKey` and node worker count.

3. **Pool sizing for `AggCompleteRecord`.** Per-scan record
   count is small (≤ MAX_CTES_PER_QUERY, ~10), plus one phase record
   per CTE phase. Existing transient pool sizing should accommodate
   this, but it must be sized explicitly rather than assumed.

4. **Owner-routing completeness.** Every signal that mutates CTE
   completion/redistribution state must route to the destination
   owner. Missing just one path reintroduces multi-LDM races. Add
   debug assertions in each handler that the current instance is the
   expected owner.

5. **Reproduction reliability.** The race is timing-sensitive
   (~1:50 reproduction without diagnostics). D6's 50× stress loop
   raises the chance to ≥ 1 per MTR run; D11's ERROR_INSERT-jittered
   block test is fully deterministic. Both are needed — D6 catches
   regressions in the integrated path, D11 catches them in
   isolation.

6. **Dropped signals.** With C's "drop stale CONF" behaviour, we
   silently lose signals that previously crashed the node. Logging
   under `DEB_JOIN_AGG` is mandatory at every drop site so a real
   protocol regression can still be diagnosed from production
   logs.

7. **Node failure.** Full per-aggregation-record node-failure accounting is
   deliberately deferred. During this phase, do not make the old
   scan-level node-failure logic pretend it has per-record precision.
   It is acceptable to fail/abort conservatively on node failure
   while the unified completion state model stabilizes.

## What we're not doing

- **JOIN_AGG_RELEASE accounting.** The `m_aggNodesOutstanding` /
  `m_joinAggNodes->m_aggNodesPending` machinery for the main-query
  release path stays as-is. It's a per-scan counter for a per-scan
  resource, not a per-CTE one — same architecture is correct
  there.

- **Refactoring `m_aggErrorCode`.** Today `scanptr.p->m_aggErrorCode`
  collects the first error from any CTE's failure across the scan.
  C makes per-aggregation-record error codes available
  (`AggCompleteRecord::m_errorCode`) but the propagation to the API
  layer keeps the existing single-error convention. Per-CTE error
  reporting to the API is out of scope.

- **Aggregator merge phase concurrency.**
  `continueJoinAggMerge`'s per-thread merge into interpreter[0]
  predates this work and uses its own protection model
  (single-threaded merge driven by CONTINUEB). E owner routing covers
  the cte_mode branch of `continueJoinAggMerge`; the non-cte path is
  unchanged.

- **CTE redistribution flow control.** The `needConf` /
  `m_cte_waiting_conf` pacing in `continueJoinAggRedistribute` is
  correct as-is and stays.

- **Full node-failure correctness for aggregation records.** This is
  important, but it is large enough to be its own follow-up. Phase L
  should not block on perfect node-failure recovery while fixing the
  deterministic duplicate/stale completion races.
