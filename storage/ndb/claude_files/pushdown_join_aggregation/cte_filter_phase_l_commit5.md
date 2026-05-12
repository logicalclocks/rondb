# Phase L commit 5 — cleanup follow-up

After Phase L commits 1-4 landed (kernel idempotency + single-owner LDM
routing + DBTC AggCompleteRecord + MTR/block-test coverage), two
pieces of legacy state are now strictly redundant. This commit removes
them.

## What is removed

### 1. `JoinAggregationState::m_redist_mutex` (DBLQH)

Single-owner LDM routing (Phase L commit 1, E.1) routes every signal
that mutates an aggregation's `JoinAggregationState`
— `JOIN_AGG_COMPLETE_REQ`, `JOIN_AGG_REDISTRIBUTE_REQ`,
`JOIN_AGG_REDISTRIBUTE_CONF/REF`, `JOIN_AGG_FINAL_REP`, and the
`CONTINUEB` redistribute-queue drain — to the LDM instance returned in
`JOIN_AGG_SETUP_CONF`'s `ownerInstance`. With only one LDM instance
ever touching a given state, the mutex protects nothing.

Removed:
- `NdbMutex m_redist_mutex` field in `JoinAggregationState.hpp`.
- `NdbMutex_Init` from constructor and `Dblqh::execJOIN_AGG_SETUP_REQ`
  CTE-mode block in `DblqhProxy.cpp:2444`.
- `NdbMutex_Deinit` from destructor.
- All `NdbMutex_Lock` / `NdbMutex_Unlock` pairs in
  `Dblqh::execJOIN_AGG_REDISTRIBUTE_REQ` (5 sites) and
  `Dblqh::processRedistQueue` (4 sites).
- `#include <NdbMutex.h>` from `JoinAggregationState.hpp` (the one
  user is gone).
- Stale comments referring to mutex serialization in
  `execJOIN_AGG_REDISTRIBUTE_REQ` and `processRedistQueue`; replaced
  with notes explaining the owner-routing invariant.

The `ndbassert(state->m_owner_instance == instance())` checks already
in place at every former mutex acquisition site (Phase L commit 1)
remain — they prove the invariant on every signal entry.

### 2. `ScanRecord::m_cteCompleteOutstanding` (DBTC)

Phase L commit 2 introduced `AggCompleteRecord::m_outstanding`
(per-aggregation-record per-node bitmask + count) and
`ScanRecord::m_ctePhaseRemaining` (per-CTE-phase pending-record
count). Together they fully replace the legacy scan-level
`m_cteCompleteOutstanding` counter that previously tracked the sum of
per-node CONFs across all CTEs in the current phase.

Removed:
- `m_cteCompleteOutstanding` field declaration in
  `Dbtc.hpp:2062`.
- Initialiser in `Dbtc::createScanRec`
  (`DbtcMain.cpp:16332`).
- Reset in `sendCteCompleteReqsForPhase` (line 29714) — already
  redundant alongside `m_ctePhaseRemaining = 0`.
- Per-node increment in the CTE COMPLETE_REQ send loop
  (line 29833).
- Mirror-write decrements in `execJOIN_AGG_COMPLETE_CONF`
  (lines 29423-29424) and `execJOIN_AGG_COMPLETE_REF`
  (lines 29516-29517).
- The `m_cteCompleteOutstanding == 0` early-out check (line 29845)
  is replaced by `m_ctePhaseRemaining == 0`.

The legacy main-aggregation counters `m_aggNodesOutstanding` /
`m_joinAggNodes->m_aggNodesPending` are intentionally **not**
retired in this commit. They are still consulted by the DBTC
node-failure path (`handleJoinAggNodeFailure`) and the plan defers
full per-record node-failure accounting. The mirror writes for
`KIND_MAIN` records in the COMPLETE_CONF/REF handlers therefore stay,
guarded by a `if (rec.p->m_kind == KIND_MAIN)` so CTE records no
longer touch them.

## Files

- `storage/ndb/src/kernel/blocks/dblqh/JoinAggregationState.hpp`
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`
- `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp`
- `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`
- `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ndbd ndbmtd \
                         testJoinAggIdempotency \
                         testCteLookup testStarJoinAgg
cd debug_build/mysql-test
./mtr --suite=ronsql                                     # full suite
./mtr --suite=ndb_push_agg                               # block tests
./mtr --suite=ndb_push_agg testJoinAggIdempotency \
      --mysqld=--ndb-num-lqh-workers=4                   # D11 stress
```

No functional change expected — only redundant state removed.

## Out of scope

- Per-aggregation-record node-failure correctness (legacy
  `m_aggNodesOutstanding` / `m_aggNodes` / `m_aggNodesPending` mirror
  writes for `KIND_MAIN` stay).
- `WAIT_CTE_COMPLETE` / `WAIT_JOIN_AGG_COMPLETE` `scanState` checks
  remain as broad scan-lifecycle guards (not per-CONF authority).
