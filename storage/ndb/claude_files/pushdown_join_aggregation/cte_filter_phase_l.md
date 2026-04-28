# CTE filter Phase L — JOIN_AGG_COMPLETE robustness: idempotency, per-CTE state in DBTC, concurrent-access serialization

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
each contributes to a single coherent story: the CTE-completion
phase must be idempotent, scoped per-CTE, and free of memory
unsafety.

## Goal

Make `JOIN_AGG_COMPLETE` reliable under multi-batch chained-CTE
load. Concretely:

- DBLQH never sends duplicate `JOIN_AGG_COMPLETE_CONF` for a single
  CTE's `JOIN_AGG_COMPLETE_REQ` (A, B).
- DBLQH never broadcasts duplicate FINAL_REPs (B).
- DBTC tracks each CTE's completion independently; stale or
  duplicate CONFs are detected and dropped (C).
- All multi-LDM accesses to `JoinAggregationState`'s
  CTE-redistribution fields are serialized (E).

## Five sub-phases — A, B, C, D, E

A and B are surgical kernel fixes. C is a DBTC architectural
cleanup. D is the test coverage. E is the concurrency
serialization. Land in commit order: 1=A+B+D1-D6, 2=E.2,
3=C+D7-D9, 4=D11, then 5=E.1 (architectural follow-up).

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

### B — `continueJoinAggRedistribute` idempotency

`storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp:20580`

```cpp
void Dblqh::continueJoinAggRedistribute(Signal *signal,
                                         Uint32 aggStateKey) {
  JoinAggregationState *state = getJoinAggState(aggStateKey);
  ndbrequire(state != nullptr);
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

### C — Per-CTE completion state in DBTC

**Problem:** today `ScanRecord` holds a single
`m_cteCompleteOutstanding`, `m_aggNodesPending`,
`m_aggNodesOutstanding`, and `scanState` for *all* CTEs in *all*
phases. Late or duplicate CONFs mix into the wrong CTE's
accounting; `cteAdvancePhase` fires on a corrupt counter; the next
phase sees a `scanState` already advanced past
`WAIT_CTE_COMPLETE`.

**Fix:** introduce a per-CTE record, linked from `ScanRecord`.
Every `JOIN_AGG_COMPLETE_REQ` allocates one; every `_CONF` looks it
up by `(scanPtr, aggStateKey)`; phase advance is driven by "all
records in this phase complete?" rather than the global counter.

**New struct** in `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`:

```cpp
struct CteAggCompleteRecord {
  Uint32 m_magic;
  Uint32 m_nextI;                              // singly-linked, RNIL = tail
  Uint32 m_cteIndex;                           // index into m_cteInfos
  Uint32 m_phase;                              // CTE phase
  Uint32 m_aggStateKeys[MAX_NDB_NODES];        // per-node aggKey
  NdbNodeBitmask m_aggNodesPending;            // CONFs still expected
  Uint32 m_outstanding;                        // |m_aggNodesPending|
  bool   m_complete;
  bool   m_failed;
  Uint32 m_errorCode;
};
```

**Pool:** `TransientPool<CteAggCompleteRecord>` mirroring existing
scan/api pools.

**`ScanRecord` additions:**

```cpp
Uint32 m_cteRecordsHead;     // RNIL or first record (linked list head)
Uint32 m_cteRecordsCount;    // total live records on this scan
Uint32 m_ctePhaseRemaining;  // records in current phase not yet complete
                             //   — phase-advance trigger
```

Replaces `m_cteCompleteOutstanding`, `m_aggNodesPending`,
`m_aggNodesOutstanding` for the CTE side. The main-query agg path
keeps its existing single-set fields (it doesn't have the
per-CTE-multiplicity problem).

**`Dbtc::sendCteCompleteReqsForPhase`** (line 29522) changes:

- Set `m_ctePhaseRemaining = 0` (per phase, not per scan).
- For each CTE in the phase:
  - Allocate a `CteAggCompleteRecord`.
  - Populate `m_phase`, `m_cteIndex`, `m_aggStateKeys[]`,
    `m_aggNodesPending` from `cteNodes->m_aggNodes`,
    `m_outstanding = popcnt`.
  - Append to `scanptr.p->m_cteRecordsHead`.
  - Increment `m_ctePhaseRemaining`.
- For each node in the CTE: send `JOIN_AGG_COMPLETE_REQ`
  (signal-format unchanged). The per-CTE record owns the
  `m_outstanding` count instead of `scanptr.p->`.

**`Dbtc::execJOIN_AGG_COMPLETE_CONF`** (around line 29283):

```cpp
ScanRecordPtr scanptr;
scanptr.i = conf->senderData;
scanRecordPool.getPtr(scanptr);

if (scanptr.p->scanState == ScanRecord::WAIT_CTE_COMPLETE) {
  // CTE-side path — look up by aggStateKey
  CteAggCompleteRecord *rec = findCteCompleteRecord(scanptr,
                                                     conf->aggStateKey,
                                                     senderNodeId);
  if (rec == nullptr) {
    // stale duplicate — log + drop silently
    DEB_JOIN_AGG(("(%u) drop stale COMPLETE_CONF aggKey=%u node=%u",
                  instance(), conf->aggStateKey, senderNodeId));
    return;
  }
  if (!rec->m_aggNodesPending.get(senderNodeId)) {
    // already-counted node — duplicate CONF for this CTE
    DEB_JOIN_AGG(("(%u) drop duplicate COMPLETE_CONF aggKey=%u node=%u",
                  instance(), conf->aggStateKey, senderNodeId));
    return;
  }
  rec->m_aggNodesPending.clear(senderNodeId);
  rec->m_outstanding--;
  if (rec->m_outstanding == 0) {
    rec->m_complete = true;
    scanptr.p->m_ctePhaseRemaining--;
    if (scanptr.p->m_ctePhaseRemaining == 0) {
      cteAdvancePhase(signal, scanptr);
    }
  }
  return;
}

// Main-query agg path (unchanged): scanState must be
// WAIT_JOIN_AGG_COMPLETE.
ndbrequire(scanptr.p->scanState == ScanRecord::WAIT_JOIN_AGG_COMPLETE);
// ... existing main-query handling
```

The `WAIT_CTE_COMPLETE` precondition for the CTE path goes away —
the per-CTE record is the source of truth, and a stale CONF is
detected by lookup miss rather than by a state assertion. Phase-1
CONFs that arrive before phase 0 finishes find their per-CTE
record and slot in normally.

**`Dbtc::execJOIN_AGG_COMPLETE_REF`:** same lookup pattern. Set
`rec->m_failed`, `rec->m_errorCode`, decrement counters; bubble up
to the abort path when `m_ctePhaseRemaining == 0`.

**Lookup helper:**

```cpp
CteAggCompleteRecord *findCteCompleteRecord(ScanRecordPtr scanptr,
                                             Uint32 aggStateKey,
                                             Uint32 nodeId) {
  Uint32 i = scanptr.p->m_cteRecordsHead;
  while (i != RNIL) {
    CteAggCompleteRecord *rec = pool.getPtr(i);
    if (rec->m_aggStateKeys[nodeId] == aggStateKey) return rec;
    i = rec->m_nextI;
  }
  return nullptr;
}
```

≤ ~10 records per scan in practice; linear walk is fine.

**Cleanup:** records stay linked across phase boundaries (needed
for late-CONF deduplication). `releaseCteAggCompleteRecords(scanptr)`
walks the list and releases all records back to the pool, called
from scan teardown and the abort path.

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
replay phase-0 CONF after phase 1 has started. With **C**, DBTC's
`findCteCompleteRecord` returns nullptr → log + drop. Without
**C**, asserts.

**D11 — `testJoinAggConcurrentRedistribute`:** 4-LDM-thread
cluster. Fire `JOIN_AGG_COMPLETE_REQ` and
`JOIN_AGG_REDISTRIBUTE_REQ` at different LDM instances
simultaneously, with ERROR_INSERT µs jitter between handlers. Run
1000 iterations. Without **E**, expect intermittent corruption;
with **E**, all 1000 pass.

### E — Serialize concurrent multi-LDM access

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
today. Every other path is unprotected.

#### E.2 — Mutex extension (short-term, ship now)

Acquire `m_redist_mutex` at every entry that touches the CTE
fields, release at every exit. Specifically:

- `execJOIN_AGG_COMPLETE_REQ` (the `m_cte_mode` branch from line
  18378)
- `continueJoinAggMerge` (the cte_mode branch at line 18631+)
- `continueJoinAggRedistribute` (whole function body)
- `execJOIN_AGG_FINAL_REP`
- `execJOIN_AGG_REDISTRIBUTE_CONF` / `_REF`
- `checkCteReady`
- The `CONTINUEB` dispatchers (lines 1093, 1101) before calling
  the continue helpers

**Critical:** `sendSignal` while holding the mutex risks deadlock
if the recipient blocks on the same lock. Build the signal payload
under the lock, drop the lock, then `sendSignal`. Some helpers
(notably the FINAL_REP broadcast in `redistribution_done`) need to
be split into "build" and "send" halves.

#### E.1 — Single-owner LDM routing (long-term, follow-up)

Assign each `aggStateKey` an "owning" LDM instance at
`JOIN_AGG_SETUP_REQ` time
(`owner = aggStateKey % m_lqh_workers + 1`). Senders compute owner
before sending: DBTC's `sendCteCompleteReqsForPhase` and DBLQH's
remote `sendSignal`s for FINAL_REP / REDISTRIBUTE_REQ.

Eliminates the mutex entirely: only one LDM ever touches a given
state. Needs `JOIN_AGG_SETUP_CONF` to carry the owner instance so
senders know the routing.

Land E.1 as a separate commit after the rest of Phase L
stabilises. E.2 stays as the safety net in case any path is
missed during E.1's audit.

## Files

- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` — A (line
  20997), B (line 20580), E.2 (mutex extensions across ~6
  functions).
- `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` —
  `CteAggCompleteRecord` struct, pool decl, `ScanRecord` field
  additions.
- `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` — pool init,
  `sendCteCompleteReqsForPhase` rewrite (line 29522),
  `execJOIN_AGG_COMPLETE_CONF/_REF` rewrite (around 29283),
  `cteAdvancePhase` driver swap, `releaseCteAggCompleteRecords`
  helper, `findCteCompleteRecord` helper.
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

# A + B + D1-D6 (commit 1)
./mtr --suite=ronsql ronsql_cte_chained
./mtr --suite=ronsql ronsql_cte_multi_batch     # D6 stress wraps
                                                  # tests 1-4 × 50
./mtr --suite=ronsql                             # full suite — no
                                                  # regressions

# E.2 (commit 2) — same checks, expect no change in functional
# behaviour, just memory-safety guarantee under concurrency

# C + D7-D9 (commit 3)
./mtr --suite=ndb_push_agg testJoinAggIdempotency
./mtr --suite=ronsql                             # full suite

# D11 (commit 4)
./mtr --suite=ndb_push_agg testJoinAggIdempotency \
      --mysqld=--ndb-num-lqh-workers=4

# E.1 (commit 5, follow-up) — drops the mutex
./mtr --suite=ronsql                             # full suite
./mtr --suite=ndb_push_agg                       # block tests
```

## Commit cadence

| Commit | Contents | Approx LoC |
|---|---|---|
| 1 | A + B + D1-D6 (MTR functional regressions) | ~150 |
| 2 | E.2 (mutex extension) | ~50-80 |
| 3 | C + D7-D9 (DBTC per-CTE state + idempotency block tests) | ~400-600 |
| 4 | D11 (concurrency block test) | ~150 |
| 5 (later) | E.1 (single-owner routing refactor) — drops mutex | ~200 |

Plan-doc deliverables: this file (Phase L). Optionally a separate
`cte_agg_complete_state.md` for the deeper C details once
implementation starts.

## Risks

1. **C is a non-trivial DBTC refactor.** Pool init order, signal-ID
   / senderData compatibility, and node-failure cleanup all need
   careful handling. Lands in its own commit so it's individually
   testable and revertable.

2. **Backward compatibility of signals.** Wire formats are
   unchanged. Older nodes still work as long as they're not running
   this DBTC. With single-branch upgrade-locked development that's
   fine.

3. **Pool sizing for `CteAggCompleteRecord`.** Per-scan record
   count is small (≤ MAX_CTES_PER_QUERY, ~10). Existing transient
   pool sizing accommodates easily.

4. **Mutex deadlock risk in E.2.** `sendSignal` while holding
   `m_redist_mutex` could deadlock if the recipient takes the same
   lock. Mitigation: every `sendSignal` site that's currently inside
   a critical section gets split — payload built under lock, lock
   released, signal sent. Audit checklist included in commit 2.

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

## What we're not doing

- **JOIN_AGG_RELEASE accounting.** The `m_aggNodesOutstanding` /
  `m_joinAggNodes->m_aggNodesPending` machinery for the main-query
  release path stays as-is. It's a per-scan counter for a per-scan
  resource, not a per-CTE one — same architecture is correct
  there.

- **Refactoring `m_aggErrorCode`.** Today `scanptr.p->m_aggErrorCode`
  collects the first error from any CTE's failure across the scan.
  C makes per-CTE error codes available
  (`CteAggCompleteRecord::m_errorCode`) but the propagation to the
  API layer keeps the existing single-error convention. Per-CTE
  error reporting to the API is out of scope.

- **Aggregator merge phase concurrency.**
  `continueJoinAggMerge`'s per-thread merge into interpreter[0]
  predates this work and uses its own protection model
  (single-threaded merge driven by CONTINUEB). E.2 covers the
  cte_mode branch of `continueJoinAggMerge`; the non-cte path is
  unchanged.

- **CTE redistribution flow control.** The `needConf` /
  `m_cte_waiting_conf` pacing in `continueJoinAggRedistribute` is
  correct as-is and stays.

- **Replacing the mutex with something fancier.** E.2 keeps
  `m_redist_mutex`. The lock-free / sharded alternatives are E.1's
  territory.
