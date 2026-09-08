# Per-CTE DAG scheduling (replacing the CTE phase barrier)

**Status: IMPLEMENTED (September 2026), pending user build + block-test
runs + full ronsql_cte regression ×5 + first record of the dag family.**

## The problem (maintainer statement)

The barrier is per phase, not per dependency.  If CTE C depends only
on CTE A, but CTE B is an independent phase-0 CTE that takes much
longer, C still waits for B, because phase 1 starts only when all of
phase 0 has completed and been redistributed.  A DAG scheduler starts
C as soon as A is READY.

## What the phase machinery was (pre-change inventory)

- Phases computed from depMasks in BOTH blocks (DBTC
  `DbtcMain.cpp:30585`, DBSPJ `DbspjMain.cpp:1793`): `phase[c] = 0` if
  no deps else `1 + max(phase[dep])`.  DBTC's `m_cteInfos` index IS
  the cteId (depMask bits index the array).
- DBSPJ `checkPrepareComplete` started every phase-0 CTE root together
  (so CONCURRENT CTE subtrees on one instance were already the shipped
  reality — the per-request `m_outstanding`/`m_cnt_active` quiescence
  model and `handleCtePhaseNextBatch`'s multi-active restart handle
  them).  `batchComplete`'s `RT_CTE_PHASE` arm: at `m_outstanding ==
  0`, restart still-active scans, or if `m_cnt_active == 0` report the
  whole PHASE done (`CTE_PHASE_COMPLETE_REP{phase}`) — one rep per
  DBSPJ worker per phase.
- DBTC counted phase reps against `m_cteScanReportsExpected` (live
  CteScanFragHandles); when all in → `sendCteCompleteReqsForPhase`
  (per-CTE JOIN_AGG_COMPLETE_REQs, per-CTE `AggCompleteRecord`
  KIND_CTE — the per-CTE structure ALREADY existed here), scanState →
  WAIT_CTE_COMPLETE, `m_ctePhaseRemaining` = #CTEs in phase.  On the
  last KIND_CTE record completing → `cteAdvancePhase`: either
  `sendCtePhaseStartReqs(phase+1)` (DBSPJ marks phases < p READY,
  starts phase-p roots, RESETS m_outstanding/m_cnt_active) or
  `sendCteStartMainReqs` (all READY, start main root).
- Parked probes (`cte_lookup_parent_row` pending count) are defensive
  dead code: dependents never start before their deps are READY, so
  the park arm is unreachable by scheduling — an invariant the DAG
  scheduler preserves (there is still no flush site).

## The DAG design

Scheduling stops using phases entirely; depMasks are the truth.
The phase representation is deleted end to end (see the section
below).

**Start rule** — a CTE's roots start (on every DBSPJ worker) only
after every CTE in its depMask is READY cluster-wide.  Initially:
every `depMask == 0` CTE starts at `checkPrepareComplete` (the old
phase-0 set, now selected by mask).

**Per-CTE local completion (DBSPJ)** — at each quiescence point
(`m_outstanding == 0`, the existing RT_CTE_PHASE arm), sweep the tree
nodes once building an `activeCteMask` (cteIds owning a `TN_ACTIVE`
node; every subtree node carries `m_cteId`).  Every context in the new
`CTE_MATERIALIZING` state (now actually set at start — it was a
never-set enum) whose bit is clear is locally done: transition to the
new `CTE_LOCAL_DONE` and send `CTE_PHASE_COMPLETE_REP{cteId}` (the
struct's `phase` word is replaced by `cteId`, SignalLength stays 5).  Then, as before, restart any
still-active scans via `handleCtePhaseNextBatch`.  Justification: at
global quiescence nothing is outstanding anywhere, so "no TN_ACTIVE
node in c's subtree" is exactly the per-CTE projection of the old
whole-phase test (`m_cnt_active == 0`); lookup-type children never
hold TN_ACTIVE (their work lives in m_outstanding), while scan-type
children do.  A done CTE's report can wait for the next JOINT
quiescence point of concurrently running scans — bounded by one batch
round, the same cadence concurrent phase-0 CTEs always had.

**Per-CTE redistribute (DBTC)** — reps are counted per (handle,
cteId): `CteScanFragHandle::m_cteReportedMask` (replaces
`m_lastCompletePhase`; duplicate bit = loud ndbrequire as before) and
`CteInfo::scanReports`.  When a CTE's count reaches
`m_cteScanReportsExpected` → `sendCteCompleteReqsForCte(c)` (the loop
body of the old per-phase sender, unchanged wire).  scanState stays
RUNNING throughout — WAIT_CTE_COMPLETE is never entered (a single
scalar can't represent overlapping scans + redistributes; the enum and
scanError's tolerance of it remain).

**Per-CTE READY (DBTC → DBSPJ)** — when CTE c's KIND_CTE record
completes, `cteMarkReady(c)`: set `m_cteReadyMask` bit,
`m_ctesReadyCount++`.  All ready → `sendCteStartMainReqs` (unchanged;
DBSPJ's `execCTE_START_MAIN_REQ` marks every context READY, so the
last CTE needs no individual broadcast).  Otherwise, if ANY other CTE
has bit c in its depMask — startable now or not, since DBSPJ's mask
check needs the READY state recorded for LATER satisfaction — send
`CTE_PHASE_START_REQ{cteId=c}` (the struct's `phase` word is replaced
by `cteId`; the signal is the per-CTE READY broadcast) to every
handle, and fold
the now-startable set into `m_cteStartedMask`.  DBSPJ's handler marks
c READY (from LOCAL_DONE), then starts every `CTE_NOT_STARTED` context
whose `(depMask & ~readyMask) == 0`, marking them MATERIALIZING.  The
old handler's `m_outstanding = 0 / m_cnt_active = 0` "residual" resets
are DELETED — under concurrency those counters legitimately carry
other CTEs' in-flight work, and per-CTE reports are only ever sent at
true quiescence points so there is no residue to clean.

**Probe legality is unchanged**: a CTE's consumers (dependent-CTE
bodies, main query) start only after DBTC saw every COMPLETE_CONF for
it, i.e. after every node's state passed the redistribute + FINAL_REP
barrier into kernel CTE_READY — the same guarantee the phase system
gave, minus the over-waiting.  Per-CTE COMPLETE while sibling scans
still feed OTHER states is safe: agg states are fully per-CTE
(aggStateKeys, mutex-free per-state merge; the D6/D22 hardening made
all addressing explicit).

**Failure semantics preserved, one improvement**: worker loss /
SCAN_FRAGREF / timeout still retire the handle, set m_aggPhaseFailed,
and abort when the next per-CTE threshold fires (release-then-abort
arm moved into the per-CTE trigger).  A KIND_CTE COMPLETE_REF still
swallows per the existing TODO (the CTE is marked ready so scheduling
proceeds and the main query surfaces the downstream error), exactly as
`cteAdvancePhase` did.  `retireCteScanFragHandle` mirrors its counter
hygiene per-CTE (decrement scanReports for bits the handle had
reported; expected-- as before).

**Timers** — the CTE frag timer becomes started-set-aware: a handle's
timer is stopped when its reported mask covers `m_cteStartedMask`
(nothing currently expected from it — the old "stopped during
redistribute" behavior), rearmed on each rep otherwise, and restarted
on every handle whose mask no longer covers a GROWN started set (the
READY broadcast that starts dependents).  `isCteScanFragTimerStopped`
and `cteAggResponsesOutstanding` use the new predicates
(`cteStageActive(scanP)` = numCtes > 0 && readyCount < numCtes,
mask-coverage instead of m_lastCompletePhase).

## Phases are REMOVED, not demoted (maintainer direction)

The first cut kept phase numbers as "debug metadata"; the maintainer
called that out — the scheduler's truth is the dependency bitmap
(`depMask`, bit c = waits on cteId c; the waiting set at any moment is
`depMask & ~readyMask`), so the phase representation was deleted
end to end: the `phase` word is gone from the CTE_KEYS wire block
(writer DbtcMain.cpp:31311, reader DbspjMain.cpp:1716) and from both
signal structs; `CteInfo::phase` + the derivation loop,
`m_ctePhaseCount` / `m_cteCurrentPhase` / `m_ctePhaseRemaining` /
`m_cteScanReportsReceived` (DBTC), `CteContext::m_phase` /
`Request::m_cteCurrentPhase` / `m_ctePhaseCount` /
`ParsedCteMeta::phase` (DBSPJ), `AggCompleteRecord::m_phase`, and the
handle's `m_lastCompletePhase` (→ `m_cteReportedMask`) are all
removed.  The CTE-stage predicates became `cteStageActive()` =
`numCtes > 0 && m_ctesReadyCount < numCtes`; `WAIT_CTE_COMPLETE` is
never entered (the scan stays RUNNING — scans and redistributes
overlap; enum value kept for state-dump decoding).  Only the GSN /
struct NAMES keep "PHASE" (renaming GSNs is churn without value; a
comment marks them historical).

## What was deliberately NOT changed

- No version gate (26.04/26.10 alpha policy, same-version clusters);
  the CTE_KEYS wire block and both signal structs reshape freely.
- The legacy `CTE_SCAN_COMPLETE_REP` translator is retired to a
  log-and-drop stub — it carried neither cteId nor transid; nothing
  in-tree sends it.
- Parked-probe machinery untouched (still defensive dead code; the
  new `CTE_LOCAL_DONE` state parks like MATERIALIZING).
- No main-query early start: main needs all CTEs, and any CTE consumed
  only by another CTE completes before its consumer anyway.
- A CTE nobody depends on gets no individual READY broadcast — its
  worker-side READY state catches up at CTE_START_MAIN_REQ (which
  marks every context READY); DBSPJ's `m_ctesReady` counter therefore
  lags DBTC's until start-main, and is only used as the
  "CTE stage active" gate.

## Latency effect

For `WITH a AS (...fast...), b AS (...slow...), c AS (...reads a...)
SELECT ... [uses b and c]`: c's materialization now overlaps b's,
saving up to the whole duration of c (scan + redistribute) whenever b
dominates.  Two-phase TPC-H shapes (`cte_tpch_q15`/`q22`) and the
obc/chained families are the benchmark targets.

## Tests

- MTR `body_cte_dag.inc` (dag-1..5 ×5 topology wrappers
  `ronsql_cte_dd_cte_dag`): C←A beside an independent B (both
  probe orders), diamond D←{A,B}, 3-chain A→B→C, three independents,
  and the motivating skew shape (tiny A + big B + C←A) — all strict
  value-compares vs mysqld.
- Regression net: the entire ronsql_cte suite ×5 (every multi-CTE
  family — obc two-CTE, chained CTEs, big-07a/b/c, q15/q22 rewrites)
  plus testCteNdbApi / testCteNdbApiFilter / testJoinAggNdbApi block
  suites — all of which exercise 2-phase queries through the new
  scheduler.
- Benchmarks (pending): `.bench_ronsql cte_tpch_q15 / cte_tpch_q22`
  before/after on prod_build.
