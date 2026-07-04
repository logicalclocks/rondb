# MUTEX_FREE CTE build with thread-merge before redistribute

> **Status (2026-07-04): Step 1 SHIPPED** — the DBTC flip is applied
> (DbtcMain.cpp:30134 now sends `STRATEGY_MUTEX_FREE | CTE_MODE_FLAG`).
> Builds + tests run by Mikael; CTE benchmarks show **mostly 2-3x speedup**.
> `STRATEGY_MUTEX_BASED` has no remaining senders — it survives only as the
> DblqhProxy decode fallback for old-version DBTCs.  Steps 3-4 (memory-budget
> review on lg_* data, eviction error-insert gating) remain open watch items.
>
> **Open flakes observed once during validation (both passed on retry,
> investigation deferred):** (1) `ndb_push_agg_dist.testCteDbtc` Test 6
> (two scalar CTEs, multi-phase) returned COUNT=3/SUM=120 vs expected 5/150 —
> exactly one node's main-agg contribution missing; the scalar-CTE merge adds
> no CONTINUEB yield (≤256 groups) so the flip's timing delta is µs-scale, and
> the test harness `collectResults` stops at SCAN_TABCONF EndOfData without
> counting expected TRANSID_AI records (DBTC→API vs DBLQH→API arrive on
> different links, so a pre-existing delivery race is plausible).  (2)
> `ronsql.ronsql_basic` data-node crash `ndbrequire(connectedToNode)` at
> DbtupExecQuery.cpp:5682 (`SendAggregationResult`) — the **normal-scan**
> AggInterpreter drain hit a disconnected API node mid-scan; path untouched by
> this flip, and unlike `sendReadAttrinfo` (DbtupBuffer.cpp:342, which falls
> back to TRANSID_AI_R routing when `!connectedToNode`) the agg drain has no
> graceful-disconnect handling — pre-existing robustness gap, fix separately.

## Problem

CTE materialization aggregation states are hardcoded to the mutex strategy:
DBTC sends every CTE `JOIN_AGG_SETUP_REQ` with
`STRATEGY_MUTEX_BASED | CTE_MODE_FLAG` (DbtcMain.cpp:30134-30136), while the
main-query aggregation uses `STRATEGY_MUTEX_FREE` (DbtcMain.cpp:30034).

MUTEX_BASED means one shared `JoinAggInterpreter` per node
(`Dblqh::getJoinAggInterpreter`, DblqhMain.cpp:15286), and **every row** fed
during the CTE body scan takes a global `std::mutex` in
`processRecWithLinkedAttrs` (JoinAggInterpreter.cpp:685-686) around all of its
aggregation work (GROUP BY key read, hash, hash-table probe/insert,
accumulator updates). The body scan runs in parallel across all
fragments/LDM threads, but the aggregation serializes on one lock and one
hash table per node, plus cache-line contention between LDM threads.

Measured impact: `cte_tpch_q15` (~225k lineitem rows into the `revenue` CTE)
and `cte_tpch_q22` (1.5M orders rows into `cust_orders`) run ~4x slower than
the MySQL baselines; the serialized CTE build is the dominant or co-dominant
cost (q22 additionally pays per-row CTE_LOOKUP probes, tracked separately).

## Goal

Build CTE materialization states MUTEX_FREE (one `JoinAggInterpreter` per
thread, no per-row locking), then merge the per-thread hash tables into
`interpreters[0]` on the owner LDM instance **before** redistribution /
CTE_READY, so all post-barrier consumers (redistribute walk, CTE_LOOKUP
probes, CTE_SCAN drain, scalar reads) keep seeing a single per-node table.

## Key discovery: the machinery already exists

The audit (2026-07-04 session) found that every piece is already in the tree,
built for the MUTEX_FREE main-query aggregation and written strategy- and
CTE-agnostic. The functional change is essentially **one flip in DBTC**.

| # | Piece | Where | Status |
|---|-------|-------|--------|
| 1 | Per-thread interpreter allocation, **including CTE config** (`setCteMode`, `initGBTypesFromMetadata`, multi-leaf `cacheMultiLeafAggOps`, per-thread chunk allocator) | DblqhProxy.cpp:2775-2831 | exists, CTE-aware |
| 2 | Thread-merge loop with CONTINUEB slicing (`mergeFrom` into `interps[0]`, `ZCONTINUE_JOIN_AGG_MERGE`, 256 groups/batch, heartbeats per batch) | `Dblqh::continueJoinAggMerge`, DblqhMain.cpp:18843-18896 | exists |
| 3 | CTE tail **after** the merge loop: single-node → `CTE_READY` + COMPLETE_CONF; multi-node → `processRedistQueue` → `CTE_REDISTRIBUTING` → `continueJoinAggRedistribute` | DblqhMain.cpp:18924-18977 | exists — flipping the strategy automatically inserts the merge before redistribute |
| 4 | COMPLETE dispatch already strategy-branched (MUTEX_FREE → merge_idx=1; MUTEX_BASED → merge_idx=num_threads, skip) | DblqhMain.cpp:18688-18702 | exists |
| 5 | `mergeFrom` handles scalar accumulators (`n_gb_cols==0` → `mergeAccumulators` + `m_processed_rows`), string MIN/MAX slots (`ensureStringResultsFrom`), and groups (bucket-wise `findInBucket` → merge, else zero-copy `insertRaw` + chunk-list splice) | JoinAggInterpreter.cpp:1065-1167 | exists, complete |
| 6 | CTE_LOOKUP probe uses `getJoinAggResultInterpreter` (= `[0]` post-merge) | `execCTE_LOOKUP_REQ`, DblqhMain.cpp:19999+ | already correct |
| 7 | Redistribute walk uses `getJoinAggResultInterpreter` | `continueJoinAggRedistribute`, DblqhMain.cpp:21229+ | already correct |
| 8 | Inbound cross-node groups merge into `getJoinAggResultInterpreter`; queued during FINALIZING/SENDING_RESULTS and drained post-merge | `execJOIN_AGG_REDISTRIBUTE_REQ`, DblqhMain.cpp:21557-21608 | already correct |
| 9 | `cteScanAggFeed` reads source via result interpreter, feeds target via `getJoinAggInterpreter` (per-thread) | DblqhMain.cpp:20246+ | already correct |
| 10 | CTE_SCAN drain / scalar emit use result interpreter | DblqhMain.cpp:20538+ | already correct |
| 11 | DBSPJ `cte_lookup_send` hash metadata is strategy-aware (`m_per_thread_interpreters[0]` when MUTEX_FREE) | DbspjMain.cpp:6617-6620 | already correct |
| 12 | Teardown handles per-thread interpreters ("Phase 2" arm) and 4c bounded teardown | DblqhProxy.cpp:3089-3130 | already correct |
| 13 | ERROR_INSERT 5116 `setMaxGroups(3)` has both strategy arms | DblqhProxy.cpp:2841-2847 | already correct |

`scanPtr->m_agg_interpreter` hits elsewhere are the normal-scan
`AggInterpreter` on ScanRecord — unrelated. The only direct
`state->m_agg_interpreter` dereferences are DblqhProxy setup/teardown, both
strategy-branched.

## Why correctness holds

- **Feeds finish before the merge starts.** DBTC sends the CTE
  `JOIN_AGG_COMPLETE_REQ` only after every body-scan fragment cluster-wide
  has reported complete; the merge runs from that handler. So no thread is
  feeding `interps[1..N-1]` while `mergeFrom` drains them.
- **Single-writer on `[0]`.** Phase L pins all COMPLETE / REDISTRIBUTE /
  FINAL_REP work for a state to one owner LDM instance
  (`m_owner_instance`, DblqhProxy.cpp:2443-2447; `ndbassert` at
  DblqhMain.cpp:21499). The thread-merge, inbound REDISTRIBUTE merges, queue
  drain, probes, and the redistribute walk all execute on that instance —
  no concurrent access to `[0]` by construction.
- **Early inbound merges commute.** A remote node that processes its
  COMPLETE first can send REDISTRIBUTE_REQ while our state is still
  SETUP_COMPLETE; the immediate-merge branch folds it into `[0]` *before*
  our thread-merge. Aggregation merges are commutative/associative per op,
  and both merges run on the owner thread, so ordering is irrelevant.
  (Cluster-wide, all feeds are done before any node redistributes, so this
  never races with local feeds either.)
- **Probes start only after CTE_READY.** The main-query scan (and any
  dependent CTE body) is orchestrated by DBTC to start after the CTE
  barrier, and it probes the merged `[0]` table. The D26 per-LDM xfrm
  scratch reasoning is unchanged.
- **Feed-side thread-id domain is proven.** `getJoinAggInterpreter` indexes
  by `getThreadId()` with `ndbrequire(thr_idx < m_num_threads)`
  (`m_num_threads = ndbMtQueryWorkers`, DblqhProxy.cpp:2430). The exact same
  feed paths (`handleJoinAggRow`, `cteLookupAggFeed` target,
  `execJOIN_AGG_NULL_ROW`) already feed MUTEX_FREE **main** aggregation
  states in production today.
- **No lazy-init hazard for zero-row threads.** CTE-mode GB types come from
  `initGBTypesFromMetadata` at setup on every per-thread interpreter
  (DblqhProxy.cpp:2817-2827), not from the first row.

## Implementation steps

### Step 0 — baseline measurements (Mikael runs)

Record before/after numbers on the same topology and dataset:

```
.bench_ronsql cte_tpch_q15     .bench_sql tpch_q15
.bench_ronsql cte_tpch_q13     .bench_sql tpch_q13
.bench_ronsql cte_tpch_q22     .bench_sql tpch_q22
.bench_ronsql fs_batch / offline_fs_* (CTE-based entries)
```

Optionally 1-LDM vs 8-LDM runs of q15: pre-change the CTE build should show
near-zero LDM scaling (mutex-serialized); post-change it should scale.

### Step 1 — the flip (DBTC)

DbtcMain.cpp:30134-30136:

```cpp
req->concurrencyStrategy =
    JoinAggSetupReq::STRATEGY_MUTEX_FREE |
    JoinAggSetupReq::CTE_MODE_FLAG;
```

No signal-format change: `concurrencyStrategy` already carries the value and
the receiving DblqhProxy code has handled both strategies (with CTE config on
the MUTEX_FREE arm) since the per-thread path was written. Single-row CTEs
(`CTE_SINGLE_ROW`, setup to one node only) take the same path.

### Step 2 — audit confirmations during bring-up

The table above was verified by reading; confirm at runtime with DEB_CTE /
DEB_JOIN_AGG enabled on one multi-node topology:

1. Merge runs before redistribute: `continueJoinAggMerge` logs then
   `CTE COMPLETE: multi-node redistribution starting`.
2. `execCTE_LOOKUP_REQ` probe hits find groups fed on non-owner threads
   (i.e. the merge actually moved them into `[0]`).
3. Scalar CTE (I.17 watermark shape): per-thread scalar accumulators fold
   via `mergeFrom`'s `n_gb_cols==0` arm before the `keyLen==0`
   REDISTRIBUTE variant ships `[0]`'s `m_agg_results`.
4. Early-arriving REDISTRIBUTE_REQ (state SETUP_COMPLETE / CTE_BUILDING on
   the receiver) still produces correct totals — covered by multi-node MTR
   with skewed node speeds; no code change expected.

### Step 3 — memory budget review

Setup computes `budget_pages = max(available/100, 4)` per state and the
MUTEX_FREE arm splits it `per_thread_budget = max(budget_pages/num_threads,
4)` (DblqhProxy.cpp:2708-2720, 2791-2794).

Considerations:

- **Transient duplicate groups.** Until the merge, a group key touched by
  all N threads occupies N entries (one per thread table). Worst case
  transient footprint ≈ `num_threads ×` today's group memory. For a
  100k-group CTE at ~64-128 B/group that's ~50-100 MB at 8 threads vs
  ~6-13 MB today. The merge frees duplicates (`freeGroupData`) and splices
  disjoint chunks without copying.
- **Per-thread budget skew.** A thread that happens to see most groups gets
  only `budget/num_threads` before `bookMoreMemory` has to extend against
  `available_pages`. Verify the growth policy under per-thread budgets on
  the `ronsql_large` (lg_*) dataset; if large CTE builds start failing with
  OOM aborts where they previously fit, bump the CTE-state budget (e.g.
  don't divide by `num_threads` for CTE-mode states, since the total group
  volume is the same table split N ways plus duplicates).
- Start with no change; measure on tpch + lg_* and adjust only if needed.

### Step 4 — eviction interplay (verify only)

Eviction (`AGG_EVICT_NEEDED` → `sendEvictedAggGroup` → TRANSID_AI to the
coordinator) only triggers when `setMaxGroups` was called (ERROR_INSERT
5116/5090). Evicting from a CTE materialization would corrupt the table that
redistribute/probes rely on — that hazard is pre-existing under MUTEX_BASED
and unchanged by the flip. Confirm no CTE test enables 5116/5090 against a
CTE-mode state; if any does, gate the error insert to non-CTE states.

### Step 5 — tests (Mikael runs)

Block unit tests (from debug_build, `-c <connect_string> -m <mysql_port>`):

- `testCteNdbApi`, `testCteNdbApiOuterJoin` — full CTE topology sweep
- `testJoinAggSpj`, `testJoinAggNdbApi` — main-agg regression (unchanged path)
- `testVarcharMinMax` — string MIN/MAX slots now merged **across threads**
  (S.1/S.2 ownership exercised in `mergeFrom`, previously only cross-node)

MTR, all 5 topology suites (`ronsql_cte`, `ronsql_cte_ng1r3`,
`ronsql_cte_ng2r2`, `ronsql_cte_ng2r3`, `ronsql_cte_ng4r2`):

- full `ronsql_cte` data-driven suite — the ~81-case green envelope is the
  regression net; watch the redistribute-sensitive cases (D22 wrong-COUNT
  shape) specifically
- `ronsql_minmax_string`, `ronsql_cte_partial_key` (4d regression),
  `ronsql_cte_outer_join`, `ronsql_cte_index_body`, agg-d17a..j (DATE /
  temporal MIN/MAX)

The multi-node suites matter most: thread-merge output is the input to
redistribution, so ng2r2/ng2r3/ng4r2 exercise merge→redistribute→merge.

### Step 6 — benchmark validation

Re-run Step 0. Expected:

- `cte_tpch_q15`: largest gain — its RonSQL cost is dominated by the
  serialized 225k-row build; the build should now scale with LDM count.
- `cte_tpch_q13` / `q22`: CTE build (1.5M rows) parallelizes; q22 keeps the
  per-row CTE_LOOKUP probe cost (separate follow-up: key-batched
  CTE_LOOKUP_REQ per owner node).
- Main-agg-only benchmarks (bench_q12*, bench_q9_dbtc): unchanged.

## Risks / watch items

- **Redist queue growth during a longer FINALIZING window.** The thread-
  merge extends FINALIZING; inbound groups queue via `redistAlloc` pages and
  overflow aborts with `ZCTE_LOOKUP_OUTPUT_OVERFLOW` (DblqhMain.cpp:21568).
  The queue can at worst hold the full inbound group set, which is the same
  data it would hold if senders redistributed while we were still slower for
  any other reason — low risk, but watch for this error on big multi-node
  CTEs.
- **VM_TRACE merge batch is 2** (DblqhMain.cpp:18862): debug builds will do
  ~groups/2 CONTINUEB rounds per thread pair. Fine for the current suites'
  group counts; if debug MTR wall time regresses noticeably, raise the
  VM_TRACE batch (it only needs to be small enough to exercise the yield
  path).
- **Merge is single-threaded on the owner instance.** O(total groups across
  threads), mostly zero-copy `insertRaw` + chunk splice for disjoint groups.
  For 100k-group CTEs this is tens of ms — strictly better than serializing
  millions of per-row feeds, but it is a new serial phase; if it ever shows
  up, per-bucket-range parallel merge is the escalation path (not planned).
- **Mixed-version clusters.** No new signal fields or flags; both strategies
  predate CTE mode on the receive side. No version gate needed.

## Interaction with in-flight plans

- **LOCAL execution mode** (`local_execution_mode_plan.md`): LOCAL places a
  single aggregation state on the TC node; feeds arrive via the networked
  agg feed on that node. MUTEX_FREE per-thread build + merge applies there
  unchanged (feeds land on whichever local threads execute them). No
  conflict; the flip happens in the same DBTC send site LOCAL will touch.
- **QN_AGGREGATE tree-node alternative**
  (`aggregation_treenode_alternative_plan.md`): feeds become JOIN_AGG_FEED_REQ
  from SPJ; they still select the interpreter via `getJoinAggInterpreter`,
  so the strategy choice is orthogonal.
- **CTE_LOOKUP batching follow-up** (q22): independent; both are needed for
  q22 parity.

## Expected outcome

CTE build phase throughput scales with LDM/query-worker count instead of
being mutex-serialized per node; q15-class queries (build-dominated) should
close most of the 4x gap against the MySQL baseline, q22-class queries
improve partially (probe phase remains until CTE_LOOKUP batching lands).
