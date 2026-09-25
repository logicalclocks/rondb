# M3 after census run 6 — work plan

Census run 6 (2026-09-24, benchmark computer, artifacts in
`/Users/mikael/census_run6/`: `census_run6_{core,fs,fs_hw,tpch_cte}` on one
cluster in that order, `census_f27_sf1` = offline_fs on an earlier cluster)
was analysed on 2026-09-25 by five parallel investigations: memory trends,
a query-memory code audit, regressions, queries worth a look, and leak
tests.  This plan orders the follow-up work: **A memory leaks**, then
**B regressions and measurement**, then **C specific queries**.

## What run 6 established

- **Configuration was not X0.**  The box's `census.cnf` still had
  `#NumCPUs=15` and every cpubind line commented out: the data nodes ran
  the suite default NumCPUs=4 (2 LDM threads per node), unpinned — like
  run 4.  Comparisons with run 5 (X0) are confounded.
- **No code regression.**  Every apparent regression is explained by the
  configuration, the RDRS head-of-line blocking (B2) or the tpch_cte
  pairing bug (the mysqld arm ran the official TPC-H SQL; fixed in
  `ronsql_bench_matrix.py`, 97f7b2954ca).  `attempts=1` everywhere; the
  only retries were offline_fs_wide's expected out-of-memory failure.
- **F24 fixes worked:** offline_fs_scalar 993 → 168 ms, offline_fs_batch
  1538 → 211 ms, tpch q2 / q13 / q22 single request ~1.1 s → 113 / 139 /
  138 ms; offline_fs_batch at T=8 completes (run 4 lost its data nodes,
  run 5 failed with 1869).  core_group_many unchanged (F24 (b), C1).
- **Memory:** no per-request leak in the census data.  Idle QUERY_MEMORY
  rose by +15/+17 pages (≈0.5 MB) per node over 2.84 M requests, in
  one-off steps of 1–6 pages, only after CTE shapes; TRANSACTION_MEMORY one
  step of +4 pages; global memory outside query and transaction memory
  grew ~31 MB per node and never came back (90 % during mysqld's official
  Q11).  The code audit explains the query-memory steps as bounded
  per-instance pages (A3) — but found real leaks the census never
  exercised (string MIN/MAX, A1) and error-path defects (A2).

## A. Memory leaks (first)

Tests: `mysql-test/suite/ronsql_large/t/ronsql_large_mem_leak*.test`
(97f7b2954ca; results predicted — record after the first green run).
Harness: warm-up with 8 concurrent clients (allocates the per-instance
pages), rounds of 1 / 10 / 50 executions, per-node QUERY_MEMORY and
TRANSACTION_MEMORY settled 5 s and required back at the round-1 level
with zero tolerance, DUMP 2361–2364 / 2560 / 2650 at the end.

**A0. Baseline the tests.**  Run `_controls` first (harness sanity), then
the others; `_strings` is expected to fail until A1.  Fix harness issues
(shapes outside the RonSQL envelope, strict-diff format differences,
error-insert outcomes in `_errors`) before trusting a LEAK line.

**A1. String MIN/MAX slot leaks (confirmed in code; per group, per string
slot, ≥ 32 B each, in `lc_ndbd_pool` memory so a leak pins 2 MB segments).**
- L1 `continueJoinAggSend` (DblqhMain.cpp ~20891): sent groups leave the
  map by `eraseAndNext` without `freeGroupStringSlots`; the eviction path
  (JoinAggInterpreter.cpp ~1435) and plain aggregation (AggInterpreter.cpp
  ~483) do free them.  Teardown walks only the map, so the buffers are lost.
- L2 the redistribution walk (`continueJoinAggRedistribute`, ~24060): same
  pattern after the group was packed (`appendRedistBatch` /
  `sendRedistributeGroup` encode the payload first, so freeing after the
  encode is safe).
- L3 `mergeStringAccumulator` (JoinAggInterpreter.cpp ~1549) returns
  without freeing the losing source value; `mergeFrom` then frees the
  group data without its string slots (~1770, ~1773).
Fix: free the slots at the three sites; test `_strings` must turn green.

**A2. Error-path defects (from the audit; verify each, then fix).**
- `mergeOneGroup` new-key path: on a `copyStringAggSlot` failure the
  not-yet-copied `dst_items[j]` still alias `local_items[j]`, so both are
  freed (double free) (JoinAggInterpreter.cpp ~1896–1913).
- Redistribution-receive error paths free the sender's foreign
  `val_ptr`s (~1849, ~1969).
- Abort during the batched MUTEX_FREE merge (> 256 groups): groups moved
  by `insertRawInBucket` still live in the source interpreter's chunks
  until the chunk splice, so tearing down slot 0 frees into another
  interpreter's chunk list (use-after-free / double free).
- DBSPJ `cleanup` RS_ABORTED path returns before freeing `m_aggStateKeys`,
  `m_cteAggStateKeys`, `m_cteContexts` and the arena (DbspjMain.cpp
  ~4685–4690); trigger: TC failure while RS_WAITING.
- `sendJoinAggSetupRef` (DblqhProxy.cpp ~2402–2455) frees buffers without
  nulling the fields; in release builds `ArrayPool::getPtr` returns free
  slots, so a later RELEASE (e.g. from `execJOIN_AGG_NODE_FAIL_REP`) can
  free them again.
- Unverified: a REDISTRIBUTE_REQ after RELEASE queuing pages that are
  never freed (~24366 checks only `isAborting`); the node-failure sweep
  skipping FINALIZING / SENDING_RESULTS states; `m_leaf_programs`
  allocated without clearing (~2914), so a REF inside the leaf loop
  releases garbage JIT handles.
Tests: `_errors` (5125, 5149, 5137, 5130, 8130/8131, …) and the
testNodeRestart CTE cases; add error inserts where a path has none.

**A3. Bounded one-off pages (not leaks; remove or document).**
- `c_cteScanIterStatePool` is `init`-ed but never `startup()`-ed
  (DblqhInit.cpp ~440): the first seize on each LDM instance takes a
  transient page that a TransientPool never releases (`canRelease`
  requires `m_top > 0`).  Fix: `while (c_cteScanIterStatePool.startup())
  refresh_watch_dog();` after the init; optionally `checkPoolShrinkNeed`
  in `releaseCteScanIterState`.  Scalar-over-CTE should then show +0.
- DBSPJ arena: `RWPool` keeps its current page per SPJ instance
  (RWPool.hpp ~213).  DBTC `c_aggCompleteRecordPool` /
  `c_cteScanFragHandlePool`: first transient page per TC instance
  (TRANSACTION_MEMORY).  Bounded by the instance counts; document.

**A4. Global memory outside query / transaction memory (~31 MB/node).**
The driver now also records JOBBUFFER, TRANSPORTER_BUFFERS, DATA_MEMORY,
SCHEMA_MEMORY, REPLICATION_MEMORY.  Suspected: job and send buffer pages
returned to `thr_safe_pool` free lists, never to the global manager
(mt.cpp ~545 / ~726) — a high-water mark, bounded.  Confirm in run 7.

**A5. Observability.**  No ndbinfo view or DUMP shows the CTE iterator
pool size or the DBSPJ arena pages, and no DUMP checks QUERY_MEMORY
`m_curr`; add them so one-offs are visible and leak tests can assert on
pool sizes.

**A6. Query memory per multi-fragment aggregate scan** (~1 MB per fragment
scan whatever the group count: core_scan_agg +44 MB, core_idx_range +36 MB
at 8 threads).  Understand (chunk / pool granularity) — it sets the
concurrency budget together with F27 (c).

## B. Regressions and measurement (second)

**B1. Census configuration.**  Uncomment `NumCPUs=15` and the cpubind
lines on the box; have the driver log the LDM count per node
(`SELECT node_id, COUNT(*) FROM ndbinfo.threads WHERE thread_name='ldm'
GROUP BY node_id`) and warn when it does not match the cnf.

**B2. RDRS head-of-line blocking (new, also a production issue).**  On
Linux drogon 1.9.7 gives each of the 64 IO loops its own SO_REUSEPORT
listener (`extra/drogon/drogon-1.9.7/lib/src/ListenerManager.cc` ~85–111,
`HttpServer.cc` ~72), so the kernel hashes connections onto loops, and
`RonSQLCtrl::ronsql` runs `ronsql_dal` synchronously on the loop
(`server/src/ronsql_ctrl.cpp` ~275): connections sharing a loop serialise
outside the timed phases.  8 connections on 64 loops collide with 37 %
probability — observed in 22 of 57 RonSQL T=8 cases, p99 and q/s off by
15–50 % (fs_batch p99 +105 % while its firstbatch improved).  In
production one long RonSQL statement stalls every request on its loop,
pk-reads included.  Steps: add the loop index to `x-ronsql-phases`;
confirm by repeated `.bench_ronsql fs_batch 8 5000` runs (efficiency
flips ~0.7 / ~0.97); fix by executing RonSQL on a worker pool off the IO
loop (stopgap for the census: more RESTNumThreads).

**B3. Triage.**  Flag cases whose http+client share of execute exceeds
10 % (loop collision) or compare server execute percentiles; the current
triage calls them REGRESSION.

**B4. Client throughput understatement.**  Effective concurrency (q/s ×
avg latency) is 6.1–7.1 of 8 on RonSQL point shapes vs 7.8 for mysqld,
even after 5e52fc493ce: ~20 % of RonSQL q/s lost client-side outside the
timed window (tools/rondb-cli/internal/client/rest.go ~175).

**B5. Run 7.**  X0 active, pairing fix, wider memory probe,
`--mem-settle 5`, one idle reading ~30 s after the last case, `--repeat 3`
for core_scan_filter / core_scan_agg (+18–20 %, unexplained), threads
1,8.

## C. Specific queries (third)

**C1. core_group_many / core_group_2k — F24 (b).**  1.03 s single request
vs 161–168 ms for the same `GROUP BY o_custkey` through the CTE path; it
bites already at 2.4k groups (511 ms vs 146 ms for 3 groups).  The drained
single-table path flushes every 4 KB (`DEF_AGG_RESULT_BATCH_BYTES`, ~50
groups) so ~1 M partials reach the API merge.  Options: count partials
per request, raise the flush threshold, or route many-group single-table
aggregates through the JoinAgg per-thread table.  Then `print` (79–108
ms, TEXT output) is a third of the cost.

**C2. fs_point** (439 vs 210 µs at T=8; firstbatch 264 µs vs mysqld's
45 µs NDB wait): an aggregate over one grouped single-table CTE with no
join pays the CTE/JoinAgg protocol (~150–200 µs).  Flatten it in the
planner into a single-table aggregate (as M1.3 did for the collect CTE);
get EXPLAIN, bench a hand-flattened twin.

**C3. fs_latest** (ORDER BY … LIMIT 100: firstbatch 641 µs at T=1):
`readTuples(LM_CommittedRead, SF_OrderBy|SF_Descending)` without a batch
size (RonSQLPreparer.cpp ~8330) lets every fragment return up to 990
rows; under an ordered merge each fragment needs at most offset + limit.
Set the batch size; add a fetched-row counter to the phases.

**C4. Snowflake points (WP-G / F13)**: 334–382 µs vs 186–223 µs (mysqld)
and ~150 µs (production twin); isolated ~8 ms execute outliers (cause
unknown: park / identity pools?).

**C5. Memory per many-group CTE query** (F27 (c)): tpch_q2 peaks at
876 MB of query memory at 8 threads (~110 MB per query, ~550 B per group
for a 5-row result).  Reduce the per-group footprint or budget admission;
confirm offline_fs_wide now fails with 20008 / 1870 (temporary, 503)
instead of 1869.

**C6. core_in_pk100** per-read aggregation cost (ndbprep 59 vs 15 µs,
firstbatch 296 vs 181 µs against the pass-through twin) — low priority.
