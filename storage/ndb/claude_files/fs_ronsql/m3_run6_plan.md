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

#### A6 analysis (2026-09-25, code reading and an allocator model; no code changed)

**What the numbers are.**  Every per-node QUERY_MEMORY peak in run 6 is a
whole number of `lc_ndbd_pool` segments (64 pages = 2 MB each).  Above
idle: core_scan_agg and core_scan_filter 11 per node, core_group_2k 10–11,
core_idx_range and core_group_few 9, core_group_many 6–7, core_avg_range
2 / 5, core_in_idx100 1 / 2, pass-through 0.  So the cost is segments
pinned, not bytes used.  A scalar aggregate fragment scan allocates one
32 KB page for the interpreter object
(`PushdownInterpreterFactory::Create`, PushdownInterpreter.cpp ~270,
called with `getThreadId()` from DbtupExecQuery.cpp ~1062) and a small
`m_buf_block` (AggInterpreterBase.cpp ~3061).  Both are freed at scan
close (`releaseScanInterpreters`, DblqhMain.cpp ~28121).  The object is
"a few hundred bytes" (AggInterpreterBase.hpp ~503), so ~95 % of the page
is unused.  Nothing else on the plain aggregate-scan path draws query
memory: the DBTC sites are JoinAgg-only, and the JIT uses `malloc`.  Four
fragment scans per query at T=8 give about 16 live pages per node, which
is ~0.5 MB of data held in 18–22 MB.

**H1: one pool per thread — confirmed, explains up to 4 of the 11.**  The
long-lived lc pool keeps one base per (resource, `thread_id & (N−1)`),
where N is the next power of two ≥ LDM + recv + main + TC threads
(`init_memory_pools` ndbd_malloc_impl.cpp ~2255, `default_map_pool_id`
~4252).  A segment goes back to the global manager only when it is
completely free (`MAX_FREE_LONG_AREAS 0`, ~4131).  With `NumCPUs=4` the
automatic configuration is 4 receive threads and no LDM, TC or main
threads (thr_config.cpp ~336).  Each receive thread hosts one LDM worker,
one query worker and a TC.  Thread ids are 0–3 (mt.cpp ~9890).  That
gives 4 query-memory pools per node, each used by one thread.
*Correction to §6 of m3_plan.md: runs 4 and 6 had 4 LDM workers on 4
receive threads, not 2 LDM threads.*  Any thread with a live aggregate
scan pins at least one 2 MB segment, whatever the group count.  The floor
is therefore 4 segments per node (8 MB) whenever every thread runs one.

**H2: segment classification bug — confirmed in code, explains 1–2 of the
11.**  `check_memory_area_pos` (ndbd_malloc_impl.cpp ~3965) is meant to
find the segment's highest non-empty free-area list at or below
`*check_pos`.  The test `if (i != (*check_pos) + 1)` (~3972) skips list
`*check_pos` itself even when it is still non-empty.  After
`lc_memseg_malloc` carves an area and moves it down a list, the segment
therefore drops to the next lower non-empty list.  On an exact fit it
drops to `POS_MEMORY_AREA_EMPTY` and leaves the base lists.  Either way it
keeps larger free areas it no longer advertises.  A free re-promotes the
segment only up to the class of the area just merged (~4164).  A 32 KB
request starts its search at list 7 (areas ≥ 64 KB, ~3479), so it cannot
use a single freed page anyway.  Once demoted, a segment that still has
several hundred KB free is invisible to page requests, and a new segment
is fetched.

Concrete sequence: a segment's big area F and a merged hole T (both
256 KB–1 MB, list 8) sit side by side.  Pages carved from F move F to
list 7, and the segment drops to 7 although T is in 8.  More pages move F
to list 6; list 7 is now empty, so the segment drops to 6.  The next
32 KB page fetches a new segment while T (~700 KB) is free.

A Python replica of malloc, memseg, split, check and free (scratchpad
`lcsim*.py`), replaying page + `m_buf_block` per scan, gives:
- One pool with 16 live scans: with the bug, a second segment ~25 % of
  the time and ~500 segment fetch/release cycles per 40 k scans.  Each
  cycle takes `mt_mem_manager_lock` and does a 64-page allocation.  With
  the one-line fix: 1 segment, 1 fetch.
- One node (4 pools, 16–32 live scans, 57 one-second samples as in the
  census): peak 5–6 segments with the bug, 4 with the fix, insensitive to
  the size of `m_buf_block`.

**Still unexplained: about 5 segments per node** (9–11 observed, at most
6 modelled).  The model assumes each pool has a single allocating thread,
which holds for scalar scans.  Explaining the gap needs either segments
fetched by several threads into one pool (the lockless-fetch race in
Status of A) or more live allocations per request than the code shows.
Measure it (A5 DUMP below) before claiming more.  GROUP BY does share a
pool across threads — see side finding (a).

**Side findings.**
- (a) `AggInterpreter::Init` calls `initChunkAllocator(/*thread_id=*/0, …)`
  (AggInterpreter.cpp ~128) after `initBufBlock` and
  `m_gb_map->init(m_thread_id)`.  It overwrites `m_thread_id`, so group
  chunks (32 KB) and string MIN/MAX slot arrays from every thread come
  from thread 0's pool.  That means cross-thread contention on one pool
  mutex and exposure to the lockless-fetch segment race.  The GROUP BY
  hash segments stay per thread.
- (b) The churn in H2 is also a CPU cost: every fetch and release goes
  through the global memory-manager lock.
- (c) `VecSearchInterpreter` uses the tail of its 32 KB page
  (VecSearchInterpreter.cpp ~89–93).  Only AggInterpreter can be
  right-sized.

**Proposed fixes (ranked; 1 and 2 written, the rest not).**
1. *Right-size the aggregation interpreter* (RonSQL-local, no allocator
   change).  Allocate `sizeof(AggInterpreter)` instead of `MEM_CHUNK_SIZE`
   in `Create` (aggregation branch) and `CreateAggForRead`
   (PushdownInterpreter.cpp ~270, ~327), or place the object at the head
   of `m_buf_block` (one allocation).  The per-scan footprint drops from
   ~33 KB to ~1–3 KB.  Small requests reuse holes, so for scalar scans the
   32 KB list-7 pattern that triggers H2 goes away.  It also helps F1b
   (one page per aggregating PK read today).  Risk: low; check that
   nothing assumes the object is page-aligned.
   *Written 2026-09-25 (not built or run yet):* `Create` (aggregation
   only; vector search keeps its page, it stores the program in the
   tail), `CreateAggForRead` and both `JoinAggInterpreter` sites in
   DblqhProxy (~3350, ~3424) allocate `sizeof(...)`.  Checked: no
   AggInterpreter / JoinAggInterpreter code uses memory past the object;
   lc pool allocations were never page-aligned (16-byte steps inside a
   segment, 8 bytes past a step), so alignment is unchanged; the free is
   size-agnostic; a write past the object trips the magic `require` in
   `lc_ndbd_pool_free`.  The object is roughly 0.9 KB (estimate from the
   member list).
2. *One-line classification fix* in `check_memory_area_pos`: return the
   first non-empty list from `*check_pos` downward, i.e. drop the
   `i != *check_pos + 1` condition.  When the list is still non-empty the
   position stays unchanged, which the caller already handles
   (`new_pos == old_pos`).  The EMPTY case then only happens for a truly
   full segment.  Effect: no demotion churn, −1 to −2 segments per node,
   fewer global-lock round trips for every lc_ndbd_pool user.  Risk: low
   and local, but it is the core allocator (the earlier allocator change
   was reverted as too intrusive), so it needs your go-ahead.
   *Written 2026-09-25 (not built or run yet):* the fix, a `VM_TRACE`
   check that lists above `*check_pos` are empty, and
   `long_segment_position_test` in `ndbd_malloc-t`.  The test carves
   32 pages, frees pages 1–8 into a second list-8 hole and allocates 8
   more.  Before the fix the 8th page fetches a second segment (checked
   with the `lcsim.py` replica); with the fix there is only one fetch.
3. *GROUP BY chunks on the executing thread*: `initChunkAllocator(m_thread_id, …)`.
   Frees need no thread id, and the chunked teardown (CONTINUEB) runs on
   the owning thread.  First confirm why 0 was chosen (JoinAgg merges
   move chunks between interpreters, but plain AggInterpreter does not).
   Effect: no shared pool 0; possibly an F24 gain.
4. Only if 1–3 are not enough: a small per-Dbtup free list of interpreter
   blocks (bigger change).  Do not cache free segments
   (`MAX_FREE_LONG_AREAS > 0`): idle levels would step, which the leak
   tests reject.

**Concurrency budget with F27 (c).**  After fix 1, an aggregate scan costs
~1–3 KB per fragment scan.  Query memory is then bounded by (a) a floor of
one 2 MB segment per busy block-thread pool and (b) the per-query group
memory of F27 (c) (~550 B per group; 70–110 MB for a many-group CTE
query).  The floor is set by the thread count, not the client count.  In
X0 (`NumCPUs=15`: 6 LDM + 4 TC + 1 main + 3 recv = 14 block threads,
16 pools) it is at most 14 × 2 MB = 28 MB per node.  Admission control
therefore only needs to budget (b).

**Verification (you run).**
- A5 first: a DUMP that prints, for RG_QUERY_MEMORY, each lc base's
  segment count, free words and `m_current_pos`, plus a segment-fetch
  counter.  Run it during an 8-client core_scan_agg to see which pools
  hold the 11 segments.  That settles the unexplained part and fix 3's
  share.
- Census slice with finer sampling:
  `python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py --build prod_build --queries core_scan_agg,core_idx_range,core_avg_range,core_group_few --threads 1,8 --mem-sample 0.2 --cpubind mysql-test/suite/ronsqlcrunch/census.cnf --out <dir>`,
  before and after each fix.  Expect T=1 ≈ 1 segment per busy thread and
  T=8 ≤ 4 per node after fixes 1 and 2.
- A leak-harness-style assertion (new `ronsql_large_mem_qm_budget`):
  8 concurrent scalar aggregates over a 4-fragment table through RDRS,
  polling `ndbinfo.resources` every 100 ms.  Require peak − idle ≤ 64 ×
  (block threads) pages per node, and back to idle after the settle.

### Status of A (2026-09-25)

Tested: ronsql_large_mem_leak, _controls and _strings pass; _errors ran
through except the mle-e5126 step below (the re-check is new, rerun).

- A1 done: L1 and L2 free the group's string slots before
  `eraseAndNext` (the payload is already in the signal / batch); L3
  `mergeFrom` frees the losing string values with the source group.
- A2 done: `mergeOneGroup` frees only the copied slots on a copy failure
  (double free); `decodeRedistributionStringSlots` clears the slots it did
  not reach on a failure, so the cleanup never frees the sender's pointer
  values; `MemChunk::owner` — `freeGroupData` unlinks an emptied chunk from
  its owner's list and `mergeFrom`'s splice re-owns the chunks, so a group
  moved by an interrupted batched merge is freed safely by slot 0's
  teardown (which the proxy chain runs before the sources');
  `sendJoinAggSetupRef` nulls what it frees; the leaf-program array is
  allocated cleared (a REF inside the fill loop released garbage JIT
  handles).
- A2 found by ronsql_large_mem_leak_strings (2026-09-25): 8 concurrent
  string-MIN/MAX queries exhausted the suite's 20 MB SharedGlobalMemory;
  the first failed group allocation asked for an eviction with an empty
  table and `ndbrequire(evict_ret == 0)` in `sendEvictedAggGroup` stopped
  two data nodes (F27 (c)).  `sendEvictedAggGroup` now returns false when
  nothing can be evicted and all seven callers fail the operation with the
  temporary 1870 / 20008; the DBTUP scan feed and error insert 4041 no
  longer evict from a CTE materialization (its table must keep every
  group; the DBLQH CTE feeds already refused).
- Allocator race found by ronsql_large_mem_leak_errors case mle-e5126
  (2026-09-25): after eviction-heavy join aggregation one data node kept
  one 2 MB lc_ndbd_pool segment (64 QUERY_MEMORY pages); a diagnostic run
  showed it survive 120 s idle and every other query type, and go with the
  next join aggregation.  Cause: `lc_mempool_long_lived_pool_malloc`
  releases the pool mutex while one thread fetches a new segment; a thread
  arriving meanwhile fetches its own holding the mutex; both were inserted,
  the first thread's retry could allocate from the second segment, and a
  segment is only released by a free that leaves it fully free — an unused
  one stayed until a later allocation in that pool used it.  Join
  aggregation is exposed because every per-thread interpreter allocates
  from the DblqhProxy thread's pool from several LDM threads.  An allocator
  fix (the lockless fetcher returning its segment when the request fits the
  existing segments after re-locking) was written and reverted
  (2026-09-25, user: too big and too intrusive for the core allocator), so
  this stays known allocator behaviour: bounded (at most one spare segment
  per racing fetch, per pool), released by the pool's next allocate / free
  cycle, but visible to exact leak tests.  The leak harness handles it
  instead (mem_leak_verify.inc reclaim re-check): when a verify times out
  with only QUERY_MEMORY steps of ≥ 64 pages, it runs two plain join
  aggregations through RDRS (mlk_reclaim.body, written by
  mem_leak_init.inc) and polls again for $mlk_recheck_max_polls (15 s); a
  spare segment is then the proxy pool's only segment, gets allocated from
  and released, while a leaked allocation keeps its segment and still
  fails.  The re-check shows only in the report file (a "reclaim query"
  line and "then the reclaim query and N s more" in the verdict line).
  The regression test written for the allocator fix
  (ronsql_large_mem_leak_evict) was dropped with it.  Open idea: per-LDM
  pools for the interpreters' chunks (removes the cross-thread contention on the proxy
  pool's mutex that triggers the race; possibly an F24 gain).  Also seen
  while reading the allocator: a lockless fetch whose backend allocation
  fails returns without decrementing m_num_active_global_malloc, so every
  later fetch of that pool holds the mutex (not fixed).
- A2 DBSPJ RS_ABORTED (2026-09-25, written, not built): a real leak.  A
  scan request waiting for SCAN_NEXTREQ (RS_WAITING) when its TC's node
  dies was parked as an RS_ABORTED tombstone for a SCAN_NEXTREQ that
  cannot come (DBTC take-over handles no scans; DBLQH closes only its
  own scans of the failed TC): Request, arena and m_aggStateKeys /
  m_cteAggStateKeys / m_cteContexts lost until node restart, and DUMP
  2650 would stop the node.  The node-failure tests never killed the TC
  while a worker waited between batches (the CTE phase never waits).
  Fix: `Dbspj::nodeFail` marks a scan whose TC failed
  `RT_REQUESTER_FAILED`; `cleanup` then releases it in full (tombstones
  stay only while the TC lives).  Same change: the DBSPJ node-failure
  sweep resumed every continuation at its first bucket (upstream too),
  so ≥ 64 weighted requests that stay in the hash held it on one prefix
  forever; it now resumes at `iter.bucket`.  Marker
  `[SPJ_ORPHANED_SCANS node= instance= failed= count=]`.  Test NF-13
  `testNodeRestart -n CteCoordinatorDiesWaitingScan T1` (wrapper
  `ndb_cte.cte_nodefail_coordinator_waiting`, autotest daily-basic--16).
- A2 REDISTRIBUTE_REQ after RELEASE (2026-09-25, hardening written, not
  built): cannot reach a live-looking state (DBTC releases only after
  every COMPLETE reply; a CONF'd owner has reconciled every peer's count,
  a REF'd one stays ERROR / NODE_FAIL_ABORT, which teardown does not
  reset), with one exception fixed: the two CTE COMPLETE refusals for a
  malformed key section REFed with the state left SETUP_COMPLETE, so a
  peer's group could be CONF'd and queued into a state being released.
  They now fail it through `abortCteRedistribution` (ERROR, peers told).
  The rule is documented at `DblqhProxy::execJOIN_AGG_RELEASE_REQ`, which
  asserts no owner phase (FINALIZING / SENDING_RESULTS /
  CTE_REDISTRIBUTING) is in progress.  Test testCteProtocol ID-8.  Not
  done (2 c): the owner's identity check reads fields the proxy thread
  re-initialises when it reuses the slot.
- A2 stale-SETUP reclaim under running consumers (2026-09-25, found while
  checking the RELEASE senders; fix written, not built): for a main
  aggregation without CTEs, `close_scan_req` on a RUNNING scan cancels
  the SETUP round and sends the fragment closes at once, and a
  SETUP_CONF arriving before the closes drained was reclaimed
  immediately.  That node's consumers (found by identity, P2c) could
  still feed the state while the RELEASE freed its programs, leaf
  programs, JIT handles and interpreters: `ndbrequire(leafIndex <
  m_num_leaves)` in `Dbtup::handleJoinAggRow` or a use-after-free.
  CTE queries are not exposed (the CTE stage defers the close while
  SETUP replies are outstanding; READY / START_MAIN need every CONF).
  Fix (DBTC `execJOIN_AGG_SETUP_CONF`): with fragments still running the
  CONF keeps its key for `releaseJoinAggResources` at scan release
  (after every fragment closed); otherwise the reclaim stays.  Test
  testCteProtocol PK-9 (5138 hold + new DBTC insert 8315, events
  JOIN_AGG_SETUP_CONF_AFTER_CANCEL / _DEFERRED).  Check the F27 case-25
  crash logs for this signature.
- A2 node-failure sweep vs owner LDM (2026-09-25, written, not built):
  skipping FINALIZING / SENDING_RESULTS is by design, but the owner's
  merge / send continuations stopped only on `m_connected`, which clears
  on DISCONNECT_REP (unordered with NODE_FAILREP): a continuation could
  outlive node-failure handling, be skipped and leak its state.
  `checkJoinAggNodeFailed` now also tests the instance's ZNODE_DOWN
  (`isJoinAggCoordinatorFailed`).  No guard for a late COMPLETE /
  SEND_CONF of the dead coordinator (user, 2026-09-25): after
  NODE_FAILREP nothing arrives from that node until it restarts, which
  needs node-failure handling complete; the park flush replaying a
  parked COMPLETE comes from the proxy like the workers' NODE_FAILREP, so
  it runs first.  Test NF-14 `testNodeRestart -n
  JoinAggCoordinatorDiesAtOwner T1` (error insert 5154 merge hold
  ignoring m_connected; wrapper `ndb_cte.cte_nodefail_joinagg_owner`,
  autotest daily-basic--16).
- A3 done: `c_cteScanIterStatePool` is started (static page at node start
  instead of a never-released transient page on first use); DBTC's
  `c_aggCompleteRecordPool` / `c_cteScanFragHandlePool` reserve one static
  record.  The idle levels move up by these pages at start and no longer
  step at run time.  DBSPJ's arena page stays (upstream RWPool).

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

*Status (2026-09-26): implemented, not yet built or tested.*  New
`server/src/ronsql_worker_pool.{hpp,cpp}`; `RonSQLCtrl::ronsql` parses,
validates and authorizes on the IO loop, then queues a heap
`RonSQLRequest` (owns everything `params` points into) for a worker,
which runs `RonSQLRequest::execute` (the former second half of the
handler) with its own Ndb object and calls the drogon callback (drogon
queues the send onto the connection's loop).  Config `RonSQL.NumThreads`
(16; 0 = old behaviour on the IO loop) and `RonSQL.MaxQueuedRequests`
(1024; full queue = 503, class `resource`).  Worker Ndb indexes sit
between the MySQL-router and TTL-purge ranges (main.cc); shutdown stops
the pool before drogon quits.  `x-ronsql-phases` gains
`,queue=<us>,loop=<n>,worker=<n>` (worker 0 = ran on the IO loop);
`ronsql_phase_stats` (ronsql + ronsql_jit) re-recorded with a
worker ≥ 1 assertion.  Not done: queue metrics in Prometheus, `queue` in
the rondb-cli breakdown, a head-of-line regression test (long statement
next to pk-reads on one loop).

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

*Status 2026-09-26: implemented, not yet built or run.*  Expected cost:
the flattened statement is an `idx_orders_custkey` scan on every fragment
with pushed aggregation, like mysqld's 45 µs NDB wait; its run 6 twins
are fs_hw_agg_point (single-table point aggregate, firstbatch 64 µs, avg
132 µs at T=8) and fs_floor (all-fragment scan, firstbatch 40 µs), so
fs_point should drop from 264 to ~45–70 µs firstbatch and to ~150–200 µs
avg at T=8, under mysqld's 210.
- `RonSQLPreparer::flatten_single_group_cte()` runs in `parse()` before
  the main aggregates are bound to their compiler.  Pattern: one CTE; the
  main is FROM it alone with only MIN / MAX of CTE columns; the body is
  one real table, grouped, every GROUP BY column bound by a top-level
  `col = literal` conjunct (checked by name at parse time), no HAVING,
  ORDER BY, LIMIT or subquery.  Each main output takes the body aggregate
  it names; the main switches to the body's compiler; body aggregates no
  output names are still bound (computed, not printed); GROUP BY and the
  CTE list are dropped.  EXPLAIN: `CTE 'x' flattened into a single-table
  aggregate`.
- Empty group: a body COUNT of a constant becomes **SUM(1)** (n, or NULL
  over no rows), since MAX(n) over an empty CTE is NULL, not 0.  SUM /
  MIN / MAX / AVG are already NULL over no rows.
- Kept on the CTE path: outer COUNT / SUM / AVG, a body COUNT(expr) (0 for
  a group of NULLs), the GROUP BY column in the main, partially bound
  GROUP BY, OR.  Soundness of "one group": RonSQL rejects cross-type
  literals, so the WHERE equality and the GROUP BY agree on one value.
- Tests: new `ronsql_cte_single_group_flatten` (suite ronsql + strict JIT
  mirror, results predicted): 13 flattened cases incl. the empty group,
  a group of NULLs, unnamed body aggregates, AVG, COUNT(1), arithmetic,
  qualified / reversed spellings, two bound GROUP BY columns, string and
  DATE keys; 6 controls pinned to the CTE path with data where a wrong
  flatten changes the answer.  `body_single_group_cte.inc` (×5 suites):
  sg-1 now pins the flatten; sg-14 / sg-15 keep sg-1 / sg-8 on the CTE
  path with an outer COUNT(*) (G4 keyed-probe root on a hit, owner-side
  AVG); sg-16 is the flattened empty group.  `ronsql_float_formatting`:
  a CTE-path twin of the one case that now flattens.
- Bench: fs_point pins the flatten and `idx_orders_custkey`; new
  `fs_point_cte` (outer COUNT(*)) keeps measuring the CTE path.
- First run (2026-09-26): suite ronsql green; the ronsql_jit mirror
  failed control sgf-c2 (CTE path, not the flatten): `MAX(cf.cq)` over
  `COUNT(qty)` of a group whose qty are all NULL read NULL instead of 0.
  Cause: the interpreter's Count() sets its slot to 0 on a group's first
  row even when the value is NULL, but the JIT branches over COUNT on a
  NULL column (`nb_convert_loads`), so the join-agg group record kept an
  undefined slot.  The API maps an undefined COUNT to 0 (RONDB-831), which
  hid it on every non-CTE path.  Fix: `JoinAggInterpreter` group records
  start COUNT slots (and AVG's hidden count) at 0, from the per-slot ops
  now extracted in Init for grouped programs — the Phase I.17 scalar
  pre-init, per group.  The JIT's null-skip is unchanged.

**C3. fs_latest** (ORDER BY … LIMIT 100: firstbatch 641 µs at T=1):
`readTuples(LM_CommittedRead, SF_OrderBy|SF_Descending)` without a batch
size (RonSQLPreparer.cpp ~8330) lets every fragment return up to 990
rows; under an ordered merge each fragment needs at most offset + limit.
Set the batch size; add a fetched-row counter to the phases.

*Status (2026-09-25, uncommitted, not built).*  Confirmed from run 6:
the 990 is the census config's `BatchSize=990` (`[api default]` via
ronsqlcrunch/my.cnf; the NDB API default is 384), and the mysqld twin
reads 4.0 batches / 3960 rows / 170 KB per request (4 fragments x 990)
to return 100 rows — both engines over-read alike.  RonSQL has no
OFFSET, so the batch is the LIMIT.
- Batch: `open_single_table_scan_op(batch_rows)` passes the batch to
  both `readTuples` calls; the pass-through scan arm passes
  max(LIMIT, 1) when the LIMIT streams (index order or no ORDER BY — so
  Phase 2's unordered LIMIT benefits too); the Phase 3 buffered sort
  and the aggregate path keep the default.  The API caps it at
  BatchSize.  Expected fs_latest: 400 rows fetched (4 x 100).
- Counter: `RonSQLPhaseStats::rows_fetched` = `Ndb::ReadRowCount`
  delta across the attempt (ronsql_op; mysqld's ndb_api_read_row_count,
  so directly comparable to section D's rows/req), appended to
  x-ronsql-phases as `fetched=`; rondb-cli prints "rows fetched N per
  request"; the matrix driver adds a `fetched` column to report C.
- Tests: `ronsql_phase_rows.inc` takes `$EXPECT_FETCHED_MAX`
  (records only the verdict: the count depends on the fragment count);
  orderby_index poi-4 / 5 / 8 and passthrough_limit pl-15 assert
  fetched <= LIMIT x fragments; new poi-19 forces a mid-merge batch
  refill (whole top-400 in one fragment of a `PARTITION BY KEY (p)`
  table, LIMIT above the 384-row BatchSize); ronsql_phase_stats
  baselines gain `,fetched=N`.

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
