# CTE main JoinAgg batch continuation: DBSPJ row accounting fix

Supersedes the earlier version of this plan, which proposed that DBTC
self-send `SCAN_NEXTREQ` to continue delivered main-scan fragments. Deeper
analysis showed the hang is caused one layer upstream, in DBSPJ row
accounting, and that DBSPJ already owns batch continuation for aggregating
queries. The fix belongs there. DBTC's role is the opposite of the earlier
proposal: it must *refuse* to get involved in case-2 mid-fragment flow
control, and fail loud if it is ever asked to.

All line numbers refer to the tree at commit `bc1594832be`.

**Status: Changes 1-4 implemented, Change 5 audit complete (findings
recorded below).  Pending: build + verification plan runs (items 1-8).**

## Trigger

The observed hang was reproduced by a RonSQL query with two aggregating CTEs
and an aggregating main SELECT:

```sql
WITH order_stats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey),
recent_stats AS (
  SELECT o_custkey AS k2, COUNT(*) AS recent_cnt
  FROM orders WHERE o_orderdate >= '1998-01-01' GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(order_stats.cnt), SUM(order_stats.spend),
       SUM(recent_stats.recent_cnt)
FROM customer AS c
JOIN order_stats ON order_stats.k = c.c_custkey
JOIN recent_stats ON recent_stats.k2 = c.c_custkey
GROUP BY c.c_nationkey;
```

Debug logs show the query reaches the main SELECT phase; the CTE hash tables
exist and the main scan is doing `CTE_LOOKUP` probes. The last useful DBTC
trace shows a main-scan fragment batch boundary:

```text
TC scanPtrI: 0, done: 0, scan_frag_conf_status: 0, scanFragPtrI: 3 ...
TC scanPtrI: 0, Send SCAN_TABCONF and wait for API
sendScanTabConf: suppress intermediate SCAN_TABCONF for CTE JoinAgg
```

No later `SCAN_NEXTREQ` or `JOIN_AGG_COMPLETE_REQ` appears for the main scan.

## Classification

Three relevant flows, and who owns batch continuation in each:

1. **CTE materialization phase.**
   DBTC suppresses `SCAN_TABCONF` (`execSCAN_FRAGCONF`, DbtcMain.cpp:18672);
   continuation is driven entirely inside DBSPJ by
   `handleCtePhaseNextBatch()` (DbspjMain.cpp:3825).

2. **Main SELECT with CTEs and a main aggregation program** (this bug).
   Raw main-scan batches are only inputs to `JoinAggInterpreter`; final rows
   are delivered after `JOIN_AGG_COMPLETE` via
   `sendJoinAggScanTabConf()` (terminal `EndOfData | 1` conf targeting the
   agg receiver, DbtcMain.cpp:31292). Batch continuation is owned by
   **DBSPJ**: `Dbspj::batchComplete()` detects an aggregating request whose
   batch delivered nothing to the API and fetches the next batch itself,
   without any DBTC/API round-trip (DbspjMain.cpp:3712):

   ```cpp
   if (!is_complete && requestPtr.p->m_errCode == 0 &&
       !requestPtr.p->m_aggNodes.isclear() &&
       requestPtr.p->m_rows == 0) {
     handleJoinAggNextBatch(signal, requestPtr);
     return;
   }
   sendConf(signal, requestPtr, is_complete);
   ```

   `handleJoinAggNextBatch()` (DbspjMain.cpp:3754) also sends the throttled
   `SCAN_HBREP` keep-alives that DBTC honors since `bc1594832be`. This is
   exactly how **non-CTE** main-aggregation scans already survive silent
   batch boundaries.

3. **Main SELECT without a main aggregation program, with or without CTEs.**
   Normal API `SCAN_TABCONF` / `SCAN_NEXTREQ` flow control is the result
   delivery protocol. DBTC forwards confs; the API drives continuation.

For case 2, DBTC should only ever see a `SCAN_FRAGCONF` when a fragment
genuinely completes (`fragmentCompleted == 1`), never at a silent
mid-fragment batch boundary. The trigger query violates that — and the
violation, not DBTC's suppression, is the bug.

## Root cause: phantom rows in `Request::m_rows`

`m_rows` means "rows delivered to the API in this batch". It becomes
`ScanFragConf::completedOps` (`sendConf`, DbspjMain.cpp:4031), which DBTC
forwards as the per-receiver row count in `SCAN_TABCONF`, which the API uses
as the number of `TRANSID_AI` rows to expect. Every counting site respects
this — except one:

| Site | Condition for counting | Correct for main-agg requests? |
|------|------------------------|--------------------------------|
| Root/child scan conf (DbspjMain.cpp:13173) | `m_aggNodes.isclear() \|\| T_AGGREGATE_LEAF`, and not `T_CTE_INDIRECT_FEED` | yes — explicitly excludes agg-consumed rows |
| CTE_SCAN conf (DbspjMain.cpp:7744) | three documented paths; agg-feed path (a) never touches `m_rows` | yes |
| Real-table lookup conf (DbspjMain.cpp:9079) | agg leaf: only `readLen` (evicted rows actually sent to API); else only `T_USER_PROJECTION` | yes |
| **CTE_LOOKUP conf (DbspjMain.cpp:6894)** | `!T_AGGREGATE_LEAF && m_cteId == RNIL` — **no main-agg guard** | **no** |

```cpp
// DbspjMain.cpp:6894 — current code
if (!(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) &&
    treeNodePtr.p->m_cteId == RNIL) {
  requestPtr.p->m_rows++;
}
```

In the trigger query only one of the two CTE_LOOKUP ops carries
`NI_AGGREGATE_LEAF` — the one holding the main aggregation program
(`m_isAggregateLeaf = options.hasAggregation()`, NdbQueryBuilder.cpp:2333).
Main-query probe nodes have `TreeNode::m_cteId == RNIL` (`m_cteId` is set
only on nodes *inside* materialization subtrees, DbspjMain.cpp:2144; the
referenced CTE id lives in `m_cteLookup_data.m_cteId`). So the **other**
CTE_LOOKUP increments `m_rows` once per probed customer row, even though in
a main-agg query its result feeds the aggregation engine and the API never
sees a single row.

Consequences per main-scan batch:

- `m_rows > 0` → the `m_rows == 0` self-continue check at
  DbspjMain.cpp:3712 fails → `sendConf` emits a mid-fragment
  `SCAN_FRAGCONF` with a phantom `completedOps`.
- DBTC queues the fragment; `sendScanTabConf` (DbtcMain.cpp:19753) moves it
  to `DELIVERED` (19841-19847), then the case-2 suppression (19902-19911)
  correctly withholds the `SCAN_TABCONF` from the API — so no
  `SCAN_NEXTREQ` can ever arrive, the fragment is parked forever, the
  completion condition (`m_delivered_scan_frags.isEmpty() &&
  m_running_scan_frags.isEmpty()`, 19855) is unreachable, and
  `sendJoinAggCompleteReqs()` is never called.

Why only long queries hang: a case-2 query whose main scan fits in one
batch produces only fragment-completion confs, which take the
`done → COMPLETED` path and reach `WAIT_JOIN_AGG_COMPLETE` before the
suppression branch.

Why forwarding the conf instead would be worse: intermediate `SCAN_TABCONF`
ops entries are dispatched to the per-worker **query** receivers
(NdbTransactionScan.cpp:154-177), so the API would add the phantom
`rowCount` to that receiver's expected rows and wait forever for
`TRANSID_AI` that never comes. The suppression has been masking wrong data;
the earlier plan would have kept masking it while adding DBTC-side
continuation machinery to work around the side effect.

## Design goals

- Fix the row accounting where it is wrong: DBSPJ.
- Case-2 batch continuation stays owned by DBSPJ
  (`handleJoinAggNextBatch`), uniform with non-CTE aggregation.
- DBTC must **not accept involvement** in case-2 mid-fragment flow
  control: any such request is a protocol violation and must fail loud
  (clean scan abort with a distinct error code), never park state that can
  only hang.
- API semantics unchanged: no intermediate `SCAN_TABCONF` for case 2, final
  aggregate rows via `JOIN_AGG_COMPLETE` / `sendJoinAggScanTabConf`.
- Cases 1 and 3 unchanged.

## Implementation

### Change 1 (the fix): guard `m_rows++` in `execCTE_LOOKUP_CONF` — DBSPJ

DbspjMain.cpp:6894 — mirror the scan-side rule (13173):

```cpp
// Count FLUSH_AI result sent to API — same as lookup_countSignal does
// for regular lookups (T_USER_PROJECTION → m_rows++). Without this,
// SCAN_FRAGCONF::completedOps undercounts and the API asserts on
// outstanding results mismatch.
//
// Skip for requests with a MAIN aggregation program.  A join with
// non-clear m_aggNodes IS an aggregate join: m_aggNodes is populated
// only from the main [nodeId, aggStateKey] pairs (aggregating CTE
// bodies never set it), so it is non-clear exactly when the main
// SELECT aggregates.  T_AGGREGATE_LEAF is the wrong test for that:
// it only marks the aggregation FEED POINT — the one op carrying the
// aggregation program — while an aggregate join can have tree nodes
// that are not T_AGGREGATE_LEAF, e.g. an internal CTE_LOOKUP whose
// columns reach the leaf's feed via linked projections.  Rows of
// such nodes never reach the API either, so counting them here
// (the old !T_AGGREGATE_LEAF check did) inflates m_rows with rows
// the API will never see.
//
// In an aggregate join the API receives nothing per batch, and the
// same m_aggNodes rule is applied in scanFrag_execSCAN_FRAGCONF.  A
// non-zero m_rows would defeat the handleJoinAggNextBatch()
// self-continue in batchComplete() and emit a mid-fragment
// SCAN_FRAGCONF that DBTC must not act on — the API never gets an
// intermediate SCAN_TABCONF for such scans, so DBTC would park the
// fragment in DELIVERED with no possible continuation (query hang).
//
// Also skip for nodes inside CTE subtrees (result feeds the
// enclosing CTE's aggregator, not the API).
if (requestPtr.p->m_aggNodes.isclear() &&
    treeNodePtr.p->m_cteId == RNIL) {
  requestPtr.p->m_rows++;
}
```

Notes:

- `m_aggNodes` is the per-data-node MAIN aggStateKey set, populated only
  from the main `[nodeId, aggStateKey]` pairs before `CTE_KEYS_MARKER`
  (DbspjMain.cpp:1581). Non-empty exactly in case 2; empty in case 3, so
  the case-3 counting this site was added for (see the existing comment
  about API undercount asserts) is preserved.
- The old `!T_AGGREGATE_LEAF` conjunct is subsumed: a main-query aggregate
  leaf implies a main aggregation program, hence `m_aggNodes` non-empty.
- Alternative considered: gate on `T_USER_PROJECTION` like the regular
  lookup arm (9089) and CTE_SCAN path (b) (7757). Functionally equivalent
  if RonSQL never registers per-op projections on aggregate queries, but
  the `m_aggNodes` form restates the rule already proven at 13173 and does
  not depend on what the client serialized.

Effect: every silent case-2 batch now has `m_rows == 0`, the
DbspjMain.cpp:3712 check fires, `handleJoinAggNextBatch()` fetches the next
batch directly from DBLQH (with `SCAN_HBREP` keep-alives), and DBTC never
sees a mid-fragment `SCAN_FRAGCONF` for case 2. The hang disappears with no
DBTC behavior change required.

### Change 2: DBTC refuses mid-fragment confs for case 2 — `execSCAN_FRAGCONF`

DBTC must not accept work it cannot complete. After Change 1, a
mid-fragment (`fragmentCompleted == 0`) `SCAN_FRAGCONF` for a case-2 scan
can only mean broken accounting upstream (or an undefined eviction
delivery, see "Deferred"). Parking the fragment in `DELIVERED` is a
guaranteed hang; instead, abort the scan cleanly.

In `execSCAN_FRAGCONF` (DbtcMain.cpp:18666-18684), the arriving conf's
`status` is in scope and the fragment has just been queued:

```cpp
if (scanptr.p->m_queued_count > /** Min */ 0) {
  jamDebug();
  if (scanptr.p->m_numCtes > 0 &&
      scanptr.p->m_cteCurrentPhase < scanptr.p->m_ctePhaseCount) {
    jam();
    /* CTE phase: suppress entirely — DBSPJ drives continuation. */
    DEB_JOIN_AGG(...);
  } else {
    if (scanptr.p->m_joinAgg &&
        scanptr.p->m_numCtes > 0 &&
        scanptr.p->m_hasMainAggProgram &&
        status == 0) {
      jam();
      /* Case-2 mid-fragment batch boundary.  DBSPJ owns continuation
       * for aggregating requests (handleJoinAggNextBatch) and must
       * never hand a silent batch to DBTC: the API gets no
       * intermediate SCAN_TABCONF for this scan, so no SCAN_NEXTREQ
       * can ever continue a DELIVERED fragment.  Receiving one here
       * means broken row accounting upstream.  Fail loud instead of
       * parking the fragment and hanging the query. */
      scanError(signal, scanptr, ZCTE_AGG_BATCH_PROTOCOL_ERROR);
      return;
    }
    sendScanTabConf(signal, scanptr, apiConnectptr);
  }
}
```

Details:

- `scanError` (DbtcMain.cpp:18414) requires `scanState == RUNNING` (true
  here: the case-2 main scan runs with all CTE phases complete, so the
  join-agg close deferral in `close_scan_req` at 19053-19060 does not
  trigger) and proceeds to a normal close: the just-queued fragment is
  drained by the `QUEUED_FOR_DELIVERY` arm (19188-19213), and the API gets
  `SCAN_TABREF` with the error code — a clean, retryable failure instead of
  a hang.
- New error code: `#define ZCTE_AGG_BATCH_PROTOCOL_ERROR 1270` in
  `Dbtc.hpp`, next to DBLQH's CTE-agg family (`ZCTE_AGG_FEED_SELF_REFERENCE
  1269`, Dblqh.hpp:504). Register in `ndberror.cpp` following the 1269
  entry (ndberror.cpp:481), e.g.
  `{ 1270, DMEC, IE, "CTE join aggregation scan received an unexpected intermediate batch boundary" }`.
- The guard deliberately fires regardless of `completedOps`. A
  `completedOps > 0` mid-fragment conf for case 2 would today mean group
  eviction (see "Deferred") — whose delivery contract is undefined for the
  CTE path; a clean error is strictly better than either hang mode.

### Change 3: tighten the `sendScanTabConf` suppression branch — DBTC

Keep the suppression (DbtcMain.cpp:19902-19911) — after Change 2 it only
ever handles per-fragment **completion** confs for case 2 while other
fragments still run, and those must still be withheld from the API (the
API's terminal conf is `sendJoinAggScanTabConf`). Make the new invariant
explicit:

- Re-document the branch: "only fragment-completion confs can reach here
  for CTE JoinAgg scans; mid-fragment boundaries are rejected in
  execSCAN_FRAGCONF and silent batches are continued inside DBSPJ."
- Add `ndbassert(scanPtr.p->m_delivered_scan_frags.isEmpty());` inside the
  suppressed return. For aggregate queries every fragment starts up front
  (`scanParallelism == fragCount`; NdbQueryOperation forces
  `m_fragsPerWorker = 1`), so `left == 0` and a completion conf always
  takes `done → COMPLETED` — no case-2 fragment can legally be parked in
  `DELIVERED`. If a future shape breaks the parallelism assumption, the
  assert catches it in debug builds instead of reintroducing a silent hang.

### Change 4: DBTC rejects non-close `SCAN_NEXTREQ` for case-2 scans

The API never receives an intermediate `SCAN_TABCONF` for a case-2 scan and
the final conf is terminal (`EndOfData | 1`, DbtcMain.cpp:31311), so the
only legitimate `SCAN_NEXTREQ` for such a scan is a close
(`stopScan == 1`). A stray or malicious data-fetch `SCAN_NEXTREQ` would run
the receiver-id loop (DbtcMain.cpp:18949) whose
`c_scan_frag_pool.getPtr` / `ndbrequire(scanFragState == DELIVERED)` can
crash the node on untrusted input.

In `execSCAN_NEXTREQ`, after the `stopScan` close dispatch (18917) and the
`CLOSING_SCAN` check (18926), add:

```cpp
if (scanP->m_joinAgg && scanP->m_numCtes > 0 &&
    scanP->m_hasMainAggProgram) {
  jam();
  /* CTE JoinAgg main scans are flow-controlled by DBSPJ; the API is
   * never given fragments to continue.  Nothing to do — and the
   * receiver-id loop below must not run on ids we never handed out. */
  g_eventLogger->warning(
      "TC %u : ignoring non-close SCAN_NEXTREQ from node %u for "
      "CTE JoinAgg scan (ACR %u)",
      instance(), refToNode(signal->senderBlockRef()), apiConnectptr.i);
  return;
}
```

Ignore-and-warn (not disconnect): the signal is harmless once dropped, and
a well-behaved API can only send it due to a version-skew bug, which should
not take the connection down.

### Change 5: audit the remaining `m_rows` arms

Sweep the other counting sites for the same class of bug on multi-batch
case-1/case-2 shapes. **Audit done; findings:**

- [x] `execCTE_SCAN_CONF` paths (b) and (leaf) (DbspjMain.cpp:7757-7771):
      **safe.** A main-query `CTE_SCAN` aggregate leaf takes accounting
      path (a): `cte_scan_sendReq` computes a non-RNIL
      `data.m_joinAggStateKey` for every `T_AGGREGATE_LEAF` node,
      including the main-query arm feeding `m_aggStateKeys[targetNodeId]`
      (DbspjMain.cpp:7554-7585).  Path (b) `T_USER_PROJECTION` cannot be
      set in an aggregate request (see next item).  Residual: the bare
      `isLeaf()` arm would count rows for a non-agg-leaf CTE_SCAN leaf in
      an aggregate main query — that is not a plannable shape (every
      main-query leaf in an agg query carries an agg program; the
      multi-leaf 0x0722 format assigns one per leaf), so no code change;
      revisit if such a shape is ever introduced.
- [x] `lookup_execLQHKEYCONF` `T_USER_PROJECTION` arm (DbspjMain.cpp:9101):
      **safe.** `parseDA` suppresses the user projection — and therefore
      never sets `T_USER_PROJECTION` — for every non-agg-leaf node of an
      `RT_AGGREGATE` request and for every non-agg-leaf node inside a CTE
      subtree (`suppressFlushAI`, DbspjMain.cpp:15339-15354).  Internal
      real-table lookups in aggregate queries cannot phantom-count.
- [x] Root-scan arm (DbspjMain.cpp:13173): **safe.** DBLQH updates the
      batch counters that become `ScanFragConf::completedOps` only in the
      non-join-agg arm of `scanTupkeyConfLab` (DblqhMain.cpp:23475-23479);
      for `m_join_agg_state_key != RNIL` scans (which includes
      CTE-materialization root scans — JoinAggFlag is set on their
      SCAN_FRAGREQ) the counters stay 0, so `completedOps == 0` for all
      silently-consumed batches regardless of the `m_aggNodes` guard.
      Note scan-feed evictions bump only `m_join_agg_evict_rows`
      (DblqhMain.cpp:23451-23453), not `completedOps` — consistent with
      the deferred eviction-contract item.
- Case-2 root scan for completeness: the main-scan root (e.g. the
  customer scan in the trigger query) delivers TRANSID_AI to DBSPJ to
  drive the probes, so its DBLQH conf legitimately reports
  `completedOps > 0` — the 13173 guard (`m_aggNodes` non-clear, root not
  `T_AGGREGATE_LEAF`) correctly keeps those rows out of `m_rows`.

## Explicitly rejected / deferred

### Rejected: DBTC self-sending `SCAN_NEXTREQ` (the previous version of this plan)

Beyond being unnecessary once the accounting is fixed, the mechanism was
hazardous:

- `execSCAN_NEXTREQ`'s validation is API-oriented. A self-sent signal
  processed after the scan closed (cross-thread job-buffer ordering allows
  an LQH close-conf to overtake it) lands in `handleSignalStateProblem`
  (DbtcMain.cpp:18787) → `disconnectMaliciousNode()` **on the node's own
  id**, plus `ndbassert(false)` in debug builds.
- The wrong-transid path replies `SCAN_TABREF` to the sender — DBTC has no
  `SCAN_TABREF` handler, so that is an ndbabort.
- It would have duplicated flow-control logic that DBSPJ already owns for
  aggregating queries, leaving the phantom `completedOps` in the conf
  stream as a trap for any future change that forwards intermediate confs.

If DBTC-side continuation is ever wanted again, it needs an internal-origin
flag bit in `ScanNextReq::stopScan` (precedent: bit 2 `sent_from_queue`)
with graceful drops on every validation failure — but Change 2's fail-loud
guard is the appropriate DBTC posture for this protocol.

### Deferred: eviction delivery contract for case 2

Group eviction (aggregator memory pressure; forced by ERROR_INSERT 5090)
sends partial-result rows to the API mid-scan and is the one *legitimate*
source of `m_rows > 0` in an aggregating batch (scan arm 13173
`T_AGGREGATE_LEAF`, lookup arm 9088 `readLen`). For the CTE path this is
undefined today: `CTE_LOOKUP_CONF` carries no eviction count at all, and
the case-2 suppression would swallow the conf while the evicted
`TRANSID_AI` already reached the agg receiver. With this plan, a case-2
eviction that produces a mid-fragment conf hits Change 2's guard and fails
with error 1270 — a clean abort documenting the gap, instead of a hang or
silent miscount. Designing the real contract (count evictions on the agg
receiver's conf entry, or buffer evicted groups until `JOIN_AGG_COMPLETE`)
is follow-up work; note that the API-side completion check
(`isAggReceiveComplete`, NdbQueryOperation.cpp:2251) uses
`received >= expected`, which tolerates under-counted expectations.

## Review points

- The DBSPJ guard must not change case-3 behavior: multi-CTE non-aggregate
  queries rely on the `m_rows++` for their API row accounting (the original
  undercount bug this site fixed).
- `m_aggNodes` must be populated before any `CTE_LOOKUP_CONF` can arrive
  (it is parsed from the SCAN_FRAGREQ agg-keys section at build time, prior
  to any probe being sent).
- Change 2's `scanError` runs with the fragment already in
  `m_queued_scan_frags` — verify `close_scan_req`'s queued-drain arm covers
  it (it does: 19188-19213, including the `m_queued_count` decrement).
- Change 4 must not block scan close (`stopScan == 1` dispatches before the
  guard) or the final `WAIT_JOIN_AGG_*` states (guard is only reachable in
  `RUNNING`-family states via the earlier `CLOSING_SCAN` check; wait states
  are only entered with empty fragment lists).
- CTE-phase suppression in `execSCAN_FRAGCONF` (case 1) remains untouched.
- Non-CTE join aggregation is unaffected: Change 1 only alters CTE_LOOKUP
  accounting; Changes 2-4 are gated on `m_numCtes > 0`.

## Verification plan

1. **Confirm the diagnosis before coding** (optional but cheap): re-run the
   trigger query with `DEBUG_CTE` enabled; the existing trace
   `execCTE_LOOKUP_CONF: after decrement ... m_rows=N` (DbspjMain.cpp:6899)
   must show `m_rows` growing on the non-agg-leaf CTE_LOOKUP node.

2. **Trigger query after Change 1.** Expected debug trace:
   - no mid-fragment `SCAN_FRAGCONF` for the main scan reaches DBTC;
   - `DBSPJ send SCAN_HBREP next-batch` lines instead (throttled, ≥20 ms);
   - per-fragment completion confs only, then `send JOIN_AGG_COMPLETE_REQ`;
   - final aggregate `SCAN_TABCONF` (`EndOfData`) to the API;
   - correct results vs mysqld baseline.

3. **Case-2 multi-batch regression.** Add a `ronsql_large`-suite case (the
   `load_ronsql_large` loader provides `lg_cust`/`lg_orders`) with two
   aggregating CTEs joined to a scanned table, sized to force several
   main-scan batch boundaries — the trigger shape, kept in the permanent
   regression net across the `ronsql_cte_ng*` topologies.

4. **Case 3 regression** (protects the original undercount fix): multi-CTE
   non-aggregate main SELECT returning enough rows to span batches; API
   must receive correct per-batch row counts and drive `SCAN_NEXTREQ`.

5. **Case 1 regression:** a CTE-materialization-heavy query still shows
   `execSCAN_FRAGCONF: suppress SCAN_TABCONF during CTE phase` and no
   API-visible intermediate confs.

6. **Non-CTE join aggregation regression** (`testJoinAggNdbApi`,
   `bench_q9_dbtc`): no behavior change — Change 1 doesn't touch their
   accounting, Changes 2-4 are `m_numCtes`-gated.

7. **Guard coverage:** temporarily revert Change 1 in a debug build and
   re-run the trigger query — Change 2 must convert the former hang into a
   clean error 1270 with `SCAN_TABREF` to the API.

8. **Eviction probe (expected error, documents the deferred gap):**
   ERROR_INSERT 5090 + the trigger query → error 1270, no hang, no crash.
