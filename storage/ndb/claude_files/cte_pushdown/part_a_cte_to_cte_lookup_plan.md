# Part A — CTE-to-CTE Lookup: Investigation & Fix Plan

## Goal

Allow a CTE's materialization subtree to contain `lookupCte()` into an
earlier CTE. SQL equivalent:

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp),
     cte1 AS (SELECT grp, SUM(cte0.total) AS total
              FROM cte_src s JOIN cte0 ON s.grp = cte0.grp
              GROUP BY cte0.grp)
SELECT s.grp, cte1.total FROM cte_src s JOIN cte1 ON s.grp = cte1.grp;
```

CTE 1 joins CTE 0 by key (point lookup), not by full scan.

## What Already Works

The runtime plumbing for feeding CTE_LOOKUP results into an enclosing
CTE's hash table is **already in place**. Specifically,
`cte_lookup_send` at `DbspjMain.cpp:5861-5884` routes CTE_LOOKUP
results into the enclosing CTE's `joinAggStateKey` when
`T_AGGREGATE_LEAF && treeNodePtr.p->m_cteId != RNIL`:

```c++
if (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) {
  Uint32 baseKey;
  if (treeNodePtr.p->m_cteId != RNIL) {
    // Inside a CTE subtree: feed into enclosing CTE's aggregation
    ...
    baseKey = requestPtr.p->m_cteAggStateKeys[encCteIdx * MAX_NDB_NODES + targetNodeId];
  } else {
    // Main query: feed into main aggregation
    baseKey = requestPtr.p->m_aggStateKeys[targetNodeId];
  }
  joinAggStateKey = JoinAggregationState::encodeAggStateKey(baseKey, leafIdx);
}
```

The NDB API side also already permits `lookupCte()` inside a subtree:
`NdbQueryBuilder::lookupCte()` has no `m_inCteSubtree` check.

The build loop (`DbspjMain.cpp:1945-1967`) already knows to NOT mark
CTE_LOOKUP / CTE_SCAN nodes inside a subtree with `T_CTE_SCAN`, to
avoid `execCTE_PHASE_START_REQ` trying to start them as subtree
roots.

## What's Missing — Three Bugs

### Bug 1 — `m_cteId` never set for CTE_LOOKUP/CTE_SCAN inside a subtree

`DbspjMain.cpp:1945-1968`:

```c++
if (ctx.m_cteSubtreeRemaining > 0 &&
    node_op != QueryNode::QN_CTE_SUBTREE) {
  ctx.m_cteSubtreeRemaining--;

  if (node_op != QueryNode::QN_CTE_LOOKUP &&
      node_op != QueryNode::QN_CTE_SCAN) {
    Ptr<TreeNode> nodePtr = ctx.m_node_list[ctx.m_cnt];
    nodePtr.p->m_bits |= TreeNode::T_CTE_SCAN;
    nodePtr.p->m_cteId = ctx.m_cteSubtreeCteId;   // <-- inside the if
    // record scanTreeNodeNo in CteContext
    ...
  }
}
```

The `m_cteId` assignment is nested inside the branch that also marks
`T_CTE_SCAN`. A CTE_LOOKUP nested inside a subtree therefore keeps the
default `m_cteId == RNIL`. Consequence: `cte_lookup_send`'s routing
logic at line 5864 falls through to the `else` branch and uses
**main**'s aggStateKey, corrupting the main aggregator and leaving
CTE 1's hash table empty.

### Bug 2 — Main-root selection picks a CTE_LOOKUP inside a subtree

Two iterations find "the first main-query node" by skipping
`T_CTE_SCAN` and `g_CteSubtreeOpInfo` containers:

- `DbspjMain.cpp:3113-3119` (`checkPrepareComplete`)
- `DbspjMain.cpp:6647-6654` (`execCTE_START_MAIN_REQ`)

```c++
for (list.first(nodePtr); !nodePtr.isNull(); list.next(nodePtr)) {
  if (!(nodePtr.p->m_bits & TreeNode::T_CTE_SCAN) &&
      nodePtr.p->m_info != &g_CteSubtreeOpInfo) {
    break;
  }
}
```

A CTE_LOOKUP inside a subtree has neither flag, so it is picked as the
"main query root" — wrong. Phase-start and main-start would try to
drive the nested CTE_LOOKUP as a top-level scan root.

### Bug 3 — `validateAggregateFlags` mis-classifies CTE_LOOKUP inside a subtree

`validateAggregateFlags` uses `T_AGGREGATE_LEAF && !T_CTE_SCAN` to
identify **main-query** aggregate leaves. Applied at
`DbspjMain.cpp:2044-2045`, `2107-2108`, `2137-2138`, `2200`.

A CTE_LOOKUP inside a subtree with `setAggregation()` option:
- has `T_AGGREGATE_LEAF` set
- does NOT have `T_CTE_SCAN` set

…and so is counted as a main-query agg leaf by this predicate. That's
semantically wrong: it feeds **CTE 1**'s hash table, not the main
aggregator. Consequences:

- `aggregate_leaf_count` is off by one
- `commonParent` validation across multiple leaves is corrupted
- `T_AGGREGATE_ANCESTOR` propagation walks the wrong tree
- Node-count balance at line 2081
  (`m_aggregate_node_count + cteNodeCount != ctx.m_cnt`) fails,
  because the nested CTE_LOOKUP is counted in neither total
  (`m_aggregate_node_count` is gated on `ctx.m_cteSubtreeRemaining == 0`
  at line 13107, which is false inside a subtree; and `cteNodeCount`
  only counts `T_CTE_SCAN` nodes plus subtree containers).

## The Fix

Use **`m_cteId != RNIL`** as the canonical "part of a CTE subtree"
predicate throughout DBSPJ, not `T_CTE_SCAN`. This predicate covers
uniformly:

- CTE subtree containers (m_cteId set in `cte_subtree_build:6130`)
- Base-table materialization nodes (m_cteId set by the build loop)
- CTE_LOOKUP / CTE_SCAN nested inside a subtree (m_cteId **will be
  set** by this fix)
- Main-query nodes: m_cteId == RNIL from `TreeNode` ctor default

### Edit 1 — Build loop (`DbspjMain.cpp:1945-1968`)

Lift the `m_cteId` assignment out of the T_CTE_SCAN branch so it
fires for every node inside a subtree. Keep `T_CTE_SCAN` marking and
`scanTreeNodeNo` recording conditional on non-CTE_LOOKUP/non-CTE_SCAN.

```c++
if (ctx.m_cteSubtreeRemaining > 0 &&
    node_op != QueryNode::QN_CTE_SUBTREE) {
  ctx.m_cteSubtreeRemaining--;
  Ptr<TreeNode> nodePtr = ctx.m_node_list[ctx.m_cnt];

  /* All nodes inside a subtree belong to the enclosing CTE —
   * record the cteId so cte_lookup_send() can route CTE_LOOKUP
   * feeds into the enclosing CTE's aggStateKey. */
  nodePtr.p->m_cteId = ctx.m_cteSubtreeCteId;

  if (node_op != QueryNode::QN_CTE_LOOKUP &&
      node_op != QueryNode::QN_CTE_SCAN) {
    /* Base-table node — mark as a CTE materialization root and
     * record it as the subtree's scan tree node. CTE_LOOKUP /
     * CTE_SCAN nested inside a subtree read from OTHER CTEs and
     * must not become subtree roots. */
    jam();
    nodePtr.p->m_bits |= TreeNode::T_CTE_SCAN;
    for (Uint32 i = 0; i < requestPtr.p->m_numCtes; i++) {
      if (requestPtr.p->m_cteContexts[i].m_cteId ==
          ctx.m_cteSubtreeCteId) {
        requestPtr.p->m_cteContexts[i].m_scanTreeNodeNo =
            nodePtr.p->m_node_no;
        break;
      }
    }
  }
}
```

### Edit 2 — Main-root selection (`DbspjMain.cpp:3115`, `6650`)

Replace the T_CTE_SCAN / container filter with a single `m_cteId ==
RNIL` check. Main-query nodes have `m_cteId == RNIL`; every subtree
node (container, materialization leaf, nested CTE_LOOKUP/CTE_SCAN)
has it set.

```c++
for (list.first(nodePtr); !nodePtr.isNull(); list.next(nodePtr)) {
  if (nodePtr.p->m_cteId == RNIL) {
    break;   // first main-query node
  }
}
```

### Edit 3 — `validateAggregateFlags` (`DbspjMain.cpp:2044-2200`)

Replace `!(m_bits & T_CTE_SCAN)` with `m_cteId == RNIL` in all four
main-leaf gating sites (2044-2045, 2107-2108, 2137-2138, 2200). Also
update the `cteNodeCount` counting loop at 2068-2080 to count
`m_cteId != RNIL` uniformly instead of the two-pass T_CTE_SCAN +
container count.

```c++
Uint32 cteNodeCount = 0;
for (list.first(treeNodePtr); !treeNodePtr.isNull();
     list.next(treeNodePtr)) {
  if (treeNodePtr.p->m_cteId != RNIL) {
    cteNodeCount++;
  }
}
```

This count now includes: CTE containers, base-table materialization
nodes, and CTE_LOOKUP / CTE_SCAN nested inside a subtree — all
accounted for in the "not a main query node" bucket. The
`m_aggregate_node_count + cteNodeCount == ctx.m_cnt` balance
restored.

## Test: testCteNdbApi Test 5 — CTE-to-CTE Lookup

The three bugs manifest in a **single query structure**. None of
them can be triggered in isolation because they all fire on the
same code path (nested CTE_LOOKUP with `setAggregation`). Rather
than inventing synthetic per-bug test queries, we use one target
query and let its failure mode progress as each bug is fixed.

### Target query

```cpp
// cte0: GROUP BY grp, SUM(val) = {(1,30),(2,70),(3,50)}
// cte1: scan cte_src, for each row lookupCte(0) by grp,
//       aggregate via NdbAggregator(virtTab) —
//       GroupBy(grp)
//       LoadLinkedColumn(1, 0, totalCol)  [cte0.total]
//       Sum(linked_total)
//     = {(1, 30+30=60), (2, 70+70=140), (3, 50)}
// main: scan cte_src, lookupCte(1) by grp — 5 rows
```

Structure:
```cpp
/* CTE 0 — existing Test 2 pattern */
qb->beginCteSubtree(0);
  scanA = qb->scanTable(src);
  qb->readTuple(src, {linked(scanA,"pk")}, opts_with_cte0Agg);
qb->endCteSubtree();
qb->defineCte(0, src, cte0Agg, /*depMask=*/0);

/* CTE 1 — NEW: lookupCte() INSIDE the subtree */
qb->beginCteSubtree(1);
  scanB = qb->scanTable(src);
  qb->lookupCte(0, 2, virt, {linked(scanB,"grp")},
                opts_with_cte1Agg_MatchNonNull);
qb->endCteSubtree();
qb->defineCte(1, virt, cte1Agg, /*depMask=*/1);  // depends on CTE 0

/* Main query */
scanM = qb->scanTable(src);
qb->lookupCte(1, 2, virt, {linked(scanM,"grp")}, mainOpts);
```

Expected when all three bugs are fixed: 5 output rows, each with
`(grp, cte1.total)` matching the table above. Stored as an expected
result map keyed by `grp`.

## Incremental Fix Sequence — One Bug at a Time

The order below fixes bugs in the order they manifest at runtime,
which is the reverse of how they appear in the source: build-time
validation (Bug 3) fails first, then main-root selection (Bug 2)
fails during phase-start / main-start, then the routing bug (Bug 1)
fails during row delivery.

Each step is two commits:
1. Test commit — adds or refines Test 5 so it fails at the next
   bug. After this commit, `testCteNdbApi` is RED with a specific
   symptom that proves the bug exists.
2. Fix commit — applies the Edit for that bug. After this commit,
   `testCteNdbApi` fails *further along* (or passes on the last
   step), proving the fix works.

The branch is temporarily red between each pair. This is fine on a
dev branch but **the full sequence (6 commits) must land together**
before merging upstream.

### Step 1 — Test 5 skeleton exposes Bug 0 (observed: DBLQH hang)

**Commit 1a (test, fails):** Add Test 5 with the full target query
structure. Wire it into `main()` after Test 4. Run it.

**Originally predicted symptom:** `DbspjErr::InvalidAggregateFlags`
from `validateAggregateFlags` because the nested `lookupCte(0)`
with `setAggregation` was expected to be mis-classified as a
main-query aggregate leaf.

**Actually observed symptom (2026-04-14 run):** Test 5 passes Tests
1-4, then on Test 5:
- CTE 0 materializes successfully (trace shows
  `handleCtePhaseComplete` at `DbspjMain.cpp:6551` reached for
  phase 0).
- DBTC sends `CTE_PHASE_START_REQ` for phase 1.
- DBLQH threads (instances 1 and 2) get stuck processing
  `SCAN_FRAGREQ` for CTE 1's scan — watchdog fires after ~3
  seconds reporting `Ndb kernel thread 1/2 is stuck in:
  JobHandling in block: 247, gsn: 353`. Node is killed and MTR
  reports a data node crash.

**Why the prediction was wrong:** `validateAggregateFlags` at
`DbspjMain.cpp:2033` early-returns when `RT_AGGREGATE` is not set
on the Request. `RT_AGGREGATE` is only set at
`DbspjMain.cpp:13107-13110` when `ctx.m_cteSubtreeRemaining == 0`
AND `NI_AGGREGATE` is set on the node. Test 5's main query has no
aggregate leaves, so main-query nodes don't get `NI_AGGREGATE`,
and CTE-subtree nodes do set `NI_AGGREGATE` but are inside a
subtree so `cteSubtreeRemaining > 0`. The net result: the
validateAggregateFlags body never executes for Test 5, and **Bug 3
as originally stated doesn't manifest with this query
structure**.

**What the observed symptom reveals — crash root cause:**

The ~3s "hang" is not a real deadlock, it's a watchdog catching
a thread that is about to segfault. The actual backtrace:

```
JoinAggInterpreter::initGBTypes
JoinAggInterpreter::ProcessRec
JoinAggInterpreter::processRecWithLinkedAttrs
Dbtup::handleJoinAggRow
Dbtup::prepareAndHandleJoinAggRow
Dbtup::interpreterStartLab
Dbtup::handleReadReq
Dbtup::execTUPKEYREQ
Dblqh::next_scanconf_tupkeyreq     ← scan path
...
Dblqh::execSCAN_FRAGREQ
```

**Root cause — a design conflict in the CTE subtree model:**

`DbspjMain.cpp:10601-10609` sets `JoinAggFlag=1` on the
SCAN_FRAGREQ of **every** T_CTE_SCAN scan node:

```c++
if (treeNodePtr.p->m_bits & TreeNode::T_CTE_SCAN) {
  ScanFragReq::setJoinAggFlag(req->requestInfo, 1);
  agg_extra = 1;   // ... cteAggKey attached below
}
```

This means DBLQH runs the CTE's aggregator interpreter on every
row the scan produces. For Test 2's CTE 0 this works: `cte0Agg`
uses `GroupBy("grp")` + `LoadColumn("val", 0)` — direct column
references that exist in the scan row.

For Test 5's CTE 1, `cte1Agg` uses `GroupByLinked(0, grpCol)` +
`LoadLinkedColumn(1, 0, totalCol)` because its inputs are
supposed to come from the CTE_LOOKUP(0) result rows, not from
the scanned base-table rows. When DBLQH passes a scan row
through this linked-column program, `initGBTypes` walks a
non-existent linked-attr buffer (`m_linked_attr_data` points at
data that does not follow the CTE_LOOKUP linked-attr format),
reads garbage `tableId`/`linkedAttrId`, then dereferences an
invalid `tab->tabDescriptor` — segfault at `initGBTypes + 1476`.

**The design gap:**

The existing CTE subtree model assumes *the base scan IS the
aggregator feed*. Test 2's CTE 0 fits this because its
aggregator uses direct scan columns. Part A's CTE 1 breaks this
assumption: the intended feed is **the CTE_LOOKUP result**, not
the scan row. The base scan should just drive the subtree (one
CTE_LOOKUP_REQ per scanned row); only the CTE_LOOKUP result
should flow into the aggregator.

**Revised bug list for Part A:**

- **Bug 0 (new — the blocker):** `scanFrag_send` sets
  `JoinAggFlag=1` on T_CTE_SCAN scans unconditionally, which
  routes scan rows through the CTE's aggregator. For CTE 1 the
  aggregator uses linked columns that scan rows don't carry,
  causing a segfault. The fix needs to recognize when a subtree
  feeds its aggregator indirectly (via a nested CTE_LOOKUP with
  `setAggregation`) and suppress `JoinAggFlag` on the scan in
  that case. Additionally the scan must not be the
  aggregation-feeder at all — the nested CTE_LOOKUP's
  `cte_lookup_send` at `DbspjMain.cpp:5861-5884` already does
  the right thing via `joinAggStateKey` when `m_cteId` is set
  (which is Bug 1).

- **Bug 1 (still valid):** `m_cteId` not set on nested
  CTE_LOOKUP nodes inside a subtree. Even after Bug 0 is fixed,
  `cte_lookup_send` needs `m_cteId != RNIL` to route the
  CTE_LOOKUP result into CTE 1's `joinAggStateKey` rather than
  main's.

- **Bug 2 (still valid but may not fire in Test 5):** Main-root
  selection picks up CTE_LOOKUP nested in subtree. Need to
  verify whether Test 5 even gets to main-root selection after
  Bug 0 + Bug 1 fixes land.

- **Bug 3 (doesn't fire for Test 5):** `validateAggregateFlags`
  short-circuits because `RT_AGGREGATE` is not set on the
  Request. Test 5's main query has no aggregate leaves, so
  `NI_AGGREGATE` on main ops is cleared and RT_AGGREGATE stays
  off. Bug 3 would only manifest in a query where the main also
  aggregates *and* references a nested CTE_LOOKUP from a
  subtree. Keep the fix in the plan but demote its priority.

**Next step:** investigate Bug 0 fix options. The cleanest
change is at build time: when a CTE subtree contains a nested
CTE_LOOKUP/CTE_SCAN with `setAggregation` (indirect-feed case),
mark the subtree's base scan with a new bit (e.g.
`T_CTE_SCAN_INDIRECT`) or just clear the JoinAggFlag branch
under a condition. The scan still scans, produces rows to DBSPJ,
and DBSPJ drives CTE_LOOKUP_REQs via `cte_parent_row` → the
CTE_LOOKUP result feeds the aggregator via `joinAggStateKey`
(Bug 1 fix required here).

Verification of Bug 0: rebuild `testCteNdbApi`, run it — Tests
1-4 pass, Test 5 causes a data node crash. Backtrace confirms
crash in `JoinAggInterpreter::initGBTypes` via
`handleJoinAggRow` on the scan path. **The test has
successfully exposed Bug 0.**

## Post-Mortem — What the Six Fixes Actually Were

The original three-bug plan turned out to be incomplete. Working
through Test 5 surfaced three more bugs underneath the ones the
plan anticipated. Final tally — all six fixed in commit
`35eadbe067b`:

### Bug 0 — `JoinAggFlag` set on every `T_CTE_SCAN` scan

**Symptom:** segfault in `JoinAggInterpreter::initGBTypes` on the
DBLQH-side scan-row interpreter path, caught by the watchdog
after ~3s of "scanning". Backtrace:
`Dblqh::execSCAN_FRAGREQ → ... → Dbtup::handleJoinAggRow →
processRecWithLinkedAttrs → ProcessRec → initGBTypes`.

**Root cause:** `scanFrag_send` at `DbspjMain.cpp:10601-10609`
unconditionally set `JoinAggFlag=1` and attached the CTE
aggStateKey for every `T_CTE_SCAN` scan, routing scan rows
through the CTE's aggregator interpreter in DBLQH. For CTE 1 the
aggregator program uses `GroupByLinked` / `LoadLinkedColumn` —
its inputs are CTE_LOOKUP result rows, not base scan rows —
and `initGBTypes` walked an invalid linked-attr buffer.

**Fix:** introduce `TreeNode::T_CTE_INDIRECT_FEED`. Set on the
subtree's base scan when the subtree's aggregate leaf is a
nested `QN_CTE_LOOKUP` / `QN_CTE_SCAN`. `scanFrag_send` skips
the JoinAggFlag branch for these scans; rows flow back to DBSPJ
as plain TRANSID_AI and drive `cte_parent_row` →
`cte_lookup_send` instead.

### Bug 1 — `m_cteId` not set on nested CTE_LOOKUP / CTE_SCAN

**Symptom:** `cte_lookup_send` routed the nested CTE_LOOKUP's
result to the main aggregator's `aggStateKey` instead of the
enclosing CTE 1's, because `treeNodePtr.p->m_cteId == RNIL`.

**Root cause:** the build loop at `DbspjMain.cpp:1945-1968` only
set `m_cteId` inside the `if (op != QN_CTE_LOOKUP && op !=
QN_CTE_SCAN)` branch (the same branch that marks `T_CTE_SCAN`).
Nested CTE_LOOKUP / CTE_SCAN children kept the default RNIL.

**Fix:** lift the `m_cteId` assignment out of the branch so
every node inside a subtree is tagged with the enclosing cteId.

### Bug 4 — pre-build CteContext metadata update silently no-ops

**Symptom (after Bug 0 fix):** all CTEs ran in parallel at phase
0 even though CTE 1 had `depMask=1`. CTE 0 wasn't yet
`CTE_READY` when CTE 1's scan rows arrived → `cte_parent_row`
queued lookups that never flushed.

**Root cause:** DBSPJ's aggKeys parse block at line 1436 runs
**before** `build()`. The `for (i=0; i<m_numCtes; i++)` loop
that updates `m_cteContexts[i].m_phase / m_depMask / m_flags`
iterates 0 times because `m_numCtes` is 0 at parse time —
`cte_build()` only creates the contexts later. The phase write
silently vanishes; contexts get the default `phase=0` and
`m_ctePhaseCount` is computed as 1.

**Fix:** save the parsed CTE metadata into a stack-local
`ParsedCteMeta parsedCteMeta[64]` temp array during the parse
block. After `build()` returns, walk the temp array and apply
the metadata to the now-existing `m_cteContexts[i]`. Recompute
`m_ctePhaseCount` from the real per-context phases.

### Bug 5 — dead `CTE_MATERIALIZING` state check

**Symptom (after Bug 4 fix):** even though CTE 1 now had
`phase=1` correctly, `execCTE_PHASE_START_REQ(phase=1)` did not
transition CTE 0 from `CTE_NOT_STARTED` to `CTE_READY`.
`cte_parent_row` again saw the not-ready state and queued
lookups forever.

**Root cause:** the transition at `DbspjMain.cpp:6664-6669` was
gated on `ctx.m_state == CteContext::CTE_MATERIALIZING`, but
**`CTE_MATERIALIZING` is never assigned anywhere in the
codebase**. CTE contexts only ever go from `CTE_NOT_STARTED` to
`CTE_READY` (either via this dead-coded branch, or via
`execCTE_START_MAIN_REQ` which sets all unconditionally). For
single-phase queries the latter rescue path masked the bug; for
multi-phase queries it surfaced.

**Fix:** drop the `CTE_MATERIALIZING` check. Transition any
earlier-phase CTE that isn't already `CTE_READY` / `CTE_FAILED`.

### Bug 2 — main-root selection picks nested CTE_LOOKUP

**Symptom (after Bug 5 fix):** `execCTE_START_MAIN_REQ` logged
`start root node 5 (all 2 CTEs READY)` — node 5 is the nested
`lookupCte(0)` inside CTE 1's subtree, not the main scan
(which is node 6). Starting the nested CTE_LOOKUP as the "main
scan" did nothing — outstanding immediately reached 0, the
query terminated with no rows delivered, and the test never
saw output.

**Root cause:** the main-root selection iteration at line 6738
filtered on `!(m_bits & T_CTE_SCAN) && m_info != &g_CteSubtreeOpInfo`.
With Bug 1 fixed (m_cteId set on nested CTE_LOOKUP) but Bug 0's
T_CTE_SCAN exclusion still in place, the nested CTE_LOOKUP has
neither bit and was the first match.

**Fix:** replace the bit-pair filter with `m_cteId == RNIL` —
the canonical "not part of a subtree" predicate. (The original
plan called this "Edit 2 — Main-root selection" with two sites,
but only the `execCTE_START_MAIN_REQ` site fires on this path.)

### Bug 6 — `m_rows` over-counts T_CTE_INDIRECT_FEED scan rows

**Symptom (after Bug 2 fix):** TRANSID_AI rows flowed correctly,
data node completed the query and sent SCAN_TABCONF with
`requestInfo=0x80000001` (op_count=1, EndOfData), `rowCount=15`.
The API processed it but `worker->isFragBatchComplete()` was
false — outstanding accounting was off by 5. With 1 worker and
`m_pendingWorkers` stuck at 1, `nextResult` blocked forever.

**Root cause:** `scanFrag_execSCAN_FRAGCONF` at line 11566
unconditionally accumulated `m_rows += rows` whenever
`m_aggNodes.isclear()` was true (i.e. no main aggregation). The
T_CTE_INDIRECT_FEED scan returns 5 rows to DBSPJ to drive
`cte_parent_row` for the nested CTE_LOOKUP, but those rows are
**not delivered to the API**. They were nonetheless added to
`m_rows`, which becomes `SCAN_FRAGCONF::completedOps` and then
the API's per-worker `rowCount` in SCAN_TABCONF. The API
expected 15 TRANSID_AI rows but only 10 arrived (5 for main
scan + 5 for main CTE_LOOKUP); outstanding stayed at 5; hang.

**Fix:** add `&& !(m_bits & T_CTE_INDIRECT_FEED)` to the
accumulation condition. Indirect-feed scan rows do not
contribute to the API row count.

### Bug 3 — does not fire for Test 5

The original plan called this `validateAggregateFlags` mis-
classification. The check is gated on `RT_AGGREGATE` which is
set only when a node has `NI_AGGREGATE` AND
`ctx.m_cteSubtreeRemaining == 0`. For Test 5 the main query has
no aggregate leaves, so no main op gets `NI_AGGREGATE`, so
`RT_AGGREGATE` stays off, so `validateAggregateFlags` early-
returns. The bug is real but only manifests on a query where
the main is **also** an aggregation that references a nested
CTE_LOOKUP — left for a future test.

## Verification

`testCteNdbApi` Tests 1-5 all pass on the fixed build. Tests 1-4
unchanged (single-CTE patterns); Test 5 produces the expected 5
rows with correct `cte1.total` values per `grp`.

Regression suites still to run before merging upstream:
`testCteDbtc`, `ndb_push_agg` MTR suite, `testJoinAggSpj`.

## What's Explicitly NOT in Part A

- **Query planner emission** (`QueryPlanner.cpp`) — deferred; we're
  testing via direct NDB API construction.
- **CTE_SCAN inside a subtree** (reading one CTE via full scan from
  another) — the build-loop fix will propagate `m_cteId` to
  CTE_SCAN too, but the API surface (`scanCte()`) doesn't exist yet
  and adding it is a separate piece of work.
- **Multi-level CTE chains** (CTE 2 depends on CTE 1 depends on CTE
  0) — phase machinery should handle this once Part A basic case
  works, but not explicitly tested here.
- **Outer-join CTE_LOOKUP** — separate milestone.
