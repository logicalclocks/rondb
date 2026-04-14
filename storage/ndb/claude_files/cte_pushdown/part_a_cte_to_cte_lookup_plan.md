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

**Commit 1b (fix):** Apply Edit 3 (`validateAggregateFlags` —
replace `!(m_bits & T_CTE_SCAN)` with `m_cteId == RNIL` at the four
main-leaf gating sites plus the `cteNodeCount` loop at 2068-2080).

Verification: rebuild, rerun. Test 5 now fails further along —
either with wrong results (Bug 2 starts CTE_LOOKUP as main root,
producing 0 rows or garbage) or with an assertion fire from
attempting to start a CTE_LOOKUP as a main-root scan. **Bug 3
verified fixed; Bug 2 now exposed.**

### Step 2 — Test 5 exposes Bug 2 after Bug 3 fix

**Commit 2a (test, refines expectation):** Update Test 5's error
reporting so the Bug 2 symptom is clearly distinguished from the
Bug 3 symptom (e.g. "expected <N> rows but got 0, main root is
not a scan"). If the symptom is an assertion crash, add a note in
the test comment describing how to recognise it.

No new code behaviour change — this commit just makes the red
state legible.

**Commit 2b (fix):** Apply Edit 2 (main-root selection at
`DbspjMain.cpp:3115` and `6650`, replace `!T_CTE_SCAN &&
!g_CteSubtreeOpInfo` with `m_cteId == RNIL`).

Verification: rebuild, rerun. Test 5 now fails further along.
Build + main-root works, CTE 0 materializes, CTE 1 begins
materialization phase, but CTE_LOOKUP results are routed to
main's aggregator due to Bug 1. Expected symptom: Test 5 receives
wrong `cte1.total` values (either zero, or main-aggregator
corruption), or main query produces wrong row count. **Bug 2
verified fixed; Bug 1 now exposed.**

### Step 3 — Test 5 exposes Bug 1 after Bugs 2 & 3 fixes

**Commit 3a (test, refines expectation):** Update Test 5's
failure message for the Bug 1 symptom. Can add a
`DEB_CTE_API`-gated trace dump of the per-`grp` `cte1.total`
values so the routing corruption is easy to see in the log.

**Commit 3b (fix):** Apply Edit 1 (build loop at
`DbspjMain.cpp:1945-1968`, lift the `m_cteId` assignment out of
the T_CTE_SCAN branch).

Verification: rebuild, rerun. Test 5 should **pass**. All three
bugs verified fixed end-to-end, and the full CTE-to-CTE lookup
path works.

### Step 4 — Regression

After all three bugs are fixed and Test 5 passes:

1. Full `testCteNdbApi` suite — Tests 1-4 must still pass
   (sanity check that our fixes didn't break them).
2. `testCteDbtc` — regression for the signal-level CTE path.
3. `ndb_push_agg` MTR suite — catches anything that relied on
   the old `!T_CTE_SCAN` predicate that we replaced with
   `m_cteId == RNIL`.

If any regression appears, likely suspects are the
`validateAggregateFlags` edits (`m_cteId == RNIL` might be
stricter or looser than `!T_CTE_SCAN` for some edge case we
didn't anticipate — in particular for existing CTE 0 subtrees
where T_CTE_SCAN is set and m_cteId is also set; both conditions
should give the same answer, but we must verify for containers
and for the `m_info == &g_CteSubtreeOpInfo` case).

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
