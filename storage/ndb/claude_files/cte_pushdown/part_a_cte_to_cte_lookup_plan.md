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

## Test: testCteNdbApi Test 5

Add a new test exercising CTE-to-CTE lookup:

```cpp
// cte0: GROUP BY grp, SUM(val) = {(1,30),(2,70),(3,50)}
// cte1: scan cte_src, for each row lookupCte(0) by grp,
//       aggregate via NdbAggregator(virtTab) —
//       GroupBy(grp)   [scan row]
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

Expected: 5 output rows (or main aggregation if we prefer to sum the
cte1 totals).

## Verification Plan

1. Build `ndbmtd` + `testCteNdbApi`
2. Run full testCteNdbApi suite — Tests 1-4 must still pass
3. Run Test 5 — debug whatever surfaces (build validation failures,
   phase sequencing, key routing, agg-state corruption)
4. If it passes: run `testCteDbtc` regression to ensure nothing
   broke in the signal-level CTE path
5. Run `ndb_push_agg` MTR suite

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
